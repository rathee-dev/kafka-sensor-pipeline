import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { createAggregator } from '../../src/lib/aggregate.js';
import { loadConfig } from '../../src/lib/config.js';
import { buildEventStream } from '../../src/lib/events.js';
import { createLogger } from '../../src/lib/logger.js';
import { SensorConsumer } from '../../src/consumer.js';

const silentLogger = createLogger({ level: 'silent', name: 'test' });

function createFakeProducer() {
  return {
    sends: [],
    async send({ topic, messages }) {
      this.sends.push({ topic, messages });
      return { topic, count: messages.length };
    },
  };
}

/** Builds a consumer wired to in-memory sinks - no broker, no filesystem. */
function consumerWith(entries, env = {}) {
  const config = loadConfig({
    KAFKA_BROKERS: 'localhost:9092',
    ANOMALY_HIGH_C: '80',
    VIBRATION_THRESHOLD_MM_S: '5',
    ...env,
  });
  const producer = createFakeProducer();
  const records = [];
  const operations = [];
  const consumer = new SensorConsumer({
    kafka: null,
    config,
    logger: silentLogger,
    aggregator: createAggregator(config),
    producer,
    appendRecord: async (record) => {
      operations.push(`append:${record.type}`);
      records.push(record);
    },
    writeSummaryFile: async (snapshot) => {
      operations.push('summary');
      consumer.snapshotWritten = snapshot;
    },
  });

  const batch = {
    topic: config.topic,
    partition: 0,
    highWatermark: '10',
    messages: entries.map((entry, index) => {
      const isEvent = typeof entry === 'object' && entry !== null && !('raw' in entry);
      return {
        offset: String(index),
        key: isEvent ? entry.sensorId : null,
        value: Buffer.from(isEvent ? JSON.stringify(entry) : entry.raw),
        timestamp: String(Date.parse('2026-09-30T10:00:00.000Z')),
        headers: {},
      };
    }),
  };

  return { consumer, producer, records, operations, batch, config };
}

function sensorReadings(count = 3, overrides = {}) {
  return buildEventStream({
    sensorCount: 1,
    readingsPerSensor: count,
    seed: 7,
    runId: 'test-run',
    startTimeMs: Date.parse('2026-09-30T10:00:00.000Z'),
    spikeSensorIndex: -1, // no anomalies unless a test asks for them
    ...overrides,
  });
}

describe('SensorConsumer.handleBatch', () => {
  it('aggregates valid readings, writes one JSONL record each and resolves every offset', async () => {
    const events = sensorReadings(3);
    const { consumer, records, operations, batch } = consumerWith(events);

    const resolved = [];
    const outcomes = await consumer.handleBatch(batch, (offset) => {
      operations.push(`resolve:${offset}`);
      resolved.push(offset);
    });

    assert.deepEqual(outcomes, { processed: 3, deadLettered: 0, anomalies: 0, duplicates: 0 });
    assert.deepEqual(resolved, ['0', '1', '2']);
    assert.equal(records.length, 3);
    assert.equal(records.every((record) => record.type === 'reading'), true);
    assert.equal(records[0].event.eventId, events[0].eventId);
    assert.equal(records[0].partition, 0);
    assert.deepEqual(consumer.stats, {
      consumed: 3,
      processed: 3,
      duplicates: 0,
      outOfOrder: 0,
      gaps: 0,
      deadLettered: 0,
      anomaliesPublished: 0,
      batches: 1,
    });

    const snapshot = consumer.aggregator.snapshot();
    assert.equal(snapshot.totals.readings, 3);
    assert.equal(snapshot.sensors[0].sequenceGaps, 0);
  });

  it('resolves offsets only after the record was processed', async () => {
    const { consumer, operations, batch } = consumerWith(sensorReadings(2));
    await consumer.handleBatch(batch, (offset) => operations.push(`resolve:${offset}`));
    assert.deepEqual(operations, ['append:reading', 'resolve:0', 'append:reading', 'resolve:1']);
  });

  it('dead-letters unparseable payloads and keeps the partition moving', async () => {
    const entries = [
      sensorReadings(1)[0],
      { raw: '' },
      { raw: '{"sensorId": ' },
    ];
    const { consumer, producer, records, batch, config } = consumerWith(entries);

    const resolved = [];
    const outcomes = await consumer.handleBatch(batch, (offset) => resolved.push(offset));

    assert.equal(outcomes.processed, 1);
    assert.equal(outcomes.deadLettered, 2);
    assert.equal(outcomes.anomalies, 0);
    assert.deepEqual(resolved, ['0', '1', '2'], 'the bad offsets are still resolved');
    assert.equal(consumer.aggregator.snapshot().totals.readings, 1, 'bad data never reaches the aggregator');

    assert.equal(producer.sends.length, 2);
    for (const send of producer.sends) {
      assert.equal(send.topic, config.deadLetterTopic);
      const record = JSON.parse(send.messages[0].value);
      assert.equal(record.type, 'dead-letter');
      assert.equal(record.reason, 'invalid-json');
      assert.equal(record.source.partition, 0);
      assert.equal(send.messages[0].headers['event-type'], 'sensor.reading.dlt');
    }

    const dltRecords = records.filter((record) => record.type === 'dead-letter');
    assert.equal(dltRecords.length, 2);
    assert.equal(dltRecords[0].raw, '');
    assert.equal(dltRecords[1].raw, '{"sensorId": ');
    assert.ok(dltRecords[0].errors[0].length > 0);
  });

  it('dead-letters schema-invalid events with the failing fields listed', async () => {
    const entries = [sensorReadings(1)[0], { eventId: 'broken-1', sensorId: 'sensor-01' }];
    const { consumer, producer, batch, config } = consumerWith(entries);

    const outcomes = await consumer.handleBatch(batch);

    assert.equal(outcomes.deadLettered, 1);
    assert.equal(outcomes.processed, 1);
    const send = producer.sends.at(-1);
    assert.equal(send.topic, config.deadLetterTopic);
    const record = JSON.parse(send.messages[0].value);
    assert.equal(record.reason, 'schema-invalid');
    assert.ok(record.errors.includes('temperatureC is required'));
    assert.ok(record.errors.includes('sequence is required'));
  });

  it('re-publishes threshold breaches to the anomaly topic keyed by sensorId', async () => {
    const entries = [
      sensorReadings(1)[0],
      { ...sensorReadings(1)[0], eventId: 'hot-1', sequence: 2, temperatureC: 120, vibrationMmS: 9.9 },
    ];
    const { consumer, producer, batch, config } = consumerWith(entries);

    const outcomes = await consumer.handleBatch(batch);

    assert.equal(outcomes.anomalies, 1);
    assert.equal(producer.sends.length, 1);
    const send = producer.sends[0];
    assert.equal(send.topic, config.anomalyTopic);
    assert.equal(send.messages[0].key, 'sensor-01', 'anomalies stay ordered per sensor');
    const anomaly = JSON.parse(send.messages[0].value);
    assert.equal(anomaly.type, 'anomaly');
    assert.equal(anomaly.eventId, 'hot-1');
    assert.match(anomaly.reason, /temperatureC 120 > 80/);
    assert.equal(consumer.stats.anomaliesPublished, 1);
  });

  it('de-duplicates replayed records inside a single batch (idempotent consumer)', async () => {
    const event = sensorReadings(1)[0];
    const { consumer, batch } = consumerWith([event, { ...event }]);

    const outcomes = await consumer.handleBatch(batch);

    assert.equal(outcomes.processed, 2);
    assert.equal(outcomes.duplicates, 1);
    assert.equal(consumer.stats.duplicates, 1);
    const snapshot = consumer.aggregator.snapshot();
    assert.equal(snapshot.totals.readings, 1, 'the replay did not skew the averages');
    assert.equal(snapshot.totals.duplicates, 1);
  });

  it('survives an empty batch', async () => {
    const { consumer, batch } = consumerWith([]);
    const outcomes = await consumer.handleBatch(batch);
    assert.deepEqual(outcomes, { processed: 0, deadLettered: 0, anomalies: 0, duplicates: 0 });
    assert.equal(consumer.stats.batches, 1);
  });

  it('keeps working without a producer attached (logs instead of crashing)', async () => {
    const { consumer, records, batch } = consumerWith([{ raw: 'not json' }]);
    consumer.producer = null;

    const outcomes = await consumer.handleBatch(batch);

    assert.equal(outcomes.deadLettered, 1);
    assert.equal(records.filter((record) => record.type === 'dead-letter').length, 1);
  });
});