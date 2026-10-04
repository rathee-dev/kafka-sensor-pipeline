/**
 * End-to-end test against a real broker.
 *
 * Runs automatically in CI (GitHub Actions starts a Kafka service container)
 * and locally whenever KAFKA_BROKERS is exported. Skipped otherwise, so
 * `npm test` still works on a laptop with nothing installed:
 *
 *   KAFKA_BROKERS=localhost:9092 KAFKA_SSL=false npm run test:integration
 *   SKIP_INTEGRATION=1 npm run test:integration     # force skip
 */

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { SensorConsumer } from '../../src/consumer.js';
import { loadConfig } from '../../src/lib/config.js';
import { buildEventStream } from '../../src/lib/events.js';
import { createKafka, ensureTopics } from '../../src/lib/kafka.js';
import { createLogger } from '../../src/lib/logger.js';
import { SensorProducer } from '../../src/producer.js';

const brokers = (process.env.KAFKA_BROKERS ?? '').trim();
const skip = !brokers || process.env.SKIP_INTEGRATION === '1';

const runId = `it-${Date.now()}`;
const TOPIC = `it-readings-${runId}`;
const ANOMALY_TOPIC = `it-anomalies-${runId}`;
const DLT_TOPIC = `it-dlt-${runId}`;
const SENSOR_COUNT = 3;
const READINGS = 5;
const EXPECTED_READINGS = SENSOR_COUNT * READINGS;

const logger = createLogger({ level: process.env.LOG_LEVEL ?? 'warn', name: 'integration' });

function testConfig(extra = {}) {
  return loadConfig({
    KAFKA_BROKERS: brokers,
    KAFKA_SSL: process.env.KAFKA_SSL ?? 'false',
    KAFKA_USERNAME: process.env.KAFKA_USERNAME ?? '',
    KAFKA_PASSWORD: process.env.KAFKA_PASSWORD ?? '',
    KAFKA_SASL_MECHANISM: process.env.KAFKA_SASL_MECHANISM ?? '',
    KAFKA_CLIENT_ID: `it-${runId}`,
    KAFKA_READINGS_TOPIC: TOPIC,
    KAFKA_ANOMALY_TOPIC: ANOMALY_TOPIC,
    KAFKA_DLT_TOPIC: DLT_TOPIC,
    KAFKA_PARTITIONS: '2',
    KAFKA_REPLICATION_FACTOR: '1',
    ANOMALY_HIGH_C: '80',
    PUBLISH_INTERVAL_MS: '0',
    ...extra,
  });
}

describe('pipeline against a real Kafka broker', { skip, concurrency: false }, () => {
  const state = { summary: null, stats: null, appended: [], deadLetters: [] };

  it('provisions the topics with the requested partition count', async () => {
    const config = testConfig();
    const kafka = createKafka(config, { logLevel: config.logLevel });
    const result = await ensureTopics(kafka, {
      topics: [TOPIC, ANOMALY_TOPIC, DLT_TOPIC],
      partitions: 2,
      replicationFactor: 1,
      logger,
      timeout: 60_000,
    });

    assert.deepEqual(result.created.sort(), [ANOMALY_TOPIC, DLT_TOPIC, TOPIC].sort());
    assert.deepEqual(result.partitionCounts, { [TOPIC]: 2, [ANOMALY_TOPIC]: 2, [DLT_TOPIC]: 2 });
  });

  it('publishes keyed readings, one replayed event and one poison message', async () => {
    const config = testConfig();
    const kafka = createKafka(config, { logLevel: config.logLevel });
    const producer = new SensorProducer({ kafka, config, logger });
    await producer.connect();

    const events = buildEventStream({
      sensorCount: SENSOR_COUNT,
      readingsPerSensor: READINGS,
      seed: 1234,
      runId,
    });

    try {
      for (let index = 0; index < events.length; index += SENSOR_COUNT) {
        await producer.publish(events.slice(index, index + SENSOR_COUNT));
      }

      // At-least-once delivery in practice: re-send one already published event.
      await producer.publish([events[0]]);

      // A poison message a naive consumer would crash on.
      await producer.producer.send({
        topic: TOPIC,
        messages: [{ key: 'sensor-01', value: '{"sensorId": "sensor-01", "temp' }],
      });
    } finally {
      await producer.disconnect();
    }
  });

  it('consumes, aggregates per sensor and dead-letters the poison message', async () => {
    const config = testConfig({ KAFKA_CONSUMER_GROUP: `it-analytics-${runId}`, CONSUMER_IDLE_TIMEOUT_MS: '6000' });
    const kafka = createKafka(config, { logLevel: config.logLevel });
    const sideProducer = kafka.producer({ allowAutoTopicCreation: false });
    await sideProducer.connect();

    const consumer = new SensorConsumer({
      kafka,
      config,
      logger,
      producer: sideProducer,
      idleTimeoutMs: 6_000,
      appendRecord: async (record) => {
        state.appended.push(record);
        if (record.type === 'dead-letter') state.deadLetters.push(record);
      },
      writeSummaryFile: async (snapshot) => {
        state.summary = snapshot;
      },
    });

    try {
      await consumer.run();
    } finally {
      await sideProducer.disconnect();
    }
    state.stats = consumer.stats;
    const { summary, stats } = state;

    // Every reading arrived exactly once logically, despite the replay.
    assert.equal(summary.totals.readings, EXPECTED_READINGS, 'the replayed event was de-duplicated');
    assert.equal(summary.totals.sensors, SENSOR_COUNT);
    assert.equal(summary.totals.duplicates, 1);
    assert.ok(summary.totals.anomalies >= 1, 'the spiking sensor produced anomalies');

    // Keying by sensorId means per-sensor ordering holds and nothing is lost.
    assert.equal(summary.totals.outOfOrder, 0);
    assert.equal(summary.totals.sequenceGaps, 0);
    assert.equal(summary.totals.missingReadings, 0);
    for (const sensor of summary.sensors) {
      assert.equal(sensor.readings, READINGS);
      assert.equal(sensor.lastSequence, READINGS);
    }

    // 15 readings + 1 replay + 1 poison message were fetched; the replay went
    // through the happy path but did not skew the aggregation.
    assert.equal(stats.consumed, EXPECTED_READINGS + 2);
    assert.equal(stats.processed, EXPECTED_READINGS + 1);
    assert.equal(stats.duplicates, 1);
    assert.equal(stats.deadLettered, 1);
    assert.equal(state.appended.filter((record) => record.type === 'reading').length, EXPECTED_READINGS + 1);
  });

  it('republishes anomalies and dead letters onto their own topics', async () => {
    const config = testConfig();
    const kafka = createKafka(config, { logLevel: config.logLevel });

    const anomalies = await readOneRecord(kafka, ANOMALY_TOPIC, `it-anomaly-reader-${runId}`);
    assert.equal(anomalies.type, 'anomaly');
    assert.equal(anomalies.sensorId, 'sensor-01');
    assert.match(anomalies.reason, /temperatureC/);

    const deadLetter = await readOneRecord(kafka, DLT_TOPIC, `it-dlt-reader-${runId}`);
    assert.equal(deadLetter.type, 'dead-letter');
    assert.equal(deadLetter.reason, 'invalid-json');
    assert.equal(deadLetter.source.topic, TOPIC);
    assert.equal(state.deadLetters.length, 1);
  });
});

function readOneRecord(kafka, topic, groupId, timeoutMs = 20_000) {
  const consumer = kafka.consumer({ groupId });
  return (async () => {
    await consumer.connect();
    try {
      await consumer.subscribe({ topic, fromBeginning: true });
      return await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error(`no record arrived on ${topic} within ${timeoutMs}ms`));
        }, timeoutMs);
        consumer
          .run({
            eachMessage: async ({ message }) => {
              clearTimeout(timer);
              resolve(JSON.parse(message.value.toString('utf8')));
            },
          })
          .catch(reject);
      });
    } finally {
      await consumer.disconnect();
    }
  })();
}