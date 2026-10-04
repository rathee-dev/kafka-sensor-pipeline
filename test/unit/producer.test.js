import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DEFAULTS, loadConfig } from '../../src/lib/config.js';
import { buildEventStream } from '../../src/lib/events.js';
import { createLogger } from '../../src/lib/logger.js';
import { SensorProducer } from '../../src/producer.js';

const silentLogger = createLogger({ level: 'silent', name: 'test' });

function createFakeProducer({ failOnSend = false } = {}) {
  return {
    connected: false,
    disconnects: 0,
    sends: [],
    async connect() {
      this.connected = true;
    },
    async disconnect() {
      this.connected = false;
      this.disconnects += 1;
    },
    async send({ topic, messages }) {
      if (failOnSend) throw new Error('broker unavailable');
      this.sends.push({ topic, messages });
      return { topic, count: messages.length };
    },
  };
}

function producerWith(env, producerOptions) {
  const config = loadConfig({
    KAFKA_BROKERS: 'localhost:9092',
    SENSOR_COUNT: '3',
    READINGS_PER_SENSOR: '4',
    PUBLISH_INTERVAL_MS: '0',
    EVENT_SEED: '42',
    ...env,
  });
  const producer = createFakeProducer(producerOptions);
  const instance = new SensorProducer({
    kafka: null,
    config,
    logger: silentLogger,
    producer,
    randomIdFactory: () => 'fixed-random',
  });
  return { instance, producer, config };
}

describe('SensorProducer', () => {
  it('connects, publishes one record per reading to the readings topic, then disconnects', async () => {
    const { instance, producer, config } = producerWith();

    await instance.connect();
    assert.equal(producer.connected, true);

    const result = await instance.run();

    assert.deepEqual(result, { sent: 12, failed: 0 });
    assert.equal(config.sensorCount * config.readingsPerSensor, 12);
    const totalRecords = producer.sends.reduce((sum, send) => sum + send.messages.length, 0);
    assert.equal(totalRecords, 12, 'every reading was published');
    for (const send of producer.sends) assert.equal(send.topic, config.topic);

    await instance.disconnect();
    assert.equal(producer.connected, false);
    assert.equal(producer.disconnects, 1);
  });

  it('keys records by sensorId and sends parseable JSON with headers', async () => {
    const { instance, producer } = producerWith();
    const events = buildEventStream({
      sensorCount: 2,
      readingsPerSensor: 2,
      seed: 1,
      runId: 'r',
    });

    await instance.publish(events);

    assert.equal(producer.sends.length, 1, 'a single batch');
    const { messages } = producer.sends[0];
    assert.deepEqual(messages.map((message) => message.key), ['sensor-01', 'sensor-02', 'sensor-01', 'sensor-02']);
    for (const [index, message] of messages.entries()) {
      const decoded = JSON.parse(message.value);
      assert.equal(decoded.eventId, events[index].eventId);
      assert.deepEqual(decoded, events[index]);
      assert.equal(message.headers['event-type'], 'sensor.reading');
      assert.equal(message.headers['schema-version'], '1');
    }
  });

  it('honours KEY_MODE=roundrobin (null keys) and KEY_MODE=random (scattered keys)', async () => {
    const roundrobin = producerWith({ KEY_MODE: 'roundrobin' });
    await roundrobin.instance.publish(buildEventStream({ sensorCount: 2, readingsPerSensor: 2, seed: 1, runId: 'r' }));
    assert.deepEqual(roundrobin.producer.sends[0].messages.map((m) => m.key), [null, null, null, null]);

    const random = producerWith({ KEY_MODE: 'random' });
    await random.instance.publish(buildEventStream({ sensorCount: 2, readingsPerSensor: 2, seed: 1, runId: 'r' }));
    assert.deepEqual(random.producer.sends[0].messages.map((m) => m.key), ['fixed-random', 'fixed-random', 'fixed-random', 'fixed-random']);
  });

  it('surfaces broker failures instead of pretending the data was sent', async () => {
    const { instance, producer } = producerWith({}, { failOnSend: true });
    await assert.rejects(() => instance.publish(buildEventStream({ sensorCount: 1, readingsPerSensor: 1, seed: 1, runId: 'r' })), /broker unavailable/);
    assert.equal(instance.failed, 1);
    assert.equal(instance.sent, 0);
    assert.deepEqual(producer.sends, []);
  });

  it('creates an idempotent producer by default', () => {
    const config = loadConfig({ KAFKA_BROKERS: 'localhost:9092' });
    assert.equal(config.keyMode, DEFAULTS.keyMode);

    let capturedOptions = null;
    const kafka = {
      producer: (options) => {
        capturedOptions = options;
        return createFakeProducer();
      },
    };
    new SensorProducer({ kafka, config, logger: silentLogger });
    assert.equal(capturedOptions.idempotent, true);
    assert.equal(capturedOptions.allowAutoTopicCreation, false);
  });
});