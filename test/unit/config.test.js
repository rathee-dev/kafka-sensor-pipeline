import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { ConfigError, DEFAULTS, loadConfig, parseBoolean, parseList } from '../../src/lib/config.js';

describe('config', () => {
  it('requires KAFKA_BROKERS', () => {
    assert.throws(() => loadConfig({}), ConfigError);
    assert.throws(() => loadConfig({ KAFKA_BROKERS: '  ' }), /KAFKA_BROKERS is required/);
  });

  it('parses a comma separated broker list and applies defaults', () => {
    const config = loadConfig({ KAFKA_BROKERS: 'a:9092, b:9092 ,,c:9092' });
    assert.deepEqual(config.brokers, ['a:9092', 'b:9092', 'c:9092']);
    assert.equal(config.topic, DEFAULTS.topic);
    assert.equal(config.consumerGroup, DEFAULTS.consumerGroup);
    assert.equal(config.partitions, DEFAULTS.partitions);
    assert.equal(config.keyMode, 'sensor');
    // No credentials -> plaintext local broker defaults to TLS off.
    assert.equal(config.ssl, false);
    assert.equal(config.sasl, undefined);
  });

  it('enables TLS and SASL when credentials are supplied', () => {
    const config = loadConfig({
      KAFKA_BROKERS: 'redpanda-1.euw1.aws.redpanda.com:9092',
      KAFKA_USERNAME: 'me',
      KAFKA_PASSWORD: 'secret',
    });
    assert.equal(config.ssl, true);
    assert.equal(config.sasl.mechanism, 'scram-sha-256');
    assert.equal(config.sasl.username, 'me');
  });

  it('rejects half-configured credentials and bad enums/numbers', () => {
    assert.throws(
      () => loadConfig({ KAFKA_BROKERS: 'localhost:9092', KAFKA_USERNAME: 'me' }),
      /must be set together/,
    );
    assert.throws(
      () => loadConfig({ KAFKA_BROKERS: 'localhost:9092', KEY_MODE: 'nope' }),
      /KEY_MODE must be one of/,
    );
    assert.throws(
      () => loadConfig({ KAFKA_BROKERS: 'localhost:9092', KAFKA_PARTITIONS: 'lots' }),
      /must be a finite number/,
    );
    assert.throws(
      () => loadConfig({ KAFKA_BROKERS: 'localhost:9092', KAFKA_PARTITIONS: '2.5' }),
      /must be an integer/,
    );
    assert.throws(
      () =>
        loadConfig({
          KAFKA_BROKERS: 'localhost:9092',
          KAFKA_PARTITIONS: '1',
          KAFKA_REPLICATION_FACTOR: '3',
        }),
      /cannot exceed/,
    );
  });

  it('honours overrides and boolean coercion', () => {
    const config = loadConfig({
      KAFKA_BROKERS: 'localhost:9092',
      KAFKA_SSL: 'no',
      KAFKA_READINGS_TOPIC: 'readings',
      KAFKA_CONSUMER_GROUP: 'g1',
      SENSOR_COUNT: '6',
      ANOMALY_HIGH_C: '70.5',
      KEY_MODE: 'RANDOM',
      CONSUMER_IDLE_TIMEOUT_MS: '3000',
    });
    assert.equal(config.ssl, false);
    assert.equal(config.topic, 'readings');
    assert.equal(config.consumerGroup, 'g1');
    assert.equal(config.sensorCount, 6);
    assert.equal(config.anomalyHighC, 70.5);
    assert.equal(config.keyMode, 'random');
    assert.equal(config.consumerIdleTimeoutMs, 3000);

    assert.equal(parseBoolean('TRUE', false), true);
    assert.equal(parseBoolean('off', true), false);
    assert.equal(parseBoolean(undefined, true), true);
    assert.throws(() => parseBoolean('maybe', true), ConfigError);
    assert.deepEqual(parseList(''), []);
  });
});