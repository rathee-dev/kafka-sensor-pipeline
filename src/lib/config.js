/**
 * Environment -> validated runtime configuration.
 *
 * Kept as a pure function so it can be unit tested without a broker or a
 * process.env mutation.
 */

export class ConfigError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConfigError';
  }
}

export const DEFAULTS = Object.freeze({
  clientId: 'sensor-pipeline',
  topic: 'sensor-readings',
  anomalyTopic: 'sensor-anomalies',
  deadLetterTopic: 'sensor-readings-dlt',
  consumerGroup: 'sensor-analytics',
  partitions: 3,
  replicationFactor: 1,
  ssl: true,
  saslMechanism: 'scram-sha-256',
  saslEndpoint: 'init_transient',
  sessionTimeoutMs: 30_000,
  heartbeatIntervalMs: 3_000,
  sensorCount: 3,
  readingsPerSensor: 10,
  publishIntervalMs: 200,
  keyMode: 'sensor',
  eventSeed: 20_260_930,
  anomalyHighC: 80,
  anomalyLowC: -10,
  vibrationThresholdMmS: 5,
  consumerIdleTimeoutMs: 0,
  outputDir: 'output',
  logLevel: 'info',
});

const KEY_MODES = ['sensor', 'roundrobin', 'random'];
const TRUTHY = new Set(['1', 'true', 'yes', 'on', 'y']);
const FALSY = new Set(['0', 'false', 'no', 'off', 'n']);

export function parseBoolean(raw, fallback) {
  if (raw === undefined || raw === null || raw === '') return fallback;
  const value = String(raw).trim().toLowerCase();
  if (TRUTHY.has(value)) return true;
  if (FALSY.has(value)) return false;
  throw new ConfigError(`Expected a boolean-ish value but received "${raw}"`);
}

export function parseNumber(raw, fallback, name) {
  if (raw === undefined || raw === null || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    throw new ConfigError(`${name} must be a finite number, received "${raw}"`);
  }
  return value;
}

export function parseInteger(raw, fallback, name, { min = 0 } = {}) {
  const value = parseNumber(raw, fallback, name);
  if (!Number.isInteger(value)) {
    throw new ConfigError(`${name} must be an integer, received "${raw}"`);
  }
  if (value < min) {
    throw new ConfigError(`${name} must be >= ${min}, received "${raw}"`);
  }
  return value;
}

export function parseList(raw) {
  if (!raw) return [];
  return String(raw)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

/**
 * Builds the configuration object used by both the producer and the consumer.
 *
 * @param {NodeJS.ProcessEnv} env
 */
export function loadConfig(env = process.env) {
  const brokers = parseList(env.KAFKA_BROKERS);
  if (brokers.length === 0) {
    throw new ConfigError(
      'KAFKA_BROKERS is required, e.g. KAFKA_BROKERS=localhost:9092 ' +
        '(or redpanda-1234.aws.redpanda.com:9092 for a managed cluster)',
    );
  }

  const username = env.KAFKA_USERNAME || '';
  const password = env.KAFKA_PASSWORD || '';
  if (Boolean(username) !== Boolean(password)) {
    throw new ConfigError(
      'KAFKA_USERNAME and KAFKA_PASSWORD must be set together (got only one of them)',
    );
  }
  const useSasl = Boolean(username);
  const ssl = parseBoolean(env.KAFKA_SSL, useSasl ? DEFAULTS.ssl : false);

  const keyMode = (env.KEY_MODE || DEFAULTS.keyMode).trim().toLowerCase();
  if (!KEY_MODES.includes(keyMode)) {
    throw new ConfigError(`KEY_MODE must be one of ${KEY_MODES.join(', ')}, received "${env.KEY_MODE}"`);
  }

  const partitions = parseInteger(env.KAFKA_PARTITIONS, DEFAULTS.partitions, 'KAFKA_PARTITIONS', {
    min: 1,
  });
  const replicationFactor = parseInteger(
    env.KAFKA_REPLICATION_FACTOR,
    DEFAULTS.replicationFactor,
    'KAFKA_REPLICATION_FACTOR',
    { min: 1 },
  );
  if (replicationFactor > partitions) {
    throw new ConfigError(
      `KAFKA_REPLICATION_FACTOR (${replicationFactor}) cannot exceed KAFKA_PARTITIONS (${partitions})`,
    );
  }

  return {
    brokers,
    clientId: env.KAFKA_CLIENT_ID || DEFAULTS.clientId,
    ssl,
    sasl: useSasl
      ? {
          mechanism: env.KAFKA_SASL_MECHANISM || DEFAULTS.saslMechanism,
          username,
          password,
        }
      : undefined,
    saslEndpoint: env.KAFKA_SASL_ENDPOINT || DEFAULTS.saslEndpoint,
    topic: env.KAFKA_READINGS_TOPIC || DEFAULTS.topic,
    anomalyTopic: env.KAFKA_ANOMALY_TOPIC || DEFAULTS.anomalyTopic,
    deadLetterTopic: env.KAFKA_DLT_TOPIC || DEFAULTS.deadLetterTopic,
    partitions,
    replicationFactor,
    consumerGroup: env.KAFKA_CONSUMER_GROUP || DEFAULTS.consumerGroup,
    sessionTimeoutMs: parseInteger(
      env.KAFKA_SESSION_TIMEOUT_MS,
      DEFAULTS.sessionTimeoutMs,
      'KAFKA_SESSION_TIMEOUT_MS',
      { min: 1_000 },
    ),
    heartbeatIntervalMs: parseInteger(
      env.KAFKA_HEARTBEAT_INTERVAL_MS,
      DEFAULTS.heartbeatIntervalMs,
      'KAFKA_HEARTBEAT_INTERVAL_MS',
      { min: 100 },
    ),
    sensorCount: parseInteger(env.SENSOR_COUNT, DEFAULTS.sensorCount, 'SENSOR_COUNT', { min: 1 }),
    readingsPerSensor: parseInteger(
      env.READINGS_PER_SENSOR,
      DEFAULTS.readingsPerSensor,
      'READINGS_PER_SENSOR',
      { min: 1 },
    ),
    publishIntervalMs: parseInteger(
      env.PUBLISH_INTERVAL_MS,
      DEFAULTS.publishIntervalMs,
      'PUBLISH_INTERVAL_MS',
      { min: 0 },
    ),
    keyMode,
    eventSeed: parseInteger(env.EVENT_SEED, DEFAULTS.eventSeed, 'EVENT_SEED'),
    anomalyHighC: parseNumber(env.ANOMALY_HIGH_C, DEFAULTS.anomalyHighC, 'ANOMALY_HIGH_C'),
    anomalyLowC: parseNumber(env.ANOMALY_LOW_C, DEFAULTS.anomalyLowC, 'ANOMALY_LOW_C'),
    vibrationThresholdMmS: parseNumber(
      env.VIBRATION_THRESHOLD_MM_S,
      DEFAULTS.vibrationThresholdMmS,
      'VIBRATION_THRESHOLD_MM_S',
    ),
    consumerIdleTimeoutMs: parseInteger(
      env.CONSUMER_IDLE_TIMEOUT_MS,
      DEFAULTS.consumerIdleTimeoutMs,
      'CONSUMER_IDLE_TIMEOUT_MS',
      { min: 0 },
    ),
    outputDir: env.OUTPUT_DIR || DEFAULTS.outputDir,
    logLevel: (env.LOG_LEVEL || DEFAULTS.logLevel).toLowerCase(),
  };
}

export function redactedConfigSummary(config) {
  return {
    brokers: config.brokers,
    ssl: config.ssl,
    sasl: config.sasl ? `${config.sasl.mechanism} (user=${config.sasl.username})` : 'none',
    topic: config.topic,
    anomalyTopic: config.anomalyTopic,
    deadLetterTopic: config.deadLetterTopic,
    partitions: config.partitions,
    replicationFactor: config.replicationFactor,
    consumerGroup: config.consumerGroup,
    keyMode: config.keyMode,
  };
}