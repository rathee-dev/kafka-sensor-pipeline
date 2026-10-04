/**
 * Kafka client construction + idempotent topic provisioning.
 *
 * The client factory takes the injected Kafka constructor so unit tests can
 * assert on the exact client options without opening a socket.
 */

import { Kafka, logLevel as KafkaLogLevel } from 'kafkajs';

export function buildClientOptions(config, { logLevel = 'info' } = {}) {
  const options = {
    clientId: config.clientId,
    brokers: config.brokers,
    ssl: config.ssl,
    connectionTimeout: 10_000,
    requestTimeout: 30_000,
    enforceRequestTimeout: false,
    retry: { initialRetryTime: 300, retries: 5 },
    logLevel: mapLogLevel(logLevel),
  };

  if (config.sasl) {
    options.sasl = {
      mechanism: config.sasl.mechanism,
      username: config.sasl.username,
      password: config.sasl.password,
    };
    // Redpanda Cloud and Confluent Cloud both need the transient variant of
    // the SASL handshake; older/self-hosted brokers want plain `init`.
    if (config.saslEndpoint) {
      options.saslOptions = { endpoint: config.saslEndpoint };
    }
  }

  return options;
}

function mapLogLevel(level) {
  switch (level) {
    case 'debug':
      return KafkaLogLevel.DEBUG;
    case 'trace':
      return KafkaLogLevel.INFO;
    default:
      return KafkaLogLevel.WARN;
  }
}

export function createKafka(config, { logLevel = 'info', KafkaCtor = Kafka } = {}) {
  return new KafkaCtor(buildClientOptions(config, { logLevel }));
}

/**
 * Creates the topics if they are missing and returns what was created.
 * Safe to run repeatedly - existing topics (and their partition counts) are
 * left untouched, which matters because re-shaping a partition changes the
 * key -> partition mapping.
 */
export async function ensureTopics(
  kafka,
  { topics, partitions = 3, replicationFactor = 1, logger, timeout = 30_000 } = {},
) {
  const admin = kafka.admin();
  await admin.connect();

  try {
    const existing = new Set(await admin.listTopics());
    const missing = topics.filter((topic) => !existing.has(topic));

    if (missing.length > 0) {
      await withTimeout(
        admin.createTopics({
          waitForLeaders: true,
          topics: missing.map((topic) => ({
            topic,
            numPartitions: partitions,
            replicationFactor,
          })),
        }),
        timeout,
        `createTopics(${missing.join(',')}) timed out`,
      );
      logger?.info(`created topics: ${missing.join(', ')}`, { partitions, replicationFactor });
    } else {
      logger?.debug('all topics already exist');
    }

    const descriptions = await admin.fetchTopicMetadata({ topics: missing.length > 0 ? missing : topics });
    const partitionCounts = {};
    for (const topic of descriptions.topics) {
      partitionCounts[topic.name] = topic.partitions.length;
    }

    return { created: missing, alreadyExisted: topics.filter((t) => existing.has(t)), partitionCounts };
  } finally {
    await admin.disconnect();
  }
}

function withTimeout(promise, ms, message) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}