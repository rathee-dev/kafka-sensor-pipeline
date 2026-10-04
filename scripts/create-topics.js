/**
 * Creates the three topics if they are missing. Idempotent - run it before the
 * producer/consumer, or let the producer do it.
 */

import { loadConfig, redactedConfigSummary } from '../src/lib/config.js';
import { createKafka, ensureTopics } from '../src/lib/kafka.js';
import { createLogger } from '../src/lib/logger.js';

const config = loadConfig();
const logger = createLogger({ level: config.logLevel, name: 'topics' });
const kafka = createKafka(config, { logLevel: config.logLevel });

logger.info('provisioning topics', redactedConfigSummary(config));

ensureTopics(kafka, {
  topics: [config.topic, config.anomalyTopic, config.deadLetterTopic],
  partitions: config.partitions,
  replicationFactor: config.replicationFactor,
  logger,
})
  .then((result) => {
    logger.info(`created: [${result.created.join(', ')}]`, result.partitionCounts);
    logger.info(`already present: [${result.alreadyExisted.join(', ')}]`);
  })
  .catch((error) => {
    logger.error('topic provisioning failed', error.stack ?? error.message);
    process.exitCode = 1;
  });