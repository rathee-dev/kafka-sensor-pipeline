/**
 * Consumer: the analytics half of the pipeline.
 *
 * Responsibilities, in order of importance for the demo:
 *   1. process every record of the readings topic exactly once *logically*
 *      (at-least-once delivery + eventId de-duplication);
 *   2. commit offsets only after processing, so a crash replays instead of
 *      silently losing data;
 *   3. detect anomalies and re-publish them to a dedicated topic (fan-out);
 *   4. route un-parseable / schema-invalid records to a dead-letter topic
 *      instead of throwing and blocking the partition (poison pill);
 *   5. print + persist a summary when it shuts down.
 *
 * Several instances sharing KAFKA_CONSUMER_GROUP split the partitions between
 * them, which is the horizontal-scaling story.
 */

import { appendFile, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

import { createAggregator, formatSummary } from './lib/aggregate.js';
import { loadConfig, redactedConfigSummary } from './lib/config.js';
import { validateEvent } from './lib/events.js';
import { createKafka } from './lib/kafka.js';
import { createLogger } from './lib/logger.js';

const RAW_VALUE_LIMIT = 500;

export class SensorConsumer {
  constructor({
    kafka,
    config,
    logger,
    aggregator,
    appendRecord = makeJsonlAppender(config.outputDir),
    writeSummaryFile = makeSummaryWriter(config.outputDir),
    producer,
    idleTimeoutMs = 0,
  }) {
    this.kafka = kafka;
    this.config = config;
    this.logger = logger;
    this.aggregator = aggregator ?? createAggregator(config);
    this.appendRecord = appendRecord;
    this.writeSummaryFile = writeSummaryFile;
    this.idleTimeoutMs = idleTimeoutMs;
    this.producer = producer ?? null;
    this.consumer = kafka ? kafka.consumer({ groupId: config.consumerGroup }) : null;
    this.stopping = false;
    this.idleTimer = null;
    this.resolveStopped = null;
    this.stats = {
      consumed: 0,
      processed: 0,
      duplicates: 0,
      outOfOrder: 0,
      gaps: 0,
      deadLettered: 0,
      anomaliesPublished: 0,
      batches: 0,
    };
  }

  async connect() {
    await this.consumer.connect();
    this.consumer.on(this.consumer.events.GROUP_JOIN, (event) => {
      const payload = event.payload ?? {};
      this.logger.info(`joined group "${payload.groupId}" as ${payload.memberId}`, {
        assignedPartitions: Object.entries(payload.memberAssignment ?? {}).map(
          ([topic, partitions]) => `${topic}:[${partitions.join(',')}]`,
        ),
      });
    });
    this.consumer.on(this.consumer.events.CRASH, (event) => {
      this.logger.error('consumer crashed', event.payload?.error?.message);
    });
    this.logger.info(`consumer connected (group "${this.config.consumerGroup}")`);
  }

  resetIdleTimer(onIdle) {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    if (!this.idleTimeoutMs) return;
    this.idleTimer = setTimeout(onIdle, this.idleTimeoutMs);
    this.idleTimer.unref?.();
  }

  /**
   * Processes one fetched batch. Exposed separately from `run()` so tests can
   * drive it with synthetic batches.
   */
  async handleBatch(batch, resolveOffset = () => {}) {
    this.stats.batches += 1;
    const outcomes = { processed: 0, deadLettered: 0, anomalies: 0, duplicates: 0 };

    for (const message of batch.messages ?? []) {
      this.stats.consumed += 1;

      let event = null;
      let parseError = null;
      try {
        event = JSON.parse(message.value?.toString('utf8') ?? '');
      } catch (error) {
        parseError = error.message;
      }

      if (parseError) {
        outcomes.deadLettered += 1;
        this.stats.deadLettered += 1;
        await this.deadLetter({
          reason: 'invalid-json',
          errors: [parseError],
          message,
          batch,
        });
        resolveOffset(message.offset);
        continue;
      }

      const validation = validateEvent(event);
      if (!validation.valid) {
        outcomes.deadLettered += 1;
        this.stats.deadLettered += 1;
        await this.deadLetter({
          reason: 'schema-invalid',
          errors: validation.errors,
          message,
          batch,
        });
        resolveOffset(message.offset);
        continue;
      }

      const result = this.aggregator.ingest(event);
      if (result.status === 'duplicate') {
        outcomes.duplicates += 1;
        this.stats.duplicates += 1;
      }
      if (result.status === 'out_of_order') {
        this.stats.outOfOrder += 1;
      }
      if (result.status === 'gap') {
        this.stats.gaps += 1;
      }

      await this.appendRecord({
        type: 'reading',
        topic: batch.topic,
        partition: batch.partition,
        offset: message.offset,
        status: result.status,
        event,
      });

      this.logger.info(
        `[consumer] 📥 received ${event.sensorId} (seq ${event.sequence}): ` +
          `temp=${event.temperatureC}°C, vib=${event.vibrationMmS}mm/s ` +
          `[partition ${batch.partition}, offset ${message.offset}]`,
      );

      if (result.anomaly) {
        outcomes.anomalies += 1;
        await this.publishAnomaly(result.anomaly, event);
      }

      outcomes.processed += 1;
      this.stats.processed += 1;
      // Offset is resolved only after the record was durably handled.
      resolveOffset(message.offset);
    }

    this.logger.debug(
      `batch topic=${batch.topic} partition=${batch.partition} processed=${outcomes.processed} ` +
        `dlt=${outcomes.deadLettered} anomalies=${outcomes.anomalies}`,
    );

    return outcomes;
  }

  async deadLetter({ reason, errors, message, batch }) {
    const record = {
      type: 'dead-letter',
      reason,
      errors,
      deadLetteredAt: new Date().toISOString(),
      source: {
        topic: batch?.topic ?? null,
        partition: batch?.partition ?? null,
        offset: message.offset,
      },
      raw: (message.value?.toString('utf8') ?? '').slice(0, RAW_VALUE_LIMIT),
    };

    await this.appendRecord(record);

    if (!this.producer) {
      this.logger.warn('dead-lettered a record but no producer is attached (set KAFKA_DLT_TOPIC wiring)');
      return;
    }

    try {
      await this.producer.send({
        topic: this.config.deadLetterTopic,
        messages: [
          {
            key: record.source.partition !== null ? `p${record.source.partition}` : null,
            value: JSON.stringify(record),
            headers: { 'event-type': 'sensor.reading.dlt', 'reason': reason },
          },
        ],
      });
    } catch (error) {
      this.logger.error('failed to publish dead-letter record', error.message);
    }
  }

  async publishAnomaly(anomaly, event) {
    if (!this.producer) {
      this.logger.warn('anomaly detected but no producer is attached', anomaly);
      return;
    }
    try {
      await this.producer.send({
        topic: this.config.anomalyTopic,
        messages: [
          {
            key: anomaly.sensorId,
            value: JSON.stringify({ type: 'anomaly', ...anomaly, runId: event.eventId }),
            headers: { 'event-type': 'sensor.anomaly' },
          },
        ],
      });
      this.stats.anomaliesPublished += 1;
      this.logger.warn(`ANOMALY ${anomaly.sensorId}: ${anomaly.reason}`);
    } catch (error) {
      this.logger.error('failed to publish anomaly', error.message);
    }
  }

  /** Runs until SIGINT/SIGTERM (or the idle timeout fires). */
  async run() {
    await this.connect();
    await this.consumer.subscribe({ topic: this.config.topic, fromBeginning: true });

    // kafkajs' `consumer.run()` resolves early on modern Node while fetching
    // continues in the background, so `run()` is driven by an explicit
    // "stopped" promise that only settles once `consumer.stop()` completed.
    const stopped = new Promise((resolve) => {
      this.resolveStopped = resolve;
    });

    const onSignal = (signal) => {
      this.stop(signal).catch((error) => this.logger.error('shutdown failed', error.message));
    };
    process.once('SIGINT', onSignal);
    process.once('SIGTERM', onSignal);

    this.resetIdleTimer(() => {
      this.stop('idle-timeout').catch(() => {});
    });

    let snapshot = null;
    let runError = null;
    try {
      this.consumer
        .run({
          autoCommit: false,
          eachBatchAutoResolve: false,
          partitionsConsumedConcurrently: this.config.partitions,
          eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat }) => {
            this.resetIdleTimer(() => {
              this.stop('idle-timeout').catch(() => {});
            });
            const started = Date.now();
            await this.handleBatch(batch, resolveOffset);
            await heartbeat();
            await commitOffsetsIfNecessary();
            const elapsed = Date.now() - started;
            this.logger.info(
              `processed ${batch.messages.length} record(s) from ${batch.topic}[${batch.partition}] ` +
                `offsets ${batch.messages[0]?.offset}-${batch.messages.at(-1)?.offset} in ${elapsed}ms`,
            );
          },
        })
        .catch((error) => {
          runError = error;
          this.stop('error').catch(() => {});
        });

      await stopped;

      if (runError) throw runError;
    } finally {
      process.off('SIGINT', onSignal);
      process.off('SIGTERM', onSignal);
      if (this.idleTimer) clearTimeout(this.idleTimer);
      snapshot = this.aggregator.snapshot();
      snapshot.consumer = { ...this.stats };
      await this.writeSummaryFile(snapshot);
      if (this.consumer.isRunning?.()) await this.consumer.stop().catch(() => {});
      await this.consumer.disconnect();
    }

    return snapshot;
  }

  /** Graceful shutdown: stop fetching, finish the in-flight batch, then settle. */
  async stop(reason) {
    if (this.stopping) return;
    this.stopping = true;
    this.logger.info(`stopping: ${reason}`);
    try {
      if (this.consumer.isRunning?.() !== false) {
        await this.consumer.stop();
      }
    } catch (error) {
      this.logger.error('consumer.stop() failed', error.message);
    } finally {
      this.resolveStopped?.();
    }
  }
}

function makeJsonlAppender(outputDir) {
  let ready = null;
  return async function appendRecord(record) {
    ready ??= mkdir(outputDir, { recursive: true });
    await ready;
    await appendFile(join(outputDir, 'events.jsonl'), `${JSON.stringify(record)}\n`, 'utf8');
  };
}

function makeSummaryWriter(outputDir) {
  return async function writeSummaryFile(snapshot) {
    await mkdir(outputDir, { recursive: true });
    await writeFile(join(outputDir, 'summary.json'), `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
  };
}

export async function runConsumer({
  config = loadConfig(),
  logger = createLogger({ level: config.logLevel, name: 'consumer' }),
  kafka,
} = {}) {
  const client = kafka ?? createKafka(config, { logLevel: config.logLevel });
  const producer = client.producer({ allowAutoTopicCreation: false });
  const consumer = new SensorConsumer({
    kafka: client,
    config,
    logger,
    producer,
    idleTimeoutMs: config.consumerIdleTimeoutMs,
  });

  await producer.connect();
  try {
    return await consumer.run();
  } finally {
    await producer.disconnect();
  }
}

const invokedDirectly = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (invokedDirectly) {
  const config = loadConfig();
  const logger = createLogger({ level: config.logLevel, name: 'consumer' });
  logger.info('starting', redactedConfigSummary(config));
  runConsumer({ config, logger })
    .then((snapshot) => {
      logger.info('summary\n' + formatSummary(snapshot));
      logger.info('wrote output/summary.json and output/events.jsonl');
    })
    .catch((error) => {
      logger.error('consumer failed', error.stack ?? error.message);
      process.exitCode = 1;
    });
}