/**
 * Producer: turns deterministic mock telemetry into Kafka records.
 *
 * The interesting decision is the record key. Keying by `sensorId` makes the
 * broker hash every reading of a sensor to the same partition, which is what
 * gives us per-sensor ordering; see README "Ordering guarantees".
 */

import { pathToFileURL } from 'node:url';

import { Partitioners } from 'kafkajs';
import { loadConfig, redactedConfigSummary } from './lib/config.js';
import { buildEventStream, messageKeyFor, createRandom, makeSensorEvent } from './lib/events.js';
import { createKafka } from './lib/kafka.js';
import { createLogger } from './lib/logger.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class SensorProducer {
  constructor({ kafka, config, logger, producer, randomIdFactory }) {
    this.kafka = kafka;
    this.config = config;
    this.logger = logger;
    // DefaultPartitioner: murmur2(key) -> partition for keyed records, sticky
    // batching for null keys. Being explicit keeps the ordering story honest.
    this.producer =
      producer ??
      kafka.producer({
        allowAutoTopicCreation: false,
        idempotent: true,
        // Idempotent producer => retry forever on transient errors rather than
        // dropping telemetry (KafkaJS requires this for exactly-once producer
        // semantics).
        retry: { initialRetryTime: 300, retries: Number.MAX_SAFE_INTEGER },
        createPartitioner: Partitioners.DefaultPartitioner,
      });
    this.randomIdFactory = randomIdFactory;
    this.sent = 0;
    this.failed = 0;
  }

  async connect() {
    await this.producer.connect();
    this.logger.info('producer connected', this.config.brokers);
  }

  /**
   * Publishes one record per event, honouring the configured key mode.
   * @returns {Promise<{sent: number, failed: number}>}
   */
  async publish(events) {
    const messages = events.map((event) => ({
      key: messageKeyFor(event, this.config.keyMode, this.randomIdFactory),
      value: JSON.stringify(event),
      headers: {
        'event-type': 'sensor.reading',
        'schema-version': String(event.schemaVersion),
        'source': this.config.clientId,
      },
    }));

    try {
      await this.producer.send({ topic: this.config.topic, messages });
      this.sent += messages.length;
      this.logger.debug(`sent ${messages.length} record(s) to ${this.config.topic}`, {
        keys: [...new Set(messages.map((m) => m.key))],
      });
    } catch (error) {
      this.failed += messages.length;
      this.logger.error(`failed to publish batch of ${messages.length}`, error.message);
      throw error;
    }

    return { sent: messages.length, failed: 0 };
  }

  /** Generates the whole run and publishes it batch-by-batch (or continuously). */
  async run() {
    const runId = new Date().toISOString().replace(/[:.]/g, '-');
    if (this.config.continuous) {
      return this.runContinuous(runId);
    }

    const stream = buildEventStream({
      sensorCount: this.config.sensorCount,
      readingsPerSensor: this.config.readingsPerSensor,
      intervalMs: this.config.publishIntervalMs || 1,
      seed: this.config.eventSeed,
      runId,
    });

    const batchSize = Math.max(1, this.config.sensorCount);
    this.logger.info(
      `publishing ${stream.length} readings from ${this.config.sensorCount} sensor(s) ` +
        `to "${this.config.topic}" (key mode: ${this.config.keyMode})`,
    );

    for (let index = 0; index < stream.length; index += batchSize) {
      const batch = stream.slice(index, index + batchSize);
      await this.publish(batch);
      for (const ev of batch) {
        this.logger.info(
          `[producer] 🚀 sent ${ev.sensorId} (seq ${ev.sequence}): temp=${ev.temperatureC}°C, vib=${ev.vibrationMmS}mm/s`,
        );
      }
      if (this.config.publishIntervalMs > 0) await sleep(this.config.publishIntervalMs);
    }

    this.logger.info(`publish complete: ${this.sent} sent, ${this.failed} failed`);
    return { sent: this.sent, failed: this.failed };
  }

  /** Streams mock sensor readings continuously until SIGINT/SIGTERM. */
  async runContinuous(runId) {
    this.logger.info(
      `Starting continuous streaming from ${this.config.sensorCount} sensor(s) ` +
        `to "${this.config.topic}" (interval: ${this.config.publishIntervalMs || 1000}ms). Press Ctrl+C to stop.`,
    );

    let sequence = 1;
    let stopping = false;
    const onSignal = () => { stopping = true; };
    process.once('SIGINT', onSignal);
    process.once('SIGTERM', onSignal);

    const random = createRandom(this.config.eventSeed);

    while (!stopping) {
      const timestampMs = Date.now();
      const batch = [];
      for (let sIdx = 0; sIdx < this.config.sensorCount; sIdx++) {
        const sensorId = `sensor-${String(sIdx + 1).padStart(2, '0')}`;
        const spiking = sIdx === 0 && sequence % 8 === 0;
        const baseTemp = 21.5 + (random() - 0.5) * 3;
        const temperatureC = spiking ? 85 + random() * 9 : baseTemp;
        const vibrationMmS = spiking ? 6 + random() * 2 : 0.05 + random() * 0.4;
        batch.push(
          makeSensorEvent({
            sensorId,
            rackId: `rack-${String.fromCharCode(65 + sIdx)}`,
            sequence,
            timestampMs,
            temperatureC,
            humidityPct: 45 + random() * 10,
            vibrationMmS,
            runId,
          }),
        );
      }
      sequence++;
      await this.publish(batch);
      for (const ev of batch) {
        this.logger.info(
          `[producer] 🚀 sent ${ev.sensorId} (seq ${ev.sequence}): temp=${ev.temperatureC}°C, vib=${ev.vibrationMmS}mm/s`,
        );
      }
      if (this.config.publishIntervalMs > 0 && !stopping) {
        await sleep(this.config.publishIntervalMs);
      }
    }

    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
    this.logger.info(`stream stopped: ${this.sent} sent, ${this.failed} failed`);
    return { sent: this.sent, failed: this.failed };
  }

  async disconnect() {
    await this.producer.disconnect();
    this.logger.info('producer disconnected');
  }
}

export async function runProducer({ config = loadConfig(), logger = createLogger({ level: config.logLevel, name: 'producer' }), kafka } = {}) {
  const client = kafka ?? createKafka(config, { logLevel: config.logLevel });
  const producer = new SensorProducer({
    kafka: client,
    config,
    logger,
    randomIdFactory: createRandom(config.eventSeed + 7),
  });

  await producer.connect();
  try {
    return await producer.run();
  } finally {
    await producer.disconnect();
  }
}

const invokedDirectly = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (invokedDirectly) {
  const config = loadConfig();
  const logger = createLogger({ level: config.logLevel, name: 'producer' });
  logger.info('starting', redactedConfigSummary(config));
  runProducer({ config, logger }).catch((error) => {
    logger.error('producer failed', error.stack ?? error.message);
    process.exitCode = 1;
  });
}