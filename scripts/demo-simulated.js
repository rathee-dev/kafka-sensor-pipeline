#!/usr/bin/env node
/**
 * Zero-dependency in-memory pipeline demo.
 *
 * Demonstrates the full event-driven IoT pipeline without requiring Docker,
 * Java, or a running Apache Kafka broker:
 *   1. Generates realistic sensor telemetry (temperature, humidity, vibration).
 *   2. Demonstrates at-least-once delivery by replaying a duplicate record.
 *   3. Demonstrates dead-letter routing by injecting an unparseable poison record.
 *   4. Consumer ingests batches, detects anomalies, ignores duplicates, routes bad data to DLT.
 *   5. Writes output/events.jsonl and output/summary.json.
 *   6. Prints the final analytics table.
 */

import { SensorConsumer } from '../src/consumer.js';
import { formatSummary } from '../src/lib/aggregate.js';
import { loadConfig } from '../src/lib/config.js';
import { buildEventStream } from '../src/lib/events.js';
import { createLogger } from '../src/lib/logger.js';

async function main() {
  const logger = createLogger({ level: 'info', name: 'demo' });
  const config = loadConfig({
    KAFKA_BROKERS: 'simulated-in-memory:9092',
    KAFKA_SSL: 'false',
    SENSOR_COUNT: '4',
    READINGS_PER_SENSOR: '10',
    ANOMALY_HIGH_C: '80',
    VIBRATION_THRESHOLD_MM_S: '5',
    OUTPUT_DIR: 'output',
  });

  console.log('================================================================');
  console.log('  Kafka IoT Telemetry Pipeline (Zero-Dependency In-Memory Demo)  ');
  console.log('================================================================\n');

  logger.info('Initializing pipeline with 4 mock sensors, 10 readings each...');

  // Track routed events for reporting
  const anomalyMessages = [];
  const dltMessages = [];

  const fakeProducer = {
    async send({ topic, messages }) {
      for (const msg of messages) {
        if (topic === config.anomalyTopic) {
          anomalyMessages.push(msg);
          const parsed = JSON.parse(msg.value);
          logger.warn(`ANOMALY ${parsed.sensorId}: ${parsed.reason} -> published to ${topic}`);
        } else if (topic === config.deadLetterTopic) {
          dltMessages.push(msg);
          const parsed = JSON.parse(msg.value);
          logger.warn(`POISON PILL -> routed to ${topic} (reason: ${parsed.reason})`);
        }
      }
    },
  };

  const consumer = new SensorConsumer({
    kafka: null,
    config,
    logger,
    producer: fakeProducer,
  });

  // Step 1: Generate stream
  const runId = new Date().toISOString().replace(/[:.]/g, '-');
  const stream = buildEventStream({
    sensorCount: 4,
    readingsPerSensor: 10,
    seed: 20260930,
    runId,
  });

  logger.info(`Generated ${stream.length} valid sensor readings.`);

  // Step 2: Prepare batch with simulated network anomalies:
  // - Valid events
  // - 1 replayed duplicate event (demonstrating at-least-once deduplication)
  // - 1 malformed poison-pill event (demonstrating dead-letter queue resilience)
  const messages = stream.map((event, index) => ({
    offset: String(index),
    key: event.sensorId,
    value: Buffer.from(JSON.stringify(event)),
    timestamp: String(Date.now()),
    headers: {},
  }));

  // Inject duplicate
  messages.push({
    offset: String(messages.length),
    key: stream[0].sensorId,
    value: Buffer.from(JSON.stringify(stream[0])),
    timestamp: String(Date.now()),
    headers: {},
  });

  // Inject corrupt message
  messages.push({
    offset: String(messages.length),
    key: 'sensor-01',
    value: Buffer.from('{"sensorId": "sensor-01", "corrupt_json: true'),
    timestamp: String(Date.now()),
    headers: {},
  });

  logger.info(`Simulating transmission of ${messages.length} messages (40 valid + 1 replay + 1 poison pill)...`);

  // Step 3: Consumer processes the batch
  await consumer.handleBatch({
    topic: config.topic,
    partition: 0,
    messages,
  });

  // Step 4: Write summary & artifacts
  const snapshot = consumer.aggregator.snapshot();
  snapshot.consumer = { ...consumer.stats };
  await consumer.writeSummaryFile(snapshot);

  console.log('\n================================================================');
  console.log('                      PIPELINE SUMMARY REPORT                   ');
  console.log('================================================================');
  console.log(formatSummary(snapshot));
  console.log('================================================================');
  console.log(`✓ Valid readings processed : ${snapshot.totals.readings}`);
  console.log(`✓ Anomalies detected       : ${snapshot.totals.anomalies} (routed to "${config.anomalyTopic}")`);
  console.log(`✓ Duplicate events caught  : ${snapshot.consumer.duplicates} (deduplicated, averages intact)`);
  console.log(`✓ Poison pills isolated    : ${snapshot.consumer.deadLettered} (routed to "${config.deadLetterTopic}")`);
  console.log(`✓ Artifacts persisted      : output/events.jsonl, output/summary.json`);
  console.log('================================================================\n');
}

main().catch((err) => {
  console.error('Demo failed:', err);
  process.exit(1);
});
