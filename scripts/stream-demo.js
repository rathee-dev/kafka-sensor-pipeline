#!/usr/bin/env node
/**
 * Continuous Live Streaming Demo.
 *
 * Runs an ongoing real-time event pipeline until interrupted (Ctrl+C).
 * Automatically uses real Kafka if reachable, or runs live in-memory streaming
 * with simulated network delays so you can see producer & consumer interact in real-time.
 */

import { SensorConsumer } from '../src/consumer.js';
import { formatSummary } from '../src/lib/aggregate.js';
import { loadConfig } from '../src/lib/config.js';
import { makeSensorEvent, createRandom } from '../src/lib/events.js';
import { createLogger } from '../src/lib/logger.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const logger = createLogger({ level: 'info', name: 'stream' });
  const config = loadConfig({
    KAFKA_BROKERS: 'localhost:9092',
    KAFKA_SSL: 'false',
    SENSOR_COUNT: '3',
    ANOMALY_HIGH_C: '80',
    VIBRATION_THRESHOLD_MM_S: '5',
    OUTPUT_DIR: 'output',
  });

  console.log('================================================================');
  console.log('     Live Real-Time IoT Telemetry Stream (Press Ctrl+C to Stop) ');
  console.log('================================================================\n');

  logger.info('Initializing live stream pipeline for 3 sensors...');

  const fakeProducer = {
    async send({ topic, messages }) {
      for (const msg of messages) {
        if (topic === config.anomalyTopic) {
          const parsed = JSON.parse(msg.value);
          console.log(`\x1b[33m[ALERT] 🚨 ANOMALY on ${parsed.sensorId}: ${parsed.reason} -> Published to '${topic}'\x1b[0m`);
        } else if (topic === config.deadLetterTopic) {
          const parsed = JSON.parse(msg.value);
          console.log(`\x1b[31m[DLT]   ⚠️ POISON PILL: ${parsed.reason} -> Routed to '${topic}'\x1b[0m`);
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

  let running = true;
  const onSignal = () => {
    running = false;
  };
  process.once('SIGINT', onSignal);
  process.once('SIGTERM', onSignal);

  let sequence = 1;
  const runId = new Date().toISOString().replace(/[:.]/g, '-');
  const random = createRandom(Date.now());
  let offsetCounter = 0;

  console.log('Streaming active. Producing & consuming readings every 1s...\n');

  while (running) {
    const timestampMs = Date.now();
    const batchMessages = [];

    for (let sIdx = 0; sIdx < config.sensorCount; sIdx++) {
      const sensorId = `sensor-${String(sIdx + 1).padStart(2, '0')}`;
      // Cause a spike every 7 sequences on sensor-01 to show anomaly detection
      const spiking = sIdx === 0 && sequence > 2 && sequence % 6 === 0;
      const baseTemp = 21.0 + (random() - 0.5) * 3;
      const temperatureC = spiking ? 86 + random() * 8 : baseTemp;
      const vibrationMmS = spiking ? 6.2 + random() * 2 : 0.08 + random() * 0.3;

      const event = makeSensorEvent({
        sensorId,
        rackId: `rack-${String.fromCharCode(65 + sIdx)}`,
        sequence,
        timestampMs,
        temperatureC,
        humidityPct: 45 + random() * 10,
        vibrationMmS,
        runId,
      });

      const partition = sIdx % config.partitions;
      const offset = String(offsetCounter++);

      // 1. Producer logs emission
      const timeStr = new Date().toLocaleTimeString();
      console.log(
        `\x1b[36m[${timeStr}] [producer] 🚀 Emitted ${sensorId} (seq ${sequence}): ` +
          `temp=${event.temperatureC}°C, vib=${event.vibrationMmS}mm/s -> topic: ${config.topic}[P${partition}]\x1b[0m`,
      );

      batchMessages.push({
        offset,
        key: sensorId,
        value: Buffer.from(JSON.stringify(event)),
        timestamp: String(timestampMs),
        partition,
      });
    }

    // Occasionally inject a duplicate or poison pill to demonstrate resilience
    if (sequence === 8) {
      console.log(`\x1b[35m[TEST] 🧪 Simulating network replay: sending duplicate event for sensor-01...\x1b[0m`);
      const dup = JSON.parse(batchMessages[0].value.toString());
      batchMessages.push({
        offset: String(offsetCounter++),
        key: dup.sensorId,
        value: Buffer.from(JSON.stringify(dup)),
        timestamp: String(timestampMs),
        partition: 0,
      });
    } else if (sequence === 12) {
      console.log(`\x1b[35m[TEST] 🧪 Simulating sensor packet corruption: injecting bad JSON payload...\x1b[0m`);
      batchMessages.push({
        offset: String(offsetCounter++),
        key: 'sensor-02',
        value: Buffer.from('{"sensorId": "sensor-02", "corrupt_data": '),
        timestamp: String(timestampMs),
        partition: 1,
      });
    }

    // Small realistic transmission delay (50ms)
    await sleep(50);

    // 2. Consumer processes batch
    for (const msg of batchMessages) {
      await consumer.handleBatch({
        topic: config.topic,
        partition: msg.partition ?? 0,
        messages: [msg],
      });
    }

    sequence++;
    await sleep(1000);
  }

  console.log('\n\n================================================================');
  console.log('              LIVE STREAMING SESSION SUMMARY                    ');
  console.log('================================================================');
  const snapshot = consumer.aggregator.snapshot();
  snapshot.consumer = { ...consumer.stats };
  await consumer.writeSummaryFile(snapshot);
  console.log(formatSummary(snapshot));
  console.log('================================================================');
  console.log(`✓ Processed   : ${snapshot.totals.readings} readings`);
  console.log(`✓ Anomalies   : ${snapshot.totals.anomalies} flagged and alerted`);
  console.log(`✓ Duplicates  : ${snapshot.consumer.duplicates} safely discarded`);
  console.log(`✓ Poison Pills: ${snapshot.consumer.deadLettered} routed to Dead-Letter Queue`);
  console.log(`✓ Output file : output/summary.json and output/events.jsonl`);
  console.log('================================================================\n');
}

main().catch((err) => {
  console.error('Streaming error:', err);
  process.exit(1);
});
