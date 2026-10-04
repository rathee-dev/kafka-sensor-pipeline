/**
 * Deterministic mock telemetry generator.
 *
 * The generator is seeded so a demo run is reproducible: the same EVENT_SEED
 * always yields the same events, which is what makes the assertions in
 * test/unit/events.test.js and the screenshots in the README stable.
 */

/** mulberry32 - small, fast, deterministic PRNG. */
export function createRandom(seed) {
  let state = seed >>> 0;
  return function random() {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function round(value, decimals) {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function sensorIds(count) {
  return Array.from({ length: count }, (_, index) => `sensor-${String(index + 1).padStart(2, '0')}`);
}

export function makeEventId(sensorId, sequence, runId) {
  return `${runId}-${sensorId}-${String(sequence).padStart(6, '0')}`;
}

export function makeSensorEvent({
  sensorId,
  rackId,
  sequence,
  timestampMs,
  temperatureC,
  humidityPct,
  vibrationMmS,
  runId,
}) {
  return {
    schemaVersion: 1,
    eventId: makeEventId(sensorId, sequence, runId),
    sensorId,
    rackId,
    sequence,
    ts: new Date(timestampMs).toISOString(),
    temperatureC: round(temperatureC, 2),
    humidityPct: round(humidityPct, 2),
    vibrationMmS: round(vibrationMmS, 3),
  };
}

/**
 * Builds a warm-up ramp followed by steady state readings, and deliberately
 * spikes one sensor past the anomaly threshold so the consumer has something
 * to detect during a demo.
 */
export function buildEventStream({
  sensorCount = 3,
  readingsPerSensor = 10,
  intervalMs = 1_000,
  startTimeMs = Date.now(),
  seed = 1,
  runId = 'run',
  spikeSensorIndex = 0,
} = {}) {
  const random = createRandom(seed);
  const sensors = sensorIds(sensorCount);
  const events = [];

  for (let index = 0; index < readingsPerSensor; index += 1) {
    const timestampMs = startTimeMs + index * intervalMs;
    for (const [sensorIndex, sensorId] of sensors.entries()) {
      const warmingUp = index < 3;
      const baseTemp = warmingUp ? 18 + index * 1.5 : 21.5;
      const noise = (random() - 0.5) * 2;
      const spiking = sensorIndex === spikeSensorIndex && index >= readingsPerSensor - 3;
      const temperatureC = spiking ? 84 + random() * 9 : baseTemp + noise;
      events.push(
        makeSensorEvent({
          sensorId,
          rackId: `rack-${String.fromCharCode(65 + sensorIndex)}`,
          sequence: index + 1,
          timestampMs,
          temperatureC,
          humidityPct: 45 + random() * 10,
          vibrationMmS: spiking ? 6 + random() * 2 : 0.05 + random() * 0.4,
          runId,
        }),
      );
    }
  }

  return events;
}

const REQUIRED_FIELDS = {
  schemaVersion: 'number',
  eventId: 'string',
  sensorId: 'string',
  sequence: 'number',
  ts: 'string',
  temperatureC: 'number',
};

export const REQUIRED_FIELDS_LIST = Object.keys(REQUIRED_FIELDS);

/**
 * Schema check used by the consumer before an event reaches the aggregation
 * logic. Returns every problem instead of throwing on the first one so the
 * dead-letter record is useful.
 */
export function validateEvent(event) {
  const errors = [];

  if (event === null || typeof event !== 'object' || Array.isArray(event)) {
    return { valid: false, errors: ['event must be a JSON object'] };
  }

  for (const [field, type] of Object.entries(REQUIRED_FIELDS)) {
    const value = event[field];
    if (value === undefined || value === null) {
      errors.push(`${field} is required`);
    } else if (typeof value !== type) {
      errors.push(`${field} must be a ${type}, received ${typeof value}`);
    } else if (type === 'number' && !Number.isFinite(value)) {
      errors.push(`${field} must be a finite number`);
    }
  }

  if (typeof event.eventId === 'string' && event.eventId.length > 200) {
    errors.push('eventId must be <= 200 characters');
  }
  if (typeof event.ts === 'string' && Number.isNaN(Date.parse(event.ts))) {
    errors.push('ts must be an ISO-8601 timestamp');
  }

  return { valid: errors.length === 0, errors };
}

/**
 * Decides which Kafka key a record is published with.
 *
 * - `sensor`     key = sensorId -> every reading of a sensor lands in the same
 *                partition, so the consumer observes them in publish order.
 * - `roundrobin` key = null      -> the broker sticky-batches per partition.
 * - `random`     key = randomId  -> same sensor can hit several partitions, so
 *                per-sensor ordering is lost (used to demonstrate the point).
 */
export function messageKeyFor(event, keyMode, randomIdFactory) {
  switch (keyMode) {
    case 'sensor':
      return event.sensorId ?? null;
    case 'roundrobin':
      return null;
    case 'random':
      // Kafka keys must be strings (or null), so coerce whatever the factory
      // returns - including numbers from a PRNG.
      return randomIdFactory ? String(randomIdFactory()) : Math.random().toString(36).slice(2, 10);
    default:
      throw new Error(`unknown key mode: ${keyMode}`);
  }
}