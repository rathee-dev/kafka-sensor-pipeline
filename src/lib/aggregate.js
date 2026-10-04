/**
 * Pure, in-memory aggregation of sensor readings.
 *
 * Everything here is synchronous and side-effect free (apart from its own
 * state) which is what lets us test the "business logic" of the consumer
 * without a broker.
 *
 * The aggregator is deliberately stateful *per sensor* rather than per process:
 * because a sensor's readings always land in one partition, a given sensor is
 * only ever handled by one consumer instance, so per-sensor state needs no
 * coordination between instances.
 */

const MAX_TRACKED_EVENT_IDS = 5_000;

export function createAggregator({
  anomalyHighC = 80,
  anomalyLowC = -10,
  vibrationThresholdMmS = 5,
} = {}) {
  /** @type {Map<string, object>} */
  const sensors = new Map();
  /** Bounded set of already-applied eventIds, used for idempotency. */
  const seenEventIds = new Set();
  /** @type {Map<string, number>} */
  const duplicatesBySensor = new Map();
  const anomalies = [];
  let duplicateCount = 0;
  let outOfOrderCount = 0;
  let sequenceGapCount = 0;
  let missingSequenceCount = 0;

  function stateFor(sensorId) {
    let state = sensors.get(sensorId);
    if (!state) {
      state = {
        sensorId,
        rackId: null,
        count: 0,
        temperatureSumC: 0,
        minTemperatureC: null,
        maxTemperatureC: null,
        lastTemperatureC: null,
        humiditySumPct: 0,
        maxVibrationMmS: 0,
        lastSequence: 0,
        lastEventTs: null,
        sequenceGaps: 0,
        anomalies: 0,
      };
      sensors.set(sensorId, state);
    }
    return state;
  }

  function noteDuplicate(sensorId) {
    duplicateCount += 1;
    duplicatesBySensor.set(sensorId, (duplicatesBySensor.get(sensorId) ?? 0) + 1);
  }

  /**
   * Applies one validated event.
   *
   * Re-delivery is expected: the consumer commits offsets *after* processing,
   * which gives at-least-once delivery. `eventId` de-duplication makes the
   * aggregation idempotent, so a replay does not skew the averages.
   *
   * @returns {{status: 'accepted'|'duplicate'|'gap'|'out_of_order', anomaly: object|null}}
   */
  function ingest(event) {
    if (seenEventIds.has(event.eventId)) {
      noteDuplicate(event.sensorId);
      return { status: 'duplicate', anomaly: null };
    }
    if (seenEventIds.size < MAX_TRACKED_EVENT_IDS) {
      seenEventIds.add(event.eventId);
    }

    const state = stateFor(event.sensorId);
    const previousSequence = state.lastSequence;

    let status = 'accepted';
    if (previousSequence !== 0 && event.sequence < previousSequence) {
      outOfOrderCount += 1;
      state.sequenceGaps += 1;
      status = 'out_of_order';
    } else if (previousSequence !== 0 && event.sequence > previousSequence + 1) {
      sequenceGapCount += 1;
      missingSequenceCount += event.sequence - previousSequence - 1;
      state.sequenceGaps += 1;
      status = 'gap';
    }

    state.count += 1;
    state.temperatureSumC += event.temperatureC;
    state.minTemperatureC =
      state.minTemperatureC === null ? event.temperatureC : Math.min(state.minTemperatureC, event.temperatureC);
    state.maxTemperatureC =
      state.maxTemperatureC === null ? event.temperatureC : Math.max(state.maxTemperatureC, event.temperatureC);
    state.lastTemperatureC = event.temperatureC;
    state.humiditySumPct += event.humidityPct ?? 0;
    state.maxVibrationMmS = Math.max(state.maxVibrationMmS, event.vibrationMmS ?? 0);
    state.lastSequence = Math.max(previousSequence, event.sequence);
    state.lastEventTs = event.ts;
    if (event.rackId) state.rackId = event.rackId;

    const anomaly = detectAnomalies(event);
    if (anomaly) {
      state.anomalies += 1;
      anomalies.push(anomaly);
    }

    return { status, anomaly };
  }

  function detectAnomalies(event) {
    const reasons = [];
    if (event.temperatureC > anomalyHighC) {
      reasons.push(`temperatureC ${event.temperatureC} > ${anomalyHighC}`);
    }
    if (event.temperatureC < anomalyLowC) {
      reasons.push(`temperatureC ${event.temperatureC} < ${anomalyLowC}`);
    }
    if ((event.vibrationMmS ?? 0) > vibrationThresholdMmS) {
      reasons.push(`vibrationMmS ${event.vibrationMmS} > ${vibrationThresholdMmS}`);
    }
    if (reasons.length === 0) return null;
    return {
      eventId: event.eventId,
      sensorId: event.sensorId,
      rackId: event.rackId ?? null,
      ts: event.ts,
      sequence: event.sequence,
      temperatureC: event.temperatureC,
      vibrationMmS: event.vibrationMmS ?? null,
      reason: reasons.join('; '),
    };
  }

  /** Deterministic, JSON-friendly view of the current state. */
  function snapshot() {
    const sensorList = [...sensors.values()]
      .sort((a, b) => a.sensorId.localeCompare(b.sensorId))
      .map((state) => ({
        sensorId: state.sensorId,
        rackId: state.rackId,
        readings: state.count,
        avgTemperatureC: round(state.temperatureSumC / state.count, 2),
        minTemperatureC: round(state.minTemperatureC, 2),
        maxTemperatureC: round(state.maxTemperatureC, 2),
        avgHumidityPct: round(state.humiditySumPct / state.count, 2),
        maxVibrationMmS: round(state.maxVibrationMmS, 3),
        lastSequence: state.lastSequence,
        lastEventTs: state.lastEventTs,
        sequenceGaps: state.sequenceGaps,
        duplicates: duplicatesBySensor.get(state.sensorId) ?? 0,
        anomalies: state.anomalies,
      }));

    const totalReadings = sensorList.reduce((sum, sensor) => sum + sensor.readings, 0);

    return {
      generatedAt: new Date().toISOString(),
      totals: {
        sensors: sensorList.length,
        readings: totalReadings,
        duplicates: duplicateCount,
        outOfOrder: outOfOrderCount,
        sequenceGaps: sequenceGapCount,
        missingReadings: missingSequenceCount,
        anomalies: anomalies.length,
      },
      thresholds: { anomalyHighC, anomalyLowC, vibrationThresholdMmS },
      sensors: sensorList,
      anomalies,
    };
  }

  return { ingest, snapshot, detectAnomalies };
}

function round(value, decimals) {
  if (value === null || !Number.isFinite(value)) return null;
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

/** Small console table used at the end of a demo run. */
export function formatSummary(snapshot) {
  const lines = [];
  lines.push('sensor      readings  avgT(C)  minT(C)  maxT(C)  maxVib  gaps  anomalies');
  for (const sensor of snapshot.sensors) {
    lines.push(
      [
        sensor.sensorId.padEnd(11),
        String(sensor.readings).padStart(8),
        String(sensor.avgTemperatureC).padStart(7),
        String(sensor.minTemperatureC).padStart(8),
        String(sensor.maxTemperatureC).padStart(8),
        String(sensor.maxVibrationMmS).padStart(7),
        String(sensor.sequenceGaps).padStart(5),
        String(sensor.anomalies).padStart(10),
      ].join(' '),
    );
  }
  const totals = snapshot.totals;
  lines.push(
    `total: ${totals.readings} readings, ${totals.anomalies} anomalies, ` +
      `${totals.duplicates} duplicates, ${totals.outOfOrder} out-of-order, ` +
      `${totals.missingReadings} missing`,
  );
  return lines.join('\n');
}