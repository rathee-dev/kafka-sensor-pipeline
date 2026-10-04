import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { createAggregator, formatSummary } from '../../src/lib/aggregate.js';

function reading(sensorId, sequence, overrides = {}) {
  return {
    schemaVersion: 1,
    eventId: overrides.eventId ?? `${sensorId}-${sequence}`,
    sensorId,
    rackId: 'rack-A',
    sequence,
    ts: new Date(Date.parse('2026-09-30T10:00:00.000Z') + sequence * 1_000).toISOString(),
    temperatureC: 20,
    humidityPct: 50,
    vibrationMmS: 0.1,
    ...overrides,
  };
}

describe('aggregator', () => {
  it('computes per-sensor aggregates (count, avg, min, max, max vibration)', () => {
    const aggregator = createAggregator();
    aggregator.ingest(reading('sensor-01', 1, { temperatureC: 20, humidityPct: 40, vibrationMmS: 0.1 }));
    aggregator.ingest(reading('sensor-01', 2, { temperatureC: 24, humidityPct: 60, vibrationMmS: 0.9 }));
    aggregator.ingest(reading('sensor-01', 3, { temperatureC: 16, humidityPct: 50, vibrationMmS: 0.4 }));

    const snapshot = aggregator.snapshot();
    assert.equal(snapshot.totals.sensors, 1);
    assert.equal(snapshot.totals.readings, 3);
    const [sensor] = snapshot.sensors;
    assert.equal(sensor.readings, 3);
    assert.equal(sensor.avgTemperatureC, 20);
    assert.equal(sensor.minTemperatureC, 16);
    assert.equal(sensor.maxTemperatureC, 24);
    assert.equal(sensor.avgHumidityPct, 50);
    assert.equal(sensor.maxVibrationMmS, 0.9);
    assert.equal(sensor.lastSequence, 3);
    assert.equal(sensor.sequenceGaps, 0);
  });

  it('keeps sensor state isolated and returns sensors sorted by id', () => {
    const aggregator = createAggregator();
    aggregator.ingest(reading('sensor-03', 1, { temperatureC: 30 }));
    aggregator.ingest(reading('sensor-01', 1, { temperatureC: 10 }));
    aggregator.ingest(reading('sensor-02', 1, { temperatureC: 20 }));

    const snapshot = aggregator.snapshot();
    assert.deepEqual(
      snapshot.sensors.map((sensor) => sensor.sensorId),
      ['sensor-01', 'sensor-02', 'sensor-03'],
    );
    assert.equal(snapshot.totals.readings, 3);
    assert.equal(snapshot.sensors[0].avgTemperatureC, 10);
    assert.equal(snapshot.sensors[2].avgTemperatureC, 30);
  });

  it('is idempotent: a redelivered eventId is ignored (at-least-once delivery)', () => {
    const aggregator = createAggregator();
    const event = reading('sensor-01', 1, { temperatureC: 20, eventId: 'dup-1' });
    const first = aggregator.ingest(event);
    const second = aggregator.ingest(event);
    const third = aggregator.ingest(event);

    assert.equal(first.status, 'accepted');
    assert.equal(second.status, 'duplicate');
    assert.equal(third.status, 'duplicate');

    const snapshot = aggregator.snapshot();
    assert.equal(snapshot.totals.readings, 1, 'counted once');
    assert.equal(snapshot.totals.duplicates, 2);
    assert.equal(snapshot.sensors[0].duplicates, 2);
  });

  it('flags temperature and vibration threshold breaches as anomalies', () => {
    const aggregator = createAggregator({ anomalyHighC: 80, anomalyLowC: -10, vibrationThresholdMmS: 5 });

    assert.equal(aggregator.ingest(reading('sensor-01', 1, { temperatureC: 79.9 })).anomaly, null);

    const hot = aggregator.ingest(
      reading('sensor-01', 2, { temperatureC: 91.2, vibrationMmS: 6.4 }),
    );
    assert.equal(hot.status, 'accepted');
    assert.equal(hot.anomaly.sensorId, 'sensor-01');
    assert.match(hot.anomaly.reason, /temperatureC 91.2 > 80/);
    assert.match(hot.anomaly.reason, /vibrationMmS 6.4 > 5/);

    const cold = aggregator.ingest(reading('sensor-02', 1, { temperatureC: -30 }));
    assert.match(cold.anomaly.reason, /temperatureC -30 < -10/);

    const snapshot = aggregator.snapshot();
    assert.equal(snapshot.totals.anomalies, 2);
    assert.equal(snapshot.sensors.find((s) => s.sensorId === 'sensor-01').anomalies, 1);
    assert.equal(snapshot.sensors.find((s) => s.sensorId === 'sensor-02').anomalies, 1);
  });

  it('detects missing and out-of-order sequences to expose broken ordering', () => {
    const aggregator = createAggregator();
    assert.equal(aggregator.ingest(reading('sensor-01', 1)).status, 'accepted');
    assert.equal(aggregator.ingest(reading('sensor-01', 4)).status, 'gap');
    assert.equal(aggregator.ingest(reading('sensor-01', 2)).status, 'out_of_order');

    const snapshot = aggregator.snapshot();
    assert.equal(snapshot.totals.sequenceGaps, 1);
    assert.equal(snapshot.totals.missingReadings, 2, 'readings 2 and 3 arrived late');
    assert.equal(snapshot.totals.outOfOrder, 1);
    assert.equal(snapshot.sensors[0].lastSequence, 4);
  });

  it('formats a summary table', () => {
    const aggregator = createAggregator();
    aggregator.ingest(reading('sensor-01', 1, { temperatureC: 20 }));
    const text = formatSummary(aggregator.snapshot());
    assert.match(text, /sensor-01/);
    assert.match(text, /total: 1 readings, 0 anomalies/);
  });
});