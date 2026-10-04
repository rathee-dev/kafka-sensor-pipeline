import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  buildEventStream,
  createRandom,
  makeEventId,
  makeSensorEvent,
  messageKeyFor,
  validateEvent,
} from '../../src/lib/events.js';

const baseArgs = {
  sensorCount: 3,
  readingsPerSensor: 5,
  intervalMs: 1_000,
  startTimeMs: Date.parse('2026-09-30T10:00:00.000Z'),
  seed: 42,
  runId: 'run1',
};

describe('event generator', () => {
  it('is deterministic for a given seed', () => {
    const first = buildEventStream(baseArgs);
    const second = buildEventStream(baseArgs);
    assert.deepEqual(first, second);
    assert.notDeepEqual(first, buildEventStream({ ...baseArgs, seed: 43 }));
    assert.deepEqual(createRandom(7)(), createRandom(7)());
  });

  it('emits sensorCount * readingsPerSensor well-formed, uniquely identified events', () => {
    const events = buildEventStream(baseArgs);
    assert.equal(events.length, 15);

    const ids = new Set();
    const perSensorSequences = new Map();
    for (const event of events) {
      assert.equal(validateEvent(event).valid, true);
      assert.equal(new Set(Object.keys(event)).size, Object.keys(event).length, 'no duplicate keys');
      ids.add(event.eventId);
      if (!perSensorSequences.has(event.sensorId)) perSensorSequences.set(event.sensorId, []);
      perSensorSequences.get(event.sensorId).push(event.sequence);
    }

    assert.equal(ids.size, 15, 'eventIds are unique');
    assert.equal(perSensorSequences.size, 3);
    for (const sequences of perSensorSequences.values()) {
      // One reading per sensor per tick, in ascending order.
      assert.deepEqual(sequences, [1, 2, 3, 4, 5]);
    }
    assert.deepEqual([...perSensorSequences.keys()], ['sensor-01', 'sensor-02', 'sensor-03']);
  });

  it('produces timestamps one interval apart and a spiky sensor for anomaly detection', () => {
    const events = buildEventStream(baseArgs);
    const firstTick = events.filter((event) => event.sequence === 1);
    const secondTick = events.filter((event) => event.sequence === 2);
    assert.equal(Date.parse(secondTick[0].ts) - Date.parse(firstTick[0].ts), 1_000);
    assert.equal(firstTick.every((event) => event.ts === firstTick[0].ts), true);

    const spiky = events.filter((event) => event.sensorId === 'sensor-01' && event.temperatureC > 80);
    assert.ok(spiky.length >= 1, 'spike sensor crosses the 80C threshold');
    assert.ok(spiky.every((event) => event.vibrationMmS > 5));
  });

  it('builds a stable eventId from sensorId + sequence + runId', () => {
    assert.equal(makeEventId('sensor-07', 12, 'run1'), 'run1-sensor-07-000012');
    const event = makeSensorEvent({
      sensorId: 'sensor-01',
      rackId: 'rack-A',
      sequence: 1,
      timestampMs: Date.parse('2026-09-30T10:00:00.000Z'),
      temperatureC: 21.4567,
      humidityPct: 45.4321,
      vibrationMmS: 0.12345,
      runId: 'run1',
    });
    assert.equal(event.ts, '2026-09-30T10:00:00.000Z');
    assert.equal(event.temperatureC, 21.46);
    assert.equal(event.humidityPct, 45.43);
    assert.equal(event.vibrationMmS, 0.123);
    assert.equal(event.schemaVersion, 1);
  });
});

describe('message keys', () => {
  const event = { sensorId: 'sensor-02', eventId: 'x' };

  it('keys by sensorId in sensor mode', () => {
    assert.equal(messageKeyFor(event, 'sensor'), 'sensor-02');
  });

  it('uses a null key (broker sticky partitioner) in roundrobin mode', () => {
    assert.equal(messageKeyFor(event, 'roundrobin'), null);
  });

  it('scatters keys in random mode via the injected factory', () => {
    let counter = 0;
    const factory = () => `r${++counter}`;
    assert.equal(messageKeyFor(event, 'random', factory), 'r1');
    assert.equal(messageKeyFor(event, 'random', factory), 'r2');
    // PRNGs return numbers; Kafka only accepts string keys.
    assert.equal(typeof messageKeyFor(event, 'random', createRandom(1)), 'string');
    assert.equal(typeof messageKeyFor(event, 'random'), 'string');
  });

  it('rejects an unknown mode', () => {
    assert.throws(() => messageKeyFor(event, 'nope'), /unknown key mode/);
  });
});

describe('schema validation', () => {
  it('rejects non-objects and reports every missing field', () => {
    assert.deepEqual(validateEvent(null), { valid: false, errors: ['event must be a JSON object'] });
    assert.deepEqual(validateEvent([1, 2]), { valid: false, errors: ['event must be a JSON object'] });

    const result = validateEvent({ eventId: 'e' });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some((error) => error.includes('sensorId is required')));
    assert.ok(result.errors.some((error) => error.includes('temperatureC is required')));
  });

  it('rejects wrong types, bad timestamps and NaN numbers', () => {
    const event = {
      schemaVersion: 1,
      eventId: 'e',
      sensorId: 42,
      sequence: 'one',
      ts: 'yesterday',
      temperatureC: Number.NaN,
    };
    const result = validateEvent(event);
    assert.equal(result.valid, false);
    assert.ok(result.errors.includes('sensorId must be a string, received number'));
    assert.ok(result.errors.includes('sequence must be a number, received string'));
    assert.ok(result.errors.includes('ts must be an ISO-8601 timestamp'));
    assert.ok(result.errors.includes('temperatureC must be a finite number'));
  });

  it('accepts a minimal valid event and rejects an over-long eventId', () => {
    assert.equal(
      validateEvent({
        schemaVersion: 1,
        eventId: 'e',
        sensorId: 's',
        sequence: 1,
        ts: '2026-09-30T10:00:00.000Z',
        temperatureC: 20,
      }).valid,
      true,
    );
    assert.equal(validateEvent({ eventId: 'x'.repeat(201) }).valid, false);
  });
});