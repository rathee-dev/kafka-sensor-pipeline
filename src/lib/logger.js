/**
 * Minimal structured logger (no dependencies, pretty in a terminal).
 */

const LEVELS = { silent: 100, error: 50, warn: 40, info: 30, debug: 20, trace: 10 };

export function createLogger({ level = 'info', name = 'app', sink = console } = {}) {
  const threshold = LEVELS[level] ?? LEVELS.info;

  function emit(levelName, message, meta) {
    if (LEVELS[levelName] < threshold) return;
    const line = `[${new Date().toISOString()}] ${levelName.toUpperCase().padEnd(5)} ${name}: ${message}`;
    if (meta === undefined) {
      sink.log(line);
      return;
    }
    sink.log(line, typeof meta === 'string' ? meta : JSON.stringify(meta));
  }

  return {
    level,
    error: (message, meta) => emit('error', message, meta),
    warn: (message, meta) => emit('warn', message, meta),
    info: (message, meta) => emit('info', message, meta),
    debug: (message, meta) => emit('debug', message, meta),
    trace: (message, meta) => emit('trace', message, meta),
    child: (childName) => createLogger({ level, name: `${name}:${childName}`, sink }),
  };
}