// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Lazy application-facing facade; the native ops and internals stay private.
(() => {
  // Bound work and retained payloads before crossing the native seam. Never
  // invoke toJSON; cycles, BigInts and Error messages are safe telemetry values.
  const normalize = (value, budget, depth = 0, seen = new Set()) => {
    if (--budget.nodes < 0 || depth > 6) return "[truncated]";
    if (value === null || typeof value === "boolean") return value;
    if (typeof value === "number") return Number.isFinite(value) ? value : String(value);
    if (typeof value === "string" || typeof value === "bigint") {
      const text = String(value).slice(0, Math.min(4096, Math.max(0, budget.chars)));
      budget.chars -= text.length;
      return text;
    }
    if (typeof value !== "object") return `[${typeof value}]`;
    if (seen.has(value)) return "[circular]";
    seen.add(value);
    let result;
    if (value instanceof Error) {
      result = { name: normalize(value.name, budget, depth + 1, seen), message: normalize(value.message, budget, depth + 1, seen), stack: normalize(value.stack, budget, depth + 1, seen) };
    } else if (Array.isArray(value)) {
      result = [];
      for (let i = 0; i < Math.min(value.length, 64) && budget.nodes > 0 && budget.chars > 0; i++) result.push(normalize(value[i], budget, depth + 1, seen));
    } else {
      result = Object.create(null);
      let count = 0;
      for (const key in value) {
        if (!Object.hasOwn(value, key)) continue;
        if (++count > 64 || budget.nodes <= 0 || budget.chars <= 0) break;
        const item = value[key];
        if (item === undefined) continue;
        const name = key.slice(0, 256);
        budget.chars -= name.length;
        result[name] = normalize(item, budget, depth + 1, seen);
      }
    }
    seen.delete(value);
    return result;
  };
  const emit = (op, record) => {
    if (__telemetry_current() === undefined) return false;
    try {
      return op(JSON.stringify(normalize(record, { nodes: 1024, chars: 12000 })));
    } catch {
      // A throwing getter/formatter must not fail the request.
      return false;
    }
  };
  __celld.__telemetryModule = Object.freeze({
    currentContext: () => __telemetry_current(),
    withContext: (context, callback) => __telemetry_with_context(
      `00-${context.traceId}-${context.spanId}-${context.sampled ? "01" : "00"}`, callback,
    ),
    recordSpan: record => emit(__telemetry_span, record),
    recordLog: record => emit(__telemetry_log, record),
  });
})();
