// Cloudflare's custom span helpers remain callable when tracing is disabled.
// celld exports its own runtime telemetry separately; these compatibility
// spans are deliberately unsampled (isTraced=false), rather than pretending
// that application spans were recorded or exported.
(() => {
  const token = {};
  class Span {
    constructor(key) {
      if (key !== token) throw new TypeError("Illegal constructor");
    }
    get isTraced() { return false; }
    setAttribute() { return this; }
    setAttributes() { return this; }
    end() {}
  }
  const startSpan = () => new Span(token);
  const startActiveSpan = (name, callback, ...args) =>
    callback(startSpan(name), ...args);
  const enterSpan = (name, callback, ...args) => {
    const span = startSpan(name);
    try {
      const result = callback(span, ...args);
      if (result instanceof Promise) {
        // Keep the callback's original promise and rejection reason. Attaching
        // an observer ends the span after either settlement without replacing
        // the value returned to the caller.
        result.then(() => span.end(), () => span.end());
      } else {
        span.end();
      }
      return result;
    } catch (error) {
      span.end();
      throw error;
    }
  };
  __celld.__cf.tracing = { Span, startSpan, startActiveSpan, enterSpan };
})();
