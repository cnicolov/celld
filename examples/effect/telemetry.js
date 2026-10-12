// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Effect 4 adapter for celld's native recorder. No OTLP exporter or transport.
import * as Cause from "effect/Cause";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Tracer from "effect/Tracer";
import * as Native from "celld:telemetry";

const levels = { Trace: 1, Debug: 5, Info: 9, Warn: 13, Error: 17, Fatal: 21 };
const kinds = { internal: 1, server: 2, client: 3, producer: 4, consumer: 5 };
const evaluate = "~effect/Effect/evaluate";

/** May be shared by requests: the parent is read at span creation, never when
 * the layer is constructed. Native queue admission is synchronous/nonblocking. */
export function layer({ serviceName, mergeWithExisting = false } = {}) {
  class Span extends Tracer.NativeSpan {
    constructor(options) {
      const ambient = Native.currentContext();
      const parent = !options.root && Option.isNone(options.parent) && ambient
        ? Option.some(Tracer.externalSpan(ambient))
        : options.parent;
      super({ ...options, links: options.links.slice(0, 32), parent, sampled: options.sampled && ambient?.sampled !== false && Option.getOrUndefined(parent)?.sampled !== false });
    }
    attribute(key, value) {
      if (this.attributes.size < 64 || this.attributes.has(key)) super.attribute(key, value);
    }
    event(name, time, attributes) {
      if (this.events.length < 32) super.event(name, time, attributes);
    }
    addLinks(links) {
      super.addLinks(links.slice(0, Math.max(0, 32 - this.links.length)));
    }
    end(time, exit) {
      if (this.status._tag === "Ended") return;
      super.end(time, exit);
      if (!this.sampled) return;
      try {
        const interrupted = exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause);
        const statusCode = exit._tag === "Success" ? 1 : interrupted ? 0 : 2;
        const attributes = Object.fromEntries(this.attributes);
        if (interrupted) attributes["effect.fiber.interrupted"] = true;
        const events = this.events.map(([name, time, attributes]) => ({ name, timeUnixNano: String(time), attributes }));
        const errors = exit._tag === "Failure" && !interrupted ? Cause.prettyErrors(exit.cause, { includeCauseInStack: true }) : [];
        for (const error of errors.slice(0, Math.max(0, 32 - events.length))) events.push({
          name: "exception", timeUnixNano: String(time), attributes: {
            "exception.type": error.name, "exception.message": error.message, "exception.stacktrace": error.stack,
          },
        });
        Native.recordSpan({
          traceId: this.traceId, spanId: this.spanId,
          parentSpanId: Option.getOrUndefined(this.parent)?.spanId,
          name: this.name, kind: kinds[this.kind], sampled: this.sampled,
          startTimeUnixNano: String(this.startTime), endTimeUnixNano: String(time),
          statusCode, error: errors[0]?.message,
          metadata: {
            serviceName, scopeName: "effect", statusCode, attributes, events,
            links: this.links.map(({ span, attributes }) => ({ traceId: span.traceId, spanId: span.spanId, attributes })),
          },
        });
      } catch {
        // Telemetry, including error formatting, cannot turn success into failure.
      }
    }
  }

  const tracer = Tracer.make({
    span: options => new Span(options),
    context(primitive, fiber) {
      const span = fiber.cache.span;
      const run = () => primitive[evaluate](fiber);
      // Activate for one synchronous evaluation, not the span's whole async
      // lifetime. CPED carries it into native awaits and restores other fibers.
      return span ? Native.withContext(span, run) : run();
    },
  });
  const logger = Logger.make(options => {
    try {
      const span = options.fiber.cache.span;
      const attributes = { ...options.fiber.getRef(References.CurrentLogAnnotations), "effect.fiberId": options.fiber.id };
      for (const [name, start] of options.fiber.getRef(References.CurrentLogSpans)) attributes[`effect.log_span.${name}`] = options.date.getTime() - start;
      const errors = Cause.prettyErrors(options.cause, { includeCauseInStack: true });
      if (errors.length) Object.assign(attributes, {
        "exception.type": errors[0].name, "exception.message": errors[0].message,
        "exception.stacktrace": errors.map(error => error.stack).join("\n"),
      });
      const messages = Array.isArray(options.message) ? options.message : [options.message];
      const body = messages.length === 1 ? messages[0] : messages;
      Native.recordLog({
        traceId: span?.traceId, spanId: span?.spanId,
        timeUnixNano: String(BigInt(options.date.getTime()) * 1000000n),
        severityNumber: levels[options.logLevel],
        body: typeof body === "string" ? body : messages.map(message => typeof message === "string" ? message : String(message)).join(" "),
        metadata: { serviceName, scopeName: "effect", body, attributes },
      });
    } catch {
      // Includes unsupported custom log levels and throwing application values.
    }
  });
  return Layer.merge(Layer.succeed(Tracer.Tracer, tracer), Logger.layer([logger], { mergeWithExisting }));
}
