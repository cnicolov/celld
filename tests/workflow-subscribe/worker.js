import { DurableObject, WorkflowEntrypoint } from "cloudflare:workers";

export class Evidence extends DurableObject {
  append(value) {
    const values = this.ctx.storage.kv.get("values") ?? [];
    values.push(value);
    this.ctx.storage.kv.put("values", values);
  }
  values() { return this.ctx.storage.kv.get("values") ?? []; }
}

export class Probe extends WorkflowEntrypoint {
  async run(event, step) {
    const mode = event.payload.mode;
    const evidence = this.env.EVIDENCE.getByName(event.instanceId);
    await step.do("stable", async () => {
      await evidence.append("stable");
      return { stable: true };
    });
    if (mode === "wait" || mode === "wait-timeout") {
      return await step.waitForEvent("gate", { type: "approved", timeout: "1 second" });
    }
    if (mode === "sleep") {
      await step.sleep("nap", 80);
      await step.sleepUntil("until", Date.now() + 80);
      return "awake";
    }
    if (mode === "parallel") {
      return await Promise.all([1, 2].map((value) => step.do("same", () => value)));
    }
    if (mode === "fetch" || mode === "parallel-fetch") {
      const read = (branch) => step.do("downstream", async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        const response = await fetch(`${event.payload.url}?branch=${branch}`);
        console.log(`workflow-fetch ${event.instanceId} branch=${branch}`);
        return await response.json();
      });
      return mode === "fetch" ? await read(1) : await Promise.all([read(1), read(2)]);
    }
    if (mode === "large") {
      return await step.do("large", () => new Uint8Array(1048577));
    }
    if (mode === "many") {
      for (let index = 0; index < 35; index++) await step.do("loop", () => index);
      return "many";
    }
    if (mode === "fractional") {
      const value = await step.do("fractional", {
        retries: { limit: 1, delay: 30.5 }, timeout: 5000.5,
      }, (ctx) => { if (ctx.attempt === 1) throw new Error("retry fractional"); return "retried"; });
      await step.sleep("fractional-nap", 20.5);
      return value;
    }
    if (mode === "policy-error") {
      await step.do("policy", {
        retries: { limit: 1, delay: () => { throw new Error("policy failed"); } },
      }, () => { throw new Error("callback failed"); });
    }
    return await step.do("supplier", {
      retries: { limit: mode === "retry" || mode === "timeout" ? 2 : 0,
        delay: mode === "retry" ? () => 30 : 30, backoff: "constant" },
      timeout: mode === "timeout" ? 40 : 5000,
    }, async (ctx) => {
      await evidence.append({ attempt: ctx.attempt });
      if (mode === "timeout") await new Promise(() => {});
      if (mode === "crash" || mode === "terminate") {
        await new Promise((resolve) => setTimeout(resolve, 1500));
      }
      if (mode === "retry" && ctx.attempt < 3) throw new Error(`retry ${ctx.attempt}`);
      return { attempt: ctx.attempt };
    });
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const id = url.searchParams.get("id");
    try {
      if (url.pathname === "/health") return Response.json({ ok: true });
      if (url.pathname === "/upstream") {
        return Response.json({ traceparent: request.headers.get("traceparent") });
      }
      if (url.pathname === "/evidence") {
        return Response.json(await env.EVIDENCE.getByName(id).values());
      }
      if (url.pathname === "/create") {
        await env.PROBE.create({ id, params: {
          mode: url.searchParams.get("mode"), url: `${url.origin}/upstream`,
        },
          ...(url.searchParams.has("retention") ? { retention: {
            successRetention: Number(url.searchParams.get("retention")),
            errorRetention: Number(url.searchParams.get("retention")),
          } } : {}),
        });
        return Response.json({ id });
      }
      const instance = await env.PROBE.get(id);
      if (url.pathname === "/status") return Response.json(await instance.status());
      if (url.pathname === "/signal") {
        await instance.sendEvent({ type: "approved", payload: { approved: true } });
        return Response.json({ ok: true });
      }
      if (url.pathname === "/pause") await instance.pause();
      if (url.pathname === "/resume") await instance.resume();
      if (url.pathname === "/restart") await instance.restart();
      if (url.pathname === "/delete") await instance.delete();
      if (url.pathname === "/terminate") await instance.terminate();
      if (["/pause", "/resume", "/restart", "/delete", "/terminate"].includes(url.pathname)) {
        return Response.json({ ok: true });
      }
      const options = JSON.parse(url.searchParams.get("options") ?? "{}");
      const subscription = await instance.subscribe(options);
      if (url.pathname === "/socket") {
        const [client, server] = Object.values(new WebSocketPair());
        server.accept();
        const dispose = () => subscription[Symbol.dispose]();
        server.addEventListener("close", dispose);
        server.addEventListener("error", dispose);
        ctx.waitUntil((async () => {
          try {
            while (true) {
              const result = await subscription.next();
              if (result.done) break;
              server.send(JSON.stringify(result.value));
            }
            server.close(1000, "Ended");
          } catch {
            server.close(1011, "Reconnect");
          } finally { dispose(); }
        })());
        return new Response(null, { status: 101, webSocket: client });
      }
      if (url.pathname === "/using") {
        subscription[Symbol.dispose]();
        using scoped = await instance.subscribe(options);
        return Response.json(await scoped.next());
      }
      if (url.pathname === "/dispose") {
        const next = subscription.next();
        const concurrent = await subscription.next().then(() => false, () => true);
        subscription[Symbol.dispose]();
        subscription[Symbol.dispose]();
        return Response.json({ next: await next, later: await subscription.next(), concurrent });
      }
      const events = [];
      const maximum = Number(url.searchParams.get("max") ?? Infinity);
      try {
        while (events.length < maximum) {
          const result = await subscription.next();
          if (result.done) {
            return Response.json({ events, done: true, again: await subscription.next() });
          }
          events.push(result.value);
        }
        return Response.json({ events, done: false });
      } finally {
        subscription[Symbol.dispose]();
      }
    } catch (error) {
      return Response.json({ error: error.message }, { status: 400 });
    }
  },
};
