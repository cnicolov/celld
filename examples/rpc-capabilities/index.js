import { DurableObject, RpcTarget, WorkerEntrypoint, tracing } from "cloudflare:workers";

class Session extends RpcTarget {
  constructor(store) { super(); this.store = store; }
  hello() { return "returned RPC capability works"; }
  get nested() { return { answer: 42 }; }
  async write(value) {
    await this.store.ctx.storage.put("value", value);
    return await this.store.ctx.storage.get("value");
  }
  fail() { throw new TypeError("capability error"); }
  record(value) { (this.values ??= []).push(value); return this.values.join(","); }
  wait() { return new Promise(resolve => { this.resume = resolve; }); }
  finish() { this.resume("interleaved calls work"); }
  [Symbol.dispose]() { this.store.disposals++; }
}

export class SessionStore extends DurableObject {
  disposals = 0;
  scalar() { return "scalar RPC works"; }
  getSession() { return new Session(this); }
  async callback(subscriber) { return await subscriber.ready("callback works"); }
  async retainCallback(subscriber) {
    using retained = subscriber.dup();
    return await retained.ready("duplicate callback works");
  }
  async forward(session) { return session; }
  async invoke(session) { return await session.hello(); }
  async service(service) { return await service.greet("isolate"); }
  async reentrant(subscriber) {
    return await this.ctx.blockConcurrencyWhile(async () =>
      await subscriber.reentrant(new Session(this)));
  }
  callable() { return (value) => `callable ${value}`; }
  countDisposals() { return this.disposals; }
  tree() {
    const store = this;
    return { answer: 42, [Symbol.dispose]() { store.disposals++; } };
  }
  aliases() {
    const session = new Session(this);
    return { first: session, second: session };
  }
  stream() {
    return new ReadableStream({ start(controller) {
      controller.enqueue(new TextEncoder().encode("stream works")); controller.close();
    } });
  }
}

class Subscriber extends RpcTarget {
  ready(value) { return value; }
  async reentrant(session) { return await session.hello(); }
}

export class Greeter extends WorkerEntrypoint {
  greet(name) { return `${this.ctx.props.greeting}, ${name}`; }
}

const CHILD = `
  import { WorkerEntrypoint, RpcTarget } from "cloudflare:workers";
  class Target extends RpcTarget { hello() { return "loaded target works"; } }
  export default class extends WorkerEntrypoint {
    target() { return new Target(); }
    async callback(subscriber) { return await subscriber.ready("loaded callback works"); }
    forward(session) { return session; }
  }
`;

export default {
  async fetch(request, env, ctx) {
    const store = env.SESSIONS.getByName("probe");
    const relay = env.SESSIONS.getByName("relay");
    const name = new URL(request.url).pathname.slice(1);
    try {
      switch (name) {
        case "tracing": {
          const sync = tracing.enterSpan("invoke_agent", (span, value) => {
            span.setAttribute("gen_ai.operation.name", "invoke_agent");
            span.setAttributes({ count: 1, enabled: true, missing: undefined });
            return value + 1;
          }, 41);
          const asyncValue = await tracing.enterSpan("execute_tool", async span => {
            await Promise.resolve();
            span.setAttributes({ tool: "test" }).end();
            return 42;
          });
          const active = tracing.startActiveSpan("chat", (span, value) => {
            span.setAttributes({ model: "test" }).end();
            return value;
          }, "active span works");
          const manual = tracing.startSpan("manual");
          const instance = manual instanceof tracing.Span;
          manual.end(); manual.end();
          const error = new Error("callback error");
          let syncError = false, asyncError = false;
          try { tracing.enterSpan("sync error", () => { throw error; }); }
          catch (caught) { syncError = caught === error; }
          try { await tracing.enterSpan("async error", async () => { throw error; }); }
          catch (caught) { asyncError = caught === error; }
          return Response.json({ sync, asyncValue, active, instance, syncError, asyncError, isTraced: manual.isTraced });
        }
        case "scalar": return new Response(await store.scalar());
        case "capability": {
          using session = await store.getSession();
          return new Response(await session.hello());
        }
        case "property": {
          using session = await store.getSession();
          return Response.json(await session.nested.answer);
        }
        case "callback": return new Response(await store.callback(new Subscriber()));
        case "callback-dup": return new Response(await store.retainCallback(new Subscriber()));
        case "service": return new Response(await store.service(ctx.exports.Greeter({ props: { greeting: "hello" } })));
        case "callable": {
          using callable = await store.callable();
          return new Response(await callable("works"));
        }
        case "loaded": {
          const child = env.LOADER.get("rpc-child", () => ({
            compatibilityDate: "2026-09-04", mainModule: "child.js", modules: { "child.js": CHILD },
          })).getEntrypoint();
          using own = await child.target();
          using forwarded = await child.forward(await store.getSession());
          return Response.json([
            await own.hello(), await child.callback(new Subscriber()), await forwarded.hello(),
          ]);
        }
        case "reentrant": return new Response(await store.reentrant(new Subscriber()));
        case "forward": {
          using session = await relay.forward(await store.getSession());
          return new Response(await session.hello());
        }
        case "invoke": return new Response(await relay.invoke(await store.getSession()));
        case "duplicate": {
          const session = await store.getSession();
          using duplicate = session.dup();
          session[Symbol.dispose]();
          return new Response(await duplicate.hello());
        }
        case "disposed": {
          const session = await store.getSession();
          session[Symbol.dispose]();
          return new Response(await session.hello());
        }
        case "eager-dispose": {
          const session = await store.getSession();
          const call = session.hello();
          session[Symbol.dispose]();
          return new Response(await call);
        }
        case "order": {
          using session = await store.getSession();
          const replies = await Promise.all([session.record(1), session.record(2), session.record(3)]);
          return Response.json(replies);
        }
        case "interleave": {
          using session = await store.getSession();
          const pending = session.wait();
          await session.finish();
          return new Response(await pending);
        }
        case "error": {
          using session = await store.getSession();
          return new Response(await session.fail());
        }
        case "write": {
          using session = await store.getSession();
          return Response.json(await session.write("durable capability write"));
        }
        case "stream": {
          const reader = (await store.stream()).getReader();
          const first = await reader.read();
          first.value = new TextDecoder().decode(first.value);
          const last = await reader.read();
          return Response.json({ first, last });
        }
        case "disposals": return Response.json(await store.countDisposals());
        case "tree": {
          const result = await store.tree();
          result[Symbol.dispose](); result[Symbol.dispose]();
          return Response.json(result.answer);
        }
        case "rollback": {
          using session = await store.getSession();
          let stubError, streamError;
          try { await relay.invoke({ session, invalid: Symbol("uncloneable") }); }
          catch (error) { stubError = error.name; }
          const stream = new ReadableStream({ start(controller) { controller.close(); } });
          try { await relay.invoke({ stream, invalid: Symbol("uncloneable") }); }
          catch (error) { streamError = error.name; }
          return Response.json({ stubError, streamError, locked: stream.locked, hello: await session.hello() });
        }
        case "aliases": {
          using result = await store.aliases();
          using retained = result.second.dup();
          const same = result.first === result.second;
          result[Symbol.dispose]();
          return Response.json({ same, hello: await retained.hello() });
        }
        default: return new Response("Unknown probe", { status: 404 });
      }
    } catch (error) {
      return Response.json({ name: error.name, message: error.message, remote: error.remote ?? false }, { status: 500 });
    }
  },
};
