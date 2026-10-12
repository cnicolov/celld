import { WorkflowEntrypoint } from "cloudflare:workers";

export class ReportBuilder extends WorkflowEntrypoint {
  async run(event, step) {
    // A completed step returns its stored result when the Workflow resumes.
    return await step.do("build report", async () => {
      const response = await fetch(event.payload.url);
      if (!response.ok) throw new Error(`source answered ${response.status}`);
      const text = await response.text();
      return { bytes: text.length, lines: text.split("\n").length };
    });
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const id = url.searchParams.get("id");

    if (url.pathname === "/create") {
      const instance = await env.REPORTS.create({
        params: { url: url.searchParams.get("url") ?? "https://example.com" },
      });
      return Response.json({ id: instance.id });
    }

    if (url.pathname === "/status" && id) {
      const instance = await env.REPORTS.get(id);
      return Response.json(await instance.status());
    }

    if (url.pathname === "/events" && id) {
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("WebSocket upgrade required", { status: 426 });
      }
      const instance = await env.REPORTS.get(id);
      const subscription = await instance.subscribe({
        ...(url.searchParams.has("cursor")
          ? { cursor: Number(url.searchParams.get("cursor")) } : {}),
      });
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
          server.close(1000, "Workflow ended");
        } catch {
          server.close(1011, "Reconnect with the last processed eventId");
        } finally {
          dispose();
        }
      })());
      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response("Use /create?url=URL, /status?id=ID or WebSocket /events?id=ID.", {
      status: 404,
    });
  },
};
