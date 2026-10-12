// Copyright 2026 Deno Land Inc. Apache-2.0 license.

export class GPUCell {
  constructor(state) { this.state = state; }

  async count() { return (await this.state.storage.get("count")) || 0; }

  async fetch(request) {
    const url = new URL(request.url);
    const mode = url.searchParams.get("mode");
    if (mode === "alarm") await this.state.storage.setAlarm(Date.now() + 500);
    if (mode !== "read" && mode !== "alarm") {
      await this.state.storage.put("count", (await this.count()) + 1);
    }
    return Response.json({
      id: this.state.id.toString(), count: await this.count(),
      alarms: (await this.state.storage.get("alarms")) || 0,
    });
  }

  async alarm() {
    await this.state.storage.put("alarms", ((await this.state.storage.get("alarms")) || 0) + 1);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") || "gpu-session";
    try {
      // Ordinary get()/getByName() work too once the ID has been registered.
      const cell = url.searchParams.get("mode") === "legacy"
        ? env.MYCELL.getByName(name)
        : await env.MYCELL.create({
          ...(url.searchParams.has("unique") ? {} : { name }),
          placement: url.searchParams.get("placement") || "gpu",
        });
      if (url.searchParams.get("mode") === "register") return Response.json({ id: cell.id.toString() });
      if (url.searchParams.get("mode") === "rpc") return Response.json({ id: cell.id.toString(), count: await cell.count() });
      return await cell.fetch(request);
    } catch (error) {
      return Response.json({ error: error.message }, { status: 503 });
    }
  },
};
