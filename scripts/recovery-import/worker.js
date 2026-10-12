// Real SQLite payloads, independent client checksums, and idempotent row IDs.
const ROW_BYTES = 16384;

export class ImportStore {
  constructor(state) {
    this.state = state;
    this.sql = state.storage.sql;
    this.sql.exec("CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)");
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/import" && request.method === "POST") {
      const first = Number(url.searchParams.get("first"));
      const bytes = new Uint8Array(await request.arrayBuffer());
      if (!Number.isSafeInteger(first) || first < 0 || !bytes.length || bytes.length % ROW_BYTES) {
        return new Response("invalid import range", { status: 400 });
      }
      this.state.storage.transactionSync(() => {
        for (let offset = 0; offset < bytes.length; offset += ROW_BYTES) {
          const id = first + offset / ROW_BYTES;
          const payload = bytes.slice(offset, offset + ROW_BYTES);
          const existing = this.sql.exec("SELECT payload FROM records WHERE id = ?", id).toArray();
          if (existing.length) {
            const prior = new Uint8Array(existing[0].payload);
            if (prior.length !== payload.length || prior.some((byte, i) => byte !== payload[i])) {
              throw new Error(`row ID ${id} reused with different bytes`);
            }
          } else {
            this.sql.exec("INSERT INTO records (id, payload) VALUES (?, ?)", id, payload);
          }
        }
      });
      return Response.json({ first, rows: bytes.length / ROW_BYTES, bytes: bytes.length });
    }
    if (url.pathname === "/read") {
      const first = Number(url.searchParams.get("first"));
      const count = Number(url.searchParams.get("count"));
      if (!Number.isSafeInteger(first) || !Number.isSafeInteger(count) || first < 0 || count < 1 || count > 64) {
        return new Response("invalid read range", { status: 400 });
      }
      const rows = this.sql.exec(
        "SELECT id, payload FROM records WHERE id >= ? AND id < ? ORDER BY id", first, first + count,
      ).toArray();
      if (rows.length !== count || rows.some((row, i) => row.id !== first + i)) {
        return new Response("missing records", { status: 409 });
      }
      const bytes = new Uint8Array(count * ROW_BYTES);
      rows.forEach((row, i) => bytes.set(new Uint8Array(row.payload), i * ROW_BYTES));
      return new Response(bytes, { headers: { "Content-Type": "application/octet-stream" } });
    }
    if (url.pathname === "/stats") {
      return Response.json(this.sql.exec("SELECT count(*) AS rows, coalesce(sum(length(payload)), 0) AS bytes, max(id) AS last FROM records").one());
    }
    if (url.pathname === "/integrity") {
      return Response.json(this.sql.exec("PRAGMA quick_check").toArray());
    }
    return new Response("not found", { status: 404 });
  }
}

export default {
  fetch(request, env) {
    const id = env.IMPORT.idFromName(new URL(request.url).searchParams.get("name") ?? "bulk-import");
    return env.IMPORT.get(id).fetch(request);
  },
};
