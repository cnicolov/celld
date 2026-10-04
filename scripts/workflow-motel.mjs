// node scripts/workflow-motel.mjs [project] [port]
import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
execFileSync("motel", ["daemon"], { stdio: "inherit" });
const endpoint = JSON.parse(execFileSync("motel", ["endpoints"], { encoding: "utf8" }));
const environment = {
  ...process.env,
  CELLD_OTEL: process.env.CELLD_OTEL ?? endpoint.baseUrl,
  CELLD_OTEL_FLUSH_MS: process.env.CELLD_OTEL_FLUSH_MS ?? "1000",
  OTEL_SERVICE_NAME: process.env.OTEL_SERVICE_NAME ?? "celld-workflows",
  OTEL_TRACES_SAMPLER: process.env.OTEL_TRACES_SAMPLER ?? "always_on",
};
const project = resolve(root, process.argv[2] ?? "tests/workflow-subscribe");
const port = process.argv[3] ?? "9889";
console.log(`celld ${project} → ${environment.CELLD_OTEL} (${environment.OTEL_SERVICE_NAME})`);
const child = spawn(process.env.CELLD_BINARY ?? resolve(root, "target/lab/celld"),
  ["dev", project, "--port", port, "--no-watch", "--logs"],
  { stdio: "inherit", env: environment });
process.on("SIGINT", () => child.kill("SIGINT"));
process.on("SIGTERM", () => child.kill("SIGTERM"));
const [code] = await once(child, "exit");
process.exitCode = code ?? 1;
