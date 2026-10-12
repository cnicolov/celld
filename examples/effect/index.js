// Copyright 2026 Deno Land Inc. Apache-2.0 license.
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Telemetry from "./telemetry.js";

const telemetry = Telemetry.layer({ serviceName: "effect-checkout" });

export default {
  async fetch(request) {
    const customer = new URL(request.url).searchParams.get("customer") || "alice";
    const program = Effect.gen(function* () {
      yield* Effect.logInfo("checkout started", { customer });
      yield* Effect.sleep(10).pipe(Effect.withSpan("effect.cart"));
      yield* Effect.logInfo("cart ready");
      return { customer, checkedOut: true };
    }).pipe(Effect.withSpan("effect.checkout"), Effect.annotateLogs({ "customer.id": customer }), Effect.provide(telemetry));
    const exit = await Effect.runPromiseExit(program);
    return Response.json(Exit.isSuccess(exit) ? exit.value : { error: "checkout failed" }, { status: Exit.isSuccess(exit) ? 200 : 500 });
  },
};
