#!/usr/bin/env node
/**
 * Multi-process worker for P3B-M1 durable ledger / claim races.
 * Usage: node scripts/he-intel-p3b-m1-multiprocess-worker-v1.mjs --mode <mode> --ledger <path> ...
 */
import {
  createDurableM1BudgetLedger,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/durable-budget-ledger.js";
import {
  createExecutionGateway,
  buildOperationId,
} from "../lib/hotel-intelligence/research-engine-v1/execution-gateway.js";
import { buildM1GatewayAdapterMap } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/adapters.js";
import { PROVIDER_IDS } from "../lib/hotel-intelligence/research-engine-v1/provider-interface.js";
import { P3B_M1_FOUNDER_PAID_AUTH_ID } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/contract.js";

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

const mode = arg("--mode");
const ledgerPath = arg("--ledger");
const workerId = arg("--worker-id", `w_${process.pid}`);
const opId = arg("--op-id", "op_shared");
const hotelId = arg("--hotel-id", "recLuxvwwxID7U2B8");
const cost = Number(arg("--cost", "1.0"));
const runCap = Number(arg("--run-cap", "5"));
const hotelCap = Number(arg("--hotel-cap", "2.5"));

function out(obj) {
  process.stdout.write(JSON.stringify({ pid: process.pid, workerId, ...obj }) + "\n");
}

try {
  if (!mode || !ledgerPath) {
    out({ ok: false, error: "MISSING_ARGS" });
    process.exit(2);
  }

  const ledger = createDurableM1BudgetLedger({
    persistPath: ledgerPath,
    memoryOnly: false,
    runUsdCap: runCap,
    perHotelUsdCap: hotelCap,
    workerId,
  });

  if (ledger.isFatal()) {
    out({ ok: false, fatal: true, ...ledger.getFatal() });
    process.exit(3);
  }

  if (mode === "reserve") {
    const r = ledger.reserve({
      operation_id: opId,
      hotel_id: hotelId,
      provider: PROVIDER_IDS.EXA,
      operation: "search_lite",
      cost_usd: cost,
      cost_usd_status: "ESTIMATED",
    });
    out({ mode, ...r });
    process.exit(r.ok ? 0 : 1);
  }

  if (mode === "claim") {
    ledger.reserve({
      operation_id: opId,
      hotel_id: hotelId,
      provider: PROVIDER_IDS.EXA,
      operation: "search_lite",
      cost_usd: cost,
      cost_usd_status: "ESTIMATED",
    });
    const c = ledger.claimDispatch(opId, { workerId });
    out({ mode, ...c });
    process.exit(c.ok ? 0 : 1);
  }

  if (mode === "dispatch") {
    let dispatchCount = 0;
    const gateway = createExecutionGateway({
      ledger,
      workerId,
      adapters: buildM1GatewayAdapterMap({
        useMockTransports: false,
        exaTransport: async (req) => {
          dispatchCount += 1;
          // Hold briefly so peer can race
          await new Promise((r) => setTimeout(r, Number(arg("--hold-ms", "50"))));
          return {
            ok: true,
            cost_usd: 0.004,
            cost_usd_status: "REPORTED",
            provider_cost_usd: 0.004,
            model_token_cost_usd: 0,
            orchestration_cost_usd: 0.002,
            facts: [],
            operation_id: req.operation_id,
          };
        },
      }),
      paidProvidersEnabled: true,
      allowBenchmarkPaidOverride: true,
      founderPaidAuthorization: {
        ok: true,
        authId: P3B_M1_FOUNDER_PAID_AUTH_ID,
      },
    });
    const fixedOp =
      opId ||
      buildOperationId({
        caseId: arg("--case-id", "mp"),
        hotelId,
        provider: PROVIDER_IDS.EXA,
        operation: "search_lite",
        params: { numResults: 8 },
        attempt: 0,
      });
    const result = await gateway.execute({
      provider: PROVIDER_IDS.EXA,
      operation: "search_lite",
      hotelId,
      caseId: arg("--case-id", "mp"),
      operationId: fixedOp,
      params: { numResults: 8 },
    });
    out({
      mode,
      dispatchCount,
      ok: result.ok,
      error: result.error || null,
      replayed: result.replayed === true,
      operation_id: result.operation_id || fixedOp,
      in_flight: result.in_flight === true,
    });
    process.exit(result.ok || result.replayed ? 0 : 1);
  }

  out({ ok: false, error: "UNKNOWN_MODE", mode });
  process.exit(2);
} catch (err) {
  out({
    ok: false,
    error: String(err?.code || err?.message || err),
  });
  process.exit(1);
}
