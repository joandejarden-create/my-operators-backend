#!/usr/bin/env node
/**
 * Proposed paid live ownership experiment entrypoint.
 * Always refuses actual spend until founder explicitly authorizes the $5 envelope
 * for THIS experiment (separate from PR merge / auth flag shape alone).
 */
import {
  resolveP3BM1FounderPaidAuthorization,
  P3B_M1_FOUNDER_PAID_AUTH_ID,
  buildExactPaidExecutionCommand,
  buildLiveExperimentManifest,
  buildP3BM1PlannedOperations,
  probeAccountPricingPresence,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

const enableLiveNetwork =
  process.argv.includes("--enable-live-network") ||
  String(process.env.HE_INTEL_P3B_M1_LIVE_NETWORK || "0") === "1";

const spendAuthorized =
  process.argv.includes("--founder-spend-authorized") ||
  String(process.env.HE_INTEL_P3B_M1_SPEND_AUTHORIZED || "0") === "1";

const auth = resolveP3BM1FounderPaidAuthorization({
  authId: arg("--founder-auth") || "",
  confirmPaidExecution: process.argv.includes("--confirm-paid-execution"),
  authorizedBudgetUsd: Number(arg("--authorized-budget-usd") || 0),
});

const manifest = buildLiveExperimentManifest();
const planned = buildP3BM1PlannedOperations();
const cmd = buildExactPaidExecutionCommand();
const pricing = probeAccountPricingPresence(process.env);

if (!auth.ok) {
  console.log(
    JSON.stringify(
      {
        ok: false,
        error: auth.error,
        message: auth.message,
        requiredAuthId: P3B_M1_FOUNDER_PAID_AUTH_ID,
        enableLiveNetwork,
        spendAuthorized,
        proposed: cmd,
        plannedTotals: planned.totals,
      },
      null,
      2
    )
  );
  process.exit(2);
}

if (!enableLiveNetwork) {
  console.log(
    JSON.stringify(
      {
        ok: false,
        error: "LIVE_NETWORK_FLAG_REQUIRED",
        message: "Auth shape accepted; --enable-live-network still required.",
        auth,
      },
      null,
      2
    )
  );
  process.exit(2);
}

if (!spendAuthorized) {
  console.log(
    JSON.stringify(
      {
        ok: false,
        error: "P3B_M1_EXPERIMENT_SPEND_NOT_AUTHORIZED",
        message:
          "Auth + live-network flags present, but the $5 experiment envelope is NOT authorized for actual expenditure. Pass --founder-spend-authorized only after separate explicit founder approval of this paid run.",
        auth,
        enableLiveNetwork: true,
        pricingPresence: pricing,
        hotels: manifest.cohort.hotels,
        exactCommand: cmd.phase2_paid_only_after_explicit_founder_approval,
        note: "This prompt / prior PR merges / auth flag availability are not spend permission.",
      },
      null,
      2
    )
  );
  process.exit(3);
}

// Even with spend flag shape, this readiness PR does not dispatch paid calls.
console.log(
  JSON.stringify(
    {
      ok: false,
      error: "P3B_M1_EXPERIMENT_DISPATCH_NOT_WIRED_IN_READINESS_PR",
      message:
        "Founder spend authorization flag accepted for shape validation only. Paid dispatch remains unwired in this no-spend readiness PR — execute only after a subsequent authorized execution task.",
      auth,
      spendAuthorized: true,
      enableLiveNetwork: true,
    },
    null,
    2
  )
);
process.exit(3);
