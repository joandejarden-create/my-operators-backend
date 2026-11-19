#!/usr/bin/env node
/**
 * Proposed live paid entrypoint for P3B-M1.
 * Always refuse actual paid spend while proposed $5 remains NOT AUTHORIZED.
 * Validates auth + live-network flag shape for founder review.
 */
import {
  resolveP3BM1FounderPaidAuthorization,
  P3B_M1_FOUNDER_PAID_AUTH_ID,
  buildAuthorizedExecutionCommand,
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

const auth = resolveP3BM1FounderPaidAuthorization({
  authId: arg("--founder-auth") || "",
  confirmPaidExecution: process.argv.includes("--confirm-paid-execution"),
  authorizedBudgetUsd: Number(arg("--authorized-budget-usd") || 0),
});

const planned = buildP3BM1PlannedOperations();
const cmd = buildAuthorizedExecutionCommand();
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
        proposed: cmd,
        plannedTotals: planned.totals,
        pricingPresence: pricing,
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
        message:
          "Founder auth shape accepted, but --enable-live-network (or HE_INTEL_P3B_M1_LIVE_NETWORK=1) is required for any paid dispatch.",
        auth,
        proposed: cmd,
      },
      null,
      2
    )
  );
  process.exit(2);
}

// Hard stop: proposed $5 remains NOT AUTHORIZED for actual expenditure.
console.log(
  JSON.stringify(
    {
      ok: false,
      error: "P3B_M1_PAID_SPEND_NOT_AUTHORIZED_BY_FOUNDER",
      message:
        "Auth + live-network flags present, but the proposed $5 maximum remains NOT AUTHORIZED for actual expenditure. Live activation wiring is validated offline; do not dispatch paid calls until founder explicitly authorizes spend.",
      auth,
      enableLiveNetwork: true,
      transportsWired: true,
      plannedTotals: planned.totals,
      pricingPresence: pricing,
      preciseCommand: cmd.preciseAuthorizedCommand,
    },
    null,
    2
  )
);
process.exit(3);
