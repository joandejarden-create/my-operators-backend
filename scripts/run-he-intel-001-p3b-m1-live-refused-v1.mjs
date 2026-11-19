#!/usr/bin/env node
/**
 * Proposed live paid entrypoint — always documents gate; refuses unless founder auth present.
 * Even with auth flags, this readiness build does not perform live network paid calls
 * until a subsequent founder-authorized execution task wires live transports.
 */
import {
  resolveP3BM1FounderPaidAuthorization,
  P3B_M1_FOUNDER_PAID_AUTH_ID,
  buildProposedExecutionCommand,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

const auth = resolveP3BM1FounderPaidAuthorization({
  authId: arg("--founder-auth") || "",
  confirmPaidExecution: process.argv.includes("--confirm-paid-execution"),
  authorizedBudgetUsd: Number(arg("--authorized-budget-usd") || 0),
});

if (!auth.ok) {
  console.log(
    JSON.stringify(
      {
        ok: false,
        error: auth.error,
        message: auth.message,
        requiredAuthId: P3B_M1_FOUNDER_PAID_AUTH_ID,
        proposed: buildProposedExecutionCommand(),
      },
      null,
      2
    )
  );
  process.exit(2);
}

console.log(
  JSON.stringify(
    {
      ok: false,
      error: "P3B_M1_LIVE_TRANSPORT_NOT_WIRED_IN_READINESS_PR",
      message:
        "Founder auth shape accepted, but this PR is readiness-only. Live paid transports remain unwired until a separate execution authorization task.",
      auth,
    },
    null,
    2
  )
);
process.exit(3);
