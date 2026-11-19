#!/usr/bin/env node
/**
 * GDI 20-opp Phase 2 live-research PREPARATION runner.
 *
 * Default: mock / $0 (no network paid calls, no production writes).
 *
 * Live $0 official_page (founder auth required):
 *   set GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH=1
 *   node scripts/run-gdi-20opp-live-research-prep-v1.mjs --mode live_public
 *
 * Paid ≤$20: intentionally refused by this prep harness until a later
 * founder-authorized execution PR.
 */

import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runFiveCasePrepExperiment,
  LIVE_PUBLIC_AUTH_ENV,
  LIVE_PAID_AUTH_ENV,
} from "../lib/group-demand-intelligence/experiments/gdi-20opp-live-research-prep-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
const modeArg = args.includes("--mode")
  ? args[args.indexOf("--mode") + 1]
  : "mock";
const authorizePaid = args.includes("--authorize-paid-research");

if (authorizePaid || modeArg === "paid") {
  console.error(
    JSON.stringify(
      {
        ok: false,
        refused: true,
        reason:
          "Paid ≤$20 execution is NOT authorized by this prep harness. Obtain separate founder authorization before enabling paid providers.",
        requiredEnv: LIVE_PAID_AUTH_ENV,
        allowedNow: ["mock", `live_public with ${LIVE_PUBLIC_AUTH_ENV}=1`],
      },
      null,
      2,
    ),
  );
  process.exit(2);
}

const { report, outDir } = await runFiveCasePrepExperiment({
  root: ROOT,
  mode: modeArg,
  authorizeLivePublicResearch:
    modeArg === "live_public" ||
    String(process.env[LIVE_PUBLIC_AUTH_ENV] || "") === "1",
  authorizePaidResearch: false,
});

console.log(
  JSON.stringify(
    {
      ok: true,
      experiment: report.schema,
      mode: report.auth.mode,
      actualPaidUsdTotal: report.totals.actualPaidUsdTotal,
      cases: report.totals.cases,
      genuinelyNewVerifiedTotal: report.totals.genuinelyNewVerifiedTotal,
      gatedPaid: report.gatedPaidExecution.status,
      outDir: path.relative(ROOT, outDir).replace(/\\/g, "/"),
    },
    null,
    2,
  ),
);
