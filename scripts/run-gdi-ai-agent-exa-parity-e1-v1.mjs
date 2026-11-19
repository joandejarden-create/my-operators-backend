#!/usr/bin/env node
/**
 * E1 live Exa-parity — requires GDI_AI_AGENT_EXA_PARITY_AUTH=1,
 * GDI_EXA_AGENT_LIVE=1, and EXA_API_KEY. Hard ceiling $20 actual paid.
 *
 * Dotenv must load before harness import (ESM hoist) — use dynamic import.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [
  path.join(ROOT, ".env"),
  "C:/Dev/dealality/my-operators-backend/.env",
]) {
  if (fs.existsSync(p)) dotenv.config({ path: p });
}

const {
  assertE1Preflight,
  runE1Experiment,
  E1_AUTHORIZATION_ID,
} = await import(
  "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e1-harness-v1.js"
);

const preflight = assertE1Preflight(process.env);
if (!preflight.ok) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        status: "E1_BLOCKED",
        blockers: preflight.blockers,
        actualPaidUsd: 0,
        authorizationId: E1_AUTHORIZATION_ID,
        note: preflight.note,
      },
      null,
      2,
    ),
  );
  const blocked = await runE1Experiment({ writeReports: true });
  console.error(
    JSON.stringify(
      {
        reportFile: blocked.reportFile || null,
        decision: blocked.decision,
      },
      null,
      2,
    ),
  );
  process.exit(5);
}

const report = await runE1Experiment({ writeReports: true });
console.log(
  JSON.stringify(
    {
      ok: report.ok === true,
      status: report.status,
      wave: report.wave,
      actualPaidUsd: report.actualPaidUsd,
      focusCaseCount: report.focusCaseCount,
      decision: report.decision?.decision || null,
      reason: report.decision?.reason || null,
      materialCaseIds: report.materialCaseIds || [],
      reportFile: report.reportFile || null,
      authorizationId: report.authorizationId,
      disclaimer: report.disclaimer,
    },
    null,
    2,
  ),
);
process.exitCode = report.status === "E1_COMPLETE" ? 0 : 6;
