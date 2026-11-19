#!/usr/bin/env node
/**
 * Default: Wave 0 offline parity ($0, no network).
 * Wave 1: requires GDI_AI_AGENT_LIVE_PUBLIC_AUTH=1 and fair active Arm A/B.
 */
import {
  assertFairWave1Capability,
  runExperiment,
  WAVE1_AUTH_ENV,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-incremental-value-experiment-v1.js";

const waveArg = process.argv.find((a) => a.startsWith("--wave="));
const wave = waveArg ? waveArg.split("=")[1] : "wave0";

if (wave === "wave1" || wave === "live_public") {
  if (String(process.env[WAVE1_AUTH_ENV] || "") !== "1") {
    console.error(
      JSON.stringify(
        {
          ok: false,
          blocked: true,
          reason: `${WAVE1_AUTH_ENV}=1 required for Wave 1`,
        },
        null,
        2,
      ),
    );
    process.exit(2);
  }
  const readiness = assertFairWave1Capability();
  if (!readiness.ok) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          blocked: true,
          reason: "FAIR_WAVE1_CAPABILITY_MISSING",
          missing: readiness.missing,
        },
        null,
        2,
      ),
    );
    process.exit(3);
  }
}

const report = await runExperiment({ wave, writeReports: true });
console.log(
  JSON.stringify(
    {
      ok: report.blocked !== true,
      wave: report.wave,
      wave1Executed: report.wave1Executed,
      decision: report.decision,
      focusCaseCount: report.focusCaseCount,
      paidUsd: report.paidUsd,
      conclusionScope: report.conclusionScope,
      doesNotEstablish: report.doesNotEstablish,
      reportFile: report.reportFile || null,
      disclaimer: report.disclaimer,
    },
    null,
    2,
  ),
);
process.exitCode = report.blocked ? 3 : 0;
