#!/usr/bin/env node
/**
 * Default: Wave 0 offline parity ($0, no network).
 * Wave 1 requires GDI_AI_AGENT_LIVE_PUBLIC_AUTH=1 — not started by this script alone.
 */
import { runExperiment } from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-incremental-value-experiment-v1.js";

const waveArg = process.argv.find((a) => a.startsWith("--wave="));
const wave = waveArg ? waveArg.split("=")[1] : "wave0";

const report = await runExperiment({ wave, writeReports: true });
console.log(
  JSON.stringify(
    {
      ok: true,
      wave: report.wave,
      wave1Executed: report.wave1Executed,
      decision: report.decision,
      focusCaseCount: report.focusCaseCount,
      paidUsd: report.paidUsd,
      conclusionScope: report.conclusionScope,
      doesNotEstablish: report.doesNotEstablish,
      disclaimer: report.disclaimer,
    },
    null,
    2,
  ),
);
