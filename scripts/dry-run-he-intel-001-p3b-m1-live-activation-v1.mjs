#!/usr/bin/env node
/**
 * Dry-run P3B-M1 live activation validation — $0, no paid network.
 */
import fs from "node:fs";
import path from "node:path";
import { runP3BM1LiveActivationValidation } from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";

const outDir = path.join(
  process.cwd(),
  "reports/hotel-explorer/he-intel-001-research-benchmark"
);
fs.mkdirSync(outDir, { recursive: true });

const report = await runP3BM1LiveActivationValidation({ memoryOnly: true });
const outPath = path.join(outDir, "p3b-m1-live-activation-dry-run.json");
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      phase: report.phase,
      paidExecutionAuthorized: report.paidExecutionAuthorized,
      phaseASafetyOk: report.phaseASafety?.ok,
      liveNetworkBlocked: report.liveProviderWiring?.liveNetworkDefaultBlocked,
      hotels: report.requestManifest?.hotels?.map((h) => h.name),
      worstCaseUsd: report.requestManifest?.worstCaseSpendUsd,
      executionCommand: report.executionCommand,
      outPath,
    },
    null,
    2
  )
);

process.exit(report.ok ? 0 : 1);
