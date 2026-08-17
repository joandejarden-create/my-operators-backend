#!/usr/bin/env node
/**
 * Phase 3B.4 execute — Gemini + Claude completion only.
 * LIVE_OPENAI_CALLS=0 · LIVE_PERPLEXITY_CALLS=0
 */
import { executePhase3b4 } from "../lib/ai-visibility/phase3b4-orchestrator.js";
import { buildPhase3b4FinalReport } from "../lib/ai-visibility/phase3b4-report.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

console.log(
  JSON.stringify(
    {
      phase: "3B.4_EXECUTE",
      LIVE_OPENAI_CALLS: 0,
      LIVE_PERPLEXITY_CALLS: 0,
      scopes: {
        gemini: "model probe → 12 validation → 84 baseline if GO",
        claude: "tool audit + billing probe → 12 validation → 84 baseline if GO",
      },
    },
    null,
    2
  )
);

const report = await executePhase3b4({ force: false });
const finalReport = buildPhase3b4FinalReport(report);

const outDir = path.join(ROOT, "data", "ai-visibility", "runtime", "phase3b4-reports");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, `phase3b4_final_${Date.now()}.json`);
fs.writeFileSync(outPath, JSON.stringify({ report, finalReport }, null, 2), "utf8");

console.log("\n--- FINAL REPORT ---\n");
console.log(finalReport.markdown);
console.log(`\nReport saved: ${outPath}`);

const status = finalReport.BUILD_STATUS || "";
if (status.includes("BLOCKED")) process.exit(3);
if (status.includes("PARTIAL")) process.exit(1);
process.exit(0);
