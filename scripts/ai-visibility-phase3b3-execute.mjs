#!/usr/bin/env node
/**
 * Phase 3B.3 — Execute multi-provider baseline expansion (live).
 * Gemini validation+baseline · Perplexity baseline · Claude conditional.
 * OpenAI: 0 calls.
 */
import { executePhase3b3 } from "../lib/ai-visibility/phase3b3-orchestrator.js";
import { buildPhase3b3FinalReport } from "../lib/ai-visibility/phase3b3-report.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

console.log(
  JSON.stringify(
    {
      phase: "3B.3_EXECUTE",
      LIVE_OPENAI_CALLS: 0,
      scopes: {
        gemini: "12 validation + 84 baseline if GO",
        perplexity: "84 baseline ($15 cap)",
        claude: "billing probe + 12 validation + 84 baseline if GO",
      },
    },
    null,
    2
  )
);

const report = await executePhase3b3({ force: false });

const finalReport = buildPhase3b3FinalReport(report);
const outDir = path.join(ROOT, "data", "ai-visibility", "runtime", "phase3b3-reports");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, `phase3b3_${Date.now()}.json`);
fs.writeFileSync(outPath, JSON.stringify({ report, finalReport }, null, 2), "utf8");

console.log("\n--- FINAL REPORT ---\n");
console.log(finalReport.markdown);
console.log(`\nReport saved: ${outPath}`);

const status = finalReport.BUILD_STATUS || "";
if (status.includes("BLOCKED")) process.exit(3);
if (status.includes("PARTIAL")) process.exit(1);
process.exit(0);
