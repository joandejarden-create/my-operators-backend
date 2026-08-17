#!/usr/bin/env node
/**
 * Session-only env bootstrap for Phase 2A live validation.
 * Maps FDD_INTELLIGENCE_MODEL_API_KEY -> OPENAI_API_KEY without printing secrets.
 * Never writes secrets to disk.
 */
import "../load-env.js";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function verifyGates() {
  const fddKey = String(process.env.FDD_INTELLIGENCE_MODEL_API_KEY || "").trim();
  if (!fddKey) {
    console.error("MAPPING_FAILED: FDD_INTELLIGENCE_MODEL_API_KEY missing");
    process.exit(1);
  }
  process.env.OPENAI_API_KEY = fddKey;
  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  if (!process.env.AI_VISIBILITY_MAX_TEST_RUNS) {
    process.env.AI_VISIBILITY_MAX_TEST_RUNS = "20";
  }
  process.env.AI_VISIBILITY_MODEL = "gpt-5.6";
  process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS = "180000";

  const maxRuns = parseInt(process.env.AI_VISIBILITY_MAX_TEST_RUNS, 10);
  console.log(
    JSON.stringify(
      {
        EXISTING_FDD_KEY_REUSED: "YES",
        OPENAI_API_KEY_RESOLVED: process.env.OPENAI_API_KEY ? "PRESENT" : "MISSING",
        AI_VISIBILITY_ENABLED: process.env.AI_VISIBILITY_ENABLED,
        AI_VISIBILITY_LIVE_TEST: process.env.AI_VISIBILITY_LIVE_TEST,
        AI_VISIBILITY_MODEL: process.env.AI_VISIBILITY_MODEL,
        AI_VISIBILITY_MAX_TEST_RUNS: maxRuns,
        SECRET_EXPOSURE: "NONE",
      },
      null,
      2
    )
  );

  if (maxRuns > 20) {
    console.error("STOP: AI_VISIBILITY_MAX_TEST_RUNS exceeds 20");
    process.exit(1);
  }
}

const args = process.argv.slice(2);
if (args[0] === "--verify-only") {
  verifyGates();
  process.exit(0);
}

verifyGates();

const script = args[0];
const scriptArgs = args.slice(1);
if (!script) {
  console.error("Usage: node scripts/ai-visibility-phase2a-live-env.mjs [--verify-only] <script> [...args]");
  process.exit(1);
}

const result = spawnSync(process.execPath, [script, ...scriptArgs], {
  cwd: ROOT,
  env: process.env,
  encoding: "utf8",
  stdio: "inherit",
  windowsHide: true,
});

process.exit(result.status ?? 1);
