#!/usr/bin/env node
/**
 * Session-only env bootstrap for Phase 3A.11 Wave-1 live OpenAI showcase.
 * Maps FDD_INTELLIGENCE_MODEL_API_KEY -> OPENAI_API_KEY without printing secrets.
 * Never writes secrets to disk.
 *
 * Unlike Phase 2A live-env:
 * - Allows up to 168 provider attempts (84 logical × 2)
 * - Sets hard batch cost cap to founder-approved $125
 * - Forces Wave-1 store namespace
 */
import "../load-env.js";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WAVE1_ROOT } from "../lib/ai-visibility/storage/resolve-store-root.js";
import { WAVE1_HARD_CAP_USD } from "../lib/ai-visibility/wave1-cost.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function verifyGates() {
  const fddKey = String(process.env.FDD_INTELLIGENCE_MODEL_API_KEY || "").trim();
  const openaiKey = String(process.env.OPENAI_API_KEY || "").trim();
  if (!openaiKey && !fddKey) {
    console.error("MAPPING_FAILED: OPENAI_API_KEY and FDD_INTELLIGENCE_MODEL_API_KEY missing");
    process.exit(1);
  }
  if (!openaiKey && fddKey) {
    process.env.OPENAI_API_KEY = fddKey;
  }

  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.AI_VISIBILITY_MODEL = process.env.AI_VISIBILITY_MODEL || "gpt-5.6";
  process.env.AI_VISIBILITY_MAX_BATCH_COST_USD = String(WAVE1_HARD_CAP_USD);
  process.env.AI_VISIBILITY_MAX_TEST_RUNS = "168";
  process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS =
    process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || "180000";
  process.env.AI_VISIBILITY_STORE_ROOT = process.env.AI_VISIBILITY_STORE_ROOT || WAVE1_ROOT;
  process.env.AI_VISIBILITY_PROVIDER = "openai";

  console.log(
    JSON.stringify(
      {
        EXISTING_FDD_KEY_REUSED: fddKey && !openaiKey ? "YES" : openaiKey ? "OPENAI_DIRECT" : "NO",
        OPENAI_API_KEY_RESOLVED: process.env.OPENAI_API_KEY ? "PRESENT" : "MISSING",
        AI_VISIBILITY_ENABLED: process.env.AI_VISIBILITY_ENABLED,
        AI_VISIBILITY_LIVE_TEST: process.env.AI_VISIBILITY_LIVE_TEST,
        AI_VISIBILITY_MODEL: process.env.AI_VISIBILITY_MODEL,
        AI_VISIBILITY_MAX_BATCH_COST_USD: process.env.AI_VISIBILITY_MAX_BATCH_COST_USD,
        AI_VISIBILITY_MAX_TEST_RUNS: process.env.AI_VISIBILITY_MAX_TEST_RUNS,
        AI_VISIBILITY_STORE_ROOT: process.env.AI_VISIBILITY_STORE_ROOT,
        SECRET_EXPOSURE: "NONE",
        PHASE: "3A.11",
      },
      null,
      2
    )
  );
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
  console.error(
    "Usage: node scripts/ai-visibility-phase3a11-live-env.mjs [--verify-only] <script> [...args]"
  );
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
