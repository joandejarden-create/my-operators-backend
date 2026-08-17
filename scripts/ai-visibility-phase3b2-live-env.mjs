#!/usr/bin/env node
/**
 * Phase 3B.2 live env bootstrap — maps credentials without printing secrets.
 * Does NOT map GOOGLE_MAPS_API_KEY to Gemini.
 */
import "../load-env.js";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { preflightProviderCredentials } from "../lib/ai-visibility/provider-credentials.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function verifyGates() {
  const cred = preflightProviderCredentials();

  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS =
    process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || "180000";
  process.env.AI_VISIBILITY_GEMINI_MODEL =
    process.env.AI_VISIBILITY_GEMINI_MODEL || "gemini-2.5-flash";
  process.env.AI_VISIBILITY_PERPLEXITY_MODEL =
    process.env.AI_VISIBILITY_PERPLEXITY_MODEL || "sonar";
  process.env.AI_VISIBILITY_CLAUDE_MODEL =
    process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6";

  console.log(
    JSON.stringify(
      {
        PHASE: "3B.2",
        ...cred,
        AI_VISIBILITY_ENABLED: process.env.AI_VISIBILITY_ENABLED,
        AI_VISIBILITY_LIVE_TEST: process.env.AI_VISIBILITY_LIVE_TEST,
        AI_VISIBILITY_GEMINI_MODEL: process.env.AI_VISIBILITY_GEMINI_MODEL,
        AI_VISIBILITY_PERPLEXITY_MODEL: process.env.AI_VISIBILITY_PERPLEXITY_MODEL,
        AI_VISIBILITY_CLAUDE_MODEL: process.env.AI_VISIBILITY_CLAUDE_MODEL,
        OPENAI_CALLS: 0,
        SECRET_EXPOSURE: "NONE",
      },
      null,
      2
    )
  );

  return cred;
}

const args = process.argv.slice(2);
if (args[0] === "--verify-only") {
  verifyGates();
  process.exit(0);
}

const cred = verifyGates();
if (cred.AUTH_PREFLIGHT_READY !== "YES") {
  console.error("No provider credentials available for validation");
  process.exit(1);
}

const script = args[0];
const scriptArgs = args.slice(1);
if (!script) {
  console.error(
    "Usage: node scripts/ai-visibility-phase3b2-live-env.mjs [--verify-only] <script> [...args]"
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
