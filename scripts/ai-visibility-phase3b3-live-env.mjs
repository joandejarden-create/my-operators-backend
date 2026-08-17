#!/usr/bin/env node
/**
 * Phase 3B.3 live env bootstrap — credential preflight without secret exposure.
 */
import "../load-env.js";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { preflightAllProviderCredentials } from "../lib/ai-visibility/provider-credentials.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function verifyGates() {
  const cred = preflightAllProviderCredentials();

  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS =
    process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || "300000";
  process.env.AI_VISIBILITY_GEMINI_MODEL =
    process.env.AI_VISIBILITY_GEMINI_MODEL || "gemini-3-flash-preview";
  process.env.AI_VISIBILITY_PERPLEXITY_MODEL =
    process.env.AI_VISIBILITY_PERPLEXITY_MODEL || "sonar";
  process.env.AI_VISIBILITY_CLAUDE_MODEL =
    process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6";

  console.log(
    JSON.stringify(
      {
        PHASE: "3B.3",
        OPENAI: cred.OPENAI_CREDENTIAL || "MISSING",
        OPENAI_ENV: cred.OPENAI_ENV_VAR_USED || null,
        GEMINI: cred.GEMINI_CREDENTIAL,
        GEMINI_ENV: cred.GEMINI_ENV_VAR_USED || "GEMINI_API_KEY",
        PERPLEXITY: cred.PERPLEXITY_CREDENTIAL,
        PERPLEXITY_ENV: cred.PERPLEXITY_ENV_VAR_USED || "PERPLEXITY_API_KEY",
        CLAUDE: cred.CLAUDE_CREDENTIAL,
        CLAUDE_ENV: cred.CLAUDE_ENV_VAR_USED || "ANTHROPIC_API_KEY",
        AI_VISIBILITY_ENABLED: process.env.AI_VISIBILITY_ENABLED,
        AI_VISIBILITY_LIVE_TEST: process.env.AI_VISIBILITY_LIVE_TEST,
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
const script = args[0];
const scriptArgs = args.slice(1);
if (!script) {
  console.error(
    "Usage: node scripts/ai-visibility-phase3b3-live-env.mjs [--verify-only] <script> [...args]"
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
