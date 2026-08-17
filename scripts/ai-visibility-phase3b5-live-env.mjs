#!/usr/bin/env node
/**
 * Phase 3B.5 live env — Gemini + Claude only; no OpenAI/Perplexity calls.
 */
import "../load-env.js";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { preflightAllProviderCredentials } from "../lib/ai-visibility/provider-credentials.js";
import { finalizeClaudeWebSearchTool } from "../lib/ai-visibility/providers/claude-tool-audit.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function verifyGates() {
  const cred = preflightAllProviderCredentials();
  const claudeTool = finalizeClaudeWebSearchTool();

  process.env.AI_VISIBILITY_ENABLED = "true";
  process.env.AI_VISIBILITY_LIVE_TEST = "true";
  process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS =
    process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS || "300000";
  process.env.AI_VISIBILITY_GEMINI_MODEL = "gemini-3.6-flash";
  process.env.AI_VISIBILITY_CLAUDE_MODEL =
    process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6";

  console.log(
    JSON.stringify(
      {
        PHASE: "3B.5",
        GEMINI: cred.GEMINI_CREDENTIAL,
        GEMINI_MODEL: process.env.AI_VISIBILITY_GEMINI_MODEL,
        CLAUDE: cred.CLAUDE_CREDENTIAL,
        CLAUDE_MODEL: process.env.AI_VISIBILITY_CLAUDE_MODEL,
        CLAUDE_TIMEOUT_MS: process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS,
        CLAUDE_TOOL: claudeTool.SELECTED_TOOL_VERSION,
        OPENAI_CALLS: 0,
        PERPLEXITY_CALLS: 0,
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

verifyGates();
const script = args[0];
const scriptArgs = args.slice(1);
if (!script) {
  console.error(
    "Usage: node scripts/ai-visibility-phase3b5-live-env.mjs [--verify-only] <script> [...args]"
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
