#!/usr/bin/env node
/**
 * Phase 3B.4 live env — credential preflight; no OpenAI/Perplexity calls.
 * Does NOT pre-select gemini-3-flash-preview — model finalized by probe.
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
  // Do NOT force gemini-3-flash-preview — Phase 3B.4 probes gemini-3.6-flash first.
  delete process.env.AI_VISIBILITY_GEMINI_MODEL;
  process.env.AI_VISIBILITY_CLAUDE_MODEL =
    process.env.AI_VISIBILITY_CLAUDE_MODEL || "claude-sonnet-4-6";

  console.log(
    JSON.stringify(
      {
        PHASE: "3B.4",
        OPENAI: cred.OPENAI_CREDENTIAL || "MISSING",
        OPENAI_ENV: cred.OPENAI_ENV_VAR_USED || "OPENAI_API_KEY",
        GEMINI: cred.GEMINI_CREDENTIAL,
        GEMINI_ENV: cred.GEMINI_ENV_VAR_USED || "GEMINI_API_KEY",
        PERPLEXITY: cred.PERPLEXITY_CREDENTIAL,
        PERPLEXITY_ENV: cred.PERPLEXITY_ENV_VAR_USED || "PERPLEXITY_API_KEY",
        CLAUDE: cred.CLAUDE_CREDENTIAL,
        CLAUDE_ENV: cred.CLAUDE_ENV_VAR_USED || "ANTHROPIC_API_KEY",
        CLAUDE_TIMEOUT_MS: process.env.AI_VISIBILITY_PROVIDER_TIMEOUT_MS,
        CLAUDE_TOOL: claudeTool.SELECTED_TOOL_VERSION,
        CLAUDE_ALLOWED_CALLERS: claudeTool.SELECTED_ALLOWED_CALLERS,
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
    "Usage: node scripts/ai-visibility-phase3b4-live-env.mjs [--verify-only] <script> [...args]"
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
