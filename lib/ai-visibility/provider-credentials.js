/**
 * Provider credential resolution (Phase 3B.2).
 * Never prints secret values — names and status only.
 */

const GEMINI_CANDIDATES = [
  "GEMINI_API_KEY",
  "GOOGLE_GENAI_API_KEY",
  "GOOGLE_GENERATIVE_AI_API_KEY",
  "FDD_GEMINI_API_KEY",
  "FDD_GOOGLE_GENAI_API_KEY",
];

const PERPLEXITY_CANDIDATES = ["PERPLEXITY_API_KEY", "PPLX_API_KEY", "FDD_PERPLEXITY_API_KEY"];

const CLAUDE_CANDIDATES = ["ANTHROPIC_API_KEY", "CLAUDE_API_KEY", "FDD_ANTHROPIC_API_KEY"];

const OPENAI_CANDIDATES = ["OPENAI_API_KEY", "FDD_OPENAI_API_KEY"];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * @param {string[]} candidates
 * @param {string} targetEnvVar — canonical env var adapters read
 */
function resolveCredential(candidates, targetEnvVar) {
  const direct = nz(process.env[targetEnvVar]);
  if (direct) {
    return {
      status: "PRESENT",
      envVarUsed: targetEnvVar,
      mapped: false,
    };
  }

  for (const name of candidates) {
    if (name === targetEnvVar) continue;
    const val = nz(process.env[name]);
    if (!val) continue;
    process.env[targetEnvVar] = val;
    return {
      status: "MAPPED",
      envVarUsed: targetEnvVar,
      mappedFrom: name,
      mapped: true,
    };
  }

  return {
    status: "MISSING",
    envVarUsed: null,
    mapped: false,
  };
}

export function resolveGeminiCredential() {
  // Do NOT map GOOGLE_MAPS_API_KEY — different product.
  return resolveCredential(GEMINI_CANDIDATES, "GEMINI_API_KEY");
}

export function resolvePerplexityCredential() {
  return resolveCredential(PERPLEXITY_CANDIDATES, "PERPLEXITY_API_KEY");
}

export function resolveClaudeCredential() {
  return resolveCredential(CLAUDE_CANDIDATES, "ANTHROPIC_API_KEY");
}

export function resolveOpenAiCredential() {
  return resolveCredential(OPENAI_CANDIDATES, "OPENAI_API_KEY");
}

/**
 * Preflight all three new providers — never exposes secrets.
 */
export function preflightProviderCredentials() {
  const gemini = resolveGeminiCredential();
  const perplexity = resolvePerplexityCredential();
  const claude = resolveClaudeCredential();

  const ready = [gemini, perplexity, claude].some((c) => c.status === "PRESENT" || c.status === "MAPPED");

  return preflightProviderCredentialsWithOpenAi({ gemini, perplexity, claude, ready });
}

/**
 * Full credential preflight including OpenAI (names/status only).
 */
export function preflightAllProviderCredentials() {
  const openai = resolveOpenAiCredential();
  const gemini = resolveGeminiCredential();
  const perplexity = resolvePerplexityCredential();
  const claude = resolveClaudeCredential();
  const ready = [openai, gemini, perplexity, claude].some(
    (c) => c.status === "PRESENT" || c.status === "MAPPED"
  );
  return preflightProviderCredentialsWithOpenAi({
    openai,
    gemini,
    perplexity,
    claude,
    ready,
  });
}

function preflightProviderCredentialsWithOpenAi({ openai, gemini, perplexity, claude, ready }) {
  return {
    ...(openai
      ? {
          OPENAI_CREDENTIAL: openai.status,
          OPENAI_ENV_VAR_USED: openai.envVarUsed || null,
          OPENAI_MAPPED_FROM: openai.mappedFrom || null,
        }
      : {}),

    GEMINI_CREDENTIAL: gemini.status,
    GEMINI_ENV_VAR_USED: gemini.envVarUsed || (gemini.mappedFrom ? "GEMINI_API_KEY" : null),
    GEMINI_MAPPED_FROM: gemini.mappedFrom || null,

    PERPLEXITY_CREDENTIAL: perplexity.status,
    PERPLEXITY_ENV_VAR_USED: perplexity.envVarUsed || null,
    PERPLEXITY_MAPPED_FROM: perplexity.mappedFrom || null,

    CLAUDE_CREDENTIAL: claude.status,
    CLAUDE_ENV_VAR_USED: claude.envVarUsed || null,
    CLAUDE_MAPPED_FROM: claude.mappedFrom || null,
    CLAUDE_EXECUTION_READY:
      claude.status === "PRESENT" || claude.status === "MAPPED" ? true : false,

    AUTH_PREFLIGHT_READY: ready ? "YES" : "NO",
    SECRET_EXPOSURE: "NONE",
  };
}
