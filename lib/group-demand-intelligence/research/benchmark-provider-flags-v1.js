/**
 * Exa and Firecrawl stay off unless the benchmark evaluation flag is set.
 * Credential checks report presence and length only.
 */

export const BENCHMARK_EVAL_ENV = "GDI_BENCHMARK_PROVIDER_EVAL";
export const BENCHMARK_LIVE_ENV = "GDI_BENCHMARK_PROVIDER_LIVE";

const CREDENTIAL_ENV = Object.freeze({
  exa: "EXA_API_KEY",
  firecrawl: "FIRECRAWL_API_KEY",
});

function enabled(value) {
  return String(value ?? "").trim() === "1";
}

export function benchmarkCredentialName(provider) {
  return CREDENTIAL_ENV[provider] || null;
}

export function readBenchmarkFlags(env = process.env) {
  return {
    evaluationEnabled: enabled(env[BENCHMARK_EVAL_ENV]),
    liveEnabled: enabled(env[BENCHMARK_LIVE_ENV]),
  };
}

/**
 * @returns {{ configured: boolean, length: number, envName: string }}
 */
export function credentialPresence(provider, env = process.env) {
  const envName = benchmarkCredentialName(provider);
  const raw = envName ? String(env[envName] ?? "").trim() : "";
  return {
    configured: Boolean(envName) && raw.length > 0,
    length: raw.length,
    envName: envName || "",
  };
}

export function readProviderCredential(provider, env = process.env) {
  const presence = credentialPresence(provider, env);
  if (!presence.configured) return "";
  return String(env[presence.envName] ?? "").trim();
}

export function assertBenchmarkEvaluationEnabled(env = process.env) {
  if (!readBenchmarkFlags(env).evaluationEnabled) {
    const err = new Error("benchmark_provider_eval_disabled");
    err.code = "BENCHMARK_EVAL_DISABLED";
    throw err;
  }
}
