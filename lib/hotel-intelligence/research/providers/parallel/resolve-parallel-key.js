/**
 * Resolve Parallel API key from environment.
 * Repo convention: PARALLEL_KEY. Fallback: PARALLEL_API_KEY.
 * Never log the key value.
 */

export function resolveParallelKey(env = process.env) {
  return String(env.PARALLEL_KEY || env.PARALLEL_API_KEY || "").trim();
}

export function isParallelConfigured(env = process.env) {
  return Boolean(resolveParallelKey(env));
}

export function isParallelResearchEnabled(env = process.env) {
  const v = String(env.HOTEL_INTELLIGENCE_PARALLEL_ENABLED || "0")
    .trim()
    .toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}
