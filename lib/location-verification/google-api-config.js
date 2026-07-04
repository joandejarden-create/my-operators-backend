/**
 * Google Places pre-import verification — env + CLI configuration.
 * API keys are never logged.
 */

const DEFAULTS = {
  maxRequestsPerRun: 150,
  delayMs: 200,
  maxResults: 5,
  cacheEnabled: true,
  maxSearchQueriesPerCandidate: 3,
};

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function parseBool(value, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  if (value === true || value === 1 || value === "1") return true;
  if (value === false || value === 0 || value === "0") return false;
  const s = String(value).toLowerCase();
  if (["true", "yes", "on"].includes(s)) return true;
  if (["false", "no", "off"].includes(s)) return false;
  return fallback;
}

/** Prefer GOOGLE_PLACES_API_KEY when both are set. */
export function resolveGoogleApiKey() {
  const places = String(process.env.GOOGLE_PLACES_API_KEY || "").trim();
  const maps = String(process.env.GOOGLE_MAPS_API_KEY || "").trim();
  return places || maps || "";
}

/**
 * @param {string[]} [argv]
 */
export function parseGoogleVerificationCli(argv = process.argv.slice(2)) {
  function getArg(name, fallback = "") {
    const idx = argv.indexOf(name);
    return idx >= 0 ? argv[idx + 1] : fallback;
  }

  const cacheFlag = argv.includes("--cache");
  const noCacheFlag = argv.includes("--no-cache");

  let cacheEnabled = parseBool(process.env.GOOGLE_PLACES_VERIFY_CACHE_ENABLED, DEFAULTS.cacheEnabled);
  if (cacheFlag) cacheEnabled = true;
  if (noCacheFlag) cacheEnabled = false;

  return {
    file: getArg("--file"),
    country: getArg("--country", ""),
    city: getArg("--city", ""),
    output: getArg("--output", ""),
    verifiedOutput: getArg("--verified-output", ""),
    limit: parsePositiveInt(getArg("--limit", "0"), 0),
    maxRequests: parsePositiveInt(
      getArg("--max-requests", process.env.GOOGLE_PLACES_VERIFY_MAX_REQUESTS_PER_RUN),
      DEFAULTS.maxRequestsPerRun
    ),
    delayMs: parsePositiveInt(
      getArg("--delay-ms", process.env.GOOGLE_PLACES_VERIFY_DELAY_MS),
      DEFAULTS.delayMs
    ),
    maxResults: parsePositiveInt(
      getArg("--max-results", process.env.GOOGLE_PLACES_VERIFY_MAX_RESULTS),
      DEFAULTS.maxResults
    ),
    cacheEnabled,
    forceRefresh: argv.includes("--force-refresh"),
    allowMedium: argv.includes("--allow-medium"),
    verbose: argv.includes("--verbose"),
    dryRun: argv.includes("--dry-run"),
    maxSearchQueriesPerCandidate: DEFAULTS.maxSearchQueriesPerCandidate,
  };
}

/**
 * @param {number} candidateCount
 * @param {number} cacheMissCount
 * @param {{ maxSearchQueriesPerCandidate?: number }} [options]
 */
export function estimateVerificationApiRequests(candidateCount, cacheMissCount, options = {}) {
  const maxQueries = options.maxSearchQueriesPerCandidate ?? DEFAULTS.maxSearchQueriesPerCandidate;
  const misses = Math.max(0, Math.min(candidateCount, cacheMissCount));
  const estimatedMaxTextSearch = misses * maxQueries;
  const estimatedMaxPlaceDetails = misses;
  return {
    candidateCount,
    cacheMissCount: misses,
    estimatedMaxTextSearch,
    estimatedMaxPlaceDetails,
    estimatedMaxTotal: estimatedMaxTextSearch + estimatedMaxPlaceDetails,
  };
}

export function printMissingApiKeyInstructions() {
  console.log("Google API key not found.");
  console.log("");
  console.log("Set one of these in .env (prefer GOOGLE_PLACES_API_KEY when both are set):");
  console.log("  GOOGLE_PLACES_API_KEY=your_key");
  console.log("  GOOGLE_MAPS_API_KEY=your_key");
  console.log("");
  console.log("Optional guardrails:");
  console.log("  GOOGLE_PLACES_VERIFY_MAX_REQUESTS_PER_RUN=150");
  console.log("  GOOGLE_PLACES_VERIFY_DELAY_MS=200");
  console.log("  GOOGLE_PLACES_VERIFY_MAX_RESULTS=5");
  console.log("  GOOGLE_PLACES_VERIFY_CACHE_ENABLED=true");
  console.log("");
  console.log("No verification report or clean fixture was written.");
}

export { DEFAULTS as GOOGLE_VERIFICATION_DEFAULTS };
