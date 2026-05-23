/**
 * Reliable Verified Independent Hotel Census dedupe index loader (read-only).
 */

import { VERIFIED_TABLE, VERIFIED_FIELDS } from "./fields.js";
import { normalizeKey, parseCoords } from "./match-current-census.js";

const DEFAULT_OPTS = {
  maxRetries: 5,
  initialDelayMs: 2000,
  maxDelayMs: 60000,
  pageSize: 100,
  pageDelayMs: 250,
  requestTimeoutMs: 180000,
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function backoffDelay(attempt, opts) {
  const base = opts.initialDelayMs * 2 ** attempt;
  return Math.min(base, opts.maxDelayMs);
}

function isRetryableError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("econnaborted") ||
    msg.includes("etimedout") ||
    msg.includes("timeout") ||
    msg.includes("socket hang up") ||
    msg.includes("network") ||
    msg.includes("503") ||
    msg.includes("502") ||
    msg.includes("429") ||
    msg.includes("rate limit")
  );
}

/**
 * Single paginated pass (pageSize + inter-page delay).
 * @param {import('airtable').Base} base
 */
async function loadVerifiedIndexOnce(base, tableName, opts) {
  const dedupeKeys = new Set();
  const candidateLinks = new Set();
  const geoNameKeys = [];
  let verifiedRecordsLoaded = 0;
  let pageCount = 0;

  const fields = [
    VERIFIED_FIELDS.verifiedDedupeKey,
    VERIFIED_FIELDS.verifiedHotelName,
    VERIFIED_FIELDS.verifiedCountry,
    VERIFIED_FIELDS.verifiedLatitude,
    VERIFIED_FIELDS.verifiedLongitude,
    VERIFIED_FIELDS.primarySourceCandidate,
  ];

  const runPageLoop = () =>
    new Promise((resolve, reject) => {
      base(tableName)
        .select({ fields, pageSize: opts.pageSize })
        .eachPage(
          (page, next) => {
            pageCount++;
            for (const rec of page) {
              verifiedRecordsLoaded++;
              const f = rec.fields;
              const dk = f[VERIFIED_FIELDS.verifiedDedupeKey];
              if (dk) dedupeKeys.add(normalizeKey(dk));

              const links = f[VERIFIED_FIELDS.primarySourceCandidate];
              const ids = Array.isArray(links) ? links : links ? [links] : [];
              for (const id of ids) candidateLinks.add(id);

              const coords = parseCoords(
                f[VERIFIED_FIELDS.verifiedLatitude],
                f[VERIFIED_FIELDS.verifiedLongitude]
              );
              const nm = normalizeKey(f[VERIFIED_FIELDS.verifiedHotelName]);
              const co = normalizeKey(f[VERIFIED_FIELDS.verifiedCountry]);
              if (nm && co && coords) {
                geoNameKeys.push({ nm, co, coords, recordId: rec.id });
              }
            }
            if (opts.pageDelayMs > 0) {
              setTimeout(next, opts.pageDelayMs);
            } else {
              next();
            }
          },
          (err) => (err ? reject(err) : resolve())
        );
    });

  const timeoutMs = opts.requestTimeoutMs;
  await Promise.race([
    runPageLoop(),
    new Promise((_, reject) => {
      setTimeout(
        () =>
          reject(
            new Error(`Verified index load timed out after ${timeoutMs}ms`)
          ),
        timeoutMs
      );
    }),
  ]);

  return {
    dedupeKeys,
    candidateLinks,
    geoNameKeys,
    verifiedRecordsLoaded,
    pageCount,
  };
}

/**
 * @param {import('airtable').Base} base
 * @param {string} [tableName]
 * @param {Partial<typeof DEFAULT_OPTS>} [userOpts]
 */
export async function loadVerifiedDedupeIndexRobust(
  base,
  tableName = VERIFIED_TABLE,
  userOpts = {}
) {
  const opts = { ...DEFAULT_OPTS, ...userOpts };
  const startedAt = Date.now();
  let retryCount = 0;
  let lastError = null;

  for (let attempt = 0; attempt <= opts.maxRetries; attempt++) {
    try {
      const result = await loadVerifiedIndexOnce(base, tableName, opts);
      const loadDurationMs = Date.now() - startedAt;
      const meta = {
        verifiedRecordsLoaded: result.verifiedRecordsLoaded,
        dedupeKeysIndexed: result.dedupeKeys.size,
        candidateLinksIndexed: result.candidateLinks.size,
        geoNameKeysIndexed: result.geoNameKeys.length,
        pageCount: result.pageCount,
        loadDurationMs,
        retryCount,
        pageSize: opts.pageSize,
        requestTimeoutMs: opts.requestTimeoutMs,
        success: true,
      };
      return {
        dedupeKeys: result.dedupeKeys,
        candidateLinks: result.candidateLinks,
        geoNameKeys: result.geoNameKeys,
        meta,
      };
    } catch (err) {
      lastError = err;
      if (attempt >= opts.maxRetries || !isRetryableError(err)) {
        break;
      }
      retryCount++;
      const wait = backoffDelay(attempt, opts);
      console.error(
        `Verified index load attempt ${attempt + 1} failed (${err.message || err}); retry ${retryCount}/${opts.maxRetries} in ${wait}ms…`
      );
      await sleep(wait);
    }
  }

  const loadDurationMs = Date.now() - startedAt;
  throw Object.assign(
    new Error(
      `Verified dedupe index load failed after ${retryCount} retries: ${lastError?.message || lastError}`
    ),
    {
      retryCount,
      loadDurationMs,
      cause: lastError,
    }
  );
}

/**
 * Policy wrapper for backwards-match: abort on apply if load fails.
 * @param {object} policy
 * @param {boolean} policy.apply
 * @param {boolean} policy.allowMissingVerifiedIndex
 */
export async function loadVerifiedIndexWithPolicy(base, policy = {}) {
  const { apply = false, allowMissingVerifiedIndex = false } = policy;

  try {
    const loaded = await loadVerifiedDedupeIndexRobust(base, VERIFIED_TABLE);
    logVerifiedIndexMeta(loaded.meta);
    return { index: loaded, loadFailed: false, meta: loaded.meta };
  } catch (err) {
    const meta = {
      success: false,
      retryCount: err.retryCount ?? 0,
      loadDurationMs: err.loadDurationMs ?? 0,
      error: err.message || String(err),
    };

    if (apply) {
      throw new Error(
        `Aborting --apply: Verified dedupe index could not be loaded (${meta.error}).`
      );
    }

    if (!allowMissingVerifiedIndex) {
      throw new Error(
        `Verified dedupe index load failed (${meta.error}). Re-run with --allow-missing-verified-index to continue dry-run without already-verified detection.`
      );
    }

    console.error(
      `Warning: Verified dedupe index unavailable (${meta.error}); continuing with empty index (--allow-missing-verified-index).`
    );
    return {
      index: {
        dedupeKeys: new Set(),
        candidateLinks: new Set(),
        geoNameKeys: [],
      },
      loadFailed: true,
      meta,
    };
  }
}

export function logVerifiedIndexMeta(meta) {
  if (!meta?.success) return;
  console.error("--- Verified dedupe index loaded ---");
  console.error(`  Verified records loaded:   ${meta.verifiedRecordsLoaded}`);
  console.error(`  Dedupe keys indexed:     ${meta.dedupeKeysIndexed}`);
  console.error(`  Candidate links indexed: ${meta.candidateLinksIndexed}`);
  console.error(`  Geo-name keys indexed:   ${meta.geoNameKeysIndexed}`);
  console.error(`  Pages fetched:           ${meta.pageCount}`);
  console.error(`  Load duration (ms):      ${meta.loadDurationMs}`);
  console.error(`  Retries used:            ${meta.retryCount}`);
}
