/**
 * Run Google Places pre-import verification over candidate fixture points.
 */

import { resolveGoogleApiKey, estimateVerificationApiRequests } from "./google-api-config.js";
import {
  buildVerificationCacheKey,
  loadVerificationCache,
  saveVerificationCache,
  serializeCacheEntry,
  deserializeCacheEntry,
  GOOGLE_VERIFICATION_CACHE_PATH,
} from "./google-verification-cache.js";
import {
  verifyCandidateWithGoogle,
  buildVerifiedCleanPoint,
  googleTextSearch,
  googlePlaceDetails,
} from "./google-places-verifier.js";
import {
  buildVerificationReportPayload,
  buildCleanVerifiedFixturePayload,
} from "./build-verification-report.js";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function filterCandidates(points, { country, city, limit }) {
  let candidates = Array.isArray(points) ? [...points] : [];
  if (country) {
    candidates = candidates.filter(
      (p) => String(p.country || "").toLowerCase() === String(country).toLowerCase()
    );
  }
  if (city) {
    candidates = candidates.filter(
      (p) => String(p.city || "").toLowerCase() === String(city).toLowerCase()
    );
  }
  if (limit > 0) candidates = candidates.slice(0, limit);
  return candidates;
}

function countCacheMisses(candidates, cacheDoc, { forceRefresh }) {
  if (forceRefresh) return candidates.length;
  let misses = 0;
  for (const c of candidates) {
    const key = buildVerificationCacheKey(c);
    if (!cacheDoc.entries[key]) misses += 1;
  }
  return misses;
}

/**
 * @param {object} input
 * @param {object} input.payload — fixture JSON
 * @param {object} input.options — CLI/config options
 */
export async function runGooglePlacesVerification(input) {
  const payload = input.payload || {};
  const options = input.options || {};
  const candidates = filterCandidates(payload.points, options);
  const verifiedAt = new Date().toISOString();
  const country = options.country || payload.country || "";
  const cachePath = options.cachePath || GOOGLE_VERIFICATION_CACHE_PATH;

  const apiKey = options.apiKey ?? resolveGoogleApiKey();
  if (!apiKey && !options.searchTextFn) {
    return {
      ok: false,
      error: "missing_api_key",
      candidates,
    };
  }

  const cacheDoc = options.cacheEnabled ? loadVerificationCache(cachePath) : { version: 1, entries: {} };
  const cacheMissCount = options.cacheEnabled
    ? countCacheMisses(candidates, cacheDoc, options)
    : candidates.length;

  const estimate = estimateVerificationApiRequests(candidates.length, cacheMissCount, {
    maxSearchQueriesPerCandidate: options.maxSearchQueriesPerCandidate,
  });

  const preflight = {
    candidateCount: candidates.length,
    cacheEnabled: options.cacheEnabled === true,
    cacheMissCount,
    ...estimate,
    maxRequests: options.maxRequests,
    reportPath: options.output || "",
    verifiedOutputPath: options.verifiedOutput || "",
  };

  if (estimate.estimatedMaxTotal > options.maxRequests) {
    return {
      ok: false,
      error: "max_requests_exceeded",
      preflight,
      message:
        `Estimated API requests (${estimate.estimatedMaxTotal}) exceed configured max (${options.maxRequests}). ` +
        "Use --limit to reduce candidates or increase --max-requests.",
    };
  }

  if (options.dryRun) {
    return {
      ok: true,
      dryRun: true,
      preflight,
      report: null,
      cleanFixture: null,
      summary: { verifiedRecords: 0, excludedRecords: 0, candidateCount: candidates.length },
    };
  }

  let cacheHits = 0;
  let cacheMisses = 0;
  let apiRequestsMade = 0;
  const reportRecords = [];
  const cleanRows = [];
  let excludedCount = 0;

  let manuallyVerifiedCount = 0;

  for (let i = 0; i < candidates.length; i += 1) {
    const candidate = candidates[i];
    const cacheKey = buildVerificationCacheKey(candidate);
    let verification = null;
    let queryUsed = "";

    if (candidate.manuallyVerified === true) {
      verification = {
        candidateName: candidate.name,
        candidateCity: candidate.city,
        candidateCountry: candidate.country,
        candidateLatitude: candidate.latitude,
        candidateLongitude: candidate.longitude,
        candidatePointType: candidate.pointType,
        verificationStatus: "Verified",
        matchConfidence: "High",
        verificationNotes: "manual_corridor_verification; Google POI match skipped",
        recommendedName: candidate.name,
        recommendedLatitude: candidate.latitude,
        recommendedLongitude: candidate.longitude,
        sourceRow: candidate,
      };
      reportRecords.push(verification);
      const clean = buildVerifiedCleanPoint(candidate, verification, {
        allowMedium: options.allowMedium === true,
      });
      if (clean) {
        cleanRows.push(clean);
        manuallyVerifiedCount += 1;
      } else {
        excludedCount += 1;
      }
      if (options.verbose) {
        process.stdout.write(`[${i + 1}/${candidates.length}] ${candidate.name} → Manual\n`);
      }
      continue;
    }

    if (options.cacheEnabled && !options.forceRefresh && cacheDoc.entries[cacheKey]) {
      verification = deserializeCacheEntry(cacheDoc.entries[cacheKey]);
      cacheHits += 1;
    } else {
      cacheMisses += 1;
      const searchTextFn = async (query) => {
        queryUsed = query;
        apiRequestsMade += 1;
        const hits = options.searchTextFn
          ? await options.searchTextFn(query)
          : await googleTextSearch(query, apiKey, options.maxResults);
        if (options.delayMs > 0) await sleep(options.delayMs);
        return hits;
      };
      const placeDetailsFn = async (placeId) => {
        apiRequestsMade += 1;
        const details = options.placeDetailsFn
          ? await options.placeDetailsFn(placeId)
          : await googlePlaceDetails(placeId, apiKey);
        if (options.delayMs > 0) await sleep(options.delayMs);
        return details;
      };

      verification = await verifyCandidateWithGoogle(candidate, {
        apiKey,
        searchTextFn,
        placeDetailsFn,
        maxResults: options.maxResults,
      });

      if (options.cacheEnabled) {
        cacheDoc.entries[cacheKey] = serializeCacheEntry(verification, queryUsed);
        saveVerificationCache(cacheDoc, cachePath);
      }
    }

    reportRecords.push(verification);
    const clean = buildVerifiedCleanPoint(candidate, verification, {
      allowMedium: options.allowMedium === true,
    });
    if (!clean) {
      excludedCount += 1;
      continue;
    }
    cleanRows.push(clean);

    if (options.verbose) {
      process.stdout.write(
        `[${i + 1}/${candidates.length}] ${candidate.name || candidate.candidateName} → ${verification.matchConfidence}\n`
      );
    }
  }

  const report = buildVerificationReportPayload({
    verifiedAt,
    country,
    inputFile: options.inputFile || options.file || "",
    candidateCount: candidates.length,
    records: reportRecords,
    excludedRecords: excludedCount,
    cacheHits,
    cacheMisses,
    apiRequestsMade,
    allowMedium: options.allowMedium,
    notes: options.notes || [],
  });

  const cleanFixture = buildCleanVerifiedFixturePayload({
    market: payload.market || country,
    country,
    region: payload.region || "",
    verifiedAt,
    verifiedRecords: cleanRows.length,
    manuallyVerifiedRecords: manuallyVerifiedCount,
    excludedRecords: excludedCount,
    points: cleanRows,
  });

  return {
    ok: true,
    preflight,
    report,
    cleanFixture,
    summary: {
      candidateCount: candidates.length,
      verifiedRecords: cleanRows.length,
      excludedRecords: excludedCount,
      cacheHits,
      cacheMisses,
      apiRequestsMade,
    },
  };
}

export function printVerificationPreflight(preflight) {
  console.log("");
  console.log("── Google Places verification preflight ──");
  console.log("Candidate count:", preflight.candidateCount);
  console.log("Estimated max Text Search requests:", preflight.estimatedMaxTextSearch);
  console.log("Estimated max Place Details requests:", preflight.estimatedMaxPlaceDetails);
  console.log("Estimated max total API requests:", preflight.estimatedMaxTotal);
  console.log("Configured max requests per run:", preflight.maxRequests);
  console.log("Cache enabled:", preflight.cacheEnabled ? "yes" : "no");
  if (preflight.cacheMissCount != null) {
    console.log("Cache misses (estimated live calls):", preflight.cacheMissCount);
  }
  console.log("Report path:", preflight.reportPath || "(not set)");
  console.log("Clean verified output:", preflight.verifiedOutputPath || "(not set)");
  console.log("──────────────────────────────────────────");
  console.log("");
}
