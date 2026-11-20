/**
 * Controlled search-strategy evaluation against fixtures + optional live cohort stats.
 */

import { extractFranchiseExpirationSchedule } from "./franchise-schedule-extractor-v2.js";
import { extractContractFindingsFromDocument } from "./contract-extractor-v1.js";
import { discoverHotelsFromDocument, dedupeDiscoveredHotels } from "./hotel-discovery-v1.js";
import { scoreTaxonomyHits, SEARCH_TAXONOMY_VERSION } from "./search-taxonomy-v2.js";
import { HISTORICAL_APPLICABILITY } from "./contract.js";

export const SEARCH_STRATEGY_EVAL_V2 = "sec-edgar-search-strategy-eval-v2";

/**
 * Evaluate extractors against a labeled fixture document.
 *
 * @param {object} fixture
 * @param {string} fixture.text
 * @param {string[]} fixture.expectedHotels — known hotel names
 * @param {Array<{hotel:string, expiration:string}>} [fixture.expectedExpirations]
 * @param {object} [meta]
 */
export function evaluateAgainstFixture(fixture, meta = {}) {
  const doc = { text: fixture.text || "" };
  const schedule = extractFranchiseExpirationSchedule(doc, meta);
  const discovery = discoverHotelsFromDocument(doc, meta);
  const contracts = extractContractFindingsFromDocument(doc, meta);
  const hotels = dedupeDiscoveredHotels([
    ...(schedule.hotels || []),
    ...(discovery.hotels || []),
  ]);

  const expectedHotels = (fixture.expectedHotels || []).map((h) => norm(h));
  const foundNames = hotels.map((h) => norm(h.hotelName));
  const tpHotels = expectedHotels.filter((e) => foundNames.some((f) => f.includes(e) || e.includes(f)));
  const recallHotels = expectedHotels.length ? tpHotels.length / expectedHotels.length : null;
  const precisionHotels = foundNames.length
    ? tpHotels.length / Math.max(1, new Set(foundNames).size)
    : null;

  const expectedExp = fixture.expectedExpirations || [];
  let expHits = 0;
  for (const e of expectedExp) {
    const hit = (schedule.findings || []).some(
      (f) =>
        norm(f.attributedHotelName || "").includes(norm(e.hotel)) &&
        String(f.primaryDate || "").includes(String(e.expiration)),
    );
    if (hit) expHits += 1;
  }
  const recallExpirations = expectedExp.length ? expHits / expectedExp.length : null;

  const histVerified = (schedule.findings || []).filter(
    (f) => f.historicalApplicability === HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
  ).length;

  // False positives: discovered hotels not near any expected name (when expectations provided)
  let falsePositives = 0;
  if (expectedHotels.length) {
    for (const f of foundNames) {
      if (!expectedHotels.some((e) => f.includes(e) || e.includes(f))) falsePositives += 1;
    }
  }

  const taxonomy = scoreTaxonomyHits(doc.text);

  return {
    version: SEARCH_STRATEGY_EVAL_V2,
    taxonomyVersion: SEARCH_TAXONOMY_VERSION,
    hotelsDiscovered: hotels.length,
    scheduleHotels: schedule.hotelCount,
    contractFindings: contracts.findingCount,
    historicallyVerifiedExpirations: histVerified,
    recallHotels,
    precisionHotelsApprox: precisionHotels,
    recallExpirations,
    falsePositiveHotelCount: falsePositives,
    duplicateHotelNames: foundNames.length - new Set(foundNames).size,
    taxonomyHitTotal: taxonomy.total,
    taxonomyByFamily: taxonomy.byFamily,
    sampleHotels: hotels.slice(0, 12).map((h) => ({
      hotelName: h.hotelName,
      city: h.city,
      stateRegion: h.stateRegion,
      discoveryMethod: h.discoveryMethod,
    })),
  };
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Aggregate coverage accounting for a research wave.
 */
export function buildCoverageAccounting(opts = {}) {
  const eligible = Number(opts.eligibleFilings || 0);
  const processed = Number(opts.processedFilings || 0);
  const remaining = Math.max(0, eligible - processed);
  const newHotels = Number(opts.newUniqueHotels || 0);
  const verifiedDates = Number(opts.verifiedContractDates || 0);
  const spend = Number(opts.paidSpendUsd || 0);
  return {
    version: SEARCH_STRATEGY_EVAL_V2,
    eligibleFilings: eligible,
    processedFilings: processed,
    remainingFilings: remaining,
    coverageRatio: eligible ? processed / eligible : null,
    newUniqueHotels: newHotels,
    verifiedOwnersOperators: Number(opts.verifiedOwnersOperators || 0),
    verifiedHistoricalRelationships: Number(opts.verifiedHistoricalRelationships || 0),
    verifiedContractDates: verifiedDates,
    falsePositiveRate: opts.falsePositiveRate != null ? opts.falsePositiveRate : null,
    duplicateRate: opts.duplicateRate != null ? opts.duplicateRate : null,
    costPerNewVerifiedFinding:
      verifiedDates + newHotels > 0 ? spend / (verifiedDates + newHotels) : null,
    note: "Batch finish ≠ comprehensive coverage. Always report eligible vs processed vs remaining.",
  };
}
