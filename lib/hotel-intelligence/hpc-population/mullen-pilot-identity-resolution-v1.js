/**
 * Mullen pilot — DISCOVER → MATCH identity resolution (read-only).
 * HPC-native by default (HE legacy Hotel Census independence).
 * Legacy census search permanently disabled (founder HPC-only correction).
 * Optional legacy census search only when allowLegacyHotelCensusRead /
 * HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1.
 * Never writes Airtable. Never invents CREATE authorization.
 */

import { performLiveHpcIdentityCollisionCheck } from "../../hotel-census/hpc-live-identity-collision-v1.js";
import { searchHotelCensusIdentityCandidates } from "./hotel-census-identity-search-v1.js";
import { migrationReadAllowed } from "./legacy-hotel-census-access-guard-v1.js";
import {
  IDENTITY_OUTCOME,
  MULLEN_PILOT_COHORT_VERSION,
  MULLEN_PILOT_HOTELS,
  MULLEN_PILOT_BASE_ID_CANONICAL,
  MULLEN_PILOT_HPC_TABLE_ID,
  founderPreviewUrl,
  mapHpcCollisionToIdentityOutcome,
} from "./mullen-pilot-cohort-v1.js";

export const MULLEN_PILOT_IDENTITY_RESOLUTION_V1 = "MULLEN_PILOT_IDENTITY_RESOLUTION_V1";

function normKey(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function jaccardSimple(a, b) {
  const ta = new Set(normKey(a).split(" ").filter(Boolean));
  const tb = new Set(normKey(b).split(" ").filter(Boolean));
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter += 1;
  return inter / (ta.size + tb.size - inter);
}

/**
 * Resolve one pilot hotel against HPC (default).
 * @param {object} hotel
 * @param {object} [opts]
 * @param {boolean} [opts.allowLegacyHotelCensusRead]
 */
export async function resolvePilotHotelIdentity(hotel, opts = {}) {
  const startedAt = new Date().toISOString();
  const allowLegacy = migrationReadAllowed(opts);
  const collision = await performLiveHpcIdentityCollisionCheck({
    hotelName: hotel.hotelName,
    aliases: hotel.aliases || [],
    city: hotel.city,
    country: hotel.country,
    brand: hotel.brand,
    knownHpcId: hotel.knownHpcId || null,
  });

  let census = {
    ok: true,
    hits: [],
    queryLog: [],
    skipped: !allowLegacy,
    skipReason: allowLegacy
      ? null
      : "HE_LEGACY_HOTEL_CENSUS_INDEPENDENCE — HPC-only identity resolution",
  };
  if (allowLegacy) {
    try {
      census = await searchHotelCensusIdentityCandidates({
        hotelName: hotel.hotelName,
        aliases: hotel.aliases || [],
        city: hotel.city,
        brand: hotel.brand,
        country: hotel.country,
        allowLegacyHotelCensusRead: true,
      });
      census.skipped = false;
    } catch (err) {
      census = {
        ok: false,
        hits: [],
        queryLog: [],
        skipped: false,
        error: err.message || String(err),
      };
    }
  }

  const strongCensus = allowLegacy
    ? (census.hits || []).filter((h) => h.score >= 0.72)
    : [];
  let identityOutcome = mapHpcCollisionToIdentityOutcome(collision, strongCensus);

  // Exact HPC name match may still land in collision review when weaker near-neighbors exist
  // (e.g. Dreams Cap Cana vs Secrets Cap Cana). Promote only on exact/near-exact top hit.
  let exactNamePromotion = null;
  if (
    identityOutcome === IDENTITY_OUTCOME.AMBIGUOUS_MATCH &&
    Array.isArray(collision?.candidates) &&
    collision.candidates.length
  ) {
    const top = collision.candidates[0];
    const topName = String(top?.name || top?.canonicalName || "");
    if (
      top?.id &&
      Number(top.score) >= 0.98 &&
      (jaccardSimple(hotel.hotelName, topName) >= 0.92 ||
        normKey(hotel.hotelName) === normKey(topName))
    ) {
      identityOutcome = IDENTITY_OUTCOME.MATCHED_HPC;
      exactNamePromotion = {
        promotedFrom: "AMBIGUOUS_MATCH",
        reason: "exact_or_near_exact_property_name",
        nearNeighbors: collision.candidates.slice(1, 4).map((c) => ({
          id: c.id,
          name: c.name,
          score: c.score,
        })),
      };
      collision.hpcRecordId = top.id;
      collision.verifiedRecord = top;
      collision.matchScore = top.score;
      collision.matchMethod = "exact_name_amid_neighbors";
    }
  }

  // Alias match: HPC record name aligns with a prior/brand alias more than the listing name.
  let aliasMatched = false;
  if (
    identityOutcome === IDENTITY_OUTCOME.MATCHED_HPC &&
    collision?.hpcRecordId &&
    !hotel.knownHpcId &&
    (hotel.aliases || []).length
  ) {
    const hitName = String(
      collision.verifiedRecord?.name || collision.verifiedRecord?.canonicalName || ""
    );
    const listingScore = jaccardSimple(hotel.hotelName, hitName);
    const aliasScore = Math.max(
      0,
      ...(hotel.aliases || []).map((a) => jaccardSimple(a, hitName))
    );
    aliasMatched = aliasScore >= 0.72 && aliasScore > listingScore + 0.08;
  }

  const finalOutcome =
    identityOutcome === IDENTITY_OUTCOME.MATCHED_HPC && aliasMatched
      ? IDENTITY_OUTCOME.MATCHED_EXISTING_ALIAS
      : identityOutcome;

  const hpcId = collision?.hpcRecordId || null;

  return {
    pilotKey: hotel.pilotKey,
    hotelName: hotel.hotelName,
    identityOutcome: finalOutcome,
    hpcRecordId: hpcId,
    hotelCensusRecordIds: strongCensus.map((h) => h.id),
    matchScore: collision?.matchScore ?? strongCensus[0]?.score ?? null,
    matchMethod: collision?.matchMethod || null,
    hpcCollisionOutcome: collision?.outcome || null,
    hpcCandidates: (collision?.candidates || []).slice(0, 5).map((c) => ({
      id: c.id,
      name: c.name,
      brand: c.brand,
      city: c.city,
      country: c.country,
      score: c.score,
    })),
    hotelCensusHits: strongCensus.slice(0, 5),
    founderPreviewUrl: founderPreviewUrl(hpcId),
    writeBlocked: true,
    applyAuthorized: false,
    notes: hotel.notes || null,
    checkedAt: startedAt,
    exactNamePromotion,
    evidence: {
      hpcQueryLog: collision?.queryLog || null,
      censusQueryLog: census.queryLog || null,
      censusError: census.error || null,
      censusSkipped: Boolean(census.skipped),
      censusSkipReason: census.skipReason || null,
      legacyHotelCensusAccess: allowLegacy ? "MIGRATION_READ" : "BLOCKED_HPC_ONLY",
    },
  };
}

/**
 * Resolve the full eight-hotel cohort (sequential to respect Airtable rate limits).
 * Default: HPC-only (no legacy Hotel Census requests).
 */
export async function resolveMullenPilotIdentities({
  hotels = MULLEN_PILOT_HOTELS,
  allowLegacyHotelCensusRead = false,
} = {}) {
  const startedAt = new Date().toISOString();
  const results = [];
  for (const hotel of hotels) {
    // eslint-disable-next-line no-await-in-loop
    results.push(
      await resolvePilotHotelIdentity(hotel, { allowLegacyHotelCensusRead })
    );
  }

  const summary = {
    total: results.length,
    MATCHED_HPC: results.filter((r) => r.identityOutcome === IDENTITY_OUTCOME.MATCHED_HPC).length,
    MATCHED_EXISTING_ALIAS: results.filter(
      (r) => r.identityOutcome === IDENTITY_OUTCOME.MATCHED_EXISTING_ALIAS
    ).length,
    MATCHED_HOTEL_CENSUS: results.filter(
      (r) => r.identityOutcome === IDENTITY_OUTCOME.MATCHED_HOTEL_CENSUS
    ).length,
    NEW_HPC_CANDIDATE: results.filter(
      (r) => r.identityOutcome === IDENTITY_OUTCOME.NEW_HPC_CANDIDATE
    ).length,
    AMBIGUOUS_MATCH: results.filter((r) => r.identityOutcome === IDENTITY_OUTCOME.AMBIGUOUS_MATCH)
      .length,
    IDENTITY_UNRESOLVED: results.filter(
      (r) => r.identityOutcome === IDENTITY_OUTCOME.IDENTITY_UNRESOLVED
    ).length,
  };

  return {
    contract: MULLEN_PILOT_IDENTITY_RESOLUTION_V1,
    version: MULLEN_PILOT_COHORT_VERSION,
    phase: "PHASE_1_IDENTITY_RESOLUTION",
    applyAuthorized: false,
    dryRun: true,
    airtableWrites: 0,
    canonicalBaseId: MULLEN_PILOT_BASE_ID_CANONICAL,
    hpcTableId: MULLEN_PILOT_HPC_TABLE_ID,
    startedAt,
    completedAt: new Date().toISOString(),
    summary,
    results,
    nextPhaseGate:
      "STOP before production Airtable apply — complete Phase 2–4 dry-run packet for founder review.",
  };
}
