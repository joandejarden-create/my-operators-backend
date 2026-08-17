/**
 * Tripadvisor room-count enrichment orchestration (read-only toward census).
 *
 * Data model (local / staged — not auto-written to Airtable):
 *   rooms_authoritative, rooms_candidate, rooms_source, rooms_source_url,
 *   rooms_verified_at, rooms_confidence, rooms_verification_status
 * + tripadvisor_id, website, email, phone, hotelClass, amenities
 */

import {
  TRIPADVISOR_ROOMS_VERSION,
  MAP_PROVIDER_TRIPADVISOR_APIFY,
  ROOMS_VERIFICATION_STATUS,
  ROOM_COMPARE,
  PPE_USD,
} from "./constants.js";
import { buildTripadvisorResolutionPlan } from "./query-urls.js";
import {
  matchTripadvisorHotel,
  classifyRoomCompare,
  usableTripadvisorRooms,
  TRIPADVISOR_MATCH_VERSION,
} from "./match.js";
import { verifyTripadvisorRoomCandidate, TRIPADVISOR_VERIFY_VERSION } from "./verify.js";

export const TRIPADVISOR_ENRICH_VERSION = "tripadvisor-enrich-v2";

function emptyEnrichment(hotel, overrides = {}) {
  return {
    dealality_record_id: hotel.record_id || hotel.hotel_id || null,
    dealality_name: hotel.name || null,
    dealality_country: hotel.country || null,
    dealality_city: hotel.city || null,
    rooms_authoritative:
      hotel.rooms != null && Number.isFinite(Number(hotel.rooms))
        ? Number(hotel.rooms)
        : hotel.room_count != null && Number.isFinite(Number(hotel.room_count))
          ? Number(hotel.room_count)
          : null,
    rooms_candidate: null,
    rooms_source: null,
    rooms_source_url: null,
    rooms_verified_at: null,
    rooms_confidence: null,
    rooms_verification_status: ROOMS_VERIFICATION_STATUS.UNRESOLVED,
    room_compare_vs_authoritative: ROOM_COMPARE.NO_MATCH,
    match_confidence: null,
    false_match_rejected: false,
    rejection: null,
    tripadvisor: {
      id: null,
      name: null,
      webUrl: null,
      website: null,
      email: null,
      phone: null,
      hotelClass: null,
      amenities: null,
      numberOfRooms: null,
    },
    resolution_plan: null,
    verification: null,
    ...overrides,
  };
}

/**
 * Enrich one hotel from a Tripadvisor Actor result pool (offline or MCP-fetched).
 * Never writes Airtable.
 *
 * @param {object} hotel
 * @param {object[]} taPool
 * @param {object} [opts]
 */
export async function enrichHotelTripadvisorRooms(hotel, taPool, opts = {}) {
  const retrievedAt = new Date().toISOString().slice(0, 10);
  const plan = buildTripadvisorResolutionPlan(hotel);
  const base = emptyEnrichment(hotel, { resolution_plan: plan });

  const { match, rejection } = matchTripadvisorHotel(hotel, taPool);
  if (!match) {
    const falseRejected =
      rejection?.reason === "sister_brand_room_conflict" ||
      rejection?.reason === "sister_brand_or_property_token_mismatch";
    return emptyEnrichment(hotel, {
      resolution_plan: plan,
      rejection,
      false_match_rejected: Boolean(falseRejected),
      rooms_verification_status: falseRejected
        ? ROOMS_VERIFICATION_STATUS.FALSE_MATCH_REJECTED
        : ROOMS_VERIFICATION_STATUS.UNRESOLVED,
      room_compare_vs_authoritative: ROOM_COMPARE.NO_MATCH,
    });
  }

  const it = match.item;
  const taRooms = usableTripadvisorRooms(it) ? Number(it.numberOfRooms) : null;
  const authoritative = base.rooms_authoritative;

  let room_compare_vs_authoritative = ROOM_COMPARE.NO_MATCH;
  if (authoritative != null) {
    room_compare_vs_authoritative = classifyRoomCompare(authoritative, taRooms, it);
  } else if (taRooms == null) {
    room_compare_vs_authoritative = ROOM_COMPARE.MISSING;
  }

  const tripadvisor = {
    id: it.id != null ? String(it.id) : null,
    name: it.name || null,
    webUrl: it.webUrl || null,
    website: it.website || null,
    email: it.email || null,
    phone: it.phone || null,
    hotelClass: it.hotelClass != null ? String(it.hotelClass) : null,
    amenities: Array.isArray(it.amenities) ? it.amenities.slice(0, 40) : null,
    numberOfRooms: taRooms,
  };

  // Authoritative path: never overwrite — log comparison; still attach TA side data
  if (authoritative != null) {
    let status = ROOMS_VERIFICATION_STATUS.AUTHORITATIVE_MISSING_TA;
    if (room_compare_vs_authoritative === ROOM_COMPARE.EXACT)
      status = ROOMS_VERIFICATION_STATUS.AUTHORITATIVE_EXACT;
    else if (room_compare_vs_authoritative === ROOM_COMPARE.NEAR_MATCH)
      status = ROOMS_VERIFICATION_STATUS.AUTHORITATIVE_NEAR;
    else if (room_compare_vs_authoritative === ROOM_COMPARE.CONFLICT)
      status = ROOMS_VERIFICATION_STATUS.AUTHORITATIVE_CONFLICT;

    return emptyEnrichment(hotel, {
      resolution_plan: plan,
      rooms_candidate: taRooms,
      rooms_source: MAP_PROVIDER_TRIPADVISOR_APIFY,
      rooms_source_url: it.webUrl || null,
      rooms_verified_at: retrievedAt,
      rooms_confidence: match.confidence === "high" ? 0.7 : match.confidence === "medium" ? 0.6 : 0.45,
      rooms_verification_status: status,
      room_compare_vs_authoritative,
      match_confidence: match.confidence,
      match_score: match.score,
      tripadvisor,
      note: "authoritative_rooms_preserved_no_overwrite",
    });
  }

  // Missing authoritative rooms → candidate + verification waterfall
  if (taRooms == null) {
    return emptyEnrichment(hotel, {
      resolution_plan: plan,
      rooms_source: MAP_PROVIDER_TRIPADVISOR_APIFY,
      rooms_source_url: it.webUrl || null,
      rooms_verified_at: retrievedAt,
      rooms_verification_status: ROOMS_VERIFICATION_STATUS.UNRESOLVED,
      room_compare_vs_authoritative: ROOM_COMPARE.MISSING,
      match_confidence: match.confidence,
      match_score: match.score,
      tripadvisor,
    });
  }

  const hotelForVerify = {
    ...hotel,
    // Prefer census official URL; fall back to Tripadvisor-listed property website
    website: hotel.website || it.website || null,
    websites: [hotel.website, it.website].filter(Boolean),
  };

  const verification = await verifyTripadvisorRoomCandidate(hotelForVerify, taRooms, opts);

  return emptyEnrichment(hotel, {
    resolution_plan: plan,
    rooms_candidate: taRooms,
    rooms_source: MAP_PROVIDER_TRIPADVISOR_APIFY,
    rooms_source_url: it.webUrl || null,
    rooms_verified_at: retrievedAt,
    rooms_confidence: verification.rooms_confidence,
    rooms_verification_status: verification.rooms_verification_status,
    preferred_verified_rooms: verification.preferred_verified_rooms ?? null,
    room_compare_vs_authoritative: ROOM_COMPARE.MISSING,
    match_confidence: match.confidence,
    match_score: match.score,
    tripadvisor,
    verification: {
      version: verification.version,
      agreeing_count: verification.agreeing.length,
      conflicting_count: verification.conflicting.length,
      independent_count: verification.independent_observations.length,
      official_website_room_count_found: verification.official_website_room_count_found,
      official_pdf_factsheet_found: verification.official_pdf_factsheet_found,
      secondary_source_verifications: verification.secondary_source_verifications,
      multi_source_independence: verification.multi_source_independence,
      conflict_analysis: verification.conflict_analysis,
      cost_signals: verification.cost_signals,
      steps: verification.steps,
      agreeing: verification.agreeing.slice(0, 8),
      conflicting: verification.conflicting.slice(0, 8),
      evidence_audit: verification.agreeing
        .concat(verification.conflicting)
        .slice(0, 12)
        .map((e) => ({
          value: e.value,
          provider: e.provider,
          source_category: e.source_category,
          url: e.url,
          quote: e.quote || e.evidence_text,
          independence_cluster: e.independence_cluster,
          is_pdf_factsheet: e.is_pdf_factsheet,
          retrieved: e.retrieval_timestamp,
        })),
    },
  });
}

/**
 * Batch enrich.
 * @param {object[]} hotels
 * @param {object[]} taPool
 * @param {object} [opts]
 */
export async function enrichHotelsTripadvisorRooms(hotels, taPool, opts = {}) {
  const rows = [];
  for (const hotel of hotels || []) {
    // eslint-disable-next-line no-await-in-loop
    const row = await enrichHotelTripadvisorRooms(hotel, taPool, opts);
    rows.push(row);
  }
  return summarizeEnrichmentBatch(rows, opts.cost);
}

export function summarizeEnrichmentBatch(rows, cost = null) {
  const list = rows || [];
  const candidates = list.filter((r) => r.rooms_candidate != null);
  const missingAuthCandidates = candidates.filter((r) => r.rooms_authoritative == null);
  const primary = list.filter(
    (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.VERIFIED_PRIMARY_SOURCE
  );
  const multi = list.filter(
    (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.VERIFIED_MULTI_SOURCE
  );
  const single = list.filter(
    (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.CANDIDATE_SINGLE_SOURCE
  );
  const independenceUncertain = list.filter(
    (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.SOURCE_INDEPENDENCE_UNCERTAIN
  );
  const conflicts = list.filter(
    (r) =>
      r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.CONFLICT_REVIEW_REQUIRED ||
      r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.AUTHORITATIVE_CONFLICT
  );
  const unresolved = list.filter(
    (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.UNRESOLVED
  );
  const falseRejects = list.filter((r) => r.false_match_rejected);
  const verifiedCount = primary.length + multi.length;
  const totalUsd = cost?.total_usd ?? null;
  const taMatches = list.filter((r) => r.tripadvisor?.id);
  const officialFound = list.filter((r) => r.verification?.official_website_room_count_found).length;
  const pdfFound = list.filter((r) => r.verification?.official_pdf_factsheet_found).length;
  const secondary = list.reduce(
    (n, r) => n + Number(r.verification?.secondary_source_verifications || 0),
    0
  );

  const candN = missingAuthCandidates.length || 0;
  const sampleN = list.length || 1;

  return {
    version: TRIPADVISOR_ENRICH_VERSION,
    match_version: TRIPADVISOR_MATCH_VERSION,
    verify_version: TRIPADVISOR_VERIFY_VERSION,
    module_version: TRIPADVISOR_ROOMS_VERSION,
    production_writes: false,
    counts: {
      hotels: list.length,
      tripadvisor_matches: taMatches.length,
      tripadvisor_room_candidates: candN,
      new_room_candidates: candN,
      primary_source_verified: primary.length,
      multi_source_verified: multi.length,
      single_source_candidates: single.length,
      source_independence_uncertain: independenceUncertain.length,
      conflicts: conflicts.length,
      unresolved: unresolved.length,
      false_match_rejections: falseRejects.length,
      official_website_room_count_found: officialFound,
      official_pdf_factsheet_found: pdfFound,
      secondary_source_verifications: secondary,
      authoritative_exact: list.filter(
        (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.AUTHORITATIVE_EXACT
      ).length,
      authoritative_near: list.filter(
        (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.AUTHORITATIVE_NEAR
      ).length,
      authoritative_conflict: list.filter(
        (r) => r.rooms_verification_status === ROOMS_VERIFICATION_STATUS.AUTHORITATIVE_CONFLICT
      ).length,
    },
    rates: {
      tripadvisor_match_rate:
        Math.round((1000 * taMatches.length) / sampleN) / 10,
      room_candidate_rate:
        Math.round((1000 * candN) / sampleN) / 10,
      candidate_to_verified_conversion:
        candN > 0 ? Math.round((1000 * verifiedCount) / candN) / 10 : null,
      room_resolution_rate:
        Math.round((1000 * verifiedCount) / sampleN) / 10,
    },
    cost: cost || null,
    cost_per_verified_room_count:
      verifiedCount > 0 && totalUsd != null
        ? Math.round((totalUsd / verifiedCount) * 10000) / 10000
        : null,
    cost_per_room_candidate:
      candN > 0 && totalUsd != null
        ? Math.round((totalUsd / candN) * 10000) / 10000
        : null,
    ppe_reference_usd: PPE_USD,
    rows: list,
  };
}
