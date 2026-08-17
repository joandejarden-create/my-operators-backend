/**
 * Discovery Factory pipeline — tiered staging, no production writes.
 */

import {
  resolveDiscoveryCity,
  CITY_INFER_VERSION,
} from "./city-infer.js";
import {
  assignDiscoveryConfidence,
  DISCOVERY_TIER,
  STAGE_STATUS,
  DISCOVERY_FACTORY_VERSION,
} from "./confidence.js";
import { resolveHotelIdentity, MATCH_STATUS } from "../identity-resolve.js";
import {
  generateDealalityHotelId,
  createEmptyCanonicalHotel,
} from "../canonical-hotel.js";
import { MAP_CENSUS_FIELDS } from "../map_hotel_intelligence_fields.js";
import { normName } from "../../research-engine-v2/census-autopilot-v2/identity-dedupe.js";

export { DISCOVERY_FACTORY_VERSION, CITY_INFER_VERSION };

const NON_HOTEL_RE =
  /\b(convention center|centro de convenciones|stadium|arena|airport lounge|coworking|office building|apartment only|vacation rental only|ballroom|auditorium)\b/i;

/**
 * Process one candidate through the factory pipeline.
 */
export function processDiscoveryCandidate(candidate, censusRecords, opts = {}) {
  const t0 = Date.now();
  const name = String(candidate.property_name || candidate.origin_name || "").trim();
  const country = String(candidate.country || candidate.origin_country || "").trim();

  if (!name || NON_HOTEL_RE.test(name)) {
    return finalize(
      {
        candidate_id: candidate.candidate_id,
        name,
        country,
        tier: DISCOVERY_TIER.C,
        stage_status: STAGE_STATUS.REJECTED,
        identity_confidence: 0.1,
        reasons: [!name ? "missing_name" : "non_hotel_name"],
      },
      t0
    );
  }

  const cityResult = resolveDiscoveryCity(candidate);
  const resolveInput = {
    name,
    city: cityResult.city,
    country,
    address: candidate.address || null,
    website: candidate.website || candidate.origin_url || null,
    brand: candidate.brand_text || null,
    external_ids: candidate.external_ids || {},
  };

  const resolved = resolveHotelIdentity(resolveInput, censusRecords, {
    idRegistry: opts.idRegistry,
  });

  const conf = assignDiscoveryConfidence({
    name,
    country,
    cityResult,
    resolveResult: resolved,
    source_type: candidate.source_type,
  });

  let hotelId = resolved.hotel_id || null;
  let staged = null;

  if (
    conf.stage_status === STAGE_STATUS.READY_FOR_IMPORT ||
    conf.stage_status === STAGE_STATUS.REVIEW_REQUIRED
  ) {
    hotelId = generateDealalityHotelId();
    staged = createEmptyCanonicalHotel({
      hotel_id: hotelId,
      identity: {
        official_name: name,
        display_name: titleCaseName(name),
      },
      location: {
        city: cityResult.city,
        country,
        address_line_1: candidate.address || null,
      },
      brand: {
        brand_name: candidate.brand_text || null,
        parent_company_name: candidate.chain_text || null,
        independent: !candidate.brand_text,
      },
      digital: {
        website: candidate.website || candidate.origin_url || null,
      },
      verification: {
        record_confidence: conf.identity_confidence,
        review_status: conf.stage_status,
      },
      linkages: {
        external_ids: [
          candidate.external_ids?.cvent_id
            ? { provider: "cvent", external_id: candidate.external_ids.cvent_id }
            : null,
          candidate.external_ids?.hbx_code
            ? {
                provider: "hotelbeds",
                external_id: String(candidate.external_ids.hbx_code),
              }
            : null,
        ].filter(Boolean),
      },
    });
    staged.discovery = {
      factory_version: DISCOVERY_FACTORY_VERSION,
      city_infer_version: CITY_INFER_VERSION,
      tier: conf.tier,
      stage_status: conf.stage_status,
      identity_confidence: conf.identity_confidence,
      city_confidence: conf.city_confidence,
      city_method: cityResult.method,
      city_alternate: cityResult.alternate_city || null,
      candidate_id: candidate.candidate_id,
      source_type: candidate.source_type,
      source_url: candidate.origin_url || candidate.source_url || null,
      evidence: {
        match_status: resolved.match_status,
        match_score: resolved.match_score,
        matching_reasons: resolved.matching_reasons || [],
        confidence_reasons: conf.reasons,
      },
      batch_id: opts.batchId || null,
    };
  }

  return finalize(
    {
      candidate_id: candidate.candidate_id,
      name,
      country,
      city: cityResult.city,
      city_inferred: cityResult.inferred,
      city_method: cityResult.method,
      city_confidence: cityResult.confidence,
      multi_city: Boolean(cityResult.multi_city),
      hotel_id: hotelId,
      match_status: resolved.match_status,
      match_score: resolved.match_score,
      tier: conf.tier,
      stage_status: conf.stage_status,
      identity_confidence: conf.identity_confidence,
      reasons: conf.reasons,
      review_required: conf.review_required,
      staged,
    },
    t0
  );
}

/**
 * Run a factory batch.
 */
export function runDiscoveryFactoryBatch(candidates, censusRecords, opts = {}) {
  const limit = opts.limit != null ? Number(opts.limit) : 500;
  const offset = opts.offset != null ? Number(opts.offset) : 0;
  const batchId =
    opts.batchId ||
    `factory_${String(opts.country || "multi")
      .toLowerCase()
      .replace(/\s+/g, "_")}_${limit}_${Date.now()}`;

  const slice = candidates.slice(offset, offset + limit);
  const results = [];
  const ready = [];
  const review = [];
  const rejected = [];
  const matched = [];
  let totalConf = 0;
  let totalMs = 0;

  for (const c of slice) {
    const row = processDiscoveryCandidate(c, censusRecords, {
      ...opts,
      batchId,
    });
    results.push(row);
    totalConf += row.identity_confidence || 0;
    totalMs += row.processing_ms || 0;

    if (row.stage_status === STAGE_STATUS.READY_FOR_IMPORT) {
      ready.push(row);
    } else if (row.stage_status === STAGE_STATUS.REVIEW_REQUIRED) {
      review.push(row);
    } else if (row.stage_status === STAGE_STATUS.MATCHED_EXISTING) {
      matched.push(row);
    } else {
      rejected.push(row);
    }
  }

  const processed = results.length || 1;
  const metrics = {
    hotels_before:
      opts.hotelsBefore != null ? Number(opts.hotelsBefore) : censusRecords.length,
    hotels_after_production:
      opts.hotelsBefore != null ? Number(opts.hotelsBefore) : censusRecords.length,
    candidates_processed: results.length,
    ready_for_import: ready.length,
    review_required: review.length,
    rejected: rejected.length,
    matched_existing: matched.length,
    duplicates_prevented: matched.length,
    duplicate_rate_pct: Math.round((matched.length / processed) * 1000) / 10,
    avg_identity_confidence:
      Math.round((totalConf / processed) * 1000) / 1000,
    avg_processing_ms: Math.round(totalMs / processed),
    tier_a_pct: Math.round((ready.length / processed) * 1000) / 10,
    tier_b_pct: Math.round((review.length / processed) * 1000) / 10,
    tier_c_pct: Math.round(((rejected.length + matched.length) / processed) * 1000) / 10,
    review_burden_pct: Math.round((review.length / processed) * 1000) / 10,
  };

  return {
    version: DISCOVERY_FACTORY_VERSION,
    batch_id: batchId,
    country: opts.country || null,
    airtable_writes: false,
    enrichment_ran: false,
    metrics,
    ready_for_import: ready.map(stripStaged),
    review_required: review.map(stripStaged),
    rejected: rejected.map(stripStaged),
    matched_existing: matched.map(stripStaged),
    staged_hotels: [...ready, ...review].map((r) => r.staged).filter(Boolean),
    results,
  };
}

function stripStaged(row) {
  const { staged, ...rest } = row;
  return rest;
}

function finalize(row, t0) {
  return { ...row, processing_ms: Date.now() - t0 };
}

function titleCaseName(name) {
  return String(name)
    .split(/\s+/)
    .map((w) => (w ? w.charAt(0).toUpperCase() + w.slice(1).toLowerCase() : w))
    .join(" ");
}

export function filterCensusByCountry(records, country) {
  const want = normName(country);
  return (records || []).filter(
    (r) => normName(r.fields?.[MAP_CENSUS_FIELDS.country]) === want
  );
}
