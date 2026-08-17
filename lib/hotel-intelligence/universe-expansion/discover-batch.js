/**
 * Identity discovery batch — stage NEW_HOTEL locally; never write Airtable.
 */

import fs from "node:fs";
import path from "node:path";
import { resolveDiscoveryCity } from "./infer-city.js";
import {
  DISCOVERY_STATUS,
  mapIdentityToDiscoveryStatus,
} from "./statuses.js";
import { resolveHotelIdentity, MATCH_STATUS } from "../identity-resolve.js";
import {
  generateDealalityHotelId,
  createEmptyCanonicalHotel,
} from "../canonical-hotel.js";
import { MAP_CENSUS_FIELDS } from "../map_hotel_intelligence_fields.js";
import { normName } from "../../research-engine-v2/census-autopilot-v2/identity-dedupe.js";

export const DISCOVER_BATCH_VERSION = "universe-expansion-discover-batch-v1";

const NON_HOTEL_RE =
  /\b(convention center|centro de convenciones|stadium|arena|airport lounge|coworking|office building|apartment only|vacation rental only)\b/i;

/**
 * Load country candidates from master universe files (prefer prior holds).
 */
export function loadCountryCandidatesFromFiles(country, opts = {}) {
  const root = opts.root || process.cwd();
  const dir = path.join(
    root,
    "data/research-engine-v2/census-autopilot-v2-full-universe/candidates"
  );
  const want = normName(country);
  const holdsFp = path.join(
    root,
    "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
  );
  let holdIds = null;
  if (opts.onlyHolds !== false && fs.existsSync(holdsFp)) {
    const holds = JSON.parse(fs.readFileSync(holdsFp, "utf8"));
    holdIds = new Set(
      Object.entries(holds.by_candidate_id || {})
        .filter(([, h]) => normName(h.country) === want)
        .map(([id]) => id)
    );
  }

  const out = [];
  if (!fs.existsSync(dir)) return out;
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
    const raw = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
    const arr = Array.isArray(raw) ? raw : raw.candidates || [];
    for (const c of arr) {
      const countryName = c.origin_country || c.country;
      if (normName(countryName) !== want) continue;
      const candidate_id =
        c.candidate_id ||
        `univ_${String(c.origin_name || c.name || "").slice(0, 40)}`;
      if (holdIds && holdIds.size > 0 && !holdIds.has(candidate_id)) continue;
      out.push({
        candidate_id,
        property_name: c.origin_name || c.name || null,
        country: countryName,
        city: c.origin_city || c.city || null,
        origin_url: c.origin_url || null,
        source_url: c.origin_url || null,
        website: c.website || null,
        brand_text: c.brand || null,
        chain_text: c.family || null,
        source_type:
          c.candidate_origin === "CVENT_CHALLENGE"
            ? "cvent_candidate"
            : "independent_discovery",
        external_ids: {
          cvent_id:
            c.candidate_origin === "CVENT_CHALLENGE"
              ? c.origin_source_record_id || null
              : null,
        },
        raw_origin: c.candidate_origin,
      });
    }
  }
  return out;
}

/**
 * Run discovery for a batch of candidates against census records.
 * @param {object[]} candidates
 * @param {object[]} censusRecords — { id, fields }
 * @param {object} [opts]
 */
export function runDiscoveryBatch(candidates, censusRecords, opts = {}) {
  const limit = opts.limit != null ? Number(opts.limit) : 250;
  const batchId =
    opts.batchId ||
    `discovery_${String(opts.country || "multi")
      .toLowerCase()
      .replace(/\s+/g, "_")}_${Date.now()}`;

  const slice = candidates.slice(0, limit);
  const results = [];
  const metrics = {
    hotels_before:
      opts.hotelsBefore != null ? Number(opts.hotelsBefore) : censusRecords.length,
    processed: 0,
    DISCOVERED: 0,
    MATCHED: 0,
    NEW_HOTEL: 0,
    AMBIGUOUS: 0,
    REVIEW_REQUIRED: 0,
    REJECTED: 0,
    city_inferred: 0,
    duplicates_prevented: 0,
    brand_labels: new Set(),
    independent_new: 0,
  };

  const stagedHotels = [];
  const reviewItems = [];

  for (const c of slice) {
    metrics.processed += 1;
    const name = String(c.property_name || "").trim();
    if (!name) {
      const row = baseRow(c, DISCOVERY_STATUS.REJECTED, {
        reason: "missing_name",
      });
      results.push(row);
      metrics.REJECTED += 1;
      continue;
    }
    if (NON_HOTEL_RE.test(name)) {
      const row = baseRow(c, DISCOVERY_STATUS.REJECTED, {
        reason: "non_hotel_name",
      });
      results.push(row);
      metrics.REJECTED += 1;
      continue;
    }

    const cityRes = resolveDiscoveryCity(c);
    if (cityRes.inferred) metrics.city_inferred += 1;

    const country = String(c.country || "").trim();
    const hasMinFields = Boolean(name && country && cityRes.city);

    const resolveInput = {
      name,
      city: cityRes.city,
      country,
      address: c.address || null,
      website: c.website || c.origin_url || null,
      brand: c.brand_text || null,
      external_ids: c.external_ids || {},
    };

    const resolved = resolveHotelIdentity(resolveInput, censusRecords, {
      idRegistry: opts.idRegistry,
    });

    let discoveryStatus = mapIdentityToDiscoveryStatus(resolved, {
      hasMinFields,
      inferredCity: cityRes.inferred,
    });

    // Missing city after inference → review
    if (!cityRes.city) {
      discoveryStatus = DISCOVERY_STATUS.REVIEW_REQUIRED;
    }

    // Strong/exact match = duplicate prevented (already in census)
    if (discoveryStatus === DISCOVERY_STATUS.MATCHED) {
      metrics.duplicates_prevented += 1;
    }

    // Ambiguous always review
    if (discoveryStatus === DISCOVERY_STATUS.AMBIGUOUS) {
      // keep AMBIGUOUS
    }

    // Inferred-city news stay REVIEW_REQUIRED until human/geo confirm;
    // still stage as provisional discovery shells for universe tracking.
    let hotelId = resolved.hotel_id || null;
    let staged = null;

    if (
      discoveryStatus === DISCOVERY_STATUS.NEW_HOTEL ||
      (discoveryStatus === DISCOVERY_STATUS.REVIEW_REQUIRED &&
        hasMinFields &&
        resolved.match_status === MATCH_STATUS.NEW)
    ) {
      hotelId = generateDealalityHotelId();
      staged = createEmptyCanonicalHotel({
        hotel_id: hotelId,
        identity: {
          official_name: name,
          display_name: titleCaseName(name),
        },
        location: {
          city: cityRes.city,
          country,
          address_line_1: c.address || null,
        },
        brand: {
          brand_name: c.brand_text || null,
          parent_company_name: c.chain_text || null,
          independent: !c.brand_text,
        },
        digital: {
          website: c.website || c.origin_url || null,
        },
        verification: {
          record_confidence: cityRes.inferred ? "medium" : "high",
          review_status: discoveryStatus,
        },
        linkages: {
          external_ids: [
            c.external_ids?.cvent_id
              ? { provider: "cvent", external_id: c.external_ids.cvent_id }
              : null,
            c.external_ids?.hbx_code
              ? { provider: "hotelbeds", external_id: String(c.external_ids.hbx_code) }
              : null,
          ].filter(Boolean),
        },
      });
      staged.discovery = {
        status: discoveryStatus,
        batch_id: batchId,
        candidate_id: c.candidate_id,
        city_infer_method: cityRes.method,
        city_infer_confidence: cityRes.confidence,
        source_type: c.source_type,
        source_url: c.origin_url || c.source_url || null,
        evidence: {
          match_status: resolved.match_status,
          match_score: resolved.match_score,
          matching_reasons: resolved.matching_reasons || [],
        },
      };
      stagedHotels.push(staged);
      if (!c.brand_text) metrics.independent_new += 1;
      if (c.brand_text) metrics.brand_labels.add(String(c.brand_text));
    }

    if (
      discoveryStatus === DISCOVERY_STATUS.AMBIGUOUS ||
      discoveryStatus === DISCOVERY_STATUS.REVIEW_REQUIRED
    ) {
      reviewItems.push({
        candidate_id: c.candidate_id,
        name,
        city: cityRes.city,
        country,
        discovery_status: discoveryStatus,
        match_status: resolved.match_status,
        city_inferred: cityRes.inferred,
        source_url: c.origin_url || null,
        hotel_id: hotelId,
      });
    }

    metrics[discoveryStatus] = (metrics[discoveryStatus] || 0) + 1;

    results.push({
      ...baseRow(c, discoveryStatus, {
        city: cityRes.city,
        city_inferred: cityRes.inferred,
        city_method: cityRes.method,
        hotel_id: hotelId,
        match_status: resolved.match_status,
        match_score: resolved.match_score,
        matching_reasons: resolved.matching_reasons || [],
        candidate_matches: (resolved.candidate_matches || []).slice(0, 3),
      }),
    });
  }

  // Provisional "after" = production census + unique NEW/REVIEW staged (not production)
  const stagedNew = stagedHotels.length;
  const hotelsAfterProvisional = metrics.hotels_before + stagedNew;

  return {
    version: DISCOVER_BATCH_VERSION,
    batch_id: batchId,
    country: opts.country || null,
    airtable_writes: false,
    enrichment_ran: false,
    metrics: {
      hotels_before: metrics.hotels_before,
      hotels_after_provisional_staged: hotelsAfterProvisional,
      hotels_after_production: metrics.hotels_before,
      new_hotels_staged: metrics.NEW_HOTEL,
      review_required_staged_shells: stagedHotels.filter(
        (h) => h.discovery?.status === DISCOVERY_STATUS.REVIEW_REQUIRED
      ).length,
      matched_existing: metrics.MATCHED,
      duplicates_prevented: metrics.duplicates_prevented,
      ambiguous: metrics.AMBIGUOUS,
      review_queue: reviewItems.length,
      rejected: metrics.REJECTED,
      city_inferred: metrics.city_inferred,
      processed: metrics.processed,
      brand_label_count: metrics.brand_labels.size,
      independent_new_estimate: metrics.independent_new,
      status_counts: {
        DISCOVERED: metrics.DISCOVERED,
        MATCHED: metrics.MATCHED,
        NEW_HOTEL: metrics.NEW_HOTEL,
        AMBIGUOUS: metrics.AMBIGUOUS,
        REVIEW_REQUIRED: metrics.REVIEW_REQUIRED,
        REJECTED: metrics.REJECTED,
      },
    },
    validation: validateBatch(metrics, stagedHotels, reviewItems),
    staged_hotels: stagedHotels,
    review_items: reviewItems,
    results,
  };
}

function validateBatch(metrics, stagedHotels, reviewItems) {
  const processed = metrics.processed || 1;
  const dupRate = metrics.MATCHED / processed;
  const ambRate = metrics.AMBIGUOUS / processed;
  const reviewRate = reviewItems.length / processed;
  const reject = dupRate > 0.85 && metrics.NEW_HOTEL + metrics.REVIEW_REQUIRED < 5;
  return {
    pass: !reject,
    duplicate_rate: Math.round(dupRate * 1000) / 10,
    ambiguous_rate: Math.round(ambRate * 1000) / 10,
    review_burden_rate: Math.round(reviewRate * 1000) / 10,
    reject_reason: reject
      ? "duplicate_rate_unacceptable_near_full_match_little_new"
      : null,
    note: "Inferred-city discoveries are REVIEW_REQUIRED shells — not production inserts.",
  };
}

function baseRow(c, status, extra = {}) {
  return {
    candidate_id: c.candidate_id,
    property_name: c.property_name,
    country: c.country,
    discovery_status: status,
    source_type: c.source_type,
    ...extra,
  };
}

function titleCaseName(name) {
  return String(name)
    .split(/\s+/)
    .map((w) => (w ? w.charAt(0).toUpperCase() + w.slice(1).toLowerCase() : w))
    .join(" ");
}

/**
 * Filter census records to a country for faster resolve.
 */
export function filterCensusByCountry(records, country) {
  const want = normName(country);
  return (records || []).filter(
    (r) => normName(r.fields?.[MAP_CENSUS_FIELDS.country]) === want
  );
}
