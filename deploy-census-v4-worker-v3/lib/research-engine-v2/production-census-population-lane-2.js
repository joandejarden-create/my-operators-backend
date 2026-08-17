/**
 * Production Census Population Lane 2:
 * - Provenance backfill for existing coordinates (no lat/lng change)
 * - Safe VIC gap enrichment (amenities / type / asset / market / flags)
 * - Geocode proposals stay blocked until provider/storage decision
 *
 * No Brand Explorer writes. No owner/operator/rooms/dates. No fabricated coords.
 */

import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  MAP_FIRST_PASS,
  loadActiveBrandUniverse,
  mapCensusBrand,
  loadVicClaimIndex,
  resolveDealalityMarketSubmarket,
  FORBIDDEN_WRITE_FIELDS as FIRST_PASS_FORBIDDEN,
} from "./production-census-first-pass-enrichment.js";
import { resolveGeocodingProvider } from "./production-census-geocoding-providers.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const LANE2_VERSION = "production-census-population-lane-2-v1";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
export const EXPECTED_RECORD_COUNT = 666;
export const EXPECTED_FIELD_COUNT = 108;

export const STATUS = Object.freeze({
  DRY_RUN_READY: "production_census_population_lane_2_dry_run_ready_for_founder_review",
  APPLIED: "production_census_population_lane_2_applied_ready_for_next_population_lane",
  BLOCKED_SOURCE: "production_census_population_lane_2_blocked_by_source_quality",
  BLOCKED_PROVIDER: "production_census_population_lane_2_blocked_by_provider_decision",
  CONFIRMATION_MISSING: "production_census_population_lane_2_confirmation_missing",
});

export const ALLOWED_PROVENANCE_FIELDS = Object.freeze([
  MAP_FIRST_PASS.addressConfidence,
  MAP_FIRST_PASS.addressSourceUrl,
  MAP_FIRST_PASS.coordinateSourceType,
  MAP_FIRST_PASS.coordinateConfidence,
  MAP_FIRST_PASS.geocodeProvider,
  MAP_FIRST_PASS.geocodeMethod,
  MAP_FIRST_PASS.geocodeReviewedDate,
  MAP_FIRST_PASS.lastReviewed,
]);

export const ALLOWED_ENRICHMENT_FIELDS = Object.freeze([
  MAP_FIRST_PASS.descriptionSource,
  MAP_FIRST_PASS.descriptionAi,
  MAP_FIRST_PASS.amenitiesSource,
  MAP_FIRST_PASS.amenitiesTags,
  MAP_FIRST_PASS.propertyType,
  MAP_FIRST_PASS.assetContext,
  MAP_FIRST_PASS.marketSubmarket,
  MAP_FIRST_PASS.flagFb,
  MAP_FIRST_PASS.flagMeeting,
  MAP_FIRST_PASS.flagResort,
  MAP_FIRST_PASS.flagExtendedStay,
  MAP_FIRST_PASS.flagMixedUse,
  MAP_FIRST_PASS.flagResidences,
  MAP_FIRST_PASS.enrichmentStatus,
  MAP_FIRST_PASS.enrichmentPriority,
  MAP_FIRST_PASS.lastReviewed,
]);

export const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  ...FIRST_PASS_FORBIDDEN,
  "Latitude",
  "Longitude",
]);

export const APPLY_CONFIRM_FLAGS = Object.freeze([
  "--confirm-census-population-lane-2",
  "--confirm-official-public-sources-only",
  "--confirm-no-brand-explorer-writes",
  "--confirm-no-owner-operator-writes",
  "--confirm-no-room-date-writes",
  "--confirm-no-recent-momentum",
  "--confirm-held-records-blocked",
  "--confirm-no-fake-completeness",
]);

const BATCH_SIZE = 10;
const FIRST_PASS_DRY =
  "reports/research-engine-v2/production-census-first-pass-enrichment-dry-run.json";
const GEOCODE_DRY =
  "reports/research-engine-v2/production-census-address-geocode-resolver-dry-run.json";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function isBlank(v) {
  return v == null || v === "" || (typeof v === "string" && !v.trim());
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function readJson(rel) {
  const p = join(ROOT, rel);
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}
function isValidCoordPair(lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat === 0 && lng === 0) return false;
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return false;
  return true;
}
function claimValue(claims, field) {
  return (claims || []).find((x) => x.field === field && x.value != null && x.value !== "") || null;
}

export function parseLane2Args(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--") && !a.includes("=")));
  const confirms = APPLY_CONFIRM_FLAGS.filter((f) => flags.has(f));
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    skipBeGates: flags.has("--skip-be-gates"),
    confirms,
    allConfirms: confirms.length === APPLY_CONFIRM_FLAGS.length,
  };
}

export function checkLane2EnvFlags() {
  const flags = {
    ALLOW_PRODUCTION_CENSUS_POPULATION_LANE_2:
      process.env.ALLOW_PRODUCTION_CENSUS_POPULATION_LANE_2 === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES: process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES === "1",
    CONFIRM_NO_OWNER_OPERATOR_WRITES: process.env.CONFIRM_NO_OWNER_OPERATOR_WRITES === "1",
    CONFIRM_NO_ROOM_DATE_WRITES: process.env.CONFIRM_NO_ROOM_DATE_WRITES === "1",
  };
  return { allOk: Object.values(flags).every(Boolean), flags };
}

export function providerDecisionStatus() {
  const info = resolveGeocodingProvider(process.env.GEOCODING_PROVIDER);
  const mapboxOk =
    info.provider === "mapbox" &&
    info.credentials_ok &&
    info.permanent_storage_enabled === true;
  const googleOk =
    info.provider === "google" &&
    info.credentials_ok &&
    info.storage_terms_reviewed === true;
  return {
    provider_info: info,
    approved_for_coordinate_apply: mapboxOk || googleOk,
    block_reason: mapboxOk || googleOk
      ? null
      : "provider_or_storage_terms_not_confirmed",
    recommended:
      "Prefer Mapbox Permanent (MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1). Google only if GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED=1.",
  };
}

/**
 * Map first-pass coordinate source string → v1.1.3 provenance enums.
 */
export function mapCoordinateProvenance(coordMeta) {
  if (!coordMeta?.source_url && !coordMeta?.source) return null;
  const source = String(coordMeta.source || "");
  const url = coordMeta.source_url || null;
  const confidence =
    coordMeta.confidence === "High" || coordMeta.confidence === "Medium"
      ? coordMeta.confidence
      : null;
  if (!confidence || !url) return null;

  let coordinateSourceType = "existing_source";
  let geocodeProvider = "Existing Source";
  let geocodeMethod = "structured_data_extraction";

  if (/hilton directory localization\.coordinate/i.test(source)) {
    coordinateSourceType = "structured_data_extraction";
    geocodeProvider = "Existing Source";
    geocodeMethod = "structured_data_extraction";
  } else if (/choice regional geolocation/i.test(source)) {
    coordinateSourceType = "structured_data_extraction";
    geocodeProvider = "Existing Source";
    geocodeMethod = "structured_data_extraction";
  } else if (/ihg hoteldetail embedded/i.test(source)) {
    coordinateSourceType = "embedded_map_extraction";
    geocodeProvider = "Official Page";
    geocodeMethod = "embedded_map_extraction";
  } else if (/official|property page|hoteldetail/i.test(source)) {
    coordinateSourceType = "official_coordinates";
    geocodeProvider = "Official Page";
    geocodeMethod = "official_coordinates";
  } else {
    // Unknown source text — do not invent
    return null;
  }

  return {
    [MAP_FIRST_PASS.coordinateSourceType]: coordinateSourceType,
    [MAP_FIRST_PASS.coordinateConfidence]: confidence,
    [MAP_FIRST_PASS.geocodeProvider]: geocodeProvider,
    [MAP_FIRST_PASS.geocodeMethod]: geocodeMethod,
    [MAP_FIRST_PASS.geocodeReviewedDate]: todayIsoDate(),
    [MAP_FIRST_PASS.lastReviewed]: todayIsoDate(),
    _meta: { source, url, mapped_from: "first_pass_proposal_index" },
  };
}

function structuredTagsFromAmenities(text) {
  const raw = String(text || "");
  const tags = [];
  const rules = [
    [/restaurant|dining|f&b|bar/i, "F&B"],
    [/meeting|conference|ballroom/i, "Meeting Space"],
    [/pool/i, "Pool"],
    [/spa/i, "Spa"],
    [/fitness|gym/i, "Fitness"],
    [/parking/i, "Parking"],
    [/wifi|wi-fi|wireless/i, "Wi-Fi"],
    [/airport/i, "Airport Access"],
    [/beach/i, "Beach"],
    [/kitchen|kitchenette/i, "Kitchenette"],
  ];
  for (const [re, tag] of rules) {
    if (re.test(raw)) tags.push(tag);
  }
  return [...new Set(tags)];
}

function inferFlags(amenText, brandName, propertyName) {
  const hay = `${amenText || ""} ${brandName || ""} ${propertyName || ""}`;
  const flags = {};
  if (/restaurant|dining|f&b|bar \/ lounge|food/i.test(hay)) flags[MAP_FIRST_PASS.flagFb] = true;
  if (/meeting|conference|ballroom|event space/i.test(hay)) flags[MAP_FIRST_PASS.flagMeeting] = true;
  if (/resort|leisure|outdoor pool|beach|spa/i.test(hay)) flags[MAP_FIRST_PASS.flagResort] = true;
  if (/extended stay|kitchenette|staybridge|homewood|candlewood/i.test(hay)) {
    flags[MAP_FIRST_PASS.flagExtendedStay] = true;
  }
  if (/mixed.?use/i.test(hay)) flags[MAP_FIRST_PASS.flagMixedUse] = true;
  if (/branded residence|\bresidences\b/i.test(hay)) flags[MAP_FIRST_PASS.flagResidences] = true;
  return flags;
}

function inferPropertyType(amenText, brandName, propertyName, flags) {
  const hay = `${amenText || ""} ${brandName || ""} ${propertyName || ""}`;
  if (flags[MAP_FIRST_PASS.flagExtendedStay] || /extended stay|staybridge|homewood|candlewood/i.test(hay)) {
    return "Extended Stay";
  }
  if (/all.?inclusive/i.test(hay)) return "All-Inclusive";
  if (flags[MAP_FIRST_PASS.flagResort] || /resort/i.test(hay)) return "Resort";
  if (/boutique|kimpton|curio|tapestry|autograph|design hotels/i.test(hay)) return "Boutique Hotel";
  if (brandName) return "Hotel";
  return null;
}

function inferAssetContext(fields, amenText, market) {
  const hay = `${fields[MAP_FIRST_PASS.propertyName] || ""} ${fields[MAP_FIRST_PASS.city] || ""} ${amenText || ""} ${market || ""}`;
  if (/airport|aeropuerto/i.test(hay)) return "Airport";
  if (/beach|waterfront|zona hotelera|riviera|cabo|vallarta|tulum|cozumel/i.test(hay)) {
    return "Beach / Waterfront";
  }
  if (/resort destination|all.?inclusive|iberostar/i.test(hay)) return "Resort Destination";
  if (/mexico city|monterrey|guadalajara|polanco|santa fe|centro|urban/i.test(hay)) return "Urban";
  return null;
}

function summarizeDescription(text) {
  const t = String(text || "").replace(/\s+/g, " ").trim();
  if (t.length < 40) return null;
  if (t.length <= 280) return t;
  return `${t.slice(0, 277).trim()}…`;
}

async function listAllRecords(baseId, token, tableId, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${tableId} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function batchPatch(baseId, token, tableId, updates) {
  const errors = [];
  let updated = 0;
  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const chunk = updates.slice(i, i + BATCH_SIZE).map((u) => ({
      id: u.id,
      fields: u.fields,
    }));
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) {
      errors.push({ status: res.status, error: json.error || json, chunk_start: i });
    } else {
      updated += (json.records || []).length;
    }
    await sleep(220);
  }
  return { updated, errors };
}

function sanitizePatch(patch) {
  const out = {};
  const allowed = new Set([...ALLOWED_PROVENANCE_FIELDS, ...ALLOWED_ENRICHMENT_FIELDS]);
  for (const [k, v] of Object.entries(patch || {})) {
    if (k.startsWith("_")) continue;
    if (FORBIDDEN_WRITE_FIELDS.includes(k)) continue;
    if (!allowed.has(k)) continue;
    if (v === undefined) continue;
    out[k] = v;
  }
  return out;
}

/**
 * Build provenance + enrichment proposal for one record.
 */
export function proposeLane2Record(record, ctx) {
  const fields = record.fields || {};
  const key = fields[MAP_FIRST_PASS.identityKey];
  const held = fields[MAP_FIRST_PASS.humanReview] === true;
  const brandMap = mapCensusBrand(fields, ctx.universe);
  const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "");
  const brandUnconfirmed = affiliation === "Brand-Unconfirmed";
  const hasCoords = isValidCoordPair(
    Number(fields[MAP_FIRST_PASS.latitude]),
    Number(fields[MAP_FIRST_PASS.longitude])
  );

  const base = {
    record_id: record.id,
    identity_key: key,
    property_name: fields[MAP_FIRST_PASS.propertyName],
    brand: fields[MAP_FIRST_PASS.currentBrand],
    brand_mapping: brandMap,
    has_coordinates: hasCoords,
  };

  if (held) {
    return { ...base, eligible: false, block_reason: "human_review_required", patch: {}, lanes: [] };
  }
  if (brandUnconfirmed) {
    return { ...base, eligible: false, block_reason: "brand_unconfirmed", patch: {}, lanes: [] };
  }
  if (!brandMap.active) {
    return { ...base, eligible: false, block_reason: "not_in_active_universe", patch: {}, lanes: [] };
  }

  /** @type {Record<string, unknown>} */
  const patch = {};
  const lanes = [];
  const sources = [];

  // --- Provenance backfill (coords unchanged) ---
  const fp = key ? ctx.firstPassByKey.get(key) : null;
  const alreadyHasProvenance = !isBlank(fields[MAP_FIRST_PASS.coordinateSourceType]);
  if (hasCoords && !alreadyHasProvenance) {
    const mapped = mapCoordinateProvenance(fp?.coordinate || null);
    if (mapped) {
      const meta = mapped._meta;
      delete mapped._meta;
      Object.assign(patch, mapped);
      lanes.push("provenance_backfill");
      sources.push({
        lane: "provenance_backfill",
        source_url: meta?.url || fp?.coordinate?.source_url,
        source: fp?.coordinate?.source,
        fields: Object.keys(mapped),
      });
    } else {
      base.provenance_review = "unclear_source_leave_blank";
    }
  } else if (hasCoords && alreadyHasProvenance) {
    base.provenance_review = "already_populated";
  } else if (!hasCoords) {
    base.provenance_review = "no_coordinates";
  }

  // --- Safe enrichment gaps from VIC ---
  const vic = key ? ctx.vic.byId.get(key) : null;
  const amenClaim = vic ? claimValue(vic.field_claims, "Amenities") : null;
  if (
    amenClaim &&
    isBlank(fields[MAP_FIRST_PASS.amenitiesSource]) &&
    amenClaim.evidence_url &&
    amenClaim.confidence &&
    amenClaim.confidence !== "Low" &&
    amenClaim.confidence !== "Insufficient"
  ) {
    const tags = structuredTagsFromAmenities(amenClaim.value);
    patch[MAP_FIRST_PASS.amenitiesSource] = String(amenClaim.value);
    patch[MAP_FIRST_PASS.amenitiesTags] = tags.join("\n");
    lanes.push("amenities");
    sources.push({
      lane: "amenities",
      source_url: amenClaim.evidence_url,
      confidence: amenClaim.confidence,
    });

    const flags = inferFlags(
      amenClaim.value,
      fields[MAP_FIRST_PASS.currentBrand],
      fields[MAP_FIRST_PASS.propertyName]
    );
    for (const [fk, fv] of Object.entries(flags)) {
      if (isBlank(fields[fk]) && fv === true) patch[fk] = true;
    }
    if (Object.keys(flags).length) lanes.push("strategic_flags");
  }

  const descClaim =
    (vic && claimValue(vic.field_claims, "Hotel Description - Source Text")) ||
    (vic && claimValue(vic.field_claims, "description")) ||
    (vic && claimValue(vic.field_claims, "Description"));
  if (
    descClaim &&
    isBlank(fields[MAP_FIRST_PASS.descriptionSource]) &&
    descClaim.evidence_url &&
    descClaim.confidence !== "Low"
  ) {
    const text = String(descClaim.value);
    const summary = summarizeDescription(text);
    if (summary) {
      patch[MAP_FIRST_PASS.descriptionSource] = text;
      patch[MAP_FIRST_PASS.descriptionAi] = summary;
      lanes.push("description");
      sources.push({
        lane: "description",
        source_url: descClaim.evidence_url,
        confidence: descClaim.confidence,
      });
    }
  }

  const amenText =
    patch[MAP_FIRST_PASS.amenitiesSource] || fields[MAP_FIRST_PASS.amenitiesSource] || "";
  if (isBlank(fields[MAP_FIRST_PASS.propertyType])) {
    const flagsForType = {
      [MAP_FIRST_PASS.flagExtendedStay]:
        patch[MAP_FIRST_PASS.flagExtendedStay] || fields[MAP_FIRST_PASS.flagExtendedStay],
      [MAP_FIRST_PASS.flagResort]:
        patch[MAP_FIRST_PASS.flagResort] || fields[MAP_FIRST_PASS.flagResort],
    };
    const pType = inferPropertyType(
      amenText,
      fields[MAP_FIRST_PASS.currentBrand],
      fields[MAP_FIRST_PASS.propertyName],
      flagsForType
    );
    // Only write when grounded in amenities or strong brand pattern (not bare Hotel default without amenity support)
    if (pType && (amenText || pType !== "Hotel")) {
      if (pType === "Hotel" && !amenText) {
        /* skip weak default */
      } else {
        patch[MAP_FIRST_PASS.propertyType] = pType;
        lanes.push("property_type");
      }
    }
  }

  if (isBlank(fields[MAP_FIRST_PASS.marketSubmarket])) {
    const mkt = resolveDealalityMarketSubmarket(fields);
    if (mkt?.ok && mkt.value) {
      patch[MAP_FIRST_PASS.marketSubmarket] = mkt.value;
      lanes.push("market_submarket");
      sources.push({ lane: "market_submarket", source: "dealality_corridor_inference" });
    }
  }

  if (isBlank(fields[MAP_FIRST_PASS.assetContext])) {
    const asset = inferAssetContext(
      fields,
      amenText,
      patch[MAP_FIRST_PASS.marketSubmarket] || fields[MAP_FIRST_PASS.marketSubmarket]
    );
    if (asset) {
      patch[MAP_FIRST_PASS.assetContext] = asset;
      lanes.push("asset_context");
    }
  }

  const clean = sanitizePatch(patch);
  if (Object.keys(clean).length) {
    clean[MAP_FIRST_PASS.enrichmentStatus] = "Partial";
    clean[MAP_FIRST_PASS.enrichmentPriority] = "Medium";
    clean[MAP_FIRST_PASS.lastReviewed] = todayIsoDate();
  }

  return {
    ...base,
    eligible: true,
    block_reason: null,
    patch: clean,
    lanes: [...new Set(lanes)],
    sources,
  };
}

function countField(proposals, field) {
  return proposals.filter((p) => p.patch && p.patch[field] != null).length;
}

export async function runLane2DryRun() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token) throw new Error("AIRTABLE_PAT missing");
  if (!bases?.target_base_id) throw new Error("AIRTABLE_BASE_ID_ALT missing");

  const universe = loadActiveBrandUniverse();
  const vic = loadVicClaimIndex();
  const firstPass = readJson(FIRST_PASS_DRY);
  const geocodeDry = readJson(GEOCODE_DRY);
  const provider = providerDecisionStatus();

  const firstPassByKey = new Map();
  for (const p of firstPass?.proposal_index || []) {
    if (p.identity_key) firstPassByKey.set(p.identity_key, p);
  }

  const censusRows = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    ...Object.values(MAP_FIRST_PASS),
    "Owner Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Renovation / Conversion Date",
    "Affiliation Start Date",
  ]);

  const proposals = [];
  for (const row of censusRows) {
    proposals.push(
      proposeLane2Record(row, { universe, vic, firstPassByKey })
    );
  }

  const eligible = proposals.filter((p) => p.eligible);
  const blocked = proposals.filter((p) => !p.eligible);
  const withPatch = proposals.filter((p) => Object.keys(p.patch || {}).length > 0);
  const provenance = proposals.filter((p) => (p.lanes || []).includes("provenance_backfill"));
  const provenanceBlankUnclear = proposals.filter(
    (p) => p.has_coordinates && p.provenance_review === "unclear_source_leave_blank"
  );
  const provenanceAlready = proposals.filter(
    (p) => p.provenance_review === "already_populated"
  );

  const geocodeProposals = geocodeDry?.proposed_updates || [];
  const geocodeStatus = {
    count: geocodeProposals.length,
    ready_but_blocked: !provider.approved_for_coordinate_apply,
    provider_decision: provider,
    note: provider.approved_for_coordinate_apply
      ? "Provider approved — still requires separate founder apply approval for geocode lane"
      : "34 High/Medium proposals remain blocked until Mapbox Permanent or Google storage terms confirmed",
    sample: geocodeProposals.slice(0, 5),
    exact_airtable_update_count_if_geocode_applied: provider.approved_for_coordinate_apply
      ? geocodeProposals.length
      : 0,
  };

  const forbiddenTouches = FORBIDDEN_WRITE_FIELDS.filter((f) =>
    proposals.some((p) => p.patch && Object.prototype.hasOwnProperty.call(p.patch, f))
  );

  let status = STATUS.DRY_RUN_READY;
  if (forbiddenTouches.length) status = STATUS.BLOCKED_SOURCE;
  // Geocode blocked does not block provenance/enrichment dry-run readiness
  if (!withPatch.length && !provider.approved_for_coordinate_apply && provenance.length === 0) {
    // still ready for review of empty enrichment if provenance proposed
  }

  const sampleBeforeAfter = withPatch.slice(0, 8).map((p) => {
    const row = censusRows.find((r) => r.id === p.record_id);
    const before = {};
    const after = {};
    for (const f of Object.keys(p.patch)) {
      before[f] = row?.fields?.[f] ?? null;
      after[f] = p.patch[f];
    }
    return {
      record_id: mask(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      lanes: p.lanes,
      before,
      after,
    };
  });

  return {
    version: LANE2_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    apply_executed: false,
    status,
    base_id_masked: mask(bases.target_base_id),
    summary: {
      total_records_scanned: censusRows.length,
      records_eligible: eligible.length,
      records_blocked: blocked.length,
      records_with_patch: withPatch.length,
      provenance_backfills_proposed: provenance.length,
      provenance_left_blank_unclear: provenanceBlankUnclear.length,
      provenance_already_populated: provenanceAlready.length,
      description_updates_proposed: countField(proposals, MAP_FIRST_PASS.descriptionSource),
      amenity_updates_proposed: countField(proposals, MAP_FIRST_PASS.amenitiesSource),
      property_type_updates_proposed: countField(proposals, MAP_FIRST_PASS.propertyType),
      asset_context_updates_proposed: countField(proposals, MAP_FIRST_PASS.assetContext),
      market_submarket_updates_proposed: countField(proposals, MAP_FIRST_PASS.marketSubmarket),
      strategic_flag_updates_proposed: proposals.filter((p) =>
        (p.lanes || []).includes("strategic_flags")
      ).length,
      geocode_proposals_status: geocodeStatus,
      exact_airtable_update_count_if_applied: withPatch.length,
      field_update_counts: Object.fromEntries(
        [...ALLOWED_PROVENANCE_FIELDS, ...ALLOWED_ENRICHMENT_FIELDS].map((f) => [
          f,
          countField(proposals, f),
        ])
      ),
    },
    blocked_reason_counts: blocked.reduce((acc, p) => {
      acc[p.block_reason] = (acc[p.block_reason] || 0) + 1;
      return acc;
    }, {}),
    sample_before_after: sampleBeforeAfter,
    provenance_sample: provenance.slice(0, 10).map((p) => ({
      record_id: mask(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      patch: p.patch,
      sources: p.sources,
    })),
    provenance_review_sample: provenanceBlankUnclear.slice(0, 15).map((p) => ({
      record_id: mask(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      reason: p.provenance_review,
    })),
    geocode_lane: geocodeStatus,
    forbidden_fields_untouched: {
      fields: FORBIDDEN_WRITE_FIELDS,
      proposed_writes: forbiddenTouches,
      ok: forbiddenTouches.length === 0,
    },
    proposal_index: proposals.map((p) => ({
      record_id: p.record_id,
      identity_key: p.identity_key,
      eligible: p.eligible,
      block_reason: p.block_reason,
      lanes: p.lanes,
      patch_fields: Object.keys(p.patch || {}),
      provenance_review: p.provenance_review || null,
    })),
    /** Full proposal rows for Autopilot multi-queue orchestration (includes patch + sources). */
    proposals,
    next_step: withPatch.length
      ? "Founder review dry-run → apply lane-2 with confirm flags (geocode remain blocked until provider decision)."
      : "No enrichment/provenance patches; resolve provider decision for 34 geocode proposals or expand description sources.",
  };
}

export async function runLane2Apply(dryReport) {
  const args = parseLane2Args();
  const env = checkLane2EnvFlags();
  if (!args.apply) return dryReport;
  if (!env.allOk || !args.allConfirms) {
    return {
      version: LANE2_VERSION,
      generated_at: new Date().toISOString(),
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
      env_flags: env.flags,
      confirms_present: args.confirms,
      confirms_required: APPLY_CONFIRM_FLAGS,
    };
  }

  const dry = dryReport || (await runLane2DryRun());
  if (!dry.forbidden_fields_untouched?.ok) {
    return {
      ...dry,
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.BLOCKED_SOURCE,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  const updates = [];
  for (const p of dry.proposal_index || []) {
    if (!p.eligible || !p.patch_fields?.length) continue;
    // Rebuild patch from a fresh propose to avoid trusting stale index alone
  }

  // Re-propose live for apply safety
  const universe = loadActiveBrandUniverse();
  const vic = loadVicClaimIndex();
  const firstPass = readJson(FIRST_PASS_DRY);
  const firstPassByKey = new Map();
  for (const p of firstPass?.proposal_index || []) {
    if (p.identity_key) firstPassByKey.set(p.identity_key, p);
  }
  const censusRows = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    ...Object.values(MAP_FIRST_PASS),
  ]);

  for (const row of censusRows) {
    const p = proposeLane2Record(row, { universe, vic, firstPassByKey });
    if (!p.eligible || !Object.keys(p.patch || {}).length) continue;
    // Double-check no lat/lng
    if (p.patch.Latitude != null || p.patch.Longitude != null) continue;
    updates.push({ id: row.id, fields: p.patch, identity_key: p.identity_key, lanes: p.lanes });
  }

  const write = await batchPatch(bases.target_base_id, token, CENSUS_TABLE_ID, updates);

  // Post validation
  const post = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    MAP_FIRST_PASS.identityKey,
    MAP_FIRST_PASS.latitude,
    MAP_FIRST_PASS.longitude,
    MAP_FIRST_PASS.humanReview,
    MAP_FIRST_PASS.radarDisplayStatus,
    MAP_FIRST_PASS.coordinateSourceType,
    MAP_FIRST_PASS.geocodeProvider,
    MAP_FIRST_PASS.descriptionSource,
    MAP_FIRST_PASS.amenitiesSource,
    "Owner Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Renovation / Conversion Date",
    "Affiliation Start Date",
  ]);

  const keyCounts = new Map();
  for (const r of post) {
    const k = r.fields?.[MAP_FIRST_PASS.identityKey];
    keyCounts.set(k, (keyCounts.get(k) || 0) + 1);
  }

  const heldPublic = post.filter(
    (r) =>
      r.fields?.[MAP_FIRST_PASS.humanReview] === true &&
      (r.fields?.[MAP_FIRST_PASS.radarDisplayStatus] === "Public Map Eligible" ||
        r.fields?.[MAP_FIRST_PASS.radarDisplayStatus] === "Public List Eligible")
  ).length;

  const validation = {
    record_count: post.length,
    duplicate_identity_keys: [...keyCounts.values()].filter((n) => n > 1).length,
    coords_filled: post.filter((r) =>
      isValidCoordPair(Number(r.fields?.Latitude), Number(r.fields?.Longitude))
    ).length,
    zero_zero: post.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0).length,
    held_public_eligible: heldPublic,
    provenance_populated: post.filter(
      (r) => !isBlank(r.fields?.[MAP_FIRST_PASS.coordinateSourceType])
    ).length,
    description_filled: post.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.descriptionSource]))
      .length,
    amenities_filled: post.filter((r) => !isBlank(r.fields?.[MAP_FIRST_PASS.amenitiesSource]))
      .length,
    owner_filled: post.filter((r) => !isBlank(r.fields?.["Owner Name"])).length,
    operator_filled: post.filter((r) => !isBlank(r.fields?.["Operator / Management Company"]))
      .length,
    rooms_filled: post.filter((r) => r.fields?.["Rooms / Keys"] != null).length,
    opening_filled: post.filter((r) => !isBlank(r.fields?.["Opening Date"])).length,
    renovation_filled: post.filter((r) => !isBlank(r.fields?.["Renovation / Conversion Date"]))
      .length,
    affiliation_start_filled: post.filter(
      (r) => !isBlank(r.fields?.["Affiliation Start Date"])
    ).length,
  };
  validation.pass =
    validation.record_count === EXPECTED_RECORD_COUNT &&
    validation.duplicate_identity_keys === 0 &&
    validation.zero_zero === 0 &&
    validation.held_public_eligible === 0 &&
    validation.owner_filled === 0 &&
    validation.operator_filled === 0 &&
    validation.rooms_filled === 0 &&
    validation.opening_filled === 0 &&
    validation.renovation_filled === 0 &&
    validation.affiliation_start_filled === 0 &&
    write.errors.length === 0;

  const provider = providerDecisionStatus();

  return {
    version: LANE2_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status: validation.pass ? STATUS.APPLIED : STATUS.BLOCKED_SOURCE,
    base_id_masked: mask(bases.target_base_id),
    updates_attempted: updates.length,
    updates_written: write.updated,
    airtable_errors: write.errors,
    summary_from_dry_run: dry.summary,
    geocode_lane: {
      ...dry.geocode_lane,
      applied: false,
      blocked: !provider.approved_for_coordinate_apply,
    },
    post_apply_validation: validation,
    forbidden_fields_untouched: dry.forbidden_fields_untouched,
    next_step: provider.approved_for_coordinate_apply
      ? "Founder may approve separate geocode apply for the 34 proposals with provenance writes."
      : "Next: provider/storage decision for 34 geocode proposals; continue description source extraction lane.",
  };
}

export function renderLane2DryRunMarkdown(r) {
  const s = r.summary || {};
  return `# Production Census Population Lane 2 — Dry Run

**Status:** \`${r.status}\`  
**Generated:** ${r.generated_at}  
**Apply executed:** false

## Executive summary

| Metric | Value |
| --- | ---: |
| Scanned | ${s.total_records_scanned} |
| Eligible | ${s.records_eligible} |
| Blocked | ${s.records_blocked} |
| Provenance backfills | ${s.provenance_backfills_proposed} |
| Provenance unclear (blank) | ${s.provenance_left_blank_unclear} |
| Descriptions | ${s.description_updates_proposed} |
| Amenities | ${s.amenity_updates_proposed} |
| Property type | ${s.property_type_updates_proposed} |
| Asset context | ${s.asset_context_updates_proposed} |
| Market/Submarket | ${s.market_submarket_updates_proposed} |
| Strategic flags | ${s.strategic_flag_updates_proposed} |
| Exact updates if applied | ${s.exact_airtable_update_count_if_applied} |
| Geocode proposals | ${s.geocode_proposals_status?.count} (blocked: ${s.geocode_proposals_status?.ready_but_blocked}) |

## Geocode lane (34)

\`\`\`json
${JSON.stringify(r.geocode_lane, null, 2)}
\`\`\`

## Sample before/after

\`\`\`json
${JSON.stringify(r.sample_before_after, null, 2)}
\`\`\`

## Provenance sample

\`\`\`json
${JSON.stringify(r.provenance_sample, null, 2)}
\`\`\`

## Forbidden fields

\`\`\`json
${JSON.stringify(r.forbidden_fields_untouched, null, 2)}
\`\`\`

## Next

${r.next_step}
`;
}

export function renderLane2ApplyMarkdown(r) {
  return `# Production Census Population Lane 2 — Apply

**Status:** \`${r.status}\`  
**Generated:** ${r.generated_at}  
**Apply executed:** ${r.apply_executed}

## Summary

- Updates attempted: **${r.updates_attempted}**
- Updates written: **${r.updates_written}**
- Validation pass: **${r.post_apply_validation?.pass}**
- Geocode applied: **${r.geocode_lane?.applied === true}**

## Post-apply validation

\`\`\`json
${JSON.stringify(r.post_apply_validation, null, 2)}
\`\`\`

## Geocode lane

\`\`\`json
${JSON.stringify(r.geocode_lane, null, 2)}
\`\`\`

## Brand Explorer safety

\`\`\`json
${JSON.stringify(r.brand_explorer_safety || { pending: true }, null, 2)}
\`\`\`

## Learning ledger

\`\`\`json
${JSON.stringify(r.learning_ledger_update || { pending: true }, null, 2)}
\`\`\`

## Next

${r.next_step || ""}
`;
}
