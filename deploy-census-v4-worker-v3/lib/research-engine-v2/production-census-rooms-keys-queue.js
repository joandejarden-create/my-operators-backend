/**
 * Census queue: rooms_keys_missing
 * Early queue — High confidence official hotel room counts only for production writes.
 */

import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  MAP_FIRST_PASS,
  loadActiveBrandUniverse,
  mapCensusBrand,
  loadVicClaimIndex,
  FORBIDDEN_WRITE_FIELDS as BASE_FORBIDDEN,
} from "./production-census-first-pass-enrichment.js";
import {
  extractRoomsKeysFromOfficialHtml,
  selectBestRoomsHit,
  assessRoomsClaim,
  mapToExistingRoomsConfidence,
  ROOMS_EXTRACTOR_VERSION,
} from "./production-census-rooms-keys-extractor.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
void __dirname;
void join;

function isPropertyLevelUrl(url) {
  if (!url) return false;
  const s = String(url).toLowerCase();
  if (
    /sitemap|locations\/mexico\/[^/]*\/?$|\/mexico\/?$|choicehotels\.com\/(?:en-uk\/)?mexico(?:\/regional|\/?\?|$)|ihg\.com\/mexico$/i.test(
      s
    )
  ) {
    return false;
  }
  return (
    /hilton\.com\/en\/hotels\//i.test(s) ||
    /hoteldetail/i.test(s) ||
    /marriott\.com\/(?:en-us\/)?hotels\//i.test(s) ||
    /choicehotels\.com\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9]+/i.test(s) ||
    /ihg\.com\/[^/]+\/hotels\//i.test(s)
  );
}

function pickOfficialFetchUrl(fields) {
  const official = fields[MAP_FIRST_PASS.officialUrl];
  const source = fields[MAP_FIRST_PASS.sourceUrl];
  if (isPropertyLevelUrl(official)) return { url: official, kind: "official_property_url" };
  if (isPropertyLevelUrl(source)) return { url: source, kind: "source_url" };
  if (official) return { url: official, kind: "official_fallback_may_be_generic" };
  if (source) return { url: source, kind: "source_fallback_may_be_generic" };
  return { url: null, kind: "missing" };
}

export const ROOMS_QUEUE_VERSION = "production-census-rooms-keys-queue-v1";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
export const EXPECTED_RECORD_COUNT = 666;
export const EXPECTED_FIELD_COUNT = 108;

export const STATUS = Object.freeze({
  READY: "production_census_rooms_keys_queue_ready",
  READY_NEEDS_V114: "production_census_rooms_keys_queue_ready_needs_v114_schema",
  BLOCKED: "production_census_rooms_keys_queue_blocked",
});

/** Central field map — existing live fields + planned v1.1.4. */
export const MAP_ROOMS = Object.freeze({
  roomsKeys: "Rooms / Keys",
  // Existing live provenance (shorter names)
  confidenceExisting: "Rooms Confidence",
  sourceUrlExisting: "Rooms Source URL",
  // v1.1.4 provenance (schema apply) — founder naming aligns with Rooms Confidence / Rooms Source URL
  sourceTypePlanned: "Rooms Source Type",
  reviewedDatePlanned: "Rooms Reviewed Date",
  notesPlanned: "Rooms Notes",
  // Optional naming-parity aliases (deferred; do not rename existing fields unless founder requests)
  confidencePlanned: "Rooms / Keys Confidence",
  sourceUrlPlanned: "Rooms / Keys Source URL",
  enrichmentStatus: "Enrichment Status",
  enrichmentPriority: "Enrichment Priority",
  lastReviewed: "Last Reviewed Date",
});

export const EXISTING_ROOMS_FIELDS = Object.freeze([
  MAP_ROOMS.roomsKeys,
  MAP_ROOMS.confidenceExisting,
  MAP_ROOMS.sourceUrlExisting,
]);

export const PLANNED_V114_FIELDS = Object.freeze([
  {
    name: MAP_ROOMS.sourceTypePlanned,
    type: "singleSelect",
    options: [
      "official_property_page",
      "official_brand_directory",
      "official_hotel_website",
      "official_press_release",
      "official_development_page",
      "trusted_secondary_source",
      "steward_review",
    ],
  },
  {
    name: MAP_ROOMS.reviewedDatePlanned,
    type: "date",
  },
  {
    name: MAP_ROOMS.notesPlanned,
    type: "multilineText",
  },
  {
    name: MAP_ROOMS.confidenceExisting,
    type: "singleSelect_option_add",
    options_add: ["Hold"],
    note: "Hold added on live Rooms Confidence via v1.1.4 apply (typecast seed; Meta choices PATCH unsupported)",
  },
]);

export const ALLOWED_APPLY_FIELDS_EXISTING = Object.freeze([
  MAP_ROOMS.roomsKeys,
  MAP_ROOMS.confidenceExisting,
  MAP_ROOMS.sourceUrlExisting,
  MAP_ROOMS.enrichmentStatus,
  MAP_ROOMS.enrichmentPriority,
  MAP_ROOMS.lastReviewed,
]);

export const ALLOWED_APPLY_FIELDS_V114 = Object.freeze([
  ...ALLOWED_APPLY_FIELDS_EXISTING,
  MAP_ROOMS.sourceTypePlanned,
  MAP_ROOMS.reviewedDatePlanned,
  MAP_ROOMS.notesPlanned,
]);

export const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  ...BASE_FORBIDDEN.filter((f) => f !== "Rooms / Keys"),
  "Latitude",
  "Longitude",
  "Owner Name",
  "Developer Name",
  "Developer",
  "Operator / Management Company",
  "Opening Date",
  "Renovation / Conversion Date",
  "Renovation Date",
  "Affiliation Start Date",
  "Company Validated",
  "Brand Verified",
  "Recent Momentum",
]);

const FETCH_HEADERS = {
  "user-agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "accept-language": "en-US,en;q=0.9",
};

const READ_FIELDS = [
  MAP_FIRST_PASS.propertyName,
  MAP_FIRST_PASS.identityKey,
  MAP_FIRST_PASS.country,
  MAP_FIRST_PASS.city,
  MAP_FIRST_PASS.currentBrand,
  MAP_FIRST_PASS.brandSlug,
  MAP_FIRST_PASS.affiliationStatus,
  MAP_FIRST_PASS.sourceUrl,
  MAP_FIRST_PASS.officialUrl,
  MAP_FIRST_PASS.family,
  MAP_FIRST_PASS.humanReview,
  MAP_FIRST_PASS.dataEligible,
  MAP_ROOMS.roomsKeys,
  MAP_ROOMS.confidenceExisting,
  MAP_ROOMS.sourceUrlExisting,
  MAP_FIRST_PASS.enrichmentStatus,
];

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
function claimValue(claims, field) {
  return (claims || []).find((x) => x.field === field && x.value != null && x.value !== "") || null;
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

async function fetchOfficialPage(url) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 25000);
  try {
    const res = await fetch(url, {
      headers: FETCH_HEADERS,
      redirect: "follow",
      signal: controller.signal,
    });
    const text = await res.text();
    const blocked =
      res.status === 403 ||
      res.status === 429 ||
      /<title[^>]*>\s*access denied/i.test(text) ||
      /cf-challenge|attention required|akamai\s*block/i.test(text) ||
      (res.ok && text.length < 800 && /access denied/i.test(text));
    return {
      ok: res.ok && !blocked,
      status: res.status,
      url: res.url || url,
      text,
      blocked,
      length: text.length,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      blocked: false,
      error: err?.message || String(err),
      length: 0,
    };
  } finally {
    clearTimeout(t);
  }
}

function familyFromRecord(fields, identityKey) {
  const f = String(fields[MAP_FIRST_PASS.family] || "").trim();
  if (["Marriott", "IHG", "Hilton", "Choice"].includes(f)) return f;
  const id = String(identityKey || "");
  if (id.includes("_marriott_")) return "Marriott";
  if (id.includes("_ihg_")) return "IHG";
  if (id.includes("_hilton_")) return "Hilton";
  if (id.includes("_choice_")) return "Choice";
  return f || "Other";
}

function inferSourceType(url, method) {
  const u = String(url || "").toLowerCase();
  if (/hoteldetail|\/hotels\/|choicehotels\.com\/.+\//i.test(u)) return "official_property_page";
  if (/press|newsroom|prnewswire|businesswire/i.test(u)) return "official_press_release";
  if (/marriott\.com|hilton\.com|ihg\.com|choicehotels\.com/i.test(u)) {
    return method?.includes("directory") ? "official_brand_directory" : "official_hotel_website";
  }
  return "trusted_secondary_source";
}

/**
 * Schema snapshot for Rooms / Keys provenance.
 */
export function inspectRoomsKeysSchemaStatus(liveFieldNames = []) {
  const set = new Set(liveFieldNames);
  const existing = {
    rooms_keys: set.has(MAP_ROOMS.roomsKeys),
    rooms_confidence: set.has(MAP_ROOMS.confidenceExisting),
    rooms_source_url: set.has(MAP_ROOMS.sourceUrlExisting),
  };
  const missing_functional = PLANNED_V114_FIELDS.filter((f) => {
    if (f.type === "singleSelect_option_add") return false; // option add tracked separately
    // Accept either founder short names or optional Rooms / Keys* parity names
    if (f.name === MAP_ROOMS.sourceTypePlanned) {
      return !set.has(MAP_ROOMS.sourceTypePlanned) && !set.has("Rooms / Keys Source Type");
    }
    if (f.name === MAP_ROOMS.reviewedDatePlanned) {
      return !set.has(MAP_ROOMS.reviewedDatePlanned) && !set.has("Rooms / Keys Reviewed Date");
    }
    if (f.name === MAP_ROOMS.notesPlanned) {
      return !set.has(MAP_ROOMS.notesPlanned) && !set.has("Rooms / Keys Notes");
    }
    return !set.has(f.name);
  }).map((f) => f.name);

  const needs_v114 = missing_functional.length > 0;

  return {
    field_count_live: liveFieldNames.length || EXPECTED_FIELD_COUNT,
    existing,
    existing_field_names: EXISTING_ROOMS_FIELDS,
    existing_confidence_options_live: [
      "Exact",
      "High",
      "Medium",
      "Low",
      "Insufficient",
      "Unknown",
      "Hold",
    ],
    recommend_add_hold_to_rooms_confidence: needs_v114,
    planned_v114: PLANNED_V114_FIELDS,
    missing_planned_fields: missing_functional,
    naming_parity_deferred: [
      MAP_ROOMS.confidencePlanned,
      MAP_ROOMS.sourceUrlPlanned,
    ],
    needs_v114_schema: needs_v114,
    write_path_today: ALLOWED_APPLY_FIELDS_EXISTING,
    note: needs_v114
      ? "Live base has Rooms / Keys, Rooms Confidence, Rooms Source URL. Still missing Rooms Source Type / Rooms Reviewed Date / Rooms Notes (and/or Hold on Confidence)."
      : "v1.1.4 Rooms / Keys provenance present (Source Type, Reviewed Date, Notes). Leave blank until Autopilot rooms High-only apply.",
  };
}

export function classifyRoomsEligibility(record, ctx) {
  const fields = record.fields || {};
  const key = fields[MAP_FIRST_PASS.identityKey];
  const held = Boolean(fields[MAP_FIRST_PASS.humanReview]);
  const brandMap = mapCensusBrand(fields, ctx.universe);
  const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "");
  const brandUnconfirmed = affiliation === "Brand-Unconfirmed";
  const family = familyFromRecord(fields, key);
  const fetchUrl = pickOfficialFetchUrl(fields);
  const roomsBlank = isBlank(fields[MAP_ROOMS.roomsKeys]);

  const base = {
    record_id: record.id,
    identity_key: key,
    property_name: fields[MAP_FIRST_PASS.propertyName],
    brand: fields[MAP_FIRST_PASS.currentBrand],
    family,
    brand_mapping: brandMap,
    fetch_url: fetchUrl.url,
    fetch_url_kind: fetchUrl.kind,
    rooms_blank: roomsBlank,
  };

  if (!roomsBlank) return { ...base, eligible: false, block_reason: "rooms_already_filled" };
  if (held) return { ...base, eligible: false, block_reason: "human_review_required" };
  if (brandUnconfirmed) return { ...base, eligible: false, block_reason: "brand_unconfirmed" };
  if (!brandMap.active) {
    return { ...base, eligible: false, block_reason: "not_in_active_universe" };
  }
  if (brandMap.classification === "uncertain") {
    return { ...base, eligible: false, block_reason: "uncertain_brand_mapping" };
  }
  if (!fetchUrl.url) return { ...base, eligible: false, block_reason: "missing_source_url" };
  if (!isPropertyLevelUrl(fetchUrl.url)) {
    return { ...base, eligible: false, block_reason: "generic_directory_url_not_property_page" };
  }
  return { ...base, eligible: true, block_reason: null };
}

function buildProposalFromHit(row, hit, sourceUrl, sourceType, pageOk, opts = {}) {
  const confidence = hit.confidence;
  const canWriteHigh = confidence === "High" && !hit.rejected && hit.hotel_only !== false && pageOk;
  const schemaV114Ready = Boolean(opts.schemaV114Ready);

  /** @type {Record<string, unknown>} */
  const patch = {};
  if (canWriteHigh) {
    patch[MAP_ROOMS.roomsKeys] = hit.count;
    patch[MAP_ROOMS.confidenceExisting] = mapToExistingRoomsConfidence("High");
    patch[MAP_ROOMS.sourceUrlExisting] = sourceUrl;
    patch[MAP_ROOMS.enrichmentStatus] = "Partial";
    patch[MAP_ROOMS.enrichmentPriority] = "Medium";
    patch[MAP_ROOMS.lastReviewed] = todayIsoDate();
    if (schemaV114Ready) {
      patch[MAP_ROOMS.sourceTypePlanned] = sourceType || "trusted_secondary_source";
      patch[MAP_ROOMS.reviewedDatePlanned] = todayIsoDate();
      if (hit.note) patch[MAP_ROOMS.notesPlanned] = String(hit.note).slice(0, 2000);
    }
  }

  return {
    ...row,
    action: canWriteHigh
      ? "propose_high_write"
      : confidence === "Medium"
        ? "medium_review"
        : confidence === "Hold" || hit.rejected
          ? "hold"
          : "low_blocked",
    proposed_rooms_keys: hit.count,
    confidence,
    confidence_airtable_existing: mapToExistingRoomsConfidence(confidence),
    source_url: sourceUrl,
    source_type: sourceType,
    method: hit.method,
    mixed_use_risk: Boolean(hit.mixed_use_risk),
    hotel_only: hit.hotel_only !== false,
    notes: hit.note || hit.reject_reason || null,
    patch,
    patch_fields: Object.keys(patch),
    write_allowed_now: canWriteHigh,
    v114_fields_included: schemaV114Ready && canWriteHigh,
    v114_fields_deferred: schemaV114Ready
      ? []
      : [
          MAP_ROOMS.sourceTypePlanned,
          MAP_ROOMS.reviewedDatePlanned,
          MAP_ROOMS.notesPlanned,
        ],
  };
}

/**
 * Dry-run rooms_keys_missing queue.
 */
export async function runRoomsKeysQueueDryRun(opts = {}) {
  const limit = opts.limit ?? 100;
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token) throw new Error("AIRTABLE_PAT missing");
  if (!bases?.target_base_id) throw new Error("AIRTABLE_BASE_ID_ALT missing");

  // Live meta for schema status
  const metaRes = await fetch(
    `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(bases.target_base_id)}/tables`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const metaJson = await metaRes.json();
  const table = (metaJson.tables || []).find(
    (t) => t.id === CENSUS_TABLE_ID || t.name === "Hotel Property Census"
  );
  const liveFieldNames = (table?.fields || []).map((f) => f.name);
  const schema = inspectRoomsKeysSchemaStatus(liveFieldNames);

  const universe = loadActiveBrandUniverse();
  const vic = loadVicClaimIndex();
  const censusRows = await listAllRecords(
    bases.target_base_id,
    token,
    CENSUS_TABLE_ID,
    READ_FIELDS
  );

  const classified = censusRows.map((r) => classifyRoomsEligibility(r, { universe }));
  const eligible = classified.filter((c) => c.eligible);
  const blocked = classified.filter((c) => !c.eligible);

  // Prefer IHG (fetchable). Deprioritize bot-blocked corporate domains.
  const queue = [...eligible].sort((a, b) => {
    const rank = { IHG: 0, Choice: 1, Marriott: 2, Hilton: 3, Other: 4 };
    return (rank[a.family] ?? 9) - (rank[b.family] ?? 9);
  });

  const work = queue.slice(0, limit);
  const deferred = queue.length - work.length;

  const proposals = [];
  const fetchStats = { attempted: 0, ok: 0, blocked: 0, failed: 0, by_family: {} };
  /** @type {Record<string, number>} */
  const consecutiveBlocksByFamily = {};
  const FAMILY_BLOCK_SKIP_AFTER = 3;

  for (const row of work) {
    const fam = row.family || "Other";
    if ((consecutiveBlocksByFamily[fam] || 0) >= FAMILY_BLOCK_SKIP_AFTER) {
      proposals.push({
        ...row,
        action: "deferred",
        blocked_reason: "family_fetch_circuit_open_blocked_sources",
        confidence: "Low",
        write_allowed_now: false,
        patch: {},
      });
      continue;
    }
    if (!fetchStats.by_family[fam]) {
      fetchStats.by_family[fam] = { attempted: 0, ok: 0, blocked: 0, failed: 0 };
    }
    fetchStats.attempted += 1;
    fetchStats.by_family[fam].attempted += 1;

    const page = await fetchOfficialPage(row.fetch_url);
    await sleep(300);

    const vicRec = row.identity_key ? vic.byId.get(row.identity_key) : null;
    const vicClaim =
      (vicRec && claimValue(vicRec.field_claims, "rooms")) ||
      (vicRec && claimValue(vicRec.field_claims, "Rooms / Keys"));

    if (!page.ok) {
      if (page.blocked) {
        fetchStats.blocked += 1;
        fetchStats.by_family[fam].blocked += 1;
        consecutiveBlocksByFamily[fam] = (consecutiveBlocksByFamily[fam] || 0) + 1;
      } else {
        fetchStats.failed += 1;
        fetchStats.by_family[fam].failed += 1;
        consecutiveBlocksByFamily[fam] = 0;
      }

      // Fallback: assess VIC only as Medium review (never High without page)
      if (vicClaim) {
        const assessed = assessRoomsClaim(vicClaim, {});
        if (assessed.ok) {
          proposals.push(
            buildProposalFromHit(
              row,
              {
                count: assessed.count,
                method: assessed.method,
                confidence: "Medium",
                hotel_only: true,
                mixed_use_risk: false,
                note: assessed.reason,
              },
              vicClaim.evidence_url || row.fetch_url,
              inferSourceType(vicClaim.evidence_url || row.fetch_url, assessed.method),
              false,
              { schemaV114Ready: !schema.needs_v114_schema }
            )
          );
          continue;
        }
      }

      proposals.push({
        ...row,
        action: "blocked",
        blocked_reason: page.blocked ? "official_page_blocked" : `fetch_failed_${page.status || "err"}`,
        proposed_rooms_keys: null,
        patch: {},
        write_allowed_now: false,
      });
      continue;
    }

    fetchStats.ok += 1;
    fetchStats.by_family[fam].ok += 1;
    consecutiveBlocksByFamily[fam] = 0;

    const extracted = extractRoomsKeysFromOfficialHtml(page.text, {
      url: page.url,
      propertyName: row.property_name,
    });
    let best = selectBestRoomsHit(extracted.hits);

    // Cross-check VIC (reject known false positives)
    if (vicClaim) {
      const assessed = assessRoomsClaim(vicClaim, { html: page.text });
      if (!assessed.ok && assessed.reason?.includes("false_positive")) {
        // drop any hit that equals the bogus claim count from weak methods
        if (best && best.count === assessed.count && !String(best.method).includes("json_ld")) {
          best = null;
        }
      }
    }

    if (!best) {
      proposals.push({
        ...row,
        action: "no_room_count_found",
        blocked_reason: "no_extractable_room_count",
        extraction_meta: { patterns_matched: extracted.patterns_matched },
        proposed_rooms_keys: null,
        patch: {},
        write_allowed_now: false,
        page_fetched: true,
      });
      continue;
    }

    const sourceType = inferSourceType(page.url, best.method);
    proposals.push({
      ...buildProposalFromHit(row, best, page.url, sourceType, true, {
        schemaV114Ready: !schema.needs_v114_schema,
      }),
      page_fetched: true,
      extraction_meta: {
        patterns_matched: extracted.patterns_matched,
        hits: extracted.hits.length,
      },
    });
  }

  const high = proposals.filter((p) => p.action === "propose_high_write");
  const medium = proposals.filter((p) => p.action === "medium_review");
  const low = proposals.filter((p) => p.action === "low_blocked" || p.action === "no_room_count_found" || p.action === "blocked");
  const hold = proposals.filter((p) => p.action === "hold");

  const blockReasons = {};
  for (const b of blocked) {
    blockReasons[b.block_reason] = (blockReasons[b.block_reason] || 0) + 1;
  }
  for (const p of proposals) {
    const r = p.blocked_reason || p.action;
    if (p.action === "propose_high_write") continue;
    blockReasons[r] = (blockReasons[r] || 0) + 1;
  }

  const status = schema.needs_v114_schema ? STATUS.READY_NEEDS_V114 : STATUS.READY;
  const forAutopilot = Boolean(opts.forAutopilot);
  const idOut = (id) => (forAutopilot ? id : mask(id));

  return {
    version: ROOMS_QUEUE_VERSION,
    extractor_version: ROOMS_EXTRACTOR_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    queue: "rooms_keys_missing",
    status,
    schema,
    summary: {
      total_records_scanned: censusRows.length,
      records_eligible: eligible.length,
      records_blocked_prefilter: blocked.length,
      queue_limit: limit,
      processed: work.length,
      deferred_over_limit: deferred,
      official_sources_fetched: fetchStats.attempted,
      pages_ok: fetchStats.ok,
      pages_blocked: fetchStats.blocked,
      pages_failed: fetchStats.failed,
      room_key_counts_found: proposals.filter((p) => p.proposed_rooms_keys != null).length,
      high_confidence_proposals: high.length,
      medium_confidence_candidates: medium.length,
      low_confidence_blocked: low.length,
      hold_records: hold.length,
      exact_airtable_update_count_if_applied: high.length,
      fetch_by_family: fetchStats.by_family,
      block_reasons: blockReasons,
      learning_note:
        "VIC IHG rooms claims of 22 are known false positives from JS \\x22rooms escapes — rejected by extractor.",
    },
    sample_high: high.slice(0, 10).map((p) => ({
      record_id: idOut(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      family: p.family,
      proposed_rooms_keys: p.proposed_rooms_keys,
      confidence: p.confidence,
      method: p.method,
      source_url: p.source_url,
      source_type: p.source_type,
      notes: p.notes,
      patch_fields: p.patch_fields,
    })),
    sample_medium: medium.slice(0, 8).map((p) => ({
      record_id: idOut(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      proposed_rooms_keys: p.proposed_rooms_keys,
      method: p.method,
      source_url: p.source_url,
      notes: p.notes,
    })),
    sample_hold: hold.slice(0, 8).map((p) => ({
      record_id: idOut(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      proposed_rooms_keys: p.proposed_rooms_keys,
      method: p.method,
      notes: p.notes,
    })),
    proposals: proposals.map((p) => ({
      record_id: idOut(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      family: p.family,
      action: p.action,
      proposed_rooms_keys: p.proposed_rooms_keys ?? null,
      confidence: p.confidence || null,
      method: p.method || null,
      source_url: p.source_url || p.fetch_url || null,
      source_type: p.source_type || null,
      notes: p.notes || p.blocked_reason || null,
      write_allowed_now: Boolean(p.write_allowed_now),
      patch_fields: p.patch_fields || [],
      ...(forAutopilot
        ? {
            patch: p.patch || {},
            queue: "rooms_keys",
            current_fields: {},
            fetch_url: p.fetch_url || null,
            brand: p.brand || null,
          }
        : {}),
    })),
    commands: {
      dry_run: "npm run census:queue-run -- --queue rooms_keys_missing --dry-run --limit 100",
      apply_later:
        "npm run census:queue-run -- --queue rooms_keys_missing --apply --limit 100 --confirm-targeted-queue-apply --confirm-rooms-keys-only --confirm-official-source-room-counts-only --confirm-no-mixed-use-unit-confusion --confirm-no-owner-operator-writes --confirm-no-date-writes --confirm-no-brand-explorer-writes",
    },
    forbidden_fields: FORBIDDEN_WRITE_FIELDS,
    next_step: schema.needs_v114_schema
      ? "Approve schema v1.1.4 rooms provenance fields (Source Type / Reviewed Date / Notes + Hold), then re-run dry-run; apply High-only after founder approval."
      : "Founder review High proposals; apply with confirm flags. Medium stays review-only.",
  };
}

export function renderRoomsKeysQueueMarkdown(report) {
  const s = report.summary || {};
  const schema = report.schema || {};
  return `# Production Census Rooms / Keys Queue

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Queue:** \`rooms_keys_missing\`  
**Extractor:** ${report.extractor_version}

## 1. Executive summary

Rooms / Keys is an **early** Census queue with **High-only** production writes. Mixed-use / units / residences stay Hold. VIC false-positive room counts (IHG \`22\`) are rejected.

| Metric | Value |
| --- | ---: |
| Scanned | ${s.total_records_scanned} |
| Eligible | ${s.records_eligible} |
| Processed (limit) | ${s.processed} |
| Pages ok / blocked | ${s.pages_ok} / ${s.pages_blocked} |
| Counts found | ${s.room_key_counts_found} |
| High proposals | ${s.high_confidence_proposals} |
| Medium review | ${s.medium_confidence_candidates} |
| Low blocked | ${s.low_confidence_blocked} |
| Hold | ${s.hold_records} |
| Updates if applied | ${s.exact_airtable_update_count_if_applied} |

## 2. Current Rooms / Keys field status

\`\`\`json
${JSON.stringify(schema.existing, null, 2)}
\`\`\`

Existing live fields: ${(schema.existing_field_names || []).join(", ")}

## 3. Missing provenance fields (v1.1.4 plan)

Needs v1.1.4: **${schema.needs_v114_schema}**

Missing planned: ${(schema.missing_planned_fields || []).join(", ") || "none"}

\`\`\`json
${JSON.stringify(schema.planned_v114, null, 2)}
\`\`\`

**This task does not create schema fields.**

## 4. Queue definition

- **Name:** rooms_keys_missing
- **Early:** yes
- **Write gate:** High confidence + official page + hotel-only count
- **Medium:** review / founder approval only
- **Low / Hold:** no write

## 5. Source rules

1. Official hotel property page  
2. Official brand/property directory  
3. Official hotel website  
4. Official press release  
5. Official development/company page  
6. Trusted secondary only as Medium (not High)

## 6. Mixed-use guardrails

Do not write when count may include residences, villas, apartments, vacation ownership, total units, pipeline masterplan, or mixed hotel+residences without a hotel-only split.

## 7. Commands

\`\`\`bash
${report.commands?.dry_run}
\`\`\`

Apply later:

\`\`\`bash
${report.commands?.apply_later}
\`\`\`

## 8. Learning system updates

- Reject VIC IHG \`22\` rooms false positive (\`\\\\x22rooms\` JS escape)
- Prefer \`json_ld_numberOfRooms\` and explicit hotel-room phrases
- Split "80 hotel rooms and 40 residences" → write 80 only
- Hold on "units" / including residences / planned pipeline counts

## 9. Sample High proposals

\`\`\`json
${JSON.stringify(report.sample_high || [], null, 2)}
\`\`\`

## 10. Recommended next step

${report.next_step}
`;
}

export const ROOMS_KEYS_QUEUE = Object.freeze({
  id: "rooms_keys_missing",
  name: "Rooms / Keys missing",
  purpose:
    "Find and populate hotel room/key counts when supported by strong official source evidence",
  early: true,
  version: ROOMS_QUEUE_VERSION,
  runDryRun: runRoomsKeysQueueDryRun,
  renderMarkdown: renderRoomsKeysQueueMarkdown,
  map: MAP_ROOMS,
  allowed_apply_fields_existing: ALLOWED_APPLY_FIELDS_EXISTING,
  forbidden: FORBIDDEN_WRITE_FIELDS,
});
