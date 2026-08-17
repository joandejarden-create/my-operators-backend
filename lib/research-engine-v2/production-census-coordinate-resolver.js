/**
 * Production Census coordinate resolver — code-based, no Webhound production path.
 * Dry-run only in this lane (no Airtable apply from this module's CLI default).
 */

import { readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  loadActiveBrandUniverse,
  mapCensusBrand,
  MAP_FIRST_PASS,
  loadVicClaimIndex,
} from "./production-census-first-pass-enrichment.js";
import {
  extractCoordinatesFromOfficialHtml,
  selectBestCoordinateHit,
  geocodeOfficialAddressOnly,
  isValidCoordPair,
  matchesRejectedPin,
  COORDINATE_CRAWLER_RULES,
} from "./production-census-coordinate-extractor.js";
import {
  extractMarshaCode,
  fetchMarriottHqvCoordinates,
  MARRIOTT_HQV_LEARNING,
} from "./marriott-hqv-coordinate-client.js";
import {
  resolveDirectoryCoordinateCandidate,
  applyDeepOfficialPageSignals,
  noteUnresolvedSourcePattern,
} from "./census-autopilot-family-directory-adapters.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const RESOLVER_VERSION = "production-census-coordinate-resolver-v1";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
export const EXPECTED_RECORD_COUNT = 666;

export const STATUS = Object.freeze({
  DRY_RUN_READY: "production_census_coordinate_resolver_dry_run_ready_for_founder_review",
  NEEDS_CODE_IMPROVEMENT: "production_census_coordinate_resolver_needs_code_improvement",
  BLOCKED_STOP_WEBHOUND: "production_census_coordinate_resolver_blocked_stop_webhound_full_run",
});

const FETCH_HEADERS = {
  "user-agent":
    "Mozilla/5.0 (compatible; DealalityCensusCoordinateResolver/1.0; +https://dealality.com)",
  accept: "text/html,application/xhtml+xml,application/json",
};

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
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

export function parseResolverArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--") && !a.includes("=")));
  const getNum = (name, fallback) => {
    const hit = argv.find((a) => a.startsWith(`${name}=`));
    if (!hit) return fallback;
    const n = Number(hit.split("=")[1]);
    return Number.isFinite(n) ? n : fallback;
  };
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    fetchLimit: getNum("--fetch-limit", 40),
    delayMs: getNum("--delay-ms", 400),
    families: (() => {
      const hit = argv.find((a) => a.startsWith("--families="));
      if (!hit) return ["Marriott", "IHG"];
      return hit
        .split("=")[1]
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    })(),
    allowGeocode: flags.has("--allow-official-address-geocode"),
  };
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
    const blocked = res.status === 403 || res.status === 429 || /access denied|robot check|captcha/i.test(text);
    return {
      ok: res.ok && !blocked,
      status: res.status,
      url: res.url || url,
      text,
      blocked,
    };
  } catch (err) {
    return { ok: false, status: 0, url, text: "", blocked: false, error: err?.message || String(err) };
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

/**
 * Validate first-pass written coordinates (read-only).
 */
export function validateFirstPassCoordinates(censusRows, dryRunIndex) {
  const byId = new Map((dryRunIndex || []).map((p) => [p.record_id, p]));
  const withCoords = censusRows.filter((r) =>
    isValidCoordPair(Number(r.fields?.[MAP_FIRST_PASS.latitude]), Number(r.fields?.[MAP_FIRST_PASS.longitude]))
  );

  const safe = [];
  const needsReview = [];
  const downgradeLater = [];

  const pinMap = new Map();
  for (const r of withCoords) {
    const lat = Number(r.fields[MAP_FIRST_PASS.latitude]);
    const lng = Number(r.fields[MAP_FIRST_PASS.longitude]);
    const key = `${lat.toFixed(5)},${lng.toFixed(5)}`;
    if (!pinMap.has(key)) pinMap.set(key, []);
    pinMap.get(key).push(r);
  }

  for (const r of withCoords) {
    const lat = Number(r.fields[MAP_FIRST_PASS.latitude]);
    const lng = Number(r.fields[MAP_FIRST_PASS.longitude]);
    const held = r.fields?.[MAP_FIRST_PASS.humanReview] === true;
    const radar = r.fields?.[MAP_FIRST_PASS.radarDisplayStatus];
    const proposal = byId.get(r.id);
    const propertyName = r.fields?.[MAP_FIRST_PASS.propertyName];
    const rejected = matchesRejectedPin(lat, lng, { propertyName });
    const zeroZero = lat === 0 && lng === 0;
    const invalid = !isValidCoordPair(lat, lng);
    const sourceSupport = Boolean(proposal?.coordinate?.source_url || proposal?.source_urls?.length);
    const shared = (pinMap.get(`${lat.toFixed(5)},${lng.toFixed(5)}`) || []).length > 1;

    const row = {
      record_id: mask(r.id),
      identity_key: r.fields?.[MAP_FIRST_PASS.identityKey],
      property_name: propertyName,
      lat,
      lng,
      radar,
      held,
      source_support: sourceSupport,
      source: proposal?.coordinate?.source || null,
      source_url: proposal?.coordinate?.source_url || null,
      shared_campus_pin: shared,
    };

    if (held && (radar === "Public Map Eligible" || radar === "Public List Eligible")) {
      needsReview.push({ ...row, reason: "held_but_public_eligible" });
      continue;
    }
    if (zeroZero || invalid || rejected) {
      needsReview.push({
        ...row,
        reason: zeroZero ? "zero_zero" : invalid ? "invalid" : `rejected_pin:${rejected.label}`,
      });
      continue;
    }
    if (radar === "Public Map Eligible" && !sourceSupport) {
      needsReview.push({ ...row, reason: "public_map_without_first_pass_source_support" });
      continue;
    }
    if (shared) {
      downgradeLater.push({ ...row, reason: "shared_campus_pin_medium_confidence" });
    }
    safe.push(row);
  }

  const publicMap = censusRows.filter(
    (r) => r.fields?.[MAP_FIRST_PASS.radarDisplayStatus] === "Public Map Eligible"
  );
  const publicMapMissingCoords = publicMap.filter(
    (r) =>
      !isValidCoordPair(
        Number(r.fields?.[MAP_FIRST_PASS.latitude]),
        Number(r.fields?.[MAP_FIRST_PASS.longitude])
      )
  );

  return {
    coordinates_present: withCoords.length,
    safe_count: safe.length,
    needs_review_count: needsReview.length,
    downgrade_later_count: downgradeLater.length,
    public_map_eligible_count: publicMap.length,
    public_map_missing_coords: publicMapMissingCoords.length,
    zero_zero: withCoords.filter(
      (r) => r.fields.Latitude === 0 && r.fields.Longitude === 0
    ).length,
    held_with_coords: withCoords.filter((r) => r.fields?.[MAP_FIRST_PASS.humanReview] === true)
      .length,
    safe_sample: safe.slice(0, 8),
    needs_review: needsReview,
    downgrade_later: downgradeLater,
    code_gaps: [
      "Marriott VIC freeze lat/lng are null — production path is GraphQL HQV (phoenixShopHQVPropertyInfoCall) after MARSHA from sitemap/URL; overview HTML usually has no coords.",
      "Marriott HQV currently needs MARRIOTT_GRAPHQL_OPERATION_SIGNATURE (harvest from search __NEXT_DATA__) and may hit Akamai without browser/XHR.",
      "IHG freeze has almost no lat/lng — hoteldetail HTML/JSON lane needed; current sample fetches returned official_page_blocked.",
      "Shared campus pins remain Medium confidence; optional later steward downgrade of Public Display Confidence only (no coord rewrite in this task).",
      "Official-address geocode path requires GOOGLE_MAPS_API_KEY and street-level address (often blank on Census).",
    ],
    pass:
      withCoords.length > 0 &&
      needsReview.filter((n) => n.reason !== "shared_campus_pin_medium_confidence").length === 0 &&
      publicMapMissingCoords.length === 0 &&
      withCoords.every((r) => !(r.fields.Latitude === 0 && r.fields.Longitude === 0)),
  };
}

/**
 * Resolve one record via official page fetch + extraction (+ optional geocode).
 */
export async function resolveRecordCoordinates(record, ctx) {
  const fields = record.fields || {};
  const key = fields[MAP_FIRST_PASS.identityKey];
  const held = fields[MAP_FIRST_PASS.humanReview] === true;
  const brandMap = mapCensusBrand(fields, ctx.universe);
  const affiliation = String(fields[MAP_FIRST_PASS.affiliationStatus] || "");
  const brandUnconfirmed = affiliation === "Brand-Unconfirmed";
  const family = familyFromRecord(fields, key);
  const sourceUrl =
    fields[MAP_FIRST_PASS.officialUrl] || fields[MAP_FIRST_PASS.sourceUrl] || null;
  const hasCoords = isValidCoordPair(
    Number(fields[MAP_FIRST_PASS.latitude]),
    Number(fields[MAP_FIRST_PASS.longitude])
  );

  const base = {
    record_id: record.id,
    identity_key: key,
    property_name: fields[MAP_FIRST_PASS.propertyName],
    brand: fields[MAP_FIRST_PASS.currentBrand],
    family,
    source_url: sourceUrl,
    brand_mapping: brandMap,
  };

  if (held) {
    return { ...base, action: "blocked", blocked_reason: "human_review_required", proposal: null, page_fetched: false };
  }
  if (brandUnconfirmed) {
    return { ...base, action: "blocked", blocked_reason: "brand_unconfirmed", proposal: null, page_fetched: false };
  }
  if (!brandMap.active) {
    return { ...base, action: "blocked", blocked_reason: "not_in_active_universe", proposal: null, page_fetched: false };
  }
  if (hasCoords) {
    return { ...base, action: "already_has_valid_coordinates", proposal: null, page_fetched: false };
  }
  if (ctx.families?.length && !ctx.families.includes(family)) {
    return { ...base, action: "skipped_family_filter", blocked_reason: `family_not_in_${ctx.families.join("|")}`, proposal: null, page_fetched: false };
  }

  // Prefer VIC claims first (code path, no Webhound)
  const vic = key ? ctx.vic.byId.get(key) : null;
  if (vic) {
    const latC = (vic.field_claims || []).find((c) => c.field === "Latitude" && c.value != null);
    const lngC = (vic.field_claims || []).find((c) => c.field === "Longitude" && c.value != null);
    if (latC && lngC) {
      const lat = Number(latC.value);
      const lng = Number(lngC.value);
      if (isValidCoordPair(lat, lng) && !matchesRejectedPin(lat, lng, { propertyName: base.property_name }) && latC.evidence_url) {
        const conf = String(latC.confidence || "Medium");
        if (conf === "High" || conf === "Medium") {
          return {
            ...base,
            action: "propose",
            proposal: {
              Latitude: lat,
              Longitude: lng,
              extraction_method: `vic_claim:${latC.source}`,
              confidence: conf,
              source_url: latC.evidence_url,
            },
            page_fetched: false,
          };
        }
      }
    }
  }

  if (!ctx.doFetch) {
    return {
      ...base,
      action: "candidate_needs_fetch",
      blocked_reason: "fetch_budget_deferred",
      proposal: null,
      page_fetched: false,
    };
  }

  // Marriott: prefer GraphQL HQV (sidecar learning) before overview HTML
  if (family === "Marriott") {
    const marsha =
      extractMarshaCode(sourceUrl) ||
      extractMarshaCode(key) ||
      null;
    if (marsha) {
      const hqv = await fetchMarriottHqvCoordinates(marsha);
      if (hqv.ok && isValidCoordPair(hqv.lat, hqv.lng) && !matchesRejectedPin(hqv.lat, hqv.lng, { propertyName: base.property_name })) {
        return {
          ...base,
          action: "propose",
          proposal: {
            Latitude: hqv.lat,
            Longitude: hqv.lng,
            extraction_method: hqv.method,
            confidence: hqv.confidence,
            source_url: hqv.source_url || sourceUrl,
            property_id: hqv.property_id,
            hqv_property_name: hqv.property_name,
          },
          page_fetched: true,
        };
      }
      // Fall through to HTML probe (expected negative for Mexico overview)
      base.hqv_attempt = { ok: false, reason: hqv.reason, marsha };
      noteUnresolvedSourcePattern({
        id: `marriott_hqv_${hqv.reason || "failed"}`,
        family: "Marriott",
        pattern: `Marriott HQV failed: ${hqv.reason || "unknown"}`,
        what_code_needs_to_learn:
          "Harvest MARRIOTT_GRAPHQL_OPERATION_SIGNATURE / Akamai-safe HQV path.",
        sample_url: sourceUrl,
      });
    }
  }

  // Hilton / Choice directory geo before blocked property-page fetch (works without property URL)
  if (family === "Hilton" || family === "Choice") {
    try {
      const dirCoords = await resolveDirectoryCoordinateCandidate({
        fields,
        identityKey: key,
        family,
      });
      if (
        dirCoords.ok &&
        isValidCoordPair(dirCoords.lat, dirCoords.lng) &&
        !matchesRejectedPin(dirCoords.lat, dirCoords.lng, { propertyName: base.property_name })
      ) {
        return {
          ...base,
          action: "propose",
          proposal: {
            Latitude: dirCoords.lat,
            Longitude: dirCoords.lng,
            extraction_method: dirCoords.method,
            confidence: dirCoords.confidence || "High",
            source_url: dirCoords.source_url || sourceUrl,
          },
          page_fetched: false,
          directory_adapter: true,
        };
      }
    } catch (err) {
      base.directory_coord_error = err?.message || String(err);
    }
  }

  if (!sourceUrl) {
    return { ...base, action: "steward_review", blocked_reason: "missing_source_url", proposal: null, page_fetched: false };
  }

  const page = await fetchOfficialPage(sourceUrl);
  if (!page.ok) {
    if (page.blocked) {
      noteUnresolvedSourcePattern({
        id: `${String(family || "other").toLowerCase()}_property_page_403`,
        family,
        pattern: `${family} property URL blocked (${page.status || 403})`,
        what_code_needs_to_learn:
          "Use family directory / HQV adapters; Webhound only for repeated unresolved edge patterns.",
        sample_url: sourceUrl,
      });
    }
    return {
      ...base,
      action: "steward_review",
      blocked_reason: page.blocked ? "official_page_blocked" : `fetch_failed_${page.status || "err"}`,
      fetch_error: page.error || null,
      proposal: null,
      page_fetched: true,
    };
  }

  const deep = applyDeepOfficialPageSignals(page.text, page.url);
  const extracted = extractCoordinatesFromOfficialHtml(page.text, { url: page.url, family });
  if (
    deep.latitude != null &&
    deep.longitude != null &&
    isValidCoordPair(deep.latitude, deep.longitude) &&
    !(extracted.hits || []).length
  ) {
    extracted.hits = extracted.hits || [];
    extracted.hits.push({
      lat: deep.latitude,
      lng: deep.longitude,
      confidence: "High",
      method: "deep_official_page_signals",
      address: null,
    });
  }
  const best = selectBestCoordinateHit(extracted.hits);
  if (best) {
    // City name soft check when possible
    const city = String(fields[MAP_FIRST_PASS.city] || "").toLowerCase();
    const blob = page.text.slice(0, 50000).toLowerCase();
    const cityOk = !city || city.length < 3 || blob.includes(city.slice(0, Math.min(city.length, 12)));
    if (!cityOk && best.confidence === "Medium") {
      return {
        ...base,
        action: "steward_review",
        blocked_reason: "city_match_uncertain",
        extraction_preview: extracted.patterns_matched,
        proposal: null,
        page_fetched: true,
      };
    }
    return {
      ...base,
      action: "propose",
      proposal: {
        Latitude: best.lat,
        Longitude: best.lng,
        extraction_method: best.method,
        confidence: best.confidence,
        source_url: page.url,
        patterns_matched: extracted.patterns_matched,
        address_from_page: best.address || extracted.addresses[0]?.address || null,
      },
      page_fetched: true,
    };
  }

  // Optional geocode only with street-level official address
  if (ctx.allowGeocode && extracted.addresses[0]?.address) {
    const geo = await geocodeOfficialAddressOnly({
      name: fields[MAP_FIRST_PASS.propertyName],
      address: extracted.addresses[0].address,
      city: fields[MAP_FIRST_PASS.city],
      country: fields[MAP_FIRST_PASS.country],
    });
    if (geo.ok) {
      return {
        ...base,
        action: "propose",
        proposal: {
          Latitude: geo.lat,
          Longitude: geo.lng,
          extraction_method: geo.method,
          confidence: geo.confidence,
          source_url: page.url,
          geocode_query: geo.query,
          formatted_address: geo.formatted_address,
        },
        page_fetched: true,
      };
    }
    return {
      ...base,
      action: "steward_review",
      blocked_reason: `geocode_failed_${geo.reason}`,
      address_found: extracted.addresses[0].address,
      proposal: null,
      page_fetched: true,
    };
  }

  if (extracted.addresses[0]?.address) {
    return {
      ...base,
      action: "steward_review",
      blocked_reason: "address_found_no_coords_geocode_not_enabled",
      address_found: extracted.addresses[0].address,
      proposal: null,
      page_fetched: true,
    };
  }

  return {
    ...base,
    action: "steward_review",
    blocked_reason:
      family === "Marriott" && base.hqv_attempt
        ? `marriott_hqv_${base.hqv_attempt.reason}_html_no_coords`
        : "no_coords_extracted_from_official_page",
    patterns_matched: extracted.patterns_matched,
    hqv_attempt: base.hqv_attempt || null,
    proposal: null,
    page_fetched: true,
  };
}

export async function runCoordinateResolverDryRun(args = parseResolverArgs()) {
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token) throw new Error("AIRTABLE_PAT missing");
  if (!bases?.target_base_id) throw new Error("AIRTABLE_BASE_ID_ALT missing");

  const universe = loadActiveBrandUniverse();
  const vic = loadVicClaimIndex();

  const censusRows = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    MAP_FIRST_PASS.propertyName,
    MAP_FIRST_PASS.identityKey,
    MAP_FIRST_PASS.latitude,
    MAP_FIRST_PASS.longitude,
    MAP_FIRST_PASS.city,
    MAP_FIRST_PASS.country,
    MAP_FIRST_PASS.address,
    MAP_FIRST_PASS.currentBrand,
    MAP_FIRST_PASS.brandSlug,
    MAP_FIRST_PASS.affiliationStatus,
    MAP_FIRST_PASS.family,
    MAP_FIRST_PASS.sourceUrl,
    MAP_FIRST_PASS.officialUrl,
    MAP_FIRST_PASS.humanReview,
    MAP_FIRST_PASS.radarDisplayStatus,
    MAP_FIRST_PASS.publicCensusEligibility,
  ]);

  let dryIndex = [];
  const dryPath = join(ROOT, "reports/research-engine-v2/production-census-first-pass-enrichment-dry-run.json");
  if (existsSync(dryPath)) {
    dryIndex = readJson(dryPath).proposal_index || [];
  }

  const firstPassValidation = validateFirstPassCoordinates(censusRows, dryIndex);

  const missing = [];
  const already = [];
  const blockedPre = [];
  for (const row of censusRows) {
    const fields = row.fields || {};
    const brandMap = mapCensusBrand(fields, universe);
    const hasCoords = isValidCoordPair(
      Number(fields[MAP_FIRST_PASS.latitude]),
      Number(fields[MAP_FIRST_PASS.longitude])
    );
    if (hasCoords) {
      already.push(row);
      continue;
    }
    if (fields[MAP_FIRST_PASS.humanReview] === true) {
      blockedPre.push({ id: row.id, reason: "human_review_required" });
      continue;
    }
    if (fields[MAP_FIRST_PASS.affiliationStatus] === "Brand-Unconfirmed") {
      blockedPre.push({ id: row.id, reason: "brand_unconfirmed" });
      continue;
    }
    if (!brandMap.active) {
      blockedPre.push({ id: row.id, reason: "not_in_active_universe" });
      continue;
    }
    missing.push(row);
  }

  // Prioritize Marriott + IHG for fetch sample
  const prioritized = missing
    .map((r) => ({
      r,
      family: familyFromRecord(r.fields || {}, r.fields?.[MAP_FIRST_PASS.identityKey]),
    }))
    .sort((a, b) => {
      const rank = (f) => (args.families.includes(f) ? 0 : 1);
      return rank(a.family) - rank(b.family);
    });

  const results = [];
  let fetchUsed = 0;
  for (const { r, family } of prioritized) {
    // Prefer fetch budget for preferred families first
    const prefer = args.families.includes(family);
    const doFetch = fetchUsed < args.fetchLimit && (prefer || fetchUsed < Math.ceil(args.fetchLimit * 0.25));
    const resolved = await resolveRecordCoordinates(r, {
      universe,
      vic,
      families: null,
      doFetch,
      allowGeocode: args.allowGeocode,
    });
    results.push(resolved);
    if (resolved.page_fetched) {
      fetchUsed += 1;
      if (args.delayMs) await sleep(args.delayMs);
    }
  }

  const proposed = results.filter((r) => r.action === "propose" && r.proposal);
  const steward = results.filter((r) => r.action === "steward_review");
  const blocked = results.filter((r) => r.action === "blocked");
  const deferred = results.filter((r) => r.action === "candidate_needs_fetch");
  const byFamilyProposed = {};
  for (const p of proposed) {
    byFamilyProposed[p.family] = (byFamilyProposed[p.family] || 0) + 1;
  }

  const pagesFetched = results.filter((r) => r.page_fetched).length;
  const marriottStewardBlocked = steward.filter(
    (s) =>
      s.family === "Marriott" &&
      /official_page_blocked|marriott_hqv_|fetch_failed/i.test(String(s.blocked_reason || ""))
  ).length;

  // Ready when first-pass coords validate cleanly; flag code improvement when next-lane
  // cannot yet propose (Marriott HQV/Akamai or HTML blocked).
  let status = STATUS.DRY_RUN_READY;
  const hardFirstPassFail = (firstPassValidation.needs_review || []).some((n) =>
    ["zero_zero", "invalid", "held_but_public_eligible"].includes(String(n.reason || "").split(":")[0])
  );
  if (firstPassValidation.public_map_missing_coords > 0 || hardFirstPassFail) {
    status = STATUS.NEEDS_CODE_IMPROVEMENT;
  } else if (proposed.length === 0 && marriottStewardBlocked > 0) {
    status = STATUS.NEEDS_CODE_IMPROVEMENT;
  }

  return {
    version: RESOLVER_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    apply_executed: false,
    status,
    base_id_masked: mask(bases.target_base_id),
    args: {
      fetch_limit: args.fetchLimit,
      delay_ms: args.delayMs,
      allow_geocode: args.allowGeocode,
      preferred_families: args.families,
    },
    crawler_rules: COORDINATE_CRAWLER_RULES,
    summary: {
      total_records_scanned: censusRows.length,
      records_with_valid_coordinates: already.length,
      records_missing_coordinates_active: missing.length,
      blocked_prefilter: blockedPre.length,
      proposed_coordinate_updates: proposed.length,
      steward_review_records: steward.length,
      blocked_records: blocked.length,
      fetch_deferred_candidates: deferred.length,
      exact_airtable_update_count_if_applied: proposed.length,
      proposed_by_family: byFamilyProposed,
      pages_fetched: pagesFetched,
    },
    first_pass_coordinate_validation: firstPassValidation,
    proposed_updates: proposed.map((p) => ({
      record_id: mask(p.record_id),
      identity_key: p.identity_key,
      property_name: p.property_name,
      brand: p.brand,
      family: p.family,
      source_url: p.proposal.source_url,
      extraction_method: p.proposal.extraction_method,
      confidence: p.proposal.confidence,
      latitude: p.proposal.Latitude,
      longitude: p.proposal.Longitude,
    })),
    steward_review_sample: steward.slice(0, 40).map((s) => ({
      record_id: mask(s.record_id),
      identity_key: s.identity_key,
      property_name: s.property_name,
      family: s.family,
      reason: s.blocked_reason,
      source_url: s.source_url,
    })),
    blocked_sample: blocked.slice(0, 20).map((b) => ({
      record_id: mask(b.record_id),
      reason: b.blocked_reason,
    })),
    fields_not_touched: [
      "Owner Name",
      "Developer Name",
      "Operator / Management Company",
      "Rooms / Keys",
      "Opening Date",
      "Renovation / Conversion Date",
      "Affiliation Start Date",
      "Company Validated",
      "Brand Verified",
      "Recent Momentum",
      "Brand Explorer fields",
      "(no Airtable writes in this dry-run)",
    ],
    webhound_production_writes: 0,
    next_step:
      proposed.length > 0
        ? "Founder review of proposed Marriott/IHG page extractions → separate apply lane with confirm flags (no Webhound)."
        : status === STATUS.NEEDS_CODE_IMPROVEMENT
          ? "Marriott/IHG official pages blocked (Akamai). Next code improvement: harvest GraphQL operation signature from a rendered Marriott search page (__NEXT_DATA__.props.pageProps.operationSignatures for phoenixShopHQVPropertyInfoCall), set MARRIOTT_GRAPHQL_OPERATION_SIGNATURE, retry HQV dry-run. Do not restart Webhound for full-census coordinates."
          : "Raise --fetch-limit for Marriott/IHG official-page extraction; enable --allow-official-address-geocode only when street addresses exist",
  };
}

export function renderResolverDryRunMarkdown(report) {
  const s = report.summary || {};
  const v = report.first_pass_coordinate_validation || {};
  return `# Production Census Coordinate Resolver — Dry Run

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Apply executed:** false

## 1. Executive summary

- Scanned: **${s.total_records_scanned}**
- Already valid coordinates: **${s.records_with_valid_coordinates}**
- Active missing: **${s.records_missing_coordinates_active}**
- Proposed updates (dry-run only): **${s.proposed_coordinate_updates}**
- Steward review: **${s.steward_review_records}**
- Blocked (held / brand-unconfirmed / not active): see blocked_sample
- Fetch-deferred candidates: **${s.fetch_deferred_candidates}**
- Pages fetched: **${s.pages_fetched}**
- Exact Airtable update count if applied: **${s.exact_airtable_update_count_if_applied}**
- Webhound production writes: **${report.webhound_production_writes ?? 0}**

## 2–5. Webhound sidecar + learnings

See \`reports/research-engine-v2/webhound-coordinate-learning-sidecar-closed.md\`. Production writes from Webhound = **0**.

## 6. Coordinate resolver method

Order: Census Source/Official URL → fetch official page → JSON-LD / family payloads / map embeds / Marriott HQV GraphQL → optional official-address geocode → validate → High/Medium propose, Low → steward.

\`\`\`json
${JSON.stringify(report.crawler_rules, null, 2)}
\`\`\`

## 7. First-pass coordinate validation

(No first-pass coordinates modified in this task.)

\`\`\`json
${JSON.stringify(
  {
    coordinates_present: v.coordinates_present,
    safe_count: v.safe_count,
    needs_review_count: v.needs_review_count,
    downgrade_later_count: v.downgrade_later_count,
    public_map_eligible_count: v.public_map_eligible_count,
    public_map_missing_coords: v.public_map_missing_coords,
    zero_zero: v.zero_zero,
    held_with_coords: v.held_with_coords,
    pass: v.pass,
    code_gaps: v.code_gaps,
    needs_review: v.needs_review,
    downgrade_later: v.downgrade_later,
  },
  null,
  2
)}
\`\`\`

## 8–11. Next-lane dry-run

### Proposed (sample)

\`\`\`json
${JSON.stringify(report.proposed_updates?.slice(0, 25), null, 2)}
\`\`\`

### Steward review (sample)

\`\`\`json
${JSON.stringify(report.steward_review_sample, null, 2)}
\`\`\`

### Blocked (sample)

\`\`\`json
${JSON.stringify(report.blocked_sample, null, 2)}
\`\`\`

## 12. Fields not touched

${(report.fields_not_touched || []).map((f) => `- ${f}`).join("\n")}

## 13. Brand Explorer safety

No Brand Explorer files, fixtures, or Airtable Brand Explorer fields were written. Protected Active 62 / PVQL / semantic / momentum gates were not re-run because this lane is Census-only and made zero BE changes.

## 14. Recommended next step

${report.next_step}
`;
}
