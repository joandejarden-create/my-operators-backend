/**
 * Approval-bundle-bound Address + Asset Context preflight and apply.
 * Never re-plans. Never writes geocode / descriptions / rooms / names / BE / Brand Setup.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import { AUTOPILOT_FORBIDDEN_FIELDS } from "./census-autopilot-field-allowlist.js";
import { buildIdempotentPatch, compareFieldValues } from "./census-autopilot-idempotent-writer.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { loadVicClaimIndex } from "./production-census-first-pass-enrichment.js";
import { confirmOfficialAddress } from "./production-census-address-geocode-resolver.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const ADDRESS_ASSET_APPLY_VERSION =
  "census-autopilot-address-asset-approval-bundle-apply-v1";

export const STATUS = Object.freeze({
  CLEAN: "production_census_autopilot_address_asset_apply_clean",
  PARTIAL: "production_census_autopilot_address_asset_apply_partial_source_review",
  BLOCKED: "production_census_autopilot_address_asset_apply_blocked",
});

export const ADDRESS_LANE_FIELDS = Object.freeze([
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Last Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
]);

export const ASSET_LANE_FIELDS = Object.freeze([
  "Asset Context",
  "Last Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
]);

export const FORBIDDEN_IN_PATCH = Object.freeze([
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
  "Amenities",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "Rooms / Keys",
  "Property Name",
  "Owner Name",
  "Developer Name",
  "Operator / Management Company",
  "Opening Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
  "Brand Status",
]);

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
const EXPECTED_RECORD_COUNT = 666;
const EXPECTED_ROOMS_FILLED = 5;
const ASSET_CONTEXT_ALLOWED = new Set([
  "Urban",
  "Airport",
  "Beach / Waterfront",
  "Resort Destination",
  "Suburban",
  "Small Metro/Town",
  "Highway / Roadside",
  "Business District",
]);

const DEFAULT_BUNDLE =
  "reports/research-engine-v2/autopilot/2026-08-05_21-49-37-CALA-active-brands/approval-bundle.json";

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Current Brand",
  "City",
  "State / Region",
  "Country",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Asset Context",
  "Affiliation Status",
  "Human Review Required",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Rooms / Keys",
  "Hotel Description - Source Text",
  "Amenities - Source Text",
  "Property Type",
  "Owner Name",
  "Operator / Management Company",
  "Developer Name",
  "Opening Date",
  "Renovation / Conversion Date",
  "Mixed-Use Flag",
  "Branded Residences Flag",
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || v === "" || (typeof v === "string" && !v.trim());
}
function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function isSafeHttpUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

export function parseAddressAssetApplyArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  const get = (name) => {
    const i = argv.indexOf(name);
    return i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[i + 1] : null;
  };
  const confirms = {
    safeWrites: flags.has("--confirm-safe-writes"),
    writeToProduction: flags.has("--confirm-write-to-production-census"),
    noBrandExplorer: flags.has("--confirm-no-brand-explorer-writes"),
    noOwnerOperator:
      flags.has("--confirm-no-owner-operator") || flags.has("--confirm-no-owner-operator-writes"),
    noDateWrites: flags.has("--confirm-no-date-writes"),
    noRecentMomentum: flags.has("--confirm-no-recent-momentum"),
    noCompanyValidation: flags.has("--confirm-no-company-validation"),
    webhoundNotProduction: flags.has("--confirm-webhound-not-production-source"),
    approvalBundleBound:
      flags.has("--confirm-approval-bundle-bound") || flags.has("--approval-bundle"),
  };
  return {
    dryRun: flags.has("--dry-run") && !flags.has("--enable-production-writes"),
    apply:
      (flags.has("--enable-production-writes") || flags.has("--apply")) &&
      String(get("--mode") || (flags.has("--apply") ? "apply" : "")).toLowerCase() !== "controlled",
    mode: get("--mode") || (flags.has("--apply") ? "apply" : "preflight"),
    runDir: get("--run-dir"),
    approvalBundlePath: get("--approval-bundle"),
    batchSize: Number(get("--batch-size") || 100) || 100,
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
    enableProductionWrites: flags.has("--enable-production-writes"),
  };
}

export function checkAddressAssetApplyEnv(env = process.env) {
  const flags = {
    ALLOW_CENSUS_AUTOPILOT_APPLY: String(env.ALLOW_CENSUS_AUTOPILOT_APPLY || "").trim() === "1",
    CONFIRM_WRITE_TO_PRODUCTION_CENSUS:
      String(env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS || "").trim() === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES:
      String(env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "").trim() === "1",
    CONFIRM_NO_OWNER_OPERATOR_WRITES:
      String(env.CONFIRM_NO_OWNER_OPERATOR_WRITES || "").trim() === "1",
  };
  return { allOk: Object.values(flags).every(Boolean), flags };
}

/**
 * Classify Address Source URL into allowed source types.
 */
export function classifyAddressSourceUrl(url) {
  if (!isSafeHttpUrl(url)) return { source_type: null, reason: "invalid_url" };
  let u;
  try {
    u = new URL(url);
  } catch {
    return { source_type: null, reason: "invalid_url" };
  }
  const host = u.hostname.replace(/^www\./, "").toLowerCase();
  const path = u.pathname.toLowerCase();
  const officialHosts = [
    "hilton.com",
    "choicehotels.com",
    "marriott.com",
    "ihg.com",
    "holidayinn.com",
    "radissonhotels.com",
  ];
  const official = officialHosts.some((h) => host === h || host.endsWith(`.${h}`));
  if (!official) {
    return { source_type: "trusted_secondary_source", reason: "non_official_host", host };
  }

  // Country-wide regional directory with shared placeId — not property-specific URL
  if (
    host.includes("choicehotels.com") &&
    path.includes("/regional-hotels") &&
    u.searchParams.has("placeId")
  ) {
    return {
      source_type: "official_brand_directory",
      reason: "generic_regional_directory_shared_placeid",
      host,
      generic_directory: true,
      property_level_url: false,
    };
  }

  if (/\/hotels?\//.test(path) || /\/[a-z]{2,}\d{2,}/i.test(path)) {
    return {
      source_type: "official_property_page",
      reason: "property_path",
      host,
      generic_directory: false,
      property_level_url: true,
    };
  }

  if (path.includes("/locations/") || path.includes("/regional-hotels")) {
    return {
      source_type: "official_brand_directory",
      reason: "brand_locations_directory",
      host,
      generic_directory: true,
      property_level_url: false,
    };
  }

  return {
    source_type: "official_hotel_website",
    reason: "official_host",
    host,
    generic_directory: false,
    property_level_url: false,
  };
}

export function loadAddressAssetFrozenProposals(opts = {}) {
  const bundlePath = resolve(opts.approvalBundlePath || join(ROOT, DEFAULT_BUNDLE));
  const runDir = opts.runDir ? resolve(opts.runDir) : dirname(bundlePath);
  const dryPath = join(runDir, "dry-run.json");

  if (!existsSync(bundlePath)) {
    return { ok: false, error: `approval_bundle_missing:${bundlePath}` };
  }

  const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
  const byQueue = bundle.proposed_writes_by_queue || {};
  const dry = existsSync(dryPath) ? JSON.parse(readFileSync(dryPath, "utf8")) : null;
  const dryById = new Map(
    (dry?.proposals || []).map((p) => [`${p.record_id}:${p.queue}`, p])
  );

  const addressRaw = byQueue.address_confirmation || [];
  const assetRaw = byQueue.property_type_asset_context || [];

  if (!addressRaw.length && !assetRaw.length) {
    // Fallback: flatten dry-run High proposals for these queues only
    const fromDry = (dry?.proposals || []).filter(
      (p) =>
        p.confidence === "High" &&
        (p.queue === "address_confirmation" || p.queue === "property_type_asset_context")
    );
    if (!fromDry.length) {
      return { ok: false, error: "no_address_or_asset_proposals_in_bundle" };
    }
  }

  const frozen = [];
  const fieldViolations = [];

  for (const p of addressRaw) {
    const dryP = dryById.get(`${p.record_id}:address_confirmation`);
    const patch = { ...(p.patch || {}) };
    for (const k of Object.keys(patch)) {
      if (FORBIDDEN_IN_PATCH.includes(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "forbidden" });
      }
      if (!ADDRESS_LANE_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "outside_address_lane" });
      }
    }
    // Lane metadata — only add Last Reviewed Date (not inventing Enrichment values absent from bundle)
    if (!("Last Reviewed Date" in patch)) patch["Last Reviewed Date"] = todayIsoDate();

    frozen.push({
      record_id: p.record_id,
      identity_key: p.identity_key,
      property_name: p.property_name || dryP?.property_name,
      brand: dryP?.brand || null,
      family: dryP?.family || null,
      queue: "address_confirmation",
      lane: "address",
      confidence: p.confidence || "High",
      method: dryP?.method || "address_only:vic_claim",
      patch,
      patch_fields: Object.keys(patch),
      source_url: patch["Address Source URL"] || null,
      approval_source: "approval-bundle.json:address_confirmation",
    });
  }

  for (const p of assetRaw) {
    const dryP = dryById.get(`${p.record_id}:property_type_asset_context`);
    const patch = { ...(p.patch || {}) };
    for (const k of Object.keys(patch)) {
      if (FORBIDDEN_IN_PATCH.includes(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "forbidden" });
      }
      if (!ASSET_LANE_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "outside_asset_lane" });
      }
    }
    if (!("Last Reviewed Date" in patch)) patch["Last Reviewed Date"] = todayIsoDate();

    frozen.push({
      record_id: p.record_id,
      identity_key: p.identity_key,
      property_name: p.property_name || dryP?.property_name,
      brand: dryP?.brand || null,
      family: dryP?.family || null,
      queue: "property_type_asset_context",
      lane: "asset_context",
      confidence: p.confidence || "High",
      method: dryP?.method || "lane2:asset_context",
      patch,
      patch_fields: Object.keys(patch),
      source_url: dryP?.source_url || null,
      approval_source: "approval-bundle.json:property_type_asset_context",
    });
  }

  if (fieldViolations.some((v) => v.reason === "forbidden")) {
    return { ok: false, error: "forbidden_fields_in_bundle", fieldViolations, runDir, bundlePath };
  }

  return {
    ok: true,
    runDir,
    bundlePath,
    dryPath: existsSync(dryPath) ? dryPath : null,
    bundle,
    dry,
    frozen,
    address_count: frozen.filter((f) => f.lane === "address").length,
    asset_count: frozen.filter((f) => f.lane === "asset_context").length,
    field_violations_non_fatal: fieldViolations.filter((v) => v.reason !== "forbidden"),
  };
}

async function airtableGet(baseId, token, tableId, recordId) {
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(recordId)}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`get ${recordId} ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json;
}

async function airtablePatch(baseId, token, tableId, recordId, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(recordId)}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: false }),
    }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`patch ${recordId} ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json;
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
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function fetchSourcePage(url, cache) {
  if (cache.has(url)) return cache.get(url);
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; DealalityCensusPreflight/1.0)",
        Accept: "text/html,application/json",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(25000),
    });
    const text = await res.text();
    const entry = { ok: res.ok, status: res.status, text, error: null };
    cache.set(url, entry);
    await sleep(200);
    return entry;
  } catch (err) {
    const entry = { ok: false, status: 0, text: "", error: err?.message || String(err) };
    cache.set(url, entry);
    return entry;
  }
}

function pageContainsPropertyEvidence(pageText, propertyName, address) {
  if (!pageText) return { ok: false, reason: "empty_page" };
  const textNorm = norm(pageText);
  const nameNorm = norm(propertyName);
  const addrNorm = norm(address);
  const nameOk =
    nameNorm.length >= 8 &&
    (textNorm.includes(nameNorm) ||
      nameNorm
        .split(" ")
        .filter((t) => t.length >= 5)
        .slice(0, 4)
        .every((t) => textNorm.includes(t)));
  // Street evidence: require a distinctive address token (number or 8+ char street fragment)
  const addrTokens = addrNorm.split(" ").filter((t) => t.length >= 3);
  const numberToken = addrTokens.find((t) => /\d/.test(t));
  const longToken = addrTokens.find((t) => t.length >= 6 && !/^(avenida|avenue|calle|blvd|boulevard|carr|carretera|colonia|col)$/.test(t));
  const addrOk =
    (numberToken && textNorm.includes(numberToken)) ||
    (longToken && textNorm.includes(longToken)) ||
    (addrNorm.length >= 10 && textNorm.includes(addrNorm.slice(0, Math.min(24, addrNorm.length))));

  if (nameOk && addrOk) return { ok: true, reason: "name_and_address_on_page" };
  if (!nameOk && !addrOk) return { ok: false, reason: "name_and_address_missing_on_page" };
  if (!nameOk) return { ok: false, reason: "property_name_missing_on_page" };
  return { ok: false, reason: "address_missing_on_page" };
}

/**
 * Preflight one frozen Address or Asset Context proposal.
 */
export function preflightAddressAssetProposal(frozen, liveRecord, ctx = {}) {
  const errors = [];
  const steward_reasons = [];
  const fields = liveRecord?.fields || {};
  const vic = ctx.vic?.byId?.get(frozen.identity_key) || null;

  if (!liveRecord?.id) errors.push("record_not_found");
  if (frozen.confidence !== "High") errors.push("confidence_not_high");

  if (fields["Human Review Required"] === true || fields["Human Review Required"] === "true") {
    errors.push("held_human_review_required");
  }
  if (String(fields["Affiliation Status"] || "") === "Brand-Unconfirmed") {
    errors.push("brand_unconfirmed");
  }

  for (const k of Object.keys(frozen.patch)) {
    if (FORBIDDEN_IN_PATCH.includes(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
      errors.push(`forbidden_field:${k}`);
    }
  }
  // No geocode/coords in patch
  for (const g of [
    "Latitude",
    "Longitude",
    "Coordinate Source Type",
    "Coordinate Confidence",
    "Geocode Provider",
    "Geocode Method",
  ]) {
    if (g in (frozen.patch || {})) errors.push(`geocode_field_in_patch:${g}`);
  }

  let source_type = null;
  let source_domain = null;
  let source_class = null;

  if (frozen.lane === "address") {
    const address = frozen.patch.Address;
    const sourceUrl = frozen.patch["Address Source URL"];
    if (!isStreetLevelAddress(address)) errors.push("address_not_street_level");
    if (frozen.patch["Address Confidence"] !== "High") errors.push("address_confidence_not_high");
    if (!isSafeHttpUrl(sourceUrl)) errors.push("missing_or_invalid_source_url");

    source_class = classifyAddressSourceUrl(sourceUrl);
    source_type = source_class.source_type;
    source_domain = source_class.host || null;

    if (source_class.reason === "generic_regional_directory_shared_placeid") {
      steward_reasons.push("generic_brand_regional_directory_lacks_property_level_url");
    }

    // Identity match vs live Census + VIC
    const liveName = fields["Property Name"] || frozen.property_name;
    const liveCity = fields.City;
    const liveCountry = fields.Country || "Mexico";
    const confirmed = confirmOfficialAddress({
      address,
      propertyName: liveName,
      city: liveCity,
      state: fields["State / Region"],
      country: liveCountry,
    });
    if (!confirmed.ok) errors.push(`address_confirm:${confirmed.reason}`);

    if (vic) {
      const claim = (vic.field_claims || []).find((c) =>
        ["Address", "Address 1", "Street Address"].includes(c.field)
      );
      if (!claim || claim.confidence !== "High") {
        errors.push("vic_address_claim_not_high");
      } else if (norm(claim.value) !== norm(address)) {
        errors.push("vic_address_claim_mismatch");
      }
      if (norm(vic.name) && norm(liveName) && norm(vic.name) !== norm(liveName)) {
        // Soft: names can differ slightly; require token overlap
        const liveTokens = new Set(norm(liveName).split(" ").filter((t) => t.length >= 4));
        const vicTokens = norm(vic.name).split(" ").filter((t) => t.length >= 4);
        const overlap = vicTokens.filter((t) => liveTokens.has(t)).length;
        if (overlap < 2) errors.push("property_name_mismatch_vic_census");
      }
      if (vic.country && liveCountry && norm(vic.country) !== norm(liveCountry)) {
        errors.push("country_mismatch");
      }
    } else {
      errors.push("vic_record_missing");
    }

    // Brand directory must show property-level name + address on page
    if (
      !ctx.skipPageVerify &&
      source_class.source_type === "official_brand_directory" &&
      !source_class.reason?.includes("generic_regional")
    ) {
      const page = ctx.pageCache?.get(sourceUrl);
      if (!page?.ok) {
        steward_reasons.push("brand_directory_page_fetch_failed");
      } else {
        const evidence = pageContainsPropertyEvidence(page.text, liveName, address);
        if (!evidence.ok) {
          steward_reasons.push(`brand_directory_${evidence.reason}`);
        }
      }
    }

    // Generic regional Choice URL → steward (do not apply), even if street-level claim exists
    if (source_class.reason === "generic_regional_directory_shared_placeid") {
      // keep steward_reasons; do not hard-error so it routes to steward not hard-block
    }
  }

  if (frozen.lane === "asset_context") {
    const value = frozen.patch["Asset Context"];
    if (!ASSET_CONTEXT_ALLOWED.has(String(value))) {
      errors.push("asset_context_not_in_allowed_options");
    }
    const name = String(fields["Property Name"] || frozen.property_name || "");
    const city = String(fields.City || "");
    const brand = String(fields["Current Brand"] || frozen.brand || "");
    // Must not be brand-alone inference: require name/city support token
    const hay = norm(`${name} ${city}`);
    const urbanSupport = /centro|urban|downtown|cdmx|mexico city|queretaro|monterrey|guadalajara/.test(
      hay
    );
    const airportSupport = /airport|aeropuerto/.test(hay);
    const beachSupport = /beach|cabo|vallarta|cancun|riviera|tulum/.test(hay);
    let supported = false;
    if (value === "Urban" && urbanSupport) supported = true;
    if (value === "Airport" && airportSupport) supported = true;
    if (value === "Beach / Waterfront" && beachSupport) supported = true;
    if (!supported) {
      // If value equals brand-only guess with no geo token → block
      if (norm(brand) && !hay.replace(norm(brand), "").trim()) {
        errors.push("asset_context_inferred_from_brand_alone");
      } else {
        steward_reasons.push("asset_context_weak_name_city_support");
      }
    }
    if (fields["Mixed-Use Flag"] === true || fields["Branded Residences Flag"] === true) {
      errors.push("unsupported_mixed_use_or_residences_flag");
    }
    if (/mixed.?use|residence/i.test(String(value))) {
      errors.push("unsupported_mixed_use_residence_claim");
    }
  }

  const allowed =
    frozen.lane === "address" ? ADDRESS_LANE_FIELDS : ASSET_LANE_FIELDS;
  for (const k of Object.keys(frozen.patch)) {
    if (!allowed.includes(k)) errors.push(`unapproved_field:${k}`);
  }

  const idempotent = buildIdempotentPatch(fields, frozen.patch, {
    confidence: "High",
    allowGeocode: false,
    schemaV114Ready: true,
    threshold: "High",
  });
  if (idempotent.action === "conflict") errors.push("idempotent_conflict");
  if (idempotent.conflicts?.length) {
    for (const c of idempotent.conflicts) errors.push(`conflict:${c.field}`);
  }

  // Address overwrite guard
  if (frozen.lane === "address") {
    const addrCmp = compareFieldValues(fields.Address, frozen.patch.Address);
    if (addrCmp === "conflict") errors.push("address_already_filled_different");
  }
  if (frozen.lane === "asset_context") {
    const acCmp = compareFieldValues(fields["Asset Context"], frozen.patch["Asset Context"]);
    if (acCmp === "conflict") errors.push("asset_context_already_filled_different");
  }

  const sourceBlocked = steward_reasons.some((r) =>
    r.startsWith("generic_brand_regional") || r.includes("missing_on_page")
  );
  const hardFail = errors.length > 0;
  const steward = !hardFail && steward_reasons.length > 0;
  const pass = !hardFail && !steward && (idempotent.action === "write" || idempotent.action === "skip");

  return {
    ok: pass,
    apply: pass && idempotent.action === "write",
    skip_matching: pass && idempotent.action === "skip",
    steward,
    blocked: hardFail,
    errors,
    steward_reasons,
    source_type,
    source_domain,
    source_class,
    idempotent,
    live_snapshot: {
      identity_key: fields["Property Identity Key"] ?? null,
      property_name: fields["Property Name"] ?? null,
      brand: fields["Current Brand"] ?? null,
      city: fields.City ?? null,
      country: fields.Country ?? null,
      address: fields.Address ?? null,
      asset_context: fields["Asset Context"] ?? null,
      affiliation: fields["Affiliation Status"] ?? null,
      held: fields["Human Review Required"] ?? null,
      latitude: fields.Latitude ?? null,
      longitude: fields.Longitude ?? null,
      coordinate_source_type: fields["Coordinate Source Type"] ?? null,
      rooms_keys: fields["Rooms / Keys"] ?? null,
      description: fields["Hotel Description - Source Text"] ?? null,
      amenities: fields["Amenities - Source Text"] ?? null,
      property_type: fields["Property Type"] ?? null,
      owner: fields["Owner Name"] ?? null,
      operator: fields["Operator / Management Company"] ?? null,
      developer: fields["Developer Name"] ?? null,
      opening_date: fields["Opening Date"] ?? null,
    },
  };
}

function familyFromKey(identityKey, family) {
  if (family) return family;
  const id = String(identityKey || "");
  if (id.includes("_hilton_")) return "Hilton";
  if (id.includes("_choice_")) return "Choice";
  if (id.includes("_marriott_")) return "Marriott";
  if (id.includes("_ihg_")) return "IHG";
  return "Other";
}

/**
 * Run source preflight across frozen proposals (may fetch brand directory pages).
 */
export async function runAddressAssetPreflight(opts = {}) {
  const loaded = loadAddressAssetFrozenProposals(opts);
  if (!loaded.ok) {
    return {
      version: ADDRESS_ASSET_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      ok: false,
      error: loaded.error,
      fieldViolations: loaded.fieldViolations || null,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    return {
      version: ADDRESS_ASSET_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      ok: false,
      error: "missing_airtable_credentials",
    };
  }

  const vic = loadVicClaimIndex();
  const pageCache = new Map();

  // Prefetch unique Hilton/official brand directory URLs (not Choice generic regional)
  const urls = [
    ...new Set(
      loaded.frozen
        .filter((f) => f.lane === "address")
        .map((f) => f.source_url)
        .filter(Boolean)
    ),
  ];
  for (const url of urls) {
    const cls = classifyAddressSourceUrl(url);
    if (cls.reason === "generic_regional_directory_shared_placeid") continue;
    if (cls.source_type === "official_brand_directory" || cls.source_type === "official_property_page") {
      await fetchSourcePage(url, pageCache);
    }
  }

  const rows = [];
  for (const frozen of loaded.frozen) {
    const live = await airtableGet(
      bases.target_base_id,
      token,
      CENSUS_TABLE_ID,
      frozen.record_id
    );
    await sleep(120);
    const pf = preflightAddressAssetProposal(frozen, live, { vic, pageCache });
    rows.push({
      record_id: frozen.record_id,
      identity_key: frozen.identity_key,
      property_name: frozen.property_name,
      family: familyFromKey(frozen.identity_key, frozen.family),
      lane: frozen.lane,
      queue: frozen.queue,
      confidence: frozen.confidence,
      source_url: frozen.source_url,
      source_type: pf.source_type,
      source_domain: pf.source_domain,
      proposed_address: frozen.patch.Address || null,
      proposed_asset_context: frozen.patch["Asset Context"] || null,
      ok: pf.ok,
      apply: pf.apply,
      skip_matching: pf.skip_matching,
      steward: pf.steward,
      blocked: pf.blocked,
      errors: pf.errors,
      steward_reasons: pf.steward_reasons,
      fields_to_write: pf.apply ? pf.idempotent.fields : {},
      live_snapshot: pf.live_snapshot,
      frozen_patch: frozen.patch,
    });
  }

  const passing = rows.filter((r) => r.apply);
  const skipMatching = rows.filter((r) => r.skip_matching);
  const steward = rows.filter((r) => r.steward);
  const blocked = rows.filter((r) => r.blocked);

  const domains = {};
  const sourceTypes = {};
  const parents = {};
  for (const r of rows.filter((x) => x.lane === "address")) {
    domains[r.source_domain || "(none)"] = (domains[r.source_domain || "(none)"] || 0) + 1;
    sourceTypes[r.source_type || "(none)"] = (sourceTypes[r.source_type || "(none)"] || 0) + 1;
    parents[r.family] = (parents[r.family] || 0) + 1;
  }

  return {
    version: ADDRESS_ASSET_APPLY_VERSION,
    generated_at: new Date().toISOString(),
    ok: true,
    run_dir: loaded.runDir,
    approval_bundle: loaded.bundlePath,
    base_id_masked: mask(bases.target_base_id),
    table_id: CENSUS_TABLE_ID,
    bundle_address_count: loaded.address_count,
    bundle_asset_count: loaded.asset_count,
    source_domains_used: domains,
    source_type_breakdown: sourceTypes,
    parent_family_breakdown: parents,
    records_passing_preflight: passing.length + skipMatching.length,
    records_ready_to_apply: passing.length,
    records_already_matching: skipMatching.length,
    records_blocked_by_source_concern: steward.filter((r) =>
      (r.steward_reasons || []).some(
        (s) => s.includes("generic_brand") || s.includes("missing_on_page") || s.includes("fetch_failed")
      )
    ).length,
    records_routed_to_steward_review: steward.length,
    records_hard_blocked: blocked.length,
    exact_apply_count_after_preflight: passing.length,
    page_fetch_summary: [...pageCache.entries()].map(([url, v]) => ({
      url,
      status: v.status,
      ok: v.ok,
      error: v.error,
      bytes: v.text?.length || 0,
    })),
    rows,
    passing_ids: passing.map((r) => r.record_id),
    steward_queue: steward.map((r) => ({
      record_id: r.record_id,
      identity_key: r.identity_key,
      property_name: r.property_name,
      lane: r.lane,
      reasons: r.steward_reasons,
      source_url: r.source_url,
    })),
    blocked_queue: blocked.map((r) => ({
      record_id: r.record_id,
      identity_key: r.identity_key,
      property_name: r.property_name,
      lane: r.lane,
      errors: r.errors,
    })),
  };
}

export function renderPreflightMarkdown(report) {
  const lines = [
    `# Production Census Autopilot — Address + Asset Context Preflight`,
    ``,
    `- Generated: ${report.generated_at}`,
    `- Approval bundle: \`${report.approval_bundle || ""}\``,
    `- Bundle Address proposals: ${report.bundle_address_count}`,
    `- Bundle Asset Context proposals: ${report.bundle_asset_count}`,
    `- Exact apply count after preflight: **${report.exact_apply_count_after_preflight}**`,
    `- Passing (incl. already matching): ${report.records_passing_preflight}`,
    `- Already matching (skip): ${report.records_already_matching}`,
    `- Source-concern / steward: ${report.records_routed_to_steward_review}`,
    `- Hard blocked: ${report.records_hard_blocked}`,
    ``,
    `## Source domains`,
    ``,
    "```json",
    JSON.stringify(report.source_domains_used || {}, null, 2),
    "```",
    ``,
    `## Source type breakdown`,
    ``,
    "```json",
    JSON.stringify(report.source_type_breakdown || {}, null, 2),
    "```",
    ``,
    `## Parent / family breakdown`,
    ``,
    "```json",
    JSON.stringify(report.parent_family_breakdown || {}, null, 2),
    "```",
    ``,
    `## Steward review (source concern)`,
    ``,
    `- Count: ${(report.steward_queue || []).length}`,
    `- Typical reason: Choice \`regional-hotels?placeId=\` shared Mexico directory is not property-level URL (official property URLs exist on VIC but were not in the frozen Address Source URL — no re-plan).`,
    ``,
  ];
  return lines.join("\n");
}

/**
 * Apply only preflight-passing Address + Asset Context writes.
 */
export async function runAddressAssetApprovalBundleApply(argv = process.argv.slice(2), env = process.env) {
  const args = parseAddressAssetApplyArgs(argv);
  const envCheck = checkAddressAssetApplyEnv(env);
  const started = Date.now();

  const preflight = await runAddressAssetPreflight({
    approvalBundlePath: args.approvalBundlePath,
    runDir: args.runDir,
  });
  if (!preflight.ok) {
    return {
      ...preflight,
      apply_executed: false,
      status: STATUS.BLOCKED,
      duration_ms: Date.now() - started,
    };
  }

  const applyRequested = args.enableProductionWrites || args.mode === "apply";
  if (applyRequested && (!args.allConfirmsOk || !envCheck.allOk)) {
    return {
      version: ADDRESS_ASSET_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "confirmation_or_env_missing",
      confirms: args.confirms,
      env_flags: envCheck.flags,
      preflight,
      duration_ms: Date.now() - started,
    };
  }

  if (!applyRequested) {
    return {
      version: ADDRESS_ASSET_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: "preflight_only",
      status:
        preflight.exact_apply_count_after_preflight > 0
          ? preflight.records_routed_to_steward_review > 0
            ? STATUS.PARTIAL
            : STATUS.CLEAN
          : STATUS.BLOCKED,
      apply_executed: false,
      preflight,
      duration_ms: Date.now() - started,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();

  const writeTargetCheck = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    baseId: bases.target_base_id,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTargetCheck.ok || CENSUS_TABLE_ID !== productionHotelPropertyCensus.tableId) {
    return {
      version: ADDRESS_ASSET_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTargetCheck,
      preflight,
      duration_ms: Date.now() - started,
    };
  }

  const censusBefore = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Address",
    "Asset Context",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
    "Hotel Description - Source Text",
    "Owner Name",
    "Operator / Management Company",
    "Property Name",
  ]);
  if (censusBefore.length !== EXPECTED_RECORD_COUNT) {
    return {
      version: ADDRESS_ASSET_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: `unexpected_census_count_${censusBefore.length}`,
      preflight,
    };
  }

  const roomsFilledBefore = censusBefore.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;
  const toWrite = (preflight.rows || []).filter((r) => r.apply && Object.keys(r.fields_to_write || {}).length);

  const writeResults = [];
  for (const row of toWrite) {
    // Re-read immediately before write
    const live = await airtableGet(bases.target_base_id, token, CENSUS_TABLE_ID, row.record_id);
    await sleep(100);
    const recheck = preflightAddressAssetProposal(
      {
        record_id: row.record_id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        patch: row.frozen_patch,
        confidence: "High",
        lane: row.lane,
        queue: row.queue,
      },
      live,
      { vic: loadVicClaimIndex(), skipPageVerify: true }
    );
    // Page evidence already verified in preflight; recheck is idempotent + conflict only
    if (recheck.blocked || (recheck.errors || []).includes("address_already_filled_different")) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: false,
        skipped: true,
        reason: recheck.errors.join(",") || "recheck_blocked",
      });
      continue;
    }
    if (!recheck.apply && recheck.skip_matching) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: true,
        skipped: true,
        reason: "already_matching",
      });
      continue;
    }
    // Use original preflight fields_to_write but rebuild from recheck if write
    const fields =
      recheck.apply && Object.keys(recheck.idempotent.fields || {}).length
        ? recheck.idempotent.fields
        : row.fields_to_write;

    // Strip any accidental forbidden keys
    for (const k of Object.keys(fields)) {
      if (FORBIDDEN_IN_PATCH.includes(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
        delete fields[k];
      }
    }
    if (!Object.keys(fields).length) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: true,
        skipped: true,
        reason: "empty_patch_after_sanitize",
      });
      continue;
    }

    try {
      await airtablePatch(bases.target_base_id, token, CENSUS_TABLE_ID, row.record_id, fields);
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        lane: row.lane,
        ok: true,
        fields_written: Object.keys(fields),
        patch: fields,
      });
      await sleep(180);
    } catch (err) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        lane: row.lane,
        ok: false,
        error: err?.message || String(err),
      });
    }
  }

  const writesOk = writeResults.filter((w) => w.ok && !w.skipped).length;
  const writesFail = writeResults.filter((w) => !w.ok && !w.skipped).length;
  const writesSkipped = writeResults.filter((w) => w.skipped).length;

  // Post-verify applied rows
  const postVerify = [];
  for (const row of toWrite) {
    const live = await airtableGet(bases.target_base_id, token, CENSUS_TABLE_ID, row.record_id);
    await sleep(100);
    const f = live.fields || {};
    const snap = row.live_snapshot || {};
    postVerify.push({
      record_id: row.record_id,
      identity_key: row.identity_key,
      lane: row.lane,
      address_applied:
        row.lane === "address"
          ? norm(f.Address) === norm(row.proposed_address)
          : null,
      asset_applied:
        row.lane === "asset_context"
          ? String(f["Asset Context"] || "") === String(row.proposed_asset_context || "")
          : null,
      coords_unchanged:
        String(f.Latitude ?? "") === String(snap.latitude ?? "") &&
        String(f.Longitude ?? "") === String(snap.longitude ?? ""),
      geocode_meta_unchanged:
        String(f["Coordinate Source Type"] ?? "") === String(snap.coordinate_source_type ?? ""),
      description_unchanged:
        String(f["Hotel Description - Source Text"] ?? "") === String(snap.description ?? ""),
      amenities_unchanged:
        String(f["Amenities - Source Text"] ?? "") === String(snap.amenities ?? ""),
      rooms_unchanged: String(f["Rooms / Keys"] ?? "") === String(snap.rooms_keys ?? ""),
      property_name_unchanged:
        String(f["Property Name"] ?? "") === String(snap.property_name ?? ""),
      owner_still_blank: isBlank(f["Owner Name"]),
      operator_still_blank: isBlank(f["Operator / Management Company"]),
    });
  }

  const censusAfter = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
  ]);
  const roomsFilledAfter = censusAfter.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;

  const verifyOk = postVerify.every(
    (v) =>
      v.coords_unchanged &&
      v.description_unchanged &&
      v.amenities_unchanged &&
      v.rooms_unchanged &&
      v.property_name_unchanged &&
      v.owner_still_blank &&
      v.operator_still_blank
  );
  const roomsOk = roomsFilledAfter === EXPECTED_ROOMS_FILLED && roomsFilledAfter === roomsFilledBefore;

  let status = STATUS.PARTIAL;
  if (writesOk === 0 && (writesFail > 0 || preflight.exact_apply_count_after_preflight === 0)) {
    status = STATUS.BLOCKED;
  } else if (
    writesFail === 0 &&
    verifyOk &&
    roomsOk &&
    preflight.records_routed_to_steward_review === 0 &&
    writesOk === preflight.exact_apply_count_after_preflight
  ) {
    status = STATUS.CLEAN;
  } else if (writesOk > 0 && preflight.records_routed_to_steward_review > 0 && writesFail === 0 && verifyOk) {
    status = STATUS.PARTIAL;
  } else if (writesOk > 0 && writesFail === 0 && verifyOk) {
    status = STATUS.PARTIAL;
  } else if (writesFail > 0 && writesOk === 0) {
    status = STATUS.BLOCKED;
  }

  return {
    version: ADDRESS_ASSET_APPLY_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status,
    duration_ms: Date.now() - started,
    run_dir: preflight.run_dir,
    approval_bundle: preflight.approval_bundle,
    base_id_masked: mask(bases.target_base_id),
    table_id: CENSUS_TABLE_ID,
    airtable_writes: true,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    census_record_count_before: censusBefore.length,
    census_record_count_after: censusAfter.length,
    rooms_filled_before: roomsFilledBefore,
    rooms_filled_after: roomsFilledAfter,
    records_updated: writesOk,
    records_skipped: writesSkipped,
    records_failed: writesFail,
    exact_apply_count_after_preflight: preflight.exact_apply_count_after_preflight,
    steward_count: preflight.records_routed_to_steward_review,
    blocked_count: preflight.records_hard_blocked,
    fields_written_union: [
      ...new Set(writeResults.flatMap((w) => w.fields_written || [])),
    ],
    write_results: writeResults,
    post_verify: postVerify,
    post_verify_ok: verifyOk,
    rooms_unchanged_ok: roomsOk,
    preflight_summary: {
      source_domains_used: preflight.source_domains_used,
      source_type_breakdown: preflight.source_type_breakdown,
      parent_family_breakdown: preflight.parent_family_breakdown,
      records_ready_to_apply: preflight.records_ready_to_apply,
      records_routed_to_steward_review: preflight.records_routed_to_steward_review,
      records_hard_blocked: preflight.records_hard_blocked,
    },
    steward_queue: preflight.steward_queue,
    blocked_queue: preflight.blocked_queue,
    preflight,
  };
}

export function renderApplyMarkdown(report) {
  return [
    `# Production Census Autopilot — Address + Asset Context Apply`,
    ``,
    `- Status: **${report.status}**`,
    `- Generated: ${report.generated_at}`,
    `- Apply executed: ${report.apply_executed}`,
    `- Records updated: ${report.records_updated}`,
    `- Records skipped: ${report.records_skipped}`,
    `- Records failed: ${report.records_failed}`,
    `- Steward review: ${report.steward_count}`,
    `- Hard blocked: ${report.blocked_count}`,
    `- Census count: ${report.census_record_count_after} (expected ${EXPECTED_RECORD_COUNT})`,
    `- Rooms filled: ${report.rooms_filled_after} (expected ${EXPECTED_ROOMS_FILLED})`,
    `- Brand Explorer writes: ${report.brand_explorer_writes}`,
    `- Brand Setup writes: ${report.brand_setup_writes}`,
    `- Fields written: ${(report.fields_written_union || []).join(", ") || "(none)"}`,
    `- Post-verify OK: ${report.post_verify_ok}`,
    ``,
    `## Preflight summary`,
    ``,
    "```json",
    JSON.stringify(report.preflight_summary || {}, null, 2),
    "```",
    ``,
  ].join("\n");
}

export function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
export function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}
