/**
 * Controlled re-sourcing of stewarded Choice Address proposals.
 * Uses property-level official URLs only (VIC official_property_url preferred).
 * Never writes Airtable. Never proposes geocode / desc / amenities / rooms / names.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import {
  loadVicClaimIndex,
  MAP_FIRST_PASS,
} from "./production-census-first-pass-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { confirmOfficialAddress } from "./production-census-address-geocode-resolver.js";
import {
  warmFamilyDirectoryCaches,
  lookupChoiceRegionalRow,
  extractChoicePropertyId,
} from "./census-autopilot-family-directory-adapters.js";
import { canonicalChoicePropertyUrl } from "../choice-regional-directory-extract.js";
import { AUTOPILOT_FORBIDDEN_FIELDS } from "./census-autopilot-field-allowlist.js";
import {
  buildIdempotentPatch,
  compareFieldValues,
} from "./census-autopilot-idempotent-writer.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const CHOICE_ADDRESS_RESOURCING_VERSION =
  "census-autopilot-choice-address-resourcing-controlled-v1";

export const STATUS = Object.freeze({
  READY: "production_census_choice_address_resourcing_controlled_ready_for_apply",
  PARTIAL: "production_census_choice_address_resourcing_partial_steward_remaining",
  BLOCKED: "production_census_choice_address_resourcing_blocked",
});

export const RECORD_SET_STEWARDED_CHOICE_29 = "stewarded_choice_address_29";

export const DEFAULT_STEWARD_QUEUE =
  "reports/research-engine-v2/autopilot/2026-08-05_21-49-37-CALA-active-brands/steward-review-queue.json";

export const ALLOWED_PATCH_FIELDS = Object.freeze([
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Last Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
]);

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
const FORBIDDEN = new Set([
  ...AUTOPILOT_FORBIDDEN_FIELDS,
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
  "Rooms / Keys",
  "Property Name",
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || v === "" || (typeof v === "string" && !v.trim());
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
function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}

/**
 * True when URL is a Choice property page (…/{code}) not regional-hotels?placeId=.
 */
export function isChoicePropertyLevelUrl(url) {
  const s = String(url || "").trim();
  if (!s) return false;
  if (/regional-hotels/i.test(s) && /placeId=/i.test(s)) return false;
  if (/\/mexico\/regional-hotels/i.test(s)) return false;
  if (/\/regional-hotels\/?$/i.test(s)) return false;
  try {
    const u = new URL(s);
    if (!/choicehotels\.com$/i.test(u.hostname.replace(/^www\./, "")) &&
        !/\.choicehotels\.com$/i.test(u.hostname)) {
      return false;
    }
    const path = u.pathname.replace(/\/en-[a-z]{2}\//i, "/");
    // /{region}/{city}/{brand-slug}/{xxNNN} — Mexico MX### and CALA CB###/PN###/CR###/…
    return /\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z0-9-]+\/[a-z]{2}\d{2,3}\/?$/i.test(path);
  } catch {
    return false;
  }
}

/**
 * Load the 29 stewarded Choice Address records from prior apply steward queue.
 */
export function loadStewardedChoiceAddress29(opts = {}) {
  const path = resolve(opts.stewardQueuePath || join(ROOT, DEFAULT_STEWARD_QUEUE));
  if (!existsSync(path)) {
    return { ok: false, error: `steward_queue_missing:${path}` };
  }
  const doc = JSON.parse(readFileSync(path, "utf8"));
  const items = (doc.items || []).filter(
    (it) =>
      it.lane === "address" &&
      String(it.identity_key || "").includes("_choice_") &&
      (it.reasons || []).some((r) =>
        /generic_brand_regional|property_level|placeId|regional/i.test(String(r))
      )
  );
  if (items.length === 0) {
    // Fallback: all address-lane Choice items in steward queue
    const fallback = (doc.items || []).filter(
      (it) => it.lane === "address" && String(it.identity_key || "").includes("_choice_")
    );
    if (fallback.length !== 29 && fallback.length === 0) {
      return { ok: false, error: "no_stewarded_choice_address_items", path };
    }
    return {
      ok: true,
      path,
      expected: 29,
      items: fallback,
      note: fallback.length === 29 ? null : `loaded_${fallback.length}_not_29`,
    };
  }
  return { ok: true, path, expected: 29, items, note: null };
}

/**
 * Resolve property-level address + source URL for one Choice steward record.
 * Order: VIC official_property_url + High address claim → Choice regional card
 * with property-specific URL → steward.
 */
export async function resolveChoicePropertyLevelAddress(stewardItem, opts = {}) {
  const identityKey = stewardItem.identity_key;
  const vic = opts.vic || loadVicClaimIndex();
  const vicRec = vic.byId.get(identityKey) || null;
  const propertyId = extractChoicePropertyId({}, identityKey);

  const claim = (vicRec?.field_claims || []).find(
    (c) =>
      ["Address", "Address 1", "Street Address"].includes(c.field) &&
      c.value != null &&
      String(c.value).trim()
  );

  let card = null;
  if (opts.skipDirectory !== true) {
    const hit = await lookupChoiceRegionalRow({}, identityKey);
    if (hit.ok) card = hit.row;
  }

  const vicOfficial = vicRec?.official_property_url
    ? canonicalChoicePropertyUrl(vicRec.official_property_url)
    : null;
  const cardPropertyUrl = card?.propertyUrl
    ? canonicalChoicePropertyUrl(card.propertyUrl)
    : null;

  // Prefer VIC property-level official URL
  let sourceUrl = null;
  let sourceMethod = null;
  let sourceType = null;

  if (vicOfficial && isChoicePropertyLevelUrl(vicOfficial)) {
    sourceUrl = vicOfficial;
    sourceMethod = "vic_official_property_url";
    sourceType = "official_property_page";
  } else if (cardPropertyUrl && isChoicePropertyLevelUrl(cardPropertyUrl)) {
    sourceUrl = cardPropertyUrl;
    sourceMethod = "choice_regional_jsonld_property_url";
    sourceType = "official_property_page";
  }

  if (!sourceUrl) {
    return {
      ok: false,
      steward: true,
      reason: "no_property_level_official_url",
      identity_key: identityKey,
      property_id: propertyId,
      vic_official: vicOfficial,
      card_property_url: cardPropertyUrl,
    };
  }

  // Address: prefer VIC High claim, else Choice card street line (same property)
  let address = null;
  let addressMethod = null;
  let claimConf = null;
  if (claim && isStreetLevelAddress(claim.value) && claim.confidence === "High") {
    address = String(claim.value).trim();
    addressMethod = "vic_high_address_claim";
    claimConf = "High";
  } else if (card?.addressLine1 && isStreetLevelAddress(card.addressLine1)) {
    address = String(card.addressLine1).trim();
    addressMethod = "choice_regional_hotel_card_address";
    claimConf = "High";
  }

  if (!address) {
    return {
      ok: false,
      steward: true,
      reason: "no_street_level_address_from_official_property_source",
      identity_key: identityKey,
      source_url: sourceUrl,
    };
  }

  // If VIC claim and card disagree materially → steward
  if (
    claim &&
    card?.addressLine1 &&
    isStreetLevelAddress(claim.value) &&
    isStreetLevelAddress(card.addressLine1) &&
    norm(claim.value) !== norm(card.addressLine1)
  ) {
    // Soft: allow if one is prefix of the other
    const a = norm(claim.value);
    const b = norm(card.addressLine1);
    if (!a.includes(b.slice(0, Math.min(12, b.length))) && !b.includes(a.slice(0, Math.min(12, a.length)))) {
      return {
        ok: false,
        steward: true,
        reason: "vic_and_card_address_conflict",
        identity_key: identityKey,
        vic_address: claim.value,
        card_address: card.addressLine1,
        source_url: sourceUrl,
      };
    }
  }

  return {
    ok: true,
    identity_key: identityKey,
    property_id: propertyId,
    address,
    address_method: addressMethod,
    source_url: sourceUrl,
    source_method: sourceMethod,
    source_type: sourceType,
    confidence: "High",
    claim_confidence: claimConf,
    property_name: vicRec?.name || stewardItem.property_name || card?.name || null,
    brand: vicRec?.brand || null,
    city: vicRec?.city || card?.city || null,
    country: vicRec?.country || card?.country || "Mexico",
    card_name: card?.name || null,
    evidence_directory_url: card?.source_url || null,
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

/**
 * Live Census preflight for one resourced proposal.
 */
export function preflightChoiceAddressResourcing(resolved, liveRecord) {
  const errors = [];
  const fields = liveRecord?.fields || {};
  if (!liveRecord?.id) errors.push("record_not_found");
  if (fields[MAP_FIRST_PASS.humanReview] === true || fields["Human Review Required"] === true) {
    errors.push("held_human_review_required");
  }
  if (String(fields[MAP_FIRST_PASS.affiliationStatus] || fields["Affiliation Status"] || "") === "Brand-Unconfirmed") {
    errors.push("brand_unconfirmed");
  }
  if (!resolved?.ok) errors.push(resolved?.reason || "resolve_failed");
  if (resolved?.confidence !== "High") errors.push("confidence_not_high");
  if (!isChoicePropertyLevelUrl(resolved?.source_url)) {
    errors.push("source_url_not_property_level");
  }
  if (/regional-hotels/i.test(String(resolved?.source_url || "")) && /placeId=/i.test(String(resolved?.source_url || ""))) {
    errors.push("shared_regional_placeid_forbidden");
  }
  if (!isStreetLevelAddress(resolved?.address)) errors.push("address_not_street_level");

  const liveName = fields[MAP_FIRST_PASS.propertyName] || fields["Property Name"];
  const liveCity = fields[MAP_FIRST_PASS.city] || fields.City;
  const liveCountry = fields[MAP_FIRST_PASS.country] || fields.Country || "Mexico";
  const confirmed = confirmOfficialAddress({
    address: resolved?.address,
    propertyName: liveName || resolved?.property_name,
    city: liveCity || resolved?.city,
    state: fields[MAP_FIRST_PASS.stateRegion] || fields["State / Region"],
    country: liveCountry,
  });
  if (!confirmed.ok) errors.push(`address_confirm:${confirmed.reason}`);

  // Identity soft match
  if (resolved?.property_name && liveName) {
    const liveTokens = new Set(norm(liveName).split(" ").filter((t) => t.length >= 4));
    const resTokens = norm(resolved.property_name).split(" ").filter((t) => t.length >= 4);
    const overlap = resTokens.filter((t) => liveTokens.has(t)).length;
    if (overlap < 1 && norm(liveName) !== norm(resolved.property_name)) {
      errors.push("property_name_mismatch");
    }
  }

  const currentAddr = fields[MAP_FIRST_PASS.address] || fields.Address;
  if (!isBlank(currentAddr) && norm(currentAddr) !== norm(resolved?.address)) {
    errors.push("address_already_filled_different");
  }
  const alreadyMatching =
    !isBlank(currentAddr) && norm(currentAddr) === norm(resolved?.address);

  /** @type {Record<string, unknown>} */
  const patch = {
    Address: resolved.address,
    "Address Confidence": "High",
    "Address Source URL": resolved.source_url,
    "Last Reviewed Date": todayIsoDate(),
  };
  for (const k of Object.keys(patch)) {
    if (FORBIDDEN.has(k)) errors.push(`forbidden_field:${k}`);
    if (!ALLOWED_PATCH_FIELDS.includes(k)) errors.push(`unapproved_field:${k}`);
  }

  const hard = errors.length > 0;
  return {
    ok: !hard && !alreadyMatching,
    skip_matching: !hard && alreadyMatching,
    steward: hard && errors.every((e) => !e.startsWith("forbidden") && e !== "record_not_found"),
    blocked: hard,
    errors,
    patch: hard || alreadyMatching ? {} : patch,
    live_snapshot: {
      address: currentAddr ?? null,
      property_name: liveName ?? null,
      city: liveCity ?? null,
      country: liveCountry ?? null,
      brand: fields[MAP_FIRST_PASS.currentBrand] || fields["Current Brand"] || null,
      affiliation: fields[MAP_FIRST_PASS.affiliationStatus] || null,
      held: fields[MAP_FIRST_PASS.humanReview] ?? null,
      latitude: fields.Latitude ?? null,
      longitude: fields.Longitude ?? null,
      rooms: fields["Rooms / Keys"] ?? null,
      description: fields["Hotel Description - Source Text"] ?? null,
      amenities: fields["Amenities - Source Text"] ?? null,
      owner: fields["Owner Name"] ?? null,
    },
  };
}

/**
 * Run controlled Choice address resourcing for stewarded_choice_address_29.
 * Dry-run / controlled only — never writes.
 */
export async function runChoiceAddressResourcingControlled(opts = {}) {
  const started = Date.now();
  const loaded = loadStewardedChoiceAddress29({
    stewardQueuePath: opts.stewardQueuePath,
  });
  if (!loaded.ok) {
    return {
      version: CHOICE_ADDRESS_RESOURCING_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      ok: false,
      error: loaded.error,
      airtable_writes: false,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    return {
      version: CHOICE_ADDRESS_RESOURCING_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      ok: false,
      error: "missing_airtable_credentials",
      airtable_writes: false,
    };
  }

  await warmFamilyDirectoryCaches({ delayMs: opts.delayMs ?? 80 });
  const vic = loadVicClaimIndex();

  const rows = [];
  let propertyLevelUrlsFound = 0;

  for (const item of loaded.items) {
    const resolved = await resolveChoicePropertyLevelAddress(item, { vic });
    if (resolved.ok && isChoicePropertyLevelUrl(resolved.source_url)) {
      propertyLevelUrlsFound += 1;
    }

    let live;
    try {
      live = await airtableGet(
        bases.target_base_id,
        token,
        CENSUS_TABLE_ID,
        item.record_id
      );
      await sleep(120);
    } catch (err) {
      rows.push({
        record_id: item.record_id,
        identity_key: item.identity_key,
        property_name: item.property_name,
        ok: false,
        blocked: true,
        steward: false,
        errors: [`airtable_get_failed:${err?.message || err}`],
        resolved,
      });
      continue;
    }

    if (!resolved.ok) {
      rows.push({
        record_id: item.record_id,
        identity_key: item.identity_key,
        property_name: item.property_name,
        ok: false,
        blocked: false,
        steward: true,
        errors: [resolved.reason],
        steward_reasons: [resolved.reason],
        resolved,
        live_snapshot: {
          address: live.fields?.[MAP_FIRST_PASS.address] ?? null,
        },
      });
      continue;
    }

    const pf = preflightChoiceAddressResourcing(resolved, live);
    rows.push({
      record_id: item.record_id,
      identity_key: item.identity_key,
      property_name: item.property_name || resolved.property_name,
      family: "Choice",
      queue: "address_confirmation",
      confidence: "High",
      ok: pf.ok,
      skip_matching: pf.skip_matching,
      steward: pf.steward || (!pf.ok && !pf.skip_matching),
      blocked: pf.blocked && (pf.errors || []).some((e) => e.startsWith("forbidden") || e === "record_not_found"),
      errors: pf.errors,
      steward_reasons: pf.ok || pf.skip_matching ? [] : pf.errors,
      patch: pf.patch,
      patch_fields: Object.keys(pf.patch || {}),
      source_url: resolved.source_url,
      source_type: resolved.source_type,
      source_method: resolved.source_method,
      address_method: resolved.address_method,
      proposed_address: resolved.address,
      live_snapshot: pf.live_snapshot,
      prior_steward_source_url: item.source_url,
    });
  }

  const passing = rows.filter((r) => r.ok && Object.keys(r.patch || {}).length);
  const skipMatching = rows.filter((r) => r.skip_matching);
  const steward = rows.filter((r) => r.steward && !r.ok && !r.skip_matching);
  const blocked = rows.filter((r) => r.blocked);

  const sourceTypes = {};
  const sourceExamples = [];
  for (const r of passing) {
    sourceTypes[r.source_type || "(none)"] = (sourceTypes[r.source_type || "(none)"] || 0) + 1;
    if (sourceExamples.length < 5) {
      sourceExamples.push({
        identity_key: r.identity_key,
        address: r.proposed_address,
        source_url: r.source_url,
        method: r.source_method,
      });
    }
  }

  let status = STATUS.BLOCKED;
  if (passing.length > 0 && steward.length === 0 && blocked.length === 0) {
    status = STATUS.READY;
  } else if (passing.length > 0) {
    status = STATUS.PARTIAL;
  } else if (skipMatching.length === loaded.items.length) {
    status = STATUS.READY; // nothing to write but clean
  }

  const runId = opts.runId || buildRunId();
  const runDir =
    opts.runDir ||
    join(ROOT, "reports/research-engine-v2/autopilot", runId);

  const proposals = passing.map((r) => ({
    record_id: r.record_id,
    identity_key: r.identity_key,
    property_name: r.property_name,
    brand: r.live_snapshot?.brand || null,
    family: "Choice",
    queue: "address_confirmation",
    action: "propose_high_write",
    confidence: "High",
    write_allowed_now: true,
    patch: r.patch,
    patch_fields: r.patch_fields,
    source_url: r.source_url,
    method: `${r.source_method}+${r.address_method}`,
    notes: "choice_address_resourcing_property_level_url_only",
  }));

  const approvalBundle = {
    version: CHOICE_ADDRESS_RESOURCING_VERSION,
    status: "awaiting_founder_approval",
    stop_before_writes: true,
    airtable_writes: false,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    run_id: runId,
    mode: "controlled",
    scope: "active-brand-setup",
    region: "CALA",
    strategy: "fastest-safe",
    record_set: RECORD_SET_STEWARDED_CHOICE_29,
    queues_executed: ["address_confirmation"],
    records_proposed: proposals.length,
    fields_proposed: ["Address", "Address Confidence", "Address Source URL", "Last Reviewed Date"],
    proposed_writes_by_queue: {
      address_confirmation: proposals.map((p) => ({
        record_id: p.record_id,
        identity_key: p.identity_key,
        property_name: p.property_name,
        confidence: "High",
        patch_fields: p.patch_fields,
        patch: p.patch,
      })),
    },
    proposed_writes: proposals,
    apply_recommendation: proposals.length >= 1,
    forbidden_fields: [...FORBIDDEN],
  };

  return {
    version: CHOICE_ADDRESS_RESOURCING_VERSION,
    generated_at: new Date().toISOString(),
    status,
    ok: status !== STATUS.BLOCKED,
    mode: "controlled",
    record_set: RECORD_SET_STEWARDED_CHOICE_29,
    run_id: runId,
    run_dir: runDir,
    airtable_writes: false,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    base_id_masked: mask(bases.target_base_id),
    table_id: CENSUS_TABLE_ID,
    steward_queue_source: loaded.path,
    total_stewarded_choice_loaded: loaded.items.length,
    property_level_official_urls_found: propertyLevelUrlsFound,
    records_passing_preflight: passing.length,
    records_already_matching: skipMatching.length,
    records_still_stewarded: steward.length,
    records_blocked: blocked.length,
    exact_writes_if_applied: passing.length,
    source_type_breakdown: sourceTypes,
    source_url_examples: sourceExamples,
    duration_ms: Date.now() - started,
    rows,
    proposals,
    approval_bundle: approvalBundle,
    steward_queue: steward.map((r) => ({
      record_id: r.record_id,
      identity_key: r.identity_key,
      property_name: r.property_name,
      lane: "address",
      reasons: r.steward_reasons || r.errors,
      source_url: r.source_url || r.resolved?.source_url || null,
    })),
    blocked_queue: blocked.map((r) => ({
      record_id: r.record_id,
      identity_key: r.identity_key,
      errors: r.errors,
    })),
    validation: {
      airtable_writes: false,
      brand_explorer_writes: false,
      brand_setup_writes: false,
      owner_operator_date_writes: false,
      coordinates_proposed: false,
      descriptions_proposed: false,
      amenities_proposed: false,
      rooms_proposed: false,
      property_names_proposed: false,
      held_excluded: rows.every(
        (r) => !(r.errors || []).includes("held_human_review_required") || !r.ok
      ),
      brand_unconfirmed_excluded: rows.every(
        (r) => !(r.errors || []).includes("brand_unconfirmed") || !r.ok
      ),
      no_shared_regional_placeid_in_passing: passing.every(
        (r) => !/placeId=/i.test(String(r.source_url || ""))
      ),
    },
  };
}

function buildRunId() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  const stamp = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}_${pad(d.getUTCHours())}-${pad(d.getUTCMinutes())}-${pad(d.getUTCSeconds())}`;
  return `${stamp}-CALA-choice-address-resourcing`;
}

export function renderChoiceAddressResourcingMarkdown(report) {
  return [
    `# Production Census — Choice Address Resourcing (Controlled)`,
    ``,
    `- Status: **${report.status}**`,
    `- Generated: ${report.generated_at}`,
    `- Record set: \`${report.record_set}\``,
    `- Airtable writes: **false**`,
    `- Stewarded Choice loaded: ${report.total_stewarded_choice_loaded}`,
    `- Property-level official URLs found: ${report.property_level_official_urls_found}`,
    `- Passing preflight (would-write): **${report.records_passing_preflight}**`,
    `- Already matching: ${report.records_already_matching}`,
    `- Still stewarded: ${report.records_still_stewarded}`,
    `- Blocked: ${report.records_blocked}`,
    `- Exact writes if applied: ${report.exact_writes_if_applied}`,
    ``,
    `## Source type breakdown`,
    ``,
    "```json",
    JSON.stringify(report.source_type_breakdown || {}, null, 2),
    "```",
    ``,
    `## Source URL examples`,
    ``,
    "```json",
    JSON.stringify(report.source_url_examples || [], null, 2),
    "```",
    ``,
    `## Validation`,
    ``,
    "```json",
    JSON.stringify(report.validation || {}, null, 2),
    "```",
    ``,
    `## Next step`,
    ``,
    report.records_passing_preflight > 0
      ? `Founder review → approval-bundle-bound apply with \`--approval-bundle ${report.run_dir}/approval-bundle.json\` (Address lane only).`
      : `No passing proposals — review steward queue.`,
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

/**
 * Persist run folder + top-level reports from a resourcing result.
 */
export function persistChoiceAddressResourcingOutputs(report, opts = {}) {
  const runDir = report.run_dir;
  mkdirSync(runDir, { recursive: true });
  writeJson(join(runDir, "approval-bundle.json"), report.approval_bundle);
  writeJson(join(runDir, "choice-address-resourcing-report.json"), report);
  writeMd(
    join(runDir, "choice-address-resourcing-report.md"),
    renderChoiceAddressResourcingMarkdown(report)
  );
  writeJson(join(runDir, "steward-review-queue.json"), {
    generated_at: report.generated_at,
    count: (report.steward_queue || []).length,
    items: report.steward_queue || [],
  });
  writeMd(
    join(runDir, "summary.md"),
    [
      `# Summary — Choice Address Resourcing`,
      ``,
      `- Status: ${report.status}`,
      `- Passing: ${report.records_passing_preflight}`,
      `- Steward: ${report.records_still_stewarded}`,
      `- Airtable writes: false`,
      ``,
    ].join("\n")
  );
  writeJson(join(runDir, "dry-run.json"), {
    mode: "controlled",
    proposals: report.proposals,
    blocked: report.blocked_queue,
    steward_review_queue: report.steward_queue,
    airtable_writes: false,
    note: "choice_address_resourcing_property_level_only",
  });
  writeJson(join(runDir, "checkpoint.json"), {
    status: report.status,
    mode: "controlled",
    record_set: report.record_set,
    completion_status: "complete",
    airtable_writes: false,
    records_proposed: report.records_passing_preflight,
    updated_at: report.generated_at,
  });

  const reportsRoot = opts.reportsRoot || join(ROOT, "reports/research-engine-v2");
  const docsRoot = opts.docsRoot || join(ROOT, "docs/data-intelligence");
  writeJson(
    join(reportsRoot, "production-census-choice-address-resourcing-controlled.json"),
    report
  );
  writeMd(
    join(reportsRoot, "production-census-choice-address-resourcing-controlled.md"),
    renderChoiceAddressResourcingMarkdown(report)
  );
  writeMd(
    join(docsRoot, "production-census-choice-address-resourcing-controlled.md"),
    renderChoiceAddressResourcingMarkdown(report)
  );

  return { runDir };
}

/* -------------------------------------------------------------------------- */
/* Approval-bundle-bound APPLY (29 Choice Address only)                         */
/* -------------------------------------------------------------------------- */

export const CHOICE_ADDRESS_APPLY_VERSION =
  "census-autopilot-choice-address-approval-bundle-apply-v1";

export const APPLY_STATUS = Object.freeze({
  CLEAN: "production_census_choice_address_apply_clean",
  PARTIAL: "production_census_choice_address_apply_partial_needs_review",
  BLOCKED: "production_census_choice_address_apply_blocked",
});

export const APPLY_ALLOWED_FIELDS = Object.freeze([
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Last Reviewed Date",
]);

const EXPECTED_APPLY_COUNT = 29;
const EXPECTED_CENSUS_COUNT = 666;
const EXPECTED_ROOMS_FILLED = 5;

export function parseChoiceAddressApplyArgs(argv = process.argv.slice(2)) {
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
      flags.has("--confirm-approval-bundle-bound") || Boolean(get("--approval-bundle")),
  };
  return {
    approvalBundlePath: get("--approval-bundle"),
    runDir: get("--run-dir"),
    enableProductionWrites: flags.has("--enable-production-writes"),
    mode: get("--mode") || "apply",
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

export function checkChoiceAddressApplyEnv(env = process.env) {
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

export function loadChoiceAddressFrozenProposals(opts = {}) {
  const bundlePath = resolve(
    opts.approvalBundlePath ||
      join(
        ROOT,
        "reports/research-engine-v2/autopilot/2026-08-05_22-17-09-CALA-choice-address-resourcing/approval-bundle.json"
      )
  );
  const runDir = opts.runDir ? resolve(opts.runDir) : dirname(bundlePath);
  if (!existsSync(bundlePath)) {
    return { ok: false, error: `approval_bundle_missing:${bundlePath}` };
  }
  const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
  const raw = bundle.proposed_writes_by_queue?.address_confirmation || bundle.proposed_writes || [];
  if (raw.length !== EXPECTED_APPLY_COUNT) {
    return {
      ok: false,
      error: `expected_${EXPECTED_APPLY_COUNT}_proposals_got_${raw.length}`,
      bundlePath,
    };
  }

  const frozen = [];
  const fieldViolations = [];
  for (const p of raw) {
    const patch = {};
    for (const k of APPLY_ALLOWED_FIELDS) {
      if (k in (p.patch || {})) patch[k] = p.patch[k];
    }
    for (const k of Object.keys(p.patch || {})) {
      if (!APPLY_ALLOWED_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "outside_allowed" });
      }
      if (FORBIDDEN.has(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "forbidden" });
      }
    }
    if (!isChoicePropertyLevelUrl(patch["Address Source URL"])) {
      fieldViolations.push({
        record_id: p.record_id,
        field: "Address Source URL",
        reason: "not_property_level",
      });
    }
    if (/regional-hotels/i.test(String(patch["Address Source URL"] || "")) &&
        /placeId=/i.test(String(patch["Address Source URL"] || ""))) {
      fieldViolations.push({
        record_id: p.record_id,
        field: "Address Source URL",
        reason: "shared_regional_placeid",
      });
    }
    if (patch["Address Confidence"] !== "High") {
      fieldViolations.push({
        record_id: p.record_id,
        field: "Address Confidence",
        reason: "not_high",
      });
    }
    if (!String(p.identity_key || "").includes("_choice_")) {
      fieldViolations.push({
        record_id: p.record_id,
        field: "identity_key",
        reason: "not_choice",
      });
    }
    frozen.push({
      record_id: p.record_id,
      identity_key: p.identity_key,
      property_name: p.property_name,
      confidence: "High",
      queue: "address_confirmation",
      patch,
      patch_fields: Object.keys(patch),
      source_url: patch["Address Source URL"],
    });
  }

  if (fieldViolations.length) {
    return { ok: false, error: "bundle_field_or_source_violations", fieldViolations, bundlePath, runDir };
  }

  return { ok: true, bundlePath, runDir, bundle, frozen };
}

/**
 * Preflight one frozen Choice Address proposal against live Airtable.
 */
export function preflightChoiceAddressApplyProposal(frozen, liveRecord) {
  const errors = [];
  const fields = liveRecord?.fields || {};
  if (!liveRecord?.id) errors.push("record_not_found");
  if (frozen.confidence !== "High") errors.push("confidence_not_high");
  if (fields[MAP_FIRST_PASS.humanReview] === true || fields["Human Review Required"] === true) {
    errors.push("held_human_review_required");
  }
  if (
    String(fields[MAP_FIRST_PASS.affiliationStatus] || fields["Affiliation Status"] || "") ===
    "Brand-Unconfirmed"
  ) {
    errors.push("brand_unconfirmed");
  }
  if (!isChoicePropertyLevelUrl(frozen.patch["Address Source URL"])) {
    errors.push("source_url_not_property_level");
  }
  if (
    /regional-hotels/i.test(String(frozen.patch["Address Source URL"] || "")) &&
    /placeId=/i.test(String(frozen.patch["Address Source URL"] || ""))
  ) {
    errors.push("shared_regional_placeid_forbidden");
  }
  if (frozen.patch["Address Confidence"] !== "High") errors.push("address_confidence_not_high");
  if (!isStreetLevelAddress(frozen.patch.Address)) errors.push("address_not_street_level");

  for (const k of Object.keys(frozen.patch)) {
    if (!APPLY_ALLOWED_FIELDS.includes(k)) errors.push(`unapproved_field:${k}`);
    if (FORBIDDEN.has(k)) errors.push(`forbidden_field:${k}`);
  }

  const addrCmp = compareFieldValues(
    fields.Address ?? fields[MAP_FIRST_PASS.address],
    frozen.patch.Address
  );
  if (addrCmp === "conflict") errors.push("address_already_filled_different");

  const idempotent = buildIdempotentPatch(fields, frozen.patch, {
    confidence: "High",
    allowGeocode: false,
    schemaV114Ready: true,
    threshold: "High",
  });
  const fatalConflicts = (idempotent.conflicts || []).filter((c) =>
    ["Address", "Address Confidence", "Address Source URL"].includes(c.field)
  );
  if (fatalConflicts.length) {
    for (const c of fatalConflicts) errors.push(`conflict:${c.field}`);
  }

  const fieldsToWrite = { ...(idempotent.fields || {}) };
  for (const k of Object.keys(fieldsToWrite)) {
    if (!APPLY_ALLOWED_FIELDS.includes(k)) delete fieldsToWrite[k];
  }

  const skipMatching =
    errors.length === 0 && addrCmp === "skip" && Object.keys(fieldsToWrite).length === 0;

  return {
    ok: errors.length === 0 && (Object.keys(fieldsToWrite).length > 0 || skipMatching),
    apply: errors.length === 0 && Object.keys(fieldsToWrite).length > 0,
    skip_matching: skipMatching,
    blocked: errors.length > 0,
    errors,
    fields_to_write: fieldsToWrite,
    live_snapshot: {
      address: fields.Address ?? fields[MAP_FIRST_PASS.address] ?? null,
      address_source_url: fields["Address Source URL"] ?? null,
      property_name: fields[MAP_FIRST_PASS.propertyName] ?? fields["Property Name"] ?? null,
      latitude: fields.Latitude ?? null,
      longitude: fields.Longitude ?? null,
      rooms_keys: fields["Rooms / Keys"] ?? null,
      description: fields["Hotel Description - Source Text"] ?? null,
      amenities: fields["Amenities - Source Text"] ?? null,
      asset_context: fields["Asset Context"] ?? null,
      property_type: fields["Property Type"] ?? null,
      owner: fields["Owner Name"] ?? null,
      operator: fields["Operator / Management Company"] ?? null,
      developer: fields["Developer Name"] ?? null,
      opening_date: fields["Opening Date"] ?? null,
    },
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
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
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

/**
 * Apply frozen Choice Address approval bundle (29 records). No re-plan.
 */
export async function runChoiceAddressApprovalBundleApply(
  argv = process.argv.slice(2),
  env = process.env
) {
  const args = parseChoiceAddressApplyArgs(argv);
  const envCheck = checkChoiceAddressApplyEnv(env);
  const started = Date.now();

  const loaded = loadChoiceAddressFrozenProposals({
    approvalBundlePath: args.approvalBundlePath,
    runDir: args.runDir,
  });
  if (!loaded.ok) {
    return {
      version: CHOICE_ADDRESS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: loaded.error,
      fieldViolations: loaded.fieldViolations || null,
      airtable_writes: false,
    };
  }

  if (!args.enableProductionWrites || args.mode !== "apply") {
    return {
      version: CHOICE_ADDRESS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "enable_production_writes_required",
      airtable_writes: false,
    };
  }
  if (!args.allConfirmsOk || !envCheck.allOk) {
    return {
      version: CHOICE_ADDRESS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "confirmation_or_env_missing",
      confirms: args.confirms,
      env_flags: envCheck.flags,
      airtable_writes: false,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    return {
      version: CHOICE_ADDRESS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
    };
  }

  const writeTargetCheck = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    baseId: bases.target_base_id,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTargetCheck.ok || CENSUS_TABLE_ID !== productionHotelPropertyCensus.tableId) {
    return {
      version: CHOICE_ADDRESS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTargetCheck,
      airtable_writes: false,
    };
  }

  const censusBefore = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Address",
    "Address Source URL",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
    "Hotel Description - Source Text",
    "Amenities - Source Text",
    "Asset Context",
    "Property Type",
    "Property Name",
    "Owner Name",
    "Operator / Management Company",
  ]);
  if (censusBefore.length !== EXPECTED_CENSUS_COUNT) {
    return {
      version: CHOICE_ADDRESS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: `unexpected_census_count_${censusBefore.length}`,
      airtable_writes: false,
    };
  }
  const roomsFilledBefore = censusBefore.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;
  const addressSnapshotBefore = Object.fromEntries(
    censusBefore.map((r) => [
      r.id,
      {
        address: r.fields?.Address ?? null,
        source: r.fields?.["Address Source URL"] ?? null,
        name: r.fields?.["Property Name"] ?? null,
      },
    ])
  );

  const preflightRows = [];
  for (const frozen of loaded.frozen) {
    const live = await airtableGet(
      bases.target_base_id,
      token,
      CENSUS_TABLE_ID,
      frozen.record_id
    );
    await sleep(120);
    const pf = preflightChoiceAddressApplyProposal(frozen, live);
    preflightRows.push({
      record_id: frozen.record_id,
      identity_key: frozen.identity_key,
      property_name: frozen.property_name,
      source_url: frozen.source_url,
      proposed_address: frozen.patch.Address,
      ok: pf.ok,
      apply: pf.apply,
      skip_matching: pf.skip_matching,
      blocked: pf.blocked,
      errors: pf.errors,
      fields_to_write: pf.fields_to_write,
      live_snapshot: pf.live_snapshot,
      frozen_patch: frozen.patch,
    });
  }

  const regionalInBundle = preflightRows.filter(
    (r) => /placeId=/i.test(String(r.source_url || ""))
  );
  if (regionalInBundle.length) {
    return {
      version: CHOICE_ADDRESS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "regional_placeid_urls_in_bundle",
      regional_count: regionalInBundle.length,
      airtable_writes: false,
    };
  }

  const preflightFail = preflightRows.filter((r) => r.blocked);
  if (preflightFail.length) {
    return {
      version: CHOICE_ADDRESS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "preflight_failed",
      preflight_failures: preflightFail.map((r) => ({
        record_id: r.record_id,
        identity_key: r.identity_key,
        errors: r.errors,
      })),
      airtable_writes: false,
      run_dir: loaded.runDir,
    };
  }

  const toWrite = preflightRows.filter((r) => r.apply);
  const writeResults = [];
  for (const row of toWrite) {
    const live = await airtableGet(
      bases.target_base_id,
      token,
      CENSUS_TABLE_ID,
      row.record_id
    );
    await sleep(100);
    const recheck = preflightChoiceAddressApplyProposal(
      {
        record_id: row.record_id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        confidence: "High",
        patch: row.frozen_patch,
      },
      live
    );
    if (recheck.blocked) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: false,
        skipped: true,
        reason: recheck.errors.join("|"),
      });
      continue;
    }
    if (recheck.skip_matching || !Object.keys(recheck.fields_to_write || {}).length) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: true,
        skipped: true,
        reason: "already_matching",
      });
      continue;
    }
    try {
      await airtablePatch(
        bases.target_base_id,
        token,
        CENSUS_TABLE_ID,
        row.record_id,
        recheck.fields_to_write
      );
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: true,
        fields_written: Object.keys(recheck.fields_to_write),
        patch: recheck.fields_to_write,
      });
      await sleep(180);
    } catch (err) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: false,
        error: err?.message || String(err),
      });
    }
  }

  const writesOk = writeResults.filter((w) => w.ok && !w.skipped).length;
  const writesSkipped = writeResults.filter((w) => w.skipped).length;
  const writesFail = writeResults.filter((w) => !w.ok && !w.skipped).length;
  const idempotentSkips = preflightRows.filter((r) => r.skip_matching).length;

  const postVerify = [];
  for (const frozen of loaded.frozen) {
    const live = await airtableGet(bases.target_base_id, token, CENSUS_TABLE_ID, frozen.record_id);
    await sleep(100);
    const f = live.fields || {};
    const pre = preflightRows.find((r) => r.record_id === frozen.record_id);
    const snap = pre?.live_snapshot || {};
    postVerify.push({
      record_id: frozen.record_id,
      identity_key: frozen.identity_key,
      address_match: norm(f.Address) === norm(frozen.patch.Address),
      source_is_property_level: isChoicePropertyLevelUrl(f["Address Source URL"]),
      no_regional_placeid: !/placeId=/i.test(String(f["Address Source URL"] || "")),
      coords_unchanged:
        String(f.Latitude ?? "") === String(snap.latitude ?? "") &&
        String(f.Longitude ?? "") === String(snap.longitude ?? ""),
      description_unchanged:
        String(f["Hotel Description - Source Text"] ?? "") === String(snap.description ?? ""),
      amenities_unchanged:
        String(f["Amenities - Source Text"] ?? "") === String(snap.amenities ?? ""),
      rooms_unchanged: String(f["Rooms / Keys"] ?? "") === String(snap.rooms_keys ?? ""),
      property_name_unchanged:
        String(f["Property Name"] ?? "") === String(snap.property_name ?? ""),
      asset_context_unchanged:
        String(f["Asset Context"] ?? "") === String(snap.asset_context ?? ""),
      property_type_unchanged:
        String(f["Property Type"] ?? "") === String(snap.property_type ?? ""),
      owner_still_blank: isBlank(f["Owner Name"]),
      operator_still_blank: isBlank(f["Operator / Management Company"]),
    });
  }

  const censusAfter = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Address",
    "Address Source URL",
    "Rooms / Keys",
    "Property Name",
  ]);
  const roomsFilledAfter = censusAfter.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;
  const approvedIds = new Set(loaded.frozen.map((f) => f.record_id));
  let otherAddressChanges = 0;
  for (const r of censusAfter) {
    if (approvedIds.has(r.id)) continue;
    const before = addressSnapshotBefore[r.id];
    const afterAddr = r.fields?.Address ?? null;
    const afterSrc = r.fields?.["Address Source URL"] ?? null;
    if (
      String(before?.address ?? "") !== String(afterAddr ?? "") ||
      String(before?.source ?? "") !== String(afterSrc ?? "")
    ) {
      otherAddressChanges += 1;
    }
  }

  const verifyOk = postVerify.every(
    (v) =>
      v.address_match &&
      v.source_is_property_level &&
      v.no_regional_placeid &&
      v.coords_unchanged &&
      v.description_unchanged &&
      v.amenities_unchanged &&
      v.rooms_unchanged &&
      v.property_name_unchanged &&
      v.asset_context_unchanged &&
      v.property_type_unchanged &&
      v.owner_still_blank &&
      v.operator_still_blank
  );
  const roomsOk = roomsFilledAfter === EXPECTED_ROOMS_FILLED && roomsFilledAfter === roomsFilledBefore;
  const countOk =
    writesOk + writesSkipped + idempotentSkips === EXPECTED_APPLY_COUNT ||
    writesOk + idempotentSkips + writesSkipped === EXPECTED_APPLY_COUNT;

  let status = APPLY_STATUS.PARTIAL;
  if (writesFail > 0 && writesOk === 0) status = APPLY_STATUS.BLOCKED;
  else if (
    writesFail === 0 &&
    verifyOk &&
    roomsOk &&
    otherAddressChanges === 0 &&
    censusAfter.length === EXPECTED_CENSUS_COUNT &&
    (writesOk + idempotentSkips === EXPECTED_APPLY_COUNT ||
      writesOk + writesSkipped + idempotentSkips >= EXPECTED_APPLY_COUNT)
  ) {
    status = APPLY_STATUS.CLEAN;
  } else if (writesFail > 0) {
    status = APPLY_STATUS.PARTIAL;
  }

  return {
    version: CHOICE_ADDRESS_APPLY_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status,
    duration_ms: Date.now() - started,
    run_dir: loaded.runDir,
    approval_bundle: loaded.bundlePath,
    record_set: RECORD_SET_STEWARDED_CHOICE_29,
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
    records_skipped_matching: writesSkipped + idempotentSkips,
    records_failed: writesFail,
    expected_apply_count: EXPECTED_APPLY_COUNT,
    fields_written_union: [...new Set(writeResults.flatMap((w) => w.fields_written || []))],
    write_results: writeResults,
    post_verify: postVerify,
    post_verify_ok: verifyOk,
    rooms_unchanged_ok: roomsOk,
    other_census_address_changes: otherAddressChanges,
    count_contract_ok: countOk,
  };
}

export function renderChoiceAddressApplyMarkdown(report) {
  return [
    `# Production Census — Choice Address Apply`,
    ``,
    `- Status: **${report.status}**`,
    `- Generated: ${report.generated_at}`,
    `- Apply executed: ${report.apply_executed}`,
    `- Records updated: ${report.records_updated}`,
    `- Skipped matching: ${report.records_skipped_matching}`,
    `- Failed: ${report.records_failed}`,
    `- Census count: ${report.census_record_count_after} (expected ${EXPECTED_CENSUS_COUNT})`,
    `- Rooms filled: ${report.rooms_filled_after} (expected ${EXPECTED_ROOMS_FILLED})`,
    `- Other Census address changes: ${report.other_census_address_changes}`,
    `- Brand Explorer writes: ${report.brand_explorer_writes}`,
    `- Brand Setup writes: ${report.brand_setup_writes}`,
    `- Fields written: ${(report.fields_written_union || []).join(", ") || "(none)"}`,
    `- Post-verify OK: ${report.post_verify_ok}`,
    ``,
  ].join("\n");
}

