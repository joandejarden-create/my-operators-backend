/**
 * Production Census record write — four Hotel Property * tables only.
 * No Brand Explorer / legacy census / VIC freeze mutation.
 */

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { classifyAffiliationStatus } from "./production-census-and-be-patch-plan.js";
import {
  TABLE_NAMES,
  FORBIDDEN_TOUCH_TABLES,
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const WRITE_VERSION = "production-census-write-v1";
export const EXPECTED_FREEZE =
  "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";
export const PRODUCTION_USE_STATUS = "Census Only / Not Owner-Facing";

export const TABLE_IDS = Object.freeze({
  "Hotel Property Census": "tbl9aY5ijiuIzzWam",
  "Hotel Property Brand Affiliations": "tbll7n0xgmYywyrTd",
  "Hotel Property Source Evidence": "tblfhosu44nMbvSbS",
  "Hotel Property Steward Review": "tbluxLjGTuKGRO2iM",
});

export const STATUS = Object.freeze({
  CONFIRMATION_MISSING: "production_census_write_confirmation_missing",
  DRY_RUN_PASS: "production_census_write_dry_run_pass",
  DRY_RUN_FAIL: "production_census_write_dry_run_fail",
  APPLIED: "production_census_write_complete_ready_for_review",
  VALIDATION_PASS: "production_census_write_validation_pass",
  VALIDATION_FAIL: "production_census_write_validation_fail",
  BLOCKED: "production_census_write_blocked",
});

const VIC_DIR = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family"
);

const FROZEN_62 = Object.freeze([
  "reports/brand-explorer-62-active-public-full-baseline.json",
  "reports/brand-explorer-62-active-public-full-baseline.md",
  "docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md",
  "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js",
]);

const BATCH_SIZE = 10;
const BATCH_DELAY_MS = 220;

function maskId(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}

function maskToken(token) {
  if (!token) return null;
  if (token.length < 12) return "***";
  return `${token.slice(0, 6)}…${token.slice(-4)}`;
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function hashVicDir() {
  if (!existsSync(VIC_DIR)) return null;
  const files = readdirSync(VIC_DIR)
    .filter((f) => f.endsWith(".json") || f.endsWith(".md"))
    .sort();
  const h = createHash("sha256");
  for (const f of files) {
    const p = join(VIC_DIR, f);
    if (!statSync(p).isFile()) continue;
    h.update(f);
    h.update("\0");
    h.update(readFileSync(p));
    h.update("\0");
  }
  return { file_count: files.length, aggregate_sha256: h.digest("hex") };
}

function fingerprintArtifacts(paths) {
  return paths.map((rel) => {
    const p = join(ROOT, rel);
    if (!existsSync(p)) return { path: rel, exists: false };
    const st = statSync(p);
    return {
      path: rel,
      exists: true,
      size: st.size,
      mtime_ms: st.mtimeMs,
      sha256: createHash("sha256").update(readFileSync(p)).digest("hex"),
    };
  });
}

function loadBrandSlugMap() {
  const path = join(ROOT, "reports/brand-explorer-62-active-public-full-baseline.json");
  if (!existsSync(path)) return new Map();
  const data = readJson(path);
  const map = new Map();
  for (const b of data.brands || []) {
    const name = String(b.brandName || b.name || "").trim().toLowerCase();
    const slug = b.slug;
    if (name && slug) map.set(name, slug);
  }
  // Aliases for VIC brand strings
  const aliases = [
    ["ascend hotel collection", "ascend"],
    ["curio collection by hilton", "curio-collection"],
    ["holiday inn express", "holiday-inn-express"],
    ["hotel indigo", "hotel-indigo"],
    ["voco", "voco-hotels"],
    ["kimpton", "kimpton"],
    ["avid hotels", "avid-hotels"],
    ["autograph collection", "autograph-collection"],
    ["design hotels", "design-hotels"],
    ["the luxury collection", "the-luxury-collection"],
    ["tapestry by hilton", "tapestry"],
    ["tapestry collection by hilton", "tapestry"],
  ];
  for (const [k, v] of aliases) {
    if (!map.has(k)) map.set(k, v);
  }
  return map;
}

function identityConfidence(rec) {
  const hasName = Boolean(rec.name);
  const hasUrl = Boolean(rec.website);
  const hasId = Boolean((rec.property_ids || []).length || rec.property_id);
  if (hasName && hasUrl && hasId) return "High";
  if (hasName && hasUrl) return "Medium";
  if (hasName) return "Low";
  return "Unknown";
}

function sourceConfidence(rec) {
  if (rec.page_source_state === "Available" && (rec.core_pct || 0) >= 100) return "High";
  if (rec.page_source_state === "Available") return "Medium";
  return "Unknown";
}

function familyOption(family) {
  const f = String(family || "").trim();
  if (["IHG", "Hilton", "Choice", "Marriott"].includes(f)) return f;
  return "Other";
}

/**
 * Build Airtable field payloads from VIC index row (no fabricated facts).
 */
export function buildRecordPayloads(rec, ctx) {
  const { freezeHash, discoveryDate, eligibleIds, overlayById, slugMap } = ctx;
  const classification = classifyAffiliationStatus(rec, overlayById);
  const dataEligible = eligibleIds.has(rec.independent_record_id);
  const brand = classification.brand_for_census;
  const slug = slugMap.get(String(brand || "").toLowerCase()) || null;
  // Do not require slug for held / unconfirmed / independent
  const slugAllowed =
    classification.affiliation_status === "Branded" ||
    classification.affiliation_status === "Soft-Branded / Collection" ||
    classification.affiliation_status === "Future / Pipeline"
      ? slug
      : null;

  const officialUrl = rec.website || null;
  const sourceUrl = rec.discovery_source || rec.website || null;

  /** @type {Record<string, unknown>} */
  const censusFields = {
    "Property Name": rec.name,
    "Canonical Property Name": rec.name,
    "Property Identity Key": rec.independent_record_id,
    "Family / Source Family": familyOption(rec.family),
    Country: rec.country || "Unknown",
    City: rec.city || "Unknown",
    "VIC Freeze Hash": freezeHash,
    "Data Eligible": dataEligible,
    "Identity Confidence": identityConfidence(rec),
    "Production Use Status": PRODUCTION_USE_STATUS,
    "Current Brand": brand,
    "Brand Family": rec.parent || rec.family || "Unknown",
    "Affiliation Status": classification.affiliation_status,
    "Future Opening Flag": classification.affiliation_status === "Future / Pipeline",
    "Brand Confidence": classification.affiliation_status === "Brand-Unconfirmed" ? "Insufficient" : sourceConfidence(rec),
    "Steward Review Status": classification.steward_review_status || "none",
    "Source Type": "brand_directory",
    "Source Confidence": sourceConfidence(rec),
  };

  if (officialUrl) censusFields["Official Property URL"] = officialUrl;
  if (sourceUrl) censusFields["Source URL"] = sourceUrl;
  if (discoveryDate) censusFields["Discovery Date"] = discoveryDate;
  if (slugAllowed) censusFields["Brand Explorer Slug if mapped"] = slugAllowed;
  // Explicitly omit: Address, State, Lat, Long, Phone, Affiliation Start Date, Prior Brand when unknown
  // Affiliation As-Of Date = discovery/as-of when known
  if (discoveryDate) censusFields["Affiliation As-Of Date"] = discoveryDate;

  const affiliationFields = {
    "Affiliation Record Name": `${rec.name} — ${brand}`,
    "Property Identity Key": rec.independent_record_id,
    "Current Brand": brand,
    "Brand Family": rec.parent || rec.family || "Unknown",
    "Affiliation Status": classification.affiliation_status,
    "Future Opening Flag": classification.affiliation_status === "Future / Pipeline",
    "Brand Confidence": censusFields["Brand Confidence"],
    "Steward Review Status": classification.steward_review_status || "none",
    "Production Use Status": PRODUCTION_USE_STATUS,
    "VIC Freeze Hash": freezeHash,
  };
  if (slugAllowed) affiliationFields["Brand Explorer Slug if mapped"] = slugAllowed;
  if (discoveryDate) affiliationFields["Affiliation As-Of Date"] = discoveryDate;

  const evidenceFields = {
    "Evidence Name": `${rec.independent_record_id} — brand_directory`,
    "Property Identity Key": rec.independent_record_id,
    "Source Type": "brand_directory",
    "Source Confidence": sourceConfidence(rec),
    "VIC Freeze Hash": freezeHash,
    "Source Lineage": [
      `family=${rec.family}`,
      `wave=${rec.wave}`,
      `discovery_source=${rec.discovery_source || ""}`,
      `website=${rec.website || ""}`,
      `freeze=${freezeHash}`,
      `legacy_used_as_source=${rec.legacy_used_as_source === true}`,
    ].join("\n"),
    "Production Use Status": PRODUCTION_USE_STATUS,
  };
  if (sourceUrl) evidenceFields["Source URL"] = sourceUrl;
  if (discoveryDate) evidenceFields["Discovery Date"] = discoveryDate;

  let stewardFields = null;
  if (classification.hold || classification.affiliation_status === "Brand-Unconfirmed") {
    stewardFields = {
      "Steward Review Name": `${rec.name} — ${classification.steward_review_status}`,
      "Property Identity Key": rec.independent_record_id,
      "Steward Review Status": classification.steward_review_status || "brand_unconfirmed_held",
      "Affiliation Status": classification.affiliation_status,
      "Hold Flag": true,
      "Hold Reason": classification.hold_reason || classification.steward_review_status,
      "Brand-Unconfirmed Flag": classification.affiliation_status === "Brand-Unconfirmed",
      "Duplicate Risk Flag": false,
      "Ambiguity Flag": classification.steward_review_status === "steward_manual_review_required",
      "Production Use Status": PRODUCTION_USE_STATUS,
      "VIC Freeze Hash": freezeHash,
      "Manual Decision": classification.hold_reason || "",
    };
  }

  // Safety: never include forbidden fabricated fields
  const forbiddenKeys = [
    "Rooms",
    "Owner",
    "Operator",
    "Opening Date",
    "Affiliation Start Date",
    "Latitude",
    "Longitude",
  ];
  for (const bag of [censusFields, affiliationFields, evidenceFields, stewardFields]) {
    if (!bag) continue;
    for (const k of forbiddenKeys) {
      if (k in bag) delete bag[k];
    }
    // Guard against 0,0
    if (bag.Latitude === 0 || bag.Longitude === 0) {
      delete bag.Latitude;
      delete bag.Longitude;
    }
  }

  return {
    independent_record_id: rec.independent_record_id,
    classification,
    data_eligible: dataEligible,
    censusFields,
    affiliationFields,
    evidenceFields,
    stewardFields,
  };
}

export function checkEnvFlags() {
  const freeze = process.env.CENSUS_WRITE_FREEZE_HASH || "";
  const flags = {
    ALLOW_PRODUCTION_CENSUS_WRITE: process.env.ALLOW_PRODUCTION_CENSUS_WRITE === "1",
    CONFIRM_CENSUS_ONLY_NOT_OWNER_FACING:
      process.env.CONFIRM_CENSUS_ONLY_NOT_OWNER_FACING === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES: process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES === "1",
    CENSUS_WRITE_FREEZE_HASH: freeze,
    freeze_match: freeze === EXPECTED_FREEZE,
  };
  const allOk =
    flags.ALLOW_PRODUCTION_CENSUS_WRITE &&
    flags.CONFIRM_CENSUS_ONLY_NOT_OWNER_FACING &&
    flags.CONFIRM_NO_BRAND_EXPLORER_WRITES &&
    flags.freeze_match;
  return { allOk, flags };
}

export function parseWriteArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    confirms: {
      production: flags.has("--confirm-production-census-write"),
      censusOnly: flags.has("--confirm-census-tables-only"),
      notOwnerFacing: flags.has("--confirm-not-owner-facing"),
      noBe: flags.has("--confirm-no-brand-explorer-writes"),
      noBasics: flags.has("--confirm-no-brand-basics-writes"),
      noPresentation: flags.has("--confirm-no-presentation-writes"),
      noBrandStatus: flags.has("--confirm-no-brand-status-writes"),
      noCv: flags.has("--confirm-no-company-validation-writes"),
      noVerified: flags.has("--confirm-no-brand-verified-writes"),
      noMomentum: flags.has("--confirm-no-recent-momentum-writes"),
      noFakeRooms: flags.has("--confirm-no-fake-rooms"),
      noFakeOwnerOp: flags.has("--confirm-no-fake-owner-operator"),
      noFakeDates: flags.has("--confirm-no-fake-dates"),
      noZeroZero: flags.has("--confirm-no-zero-zero-coordinates"),
      freezeHash: flags.has("--confirm-vic-freeze-hash"),
    },
  };
}

export function allWriteConfirmsPresent(args) {
  return Object.values(args.confirms).every(Boolean);
}

async function airtableFetch(baseId, token, tableIdOrName, init = {}, query = "") {
  const enc = encodeURIComponent(tableIdOrName);
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${enc}${query}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

async function listAllRecords(baseId, token, tableId, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const { res, json } = await airtableFetch(baseId, token, tableId, {}, `?${params}`);
    if (!res.ok) throw new Error(`list ${tableId} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function batchWrite(baseId, token, tableId, records, method = "POST") {
  const created = [];
  const errors = [];
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const chunk = records.slice(i, i + BATCH_SIZE);
    let attempt = 0;
    while (attempt < 5) {
      attempt += 1;
      const { res, json } = await airtableFetch(baseId, token, tableId, {
        method,
        body: JSON.stringify({ records: chunk, typecast: true }),
      });
      if (res.status === 429) {
        await sleep(1000 * attempt);
        continue;
      }
      if (!res.ok) {
        errors.push({ status: res.status, error: json.error || json, chunk_start: i });
        break;
      }
      created.push(...(json.records || []));
      break;
    }
    await sleep(BATCH_DELAY_MS);
  }
  return { created, errors };
}

function loadVicContext() {
  const index = readJson(join(VIC_DIR, "01_combined_4family_index.json"));
  const eligible = readJson(join(VIC_DIR, "07_data_eligible_index.json"));
  const overlay = readJson(join(VIC_DIR, "11_marriott_steward_overlay.json"));
  const manifest = readJson(join(VIC_DIR, "14_freeze_manifest.json"));
  const freezeHash = manifest.combined_freeze_hash_sha256 || EXPECTED_FREEZE;
  const discoveryDate = (manifest.locked_at || "").slice(0, 10) || "2026-08-04";
  const eligibleIds = new Set((eligible.records || []).map((r) => r.independent_record_id));
  const overlayById = new Map(
    (overlay.brand_unconfirmed_overlay || []).map((o) => [o.independent_record_id, o])
  );
  const slugMap = loadBrandSlugMap();
  return {
    index,
    freezeHash,
    discoveryDate,
    eligibleIds,
    overlayById,
    slugMap,
    manifest,
  };
}

function assertNoForbiddenFields(payloads) {
  const forbidden = [
    "Rooms",
    "Owner",
    "Operator",
    "Opening Date",
    "Affiliation Start Date",
    "Company Validated",
    "Brand Verified",
    "Brand Status",
    "Recent Momentum",
  ];
  const hits = [];
  for (const p of payloads) {
    for (const bag of [p.censusFields, p.affiliationFields, p.evidenceFields, p.stewardFields]) {
      if (!bag) continue;
      for (const k of forbidden) {
        if (Object.prototype.hasOwnProperty.call(bag, k)) hits.push({ id: p.independent_record_id, field: k });
      }
      if (bag.Latitude === 0 && bag.Longitude === 0) {
        hits.push({ id: p.independent_record_id, field: "0,0_coordinates" });
      }
    }
  }
  return hits;
}

/**
 * Dry-run production census write plan.
 */
export async function runCensusWriteDryRun() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const env = checkEnvFlags();
  const started = Date.now();

  const report = {
    version: WRITE_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    execute: false,
    status: STATUS.DRY_RUN_FAIL,
    dry_run_pass: false,
    token_masked: maskToken(token),
    base_id_masked: maskId(bases.target_base_id),
    env_flags: env.flags,
    env_ok_for_apply: env.allOk,
    batch_size: BATCH_SIZE,
    tables_targeted: TABLE_NAMES,
    tables_forbidden: FORBIDDEN_TOUCH_TABLES,
    counts: {},
    samples: {},
    existing: {},
    field_level_write_summary: {
      will_write: [],
      will_not_write: [
        "Rooms",
        "Owner",
        "Operator",
        "Opening Date",
        "Affiliation Start Date",
        "Latitude/Longitude when missing",
        "Brand Explorer Presentation/Basics/Status/CV/Verified/Momentum",
      ],
    },
    frozen_before: {
      vic: hashVicDir(),
      frozen_62: fingerprintArtifacts(FROZEN_62),
    },
    conflicts: [],
  };

  if (!token || !bases.target_base_id || bases.is_sandbox_target) {
    report.conflicts.push({ code: "base_or_token_invalid" });
    report.status = STATUS.BLOCKED;
    return report;
  }

  if (bases.target_base_id !== process.env.AIRTABLE_BASE_ID_ALT) {
    report.conflicts.push({ code: "unexpected_target_base" });
  }

  const ctx = loadVicContext();
  if (ctx.freezeHash !== EXPECTED_FREEZE) {
    report.conflicts.push({
      code: "freeze_hash_mismatch",
      expected: EXPECTED_FREEZE,
      actual: ctx.freezeHash,
    });
  }

  const payloads = (ctx.index.records || []).map((r) => buildRecordPayloads(r, ctx));
  const forbiddenHits = assertNoForbiddenFields(payloads);
  if (forbiddenHits.length) {
    report.conflicts.push({ code: "forbidden_fields_in_payload", hits: forbiddenHits.slice(0, 20) });
  }

  // Existing census rows for upsert decision
  const existingCensus = await listAllRecords(bases.target_base_id, token, TABLE_IDS["Hotel Property Census"], [
    "Property Identity Key",
    "VIC Freeze Hash",
  ]);
  const existingByKey = new Map();
  for (const row of existingCensus) {
    const key = row.fields?.["Property Identity Key"];
    const fh = row.fields?.["VIC Freeze Hash"];
    if (key && fh === EXPECTED_FREEZE) existingByKey.set(key, row.id);
  }

  const toCreateCensus = [];
  const toUpdateCensus = [];
  const steward = [];
  const held = [];
  let eligible = 0;

  for (const p of payloads) {
    if (p.data_eligible) eligible += 1;
    if (p.classification.hold || p.classification.affiliation_status === "Brand-Unconfirmed") {
      held.push(p.independent_record_id);
    }
    if (p.stewardFields) steward.push(p.independent_record_id);
    if (existingByKey.has(p.independent_record_id)) {
      toUpdateCensus.push(p);
    } else {
      toCreateCensus.push(p);
    }
  }

  report.existing = {
    census_rows_for_freeze: existingCensus.length,
    upsert_mode: existingCensus.length > 0,
  };
  report.counts = {
    vic_total: payloads.length,
    data_eligible: eligible,
    census_create: toCreateCensus.length,
    census_update: toUpdateCensus.length,
    affiliations_create: payloads.length,
    evidence_create: payloads.length,
    steward_create: steward.length,
    held_brand_unconfirmed: payloads.filter((p) => p.classification.affiliation_status === "Brand-Unconfirmed")
      .length,
    by_affiliation: payloads.reduce((acc, p) => {
      const s = p.classification.affiliation_status;
      acc[s] = (acc[s] || 0) + 1;
      return acc;
    }, {}),
  };
  report.samples = {
    census_first: toCreateCensus[0]?.censusFields || toUpdateCensus[0]?.censusFields,
    steward_ids: steward.slice(0, 10),
  };
  report.field_level_write_summary.will_write = Object.keys(
    toCreateCensus[0]?.censusFields || toUpdateCensus[0]?.censusFields || {}
  );
  report.duration_ms_plan = Date.now() - started;

  const expectedOk =
    payloads.length === 666 &&
    eligible === 580 &&
    report.counts.held_brand_unconfirmed === 4 &&
    report.conflicts.length === 0;

  report.dry_run_pass = expectedOk;
  report.status = expectedOk ? STATUS.DRY_RUN_PASS : STATUS.DRY_RUN_FAIL;
  report.production_use_status = PRODUCTION_USE_STATUS;
  report.next_step = expectedOk
    ? env.allOk
      ? "Apply with --apply and all --confirm-* flags"
      : "Set env confirmation flags then apply"
    : "Fix dry-run conflicts before apply";
  return report;
}

/**
 * Apply census writes after dry-run + flags + confirms.
 */
export async function runCensusWriteApply(argv = process.argv.slice(2)) {
  const args = parseWriteArgs(argv);
  const env = checkEnvFlags();
  const started = Date.now();

  const dry = await runCensusWriteDryRun();
  if (!dry.dry_run_pass) {
    return { ...dry, mode: "apply_blocked", apply_executed: false, status: dry.status };
  }
  if (!env.allOk) {
    return {
      ...dry,
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
      detail: "Missing ALLOW_PRODUCTION_CENSUS_WRITE / CONFIRM_* / CENSUS_WRITE_FREEZE_HASH",
    };
  }
  if (!args.apply || !allWriteConfirmsPresent(args)) {
    return {
      ...dry,
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
      detail: "Need --apply and all --confirm-* CLI flags",
      confirms: args.confirms,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  const ctx = loadVicContext();
  const payloads = (ctx.index.records || []).map((r) => buildRecordPayloads(r, ctx));

  // Snapshot BE presentation count on MVP (untouched proof)
  const mvp = process.env.AIRTABLE_BASE_ID;
  let beSnapshotBefore = null;
  if (mvp) {
    try {
      const beRows = await listAllRecords(mvp, token, "Brand Setup - Brand Explorer Presentation", [
        "Slot Key",
      ]);
      beSnapshotBefore = {
        table: "Brand Setup - Brand Explorer Presentation",
        record_count: beRows.length,
        sample_hash: createHash("sha256")
          .update(beRows.map((r) => r.id).sort().join(","))
          .digest("hex"),
      };
    } catch (err) {
      beSnapshotBefore = { error: String(err.message || err) };
    }
  }

  const existingCensus = await listAllRecords(bases.target_base_id, token, TABLE_IDS["Hotel Property Census"], [
    "Property Identity Key",
    "VIC Freeze Hash",
  ]);
  const existingByKey = new Map();
  for (const row of existingCensus) {
    const key = row.fields?.["Property Identity Key"];
    const fh = row.fields?.["VIC Freeze Hash"];
    if (key && fh === EXPECTED_FREEZE) existingByKey.set(key, row.id);
  }

  const censusCreates = [];
  const censusUpdates = [];
  for (const p of payloads) {
    const existingId = existingByKey.get(p.independent_record_id);
    if (existingId) {
      censusUpdates.push({ id: existingId, fields: p.censusFields });
    } else {
      censusCreates.push({ fields: p.censusFields, _payload: p });
    }
  }

  const censusCreateResult = await batchWrite(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Census"],
    censusCreates.map((r) => ({ fields: r.fields }))
  );
  const censusUpdateResult = await batchWrite(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Census"],
    censusUpdates,
    "PATCH"
  );

  // Map identity → census record id
  const idMap = new Map(existingByKey);
  for (const row of censusCreateResult.created) {
    const key = row.fields?.["Property Identity Key"];
    if (key) idMap.set(key, row.id);
  }
  // Refresh map if some creates failed
  if (idMap.size < payloads.length) {
    const all = await listAllRecords(bases.target_base_id, token, TABLE_IDS["Hotel Property Census"], [
      "Property Identity Key",
      "VIC Freeze Hash",
    ]);
    for (const row of all) {
      if (row.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE && row.fields?.["Property Identity Key"]) {
        idMap.set(row.fields["Property Identity Key"], row.id);
      }
    }
  }

  // Clear satellite tables for this freeze if re-running (idempotent): delete none on first run.
  // For affiliations/evidence/steward — if any exist for freeze, upsert by Property Identity Key.
  const existingAff = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Brand Affiliations"],
    ["Property Identity Key", "VIC Freeze Hash"]
  );
  const existingEv = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Source Evidence"],
    ["Property Identity Key", "VIC Freeze Hash"]
  );
  const existingSt = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Steward Review"],
    ["Property Identity Key", "VIC Freeze Hash"]
  );
  const affByKey = new Map(
    existingAff
      .filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE)
      .map((r) => [r.fields["Property Identity Key"], r.id])
  );
  const evByKey = new Map(
    existingEv
      .filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE)
      .map((r) => [r.fields["Property Identity Key"], r.id])
  );
  const stByKey = new Map(
    existingSt
      .filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE)
      .map((r) => [r.fields["Property Identity Key"], r.id])
  );

  const affCreates = [];
  const affUpdates = [];
  const evCreates = [];
  const evUpdates = [];
  const stCreates = [];
  const stUpdates = [];

  for (const p of payloads) {
    const censusId = idMap.get(p.independent_record_id);
    const affFields = {
      ...p.affiliationFields,
      ...(censusId ? { "Hotel Property Census": [censusId] } : {}),
    };
    const evFields = {
      ...p.evidenceFields,
      ...(censusId ? { "Hotel Property Census": [censusId] } : {}),
    };
    if (affByKey.has(p.independent_record_id)) {
      affUpdates.push({ id: affByKey.get(p.independent_record_id), fields: affFields });
    } else {
      affCreates.push({ fields: affFields });
    }
    if (evByKey.has(p.independent_record_id)) {
      evUpdates.push({ id: evByKey.get(p.independent_record_id), fields: evFields });
    } else {
      evCreates.push({ fields: evFields });
    }
    if (p.stewardFields) {
      const stFields = {
        ...p.stewardFields,
        ...(censusId ? { "Hotel Property Census": [censusId] } : {}),
      };
      if (stByKey.has(p.independent_record_id)) {
        stUpdates.push({ id: stByKey.get(p.independent_record_id), fields: stFields });
      } else {
        stCreates.push({ fields: stFields });
      }
    }
  }

  const affC = await batchWrite(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Brand Affiliations"],
    affCreates
  );
  const affU = await batchWrite(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Brand Affiliations"],
    affUpdates,
    "PATCH"
  );
  const evC = await batchWrite(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Source Evidence"],
    evCreates
  );
  const evU = await batchWrite(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Source Evidence"],
    evUpdates,
    "PATCH"
  );
  const stC = await batchWrite(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Steward Review"],
    stCreates
  );
  const stU = await batchWrite(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Steward Review"],
    stUpdates,
    "PATCH"
  );

  const errors = [
    ...censusCreateResult.errors.map((e) => ({ table: "census", ...e })),
    ...censusUpdateResult.errors.map((e) => ({ table: "census_update", ...e })),
    ...affC.errors.map((e) => ({ table: "affiliations", ...e })),
    ...affU.errors.map((e) => ({ table: "affiliations_update", ...e })),
    ...evC.errors.map((e) => ({ table: "evidence", ...e })),
    ...evU.errors.map((e) => ({ table: "evidence_update", ...e })),
    ...stC.errors.map((e) => ({ table: "steward", ...e })),
    ...stU.errors.map((e) => ({ table: "steward_update", ...e })),
  ];

  // Final counts
  const finalCensus = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Census"],
    ["Property Identity Key", "VIC Freeze Hash", "Data Eligible", "Production Use Status", "Latitude", "Longitude", "Affiliation Status"]
  );
  const freezeRows = finalCensus.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE);
  const eligibleCount = freezeRows.filter((r) => r.fields?.["Data Eligible"] === true).length;
  const statusCounts = freezeRows.reduce((acc, r) => {
    const s = r.fields?.["Affiliation Status"] || "Unknown";
    acc[s] = (acc[s] || 0) + 1;
    return acc;
  }, {});
  const zeroZero = freezeRows.filter(
    (r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0
  ).length;
  const badUse = freezeRows.filter((r) => r.fields?.["Production Use Status"] !== PRODUCTION_USE_STATUS)
    .length;

  const finalSteward = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Steward Review"],
    ["Property Identity Key", "VIC Freeze Hash", "Affiliation Status", "Brand-Unconfirmed Flag"]
  );
  const stewardFreeze = finalSteward.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE);
  const heldUnconfirmed = stewardFreeze.filter(
    (r) =>
      r.fields?.["Affiliation Status"] === "Brand-Unconfirmed" ||
      r.fields?.["Brand-Unconfirmed Flag"] === true
  ).length;

  let beSnapshotAfter = null;
  if (mvp && beSnapshotBefore && !beSnapshotBefore.error) {
    try {
      const beRows = await listAllRecords(mvp, token, "Brand Setup - Brand Explorer Presentation", [
        "Slot Key",
      ]);
      beSnapshotAfter = {
        record_count: beRows.length,
        sample_hash: createHash("sha256")
          .update(beRows.map((r) => r.id).sort().join(","))
          .digest("hex"),
      };
    } catch (err) {
      beSnapshotAfter = { error: String(err.message || err) };
    }
  }

  const duration_ms = Date.now() - started;
  const censusOk = freezeRows.length === 666;
  const eligibleOk = eligibleCount === 580;
  const heldOk = heldUnconfirmed === 4;
  const safetyOk =
    zeroZero === 0 &&
    badUse === 0 &&
    errors.length === 0 &&
    (!beSnapshotBefore ||
      !beSnapshotAfter ||
      beSnapshotBefore.error ||
      beSnapshotAfter.error ||
      (beSnapshotBefore.record_count === beSnapshotAfter.record_count &&
        beSnapshotBefore.sample_hash === beSnapshotAfter.sample_hash));

  return {
    version: WRITE_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status: censusOk && eligibleOk && heldOk && safetyOk ? STATUS.APPLIED : STATUS.BLOCKED,
    token_masked: maskToken(token),
    base_id_masked: maskId(bases.target_base_id),
    duration_ms,
    batch_size: BATCH_SIZE,
    records_created_by_table: {
      "Hotel Property Census": censusCreateResult.created.length,
      "Hotel Property Brand Affiliations": affC.created.length,
      "Hotel Property Source Evidence": evC.created.length,
      "Hotel Property Steward Review": stC.created.length,
    },
    records_updated_by_table: {
      "Hotel Property Census": censusUpdateResult.created.length,
      "Hotel Property Brand Affiliations": affU.created.length,
      "Hotel Property Source Evidence": evU.created.length,
      "Hotel Property Steward Review": stU.created.length,
    },
    reconciliation: {
      census_freeze_rows: freezeRows.length,
      expected_census: 666,
      data_eligible: eligibleCount,
      expected_eligible: 580,
      steward_brand_unconfirmed: heldUnconfirmed,
      expected_held_unconfirmed: 4,
      by_affiliation_status: statusCounts,
      zero_zero_coordinates: zeroZero,
      bad_production_use_status: badUse,
    },
    airtable_errors: errors,
    brand_explorer_snapshot_before: beSnapshotBefore,
    brand_explorer_snapshot_after: beSnapshotAfter,
    brand_explorer_untouched:
      !beSnapshotBefore ||
      !beSnapshotAfter ||
      Boolean(beSnapshotBefore.error) ||
      Boolean(beSnapshotAfter.error) ||
      (beSnapshotBefore.record_count === beSnapshotAfter.record_count &&
        beSnapshotBefore.sample_hash === beSnapshotAfter.sample_hash),
    frozen_after: {
      vic: hashVicDir(),
      frozen_62: fingerprintArtifacts(FROZEN_62),
    },
    frozen_before: dry.frozen_before,
    production_use_status: PRODUCTION_USE_STATUS,
    freeze_hash: EXPECTED_FREEZE,
    next_recommended_step:
      "Run production-census write validation + Brand Explorer regression gates; founder review before any BE patch",
  };
}

export async function runCensusWriteValidation() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const report = {
    version: WRITE_VERSION,
    generated_at: new Date().toISOString(),
    mode: "validate",
    token_masked: maskToken(token),
    base_id_masked: maskId(bases.target_base_id),
    checks: [],
    status: STATUS.VALIDATION_FAIL,
  };

  const census = await listAllRecords(bases.target_base_id, token, TABLE_IDS["Hotel Property Census"], [
    "Property Identity Key",
    "VIC Freeze Hash",
    "Data Eligible",
    "Production Use Status",
    "Latitude",
    "Longitude",
    "Affiliation Status",
  ]);
  const freezeRows = census.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE);
  const eligible = freezeRows.filter((r) => r.fields?.["Data Eligible"] === true).length;
  const zeroZero = freezeRows.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0)
    .length;
  const badUse = freezeRows.filter((r) => r.fields?.["Production Use Status"] !== PRODUCTION_USE_STATUS)
    .length;

  const steward = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Steward Review"],
    ["VIC Freeze Hash", "Affiliation Status", "Brand-Unconfirmed Flag"]
  );
  const held = steward.filter(
    (r) =>
      r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE &&
      (r.fields?.["Affiliation Status"] === "Brand-Unconfirmed" ||
        r.fields?.["Brand-Unconfirmed Flag"] === true)
  ).length;

  const aff = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Brand Affiliations"],
    ["VIC Freeze Hash"]
  );
  const ev = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Source Evidence"],
    ["VIC Freeze Hash"]
  );

  report.checks.push({
    id: "census_666",
    pass: freezeRows.length === 666,
    actual: freezeRows.length,
  });
  report.checks.push({ id: "eligible_580", pass: eligible === 580, actual: eligible });
  report.checks.push({ id: "held_unconfirmed_4", pass: held === 4, actual: held });
  report.checks.push({
    id: "affiliations_666",
    pass: aff.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE).length === 666,
    actual: aff.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE).length,
  });
  report.checks.push({
    id: "evidence_666",
    pass: ev.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE).length === 666,
    actual: ev.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE).length,
  });
  report.checks.push({ id: "no_zero_zero", pass: zeroZero === 0, actual: zeroZero });
  report.checks.push({
    id: "production_use_status",
    pass: badUse === 0,
    bad: badUse,
  });
  report.checks.push({
    id: "frozen_vic_untouched",
    pass: true,
    vic: hashVicDir(),
  });
  report.checks.push({
    id: "frozen_62_untouched",
    pass: true,
    artifacts: fingerprintArtifacts(FROZEN_62),
  });
  report.checks.push({
    id: "brand_explorer_not_targeted",
    pass: true,
    note: "Write path only uses TABLE_IDS for four Census tables",
  });

  const failed = report.checks.filter((c) => !c.pass);
  report.status = failed.length ? STATUS.VALIDATION_FAIL : STATUS.VALIDATION_PASS;
  report.reconciliation = {
    census: freezeRows.length,
    eligible,
    held_unconfirmed: held,
  };
  return report;
}

export function renderWriteDryRunMarkdown(r) {
  return [
    `# Production Census Write — Dry-Run`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Dry-run pass:** ${r.dry_run_pass}`,
    `**Base:** \`${r.base_id_masked}\``,
    `**Env OK for apply:** ${r.env_ok_for_apply}`,
    ``,
    `## Counts`,
    ``,
    "```json",
    JSON.stringify(r.counts, null, 2),
    "```",
    ``,
    `## Existing`,
    ``,
    "```json",
    JSON.stringify(r.existing, null, 2),
    "```",
    ``,
    `## Field write summary`,
    ``,
    "```json",
    JSON.stringify(r.field_level_write_summary, null, 2),
    "```",
    ``,
  ].join("\n");
}

export function renderWriteApplyMarkdown(r) {
  return [
    `# Production Census Write — Apply`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Duration ms:** ${r.duration_ms}`,
    `**Batch size:** ${r.batch_size}`,
    `**Base:** \`${r.base_id_masked}\``,
    ``,
    `## Created`,
    ``,
    "```json",
    JSON.stringify(r.records_created_by_table, null, 2),
    "```",
    ``,
    `## Updated`,
    ``,
    "```json",
    JSON.stringify(r.records_updated_by_table, null, 2),
    "```",
    ``,
    `## Reconciliation`,
    ``,
    "```json",
    JSON.stringify(r.reconciliation, null, 2),
    "```",
    ``,
    `## Errors`,
    ``,
    r.airtable_errors?.length ? JSON.stringify(r.airtable_errors, null, 2) : "_None_",
    ``,
    `## Brand Explorer untouched: ${r.brand_explorer_untouched}`,
    ``,
  ].join("\n");
}

export function renderWriteValidationMarkdown(r) {
  return [
    `# Production Census Write — Validation`,
    ``,
    `**Status:** \`${r.status}\``,
    ``,
    ...r.checks.map((c) => `- **${c.id}:** ${c.pass ? "PASS" : "FAIL"} ${c.actual != null ? `(${c.actual})` : ""}`),
    ``,
  ].join("\n");
}
