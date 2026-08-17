/**
 * Full CALA 15K shell format + source/candidate-brand backfill v1.
 * Patches DR/CR/PA shell inserts only. Never Current Brand from unvalidated Cvent.
 *
 * Objective: full-cala-15k-shell-format-source-brand-backfill-v1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import { normName } from "./census-autopilot-v2/identity-dedupe.js";
import {
  buildCanonicalBrandDictionary,
  lookupCanonicalBrand,
} from "./census-brand-canonical-dictionary.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const SHELL_FORMAT_BACKFILL_OBJECTIVE =
  "full-cala-15k-shell-format-source-brand-backfill-v1";
export const SHELL_FORMAT_BACKFILL_VERSION =
  "full-cala-15k-shell-format-source-brand-backfill-v1";

export const SHELL_FORMAT_BACKFILL_STATUS = Object.freeze({
  COMPLETE:
    "production_census_full_cala_15k_shell_format_source_brand_backfill_v1_complete",
  PARTIAL_SCHEMA:
    "production_census_full_cala_15k_shell_format_source_brand_backfill_v1_partial_schema_remaining",
  PARTIAL_BRAND:
    "production_census_full_cala_15k_shell_format_source_brand_backfill_v1_partial_brand_validation_needed",
  BLOCKED:
    "production_census_full_cala_15k_shell_format_source_brand_backfill_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] ||
  productionHotelPropertyCensus.tableId ||
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const DEFAULT_COUNTRIES = Object.freeze([
  "Dominican Republic",
  "Costa Rica",
  "Panama",
]);

const SHELL_BATCH_ID = "full-cala-15k-census-shell-insert-v1";

/** Known hotel/brand phrases — longest first for protection. */
const HOTEL_PHRASE_CASING = Object.freeze([
  "Grand Fiesta Americana",
  "Fiesta Americana",
  "Live Aqua",
  "Camino Real",
  "Quinta Real",
  "One Hotels",
  "Krystal Grand",
  "Hoteles Misión",
  "Hilton Garden Inn",
  "Hampton by Hilton",
  "Homewood Suites",
  "Holiday Inn Express",
  "Holiday Inn",
  "Crowne Plaza",
  "Hotel Indigo",
  "Iberostar Selection",
  "Iberostar Waves",
  "Hyatt Centric",
  "Hyatt Place",
  "City Express",
  "Casa de Campo",
  "Radisson RED",
  "InterContinental",
  "AC Hotel",
  "JW Marriott",
  "B&B",
  "Krystal",
  "Emporio",
  "Posadas",
  "Gamma",
]);

const HOTEL_TOKEN_CASING = Object.freeze({
  ac: "AC",
  jw: "JW",
  w: "W",
  trs: "TRS",
  nh: "NH",
  ihg: "IHG",
  riu: "RIU",
  amr: "AMR",
  ghl: "GHL",
  h10: "H10",
  melia: "Meliá",
  "meliá": "Meliá",
  barcelo: "Barceló",
  "barceló": "Barceló",
  secrets: "Secrets",
  dreams: "Dreams",
  breathless: "Breathless",
  "zoetry": "Zoëtry",
  "zoëtry": "Zoëtry",
  hyatt: "Hyatt",
  hilton: "Hilton",
  marriott: "Marriott",
  radisson: "Radisson",
  iberostar: "Iberostar",
  indigo: "Indigo",
});

const SMALL_WORDS = new Set([
  "a",
  "an",
  "and",
  "at",
  "by",
  "de",
  "del",
  "la",
  "las",
  "los",
  "of",
  "on",
  "the",
  "y",
  "e",
]);

const FORBIDDEN = new Set([
  "Rooms / Keys",
  "Latitude",
  "Longitude",
  "Owner Name",
  "Operator / Management Company",
  "Developer Name",
  "Opening Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
  "Brand Status",
  "Current Brand",
  "Brand Family",
  "Family / Source Family",
  "Public Display Review Status",
  "Radar Display Status",
  "Production Use Status",
]);

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}
function singleSelect(name, optionNames, description) {
  return {
    name,
    type: "singleSelect",
    description,
    options: { choices: choices(optionNames) },
  };
}
function text(name, description) {
  return { name, type: "singleLineText", description };
}
function longText(name, description) {
  return { name, type: "multilineText", description };
}
function numberField(name, description) {
  return {
    name,
    type: "number",
    description,
    options: { precision: 0 },
  };
}
function dateField(name, description) {
  return {
    name,
    type: "date",
    description,
    options: { dateFormat: { name: "iso" } },
  };
}

export function buildShellBackfillFieldSpecs() {
  return [
    singleSelect(
      "Discovery Source",
      [
        "Cvent Candidate / Not Field Source",
        "HBX Content API",
        "Cvent + HBX Candidate",
        "Independent Census Candidate",
      ],
      "Shell discovery provenance — not field-level SoT"
    ),
    singleSelect(
      "Source Candidate Type",
      [
        "Shell Identity",
        "HBX Linked Shell",
        "Cvent Identity Candidate",
        "Multi-Source Candidate",
      ],
      "Shell candidate classification"
    ),
    numberField("Candidate Source Count", "Distinct candidate sources contributing to shell"),
    singleSelect(
      "Review Status",
      ["Needs Review", "Internal Only", "Approved", "Hold"],
      "Shell / candidate review gate"
    ),
    text("Shell Insert Batch ID", "full-cala-15k-census-shell-insert-v1"),
    singleSelect(
      "Shell Insert Country Batch",
      ["Dominican Republic", "Costa Rica", "Panama", "Colombia", "Mexico", "Other"],
      "Country batch that inserted this shell"
    ),
    dateField("Shell Insert Date", "Date shell was inserted"),
    text("Shell Insert Source Mix", "Source mix label for the shell"),
    singleSelect(
      "Shell Dedupe Confidence",
      ["High", "Medium", "Review Needed"],
      "Dedupe confidence at shell insert"
    ),
    text("Candidate Brand Text", "Unvalidated brand text from candidate sources"),
    text("Candidate Brand Family", "Unvalidated / dictionary-inferred family signal"),
    singleSelect(
      "Candidate Brand Source",
      [
        "Cvent Candidate / Not Field Source",
        "HBX Content API",
        "Cvent + HBX Candidate",
        "Brand Dictionary Inference",
        "Independent Census Candidate",
      ],
      "Where Candidate Brand Text came from"
    ),
    singleSelect(
      "Candidate Brand Confidence",
      ["Candidate", "Medium", "High", "Low"],
      "Confidence of candidate brand signal"
    ),
    singleSelect(
      "Brand Validation Status",
      [
        "Unvalidated / Needs Review",
        "Needs Review",
        "Validated",
        "Conflict",
        "Independent",
      ],
      "Whether Current Brand may be populated"
    ),
  ];
}

export const SHELL_BACKFILL_FIELD_NAMES = Object.freeze(
  buildShellBackfillFieldSpecs().map((s) => s.name)
);

const ALLOWED_WRITE = new Set([
  "Canonical Property Name",
  "Last Reviewed Date",
  "HBX Hotel Code",
  "HBX Chain Code",
  "HBX Category Code",
  "HBX Linkage Confidence",
  ...SHELL_BACKFILL_FIELD_NAMES,
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}
function isBlank(v) {
  return v == null || !String(v).trim();
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Family / Source investigation — existing field is Family / Source Family.
 */
export function investigateFamilySourceField() {
  return {
    field_name_requested: "Family / Source",
    field_name_actual: "Family / Source Family",
    type: "singleSelect",
    semantics:
      "Brand / source-family affiliation options (IHG, Hilton, Marriott, Independent, etc.) — NOT discovery provenance.",
    current_usage:
      "Used as Brand Family / parent-company family signal alongside Brand Family in Census Autopilot.",
    recommended_handling:
      "Do not backfill from unvalidated Cvent/HBX chain text. Use Candidate Brand Family for unvalidated signals. Leave Family / Source Family blank until validated.",
    backfill_this_mission: false,
  };
}

/**
 * Smart hotel proper case — preserves known brand phrases/acronyms.
 */
export function toSmartHotelProperCase(raw) {
  const input = String(raw || "").replace(/\s+/g, " ").trim();
  if (!input) return null;

  // Already mixed-case and not ALL CAPS / all lower — still normalize tokens carefully
  let working = input;
  const placeholders = [];
  const phrases = [...HOTEL_PHRASE_CASING].sort((a, b) => b.length - a.length);
  for (const phrase of phrases) {
    const re = new RegExp(phrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "ig");
    working = working.replace(re, () => {
      const token = `__PH${placeholders.length}__`;
      placeholders.push(phrase);
      return token;
    });
  }

  const words = working.split(" ");
  const out = words.map((w, i) => {
    const ph = w.match(/^__PH(\d+)__$/);
    if (ph) return placeholders[Number(ph[1])];

    const bare = w.replace(/^[^A-Za-z0-9À-ÿ]+|[^A-Za-z0-9À-ÿ]+$/g, "");
    const lower = bare.toLowerCase();
    if (HOTEL_TOKEN_CASING[lower]) {
      return w.replace(bare, HOTEL_TOKEN_CASING[lower]);
    }
    if (i > 0 && SMALL_WORDS.has(lower) && bare === w) {
      return lower;
    }
    if (!bare) return w;
    // Preserve internal accents; title-case first letter only
    const titled = bare.charAt(0).toUpperCase() + bare.slice(1).toLowerCase();
    // Restore known accented forms if dictionary has them
    const restored = HOTEL_TOKEN_CASING[titled.toLowerCase()] || titled;
    return w.replace(bare, restored);
  });

  return out.join(" ").replace(/\s+/g, " ").trim();
}

export function resolveShellFormatBackfillGates(env = process.env) {
  const flag = (k) => String(env[k] || "0").trim() === "1";
  const blockers = [];
  if (flag("ENABLE_CURRENT_BRAND_WRITES")) {
    blockers.push("ENABLE_CURRENT_BRAND_WRITES_must_be_0");
  }
  if (flag("ENABLE_ROOMS_WRITES")) blockers.push("ENABLE_ROOMS_WRITES_must_be_0");
  if (flag("ENABLE_OWNER_OPERATOR_WRITES")) {
    blockers.push("ENABLE_OWNER_OPERATOR_WRITES_must_be_0");
  }
  if (flag("ENABLE_DATE_WRITES")) blockers.push("ENABLE_DATE_WRITES_must_be_0");
  if (flag("ENABLE_COORDINATE_WRITES")) {
    blockers.push("ENABLE_COORDINATE_WRITES_must_be_0");
  }
  if (flag("ENABLE_PUBLIC_DISPLAY_WRITES")) {
    blockers.push("ENABLE_PUBLIC_DISPLAY_WRITES_must_be_0");
  }
  return {
    ok: blockers.length === 0,
    blockers,
    shell_mission: flag("ENABLE_FULL_CALA_15K_CENSUS_SHELL"),
    format_backfill: flag("ENABLE_CENSUS_SHELL_FORMAT_BACKFILL"),
    candidate_brand: flag("ENABLE_CANDIDATE_BRAND_FIELDS"),
    current_brand: false,
  };
}

async function metaFetch(baseId, token, pathAndQuery, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${pathAndQuery}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const textBody = await res.text();
  let json;
  try {
    json = textBody ? JSON.parse(textBody) : {};
  } catch {
    json = { raw: textBody };
  }
  return { res, json };
}

async function listCensusTable(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`meta_tables_failed:${res.status}`);
  const table = (json.tables || []).find(
    (t) => t.id === CENSUS_TABLE_ID || t.name === "Hotel Property Census"
  );
  if (!table) throw new Error("hotel_property_census_not_found");
  return table;
}

export async function ensureShellBackfillSchemaFields({
  baseId,
  token,
  apply = false,
  log = () => {},
} = {}) {
  const table = await listCensusTable(baseId, token);
  const existingNames = new Set((table.fields || []).map((f) => f.name));
  const specs = buildShellBackfillFieldSpecs();
  const found = [];
  const toCreate = [];
  for (const spec of specs) {
    if (existingNames.has(spec.name)) found.push(spec.name);
    else toCreate.push(spec);
  }

  const created = [];
  const errors = [];
  const manual_instructions = [];

  if (!apply) {
    for (const spec of toCreate) {
      manual_instructions.push({
        table: "Hotel Property Census",
        field: spec.name,
        type: spec.type,
        options: spec.options?.choices?.map((c) => c.name) || null,
        description: spec.description || null,
      });
    }
    return {
      found,
      missing: toCreate.map((s) => s.name),
      created,
      errors,
      dry_run: true,
      manual_instructions,
    };
  }

  for (const spec of toCreate) {
    let ok = false;
    for (let attempt = 1; attempt <= 4 && !ok; attempt += 1) {
      const body = {
        name: spec.name,
        type: spec.type,
        description: spec.description,
        ...(spec.options ? { options: spec.options } : {}),
      };
      const { res, json } = await metaFetch(
        baseId,
        token,
        `/tables/${encodeURIComponent(table.id)}/fields`,
        { method: "POST", body: JSON.stringify(body) }
      );
      if (res.status === 429) {
        await sleep(1000 * attempt);
        continue;
      }
      if (!res.ok) {
        errors.push({ name: spec.name, status: res.status, error: json.error || json });
        manual_instructions.push({
          table: "Hotel Property Census",
          field: spec.name,
          type: spec.type,
          options: spec.options?.choices?.map((c) => c.name) || null,
          reason: `create_failed_${res.status}`,
        });
        break;
      }
      created.push({ name: spec.name, type: spec.type, id: json.id });
      existingNames.add(spec.name);
      log(`[shell-backfill] created ${spec.name}`);
      ok = true;
    }
    await sleep(280);
  }

  return {
    found,
    missing: toCreate.map((s) => s.name).filter((n) => !created.some((c) => c.name === n)),
    created,
    errors,
    dry_run: false,
    manual_instructions,
  };
}

export function parseNotes(notes) {
  const s = String(notes || "");
  const sources = [];
  const mSrc = s.match(/sources=([^\n]+)/i);
  if (mSrc) sources.push(...mSrc[1].split(",").map((x) => x.trim()).filter(Boolean));
  const mCode = s.match(/hotel_code=(\d+)/i);
  const mChain = s.match(/chain_text=([^\n]+)/i);
  return {
    sources,
    hbx_code: mCode ? Number(mCode[1]) : null,
    chain_text: mChain ? mChain[1].trim() : null,
    is_cvent: /Cvent Candidate/i.test(s) || sources.includes("cvent_candidate"),
    is_hbx:
      /hbx_content_api/i.test(s) ||
      sources.includes("hbx_content_api") ||
      Boolean(mCode),
    is_shell_marker:
      /dedupe_class_pending_insert/i.test(s) ||
      /Candidate identity only/i.test(s) ||
      /hbx_linkage/i.test(s),
  };
}

function loadCandidateIndexes() {
  const byHbx = new Map();
  const byNameCountry = new Map();

  const hbxPath = path.join(
    ROOT,
    "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"
  );
  if (fs.existsSync(hbxPath)) {
    const j = JSON.parse(fs.readFileSync(hbxPath, "utf8"));
    for (const c of j.candidates || []) {
      const row = {
        source_type: "hbx_content_api",
        brand_text: null,
        chain_text: c.chain_code || null,
        category: c.category || null,
        hbx_code: c.hbx_hotel_code,
        name: c.name,
        country: c.country,
      };
      if (c.hbx_hotel_code != null) byHbx.set(Number(c.hbx_hotel_code), row);
      const key = `${normName(c.name)}|${normName(c.country)}`;
      if (!byNameCountry.has(key)) byNameCountry.set(key, row);
    }
  }

  const dir = path.join(
    ROOT,
    "data/research-engine-v2/census-autopilot-v2-full-universe/candidates"
  );
  if (fs.existsSync(dir)) {
    for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
      const raw = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
      const arr = Array.isArray(raw) ? raw : raw.candidates || raw.records || [];
      for (const c of arr) {
        const name = c.origin_name || c.name;
        const country = c.origin_country || c.country;
        if (!name || !country) continue;
        const row = {
          source_type:
            c.candidate_origin === "CVENT_CHALLENGE"
              ? "cvent_candidate"
              : "independent_discovery",
          brand_text: c.brand || null,
          chain_text: c.family || null,
          category: null,
          hbx_code: null,
          name,
          country,
        };
        const key = `${normName(name)}|${normName(country)}`;
        const existing = byNameCountry.get(key);
        if (!existing) byNameCountry.set(key, row);
        else if (existing.source_type === "hbx_content_api" && row.brand_text) {
          existing.brand_text = existing.brand_text || row.brand_text;
          existing.merged_cvent = true;
        } else if (!existing.brand_text && row.brand_text) {
          existing.brand_text = row.brand_text;
        }
      }
    }
  }

  return { byHbx, byNameCountry };
}

function resolveCandidateForRecord(fields, notesMeta, indexes) {
  const hbx =
    fields["HBX Hotel Code"] != null && String(fields["HBX Hotel Code"]).trim()
      ? Number(fields["HBX Hotel Code"])
      : notesMeta.hbx_code;
  if (hbx != null && indexes.byHbx.has(hbx)) {
    return { ...indexes.byHbx.get(hbx), match_via: "hbx_hotel_code" };
  }
  const key = `${normName(fields["Property Name"] || fields["Canonical Property Name"])}|${normName(fields.Country)}`;
  if (indexes.byNameCountry.has(key)) {
    return { ...indexes.byNameCountry.get(key), match_via: "name_country" };
  }
  return null;
}

/**
 * Build patch for one shell record.
 */
export function buildShellBackfillPatch(fields, opts = {}) {
  const {
    fieldSet = new Set(),
    candidate = null,
    notesMeta = {},
    allowCandidateBrand = true,
    countryBatch = null,
    brandDictionary = null,
  } = opts;

  const patch = {};
  const skipped = [];
  const conflicts = [];

  const setIf = (field, value, { blankOnly = true } = {}) => {
    if (!fieldSet.has(field) && !ALLOWED_WRITE.has(field)) {
      skipped.push({ field, reason: "not_allowed" });
      return;
    }
    if (!fieldSet.has(field)) {
      skipped.push({ field, reason: "schema_missing" });
      return;
    }
    if (value == null || value === "") {
      skipped.push({ field, reason: "no_value" });
      return;
    }
    if (FORBIDDEN.has(field) || isForbiddenAutopilotField(field)) {
      skipped.push({ field, reason: "forbidden" });
      return;
    }
    if (!ALLOWED_WRITE.has(field)) {
      skipped.push({ field, reason: "not_in_allowlist" });
      return;
    }
    const existing = fields[field];
    if (blankOnly && !isBlank(existing)) {
      if (String(existing).trim() === String(value).trim()) {
        skipped.push({ field, reason: "already_same" });
      } else if (field === "Canonical Property Name") {
        // Allow casing-only upgrade when compare-equal ignoring case
        if (
          String(existing).trim().toLowerCase() === String(value).trim().toLowerCase()
        ) {
          patch[field] = value;
        } else {
          conflicts.push({ field, existing, candidate: value });
        }
      } else {
        skipped.push({ field, reason: "already_populated" });
      }
      return;
    }
    patch[field] = value;
  };

  // 1) Canonical proper case — Property Name preserved
  const prop = String(fields["Property Name"] || "").trim();
  const canonExisting = String(fields["Canonical Property Name"] || "").trim();
  const sourceForCanon = prop || canonExisting;
  const proper = toSmartHotelProperCase(sourceForCanon);
  if (proper && proper !== canonExisting) {
    setIf("Canonical Property Name", proper, { blankOnly: false });
  }

  // 2) Source provenance
  const sources = new Set([
    ...(notesMeta.sources || []),
    ...(candidate?.source_type ? [candidate.source_type] : []),
  ]);
  if (notesMeta.is_cvent) sources.add("cvent_candidate");
  if (notesMeta.is_hbx || fields["HBX Hotel Code"]) sources.add("hbx_content_api");

  const hasCvent = [...sources].some((s) => /cvent/i.test(s));
  const hasHbx = [...sources].some((s) => /hbx/i.test(s)) || Boolean(fields["HBX Hotel Code"]);
  const hasIndependent = [...sources].some((s) => /independent/i.test(s));

  let discoverySource = "Independent Census Candidate";
  if (hasCvent && hasHbx) discoverySource = "Cvent + HBX Candidate";
  else if (hasHbx) discoverySource = "HBX Content API";
  else if (hasCvent) discoverySource = "Cvent Candidate / Not Field Source";

  let sourceCandidateType = "Shell Identity";
  if (hasCvent && hasHbx) sourceCandidateType = "Multi-Source Candidate";
  else if (hasHbx) sourceCandidateType = "HBX Linked Shell";
  else if (hasCvent) sourceCandidateType = "Cvent Identity Candidate";

  const sourceCount = [hasCvent, hasHbx, hasIndependent].filter(Boolean).length || 1;

  setIf("Discovery Source", discoverySource);
  setIf("Source Candidate Type", sourceCandidateType);
  setIf("Candidate Source Count", sourceCount);
  setIf("Review Status", "Internal Only");
  setIf("Shell Insert Batch ID", SHELL_BATCH_ID);
  setIf(
    "Shell Insert Country Batch",
    countryBatch || fields.Country || null
  );
  setIf("Shell Insert Date", todayIsoDate());
  setIf(
    "Shell Insert Source Mix",
    [...sources].sort().join("+") || discoverySource
  );
  setIf(
    "Shell Dedupe Confidence",
    hasHbx ? "High" : hasCvent ? "Medium" : "Review Needed"
  );

  // HBX identity blanks only
  if (notesMeta.hbx_code != null) {
    setIf("HBX Hotel Code", String(notesMeta.hbx_code));
  }
  const chain =
    fields["HBX Chain Code"] ||
    notesMeta.chain_text ||
    candidate?.chain_text ||
    null;
  if (chain) setIf("HBX Chain Code", String(chain).trim());
  if (candidate?.category) setIf("HBX Category Code", String(candidate.category).trim());
  if (hasHbx) setIf("HBX Linkage Confidence", "High");

  // 3) Candidate brand — never Current Brand / Brand Family / Family / Source Family
  if (allowCandidateBrand) {
    let brandText = candidate?.brand_text || null;
    let brandSource = null;
    let brandConfidence = "Candidate";
    let validationStatus = "Unvalidated / Needs Review";
    let familyText = null;

    if (hasHbx && (candidate?.chain_text || notesMeta.chain_text || fields["HBX Chain Code"])) {
      const hbxChain = String(
        candidate?.chain_text || notesMeta.chain_text || fields["HBX Chain Code"]
      ).trim();
      if (!brandText) brandText = hbxChain;
      brandSource = hasCvent && candidate?.brand_text
        ? "Cvent + HBX Candidate"
        : "HBX Content API";
      brandConfidence = "Medium";
      validationStatus = "Needs Review";
    } else if (hasCvent && candidate?.brand_text) {
      brandText = candidate.brand_text;
      brandSource = "Cvent Candidate / Not Field Source";
      brandConfidence = "Candidate";
      validationStatus = "Unvalidated / Needs Review";
    }

    if (brandText) {
      if (brandDictionary) {
        const looked = lookupCanonicalBrand(brandText, brandDictionary, {
          propertyName: prop,
        });
        if (looked?.ok && looked.entry) {
          familyText =
            looked.entry.brand_family ||
            looked.entry.parent_company ||
            looked.entry.family ||
            null;
        }
      }
    }

    if (brandText) {
      setIf("Candidate Brand Text", String(brandText).trim());
      setIf(
        "Candidate Brand Source",
        brandSource || "Independent Census Candidate"
      );
      setIf("Candidate Brand Confidence", brandConfidence);
      setIf("Brand Validation Status", validationStatus);
    }
    if (familyText) {
      setIf("Candidate Brand Family", String(familyText).trim());
    }
  }

  if (Object.keys(patch).length) {
    patch["Last Reviewed Date"] = todayIsoDate();
  }

  // Final strip
  for (const k of Object.keys(patch)) {
    if (FORBIDDEN.has(k) || isForbiddenAutopilotField(k) || !ALLOWED_WRITE.has(k)) {
      delete patch[k];
    }
  }

  return {
    ok: Object.keys(patch).length > 0,
    patch,
    skipped,
    conflicts,
    property_name_preserved: prop,
  };
}

export function isShellRecordForBackfill(fields, countries) {
  const country = String(fields.Country || "").trim();
  if (!countries.includes(country)) return false;
  if (fields["Enrichment Status"] !== "Discovered — pending enrichment") return false;
  if (fields["Public Display Review Status"] !== "Hold") return false;
  if (fields["Human Review Required"] !== true) return false;
  const idKey = String(fields["Property Identity Key"] || "");
  const notes = parseNotes(fields["Notes for Steward"]);
  return idKey.startsWith("shell_") || notes.is_shell_marker;
}

async function listShellRecords(baseId, token, countries) {
  const out = [];
  let offset;
  const countryOr = countries.map((c) => `{Country}='${c.replace(/'/g, "\\'")}'`).join(",");
  const formula = `AND({Enrichment Status}='Discovered — pending enrichment',{Public Display Review Status}='Hold',OR(${countryOr}))`;
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    for (const f of [
      "Property Name",
      "Canonical Property Name",
      "Property Identity Key",
      "Country",
      "City",
      "Notes for Steward",
      "Enrichment Status",
      "Public Display Review Status",
      "Human Review Required",
      "Current Brand",
      "Brand Family",
      "Family / Source Family",
      "HBX Hotel Code",
      "HBX Chain Code",
      "HBX Category Code",
      "HBX Linkage Confidence",
      "Last Reviewed Date",
      ...SHELL_BACKFILL_FIELD_NAMES,
    ]) {
      params.append("fields[]", f);
    }
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census_list_failed:${res.status}`);
    for (const r of json.records || []) {
      if (isShellRecordForBackfill(r.fields || {}, countries)) out.push(r);
    }
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function applyPatches(proposals, { baseId, token, log }) {
  let updates = 0;
  const errors = [];
  const counts = {
    canonical_fixed: 0,
    provenance: 0,
    candidate_brand_text: 0,
    candidate_brand_family: 0,
    candidate_brand_source: 0,
    brand_validation_status: 0,
    current_brand: 0,
    brand_family: 0,
  };

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (!ALLOWED_WRITE.has(k)) continue;
          if (FORBIDDEN.has(k) || isForbiddenAutopilotField(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
        }
        if (fields["Canonical Property Name"]) counts.canonical_fixed += 1;
        if (fields["Discovery Source"] || fields["Shell Insert Batch ID"]) {
          counts.provenance += 1;
        }
        if (fields["Candidate Brand Text"]) counts.candidate_brand_text += 1;
        if (fields["Candidate Brand Family"]) counts.candidate_brand_family += 1;
        if (fields["Candidate Brand Source"]) counts.candidate_brand_source += 1;
        if (fields["Brand Validation Status"]) counts.brand_validation_status += 1;
        if (fields["Current Brand"]) counts.current_brand += 1;
        if (fields["Brand Family"]) counts.brand_family += 1;
        return { id: p.record_id, fields };
      })
      .filter((u) => Object.keys(u.fields).length > 0);

    if (!records.length) continue;

    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records, typecast: true }),
      }
    );
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      errors.push({ status: res.status, error: json.error || json, batch: true });
      log?.(`[shell-backfill] batch ${res.status}; one-by-one`);
      for (const rec of records) {
        const one = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
          {
            method: "PATCH",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ records: [rec], typecast: true }),
          }
        );
        const oneJson = await one.json().catch(() => ({}));
        if (!one.ok) {
          errors.push({ status: one.status, error: oneJson.error || oneJson, id: rec.id });
        } else updates += 1;
        await sleep(150);
      }
    } else {
      updates += records.length;
    }
    await sleep(200);
  }

  return { updates, errors, counts };
}

function renderMd(report) {
  return `# Full CALA 15K Shell Format + Source/Candidate Brand Backfill v1

**Status:** \`${report.status}\`  
**Objective:** \`${report.objective}\`  
**Generated:** ${report.generated_at}  
**Dry run:** ${report.dry_run}  
**Airtable writes:** **${report.airtable_writes}**

## Scope
- Countries: ${(report.countries || []).join(", ")}
- Shell records reviewed: **${report.records_reviewed}**
- Records updated: **${report.records_updated}**

## Canonical Property Name
- Fixed: **${report.canonical_names_fixed}**

## Source / provenance
- Provenance writes: **${report.provenance_writes}**

## Candidate brand
- Candidate Brand Text: **${report.candidate_brand_text_writes}**
- Candidate Brand Family: **${report.candidate_brand_family_writes}**
- Candidate Brand Source: **${report.candidate_brand_source_writes}**
- Brand Validation Status: **${report.brand_validation_status_writes}**
- Current Brand writes: **${report.current_brand_writes}** (must be 0)
- Brand Family writes: **${report.brand_family_writes}** (must be 0)

## Family / Source
${JSON.stringify(report.family_source_investigation, null, 2)}

## Schema
- Created: **${(report.schema?.created || []).length}**
- Missing: **${(report.schema?.missing || []).length}**
${(report.schema?.missing || []).map((f) => `- \`${f}\``).join("\n") || ""}

## Confirmations
- Hotel Property Census only: **true**
- No Current Brand from unvalidated Cvent: **true**
- Shells remain Hold / HR Required: **true**
- Restricted fields untouched: **true**
`;
}

function persist(report) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  writeJson(
    path.join(reportsDir, "full-cala-15k-shell-format-source-brand-backfill-v1.json"),
    report
  );
  const md = renderMd(report);
  writeMd(
    path.join(reportsDir, "full-cala-15k-shell-format-source-brand-backfill-v1.md"),
    md
  );
  writeMd(
    path.join(docsDir, "full-cala-15k-shell-format-source-brand-backfill-v1.md"),
    md
  );
}

function parseCountries(args = {}, argv = []) {
  if (Array.isArray(args.countries) && args.countries.length) return args.countries;
  if (typeof args.countries === "string" && args.countries.trim()) {
    return args.countries.split(",").map((s) => s.trim()).filter(Boolean);
  }
  const i = argv.indexOf("--countries");
  if (i >= 0 && argv[i + 1]) {
    return String(argv[i + 1])
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  if (args.country) return [args.country];
  return [...DEFAULT_COUNTRIES];
}

/**
 * @param {object} opts
 */
export async function runFullCala15kShellFormatSourceBrandBackfillV1(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const args = opts.args || {};
  const argv = opts.argv || [];
  const generated_at = new Date().toISOString();
  const gates = resolveShellFormatBackfillGates(env);
  const countries = parseCountries(args, argv);
  const family_source_investigation = investigateFamilySourceField();

  if (!gates.ok) {
    const report = {
      ok: false,
      status: SHELL_FORMAT_BACKFILL_STATUS.BLOCKED,
      objective: SHELL_FORMAT_BACKFILL_OBJECTIVE,
      generated_at,
      reason: "gate_blockers",
      blockers: gates.blockers,
      airtable_writes: 0,
      dry_run: true,
      countries,
      family_source_investigation,
    };
    persist(report);
    return report;
  }

  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      gates.shell_mission &&
      gates.format_backfill
  );

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.target_base_id || base?.baseId || env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
  } catch (err) {
    const report = {
      ok: false,
      status: SHELL_FORMAT_BACKFILL_STATUS.BLOCKED,
      objective: SHELL_FORMAT_BACKFILL_OBJECTIVE,
      generated_at,
      reason: String(err?.message || err).slice(0, 300),
      airtable_writes: 0,
      dry_run: true,
      countries,
      family_source_investigation,
    };
    persist(report);
    return report;
  }

  log(`[shell-backfill] schema repair apply=${enableWrites}`);
  const schemaResult = await ensureShellBackfillSchemaFields({
    baseId,
    token,
    apply: enableWrites,
    log,
  });

  const tableAfter = await listCensusTable(baseId, token);
  const fieldSet = new Set((tableAfter.fields || []).map((f) => f.name));
  const stillMissing = SHELL_BACKFILL_FIELD_NAMES.filter((n) => !fieldSet.has(n));

  if (stillMissing.length && enableWrites) {
    // Continue with available fields; mark partial schema
  }

  if (!fieldSet.has("Canonical Property Name")) {
    const report = {
      ok: true,
      status: SHELL_FORMAT_BACKFILL_STATUS.PARTIAL_SCHEMA,
      objective: SHELL_FORMAT_BACKFILL_OBJECTIVE,
      generated_at,
      reason: "Canonical Property Name schema_missing",
      schema: { created: schemaResult.created, missing: ["Canonical Property Name", ...stillMissing] },
      airtable_writes: 0,
      dry_run: !enableWrites,
      countries,
      family_source_investigation,
      records_reviewed: 0,
      records_updated: 0,
      canonical_names_fixed: 0,
      provenance_writes: 0,
      candidate_brand_text_writes: 0,
      candidate_brand_family_writes: 0,
      candidate_brand_source_writes: 0,
      brand_validation_status_writes: 0,
      current_brand_writes: 0,
      brand_family_writes: 0,
    };
    persist(report);
    return report;
  }

  log(`[shell-backfill] loading candidate indexes…`);
  const indexes = loadCandidateIndexes();
  let brandDictionary = null;
  try {
    brandDictionary = buildCanonicalBrandDictionary({ region: "CALA" });
  } catch (err) {
    log(`[shell-backfill] brand dictionary unavailable: ${err?.message || err}`);
  }
  log(`[shell-backfill] listing shells for ${countries.join(", ")}`);
  const shells = await listShellRecords(baseId, token, countries);
  log(`[shell-backfill] shells matched=${shells.length}`);

  const proposals = [];
  const conflicts = [];
  let dryCanonical = 0;
  let dryProvenance = 0;
  let dryBrandText = 0;
  let dryBrandFamily = 0;
  let dryBrandSource = 0;
  let dryValidation = 0;

  for (const rec of shells) {
    const fields = rec.fields || {};
    const notesMeta = parseNotes(fields["Notes for Steward"]);
    const candidate = resolveCandidateForRecord(fields, notesMeta, indexes);
    const built = buildShellBackfillPatch(fields, {
      fieldSet,
      candidate,
      notesMeta,
      allowCandidateBrand: gates.candidate_brand || !enableWrites,
      countryBatch: fields.Country,
      brandDictionary,
    });
    for (const c of built.conflicts) {
      conflicts.push({ ...c, record_id: rec.id });
    }
    if (!built.ok) continue;
    if (built.patch["Canonical Property Name"]) dryCanonical += 1;
    if (built.patch["Discovery Source"] || built.patch["Shell Insert Batch ID"]) {
      dryProvenance += 1;
    }
    if (built.patch["Candidate Brand Text"]) dryBrandText += 1;
    if (built.patch["Candidate Brand Family"]) dryBrandFamily += 1;
    if (built.patch["Candidate Brand Source"]) dryBrandSource += 1;
    if (built.patch["Brand Validation Status"]) dryValidation += 1;
    proposals.push({ record_id: rec.id, patch: built.patch, country: fields.Country });
  }

  let updates = 0;
  let writeErrors = [];
  let counts = {
    canonical_fixed: dryCanonical,
    provenance: dryProvenance,
    candidate_brand_text: dryBrandText,
    candidate_brand_family: dryBrandFamily,
    candidate_brand_source: dryBrandSource,
    brand_validation_status: dryValidation,
    current_brand: 0,
    brand_family: 0,
  };

  if (enableWrites && proposals.length) {
    const applied = await applyPatches(proposals, { baseId, token, log });
    updates = applied.updates;
    writeErrors = applied.errors;
    counts = applied.counts;
  }

  let status = SHELL_FORMAT_BACKFILL_STATUS.COMPLETE;
  if (stillMissing.length) {
    status = SHELL_FORMAT_BACKFILL_STATUS.PARTIAL_SCHEMA;
  } else if (enableWrites && updates === 0 && proposals.length && writeErrors.length) {
    status = SHELL_FORMAT_BACKFILL_STATUS.BLOCKED;
  } else if (counts.candidate_brand_text > 0 && counts.current_brand === 0) {
    // Candidate signals written; Current Brand intentionally held
    status = SHELL_FORMAT_BACKFILL_STATUS.COMPLETE;
  }

  const report = {
    ok: status !== SHELL_FORMAT_BACKFILL_STATUS.BLOCKED,
    status,
    secondary_status:
      counts.candidate_brand_text > 0 && counts.current_brand === 0
        ? SHELL_FORMAT_BACKFILL_STATUS.PARTIAL_BRAND
        : null,
    objective: SHELL_FORMAT_BACKFILL_OBJECTIVE,
    version: SHELL_FORMAT_BACKFILL_VERSION,
    generated_at,
    dry_run: !enableWrites,
    airtable_writes: updates,
    countries,
    records_reviewed: shells.length,
    proposals_ready: proposals.length,
    records_updated: enableWrites ? updates : 0,
    canonical_names_fixed: counts.canonical_fixed,
    provenance_writes: counts.provenance,
    candidate_brand_text_writes: counts.candidate_brand_text,
    candidate_brand_family_writes: counts.candidate_brand_family,
    candidate_brand_source_writes: counts.candidate_brand_source,
    brand_validation_status_writes: counts.brand_validation_status,
    current_brand_writes: counts.current_brand,
    brand_family_writes: counts.brand_family,
    conflicts_count: conflicts.length,
    conflicts: conflicts.slice(0, 40),
    write_errors: writeErrors.slice(0, 15),
    schema: {
      found: [...fieldSet].filter((n) => SHELL_BACKFILL_FIELD_NAMES.includes(n)),
      created: schemaResult.created,
      missing: stillMissing,
    },
    family_source_investigation,
    confirmations: {
      hotel_property_census_only: true,
      no_brand_explorer: true,
      no_brand_setup: true,
      no_rooms_keys: true,
      no_coords_media: true,
      no_owner_operator_dates: true,
      no_current_brand_from_unvalidated_cvent: counts.current_brand === 0,
      shells_remain_hold_hr: true,
      family_source_family_not_written: true,
    },
  };

  persist(report);
  log(
    `[shell-backfill] status=${report.status} reviewed=${report.records_reviewed} updated=${report.records_updated} canonical=${report.canonical_names_fixed}`
  );
  return report;
}
