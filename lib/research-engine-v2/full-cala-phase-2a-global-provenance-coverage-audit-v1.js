/**
 * Phase 2A — Global Census Provenance + Coverage Audit (READ-ONLY).
 *
 * No production Census writes. No enrichment. No shell inserts. No brand promotion.
 *
 * Status target:
 *   production_census_full_cala_phase_2a_global_provenance_coverage_audit_complete
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { normName } from "./census-autopilot-v2/identity-dedupe.js";
import { parseNotes } from "./full-cala-15k-shell-format-source-brand-backfill-v1.js";
import {
  classifyShellOrigin,
  auditShellCventProvenance,
} from "./cvent-provenance-audit-v1.js";
import {
  CENSUS_TABLE_ID,
  MATCH,
  loadMasterUniverseCandidates,
  loadHbxCandidates,
  mergeCandidateUniverses,
  classifyAgainstCensus,
} from "./full-cala-15k-census-shell-insert-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const PHASE_2A_OBJECTIVE =
  "full-cala-phase-2a-global-provenance-coverage-audit-v1";
export const PHASE_2A_VERSION =
  "full-cala-phase-2a-global-provenance-coverage-audit-v1";

export const PHASE_2A_STATUS = Object.freeze({
  COMPLETE:
    "production_census_full_cala_phase_2a_global_provenance_coverage_audit_complete",
  COMPLETE_WITH_GAPS:
    "production_census_full_cala_phase_2a_global_provenance_coverage_audit_complete_with_gaps",
  BLOCKED:
    "production_census_full_cala_phase_2a_global_provenance_coverage_audit_blocked",
});

export const NEXT_ACTION = Object.freeze({
  HBX_ENRICHMENT: "PROCEED_HBX_LINKAGE_AND_CONTACT_ENRICHMENT",
  SOURCE_GAP: "PROCEED_SOURCE_GAP_DISCOVERY",
  PROVENANCE_FIRST: "REMEDIATE_PROVENANCE_FIRST",
  DUPLICATES_FIRST: "REMEDIATE_DUPLICATES_FIRST",
  FOUNDER_STOP: "STOP_FOR_FOUNDER_REVIEW",
});

const SHELL_BATCH_ID = "full-cala-15k-census-shell-insert-v1";
const EXPECTED_CENSUS = 5956;
const ESTIMATED_CALA_TARGET = 15000;

const AUDIT_FIELDS_CORE = [
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Country",
  "City",
  "Address",
  "Official Property URL",
  "Phone",
  "Production Use Status",
  "Review Status",
  "Enrichment Status",
  "Public Display Review Status",
  "Radar Display Status",
  "Human Review Required",
  "Discovery Source",
  "Source Candidate Type",
  "Candidate Source Count",
  "Shell Insert Batch ID",
  "Shell Insert Country Batch",
  "Shell Insert Date",
  "Shell Insert Source Mix",
  "Shell Dedupe Confidence",
  "Notes for Steward",
  "HBX Hotel Code",
  "HBX Chain Code",
  "HBX Category Code",
  "HBX Linkage Confidence",
  "HBX Source Status",
  "HBX Content Review Status",
  "Candidate Brand Text",
  "Candidate Brand Family",
  "Candidate Brand Source",
  "Candidate Brand Confidence",
  "Brand Validation Status",
  "Current Brand",
  "Brand Family",
  "Family / Source Family",
  "Company Validated",
  "Rooms / Keys",
  "Owner Name",
  "Operator / Management Company",
  "Developer Name",
  "Opening Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Latitude",
  "Longitude",
  "Hotel Description - AI Summary",
  "Amenities - Structured Tags",
];

/** Optional fields — dropped silently if schema lacks them. */
const AUDIT_FIELDS_OPTIONAL = ["Brand Verified", "Brand Status"];

const CALA_COUNTRIES = [
  "Mexico",
  "Colombia",
  "Brazil",
  "Argentina",
  "Chile",
  "Peru",
  "Ecuador",
  "Bolivia",
  "Paraguay",
  "Uruguay",
  "Venezuela",
  "Costa Rica",
  "Panama",
  "Nicaragua",
  "Honduras",
  "Guatemala",
  "El Salvador",
  "Belize",
  "Dominican Republic",
  "Cuba",
  "Puerto Rico",
  "Jamaica",
  "Haiti",
  "Bahamas",
  "Trinidad and Tobago",
  "Barbados",
  "Aruba",
  "Curaçao",
  "Cayman Islands",
  "Guyana",
  "Suriname",
  "French Guiana",
];

const EXPECTED_SHELL_BY_COUNTRY_BATCH = Object.freeze({
  "Dominican Republic": 416,
  "Costa Rica": 641, // 500 + 141
  Panama: 280,
  Colombia: 793, // 500 + 293
  Mexico: 1265, // 500+500+265
});

const VALIDATED_BY_CVENT_RE =
  /validated\s+by\s+cvent|cvent\s+validated|field[- ]level\s+source.*cvent|cvent.*source\s+of\s+truth/i;

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
function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}
function isBlank(v) {
  return v == null || !String(v).trim();
}
function domainOf(url) {
  try {
    const u = new URL(String(url || "").trim());
    return u.hostname.replace(/^www\./i, "").toLowerCase() || null;
  } catch {
    return null;
  }
}
function normPhone(p) {
  const d = String(p || "").replace(/\D+/g, "");
  return d.length >= 7 ? d : null;
}
function bump(map, key, n = 1) {
  const k = key == null || key === "" ? "(blank)" : String(key);
  map[k] = (map[k] || 0) + n;
}
function coverage(present, total) {
  return {
    present,
    missing: Math.max(0, total - present),
    pct: total ? Number(((100 * present) / total).toFixed(1)) : 0,
  };
}

function isShellRecord(fields) {
  const idKey = String(fields["Property Identity Key"] || "");
  const batch = String(fields["Shell Insert Batch ID"] || "");
  const notes = parseNotes(fields["Notes for Steward"]);
  // Strict full-cala-15k shell lineage only (do not treat other pending-enrichment rows as shells).
  if (batch === SHELL_BATCH_ID) return true;
  if (idKey.startsWith("shell_") && (notes.is_shell_marker || notes.is_hbx || notes.is_cvent)) {
    return true;
  }
  return false;
}

async function listAllCensusRecords(baseId, token, log) {
  // Prefer full-record pages (no fields[]) so schema drift cannot 422 the audit.
  // Falls back to a pruned fields[] list only if full pages are rejected.
  const tryFull = await listCensusPages(baseId, token, null, log);
  if (tryFull.ok) return tryFull.records;

  log?.(
    `[phase-2a] full-record list failed (${tryFull.error}); retrying with core fields[]…`
  );
  let fields = [...AUDIT_FIELDS_CORE];
  for (const opt of AUDIT_FIELDS_OPTIONAL) {
    // probe later via first record keys only — do not request optional unknowns
  }
  const core = await listCensusPages(baseId, token, fields, log);
  if (!core.ok) {
    throw new Error(`census_list_failed:${core.error}`);
  }
  return core.records;
}

async function listCensusPages(baseId, token, fields, log) {
  const records = [];
  let offset;
  let pages = 0;
  try {
    do {
      const params = new URLSearchParams({ pageSize: "100" });
      if (offset) params.set("offset", offset);
      if (fields) {
        for (const f of fields) params.append("fields[]", f);
      }
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        return {
          ok: false,
          error: `${res.status}:${json?.error?.message || ""}`,
          records,
        };
      }
      records.push(...(json.records || []));
      offset = json.offset;
      pages += 1;
      if (pages % 10 === 0) log?.(`[phase-2a] listed ${records.length} records…`);
      await sleep(110);
    } while (offset);
    return { ok: true, records };
  } catch (err) {
    return { ok: false, error: String(err?.message || err), records };
  }
}

function collectManifestExpectations() {
  const expectedTotal = Object.values(EXPECTED_SHELL_BY_COUNTRY_BATCH).reduce(
    (a, b) => a + b,
    0
  );
  const checkpoint = readJson(
    path.join(
      ROOT,
      "data/research-engine-v2/full-cala-15k-census-shell/full-cala-15k-checkpoint.json"
    ),
    { batches: [] }
  );
  const checkpointInserted = (checkpoint.batches || []).reduce(
    (a, b) => a + (Number(b.inserted) || 0),
    0
  );
  const orchFinal = readJson(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-15k-shell-orchestrator-final.json"
    ),
    {}
  );
  const orchShells = Number(orchFinal.SHELLS_ADDED_THIS_RUN) || 0;

  const allowlistPaths = [
    "reports/research-engine-v2/full-cala-15k-colombia-batch-2-allowlist.json",
    "reports/research-engine-v2/full-cala-shell-orchestrator/orch_2026-08-09T19-19-46-533Z/orch_2026-08-09T19-19-46-533Z__Costa_Rica__2-allowlist.json",
  ];
  const allowlists = [];
  for (const rel of allowlistPaths) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    const j = readJson(fp, {});
    allowlists.push({
      path: rel,
      count:
        j.fingerprint?.count ||
        j.allowlist_fingerprint?.count ||
        (j.records || []).length ||
        0,
      first:
        j.fingerprint?.first_candidate_id ||
        j.allowlist_fingerprint?.first_candidate_id ||
        j.records?.[0]?.candidate_id ||
        null,
    });
  }

  return {
    expected_shell_total: expectedTotal,
    expected_by_country: { ...EXPECTED_SHELL_BY_COUNTRY_BATCH },
    checkpoint_inserted: checkpointInserted,
    orchestrator_shells_added: orchShells,
    checkpoint_plus_orch: checkpointInserted + orchShells,
    allowlists,
  };
}

function buildCensusIndexFromRecords(records) {
  const byNameCountry = new Map();
  const byDomain = new Map();
  const byPhone = new Map();
  const byHbx = new Map();
  const byIdentityKey = new Map();
  for (const r of records) {
    const f = r.fields || {};
    const name = normName(f["Canonical Property Name"] || f["Property Name"]);
    const country = normName(f.Country);
    const key = `${name}|${country}`;
    if (!byNameCountry.has(key)) byNameCountry.set(key, []);
    byNameCountry.get(key).push(r);
    const dom = domainOf(f["Official Property URL"]);
    if (dom) {
      if (!byDomain.has(dom)) byDomain.set(dom, []);
      byDomain.get(dom).push(r);
    }
    const ph = normPhone(f.Phone);
    if (ph) {
      if (!byPhone.has(ph)) byPhone.set(ph, []);
      byPhone.get(ph).push(r);
    }
    const idKey = String(f["Property Identity Key"] || "").trim();
    if (idKey) byIdentityKey.set(idKey, r);
    const fieldCode = String(f["HBX Hotel Code"] || "").trim();
    if (fieldCode && /^\d+$/.test(fieldCode)) {
      const n = Number(fieldCode);
      if (!byHbx.has(n)) byHbx.set(n, []);
      byHbx.get(n).push(r);
    } else {
      const notes = String(f["Notes for Steward"] || "");
      const m = notes.match(/hotel_code=(\d+)/);
      if (m) {
        const n = Number(m[1]);
        if (!byHbx.has(n)) byHbx.set(n, []);
        byHbx.get(n).push(r);
      }
    }
  }
  return {
    records,
    byNameCountry,
    byDomain,
    byPhone,
    byHbx,
    byIdentityKey,
    count: records.length,
  };
}

function assessDuplicates(records) {
  const groups = {
    duplicate_high: [],
    duplicate_review: [],
  };
  const byHbx = new Map();
  const byNameCountry = new Map();
  const byDomain = new Map();
  const byPhone = new Map();

  for (const r of records) {
    const f = r.fields || {};
    const hbx = String(f["HBX Hotel Code"] || "").trim();
    if (hbx && /^\d+$/.test(hbx)) {
      if (!byHbx.has(hbx)) byHbx.set(hbx, []);
      byHbx.get(hbx).push(r);
    }
    const nk = `${normName(f["Canonical Property Name"] || f["Property Name"])}|${normName(f.Country)}`;
    if (nk !== "|") {
      if (!byNameCountry.has(nk)) byNameCountry.set(nk, []);
      byNameCountry.get(nk).push(r);
    }
    const dom = domainOf(f["Official Property URL"]);
    if (dom) {
      if (!byDomain.has(dom)) byDomain.set(dom, []);
      byDomain.get(dom).push(r);
    }
    const ph = normPhone(f.Phone);
    if (ph) {
      if (!byPhone.has(ph)) byPhone.set(ph, []);
      byPhone.get(ph).push(r);
    }
  }

  const seenPair = new Set();
  function addGroup(bucket, reason, rows) {
    if (rows.length < 2) return;
    const ids = rows.map((r) => r.id).sort();
    const key = ids.join("|");
    if (seenPair.has(key)) return;
    seenPair.add(key);
    groups[bucket].push({
      reason,
      size: rows.length,
      record_ids: ids,
      countries: [...new Set(rows.map((r) => r.fields?.Country).filter(Boolean))],
      names: rows.map((r) => r.fields?.["Property Name"]).slice(0, 5),
    });
  }

  for (const [code, rows] of byHbx) {
    if (rows.length > 1) addGroup("duplicate_high", `hbx_hotel_code:${code}`, rows);
  }
  for (const [nk, rows] of byNameCountry) {
    if (rows.length < 2) continue;
    const hbxCodes = [
      ...new Set(
        rows
          .map((r) => String(r.fields?.["HBX Hotel Code"] || "").trim())
          .filter((x) => /^\d+$/.test(x))
      ),
    ];
    // High only when every row shares one identical HBX code (also covered by byHbx).
    const allHaveSameHbx =
      hbxCodes.length === 1 &&
      rows.every(
        (r) => String(r.fields?.["HBX Hotel Code"] || "").trim() === hbxCodes[0]
      );
    if (allHaveSameHbx) continue; // already counted via HBX
    addGroup("duplicate_review", `name_country:${nk}`, rows);
  }
  for (const [dom, rows] of byDomain) {
    if (rows.length > 1) {
      const countries = new Set(rows.map((r) => normName(r.fields?.Country)));
      if (countries.size === 1) {
        addGroup("duplicate_review", `domain:${dom}`, rows);
      }
    }
  }
  for (const [ph, rows] of byPhone) {
    if (rows.length > 1) {
      const countries = new Set(rows.map((r) => normName(r.fields?.Country)));
      if (countries.size === 1) {
        addGroup("duplicate_review", `phone:${ph}`, rows);
      }
    }
  }

  const countryRisk = {};
  for (const g of [...groups.duplicate_high, ...groups.duplicate_review]) {
    for (const c of g.countries) bump(countryRisk, c);
  }

  return {
    duplicate_high_groups: groups.duplicate_high.length,
    duplicate_review_groups: groups.duplicate_review.length,
    duplicate_high_sample: groups.duplicate_high.slice(0, 25),
    duplicate_review_sample: groups.duplicate_review.slice(0, 25),
    countries_highest_duplicate_risk: Object.entries(countryRisk)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 15)
      .map(([country, groups_touched]) => ({ country, groups_touched })),
  };
}

function recommendNextAction(ctx) {
  const {
    provenance_anomalies,
    protected_field_anomalies,
    brand_shell_anomalies,
    hbx_duplicate_groups,
    duplicate_high_groups,
    estimated_undiscovered_gap,
    held_total,
    brazil_held,
  } = ctx;

  if (
    provenance_anomalies > 50 ||
    protected_field_anomalies > 20 ||
    brand_shell_anomalies > 20
  ) {
    return {
      action: NEXT_ACTION.PROVENANCE_FIRST,
      rationale:
        "Material provenance / protected-field / brand contamination anomalies on shell records.",
    };
  }
  if (hbx_duplicate_groups > 10 || duplicate_high_groups > 40) {
    return {
      action: NEXT_ACTION.DUPLICATES_FIRST,
      rationale:
        "Material high-confidence duplicate / shared HBX Hotel Code risk before enrichment.",
    };
  }
  if (
    estimated_undiscovered_gap >= 4000 ||
    brazil_held >= 3000 ||
    held_total >= 7000
  ) {
    return {
      action: NEXT_ACTION.SOURCE_GAP,
      rationale:
        "Large held weak pools + sizable undiscovered gap; discovery/source coverage is the binding constraint (Brazil dominant).",
    };
  }
  return {
    action: NEXT_ACTION.HBX_ENRICHMENT,
    rationale:
      "Shell integrity is largely clean; next value is strengthening identity/contact on known HBX-linked shells and held candidates with recoverable evidence.",
  };
}

export async function runFullCalaPhase2aGlobalProvenanceCoverageAuditV1(
  opts = {}
) {
  const log = opts.log || (() => {});
  const generated_at = new Date().toISOString();

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.target_base_id || base?.baseId || process.env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
    if (CENSUS_TABLE_ID !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
      throw new Error(
        `target_table_id_mismatch:${CENSUS_TABLE_ID}!=${PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID}`
      );
    }
  } catch (err) {
    return {
      ok: false,
      AUDIT_STATUS: PHASE_2A_STATUS.BLOCKED,
      STOP_REASON: String(err?.message || err).slice(0, 400),
      FOUNDER_DECISION_REQUIRED: "YES",
      FOUNDER_DECISION: "Resolve Airtable / target-table access before audit.",
      production_writes: false,
      generated_at,
    };
  }

  log("[phase-2a] listing full Hotel Property Census (read-only)…");
  const records = await listAllCensusRecords(baseId, token, log);
  const productionCount = records.length;
  log(`[phase-2a] production count=${productionCount}`);

  const byCountry = {};
  const byProductionUse = {};
  const byReviewStatus = {};
  const byEnrichment = {};
  const byPublicDisplay = {};
  const byRadar = {};
  let hrTrue = 0;
  let hrFalse = 0;
  let hrBlank = 0;

  const shells = [];
  const shellStateAnomalies = [];
  const provenanceAnomalies = [];
  const protectedFieldAnomalies = [];
  const brandShellAnomalies = [];
  const cventValidationLanguage = [];

  const hbxCodes = [];
  const hbxByCountry = {};
  const hbxChain = {};
  const hbxCategory = {};
  const hbxLinkage = {};
  const hbxSourceStatus = {};
  const hbxContentReview = {};
  const malformedHbx = [];

  const shellSourceMix = { hbx_only: 0, cvent_plus_hbx: 0, cvent_only: 0, other: 0 };
  const shellSourceMixByCountry = {};
  const shellByCountry = {};
  const shellByCountryBatch = {};

  let shellBatchIdPresent = 0;
  let shellCountryBatchPresent = 0;
  let shellDatePresent = 0;
  let shellMixPresent = 0;
  let discoveryPresent = 0;
  let sourceCandidateTypePresent = 0;
  let candidateSourceCountPresent = 0;
  let shellDedupePresent = 0;
  let candidateBrandPresent = 0;

  for (const r of records) {
    const f = r.fields || {};
    const country = String(f.Country || "").trim() || "(blank)";
    bump(byCountry, country);
    bump(byProductionUse, f["Production Use Status"]);
    bump(byReviewStatus, f["Review Status"]);
    bump(byEnrichment, f["Enrichment Status"]);
    bump(byPublicDisplay, f["Public Display Review Status"]);
    bump(byRadar, f["Radar Display Status"]);
    if (f["Human Review Required"] === true) hrTrue += 1;
    else if (f["Human Review Required"] === false) hrFalse += 1;
    else hrBlank += 1;

    const hbxRaw = String(f["HBX Hotel Code"] || "").trim();
    if (hbxRaw) {
      if (/^\d+$/.test(hbxRaw)) {
        hbxCodes.push(hbxRaw);
        bump(hbxByCountry, country);
      } else {
        malformedHbx.push({ id: r.id, country, value: hbxRaw });
      }
    }
    if (!isBlank(f["HBX Chain Code"])) bump(hbxChain, f["HBX Chain Code"]);
    if (!isBlank(f["HBX Category Code"])) bump(hbxCategory, f["HBX Category Code"]);
    if (!isBlank(f["HBX Linkage Confidence"]))
      bump(hbxLinkage, f["HBX Linkage Confidence"]);
    if (!isBlank(f["HBX Source Status"])) bump(hbxSourceStatus, f["HBX Source Status"]);
    if (!isBlank(f["HBX Content Review Status"]))
      bump(hbxContentReview, f["HBX Content Review Status"]);

    if (!isShellRecord(f)) continue;
    shells.push(r);
    bump(shellByCountry, country);
    bump(shellByCountryBatch, f["Shell Insert Country Batch"] || "(blank)");

    if (!isBlank(f["Shell Insert Batch ID"])) shellBatchIdPresent += 1;
    if (!isBlank(f["Shell Insert Country Batch"])) shellCountryBatchPresent += 1;
    if (!isBlank(f["Shell Insert Date"])) shellDatePresent += 1;
    if (!isBlank(f["Shell Insert Source Mix"])) shellMixPresent += 1;
    if (!isBlank(f["Discovery Source"])) discoveryPresent += 1;
    if (!isBlank(f["Source Candidate Type"])) sourceCandidateTypePresent += 1;
    if (f["Candidate Source Count"] != null) candidateSourceCountPresent += 1;
    if (!isBlank(f["Shell Dedupe Confidence"])) shellDedupePresent += 1;
    if (!isBlank(f["Candidate Brand Text"])) candidateBrandPresent += 1;

    // Shell state anomalies (unexpected owner-facing / public / validated)
    const pus = String(f["Production Use Status"] || "");
    const pub = String(f["Public Display Review Status"] || "");
    const radar = String(f["Radar Display Status"] || "");
    if (pus && pus !== "Census Only / Not Owner-Facing") {
      shellStateAnomalies.push({
        id: r.id,
        country,
        type: "unexpected_production_use_status",
        value: pus,
      });
    }
    if (pub && pub !== "Hold") {
      shellStateAnomalies.push({
        id: r.id,
        country,
        type: "unexpected_public_display",
        value: pub,
      });
    }
    if (radar && radar !== "Hold") {
      shellStateAnomalies.push({
        id: r.id,
        country,
        type: "unexpected_radar_display",
        value: radar,
      });
    }
    if (f["Human Review Required"] !== true) {
      shellStateAnomalies.push({
        id: r.id,
        country,
        type: "human_review_required_not_true",
        value: f["Human Review Required"],
      });
    }
    if (
      f["Enrichment Status"] &&
      f["Enrichment Status"] !== "Discovered — pending enrichment"
    ) {
      // Not necessarily anomalous if later enrichment started — flag for review
      shellStateAnomalies.push({
        id: r.id,
        country,
        type: "enrichment_status_not_discovered_pending",
        value: f["Enrichment Status"],
        severity: "info",
      });
    }
    if (f["Company Validated"] === true || f["Brand Verified"] === true) {
      shellStateAnomalies.push({
        id: r.id,
        country,
        type: "shell_marked_validated",
        company_validated: f["Company Validated"],
        brand_verified: f["Brand Verified"],
      });
    }

    const origin = classifyShellOrigin(f);
    if (!shellSourceMixByCountry[country]) {
      shellSourceMixByCountry[country] = {
        hbx_only: 0,
        cvent_plus_hbx: 0,
        cvent_only: 0,
        other: 0,
      };
    }
    if (origin.origin === "hbx_only") {
      shellSourceMix.hbx_only += 1;
      shellSourceMixByCountry[country].hbx_only += 1;
    } else if (origin.origin === "cvent_plus_hbx") {
      shellSourceMix.cvent_plus_hbx += 1;
      shellSourceMixByCountry[country].cvent_plus_hbx += 1;
    } else if (origin.origin === "cvent_only") {
      shellSourceMix.cvent_only += 1;
      shellSourceMixByCountry[country].cvent_only += 1;
    } else {
      shellSourceMix.other += 1;
      shellSourceMixByCountry[country].other += 1;
    }

    const cventAudit = auditShellCventProvenance(r);
    for (const issue of cventAudit.issues || []) {
      if (
        issue === "says_validated_by_cvent" ||
        issue.startsWith("cvent_") ||
        issue.includes("validated")
      ) {
        provenanceAnomalies.push({
          id: r.id,
          country,
          issue,
          discovery: f["Discovery Source"] || null,
        });
      }
    }
    const blob = [
      f["Discovery Source"],
      f["Source Candidate Type"],
      f["Candidate Brand Source"],
      f["Notes for Steward"],
      f["Shell Insert Source Mix"],
    ]
      .map((x) => String(x || ""))
      .join("\n");
    if (VALIDATED_BY_CVENT_RE.test(blob)) {
      cventValidationLanguage.push({
        id: r.id,
        country,
        discovery: f["Discovery Source"] || null,
      });
    }

    // Brand safety — shell should not have promoted Current Brand / Brand Family / etc.
    for (const field of [
      "Current Brand",
      "Brand Family",
      "Family / Source Family",
      "Brand Verified",
      "Brand Status",
      "Company Validated",
    ]) {
      const v = f[field];
      if (v === true || (v != null && String(v).trim() !== "")) {
        brandShellAnomalies.push({
          id: r.id,
          country,
          field,
          value: typeof v === "boolean" ? v : String(v).slice(0, 80),
          classification: "B_possible_shell_introduced",
          note:
            "Shell-created record has prohibited brand/validation field populated. Shell insert path does not write these; treat as contamination unless proven pre-existing via non-shell lineage.",
        });
      }
    }

    // Protected enrichment fields — should be empty on pure shells
    const protectedChecks = [
      ["Rooms / Keys", f["Rooms / Keys"]],
      ["Owner Name", f["Owner Name"]],
      ["Operator / Management Company", f["Operator / Management Company"]],
      ["Developer Name", f["Developer Name"]],
      ["Opening Date", f["Opening Date"]],
      ["Renovation / Conversion Date", f["Renovation / Conversion Date"]],
      ["Affiliation Start Date", f["Affiliation Start Date"]],
      ["Recent Momentum", f["Recent Momentum"]],
      ["Latitude", f["Latitude"]],
      ["Longitude", f["Longitude"]],
      ["Hotel Description - AI Summary", f["Hotel Description - AI Summary"]],
      ["Amenities - Structured Tags", f["Amenities - Structured Tags"]],
    ];
    for (const [field, v] of protectedChecks) {
      if (v === true || (v != null && String(v).trim() !== "")) {
        protectedFieldAnomalies.push({
          id: r.id,
          country,
          field,
          value: String(v).slice(0, 60),
          classification: "B_possible_shell_introduced",
        });
      }
    }
  }

  const shellTotal = shells.length;
  const manifestMeta = collectManifestExpectations();

  // HBX uniqueness
  const hbxFreq = {};
  for (const c of hbxCodes) bump(hbxFreq, c);
  const duplicateHbxCodes = Object.entries(hbxFreq)
    .filter(([, n]) => n > 1)
    .map(([code, count]) => ({ code, count }))
    .sort((a, b) => b.count - a.count);
  const uniqueHbx = Object.keys(hbxFreq).length;

  // Lineage reconciliation
  const lineage = {
    expected_shell_inserts: manifestMeta.expected_shell_total,
    production_shells_found: shellTotal,
    delta_expected_minus_found:
      manifestMeta.expected_shell_total - shellTotal,
    checkpoint_inserted: manifestMeta.checkpoint_inserted,
    orchestrator_shells_added: manifestMeta.orchestrator_shells_added,
    checkpoint_plus_orch: manifestMeta.checkpoint_plus_orch,
    by_country_expected: manifestMeta.expected_by_country,
    by_country_found: shellByCountry,
    by_country_batch_found: shellByCountryBatch,
    country_deltas: {},
    missing_batch_ids: shellTotal - shellBatchIdPresent,
    duplicate_batch_id_value: SHELL_BATCH_ID,
    note: "Shell Insert Batch ID is intentionally shared across full-cala-15k wave (not per-country unique).",
    coverage: {
      shell_insert_batch_id: coverage(shellBatchIdPresent, shellTotal),
      shell_insert_country_batch: coverage(shellCountryBatchPresent, shellTotal),
      shell_insert_date: coverage(shellDatePresent, shellTotal),
      shell_insert_source_mix: coverage(shellMixPresent, shellTotal),
      discovery_source: coverage(discoveryPresent, shellTotal),
      source_candidate_type: coverage(sourceCandidateTypePresent, shellTotal),
      candidate_source_count: coverage(candidateSourceCountPresent, shellTotal),
      shell_dedupe_confidence: coverage(shellDedupePresent, shellTotal),
      candidate_brand_text: coverage(candidateBrandPresent, shellTotal),
    },
    allowlists: manifestMeta.allowlists,
  };
  for (const [c, expected] of Object.entries(manifestMeta.expected_by_country)) {
    const found = shellByCountry[c] || 0;
    lineage.country_deltas[c] = {
      expected,
      found,
      delta: expected - found,
    };
  }

  log("[phase-2a] duplicate risk assessment…");
  const dup = assessDuplicates(records);

  // Holds ledger
  log("[phase-2a] reconciling holds ledger + candidate evidence…");
  const holds = readJson(
    path.join(
      ROOT,
      "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
    ),
    { by_candidate_id: {} }
  );
  const holdEntries = Object.entries(holds.by_candidate_id || {});
  const heldByCountry = {};
  const heldByClass = {};
  const heldByReason = {};
  for (const [, h] of holdEntries) {
    bump(heldByCountry, h.country || "(blank)");
    bump(heldByClass, h.class || "(blank)");
    bump(heldByReason, h.reason || "(blank)");
  }

  const universe = loadMasterUniverseCandidates();
  const hbx = loadHbxCandidates();
  const { merged } = mergeCandidateUniverses(universe, hbx);
  const byCandidateId = new Map(merged.map((c) => [c.candidate_id, c]));

  const holdDetailByCountry = {};
  for (const [cid, h] of holdEntries) {
    const country = h.country || "(blank)";
    if (!holdDetailByCountry[country]) {
      holdDetailByCountry[country] = {
        total_held: 0,
        cvent_only_held: 0,
        hbx_related_held: 0,
        missing_city: 0,
        missing_address: 0,
        missing_website: 0,
        generic_or_weak_name: 0,
        duplicate_ambiguity: 0,
        other_reason: 0,
        by_reason: {},
        by_class: {},
      };
    }
    const row = holdDetailByCountry[country];
    row.total_held += 1;
    bump(row.by_reason, h.reason || "(blank)");
    bump(row.by_class, h.class || "(blank)");
    const c = byCandidateId.get(cid);
    const hasHbx = Boolean(c?.external_ids?.hbx_code);
    const isCvent =
      c?.source_type === "cvent_candidate" ||
      (c?.merged_sources || []).includes("cvent_candidate");
    if (hasHbx) row.hbx_related_held += 1;
    if (isCvent && !hasHbx) row.cvent_only_held += 1;
    if (!c) {
      row.other_reason += 1;
      continue;
    }
    if (isBlank(c.city)) row.missing_city += 1;
    if (isBlank(c.address)) row.missing_address += 1;
    if (isBlank(c.website)) row.missing_website += 1;
    if (/weak|generic|missing_city|insufficient|non_hotel/i.test(h.reason || "")) {
      row.generic_or_weak_name += 1;
    }
    if (/dup|duplicate/i.test(h.reason || "") || /dup/i.test(h.class || "")) {
      row.duplicate_ambiguity += 1;
    }
  }

  // Coverage analysis vs production
  log("[phase-2a] classifying candidate universe vs production…");
  const index = buildCensusIndexFromRecords(records);
  // Adapt byHbx to Map(code -> first record) for classifyAgainstCensus compatibility
  const indexForClassify = {
    ...index,
    byHbx: new Map(
      [...index.byHbx.entries()].map(([k, arr]) => [k, Array.isArray(arr) ? arr[0] : arr])
    ),
  };

  const byMatchClass = {};
  const potentialByCountry = {};
  const existingByCountry = {};
  const sourceStockByCountry = {};
  for (const c of merged) {
    c.merged_sources = c.merged_sources || [c.source_type];
    const cls = classifyAgainstCensus(c, indexForClassify);
    bump(byMatchClass, cls.match_class);
    const country = c.country || "(blank)";
    bump(sourceStockByCountry, country);
    if (
      cls.match_class === MATCH.NEW_HIGH ||
      cls.match_class === MATCH.NEW_MEDIUM
    ) {
      bump(potentialByCountry, country);
    }
    if (
      cls.match_class === MATCH.EXISTING_HIGH ||
      cls.match_class === MATCH.EXISTING_MEDIUM
    ) {
      bump(existingByCountry, country);
    }
  }

  const heldTotal = holdEntries.length;
  const mexicoHeld = heldByCountry.Mexico || 0;
  const colombiaHeld = heldByCountry.Colombia || 0;
  const brazilHeld = heldByCountry.Brazil || 0;

  // Discovered potential = production + unique HOLD candidates (known IDs).
  // Weak HOLDs inflate this vs production-ready inventory; report both.
  const currentlyDiscoveredPotential = productionCount + heldTotal;
  const weakHeldApprox =
    (heldByClass.weak_identity_hold || 0) +
    (heldByClass.shell_insert_with_review || 0) +
    (heldByClass.insufficient_data_hold || 0);
  const qualifiedDiscoveredApprox = productionCount; // production only is qualified
  // Gap vs 15k aspirational target using production+held; if over target, gap=0 but note quality.
  const rawGap = ESTIMATED_CALA_TARGET - currentlyDiscoveredPotential;
  const estimatedUndiscoveredGap = Math.max(0, rawGap);
  // Alternate: gap if weak holds are NOT counted as discovered hotels
  const gapExcludingWeakHolds = Math.max(
    0,
    ESTIMATED_CALA_TARGET - (productionCount + Math.max(0, heldTotal - weakHeldApprox))
  );

  const otherClassifiedPotentials = Math.max(
    0,
    (byMatchClass[MATCH.NEW_HIGH] || 0) +
      (byMatchClass[MATCH.NEW_MEDIUM] || 0) -
      heldTotal
  );

  // Underrepresentation: CALA countries with low production relative to held+source stock
  const underrep = [];
  for (const country of CALA_COUNTRIES) {
    const prod = byCountry[country] || 0;
    const held = heldByCountry[country] || 0;
    const stock = sourceStockByCountry[country] || 0;
    const potential = potentialByCountry[country] || 0;
    underrep.push({
      country,
      production: prod,
      held,
      source_stock: stock,
      potential_new_class: potential,
      production_share_of_stock:
        stock > 0 ? Number(((100 * prod) / stock).toFixed(1)) : null,
    });
  }
  underrep.sort((a, b) => {
    // Prefer large held + low production
    const as = (a.held || 0) + (a.source_stock || 0) - (a.production || 0) * 2;
    const bs = (b.held || 0) + (b.source_stock || 0) - (b.production || 0) * 2;
    return bs - as;
  });

  const countriesLittleOrNoInventory = CALA_COUNTRIES.filter((c) => {
    const prod = byCountry[c] || 0;
    const held = heldByCountry[c] || 0;
    const stock = sourceStockByCountry[c] || 0;
    return prod + held + stock < 25;
  });

  const countriesSourceExhaustedSafe = Object.keys(byCountry)
    .filter((c) => (potentialByCountry[c] || 0) === 0 && (byCountry[c] || 0) > 0)
    .slice(0, 30);

  const provenanceAnomalyCount =
    provenanceAnomalies.length +
    cventValidationLanguage.length +
    shellStateAnomalies.filter((a) => a.severity !== "info").length;
  const protectedCount = protectedFieldAnomalies.length;
  const brandCount = brandShellAnomalies.length;

  const next = recommendNextAction({
    provenance_anomalies: provenanceAnomalyCount,
    protected_field_anomalies: protectedCount,
    brand_shell_anomalies: brandCount,
    hbx_duplicate_groups: duplicateHbxCodes.length,
    duplicate_high_groups: dup.duplicate_high_groups,
    estimated_undiscovered_gap: Math.max(
      estimatedUndiscoveredGap,
      gapExcludingWeakHolds
    ),
    held_total: heldTotal,
    brazil_held: brazilHeld,
  });

  const hasMaterialGaps =
    estimatedUndiscoveredGap >= 2000 ||
    gapExcludingWeakHolds >= 2000 ||
    heldTotal >= 5000 ||
    dup.duplicate_high_groups > 0 ||
    provenanceAnomalyCount > 0 ||
    protectedCount > 0 ||
    brandCount > 0;

  const auditStatus = hasMaterialGaps
    ? PHASE_2A_STATUS.COMPLETE_WITH_GAPS
    : PHASE_2A_STATUS.COMPLETE;

  const founderDecisionRequired =
    next.action === NEXT_ACTION.FOUNDER_STOP ? "YES" : "NO";

  const report = {
    ok: true,
    AUDIT_STATUS: auditStatus,
    PHASE_1_STATUS:
      "production_census_full_cala_shell_universe_exhausted_pending_enrichment",
    objective: PHASE_2A_OBJECTIVE,
    version: PHASE_2A_VERSION,
    production_writes: false,
    production_table_id: CENSUS_TABLE_ID,
    generated_at,
    PRODUCTION_CENSUS_COUNT: productionCount,
    expected_census_count: EXPECTED_CENSUS,
    census_count_delta_vs_expected: productionCount - EXPECTED_CENSUS,
    SHELLS_RECONCILED: {
      expected: manifestMeta.expected_shell_total,
      found_in_production: shellTotal,
      delta: manifestMeta.expected_shell_total - shellTotal,
      lineage,
    },
    part1_production_reconciliation: {
      total: productionCount,
      by_country: byCountry,
      by_production_use_status: byProductionUse,
      by_review_status: byReviewStatus,
      by_enrichment_status: byEnrichment,
      by_public_display_review_status: byPublicDisplay,
      by_radar_display_status: byRadar,
      human_review_required: { true: hrTrue, false: hrFalse, blank: hrBlank },
      shell_state_anomalies_count: shellStateAnomalies.filter(
        (a) => a.severity !== "info"
      ).length,
      shell_state_anomalies_sample: shellStateAnomalies
        .filter((a) => a.severity !== "info")
        .slice(0, 40),
      shell_enrichment_info_sample: shellStateAnomalies
        .filter((a) => a.severity === "info")
        .slice(0, 20),
    },
    part2_shell_lineage: lineage,
    part3_hbx_linkage: {
      total_with_hbx_hotel_code: hbxCodes.length,
      unique_hbx_hotel_codes: uniqueHbx,
      duplicate_hbx_hotel_codes: duplicateHbxCodes.length,
      duplicate_hbx_sample: duplicateHbxCodes.slice(0, 30),
      malformed_hbx_hotel_codes: malformedHbx.length,
      malformed_sample: malformedHbx.slice(0, 20),
      coverage_by_country: hbxByCountry,
      chain_code_distinct: Object.keys(hbxChain).length,
      category_code_distinct: Object.keys(hbxCategory).length,
      linkage_confidence_distribution: hbxLinkage,
      source_status_distribution: hbxSourceStatus,
      content_review_status_distribution: hbxContentReview,
    },
    part4_source_mix: {
      shell_global: shellSourceMix,
      shell_by_country: shellSourceMixByCountry,
      cvent_validation_language_anomalies: cventValidationLanguage.length,
      cvent_validation_language_sample: cventValidationLanguage.slice(0, 20),
      provenance_issue_sample: provenanceAnomalies.slice(0, 30),
    },
    part5_brand_safety: {
      shell_records_audited: shellTotal,
      prohibited_field_hits_on_shells: brandCount,
      anomalies_sample: brandShellAnomalies.slice(0, 40),
      candidate_brand_coverage: coverage(candidateBrandPresent, shellTotal),
      note:
        "Classification B = populated on a shell-identified record. Shell insert path never writes Current Brand / Brand Family / Brand Verified / Brand Status / Company Validated.",
    },
    part6_protected_fields: {
      anomalies_count: protectedCount,
      anomalies_sample: protectedFieldAnomalies.slice(0, 40),
    },
    part7_duplicates: dup,
    part8_holds: {
      total_held: heldTotal,
      by_country: heldByCountry,
      by_class: heldByClass,
      by_reason: heldByReason,
      detail_by_country: holdDetailByCountry,
      mexico_held: mexicoHeld,
      colombia_held: colombiaHeld,
      brazil_held: brazilHeld,
      evidence_note:
        "missing_city/address/website counts can overlap on the same candidate.",
    },
    part9_coverage: {
      production_by_country: byCountry,
      held_by_country: heldByCountry,
      existing_match_by_country: existingByCountry,
      potential_new_by_country: potentialByCountry,
      source_stock_by_country: sourceStockByCountry,
      by_match_class: byMatchClass,
      countries_little_or_no_discovery_inventory: countriesLittleOrNoInventory,
      most_underrepresented: underrep.slice(0, 20),
      brazil: {
        production: byCountry.Brazil || 0,
        held: brazilHeld,
        source_stock: sourceStockByCountry.Brazil || 0,
        potential_new: potentialByCountry.Brazil || 0,
      },
    },
    part10_gap: {
      CURRENT_PRODUCTION_CENSUS: productionCount,
      IDENTIFIED_HELD_UNIQUE_CANDIDATES: heldTotal,
      OTHER_CLASSIFIED_UNIQUE_POTENTIALS_APPROX: otherClassifiedPotentials,
      CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE: currentlyDiscoveredPotential,
      QUALIFIED_PRODUCTION_ONLY: qualifiedDiscoveredApprox,
      WEAK_OR_REVIEW_HELD_APPROX: weakHeldApprox,
      ESTIMATED_CALA_TARGET: ESTIMATED_CALA_TARGET,
      ESTIMATED_UNDISCOVERED_GAP: estimatedUndiscoveredGap,
      ESTIMATED_GAP_EXCLUDING_WEAK_HOLDS: gapExcludingWeakHolds,
      DISCOVERED_BUT_NEEDS_ENRICHMENT: {
        production_shells_pending_enrichment: shellTotal,
        held_weak_identity: heldTotal,
      },
      NOT_YET_DISCOVERED_SOURCE_COVERAGE_GAP: Math.max(
        estimatedUndiscoveredGap,
        gapExcludingWeakHolds
      ),
      precision_note:
        "Universe = production Census + unique HOLD ledger candidates. Many HOLDs are weak Cvent-only and are NOT production-ready hotels. 15k is aspirational. Prefer ESTIMATED_GAP_EXCLUDING_WEAK_HOLDS when judging true source-coverage shortfall vs weak-identity backlog.",
    },
    part11_recommendation: {
      NEXT_RECOMMENDED_ACTION: next.action,
      rationale: next.rationale,
      FOUNDER_DECISION_REQUIRED: founderDecisionRequired,
      FOUNDER_DECISION: null,
    },
    // Flat return fields
    PROVENANCE_ANOMALIES: provenanceAnomalyCount,
    PROTECTED_FIELD_ANOMALIES: protectedCount,
    HBX_CODE_DUPLICATES: duplicateHbxCodes.length,
    HIGH_CONFIDENCE_DUPLICATE_GROUPS: dup.duplicate_high_groups,
    REVIEW_DUPLICATE_GROUPS: dup.duplicate_review_groups,
    TOTAL_HELD_CANDIDATES: heldTotal,
    HELD_BY_COUNTRY: heldByCountry,
    BRAZIL_HELD_COUNT: brazilHeld,
    MEXICO_HELD_COUNT: mexicoHeld,
    COLOMBIA_HELD_COUNT: colombiaHeld,
    CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE: currentlyDiscoveredPotential,
    ESTIMATED_UNDISCOVERED_GAP: Math.max(
      estimatedUndiscoveredGap,
      gapExcludingWeakHolds
    ),
    ESTIMATED_GAP_EXCLUDING_WEAK_HOLDS: gapExcludingWeakHolds,
    MOST_UNDERREPRESENTED_COUNTRIES: underrep.slice(0, 12).map((r) => r.country),
    NEXT_RECOMMENDED_ACTION: next.action,
    FOUNDER_DECISION_REQUIRED: founderDecisionRequired,
    secondary_statuses: {
      duplicate_review_needed:
        dup.duplicate_high_groups + dup.duplicate_review_groups > 0,
      provenance_remediation_needed:
        provenanceAnomalyCount > 0 || protectedCount > 0 || brandCount > 0,
      source_coverage_gaps:
        Math.max(estimatedUndiscoveredGap, gapExcludingWeakHolds) >= 2000 ||
        brazilHeld >= 3000,
      enrichment_backlog: shellTotal > 0,
      weak_identity_hold_backlog: heldTotal >= 1000,
    },
  };

  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/full-cala-phase-2a-global-provenance-coverage-audit.json"
  );
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/full-cala-phase-2a-global-provenance-coverage-audit.md"
  );
  const docMd = path.join(
    ROOT,
    "docs/data-intelligence/full-cala-phase-2a-global-provenance-coverage-audit.md"
  );
  writeJson(reportJson, report);
  const md = renderPhase2aMd(report);
  writeMd(reportMd, md);
  writeMd(docMd, md);
  report.report_paths = {
    json: "reports/research-engine-v2/full-cala-phase-2a-global-provenance-coverage-audit.json",
    md: "reports/research-engine-v2/full-cala-phase-2a-global-provenance-coverage-audit.md",
    docs: "docs/data-intelligence/full-cala-phase-2a-global-provenance-coverage-audit.md",
  };

  log(
    `[phase-2a] STATUS=${auditStatus} census=${productionCount} shells=${shellTotal} held=${heldTotal} next=${next.action}`
  );
  return report;
}

function renderPhase2aMd(r) {
  const topHeld = Object.entries(r.HELD_BY_COUNTRY || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .map(([c, n]) => `| ${c} | ${n} |`)
    .join("\n");
  const topProd = Object.entries(r.part1_production_reconciliation?.by_country || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .map(([c, n]) => `| ${c} | ${n} |`)
    .join("\n");

  return `# Phase 2A — Global Census Provenance + Coverage Audit

**AUDIT_STATUS:** \`${r.AUDIT_STATUS}\`  
**Production writes:** **false** (read-only)  
**Table:** Hotel Property Census (\`${r.production_table_id}\`)  
**Generated:** ${r.generated_at}

## Executive return

| Field | Value |
| --- | ---: |
| PRODUCTION_CENSUS_COUNT | ${r.PRODUCTION_CENSUS_COUNT} |
| Shells expected / found | ${r.SHELLS_RECONCILED?.expected} / ${r.SHELLS_RECONCILED?.found_in_production} |
| PROVENANCE_ANOMALIES | ${r.PROVENANCE_ANOMALIES} |
| PROTECTED_FIELD_ANOMALIES | ${r.PROTECTED_FIELD_ANOMALIES} |
| HBX_CODE_DUPLICATES | ${r.HBX_CODE_DUPLICATES} |
| HIGH_CONFIDENCE_DUPLICATE_GROUPS | ${r.HIGH_CONFIDENCE_DUPLICATE_GROUPS} |
| REVIEW_DUPLICATE_GROUPS | ${r.REVIEW_DUPLICATE_GROUPS} |
| TOTAL_HELD_CANDIDATES | ${r.TOTAL_HELD_CANDIDATES} |
| BRAZIL_HELD_COUNT | ${r.BRAZIL_HELD_COUNT} |
| MEXICO_HELD_COUNT | ${r.MEXICO_HELD_COUNT} |
| COLOMBIA_HELD_COUNT | ${r.COLOMBIA_HELD_COUNT} |
| CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE | ${r.CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE} |
| ESTIMATED_UNDISCOVERED_GAP | ${r.ESTIMATED_UNDISCOVERED_GAP} |
| NEXT_RECOMMENDED_ACTION | ${r.NEXT_RECOMMENDED_ACTION} |
| FOUNDER_DECISION_REQUIRED | ${r.FOUNDER_DECISION_REQUIRED} |

## Gap reconciliation

\`\`\`
CURRENT PRODUCTION CENSUS                         ${r.part10_gap.CURRENT_PRODUCTION_CENSUS}
+ IDENTIFIED HELD UNIQUE CANDIDATES               ${r.part10_gap.IDENTIFIED_HELD_UNIQUE_CANDIDATES}
= CURRENTLY DISCOVERED POTENTIAL UNIVERSE         ${r.part10_gap.CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE}

ESTIMATED CALA TARGET (~)                         ${r.part10_gap.ESTIMATED_CALA_TARGET}
− DISCOVERED POTENTIAL                            ${r.part10_gap.CURRENTLY_DISCOVERED_POTENTIAL_UNIVERSE}
= ESTIMATED UNDISCOVERED / SOURCE COVERAGE GAP    ${r.part10_gap.ESTIMATED_UNDISCOVERED_GAP}
\`\`\`

${r.part10_gap.precision_note}

### Discovered but needs enrichment
- Production shells pending enrichment: **${r.part10_gap.DISCOVERED_BUT_NEEDS_ENRICHMENT.production_shells_pending_enrichment}**
- Held weak identity: **${r.part10_gap.DISCOVERED_BUT_NEEDS_ENRICHMENT.held_weak_identity}**

## Production by country (top)

| Country | Count |
| --- | ---: |
${topProd}

## Held candidates (top)

| Country | Held |
| --- | ---: |
${topHeld}

## Recommendation

**${r.NEXT_RECOMMENDED_ACTION}**

${r.part11_recommendation?.rationale || ""}

## Secondary flags

- Duplicate review needed: **${r.secondary_statuses?.duplicate_review_needed}**
- Provenance remediation needed: **${r.secondary_statuses?.provenance_remediation_needed}**
- Source coverage gaps: **${r.secondary_statuses?.source_coverage_gaps}**
- Enrichment backlog: **${r.secondary_statuses?.enrichment_backlog}**

## Safety

- No production Census writes
- No Brand Explorer / Brand Setup / VIC / old Census writes
- Shell identity gate not weakened
- Phase 1 shell insertion not restarted
`;
}
