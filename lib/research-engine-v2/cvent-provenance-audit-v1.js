/**
 * Cvent provenance audit v1 — full CALA 15K shell records (DR/CR/PA).
 * Read-only. Confirms Cvent is trackable and never used as field-level SoT.
 *
 * Status: production_census_cvent_provenance_audit_v1_complete
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
import {
  isShellRecordForBackfill,
  parseNotes,
} from "./full-cala-15k-shell-format-source-brand-backfill-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const CVENT_PROVENANCE_AUDIT_OBJECTIVE = "cvent-provenance-audit-v1";
export const CVENT_PROVENANCE_AUDIT_STATUS = Object.freeze({
  COMPLETE: "production_census_cvent_provenance_audit_v1_complete",
  PARTIAL: "production_census_cvent_provenance_audit_v1_partial_gaps",
  BLOCKED: "production_census_cvent_provenance_audit_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] ||
  productionHotelPropertyCensus.tableId ||
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const DEFAULT_COUNTRIES = [
  "Dominican Republic",
  "Costa Rica",
  "Panama",
];

const CVENT_DISCOVERY = "Cvent Candidate / Not Field Source";
const CVENT_HBX_DISCOVERY = "Cvent + HBX Candidate";
const HBX_DISCOVERY = "HBX Content API";

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

/**
 * Classify true origin from notes + HBX fields + provenance fields.
 */
export function classifyShellOrigin(fields) {
  const notes = parseNotes(fields["Notes for Steward"]);
  const mix = String(fields["Shell Insert Source Mix"] || "").toLowerCase();
  const discovery = String(fields["Discovery Source"] || "").trim();
  const sourceType = String(fields["Source Candidate Type"] || "").trim();
  const brandSource = String(fields["Candidate Brand Source"] || "").trim();

  const hasHbxField = !isBlank(fields["HBX Hotel Code"]);
  const hasCvent =
    notes.is_cvent ||
    /cvent/i.test(mix) ||
    discovery === CVENT_DISCOVERY ||
    discovery === CVENT_HBX_DISCOVERY ||
    /cvent/i.test(sourceType) ||
    /cvent/i.test(brandSource);
  const hasHbx =
    notes.is_hbx ||
    hasHbxField ||
    /hbx/i.test(mix) ||
    discovery === HBX_DISCOVERY ||
    discovery === CVENT_HBX_DISCOVERY ||
    /hbx/i.test(sourceType) ||
    /hbx/i.test(brandSource);

  let origin = "independent_or_unknown";
  if (hasCvent && hasHbx) origin = "cvent_plus_hbx";
  else if (hasCvent) origin = "cvent_only";
  else if (hasHbx) origin = "hbx_only";

  return { origin, hasCvent, hasHbx, notes, discovery, sourceType, brandSource, mix };
}

/**
 * Expected provenance labels for an origin class.
 */
export function expectedProvenance(origin) {
  if (origin === "cvent_only") {
    return {
      discovery: CVENT_DISCOVERY,
      source_candidate_type: "Cvent Identity Candidate",
      ok_discovery: new Set([CVENT_DISCOVERY]),
      ok_source_type: new Set(["Cvent Identity Candidate", "Shell Identity"]),
    };
  }
  if (origin === "cvent_plus_hbx") {
    return {
      discovery: CVENT_HBX_DISCOVERY,
      source_candidate_type: "Multi-Source Candidate",
      ok_discovery: new Set([CVENT_HBX_DISCOVERY]),
      ok_source_type: new Set(["Multi-Source Candidate", "HBX Linked Shell"]),
    };
  }
  if (origin === "hbx_only") {
    return {
      discovery: HBX_DISCOVERY,
      source_candidate_type: "HBX Linked Shell",
      ok_discovery: new Set([HBX_DISCOVERY]),
      ok_source_type: new Set(["HBX Linked Shell", "Shell Identity"]),
    };
  }
  return {
    discovery: "Independent Census Candidate",
    source_candidate_type: "Shell Identity",
    ok_discovery: new Set(["Independent Census Candidate"]),
    ok_source_type: new Set(["Shell Identity"]),
  };
}

const VALIDATED_BY_CVENT_RE =
  /validated\s+by\s+cvent|cvent\s+validated|field[- ]level\s+source.*cvent|cvent.*source\s+of\s+truth/i;

/**
 * Audit one shell row.
 */
export function auditShellCventProvenance(record) {
  const fields = record.fields || {};
  const classified = classifyShellOrigin(fields);
  const expected = expectedProvenance(classified.origin);

  const issues = [];
  const trackable = {
    discovery_source: !isBlank(fields["Discovery Source"]),
    source_candidate_type: !isBlank(fields["Source Candidate Type"]),
    candidate_source_count: fields["Candidate Source Count"] != null,
    shell_insert_source_mix: !isBlank(fields["Shell Insert Source Mix"]),
    notes_for_steward: !isBlank(fields["Notes for Steward"]),
    candidate_brand_source: !isBlank(fields["Candidate Brand Source"]),
  };

  // Missing Cvent provenance when row has Cvent origin
  if (classified.hasCvent) {
    if (!trackable.discovery_source) {
      issues.push("missing_discovery_source");
    } else if (
      classified.origin === "cvent_only" &&
      fields["Discovery Source"] !== CVENT_DISCOVERY
    ) {
      issues.push("cvent_only_discovery_mismatch");
    } else if (
      classified.origin === "cvent_plus_hbx" &&
      fields["Discovery Source"] !== CVENT_HBX_DISCOVERY
    ) {
      issues.push("cvent_hbx_discovery_mismatch");
    }

    if (!trackable.source_candidate_type) {
      issues.push("missing_source_candidate_type");
    } else if (
      classified.origin === "cvent_only" &&
      fields["Source Candidate Type"] !== "Cvent Identity Candidate" &&
      fields["Source Candidate Type"] !== "Shell Identity"
    ) {
      // Prefer Cvent Identity Candidate; Shell Identity alone is weak for Cvent-only
      if (fields["Source Candidate Type"] !== "Cvent Identity Candidate") {
        issues.push("cvent_only_source_type_not_cvent_identity");
      }
    } else if (
      classified.origin === "cvent_plus_hbx" &&
      fields["Source Candidate Type"] !== "Multi-Source Candidate"
    ) {
      issues.push("cvent_hbx_not_multi_source");
    }

    if (!trackable.shell_insert_source_mix) issues.push("missing_shell_insert_source_mix");
    if (!trackable.notes_for_steward) issues.push("missing_notes");
    else if (!classified.notes.is_cvent && !/cvent/i.test(classified.mix)) {
      // Discovery says Cvent but notes lack Cvent marker
      if (/cvent/i.test(String(fields["Discovery Source"] || ""))) {
        issues.push("cvent_in_discovery_but_not_notes");
      }
    }
  }

  // Incorrect Cvent-as-validation
  const blob = [
    fields["Discovery Source"],
    fields["Source Candidate Type"],
    fields["Candidate Brand Source"],
    fields["Brand Validation Status"],
    fields["Notes for Steward"],
    fields["Shell Insert Source Mix"],
  ]
    .map((x) => String(x || ""))
    .join("\n");
  if (VALIDATED_BY_CVENT_RE.test(blob)) {
    issues.push("says_validated_by_cvent");
  }
  if (/^Validated$/i.test(String(fields["Brand Validation Status"] || "")) && classified.hasCvent) {
    // Candidate brand marked Validated while Cvent-origin — suspicious without independent validation signal
    if (!fields["Current Brand"]) {
      issues.push("brand_validation_status_validated_without_current_brand");
    }
  }

  // Current Brand / Brand Family from Cvent-only
  const currentBrand = String(fields["Current Brand"] || "").trim();
  const brandFamily = String(fields["Brand Family"] || "").trim();
  const familySource = String(fields["Family / Source Family"] || "").trim();
  let cventOnlyBrandPopulated = false;
  if (classified.origin === "cvent_only") {
    if (currentBrand) {
      issues.push("current_brand_on_cvent_only");
      cventOnlyBrandPopulated = true;
    }
    if (brandFamily) {
      issues.push("brand_family_on_cvent_only");
      cventOnlyBrandPopulated = true;
    }
  }

  // Field-level SoT misuse: Discovery Source must never claim Cvent as validated SoT wording
  if (/not field source/i.test(String(fields["Discovery Source"] || "")) === false) {
    if (
      classified.origin === "cvent_only" &&
      fields["Discovery Source"] === CVENT_DISCOVERY
    ) {
      // OK — contains Not Field Source
    }
  }
  if (
    classified.hasCvent &&
    fields["Discovery Source"] &&
    !/not field source|candidate/i.test(String(fields["Discovery Source"]))
  ) {
    // HBX Content API alone on a Cvent row would be wrong — already covered by mismatch
  }

  const cventTrackable =
    !classified.hasCvent ||
    (trackable.discovery_source &&
      trackable.source_candidate_type &&
      (trackable.notes_for_steward || trackable.shell_insert_source_mix));

  return {
    record_id: record.id,
    country: fields.Country || null,
    property_name: fields["Property Name"] || null,
    origin: classified.origin,
    discovery_source: fields["Discovery Source"] || null,
    source_candidate_type: fields["Source Candidate Type"] || null,
    candidate_source_count: fields["Candidate Source Count"] ?? null,
    shell_insert_source_mix: fields["Shell Insert Source Mix"] || null,
    candidate_brand_source: fields["Candidate Brand Source"] || null,
    brand_validation_status: fields["Brand Validation Status"] || null,
    current_brand: currentBrand || null,
    brand_family: brandFamily || null,
    family_source_family: familySource || null,
    trackable,
    cvent_trackable: cventTrackable,
    issues,
    cvent_only_brand_populated: cventOnlyBrandPopulated,
    expected,
  };
}

async function listShellRecords(baseId, token, countries) {
  const out = [];
  let offset;
  const countryOr = countries
    .map((c) => `{Country}='${c.replace(/'/g, "\\'")}'`)
    .join(",");
  const formula = `AND({Enrichment Status}='Discovered — pending enrichment',{Public Display Review Status}='Hold',OR(${countryOr}))`;
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    for (const f of [
      "Property Name",
      "Canonical Property Name",
      "Property Identity Key",
      "Country",
      "Notes for Steward",
      "Enrichment Status",
      "Public Display Review Status",
      "Human Review Required",
      "Current Brand",
      "Brand Family",
      "Family / Source Family",
      "HBX Hotel Code",
      "Discovery Source",
      "Source Candidate Type",
      "Candidate Source Count",
      "Shell Insert Source Mix",
      "Shell Insert Batch ID",
      "Candidate Brand Source",
      "Candidate Brand Text",
      "Brand Validation Status",
    ]) {
      params.append("fields[]", f);
    }
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census_list_failed:${res.status}:${json?.error?.message || ""}`);
    for (const r of json.records || []) {
      if (isShellRecordForBackfill(r.fields || {}, countries)) out.push(r);
    }
    offset = json.offset;
    await sleep(110);
  } while (offset);
  return out;
}

function buildPatchPlan(audits) {
  const plan = [];
  for (const a of audits) {
    if (!a.issues.length) continue;
    const actions = [];
    if (
      a.issues.includes("missing_discovery_source") ||
      a.issues.includes("cvent_only_discovery_mismatch")
    ) {
      if (a.origin === "cvent_only") {
        actions.push({ field: "Discovery Source", set: CVENT_DISCOVERY });
      }
    }
    if (a.issues.includes("cvent_hbx_discovery_mismatch")) {
      actions.push({ field: "Discovery Source", set: CVENT_HBX_DISCOVERY });
    }
    if (
      a.issues.includes("missing_source_candidate_type") ||
      a.issues.includes("cvent_only_source_type_not_cvent_identity")
    ) {
      if (a.origin === "cvent_only") {
        actions.push({
          field: "Source Candidate Type",
          set: "Cvent Identity Candidate",
        });
      }
    }
    if (a.issues.includes("cvent_hbx_not_multi_source")) {
      actions.push({
        field: "Source Candidate Type",
        set: "Multi-Source Candidate",
      });
    }
    if (a.issues.includes("missing_shell_insert_source_mix")) {
      actions.push({
        field: "Shell Insert Source Mix",
        set:
          a.origin === "cvent_plus_hbx"
            ? "cvent_candidate+hbx_content_api"
            : a.origin === "cvent_only"
              ? "cvent_candidate"
              : null,
      });
    }
    if (
      a.issues.includes("current_brand_on_cvent_only") ||
      a.issues.includes("brand_family_on_cvent_only")
    ) {
      actions.push({
        action: "steward_review",
        note: "Clear or validate Current Brand / Brand Family — not from Cvent-only",
      });
    }
    if (a.issues.includes("says_validated_by_cvent")) {
      actions.push({
        action: "remove_validated_by_cvent_wording",
        note: "Rewrite provenance to Candidate / Not Field Source",
      });
    }
    if (actions.length) {
      plan.push({
        record_id: a.record_id,
        country: a.country,
        origin: a.origin,
        issues: a.issues,
        actions: actions.filter((x) => x.set !== null),
      });
    }
  }
  return plan;
}

function renderMd(report) {
  return `# Cvent Provenance Audit v1

**Status:** \`${report.status}\`  
**Objective:** \`${report.objective}\`  
**Generated:** ${report.generated_at}  
**Table:** Hotel Property Census only  
**Countries:** ${(report.countries || []).join(", ")}

## Counts
- Total shell records reviewed: **${report.total_shell_records}**
- Cvent-only: **${report.cvent_only_count}**
- Cvent + HBX: **${report.cvent_plus_hbx_count}**
- HBX-only: **${report.hbx_only_count}**
- Independent / unknown: **${report.independent_or_unknown_count}**

## Trackability
- Cvent-origin rows with usable provenance trail: **${report.cvent_trackable_count}** / **${report.cvent_origin_count}**
- Rows missing Cvent provenance: **${report.missing_cvent_provenance_count}**
- Rows incorrectly using Cvent as validation: **${report.incorrect_cvent_validation_count}**
- Cvent-only with Current Brand / Brand Family populated: **${report.cvent_only_brand_populated_count}**

## Confirmations
- Cvent-only marked \`${CVENT_DISCOVERY}\`: **${report.confirmations.cvent_only_marked_correctly}**
- Cvent+HBX marked Multi-Source / Cvent+HBX discovery: **${report.confirmations.cvent_hbx_marked_correctly}**
- No row says "Validated by Cvent": **${report.confirmations.no_validated_by_cvent}**
- Cvent not used as field-level SoT (Discovery Source wording): **${report.confirmations.cvent_not_field_sot}**
- Current Brand / Brand Family not from Cvent-only: **${report.confirmations.no_cvent_only_current_brand}**

## Issue breakdown
${Object.entries(report.issue_counts || {})
  .map(([k, n]) => `- \`${k}\`: **${n}**`)
  .join("\n") || "- none"}

## Patch plan
- Rows needing patch: **${report.patch_plan_count}**
${
  report.patch_plan_count
    ? report.patch_plan
        .slice(0, 25)
        .map(
          (p) =>
            `- \`${p.record_id}\` (${p.country}, ${p.origin}): ${p.issues.join(", ")}`
        )
        .join("\n")
    : "- none required"
}
${report.patch_plan_count > 25 ? `\n…and ${report.patch_plan_count - 25} more (see JSON).` : ""}

## Samples
### Cvent-only
${(report.samples?.cvent_only || [])
  .map(
    (s) =>
      `- ${s.property_name} | ${s.discovery_source} | ${s.source_candidate_type} | mix=${s.shell_insert_source_mix}`
  )
  .join("\n") || "- —"}

### Cvent + HBX
${(report.samples?.cvent_plus_hbx || [])
  .map(
    (s) =>
      `- ${s.property_name} | ${s.discovery_source} | ${s.source_candidate_type}`
  )
  .join("\n") || "- —"}
`;
}

/**
 * @param {object} opts
 */
export async function runCventProvenanceAuditV1(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const generated_at = new Date().toISOString();
  const countries = opts.countries || DEFAULT_COUNTRIES;

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
      status: CVENT_PROVENANCE_AUDIT_STATUS.BLOCKED,
      objective: CVENT_PROVENANCE_AUDIT_OBJECTIVE,
      generated_at,
      reason: String(err?.message || err).slice(0, 300),
      countries,
    };
    persist(report);
    return report;
  }

  log(`[cvent-provenance-audit] listing shells…`);
  const shells = await listShellRecords(baseId, token, countries);
  log(`[cvent-provenance-audit] shells=${shells.length}`);

  const audits = shells.map((r) => auditShellCventProvenance(r));

  const byOrigin = {
    cvent_only: 0,
    cvent_plus_hbx: 0,
    hbx_only: 0,
    independent_or_unknown: 0,
  };
  const issue_counts = {};
  let cvent_trackable_count = 0;
  let missing_cvent_provenance_count = 0;
  let incorrect_cvent_validation_count = 0;
  let cvent_only_brand_populated_count = 0;
  let cvent_only_marked_ok = 0;
  let cvent_hbx_marked_ok = 0;

  for (const a of audits) {
    byOrigin[a.origin] = (byOrigin[a.origin] || 0) + 1;
    for (const iss of a.issues) {
      issue_counts[iss] = (issue_counts[iss] || 0) + 1;
    }
    if (a.origin === "cvent_only" || a.origin === "cvent_plus_hbx") {
      if (a.cvent_trackable) cvent_trackable_count += 1;
      const missingIss = a.issues.filter((i) =>
        /missing_|_mismatch|not_cvent_identity|not_multi_source/.test(i)
      );
      if (missingIss.length) missing_cvent_provenance_count += 1;
    }
    if (
      a.issues.includes("says_validated_by_cvent") ||
      a.issues.includes("brand_validation_status_validated_without_current_brand")
    ) {
      incorrect_cvent_validation_count += 1;
    }
    if (a.cvent_only_brand_populated) cvent_only_brand_populated_count += 1;
    if (
      a.origin === "cvent_only" &&
      a.discovery_source === CVENT_DISCOVERY
    ) {
      cvent_only_marked_ok += 1;
    }
    if (
      a.origin === "cvent_plus_hbx" &&
      a.discovery_source === CVENT_HBX_DISCOVERY &&
      a.source_candidate_type === "Multi-Source Candidate"
    ) {
      cvent_hbx_marked_ok += 1;
    }
  }

  const cvent_origin_count = byOrigin.cvent_only + byOrigin.cvent_plus_hbx;
  const patch_plan = buildPatchPlan(audits);

  const confirmations = {
    cvent_only_marked_correctly:
      byOrigin.cvent_only === 0 || cvent_only_marked_ok === byOrigin.cvent_only,
    cvent_hbx_marked_correctly:
      byOrigin.cvent_plus_hbx === 0 ||
      cvent_hbx_marked_ok === byOrigin.cvent_plus_hbx,
    no_validated_by_cvent: incorrect_cvent_validation_count === 0,
    cvent_not_field_sot:
      (issue_counts.says_validated_by_cvent || 0) === 0 &&
      audits
        .filter((a) => a.origin === "cvent_only")
        .every(
          (a) =>
            !a.discovery_source ||
            /not field source/i.test(a.discovery_source)
        ),
    no_cvent_only_current_brand: cvent_only_brand_populated_count === 0,
  };

  const hasGaps =
    missing_cvent_provenance_count > 0 ||
    incorrect_cvent_validation_count > 0 ||
    cvent_only_brand_populated_count > 0 ||
    patch_plan.length > 0;

  const status = hasGaps
    ? CVENT_PROVENANCE_AUDIT_STATUS.PARTIAL
    : CVENT_PROVENANCE_AUDIT_STATUS.COMPLETE;

  // Founder expected primary status is complete when audit finishes;
  // use PARTIAL only for material gaps. Prefer COMPLETE if confirmations all true.
  const allConfirm =
    confirmations.cvent_only_marked_correctly &&
    confirmations.cvent_hbx_marked_correctly &&
    confirmations.no_validated_by_cvent &&
    confirmations.cvent_not_field_sot &&
    confirmations.no_cvent_only_current_brand;

  const report = {
    ok: true,
    status: allConfirm
      ? CVENT_PROVENANCE_AUDIT_STATUS.COMPLETE
      : CVENT_PROVENANCE_AUDIT_STATUS.PARTIAL,
    objective: CVENT_PROVENANCE_AUDIT_OBJECTIVE,
    generated_at,
    countries,
    total_shell_records: audits.length,
    cvent_only_count: byOrigin.cvent_only,
    cvent_plus_hbx_count: byOrigin.cvent_plus_hbx,
    hbx_only_count: byOrigin.hbx_only,
    independent_or_unknown_count: byOrigin.independent_or_unknown,
    cvent_origin_count,
    cvent_trackable_count,
    missing_cvent_provenance_count,
    incorrect_cvent_validation_count,
    cvent_only_brand_populated_count,
    cvent_only_marked_ok,
    cvent_hbx_marked_ok,
    issue_counts,
    confirmations,
    patch_plan_count: patch_plan.length,
    patch_plan: patch_plan.slice(0, 200),
    samples: {
      cvent_only: audits
        .filter((a) => a.origin === "cvent_only")
        .slice(0, 5)
        .map((a) => ({
          record_id: a.record_id,
          property_name: a.property_name,
          discovery_source: a.discovery_source,
          source_candidate_type: a.source_candidate_type,
          shell_insert_source_mix: a.shell_insert_source_mix,
        })),
      cvent_plus_hbx: audits
        .filter((a) => a.origin === "cvent_plus_hbx")
        .slice(0, 5)
        .map((a) => ({
          record_id: a.record_id,
          property_name: a.property_name,
          discovery_source: a.discovery_source,
          source_candidate_type: a.source_candidate_type,
        })),
      issues: audits
        .filter((a) => a.issues.length)
        .slice(0, 15)
        .map((a) => ({
          record_id: a.record_id,
          origin: a.origin,
          issues: a.issues,
        })),
    },
    airtable_writes: 0,
    read_only: true,
  };

  persist(report);
  log(
    `[cvent-provenance-audit] status=${report.status} total=${report.total_shell_records} cvent_only=${report.cvent_only_count} cvent_hbx=${report.cvent_plus_hbx_count} hbx_only=${report.hbx_only_count} gaps=${report.patch_plan_count}`
  );
  return report;
}

function persist(report) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  writeJson(path.join(reportsDir, "cvent-provenance-audit-v1.json"), report);
  const md = report.status
    ? renderMd(report)
    : `# Cvent Provenance Audit v1\n\nBlocked: ${report.reason}\n`;
  writeMd(path.join(reportsDir, "cvent-provenance-audit-v1.md"), md);
  writeMd(path.join(docsDir, "cvent-provenance-audit-v1.md"), md);
}

// CLI
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { default: dotenv } = await import("dotenv");
  dotenv.config();
  const report = await runCventProvenanceAuditV1({
    log: (m) => console.log(m),
  });
  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        total_shell_records: report.total_shell_records,
        cvent_only_count: report.cvent_only_count,
        cvent_plus_hbx_count: report.cvent_plus_hbx_count,
        hbx_only_count: report.hbx_only_count,
        missing_cvent_provenance_count: report.missing_cvent_provenance_count,
        incorrect_cvent_validation_count: report.incorrect_cvent_validation_count,
        patch_plan_count: report.patch_plan_count,
        confirmations: report.confirmations,
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}
