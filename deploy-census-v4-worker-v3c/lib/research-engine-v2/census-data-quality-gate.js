/**
 * Census Data Quality Gate — Autopilot queue + reporting.
 *
 * Runs city/state normalization + canonical identity cleanup (High only).
 * Gates inserts and Mapbox coordinate completion on clean core identity.
 *
 * Write target: Hotel Property Census only (tbl9aY5ijiuIzzWam).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  AUTOPILOT_ALLOWED_WRITE_FIELDS,
  AUTOPILOT_FORBIDDEN_FIELDS,
  isForbiddenAutopilotField,
} from "./census-autopilot-field-allowlist.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import {
  CANONICAL_PROPERTY_NAME_FIELD,
  buildCanonicalDuplicateIndex,
  proposeCanonicalPropertyNameWrite,
} from "./census-canonical-property-name.js";
import { isPropertyLevelUrl } from "./production-census-description-extraction.js";
import {
  CITY_CLASS,
  classifyAndNormalizeCityState,
} from "./census-city-state-normalizer.js";
import {
  CORE_IDENTITY_QUALITY_VERSION,
  QUALITY_GATE_STATUS,
  classifyCoreIdentityQuality,
  evaluateCoordinateIdentityGate,
  evaluateInsertIdentityGate,
} from "./census-core-identity-quality.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const DATA_QUALITY_GATE_VERSION = "census-data-quality-gate-v1";
export const CORE_IDENTITY_QUALITY_QUEUE_ID = "core_identity_quality";
export const CITY_STATE_NORMALIZATION_QUEUE_ID = "city_state_normalization";

export const DATA_QUALITY_GATE_COMPLETION_STATUS = Object.freeze({
  APPLIED_CLEAN: "production_census_core_identity_quality_gate_applied_clean",
  PARTIAL_STEWARD:
    "production_census_core_identity_quality_gate_partial_steward_remaining",
  READY_NEEDS_PRODUCTION_CYCLE:
    "production_census_core_identity_quality_gate_ready_needs_production_cycle",
  BLOCKED: "production_census_core_identity_quality_gate_blocked",
});

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

function sanitizePatch(patch) {
  const out = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (isForbiddenAutopilotField(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
      throw new Error(`protected_field_in_quality_gate_patch:${k}`);
    }
    if (!AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(k)) continue;
    if (v == null || v === "") continue;
    out[k] = v;
  }
  return out;
}

/**
 * Run core identity quality gate over Census records (controlled: proposals only).
 */
export function runCoreIdentityQualityGate(opts = {}) {
  const censusRecords = opts.censusRecords || [];
  const sot = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: productionHotelPropertyCensus.tableId,
  });
  if (!sot.ok) {
    return {
      ok: false,
      status: DATA_QUALITY_GATE_COMPLETION_STATUS.BLOCKED,
      error: BLOCKED_WRONG_CENSUS_TARGET,
      source_of_truth: sot,
      proposals: [],
    };
  }

  const dupIndex = buildCanonicalDuplicateIndex(censusRecords, {
    isPropertyLevelUrl,
  });
  const counters = {
    records_scanned: censusRecords.length,
    quality_pass: 0,
    autofix_high: 0,
    source_lookup_needed: 0,
    steward_review_required: 0,
    duplicate_risk: 0,
    blocked_identity_conflict: 0,
    blocked_dirty_core_identity: 0,
    unknown_city: 0,
    descriptor_city: 0,
    city_state_mixed: 0,
    case_or_accent_normalized: 0,
    city_state_split: 0,
    canonical_blank: 0,
    canonical_conflict: 0,
    canonical_fixes: 0,
    safe_autofix_proposals: 0,
    coordinate_blocked_dirty_identity: 0,
  };

  const proposals = [];
  const steward = [];
  const blocked = [];
  const examples = [];
  const byReadiness = {};

  for (const rec of censusRecords) {
    const classified = classifyCoreIdentityQuality(rec, {
      canonicalFieldExists: opts.canonicalFieldExists !== false,
    });
    const city = classified.city;
    const canon = classified.canonical;

    byReadiness[classified.quality.readiness] =
      (byReadiness[classified.quality.readiness] || 0) + 1;

    if (city.class === CITY_CLASS.UNKNOWN) counters.unknown_city += 1;
    if (city.class === CITY_CLASS.DESCRIPTOR) counters.descriptor_city += 1;
    if (city.class === CITY_CLASS.MIXED_UNRESOLVED) counters.city_state_mixed += 1;
    if (
      city.class === CITY_CLASS.CASE_NORMALIZE ||
      city.class === CITY_CLASS.ACCENT_NORMALIZE
    ) {
      counters.case_or_accent_normalized += 1;
    }
    if (city.class === CITY_CLASS.SPLIT_CITY_STATE) counters.city_state_split += 1;
    if (isBlank(rec.fields?.[CANONICAL_PROPERTY_NAME_FIELD])) counters.canonical_blank += 1;
    if (
      String(canon.status || "").includes("conflict") ||
      canon.status === "populated_conflict_needs_review"
    ) {
      counters.canonical_conflict += 1;
    }
    if (classified.coordinate_blocked) counters.coordinate_blocked_dirty_identity += 1;

    /** @type {Record<string, unknown>} */
    const patch = {};

    if (classified.patch_city_state) {
      Object.assign(patch, classified.patch_city_state);
    }

    // Canonical via duplicate-safe proposer
    const canonProp = proposeCanonicalPropertyNameWrite(rec, dupIndex, {
      fieldExists: opts.canonicalFieldExists !== false,
      isPropertyLevelUrl,
    });
    if (canonProp.action === "autofill" || canonProp.action === "cleanup") {
      Object.assign(patch, canonProp.patch || {});
      counters.canonical_fixes += 1;
    } else if (canonProp.action === "steward") {
      classified.gate_status = QUALITY_GATE_STATUS.DUPLICATE_RISK;
      counters.duplicate_risk += 1;
      steward.push({
        record_id: rec.id,
        reason: canonProp.classified?.reason || "canonical_duplicate_risk",
        field: CANONICAL_PROPERTY_NAME_FIELD,
        existing: canonProp.classified?.existing,
        candidate: canonProp.classified?.candidate,
      });
    }

    const sanitized = sanitizePatch(patch);
    if (Object.keys(sanitized).length) {
      counters.safe_autofix_proposals += 1;
      counters.autofix_high += 1;
      if (examples.length < 30) {
        examples.push({
          record_id: rec.id,
          before: Object.fromEntries(
            Object.keys(sanitized).map((k) => [k, rec.fields?.[k] ?? null])
          ),
          after: sanitized,
          gate_status: QUALITY_GATE_STATUS.AUTOFIX_HIGH,
        });
      }
      proposals.push({
        record_id: rec.id,
        identity_key: rec.fields?.[MAP_FIRST_PASS.identityKey] || null,
        property_name: rec.fields?.[MAP_FIRST_PASS.propertyName] || null,
        brand: rec.fields?.[MAP_FIRST_PASS.currentBrand] || null,
        family: rec.fields?.[MAP_FIRST_PASS.family] || null,
        queue: CORE_IDENTITY_QUALITY_QUEUE_ID,
        action: "propose_high_write",
        confidence: "High",
        write_allowed_now: true,
        allow_normalization_overwrite: true,
        patch: sanitized,
        current_fields: Object.fromEntries(
          Object.keys(sanitized).map((k) => [k, rec.fields?.[k] ?? null])
        ),
        method: "core_identity_quality_gate",
        notes: "High-only city/state/canonical normalization; no weak inference",
        quality_score: classified.quality.score,
        readiness: classified.quality.readiness,
      });
    } else {
      switch (classified.gate_status) {
        case QUALITY_GATE_STATUS.QUALITY_PASS:
          counters.quality_pass += 1;
          break;
        case QUALITY_GATE_STATUS.SOURCE_LOOKUP_NEEDED:
          counters.source_lookup_needed += 1;
          break;
        case QUALITY_GATE_STATUS.STEWARD_REVIEW_REQUIRED:
          counters.steward_review_required += 1;
          steward.push({
            record_id: rec.id,
            reason: classified.blockers.join(",") || classified.gate_status,
            city_class: city.class,
            canonical_status: canon.status,
          });
          break;
        case QUALITY_GATE_STATUS.DUPLICATE_RISK:
          counters.duplicate_risk += 1;
          break;
        case QUALITY_GATE_STATUS.BLOCKED_IDENTITY_CONFLICT:
          counters.blocked_identity_conflict += 1;
          blocked.push({ record_id: rec.id, reason: classified.gate_status });
          break;
        case QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY:
          counters.blocked_dirty_core_identity += 1;
          blocked.push({
            record_id: rec.id,
            reason: classified.blockers.join(",") || classified.gate_status,
            city_class: city.class,
            canonical_status: canon.status,
          });
          break;
        case QUALITY_GATE_STATUS.AUTOFIX_HIGH:
          // pending fix but no patch (e.g. canonical deferred) — count steward/source
          counters.steward_review_required += 1;
          break;
        default:
          counters.steward_review_required += 1;
      }
    }
  }

  let status = DATA_QUALITY_GATE_COMPLETION_STATUS.READY_NEEDS_PRODUCTION_CYCLE;
  if (counters.safe_autofix_proposals === 0 && counters.steward_review_required === 0 && counters.blocked_dirty_core_identity === 0) {
    status = DATA_QUALITY_GATE_COMPLETION_STATUS.APPLIED_CLEAN;
  } else if (
    counters.steward_review_required > 0 ||
    counters.blocked_dirty_core_identity > 0 ||
    counters.duplicate_risk > 0
  ) {
    status =
      counters.safe_autofix_proposals > 0
        ? DATA_QUALITY_GATE_COMPLETION_STATUS.PARTIAL_STEWARD
        : DATA_QUALITY_GATE_COMPLETION_STATUS.PARTIAL_STEWARD;
  }

  const report = {
    ok: true,
    version: DATA_QUALITY_GATE_VERSION,
    core_identity_version: CORE_IDENTITY_QUALITY_VERSION,
    queue_id: CORE_IDENTITY_QUALITY_QUEUE_ID,
    generated_at: new Date().toISOString(),
    status,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    airtable_writes: false,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    counters,
    by_readiness: byReadiness,
    exact_fields_written: ["City", "State / Region", CANONICAL_PROPERTY_NAME_FIELD],
    proposals,
    steward_review: steward.slice(0, 300),
    blocked: blocked.slice(0, 300),
    before_after_examples: examples,
    high_proposals: proposals.length,
  };

  if (opts.writeReports !== false) {
    writeDataQualityGateReports(report, { runDir: opts.runDir || null });
  }

  return report;
}

export function writeDataQualityGateReports(report, opts = {}) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(
    reportsDir,
    "production-census-core-identity-quality-gate.json"
  );
  const mdPath = path.join(
    reportsDir,
    "production-census-core-identity-quality-gate.md"
  );
  const docsPath = path.join(
    docsDir,
    "production-census-core-identity-quality-gate.md"
  );
  const md = renderDataQualityGateMarkdown(report);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(
    docsPath,
    `# Census Core Identity Quality Gate

${md}

## Production-cycle order

1. source_discovery  
2. core_identity_quality  
3. clean inserts only  
4. canonical + city/state (via this gate / key_field_completion)  
5. key_field_completion  
6. address_confirmation  
7. coordinate_completion (only if identity gate passes)  
8. enrichment queues  
9. radar/public readiness  

## Commands

\`\`\`bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \\
  --strategy fastest-safe --queue core_identity_quality --run-until-complete --batch-size 250
\`\`\`
`,
    "utf8"
  );

  if (opts.runDir) {
    fs.mkdirSync(opts.runDir, { recursive: true });
    fs.writeFileSync(
      path.join(opts.runDir, "core-identity-quality-gate.json"),
      JSON.stringify(report, null, 2),
      "utf8"
    );
  }

  return { jsonPath, mdPath, docsPath };
}

export function renderDataQualityGateMarkdown(report) {
  const c = report.counters || {};
  const examples = (report.before_after_examples || [])
    .slice(0, 15)
    .map((e) => {
      const before = JSON.stringify(e.before);
      const after = JSON.stringify(e.after);
      return `- \`${e.record_id}\`: ${before} → ${after}`;
    })
    .join("\n");

  return `# Production Census — Core Identity Quality Gate

**Status:** \`${report.status}\`  
**Generated:** ${report.generated_at}  
**Write target:** ${report.write_target?.base} → ${report.write_target?.table} (\`${report.write_target?.table_id}\`)  
**Airtable writes:** ${report.airtable_writes ? "yes" : "no (controlled)"}

${
  report.production_apply
    ? `## Production apply (completed)

| Metric | Count |
|--------|------:|
| Records auto-fixed (City / State) | ${report.production_apply.records_auto_fixed ?? 0} |
| Fields written | ${(report.production_apply.fields_written || []).join(", ") || "—"} |
| Remaining High autofix proposals | ${c.safe_autofix_proposals ?? 0} |
| Apply run | \`${report.production_apply.run_id || "—"}\` |

`
    : ""
}## Counts

| Metric | Count |
|--------|------:|
| Records scanned | ${c.records_scanned ?? 0} |
| Quality pass | ${c.quality_pass ?? 0} |
| Safe High autofix proposals | ${c.safe_autofix_proposals ?? 0} |
| Unknown city | ${c.unknown_city ?? 0} |
| Descriptor city | ${c.descriptor_city ?? 0} |
| City/state mixed unresolved | ${c.city_state_mixed ?? 0} |
| Case/accent normalize candidates | ${c.case_or_accent_normalized ?? 0} |
| City/state split candidates | ${c.city_state_split ?? 0} |
| Canonical blank | ${c.canonical_blank ?? 0} |
| Canonical conflict | ${c.canonical_conflict ?? 0} |
| Canonical fixes proposed | ${c.canonical_fixes ?? 0} |
| Steward review | ${c.steward_review_required ?? 0} |
| Duplicate risk | ${c.duplicate_risk ?? 0} |
| Blocked dirty core identity | ${c.blocked_dirty_core_identity ?? 0} |
| Coordinate blocked (dirty identity) | ${c.coordinate_blocked_dirty_identity ?? 0} |

## Readiness

${Object.entries(report.by_readiness || {})
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n") || "_n/a_"}

## Fields written (High proposals)

${(report.exact_fields_written || []).map((f) => `- ${f}`).join("\n")}

## Before / after examples

${examples || "_None_"}

## Guards

- No weak city inference from hotel name / country / coordinates
- No Mapbox geocode for dirty identity
- Brand Setup / Brand Explorer / VIC / owner-operator-dates blocked
`;
}

// Re-exports for callers
export {
  evaluateInsertIdentityGate,
  evaluateCoordinateIdentityGate,
  classifyCoreIdentityQuality,
  classifyAndNormalizeCityState,
  QUALITY_GATE_STATUS,
};
