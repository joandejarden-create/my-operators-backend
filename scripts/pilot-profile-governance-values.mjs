#!/usr/bin/env node
/**
 * Controlled end-to-end governance trust-label pilot for one Brand + one Operator profile.
 *
 * Populates P1 governance fields on Brand Setup - Brand Basics and Operator Setup - Master
 * with approved QA values. Default dry-run; --apply required for writes.
 *
 * Usage:
 *   node scripts/pilot-profile-governance-values.mjs --brand "BRAND NAME" --operator "OPERATOR NAME"
 *   node scripts/pilot-profile-governance-values.mjs --brand recXXX --operator recYYY --apply
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  MAP_PROFILE_GOVERNANCE_AIRTABLE,
  GOVERNANCE_VALIDATION_STATUS,
  GOVERNANCE_USAGE_PERMISSION,
} from "../lib/profile-governance/profile-governance-fields.js";
import {
  extractProfileGovernanceRaw,
  normalizeProfileGovernance,
} from "../lib/profile-governance/normalize-profile-governance.js";
import { P1_GOVERNANCE_FIELD_ALIASES } from "../lib/brand-operator-validation-audit/p1-profile-governance-field-specs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "profile-governance-pilot-values.json");
const REPORT_MD = join(ROOT, "reports", "profile-governance-pilot-values.md");

const BRAND_TABLE = "Brand Setup - Brand Basics";
const BRAND_NAME_FIELD = "Brand Name";
const OPERATOR_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";
const OPERATOR_NAME_FIELDS = [
  process.env.AIRTABLE_OPERATOR_COMPANY_NAME_FIELD || "company_name",
  "Company Name",
  "company_name",
];

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;

/** Pilot values keyed by governance API keys → written via MAP_PROFILE_GOVERNANCE_AIRTABLE */
const BRAND_PILOT_VALUES = {
  validationStatus: "Company Published",
  usagePermission: "Platform Display Allowed",
  sourceType: "Company PDF / Brochure",
  sourceRegion: "Global Reference",
  confidenceLevel: "Medium",
  externalDisplayStatus: "Show Trust Label",
  lastReviewedDate: "2026-07-06",
  companyValidated: false,
  evidenceNotes: "Pilot governance value for Explorer trust label QA.",
  missingDataFlags: "Company validation not yet completed.",
  internalNotes: "Pilot value. Remove or update after QA.",
};

const OPERATOR_PILOT_VALUES = {
  validationStatus: "Source-Informed",
  usagePermission: "Platform Display Allowed",
  sourceType: "Company Website",
  sourceRegion: "CALA-Specific",
  confidenceLevel: "Medium",
  externalDisplayStatus: "Show Trust Label",
  lastReviewedDate: "2026-07-06",
  companyValidated: false,
  evidenceNotes: "Pilot governance value for Explorer trust label QA.",
  missingDataFlags: "Company validation not yet completed.",
  internalNotes: "Pilot value. Remove or update after QA.",
};

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function isRecordId(value) {
  return /^rec[a-zA-Z0-9]{10,}$/.test(String(value || "").trim());
}

function escapeFormulaString(value) {
  return String(value || "")
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/\n/g, " ")
    .trim();
}

function readFieldValue(fields, columnName) {
  const raw = fields?.[columnName];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return raw.trim() || null;
  if (typeof raw === "boolean") return raw;
  if (typeof raw === "object" && raw.name) return String(raw.name).trim() || null;
  return String(raw).trim() || null;
}

function resolveWriteColumn(canonicalColumn, entityType, recordFields) {
  const keys = Object.keys(recordFields || {});
  if (keys.includes(canonicalColumn)) return canonicalColumn;

  const aliasConfig = P1_GOVERNANCE_FIELD_ALIASES[canonicalColumn];
  if (aliasConfig?.aliases?.length) {
    for (const alias of aliasConfig.aliases) {
      if (keys.includes(alias)) return alias;
    }
    if (entityType === "operator" && canonicalColumn === "Confidence Level") {
      return "Data Confidence Level";
    }
  }
  return canonicalColumn;
}

function pilotValuesToAirtable(pilotValues, { entityType, recordFields } = {}) {
  const fields = {};
  const columnMap = {};
  for (const [apiKey, value] of Object.entries(pilotValues)) {
    if (value === undefined) continue;
    const canonical = MAP_PROFILE_GOVERNANCE_AIRTABLE[apiKey];
    if (!canonical) {
      throw new Error(`Missing Airtable column mapping for pilot key: ${apiKey}`);
    }
    const live = resolveWriteColumn(canonical, entityType, recordFields);
    fields[live] = value;
    columnMap[canonical] = live;
  }
  return { fields, columnMap };
}

function readGovernanceColumnValue(currentFields, canonicalColumn, liveColumn) {
  const live = liveColumn || canonicalColumn;
  const fromLive = readFieldValue(currentFields, live);
  if (fromLive != null) return fromLive;
  if (live !== canonicalColumn) return readFieldValue(currentFields, canonicalColumn);
  return null;
}

function assessProtection(currentFields, entityType) {
  const raw = extractProfileGovernanceRaw(currentFields, { entityType });
  const warnings = [];

  if (raw.companyValidated === true) {
    warnings.push("Company Validated checkbox is true — record protected (no overwrite).");
  }
  if (raw.validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated) {
    warnings.push("Validation Status is Company Validated — record protected (no overwrite).");
  }
  if (raw.companyValidationDate) {
    warnings.push(
      `Company Validation Date is set (${raw.companyValidationDate}) — record protected (no overwrite).`
    );
  }
  if (raw.usagePermission === GOVERNANCE_USAGE_PERMISSION.doNotUse) {
    warnings.push("Usage Permission is Do Not Use — record protected (no overwrite).");
  }

  return {
    blocked: warnings.length > 0,
    warnings,
    currentRaw: raw,
  };
}

function diffPilotFields(currentFields, desiredLiveFields, columnMap) {
  const wouldUpdate = [];
  const unchanged = [];
  const skipped = [];
  const entries = columnMap
    ? Object.entries(columnMap)
    : Object.entries(desiredLiveFields).map(([col, val]) => [col, col]);

  for (const [canonical, live] of entries) {
    const desired = desiredLiveFields[live];
    const currentNorm = readGovernanceColumnValue(currentFields, canonical, live);
    const desiredNorm =
      typeof desired === "boolean" ? desired : readFieldValue({ [live]: desired }, live);

    if (canonical === MAP_PROFILE_GOVERNANCE_AIRTABLE.companyValidationDate) {
      skipped.push({ field: canonical, liveColumn: live, reason: "Never written by pilot script" });
      continue;
    }

    if (typeof desired === "boolean") {
      const currentBool = currentFields?.[live] === true || currentFields?.[canonical] === true;
      if (currentBool === desired) unchanged.push({ field: canonical, liveColumn: live, value: currentBool });
      else wouldUpdate.push({ field: canonical, liveColumn: live, from: currentBool, to: desired });
      continue;
    }

    if (currentNorm === desiredNorm) {
      unchanged.push({ field: canonical, liveColumn: live, value: currentNorm });
    } else {
      wouldUpdate.push({
        field: canonical,
        liveColumn: live,
        from: currentNorm,
        to: desiredNorm,
      });
    }
  }

  return { wouldUpdate, unchanged, skipped };
}

async function resolveByName(base, table, nameField, target) {
  const trimmed = String(target || "").trim();
  if (!trimmed) {
    return { status: "not_found", error: "Empty name target" };
  }

  const escaped = escapeFormulaString(trimmed);
  let exact;
  try {
    exact = await base(table)
      .select({
        filterByFormula: `{${nameField}} = "${escaped}"`,
        maxRecords: 10,
      })
      .all();
  } catch (err) {
    const msg = err.message || String(err);
    if (/unknown field names/i.test(msg) || /invalid filter/i.test(msg)) {
      return {
        status: "not_found",
        nameField,
        error: `Name field "${nameField}" not available on ${table}`,
        invalidNameField: true,
      };
    }
    throw err;
  }

  if (exact.length === 1) {
    return { status: "found", record: exact[0], matchType: "exact_name", nameField };
  }
  if (exact.length > 1) {
    return {
      status: "ambiguous",
      nameField,
      candidates: exact.map((r) => ({
        id: r.id,
        name: readFieldValue(r.fields, nameField),
      })),
    };
  }

  const lower = escapeFormulaString(trimmed.toLowerCase());
  let nearExact;
  try {
    nearExact = await base(table)
      .select({
        filterByFormula: `LOWER(TRIM({${nameField}})) = "${lower}"`,
        maxRecords: 10,
      })
      .all();
  } catch (err) {
    const msg = err.message || String(err);
    if (/unknown field names/i.test(msg) || /invalid filter/i.test(msg)) {
      return {
        status: "not_found",
        nameField,
        error: `Name field "${nameField}" not available on ${table}`,
        invalidNameField: true,
      };
    }
    throw err;
  }

  if (nearExact.length === 1) {
    return { status: "found", record: nearExact[0], matchType: "near_exact_name", nameField };
  }
  if (nearExact.length > 1) {
    return {
      status: "ambiguous",
      nameField,
      candidates: nearExact.map((r) => ({
        id: r.id,
        name: readFieldValue(r.fields, nameField),
      })),
    };
  }

  return { status: "not_found", nameField, error: `No match for "${trimmed}" on ${nameField}` };
}

async function resolveRecord(base, table, nameFields, target, entityLabel) {
  const trimmed = String(target || "").trim();
  if (!trimmed) {
    return { status: "missing_target", error: `${entityLabel} target not provided` };
  }

  if (isRecordId(trimmed)) {
    try {
      const record = await base(table).find(trimmed);
      return { status: "found", record, matchType: "record_id" };
    } catch (err) {
      return {
        status: "not_found",
        error: `${entityLabel} record ID not found: ${trimmed} (${err.message || err})`,
      };
    }
  }

  const fields = Array.isArray(nameFields) ? nameFields : [nameFields];
  const tried = [];
  for (const nameField of [...new Set(fields)]) {
    const result = await resolveByName(base, table, nameField, trimmed);
    tried.push({
      nameField,
      status: result.status,
      invalidNameField: Boolean(result.invalidNameField),
      error: result.error || null,
    });
    if (result.invalidNameField) continue;
    if (result.status === "found" || result.status === "ambiguous") {
      return { ...result, entityLabel, table };
    }
  }

  return {
    status: "not_found",
    entityLabel,
    table,
    error: `No ${entityLabel} matched "${trimmed}"`,
    tried,
  };
}

function buildQaLinks(entityType, recordId, displayName) {
  const id = encodeURIComponent(recordId);
  const name = encodeURIComponent(displayName || "");
  if (entityType === "brand") {
    return {
      explorer: `/brand-explorer-combined?id=${id}`,
      explorerAlt: `/brand-explorer-combined?brandId=${id}`,
      api: `/api/brand-library/brand?brandId=${id}`,
      nameHint: displayName ? ` (${displayName})` : "",
    };
  }
  return {
    explorer: `/operator-explorer-gold-mock.html?id=${id}`,
    explorerAlt: `/operator-dna-profile.html?id=${id}`,
    api: `/api/intake/third-party-operators/${id}`,
    nameHint: displayName ? ` (${displayName})` : "",
  };
}

function processEntity({
  entityType,
  resolveResult,
  pilotValues,
  sourceTable,
  nameField,
}) {
  const entry = {
    entityType,
    sourceTable,
    target: resolveResult.target,
    status: resolveResult.status,
    record: null,
    protected: null,
    fieldDiff: null,
    write: null,
    expectedGovernance: null,
    qa: null,
    errors: [],
  };

  if (resolveResult.status === "missing_target") {
    entry.errors.push(resolveResult.error);
    return entry;
  }

  if (resolveResult.status === "not_found") {
    entry.errors.push(resolveResult.error || "Record not found");
    if (resolveResult.tried) entry.searchAttempts = resolveResult.tried;
    return entry;
  }

  if (resolveResult.status === "ambiguous") {
    entry.errors.push("Multiple records matched — no write performed");
    entry.candidates = resolveResult.candidates;
    entry.nameField = resolveResult.nameField;
    return entry;
  }

  const record = resolveResult.record;
  const fields = record.fields || {};
  const displayName =
    entityType === "brand"
      ? readFieldValue(fields, BRAND_NAME_FIELD)
      : readFieldValue(fields, resolveResult.nameField || OPERATOR_NAME_FIELDS[0]) ||
        readFieldValue(fields, "Company Name") ||
        readFieldValue(fields, "company_name");

  entry.record = {
    id: record.id,
    name: displayName,
    matchType: resolveResult.matchType,
    nameField: resolveResult.nameField || nameField,
  };

  entry.qa = buildQaLinks(entityType, record.id, displayName);

  const protection = assessProtection(fields, entityType);
  entry.protected = protection;

  const { fields: desiredLiveFields, columnMap } = pilotValuesToAirtable(pilotValues, {
    entityType,
    recordFields: fields,
  });
  entry.columnMap = columnMap;
  entry.fieldDiff = diffPilotFields(fields, desiredLiveFields, columnMap);

  const mergedFields = { ...fields, ...desiredLiveFields };
  entry.expectedGovernance = normalizeProfileGovernance(mergedFields, {
    entityType,
    sourceTable,
  });

  if (protection.blocked) {
    entry.write = { status: "skipped", reason: "protected_fields" };
    entry.errors.push(...protection.warnings);
    return entry;
  }

  if (!entry.fieldDiff.wouldUpdate.length) {
    entry.write = { status: "skipped", reason: "no_changes" };
  } else {
    entry.write = {
      status: DRY_RUN ? "dry_run" : "pending_apply",
      patch: Object.fromEntries(
        entry.fieldDiff.wouldUpdate.map((row) => [
          row.liveColumn || row.field,
          desiredLiveFields[row.liveColumn || row.field],
        ])
      ),
    };
  }

  return entry;
}

function buildMarkdown(report) {
  const lines = [
    "# Profile governance pilot values",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Base: \`${report.baseId}\``,
    "",
    "## Targets",
    "",
    `- Brand: \`${report.targets.brand || "(not provided)"}\``,
    `- Operator: \`${report.targets.operator || "(not provided)"}\``,
    "",
  ];

  for (const entity of [report.brand, report.operator]) {
    if (!entity) continue;
    const title = entity.entityType === "brand" ? "Brand" : "Operator";
    lines.push(`## ${title}`, "");
    lines.push(`- Status: **${entity.status}**`);
    if (entity.record) {
      lines.push(
        `- Record: \`${entity.record.id}\` — ${entity.record.name || "(no name)"} (${entity.record.matchType})`
      );
    }
    if (entity.candidates?.length) {
      lines.push("- Ambiguous candidates:");
      for (const c of entity.candidates) {
        lines.push(`  - \`${c.id}\` — ${c.name || "(no name)"}`);
      }
    }
    if (entity.protected?.warnings?.length) {
      lines.push("- Protected-field warnings:");
      for (const w of entity.protected.warnings) {
        lines.push(`  - ${w}`);
      }
    }
    if (entity.fieldDiff) {
      if (entity.fieldDiff.wouldUpdate.length) {
        lines.push("", `### Fields ${report.mode === "apply" ? "updated" : "to update"}`, "");
        lines.push("| Field | From | To |");
        lines.push("|-------|------|-----|");
        for (const row of entity.fieldDiff.wouldUpdate) {
          const colLabel =
            row.liveColumn && row.liveColumn !== row.field
              ? `\`${row.field}\` → \`${row.liveColumn}\``
              : `\`${row.field}\``;
          lines.push(
            `| ${colLabel} | ${row.from == null ? "—" : JSON.stringify(row.from)} | ${JSON.stringify(row.to)} |`
          );
        }
      }
      if (entity.fieldDiff.unchanged.length) {
        lines.push("", "### Fields unchanged", "");
        for (const row of entity.fieldDiff.unchanged) {
          lines.push(`- \`${row.field}\`: ${JSON.stringify(row.value)}`);
        }
      }
    }
    if (entity.write) {
      lines.push("", `- Write: **${entity.write.status}**${entity.write.reason ? ` (${entity.write.reason})` : ""}`);
      if (entity.write.airtableResult) {
        lines.push(`- Airtable: ${entity.write.airtableResult}`);
      }
    }
    if (entity.expectedGovernance) {
      lines.push("", "### Expected normalized governance", "");
      lines.push("```json");
      lines.push(
        JSON.stringify(
          {
            displayLabel: entity.expectedGovernance.displayLabel,
            displaySubtitle: entity.expectedGovernance.displaySubtitle,
            validationStatus: entity.expectedGovernance.validationStatus,
            usagePermission: entity.expectedGovernance.usagePermission,
            externalDisplayStatus: entity.expectedGovernance.externalDisplayStatus,
            lastReviewedDate: entity.expectedGovernance.lastReviewedDate,
            sourceRegion: entity.expectedGovernance.sourceRegion,
            confidenceLevel: entity.expectedGovernance.confidenceLevel,
          },
          null,
          2
        )
      );
      lines.push("```");
      lines.push(
        `- Expected **displayLabel**: ${entity.expectedGovernance.displayLabel == null ? "*(none — no chip)*" : `\`${entity.expectedGovernance.displayLabel}\``}`
      );
      lines.push(
        `- Expected **displaySubtitle**: ${entity.expectedGovernance.displaySubtitle == null ? "*(none)*" : `\`${entity.expectedGovernance.displaySubtitle}\``}`
      );
    }
    if (entity.qa && entity.record) {
      lines.push("", "### Manual QA", "");
      lines.push(`1. Open Explorer: [\`${entity.qa.explorer}\`](${entity.qa.explorer})`);
      lines.push(`2. Confirm header trust chip shows expected label when \`displayLabel\` is set.`);
      lines.push(`3. Optional API check: \`${entity.qa.api}\``);
    }
    if (entity.errors.length) {
      lines.push("", "### Errors / skips", "");
      for (const e of entity.errors) lines.push(`- ${e}`);
    }
    lines.push("");
  }

  lines.push("## Summary", "");
  lines.push(`- Brand write: ${report.summary.brandWrite}`);
  lines.push(`- Operator write: ${report.summary.operatorWrite}`);
  lines.push(`- Records modified: ${report.summary.recordsModified}`);
  if (DRY_RUN) {
    lines.push("", "> Dry-run only. Re-run with `--apply` after founder approval to write Airtable.");
  }
  return lines.join("\n");
}

async function applyPatch(base, table, recordId, patchFields) {
  if (!patchFields || !Object.keys(patchFields).length) return { applied: false, reason: "empty_patch" };
  await base(table).update(recordId, patchFields, { typecast: true });
  return { applied: true, fieldCount: Object.keys(patchFields).length };
}

async function main() {
  const brandTarget = argValue("--brand");
  const operatorTarget = argValue("--operator");

  if (!brandTarget || !operatorTarget) {
    console.error(
      "Usage: node scripts/pilot-profile-governance-values.mjs --brand \"<name or rec…>\" --operator \"<name or rec…>\" [--dry-run|--apply]"
    );
    process.exit(1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const generatedAt = new Date().toISOString();

  console.log(`[pilot-profile-governance] mode=${DRY_RUN ? "dry-run" : "apply"}`);
  console.log(`[pilot-profile-governance] brand target: ${brandTarget}`);
  console.log(`[pilot-profile-governance] operator target: ${operatorTarget}`);

  const brandResolve = await resolveRecord(
    base,
    BRAND_TABLE,
    BRAND_NAME_FIELD,
    brandTarget,
    "Brand"
  );
  brandResolve.target = brandTarget;

  const operatorResolve = await resolveRecord(
    base,
    OPERATOR_TABLE,
    OPERATOR_NAME_FIELDS,
    operatorTarget,
    "Operator"
  );
  operatorResolve.target = operatorTarget;

  const brand = processEntity({
    entityType: "brand",
    resolveResult: brandResolve,
    pilotValues: BRAND_PILOT_VALUES,
    sourceTable: BRAND_TABLE,
    nameField: BRAND_NAME_FIELD,
  });

  const operator = processEntity({
    entityType: "operator",
    resolveResult: operatorResolve,
    pilotValues: OPERATOR_PILOT_VALUES,
    sourceTable: OPERATOR_TABLE,
    nameField: OPERATOR_NAME_FIELDS[0],
  });

  let recordsModified = 0;

  if (APPLY) {
    for (const entity of [brand, operator]) {
      if (entity.write?.status !== "pending_apply" || !entity.record?.id) continue;
      const table = entity.entityType === "brand" ? BRAND_TABLE : OPERATOR_TABLE;
      const patch = entity.write.patch || {};
      try {
        const result = await applyPatch(base, table, entity.record.id, patch);
        entity.write.status = "applied";
        entity.write.airtableResult = `updated ${result.fieldCount} field(s)`;
        recordsModified += 1;
        console.log(
          `[pilot-profile-governance] applied ${entity.entityType} ${entity.record.id} (${result.fieldCount} fields)`
        );
      } catch (err) {
        entity.write.status = "failed";
        entity.write.airtableResult = err.message || String(err);
        entity.errors.push(`Airtable update failed: ${entity.write.airtableResult}`);
        console.error(
          `[pilot-profile-governance] FAIL ${entity.entityType} ${entity.record.id}:`,
          entity.write.airtableResult
        );
      }
      await new Promise((r) => setTimeout(r, 250));
    }
  }

  const report = {
    generatedAt,
    mode: DRY_RUN ? "dry-run" : "apply",
    baseId,
    targets: { brand: brandTarget, operator: operatorTarget },
    brand,
    operator,
    summary: {
      brandWrite: brand.write?.status || brand.status,
      operatorWrite: operator.write?.status || operator.status,
      recordsModified,
    },
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);

  const hasHardFailure =
    brand.status === "not_found" ||
    operator.status === "not_found" ||
    brand.status === "ambiguous" ||
    operator.status === "ambiguous" ||
    brand.write?.status === "failed" ||
    operator.write?.status === "failed";

  if (hasHardFailure) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
