/**
 * Controlled Platform Field Publishing v2 — guarded single-field writes.
 * @see docs/data-intelligence/controlled-platform-field-publishing-v2.md
 */
import {
  BLOCKED_DESTINATION_FIELDS,
  FIELD_PUBLISHING_POLICIES,
  PUBLISH_MODES,
} from "./approved-intelligence-field-publishing.js";
import {
  RISK_LEVELS,
  buildFieldSuggestionsFromAudit,
} from "./approved-intelligence-field-suggestions.js";
import {
  NEW_BASE_PLATFORM_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../../api/lib/operator-setup-new-base-read.js";
import { loadFieldPublishingAuditForEntity } from "./field-publishing-entity-loader.js";
import { getPartnerFactById } from "./airtable-facts.js";
import { getPartnerSourceById } from "./airtable-source.js";

export const CONTROLLED_PUBLISH_VERSION = "v2";
export const APPROVAL_FLAG = "approve-controlled-field-publish";
export const APPROVAL_CLI_FLAG = `--${APPROVAL_FLAG}`;
export const CORRECTION_APPROVAL_FLAG = "approve-controlled-field-correction";
export const CORRECTION_APPROVAL_CLI_FLAG = `--${CORRECTION_APPROVAL_FLAG}`;

export const REPORT_JSON_NAME = "controlled-platform-field-publishing.json";
export const REPORT_MD_NAME = "controlled-platform-field-publishing.md";
export const CORRECTION_REPORT_JSON_SUFFIX = "controlled-platform-field-publishing-correction";
export const CORRECTION_REPORT_MD_SUFFIX = "controlled-platform-field-publishing-correction";

export const SUPPORTED_ENTITY_TYPES_V2 = ["operator"];

/**
 * v2 allowlist — only these destination fields may be written.
 * Key = CLI --destination-field value (form/prefill name).
 */
export const V2_ALLOWED_OPERATOR_DESTINATIONS = {
  specificMarkets: {
    destinationTable: NEW_BASE_PLATFORM_TABLE,
    destinationField: "specificMarkets",
    expectedFactKey: "op.markets.regionsSupported",
    fieldType: "longText",
  },
};

export function controlledPublishReportFileNames(targetRecId) {
  return {
    perEntityJson: `controlled-platform-field-publishing-${targetRecId}.json`,
    perEntityMd: `controlled-platform-field-publishing-${targetRecId}.md`,
    latestJson: REPORT_JSON_NAME,
    latestMd: REPORT_MD_NAME,
  };
}

export function controlledPublishCorrectionReportFileNames(targetRecId) {
  return {
    perEntityJson: `${CORRECTION_REPORT_JSON_SUFFIX}-${targetRecId}.json`,
    perEntityMd: `${CORRECTION_REPORT_MD_SUFFIX}-${targetRecId}.md`,
    runJson: `${CORRECTION_REPORT_JSON_SUFFIX}-run-${targetRecId}.json`,
    runMd: `${CORRECTION_REPORT_MD_SUFFIX}-run-${targetRecId}.md`,
  };
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function isBlank(v) {
  return nz(v) === "";
}

function isApprovedFactStatus(status) {
  const s = nz(status);
  return s === "Approved" || s === "Edited";
}

export function buildSuggestionKey(targetRecId, factId, factKey) {
  return `${targetRecId}:${factId}:${factKey}`;
}

export function findSuggestion(suggestionsReport, { factId, suggestionKey }) {
  const pool = suggestionsReport?.suggestions || [];
  if (factId) {
    return pool.find((s) => s.sourceFactId === factId) || null;
  }
  if (suggestionKey) {
    return pool.find((s) => s.suggestionId === suggestionKey) || null;
  }
  return null;
}

/**
 * Validate all v2 safety gates. Returns plan object for dry-run/apply.
 */
export function validateControlledPublishGates(input) {
  const {
    entityType,
    targetRecId,
    destinationFieldKey,
    suggestion,
    fact,
    source,
    governance,
    liveValue,
    applyRequested,
    approvalPresent,
    factId,
    suggestionKey,
  } = input;

  const failures = [];
  const gates = {};

  const allowDest = V2_ALLOWED_OPERATOR_DESTINATIONS[destinationFieldKey];

  gates.entityTypeSupported = SUPPORTED_ENTITY_TYPES_V2.includes(entityType);
  if (!gates.entityTypeSupported) failures.push("unsupported_entity_type");

  gates.destinationAllowlisted = Boolean(allowDest);
  if (!gates.destinationAllowlisted) failures.push("destination_not_allowlisted");

  gates.factOrSuggestionProvided = Boolean(factId || suggestionKey);
  if (!gates.factOrSuggestionProvided) failures.push("missing_fact_id_or_suggestion_key");

  gates.suggestionFound = Boolean(suggestion);
  if (!gates.suggestionFound) failures.push("suggestion_not_found");

  const reviewStatus = fact?.humanReviewStatus || null;
  gates.factApproved = isApprovedFactStatus(reviewStatus);
  if (!gates.factApproved) failures.push("fact_not_approved");

  gates.sourceExplorerApproved =
    source && nz(source.approvedForExplorerUse) === "Yes";
  if (!gates.sourceExplorerApproved) failures.push("source_not_explorer_approved");

  gates.governanceDisplayAllowed = governance?.displayAllowed !== false;
  if (!gates.governanceDisplayAllowed) failures.push("governance_display_not_allowed");

  gates.controlledClassification =
    suggestion?.classification === PUBLISH_MODES.controlledPublishCandidate;
  if (suggestion && !gates.controlledClassification) {
    failures.push("not_controlled_publish_candidate");
  }

  gates.riskLow = suggestion?.riskLevel === RISK_LEVELS.low;
  if (suggestion && !gates.riskLow) failures.push("risk_not_low");

  gates.destinationBlank = isBlank(liveValue);
  if (!gates.destinationBlank) failures.push("destination_not_blank");

  const destFieldName = allowDest?.destinationField || suggestion?.destinationField;
  gates.notGovernanceField = !BLOCKED_DESTINATION_FIELDS.has(destFieldName);
  if (!gates.notGovernanceField) failures.push("destination_is_governance_field");

  gates.notCompanyValidated =
    destFieldName !== "Company Validated" && destFieldName !== "Company Validation Date";
  if (!gates.notCompanyValidated) failures.push("destination_is_company_validated");

  const policy = FIELD_PUBLISHING_POLICIES[suggestion?.factKey || ""];
  gates.notIdentityField = !policy?.identityField;
  if (policy?.identityField) failures.push("identity_field_blocked");

  gates.notScoringOrFit =
    !String(suggestion?.factKey || "").startsWith("op.dealFit.") &&
    !String(suggestion?.factKey || "").startsWith("op.meta.");
  if (!gates.notScoringOrFit) failures.push("scoring_or_fit_field_blocked");

  if (allowDest && suggestion) {
    gates.factKeyMatches =
      suggestion.factKey === allowDest.expectedFactKey &&
      suggestion.destinationField === allowDest.destinationField;
    if (!gates.factKeyMatches) failures.push("fact_key_or_destination_mismatch");
  }

  gates.applyFlagPresent = applyRequested === true;
  gates.approvalTokenPresent = approvalPresent === true;

  if (applyRequested && !approvalPresent) {
    failures.push("apply_without_approval_token");
  }

  const eligible = failures.length === 0;

  return {
    ok: eligible,
    failures: [...new Set(failures)],
    gates,
    plan: eligible
      ? {
          entityType,
          targetRecId,
          entityName: suggestion?.entityName || null,
          sourceFactId: suggestion?.sourceFactId,
          sourceId: suggestion?.sourceId,
          factKey: suggestion?.factKey,
          suggestionId: suggestion?.suggestionId,
          destinationTable: allowDest?.destinationTable || suggestion?.destinationTable,
          destinationField: allowDest?.destinationField || suggestion?.destinationField,
          destinationFieldKey,
          previousValue: nz(liveValue) || null,
          newValue: suggestion?.proposedValue || suggestion?.approvedValue,
          sourceIds: suggestion?.sourceId ? [suggestion.sourceId] : [],
          mode: applyRequested && approvalPresent ? "apply" : "dry-run",
        }
      : null,
  };
}

/**
 * Re-read live destination value for operator platform row.
 */
export async function readOperatorDestinationLiveValue(targetRecId, destinationFieldKey) {
  const allow = V2_ALLOWED_OPERATOR_DESTINATIONS[destinationFieldKey];
  if (!allow) return { value: null, recordId: null, rowCount: 0 };

  const rows = await fetchRecordsLinkedToMaster(allow.destinationTable, targetRecId);
  if (!rows.length) {
    return { value: null, recordId: null, rowCount: 0 };
  }
  const row = rows[0];
  const value = row.fields?.[allow.destinationField];
  return {
    value,
    recordId: row.id,
    rowCount: rows.length,
  };
}

/**
 * Build full controlled publish run (dry-run or apply).
 */
export async function runControlledPlatformFieldPublish(options) {
  const {
    entityType,
    targetRecId,
    destinationFieldKey,
    factId = null,
    suggestionKey = null,
    apply = false,
    approvalPresent = false,
  } = options;

  const { audit, sources, facts } = await loadFieldPublishingAuditForEntity(
    entityType,
    targetRecId
  );
  const suggestionsReport = buildFieldSuggestionsFromAudit(audit, sources, facts);

  const suggestion = findSuggestion(suggestionsReport, { factId, suggestionKey });

  const factRecord = factId
    ? facts.find((f) => f.id === factId) || (await getPartnerFactById(factId).catch(() => null))
    : suggestion
      ? facts.find((f) => f.id === suggestion.sourceFactId)
      : null;

  const sourceId = factRecord?.sourceRecordId || suggestion?.sourceId;
  const source = sourceId
    ? sources.find((s) => s.id === sourceId) || (await getPartnerSourceById(sourceId).catch(() => null))
    : null;

  const live = await readOperatorDestinationLiveValue(targetRecId, destinationFieldKey);

  const validation = validateControlledPublishGates({
    entityType,
    targetRecId,
    destinationFieldKey,
    suggestion,
    fact: factRecord,
    source,
    governance: suggestionsReport.governance,
    liveValue: live.value,
    applyRequested: apply,
    approvalPresent,
    factId,
    suggestionKey,
  });

  const result = {
    controlledPublishVersion: CONTROLLED_PUBLISH_VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply && approvalPresent ? "apply" : "dry-run",
    entityType,
    targetRecId,
    entityName: audit.entityName,
    destinationFieldKey,
    factId: factId || suggestion?.sourceFactId || null,
    suggestionKey: suggestionKey || suggestion?.suggestionId || null,
    validation,
    suggestion: suggestion || null,
    liveDestination: {
      recordId: live.recordId,
      rowCount: live.rowCount,
      previousValue: nz(live.value) || null,
    },
    writeResult: null,
    rollback: null,
  };

  if (!validation.ok) {
    result.blocked = true;
    return result;
  }

  result.plan = validation.plan;
  result.rollback = {
    note: "To revert, restore previousValue on the destination record.",
    destinationTable: validation.plan.destinationTable,
    destinationField: validation.plan.destinationField,
    destinationRecordId: live.recordId,
    previousValue: validation.plan.previousValue,
  };

  if (apply && approvalPresent) {
    if (!live.recordId) {
      result.writeResult = {
        ok: false,
        error: "platform_row_not_found",
        message: "No Operator Setup - Platform & Markets row linked to master; v2 does not create rows.",
      };
      result.blocked = true;
      return result;
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const tableEnc = encodeURIComponent(validation.plan.destinationTable);
    const url = `https://api.airtable.com/v0/${baseId}/${tableEnc}/${encodeURIComponent(live.recordId)}`;
    const fields = { [validation.plan.destinationField]: validation.plan.newValue };
    const { ok, status, json } = await airtableFetchJson(url, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    });

    result.writeResult = {
      ok,
      status,
      destinationRecordId: live.recordId,
      fieldsWritten: fields,
      error: ok ? null : json?.error?.message || "airtable_patch_failed",
    };

    if (ok) {
      const verify = await readOperatorDestinationLiveValue(targetRecId, destinationFieldKey);
      result.postApplyValue = nz(verify.value) || null;
    }
  }

  return result;
}

/**
 * Validate steward correction gates (populated destination overwrite path).
 * @see docs/data-intelligence/controlled-platform-field-publishing-v2.md §9
 */
export function validateControlledPublishCorrectionGates(input) {
  const {
    entityType,
    targetRecId,
    destinationFieldKey,
    correctValue,
    reason,
    liveValue,
    applyRequested,
    approvalPresent,
  } = input;

  const failures = [];
  const gates = {};
  const allowDest = V2_ALLOWED_OPERATOR_DESTINATIONS[destinationFieldKey];

  gates.entityTypeSupported = SUPPORTED_ENTITY_TYPES_V2.includes(entityType);
  if (!gates.entityTypeSupported) failures.push("unsupported_entity_type");

  gates.destinationAllowlisted = Boolean(allowDest);
  if (!gates.destinationAllowlisted) failures.push("destination_not_allowlisted");

  gates.correctValueProvided = nz(correctValue) !== "";
  if (!gates.correctValueProvided) failures.push("missing_correct_value");

  gates.reasonProvided = nz(reason) !== "";
  if (!gates.reasonProvided) failures.push("missing_correction_reason");

  gates.destinationPopulated = !isBlank(liveValue);
  if (!gates.destinationPopulated) failures.push("destination_not_populated");

  const destFieldName = allowDest?.destinationField;
  gates.notGovernanceField = !BLOCKED_DESTINATION_FIELDS.has(destFieldName);
  if (!gates.notGovernanceField) failures.push("destination_is_governance_field");

  gates.notCompanyValidated =
    destFieldName !== "Company Validated" && destFieldName !== "Company Validation Date";
  if (!gates.notCompanyValidated) failures.push("destination_is_company_validated");

  gates.valueDiffers =
    gates.correctValueProvided &&
    nz(correctValue) !== nz(liveValue);
  if (gates.correctValueProvided && !gates.valueDiffers) {
    failures.push("correct_value_matches_live_value");
  }

  gates.applyFlagPresent = applyRequested === true;
  gates.approvalTokenPresent = approvalPresent === true;

  if (applyRequested && !approvalPresent) {
    failures.push("apply_without_correction_approval_token");
  }

  const eligible = failures.length === 0;

  return {
    ok: eligible,
    failures: [...new Set(failures)],
    gates,
    plan: eligible
      ? {
          entityType,
          targetRecId,
          destinationTable: allowDest?.destinationTable,
          destinationField: allowDest?.destinationField,
          destinationFieldKey,
          previousValue: nz(liveValue) || null,
          newValue: nz(correctValue),
          correctionReason: nz(reason),
          mode:
            applyRequested && approvalPresent ? "correction-apply" : "correction-dry-run",
        }
      : null,
  };
}

/**
 * Steward-reviewed correction run (dry-run or apply). Does not modify PI facts or governance.
 */
export async function runControlledPlatformFieldCorrection(options) {
  const {
    entityType,
    targetRecId,
    destinationFieldKey,
    correctValue,
    reason,
    apply = false,
    approvalPresent = false,
    stewardContext = null,
  } = options;

  const live = await readOperatorDestinationLiveValue(targetRecId, destinationFieldKey);
  const allowDest = V2_ALLOWED_OPERATOR_DESTINATIONS[destinationFieldKey];

  const validation = validateControlledPublishCorrectionGates({
    entityType,
    targetRecId,
    destinationFieldKey,
    correctValue,
    reason,
    liveValue: live.value,
    applyRequested: apply,
    approvalPresent,
  });

  const result = {
    controlledPublishVersion: CONTROLLED_PUBLISH_VERSION,
    correctionMode: true,
    generatedAt: new Date().toISOString(),
    mode: apply && approvalPresent ? "correction-apply" : "correction-dry-run",
    entityType,
    targetRecId,
    entityName: stewardContext?.entityName || null,
    destinationFieldKey,
    correctValue: nz(correctValue) || null,
    correctionReason: nz(reason) || null,
    stewardContext: stewardContext || null,
    validation,
    liveDestination: {
      recordId: live.recordId,
      rowCount: live.rowCount,
      previousValue: nz(live.value) || null,
    },
    writeResult: null,
    rollback: null,
  };

  if (!validation.ok) {
    result.blocked = true;
    return result;
  }

  result.plan = validation.plan;
  result.rollback = {
    note: "To revert correction, restore previousValue on the destination record.",
    destinationTable: validation.plan.destinationTable,
    destinationField: validation.plan.destinationField,
    destinationRecordId: live.recordId,
    previousValue: validation.plan.previousValue,
  };

  if (apply && approvalPresent) {
    if (!live.recordId) {
      result.writeResult = {
        ok: false,
        error: "platform_row_not_found",
        message: "No Operator Setup - Platform & Markets row linked to master.",
      };
      result.blocked = true;
      return result;
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const tableEnc = encodeURIComponent(validation.plan.destinationTable);
    const url = `https://api.airtable.com/v0/${baseId}/${tableEnc}/${encodeURIComponent(live.recordId)}`;
    const fields = { [validation.plan.destinationField]: validation.plan.newValue };
    const { ok, status, json } = await airtableFetchJson(url, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    });

    result.writeResult = {
      ok,
      status,
      destinationRecordId: live.recordId,
      fieldsWritten: fields,
      error: ok ? null : json?.error?.message || "airtable_patch_failed",
    };

    if (ok) {
      const verify = await readOperatorDestinationLiveValue(targetRecId, destinationFieldKey);
      result.postApplyValue = nz(verify.value) || null;
    }
  }

  return result;
}

export function buildControlledPublishCorrectionMarkdown(report) {
  const ctx = report.stewardContext || {};
  const lines = [
    "# Controlled Platform Field Publishing — Steward Correction",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: **${report.controlledPublishVersion}** (correction mode)`,
    `Mode: **${report.mode}**`,
    "",
    "## Entity",
    "",
    `- **${ctx.entityName || report.entityName || "—"}** (\`${report.targetRecId}\`)`,
    `- Type: ${report.entityType}`,
    "",
    "## Destination",
    "",
    `- Table: **${ctx.destinationTable || report.plan?.destinationTable || "—"}**`,
    `- Field: \`${ctx.destinationField || report.plan?.destinationField || report.destinationFieldKey}\``,
    `- Record: \`${report.liveDestination?.recordId || ctx.destinationRecordId || "—"}\``,
    "",
    "## Values",
    "",
    `- **Current live:** ${report.liveDestination?.previousValue ?? "—"}`,
    `- **Recommended corrected:** ${report.correctValue ?? report.plan?.newValue ?? "—"}`,
    `- **Reason:** ${report.correctionReason ?? "—"}`,
    "",
  ];

  if (ctx.evidenceSource) {
    lines.push("## Evidence", "");
    lines.push(`- Source: \`${ctx.evidenceSource.sourceId}\` — ${ctx.evidenceSource.title}`);
    if (ctx.evidenceSource.url) lines.push(`- URL: ${ctx.evidenceSource.url}`);
    lines.push("");
  }

  if (ctx.regionalContextNote) {
    lines.push("## Regional context vs specificMarkets", "", ctx.regionalContextNote, "");
  }

  if (!report.validation.ok) {
    lines.push("## Blocked", "");
    for (const f of report.validation.failures) lines.push(`- ${f}`);
    lines.push("");
    return lines.join("\n");
  }

  if (report.plan) {
    lines.push(
      "## Correction plan",
      "",
      `- Previous: ${report.plan.previousValue ?? "—"}`,
      `- New: **${report.plan.newValue}**`,
      ""
    );
  }

  lines.push(
    "## Safety confirmations",
    "",
    "- Not a governance correction",
    "- Company Validated / Company Validation Date untouched",
    "- PI facts, sources, and scoring unchanged",
    "- Requires explicit `--approve-controlled-field-correction` before apply",
    ""
  );

  if (report.rollback) {
    lines.push("## Rollback", "");
    lines.push(`- Restore \`${report.rollback.destinationField}\` to: ${report.rollback.previousValue ?? "(empty)"}`);
    lines.push("");
  }

  return lines.join("\n");
}

export function buildControlledPublishMarkdown(report) {
  const lines = [
    "# Controlled Platform Field Publishing",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: **${report.controlledPublishVersion}**`,
    `Mode: **${report.mode}**`,
    `Entity: ${report.entityType} — **${report.entityName}** (\`${report.targetRecId}\`)`,
    "",
  ];

  if (!report.validation.ok) {
    lines.push("## Blocked", "");
    lines.push("Publish **not eligible**. Failures:");
    for (const f of report.validation.failures) lines.push(`- ${f}`);
    lines.push("");
    lines.push("### Gate checklist", "");
    for (const [k, v] of Object.entries(report.validation.gates || {})) {
      lines.push(`- ${k}: ${v ? "pass" : "fail"}`);
    }
    lines.push("");
    return lines.join("\n");
  }

  const p = report.plan;
  lines.push(
    "## Publish plan",
    "",
    `- Fact: \`${p.sourceFactId}\` · Key: \`${p.factKey}\``,
    `- Source: \`${(p.sourceIds || []).join(", ")}\``,
    `- Destination: **${p.destinationTable}** → \`${p.destinationField}\``,
    `- Previous: ${p.previousValue ?? "—"}`,
    `- New: **${p.newValue}**`,
    ""
  );

  if (report.writeResult) {
    lines.push("## Write result", "");
    lines.push(`- Success: **${report.writeResult.ok ? "yes" : "no"}**`);
    if (report.writeResult.error) lines.push(`- Error: ${report.writeResult.error}`);
    if (report.postApplyValue != null) {
      lines.push(`- Post-apply live value: ${report.postApplyValue}`);
    }
    lines.push("");
  }

  if (report.rollback) {
    lines.push("## Rollback", "");
    lines.push(`- Record: \`${report.rollback.destinationRecordId}\``);
    lines.push(`- Restore \`${report.rollback.destinationField}\` to: ${report.rollback.previousValue ?? "(empty)"}`);
    lines.push("");
  }

  lines.push("## Safety", "");
  lines.push("- Single-field allowlisted write only");
  lines.push("- No governance / Company Validated / scoring writes");
  lines.push("");

  return lines.join("\n");
}
