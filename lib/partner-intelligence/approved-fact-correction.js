/**
 * Approved Fact Correction v1 — steward-reviewed PI fact value corrections.
 * @see docs/data-intelligence/approved-fact-correction-v1.md
 */
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { FIELD_PUBLISHING_POLICIES } from "./approved-intelligence-field-publishing.js";
import { EXCLUDED_RECOMMENDATION_STATUSES } from "./stewardship-package.js";
import { getPartnerFactById, patchPartnerFact } from "./airtable-facts.js";

export const FACT_CORRECTION_VERSION = "v1";
export const APPROVAL_FLAG = "approve-approved-fact-correction";
export const APPROVAL_CLI_FLAG = `--${APPROVAL_FLAG}`;

export const REPORT_JSON_NAME = "approved-fact-correction.json";
export const REPORT_MD_NAME = "approved-fact-correction.md";

/** Statuses eligible for correction. */
export const CORRECTABLE_REVIEW_STATUSES = new Set(["Approved", "Edited"]);

/** Statuses that block correction (includes pending and excluded). */
export const BLOCKED_REVIEW_STATUSES = new Set([
  "Pending",
  "Rejected",
  "Needs More Source",
  "Quarantined",
  "Superseded",
  "Invalid",
  "Do Not Use",
  ...EXCLUDED_RECOMMENDATION_STATUSES,
]);

/** Only these Airtable columns may be written on apply. */
export const CORRECTION_PATCH_ALLOWLIST = new Set([
  MAP_PARTNER_FACT.approvedValue,
  MAP_PARTNER_FACT.humanReviewStatus,
  MAP_PARTNER_FACT.reviewerNotes,
  MAP_PARTNER_FACT.lastUpdated,
]);

export function factCorrectionReportFileNames(factId) {
  return {
    perFactJson: `approved-fact-correction-${factId}.json`,
    perFactMd: `approved-fact-correction-${factId}.md`,
    latestJson: REPORT_JSON_NAME,
    latestMd: REPORT_MD_NAME,
  };
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

export function getFactDisplayValue(fact) {
  return nz(fact?.approvedValue) || nz(fact?.extractedValue);
}

export function recommendHumanReviewStatusAfterCorrection(currentStatus) {
  // Steward manual correction convention: Edited signals steward-adjusted approved value.
  if (currentStatus === "Edited") return "Edited";
  if (currentStatus === "Approved") return "Edited";
  return currentStatus;
}

export function buildReviewerNotesEntry({
  date = todayIsoDate(),
  previousApprovedValue,
  correctedValue,
  reason,
  evidenceSourceId = null,
}) {
  const reasonText = nz(reason).replace(/\.+$/, "");
  let line = `Approved fact correction ${date}: Approved Value changed from '${previousApprovedValue}' to '${correctedValue}'. Reason: ${reasonText}.`;
  if (evidenceSourceId) {
    line += ` Evidence: source ${evidenceSourceId}.`;
  }
  return line;
}

export function appendReviewerNotes(existingNotes, entry) {
  const prior = nz(existingNotes);
  if (!prior) return entry;
  return `${prior}\n\n${entry}`;
}

export function validateApprovedFactCorrectionGates(input) {
  const {
    fact,
    correctValue,
    reason,
    applyRequested,
    approvalPresent,
    evidenceSourceId = null,
    allowIdentityCorrection = false,
  } = input;

  const failures = [];
  const gates = {};

  gates.factExists = Boolean(fact);
  if (!gates.factExists) failures.push("fact_not_found");

  const reviewStatus = nz(fact?.humanReviewStatus);
  gates.reviewStatusCorrectable = CORRECTABLE_REVIEW_STATUSES.has(reviewStatus);
  if (fact && !gates.reviewStatusCorrectable) {
    if (BLOCKED_REVIEW_STATUSES.has(reviewStatus) || reviewStatus) {
      failures.push("fact_review_status_not_correctable");
    } else {
      failures.push("fact_review_status_missing");
    }
  }

  gates.correctValueProvided = nz(correctValue) !== "";
  if (!gates.correctValueProvided) failures.push("missing_correct_value");

  gates.reasonProvided = nz(reason) !== "";
  if (!gates.reasonProvided) failures.push("missing_correction_reason");

  const currentApproved = nz(fact?.approvedValue);
  const currentDisplay = fact ? getFactDisplayValue(fact) : "";
  gates.valueDiffers =
    gates.correctValueProvided && nz(correctValue) !== currentDisplay;
  if (gates.correctValueProvided && !gates.valueDiffers) {
    failures.push("correct_value_matches_current_display_value");
  }

  const policy = FIELD_PUBLISHING_POLICIES[fact?.fieldName || ""];
  gates.notIdentityField = !policy?.identityField || allowIdentityCorrection;
  if (policy?.identityField && !allowIdentityCorrection) {
    failures.push("identity_field_correction_blocked");
  }

  gates.extractedValuePreserved = true;

  gates.applyFlagPresent = applyRequested === true;
  gates.approvalTokenPresent = approvalPresent === true;
  if (applyRequested && !approvalPresent) {
    failures.push("apply_without_correction_approval_token");
  }

  const recommendedStatus = fact
    ? recommendHumanReviewStatusAfterCorrection(reviewStatus)
    : null;

  const reviewerNotesEntry =
    fact && gates.correctValueProvided && gates.reasonProvided
      ? buildReviewerNotesEntry({
          previousApprovedValue: currentApproved || currentDisplay,
          correctedValue: nz(correctValue),
          reason: nz(reason),
          evidenceSourceId: evidenceSourceId || null,
        })
      : null;

  const patchPreview =
    fact && failures.length === 0
      ? buildFactCorrectionPatch(fact, {
          correctValue: nz(correctValue),
          reason: nz(reason),
          evidenceSourceId,
        })
      : null;

  const eligible = failures.length === 0;

  return {
    ok: eligible,
    failures: [...new Set(failures)],
    gates,
    current: fact
      ? {
          extractedValue: nz(fact.extractedValue) || null,
          approvedValue: currentApproved || null,
          displayValue: currentDisplay || null,
          humanReviewStatus: reviewStatus || null,
          reviewerNotes: nz(fact.reviewerNotes) || null,
        }
      : null,
    recommendedHumanReviewStatus: recommendedStatus,
    reviewerNotesEntry,
    reviewerNotesPreview: patchPreview?.fields?.[MAP_PARTNER_FACT.reviewerNotes] || null,
    patchPreview: patchPreview?.fields || null,
    rollbackApprovedValue: currentApproved || currentDisplay || null,
    plan: eligible
      ? {
          factId: fact.id,
          factKey: fact.fieldName,
          previousApprovedValue: currentApproved || currentDisplay,
          correctedApprovedValue: nz(correctValue),
          humanReviewStatus: recommendedStatus,
          extractedValuePreserved: nz(fact.extractedValue) || null,
          sourceRecordId: fact.sourceRecordId || null,
          operatorId: fact.operatorId || null,
          brandId: fact.brandId || null,
          mode: applyRequested && approvalPresent ? "apply" : "dry-run",
        }
      : null,
  };
}

export function buildFactCorrectionPatch(fact, { correctValue, reason, evidenceSourceId = null }) {
  const currentApproved = nz(fact.approvedValue);
  const currentDisplay = getFactDisplayValue(fact);
  const reviewStatus = nz(fact.humanReviewStatus);
  const entry = buildReviewerNotesEntry({
    previousApprovedValue: currentApproved || currentDisplay,
    correctedValue: nz(correctValue),
    reason: nz(reason),
    evidenceSourceId,
  });

  const fields = {
    [MAP_PARTNER_FACT.approvedValue]: nz(correctValue),
    [MAP_PARTNER_FACT.humanReviewStatus]: recommendHumanReviewStatusAfterCorrection(reviewStatus),
    [MAP_PARTNER_FACT.reviewerNotes]: appendReviewerNotes(fact.reviewerNotes, entry),
    [MAP_PARTNER_FACT.lastUpdated]: todayIsoDate(),
  };

  for (const key of Object.keys(fields)) {
    if (!CORRECTION_PATCH_ALLOWLIST.has(key)) {
      throw new Error(`patch_field_not_allowlisted:${key}`);
    }
  }

  return { fields, reviewerNotesEntry: entry };
}

export async function runApprovedFactCorrection(options) {
  const {
    factId,
    correctValue,
    reason,
    evidenceSourceId = null,
    apply = false,
    approvalPresent = false,
    allowIdentityCorrection = false,
  } = options;

  const fact =
    (await getPartnerFactById(factId).catch((err) => {
      if (process.env.NODE_ENV !== "production") {
        console.error("[approved-fact-correction] getPartnerFactById failed:", err.message);
      }
      return null;
    })) || null;

  const validation = validateApprovedFactCorrectionGates({
    fact,
    correctValue,
    reason,
    applyRequested: apply,
    approvalPresent,
    evidenceSourceId,
    allowIdentityCorrection,
  });

  const result = {
    factCorrectionVersion: FACT_CORRECTION_VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply && approvalPresent ? "apply" : "dry-run",
    factId,
    correctValue: nz(correctValue) || null,
    correctionReason: nz(reason) || null,
    evidenceSourceId: evidenceSourceId || null,
    validation,
    fact: fact
      ? {
          id: fact.id,
          fieldName: fact.fieldName,
          operatorId: fact.operatorId,
          brandId: fact.brandId,
          sourceRecordId: fact.sourceRecordId,
          humanReviewStatus: fact.humanReviewStatus,
          extractedValue: nz(fact.extractedValue) || null,
          approvedValue: nz(fact.approvedValue) || null,
          displayValue: getFactDisplayValue(fact) || null,
          reviewerNotes: nz(fact.reviewerNotes) || null,
        }
      : null,
    writeResult: null,
    safety: {
      platformFieldsTouched: false,
      governanceTouched: false,
      companyValidatedTouched: false,
      sourcesTouched: false,
      extractedValueTouched: false,
      scoringTouched: false,
    },
    rollback: validation.rollbackApprovedValue
      ? {
          note: "Restore Approved Value and prior Reviewer Notes from report if correction must be reverted.",
          factId,
          previousApprovedValue: validation.rollbackApprovedValue,
          previousHumanReviewStatus: fact?.humanReviewStatus || null,
          previousReviewerNotes: nz(fact?.reviewerNotes) || null,
        }
      : null,
  };

  if (!validation.ok) {
    result.blocked = true;
    return result;
  }

  result.plan = validation.plan;

  if (apply && approvalPresent) {
    const { fields } = buildFactCorrectionPatch(fact, {
      correctValue,
      reason,
      evidenceSourceId,
    });

    try {
      const updated = await patchPartnerFact(factId, fields);
      result.writeResult = {
        ok: true,
        fieldsWritten: fields,
        postApply: {
          approvedValue: nz(updated.approvedValue) || null,
          humanReviewStatus: updated.humanReviewStatus || null,
          extractedValue: nz(updated.extractedValue) || null,
          reviewerNotes: nz(updated.reviewerNotes) || null,
        },
      };
    } catch (err) {
      result.writeResult = {
        ok: false,
        error: err.message || "airtable_patch_failed",
      };
      result.blocked = true;
    }
  }

  return result;
}

export function buildApprovedFactCorrectionMarkdown(report) {
  const lines = [
    "# Approved Fact Correction",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: **${report.factCorrectionVersion}**`,
    `Mode: **${report.mode}**`,
    "",
  ];

  if (report.fact) {
    lines.push(
      "## Fact",
      "",
      `- ID: \`${report.fact.id}\``,
      `- Key: \`${report.fact.fieldName}\``,
      `- Operator: ${report.fact.operatorId ? `\`${report.fact.operatorId}\`` : "—"}`,
      `- Brand: ${report.fact.brandId ? `\`${report.fact.brandId}\`` : "—"}`,
      `- Source: ${report.fact.sourceRecordId ? `\`${report.fact.sourceRecordId}\`` : "—"}`,
      ""
    );
  }

  const cur = report.validation?.current;
  if (cur) {
    lines.push(
      "## Current values",
      "",
      `- Extracted Value: ${cur.extractedValue ?? "—"}`,
      `- Approved Value: ${cur.approvedValue ?? "—"}`,
      `- Display value: ${cur.displayValue ?? "—"}`,
      `- Human Review Status: ${cur.humanReviewStatus ?? "—"}`,
      ""
    );
  }

  lines.push(
    "## Proposed correction",
    "",
    `- Corrected Approved Value: **${report.correctValue ?? "—"}**`,
    `- Reason: ${report.correctionReason ?? "—"}`,
    `- Recommended Human Review Status: **${report.validation?.recommendedHumanReviewStatus ?? "—"}**`,
    ""
  );

  if (report.validation?.reviewerNotesEntry) {
    lines.push("## Reviewer Notes entry", "", report.validation.reviewerNotesEntry, "");
  }

  if (!report.validation?.ok) {
    lines.push("## Blocked", "");
    for (const f of report.validation.failures) lines.push(`- ${f}`);
    lines.push("");
    return lines.join("\n");
  }

  lines.push("## Safety", "");
  lines.push("- Extracted Value preserved (not in patch)");
  lines.push("- Sources untouched");
  lines.push("- Platform fields untouched");
  lines.push("- Governance / Company Validated untouched");
  lines.push("");

  if (report.rollback) {
    lines.push("## Rollback", "");
    lines.push(`- Restore Approved Value: ${report.rollback.previousApprovedValue ?? "—"}`);
    lines.push("");
  }

  return lines.join("\n");
}
