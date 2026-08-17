/**
 * Pilot Target List — Airtable field descriptions (internal GTM outreach table).
 *
 * Used by scripts/setup-pilot-target-list-field-descriptions.mjs
 * Keys are exact Airtable column names (see pilot-target-list-field-map.js).
 *
 * This table supports manual/founder-led outreach and mail-merge preparation only —
 * not automated sending.
 */

import { MAP_PILOT_TARGET_LIST } from "./pilot-target-list-field-map.js";

/** @type {Record<string, string>} Airtable field name → description */
export const PILOT_TARGET_LIST_FIELD_DESCRIPTIONS = {
  [MAP_PILOT_TARGET_LIST.name]:
    "Primary contact name for the pilot outreach target.",

  [MAP_PILOT_TARGET_LIST.company]:
    "Company associated with the target. Used for context, filtering, and mail merge company name.",

  [MAP_PILOT_TARGET_LIST.role]:
    "Contact's current role or title.",

  [MAP_PILOT_TARGET_LIST.category]:
    "Original high-level category for the target. Kept for existing reporting and list segmentation.",

  [MAP_PILOT_TARGET_LIST.outreachSegment]:
    "Pilot outreach segment used for messaging, prioritization, and drafting.",

  [MAP_PILOT_TARGET_LIST.region]:
    "Broader geographic coverage or context for the target. This may include multiple regions. Pilot Region is the primary dropdown for current pilot outreach focus.",

  [MAP_PILOT_TARGET_LIST.pilotRegion]:
    "Primary region focus for the pilot outreach target. Use CALA/Mexico/Caribbean/Central America/South America/Latin America for first-wave real pilot opportunities. Use non-CALA regions mainly for feedback, referrals, or workflow validation unless otherwise approved.",

  [MAP_PILOT_TARGET_LIST.email]:
    "Email address used for manual outreach or mail merge export. Required for email-based sends.",

  [MAP_PILOT_TARGET_LIST.linkedInUrl]:
    "LinkedIn profile or company URL used for research and manual LinkedIn outreach.",

  [MAP_PILOT_TARGET_LIST.warmIntro]:
    "Indicates whether this target may be reachable through a warm introduction.",

  [MAP_PILOT_TARGET_LIST.introSource]:
    "Person, relationship, or channel that may be able to provide an introduction.",

  [MAP_PILOT_TARGET_LIST.warmIntroContact]:
    "Specific person who may be asked to make the introduction.",

  [MAP_PILOT_TARGET_LIST.relationshipStrength]:
    "How warm or established the relationship is before outreach.",

  [MAP_PILOT_TARGET_LIST.priority]:
    "Outreach priority for pilot execution. Use P1 for highest-priority targets.",

  [MAP_PILOT_TARGET_LIST.pilotRelevance]:
    "How relevant this target is to the owner/advisor pilot based on likely fit or usefulness.",

  [MAP_PILOT_TARGET_LIST.pilotFit]:
    "Practical fit for pilot participation or useful pilot feedback.",

  [MAP_PILOT_TARGET_LIST.likelyContribution]:
    "What this person may contribute to the pilot, such as a real deal, owner feedback, advisor feedback, operator perspective, or referral.",

  [MAP_PILOT_TARGET_LIST.whyTheyMatter]:
    "Narrative explanation of why this target matters and why we may contact them.",

  [MAP_PILOT_TARGET_LIST.outreachMessageAngle]:
    "High-level reason for outreach. For brands/operators, use criteria input, operator perspective, or owner-opt-in referral only — do not request confidential owner pipelines.",

  [MAP_PILOT_TARGET_LIST.messageAngle]:
    "Narrative version of the message angle, if this field exists separately. Use for a short explanation of why the outreach is relevant.",

  [MAP_PILOT_TARGET_LIST.personalizationLine]:
    "Custom first line or founder-led context to personalize the message.",

  [MAP_PILOT_TARGET_LIST.emailSubject]:
    "Subject line for the email or mail merge export.",

  [MAP_PILOT_TARGET_LIST.emailDraft]:
    "Working draft of the email. Not the final send version unless copied to Final Approved Email.",

  [MAP_PILOT_TARGET_LIST.finalApprovedEmail]:
    "Final approved email body used for manual sending or mail merge export.",

  [MAP_PILOT_TARGET_LIST.linkedInDmDraft]:
    "Short message draft for LinkedIn outreach.",

  [MAP_PILOT_TARGET_LIST.followUpDraft]:
    "Draft follow-up message to use if there is no response or if a reply requires a next step.",

  [MAP_PILOT_TARGET_LIST.status]:
    "Legacy or existing status field. Keep for historical tracking if already used.",

  [MAP_PILOT_TARGET_LIST.outreachStatus]:
    "Manual outreach workflow status. Approved means copy is human-approved; it does not mean sent.",

  [MAP_PILOT_TARGET_LIST.nextAction]:
    "Immediate next manual action for this target.",

  [MAP_PILOT_TARGET_LIST.lastContactDate]:
    "Date this target was last contacted.",

  [MAP_PILOT_TARGET_LIST.lastContactedDate]:
    "Date this target was last contacted. Use this if the table uses this field name instead of Last Contact Date.",

  [MAP_PILOT_TARGET_LIST.nextFollowUpDate]:
    "Date when the next follow-up should occur.",

  [MAP_PILOT_TARGET_LIST.replyNotes]:
    "Notes from replies, conversations, objections, or follow-up context.",

  [MAP_PILOT_TARGET_LIST.notes]:
    "General notes about the target, relationship, or outreach context.",

  [MAP_PILOT_TARGET_LIST.readyForMailMerge]:
    "Checked only after human approval when the record is ready for manual export/sending.",

  [MAP_PILOT_TARGET_LIST.mailMergeBatch]:
    "Batch label used for export, such as Pilot Wave 1 or Pilot Wave 2.",

  [MAP_PILOT_TARGET_LIST.sendChannel]:
    "Primary manual outreach channel. No automated sending is performed by Dealality in this phase.",

  [MAP_PILOT_TARGET_LIST.doNotContact]:
    "Check to exclude this target from outreach and mail merge exports.",

  [MAP_PILOT_TARGET_LIST.doNotContactReason]:
    "Reason this target should not be contacted.",
};

/** Field names we expect on the live table (excludes aliases not created in Airtable). */
export const PILOT_TARGET_LIST_DESCRIPTIONS_FOR_TABLE = Object.keys(
  PILOT_TARGET_LIST_FIELD_DESCRIPTIONS
).filter(
  (name) =>
    name !== MAP_PILOT_TARGET_LIST.messageAngle &&
    name !== MAP_PILOT_TARGET_LIST.lastContactedDate
);

export function hasMeaningfulDescription(description) {
  return Boolean(String(description ?? "").trim());
}

/**
 * Plan description updates without mutating Airtable.
 * @param {Array<{ id: string, name: string, description?: string | null }>} tableFields
 * @param {{ overwrite?: boolean }} options
 */
export function planPilotTargetListDescriptionUpdates(tableFields, options = {}) {
  const overwrite = Boolean(options.overwrite);
  const fieldByName = new Map((tableFields || []).map((f) => [f.name, f]));
  const tableFieldNames = new Set(fieldByName.keys());

  const report = {
    fieldsFound: [...tableFieldNames].sort(),
    descriptionsAlreadyPresent: [],
    descriptionsToAdd: [],
    descriptionsToOverwrite: [],
    descriptionsUnchanged: [],
    fieldsMissingFromTable: [],
    fieldsWithNoDescriptionMap: [],
    fieldsUpdated: [],
    fieldsSkippedExisting: [],
    fieldsFailed: [],
  };

  for (const [fieldName, targetDescription] of Object.entries(
    PILOT_TARGET_LIST_FIELD_DESCRIPTIONS
  )) {
    if (!tableFieldNames.has(fieldName)) {
      report.fieldsMissingFromTable.push(fieldName);
      continue;
    }

    const field = fieldByName.get(fieldName);
    const existing = field.description ?? null;
    const hasExisting = hasMeaningfulDescription(existing);

    if (hasExisting && !overwrite) {
      report.fieldsSkippedExisting.push({
        fieldName,
        fieldId: field.id,
        existingDescription: existing,
      });
      report.descriptionsAlreadyPresent.push(fieldName);
      continue;
    }

    if (hasExisting && overwrite && String(existing).trim() === targetDescription.trim()) {
      report.descriptionsUnchanged.push(fieldName);
      continue;
    }

    const entry = {
      fieldName,
      fieldId: field.id,
      existingDescription: existing,
      targetDescription,
    };

    if (hasExisting && overwrite) {
      report.descriptionsToOverwrite.push(entry);
    } else {
      report.descriptionsToAdd.push(entry);
    }
  }

  for (const field of tableFields || []) {
    if (!PILOT_TARGET_LIST_FIELD_DESCRIPTIONS[field.name]) {
      report.fieldsWithNoDescriptionMap.push(field.name);
    }
  }

  report.fieldsMissingFromTable.sort();
  report.fieldsWithNoDescriptionMap.sort();
  return report;
}

/** @param {typeof planPilotTargetListDescriptionUpdates extends (...args: any[]) => infer R ? R : never} plan */
export function buildManualDescriptionMarkdown(plan, tableMeta = {}) {
  const lines = [
    "# Pilot Target List — manual field descriptions",
    "",
    "Use if Airtable Meta API description updates are unavailable.",
    "",
    `Base: \`${tableMeta.baseId || "AIRTABLE_GTM_BASE_ID"}\``,
    `Table: **${tableMeta.tableName || "Pilot Target List"}** (\`${tableMeta.tableId || ""}\`)`,
    "",
    "## Fields to add or update",
    "",
  ];

  const pending = [...plan.descriptionsToAdd, ...plan.descriptionsToOverwrite];
  for (const item of pending) {
    lines.push(`### ${item.fieldName}`);
    lines.push("");
    lines.push(item.targetDescription);
    lines.push("");
  }

  if (plan.fieldsSkippedExisting.length) {
    lines.push("## Fields skipped (existing descriptions)");
    lines.push("");
    for (const item of plan.fieldsSkippedExisting) {
      lines.push(`- **${item.fieldName}**: ${item.existingDescription}`);
    }
    lines.push("");
  }

  if (plan.fieldsMissingFromTable.length) {
    lines.push("## Fields in map but not on table");
    lines.push("");
    for (const name of plan.fieldsMissingFromTable) {
      lines.push(`- ${name} — ${PILOT_TARGET_LIST_FIELD_DESCRIPTIONS[name]}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}
