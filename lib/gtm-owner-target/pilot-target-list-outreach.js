/**
 * Pilot Target List outreach helpers — field specs, validation, mail-merge row shaping.
 */
import {
  MAP_PILOT_TARGET_LIST,
  PILOT_FIELD_EQUIVALENTS,
  PILOT_OUTREACH_FIELDS_TO_ENSURE,
  VAL_PILOT_FIT,
  VAL_PILOT_OUTREACH_SEGMENT,
  VAL_PILOT_OUTREACH_STATUS,
  VAL_PILOT_REGION,
  VAL_PILOT_SEND_CHANNEL,
} from "./pilot-target-list-field-map.js";

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

/** @returns {import("./pilot-target-list-field-map.js").MAP_PILOT_TARGET_LIST[keyof typeof MAP_PILOT_TARGET_LIST][]} */
export function getExistingPilotTargetFieldNames() {
  return [
    MAP_PILOT_TARGET_LIST.name,
    MAP_PILOT_TARGET_LIST.company,
    MAP_PILOT_TARGET_LIST.role,
    MAP_PILOT_TARGET_LIST.category,
    MAP_PILOT_TARGET_LIST.region,
    MAP_PILOT_TARGET_LIST.whyTheyMatter,
    MAP_PILOT_TARGET_LIST.warmIntro,
    MAP_PILOT_TARGET_LIST.status,
    MAP_PILOT_TARGET_LIST.relationshipStrength,
    MAP_PILOT_TARGET_LIST.pilotRelevance,
    MAP_PILOT_TARGET_LIST.likelyContribution,
    MAP_PILOT_TARGET_LIST.nextAction,
    MAP_PILOT_TARGET_LIST.lastContactDate,
    MAP_PILOT_TARGET_LIST.nextFollowUpDate,
    MAP_PILOT_TARGET_LIST.outreachMessageAngle,
    MAP_PILOT_TARGET_LIST.priority,
  ];
}

/** Airtable Meta API field specs for fields the setup script may create. */
export function buildOutreachFieldSpecs() {
  return [
    {
      mapKey: "email",
      spec: { name: MAP_PILOT_TARGET_LIST.email, type: "email" },
    },
    {
      mapKey: "linkedInUrl",
      spec: { name: MAP_PILOT_TARGET_LIST.linkedInUrl, type: "url" },
    },
    {
      mapKey: "notes",
      spec: { name: MAP_PILOT_TARGET_LIST.notes, type: "multilineText" },
    },
    {
      mapKey: "pilotRegion",
      spec: {
        name: MAP_PILOT_TARGET_LIST.pilotRegion,
        type: "singleSelect",
        options: choices(VAL_PILOT_REGION),
        description: "Structured region focus for pilot targeting (CALA-first, not CALA-only).",
      },
    },
    {
      mapKey: "outreachSegment",
      spec: {
        name: MAP_PILOT_TARGET_LIST.outreachSegment,
        type: "singleSelect",
        options: choices(VAL_PILOT_OUTREACH_SEGMENT),
        description: "Pilot outreach segment (broader than Category picklist).",
      },
    },
    {
      mapKey: "pilotFit",
      spec: {
        name: MAP_PILOT_TARGET_LIST.pilotFit,
        type: "singleSelect",
        options: choices(VAL_PILOT_FIT),
        description: "Pilot fit for this wave (complements Pilot Relevance).",
      },
    },
    {
      mapKey: "personalizationLine",
      spec: {
        name: MAP_PILOT_TARGET_LIST.personalizationLine,
        type: "multilineText",
        description: "Custom first line or founder-led context.",
      },
    },
    {
      mapKey: "emailSubject",
      spec: { name: MAP_PILOT_TARGET_LIST.emailSubject, type: "singleLineText" },
    },
    {
      mapKey: "emailDraft",
      spec: {
        name: MAP_PILOT_TARGET_LIST.emailDraft,
        type: "multilineText",
        description: "Draft email body — not necessarily approved.",
      },
    },
    {
      mapKey: "finalApprovedEmail",
      spec: {
        name: MAP_PILOT_TARGET_LIST.finalApprovedEmail,
        type: "multilineText",
        description: "Final approved copy for manual send or mail merge export.",
      },
    },
    {
      mapKey: "linkedInDmDraft",
      spec: { name: MAP_PILOT_TARGET_LIST.linkedInDmDraft, type: "multilineText" },
    },
    {
      mapKey: "followUpDraft",
      spec: { name: MAP_PILOT_TARGET_LIST.followUpDraft, type: "multilineText" },
    },
    {
      mapKey: "outreachStatus",
      spec: {
        name: MAP_PILOT_TARGET_LIST.outreachStatus,
        type: "singleSelect",
        options: choices(VAL_PILOT_OUTREACH_STATUS),
        description: "Pilot outreach workflow status (separate from legacy Status field).",
      },
    },
    {
      mapKey: "replyNotes",
      spec: { name: MAP_PILOT_TARGET_LIST.replyNotes, type: "multilineText" },
    },
    {
      mapKey: "readyForMailMerge",
      spec: {
        name: MAP_PILOT_TARGET_LIST.readyForMailMerge,
        type: "checkbox",
        options: { icon: "check", color: "greenBright" },
      },
    },
    {
      mapKey: "mailMergeBatch",
      spec: {
        name: MAP_PILOT_TARGET_LIST.mailMergeBatch,
        type: "singleLineText",
        description: "e.g. Pilot Wave 1, Pilot Wave 2",
      },
    },
    {
      mapKey: "sendChannel",
      spec: {
        name: MAP_PILOT_TARGET_LIST.sendChannel,
        type: "singleSelect",
        options: choices(VAL_PILOT_SEND_CHANNEL),
      },
    },
    {
      mapKey: "introSource",
      spec: { name: MAP_PILOT_TARGET_LIST.introSource, type: "singleLineText" },
    },
    {
      mapKey: "warmIntroContact",
      spec: { name: MAP_PILOT_TARGET_LIST.warmIntroContact, type: "singleLineText" },
    },
    {
      mapKey: "doNotContact",
      spec: {
        name: MAP_PILOT_TARGET_LIST.doNotContact,
        type: "checkbox",
        options: { icon: "xCheckbox", color: "redBright" },
      },
    },
    {
      mapKey: "doNotContactReason",
      spec: { name: MAP_PILOT_TARGET_LIST.doNotContactReason, type: "multilineText" },
    },
  ].filter((item) => PILOT_OUTREACH_FIELDS_TO_ENSURE.includes(item.mapKey));
}

/**
 * Required → existing → add new mapping for operator review.
 * @param {Set<string>} existingFieldNames
 */
export function buildFieldMappingReport(existingFieldNames) {
  const existing = existingFieldNames || new Set();

  const rows = [
    row("Name", MAP_PILOT_TARGET_LIST.name, existing),
    row("Company", MAP_PILOT_TARGET_LIST.company, existing, "Linked record → Companies"),
    row("Role", MAP_PILOT_TARGET_LIST.role, existing),
    row("Category", MAP_PILOT_TARGET_LIST.category, existing, "Partial overlap with Outreach Segment"),
    row("Region", MAP_PILOT_TARGET_LIST.region, existing),
    row("Pilot Region", MAP_PILOT_TARGET_LIST.pilotRegion, existing, "Structured dropdown region focus"),
    row("Email", MAP_PILOT_TARGET_LIST.email, existing),
    row("LinkedIn URL", MAP_PILOT_TARGET_LIST.linkedInUrl, existing),
    row("Warm Intro?", MAP_PILOT_TARGET_LIST.warmIntro, existing),
    row("Status (legacy)", MAP_PILOT_TARGET_LIST.status, existing, "Keep — not Outreach Status workflow"),
    row("Priority", MAP_PILOT_TARGET_LIST.priority, existing, "P1/P2/P3 already configured"),
    row("Pilot Relevance", MAP_PILOT_TARGET_LIST.pilotRelevance, existing, "High/Medium/Low — keep alongside Pilot Fit"),
    row("Likely Contribution", MAP_PILOT_TARGET_LIST.likelyContribution, existing),
    row("Next Action", MAP_PILOT_TARGET_LIST.nextAction, existing),
    row("Last Contacted Date", MAP_PILOT_TARGET_LIST.lastContactDate, existing, "Existing Last Contact Date"),
    row("Next Follow-Up Date", MAP_PILOT_TARGET_LIST.nextFollowUpDate, existing),
    row("Message Angle (taxonomy)", MAP_PILOT_TARGET_LIST.outreachMessageAngle, existing, "Single select angle picklist"),
    row("Message Angle (free text)", MAP_PILOT_TARGET_LIST.whyTheyMatter, existing, "Use Why They Matter for narrative"),
    row("Relationship Strength", MAP_PILOT_TARGET_LIST.relationshipStrength, existing),
    row("Notes", MAP_PILOT_TARGET_LIST.notes, existing),
    row("Outreach Segment", MAP_PILOT_TARGET_LIST.outreachSegment, existing, "Category is related but different options"),
    row("Pilot Fit", MAP_PILOT_TARGET_LIST.pilotFit, existing),
    row("Personalization Line", MAP_PILOT_TARGET_LIST.personalizationLine, existing),
    row("Email Subject", MAP_PILOT_TARGET_LIST.emailSubject, existing),
    row("Email Draft", MAP_PILOT_TARGET_LIST.emailDraft, existing),
    row("Final Approved Email", MAP_PILOT_TARGET_LIST.finalApprovedEmail, existing),
    row("LinkedIn DM Draft", MAP_PILOT_TARGET_LIST.linkedInDmDraft, existing),
    row("Follow-Up Draft", MAP_PILOT_TARGET_LIST.followUpDraft, existing),
    row("Outreach Status", MAP_PILOT_TARGET_LIST.outreachStatus, existing, "New workflow field — legacy Status unchanged"),
    row("Reply Notes", MAP_PILOT_TARGET_LIST.replyNotes, existing),
    row("Ready for Mail Merge", MAP_PILOT_TARGET_LIST.readyForMailMerge, existing),
    row("Mail Merge Batch", MAP_PILOT_TARGET_LIST.mailMergeBatch, existing),
    row("Send Channel", MAP_PILOT_TARGET_LIST.sendChannel, existing),
    row("Intro Source", MAP_PILOT_TARGET_LIST.introSource, existing),
    row("Warm Intro Contact", MAP_PILOT_TARGET_LIST.warmIntroContact, existing),
    row("Do Not Contact", MAP_PILOT_TARGET_LIST.doNotContact, existing),
    row("Do Not Contact Reason", MAP_PILOT_TARGET_LIST.doNotContactReason, existing),
  ];

  for (const [requiredKey, equiv] of Object.entries(PILOT_FIELD_EQUIVALENTS)) {
    const equivName = MAP_PILOT_TARGET_LIST[equiv.existingKey];
    const labelByKey = {
      messageAngle: "Message Angle (taxonomy)",
      lastContactedDate: "Last Contacted Date",
    };
    const targetIdx = rows.findIndex((r) => r.requiredField === labelByKey[requiredKey]);
    if (targetIdx >= 0) {
      rows[targetIdx].existingField = equivName;
      rows[targetIdx].addNewField = "No";
      rows[targetIdx].notes = equiv.notes;
    }
  }

  return rows;
}

function row(requiredField, airtableName, existingSet, notes = "") {
  const exists = existingSet.has(airtableName);
  return {
    requiredField,
    existingField: exists ? airtableName : null,
    addNewField: exists ? "No" : "Yes",
    notes,
  };
}

/** @param {string} fullName */
export function splitContactName(fullName) {
  const trimmed = String(fullName || "").trim();
  if (!trimmed) {
    return { firstName: "", lastName: "", fullName: "" };
  }
  const parts = trimmed.split(/\s+/);
  if (parts.length === 1) {
    return { firstName: parts[0], lastName: "", fullName: trimmed };
  }
  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(" "),
    fullName: trimmed,
  };
}

/**
 * Resolve company display name from linked record ids.
 * @param {unknown} companyField
 * @param {Map<string, string>} companyNameById
 */
export function resolveCompanyName(companyField, companyNameById) {
  if (typeof companyField === "string" && companyField.trim()) return companyField.trim();
  if (Array.isArray(companyField) && companyField.length) {
    const names = companyField
      .map((id) => companyNameById.get(id))
      .filter(Boolean);
    if (names.length) return names.join(", ");
  }
  return "";
}

/**
 * @param {Record<string, unknown>} fields
 * @param {Map<string, string>} companyNameById
 */
export function recordToMailMergeRow(recordId, fields, companyNameById, options = {}) {
  const channel = String(options.channel || "Email");
  const batchFilter = options.batch ? String(options.batch).trim() : "";
  const statusRequired = String(options.status || "Approved");

  const warnings = [];
  const outreachStatus = String(fields[MAP_PILOT_TARGET_LIST.outreachStatus] || "");
  const ready = Boolean(fields[MAP_PILOT_TARGET_LIST.readyForMailMerge]);
  const doNotContact = Boolean(fields[MAP_PILOT_TARGET_LIST.doNotContact]);
  const email = String(fields[MAP_PILOT_TARGET_LIST.email] || "").trim();
  const subject = String(fields[MAP_PILOT_TARGET_LIST.emailSubject] || "").trim();
  const message = String(fields[MAP_PILOT_TARGET_LIST.finalApprovedEmail] || "").trim();
  const sendChannel = String(fields[MAP_PILOT_TARGET_LIST.sendChannel] || channel).trim();
  const mailMergeBatch = String(fields[MAP_PILOT_TARGET_LIST.mailMergeBatch] || "").trim();
  const linkedIn = String(fields[MAP_PILOT_TARGET_LIST.linkedInUrl] || "").trim();
  const pilotRegion = String(fields[MAP_PILOT_TARGET_LIST.pilotRegion] || "").trim();
  const nameParts = splitContactName(fields[MAP_PILOT_TARGET_LIST.name]);

  if (doNotContact) {
    return { skip: true, reason: "do_not_contact", warnings, row: null };
  }
  if (outreachStatus !== statusRequired) {
    return { skip: true, reason: "status_mismatch", warnings, row: null };
  }
  if (!ready) {
    return { skip: true, reason: "not_ready_for_mail_merge", warnings, row: null };
  }
  if (batchFilter && mailMergeBatch !== batchFilter) {
    return { skip: true, reason: "batch_mismatch", warnings, row: null };
  }
  if (sendChannel !== channel) {
    return { skip: true, reason: "channel_mismatch", warnings, row: null };
  }
  if (!subject) warnings.push("missing_email_subject");
  if (!message) warnings.push("missing_final_approved_email");
  if (channel === "Email" && !email) warnings.push("missing_email");

  if (warnings.length) {
    return { skip: true, reason: "incomplete", warnings, row: null, recordId, name: nameParts.fullName };
  }

  return {
    skip: false,
    warnings,
    row: {
      email,
      first_name: nameParts.firstName,
      last_name: nameParts.lastName,
      full_name: nameParts.fullName,
      company: resolveCompanyName(fields[MAP_PILOT_TARGET_LIST.company], companyNameById),
      role: String(fields[MAP_PILOT_TARGET_LIST.role] || ""),
      pilot_region: pilotRegion,
      subject,
      message,
      linkedin_url: linkedIn,
      send_channel: sendChannel,
      mail_merge_batch: mailMergeBatch,
      airtable_record_id: recordId,
    },
  };
}

export const MAIL_MERGE_CSV_HEADERS = [
  "email",
  "first_name",
  "last_name",
  "full_name",
  "company",
  "role",
  "pilot_region",
  "subject",
  "message",
  "linkedin_url",
  "send_channel",
  "mail_merge_batch",
  "airtable_record_id",
];

export function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function rowsToCsv(rows) {
  const lines = [MAIL_MERGE_CSV_HEADERS.join(",")];
  for (const row of rows) {
    lines.push(MAIL_MERGE_CSV_HEADERS.map((h) => csvEscape(row[h])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

/** Manual Airtable view setup instructions (Meta API view creation is not used). */
export function getPilotOutreachViewInstructions() {
  return [
    {
      name: "Pilot Outreach Pipeline",
      purpose: "Daily working view for outreach.",
      filter: "None (all active targets)",
      visibleFields: [
        MAP_PILOT_TARGET_LIST.name,
        MAP_PILOT_TARGET_LIST.company,
        MAP_PILOT_TARGET_LIST.role,
        MAP_PILOT_TARGET_LIST.category,
        MAP_PILOT_TARGET_LIST.outreachSegment,
        MAP_PILOT_TARGET_LIST.pilotRegion,
        MAP_PILOT_TARGET_LIST.region,
        MAP_PILOT_TARGET_LIST.priority,
        MAP_PILOT_TARGET_LIST.pilotFit,
        MAP_PILOT_TARGET_LIST.pilotRelevance,
        MAP_PILOT_TARGET_LIST.warmIntro,
        MAP_PILOT_TARGET_LIST.relationshipStrength,
        MAP_PILOT_TARGET_LIST.likelyContribution,
        MAP_PILOT_TARGET_LIST.outreachMessageAngle,
        MAP_PILOT_TARGET_LIST.whyTheyMatter,
        MAP_PILOT_TARGET_LIST.outreachStatus,
        MAP_PILOT_TARGET_LIST.nextAction,
        MAP_PILOT_TARGET_LIST.lastContactDate,
        MAP_PILOT_TARGET_LIST.nextFollowUpDate,
        MAP_PILOT_TARGET_LIST.replyNotes,
      ],
      sort: [{ field: MAP_PILOT_TARGET_LIST.priority, direction: "asc" }],
    },
    {
      name: "Drafting Queue",
      purpose: "Copy drafting and review.",
      filter: `OR({${MAP_PILOT_TARGET_LIST.outreachStatus}}='Draft Needed',{${MAP_PILOT_TARGET_LIST.outreachStatus}}='Drafted',{${MAP_PILOT_TARGET_LIST.outreachStatus}}='Needs Review')`,
      visibleFields: [
        MAP_PILOT_TARGET_LIST.name,
        MAP_PILOT_TARGET_LIST.company,
        MAP_PILOT_TARGET_LIST.role,
        MAP_PILOT_TARGET_LIST.outreachSegment,
        MAP_PILOT_TARGET_LIST.pilotRegion,
        MAP_PILOT_TARGET_LIST.priority,
        MAP_PILOT_TARGET_LIST.pilotFit,
        MAP_PILOT_TARGET_LIST.outreachMessageAngle,
        MAP_PILOT_TARGET_LIST.whyTheyMatter,
        MAP_PILOT_TARGET_LIST.personalizationLine,
        MAP_PILOT_TARGET_LIST.emailSubject,
        MAP_PILOT_TARGET_LIST.emailDraft,
        MAP_PILOT_TARGET_LIST.linkedInDmDraft,
        MAP_PILOT_TARGET_LIST.followUpDraft,
      ],
    },
    {
      name: "Approved for Send / Mail Merge",
      purpose: "Ready rows for manual send or CSV export.",
      filter: `AND({${MAP_PILOT_TARGET_LIST.outreachStatus}}='Approved',{${MAP_PILOT_TARGET_LIST.readyForMailMerge}},NOT({${MAP_PILOT_TARGET_LIST.doNotContact}))`,
      visibleFields: [
        MAP_PILOT_TARGET_LIST.name,
        MAP_PILOT_TARGET_LIST.email,
        MAP_PILOT_TARGET_LIST.company,
        MAP_PILOT_TARGET_LIST.role,
        MAP_PILOT_TARGET_LIST.pilotRegion,
        MAP_PILOT_TARGET_LIST.emailSubject,
        MAP_PILOT_TARGET_LIST.finalApprovedEmail,
        MAP_PILOT_TARGET_LIST.linkedInDmDraft,
        MAP_PILOT_TARGET_LIST.sendChannel,
        MAP_PILOT_TARGET_LIST.mailMergeBatch,
        MAP_PILOT_TARGET_LIST.readyForMailMerge,
        MAP_PILOT_TARGET_LIST.outreachStatus,
        MAP_PILOT_TARGET_LIST.lastContactDate,
        MAP_PILOT_TARGET_LIST.nextFollowUpDate,
        MAP_PILOT_TARGET_LIST.doNotContact,
        MAP_PILOT_TARGET_LIST.doNotContactReason,
      ],
    },
    {
      name: "Follow-Up Needed",
      purpose: "Replies and scheduled follow-ups.",
      filter: `OR({${MAP_PILOT_TARGET_LIST.outreachStatus}}='Follow-Up Needed',IS_BEFORE({${MAP_PILOT_TARGET_LIST.nextFollowUpDate}}, TODAY()))`,
      visibleFields: [
        MAP_PILOT_TARGET_LIST.name,
        MAP_PILOT_TARGET_LIST.email,
        MAP_PILOT_TARGET_LIST.company,
        MAP_PILOT_TARGET_LIST.role,
        MAP_PILOT_TARGET_LIST.pilotRegion,
        MAP_PILOT_TARGET_LIST.sendChannel,
        MAP_PILOT_TARGET_LIST.lastContactDate,
        MAP_PILOT_TARGET_LIST.nextFollowUpDate,
        MAP_PILOT_TARGET_LIST.followUpDraft,
        MAP_PILOT_TARGET_LIST.replyNotes,
        MAP_PILOT_TARGET_LIST.outreachStatus,
        MAP_PILOT_TARGET_LIST.notes,
        MAP_PILOT_TARGET_LIST.doNotContact,
      ],
    },
  ];
}
