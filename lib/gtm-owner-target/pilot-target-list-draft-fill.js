/**
 * Pilot Target List — draft fill logic (pure functions, no Airtable writes).
 */
import { MAP_PILOT_TARGET_LIST } from "./pilot-target-list-field-map.js";
import {
  EMAIL_DRAFT_CTA,
  PILOT_OUTREACH_SENDER_NAME,
  SEGMENT_DRAFT_TEMPLATES,
  buildFollowUpDraft,
  buildLinkedInDm,
  pickSubject,
} from "./pilot-outreach-draft-templates.js";
import { splitContactName } from "./pilot-target-list-outreach.js";

const F = MAP_PILOT_TARGET_LIST;

/** Fields the fill script may write (never Final Approved Email or Ready for Mail Merge). */
export const FILLABLE_FIELD_KEYS = [
  "outreachSegment",
  "pilotFit",
  "whyTheyMatter",
  "personalizationLine",
  "emailSubject",
  "emailDraft",
  "linkedInDmDraft",
  "followUpDraft",
  "outreachStatus",
  "nextAction",
  "sendChannel",
  "notes",
];

export const CONTEXT_FIELD_KEYS = [
  "name",
  "company",
  "role",
  "category",
  "pilotRegion",
  "region",
  "whyTheyMatter",
  "outreachMessageAngle",
  "priority",
  "pilotRelevance",
  "likelyContribution",
];

export const DEFAULT_ELIGIBLE_OUTREACH_STATUSES = new Set([
  "",
  "Not Started",
  "Draft Needed",
]);

export const BLOCKED_OUTREACH_STATUSES = new Set(["Approved", "Sent"]);

/** @param {unknown} value */
export function hasText(value) {
  if (value == null) return false;
  if (Array.isArray(value)) return value.length > 0;
  return Boolean(String(value).trim());
}

/** @param {Record<string, unknown>} fields */
export function hasMinimumContext(fields) {
  return CONTEXT_FIELD_KEYS.some((key) => hasText(fields[F[key]]));
}

/** @param {unknown} companyField @param {Map<string, string>} companyNameById */
export function resolveCompanyLabel(companyField, companyNameById = new Map()) {
  if (typeof companyField === "string" && companyField.trim()) return companyField.trim();
  if (Array.isArray(companyField) && companyField.length) {
    const names = companyField.map((id) => companyNameById.get(id)).filter(Boolean);
    if (names.length) return names.join(", ");
  }
  return "";
}

/** @param {string} category */
export function inferOutreachSegment(category, role = "") {
  const c = String(category || "").trim().toLowerCase();
  const r = String(role || "").trim().toLowerCase();

  if (c === "owner" || /owner|investor|developer/.test(r)) return "Owner / Investor";
  if (c === "advisor" || c === "broker" || c === "consultant") return "Advisor / Consultant / Broker";
  if (c.includes("referral") || c.includes("brand")) return "Brand / Referral Source";
  if (c === "operator" || (c.includes("operator") && !c.includes("referral"))) return "Operator";
  if (/capital|investment|lender|fund/.test(r)) return "Capital Partner";

  if (/advisor|consultant|broker/.test(r)) return "Advisor / Consultant / Broker";
  if (/operator|management/.test(r)) return "Operator";
  if (/owner|investor|developer/.test(r)) return "Owner / Investor";

  return "Other";
}

/** @param {Record<string, unknown>} fields */
export function inferPilotFit(fields) {
  const relevance = String(fields[F.pilotRelevance] || "").trim();
  const priority = String(fields[F.priority] || "").trim();
  const relationship = String(fields[F.relationshipStrength] || "").trim();
  const warmIntro = Boolean(fields[F.warmIntro]);
  const category = String(fields[F.category] || "").trim();

  if (relevance === "Low") return { fit: "Weak Fit", confidence: "medium" };
  if (category && /referral source/i.test(category) && relevance !== "High") {
    return { fit: "Feedback / Referral Only", confidence: "medium" };
  }
  if (
    relevance === "High" ||
    (priority === "P1" && (relationship === "Strong Warm Relationship" || relationship === "Known Contact" || warmIntro))
  ) {
    return { fit: "Strong Pilot Candidate", confidence: "high" };
  }
  if (
    relevance === "Medium" ||
    priority === "P2" ||
    relationship === "Met Once" ||
    relationship === "LinkedIn / Light Connection" ||
    warmIntro
  ) {
    return { fit: "Possible Pilot Candidate", confidence: "medium" };
  }
  if (relationship === "Strong Warm Relationship" || relationship === "Known Contact") {
    return { fit: "Possible Pilot Candidate", confidence: "low" };
  }
  return { fit: "Weak Fit", confidence: "low" };
}

function normalizePilotRegion(value) {
  const v = String(value || "").trim();
  return v;
}

function isCalaFirstPilotRegion(region) {
  return new Set(["CALA", "Mexico", "Caribbean", "Central America", "South America", "Latin America"]).has(
    normalizePilotRegion(region)
  );
}

function isNonCalaFeedbackRegion(region) {
  return new Set(["United States / Canada", "Europe / Spain", "Global / Multi-Region", "Other"]).has(
    normalizePilotRegion(region)
  );
}

function buildEmailDraftWithContext(firstName, segment, personalizationLine, regionNote) {
  const tpl = SEGMENT_DRAFT_TEMPLATES[segment] || SEGMENT_DRAFT_TEMPLATES.Other;
  const greeting = firstName ? `Hi ${firstName},` : "Hi,";
  const body = [
    greeting,
    "",
    personalizationLine || tpl.personalization,
    "",
    tpl.emailPilotLine,
  ];
  if (regionNote) {
    body.push("", regionNote);
  }
  body.push(
    "",
    tpl.emailIdeaLine,
    "",
    tpl.emailSmallGroupLine,
    "",
    EMAIL_DRAFT_CTA,
    "",
    "At your service,",
    PILOT_OUTREACH_SENDER_NAME
  );
  return body.join("\n");
}

/** @param {Record<string, unknown>} fields @param {string} segment */
export function inferWhyTheyMatter(fields, segment, companyLabel) {
  if (hasText(fields[F.whyTheyMatter])) return null;
  const tpl = SEGMENT_DRAFT_TEMPLATES[segment] || SEGMENT_DRAFT_TEMPLATES.Other;
  const parts = [tpl.whyTheyMatter];
  const category = String(fields[F.category] || "").trim();
  if (category) parts.push(`Category: ${category}.`);
  if (companyLabel) parts.push(`Company context: ${companyLabel}.`);
  return parts.join(" ");
}

/** @param {Record<string, unknown>} fields */
export function inferSendChannel(fields) {
  const email = hasText(fields[F.email]);
  const linkedIn = hasText(fields[F.linkedInUrl]);
  const warmIntro = Boolean(fields[F.warmIntro]);

  if (email) return { channel: "Email", confidence: "high" };
  if (warmIntro) return { channel: "Warm Intro", confidence: "medium" };
  if (linkedIn) return { channel: "LinkedIn", confidence: "high" };
  return { channel: "", confidence: "low" };
}

/** @param {Record<string, unknown>} fields @param {string[]} reviewReasons */
export function inferOutreachStatus(fields, reviewReasons, draftsCreated) {
  const current = String(fields[F.outreachStatus] || "").trim();
  if (BLOCKED_OUTREACH_STATUSES.has(current)) return null;

  if (reviewReasons.length) return "Needs Review";
  if (draftsCreated) return "Drafted";
  return "Draft Needed";
}

/** @param {Record<string, unknown>} fields @param {string[]} reviewReasons @param {{ channel: string }} sendChannel */
export function inferNextAction(fields, reviewReasons, sendChannel) {
  const actions = [];
  if (!hasText(fields[F.email]) && (sendChannel.channel === "Email" || !sendChannel.channel)) {
    actions.push("Find email address");
  }
  if (!hasText(fields[F.linkedInUrl]) && sendChannel.channel === "LinkedIn") {
    actions.push("Find LinkedIn URL");
  }
  if (Boolean(fields[F.warmIntro]) && !hasText(fields[F.introSource]) && !hasText(fields[F.warmIntroContact])) {
    actions.push("Confirm warm intro path");
  }
  if (reviewReasons.includes("segment_unclear")) actions.push("Confirm segment");
  if (reviewReasons.includes("pilot_fit_unclear")) actions.push("Confirm pilot fit");
  if (!hasText(fields[F.emailDraft]) && !hasText(fields[F.linkedInDmDraft])) {
    actions.push("Review draft and approve message");
  } else {
    actions.push("Review draft and approve message");
  }
  if (sendChannel.channel === "Email" && hasText(fields[F.email])) {
    actions.push("Send manual founder email");
  } else if (sendChannel.channel === "LinkedIn" && hasText(fields[F.linkedInUrl])) {
    actions.push("Send LinkedIn DM");
  } else if (sendChannel.channel === "Warm Intro") {
    actions.push("Confirm warm intro path");
  } else if (!sendChannel.channel) {
    actions.push("Confirm channel");
  }

  return [...new Set(actions)].slice(0, 3).join("; ");
}

/**
 * @param {object} params
 * @param {string} params.recordId
 * @param {Record<string, unknown>} params.fields
 * @param {Map<string, string>} [params.companyNameById]
 * @param {boolean} [params.overwrite]
 * @param {Set<string>} [params.eligibleStatuses]
 */
export function buildDraftFillPlan(params) {
  const {
    recordId,
    fields,
    companyNameById = new Map(),
    overwrite = false,
    eligibleStatuses = DEFAULT_ELIGIBLE_OUTREACH_STATUSES,
  } = params;

  const result = {
    recordId,
    name: String(fields[F.name] || ""),
    skipped: false,
    skipReason: null,
    reviewReasons: [],
    segmentConfidence: "high",
    fitConfidence: "high",
    changes: /** @type {Record<string, { before: unknown, after: unknown }>} */ ({}),
    patch: /** @type {Record<string, unknown>} */ ({}),
  };

  if (Boolean(fields[F.doNotContact])) {
    result.skipped = true;
    result.skipReason = "do_not_contact";
    return result;
  }

  const currentStatus = String(fields[F.outreachStatus] || "").trim();
  if (BLOCKED_OUTREACH_STATUSES.has(currentStatus)) {
    result.skipped = true;
    result.skipReason = "already_approved_or_sent";
    return result;
  }
  if (!eligibleStatuses.has(currentStatus)) {
    result.skipped = true;
    result.skipReason = "outreach_status_not_eligible";
    return result;
  }
  if (!hasMinimumContext(fields)) {
    result.skipped = true;
    result.skipReason = "insufficient_context";
    return result;
  }

  const companyLabel = resolveCompanyLabel(fields[F.company], companyNameById);
  const category = String(fields[F.category] || "").trim();
  const role = String(fields[F.role] || "").trim();
  const pilotRegion = String(fields[F.pilotRegion] || "").trim();
  const segment = inferOutreachSegment(category, role);
  if (segment === "Other" && !category && !role) {
    result.reviewReasons.push("segment_unclear");
    result.segmentConfidence = "low";
  } else if (segment === "Other") {
    result.reviewReasons.push("segment_unclear");
    result.segmentConfidence = "medium";
  }

  const { fit, confidence: fitConfidence } = inferPilotFit(fields);
  result.fitConfidence = fitConfidence;
  if (fitConfidence === "low") result.reviewReasons.push("pilot_fit_unclear");

  const firstName = splitContactName(String(fields[F.name] || "")).firstName;
  const tpl = SEGMENT_DRAFT_TEMPLATES[segment] || SEGMENT_DRAFT_TEMPLATES.Other;
  const nonCalaNote =
    isNonCalaFeedbackRegion(pilotRegion) &&
    (segment === "Owner / Investor" || segment === "Advisor / Consultant / Broker")
      ? "The platform is currently most developed around CALA, but I'd value your perspective on the workflow itself."
      : "";
  const likelyPilotParticipant =
    isCalaFirstPilotRegion(pilotRegion) &&
    (segment === "Owner / Investor" || segment === "Advisor / Consultant / Broker");

  const proposed = {
    [F.outreachSegment]: segment,
    [F.pilotFit]: fit,
    [F.whyTheyMatter]: inferWhyTheyMatter(fields, segment, companyLabel),
    [F.personalizationLine]: likelyPilotParticipant
      ? tpl.personalization
      : nonCalaNote
        ? `${tpl.personalization} ${nonCalaNote}`
        : tpl.personalization,
    [F.emailSubject]: pickSubject(segment, recordId),
    [F.emailDraft]: buildEmailDraftWithContext(
      firstName,
      segment,
      tpl.personalization,
      nonCalaNote
    ),
    [F.linkedInDmDraft]: buildLinkedInDm(firstName, segment),
    [F.followUpDraft]: buildFollowUpDraft(firstName),
  };

  const sendChannel = inferSendChannel(fields);
  if (sendChannel.channel) proposed[F.sendChannel] = sendChannel.channel;
  if (!sendChannel.channel) result.reviewReasons.push("channel_unclear");

  if (!hasText(fields[F.email])) result.reviewReasons.push("missing_email");
  if (!hasText(fields[F.linkedInUrl]) && sendChannel.channel === "LinkedIn") {
    result.reviewReasons.push("missing_linkedin");
  }

  let draftsCreated = false;
  const draftFieldNames = new Set([F.emailDraft, F.linkedInDmDraft, F.followUpDraft]);

  for (const key of FILLABLE_FIELD_KEYS) {
    const airtableName = F[key];
    if (!airtableName || airtableName === F.finalApprovedEmail || airtableName === F.readyForMailMerge) {
      continue;
    }
    if (key === "outreachStatus" || key === "nextAction") continue;

    const value = proposed[airtableName];
    if (value == null || !hasText(value)) continue;

    const before = fields[airtableName];
    const beforeEmpty = !hasText(before);
    if (!beforeEmpty && !overwrite) continue;
    if (!beforeEmpty && overwrite && String(before).trim() === String(value).trim()) continue;

    result.changes[airtableName] = { before: before ?? null, after: value };
    result.patch[airtableName] = value;
    if (draftFieldNames.has(airtableName)) draftsCreated = true;
  }

  const status = inferOutreachStatus(fields, result.reviewReasons, draftsCreated);
  if (status && (!hasText(fields[F.outreachStatus]) || overwrite)) {
    if (String(fields[F.outreachStatus] || "").trim() !== status) {
      result.changes[F.outreachStatus] = { before: fields[F.outreachStatus] ?? null, after: status };
      result.patch[F.outreachStatus] = status;
    }
  }

  const nextAction = inferNextAction(fields, result.reviewReasons, sendChannel);
  if (nextAction && (!hasText(fields[F.nextAction]) || overwrite)) {
    if (String(fields[F.nextAction] || "").trim() !== nextAction) {
      result.changes[F.nextAction] = { before: fields[F.nextAction] ?? null, after: nextAction };
      result.patch[F.nextAction] = nextAction;
    }
  }

  if (segment === "Other" && !hasText(fields[F.notes]) && (!hasText(fields[F.notes]) || overwrite)) {
    const note = "Confirm segment before outreach.";
    result.changes[F.notes] = { before: fields[F.notes] ?? null, after: note };
    result.patch[F.notes] = note;
  }

  // Safety guards — never write these
  delete result.patch[F.finalApprovedEmail];
  delete result.patch[F.readyForMailMerge];
  if (result.patch[F.outreachStatus] === "Approved" || result.patch[F.outreachStatus] === "Sent") {
    delete result.patch[F.outreachStatus];
    delete result.changes[F.outreachStatus];
  }

  return result;
}

/** @param {Array<{ fields: Record<string, unknown> }>} records */
export function summarizePilotTargetRows(records) {
  const summary = {
    totalRows: records.length,
    eligibleContextRows: 0,
    missingEmail: 0,
    missingLinkedInUrl: 0,
    missingOutreachSegment: 0,
    missingPilotFit: 0,
    missingDraftCopy: 0,
    approvedOrSent: 0,
    doNotContact: 0,
    byCategory: {},
  };

  for (const rec of records) {
    const fields = rec.fields || {};
    if (hasMinimumContext(fields)) summary.eligibleContextRows += 1;
    if (!hasText(fields[F.email])) summary.missingEmail += 1;
    if (!hasText(fields[F.linkedInUrl])) summary.missingLinkedInUrl += 1;
    if (!hasText(fields[F.outreachSegment])) summary.missingOutreachSegment += 1;
    if (!hasText(fields[F.pilotFit])) summary.missingPilotFit += 1;
    const hasDraft =
      hasText(fields[F.emailDraft]) ||
      hasText(fields[F.linkedInDmDraft]) ||
      hasText(fields[F.finalApprovedEmail]);
    if (!hasDraft) summary.missingDraftCopy += 1;
    const status = String(fields[F.outreachStatus] || "").trim();
    if (status === "Approved" || status === "Sent") summary.approvedOrSent += 1;
    if (Boolean(fields[F.doNotContact])) summary.doNotContact += 1;
    const cat = String(fields[F.category] || "(blank)");
    summary.byCategory[cat] = (summary.byCategory[cat] || 0) + 1;
  }

  return summary;
}

export function buildDraftFillMarkdown(report) {
  const lines = [
    "# Pilot Target List — draft fill report",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: ${report.mode}`,
    "",
    "## Summary",
    "",
    `- Records inspected: ${report.recordsInspected}`,
    `- Records updated: ${report.recordsUpdated}`,
    `- Records skipped: ${report.recordsSkipped}`,
    `- Requiring human review: ${report.recordsRequiringReview.length}`,
    `- Missing email: ${report.rowsMissingEmail}`,
    `- Missing LinkedIn URL: ${report.rowsMissingLinkedInUrl}`,
    "",
  ];

  if (report.recordsRequiringReview.length) {
    lines.push("## Requires human review");
    lines.push("");
    for (const row of report.recordsRequiringReview) {
      lines.push(`- **${row.name || row.recordId}** — ${row.reviewReasons.join(", ")}`);
    }
    lines.push("");
  }

  if (report.updates?.length) {
    lines.push("## Updates");
    lines.push("");
    for (const row of report.updates) {
      lines.push(`### ${row.name || row.recordId}`);
      lines.push("");
      for (const [field, change] of Object.entries(row.changes || {})) {
        lines.push(`- **${field}**`);
        lines.push(`  - before: ${JSON.stringify(change.before)}`);
        lines.push(`  - after: ${JSON.stringify(change.after)}`);
      }
      lines.push("");
    }
  }

  return `${lines.join("\n")}\n`;
}
