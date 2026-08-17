/**
 * Pilot Target List — LinkedIn contact import helpers (validation + field mapping).
 */
import {
  MAP_PILOT_TARGET_LIST,
  VAL_PILOT_FIT,
  VAL_PILOT_OUTREACH_SEGMENT,
  VAL_PILOT_PRIORITY,
} from "./pilot-target-list-field-map.js";
import { normalizeOwnerKey } from "./normalize.js";

const F = MAP_PILOT_TARGET_LIST;

const VALID_SEGMENTS = new Set(VAL_PILOT_OUTREACH_SEGMENT);
const VALID_PRIORITIES = new Set(VAL_PILOT_PRIORITY);
const VALID_PILOT_FIT = new Set(VAL_PILOT_FIT);

/** @param {string} name */
export function normalizeContactNameKey(name) {
  return normalizeOwnerKey(name);
}

/** @param {import("./pilot-target-list-linkedin-contacts.js").LinkedInPilotContact} contact */
export function formatRoleLabel(contact) {
  const role = String(contact.role || "").trim();
  const company = String(contact.company || "").trim();
  if (role && company) return `${role}, ${company}`;
  return role || company || "";
}

/** @param {string} segment @param {string} role */
export function mapCategory(segment, role = "") {
  const s = String(segment || "").trim();
  const r = String(role || "").trim().toLowerCase();

  if (s === "Brand / Referral Source") return "Brand Referral Source";
  if (s === "Operator") return "Operator Referral Source";
  if (s === "Owner / Investor" || s === "Capital Partner") return "Owner";
  if (s === "Advisor / Consultant / Broker") {
    if (/broker/.test(r)) return "Broker";
    if (/consultant/.test(r)) return "Consultant";
    return "Advisor";
  }
  return "Consultant";
}

/** @param {string} segment @param {string} tier */
export function mapOutreachMessageAngle(segment, tier) {
  if (tier === "E") return "Feedback Ask";
  const s = String(segment || "").trim();
  if (s === "Owner / Investor" || s === "Capital Partner") return "Owner Pilot";
  if (s === "Advisor / Consultant / Broker") return "Advisor Partner";
  if (s === "Operator") return "Operator Profile";
  if (s === "Brand / Referral Source") return "Referral Ask";
  return "Feedback Ask";
}

/** @param {string} segment @param {string} tier @param {string} [priority] */
export function mapPilotRelevance(segment, tier, priority = "") {
  if (tier === "E") return "Low";
  if (tier === "A" && priority === "P1") return "High";
  if (tier === "A" || tier === "B") return priority === "P1" ? "High" : "Medium";
  if (tier === "C") return "Medium";
  return "Low";
}

/** @param {string} tier @param {string} [priority] */
export function mapPilotFit(tier, priority = "") {
  if (tier === "E") return "Not A Fit";
  if (tier === "A" && priority === "P1") return "Strong Pilot Candidate";
  if (tier === "A" || tier === "B") return priority === "P1" ? "Strong Pilot Candidate" : "Possible Pilot Candidate";
  if (tier === "C") return "Feedback / Referral Only";
  return "Follow-Up Later";
}

/** @param {string} segment */
export function mapLikelyContribution(segment) {
  const s = String(segment || "").trim();
  if (s === "Owner / Investor" || s === "Capital Partner") return ["Has Own Deals"];
  if (s === "Advisor / Consultant / Broker") return ["Can Refer Owners", "Market Feedback"];
  if (s === "Brand / Referral Source") return ["Can Refer Owners", "Can Refer Brands"];
  if (s === "Operator") return ["Can Refer Operators"];
  return ["Market Feedback"];
}

/** @param {import("./pilot-target-list-linkedin-contacts.js").LinkedInPilotContact} contact */
export function buildWhyTheyMatter(contact) {
  const roleLabel = formatRoleLabel(contact);
  const bits = [`LinkedIn connection — triage tier ${contact.tier}.`];
  if (roleLabel) bits.push(`Headline: ${roleLabel}.`);
  if (contact.tier === "E" && contact.skipReason) bits.push(contact.skipReason);
  return bits.join(" ");
}

/** @param {import("./pilot-target-list-linkedin-contacts.js").LinkedInPilotContact} contact */
export function buildNotes(contact) {
  const lines = [
    "Source: LinkedIn connection (Jul 2026 triage).",
    `Import tier: ${contact.tier}.`,
  ];
  if (contact.skipReason) lines.push(`Triage note: ${contact.skipReason}`);
  lines.push("Email and LinkedIn URL not auto-filled — add manually before send.");
  return lines.join("\n");
}

/** @param {import("./pilot-target-list-linkedin-contacts.js").LinkedInPilotContact} contact */
export function buildNextAction(contact) {
  if (contact.tier === "E") return "Review fit; archive or keep for referral only";
  return "Find LinkedIn URL; review fit; draft outreach when selected for wave 1";
}

/**
 * @param {import("./pilot-target-list-linkedin-contacts.js").LinkedInPilotContact} contact
 * @returns {{ ok: true, fields: Record<string, unknown> } | { ok: false, errors: string[] }}
 */
export function buildPilotTargetFields(contact) {
  const errors = [];
  const name = String(contact.name || "").trim();
  if (!name) errors.push("missing_name");

  const segment = String(contact.segment || "").trim();
  if (!VALID_SEGMENTS.has(segment)) errors.push(`invalid_segment:${segment || "(empty)"}`);

  const priority = String(contact.priority || "P3").trim();
  if (!VALID_PRIORITIES.has(priority)) errors.push(`invalid_priority:${priority}`);

  const tier = String(contact.tier || "").trim();
  if (!["A", "B", "C", "D", "E"].includes(tier)) errors.push(`invalid_tier:${tier || "(empty)"}`);

  if (errors.length) return { ok: false, errors };

  const roleLabel = formatRoleLabel(contact);
  const category = mapCategory(segment, roleLabel);
  const pilotFit = mapPilotFit(tier, priority);
  if (!VALID_PILOT_FIT.has(pilotFit)) errors.push(`invalid_pilot_fit:${pilotFit}`);

  if (errors.length) return { ok: false, errors };

  const fields = {
    [F.name]: name,
    [F.role]: roleLabel,
    [F.category]: category,
    [F.outreachSegment]: segment,
    [F.priority]: priority,
    [F.pilotFit]: pilotFit,
    [F.pilotRelevance]: mapPilotRelevance(segment, tier, priority),
    [F.whyTheyMatter]: buildWhyTheyMatter(contact),
    [F.warmIntro]: true,
    [F.status]: "Connected",
    [F.relationshipStrength]: tier === "A" || tier === "B" ? "Light" : "Cold",
    [F.likelyContribution]: mapLikelyContribution(segment),
    [F.outreachMessageAngle]: mapOutreachMessageAngle(segment, tier),
    [F.outreachStatus]: "Draft Needed",
    [F.introSource]: "LinkedIn",
    [F.notes]: buildNotes(contact),
    [F.nextAction]: buildNextAction(contact),
    [F.sendChannel]: "LinkedIn",
    [F.readyForMailMerge]: false,
    [F.doNotContact]: false,
  };

  return { ok: true, fields };
}

/**
 * @param {import("./pilot-target-list-linkedin-contacts.js").LinkedInPilotContact[]} contacts
 * @param {Set<string>} existingNameKeys
 */
export function buildImportPlan(contacts, existingNameKeys) {
  const toCreate = [];
  const skippedExisting = [];
  const invalid = [];

  for (const contact of contacts) {
    const key = normalizeContactNameKey(contact.name);
    if (existingNameKeys.has(key)) {
      skippedExisting.push({ name: contact.name, reason: "already_in_airtable" });
      continue;
    }

    const built = buildPilotTargetFields(contact);
    if (!built.ok) {
      invalid.push({ name: contact.name, errors: built.errors });
      continue;
    }

    toCreate.push({
      name: contact.name,
      tier: contact.tier,
      segment: contact.segment,
      priority: contact.priority,
      fields: built.fields,
    });
    existingNameKeys.add(key);
  }

  return { toCreate, skippedExisting, invalid };
}

export function summarizeImportPlan(plan) {
  const byTier = {};
  for (const row of plan.toCreate) {
    byTier[row.tier] = (byTier[row.tier] || 0) + 1;
  }
  return {
    createCount: plan.toCreate.length,
    skippedExistingCount: plan.skippedExisting.length,
    invalidCount: plan.invalid.length,
    byTier,
  };
}
