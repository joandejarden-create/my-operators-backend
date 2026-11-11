/**
 * Evidence-backed research recovery for GDI_AUTONOMOUS_HOTEL_FULL_E2E_V2.
 *
 * Evaluate → plan → recover from archived records → verify → update → re-evaluate.
 * Archive actions copy or interpret fields that already exist. They do not invent
 * demand, room blocks, buying status, dates, or publication flags.
 */

import fs from "node:fs";
import path from "node:path";
import { classifyContactQuality, computeOpportunityQualification } from "../qualification-precision.js";
import { hasUsableContactRoute } from "../prospect-publication-eligibility-v1.js";
import { deriveProspectCommercialSection } from "../priority-action-window/assignment-contract-v1.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { classifyDemandPosture } from "./gdi-three-hotel-full-e2e-v1.js";
import { detectSourceLanguage } from "../discovery/multilingual-evidence-normalize-v1.js";
import { canonicalEntityKey } from "./gdi-global-weekly-feedback-v2.js";

export const RESEARCH_RECOVERY_VERSION = "gdi-research-recovery-v2.1";

const GENERIC_WHY_NOW = /^(venue announced\.?|future cycle 20\d{2}\.?)$/i;
const UNSUPPORTED_THESIS = /no on-site lodging at\s*;/i;

function clone(value) {
  return JSON.parse(JSON.stringify(value ?? null));
}

function text(value) {
  return String(value || "").trim();
}

function whyNowSubstantive(value) {
  const raw = text(value);
  return raw.length > 24 && !GENERIC_WHY_NOW.test(raw);
}

function archivedFacts(opp = {}) {
  return (Array.isArray(opp.evidence) ? opp.evidence : []).filter(
    (item) => item && String(item.claimKind || "").toUpperCase() === "FACT" && text(item.value)
  );
}

function contactArchive(opp = {}, canonical = []) {
  const backups = (opp.backupContacts || []).filter((item) => item && item.historicalOnly !== true && (item.email || item.phone || item.url));
  const canonicalRows = canonical.filter((item) => item.opportunityId === (opp.id || opp.opportunityId) && item.hotelId === opp.hotelId);
  return { backups, canonicalRows };
}

export function loadHotelEvidenceArchive(repoRoot, hotelId) {
  const file = path.join(repoRoot, "data/group-demand-intelligence/hotels", hotelId, "canonical-contacts.json");
  if (!file || !fs.existsSync(file)) return { hotelId, canonicalContacts: [] };
  const doc = JSON.parse(fs.readFileSync(file, "utf8"));
  if (doc.hotelId && doc.hotelId !== hotelId) return { hotelId, canonicalContacts: [], rejectedForeignArchive: true };
  const canonicalContacts = [];
  for (const person of doc.people || []) {
    for (const field of Object.values(person.fields || {})) {
      const provenance = field?.provenance || {};
      if (!provenance.opportunityId || !field.value) continue;
      canonicalContacts.push({
        hotelId,
        opportunityId: provenance.opportunityId,
        kind: field.kind || null,
        value: field.value,
        verification: provenance.verificationState || null,
        name: person.displayName || null,
        role: person.title || null,
      });
    }
  }
  return { hotelId, canonicalContacts, rejectedForeignArchive: false };
}

export function assessOpportunity(opp = {}, context = {}) {
  const route = hasUsableContactRoute(opp);
  const posture = classifyDemandPosture(opp);
  const facts = archivedFacts(opp);
  const why = text(opp.whyNow);
  const venueFact = facts.find((item) => item.field === "venue") || null;
  const placedLanguage = /already selected|not transferable|mismatch vs/i.test(
    [opp.fitExplanation, opp.venueStatus, opp.eventLocationSummary, opp.priorityReason].filter(Boolean).join(" ")
  );
  const storedConfirmed = /CONFIRMED/i.test(text(opp.demandStatus));
  let eligibility = { hotelFacing: false, eligibility: "NOT_EVALUATED" };
  try {
    eligibility = evaluateProspectPublicationEligibility(opp);
  } catch {
    eligibility = { hotelFacing: false, eligibility: "EVALUATION_ERROR" };
  }
  const questions = [];
  if (!text(opp.organizationName) && facts.length === 0 && !(opp.sources || []).length && !(opp.establishedFacts || []).length) {
    questions.push({
      id: "organization_and_evidence",
      unresolved: true,
      nextSource: "A named organization and a source already stored on the record.",
    });
  }
  if (!route.ok) {
    questions.push({
      id: "contact_route",
      unresolved: true,
      nextSource: "Backup contacts on this opportunity, then the hotel canonical contact archive, then the organization's official event page.",
    });
  }
  const whyResolved =
    whyNowSubstantive(why) ||
    opp.recovery?.whyNowStatus === "CORRECTED_FROM_ARCHIVED_FACT" ||
    opp.recovery?.whyNowStatus === "UNKNOWN";
  if (!whyResolved) {
    questions.push({
      id: "why_now",
      unresolved: true,
      nextSource: venueFact
        ? "Archived venue fact on this opportunity."
        : "An official announcement already in the evidence archive. Do not invent a date.",
    });
  }
  if (!text(opp.opportunityQualification)) {
    questions.push({
      id: "qualification",
      unresolved: true,
      nextSource: "Existing thesis, room status, venue status, and contact quality on this record.",
    });
  }
  return {
    version: RESEARCH_RECOVERY_VERSION,
    opportunityId: opp.id || null,
    hotelId: context.hotelId || opp.hotelId || null,
    organization: opp.organizationName || null,
    demandMechanism: opp.opportunityType || opp.demandType || null,
    demandPosture: posture.posture,
    storedDemandStatus: opp.demandStatus || null,
    storedDemandAdopted: storedConfirmed && posture.posture === "CONFIRMED" && !placedLanguage,
    storedDemandContradiction: storedConfirmed && posture.posture !== "CONFIRMED",
    archivedFactCount: facts.length,
    placedVenueFact: venueFact
      ? { value: venueFact.value, sourceDomain: venueFact.sourceDomain || null, claimKind: "FACT" }
      : null,
    placedElsewhere: Boolean(venueFact && placedLanguage),
    whyNowStatus: whyNowSubstantive(why) ? "SUBSTANTIVE" : why ? "UNKNOWN" : "ABSENT",
    sourceLanguage: detectSourceLanguage(`${why} ${opp.hotelOpportunityThesis || ""}`),
    contactRouteUsable: route.ok === true,
    contactRouteReason: route.reason || null,
    timing: opp.eventStartDate || null,
    recommendedActionPresent: text(opp.recommendedAction).length > 12,
    evidenceConfidence: opp.evidenceConfidence ?? null,
    hotelFacing: eligibility.hotelFacing === true,
    visibilityContradiction: opp.customerVisible === true && eligibility.hotelFacing !== true,
    questions,
  };
}

function sameEvent(left, right) {
  if (!left.organizationName || !left.eventStartDate) return false;
  if (canonicalEntityKey(left.organizationName) !== canonicalEntityKey(right.organizationName)) return false;
  if (left.eventStartDate !== right.eventStartDate) return false;
  const leftTitle = canonicalEntityKey(left.title);
  const rightTitle = canonicalEntityKey(right.title);
  if (leftTitle && rightTitle) return leftTitle === rightTitle;
  return true;
}

function duplicateOf(opp, siblings = []) {
  const group = siblings.filter((row) => row.id !== opp.id && sameEvent(opp, row));
  if (!group.length) return null;
  const richness = (row) =>
    (row.sources || []).length +
    (row.evidence || []).length +
    (row.establishedFacts || []).length +
    (row.backupContacts || []).length +
    (hasUsableContactRoute(row).ok ? 5 : 0);
  const survivor = [opp, ...group].sort((a, b) => richness(b) - richness(a) || String(a.id).localeCompare(String(b.id)))[0];
  if (survivor.id === opp.id) return null;
  return survivor;
}

function unsupported(opp) {
  return (
    !text(opp.organizationName) &&
    !(opp.sources || []).length &&
    !(opp.establishedFacts || []).length &&
    archivedFacts(opp).length === 0 &&
    UNSUPPORTED_THESIS.test(text(opp.hotelOpportunityThesis))
  );
}

export function planRecoveryAction(opp, assessment, context = {}) {
  const tried = new Set(context.appliedFingerprints || []);
  const archive = contactArchive(opp, context.canonicalContacts || []);
  const options = [];
  const add = (action, fingerprint, extra) => {
    options.push({
      action,
      executableNow: true,
      costUsd: 0,
      fingerprint,
      evidenceClass: "archived_opportunity_record",
      terminal: extra.terminal === true,
      ...extra,
    });
  };

  if (context.feedbackSuppress === true) {
    add("APPLY_HOTEL_FEEDBACK", `feedback:${opp.id}`, {
      terminal: true,
      reason: "Hotel feedback for this opportunity suppresses a new pursuit. Global policy is unchanged.",
      globalPolicyChanged: false,
    });
  }
  if (unsupported(opp)) {
    add("REJECT_UNSUPPORTED", `reject:${opp.id}`, {
      terminal: true,
      reason: "Organization, sources, and facts are absent, and the thesis does not name a venue.",
      nextSource: "A sourced organization record. None is present to recover.",
    });
  }
  const duplicateSurvivor = duplicateOf(opp, context.siblings || []);
  if (duplicateSurvivor) {
    add("DISPOSITION_DUPLICATE", `duplicate:${opp.id}:${duplicateSurvivor.id}`, {
      terminal: true,
      survivorId: duplicateSurvivor.id,
      survivorRoute: hasUsableContactRoute(duplicateSurvivor).reason,
      reason: "Another opportunity has the same organization, start date, and title, with stronger archived evidence.",
    });
  }
  if (opp.eventEndDate && context.today && opp.eventEndDate < context.today && opp.standingOpportunity !== true) {
    add("DISPOSITION_SUPERSEDED", `superseded:${opp.id}:${opp.eventEndDate}`, {
      terminal: true,
      reason: "The stored event end date is before this cycle. History is kept.",
    });
  }
  if (assessment.placedElsewhere && assessment.whyNowStatus !== "SUBSTANTIVE") {
    add("RECONCILE_WHY_NOW_FROM_FACT", `why-fact:${opp.id}`, {
      reason: "The short why-now stays stored. Research disposition uses the archived venue fact.",
    });
  }
  if (!assessment.contactRouteUsable && (archive.backups.length || archive.canonicalRows.length)) {
    add("RESTORE_CONTACT_FROM_ARCHIVE", `contact:${opp.id}:${archive.backups.length}:${archive.canonicalRows.length}`, {
      reason: "A contact route exists on the opportunity backup list or the hotel canonical archive.",
    });
  } else if (!assessment.contactRouteUsable) {
    add("RECORD_CONTACT_ARCHIVE_MISS", `contact-miss:${opp.id}`, {
      reason: "Backup contacts and the hotel canonical archive have no route for this opportunity. Next source is the organization's official event page.",
      nextSource: "Official organization event contact page. Provider call is not authorized.",
    });
  }
  if (!text(opp.opportunityQualification)) {
    add("COMPUTE_QUALIFICATION_FROM_EVIDENCE", `qualification:${opp.id}`, {
      reason: "Qualification is blank. Compute it from the stored thesis, room status, venue status, and contact quality.",
    });
  }
  if (assessment.whyNowStatus !== "SUBSTANTIVE" && !assessment.placedElsewhere) {
    add("MARK_WHY_NOW_UNKNOWN", `why-unknown:${opp.id}`, {
      reason: "The stored why-now is not substantive and no archived fact supplies a replacement.",
    });
  }
  if (!opp.prospectReviewSection) {
    add("DERIVE_COMMERCIAL_SECTION", `section:${opp.id}`, {
      reason: "Packaging section is blank. Derive it. This does not add evidence.",
      evidenceClass: "read_time_packaging",
    });
  }
  const next = options.find((item) => !tried.has(item.fingerprint));
  if (next) return next;
  const remaining = assessment.questions.find((item) => item.unresolved);
  return {
    action: remaining ? `NEEDS_${remaining.id.toUpperCase()}` : "STOP_NO_YIELD",
    executableNow: false,
    costUsd: 0,
    blocker: remaining ? "BLOCKED_AUTHORIZATION" : null,
    reason: remaining
      ? `Archived records do not resolve ${remaining.id}. Next source: ${remaining.nextSource}`
      : "No further archived recovery improves this opportunity.",
    nextSource: remaining?.nextSource || null,
  };
}

function qualificationFromRecord(opp) {
  const contact = classifyContactQuality(opp);
  const roomsNotRequired =
    opp.cateringOnly === true ||
    String(opp.guestroomStatus || "").toUpperCase() === "NOT_REQUIRED" ||
    String(opp.roomDemandStatus || "").toUpperCase() === "NOT_REQUIRED";
  return computeOpportunityQualification({
    venueSourcingStatus: opp.venueSourcingStatus,
    opportunityType: opp.opportunityType,
    roomDemandStatus: opp.roomDemandStatus,
    eventLocationStatus: opp.eventLocationStatus,
    contactQuality: contact.contactQuality,
    hotelOpportunityThesis: opp.hotelOpportunityThesis,
    evidenceConfidence: opp.evidenceConfidence,
    demandTerritoryFit: opp.demandTerritoryFit,
    roomsNotRequired,
  });
}

function applyAction(opp, action, context) {
  const next = clone(opp);
  next.researchHistory = [...(opp.researchHistory || [])];
  next.recovery = { ...(opp.recovery || {}) };
  const entry = {
    at: context.today || null,
    action: action.action,
    fingerprint: action.fingerprint,
    evidenceClass: action.evidenceClass || "archived_opportunity_record",
    costUsd: 0,
  };
  if (action.action === "REJECT_UNSUPPORTED") {
    next.recovery.lifecycle = "REJECTED";
    next.recovery.reason = "organization_and_evidence_absent";
    entry.reason = next.recovery.reason;
  } else if (action.action === "DISPOSITION_DUPLICATE") {
    next.recovery.lifecycle = "SUPERSEDED";
    next.recovery.reason = "duplicate_same_organization_and_start";
    next.recovery.survivorId = action.survivorId;
    next.recovery.contactInvestigation = {
      method: "archived_sibling_opportunity",
      survivorId: action.survivorId,
      survivorRoute: action.survivorRoute || null,
      copiedContact: false,
      providerCall: false,
    };
    entry.survivorId = action.survivorId;
  } else if (action.action === "DISPOSITION_SUPERSEDED") {
    next.recovery.lifecycle = "SUPERSEDED";
    next.recovery.reason = "event_concluded";
    next.recovery.actionable = false;
  } else if (action.action === "RECONCILE_WHY_NOW_FROM_FACT") {
    const fact = archivedFacts(opp).find((item) => item.field === "venue");
    next.recovery.whyNowStatus = "CORRECTED_FROM_ARCHIVED_FACT";
    next.recovery.whyNowEvidence = {
      field: fact?.field || null,
      value: fact?.value || null,
      claimKind: "FACT",
      sourceDomain: fact?.sourceDomain || null,
    };
    next.recovery.lifecycle = "NOT_OPEN_BOOKING";
    next.recovery.reason = "archived_venue_fact_names_a_selected_venue";
    entry.sourceDomain = fact?.sourceDomain || null;
  } else if (action.action === "RESTORE_CONTACT_FROM_ARCHIVE") {
    const archive = contactArchive(opp, context.canonicalContacts || []);
    const backup = archive.backups.sort((a, b) => Number(b.confidence || 0) - Number(a.confidence || 0))[0];
    if (backup) {
      next.primaryContact = {
        name: backup.name || backup.fullName || null,
        email: backup.email || null,
        phone: backup.phone || null,
        role: backup.title || backup.role || null,
        source: backup.source || backup.sourceUrl || null,
      };
      next.recovery.contactProvenance = {
        archive: "opportunity.backupContacts",
        verification: backup.emailVerificationStatusLabel || backup.emailVerificationStatus || null,
        claimKind: backup.claimKind || null,
        quality: backup.contactQualityLabel || backup.contactQuality || null,
      };
    } else if (archive.canonicalRows[0]) {
      const row = archive.canonicalRows[0];
      next.primaryContact = {
        name: row.name || null,
        email: row.kind === "EMAIL" ? row.value : null,
        phone: row.kind === "MOBILE" || row.kind === "PHONE" ? row.value : null,
        role: row.role || null,
      };
      next.recovery.contactProvenance = {
        archive: "canonical-contacts",
        verification: row.verification || null,
        claimKind: "ARCHIVED_CANONICAL",
      };
    }
    entry.archive = next.recovery.contactProvenance?.archive || null;
    entry.verification = next.recovery.contactProvenance?.verification || null;
  } else if (action.action === "RECORD_CONTACT_ARCHIVE_MISS") {
    next.recovery.contactInvestigation = {
      method: "backup_contacts_and_canonical_archive",
      found: false,
      providerCall: false,
      nextSource: action.nextSource,
    };
  } else if (action.action === "COMPUTE_QUALIFICATION_FROM_EVIDENCE") {
    const computed = qualificationFromRecord(opp);
    next.opportunityQualification = computed.opportunityQualification || null;
    next.recovery.qualificationProvenance = "computed_from_existing_fields";
    entry.qualification = next.opportunityQualification;
  } else if (action.action === "MARK_WHY_NOW_UNKNOWN") {
    next.recovery.whyNowStatus = "UNKNOWN";
    next.recovery.reason = "stored_why_now_not_substantive";
  } else if (action.action === "DERIVE_COMMERCIAL_SECTION") {
    let eligibility = { hotelFacing: false };
    try {
      eligibility = evaluateProspectPublicationEligibility(opp);
    } catch {
      eligibility = { hotelFacing: false };
    }
    const section = deriveProspectCommercialSection(opp, { eligibility });
    next.recovery.commercialSection = section.section;
    next.recovery.commercialSectionSource = section.source;
  } else if (action.action === "APPLY_HOTEL_FEEDBACK") {
    next.recovery.feedbackApplied = true;
    next.recovery.suppressPursuit = true;
    next.recovery.globalPolicyChanged = false;
  }
  next.researchHistory.push(entry);
  return next;
}

function verifyAction(before, after, action) {
  if (before.customerVisible !== after.customerVisible) return { ok: false, reason: "publication_flag_changed" };
  if (text(before.demandStatus) !== text(after.demandStatus)) return { ok: false, reason: "demand_status_rewritten" };
  if (text(before.roomDemandStatus) !== text(after.roomDemandStatus)) return { ok: false, reason: "room_demand_rewritten" };
  if (text(before.whyNow) !== text(after.whyNow)) return { ok: false, reason: "original_why_now_replaced" };
  if (text(before.eventStartDate) !== text(after.eventStartDate) || text(before.eventEndDate) !== text(after.eventEndDate)) {
    return { ok: false, reason: "event_date_rewritten" };
  }
  if (action.action === "RESTORE_CONTACT_FROM_ARCHIVE") {
    const email = after.primaryContact?.email || "";
    const phone = after.primaryContact?.phone || "";
    const knownEmails = new Set((before.backupContacts || []).map((item) => item.email).filter(Boolean));
    const knownPhones = new Set((before.backupContacts || []).map((item) => item.phone).filter(Boolean));
    for (const row of action.canonicalRows || []) {
      if (row.kind === "EMAIL") knownEmails.add(row.value);
      if (row.kind === "MOBILE" || row.kind === "PHONE") knownPhones.add(row.value);
    }
    if (email && !knownEmails.has(email)) return { ok: false, reason: "contact_not_in_archive" };
    if (phone && !knownPhones.has(phone) && !email) return { ok: false, reason: "contact_not_in_archive" };
    if (!hasUsableContactRoute(after).ok) return { ok: false, reason: "restored_contact_not_usable" };
  }
  if (action.action === "COMPUTE_QUALIFICATION_FROM_EVIDENCE") {
    const recomputed = qualificationFromRecord(before);
    if (after.opportunityQualification !== recomputed.opportunityQualification) {
      return { ok: false, reason: "qualification_not_traceable" };
    }
  }
  if (action.action === "REJECT_UNSUPPORTED" && text(after.organizationName)) {
    return { ok: false, reason: "organization_was_invented" };
  }
  return { ok: true };
}

const TERMINAL_ACTIONS = new Set([
  "REJECT_UNSUPPORTED",
  "DISPOSITION_DUPLICATE",
  "DISPOSITION_SUPERSEDED",
  "APPLY_HOTEL_FEEDBACK",
]);

export function runOpportunityRecovery(opp, context = {}) {
  let current = clone(opp);
  current.researchHistory = [...(opp.researchHistory || [])];
  if ((opp.researchHistory || []).some((entry) => TERMINAL_ACTIONS.has(entry.action))) {
    return {
      ok: true,
      duplicate: true,
      opportunity: current,
      assessment: assessOpportunity(current, context),
      journal: [],
      stopped: { action: "IDEMPOTENT_REPLAY", executableNow: false, reason: "bounded_repeat_without_new_evidence" },
      researchCostUsd: 0,
      publicationChanged: false,
    };
  }
  const applied = new Set([
    ...(context.resumeApplied || []),
    ...(opp.researchHistory || []).map((entry) => entry.fingerprint).filter(Boolean),
  ]);
  const journal = [];
  const maxSteps = context.maxSteps ?? 6;
  for (let step = 0; step < maxSteps; step += 1) {
    const assessment = assessOpportunity(current, context);
    const canonicalRows = contactArchive(current, context.canonicalContacts || []).canonicalRows;
    const action = planRecoveryAction(current, assessment, { ...context, appliedFingerprints: applied });
    action.canonicalRows = canonicalRows;
    journal.push({ action: action.action, executableNow: action.executableNow, reason: action.reason, costUsd: action.costUsd || 0 });
    if (!action.executableNow) {
      return {
        ok: true,
        opportunity: current,
        assessment,
        journal,
        stopped: action,
        researchCostUsd: 0,
        publicationChanged: false,
      };
    }
    const updated = applyAction(current, action, context);
    const verification = verifyAction(current, updated, action);
    if (!verification.ok) {
      return {
        ok: false,
        opportunity: current,
        assessment,
        journal,
        stopped: { action: action.action, executableNow: false, reason: verification.reason },
        researchCostUsd: 0,
        publicationChanged: false,
      };
    }
    current = updated;
    applied.add(action.fingerprint);
    if (action.terminal) {
      return {
        ok: true,
        opportunity: current,
        assessment: assessOpportunity(current, context),
        journal,
        stopped: action,
        researchCostUsd: 0,
        publicationChanged: false,
      };
    }
  }
  return {
    ok: true,
    opportunity: current,
    assessment: assessOpportunity(current, context),
    journal,
    stopped: { action: "STEP_BUDGET", executableNow: false, reason: "Bounded recovery step budget reached." },
    researchCostUsd: 0,
    publicationChanged: false,
  };
}

export function runHotelRecovery({ hotelId, opportunities = [], archive = {}, today, feedback = [], maxSteps, resumeApplied } = {}) {
  const canonicalContacts = (archive.canonicalContacts || []).filter((row) => row.hotelId === hotelId);
  const results = opportunities.map((opp) => {
    const ownFeedback = feedback.find((item) => item.hotelId === hotelId && item.opportunityId === opp.id && /not relevant|rejected/i.test(String(item.relevance || "")));
    return runOpportunityRecovery(opp, {
      hotelId,
      siblings: opportunities,
      canonicalContacts,
      today,
      feedbackSuppress: Boolean(ownFeedback),
      maxSteps,
      resumeApplied,
    });
  });
  return {
    version: RESEARCH_RECOVERY_VERSION,
    hotelId,
    researchCostUsd: 0,
    airtableWrites: 0,
    localWrites: 0,
    publicationChanged: false,
    results,
  };
}

export function publicRecoveryView(result) {
  return JSON.parse(
    JSON.stringify(result, (key, value) => {
      if (/email|phone|mobile/i.test(key)) return undefined;
      if (typeof value === "string" && value.includes("@")) return "[redacted]";
      return value;
    })
  );
}
