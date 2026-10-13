/**
 * Hotel-scoped canonical contacts store for GDI share projection.
 * Separate from opportunity research packages and from validation feedback.
 *
 * Path: data/group-demand-intelligence/hotels/<hotelId>/canonical-contacts.json
 */

import fs from "node:fs";
import path from "node:path";
import { hotelDir, createId } from "./repository.js";
import {
  projectCanonicalContactForExternal,
  overlayCanonicalContactOnOpportunity,
} from "./canonical-contact-external-projection.js";

function readJson(filePath, fallback = null) {
  if (!fs.existsSync(filePath)) return fallback;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2) + "\n", "utf8");
}

export function canonicalContactsPath(hotelId) {
  return path.join(hotelDir(hotelId), "canonical-contacts.json");
}

export function loadCanonicalContacts(hotelId) {
  return readJson(canonicalContactsPath(hotelId), {
    version: "gdi_canonical_contacts_v1",
    hotelId,
    people: [],
    opportunityBindings: [],
    updatedAt: null,
  });
}

export function saveCanonicalContacts(hotelId, doc) {
  const next = {
    ...doc,
    version: "gdi_canonical_contacts_v1",
    hotelId,
    updatedAt: new Date().toISOString(),
  };
  writeJson(canonicalContactsPath(hotelId), next);
  return next;
}

/**
 * Build a person entry from current applied fields only.
 * Rolled-back / rejected fields must not be included.
 */
export function buildCanonicalPersonEntry({
  personId,
  displayName,
  organization,
  title = null,
  fields = {},
  identityDecision = null,
} = {}) {
  const cleanFields = {};
  for (const [kind, field] of Object.entries(fields || {})) {
    if (!field || field.value == null) continue;
    if (field.status === "ROLLED_BACK" || field.rolledBack === true) continue;
    if (field.canonicalStatus && field.canonicalStatus !== "CANONICAL") continue;
    cleanFields[kind] = {
      kind,
      value: field.value,
      canonicalStatus: "CANONICAL",
      phoneType: field.phoneType || null,
      provenance: field.provenance
        ? {
            // Internal retention only — never projected externally
            verificationState: field.provenance.verificationState || null,
            confidence: field.provenance.confidence || null,
            discoveredAt: field.provenance.discoveredAt || null,
            acceptedAt: field.acceptedAt || field.provenance.acceptedAt || null,
            opportunityId: field.provenance.opportunityId || null,
            surfeOutcome: field.provenance.surfeOutcome || null,
            // provider retained internally for metrics — stripped at external project
            _internalProvider: field.provenance.provider || field.provider || null,
          }
        : null,
      acceptedAt: field.acceptedAt || null,
      blockedByFeedback: field.blockedByFeedback === true,
    };
  }
  return {
    personId: personId || createId("gdi_person"),
    displayName,
    organization,
    title,
    identityDecision,
    fields: cleanFields,
    reuseBlocked: false,
  };
}

/**
 * Resolve share overlay for one opportunity from the hotel canonical store.
 */
export function resolveCanonicalOverlayForOpportunity(hotelId, opportunity) {
  const doc = loadCanonicalContacts(hotelId);
  const binding = (doc.opportunityBindings || []).find(
    (b) => b.opportunityId === opportunity?.id && b.active !== false
  );
  if (!binding) return { opportunity, projection: null, person: null };

  const person = (doc.people || []).find((p) => p.personId === binding.personId);
  if (!person || person.reuseBlocked) {
    return { opportunity, projection: null, person: null };
  }

  // Identity gate — ambiguous / rejected never project
  if (
    person.identityDecision === "AMBIGUOUS" ||
    person.identityDecision === "REJECTED" ||
    person.identityDecision === "NOT_FOUND"
  ) {
    return { opportunity, projection: null, person };
  }

  const projection = projectCanonicalContactForExternal({
    canonicalPerson: person,
    relationship: {
      eventRole: binding.eventRole || person.title,
      whyThisContact: binding.whyThisContact || null,
      role: binding.eventRole || person.title,
    },
    fieldMeta: binding.fieldMeta || {},
  });

  if (!projection) return { opportunity, projection: null, person };

  const overlaid = overlayCanonicalContactOnOpportunity(opportunity, projection, {
    preserveGenericAsBackup: binding.preserveGenericAsBackup !== false,
  });

  return { opportunity: overlaid, projection, person };
}

/**
 * Apply overlays to a list of opportunities (share / brief read path).
 */
export function applyCanonicalOverlaysToOpportunities(hotelId, opportunities = []) {
  return (opportunities || []).map((o) => {
    const { opportunity } = resolveCanonicalOverlayForOpportunity(hotelId, o);
    return opportunity;
  });
}
