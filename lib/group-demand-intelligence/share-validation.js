/**
 * GDI share hotel validation — separate from research facts and canonical contacts.
 *
 * Does NOT mutate canonical contact records automatically.
 * Stores structured feedback events for later reusable learning.
 */

import fs from "node:fs";
import path from "node:path";
import { hotelDir, createId } from "./repository.js";
import {
  HOTEL_FEEDBACK_SECTION,
  hotelValidationFingerprint,
  normalizeFeedbackNote,
} from "./hotel-feedback-subsection-persistence-v1.js";

/** Familiarity / commercial status for Rad validation (share UX). */
export const SHARE_FAMILIARITY_STATUS = Object.freeze([
  "NEVER_SEEN_BEFORE",
  "ALREADY_KNOWN",
  "ACTIVELY_PURSUING",
  "PREVIOUSLY_PURSUED_LOST",
  "BOOKED_WON",
  "NOT_RELEVANT",
  "UNSURE",
]);

export const SHARE_FAMILIARITY_LABEL = Object.freeze({
  NEVER_SEEN_BEFORE: "Never seen before",
  ALREADY_KNOWN: "Already known",
  ACTIVELY_PURSUING: "Actively pursuing",
  PREVIOUSLY_PURSUED_LOST: "Previously pursued / lost",
  BOOKED_WON: "Booked / won",
  NOT_RELEVANT: "Not relevant",
  UNSURE: "Unsure",
});

export const SHARE_COMMERCIAL_VALUE = Object.freeze([
  "WORTH_PURSUING_NOW",
  "WORTH_WATCHING",
  "NOT_WORTH_PURSUING",
]);

export const SHARE_COMMERCIAL_VALUE_LABEL = Object.freeze({
  WORTH_PURSUING_NOW: "Worth pursuing now",
  WORTH_WATCHING: "Worth watching",
  NOT_WORTH_PURSUING: "Not worth pursuing",
});

export const SHARE_CONTACT_PERSON_ASSESSMENT = Object.freeze([
  "RIGHT_PERSON",
  "RELEVANT_NOT_DECISION_MAKER",
  "WRONG_PERSON",
  "UNSURE",
]);

export const SHARE_CONTACT_PERSON_LABEL = Object.freeze({
  RIGHT_PERSON: "Right person",
  RELEVANT_NOT_DECISION_MAKER: "Relevant but not decision maker",
  WRONG_PERSON: "Wrong person",
  UNSURE: "Unsure",
});

export const SHARE_EMAIL_ASSESSMENT = Object.freeze([
  "USEFUL",
  "WRONG",
  "GENERIC",
  "NOT_TESTED",
]);

export const SHARE_EMAIL_ASSESSMENT_LABEL = Object.freeze({
  USEFUL: "Useful",
  WRONG: "Wrong",
  GENERIC: "Generic",
  NOT_TESTED: "Not tested",
});

export const SHARE_PHONE_ASSESSMENT = Object.freeze([
  "DIRECT_USABLE",
  "MAIN_SHARED_LINE",
  "WRONG",
  "NOT_TESTED",
]);

export const SHARE_PHONE_ASSESSMENT_LABEL = Object.freeze({
  DIRECT_USABLE: "Direct / usable",
  MAIN_SHARED_LINE: "Main / shared line",
  WRONG: "Wrong",
  NOT_TESTED: "Not tested",
});

/** Mapped learning events — stored for later processing; no silent canonical mutation. */
export const CONTACT_FEEDBACK_EVENT_TYPE = Object.freeze({
  RIGHT_PERSON: "RIGHT_PERSON",
  WRONG_PERSON: "WRONG_PERSON",
  RELEVANT_NOT_DECISION_MAKER: "RELEVANT_NOT_DECISION_MAKER",
  EMAIL_USEFUL: "EMAIL_USEFUL",
  EMAIL_WRONG: "EMAIL_WRONG",
  EMAIL_GENERIC: "EMAIL_GENERIC",
  PHONE_USEFUL: "PHONE_USEFUL",
  PHONE_WRONG: "PHONE_WRONG",
  PHONE_MAIN_SHARED: "PHONE_MAIN_SHARED",
});

function readJson(filePath, fallback = null) {
  if (!fs.existsSync(filePath)) return fallback;
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2) + "\n", "utf8");
}

export function shareValidationPath(hotelId) {
  return path.join(hotelDir(hotelId), "share-validation.json");
}

export function loadShareValidation(hotelId) {
  return readJson(shareValidationPath(hotelId), {
    version: "gdi_share_validation_v1",
    hotelId,
    items: [],
    feedbackEvents: [],
    updatedAt: null,
  });
}

function inList(value, list) {
  return value == null || value === "" || list.includes(value);
}

export function validateShareValidationPayload(body = {}) {
  const errors = [];
  if (!inList(body.familiarityStatus, SHARE_FAMILIARITY_STATUS)) {
    errors.push("invalid_familiarity_status");
  }
  if (!inList(body.commercialValue, SHARE_COMMERCIAL_VALUE)) {
    errors.push("invalid_commercial_value");
  }
  if (!inList(body.contactPersonAssessment, SHARE_CONTACT_PERSON_ASSESSMENT)) {
    errors.push("invalid_contact_person_assessment");
  }
  if (!inList(body.emailAssessment, SHARE_EMAIL_ASSESSMENT)) {
    errors.push("invalid_email_assessment");
  }
  if (!inList(body.phoneAssessment, SHARE_PHONE_ASSESSMENT)) {
    errors.push("invalid_phone_assessment");
  }
  return { ok: errors.length === 0, errors };
}

export function mapValidationToFeedbackEvents(item) {
  const events = [];
  const base = {
    hotelId: item.hotelId,
    opportunityId: item.opportunityId,
    validationId: item.id,
    createdAt: item.validatedAt || item.updatedAt,
  };
  if (item.contactPersonAssessment === "RIGHT_PERSON") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.RIGHT_PERSON });
  } else if (item.contactPersonAssessment === "WRONG_PERSON") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.WRONG_PERSON });
  } else if (item.contactPersonAssessment === "RELEVANT_NOT_DECISION_MAKER") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.RELEVANT_NOT_DECISION_MAKER });
  }
  if (item.emailAssessment === "USEFUL") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.EMAIL_USEFUL });
  } else if (item.emailAssessment === "WRONG") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.EMAIL_WRONG });
  } else if (item.emailAssessment === "GENERIC") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.EMAIL_GENERIC });
  }
  if (item.phoneAssessment === "DIRECT_USABLE") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.PHONE_USEFUL });
  } else if (item.phoneAssessment === "WRONG") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.PHONE_WRONG });
  } else if (item.phoneAssessment === "MAIN_SHARED_LINE") {
    events.push({ ...base, type: CONTACT_FEEDBACK_EVENT_TYPE.PHONE_MAIN_SHARED });
  }
  return events;
}

/**
 * Upsert one opportunity validation. Never mutates canonical contacts or opportunities.json.
 * Identical re-submit → no timestamp bump, no misleading history (created:false).
 * Value change → append submissionHistory + refresh latest projection fields.
 *
 * @param {object} [opts]
 * @param {string} [opts.expectedHotelValidationUpdatedAt] concurrency token
 */
export function saveShareValidationItem(hotelId, item = {}, opts = {}) {
  const check = validateShareValidationPayload(item);
  if (!check.ok) {
    const err = new Error("invalid_share_validation");
    err.code = "invalid_share_validation";
    err.errors = check.errors;
    throw err;
  }

  const doc = loadShareValidation(hotelId);
  const now = new Date().toISOString();
  const opportunityId = String(item.opportunityId || "").trim();
  if (!opportunityId) {
    const err = new Error("opportunityId_required");
    err.code = "opportunityId_required";
    throw err;
  }

  const idx = doc.items.findIndex((x) => x.opportunityId === opportunityId);
  const previous = idx >= 0 ? doc.items[idx] : null;

  const expected = opts.expectedHotelValidationUpdatedAt;
  if (
    expected != null &&
    expected !== "" &&
    previous?.validatedAt &&
    String(previous.validatedAt) !== String(expected)
  ) {
    const err = new Error("hotel_validation_conflict");
    err.code = "hotel_validation_conflict";
    err.current = previous;
    err.hotelValidationUpdatedAt = previous.validatedAt;
    throw err;
  }

  const candidate = {
    familiarityStatus: item.familiarityStatus || null,
    commercialValue: item.commercialValue || null,
    contactPersonAssessment: item.contactPersonAssessment || null,
    emailAssessment: item.emailAssessment || null,
    phoneAssessment: item.phoneAssessment || null,
    note: normalizeFeedbackNote(item.note),
  };

  if (
    previous &&
    hotelValidationFingerprint(previous) === hotelValidationFingerprint(candidate)
  ) {
    return {
      item: previous,
      doc,
      created: false,
      unchanged: true,
      hotelValidationUpdatedAt: previous.validatedAt || null,
    };
  }

  const next = {
    id: item.id || previous?.id || createId("gdi_sv"),
    hotelId,
    opportunityId,
    validator: item.validator || "SHARE_REVIEWER",
    validatedAt: now,
    hotelValidationUpdatedAt: now,
    familiarityStatus: candidate.familiarityStatus,
    commercialValue: candidate.commercialValue,
    contactPersonAssessment: candidate.contactPersonAssessment,
    emailAssessment: candidate.emailAssessment,
    phoneAssessment: candidate.phoneAssessment,
    note: candidate.note,
    // Explicit guard — share validation must not rewrite research/canonical
    doesNotOverwriteCanonicalFacts: true,
    doesNotMutateCanonicalContacts: true,
    createdAt: previous?.createdAt || now,
    updatedAt: now,
  };

  if (idx >= 0) {
    doc.items[idx] = next;
  } else {
    doc.items.push(next);
  }

  doc.submissionHistory = Array.isArray(doc.submissionHistory)
    ? doc.submissionHistory
    : [];
  doc.submissionHistory.push({
    hotelId,
    opportunityId,
    section: HOTEL_FEEDBACK_SECTION.HOTEL_VALIDATION,
    submittedAt: now,
    submitter: next.validator,
    values: {
      familiarityStatus: next.familiarityStatus,
      commercialValue: next.commercialValue,
      contactPersonAssessment: next.contactPersonAssessment,
      emailAssessment: next.emailAssessment,
      phoneAssessment: next.phoneAssessment,
      note: next.note || "",
    },
    previousValues: previous
      ? {
          familiarityStatus: previous.familiarityStatus || null,
          commercialValue: previous.commercialValue || null,
          contactPersonAssessment: previous.contactPersonAssessment || null,
          emailAssessment: previous.emailAssessment || null,
          phoneAssessment: previous.phoneAssessment || null,
          note: previous.note || "",
          validatedAt: previous.validatedAt || null,
        }
      : null,
  });
  // Cap local audit growth (canonical Decision Events remain append-only)
  if (doc.submissionHistory.length > 500) {
    doc.submissionHistory = doc.submissionHistory.slice(-500);
  }

  // Replace prior mapped learning events for this opportunity; append fresh mapped set
  doc.feedbackEvents = (doc.feedbackEvents || []).filter(
    (e) => e.opportunityId !== opportunityId
  );
  doc.feedbackEvents.push(...mapValidationToFeedbackEvents(next));
  doc.updatedAt = now;
  writeJson(shareValidationPath(hotelId), doc);
  return {
    item: next,
    doc,
    created: true,
    unchanged: false,
    hotelValidationUpdatedAt: next.validatedAt,
  };
}

export function getShareValidationForOpportunity(hotelId, opportunityId) {
  const doc = loadShareValidation(hotelId);
  return (doc.items || []).find((x) => x.opportunityId === opportunityId) || null;
}

/**
 * Roll back a failed Airtable dual-write so timestamps/UI do not claim Saved.
 * Restores previous latest item (or removes the opportunity row).
 */
export function restoreShareValidationItemAfterFailedPersist(
  hotelId,
  opportunityId,
  previousItem = null
) {
  const id = String(opportunityId || "").trim();
  if (!id) return null;
  const doc = loadShareValidation(hotelId);
  const idx = (doc.items || []).findIndex((x) => x.opportunityId === id);
  if (previousItem) {
    if (idx >= 0) doc.items[idx] = previousItem;
    else doc.items.push(previousItem);
  } else if (idx >= 0) {
    doc.items.splice(idx, 1);
  }
  if (Array.isArray(doc.submissionHistory) && doc.submissionHistory.length) {
    // Drop the most recent HOTEL_VALIDATION history row for this opportunity
    for (let i = doc.submissionHistory.length - 1; i >= 0; i -= 1) {
      const row = doc.submissionHistory[i];
      if (
        row?.opportunityId === id &&
        row?.section === HOTEL_FEEDBACK_SECTION.HOTEL_VALIDATION
      ) {
        doc.submissionHistory.splice(i, 1);
        break;
      }
    }
  }
  doc.feedbackEvents = (doc.feedbackEvents || []).filter(
    (e) => e.opportunityId !== id
  );
  if (previousItem) {
    doc.feedbackEvents.push(...mapValidationToFeedbackEvents(previousItem));
  }
  doc.updatedAt = new Date().toISOString();
  writeJson(shareValidationPath(hotelId), doc);
  return { doc };
}
