/**
 * GDI Hotel Validation / Action / Outcome — subsection persistence helpers.
 *
 * Latest values stay in share-validation.json + Decision Event projections.
 * Historical submissions append to submissionHistory (local) and Decision Events
 * (canonical Airtable Decision Events table) when values actually change.
 *
 * Timestamps are section-scoped — never a generic Last Modified Time.
 */

export const HOTEL_FEEDBACK_SECTION = Object.freeze({
  HOTEL_VALIDATION: "HOTEL_VALIDATION",
  SALES_ACTION: "SALES_ACTION",
  SALES_OUTCOME: "SALES_OUTCOME",
});

export function normalizeFeedbackNote(note) {
  return String(note || "").trim().slice(0, 2000);
}

export function hotelValidationFingerprint(item = {}) {
  return [
    item.familiarityStatus || "",
    item.commercialValue || "",
    item.contactPersonAssessment || "",
    item.emailAssessment || "",
    item.phoneAssessment || "",
    normalizeFeedbackNote(item.note),
  ].join("|");
}

export function actionFingerprint(item = {}) {
  return String(item.actionType || "").trim();
}

export function outcomeFingerprint(item = {}) {
  return [
    String(item.outcomeType || "").trim(),
    String(item.outcomeReason || item.lossReason || "").trim(),
    normalizeFeedbackNote(item.outcomeNote || item.note),
  ].join("|");
}

/**
 * Format viewer-local last-saved label. Server stores UTC ISO; UI localizes.
 * @param {string|null|undefined} iso
 * @returns {string}
 */
export function formatHotelFeedbackLastSavedLabel(iso) {
  if (!iso) return "Not yet submitted";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "Not yet submitted";
  try {
    return (
      "Last saved: " +
      d.toLocaleString(undefined, {
        day: "numeric",
        month: "short",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })
    );
  } catch {
    return `Last saved: ${iso}`;
  }
}

/**
 * Map structured hotel feedback into the weekly learning feedback blob shape
 * consumed by feedbackToResearchTask (reuse existing pipeline — no parallel system).
 */
export function hotelFeedbackToWeeklyLearningItem({
  hotelId,
  opportunityId,
  validation = null,
  actionType = null,
  outcomeType = null,
  outcomeReason = null,
} = {}) {
  const familiarity = validation?.familiarityStatus || "";
  const commercial = validation?.commercialValue || "";
  const contact = validation?.contactPersonAssessment || "";
  const email = validation?.emailAssessment || "";
  const phone = validation?.phoneAssessment || "";
  return {
    hotelId: hotelId || null,
    opportunityId: opportunityId || null,
    provenance: "HOTEL_SUBMITTED",
    status: familiarity,
    feedbackType: commercial,
    action: actionType || contact || email || phone || null,
    outcome: outcomeType || null,
    reason: outcomeReason || null,
    note: validation?.note || "",
    familiarityStatus: familiarity || null,
    commercialValue: commercial || null,
    contactPersonAssessment: contact || null,
    emailAssessment: email || null,
    phoneAssessment: phone || null,
  };
}
