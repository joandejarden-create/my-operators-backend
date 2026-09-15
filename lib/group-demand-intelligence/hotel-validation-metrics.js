/**
 * Reusable GDI hotel validation + phone pilot metrics.
 * "New" / discovery metrics require hotel validation — never inferred from CI absence.
 */

import {
  SHARE_FAMILIARITY_STATUS,
  SHARE_COMMERCIAL_VALUE,
  SHARE_PHONE_ASSESSMENT,
  SHARE_EMAIL_ASSESSMENT,
  SHARE_CONTACT_PERSON_ASSESSMENT,
} from "./share-validation.js";

function pct(n, d) {
  if (!d) return null;
  return Math.round((1000 * n) / d) / 10;
}

/**
 * Phone pilot metrics from share validation items.
 */
export function computePhonePilotValidationMetrics(items = []) {
  let tested = 0;
  let useful = 0;
  let wrong = 0;
  let mainShared = 0;
  let notTested = 0;

  for (const it of items) {
    const a = it.phoneAssessment;
    if (!a || a === "NOT_TESTED") {
      notTested += 1;
      continue;
    }
    tested += 1;
    if (a === "DIRECT_USABLE") useful += 1;
    else if (a === "WRONG") wrong += 1;
    else if (a === "MAIN_SHARED_LINE") mainShared += 1;
  }

  return {
    PHONE_TESTED_COUNT: tested,
    PHONE_USEFUL_COUNT: useful,
    PHONE_WRONG_COUNT: wrong,
    PHONE_MAIN_SHARED_COUNT: mainShared,
    PHONE_NOT_TESTED_COUNT: notTested,
    PHONE_USEFULNESS_RATE: pct(useful, tested),
    PHONE_WRONG_RATE: pct(wrong, tested),
  };
}

/**
 * Core GDI hotel validation metrics.
 * Only hotel-confirmed metrics from completed validation rows.
 */
export function computeHotelValidationMetrics({
  opportunities = [],
  validationItems = [],
} = {}) {
  const active = (opportunities || []).filter((o) => o.priority !== "DISQUALIFIED");
  const byOpp = new Map((validationItems || []).map((v) => [v.opportunityId, v]));

  const completed = active.filter((o) => {
    const v = byOpp.get(o.id);
    return v && (v.familiarityStatus || v.commercialValue);
  });

  const neverSeen = completed.filter(
    (o) => byOpp.get(o.id)?.familiarityStatus === "NEVER_SEEN_BEFORE"
  );
  const alreadyKnown = completed.filter((o) => {
    const f = byOpp.get(o.id)?.familiarityStatus;
    return (
      f === "ALREADY_KNOWN" ||
      f === "ACTIVELY_PURSUING" ||
      f === "PREVIOUSLY_PURSUED_LOST" ||
      f === "BOOKED_WON"
    );
  });
  const worthNow = completed.filter(
    (o) => byOpp.get(o.id)?.commercialValue === "WORTH_PURSUING_NOW"
  );
  const notRelevant = completed.filter(
    (o) => byOpp.get(o.id)?.familiarityStatus === "NOT_RELEVANT"
  );
  const high = active.filter((o) => o.priority === "HIGH_PRIORITY");
  const highValidatedWorth = high.filter((o) => {
    const v = byOpp.get(o.id);
    return v && v.commercialValue === "WORTH_PURSUING_NOW";
  });
  const highValidated = high.filter((o) => byOpp.has(o.id) && byOpp.get(o.id)?.commercialValue);

  const rightPerson = completed.filter(
    (o) => byOpp.get(o.id)?.contactPersonAssessment === "RIGHT_PERSON"
  );
  const personAssessed = completed.filter((o) => {
    const a = byOpp.get(o.id)?.contactPersonAssessment;
    return a && a !== "UNSURE";
  });

  const emailUseful = completed.filter(
    (o) => byOpp.get(o.id)?.emailAssessment === "USEFUL"
  );
  const emailTested = completed.filter((o) => {
    const a = byOpp.get(o.id)?.emailAssessment;
    return a && a !== "NOT_TESTED";
  });

  const phoneMetrics = computePhonePilotValidationMetrics(
    completed.map((o) => byOpp.get(o.id)).filter(Boolean)
  );

  // Incremental: never-seen + worth pursuing, or already-known with useful contact intel
  const incremental = completed.filter((o) => {
    const v = byOpp.get(o.id);
    if (!v) return false;
    if (v.familiarityStatus === "NEVER_SEEN_BEFORE" && v.commercialValue === "WORTH_PURSUING_NOW") {
      return true;
    }
    if (
      (v.familiarityStatus === "ALREADY_KNOWN" || v.familiarityStatus === "ACTIVELY_PURSUING") &&
      (v.emailAssessment === "USEFUL" || v.phoneAssessment === "DIRECT_USABLE")
    ) {
      return true;
    }
    return false;
  });

  return {
    opportunityCount: active.length,
    highPriorityCount: high.length,
    mediumPriorityCount: active.filter((o) => o.priority === "MEDIUM_PRIORITY").length,
    watchlistCount: active.filter((o) => o.priority === "WATCHLIST").length,
    validatedCount: completed.length,
    pendingValidationCount: active.length - completed.length,
    confirmedNewCount: neverSeen.length,
    alreadyKnownCount: alreadyKnown.length,
    worthPursuingNowCount: worthNow.length,
    notRelevantCount: notRelevant.length,
    validationCompletionRate: pct(completed.length, active.length),
    discoveryRate: pct(neverSeen.length, completed.length),
    actionabilityRate: pct(worthNow.length, completed.length),
    highPriorityPrecision: pct(highValidatedWorth.length, highValidated.length),
    incrementalIntelligenceRate: pct(incremental.length, completed.length),
    contactPersonAccuracyRate: pct(rightPerson.length, personAssessed.length),
    emailUsefulnessRate: pct(emailUseful.length, emailTested.length),
    phoneUsefulnessRate: phoneMetrics.PHONE_USEFULNESS_RATE,
    phone: phoneMetrics,
  };
}

export function buildShareValidationSummary({ opportunities, validationItems }) {
  const m = computeHotelValidationMetrics({ opportunities, validationItems });
  return {
    opportunities: m.opportunityCount,
    high: m.highPriorityCount,
    medium: m.mediumPriorityCount,
    watchlist: m.watchlistCount,
    validated: m.validatedCount,
    pendingValidation: m.pendingValidationCount,
    confirmedNew: m.confirmedNewCount,
    alreadyKnown: m.alreadyKnownCount,
    worthPursuingNow: m.worthPursuingNowCount,
    notRelevant: m.notRelevantCount,
    rates: {
      validationCompletionRate: m.validationCompletionRate,
      discoveryRate: m.discoveryRate,
      actionabilityRate: m.actionabilityRate,
      highPriorityPrecision: m.highPriorityPrecision,
      incrementalIntelligenceRate: m.incrementalIntelligenceRate,
      contactPersonAccuracyRate: m.contactPersonAccuracyRate,
      emailUsefulnessRate: m.emailUsefulnessRate,
      phoneUsefulnessRate: m.phoneUsefulnessRate,
    },
    phone: m.phone,
  };
}

// Re-export enums for UI/API convenience
export {
  SHARE_FAMILIARITY_STATUS,
  SHARE_COMMERCIAL_VALUE,
  SHARE_PHONE_ASSESSMENT,
  SHARE_EMAIL_ASSESSMENT,
  SHARE_CONTACT_PERSON_ASSESSMENT,
};
