/**
 * Global market profile, weekly incremental execution, and hotel feedback
 * for GDI_AUTONOMOUS_HOTEL_FULL_E2E_V2.
 *
 * Reuses the weekly refresh contract and multilingual evidence normalizer.
 * Does not publish, spend, or write another hotel's preferences.
 */

import { feedbackToResearchTask, CHANGE_CLASS, MATERIAL_CHANGE_FIELDS } from "./weekly-refresh-contract-v1.js";
import {
  detectSourceLanguage,
  normalizeGdiMultilingualEvidence,
} from "../discovery/multilingual-evidence-normalize-v1.js";
import { LANG_TERMS } from "../ten-bases-of-demand-v1/base-queries.js";

export const GLOBAL_WEEKLY_VERSION = "gdi-global-weekly-feedback-v2";

export const EXECUTION_MODE = Object.freeze({
  INITIAL_FULL_E2E: "INITIAL_FULL_E2E",
  WEEKLY_INCREMENTAL: "WEEKLY_INCREMENTAL",
});

export const FEEDBACK_PROVENANCE = Object.freeze({
  HOTEL_SUBMITTED: "HOTEL_SUBMITTED",
  THIRD_PARTY_EVIDENCE: "THIRD_PARTY_EVIDENCE",
  MODEL_INTERPRETATION: "MODEL_INTERPRETATION",
});

export function buildMarketProfile(config = {}) {
  const locale = config.locale || {};
  return {
    hotelId: config.hotelId || null,
    country: config.country || null,
    region: config.region || config.demandTerritory?.label || null,
    city: config.city || null,
    timeZone: config.timeZone || config.timezone || null,
    currency: config.currency || null,
    primaryLanguage: locale.primary || null,
    secondaryLanguages: Array.isArray(locale.secondary) ? locale.secondary : [],
    usAssumed: false,
  };
}

export function researchLanguages(profile = {}) {
  return [profile.primaryLanguage, ...(profile.secondaryLanguages || [])].filter(Boolean);
}

export function localLanguageQueries(profile = {}, topic = "congress") {
  const languages = researchLanguages(profile);
  const chosen = languages.length ? languages : [];
  return chosen.map((language) => {
    const terms = LANG_TERMS[language];
    return {
      language,
      query: terms ? terms[topic] || terms.congress : null,
      available: Boolean(terms),
    };
  });
}

export function canonicalEntityKey(name = "") {
  return String(name || "")
    .normalize("NFKC")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

export function organizationsDuplicate(left = {}, right = {}) {
  const keys = (org) =>
    [org.name, org.organizationName, ...(org.aliases || [])]
      .filter(Boolean)
      .map(canonicalEntityKey)
      .filter(Boolean);
  const a = new Set(keys(left));
  for (const key of keys(right)) {
    if (a.has(key)) return true;
  }
  return false;
}

export function normalizeEvidenceExcerpt(originalText) {
  return normalizeGdiMultilingualEvidence({ originalText });
}

export function assessInternationalContact(value = "", profile = {}) {
  const raw = String(value || "").trim();
  const e164 = /^\+[1-9]\d{6,14}$/.test(raw.replace(/[\s()-]/g, ""));
  const localDigits = /^\d{6,14}$/.test(raw.replace(/[\s()-]/g, ""));
  return {
    raw,
    country: profile.country || null,
    currency: profile.currency || null,
    acceptsInternational: e164 || localDigits,
    requiresUsFormat: false,
  };
}

export function acceptHotelFeedback(item = {}, hotelId, seenKeys = new Set()) {
  if (!item || item.hotelId !== hotelId) {
    return { ok: false, error: "CROSS_HOTEL_FEEDBACK" };
  }
  if (!item.opportunityId) return { ok: false, error: "MISSING_OPPORTUNITY" };
  if (!Object.values(FEEDBACK_PROVENANCE).includes(item.provenance)) {
    return { ok: false, error: "UNKNOWN_PROVENANCE" };
  }
  const key = item.idempotencyKey || `${item.opportunityId}:${item.submittedAt}:${item.relevance || ""}`;
  if (seenKeys.has(key)) return { ok: false, error: "DUPLICATE_FEEDBACK", duplicate: true };
  if (item.provenance === FEEDBACK_PROVENANCE.MODEL_INTERPRETATION && (item.won === true || item.lost === true)) {
    return { ok: false, error: "MODEL_CANNOT_ASSERT_OUTCOME" };
  }
  seenKeys.add(key);
  return {
    ok: true,
    record: {
      hotelId,
      opportunityId: item.opportunityId,
      submittedAt: item.submittedAt || null,
      source: item.source || null,
      relevance: item.relevance || null,
      contactStatus: item.contactStatus || null,
      qualificationStatus: item.qualificationStatus || null,
      rfpStatus: item.rfpStatus || null,
      won: item.won === true,
      lost: item.lost === true,
      lossReason: item.lossReason || null,
      note: item.note || null,
      provenance: item.provenance,
      confidence: item.confidence || null,
      shownResearchVersion: item.shownResearchVersion || null,
      shownOpportunityVersion: item.shownOpportunityVersion || null,
      idempotencyKey: key,
    },
  };
}

export function feedbackChangesHotelPlan(record) {
  const task = feedbackToResearchTask({
    hotelId: record.hotelId,
    action: record.relevance,
    outcome: record.won ? "won" : record.lost ? "lost" : "",
    reason: record.lossReason,
    note: record.note,
    status: record.qualificationStatus,
  });
  return {
    hotelId: record.hotelId,
    opportunityId: record.opportunityId,
    task,
    globalPolicyChanged: false,
  };
}

function materialDiff(prior, next) {
  return MATERIAL_CHANGE_FIELDS.some((field) => String(prior?.[field] || "") !== String(next?.[field] || ""));
}

export function describeWeeklyJob({ hotelId, idempotencyKey, priorPresent = false, lockHeld = false } = {}) {
  let status = "READY";
  if (lockHeld) status = "BLOCKED_LOCK";
  else if (!priorPresent) status = "BLOCKED_NEEDS_PRIOR_STATE";
  return {
    hotelId: hotelId || null,
    mode: EXECUTION_MODE.WEEKLY_INCREMENTAL,
    status,
    idempotencyKey: idempotencyKey || null,
    budgetUsd: 0,
    retryWithoutRestart: true,
    isolatedLock: hotelId ? `hotel:${hotelId}` : null,
    schedulerDeployed: false,
  };
}

export function runWeeklyIncremental({
  hotelId,
  prior,
  opportunities = [],
  newSignals = [],
  feedback = [],
  idempotencyKey,
  authorizePublish = false,
  today = new Date().toISOString().slice(0, 10),
} = {}) {
  if (!prior || !Array.isArray(prior.opportunities)) {
    return { ok: false, error: "MISSING_PRIOR_STATE" };
  }
  if (prior.hotelId !== hotelId) return { ok: false, error: "PRIOR_HOTEL_MISMATCH" };
  const seenRuns = new Set(prior.appliedIdempotencyKeys || []);
  if (idempotencyKey && seenRuns.has(idempotencyKey)) {
    return {
      ok: true,
      duplicate: true,
      opportunities: prior.opportunities,
      report: prior.lastWeeklyReport,
      publicationChanged: false,
    };
  }

  const priorById = new Map(prior.opportunities.map((row) => [row.id, row]));
  const next = prior.opportunities.map((row) => ({
    ...row,
    researchHistory: [...(row.researchHistory || [])],
  }));
  const changes = [];

  for (const row of next) {
    if (row.eventEndDate && row.eventEndDate < today && row.standingOpportunity !== true) {
      row.researchHistory.push({
        at: today,
        from: row.lifecycle || null,
        to: "SUPERSEDED",
        reason: "event_concluded",
      });
      row.lifecycle = "SUPERSEDED";
      row.actionable = false;
      changes.push({ opportunityId: row.id, change: CHANGE_CLASS.OCCASION_CONCLUDED_OR_STATUS_CHANGED });
    } else if ((row.evidenceGaps || []).length) {
      changes.push({ opportunityId: row.id, change: "REVISITED_UNRESOLVED" });
    }
  }

  for (const incoming of opportunities) {
    const existing = next.find((row) => row.id === incoming.id);
    if (!existing) continue;
    if (materialDiff(priorById.get(incoming.id), incoming)) {
      existing.researchHistory.push({ at: today, reason: "material_field_change" });
      for (const field of MATERIAL_CHANGE_FIELDS) {
        if (incoming[field] != null) existing[field] = incoming[field];
      }
      changes.push({ opportunityId: existing.id, change: CHANGE_CLASS.MATERIAL_UPDATE });
    }
  }

  for (const signal of newSignals) {
    if (next.some((row) => row.id === signal.id || organizationsDuplicate(row, signal))) {
      changes.push({ opportunityId: signal.id, change: CHANGE_CLASS.NO_MATERIAL_CHANGE, reason: "duplicate_identity" });
      continue;
    }
    next.push({
      ...signal,
      hotelId,
      lifecycle: "DISCOVERED",
      researchHistory: [{ at: today, reason: "new_signal" }],
    });
    changes.push({ opportunityId: signal.id, change: CHANGE_CLASS.NEW_PROSPECT });
  }

  const seenFeedback = new Set(prior.feedbackKeys || []);
  const acceptedFeedback = [];
  const rejectedFeedback = [];
  for (const item of feedback) {
    const decision = acceptHotelFeedback(item, hotelId, seenFeedback);
    if (!decision.ok) {
      rejectedFeedback.push(decision);
      continue;
    }
    acceptedFeedback.push(decision.record);
    const target = next.find((row) => row.id === decision.record.opportunityId);
    if (target) {
      target.hotelFeedback = [...(target.hotelFeedback || []), decision.record];
      target.researchHistory.push({ at: today, reason: "hotel_feedback", provenance: decision.record.provenance });
      const plan = feedbackChangesHotelPlan(decision.record);
      target.nextResearchTask = plan.task;
    }
  }

  const report = {
    version: GLOBAL_WEEKLY_VERSION,
    hotelId,
    mode: EXECUTION_MODE.WEEKLY_INCREMENTAL,
    idempotencyKey: idempotencyKey || null,
    changes,
    acceptedFeedback: acceptedFeedback.length,
    rejectedFeedback: rejectedFeedback.map((row) => row.error),
    publicationPrepared: false,
    publicationAuthorized: authorizePublish === true,
    publicationChanged: false,
  };
  return {
    ok: true,
    duplicate: false,
    opportunities: next,
    report,
    state: {
      hotelId,
      opportunities: next,
      appliedIdempotencyKeys: [...seenRuns, idempotencyKey].filter(Boolean),
      feedbackKeys: [...seenFeedback],
      lastWeeklyReport: report,
    },
  };
}

export function cycleMetrics({ previousActionable = 0, currentActionable = 0, newlyQualified = 0, feedback = [] } = {}) {
  const hotelFacts = feedback.filter((item) => item.provenance === FEEDBACK_PROVENANCE.HOTEL_SUBMITTED);
  const negative = hotelFacts.filter((item) => item.lost === true || /not relevant|rejected/i.test(String(item.relevance || "")));
  return {
    newlyQualified,
    stillActionableRate: previousActionable > 0 ? currentActionable / previousActionable : null,
    hotelResponseCount: hotelFacts.length,
    negativeFeedbackCount: negative.length,
    feedbackAbsent: hotelFacts.length === 0,
    verifiedGlobalGroundTruth: false,
  };
}

export function evaluateGlobalRuleChange() {
  return {
    mayDeployFromFeedbackAlone: false,
    requiresVersionedEvaluation: true,
    nonRegressionRequired: true,
  };
}

export { detectSourceLanguage };
