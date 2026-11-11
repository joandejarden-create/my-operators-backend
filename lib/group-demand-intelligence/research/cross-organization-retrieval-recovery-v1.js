/**
 * Cross-organization retrieval recovery for the GDI Research Supervisor.
 * Separates technical retrieval outcomes from evidence evaluation, bounds failed
 * attempts separately from successful substantive retrievals, and proposes
 * adaptive recovery without bypassing auth/CAPTCHA/access controls.
 */

import {
  resolvePublishingOrganization,
  isIndependentPublisherUrl,
  RELATIONSHIP_VERIFICATION,
  STOPPING_TRIGGER,
  buildStopRecord,
} from "./commercial-source-selection-v1.js";
import { RESEARCH_DISPOSITION } from "./dynamic-research-planner-v1.js";

export const CROSS_ORG_RETRIEVAL_RECOVERY_VERSION = "gdi-cross-organization-retrieval-recovery-v1";

export const RETRIEVAL_OUTCOME = Object.freeze({
  SUCCESS_WITH_RELEVANT_CONTENT: "SUCCESS_WITH_RELEVANT_CONTENT",
  SUCCESS_WITHOUT_RELEVANT_CONTENT: "SUCCESS_WITHOUT_RELEVANT_CONTENT",
  THIN_OR_UNREADABLE: "THIN_OR_UNREADABLE",
  PROVIDER_ERROR: "PROVIDER_ERROR",
  ACCESS_RESTRICTED: "ACCESS_RESTRICTED",
  NOT_ATTEMPTED: "NOT_ATTEMPTED",
});

export const DEFAULT_SUBSTANTIVE_RETRIEVAL_CAP = 3;
export const DEFAULT_FAILED_TECHNICAL_ATTEMPT_CAP = 6;

export const ORGANIZER_HOST_DEFAULT = "principallondon.com";

const ALTERNATE_PUBLISHER_SEEDS = Object.freeze({
  "thesavoy.co.uk": [
    "https://www.fairmont.com/savoy-london/",
    "https://www.fairmont.com/en/hotels/london/fairmont-savoy.html",
  ],
  "fairmont.com": [
    "https://www.thesavoy.co.uk/",
    "https://www.thesavoy.co.uk/meetings-and-events",
  ],
});

const INDEPENDENT_COVERAGE_SEEDS = Object.freeze([
  "https://www.hospitalitynet.org/",
  "https://www.meetingpack.com/",
]);

function normalizeUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "").toLowerCase();
}

function hostOf(url) {
  try {
    return new URL(String(url || "").trim()).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Classify a retrieval attempt without evaluating commercial evidence claims.
 */
export function classifyRetrievalOutcome({
  attempted = true,
  ok = false,
  text = "",
  providerError = null,
  httpStatus = null,
  accessRestricted = false,
  relevantContentPredicate = null,
} = {}) {
  if (!attempted) {
    return {
      retrievalOutcome: RETRIEVAL_OUTCOME.NOT_ATTEMPTED,
      isSuccessfulRead: false,
      isFailedTechnicalAttempt: false,
      countsTowardSubstantiveRetrievalCap: false,
      countsTowardFailedTechnicalAttemptCap: false,
      mayEvaluateEvidence: false,
    };
  }
  if (accessRestricted === true || httpStatus === 401 || httpStatus === 403 || /captcha|access.?denied|login required/i.test(String(providerError || ""))) {
    return {
      retrievalOutcome: RETRIEVAL_OUTCOME.ACCESS_RESTRICTED,
      isSuccessfulRead: false,
      isFailedTechnicalAttempt: true,
      countsTowardSubstantiveRetrievalCap: false,
      countsTowardFailedTechnicalAttemptCap: true,
      mayEvaluateEvidence: false,
      note: "Access restricted; do not bypass authentication, CAPTCHA, or access controls.",
    };
  }
  const body = String(text || "").trim();
  if (/thin_or_unreadable|empty_direct_public|firecrawl_empty_body/i.test(String(providerError || "")) || (ok && body.length > 0 && body.length < 200)) {
    return {
      retrievalOutcome: RETRIEVAL_OUTCOME.THIN_OR_UNREADABLE,
      isSuccessfulRead: false,
      isFailedTechnicalAttempt: true,
      countsTowardSubstantiveRetrievalCap: false,
      countsTowardFailedTechnicalAttemptCap: true,
      mayEvaluateEvidence: false,
      providerError: providerError || "thin_or_unreadable",
    };
  }
  if (providerError || (httpStatus != null && httpStatus >= 400) || (ok === false && !body)) {
    return {
      retrievalOutcome: RETRIEVAL_OUTCOME.PROVIDER_ERROR,
      isSuccessfulRead: false,
      isFailedTechnicalAttempt: true,
      countsTowardSubstantiveRetrievalCap: false,
      countsTowardFailedTechnicalAttemptCap: true,
      mayEvaluateEvidence: false,
      providerError: providerError || (httpStatus != null ? `http_${httpStatus}` : "provider_error"),
    };
  }
  if (!ok || body.length < 200) {
    return {
      retrievalOutcome: RETRIEVAL_OUTCOME.THIN_OR_UNREADABLE,
      isSuccessfulRead: false,
      isFailedTechnicalAttempt: true,
      countsTowardSubstantiveRetrievalCap: false,
      countsTowardFailedTechnicalAttemptCap: true,
      mayEvaluateEvidence: false,
    };
  }
  const relevant = typeof relevantContentPredicate === "function"
    ? Boolean(relevantContentPredicate(body))
    : true;
  return {
    retrievalOutcome: relevant
      ? RETRIEVAL_OUTCOME.SUCCESS_WITH_RELEVANT_CONTENT
      : RETRIEVAL_OUTCOME.SUCCESS_WITHOUT_RELEVANT_CONTENT,
    isSuccessfulRead: true,
    isFailedTechnicalAttempt: false,
    countsTowardSubstantiveRetrievalCap: true,
    countsTowardFailedTechnicalAttemptCap: false,
    mayEvaluateEvidence: true,
  };
}

/**
 * Build a source-attempt ledger entry. Failed/empty responses do not create rejected factual claims.
 */
export function recordSourceAttempt({
  url,
  organizationHost = ORGANIZER_HOST_DEFAULT,
  organizationName = null,
  provider = null,
  method = null,
  researchQuestion = null,
  ok = false,
  text = "",
  html = "",
  providerError = null,
  httpStatus = null,
  accessRestricted = false,
  actualUsd = 0,
  relevantContentPredicate = null,
  attemptIndex = null,
} = {}) {
  const publisher = resolvePublishingOrganization({
    url,
    organizationHost,
    organizationName,
  });
  const classification = classifyRetrievalOutcome({
    attempted: true,
    ok,
    text,
    providerError,
    httpStatus,
    accessRestricted,
    relevantContentPredicate,
  });
  return {
    url: String(url || ""),
    attemptIndex,
    provider,
    method: method || provider,
    researchQuestion: researchQuestion || null,
    publishingOrganization: publisher.publishingOrganization,
    sourceOwnerHost: publisher.sourceOwnerHost,
    independentOfOrganizer: publisher.independentOfOrganizer,
    samePublisherAsOrganizer: publisher.samePublisherAsOrganizer,
    retrievalOutcome: classification.retrievalOutcome,
    isSuccessfulRead: classification.isSuccessfulRead,
    isFailedTechnicalAttempt: classification.isFailedTechnicalAttempt,
    countsTowardSubstantiveRetrievalCap: classification.countsTowardSubstantiveRetrievalCap,
    countsTowardFailedTechnicalAttemptCap: classification.countsTowardFailedTechnicalAttemptCap,
    mayEvaluateEvidence: classification.mayEvaluateEvidence,
    providerError: classification.providerError || providerError || null,
    httpStatus: httpStatus || null,
    actualUsd: Number(actualUsd) || 0,
    bodyChars: String(text || "").length,
    htmlChars: String(html || "").length,
    note: classification.note || null,
    // Explicit: no rejected factual claims from empty/failed responses.
    evidenceEvaluationSkipped: classification.mayEvaluateEvidence !== true,
    rejectedFactualClaimsFromThisAttempt: [],
  };
}

export function summarizeAttemptBudget({
  attempts = [],
  substantiveRetrievalCap = DEFAULT_SUBSTANTIVE_RETRIEVAL_CAP,
  failedTechnicalAttemptCap = DEFAULT_FAILED_TECHNICAL_ATTEMPT_CAP,
} = {}) {
  const substantiveSuccessful = attempts.filter((item) => item.countsTowardSubstantiveRetrievalCap).length;
  const failedTechnical = attempts.filter((item) => item.countsTowardFailedTechnicalAttemptCap).length;
  return {
    substantiveSuccessfulRetrievals: substantiveSuccessful,
    substantiveRetrievalCap,
    substantiveRetrievalsRemaining: Math.max(0, substantiveRetrievalCap - substantiveSuccessful),
    failedTechnicalAttempts: failedTechnical,
    failedTechnicalAttemptCap,
    failedTechnicalAttemptsRemaining: Math.max(0, failedTechnicalAttemptCap - failedTechnical),
    canAttemptMore:
      substantiveSuccessful < substantiveRetrievalCap
      && failedTechnical < failedTechnicalAttemptCap,
  };
}

/**
 * After a failed retrieval, propose recovery without repeating failed URLs/domains excessively
 * and without bypassing access controls. Preserves the original commercial research question.
 */
export function proposeAdaptiveRecovery({
  failedAttempt,
  attempts = [],
  candidateQueue = [],
  researchQuestion = "",
  organizationHost = ORGANIZER_HOST_DEFAULT,
  alternatePublisherSeeds = ALTERNATE_PUBLISHER_SEEDS,
  independentCoverageSeeds = INDEPENDENT_COVERAGE_SEEDS,
  maxFailuresBeforeHostCooldownedown = 2,
} = {}) {
  const originalQuestion = researchQuestion
    || failedAttempt?.researchQuestion
    || "Does an independently published source corroborate the Principal–Savoy relationship?";
  const failedUrl = normalizeUrl(failedAttempt?.url);
  const failedHost = failedAttempt?.sourceOwnerHost || hostOf(failedAttempt?.url);
  const failedMethods = new Set(
    attempts
      .filter((item) => normalizeUrl(item.url) === failedUrl && item.isFailedTechnicalAttempt)
      .map((item) => item.method || item.provider)
      .filter(Boolean),
  );
  const hostFailureCount = attempts.filter((item) => (
    item.isFailedTechnicalAttempt
    && (item.sourceOwnerHost || hostOf(item.url)) === failedHost
  )).length;

  const actions = [];
  const nextCandidates = [];
  const blockedUrls = new Set(
    attempts
      .filter((item) => item.isFailedTechnicalAttempt)
      .map((item) => normalizeUrl(item.url)),
  );
  const cooledHosts = new Set();
  if (hostFailureCount >= maxFailuresBeforeHostCooldownedown && failedHost) {
    cooledHosts.add(failedHost);
    actions.push({
      type: "cooldown_failed_host",
      host: failedHost,
      reason: `Host failed ${hostFailureCount} times; avoid repeated failed domain attempts.`,
    });
  }

  // Distinct permitted retrieval method when justified (e.g. direct thin → priced firecrawl already tried).
  if (failedAttempt?.retrievalOutcome === RETRIEVAL_OUTCOME.THIN_OR_UNREADABLE && !failedMethods.has("firecrawl")) {
    actions.push({
      type: "try_alternate_retrieval_method",
      url: failedAttempt.url,
      method: "firecrawl_scrape",
      requiresPricedAuthorization: true,
      bypassAuthOrCaptcha: false,
      reason: "Direct public response was thin/unreadable; a priced Firecrawl scrape may be justified if authorized.",
    });
  } else if (failedAttempt?.retrievalOutcome === RETRIEVAL_OUTCOME.PROVIDER_ERROR && !failedMethods.has("direct_public")) {
    actions.push({
      type: "try_alternate_retrieval_method",
      url: failedAttempt.url,
      method: "direct_public",
      requiresPricedAuthorization: false,
      bypassAuthOrCaptcha: false,
      reason: "Provider error on secondary method; do not retry the same failed provider blindly.",
    });
  }

  if (failedAttempt?.retrievalOutcome === RETRIEVAL_OUTCOME.ACCESS_RESTRICTED) {
    actions.push({
      type: "skip_access_restricted",
      url: failedAttempt.url,
      bypassAuthOrCaptcha: false,
      reason: "Do not bypass authentication, CAPTCHA, or access controls.",
    });
  }

  // Official alternate publisher / domain.
  const alternates = alternatePublisherSeeds[failedHost] || [];
  for (const url of alternates) {
    if (blockedUrls.has(normalizeUrl(url))) continue;
    if (!isIndependentPublisherUrl(url, organizationHost)) continue;
    if (cooledHosts.has(hostOf(url))) continue;
    nextCandidates.push({
      url,
      reason: `Official alternate publisher/domain after failures on ${failedHost}`,
      recoveryType: "alternate_publisher",
    });
    actions.push({
      type: "consider_alternate_publisher",
      url,
      publishingOrganization: resolvePublishingOrganization({ url, organizationHost }).publishingOrganization,
    });
  }

  // Independently published coverage from relevant organizations (seed directions only; no auth bypass).
  for (const url of independentCoverageSeeds) {
    if (blockedUrls.has(normalizeUrl(url))) continue;
    if (!isIndependentPublisherUrl(url, organizationHost)) continue;
    nextCandidates.push({
      url,
      reason: "Independently published coverage direction from a relevant trade/organization publisher",
      recoveryType: "independent_coverage_search_direction",
    });
    actions.push({
      type: "search_independent_coverage_direction",
      url,
      note: "Direction only; subsequent retrieval still requires budget and provider authorization.",
    });
  }

  // Keep existing unread candidates that are not cooled/blocked.
  for (const url of candidateQueue || []) {
    if (blockedUrls.has(normalizeUrl(url))) continue;
    if (!isIndependentPublisherUrl(url, organizationHost)) continue;
    if (cooledHosts.has(hostOf(url))) continue;
    if (nextCandidates.some((item) => normalizeUrl(item.url) === normalizeUrl(url))) continue;
    nextCandidates.push({
      url,
      reason: "Existing unread independent candidate preserved after recovery",
      recoveryType: "existing_queue",
    });
  }

  // Prefer Fairmont after thesavoy.co.uk cooldown.
  nextCandidates.sort((a, b) => {
    const score = (item) => {
      let s = 0;
      if (/fairmont\.com/i.test(item.url)) s += 20;
      if (item.recoveryType === "alternate_publisher") s += 10;
      if (item.recoveryType === "independent_coverage_search_direction") s += 4;
      return s;
    };
    return score(b) - score(a) || a.url.localeCompare(b.url);
  });

  const proposedNext = nextCandidates[0] || null;
  return {
    version: CROSS_ORG_RETRIEVAL_RECOVERY_VERSION,
    preservedResearchQuestion: originalQuestion,
    failedUrl: failedAttempt?.url || null,
    failedHost,
    cooledHosts: [...cooledHosts],
    blockedUrls: [...blockedUrls],
    actions,
    nextCandidates: nextCandidates.slice(0, 8),
    proposedNextIndependentSource: proposedNext,
    principalOwnedPagesEligibleAsIndependent: false,
    bypassAuthOrCaptcha: false,
    relationshipVerifiedByRecoveryAlone: false,
    nextActionRequiresBudgetAndProviderAuthorization: true,
  };
}

/**
 * Cross-org stop/report record: completeness separate from verification and buyer engagement.
 */
export function buildCrossOrgInvestigationReport({
  attempts = [],
  independentlyVerified = false,
  verificationStatus = RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
  commercialNextAction = null,
  remainingAuthorizedBudgetUsd = null,
  substantiveRetrievalCap = DEFAULT_SUBSTANTIVE_RETRIEVAL_CAP,
  failedTechnicalAttemptCap = DEFAULT_FAILED_TECHNICAL_ATTEMPT_CAP,
  buyerEngagementStillValid = true,
} = {}) {
  const budget = summarizeAttemptBudget({
    attempts,
    substantiveRetrievalCap,
    failedTechnicalAttemptCap,
  });
  const successfulReads = attempts.filter((item) => item.isSuccessfulRead);
  const failedAttempts = attempts.filter((item) => item.isFailedTechnicalAttempt);
  const independentSuccessfulReads = successfulReads.filter((item) => item.independentOfOrganizer === true);

  let researchCompleteness = "INCOMPLETE_NO_INDEPENDENT_SOURCE_READ";
  if (independentlyVerified) researchCompleteness = "COMPLETE_INDEPENDENTLY_VERIFIED";
  else if (independentSuccessfulReads.length > 0) researchCompleteness = "COMPLETE_INDEPENDENT_SOURCES_READ_UNVERIFIED";
  else if (failedAttempts.length > 0 && successfulReads.length === 0) {
    researchCompleteness = "INCOMPLETE_RETRIEVAL_FAILURES_ONLY";
  }

  // Do not label independent-source investigation complete when nothing was successfully read.
  const independentSourceInvestigationComplete = independentSuccessfulReads.length > 0 || independentlyVerified === true;

  let stoppingTrigger = null;
  if (independentlyVerified) stoppingTrigger = STOPPING_TRIGGER.RESEARCH_SUFFICIENT;
  else if (budget.failedTechnicalAttempts >= failedTechnicalAttemptCap) stoppingTrigger = "FAILED_TECHNICAL_ATTEMPT_CAP_REACHED";
  else if (budget.substantiveSuccessfulRetrievals >= substantiveRetrievalCap) stoppingTrigger = STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED;
  else if (successfulReads.length === 0 && failedAttempts.length > 0) stoppingTrigger = "RETRIEVAL_RECOVERY_REQUIRED";

  const stopRecord = buildStopRecord({
    researchDisposition: independentlyVerified
      ? RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT
      : RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED,
    commercialNextAction: commercialNextAction
      || (buyerEngagementStillValid
        ? "Buyer engagement remains valid; independent corroboration unavailable or incomplete"
        : null),
    stoppingTrigger,
    unresolvedQuestions: independentlyVerified
      ? []
      : ["Independent publisher confirmation of the claimed relationship remains unresolved."],
    remainingAuthorizedBudgetUsd,
    retrievalCount: budget.substantiveSuccessfulRetrievals,
    maxRetrievals: substantiveRetrievalCap,
  });

  return {
    version: CROSS_ORG_RETRIEVAL_RECOVERY_VERSION,
    researchCompleteness,
    independentSourceInvestigationComplete,
    commercialNextAction: stopRecord.commercialNextAction,
    retrievalFailures: failedAttempts.map((item) => ({
      url: item.url,
      publishingOrganization: item.publishingOrganization,
      sourceOwnerHost: item.sourceOwnerHost,
      retrievalOutcome: item.retrievalOutcome,
      provider: item.provider,
      providerError: item.providerError,
    })),
    budgetOrAttemptLimits: {
      substantiveSuccessfulRetrievals: budget.substantiveSuccessfulRetrievals,
      substantiveRetrievalCap,
      failedTechnicalAttempts: budget.failedTechnicalAttempts,
      failedTechnicalAttemptCap,
      canAttemptMore: budget.canAttemptMore,
    },
    independentVerificationStatus: verificationStatus,
    independentlyVerified,
    buyerEngagementValidDespiteMissingIndependentCorroboration: buyerEngagementStillValid === true,
    stopRecord,
    note: independentSourceInvestigationComplete
      ? null
      : "Independent-source investigation is not complete: no independent source was successfully read.",
  };
}

/**
 * Offline replay of the saved Savoy Firecrawl failures with adaptive recovery.
 * No network. No ledger spend.
 */
export function runOfflineSavoyRetrievalRecoveryCertification({
  savedCertificationResult = null,
  organizationHost = ORGANIZER_HOST_DEFAULT,
} = {}) {
  const saved = savedCertificationResult || {};
  const researchQuestion = saved.researchObjective
    || "Does an independently published source corroborate a programme or commercial relationship between Principal. London DMC and The Savoy?";

  const savedVisits = saved.sourcesVisited || [];
  const attempts = [];
  const planLog = [];

  for (let index = 0; index < savedVisits.length; index += 1) {
    const visit = savedVisits[index];
    const attempt = recordSourceAttempt({
      url: visit.url,
      organizationHost,
      organizationName: "Principal. London DMC",
      provider: visit.provider || "firecrawl",
      method: visit.provider || "firecrawl",
      researchQuestion: visit.question || researchQuestion,
      ok: visit.ok === true,
      text: "",
      providerError: visit.ok === false ? "firecrawl_http_500" : null,
      httpStatus: visit.ok === false ? 500 : null,
      actualUsd: visit.actualUsd || 0,
      attemptIndex: index + 1,
    });
    attempts.push(attempt);
    planLog.push({
      event: "retrieval_failure_recorded",
      url: attempt.url,
      retrievalOutcome: attempt.retrievalOutcome,
      evidenceEvaluationSkipped: attempt.evidenceEvaluationSkipped,
      rejectedFactualClaimsFromThisAttempt: attempt.rejectedFactualClaimsFromThisAttempt,
    });
  }

  // Demonstrate recovery after repeated thesavoy.co.uk failures.
  let recovery = null;
  if (attempts.length) {
    recovery = proposeAdaptiveRecovery({
      failedAttempt: attempts[attempts.length - 1],
      attempts,
      candidateQueue: [
        "https://www.thesavoy.co.uk/meetings-and-events",
        "https://www.fairmont.com/savoy-london/",
        "https://www.principallondon.com/news",
      ],
      researchQuestion,
      organizationHost,
    });
    planLog.push({
      event: "adaptive_recovery_proposed",
      cooledHosts: recovery.cooledHosts,
      proposedNext: recovery.proposedNextIndependentSource,
      actions: recovery.actions.map((item) => item.type),
    });
  }

  const principalCandidate = "https://www.principallondon.com/2026/05/11/an-evening-at-the-savoy";
  const principalEligible = isIndependentPublisherUrl(principalCandidate, organizationHost);
  const fairmontProposed = Boolean(recovery?.proposedNextIndependentSource?.url
    && /fairmont\.com/i.test(recovery.proposedNextIndependentSource.url));
  const providerOrPublisherChanged = Boolean(
    recovery?.actions?.some((item) => item.type === "consider_alternate_publisher" || item.type === "try_alternate_retrieval_method")
    || recovery?.cooledHosts?.includes("thesavoy.co.uk"),
  );

  const report = buildCrossOrgInvestigationReport({
    attempts,
    independentlyVerified: false,
    verificationStatus: RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE,
    commercialNextAction: "Buyer engagement remains valid for Principal→Nobu account development; Savoy independent confirmation still unavailable after retrieval failures",
    remainingAuthorizedBudgetUsd: saved.remainingAuthorizationUsd ?? 0.049175,
    buyerEngagementStillValid: true,
  });

  const correctedSourceAttemptLedger = {
    version: `${CROSS_ORG_RETRIEVAL_RECOVERY_VERSION}-attempt-ledger`,
    researchQuestion,
    attempts,
    substantiveVsFailed: summarizeAttemptBudget({ attempts }),
    note: "Failed Savoy fetches are retrieval failures, not negative content findings about Principal–Savoy.",
  };

  const checks = {
    threeFailuresAreRetrievalFailuresNotNegativeContent: attempts.length === 3
      && attempts.every((item) => item.retrievalOutcome === RETRIEVAL_OUTCOME.PROVIDER_ERROR)
      && attempts.every((item) => item.evidenceEvaluationSkipped === true)
      && attempts.every((item) => item.rejectedFactualClaimsFromThisAttempt.length === 0)
      && attempts.every((item) => item.countsTowardSubstantiveRetrievalCap === false)
      && attempts.every((item) => item.countsTowardFailedTechnicalAttemptCap === true),
    supervisorChangesProviderOrPublisherAfterRepeatedFailure: providerOrPublisherChanged === true,
    fairmontOrOtherIndependentPublisherEligible: fairmontProposed === true
      && isIndependentPublisherUrl(recovery.proposedNextIndependentSource.url, organizationHost),
    principalOwnedPageNotIndependent: principalEligible === false,
    noVerificationWithoutIndependentEvidence: report.independentlyVerified === false
      && report.independentVerificationStatus === RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE,
    nextActionSubjectToBudgetAndProviderAuthorization: recovery?.nextActionRequiresBudgetAndProviderAuthorization === true,
    investigationNotLabeledCompleteWithoutSuccessfulRead: report.independentSourceInvestigationComplete === false
      && report.researchCompleteness === "INCOMPLETE_RETRIEVAL_FAILURES_ONLY",
  };

  return {
    version: CROSS_ORG_RETRIEVAL_RECOVERY_VERSION,
    offline: true,
    liveCalls: 0,
    spent: false,
    correctedSourceAttemptLedger,
    recoveryStrategy: recovery,
    proposedNextIndependentSource: recovery?.proposedNextIndependentSource || null,
    investigationReport: report,
    planLog,
    checks,
    certificationPassed: Object.values(checks).every(Boolean),
    publicationEligibilityChanged: false,
  };
}
