/**
 * Weekly GDI Refresh Contract V1 — extends research-to-page coordinator.
 * Distinguishes initial coverage backlog from refresh queue.
 * Automated publication remains OFF until explicitly enabled.
 */

export const WEEKLY_REFRESH_CONTRACT_VERSION = "gdi-weekly-refresh-contract-v1";

export const RUN_MODE = Object.freeze({
  INITIAL_COVERAGE: "initial_coverage",
  REFRESH: "refresh",
  COMBINED: "combined",
});

export const RUN_TRIGGER = Object.freeze({
  MANUAL: "manual",
  SCHEDULED: "scheduled",
  RETRY: "retry",
  SIMULATED: "simulated",
});

export const RUN_STATUS = Object.freeze({
  PENDING: "pending",
  RUNNING: "running",
  COMPLETED: "completed",
  PARTIAL: "partial",
  FAILED: "failed",
  BLOCKED: "blocked",
});

/** Change detection vs current canonical records. */
export const CHANGE_CLASS = Object.freeze({
  NEW_PROSPECT: "NEW_PROSPECT",
  MATERIAL_UPDATE: "MATERIAL_UPDATE",
  CONTACT_ENRICHED: "CONTACT_ENRICHED",
  OCCASION_CONCLUDED_OR_STATUS_CHANGED: "OCCASION_CONCLUDED_OR_STATUS_CHANGED",
  NO_MATERIAL_CHANGE: "NO_MATERIAL_CHANGE",
  NEEDS_REVIEW: "NEEDS_REVIEW",
  RESEARCH_BLOCKED: "RESEARCH_BLOCKED",
});

/** Fields that count as material commercial changes (extends weekly-delta). */
export const MATERIAL_CHANGE_FIELDS = Object.freeze([
  "eventStartDate",
  "eventEndDate",
  "destinationStatus",
  "venueStatus",
  "venueSourcingStatus",
  "opportunityType",
  "roomDemandStatus",
  "hostHotel",
  "venue",
  "organizationName",
  "proposedHotelUse",
  "hotelOpportunityThesis",
  "guestroomStatus",
  "demandUseCases",
  "growthSignals",
  "localConnection",
  "datedGrowthEvidence",
]);

export const CONTACT_CHANGE_FIELDS = Object.freeze([
  "contactEmail",
  "contactPhone",
  "contactName",
  "contactUrl",
  "primaryContactEmail",
  "primaryContactPhone",
  "primaryContactName",
  "commercialContactPath",
]);

/** Non-material: retrieval timestamps, cosmetic prose, weekly labels alone. */
export const NON_MATERIAL_FIELDS = Object.freeze([
  "lastCheckedAt",
  "lastSeenAt",
  "updatedAt",
  "retrievalTimestamp",
  "weeklyDeltaState",
  "isNewThisWeek",
  "labels",
]);

/**
 * Default cadence (days). Hotel config may override via
 * hotelConfig.weeklyRefresh.cadenceDays.
 */
export const DEFAULT_CADENCE_DAYS = Object.freeze({
  decisionDeadlineImminent: 3,
  upcomingDatedOccasion: 7,
  unresolvedHighValueGap: 7,
  contactVerification: 30,
  growthOrCorporateActivity: 14,
  stableVenueProfile: 60,
  standingAccountRefresh: 28,
  failedRetrievalRetry: 3,
  hotelFeedbackFollowUp: 7,
  defaultSegmentRefresh: 21,
});

export const PUBLICATION_POLICY = Object.freeze({
  /** Qualified new prospects — may auto-publish only when enabled + gates pass */
  QUALIFIED_NEW: "QUALIFIED_NEW",
  /** Supported material updates */
  MATERIAL_UPDATE: "MATERIAL_UPDATE",
  /** Non-material verification — update lastChecked only, no customer rewrite */
  NON_MATERIAL_VERIFICATION: "NON_MATERIAL_VERIFICATION",
  /** Ambiguous / conflicting — internal review required */
  INTERNAL_REVIEW: "INTERNAL_REVIEW",
});

/** Automated publication kill switch — OFF until proven. */
export const AUTO_PUBLISH_DEFAULT = false;

export const QUEUE_KIND = Object.freeze({
  COVERAGE_BACKLOG: "coverage_backlog",
  REFRESH: "refresh",
  CONTACT_ENRICHMENT: "contact_enrichment",
  FEEDBACK_FOLLOWUP: "feedback_followup",
  RETRY: "retry",
});

/**
 * Compare prior vs candidate opportunity; return CHANGE_CLASS + reasons.
 * Does not invent materiality from timestamps alone.
 */
export function classifyRefreshChange(prior = null, candidate = {}, opts = {}) {
  if (!prior) {
    if (opts.previouslyAssessedInternal) {
      return {
        changeClass: CHANGE_CLASS.NEEDS_REVIEW,
        publicationPolicy: PUBLICATION_POLICY.INTERNAL_REVIEW,
        reasons: ["rediscovered_previously_assessed_internal_or_rejected"],
        materialFieldsChanged: [],
        contactFieldsChanged: [],
      };
    }
    return {
      changeClass: CHANGE_CLASS.NEW_PROSPECT,
      publicationPolicy: PUBLICATION_POLICY.QUALIFIED_NEW,
      reasons: ["no_prior_canonical_match"],
      materialFieldsChanged: [],
      contactFieldsChanged: [],
    };
  }

  if (opts.researchBlocked) {
    return {
      changeClass: CHANGE_CLASS.RESEARCH_BLOCKED,
      publicationPolicy: PUBLICATION_POLICY.NON_MATERIAL_VERIFICATION,
      reasons: opts.blockReasons || ["retrieval_failed_or_inaccessible"],
      materialFieldsChanged: [],
      contactFieldsChanged: [],
    };
  }

  const materialFieldsChanged = [];
  for (const f of MATERIAL_CHANGE_FIELDS) {
    const a = normalizeComparable(prior[f]);
    const b = normalizeComparable(candidate[f]);
    if (b != null && b !== "" && a !== b) materialFieldsChanged.push(f);
  }

  const contactFieldsChanged = [];
  for (const f of CONTACT_CHANGE_FIELDS) {
    const a = normalizeComparable(prior[f]);
    const b = normalizeComparable(candidate[f]);
    if (b != null && b !== "" && a !== b) contactFieldsChanged.push(f);
  }

  const conflicting =
    opts.conflictingEvidence === true ||
    (materialFieldsChanged.includes("venueSourcingStatus") &&
      materialFieldsChanged.includes("venueStatus") &&
      opts.conflictHint === true);

  if (conflicting) {
    return {
      changeClass: CHANGE_CLASS.NEEDS_REVIEW,
      publicationPolicy: PUBLICATION_POLICY.INTERNAL_REVIEW,
      reasons: ["conflicting_venue_or_sourcing_evidence"],
      materialFieldsChanged,
      contactFieldsChanged,
    };
  }

  const statusChanged =
    materialFieldsChanged.includes("opportunityType") ||
    materialFieldsChanged.includes("venueSourcingStatus") ||
    opts.occasionConcluded === true;

  if (statusChanged && opts.occasionConcluded) {
    return {
      changeClass: CHANGE_CLASS.OCCASION_CONCLUDED_OR_STATUS_CHANGED,
      publicationPolicy: PUBLICATION_POLICY.MATERIAL_UPDATE,
      reasons: ["occasion_concluded_or_status_changed"],
      materialFieldsChanged,
      contactFieldsChanged,
    };
  }

  if (materialFieldsChanged.length > 0) {
    return {
      changeClass: CHANGE_CLASS.MATERIAL_UPDATE,
      publicationPolicy: PUBLICATION_POLICY.MATERIAL_UPDATE,
      reasons: materialFieldsChanged.map((f) => `material:${f}`),
      materialFieldsChanged,
      contactFieldsChanged,
    };
  }

  if (contactFieldsChanged.length > 0) {
    return {
      changeClass: CHANGE_CLASS.CONTACT_ENRICHED,
      publicationPolicy: PUBLICATION_POLICY.MATERIAL_UPDATE,
      reasons: contactFieldsChanged.map((f) => `contact:${f}`),
      materialFieldsChanged: [],
      contactFieldsChanged,
    };
  }

  return {
    changeClass: CHANGE_CLASS.NO_MATERIAL_CHANGE,
    publicationPolicy: PUBLICATION_POLICY.NON_MATERIAL_VERIFICATION,
    reasons: ["no_material_or_contact_delta"],
    materialFieldsChanged: [],
    contactFieldsChanged: [],
  };
}

function normalizeComparable(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.map(String).sort().join("|");
  if (typeof v === "object") return JSON.stringify(v);
  return String(v).replace(/\s+/g, " ").trim();
}

/**
 * Resolve cadence days for a work item.
 */
export function resolveCadenceDays(item = {}, hotelConfig = {}) {
  const overrides = hotelConfig?.weeklyRefresh?.cadenceDays || {};
  const cadence = { ...DEFAULT_CADENCE_DAYS, ...overrides };
  const kind = String(item.cadenceKind || item.queueKind || "").toLowerCase();

  if (item.decisionDeadlineWithinDays != null && item.decisionDeadlineWithinDays <= 14) {
    return cadence.decisionDeadlineImminent;
  }
  if (item.hasUpcomingDatedOccasion) return cadence.upcomingDatedOccasion;
  if (kind.includes("feedback")) return cadence.hotelFeedbackFollowUp;
  if (kind.includes("contact")) return cadence.contactVerification;
  if (kind.includes("growth") || kind.includes("scaling")) {
    return cadence.growthOrCorporateActivity;
  }
  if (kind.includes("venue") || kind.includes("pe") || kind.includes("stable")) {
    return cadence.stableVenueProfile;
  }
  if (kind.includes("retry") || item.lastRetrievalFailed) {
    return cadence.failedRetrievalRetry;
  }
  if (kind.includes("gap") || kind.includes("coverage")) {
    return cadence.unresolvedHighValueGap;
  }
  if (kind.includes("standing")) return cadence.standingAccountRefresh;
  return cadence.defaultSegmentRefresh;
}

/**
 * Whether a refresh check is due given lastSuccessfulCheckAt and cadence.
 * Uses injectable now (test clock).
 */
export function isCheckDue({
  lastSuccessfulCheckAt = null,
  cadenceDays = DEFAULT_CADENCE_DAYS.defaultSegmentRefresh,
  now = new Date(),
} = {}) {
  if (!lastSuccessfulCheckAt) return true;
  const last = new Date(lastSuccessfulCheckAt).getTime();
  if (!Number.isFinite(last)) return true;
  const ms = Math.max(0, Number(cadenceDays) || 0) * 86400000;
  return new Date(now).getTime() >= last + ms;
}

/**
 * Publication gate: auto-publish only when explicitly enabled AND policy allows.
 */
export function mayAutoPublish(change = {}, opts = {}) {
  const autoEnabled =
    opts.autoPublishEnabled === true && AUTO_PUBLISH_DEFAULT === false
      ? opts.forceAutoPublish === true
      : opts.autoPublishEnabled === true;
  // Default OFF — require explicit forceAutoPublish for automated writes
  if (!opts.forceAutoPublish && !autoEnabled) {
    return {
      allowed: false,
      reason: "auto_publish_disabled",
      policy: change.publicationPolicy || null,
    };
  }
  if (change.publicationPolicy === PUBLICATION_POLICY.INTERNAL_REVIEW) {
    return { allowed: false, reason: "internal_review_required", policy: change.publicationPolicy };
  }
  if (change.publicationPolicy === PUBLICATION_POLICY.NON_MATERIAL_VERIFICATION) {
    return {
      allowed: false,
      reason: "non_material_verification_only",
      policy: change.publicationPolicy,
      updateLastCheckedOnly: true,
    };
  }
  if (!opts.qualificationPassed) {
    return { allowed: false, reason: "qualification_gates_not_passed", policy: change.publicationPolicy };
  }
  return { allowed: true, reason: "gates_passed", policy: change.publicationPolicy };
}

/**
 * Feedback → research task mapping (does not invent feedback).
 */
export function feedbackToResearchTask(feedbackItem = {}) {
  const blob = [
    feedbackItem.action,
    feedbackItem.outcome,
    feedbackItem.reason,
    feedbackItem.note,
    feedbackItem.feedbackType,
    feedbackItem.status,
  ]
    .map((s) => String(s || "").toLowerCase())
    .join(" ");

  if (/wrong.?contact|bad.?contact|bounced|invalid.?email/.test(blob)) {
    return {
      queueKind: QUEUE_KIND.CONTACT_ENRICHMENT,
      priority: "HIGH",
      rationale: "Hotel reported wrong/invalid contact — queue contact resolution",
      suppressNewDiscoveryLabel: false,
    };
  }
  if (/worth.?pursu|interested|hot.?lead|pursue/.test(blob)) {
    return {
      queueKind: QUEUE_KIND.FEEDBACK_FOLLOWUP,
      priority: "HIGH",
      rationale: "Hotel marked worth pursuing — prioritize evidence gaps",
      suppressNewDiscoveryLabel: false,
    };
  }
  if (/already.?known|known.?to.?hotel|duplicate.?known/.test(blob)) {
    return {
      queueKind: QUEUE_KIND.REFRESH,
      priority: "MEDIUM",
      rationale: "Already known — enrich intelligence; do not present as newly discovered",
      suppressNewDiscoveryLabel: true,
    };
  }
  if (/not.?relevant|reject|not.?a.?fit|wrong.?market/.test(blob)) {
    return {
      queueKind: QUEUE_KIND.FEEDBACK_FOLLOWUP,
      priority: "LOW",
      rationale: "Hotel rejected — suppress repetitive recommendations unless material new evidence",
      suppressRecommendations: true,
      requireMaterialChangeToResurface: true,
    };
  }
  if (/pursuing|booked|confirmed|won|in.?progress/.test(blob)) {
    return {
      queueKind: QUEUE_KIND.REFRESH,
      priority: "LOW",
      rationale: "Hotel actively pursuing/booked — preserve pursuit state; avoid conflicting automation",
      preservePursuitState: true,
      suppressAutomatedRecommendations: true,
    };
  }
  return null;
}
