/**
 * Operator Intelligence — publication policy resolver (pure / deterministic).
 * @see docs/architecture/operator-intelligence-governance-model.md
 */

export const PUBLICATION_DECISION = Object.freeze({
  AUTO_PUBLISH: "Auto-Publish",
  PUBLISH_WITH_LABEL: "Publish With Evidence Label",
  INTERNAL_ONLY: "Internal Only",
  HUMAN_REVIEW_REQUIRED: "Human Review Required",
  REJECTED: "Rejected",
  STALE: "Stale",
  CONFLICTED: "Conflicted",
  INSUFFICIENT_EVIDENCE: "Insufficient Evidence",
});

export const PUBLICATION_CLASS = Object.freeze({
  CLASS_1: 1,
  CLASS_2: 2,
  CLASS_3: 3,
  CLASS_4: 4,
});

export const SOURCE_AUTHORITY = Object.freeze({
  PRIMARY_AUTHORITATIVE: "primary_authoritative",
  RELIABLE_INDEPENDENT: "reliable_independent",
  OPERATOR_MARKETING: "operator_marketing",
  TRADE_PRESS: "trade_press",
  PROFESSIONAL_PROFILE: "professional_profile",
  AI_OR_SNIPPET: "ai_or_snippet",
  OTHER: "other",
});

const CLASS3_CATEGORIES = new Set([
  "performance",
  "fees",
  "contract_terms",
  "project_specific_interest",
  "project_specific_brand_approval",
  "brand_managed_availability_project",
  "organizational_capacity",
  "proposed_leadership",
  "owner_satisfaction",
  "competitive_conflicts",
]);

const SENSITIVE_FLAGS = new Set([
  "negative_claim",
  "damaging",
  "litigation",
  "termination",
  "fraud",
]);

/**
 * @param {object} claim
 * @param {{ sources?: object[], now?: Date }} [ctx]
 */
export function resolvePublicationDecision(claim, ctx = {}) {
  const sources = normalizeSources(claim, ctx.sources || []);
  const now = ctx.now || new Date();
  const category = String(claim.claimCategory || claim.category || "").toLowerCase();
  const pubClass = Number(claim.publicationClass || claim.publication_class || 0);
  const conflict = String(claim.conflictStatus || claim.conflict_status || "None");
  const verification = String(claim.verificationStatus || claim.verification_status || "Unverified");
  const scoringImpact = String(claim.potentialScoreImpact || claim.scoringImpact || "None");
  const sensitive =
    claim.sensitive === true ||
    SENSITIVE_FLAGS.has(String(claim.sensitivity || "").toLowerCase()) ||
    Boolean(claim.flags?.negative);

  if (sources.some((s) => s.authority === SOURCE_AUTHORITY.AI_OR_SNIPPET || s.isSnippet || s.isAiSummary)) {
    return decision(PUBLICATION_DECISION.REJECTED, "AI summaries and search snippets are never evidence.");
  }

  if (pubClass === PUBLICATION_CLASS.CLASS_4 || claim.neverInfer === true) {
    return decision(PUBLICATION_DECISION.REJECTED, "Class 4 never-infer rule.");
  }

  if (conflict === "Hard" || conflict === "Conflicted") {
    return decision(PUBLICATION_DECISION.CONFLICTED, "Hard conflict — exception queue required.", {
      humanReviewRequired: true,
    });
  }

  if (sensitive) {
    return decision(PUBLICATION_DECISION.HUMAN_REVIEW_REQUIRED, "Sensitive or potentially damaging claim.", {
      humanReviewRequired: true,
    });
  }

  if (isStale(claim, now)) {
    return decision(PUBLICATION_DECISION.STALE, "Claim past review/expiration without corroboration.", {
      humanReviewRequired: scoringImpact === "High",
    });
  }

  if (
    pubClass === PUBLICATION_CLASS.CLASS_3 ||
    CLASS3_CATEGORIES.has(category) ||
    claim.internalOnly === true
  ) {
    return decision(
      PUBLICATION_DECISION.INTERNAL_ONLY,
      "Class 3 — project-specific or financial/outreach data remains internal."
    );
  }

  const primaryCount = sources.filter((s) => s.authority === SOURCE_AUTHORITY.PRIMARY_AUTHORITATIVE).length;
  const independentCount = sources.filter(
    (s) =>
      s.authority === SOURCE_AUTHORITY.RELIABLE_INDEPENDENT ||
      s.authority === SOURCE_AUTHORITY.TRADE_PRESS
  ).length;
  const marketingOnly =
    sources.length > 0 &&
    sources.every(
      (s) =>
        s.authority === SOURCE_AUTHORITY.OPERATOR_MARKETING ||
        s.authority === SOURCE_AUTHORITY.PROFESSIONAL_PROFILE
    );

  if (category === "performance" || claim.subject === "financial_performance") {
    if (marketingOnly || primaryCount === 0) {
      return decision(
        PUBLICATION_DECISION.REJECTED,
        "Operator marketing cannot verify performance metrics."
      );
    }
    return decision(PUBLICATION_DECISION.INTERNAL_ONLY, "Performance evidence stays internal without diligence.");
  }

  // Class 1 objective facts
  if (pubClass === PUBLICATION_CLASS.CLASS_1 || claim.objectiveFact === true) {
    if (primaryCount >= 1) {
      return decision(PUBLICATION_DECISION.AUTO_PUBLISH, "One authoritative primary source supports objective fact.");
    }
    if (independentCount >= 2 && sourcesConsistent(sources)) {
      return decision(
        PUBLICATION_DECISION.AUTO_PUBLISH,
        "Two consistent independent sources support objective fact."
      );
    }
    if (sources.length === 0) {
      return decision(PUBLICATION_DECISION.INSUFFICIENT_EVIDENCE, "No sources attached.");
    }
    if (marketingOnly) {
      return decision(
        PUBLICATION_DECISION.PUBLISH_WITH_LABEL,
        "Only operator marketing — publish with Operator Reported label if non-sensitive identity/capability.",
        { evidenceLabel: "Operator Reported" }
      );
    }
    return decision(PUBLICATION_DECISION.INSUFFICIENT_EVIDENCE, "Objective fact lacks Class 1 threshold.");
  }

  // Class 2 qualified
  if (pubClass === PUBLICATION_CLASS.CLASS_2 || claim.requiresEvidenceLabel === true) {
    if (sources.length === 0) {
      return decision(PUBLICATION_DECISION.INSUFFICIENT_EVIDENCE, "Qualified claim needs at least one source.");
    }
    const label =
      claim.evidenceLabel ||
      (marketingOnly ? "Operator Reported" : independentCount ? "Independently Referenced" : "Portfolio Supported");
    if (scoringImpact === "High" && marketingOnly) {
      return decision(PUBLICATION_DECISION.HUMAN_REVIEW_REQUIRED, "High scoring impact with marketing-only sources.", {
        humanReviewRequired: true,
        evidenceLabel: label,
      });
    }
    return decision(PUBLICATION_DECISION.PUBLISH_WITH_LABEL, "Class 2 auto-publish with evidence label.", {
      evidenceLabel: label,
    });
  }

  if (sources.length === 0) {
    return decision(PUBLICATION_DECISION.INSUFFICIENT_EVIDENCE, "No publication class and no sources.");
  }

  return decision(PUBLICATION_DECISION.HUMAN_REVIEW_REQUIRED, "Unclassified claim — exception review.", {
    humanReviewRequired: true,
  });
}

function normalizeSources(claim, fallbackSources) {
  const ids = Array.isArray(claim.sourceIds) ? claim.sourceIds : claim.sourceId ? [claim.sourceId] : [];
  if (Array.isArray(claim.sources) && claim.sources.length) return claim.sources.map(normSource);
  if (ids.length && fallbackSources.length) {
    return fallbackSources.filter((s) => ids.includes(s.id) || ids.includes(s.sourceId)).map(normSource);
  }
  if (claim.sourceType || claim.sourceAuthority) {
    return [normSource(claim)];
  }
  return [];
}

function normSource(s) {
  return {
    id: s.id || s.sourceId || null,
    authority: s.authority || s.sourceAuthority || mapSourceType(s.sourceType || s.type),
    isSnippet: Boolean(s.isSnippet || s.searchSnippet),
    isAiSummary: Boolean(s.isAiSummary || s.aiGenerated),
    dated: Boolean(s.date || s.publishedAt || s.accessedAt),
  };
}

function mapSourceType(t) {
  const x = String(t || "").toLowerCase();
  if (/ai|snippet|directory.?auto/.test(x)) return SOURCE_AUTHORITY.AI_OR_SNIPPET;
  if (/official.*(operator|hotel|brand|portfolio|corporate|press)/.test(x) || /primary/.test(x)) {
    return SOURCE_AUTHORITY.PRIMARY_AUTHORITATIVE;
  }
  if (/trade|press|news|investor|sec/.test(x)) return SOURCE_AUTHORITY.RELIABLE_INDEPENDENT;
  if (/linkedin|professional/.test(x)) return SOURCE_AUTHORITY.PROFESSIONAL_PROFILE;
  if (/marketing|operator.?site|corporate.?claim/.test(x)) return SOURCE_AUTHORITY.OPERATOR_MARKETING;
  return SOURCE_AUTHORITY.OTHER;
}

function sourcesConsistent(sources) {
  return sources.length >= 2;
}

function isStale(claim, now) {
  const exp = claim.expirationDate || claim.reviewDate || claim.expiresAt;
  if (!exp) return false;
  const d = new Date(exp);
  if (Number.isNaN(d.getTime())) return false;
  return d.getTime() < now.getTime() && !claim.corroboratedAfterExpiration;
}

function decision(status, reason, extra = {}) {
  return {
    status,
    reason,
    humanReviewRequired: Boolean(extra.humanReviewRequired),
    evidenceLabel: extra.evidenceLabel || null,
    ownerFacingConfirmed: status === PUBLICATION_DECISION.AUTO_PUBLISH,
  };
}

/**
 * Whether a claim may appear as confirmed owner-facing fact.
 */
export function isOwnerFacingConfirmed(decisionResult) {
  return decisionResult?.status === PUBLICATION_DECISION.AUTO_PUBLISH;
}

/**
 * Strong confidence cannot come from operator-reported alone.
 */
export function allowsStrongConfidence(claim, decisionResult) {
  if (!decisionResult) return false;
  if (decisionResult.status === PUBLICATION_DECISION.INTERNAL_ONLY) return false;
  if (decisionResult.evidenceLabel === "Operator Reported") return false;
  const classes = [].concat(claim.evidenceClasses || claim.evidenceClass || []);
  if (classes.some((c) => /general_claim|operator.?reported|unknown/i.test(String(c)))) return false;
  return (
    decisionResult.status === PUBLICATION_DECISION.AUTO_PUBLISH ||
    (decisionResult.status === PUBLICATION_DECISION.PUBLISH_WITH_LABEL &&
      /Independently Referenced|Verified/i.test(String(decisionResult.evidenceLabel || "")))
  );
}
