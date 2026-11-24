/**
 * Conditional Exa Agent contact escalation V1.
 *
 * Post-ladder fallback inside BUYER_ALTERNATIVE_PERSON_RESEARCH.
 * Default OFF. Dual authorization required. Medium effort only.
 * Candidates only — never auto-finalize; existing verification + ranking apply.
 *
 * Does not change Agent demand-discovery behavior.
 */
import {
  isExaAgentLiveAuthorized,
  quoteExaAgentRun,
  runExaAgentResearch,
  EXA_AGENT_EFFORT,
} from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import {
  buildExaAgentContactCreatePayload,
  mapExaAgentContactPeopleToCandidates,
  EXA_AGENT_CONTACT_INTELLIGENCE_CONTRACT_V1,
} from "../research/exa-agent/exa-agent-contact-intelligence-contract-v1.js";
import {
  isPlausibleBuyerPersonName,
  employerAffiliationPlausible,
} from "./gdi-original-full-e2e-execution-restore-v1.js";

export const GDI_EXA_AGENT_CONTACT_ESCALATION_V1 =
  "gdi-exa-agent-contact-escalation-v1";

export const EXA_AGENT_CONTACT_ESCALATION_REASON = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  FLAG_OFF: "CONTACT_ESCALATION_FLAG_OFF",
  LIVE_PROVIDER_OFF: "EXA_AGENT_LIVE_NOT_AUTHORIZED",
  WHO_ALREADY_VERIFIED: "WHO_ALREADY_HAS_VERIFIED_NAMED_PERSON",
  MISSING_EMAIL_ONLY: "MISSING_EMAIL_DOES_NOT_TRIGGER_AGENT",
  SECONDARY_ONLY_REFUSED: "SECONDARY_ONLY_REQUIRES_SEPARATE_AUTH",
  LADDER_NOT_EXHAUSTED: "LADDER_NOT_EXHAUSTED",
  LADDER_TRUNCATED_WITHOUT_AUTH: "LADDER_TRUNCATED_WITHOUT_AUTH",
  NO_ORGANIZATION: "ORGANIZATION_NOT_IDENTIFIED",
  DEMAND_NOT_VERIFIED: "DEMAND_EVIDENCE_NOT_VERIFIED",
  NOT_ALLOWLISTED: "OPPORTUNITY_NOT_ALLOWLISTED",
  BUDGET_MISSING: "BUDGET_CEILING_MISSING",
  BUDGET_INSUFFICIENT: "BUDGET_INSUFFICIENT_FOR_MEDIUM",
  ALREADY_ESCALATED: "ALREADY_ESCALATED_THIS_OPPORTUNITY",
  EFFORT_REFUSED: "NON_MEDIUM_EFFORT_REFUSED",
  RESEARCHER_IDENTITY_SKIP: "RESUME_REPLAY_SKIP_DUPLICATE",
});

export const AGENT_CANDIDATE_TYPE = Object.freeze({
  DIRECT_BUYER: "DIRECT_BUYER",
  VIABLE_ALTERNATIVE: "VIABLE_ALTERNATIVE",
  SECONDARY_CONTACT: "SECONDARY_CONTACT",
  RELEVANT_ORGANIZATIONAL_CONTACT: "RELEVANT_ORGANIZATIONAL_CONTACT",
  REJECTED: "REJECTED",
});

const MEDIUM_USD = 0.1;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function now() {
  return new Date().toISOString();
}

function opportunityIdOf(opportunity = {}) {
  return text(
    opportunity.opportunityId ||
      opportunity.id ||
      opportunity.discoveryId ||
      opportunity.gdiOpportunityId,
  );
}

function hasVerifiedNamedPerson(opportunity = {}, opts = {}) {
  if (opts.hasVerifiedNamedPerson === true) return true;
  if (opts.hasVerifiedNamedPerson === false) return false;
  const name = text(
    opportunity.primaryContact?.name || opportunity.contactName || "",
  );
  if (!name) return false;
  if (!isPlausibleBuyerPersonName(name)) return false;
  // Org-route-only / chrome must not count as WHO-complete named person
  const klass = text(
    opportunity.primaryContact?.contactClass ||
      opportunity.primaryContact?.contactClassification ||
      opportunity.primaryContact?.personKind ||
      "",
  );
  if (/ORGANIZATIONAL_ROUTE|GENERAL_BUSINESS|UNVERIFIED/i.test(klass)) {
    return false;
  }
  return true;
}

function demandVerified(opportunity = {}, opts = {}) {
  if (opts.demandVerified === true) return true;
  if (opts.demandVerified === false) return false;
  if (opportunity.demandValidated === true) return true;
  if (opportunity.demandEvidenceVerified === true) return true;
  if (/PASS|VERIFIED|VALIDATED/i.test(String(opportunity.demandValidationStatus || ""))) {
    return true;
  }
  // Conservative default for live: require explicit opt. Tests set demandVerified.
  return false;
}

function allowlisted(opportunity = {}, opts = {}) {
  const id = opportunityIdOf(opportunity);
  const list = Array.isArray(opts.exaAgentContactOpportunityAllowlist)
    ? opts.exaAgentContactOpportunityAllowlist.map((x) => text(x))
    : [];
  if (!list.length) return false;
  if (!id) return false;
  return list.includes(id);
}

function remainingBudgetUsd(opts = {}) {
  if (opts.exaAgentContactRemainingBudgetUsd != null) {
    return Number(opts.exaAgentContactRemainingBudgetUsd);
  }
  const ceiling = Number(opts.exaAgentContactAggregateCeilingUsd);
  const spent = Number(opts.exaAgentContactSpentUsd || 0);
  if (!Number.isFinite(ceiling)) return null;
  return ceiling - (Number.isFinite(spent) ? spent : 0);
}

/**
 * Pure eligibility gate — no provider I/O.
 */
export function evaluateExaAgentContactEscalationEligibility({
  opportunity = {},
  ladderExhausted = false,
  ladderTruncatedByMaxStrategies = false,
  opts = {},
} = {}) {
  const reasons = [];
  const whoVerified = hasVerifiedNamedPerson(opportunity, opts);

  if (opts.authorizeExaAgentContactEscalation !== true) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.FLAG_OFF,
      whoVerified,
    };
  }

  // Missing email alone must never trigger when WHO already complete
  if (
    whoVerified &&
    opts.triggerBecauseMissingEmail === true &&
    opts.authorizeExaAgentSecondaryOnly !== true
  ) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.MISSING_EMAIL_ONLY,
      whoVerified,
    };
  }

  if (whoVerified && opts.authorizeExaAgentSecondaryOnly !== true) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.WHO_ALREADY_VERIFIED,
      whoVerified,
    };
  }

  if (
    whoVerified &&
    opts.authorizeExaAgentSecondaryOnly === true &&
    opts.authorizeExaAgentContactEscalation === true
  ) {
    // Secondary-only path still needs explicit secondary auth (already true here)
  } else if (
    !whoVerified &&
    opts.seekSecondaryOnly === true &&
    opts.authorizeExaAgentSecondaryOnly !== true
  ) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.SECONDARY_ONLY_REFUSED,
      whoVerified,
    };
  }

  // Genuine full-ladder exhaustion OR explicitly authorized truncated-cap escalation.
  // A maxStrategies cap alone must never be treated as genuine exhaustion.
  const truncated =
    ladderTruncatedByMaxStrategies === true ||
    opts.ladderTruncatedByMaxStrategies === true;
  const truncatedAuthorized =
    opts.authorizeExaAgentTruncatedLadderEscalation === true;
  const genuinelyExhausted =
    ladderExhausted === true || opts.forceLadderExhausted === true;
  const ladderOk =
    genuinelyExhausted || (truncated && truncatedAuthorized);

  if (!ladderOk) {
    if (truncated && !truncatedAuthorized) {
      return {
        eligible: false,
        reason: EXA_AGENT_CONTACT_ESCALATION_REASON.LADDER_TRUNCATED_WITHOUT_AUTH,
        whoVerified,
        ladderTruncatedByMaxStrategies: true,
      };
    }
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.LADDER_NOT_EXHAUSTED,
      whoVerified,
    };
  }

  if (!text(opportunity.organizationName)) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.NO_ORGANIZATION,
      whoVerified,
    };
  }

  if (!demandVerified(opportunity, opts)) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.DEMAND_NOT_VERIFIED,
      whoVerified,
    };
  }

  if (!allowlisted(opportunity, opts)) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.NOT_ALLOWLISTED,
      whoVerified,
    };
  }

  const remaining = remainingBudgetUsd(opts);
  if (remaining == null || !Number.isFinite(remaining)) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.BUDGET_MISSING,
      whoVerified,
    };
  }
  const perOppCeiling = Number(opts.exaAgentContactPerOpportunityCeilingUsd ?? MEDIUM_USD);
  const need = Math.min(MEDIUM_USD, perOppCeiling);
  if (remaining + 1e-9 < need) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.BUDGET_INSUFFICIENT,
      whoVerified,
      remainingUsd: remaining,
      requiredUsd: need,
    };
  }

  const prior = opportunity.exaAgentContactEscalation || {};
  if (
    prior.executed === true &&
    prior.status === "completed" &&
    opts.allowRepeatEscalation !== true
  ) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.ALREADY_ESCALATED,
      whoVerified,
    };
  }
  if (opts.skipDuplicatePaidResearch === true && prior.attempted === true) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.RESEARCHER_IDENTITY_SKIP,
      whoVerified,
    };
  }

  const effort = String(opts.exaAgentContactEffort || EXA_AGENT_EFFORT.MEDIUM).toLowerCase();
  if (effort !== "medium") {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.EFFORT_REFUSED,
      whoVerified,
      effort,
    };
  }

  // Live provider dual-auth: contact flag already true; require live unless mock-only test
  const liveOk = isExaAgentLiveAuthorized({
    authorizeExaAgentLive: opts.authorizeExaAgentLive === true,
  });
  if (!liveOk && opts.allowMockExaAgentContact !== true) {
    return {
      eligible: false,
      reason: EXA_AGENT_CONTACT_ESCALATION_REASON.LIVE_PROVIDER_OFF,
      whoVerified,
    };
  }

  return {
    eligible: true,
    reason: EXA_AGENT_CONTACT_ESCALATION_REASON.ELIGIBLE,
    whoVerified,
    remainingUsd: remaining,
    requiredUsd: need,
    liveAuthorized: liveOk,
    reasons,
  };
}

/**
 * Classify Agent candidate after verification gates.
 * Seniority alone ≠ DIRECT_BUYER without lodging/meetings/housing evidence.
 */
export function classifyVerifiedAgentCandidate(person = {}, opts = {}) {
  const name = text(person.name || person.fullName);
  if (!isPlausibleBuyerPersonName(name)) {
    return {
      type: AGENT_CANDIDATE_TYPE.REJECTED,
      reason: "JUNK_OR_IMPLAUSIBLE_NAME",
    };
  }
  const org = text(opts.organizationName || "");
  const employerOk =
    employerAffiliationPlausible(person, org) ||
    (Array.isArray(opts.orgAliases) &&
      opts.orgAliases.some((a) =>
        `${person.currentOrganization || ""} ${person.organization || ""} ${person.relevanceEvidence || ""}`
          .toLowerCase()
          .includes(String(a).toLowerCase()),
      ));
  if (!employerOk) {
    return {
      type: AGENT_CANDIDATE_TYPE.REJECTED,
      reason: "EMPLOYER_NOT_VERIFIED",
    };
  }
  const urls = [
    person.sourceUrl,
    ...(Array.isArray(person.sourceUrls) ? person.sourceUrls : []),
  ]
    .map((u) => text(u))
    .filter((u) => /^https?:\/\//i.test(u));
  if (!urls.length) {
    return {
      type: AGENT_CANDIDATE_TYPE.REJECTED,
      reason: "NO_SOURCE_URL",
    };
  }
  // Homepage-only provenance cannot finalize WHO
  const onlyHomepage = urls.every((u) => {
    try {
      const p = new URL(u).pathname.replace(/\/+$/, "") || "/";
      return p === "/" || p === "";
    } catch {
      return false;
    }
  });
  if (onlyHomepage) {
    return {
      type: AGENT_CANDIDATE_TYPE.REJECTED,
      reason: "HOMEPAGE_ONLY_PROVENANCE",
    };
  }

  const blob = [
    person.title,
    person.role,
    person.relevanceEvidence,
    person.relationshipToOpportunity,
    person.buyerClassification,
  ]
    .filter(Boolean)
    .join(" ");

  const lodgingNegated =
    /lodging authority not|without (?:evidenced )?lodging|hotel.?sourcing (?:authority )?not|not evidenced/i.test(
      blob,
    );
  const lodgingOrMeetings =
    !lodgingNegated &&
    /hotel|housing|lodging|accommodation|meetings|events|conference|program|administrator|operations|partnership|sourcing|planner|local arrangements/i.test(
      blob,
    );
  const leadershipTitle =
    /executive director|president|ceo|board (?:member|of directors)|chair(?:man|woman|person)?\b/i.test(
      blob,
    );
  const adminOps =
    !lodgingNegated &&
    /administrator|secretary|treasurer|events? manager|conference director|local arrangements|housing (?:chair|manager|coordinator)|lodging (?:chair|manager|coordinator)/i.test(
      blob,
    );

  // Organizational seniority ≠ direct hotel-sourcing authority
  if (leadershipTitle && !lodgingOrMeetings && !adminOps) {
    return {
      type: AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE,
      reason: "SENIORITY_WITHOUT_LODGING_EVIDENCE_NOT_DIRECT_BUYER",
      sourceUrls: urls,
    };
  }
  if (adminOps || (lodgingOrMeetings && /director|manager|coordinator|administrator/i.test(blob))) {
    return {
      type: AGENT_CANDIDATE_TYPE.DIRECT_BUYER,
      reason: "EVIDENCED_OPERATIONAL_OR_MEETINGS_RESPONSIBILITY",
      sourceUrls: urls,
    };
  }
  if (lodgingOrMeetings || /committee|officer|coordinator|strategist/i.test(blob)) {
    return {
      type: AGENT_CANDIDATE_TYPE.VIABLE_ALTERNATIVE,
      reason: "CREDIBLE_ALTERNATIVE_ROUTE",
      sourceUrls: urls,
    };
  }
  return {
    type: AGENT_CANDIDATE_TYPE.RELEVANT_ORGANIZATIONAL_CONTACT,
    reason: "VERIFIED_PERSON_COMMERCIAL_PATHWAY_NOT_DEMONSTRATED",
    sourceUrls: urls,
  };
}

export function verifyAndClassifyAgentCandidates(rawCandidates = [], opts = {}) {
  const accepted = [];
  const rejected = [];
  for (const c of rawCandidates || []) {
    const classified = classifyVerifiedAgentCandidate(c, opts);
    if (classified.type === AGENT_CANDIDATE_TYPE.REJECTED) {
      rejected.push({
        name: c.name || null,
        reason: classified.reason,
        sourceUrl: c.sourceUrl || null,
      });
      continue;
    }
    accepted.push({
      ...c,
      name: text(c.name),
      sourceUrl: c.sourceUrl || classified.sourceUrls?.[0] || null,
      sourceUrls: classified.sourceUrls || c.sourceUrls || [],
      agentCandidateType: classified.type,
      agentClassificationReason: classified.reason,
      discoveryMethod: "EXA_AGENT_CONTACT_ESCALATION",
      personKind:
        classified.type === AGENT_CANDIDATE_TYPE.DIRECT_BUYER
          ? "DIRECT_BUYER"
          : classified.type === AGENT_CANDIDATE_TYPE.SECONDARY_CONTACT
            ? "SECONDARY_CONTACT"
            : "VERIFIED_VIABLE_ALTERNATIVE",
    });
  }
  return { accepted, rejected };
}

/**
 * Execute escalation when eligible. Fail-closed on errors/timeouts.
 */
export async function runExaAgentContactEscalation(opportunity = {}, opts = {}) {
  const eligibility = evaluateExaAgentContactEscalationEligibility({
    opportunity,
    ladderExhausted: opts.ladderExhausted === true,
    ladderTruncatedByMaxStrategies:
      opts.ladderTruncatedByMaxStrategies === true,
    opts,
  });

  const baseMeta = {
    version: GDI_EXA_AGENT_CONTACT_ESCALATION_V1,
    contractId: EXA_AGENT_CONTACT_INTELLIGENCE_CONTRACT_V1,
    at: now(),
    opportunityId: opportunityIdOf(opportunity),
    eligibility,
    attempted: false,
    executed: false,
    providerCalled: false,
    costUsd: 0,
    effort: "medium",
  };

  if (!eligibility.eligible) {
    return {
      ok: false,
      skipped: true,
      reason: eligibility.reason,
      meta: baseMeta,
      candidates: [],
      rejected: [],
      opportunity,
    };
  }

  const quote = quoteExaAgentRun({ effort: EXA_AGENT_EFFORT.MEDIUM });
  const payload = buildExaAgentContactCreatePayload({
    organizationName: opportunity.organizationName,
    eventTitle: opportunity.title || opportunity.eventName || "",
    hotelName: opportunity.hotelName || "",
    destination: opportunity.destination || opportunity.city || "",
    evidenceUrls: [
      opportunity.sourceUrl,
      opportunity.packagedSourceUrl,
      ...(Array.isArray(opportunity.evidenceUrls) ? opportunity.evidenceUrls : []),
    ].filter((u) => /^https?:\/\//i.test(String(u || ""))),
    hotelFitNote: opportunity.hotelFitNote || opportunity.hotelFit || "",
    effort: EXA_AGENT_EFFORT.MEDIUM,
  });

  // Never seed with prior benchmark person names
  const forbidden = Array.isArray(opts.forbiddenPersonSeeds)
    ? opts.forbiddenPersonSeeds
    : [];
  for (const name of forbidden) {
    if (name && payload.query.toLowerCase().includes(String(name).toLowerCase())) {
      return {
        ok: false,
        skipped: true,
        reason: "FORBIDDEN_SEED_IN_QUERY",
        meta: { ...baseMeta, attempted: false },
        candidates: [],
        rejected: [],
        opportunity,
      };
    }
  }

  const liveOk = isExaAgentLiveAuthorized({
    authorizeExaAgentLive: opts.authorizeExaAgentLive === true,
  });

  let agent;
  try {
    agent = await runExaAgentResearch({
      mode: "discovery",
      effort: EXA_AGENT_EFFORT.MEDIUM,
      authorizeExaAgentLive: opts.authorizeExaAgentLive === true,
      createPayloadOverride: payload,
      contractIdOverride: EXA_AGENT_CONTACT_INTELLIGENCE_CONTRACT_V1,
      mockStructured: opts.mockExaAgentContactStructured || null,
      mockStatus: opts.mockExaAgentContactStatus || "completed",
      forceMockOnly: opts.forceMockOnly === true,
      pollUntilFinished: opts.pollUntilFinished !== false,
      pollIntervalMs: opts.pollIntervalMs || 4000,
      maxPolls: opts.maxPolls || 60,
      fetchImpl: opts.fetchImpl || null,
    });
  } catch (err) {
    const meta = {
      ...baseMeta,
      attempted: true,
      executed: false,
      providerCalled: liveOk,
      status: "error",
      error: String(err?.message || err),
      quote,
    };
    return {
      ok: false,
      skipped: false,
      reason: "AGENT_ERROR_SAFE_CONTINUE",
      meta,
      candidates: [],
      rejected: [],
      opportunity: {
        ...opportunity,
        exaAgentContactEscalation: meta,
      },
    };
  }

  const costUsd = Number(agent?.costDollars?.total);
  const meta = {
    ...baseMeta,
    attempted: true,
    executed: agent?.ok === true,
    providerCalled: liveOk && opts.allowMockExaAgentContact !== true,
    status: agent?.status || null,
    runId: agent?.runId || null,
    costUsd: Number.isFinite(costUsd) ? costUsd : liveOk ? MEDIUM_USD : 0,
    quote,
    stopReason: agent?.stopReason || null,
  };

  if (!agent?.ok) {
    return {
      ok: false,
      skipped: false,
      reason: agent?.status || "AGENT_NOT_COMPLETED",
      meta,
      candidates: [],
      rejected: [],
      opportunity: {
        ...opportunity,
        exaAgentContactEscalation: meta,
      },
    };
  }

  const mapped = mapExaAgentContactPeopleToCandidates(agent.structured || {}, {
    organizationName: opportunity.organizationName,
  });
  const verified = verifyAndClassifyAgentCandidates(mapped, {
    organizationName: opportunity.organizationName,
    orgAliases: opts.orgAliases,
  });

  // Merge candidates into namedStakeholders for downstream harvest/ranking
  const nextStakeholders = [
    ...(opportunity.namedStakeholders || []),
    ...verified.accepted,
  ];

  return {
    ok: true,
    skipped: false,
    reason: "AGENT_CANDIDATES_MAPPED",
    meta: {
      ...meta,
      structuredPeople: (agent.structured?.people || []).length,
      acceptedCount: verified.accepted.length,
      rejectedCount: verified.rejected.length,
      unresolvedQuestions: agent.structured?.unresolvedQuestions || [],
    },
    candidates: verified.accepted,
    rejected: verified.rejected,
    opportunity: {
      ...opportunity,
      namedStakeholders: nextStakeholders,
      exaAgentContactEscalation: {
        ...meta,
        acceptedCount: verified.accepted.length,
        rejectedCount: verified.rejected.length,
        acceptedNames: verified.accepted.map((p) => p.name),
        rejected: verified.rejected,
      },
    },
  };
}
