/**
 * Original Full E2E execution restore helpers V1.
 *
 * Reconnects existing ORS supervisor, two-contact investigation,
 * commercial-eligibility-contract-v1, and need-date indicator into the
 * canonical Full E2E chain. Does not invent new engines or agent families.
 * Does not call Surfe/PDL. Paid Exa remains gated by caller auth.
 */

import {
  runDeepResearchOnExistingGdiOpportunity,
  createFixtureResearcher,
  mergeDeepResearchIntoOpportunity,
} from "../research/deep-research-gdi-integration-v1.js";
import {
  evaluateDeepResearchEligibility,
  ELIGIBILITY_DECISION,
} from "../research/deep-research-eligibility-v1.js";
import { classifyContactEligibility } from "../contact-eligibility-standard-v1.js";
import {
  extractPublicContactIntelligenceForOrs,
} from "../research/commercial-spine-restoration-v41.js";
import { applyScalingTwoContactStandard } from "../scaling-two-contact-standard-v1.js";
import {
  enrichOpportunityWithSecondaryNamedContact,
  evaluateSecondaryContactOpportunity,
} from "../secondary-named-contact-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";
import { attachUniversalOpportunityNeedDateIndicator } from "../need-date-demand/gdi-universal-opportunity-need-date-indicator-v2.js";
import {
  resolveExaAgentRoutingForSegment,
  buildExaOperationRoutingMetadata,
  EXA_AGENT_FAMILY,
} from "../research/exa-agent/gdi-segment-aware-exa-agent-routing-v1.js";
import { executeExaAgentOrsStrategy } from "../research/exa-agent/exa-agent-ors-strategy-v1.js";
import { executeExaEventsAgentOrsStrategy } from "../research/exa-agent/exa-agent-events-ors-strategy-v1.js";
import { executeExaPreferredLodgingOrsStrategy } from "../research/exa-agent/exa-agent-preferred-lodging-ors-strategy-v1.js";
import { EXA_AGENT_EFFORT } from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import { assessDeepResearchCompletion } from "./gdi-portfolio-cert-v1-3-evidence-integrity-actionability-v1.js";
import { resolveOpportunityContact } from "../weekly-contact-resolution-v1-2.js";
import {
  ensureSegmentDiscoveryThesisForEligibility,
} from "./gdi-segment-discovery-thesis-repair-v1.js";
import {
  ensureBuyerWhoResearchExecuted,
  hasRoleRelevantDecisionMaker,
} from "../research/autonomous-deep-research-recovery-v1.js";
import { runPublicBuyerIntelligencePass } from "../research/commercial-depth-buyer-intelligence-v1.js";
import { drainRelatedEntityFollowUpsThroughOrs } from "../research/commercial-spine-restoration-v41.js";

export const GDI_ORIGINAL_FULL_E2E_EXECUTION_RESTORE_V1 =
  "gdi-original-full-e2e-execution-restore-v1";

/** Junk / non-person strings previously hydrated as primaryContact. */
export const GDI_JUNK_PERSON_NAME_RE =
  /^(organization contact desk|member\s*(select|log-?in)|website feedback|pennsylvania(\s*ave)?|place brand developers|belgium\s*tel|survey software|amsterdam(\s+the\s+netherlands)?|the netherlands|george grenada|reduction forum|developers|conference|congress|committee|contact research required|buyer not yet identified|role not yet verified)$/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function isPlausibleBuyerPersonName(name) {
  const n = text(name);
  if (!n || n.length < 5 || n.length > 80) return false;
  if (GDI_JUNK_PERSON_NAME_RE.test(n)) return false;
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length < 2) return false;
  return /^[A-Za-zÀ-ÖØ-öø-ÿ'`. -]+$/.test(n);
}

/**
 * runDeepResearchOnExistingGdiOpportunity returns { after: { opportunity }, buyerWhoFallback }.
 * Historical restore path read deepResult.opportunity (always undefined) and discarded enrichments.
 */
export function resolveOpportunityFromDeepResearchResult(
  deepResult = {},
  fallback = {},
) {
  return (
    deepResult?.after?.opportunity ||
    deepResult?.opportunity ||
    deepResult?.canonicalGdiOpportunityAfter ||
    fallback
  );
}

export function attachBuyerWhoFallbackToOpportunity(opportunity = {}, deepResult = {}) {
  const who =
    deepResult?.buyerWhoFallback ||
    deepResult?.after?.buyerWhoFallback ||
    opportunity?.buyerWhoFallback ||
    null;
  if (!who) {
    return {
      ...opportunity,
      buyerWhoFallback: opportunity.buyerWhoFallback || null,
    };
  }
  return {
    ...opportunity,
    buyerWhoFallback: {
      executed: who.executed === true,
      reason: who.reason || null,
      buyerUrlsAttempted: who.buyerUrlsAttempted || [],
      buyerQueriesAttempted: who.buyerQueriesAttempted || [],
      at: now(),
      version: who.version || null,
    },
    deepResearchEnrichment: {
      ...(opportunity.deepResearchEnrichment &&
      typeof opportunity.deepResearchEnrichment === "object"
        ? opportunity.deepResearchEnrichment
        : {}),
      autonomousBuyerWhoFallback: who.executed === true,
      buyerWhoReason: who.reason || null,
    },
  };
}

function now() {
  return new Date().toISOString();
}

function peopleFromExaContactFields(opportunity = {}) {
  const out = [];
  const push = (c) => {
    if (!c) return;
    const name = text(c.name || c.personName || "");
    if (!name) return;
    out.push({
      name,
      role: text(c.role || c.title || c.currentTitle || ""),
      title: text(c.title || c.currentTitle || c.role || ""),
      organizationName: text(
        c.organization || c.company || opportunity.organizationName || ""
      ),
      sourceUrl: c.sourceUrl || (c.supportingSourceUrls || [])[0] || null,
      commercialClass: c.commercialClass || null,
      contactRelevanceLabel: c.contactRelevanceLabel || null,
    });
  };
  push(opportunity.primaryContact);
  push(opportunity.secondaryContact);
  if (opportunity.contactName) {
    push({
      name: opportunity.contactName,
      title: opportunity.contactRole || opportunity.contactTitle || "",
      role: opportunity.contactRole || opportunity.contactTitle || "",
      sourceUrl: opportunity.contactUrl || opportunity.organizationRouteUrl || null,
    });
  }
  const sci = opportunity.scalingContactIntelligence || {};
  push(sci.primaryContact);
  push(sci.secondaryContact);
  for (const c of sci.additionalCandidates || []) push(c);
  return out;
}

/**
 * Classify existing names, then actively attempt a distinct secondary contact
 * from harvested/saved evidence (no Surfe, no invented emails).
 * Synchronous path — does not fetch URLs.
 */
function classifyPeopleIntoContacts(opportunity = {}, people = []) {
  const classified = [];
  for (const person of people) {
    if (!text(person.name)) continue;
    if (!isPlausibleBuyerPersonName(person.name)) continue;
    if (classified.some((c) => c.name === text(person.name))) continue;
    const result = classifyContactEligibility(person, opportunity);
    classified.push({
      name: text(person.name),
      role: text(person.role || person.title || ""),
      class: result.class,
      eligible: result.eligible === true,
      salesReadyContactOk: result.salesReadyContactOk === true,
      reason: result.reason,
      sourceUrl: person.sourceUrl || null,
      buyerVersusIntermediary:
        result.class === "DIRECT_BUYER"
          ? "DIRECT_BUYER"
          : result.class === "RELEVANT_COMMERCIAL_ENTRY_POINT"
            ? "ALTERNATIVE_ENTRY_POINT"
            : "UNSUPPORTED_OR_UNCLEAR",
    });
  }
  return classified;
}

function harvestPeopleFromOpportunity(opportunity = {}) {
  const contactList = Array.isArray(opportunity.contacts)
    ? opportunity.contacts
    : [];
  const people = [
    ...peopleFromExaContactFields(opportunity),
    ...(Array.isArray(opportunity.namedStakeholders)
      ? opportunity.namedStakeholders
      : []),
    ...(Array.isArray(opportunity.namedStakeholdersFound)
      ? opportunity.namedStakeholdersFound
      : []),
    ...contactList.map((c) => ({
      name: c?.name,
      role: c?.title || c?.role,
      title: c?.title || c?.role,
      organizationName: c?.organization || opportunity.organizationName,
      sourceUrl: c?.sourceUrl,
    })),
  ];

  for (const e of opportunity.evidence || []) {
    if (!e?.statement) continue;
    try {
      const ci = extractPublicContactIntelligenceForOrs({
        textContent: e.statement,
        url: e.url,
        organizationName: opportunity.organizationName || "",
      });
      for (const p of ci.namedStakeholdersFound || []) {
        if (!text(p.name)) continue;
        people.push({
          name: p.name,
          role: p.title || p.role,
          title: p.title || p.role,
          organizationName: opportunity.organizationName,
          sourceUrl: e.url || null,
        });
      }
    } catch {
      /* ignore extract failures */
    }
  }
  return people;
}

function finalizeContactPass(opportunity, classified, secondaryEval, extras = {}) {
  let next = { ...opportunity };
  // Strip junk primaries left by weak extraction before ranking/attach.
  if (
    text(next.primaryContact?.name) &&
    !isPlausibleBuyerPersonName(next.primaryContact.name)
  ) {
    next.primaryContact = {
      ...(next.primaryContact && typeof next.primaryContact === "object"
        ? next.primaryContact
        : {}),
      name: null,
      role: null,
      title: null,
    };
    if (!isPlausibleBuyerPersonName(next.contactName)) next.contactName = null;
  }
  if (classified.length) {
    next.namedStakeholders = classified.map((c) => ({
      name: c.name,
      title: c.role,
      role: c.role,
      decisionMakerClass: c.salesReadyContactOk
        ? "VERIFIED_DECISION_MAKER"
        : c.eligible
          ? "POTENTIAL_DECISION_MAKER"
          : "RESEARCH_CANDIDATE",
      sourceUrl: c.sourceUrl,
      buyerVersusIntermediary: c.buyerVersusIntermediary || null,
    }));
  }

  const beforeSecondary = text(next.secondaryContact?.name || "");
  next = applyScalingTwoContactStandard(next, {
    force: true,
    allowSavedEvidence: true,
    skipSecondaryAttach: false,
  });
  const evalSecondary =
    secondaryEval ||
    evaluateSecondaryContactOpportunity(next, {
      force: !beforeSecondary,
      allowSavedEvidence: true,
    });
  if (evalSecondary.enrichedOpportunity) {
    next = evalSecondary.enrichedOpportunity;
  } else if (!text(next.secondaryContact?.name)) {
    next = enrichOpportunityWithSecondaryNamedContact(next, {
      force: true,
      allowSavedEvidence: true,
    });
  }

  // Secondary may also be junk — clear it.
  if (
    text(next.secondaryContact?.name) &&
    !isPlausibleBuyerPersonName(next.secondaryContact.name)
  ) {
    next.secondaryContact = null;
  }

  const eligibleNamed = classified.filter(
    (c) => c.eligible && isPlausibleBuyerPersonName(c.name),
  );
  const afterSecondary = text(next.secondaryContact?.name || "");
  // Ensure a named commercial entry point is hydrated onto primaryContact when
  // evidence classification found an eligible person but attach helpers left it blank.
  if (
    !isPlausibleBuyerPersonName(next.primaryContact?.name || next.contactName) &&
    eligibleNamed[0]?.name
  ) {
    next.primaryContact = {
      ...(next.primaryContact && typeof next.primaryContact === "object"
        ? next.primaryContact
        : {}),
      name: text(eligibleNamed[0].name),
      role: text(eligibleNamed[0].role || eligibleNamed[0].title) || null,
      title: text(eligibleNamed[0].title || eligibleNamed[0].role) || null,
      sourceUrl: eligibleNamed[0].sourceUrl || null,
    };
    if (!text(next.contactName)) next.contactName = text(eligibleNamed[0].name);
  }
  const primaryName = isPlausibleBuyerPersonName(
    next.primaryContact?.name || next.contactName || eligibleNamed[0]?.name || "",
  )
    ? text(
        next.primaryContact?.name ||
          next.contactName ||
          eligibleNamed[0]?.name ||
          "",
      )
    : "";
  if (!primaryName) {
    if (next.primaryContact) {
      next.primaryContact = {
        ...next.primaryContact,
        name: null,
        role: null,
        title: null,
      };
    }
    next.contactName = null;
  }
  const secondContactAttempted = true;
  const secondContactFound = Boolean(
    afterSecondary && afterSecondary.toLowerCase() !== primaryName.toLowerCase()
  );

  const contactGaps = [];
  if (!primaryName) {
    contactGaps.push(
      "No verified named commercially relevant primary contact yet."
    );
  }
  if (primaryName && !secondContactFound) {
    contactGaps.push(
      evalSecondary.disposition === "NEEDS_ADDITIONAL_PUBLIC_RESEARCH"
        ? "Second relevant named contact not yet evidenced — additional public research justified."
        : "Second relevant named contact not available from current evidence (not an eligibility failure)."
    );
  }

  return {
    opportunity: next,
    pass: {
      version: GDI_ORIGINAL_FULL_E2E_EXECUTION_RESTORE_V1,
      contactsInvestigated: classified.length,
      verifiedEligible: eligibleNamed.length,
      salesReadyContacts: classified.filter((c) => c.salesReadyContactOk).length,
      classifiedContacts: classified,
      contactResearchComplete: Boolean(primaryName),
      paidContactProvidersUsed: false,
      secondContactAttempted,
      secondContactFound,
      secondContactDisposition: evalSecondary.disposition || null,
      contactGaps,
      scalingContactCoverage: next.scalingContactCoverage || null,
      activelyInvestigatedSecondContact: true,
      officialSourceFollowUp: extras.officialSourceFollowUp || null,
      followUpRetrievalPerformed: extras.followUpRetrievalPerformed === true,
      researchActions: extras.researchActions || [],
    },
  };
}

export function runRestoredContactInvestigationPass(opportunity = {}) {
  const classified = classifyPeopleIntoContacts(
    opportunity,
    harvestPeopleFromOpportunity(opportunity)
  );
  return finalizeContactPass(opportunity, classified, null, {
    followUpRetrievalPerformed: false,
    researchActions: [
      {
        action: "CLASSIFY_EXISTING_EVIDENCE_CONTACTS",
        performed: true,
        count: classified.length,
      },
      {
        action: "SECOND_CONTACT_FROM_SAVED_EVIDENCE",
        performed: true,
      },
    ],
  });
}

/**
 * Active contact investigation: existing evidence + official-source retrieval
 * via resolveOpportunityContact when primary/secondary gaps remain.
 * No Surfe. No invented emails.
 */
export async function runRestoredContactInvestigationPassAsync(
  opportunity = {},
  opts = {}
) {
  const researchActions = [
    {
      action: "REVIEW_EXISTING_CONTACT_EVIDENCE",
      performed: true,
      at: now(),
    },
  ];
  let working = { ...opportunity };
  let followUpRetrievalPerformed = false;
  let officialSourceFollowUp = null;

  let classified = classifyPeopleIntoContacts(
    working,
    harvestPeopleFromOpportunity(working)
  );
  researchActions.push({
    action: "CLASSIFY_EXISTING_EVIDENCE_CONTACTS",
    performed: true,
    count: classified.length,
  });

  const hasPrimary =
    classified.some((c) => c.eligible) ||
    text(working.primaryContact?.name || working.contactName);
  const allowOfficialFollowUp = opts.allowOfficialSourceFollowUp !== false;

  if (allowOfficialFollowUp && (!hasPrimary || classified.length < 2)) {
    researchActions.push({
      action: "IDENTIFY_CONTACT_RESEARCH_GAPS",
      performed: true,
      gaps: [
        !hasPrimary ? "primary_named_buyer_missing" : null,
        classified.length < 2 ? "second_named_contact_missing" : null,
      ].filter(Boolean),
    });
    try {
      const resolved = await resolveOpportunityContact(working, {
        maxFetches: opts.maxOfficialFetches ?? 4,
        allowSurfe: false,
        authorizeSurfe: false,
      });
      followUpRetrievalPerformed = true;
      working = resolved.opportunity || working;
      officialSourceFollowUp = {
        performed: true,
        fetchesUsed: Number(resolved.fetchesUsed || 0),
        blankReason: resolved.blankReason || null,
        status: resolved.status || null,
        provider: "official_source_http",
        paidSurfe: false,
      };
      researchActions.push({
        action: "OFFICIAL_SOURCE_CONTACT_RETRIEVAL",
        performed: true,
        fetchesUsed: officialSourceFollowUp.fetchesUsed,
        blankReason: officialSourceFollowUp.blankReason,
      });
      classified = classifyPeopleIntoContacts(
        working,
        harvestPeopleFromOpportunity(working)
      );
      researchActions.push({
        action: "RECLASSIFY_AFTER_OFFICIAL_RETRIEVAL",
        performed: true,
        count: classified.length,
      });
    } catch (err) {
      officialSourceFollowUp = {
        performed: false,
        error: String(err?.message || err),
      };
      researchActions.push({
        action: "OFFICIAL_SOURCE_CONTACT_RETRIEVAL",
        performed: false,
        error: String(err?.message || err),
      });
    }
  } else {
    researchActions.push({
      action: "OFFICIAL_SOURCE_CONTACT_RETRIEVAL",
      performed: false,
      reason: hasPrimary
        ? "PRIMARY_ALREADY_PRESENT_FROM_PRIOR_RESEARCH"
        : "OFFICIAL_FOLLOWUP_DISABLED",
    });
  }

  researchActions.push({
    action: "SECOND_CONTACT_INVESTIGATION",
    performed: true,
  });

  return finalizeContactPass(working, classified, null, {
    followUpRetrievalPerformed,
    officialSourceFollowUp,
    researchActions,
  });
}

/**
 * When the demand gate already reached CONFIRMED / RELIABLE_INFERRED for rooms
 * (or BOTH), keep guestroomDemandConfidence aligned so scorecards / drawer
 * exports do not under-report demand that eligibility already accepted.
 * Does not invent CONFIRMED from SPECULATIVE. Does not weaken contact gates.
 */
export function hydrateGuestroomConfidenceFromDemandThesis(
  opportunity = {},
  demandThesis = {},
) {
  const next = { ...opportunity };
  const standard = String(demandThesis?.thesisStandard || "");
  const mayQualify = demandThesis?.mayQualifySalesReadyDemandGate === true;
  if (!mayQualify) return next;

  const conf =
    standard === "CONFIRMED_DEMAND"
      ? "CONFIRMED"
      : standard === "RELIABLE_INFERRED_DEMAND"
        ? "RELIABLE_INFERRED"
        : null;
  if (!conf) return next;

  const demandType = String(
    next.commercialDemandType || next.demandType || "",
  ).toUpperCase();
  const roomsRelevant =
    /GUEST|BOTH|ROOM|CORPORATE|PROJECT|RELOCATION|SCALING|LODGING|PREFERRED/i.test(
      demandType,
    ) ||
    Boolean(next.guestroomThesis) ||
    Boolean(next.hotelDemandHypotheses?.guestroomHypothesis);

  if (roomsRelevant && !next.guestroomDemandConfidence) {
    next.guestroomDemandConfidence = conf;
    next.demandThesis =
      next.demandThesis ||
      (standard === "CONFIRMED_DEMAND"
        ? "CONFIRMED_DEMAND"
        : "RELIABLE_INFERRED_DEMAND");
  }
  return next;
}

export function runRestoredCommercialEligibilityPass(opportunity = {}, opts = {}) {
  const result = evaluateSharedCommercialEligibility(opportunity, {
    hotel: {
      hotelId: opportunity.hotelId,
      displayName: opportunity.hotelName || opts.hotelName || "",
    },
    ...opts,
  });
  const hydrated = hydrateGuestroomConfidenceFromDemandThesis(
    opportunity,
    result?.demandThesisStandard || result?.demand || {},
  );
  return {
    opportunity: {
      ...hydrated,
      commercialEligibilityContractV1: result,
      commercialEligibilityStatus: result?.status || null,
      commercialEligibilityAt: now(),
    },
    result,
  };
}

export function runRestoredNeedDatePass(opportunity = {}, opts = {}) {
  const hotelNeedDateConfig = opts.hotelNeedDateConfig || {
    hotelId: opportunity.hotelId || null,
    status: "NOT_ESTABLISHED",
    confirmedPeriodCount: 0,
    estimatedPeriodCount: 0,
  };
  const next = attachUniversalOpportunityNeedDateIndicator(opportunity, {
    hotelNeedDateConfig,
  });
  return {
    opportunity: {
      ...next,
      needDateIntegrationVersion: GDI_ORIGINAL_FULL_E2E_EXECUTION_RESTORE_V1,
      needDateIntegrationAt: now(),
    },
    hotelNeedDateStatus:
      next.needDateIndicator?.hotelNeedDateStatus || hotelNeedDateConfig.status,
  };
}

/**
 * Optional single Exa deep seed (future live only). Never invents spend in tests.
 * Skips when opportunity already has paid deep stamp (idempotent).
 */
export async function maybeSeedExaDeepResearch(opportunity = {}, opts = {}) {
  const already =
    Boolean(opportunity.exaAgentDeepResearch) ||
    Boolean(opportunity.deepResearchJobId) ||
    opts.skipExaBecauseAlreadyPaid === true;
  if (already) {
    return {
      opportunity,
      seeded: false,
      reason: "ALREADY_HAS_DEEP_RESEARCH",
      costUsd: 0,
    };
  }
  if (opts.authorizeExaAgentLive !== true || opts.allowLiveExaDeepSeed !== true) {
    return {
      opportunity,
      seeded: false,
      reason: "LIVE_EXA_SEED_NOT_AUTHORIZED",
      costUsd: 0,
    };
  }

  const segmentId = opportunity.researchSegment || opportunity.segmentId || null;
  const routing = resolveExaAgentRoutingForSegment(segmentId, {
    stage: "DEEP_RESEARCH",
  });
  if (!routing.paidExaAllowed || !routing.deepResearch) {
    return {
      opportunity,
      seeded: false,
      reason: routing.blockReason || "AGENT_FAMILY_UNSUPPORTED",
      costUsd: 0,
      routing,
    };
  }

  const hotelCtx = {
    hotelId: opportunity.hotelId,
    displayName: opportunity.hotelName,
  };
  let exa;
  if (routing.agentFamily === EXA_AGENT_FAMILY.PREFERRED_LODGING) {
    // Dedicated Preferred Lodging executor — never Scaling/Events.
    exa = await executeExaPreferredLodgingOrsStrategy({
      strategyId: routing.deepResearch.strategyId,
      hotel: hotelCtx,
      opportunity,
      candidate: opportunity,
      authorizeExaAgentLive: true,
      effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
      segmentId: routing.segmentId || "preferred_lodging_partnerships",
      segmentLabel: routing.segmentLabel || "Preferred Lodging Partnerships",
    });
  } else if (routing.agentFamily === EXA_AGENT_FAMILY.ASSOCIATION_EVENTS) {
    exa = await executeExaEventsAgentOrsStrategy({
      strategyId: routing.deepResearch.strategyId,
      hotel: hotelCtx,
      opportunity,
      candidate: opportunity,
      authorizeExaAgentLive: true,
      effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
      segmentId: routing.segmentId,
      segmentLabel: routing.segmentLabel,
    });
  } else if (routing.agentFamily === EXA_AGENT_FAMILY.SCALING_COMPANIES) {
    exa = await executeExaAgentOrsStrategy({
      strategyId: routing.deepResearch.strategyId,
      hotel: hotelCtx,
      opportunity,
      authorizeExaAgentLive: true,
      effort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
      segmentId: routing.segmentId,
      segmentLabel: routing.segmentLabel,
    });
  } else {
    return {
      opportunity,
      seeded: false,
      reason: "NO_EXECUTABLE_AGENT_FAMILY_FOR_LIVE_SEED",
      costUsd: 0,
      routing,
    };
  }

  const runId =
    exa?.run?.runId ||
    exa?.run?.id ||
    exa?.providerRunId ||
    exa?.runId ||
    null;

  let next = opportunity;
  if (exa?.opportunity) next = { ...next, ...exa.opportunity };
  else if (exa?.validation?.sanitized) {
    const sanitized = exa.validation.sanitized;
    const fromSources = [
      ...(Array.isArray(sanitized.investigatedUrls)
        ? sanitized.investigatedUrls
        : []),
      ...(Array.isArray(sanitized.supportingSources)
        ? sanitized.supportingSources.map((x) => x?.url).filter(Boolean)
        : []),
      ...(Array.isArray(sanitized.claimToSourceMap)
        ? sanitized.claimToSourceMap.flatMap((c) =>
            Array.isArray(c?.sourceUrls) ? c.sourceUrls : [],
          )
        : []),
    ].filter((u) => /^https?:\/\//i.test(String(u || "")));
    const merged = mergeDeepResearchIntoOpportunity(next, {
      acceptedClaims: sanitized.acceptedClaims || [],
      investigatedUrls: fromSources,
      researchJobId: runId,
    });
    next = merged.opportunity || next;
  }
  // Always preserve Exa run provenance on the opportunity — ledger settle
  // must be able to read it even when mergeDeepResearchIntoOpportunity is skipped.
  if (runId) {
    next.researchJobId = next.researchJobId || runId;
    next.exaRunId = next.exaRunId || runId;
  }
  next.exaAgentRouting = buildExaOperationRoutingMetadata({
    hotel: hotelCtx,
    segmentId,
    stage: "DEEP_RESEARCH",
    extra: {
      providerRunId: runId,
      support: routing.support,
      rationale: routing.rationale,
      at: now(),
    },
  });
  // V1.3: provider run success ≠ investigation complete without evidentiary improvement.
  next.deepResearchCompletion = assessDeepResearchCompletion(next, {
    investigatedUrls: next.investigatedUrls || [],
    researchJobId: runId,
    providerRunId: runId,
    noImprovement: next.exaAgentDeepResearch?.evidenceImprovedBeyondSeed === false,
  });
  if (
    next.deepResearchCompletion &&
    next.deepResearchCompletion.complete !== true
  ) {
    next.deepResearchIncompleteReason = next.deepResearchCompletion.status;
  }

  return {
    opportunity: next,
    seeded: true,
    reason: "EXA_DEEP_SEEDED",
    costUsd: Number(exa?.run?.costDollars?.total || 0),
    providerRunId: runId,
    exa,
    routing,
  };
}

/**
 * Canonical restored deep path: eligibility → optional Exa seed → matureOpportunity ORS.
 */
export async function runRestoredOrsDeepResearchPass(
  opportunityInput = {},
  opts = {}
) {
  let opportunity = ensureSegmentDiscoveryThesisForEligibility({
    ...opportunityInput,
  });
  const stages = [];
  const remainingBudgetForStep = Number(opts.remainingBudgetUsd ?? 0);

  // Offline / already-paid ORS continuation must not be blocked by a $0 live
  // budget gate — eligibility still evaluates commercial signals; paid Exa seed
  // remains separately authorized.
  const offlineOrsContinuation =
    opts.forceOrsPaidBlocked === true ||
    opts.authorizePaidResearch !== true ||
    opts.allowLiveExaDeepSeed !== true;
  const eligibilityBudgetUsd = offlineOrsContinuation
    ? Math.max(remainingBudgetForStep, 0.5)
    : remainingBudgetForStep;

  const eligibility = evaluateDeepResearchEligibility(opportunity, {
    remainingBudgetUsd: eligibilityBudgetUsd,
  });
  stages.push({
    stage: "DEEP_RESEARCH_ELIGIBILITY",
    at: now(),
    decision: eligibility.decision,
    tier: eligibility.tier,
    eligible: eligibility.eligible === true,
    reasons: eligibility.reasons || [],
    eligibilityBudgetMode: offlineOrsContinuation
      ? "OFFLINE_ORS_CONTINUATION"
      : "LIVE_PAID_BUDGET",
  });

  if (eligibility.decision === ELIGIBILITY_DECISION.NOT_ELIGIBLE) {
    stages.push({
      stage: "DEEP_RESEARCH",
      skipped: true,
      reason: "NOT_ELIGIBLE",
      eligibilityReasons: eligibility.reasons || [],
      at: now(),
    });
    return {
      opportunity,
      deepResult: null,
      deepCostUsd: 0,
      stages,
      eligibility,
      orsInvoked: false,
    };
  }

  if (
    eligibility.decision === ELIGIBILITY_DECISION.DEFER_BUDGET_OR_PROVIDER &&
    !offlineOrsContinuation
  ) {
    stages.push({
      stage: "DEEP_RESEARCH",
      blocked: true,
      reason: "DEFER_BUDGET_OR_PROVIDER",
      at: now(),
    });
    return {
      opportunity,
      deepResult: null,
      deepCostUsd: 0,
      stages,
      eligibility,
      orsInvoked: false,
    };
  }

  let deepCostUsd = 0;
  const seed = await maybeSeedExaDeepResearch(opportunity, {
    authorizeExaAgentLive: opts.authorizeExaAgentLive === true,
    allowLiveExaDeepSeed: opts.allowLiveExaDeepSeed === true,
    skipExaBecauseAlreadyPaid: opts.skipExaBecauseAlreadyPaid === true,
    exaEffort: opts.exaEffort,
  });
  opportunity = seed.opportunity;
  deepCostUsd += Number(seed.costUsd || 0);
  if (seed.seeded) {
    stages.push({
      stage: "DEEP_RESEARCH_EXA_SEED",
      mode: "AUTHORIZED_PAID_EXA_AGENT_SEED",
      costUsd: seed.costUsd,
      agentFamily: seed.routing?.agentFamily || null,
      at: now(),
    });
  }

  const corpus =
    typeof opts.buildCorpus === "function"
      ? opts.buildCorpus(opportunity)
      : opts.corpus || {};

  const remainingAfterSeed = Math.max(
    0,
    remainingBudgetForStep - deepCostUsd,
  );
  // Authorized live Full E2E must not silently continue buyer-WHO on fixtures only.
  // ORS maturation may still be paidBlocked after seed (loop control), but the
  // researcher passed into deep research must expose execute() for buyer-WHO.
  const liveBuyerWhoAuthorized =
    opts.forceFixtureResearcher === true
      ? false
      : opts.authorizeExaAgentLive === true &&
        opts.allowLiveExaDeepSeed === true &&
        remainingAfterSeed > 0.05 &&
        opts.enableLiveBuyerWhoAfterSeed !== false;

  let researcher =
    opts.researcher ||
    (opts.liveResearcher?.execute ? opts.liveResearcher : null) ||
    (opts.buyerWhoResearcher?.execute ? opts.buyerWhoResearcher : null);

  let buyerWhoResearcherSource = "injected";
  if (!researcher?.execute) {
    if (liveBuyerWhoAuthorized) {
      // Dynamic import keeps offline/fixture unit paths free of live Exa wiring.
      const { createLiveDeepResearchResearcher } = await import(
        "../research/deep-research-gdi-live-certification-v1.js"
      );
      researcher = createLiveDeepResearchResearcher({
        ledgerPath: opts.buyerWhoLedgerPath || opts.ledgerPath || null,
      });
      buyerWhoResearcherSource = "createLiveDeepResearchResearcher";
      stages.push({
        stage: "BUYER_WHO_LIVE_RESEARCHER_ATTACHED",
        source: buyerWhoResearcherSource,
        remainingBudgetUsd: remainingAfterSeed,
        at: now(),
      });
    } else {
      researcher = createFixtureResearcher(corpus);
      buyerWhoResearcherSource = "fixture";
      stages.push({
        stage: "BUYER_WHO_FIXTURE_RESEARCHER",
        reason: liveBuyerWhoAuthorized
          ? "unexpected"
          : opts.forceFixtureResearcher === true
            ? "FORCE_FIXTURE"
            : "LIVE_BUYER_WHO_NOT_AUTHORIZED_OR_NO_BUDGET",
        at: now(),
      });
    }
  }

  // After optional Exa seed, ORS continues with paidBlocked unless an explicit
  // liveResearcher was injected for full paid ORS loops. Buyer-WHO still uses
  // researcher.execute (live when attached above) inside ensureBuyerWhoResearchExecuted.
  const paidBlocked =
    opts.liveResearcher?.execute && opts.forceOrsPaidBlocked !== true
      ? false
      : opts.forceOrsPaidBlocked === true ||
        seed.seeded === true ||
        opts.authorizeExaAgentLive !== true ||
        opts.allowLiveExaDeepSeed !== true;

  const deepResult = await runDeepResearchOnExistingGdiOpportunity({
    opportunity,
    researcher,
    corpus,
    offlineLongform: paidBlocked === true,
    options: {
      paidBlocked,
      ensureBuyerWho: opts.ensureBuyerWho !== false,
      authorizeSurfe: false,
      authorizePdl: false,
      remainingBudgetUsd: remainingAfterSeed,
      maxIterations: opts.maxIterations ?? 4,
      forbidPublication: true,
      forbidAirtable: true,
      checkpointDir: opts.checkpointDir || null,
    },
  });

  opportunity = resolveOpportunityFromDeepResearchResult(
    deepResult,
    opportunity,
  );
  opportunity = attachBuyerWhoFallbackToOpportunity(opportunity, deepResult);
  deepCostUsd += Number(deepResult.researchCostUsd || 0);
  opportunity.orsSupervisorInvoked = true;
  opportunity.orsEntrypoint =
    deepResult.orsEntrypoint || "matureOpportunity";
  opportunity.orsRestoreVersion = GDI_ORIGINAL_FULL_E2E_EXECUTION_RESTORE_V1;

  stages.push({
    stage: "DEEP_RESEARCH",
    mode: paidBlocked
      ? "ORS_SUPERVISOR_MATURE_OPPORTUNITY_OFFLINE"
      : "ORS_SUPERVISOR_MATURE_OPPORTUNITY_AUTHORIZED",
    costUsd: deepCostUsd,
    orsInvoked: true,
    orsEntrypoint: opportunity.orsEntrypoint,
    researchDisposition:
      opportunity.researchDisposition ||
      deepResult.researchDisposition ||
      null,
    buyerWhoExecuted: opportunity.buyerWhoFallback?.executed === true,
    buyerWhoReason: opportunity.buyerWhoFallback?.reason || null,
    buyerWhoResearcherSource,
    at: now(),
  });

  // Explicit BUYER_WHO stage for certification — must not rely on CONTACT_RESEARCH alone.
  let buyerWho = deepResult.buyerWhoFallback || deepResult.after?.buyerWhoFallback;
  const stillNeedsBuyerWho =
    opts.ensureBuyerWho !== false &&
    researcher?.execute &&
    !hasRoleRelevantDecisionMaker(
      opportunity.namedStakeholders ||
        opportunity.namedStakeholdersFound ||
        [],
    ) &&
    buyerWho?.executed !== true;

  if (stillNeedsBuyerWho && liveBuyerWhoAuthorized) {
    buyerWho = await ensureBuyerWhoResearchExecuted({
      opportunity,
      matured: opportunity,
      researcher,
      drainRelatedEntityFollowUpsThroughOrs,
      mergeDeepResearchIntoOpportunity,
      runPublicBuyerIntelligencePass,
      remainingBudgetUsd: Math.max(0, remainingBudgetForStep - deepCostUsd),
      targetHotel: opportunity.hotelName || "",
    });
    if (buyerWho?.executed && buyerWho.matured) {
      opportunity = {
        ...opportunity,
        ...buyerWho.matured,
        namedStakeholders: [
          ...(opportunity.namedStakeholders || []),
          ...(buyerWho.matured.namedStakeholders || []),
        ],
      };
    }
    opportunity = attachBuyerWhoFallbackToOpportunity(opportunity, {
      buyerWhoFallback: buyerWho,
    });
  }

  stages.push({
    stage: "BUYER_WHO_RESEARCH",
    executed: opportunity.buyerWhoFallback?.executed === true,
    reason: opportunity.buyerWhoFallback?.reason || buyerWho?.reason || null,
    researcherSource: buyerWhoResearcherSource,
    liveAuthorized: liveBuyerWhoAuthorized,
    queriesAttempted: (
      opportunity.buyerWhoFallback?.buyerQueriesAttempted || []
    ).length,
    urlsAttempted: (opportunity.buyerWhoFallback?.buyerUrlsAttempted || []).length,
    at: now(),
  });

  return {
    opportunity,
    deepResult,
    deepCostUsd,
    stages,
    eligibility,
    orsInvoked: true,
    exaSeeded: seed.seeded === true,
    buyerWhoFallback: opportunity.buyerWhoFallback || null,
    exaProviderRunId:
      seed?.providerRunId ||
      seed?.exa?.run?.runId ||
      seed?.exa?.run?.id ||
      opportunity?.exaAgentRouting?.providerRunId ||
      opportunity?.researchJobId ||
      opportunity?.exaRunId ||
      null,
    exaSeedReason: seed?.reason || null,
  };
}

export default {
  GDI_ORIGINAL_FULL_E2E_EXECUTION_RESTORE_V1,
  GDI_JUNK_PERSON_NAME_RE,
  isPlausibleBuyerPersonName,
  resolveOpportunityFromDeepResearchResult,
  attachBuyerWhoFallbackToOpportunity,
  runRestoredOrsDeepResearchPass,
  runRestoredContactInvestigationPass,
  runRestoredContactInvestigationPassAsync,
  hydrateGuestroomConfidenceFromDemandThesis,
  runRestoredCommercialEligibilityPass,
  runRestoredNeedDatePass,
  maybeSeedExaDeepResearch,
};
