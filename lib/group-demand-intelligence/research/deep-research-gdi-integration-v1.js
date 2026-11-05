/**
 * Deep Research → Existing GDI Integration V1
 *
 * Wires Webhound-like autonomous long-form investigation into the normal
 * Opportunity Research Supervisor path without a second orchestration architecture.
 *
 * Flow:
 *   existing GDI opportunity
 *   → seed prior evidence / outstanding questions
 *   → matureOpportunity (ORS sole orchestrator + V4.1 lead drain)
 *   → optional offline longform depth pass (fixtures / authorized corpus)
 *   → mergeDeepResearchIntoOpportunity (safe, non-destructive)
 *   → qualifyCommercialProspectV41 + honest sales brief
 *   → buildLongformResearchDossier
 *   → deepResearchResultContract (internal only; publication gates unchanged)
 *
 * No Surfe/PDL/Parallel by default. No Airtable/publish.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { matureOpportunity, resumeMatureOpportunity } from "./opportunity-research-supervisor-v1.js";
import { buildLongformResearchDossier, assertDossierCitationsResolve } from "./longform-research-dossier-v1.js";
import {
  LONGFORM_CANDIDATE,
  LONGFORM_HELD_URLS,
  runLongformOfflineInvestigation,
} from "./longform-research-certification-v1.js";
import {
  qualifyCommercialProspectV41,
  classifyContactOwnership,
  extractPublicContactIntelligenceForOrs,
  buildHonestSalesBriefV41,
  assertPaidContactProvidersDisabled,
  drainRelatedEntityFollowUpsThroughOrs,
  CONTACT_OWNERSHIP_CLASS,
  GEO_FIT_CLASS,
} from "./commercial-spine-restoration-v41.js";
import { classifyPersonResponsibility } from "../workflow/buyer-contact-enrichment-v1.js";
import { INTERNAL_DISPOSITION, REVENUE_TYPE } from "./commercial-qualification-v3.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import {
  assessRetrievalCommercialValue,
  buildEvidenceDrivenReplan,
  runPublicBuyerIntelligencePass,
  assessResearchCompleteness,
  RETRIEVAL_COMMERCIAL_VALUE,
} from "./commercial-depth-buyer-intelligence-v1.js";
import { selectNextAdaptedResearchStep } from "./research-strategy-adaptation-v1.js";
import { executeCoherentResearchAction } from "./research-action-contract-v1.js";
import {
  prepareOpportunityForAutonomousDeepResearch,
  ensureBuyerWhoResearchExecuted,
  hasRoleRelevantDecisionMaker,
  buildBuyerDiscoveryQueries,
  buildAlternativeSourceCandidates,
  resolveOrganizationHost,
  buildTargetedEnrichmentRequest,
} from "./autonomous-deep-research-recovery-v1.js";
import { BUYER_INTELLIGENCE_VALIDATION_COHORT_V1 } from "./deep-research-opportunity-selection-v1.js";
import {
  assessBuyerIntelligenceForOpportunity,
  FINDING_PROVENANCE,
} from "./buyer-intelligence-shortlist-offline-v1.js";
import {
  classifyCommercialNextAction,
  COMMERCIAL_NEXT_ACTION,
} from "./commercial-completion-buyer-access-v1.js";
import { attachSecondaryNamedContact } from "../secondary-named-contact-v1.js";
import {
  filterExhaustedHeuristicCandidates,
  applyRelatedEntityFollowUpDisposition,
  resolveRelatedEntityFollowUpStop,
  countRelatedEntityFollowUpAttempts,
} from "./related-entity-followup-disposition-v1.js";
import { STOP_REASONS } from "./research-constants-v1.js";

export const DEEP_RESEARCH_GDI_INTEGRATION_V1 = "gdi-deep-research-gdi-integration-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Bind sales-brief copy to the canonical hotel + opportunity — never Principal/Nobu defaults.
 */
export function buildOpportunityBoundHotelSalesBrief({
  opportunity = {},
  ownership = {},
  namedStakeholders = [],
  qualificationV41 = {},
  salesBrief = {},
  claims = [],
} = {}) {
  const hotel = text(opportunity.hotelName) || "target hotel";
  const hotelId = opportunity.hotelId || null;
  const opportunityId = opportunity.id || opportunity.opportunityId || null;
  const org = text(opportunity.organizationName) || "the organization";
  const pathway = text(opportunity.pathway || opportunity.commercialStage || qualificationV41.internalDisposition || "");
  const competing = text(
    opportunity.competingHotel
    || opportunity.namedHostHotel
    || qualificationV41?.geoFit?.namedHostHotel
    || "",
  );
  const thesis = text(opportunity.hotelOpportunityThesis || opportunity.demandSignalSummary || "").slice(0, 320);
  const roleDm = (namedStakeholders || []).find((s) =>
    /VERIFIED_DECISION_MAKER|POTENTIAL_DECISION_MAKER/i.test(String(s?.decisionMakerClass || ""))
    && /hotel|sourcing|housing|meeting|event|procurement|contract|travel|planner|director|manager|coordinator/i.test(
      text(s?.title || s?.role || s?.roleTitle || ""),
    ));
  const influencerOnly = !roleDm && (namedStakeholders || []).some((s) => text(s?.name));

  let whoShouldIContact;
  if (ownership.salesUsableAsProspectContact && ownership.route) {
    whoShouldIContact = {
      route: ownership.route,
      ownershipClass: ownership.ownershipClass,
      namedPerson: roleDm?.name || null,
      role: roleDm?.title || roleDm?.roleTitle || null,
      decisionMakerClass: roleDm?.decisionMakerClass || null,
      responsibilityClass: roleDm?.responsibilityClass || null,
      salesUsableAsProspectContact: true,
      contactKind: roleDm ? "ROLE_RELEVANT_NAMED" : "GENERAL_ORG_ROUTE",
    };
  } else if (ownership.consumerDomain || /UNVERIFIED|MISSING|DIRECTORY/i.test(String(ownership.ownershipClass || ""))) {
    whoShouldIContact = {
      route: null,
      gap: ownership.consumerDomain
        ? `Consumer-domain email (${ownership.route || "observed"}) is not a verified organization contact — do not promote as buyer ownership.`
        : (influencerOnly
          ? `Public extraction surfaced influencer-class mentions — not verified ${org} hotel-sourcing decision-makers.`
          : `Named hotel-sourcing decision-maker not established from public evidence for ${org}.`),
      observedNonUsableRoute: ownership.route || null,
      ownershipClass: ownership.ownershipClass || CONTACT_OWNERSHIP_CLASS.MISSING,
      namedPerson: null,
      decisionMakerClass: null,
      salesUsableAsProspectContact: false,
      contactKind: "UNRESOLVED",
    };
  } else {
    whoShouldIContact = {
      route: ownership.route || null,
      ownershipClass: ownership.ownershipClass || CONTACT_OWNERSHIP_CLASS.GENERAL_ORGANIZATION_CONTACT,
      namedPerson: null,
      role: null,
      decisionMakerClass: null,
      responsibilityClass: null,
      salesUsableAsProspectContact: Boolean(ownership.salesUsableAsProspectContact),
      contactKind: ownership.salesUsableAsProspectContact ? "GENERAL_ORG_ROUTE" : "UNRESOLVED",
      gap: ownership.salesUsableAsProspectContact
        ? null
        : `Named hotel-sourcing decision-maker not established from public evidence for ${org}.`,
    };
  }

  const isCompetitive = /COMPETITIVE_FUTURE|FUTURE_CYCLE/i.test(pathway);
  const isEvent = /EVENT_LED|PRE_SOURCING|WEEKEND|TOURNAMENT/i.test(pathway)
    || /memorial|tournament|housing/i.test(thesis + org);
  const isAccount = /ACCOUNT/i.test(pathway) || (!isCompetitive && !isEvent);

  const whyThisOrganization = thesis
    || (isCompetitive && competing
      ? `${org} runs a recurring conference / meeting cycle historically hosted at ${competing}. Credible future-cycle hotel-sourcing demand — not a claim that ${hotel} hosts the current cycle.`
      : isEvent
        ? `${org} generates event-driven lodging / housing demand that can create guestroom or meeting-space need near ${hotel}'s market. Demand mechanism must stay evidence-backed — no invented RFP.`
        : `${org} shows account-level group / meeting activity that can create lodging or meeting-space demand relevant to ${hotel}. Credible demand mechanism only where evidence supports it.`);

  const whatPotentialBusinessExists = isCompetitive && competing
    ? `Future-cycle headquarter / overflow lodging around the ${org} meeting pattern (current/historical host: ${competing}). Do not invent a ${hotel} award or open RFP.`
    : isEvent
      ? `Event-cycle guestroom / housing / meeting-space demand tied to ${org} programmes near ${hotel}. Magnitude and dates remain research gaps until evidenced.`
      : `Account-development lodging and meeting-space opportunities from ${org} programmes that fit ${hotel}'s catchment — not a claimed room block.`;

  const whyMyHotel = competing && isCompetitive
    ? `${hotel} may be relevant for future-cycle shortlists or overflow only. Preserve ${competing} as the evidenced host pattern; do not imply ${hotel} hosts the current event.`
    : `${hotel} is the bound target hotel for this opportunity (${opportunityId || "unknown id"} / ${hotelId || "unknown hotelId"}). Fit must come from this opportunity's evidence — never borrow copy from unrelated hotels.`;

  const namedAsk = roleDm?.name
    ? `Ask ${roleDm.name}${roleDm.title ? ` (${roleDm.title})` : ""} or ${ownership.route || "the organization contact"} how ${org} shortlists hotels for upcoming programmes and whether ${hotel} can be considered. Do not invent an RFP, room block, or current ${hotel} award.`
    : null;

  const whatDoWeStillNotKnow = [...new Set([
    ...(opportunity.unresolvedQuestions || []),
    `Who at ${org} owns hotel sourcing / housing / contracting decisions?`,
    isCompetitive
      ? (competing
        ? `Has the next cycle headquarter hotel been named, or does ${competing} remain the pattern?`
        : `Has the next cycle headquarter hotel been named for ${org}?`)
      : `What dated programme / cycle creates lodging demand suitable for ${hotel}?`,
    ownership.salesUsableAsProspectContact && roleDm
      ? `Is ${roleDm.name} still current, and do they own final hotel shortlisting vs a client or intermediary buyer?`
      : ownership.consumerDomain
        ? `Is there an organization-domain contact route (consumer email is not ownership-verified)?`
        : `Is there a role-relevant named buyer beyond a general organization route?`,
  ].filter(Boolean))].slice(0, 8);

  const personDmVerified = Boolean(roleDm?.name)
    && /^[A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+)+$/.test(String(roleDm.name).trim())
    && !/maritz|association|housing|provider|tournaments|global events/i.test(roleDm.name);
  const commercialNextAction = classifyCommercialNextAction({
    pathway: pathway || qualificationV41.internalDisposition,
    hotelFitCredible: !/MISMATCH_EXCLUDE|insufficient hotel/i.test(
      `${qualificationV41?.geoFit?.geoFitClass || ""} ${salesBrief.evidenceBackedHotelFit || ""}`,
    ),
    namedPersonBuyerVerified: personDmVerified,
    functionalRouteUsable: whoShouldIContact.salesUsableAsProspectContact === true,
    ownershipClass: whoShouldIContact.ownershipClass || ownership.ownershipClass,
    enrichmentJustified: !personDmVerified,
    overflowOrHostUnlocked: Boolean(qualificationV41?.geoFit?.eventQualifiedForTargetHotel)
      && !/NAMED_COMPETITOR_HOST_ELSEWHERE/i.test(String(qualificationV41?.geoFit?.geoFitClass || "")),
    speculativeOnly: /do not invent|speculative overflow|no credible/i.test(
      `${whatPotentialBusinessExists} ${salesBrief.recommendedQuestionOrPitch || ""}`,
    ) && whoShouldIContact.salesUsableAsProspectContact !== true && !personDmVerified,
  });

  // Generic inbox alone must not become READY_FOR_OUTREACH
  if (commercialNextAction.nextAction === COMMERCIAL_NEXT_ACTION.READY_FOR_OUTREACH
    && whoShouldIContact.contactKind === "GENERAL_ORG_ROUTE"
    && !personDmVerified) {
    commercialNextAction.nextAction = COMMERCIAL_NEXT_ACTION.FUNCTIONAL_ROUTE_QUALIFY;
    commercialNextAction.salesReady = false;
    commercialNextAction.rationale = "General organization inbox is usable for qualification only — not automatic sales-ready outreach";
  }

  return {
    version: DEEP_RESEARCH_GDI_INTEGRATION_V1,
    opportunityId,
    hotelId,
    hotelName: hotel,
    organizationName: org,
    pathway: pathway || null,
    whoShouldIContact,
    whyThisOrganization,
    whatPotentialBusinessExists,
    whyMyHotel,
    whyNow: roleDm?.name && personDmVerified
      ? `Public evidence supports an account introduction to a role-relevant contact while programme sourcing remains UNKNOWN — introduce before a host is locked for the next cycle.`
      : (salesBrief.reasonToContactNow || "Not sales-ready until buyer ownership and demand timing are clearer."),
    whatShouldISayOrAsk: (namedAsk && personDmVerified)
      ? namedAsk
      : (salesBrief.recommendedQuestionOrPitch
      || (whoShouldIContact.salesUsableAsProspectContact
        ? `Use ${whoShouldIContact.route} to ask who owns hotel sourcing / housing participation for programmes relevant to ${hotel}. Do not invent an RFP or room block.`
        : `Named hotel-sourcing decision-maker not established — pursue only if hotel thesis remains credible. Do not invent demand.`)),
    whatDoWeStillNotKnow,
    commercialNextAction,
    salesReady: commercialNextAction.salesReady === true,
    commercialStage: qualificationV41.internalDisposition || pathway || null,
    verifiedVsInferred: (() => {
      const foreignLeak = /Principal\.?\s*London|Nobu Portman|Rosewood London|Marylebone|principalge\.com/i;
      const orgOk = (s) => {
        const t = text(s);
        if (!t) return false;
        // Drop cross-opportunity seed facts when this brief is bound to a different org/hotel
        if (foreignLeak.test(t)
          && !/principal/i.test(org)
          && !/nobu/i.test(hotel)) {
          return false;
        }
        return true;
      };
      return {
        verified: (salesBrief.verifiedFacts || claims.slice(0, 5).map((c) => c.statement))
          .filter(orgOk),
        inferred: (salesBrief.inferredOpportunities || []).filter(orgOk),
      };
    })(),
    provenanceBound: {
      opportunityId,
      hotelId,
      hotelName: hotel,
      organizationName: org,
      crossHotelCopyForbidden: true,
      crossOpportunityCopyForbidden: true,
    },
    internalOnly: true,
    customerFacingPublicationEligible: false,
  };
}

function claimToEvidence(claim = {}) {
  return {
    statement: text(claim.statement || claim.excerpt),
    excerpt: text(claim.excerpt || claim.statement),
    quote: text(claim.statement || claim.excerpt),
    url: claim.url || null,
    evidenceId: claim.evidenceId || null,
    topic: claim.topic || null,
    accepted: claim.accepted !== false,
    provenance: claim.provenance || "deep_research_merge",
    sourceFamily: claim.sourceFamily || "organizer_publication",
  };
}

/**
 * Safe merge: never overwrite stronger existing evidence; never invent; respect Surfe non-persist.
 */
export function mergeDeepResearchIntoOpportunity(opportunity = {}, deepResult = {}) {
  const next = { ...opportunity };
  const priorEvidence = [...(opportunity.evidence || [])];
  const priorUrls = new Set(priorEvidence.map((e) => e.url).filter(Boolean));
  const priorStatements = new Set(priorEvidence.map((e) => text(e.statement || e.quote)).filter(Boolean));

  const incoming = [
    ...(deepResult.acceptedClaims || []).map(claimToEvidence),
    ...(deepResult.evidence || []),
    ...(deepResult.newFindings || []).map((f) => claimToEvidence(typeof f === "string" ? { statement: f } : f)),
  ].filter((e) => text(e.statement || e.quote));

  const added = [];
  for (const item of incoming) {
    const stmt = text(item.statement || item.quote);
    if (!stmt) continue;
    if (priorStatements.has(stmt)) continue;
    if (item.url && priorUrls.has(item.url) && priorEvidence.some((e) => text(e.statement || e.quote) === stmt)) {
      continue;
    }
    priorEvidence.push(item);
    priorStatements.add(stmt);
    if (item.url) priorUrls.add(item.url);
    added.push(item);
  }

  next.evidence = priorEvidence;
  next.investigatedUrls = [...new Set([
    ...(opportunity.investigatedUrls || []),
    ...(deepResult.investigatedUrls || []),
    ...priorEvidence.map((e) => e.url).filter(Boolean),
  ])];
  next.rejectedClaims = [
    ...(opportunity.rejectedClaims || []),
    ...(deepResult.rejectedClaims || []),
  ];
  next.unresolvedQuestions = [
    ...new Set([
      ...(opportunity.unresolvedQuestions || []),
      ...(deepResult.unresolvedGaps || deepResult.unresolvedQuestions || []),
    ]),
  ];
  next.relatedEntityFollowUps = [
    ...(opportunity.relatedEntityFollowUps || []),
    ...(deepResult.relatedEntityFollowUps || []),
  ];
  next.deepResearchJobId = deepResult.researchJobId || opportunity.deepResearchJobId || null;
  next.deepResearchStoppingReason = deepResult.stoppingTrigger || deepResult.disposition || null;
  next.internalDeepResearchEnrichment = {
    version: DEEP_RESEARCH_GDI_INTEGRATION_V1,
    mergedAt: new Date().toISOString(),
    addedEvidenceCount: added.length,
    publicationEligibilityUnchanged: true,
    surfePersisted: false,
  };

  // Contacts: only merge ownership-valid public routes; never Surfe PII
  const contactOwnership = deepResult.contactOwnership || null;
  if (contactOwnership?.salesUsableAsProspectContact && contactOwnership.route) {
    next.contact = {
      ...(opportunity.contact || {}),
      businessEmail: contactOwnership.route,
      ownershipClass: contactOwnership.ownershipClass,
      personName: contactOwnership.personName || opportunity.contact?.personName || null,
      roleTitle: contactOwnership.roleTitle || opportunity.contact?.roleTitle || null,
    };
    next.contactEmail = contactOwnership.route;
  }

  if (deepResult.namedStakeholders?.length) {
    next.namedStakeholders = [
      ...(opportunity.namedStakeholders || []),
      ...deepResult.namedStakeholders,
    ];
  }

  if (deepResult.dossier) {
    next.supervisorV2 = {
      ...(opportunity.supervisorV2 || {}),
      dossier: deepResult.dossier,
      deepResearchIntegration: DEEP_RESEARCH_GDI_INTEGRATION_V1,
    };
  }

  // Universal secondary named contact (optional; never blocks publication)
  const withSecondary = attachSecondaryNamedContact(next, {
    candidates: next.namedStakeholders || [],
  });
  return { opportunity: withSecondary, addedEvidence: added };
}

/**
 * Build the backward-compatible deep-research result contract for GDI consumers.
 */
export function buildDeepResearchResultContract({
  opportunity = {},
  matured = null,
  longform = null,
  qualificationV41 = null,
  dossierResult = null,
  salesBrief = null,
  beforeSnapshot = null,
  costs = null,
  mode = "offline_fixtures",
} = {}) {
  const relatedExecuted = (matured?.researchAttempts || [])
    .filter((a) => a.strategy === "related_entity_followup").length
    + (longform?.relatedEntityFollowUps || []).filter((l) => l.executed === true || l.retrieved === true).length
    + (longform?.retrievals || []).filter((r) => r.relatedEntity === true).length;

  return {
    version: DEEP_RESEARCH_GDI_INTEGRATION_V1,
    mode,
    simulatedVsLive: mode === "offline_fixtures"
      ? "OFFLINE_SIMULATED_FIXTURES_NOT_LIVE_DISCOVERY"
      : mode,
    canonicalHotelId: opportunity.hotelId || LONGFORM_CANDIDATE.hotelId,
    canonicalOpportunityId: opportunity.id || opportunity.opportunityId || LONGFORM_CANDIDATE.opportunityId,
    researchJobId: opportunity.deepResearchJobId
      || `deep_${opportunity.id || LONGFORM_CANDIDATE.opportunityId}_${Date.now()}`,
    checkpoint: matured?.checkpointPath || longform?.checkpointPath || null,
    investigatedOrganization: opportunity.organizationName || LONGFORM_CANDIDATE.organizationName,
    investigatedProgram: opportunity.eventName || opportunity.programName || null,
    verifiedNewFindings: (qualificationV41?.salesBrief?.verifiedFacts || [])
      .concat((longform?.acceptedClaims || []).slice(beforeSnapshot?.acceptedClaimsCount || 0).map((c) => c.statement)),
    supportedCommercialInferences: qualificationV41?.salesBrief?.inferredOpportunities || [],
    contradictions: (longform?.rejectedClaims || []).filter((r) => /contradict/i.test(r.reason || "")).map((r) => r.claim),
    unresolvedQuestions: opportunity.unresolvedQuestions
      || longform?.unresolvedGaps
      || [],
    hotelSpecificDemandImplications: qualificationV41?.salesBrief?.evidenceBackedHotelFit || null,
    commerciallyRelevantFutureCycles: qualificationV41?.geoFit?.geoFitClass === GEO_FIT_CLASS.FUTURE_CYCLE_DESTINATION_UNKNOWN
      || qualificationV41?.internalDisposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
      || qualificationV41?.internalDisposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT,
    buyerAndStakeholderIntelligence: {
      namedStakeholders: opportunity.namedStakeholders || [],
      contactOwnership: qualificationV41?.contactOwnership || null,
    },
    verifiedContactRoutes: qualificationV41?.contactOwnership?.salesUsableAsProspectContact
      ? [qualificationV41.contactOwnership]
      : [],
    contactProvenanceAndStorageEligibility: {
      surfePersisted: false,
      pdlPersisted: false,
      publicSourceOnly: true,
      storageEligible: qualificationV41?.contactOwnership?.salesUsableAsProspectContact === true,
    },
    evidenceCitations: (opportunity.evidence || []).map((e) => ({
      statement: e.statement || e.quote,
      url: e.url,
      publisher: e.sourceFamily || null,
    })),
    recommendedSalesActions: salesBrief
      ? [salesBrief.recommendedQuestionOrPitch].filter(Boolean)
      : [],
    researchCompletenessAndStoppingReason: {
      disposition: longform?.disposition || matured?.researchStatus || null,
      stoppingTrigger: longform?.stoppingTrigger || matured?.stopReason || null,
      relatedEntityLeadsExecuted: relatedExecuted,
      maxIterationsIsNotExhaustion: true,
    },
    actualProviderCosts: costs || { usd: 0, paidProviders: [] },
    researchAndContactRefreshRecommendations: [
      "Revisit official news / programme pages on a scheduled cycle",
      "Pursue named hotel-sourcing decision-maker only after public WHO evidence exists",
      "Surfe only under separate explicit authorization after named person established",
    ],
    qualificationV41: qualificationV41
      ? {
        disposition: qualificationV41.internalDisposition,
        commerciallyUsefulInternal: qualificationV41.commerciallyUsefulInternal,
        geoFitClass: qualificationV41.geoFit?.geoFitClass,
        identityOk: qualificationV41.identity?.actionableCandidate,
      }
      : null,
    dossier: dossierResult?.dossier || matured?.supervisorV2?.dossier || null,
    salesBrief,
    publicationEligibilityUnchanged: true,
    customerFacingOutputUnchanged: true,
  };
}

/**
 * Capture BEFORE snapshot of an existing GDI opportunity (no enrichment).
 */
export function captureOpportunityBeforeSnapshot(opportunity = {}) {
  const evidence = opportunity.evidence || opportunity.acceptedClaims || [];
  return {
    version: DEEP_RESEARCH_GDI_INTEGRATION_V1,
    capturedAt: new Date().toISOString(),
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    hotelId: opportunity.hotelId || null,
    hotelName: opportunity.hotelName || opportunity.targetHotel || null,
    organizationName: opportunity.organizationName || null,
    description: opportunity.demandSignalSummary || opportunity.summaryWhyHotel || opportunity.researchObjective || null,
    demandEvidence: evidence.map((e) => ({
      statement: e.statement || e.excerpt || e.quote,
      url: e.url || null,
      topic: e.topic || null,
    })),
    hotelFit: opportunity.hotelSpecificFit || opportunity.hotelFit || null,
    commercialStage: opportunity.commercialStage || opportunity.internalDisposition || null,
    pathway: opportunity.pathway || null,
    knownStakeholders: opportunity.namedStakeholders || [],
    contactEmail: opportunity.contactEmail || opportunity.contact?.businessEmail || opportunity.primaryContactEmail || null,
    contactRoutes: [
      opportunity.contactEmail,
      opportunity.contact?.businessEmail,
      opportunity.primaryContactEmail,
    ].filter(Boolean),
    unansweredQuestions: opportunity.unresolvedQuestions || opportunity.originalUnresolvedQuestion
      ? [opportunity.originalUnresolvedQuestion || opportunity.researchObjective].filter(Boolean)
      : [],
    recommendedSalesActions: opportunity.v3SalesAction || opportunity.v2NextSalesAction
      ? [opportunity.v3SalesAction || opportunity.v2NextSalesAction]
      : [],
    researchDepth: {
      evidenceCount: evidence.length,
      investigatedUrls: (opportunity.investigatedUrls || []).length,
      relatedEntityFollowUpsExecuted: (opportunity.researchAttempts || [])
        .filter((a) => a.strategy === "related_entity_followup").length,
      citations: evidence.filter((e) => e.url).length,
    },
    acceptedClaimsCount: evidence.length,
  };
}

/**
 * Seed a GDI opportunity object from the Principal London longform candidate (existing corpus).
 */
export function seedPrincipalLondonGdiOpportunity(overrides = {}) {
  const c = LONGFORM_CANDIDATE;
  return {
    id: c.opportunityId,
    opportunityId: c.opportunityId,
    hotelId: c.hotelId,
    hotelName: c.hotelName,
    organizationName: c.organizationName,
    segmentId: c.segmentId,
    pathway: c.pathway,
    hotelSpecificFit: c.hotelSpecificFit,
    researchObjective: c.researchObjective,
    originalUnresolvedQuestion: c.originalUnresolvedQuestion || c.researchObjective,
    officialSource: "https://www.principallondon.com/",
    seedUrls: [...LONGFORM_HELD_URLS],
    investigatedUrls: [...LONGFORM_HELD_URLS],
    evidence: (c.startingEvidence || []).map(claimToEvidence),
    acceptedClaims: [...(c.startingEvidence || [])],
    contactEmail: "london@principalge.com",
    organizationDomain: "principalge.com",
    officialDomain: "principallondon.com",
    contact: {
      businessEmail: "london@principalge.com",
      businessPhone: "020 7485 5500",
    },
    unresolvedQuestions: [
      "Does Principal currently publish a dated London programme needing bedrooms at Nobu Portman Square?",
      "Who at Principal owns hotel sourcing / contracting decisions?",
    ],
    rejectedClaims: [
      { claim: "Nobu is already a Principal hotel partner", reason: "not_in_cited_source" },
      { claim: "An open RFP exists for Nobu bedrooms", reason: "not_in_cited_source" },
    ],
    commercialStage: "ACCOUNT_DEVELOPMENT",
    internalDisposition: INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT,
    sourcingStatus: "UNKNOWN",
    ...overrides,
  };
}

/**
 * Build a deterministic offline researcher from a URL→text corpus (no network).
 */
export function createFixtureResearcher(corpus = {}, { relatedEntityUrls = [] } = {}) {
  const pages = new Map(Object.entries(corpus || {}));
  for (const url of relatedEntityUrls) {
    if (!pages.has(url)) pages.set(url, `Related entity page fixture for ${url}`);
  }
  return {
    async execute({ opportunity, action }) {
      const queries = action?.queries || [];
      const urls = [
        ...queries.filter((q) => /^https?:/i.test(String(q))),
        opportunity?.officialSource,
        ...(opportunity?.seedUrls || []),
      ].filter(Boolean);
      const evidence = [];
      let namedStakeholders = [];
      for (const url of urls.slice(0, 4)) {
        const pageText = pages.get(url);
        if (pageText == null) continue;
        if (!text(pageText)) continue;
        evidence.push({
          url,
          statement: text(pageText).slice(0, 400),
          quote: text(pageText).slice(0, 400),
          provenance: "offline_fixture",
          accepted: true,
          sourceFamily: /rosewood|independent/i.test(url) ? "independent_publisher" : "organizer_publication",
        });
        try {
          const ci = extractPublicContactIntelligenceForOrs({
            textContent: pageText,
            url,
            organizationName: opportunity?.organizationName || "",
          });
          namedStakeholders = namedStakeholders.concat(ci.namedStakeholdersFound || []);
        } catch { /* ignore */ }
      }
      const isLead = action?.strategy === "related_entity_followup";
      const contactEmail = opportunity?.contactEmail || opportunity?.contact?.businessEmail || null;
      return {
        disposition: evidence.length ? "EVIDENCE_FOUND" : "NO_EVIDENCE",
        newMaterialEvidence: evidence.length > 0,
        costUsd: 0,
        evidence: isLead
          ? evidence.map((e) => ({ ...e, relatedEntity: true }))
          : evidence,
        provider: "direct_public_fixture",
        deepPatch: contactEmail
          ? {
            contact: {
              businessEmail: contactEmail,
              personName: namedStakeholders[0]?.name || null,
              roleTitle: namedStakeholders[0]?.title || null,
            },
            namedStakeholdersFound: namedStakeholders.slice(0, 5),
          }
          : {},
      };
    },
  };
}

/**
 * Canonical entry: run deep research on an existing GDI opportunity via ORS.
 */
export async function runDeepResearchOnExistingGdiOpportunity({
  opportunity,
  researcher = null,
  corpus = null,
  offlineLongform = true,
  options = {},
} = {}) {
  if (!opportunity?.organizationName && !opportunity?.id) {
    throw new Error("deep_research_requires_existing_gdi_opportunity");
  }

  const paidGuard = assertPaidContactProvidersDisabled({
    authorizeSurfe: options.authorizeSurfe === true,
    authorizePdl: options.authorizePdl === true,
    surfeCalls: 0,
    pdlCalls: 0,
  });
  if (!paidGuard.ok && options.authorizeSurfe !== true && options.authorizePdl !== true) {
    /* default path ok */
  }
  if (options.authorizeSurfe === true || options.authorizePdl === true) {
    throw new Error("Deep research integration forbids Surfe/PDL in this phase — separate GO required");
  }

  const before = captureOpportunityBeforeSnapshot(opportunity);
  const pubBefore = evaluateProspectPublicationEligibility(opportunity);

  // Optional: reuse longform offline investigation for Webhound-like depth on fixtures.
  // Never fall back to Principal/Nobu LONGFORM_CANDIDATE seeds for a different opportunity.
  let longform = null;
  if (offlineLongform && corpus) {
    const oppEvidenceClaims = (opportunity.acceptedClaims && opportunity.acceptedClaims.length
      ? opportunity.acceptedClaims
      : (opportunity.evidence || []).map((e) => ({
        evidenceId: e.evidenceId || e.url,
        url: e.url,
        statement: e.statement || e.quote,
        topic: e.topic || "programme",
        accepted: true,
      }))).filter((c) => text(c.statement || c.excerpt));
    const isPrincipalSeedOpp = /principal/i.test(String(opportunity.organizationName || ""))
      && /nobu/i.test(String(opportunity.hotelName || ""));
    const startingEvidence = oppEvidenceClaims.length
      ? oppEvidenceClaims
      : (isPrincipalSeedOpp ? LONGFORM_CANDIDATE.startingEvidence : []);
    const investigatedSeed = (opportunity.investigatedUrls || []).length
      ? opportunity.investigatedUrls
      : (isPrincipalSeedOpp ? [...LONGFORM_HELD_URLS] : []);
    longform = runLongformOfflineInvestigation({
      corpus,
      startingEvidence,
      investigatedUrls: investigatedSeed,
      researchQuestion: opportunity.researchObjective
        || opportunity.hotelOpportunityThesis
        || `Deepen commercial intelligence for ${opportunity.organizationName} → ${opportunity.hotelName}`,
      entityUrlHints: options.entityUrlHints || (
        isPrincipalSeedOpp
          ? { "Rosewood London": Object.keys(corpus).find((u) => /rosewood/i.test(u)) }
          : {}
      ),
      allowEarlyBuyerStop: options.allowEarlyBuyerStop !== false,
      candidateQueue: options.candidateQueue || Object.keys(corpus),
    });
  }

  const fixtureResearcher = researcher || (corpus
    ? createFixtureResearcher(corpus, {
      relatedEntityUrls: Object.keys(corpus).filter((u) =>
        /rosewood|independent|\/team|\/people|\/leadership|\/programs|\/news|board/i.test(u)),
    })
    : null);

  if (!fixtureResearcher?.execute) {
    throw new Error("deep_research_requires_researcher_or_corpus");
  }

  // Autonomous held-source recovery: do not abort when initial URLs already investigated
  let seeded = prepareOpportunityForAutonomousDeepResearch(
    { ...opportunity },
    { resetAttempts: options.resetResearchAttempts !== false, maxAlternatives: 6 },
  );
  if (longform?.relatedEntityFollowUps?.length) {
    seeded.pendingDiscoverySearches = (longform.relatedEntityFollowUps || [])
      .filter((l) => l.suggestedUrl || l.url)
      .map((l) => ({
        entityName: l.entityName || "related_entity",
        suggestedUrl: l.suggestedUrl || l.url,
        independentPublisher: /rosewood|independent/i.test(l.suggestedUrl || l.url || ""),
        entityType: "hotel_or_accommodation_partner",
        researchQuestion: l.researchQuestion || `Validate related entity ${l.entityName}`,
      }));
  }
  if (longform?.acceptedClaims?.length) {
    const mergedSeed = mergeDeepResearchIntoOpportunity(seeded, {
      acceptedClaims: longform.acceptedClaims,
      investigatedUrls: longform.investigatedUrls,
      rejectedClaims: longform.rejectedClaims,
      unresolvedGaps: longform.unresolvedGaps,
      relatedEntityFollowUps: longform.relatedEntityFollowUps,
      researchJobId: `longform_${seeded.id || "job"}`,
      stoppingTrigger: longform.stoppingTrigger,
      disposition: longform.disposition,
    });
    seeded = mergedSeed.opportunity;
  }

  const orsOptions = {
    maxIterations: options.maxIterations ?? 4,
    // run-049: callers that authorize paid research must pass paidBlocked:false explicitly.
    // Default remains true (safe) for offline/fixture paths.
    paidBlocked: options.paidBlocked !== false,
    parallelAvailable: false,
    executeLeadFollowUps: true,
    maxLeadFollowUpsPerOpportunity: options.maxLeadFollowUpsPerOpportunity ?? 3,
    remainingBudgetUsd: options.remainingBudgetUsd ?? 1,
    targetHotel: opportunity.hotelName || LONGFORM_CANDIDATE.hotelName,
    enableSupervisorV2: true,
    generateSupervisorV2Dossier: true,
    enablePublicBuyerContactEnrichment: true,
    forbidPublication: true,
    forbidAirtable: true,
    checkpointDir: options.checkpointDir || null,
    entityUrlHints: options.entityUrlHints || {},
  };

  let matured = await matureOpportunity({
    opportunity: seeded,
    researcher: fixtureResearcher,
    options: orsOptions,
  });

  // Ensure drain counted even if ORS path hit PENDING with empty pursue earlier
  let drainExtra = null;
  if ((matured.researchAttempts || []).filter((a) => a.strategy === "related_entity_followup").length === 0
    && (seeded.pendingDiscoverySearches || []).length) {
    drainExtra = await drainRelatedEntityFollowUpsThroughOrs({
      state: matured,
      researcher: fixtureResearcher,
      targetHotel: opportunity.hotelName || LONGFORM_CANDIDATE.hotelName,
      remainingBudgetUsd: options.remainingBudgetUsd ?? 1,
      maxFollowUps: 2,
      paidBlocked: true,
    });
    Object.assign(matured, drainExtra.state);
  }

  // run-051: research-action contract — coherent strategy/question → real investigation
  // → gold brief update. Runs after ORS so commercial gaps drive the next query.
  let researchActionPass = null;
  if (options.allowResearchActionContract !== false) {
    const hotelCtx = {
      displayName: opportunity.hotelName || orsOptions.targetHotel || "",
      city: opportunity.city || opportunity.hotelCity || "",
      hotelId: opportunity.hotelId || null,
    };
    const adapted = selectNextAdaptedResearchStep(matured, hotelCtx);
    matured.researchStrategyAdaptation = adapted;
    if (adapted.assessment) {
      matured.commercialResearchAssessment = adapted.assessment;
    }
    if (adapted.executable && adapted.researchAction) {
      const remaining =
        Number(options.remainingBudgetUsd ?? orsOptions.remainingBudgetUsd ?? 0) || 0;
      const allowPaid =
        orsOptions.paidBlocked === false && remaining > 0.05;
      researchActionPass = await executeCoherentResearchAction({
        action: adapted.researchAction,
        opportunity: matured,
        hotel: hotelCtx,
        ledgerPath:
          options.ledgerPath ||
          fixtureResearcher?.ledgerPath ||
          researcher?.ledgerPath ||
          null,
        remainingBudgetUsd: remaining,
        allowPaid,
        maxPages: options.researchActionMaxPages ?? 3,
      });
      matured = {
        ...researchActionPass.opportunity,
        researchActionContractTrace: researchActionPass.trace,
        notes: [
          ...(matured.notes || []),
          ...(researchActionPass.opportunity.notes || []),
          `research_action_contract:${adapted.researchAction.strategy}:${researchActionPass.changed ? "changed" : "unchanged"}`,
        ],
      };
      if (adapted.nextResearchStep) {
        matured.nextResearchStep = {
          ...adapted.nextResearchStep,
          lastExecuted: true,
          lastChanged: researchActionPass.changed === true,
        };
      }
    }
  }

  // Evidence-driven replan: score retrievals; generic hotel pages ≠ relationship corroboration.
  let replanPass = null;
  let retrievalAssessments = [];
  let evidenceDrivenReplan = null;
  const orgHostResolved = resolveOrganizationHost(opportunity) || opportunity.organizationDomain || opportunity.officialDomain || "";
  const claimedRelated = text(
    opportunity.competingHotel
    || opportunity.namedHostHotel
    || opportunity.relatedEntityName
    || "",
  );
  const heldUrlSet = (opportunity.investigatedUrls || []).slice();
  const openQs = [...(opportunity.unresolvedQuestions || matured.unresolvedQuestions || [])];
  for (const e of matured.evidence || []) {
    if (!e?.url || /held_/i.test(e.evidenceId || "")) continue;
    const relatedHint = e.relatedEntity === true
      || (claimedRelated && new RegExp(claimedRelated.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(e.url || e.statement || ""));
    retrievalAssessments.push(assessRetrievalCommercialValue({
      url: e.url,
      pageText: e.statement || e.quote || "",
      researchQuestions: openQs,
      heldUrls: heldUrlSet,
      organizationName: opportunity.organizationName,
      organizationHost: orgHostResolved || "example.invalid",
      claimedRelatedEntity: claimedRelated || null,
      relatedEntityFollowUp: relatedHint,
    }));
  }
  const unproductive = retrievalAssessments.filter((a) =>
    a.valueClass === RETRIEVAL_COMMERCIAL_VALUE.UNPRODUCTIVE_RELATIONSHIP_PROBE
    || (a.replanRecommended && !a.materialAdvance));
  const orgLabel = opportunity.organizationName || "the organization";
  const unresolvedAfterFirst = [
    ...openQs,
    `Who owns hotel sourcing / contracting decisions at ${orgLabel}?`,
  ];
  if (options.allowReplan !== false && (unproductive.length || retrievalAssessments.length || (seeded.pendingDiscoverySearches || []).length) && unresolvedAfterFirst.length) {
    const orgHostRe = (orgHostResolved || "example.invalid").replace(/\./g, "\\.");
    evidenceDrivenReplan = buildEvidenceDrivenReplan({
      retrievalAssessment: unproductive[0] || retrievalAssessments[0] || null,
      unresolvedQuestions: unresolvedAfterFirst,
      investigatedUrls: matured.investigatedUrls || [],
      organizationName: opportunity.organizationName,
      targetHotel: opportunity.hotelName || LONGFORM_CANDIDATE.hotelName,
      contactPresent: Boolean(matured.contactEmail || matured.contact?.businessEmail || opportunity.contactEmail),
      remainingBudgetUsd: options.remainingBudgetUsd ?? 1,
      alternativeCandidates: [
        ...(options.replanAlternativeCandidates || []),
        ...(seeded.pendingDiscoverySearches || []),
        ...buildAlternativeSourceCandidates({
          ...opportunity,
          investigatedUrls: matured.investigatedUrls || opportunity.investigatedUrls,
          candidateQueue: opportunity.candidateQueue || matured.candidateQueue,
        }),
        ...Object.keys(corpus || {})
          .filter((u) => /who-we-are|team|leadership|press|news|about|people|board|staff/i.test(u))
          .map((u) => ({
            entityName: /press|independent|news/i.test(u) ? `${orgLabel} connecting / press source` : `${orgLabel} hotel-sourcing / team page`,
            suggestedUrl: u,
            independentPublisher: !new RegExp(orgHostRe, "i").test(u),
            entityType: /who-we-are|team|people|leadership|board|staff/i.test(u) ? "programme" : "hotel_or_accommodation_partner",
            researchQuestion: /team|who-we-are|people|leadership|board|staff/i.test(u)
              ? "Identify hotel-sourcing decision-maker or functional contact route"
              : `Seek independent evidence of ${orgLabel} programmes / hotel relationships — not generic hotel amenity pages`,
          })),
        ...(options.candidateQueue || seeded.candidateQueue || []).map((u) => ({
          entityName: /press|travelweekly|skift|meetingstoday/i.test(u)
            ? "Independent programme / press source"
            : `${orgLabel} hotel-sourcing / team page`,
          suggestedUrl: u,
          independentPublisher: !new RegExp(orgHostRe, "i").test(u),
          entityType: /team|who-we-are|people|leadership|board|staff/i.test(u) ? "programme" : "hotel_or_accommodation_partner",
          researchQuestion: /team|who-we-are|people|leadership|board|staff/i.test(u)
            ? "Identify hotel-sourcing decision-maker or functional contact route"
            : `Seek independent evidence of ${orgLabel} programmes / hotel relationships`,
        })),
      ],
    });
    const replanSeed = {
      ...matured,
      unresolvedQuestions: unresolvedAfterFirst,
      pendingDiscoverySearches: [
        ...(matured.pendingDiscoverySearches || []),
        ...(evidenceDrivenReplan.pendingDiscoverySearches || []),
      ],
      seedUrls: [
        ...new Set([
          ...(matured.seedUrls || []),
          ...(evidenceDrivenReplan.pendingDiscoverySearches || []).map((p) => p.suggestedUrl).filter(Boolean),
        ]),
      ],
      notes: [
        ...(matured.notes || []),
        `evidence_driven_replan:${evidenceDrivenReplan.preferredDisposition}`,
        ...unproductive.map((u) => `unproductive_retrieval:${u.valueClass}:${u.url}`),
      ],
    };
    if (evidenceDrivenReplan.worthExpectedCost || evidenceDrivenReplan.pendingDiscoverySearches?.length) {
      replanPass = await matureOpportunity({
        opportunity: replanSeed,
        researcher: fixtureResearcher,
        options: {
          ...orsOptions,
          maxIterations: Math.min(3, orsOptions.maxIterations),
          maxLeadFollowUpsPerOpportunity: 2,
        },
      });
      const rem = mergeDeepResearchIntoOpportunity(matured, {
        evidence: replanPass.evidence || [],
        investigatedUrls: replanPass.investigatedUrls || [],
        namedStakeholders: replanPass.namedStakeholders || [],
        contactOwnership: replanPass.contact?.businessEmail
          ? {
            route: replanPass.contact.businessEmail,
            ownershipClass: replanPass.contact.ownershipClass,
            salesUsableAsProspectContact: true,
            personName: replanPass.contact.personName,
            roleTitle: replanPass.contact.roleTitle,
          }
          : null,
      });
      matured = {
        ...rem.opportunity,
        researchAttempts: [
          ...(matured.researchAttempts || []),
          ...(replanPass.researchAttempts || []).map((a) => ({ ...a, replanPass: true })),
        ],
        notes: [
          ...(matured.notes || []),
          ...(replanPass.notes || []),
          "deep_research_evidence_driven_replan_executed",
        ],
        namedStakeholders: rem.opportunity.namedStakeholders || replanPass.namedStakeholders || matured.namedStakeholders,
        publicBuyerContactEnrichment: replanPass.publicBuyerContactEnrichment || matured.publicBuyerContactEnrichment,
        supervisorV2: replanPass.supervisorV2 || matured.supervisorV2,
      };
    }
  }

  // Autonomous buyer WHO fallback — do not require founder to notice skipped buyer research
  let buyerWhoFallback = null;
  if (options.ensureBuyerWho !== false) {
    buyerWhoFallback = await ensureBuyerWhoResearchExecuted({
      opportunity,
      matured,
      researcher: fixtureResearcher,
      drainRelatedEntityFollowUpsThroughOrs,
      mergeDeepResearchIntoOpportunity,
      runPublicBuyerIntelligencePass,
      remainingBudgetUsd: options.remainingBudgetUsd ?? 1,
      targetHotel: opportunity.hotelName || LONGFORM_CANDIDATE.hotelName,
    });
    if (buyerWhoFallback.executed) {
      matured = buyerWhoFallback.matured;
    }
  }

  // VoI continue: when material commercial questions remain and alternatives exist, keep researching
  let voiContinuePass = null;
  const buyerGapOpen = !hasRoleRelevantDecisionMaker(matured.namedStakeholders || []);
  const relatedPending = (matured.pendingDiscoverySearches || []).length > 0
    || (seeded.pendingDiscoverySearches || []).length > 0
    || Number(matured.relatedEntityLeadsExecuted || 0) === 0;
  const remainBudget = Number(options.remainingBudgetUsd ?? 1);
  if (options.allowVoiContinue !== false
    && remainBudget > 0
    && (buyerGapOpen || unproductive.length || relatedPending)
    && (evidenceDrivenReplan?.worthExpectedCost !== false)) {
    const buyerQueries = buildBuyerDiscoveryQueries(opportunity, opportunity.hotelName);
    const altContinueRaw = buildAlternativeSourceCandidates({
      ...matured,
      organizationName: opportunity.organizationName,
      organizationDomain: orgHostResolved,
      investigatedUrls: [
        ...(matured.investigatedUrls || []),
        ...(matured.investigatedLeadUrls || []),
      ],
      candidateQueue: [
        ...(opportunity.candidateQueue || []),
        ...(matured.candidateQueue || []),
      ],
    });
    // run-049: do not re-queue /about|/team guesses on hosts already probed.
    const altContinue = filterExhaustedHeuristicCandidates(altContinueRaw, matured, {
      minProbesPerHost: 2,
    });
    const pendingFiltered = filterExhaustedHeuristicCandidates(
      matured.pendingDiscoverySearches || [],
      matured,
    );
    const voiHasWork = altContinue.length > 0 || pendingFiltered.length > 0;
    if (!voiHasWork) {
      matured = {
        ...matured,
        notes: [
          ...(matured.notes || []),
          buyerGapOpen
            ? "voi_continue_skipped_heuristic_hosts_exhausted"
            : "voi_continue_skipped_no_productive_alternatives",
        ],
      };
      // Finalize any stale RELATED_ENTITY_FOLLOWUPS_PENDING without executable leftovers.
      if (
        matured.stopReason === STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING ||
        countRelatedEntityFollowUpAttempts(matured) > 0
      ) {
        matured = applyRelatedEntityFollowUpDisposition(
          matured,
          resolveRelatedEntityFollowUpStop({
            state: matured,
            pendingLead: null,
            maxFollowUps: orsOptions.maxLeadFollowUpsPerOpportunity ?? 3,
            remainingBudgetUsd: remainBudget,
            paidBlocked: Boolean(orsOptions.paidBlocked),
          }),
        );
      }
    } else {
      const voiSeed = {
        ...matured,
        unresolvedQuestions: unresolvedAfterFirst,
        pendingDiscoverySearches: [...pendingFiltered, ...altContinue],
        seedUrls: [...new Set([
          ...(matured.seedUrls || []),
          ...altContinue.map((a) => a.suggestedUrl),
        ])].slice(0, 8),
        notes: [
          ...(matured.notes || []),
          "voi_continue_pass_queued",
          buyerGapOpen ? "voi_buyer_gap_open" : "voi_related_or_unproductive_pivot",
        ],
      };
      voiContinuePass = await matureOpportunity({
        opportunity: voiSeed,
        researcher: fixtureResearcher,
        options: {
          ...orsOptions,
          maxIterations: Math.min(3, orsOptions.maxIterations),
          maxLeadFollowUpsPerOpportunity: 2,
          remainingBudgetUsd: remainBudget,
        },
      });
      // Force at least one buyer-discovery action through researcher when ORS under-executes
      if (
        buyerGapOpen &&
        fixtureResearcher?.execute &&
        (voiContinuePass.relatedEntityLeadsExecuted || 0) < 1 &&
        altContinue.length
      ) {
        const forced = await fixtureResearcher.execute({
          opportunity: {
            ...voiContinuePass,
            organizationName: opportunity.organizationName,
            seedUrls: altContinue.map((a) => a.suggestedUrl).slice(0, 3),
          },
          action: {
            strategy: "related_entity_followup",
            queries: [
              ...altContinue.map((a) => a.suggestedUrl).slice(0, 2),
              ...buyerQueries.slice(0, 2),
            ],
          },
        });
        const forcedMerge = mergeDeepResearchIntoOpportunity(voiContinuePass, {
          evidence: (forced.evidence || []).map((e) => ({ ...e, relatedEntity: true, topic: "voi_continue" })),
          investigatedUrls: (forced.evidence || []).map((e) => e.url).filter(Boolean),
          namedStakeholders: forced.deepPatch?.namedStakeholdersFound || [],
        });
        voiContinuePass = {
          ...forcedMerge.opportunity,
          relatedEntityLeadsExecuted: Number(forcedMerge.opportunity.relatedEntityLeadsExecuted || 0) + 1,
          notes: [...(forcedMerge.opportunity.notes || []), "voi_continue_forced_buyer_discovery"],
        };
      }
      const voiMerge = mergeDeepResearchIntoOpportunity(matured, {
        evidence: voiContinuePass.evidence || [],
        investigatedUrls: voiContinuePass.investigatedUrls || [],
        namedStakeholders: voiContinuePass.namedStakeholders || [],
      });
      matured = {
        ...voiMerge.opportunity,
        researchAttempts: [
          ...(matured.researchAttempts || []),
          ...(voiContinuePass.researchAttempts || []).map((a) => ({ ...a, voiContinuePass: true })),
        ],
        notes: [
          ...(matured.notes || []),
          ...(voiContinuePass.notes || []),
          "deep_research_voi_continue_executed",
        ],
        relatedEntityLeadsExecuted: Math.max(
          Number(matured.relatedEntityLeadsExecuted || 0),
          Number(voiContinuePass.relatedEntityLeadsExecuted || 0),
        ) + ((voiContinuePass.notes || []).includes("voi_continue_forced_buyer_discovery") ? 1 : 0),
        namedStakeholders: voiMerge.opportunity.namedStakeholders || voiContinuePass.namedStakeholders || matured.namedStakeholders,
        publicBuyerContactEnrichment: voiContinuePass.publicBuyerContactEnrichment || matured.publicBuyerContactEnrichment,
        supervisorV2: voiContinuePass.supervisorV2 || matured.supervisorV2,
        stopReason: voiContinuePass.stopReason ?? matured.stopReason,
        nextResearchStep: voiContinuePass.nextResearchStep ?? matured.nextResearchStep,
        relatedEntityFollowUpDispositions:
          voiContinuePass.relatedEntityFollowUpDispositions ||
          matured.relatedEntityFollowUpDispositions,
      };
    }
  }

  const pageTexts = (matured.evidence || []).map((e) => e.statement || e.quote || "").filter(Boolean);
  const claims = (matured.evidence || []).map((e) => ({
    statement: e.statement || e.quote,
    url: e.url,
    accepted: true,
    provenance: e.provenance || "ors",
  }));

  const orgDomain = opportunity.organizationDomain
    || opportunity.officialDomain
    || orgHostResolved
    || (opportunity.contactEmail || opportunity.contact?.businessEmail || "").split("@")[1]
    || null;
  const contactPageUrl = opportunity.officialSource
    || (orgDomain ? `https://www.${orgDomain}/contact` : null)
    || (matured.evidence || []).find((e) => /contact/i.test(e.url || ""))?.url
    || null;

  let ownership = classifyContactOwnership({
    email: matured.contact?.businessEmail || matured.contactEmail || opportunity.contactEmail,
    pageUrl: contactPageUrl || (orgDomain ? `https://www.${orgDomain}/contact` : "https://example.invalid/contact"),
    organizationName: opportunity.organizationName,
    organizationDomain: orgDomain,
    personName: matured.contact?.personName || null,
    roleTitle: matured.contact?.roleTitle || null,
    publisherKind: "ORGANIZATION_OWNED",
  });

  // Public buyer / WHO intelligence pass (existing CI + responsibility — no Surfe/PDL)
  const buyerPass = buyerWhoFallback?.buyerPass || runPublicBuyerIntelligencePass({
    evidence: [
      ...(matured.evidence || []),
      ...pageTexts.map((t, i) => ({
        url: (matured.evidence || [])[i]?.url || contactPageUrl || "",
        statement: t,
      })),
    ],
    organizationName: opportunity.organizationName,
    organizationDomain: orgDomain || "",
    segmentId: opportunity.segmentId || opportunity.segment || "professional_planners_dmc",
    existingContactEmail: ownership.salesUsableAsProspectContact
      ? (ownership.route || matured.contactEmail || opportunity.contactEmail)
      : null,
  });
  let namedStakeholders = [
    ...(matured.namedStakeholders || []),
    ...(matured.namedStakeholdersFound || []),
    ...(matured.deepPatch?.namedStakeholdersFound || []),
    ...(buyerPass.namedStakeholders || []),
  ];
  namedStakeholders = [...new Map(
    namedStakeholders
      .filter((p) => p?.name || p?.personName)
      .map((p) => {
        const name = p.name || p.personName;
        const title = text(p.title || p.role || p.roleTitle || "")
          .replace(/\s*[–—\-]\s*[a-z0-9._%+-]*@?.*$/i, "")
          .trim() || null;
        const classified = p.responsibilityClass
          ? null
          : classifyPersonResponsibility({ title, name }, opportunity, {});
        const responsibility = p.responsibilityClass
          || classified?.class
          || null;
        const roleRelevantTitle = /hotel|sourcing|housing|meeting|event|procurement|contract|travel|planner|director|manager|coordinator/i.test(title || "");
        const dmClass = p.decisionMakerClass
          || (/CONFIRMED_RELEVANT|POTENTIALLY_RELEVANT|BOOKING/i.test(responsibility || "") && roleRelevantTitle
            ? "POTENTIAL_DECISION_MAKER"
            : "INFLUENCER_OR_GENERAL");
        return [String(name).toLowerCase(), {
          name,
          title,
          email: p.email || null,
          responsibilityClass: responsibility,
          responsibilityRationale: p.responsibilityRationale || null,
          sourceUrl: p.sourceUrl || null,
          contactDataOrigin: "PUBLIC_SOURCE",
          surfeUsed: false,
          verifiedAt: p.verifiedAt || new Date().toISOString(),
          currentOrganization: opportunity.organizationName,
          decisionMakerClass: dmClass,
        }];
      }),
  ).values()];

  const roleRelevantDm = namedStakeholders.find((s) =>
    hasRoleRelevantDecisionMaker([s]));
  if (roleRelevantDm?.name && ownership.salesUsableAsProspectContact) {
    ownership.personName = ownership.personName || roleRelevantDm.name;
    ownership.roleTitle = ownership.roleTitle || roleRelevantDm.title;
    if (roleRelevantDm.decisionMakerClass === "POTENTIAL_DECISION_MAKER") {
      ownership.ownershipClass = CONTACT_OWNERSHIP_CLASS.POTENTIAL_DECISION_MAKER;
      ownership.note = "Named stakeholder with role-relevant title on public org evidence — not Surfe-verified.";
    }
  }
  if (buyerPass.decisionMakerGap || !roleRelevantDm) {
    matured.notes = [
      ...(matured.notes || []),
      `decision_maker_gap:${buyerPass.decisionMakerGap || "role_relevant_named_buyer_not_established"}`,
    ];
  }

  const boundSourceUrl = opportunity.officialSource
    || contactPageUrl
    || (orgDomain ? `https://www.${orgDomain}/` : null)
    || (claims[0]?.url || null);
  const activityLabel = text(opportunity.hotelOpportunityThesis || opportunity.researchObjective || pageTexts[0] || "")
    .slice(0, 160)
    || `${opportunity.organizationName || "Organization"} programmes`;

  const qualificationV41 = qualifyCommercialProspectV41({
    opportunity: {
      ...matured,
      organizationName: opportunity.organizationName,
      contactEmail: ownership.salesUsableAsProspectContact ? ownership.route : null,
      hotelFitRationale: text(opportunity.hotelOpportunityThesis || opportunity.hotelSpecificFit || "")
        || `${opportunity.hotelName || "Target hotel"} fit assessed from this opportunity's evidence only.`,
      demandSignalSummary: pageTexts[0] || opportunity.researchObjective || opportunity.hotelOpportunityThesis,
      sourceUrl: boundSourceUrl,
      publisherKind: "ORGANIZATION_OWNED",
      namedHostHotel: opportunity.competingHotel || opportunity.namedHostHotel || null,
      futureCycleSupported: /COMPETITIVE_FUTURE|FUTURE_CYCLE/i.test(String(opportunity.pathway || "")),
      pathway: opportunity.pathway,
      commercialStage: opportunity.commercialStage || opportunity.pathway,
    },
    acceptedClaims: claims.length ? claims : (opportunity.acceptedClaims || []).map((c) => ({ ...c, accepted: true })),
    pageTexts: pageTexts.length ? pageTexts : [(opportunity.evidence || [])[0]?.statement || ""],
    targetHotel: opportunity.hotelName || LONGFORM_CANDIDATE.hotelName,
    usableContactRoute: ownership.salesUsableAsProspectContact ? ownership.route : null,
    pageUrl: boundSourceUrl,
  });

  const salesBrief = qualificationV41.salesBrief || buildHonestSalesBriefV41({
    targetHotel: opportunity.hotelName || LONGFORM_CANDIDATE.hotelName,
    organizationName: opportunity.organizationName,
    activity: activityLabel,
    revenueTypes: [REVENUE_TYPE.GUESTROOMS],
    disposition: qualificationV41.internalDisposition,
    contactOwnership: ownership,
    geoFit: qualificationV41.geoFit,
    sourcingStatus: "UNKNOWN",
    verifiedFacts: claims.slice(0, 5).map((c) => c.statement),
    inferredOpportunities: [
      `Inferred lodging potential from verified ${opportunity.organizationName || "organization"} activity — not a claimed ${opportunity.hotelName || "hotel"} room block`,
    ],
  });

  const hotelSalesBrief = buildOpportunityBoundHotelSalesBrief({
    opportunity: {
      ...opportunity,
      unresolvedQuestions: matured.unresolvedQuestions || opportunity.unresolvedQuestions,
      hotelOpportunityThesis: opportunity.hotelOpportunityThesis || activityLabel,
      competingHotel: opportunity.competingHotel || opportunity.namedHostHotel || null,
      pathway: opportunity.pathway || qualificationV41.internalDisposition,
    },
    ownership,
    namedStakeholders,
    qualificationV41,
    salesBrief,
    claims,
  });

  const targetedEnrichmentRequest = buyerWhoFallback?.targetedEnrichmentRequest
    || (!roleRelevantDm
      ? buildTargetedEnrichmentRequest({
        opportunity,
        publicAttempts: [
          ...(buyerWhoFallback?.buyerUrlsAttempted || []),
          ...(buyerWhoFallback?.buyerQueriesAttempted || []),
          ...((matured.investigatedUrls || []).slice(-6)),
        ],
        contactOwnership: ownership,
      })
      : null);

  const dossierResult = buildLongformResearchDossier({
    hotel: {
      hotelId: opportunity.hotelId || LONGFORM_CANDIDATE.hotelId,
      name: opportunity.hotelName || LONGFORM_CANDIDATE.hotelName,
      market: opportunity.market || opportunity.eventLocationSummary || null,
      fit: opportunity.hotelSpecificFit || opportunity.hotelOpportunityThesis || null,
    },
    organization: {
      name: opportunity.organizationName,
      pathway: opportunity.pathway || "ACCOUNT_LED",
    },
    objective: opportunity.researchObjective
      || opportunity.hotelOpportunityThesis
      || `Deepen commercial intelligence for ${opportunity.organizationName} → ${opportunity.hotelName}`,
    acceptedClaims: claims.length ? claims : (opportunity.evidence || []).map((e) => ({
      statement: e.statement || e.quote,
      url: e.url,
      accepted: true,
    })),
    rejectedClaims: matured.rejectedClaims || opportunity.rejectedClaims || [],
    unresolvedGaps: hotelSalesBrief.whatDoWeStillNotKnow,
    recommendedSalesActions: [hotelSalesBrief.whatShouldISayOrAsk].filter(Boolean),
    researchDisposition: longform?.disposition || matured.researchStatus,
    stoppingTrigger: longform?.stoppingTrigger || matured.stopReason,
    supportedInferences: hotelSalesBrief.verifiedVsInferred.inferred,
    verifiedFindings: claims,
  });

  const mergeFinal = mergeDeepResearchIntoOpportunity(matured, {
    acceptedClaims: longform?.acceptedClaims || [],
    evidence: matured.evidence,
    investigatedUrls: matured.investigatedUrls,
    rejectedClaims: matured.rejectedClaims,
    unresolvedGaps: hotelSalesBrief.whatDoWeStillNotKnow,
    relatedEntityFollowUps: longform?.relatedEntityFollowUps,
    contactOwnership: ownership,
    namedStakeholders,
    dossier: dossierResult.dossier,
    researchJobId: `deep_gdi_${opportunity.id || LONGFORM_CANDIDATE.opportunityId}`,
    stoppingTrigger: longform?.stoppingTrigger || matured.stopReason,
  });

  const contract = buildDeepResearchResultContract({
    opportunity: mergeFinal.opportunity,
    matured,
    longform,
    qualificationV41,
    dossierResult,
    salesBrief: hotelSalesBrief,
    beforeSnapshot: before,
    costs: { usd: 0, paidProviders: [], note: "offline fixtures — zero paid spend" },
    mode: "offline_fixtures",
  });

  const pubAfter = evaluateProspectPublicationEligibility(mergeFinal.opportunity);
  // Production publication gate: hotel-facing / eligibility class must not flip open.
  // Contact-completeness fields may improve when named public stakeholders are found.
  const publicationGateUnchanged =
    Boolean(pubBefore?.hotelFacing) === Boolean(pubAfter?.hotelFacing)
    && String(pubBefore?.eligibility || "") === String(pubAfter?.eligibility || "")
    && hotelSalesBrief.customerFacingPublicationEligible === false
    && (dossierResult?.dossier?.customerFacingPublicationEligible !== true);

  const researchCompleteness = assessResearchCompleteness({
    unresolvedQuestions: hotelSalesBrief.whatDoWeStillNotKnow || unresolvedAfterFirst,
    namedDecisionMakerEstablished: Boolean(roleRelevantDm?.name),
    relationshipIndependentlyVerified: retrievalAssessments.some(
      (a) => a.relationshipProbe?.independentlyCorroboratesRelationship === true,
    ),
    contactUsable: ownership.salesUsableAsProspectContact === true,
    technicalStopReason: matured.stopReason || matured.commercialStoppingTrigger || null,
    remainingBudgetUsd: options.remainingBudgetUsd ?? 1,
    justifiedAlternativesRemain: Boolean(
      evidenceDrivenReplan?.worthExpectedCost
      || Boolean(voiContinuePass)
      || Boolean(targetedEnrichmentRequest),
    ),
    commerciallyUsefulInternal: qualificationV41?.commerciallyUsefulInternal === true
      || qualificationV41?.internalAccountDevelopment === true,
  });

  // Internal GDI round-trip: enriched opportunity object (same IDs) — no Airtable write
  const canonicalGdiOpportunityAfter = attachSecondaryNamedContact(
    {
      ...mergeFinal.opportunity,
      id: opportunity.id || opportunity.opportunityId,
      opportunityId: opportunity.opportunityId || opportunity.id,
      hotelId: opportunity.hotelId,
      hotelName: opportunity.hotelName,
      organizationName: opportunity.organizationName,
      namedStakeholders,
      targetedEnrichmentRequest,
      deepResearchEnrichment: {
        version: DEEP_RESEARCH_GDI_INTEGRATION_V1,
        mergedAt: new Date().toISOString(),
        researchJobId: contract.researchJobId,
        relatedEntityLeadsExecuted:
          (matured.researchAttempts || []).filter((a) => a.strategy === "related_entity_followup").length
          + (drainExtra?.relatedEntityLeadsFollowed || 0)
          + Number(matured.relatedEntityLeadsExecuted || 0)
          + (buyerWhoFallback?.executed ? 1 : 0)
          + (voiContinuePass ? 1 : 0),
        replanPassExecuted: Boolean(replanPass),
        voiContinuePassExecuted: Boolean(voiContinuePass),
        autonomousBuyerWhoFallback: Boolean(buyerWhoFallback?.executed),
        publicBuyerEnrichment: matured.publicBuyerContactEnrichment || buyerPass || buyerWhoFallback?.buyerPass || null,
        namedStakeholders,
        salesBrief: hotelSalesBrief,
        targetedEnrichmentRequest,
        dossierAttached: Boolean(dossierResult?.dossier),
        researchCompleteness: researchCompleteness.researchCompleteness,
        publicationEligibilityUnchanged: true,
        airtableWritePerformed: false,
        secondaryNamedContactAttached: true,
      },
    },
    { candidates: namedStakeholders },
  );

  return {
    version: DEEP_RESEARCH_GDI_INTEGRATION_V1,
    before,
    after: {
      opportunity: canonicalGdiOpportunityAfter,
      qualificationV41,
      contactOwnership: ownership,
      namedStakeholders,
      relatedEntityLeadsExecuted: canonicalGdiOpportunityAfter.deepResearchEnrichment.relatedEntityLeadsExecuted,
      replanPassExecuted: Boolean(replanPass),
      voiContinuePassExecuted: Boolean(voiContinuePass),
      longformDisposition: longform?.disposition || null,
      orsStopReason: matured.stopReason || null,
      orsNotes: (matured.notes || []).slice(-16),
      publicBuyerContactEnrichment: matured.publicBuyerContactEnrichment || buyerPass || null,
      researchCompleteness,
      decisionMakerGap: buyerPass.decisionMakerGap || buyerWhoFallback?.buyerPass?.decisionMakerGap || null,
      autonomousRecovery: seeded.autonomousRecovery || null,
      targetedEnrichmentRequest,
      buyerWhoFallback: buyerWhoFallback
        ? {
          executed: buyerWhoFallback.executed,
          reason: buyerWhoFallback.reason,
          buyerUrlsAttempted: buyerWhoFallback.buyerUrlsAttempted || [],
          buyerQueriesAttempted: buyerWhoFallback.buyerQueriesAttempted || [],
        }
        : null,
    },
    dossierResult,
    hotelSalesBrief,
    contract,
    buyerWhoFallback,
    targetedEnrichmentRequest,
    citationAudit: assertDossierCitationsResolve(dossierResult),
    publicationEligibilityUnchanged: publicationGateUnchanged,
    publicationEligibilityDetail: {
      before: { eligibility: pubBefore?.eligibility, hotelFacing: pubBefore?.hotelFacing },
      after: { eligibility: pubAfter?.eligibility, hotelFacing: pubAfter?.hotelFacing, readyContactClass: pubAfter?.readyContactClass },
      note: "Hotel-facing publication gate unchanged; contact-completeness may improve from public CI.",
    },
    paidGuard,
    orsEntrypoint: "matureOpportunity",
    longformInvoked: Boolean(longform),
    drainExtra,
    retrievalAssessments,
    evidenceDrivenReplan,
    buyerIntelligence: buyerPass,
    buyerIntelligenceOfflineCohort: (() => {
      const oid = opportunity.id || opportunity.opportunityId;
      if (!BUYER_INTELLIGENCE_VALIDATION_COHORT_V1.includes(oid)) return null;
      try {
        const assessment = assessBuyerIntelligenceForOpportunity({
          opportunityId: oid,
          hotelId: opportunity.hotelId,
          hotelName: opportunity.hotelName || hotelSalesBrief?.hotelName || null,
          pathway: opportunity.pathway || null,
          opportunity,
        });
        return {
          inPrincipalOfflineCohort: true,
          liveRetrievalClaimedAsVerifiedLive: false,
          findingProvenanceClassesUsed: Object.values(FINDING_PROVENANCE),
          enrichmentJustified: assessment.enrichmentJustified,
          materialContactIntelligenceImprovement: assessment.materialContactIntelligenceImprovement,
          salesNextAction: assessment.salesNextAction,
          roleRelevantVerifiedPeople: assessment.roleRelevantVerifiedPeople,
          roleRelevantUnverifiedPeople: assessment.roleRelevantUnverifiedPeople,
          functionalIntermediaries: assessment.functionalIntermediaries,
          verifiedOrganizationContactRoutes: assessment.verifiedOrganizationContactRoutes,
          targetedEnrichmentRequest: assessment.targetedEnrichmentRequest,
          findingsSummary: assessment.findingsSummary,
          note: "Stored-evidence offline buyer assessment for shortlist cohort — does not invent stakeholders; Surfe/PDL remain unauthorized.",
        };
      } catch (err) {
        return {
          inPrincipalOfflineCohort: true,
          error: String(err?.message || err),
        };
      }
    })(),
    researchCompleteness,
    replanPass: replanPass
      ? { stopReason: replanPass.stopReason, evidenceAdded: (replanPass.evidence || []).length }
      : null,
    voiContinuePass: voiContinuePass
      ? {
        executed: true,
        stopReason: voiContinuePass.stopReason || null,
        evidenceAdded: (voiContinuePass.evidence || []).length,
        notes: (voiContinuePass.notes || []).filter((n) => /voi_/i.test(String(n))).slice(-6),
      }
      : null,
    gdiRoundTrip: {
      sameOpportunityId: canonicalGdiOpportunityAfter.id === (opportunity.id || opportunity.opportunityId),
      sameHotelId: canonicalGdiOpportunityAfter.hotelId === opportunity.hotelId,
      airtableWritePerformed: false,
      internalRepresentationUpdated: true,
    },
  };
}

/**
 * Selection rationale for the offline E2E opportunity.
 */
export function selectDeepResearchE2EOpportunity() {
  return {
    selected: LONGFORM_CANDIDATE,
    reason: [
      "Existing GDI corpus opportunity (not a new discovery wave).",
      "Identifiable organization: Principal. London DMC with own-domain evidence.",
      "Hotel-specific relevance: STRONG_TARGET_HOTEL_FIT to Nobu Portman Square (London luxury incentive catchment).",
      "Material unanswered questions: dated Nobu bedroom need; named hotel-sourcing DM.",
      "Related-entity leads: Rosewood London historical base — worth independent follow-up.",
      "Contact gap: org route known (london@principalge.com); named buyer not established.",
      "Frozen longform + deep-research certification artifacts enable BEFORE/AFTER without paid calls.",
    ].join(" "),
    artifacts: [
      "reports/gdi-autonomous-longform-research-certification-v1/",
      "reports/gdi-autonomous-deep-research-certification-v1/",
      "lib/.../longform-research-certification-v1.js LONGFORM_CANDIDATE",
    ],
    notSelectedAlternatives: [
      "V4 Eureka/AACPDM — identity/geo defects; weaker as deep-research enrichment demo",
      "Generic local catering — insufficient group-demand mechanism",
    ],
  };
}

export {
  resumeMatureOpportunity,
  LONGFORM_CANDIDATE,
  LONGFORM_HELD_URLS,
  ROOT,
  assertPaidContactProvidersDisabled,
  CONTACT_OWNERSHIP_CLASS,
};
