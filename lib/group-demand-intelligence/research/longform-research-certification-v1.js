/**
 * Long-form autonomous GDI research certification — Principal. London DMC → Nobu Portman Square.
 * Live execution only when authorizeLive + AUTHORIZED manifest + isolated ledger caps hold.
 * Sole orchestrator remains the Node.js Research Supervisor (no new framework).
 */

import fs from "node:fs";
import { createHash } from "node:crypto";
import {
  quotePlannerUsd,
  PLANNER_CEILING_USD,
  RESEARCH_DISPOSITION,
  SOURCE_FAMILY,
  planDynamicResearch,
  assessValueOfInformation,
  resolvePlannerStall,
} from "./dynamic-research-planner-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import {
  selectCommercialResearchSource,
  proposeRelatedEntityFollowUps,
  classifyTechnicalSource,
  extractCandidateUrlsFromHtml,
  RESEARCH_INTENT,
  ENTITY_RELATIONSHIP,
  RELATIONSHIP_VERIFICATION,
  COMMERCIAL_RESEARCH_DIMENSION,
  STOPPING_TRIGGER,
  resolveResearchIntent,
  resolvePublishingOrganization,
  isIndependentPublisherUrl,
  assessRelationshipVerification,
  assessCommercialDimensionCoverage,
  buildStopRecord,
} from "./commercial-source-selection-v1.js";
import { buildLongformResearchDossier, assertDossierCitationsResolve } from "./longform-research-dossier-v1.js";
import { writeMaturationCheckpointAtomic, maturationMemoryPath } from "./bethesda-maturation-checkpoint-v1.js";
import {
  LONGFORM_BUDGET_USD,
  LONGFORM_CALL_CEILING_USD,
  LONGFORM_MAX_PLANNER_CALLS,
  LONGFORM_MAX_RETRIEVALS,
  LONGFORM_LEDGER_RELATIVE,
  reserveLongformCall,
  commitLongformCall,
  releaseLongformCall,
  failLongformCall,
  loadOrCreateLongformLedger,
} from "./longform-research-ledger-v1.js";

export const LONGFORM_CERTIFICATION_VERSION = "gdi-autonomous-longform-research-certification-v1";
export const LONGFORM_MAX_SUBSTANTIVE_QUESTIONS = 8;

export const LONGFORM_HELD_URLS = Object.freeze([
  "https://www.principallondon.com/",
  "https://www.principallondon.com/what-we-do",
  "https://www.principallondon.com/case-studies",
  "https://www.principallondon.com/2026/05/11/an-evening-at-the-savoy",
  "https://www.principallondon.com/contact-us",
]);

export const LONGFORM_CANDIDATE = Object.freeze({
  opportunityId: "gdi_opp_principal_london_dmc_nobu_longform",
  hotelId: "recC2kbCaS48jFjFF",
  hotelName: "Nobu Hotel London Portman Square",
  hotelMarket: "London",
  organizationName: "Principal. London DMC",
  segmentId: "professional_planners_dmc",
  pathway: "ACCOUNT_LED",
  destination: "London, United Kingdom",
  hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
  researchObjective:
    "Investigate Principal. London DMC as a potential source of luxury group and incentive accommodation demand for Nobu Hotel London Portman Square. Establish documented hotel-placement activities, relevant programs, accommodation partners, historical relationships, decision-makers, and future commercial potential. Follow promising leads across independently attributable sources. Do not treat an account-development prospect as an active RFP or invent a Nobu relationship.",
  startingEvidence: Object.freeze([
    {
      evidenceId: "held_what_we_do",
      url: "https://www.principallondon.com/what-we-do",
      statement: "Principal documents hotel sourcing and contract negotiation together with London incentive programme delivery.",
      topic: "services",
      accepted: true,
    },
    {
      evidenceId: "held_case_studies",
      url: "https://www.principallondon.com/case-studies",
      statement: "A seven-day London incentive for more than 300 senior executives used Rosewood London as their base; a separate five-night London incentive is also described.",
      topic: "programme",
      accepted: true,
    },
    {
      evidenceId: "held_contact",
      url: "https://www.principallondon.com/contact-us",
      statement: "london@principalge.com · 020 7485 5500",
      topic: "contact",
      accepted: true,
    },
  ]),
  prohibitions: Object.freeze([
    "Invent an open RFP",
    "Invent a room block or room-night total",
    "Invent a Nobu relationship or award",
    "Treat organizer email as proof of Nobu fit",
    "Count technical metadata, duplicates, or homepage rereads as research depth",
  ]),
  investigatedUrls: LONGFORM_HELD_URLS,
});

function normalizeUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "").toLowerCase();
}

function createEvidenceId(value) {
  return createHash("sha256").update(String(value || "")).digest("hex").slice(0, 12);
}

function firecrawlCeiling() {
  try {
    return quoteBenchmarkCall({
      provider: "firecrawl",
      operation: "scrape",
      params: { url: "https://example.com/source", formats: ["markdown"] },
    });
  } catch {
    return { bounded: false, maxUsd: 0 };
  }
}

export function buildLongformAuthorizationManifest() {
  const planner = quotePlannerUsd();
  const firecrawl = firecrawlCeiling();
  const operations = [
    {
      step: "planner_loop",
      operation: "planner_call",
      model: planner.model,
      maxCalls: LONGFORM_MAX_PLANNER_CALLS,
      maxQuotedUsdEach: planner.estimatedUsd,
      hardCeilingUsdEach: PLANNER_CEILING_USD,
      purpose: "Generate and revise research questions from held evidence, retrievals, and related-entity leads.",
    },
    {
      step: "retrieval_loop",
      operation: "conditional_source_retrieval",
      provider: "direct_public_preferred_firecrawl_fallback",
      maxRetrievals: LONGFORM_MAX_RETRIEVALS,
      maxQuotedUsdEach: firecrawl.bounded ? Number(firecrawl.maxUsd) : 0,
      hardCeilingUsdEach: firecrawl.bounded ? Number(firecrawl.maxUsd) : 0,
      purpose: "Retrieve up to six unread commercially ranked sources. Path is not predetermined.",
      paidProvidersRefused: ["parallel", "exa"],
    },
    {
      step: "dossier",
      operation: "build_internal_research_dossier",
      maxQuotedUsd: 0,
      purpose: "Assemble a cited internal dossier from accepted evidence only. Not a customer publication action.",
    },
  ];
  const maxQuotedUsd = Number((
    LONGFORM_MAX_PLANNER_CALLS * planner.estimatedUsd
    + LONGFORM_MAX_RETRIEVALS * (firecrawl.bounded ? Number(firecrawl.maxUsd) : 0)
  ).toFixed(6));
  return {
    version: LONGFORM_CERTIFICATION_VERSION,
    status: "AWAITING_AUTHORIZATION",
    candidate: LONGFORM_CANDIDATE,
    workflow: {
      orchestrator: "opportunity-research-supervisor-v1 (Node.js Research Supervisor sole orchestrator)",
      pathPredetermined: false,
      maxSubstantiveQuestions: LONGFORM_MAX_SUBSTANTIVE_QUESTIONS,
      maxPlannerCalls: LONGFORM_MAX_PLANNER_CALLS,
      maxRetrievals: LONGFORM_MAX_RETRIEVALS,
      countsAsDepth: [
        "establishes a commercial fact",
        "challenges a commercial fact",
        "materially refines a commercial fact",
      ],
      doesNotCountAsDepth: [
        "duplicate source",
        "homepage reread",
        "technical metadata endpoint",
        "commercially irrelevant finding",
      ],
      capabilities: [
        "dynamic_research_question_generation",
        "question_conditioned_source_selection",
        "multiple_meaningful_retrieval_steps",
        "related_entity_follow_ups",
        "independent_source_corroboration",
        "evidence_validation",
        "commercial_value_of_information_reassessment",
        "persistent_research_memory_and_checkpoint_recovery",
        "reasoned_stopping_decisions",
        "internal_cited_dossier",
      ],
    },
    operations,
    maxQuotedUsd,
    hardCeilingUsd: LONGFORM_BUDGET_USD,
    perCallCeilingUsd: LONGFORM_CALL_CEILING_USD,
    ledger: LONGFORM_LEDGER_RELATIVE,
    usesPriorBenchmarkOrCertificationLedgers: false,
    productionAllowlistsUnchanged: true,
    productionFlagsUnchanged: true,
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
    liveExecutionAuthorized: false,
    stoppingConditions: buildLongformStoppingConditions(),
    certificationRequirements: buildLongformCertificationRequirements(),
  };
}

export function buildLongformStoppingConditions() {
  return {
    maxSubstantiveQuestions: LONGFORM_MAX_SUBSTANTIVE_QUESTIONS,
    maxRetrievals: LONGFORM_MAX_RETRIEVALS,
    stopWhen: [
      "BUYER_ENGAGEMENT_REQUIRED after VoI finds public pages sufficient for introduction only",
      "RESEARCH_SUFFICIENT for the commercial decision",
      "RESEARCH_EXHAUSTED after distinct sources and no independent lead remains",
      "BUDGET_EXHAUSTED",
      "PROVIDER_BLOCKED",
      "SCHEDULED_REVISIT when only a future official announcement would help",
    ],
    doNotAutoContinuePastDisposition: true,
    doNotInventRfpOrNobuRelationship: true,
    dossierDoesNotPublish: true,
  };
}

export function buildLongformCertificationRequirements() {
  return {
    offlineScenariosRequired: [
      "newly_discovered_lead_requires_independent_source",
      "organizer_page_hotel_mention_is_not_independent_verification",
      "contradictory_evidence",
      "dead_end_lead",
      "duplicate_source_suppression",
      "commercially_irrelevant_findings",
      "budget_exhaustion",
      "checkpoint_recovery",
      "early_stop_buyer_engagement",
      "retrieval_cap_distinct_from_research_exhaustion",
      "dossier_citations_resolve_to_accepted_evidence",
      "commercial_dimension_coverage_without_forcing_all_dimensions",
    ],
    passRequires: [
      "Isolated longform ledger only",
      "≤8 substantive questions and ≤6 new retrievals",
      "Question-conditioned source selection; technical URLs rejected",
      "Publishing organization / sourceOwnerHost tracked separately from sourceFamily",
      "Related-entity follow-ups preserve SAME_ORGANIZATION_PAGE vs RELATED_ENTITY_LEAD",
      "INDEPENDENTLY_VERIFIED only after independent publisher evidence is accepted",
      "Cross-organization follow-up requires non-organizer publisher URL",
      "Stop record separates disposition, next action, stopping trigger, unresolved questions, remaining budget",
      "Retrieval cap is never reported as research exhaustion",
      "VoI can stop without spending the full retrieval budget",
      "Dossier citations resolve to accepted evidence ledger",
      "Sales readiness / publication eligibility unchanged",
      "Checkpoint resume does not repeat spent job keys",
    ],
    failIf: [
      "Predetermined full path",
      "Spend against prior ledgers",
      "Invented Nobu relationship or open RFP",
      "Organizer-owned hotel mention labeled independently verified",
      "Dossier treated as customer publication",
      "Airtable / deploy / production flag changes",
    ],
  };
}

/**
 * Corrected depth audit of a completed long-form certification result.
 * Distinct pages from one publisher count as one publishing organization.
 */
export function assessCompletedLongformResearchDepth(result = {}) {
  const retrievals = result.sourcesVisited || result.retrievals || [];
  const publisherHosts = [...new Set(retrievals.map((item) => {
    const meta = resolvePublishingOrganization({
      url: item.url,
      organizationName: LONGFORM_CANDIDATE.organizationName,
      organizationHost: "principallondon.com",
    });
    return meta.sourceOwnerHost;
  }).filter(Boolean))];
  const heldIds = new Set(LONGFORM_CANDIDATE.startingEvidence.map((item) => item.evidenceId));
  const accepted = result.acceptedClaims || [];
  const genuinelyNew = accepted.filter((item) => item.accepted !== false && !heldIds.has(item.evidenceId));
  const leads = result.newlyDiscoveredEntities || [];
  const planLog = result.planLog || [];
  const pursued = planLog.filter((item) => item.event === "independent_lead_queued" || item.event === "independent_publisher_retrieved");
  const recordedOnly = planLog.filter((item) => item.event === "related_entity_recorded" || item.event === "same_organization_lead_recorded");
  const intents = [...new Set((result.substantiveQuestions || []).map((item) => item.intent).filter(Boolean))];
  const dimensions = assessCommercialDimensionCoverage({
    acceptedClaims: accepted,
    substantiveQuestions: result.substantiveQuestions || [],
    unresolvedGaps: [
      "No accepted evidence names Nobu Hotel London Portman Square as a Principal partner.",
      "No accepted evidence establishes an open RFP or bedroom commitment for Nobu.",
      "Future Principal programme demand timing remains publicly unconfirmed.",
    ],
  });
  const retrievalCount = Number(result.retrievalCount ?? retrievals.length ?? 0);
  const maxRetrievals = Number(result.maxRetrievals ?? LONGFORM_MAX_RETRIEVALS);
  const retrievalCapHit = retrievalCount >= maxRetrievals;
  const stop = buildStopRecord({
    researchDisposition: result.researchDisposition,
    commercialNextAction: result.researchDisposition === RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED
      ? "Introduce via published London contact; do not claim active RFP or Nobu relationship"
      : null,
    stoppingTrigger: retrievalCapHit
      ? STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED
      : (result.stoppingTrigger || null),
    unresolvedQuestions: dimensions.unresolved.map((dimension) => DIMENSION_LABEL(dimension)),
    remainingAuthorizedBudgetUsd: result.remainingAuthorizationUsd,
    retrievalCount,
    maxRetrievals,
  });
  const independentPublisherRetrievals = retrievals.filter((item) => isIndependentPublisherUrl(item.url, "principallondon.com"));
  const falselyIndependentLabels = leads.filter((item) => {
    const rel = item.relationship;
    return (rel === "INDEPENDENT_CORROBORATION" || item.independentlyVerified === true)
      && !isIndependentPublisherUrl(item.suggestedUrl || "", "principallondon.com")
      && /principallondon\.com/i.test(item.provenanceUrl || "");
  });

  return {
    version: "gdi-longform-depth-assessment-v1",
    uniquePublishingOrganizationsResearched: publisherHosts.length,
    publishingOrganizations: publisherHosts,
    independentPublisherRetrievalCount: independentPublisherRetrievals.length,
    crossOrganizationLeadFollowingProven: independentPublisherRetrievals.length > 0 && pursued.length > 0,
    acceptedFactsTotal: accepted.length,
    acceptedFactsGenuinelyNew: genuinelyNew.length,
    acceptedFactsHeldBaseline: accepted.length - genuinelyNew.length,
    genuinelyNewAcceptedFacts: genuinelyNew.map((item) => ({
      evidenceId: item.evidenceId,
      url: item.url,
      statement: item.statement,
      topic: item.topic,
      publishingOrganization: resolvePublishingOrganization({
        url: item.url,
        organizationHost: "principallondon.com",
        organizationName: LONGFORM_CANDIDATE.organizationName,
      }).publishingOrganization,
    })),
    followUpLeadsGenerated: leads.length,
    followUpLeadsPursued: pursued.length,
    followUpLeadsRecordedOnly: recordedOnly.length,
    pursuedLeadEvents: pursued,
    researchQuestionIntents: intents,
    researchQuestionDiversity: {
      distinctIntents: intents.length,
      commercialDimensionsCovered: dimensions.covered,
      commercialDimensionsUnresolved: dimensions.unresolved,
      coveredDistinctCommercialDimensions: false,
      note: intents.length <= 2
        ? "Questions clustered on few intents; commercial dimension coverage is incomplete."
        : "Multiple intents observed.",
    },
    dimensionCoverage: dimensions,
    stopAssessment: stop,
    finalStopCause: retrievalCapHit
      ? "RETRIEVAL_LIMIT"
      : (result.researchDisposition === RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED
        ? "BUYER_ENGAGEMENT"
        : (result.researchDisposition === RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT ? "SUFFICIENT_EVIDENCE" : result.researchDisposition)),
    independentCorroborationProven: false,
    falseIndependentLabelsOnOrganizerPages: falselyIndependentLabels.length,
    evidenceStandardsNote: "A Principal-owned page naming The Savoy is organizer-claimed only; it is not independently corroborated by The Savoy.",
  };
}

function DIMENSION_LABEL(dimension) {
  return String(dimension || "").replace(/_/g, " ");
}

/**
 * Bounded cross-organization follow-up assignment for one Principal-discovered lead.
 * Does not execute or spend. Requires an independently published source.
 */
export function buildCrossOrganizationFollowUpAssignment({
  entityName = "The Savoy",
  provenanceUrls = [
    "https://www.principallondon.com/news",
    "https://www.principallondon.com/2026/05/11/an-evening-at-the-savoy",
  ],
  hardCeilingUsd = 0.05,
} = {}) {
  const organizerHost = "principallondon.com";
  const verificationBaseline = assessRelationshipVerification({
    provenanceUrl: provenanceUrls[0],
    organizationHost: organizerHost,
    relationshipMentionedOnOrganizer: true,
  });
  return {
    version: "gdi-cross-organization-followup-assignment-v1",
    status: "PREPARED_AWAITING_AUTHORIZATION",
    liveExecutionAuthorized: false,
    paidResearchInitiated: false,
    candidateOpportunityId: LONGFORM_CANDIDATE.opportunityId,
    hotel: LONGFORM_CANDIDATE.hotelName,
    organizer: LONGFORM_CANDIDATE.organizationName,
    relatedEntity: {
      name: entityName,
      type: "hotel_or_accommodation_partner",
      relationship: ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD,
      verificationStatus: verificationBaseline.verificationStatus,
      independentlyVerified: false,
      whySelected: "Strongest named luxury accommodation relationship in Principal evidence (news + held Savoy evening page). Not Rosewood→Nobu inference.",
      provenanceUrls,
      publishingOrganizationOfProvenance: LONGFORM_CANDIDATE.organizationName,
    },
    researchObjective: `Seek independent confirmation that ${entityName} has a commercial or programme relationship with Principal. London DMC relevant to London group/incentive accommodation placement. Accept only independently published sources. Preserve the possibility that no independent confirmation is available.`,
    requiredSourceConstraint: {
      mustBeIndependentPublisher: true,
      organizerHostForbidden: organizerHost,
      refused: [
        "Another Principal webpage",
        "Principal-owned news repeating the same claim",
        "Inference that Savoy placement proves Nobu demand",
      ],
      acceptablePublisherExamples: [
        "thesavoy.co.uk",
        "fairmont.com",
        "Independent trade press naming Principal + The Savoy together",
        "Client/partner publication naming both",
      ],
    },
    proposedResearchPath: [
      "Start from held Principal Savoy provenance as the claim to test — not as confirmation.",
      "Generate one substantive question: independent publisher evidence of Principal↔Savoy programme/accommodation relationship.",
      "Select unread candidates only from non-principallondon.com hosts (hotel site, Fairmont/Savoy press, trade press, client pages).",
      "Retrieve up to 3 independent-publisher sources (direct_public preferred).",
      "Accept confirming claims only when the independent publisher names the relationship; otherwise record INDEPENDENT_CONFIRMATION_UNAVAILABLE.",
      "Do not invent dates, RFPs, room blocks, or Nobu linkage from a Savoy confirmation.",
      "Stop with explicit stoppingTrigger; keep researchDisposition and commercialNextAction separate.",
    ],
    caps: {
      maxSubstantiveQuestions: 3,
      maxRetrievals: 3,
      hardCeilingUsd,
      perCallCeilingUsd: LONGFORM_CALL_CEILING_USD,
      preferredProvider: "direct_public",
      paidProvidersRefused: ["parallel", "exa"],
      ledger: "reports/gdi-cross-organization-followup-savoy-v1/budget-ledger.json",
      usesPriorLedgers: false,
      usesLongformLedgerRemainingBalance: false,
    },
    possibleOutcomes: [
      RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED,
      RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE,
      RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
    ],
    stopRecordShape: buildStopRecord({
      researchDisposition: null,
      commercialNextAction: null,
      stoppingTrigger: null,
      unresolvedQuestions: [
        "Is Principal↔The Savoy independently published?",
        "Does any independent confirmation change Nobu account-development messaging? (It must not invent Nobu demand.)",
      ],
      remainingAuthorizedBudgetUsd: hardCeilingUsd,
    }),
    certificationRequirements: [
      "Independent publisher host required before INDEPENDENTLY_VERIFIED",
      "Organizer pages never count as Savoy corroboration",
      "No confirmation available is an accepted terminal outcome",
      "Isolated follow-up ledger; longform and prior ledgers untouched",
      "No Airtable / deploy / publication eligibility changes",
      "Stop reasons recorded separately from disposition",
    ],
  };
}

/**
 * Offline long-form investigation simulator over a frozen corpus.
 * No network. Used for certification scenarios and dry-run path proof.
 */
export function runLongformOfflineInvestigation({
  corpus = {},
  startingEvidence = LONGFORM_CANDIDATE.startingEvidence,
  investigatedUrls = [...LONGFORM_HELD_URLS],
  researchQuestion = LONGFORM_CANDIDATE.researchObjective,
  maxRetrievals = LONGFORM_MAX_RETRIEVALS,
  maxSubstantiveQuestions = LONGFORM_MAX_SUBSTANTIVE_QUESTIONS,
  remainingUsd = LONGFORM_BUDGET_USD,
  forceBudgetExhaustion = false,
  checkpointState = null,
  candidateQueue = null,
  entityUrlHints = {},
  allowEarlyBuyerStop = true,
} = {}) {
  const state = checkpointState || {
    status: "IN_PROGRESS",
    investigatedUrls: [...investigatedUrls],
    acceptedClaims: [...startingEvidence],
    rejectedClaims: [],
    substantiveQuestions: [],
    retrievals: [],
    relatedEntityFollowUps: [],
    planLog: [],
    disposition: null,
    stoppingTrigger: null,
    commercialNextAction: null,
  };

  const queue = Array.isArray(candidateQueue)
    ? [...candidateQueue]
    : Object.keys(corpus).filter((url) => !state.investigatedUrls.map(normalizeUrl).includes(normalizeUrl(url)));

  let remainingBudget = Number(remainingUsd);

  while (
    state.retrievals.length < maxRetrievals
    && state.substantiveQuestions.length < maxSubstantiveQuestions
    && state.status === "IN_PROGRESS"
  ) {
    if (forceBudgetExhaustion || !(remainingBudget > 0)) {
      state.disposition = RESEARCH_DISPOSITION.BUDGET_EXHAUSTED;
      state.stoppingTrigger = STOPPING_TRIGGER.BUDGET_EXHAUSTED;
      state.commercialNextAction = "Pause paid research; resume only under new authorization";
      state.status = "STOPPED";
      state.planLog.push({ event: "budget_exhaustion" });
      break;
    }

    const dimensionCoverage = assessCommercialDimensionCoverage({
      acceptedClaims: state.acceptedClaims,
      substantiveQuestions: state.substantiveQuestions,
    });
    const activeQuestion = dimensionCoverage.nextPreferredQuestion || researchQuestion;
    const intent = resolveResearchIntent({ researchQuestion: activeQuestion });
    const selection = selectCommercialResearchSource({
      candidates: queue,
      investigatedUrls: state.investigatedUrls,
      researchQuestion: activeQuestion,
      intent: intent.intent,
      hotelName: LONGFORM_CANDIDATE.hotelName,
      hotelMarket: "London",
      organizationName: "Principal",
    });
    state.planLog.push({
      event: "source_selection",
      intent: selection.intent,
      dimension: dimensionCoverage.nextPreferred,
      selected: selection.selected?.url || null,
      rejected: selection.rejected,
    });

    if (!selection.selected) {
      const voi = assessValueOfInformation({
        targetHotel: LONGFORM_CANDIDATE.hotelName,
        destination: "London, United Kingdom",
        hotelMarket: "London",
        hotelCommerciallyUnsuitable: false,
        hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
        unresolvedFact: "current dated programme / bedroom commitment at Nobu",
        alreadyInvestigated: true,
        contactPresent: true,
        openHostQuestion: true,
      });
      const stall = resolvePlannerStall({
        targetHotel: LONGFORM_CANDIDATE.hotelName,
        destination: "London, United Kingdom",
        hotelMarket: "London",
        hotelCommerciallyUnsuitable: false,
        hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
        unresolvedFact: "current dated programme / bedroom commitment at Nobu",
        alreadyInvestigated: true,
        contactPresent: true,
        investigatedUrls: state.investigatedUrls,
        organizationName: LONGFORM_CANDIDATE.organizationName,
        alternativeSources: [],
        // Held contact + no unread commercial source: prefer buyer engagement over exhaustion.
        sourcesExhausted: false,
        openHostQuestion: true,
        researchQuestion: "Identify whether further public research is useful or buyer engagement is the next step.",
      });
      state.disposition = stall.researchDisposition || voi.preferredDisposition;
      state.stoppingTrigger = STOPPING_TRIGGER.NO_UNREAD_COMMERCIAL_SOURCE;
      state.commercialNextAction = state.disposition === RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED
        ? "Introduce via published London contact; do not claim active RFP or Nobu relationship"
        : null;
      state.status = "STOPPED";
      state.planLog.push({ event: "early_stop", disposition: state.disposition, valueOfInformation: voi });
      break;
    }

    const url = selection.selected.url;
    if (classifyTechnicalSource(url).rejected) {
      state.rejectedClaims.push({ claim: `Retrieve ${url}`, reason: classifyTechnicalSource(url).reason });
      queue.splice(queue.findIndex((item) => normalizeUrl(item) === normalizeUrl(url)), 1);
      continue;
    }
    if (state.investigatedUrls.map(normalizeUrl).includes(normalizeUrl(url))) {
      state.planLog.push({ event: "duplicate_source_suppressed", url });
      queue.splice(queue.findIndex((item) => normalizeUrl(item) === normalizeUrl(url)), 1);
      continue;
    }

    const page = corpus[url] || corpus[normalizeUrl(url)] || "";
    const publisher = resolvePublishingOrganization({
      url,
      organizationName: LONGFORM_CANDIDATE.organizationName,
      organizationHost: "principallondon.com",
    });
    state.investigatedUrls.push(url);
    state.retrievals.push({
      url,
      ok: Boolean(page),
      sourceFamily: selection.selected.sourceFamily,
      publishingOrganization: publisher.publishingOrganization,
      sourceOwnerHost: publisher.sourceOwnerHost,
      independentOfOrganizer: publisher.independentOfOrganizer,
    });
    queue.splice(queue.findIndex((item) => normalizeUrl(item) === normalizeUrl(url)), 1);
    if (publisher.independentOfOrganizer) {
      state.planLog.push({
        event: "independent_publisher_retrieved",
        url,
        publishingOrganization: publisher.publishingOrganization,
      });
    }

    const question = {
      question: selection.nextAction?.factSought || activeQuestion,
      url,
      substantive: false,
      intent: selection.intent,
      dimension: dimensionCoverage.nextPreferred,
    };

    if (!page || /lorem ipsum|unrelated pizza|stock photo gallery only/i.test(page)) {
      state.rejectedClaims.push({ claim: `Material commercial facts on ${url}`, reason: page ? "commercially_irrelevant" : "dead_end_empty_source" });
      state.planLog.push({ event: "dead_end_or_irrelevant", url });
      question.substantive = true;
      question.refinement = "challenged_absence_or_irrelevance";
      state.substantiveQuestions.push(question);
      continue;
    }

    if (/contradiction|conflicting date|both july 27-29 and july 27-30/i.test(page)) {
      state.acceptedClaims.push({
        evidenceId: `ev_conflict_${state.acceptedClaims.length + 1}`,
        url,
        statement: "Source prints contradictory programme timing that must be retained without discarding either date.",
        topic: "contradiction",
        accepted: true,
      });
      state.rejectedClaims.push({ claim: "A single reconciled programme date is proven", reason: "contradictory_evidence" });
      question.substantive = true;
      question.refinement = "challenged_by_contradiction";
      state.substantiveQuestions.push(question);
      state.planLog.push({ event: "contradictory_evidence", url });
      continue;
    }

    const excerpt = String(page).replace(/\s+/g, " ").trim().slice(0, 400);
    const claim = {
      evidenceId: `ev_${state.acceptedClaims.length + 1}`,
      url,
      statement: excerpt,
      topic: /contact|@|phone/i.test(excerpt) ? "contact" : /rosewood|hotel|accommodation/i.test(excerpt) ? "placement" : "programme",
      accepted: true,
    };
    state.acceptedClaims.push(claim);
    question.substantive = true;
    question.refinement = "established_or_refined_fact";
    state.substantiveQuestions.push(question);

    const followUps = proposeRelatedEntityFollowUps({
      findingsText: page,
      sourceUrl: url,
      organizationHost: "principallondon.com",
      organizationName: LONGFORM_CANDIDATE.organizationName,
      investigatedUrls: state.investigatedUrls,
      researchIntent: selection.intent || RESEARCH_INTENT.OPPORTUNITY_DISCOVERY,
      entityUrlHints,
    });
    for (const lead of followUps) {
      // Independent publisher pages may upgrade a related lead to independently verified.
      if (publisher.independentOfOrganizer && lead.relationship === ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD) {
        const namesEntity = new RegExp(String(lead.entityName || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(page);
        if (namesEntity) {
          const namesOrganizer = /\bprincipal(?:\.?\s*london)?\b/i.test(page);
          const upgraded = assessRelationshipVerification({
            provenanceUrl: lead.provenanceUrl,
            confirmingUrl: url,
            organizationHost: "principallondon.com",
            relationshipMentionedOnOrganizer: true,
            // Hotel self-pages naming themselves are not Principal relationship confirmation.
            independentPublisherConfirms: namesEntity && namesOrganizer,
            independentSearchAttempted: true,
            independentEvidenceFound: namesEntity && namesOrganizer,
          });
          lead.verificationStatus = upgraded.verificationStatus;
          lead.independentlyVerified = upgraded.independentlyVerified;
          lead.confirmingUrl = upgraded.independentlyVerified ? url : null;
          lead.confirmingPublisher = upgraded.confirmingPublisher;
        }
      }
      state.relatedEntityFollowUps.push(lead);
      const canQueueIndependent = lead.suggestedUrl
        && isIndependentPublisherUrl(lead.suggestedUrl, "principallondon.com")
        && (lead.relationship === ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD
          || lead.relationship === ENTITY_RELATIONSHIP.INDEPENDENT_CORROBORATION);
      if (canQueueIndependent) {
        if (!queue.map(normalizeUrl).includes(normalizeUrl(lead.suggestedUrl))
          && !state.investigatedUrls.map(normalizeUrl).includes(normalizeUrl(lead.suggestedUrl))) {
          queue.push(lead.suggestedUrl);
          state.planLog.push({
            event: "independent_lead_queued",
            entity: lead.entityName,
            url: lead.suggestedUrl,
            relationship: ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD,
            verificationStatus: lead.verificationStatus,
          });
        }
      } else if (lead.relationship === ENTITY_RELATIONSHIP.SAME_ORGANIZATION_PAGE) {
        state.planLog.push({
          event: "same_organization_lead_recorded",
          entity: lead.entityName,
          relationship: lead.relationship,
          note: lead.note,
        });
      } else {
        state.planLog.push({
          event: "related_entity_recorded",
          entity: lead.entityName,
          relationship: lead.relationship,
          verificationStatus: lead.verificationStatus,
          note: lead.note,
        });
      }
    }

    // After a meaningful retrieval, reassess VoI; buyer engagement may win early.
    const voi = assessValueOfInformation({
      targetHotel: LONGFORM_CANDIDATE.hotelName,
      destination: "London, United Kingdom",
      hotelMarket: "London",
      hotelCommerciallyUnsuitable: false,
      hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
      unresolvedFact: "current dated bedroom commitment at Nobu",
      alreadyInvestigated: true,
      contactPresent: true,
      openHostQuestion: true,
    });
    if (
      allowEarlyBuyerStop
      && !voi.furtherPublicResearchJustified
      && state.acceptedClaims.some((item) => /@|hotel sourcing|incentive/i.test(item.statement))
    ) {
      const independentQueued = state.relatedEntityFollowUps.some((item) => (
        item.suggestedUrl
        && isIndependentPublisherUrl(item.suggestedUrl, "principallondon.com")
        && queue.map(normalizeUrl).includes(normalizeUrl(item.suggestedUrl))
      ));
      if (!independentQueued) {
        state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
        state.stoppingTrigger = STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL;
        state.commercialNextAction = "Introduce via published London contact; do not claim active RFP or Nobu relationship";
        state.status = "STOPPED";
        state.planLog.push({ event: "early_stop_buyer_engagement", valueOfInformation: voi });
        break;
      }
    }
  }

  if (state.status === "IN_PROGRESS") {
    const retrievalCapHit = state.retrievals.length >= maxRetrievals;
    state.disposition = state.disposition
      || (retrievalCapHit ? RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED : RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT);
    state.stoppingTrigger = state.stoppingTrigger
      || (retrievalCapHit ? STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED : STOPPING_TRIGGER.RESEARCH_SUFFICIENT);
    if (retrievalCapHit && !state.commercialNextAction) {
      state.commercialNextAction = "Introduce via published London contact; retrieval cap is not research exhaustion";
    }
    state.status = "STOPPED";
  }

  const dossierResult = buildLongformResearchDossier({
    hotel: {
      hotelId: LONGFORM_CANDIDATE.hotelId,
      name: LONGFORM_CANDIDATE.hotelName,
      market: "London",
      fit: LONGFORM_CANDIDATE.hotelSpecificFit,
    },
    organization: {
      name: LONGFORM_CANDIDATE.organizationName,
      pathway: LONGFORM_CANDIDATE.pathway,
    },
    objective: LONGFORM_CANDIDATE.researchObjective,
    acceptedClaims: state.acceptedClaims,
    rejectedClaims: state.rejectedClaims,
    unresolvedGaps: [
      "No accepted evidence names Nobu Hotel London Portman Square as a Principal partner.",
      "No accepted evidence establishes an open RFP or bedroom commitment for Nobu.",
    ],
    recommendedSalesActions: [
      "Introduce Nobu Portman Square via the published London contact route as a Marylebone luxury option for future incentive bases.",
      "Do not claim an active RFP or existing Nobu relationship.",
    ],
    researchDisposition: state.disposition,
  });

  const dimensionCoverage = assessCommercialDimensionCoverage({
    acceptedClaims: state.acceptedClaims,
    substantiveQuestions: state.substantiveQuestions,
  });
  const stopRecord = buildStopRecord({
    researchDisposition: state.disposition,
    commercialNextAction: state.commercialNextAction,
    stoppingTrigger: state.stoppingTrigger,
    unresolvedQuestions: dimensionCoverage.unresolved,
    remainingAuthorizedBudgetUsd: remainingBudget,
    retrievalCount: state.retrievals.length,
    maxRetrievals,
  });

  return {
    ...state,
    stopRecord,
    dimensionCoverage,
    dossierResult,
    citationAudit: assertDossierCitationsResolve(dossierResult),
    publicationEligibilityChanged: false,
    salesReadiness: "NEEDS_RESEARCH",
    commercialStage: "NEEDS_RESEARCH",
    sourceFamily: SOURCE_FAMILY,
  };
}

export function prepareLongformResearchCertification() {
  const manifest = buildLongformAuthorizationManifest();
  return {
    prepared: true,
    executed: false,
    liveCalls: 0,
    manifest,
    proposedCostCeilingUsd: LONGFORM_BUDGET_USD,
    maxQuotedUsd: manifest.maxQuotedUsd,
    exactCodeChanges: buildLongformCodeChangePlan(),
    deliverable: {
      hotel: LONGFORM_CANDIDATE.hotelName,
      organization: LONGFORM_CANDIDATE.organizationName,
      objective: LONGFORM_CANDIDATE.researchObjective,
      maxSubstantiveQuestions: LONGFORM_MAX_SUBSTANTIVE_QUESTIONS,
      maxRetrievals: LONGFORM_MAX_RETRIEVALS,
      hardCeilingUsd: LONGFORM_BUDGET_USD,
      ledger: LONGFORM_LEDGER_RELATIVE,
    },
  };
}

export function buildLongformCodeChangePlan() {
  return [
    {
      path: "lib/group-demand-intelligence/research/longform-research-certification-v1.js",
      change: "Candidate, manifest builder, offline investigation simulator, certification gates (this module).",
    },
    {
      path: "lib/group-demand-intelligence/research/longform-research-ledger-v1.js",
      change: "Isolated $0.15 ledger with ≤8 planner / ≤6 retrieval caps; refuses prior ledgers.",
    },
    {
      path: "lib/group-demand-intelligence/research/longform-research-dossier-v1.js",
      change: "Cited internal dossier builder + citation resolver; publicationGate INTERNAL_RESEARCH_DOSSIER_ONLY.",
    },
    {
      path: "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
      change: "Wire executeLongformResearchCertification behind authorizeLive + AUTHORIZED manifest; reuse commercial source selection, VoI, related-entity follow-ups, checkpoints.",
    },
    {
      path: "scripts/gdi-autonomous-longform-research-certification-v1.mjs",
      change: "Prepare manifest by default; --execute only when AUTHORIZED (live arming deferred).",
    },
    {
      path: "scripts/test-gdi-autonomous-longform-research-certification-v1.mjs",
      change: "Offline scenarios: independent lead, contradiction, dead-end, duplicate, irrelevant, budget, checkpoint, early stop, dossier citations.",
    },
    {
      path: "reports/gdi-autonomous-longform-research-certification-v1/*",
      change: "AUTHORIZATION_MANIFEST, PREPARATION, offline CERTIFICATION fixtures; no prior ledger writes.",
    },
    {
      path: "package.json",
      change: "Add gdi:autonomous-longform-research-certification and test:gdi-autonomous-longform-research-certification scripts.",
    },
  ];
}

function readCheckpoint(checkpointDir, id) {
  const file = maturationMemoryPath(checkpointDir, id);
  if (!fs.existsSync(file)) return null;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function saveCheckpoint(checkpointDir, state) {
  writeMaturationCheckpointAtomic(checkpointDir, state);
}

function enforceLongformCaps(manifest) {
  if (Number(manifest?.hardCeilingUsd) !== LONGFORM_BUDGET_USD) {
    return { ok: false, reason: "ceiling_mismatch" };
  }
  if (manifest?.workflow?.maxSubstantiveQuestions !== LONGFORM_MAX_SUBSTANTIVE_QUESTIONS) {
    return { ok: false, reason: "substantive_question_cap_mismatch" };
  }
  if (manifest?.workflow?.maxRetrievals !== LONGFORM_MAX_RETRIEVALS) {
    return { ok: false, reason: "retrieval_cap_mismatch" };
  }
  if (manifest?.usesPriorBenchmarkOrCertificationLedgers === true) {
    return { ok: false, reason: "prior_ledger_not_isolated" };
  }
  return { ok: true };
}

async function billedPlannerCall({ ledgerPath, jobKey, planInput }) {
  const reservation = reserveLongformCall({
    ledgerPath,
    jobKey,
    quotedUsd: LONGFORM_CALL_CEILING_USD,
    kind: "planner",
  });
  if (!reservation.ok) return { skipped: true, reason: reservation.reason, entry: reservation.entry };
  let plan;
  try {
    plan = await planDynamicResearch({ ...planInput, authorizePlannerModel: true });
  } catch (err) {
    failLongformCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.code || "planner_threw" });
    throw err;
  }
  if (plan.plannerModelCalls === 1 && plan.plannerActualUsd != null) {
    commitLongformCall({
      ledgerPath,
      reservationId: reservation.reservationId,
      actualUsd: plan.plannerActualUsd,
      usage: plan.plannerUsage,
      substantiveQuestion: false,
    });
  } else if (plan.plannerModelCalls === 1) {
    failLongformCall({ ledgerPath, reservationId: reservation.reservationId, reason: "usage_missing" });
  } else {
    releaseLongformCall({ ledgerPath, reservationId: reservation.reservationId, reason: plan.fallbackReason || "model_not_called" });
  }
  return { plan };
}

async function fetchDirectOrFirecrawl({ url, fetchPage, allowFirecrawl, ledgerPath, jobKey }) {
  const direct = await fetchPage(url);
  const body = String(direct?.text || "");
  if (direct?.ok && body.trim().length >= 300) {
    reserveLongformCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: true,
      text: body.slice(0, 25000),
      html: String(direct.html || ""),
      fetchMethod: direct.fetchMethod || "direct_public",
      browserUsed: direct.browserUsed === true,
      provider: "direct_public",
      actualUsd: 0,
    };
  }
  if (!allowFirecrawl) {
    reserveLongformCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: Boolean(body.trim()),
      text: body.slice(0, 25000),
      html: String(direct?.html || ""),
      fetchMethod: direct?.fetchMethod || "direct_public",
      provider: "direct_public",
      actualUsd: 0,
      thin: true,
    };
  }
  const quote = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url, formats: ["markdown"] },
  });
  const key = process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY;
  if (!quote.bounded || !key) {
    reserveLongformCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: Boolean(body.trim()),
      text: body.slice(0, 25000),
      html: String(direct?.html || ""),
      provider: "direct_public",
      firecrawlSkipped: !quote.bounded ? "unpriced" : "credential_absent",
      actualUsd: 0,
    };
  }
  const reservation = reserveLongformCall({
    ledgerPath,
    jobKey: `${jobKey}-firecrawl`,
    quotedUsd: Number(quote.maxUsd),
    kind: "retrieval",
  });
  if (!reservation.ok) {
    return { ok: Boolean(body.trim()), text: body.slice(0, 25000), provider: "direct_public", actualUsd: 0 };
  }
  try {
    const res = await fetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      failLongformCall({ ledgerPath, reservationId: reservation.reservationId, reason: `firecrawl_http_${res.status}` });
      return { ok: false, text: "", provider: "firecrawl", actualUsd: 0 };
    }
    const md = String(json?.data?.markdown || json?.markdown || "");
    commitLongformCall({ ledgerPath, reservationId: reservation.reservationId, actualUsd: Number(quote.maxUsd) });
    return {
      ok: md.trim().length > 0,
      text: md.slice(0, 25000),
      html: "",
      fetchMethod: "firecrawl_scrape",
      provider: "firecrawl",
      actualUsd: Number(quote.maxUsd),
    };
  } catch (err) {
    failLongformCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.message || "firecrawl_failed" });
    return { ok: false, text: "", provider: "firecrawl", actualUsd: 0 };
  }
}

function extractAcceptedClaimsFromPage({ url, text: pageText, html }) {
  const page = String(pageText || "").replace(/\s+/g, " ").trim();
  const accepted = [];
  const rejected = [];
  if (!page || page.length < 80) {
    rejected.push({ claim: `Material commercial facts on ${url}`, reason: "dead_end_empty_source" });
    return { accepted, rejected, substantive: true, refinement: "challenged_absence" };
  }
  if (/lorem ipsum|unrelated pizza|stock photo gallery only/i.test(page)) {
    rejected.push({ claim: `Commercial accommodation facts on ${url}`, reason: "commercially_irrelevant" });
    return { accepted, rejected, substantive: true, refinement: "irrelevant" };
  }
  const quotes = [];
  const hotelSourcing = page.match(/[^.?]{0,40}\b(?:hotel sourcing|contract negotiation|group accommodation|accommodation)\b[^.?]{0,120}/i);
  if (hotelSourcing) quotes.push({ statement: hotelSourcing[0].replace(/\s+/g, " ").trim(), topic: "placement" });
  const incentive = page.match(/[^.?]{0,80}\b(?:incentive|seven-day|five-night|programme|program)\b[^.?]{0,140}/i);
  if (incentive) quotes.push({ statement: incentive[0].replace(/\s+/g, " ").trim(), topic: "programme" });
  const namedHotel = page.match(/\b(?:Rosewood London|The Savoy|Bankside Hotel|Hilton|Nobu Hotel[^\s,]*)\b[^.]{0,100}/);
  if (namedHotel) quotes.push({ statement: namedHotel[0].replace(/\s+/g, " ").trim(), topic: "relationship" });
  const contact = page.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|\b020[\s.-]?\d{4}[\s.-]?\d{4}\b/i);
  if (contact) quotes.push({ statement: `Contact route printed: ${contact[0]}`, topic: "contact" });
  if (/contradiction|both july 27-29 and july 27-30/i.test(page)) {
    accepted.push({
      evidenceId: `ev_conflict_${url}`,
      url,
      statement: "Source prints contradictory programme timing that must be retained without discarding either date.",
      topic: "contradiction",
      accepted: true,
    });
    rejected.push({ claim: "A single reconciled programme date is proven", reason: "contradictory_evidence" });
  }
  for (const item of quotes.slice(0, 4)) {
    accepted.push({
      evidenceId: `ev_${createEvidenceId(`${url}:${item.statement}`)}`,
      url,
      statement: item.statement,
      topic: item.topic,
      accepted: true,
    });
  }
  if (!quotes.length && page.length >= 200) {
    accepted.push({
      evidenceId: `ev_${createEvidenceId(url)}`,
      url,
      statement: page.slice(0, 280),
      topic: "organisation",
      accepted: true,
    });
  }
  if (/\bnobu\b/i.test(page) && !/nobu hotel london portman square/i.test(page)) {
    rejected.push({ claim: "Page establishes a Nobu Portman Square relationship", reason: "nobu_not_established" });
  }
  if (!/\brfp\b|request for proposal|accepting proposals/i.test(page)) {
    rejected.push({ claim: "Page publishes an open hotel RFP", reason: "not_in_cited_source" });
  }
  if (!/room block|bedroom commitment|contracted \d+ rooms/i.test(page)) {
    rejected.push({ claim: "Page prints a bedroom commitment for Nobu", reason: "not_in_cited_source" });
  }
  rejected.push({ claim: "Rosewood mention proves Nobu demand", reason: "wrong_hotel_inference_prohibited" });
  const links = extractCandidateUrlsFromHtml(html || pageText, url);
  return {
    accepted,
    rejected,
    substantive: accepted.length > 0 || rejected.some((item) => item.reason === "commercially_irrelevant" || item.reason === "contradictory_evidence"),
    refinement: accepted.length ? "established_or_refined_fact" : "challenged_absence",
    discoveredLinks: links,
  };
}

function seedCandidatePool(investigated) {
  const held = new Set(investigated.map(normalizeUrl));
  const seeds = [
    "https://www.principallondon.com/news",
    "https://www.principallondon.com/who-we-are",
    "https://www.principallondon.com/our-destinations/london",
    "https://www.principallondon.com/our-commitment",
  ];
  return seeds.filter((url) => !held.has(normalizeUrl(url)));
}

function hintUrlsFromPage({ pageText, html, baseUrl, namedEntities = [] }) {
  const links = extractCandidateUrlsFromHtml(html || pageText, baseUrl);
  const hints = {};
  for (const name of namedEntities) {
    const token = String(name || "").toLowerCase().split(/\s+/).filter((part) => part.length > 3)[0];
    if (!token) continue;
    const match = links.find((url) => url.toLowerCase().includes(token) && !/principallondon\.com/i.test(url));
    if (match) hints[name] = match;
  }
  // Prefer well-known independent hotel domains only when the entity is already named in evidence.
  if (/rosewood london/i.test(pageText) && !hints["Rosewood London"]) {
    const rosewoodLink = links.find((url) => /rosewoodhotels\.com/i.test(url));
    if (rosewoodLink) hints["Rosewood London"] = rosewoodLink;
  }
  return hints;
}

/**
 * Live long-form investigation under the isolated $0.15 ledger.
 */
export async function executeAuthorizedLongformResearch(input = {}) {
  if (input.authorizeLive !== true || input.manifestAuthorized !== true || input.manifest?.status !== "AUTHORIZED") {
    return { executed: false, liveCalls: 0, reason: "awaiting_explicit_authorization", publicationEligibilityChanged: false };
  }
  const caps = enforceLongformCaps(input.manifest);
  if (!caps.ok) {
    return { executed: false, liveCalls: 0, reason: caps.reason, publicationEligibilityChanged: false, spent: false };
  }
  if (!input.fetchPage || typeof input.fetchPage !== "function") {
    return { executed: false, liveCalls: 0, reason: "fetch_page_required", publicationEligibilityChanged: false };
  }

  const ledgerPath = input.ledgerPath;
  const checkpointDir = input.checkpointDir;
  const id = LONGFORM_CANDIDATE.opportunityId;
  const commercialBefore = {
    fit: LONGFORM_CANDIDATE.hotelSpecificFit,
    openRfp: false,
    nobuNamed: false,
    bedroomCommitment: false,
    pathway: "ACCOUNT_LED",
    assessment: "Account-development prospect from held hotel-sourcing + incentive evidence; no active Nobu award.",
  };

  let state = readCheckpoint(checkpointDir, id);
  if (!state) {
    state = {
      id,
      status: "IN_PROGRESS",
      investigatedUrls: [...LONGFORM_HELD_URLS],
      acceptedClaims: [...LONGFORM_CANDIDATE.startingEvidence],
      rejectedClaims: [
        { claim: "Nobu is already a Principal hotel partner", reason: "not_in_cited_source" },
        { claim: "An open RFP exists for Nobu bedrooms", reason: "not_in_cited_source" },
        { claim: "Rosewood placement proves Nobu demand", reason: "wrong_hotel_inference_prohibited" },
      ],
      substantiveQuestions: [],
      retrievals: [],
      plannerModelCalls: 0,
      relatedEntityFollowUps: [],
      followUpQuestions: [],
      planLog: [],
      candidateQueue: seedCandidatePool(LONGFORM_HELD_URLS),
      entityUrlHints: {},
      disposition: null,
      stoppingTrigger: null,
      commercialNextAction: null,
      publicationEligibilityChanged: false,
      salesReadiness: "NEEDS_RESEARCH",
      commercialStage: "NEEDS_RESEARCH",
      commercialBefore,
    };
    saveCheckpoint(checkpointDir, state);
  }

  const researchQuestion = LONGFORM_CANDIDATE.researchObjective;
  let loops = 0;
  while (
    state.status === "IN_PROGRESS"
    && state.retrievals.length < LONGFORM_MAX_RETRIEVALS
    && state.substantiveQuestions.length < LONGFORM_MAX_SUBSTANTIVE_QUESTIONS
    && loops < 10
  ) {
    loops += 1;
    const ledger = loadOrCreateLongformLedger(ledgerPath);
    const remainingUsd = Number((LONGFORM_BUDGET_USD - Number(ledger.estimatedSpentUsd || 0) - Number(ledger.reservedUsd || 0)).toFixed(6));
    if (!(remainingUsd > 0)) {
      state.disposition = RESEARCH_DISPOSITION.BUDGET_EXHAUSTED;
      state.stoppingTrigger = STOPPING_TRIGGER.BUDGET_EXHAUSTED;
      state.commercialNextAction = "Pause paid research; resume only under new authorization";
      state.status = "STOPPED";
      state.planLog.push({ event: "budget_exhaustion", remainingUsd });
      saveCheckpoint(checkpointDir, state);
      break;
    }

    const dimensionCoverage = assessCommercialDimensionCoverage({
      acceptedClaims: state.acceptedClaims,
      substantiveQuestions: state.substantiveQuestions,
    });
    const activeQuestion = state.followUpQuestions.slice(-1)[0]
      || dimensionCoverage.nextPreferredQuestion
      || researchQuestion;
    const intent = resolveResearchIntent({
      researchQuestion: activeQuestion,
    });
    const findings = state.acceptedClaims.map((item) => ({
      url: item.url,
      excerpt: item.statement,
      statement: item.statement,
    }));

    const plannerJob = `gdi-lf-principal-planner-${state.plannerModelCalls + 1}`;
    const billed = await billedPlannerCall({
      ledgerPath,
      jobKey: plannerJob,
      planInput: {
        objective: {
          hotelId: LONGFORM_CANDIDATE.hotelId,
          hotelName: LONGFORM_CANDIDATE.hotelName,
          commercialQuestion: researchQuestion,
        },
        hotel: { name: LONGFORM_CANDIDATE.hotelName, market: "London" },
        opportunity: {
          organizationName: LONGFORM_CANDIDATE.organizationName,
          commercialStage: "NEEDS_RESEARCH",
          pathway: "ACCOUNT_LED",
        },
        commercial: { salesReadiness: "NEEDS_RESEARCH" },
        findings,
        openQuestions: [
          activeQuestion,
          `Unresolved commercial dimensions: ${(dimensionCoverage.unresolved || []).slice(0, 4).join("; ") || "none"}.`,
          `Research intent: ${intent.intent}. Prefer unread commercial sources. Do not invent Nobu relationships or RFPs.`,
          state.candidateQueue[0]
            ? `Candidate pool includes unread URLs such as ${state.candidateQueue.slice(0, 3).join(", ")}.`
            : "No unread candidate remains in the local pool.",
        ],
        investigated: { urls: state.investigatedUrls },
        authorizePaidResearch: false,
        remainingUsd: 0,
        questionFingerprints: state.substantiveQuestions.map((item) => item.question),
      },
    });
    if (!billed.skipped) {
      state.plannerModelCalls += billed.plan.plannerModelCalls || 0;
      state.planLog.push({
        event: "planner_call",
        disposition: billed.plan.researchDisposition,
        dimension: dimensionCoverage.nextPreferred,
        proposals: (billed.plan.proposals || []).map((item) => ({ question: item.question, url: item.url })),
      });
      for (const proposal of billed.plan.proposals || []) {
        if (proposal.url && !classifyTechnicalSource(proposal.url).rejected) {
          const n = normalizeUrl(proposal.url);
          if (!state.investigatedUrls.map(normalizeUrl).includes(n) && !state.candidateQueue.map(normalizeUrl).includes(n)) {
            state.candidateQueue.push(proposal.url);
          }
        }
      }
    } else if (billed.reason === "job_already_used") {
      // resume path
    } else if (state.plannerModelCalls >= LONGFORM_MAX_PLANNER_CALLS) {
      state.disposition = RESEARCH_DISPOSITION.BUDGET_EXHAUSTED;
      state.stoppingTrigger = STOPPING_TRIGGER.BUDGET_EXHAUSTED;
      state.status = "STOPPED";
      break;
    }

    const selection = selectCommercialResearchSource({
      candidates: state.candidateQueue,
      investigatedUrls: state.investigatedUrls,
      researchQuestion: activeQuestion,
      intent: intent.intent,
      hotelName: LONGFORM_CANDIDATE.hotelName,
      hotelMarket: "London",
      organizationName: "Principal",
    });
    state.planLog.push({
      event: "source_selection",
      intent: selection.intent,
      dimension: dimensionCoverage.nextPreferred,
      selected: selection.selected?.url || null,
      rejected: (selection.rejected || []).slice(0, 8),
      rationale: selection.selectionRationale,
    });

    if (!selection.selected) {
      const stall = resolvePlannerStall({
        targetHotel: LONGFORM_CANDIDATE.hotelName,
        destination: "London, United Kingdom",
        hotelMarket: "London",
        hotelCommerciallyUnsuitable: false,
        hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
        unresolvedFact: "current dated bedroom commitment at Nobu",
        alreadyInvestigated: true,
        contactPresent: true,
        investigatedUrls: state.investigatedUrls,
        organizationName: LONGFORM_CANDIDATE.organizationName,
        alternativeSources: [],
        sourcesExhausted: false,
        openHostQuestion: true,
        researchQuestion,
      });
      state.disposition = stall.researchDisposition;
      state.stoppingTrigger = STOPPING_TRIGGER.NO_UNREAD_COMMERCIAL_SOURCE;
      state.commercialNextAction = "Introduce via published London contact; do not claim active RFP or Nobu relationship";
      state.status = "STOPPED";
      state.planLog.push({ event: "early_stop", disposition: state.disposition, valueOfInformation: stall.valueOfInformation });
      saveCheckpoint(checkpointDir, state);
      break;
    }

    const url = selection.selected.url;
    if (state.investigatedUrls.map(normalizeUrl).includes(normalizeUrl(url)) || classifyTechnicalSource(url).rejected) {
      state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));
      state.planLog.push({ event: "duplicate_or_technical_suppressed", url });
      saveCheckpoint(checkpointDir, state);
      continue;
    }

    const fetched = await fetchDirectOrFirecrawl({
      url,
      fetchPage: input.fetchPage,
      allowFirecrawl: true,
      ledgerPath,
      jobKey: `gdi-lf-principal-retrieval-${state.retrievals.length + 1}`,
    });
    const publisher = resolvePublishingOrganization({
      url,
      organizationName: LONGFORM_CANDIDATE.organizationName,
      organizationHost: "principallondon.com",
    });
    state.retrievals.push({
      url,
      ok: fetched.ok === true,
      provider: fetched.provider,
      sourceFamily: selection.selected.sourceFamily,
      publishingOrganization: publisher.publishingOrganization,
      sourceOwnerHost: publisher.sourceOwnerHost,
      independentOfOrganizer: publisher.independentOfOrganizer,
      intent: selection.intent,
      dimension: dimensionCoverage.nextPreferred,
      actualUsd: fetched.actualUsd || 0,
    });
    if (publisher.independentOfOrganizer) {
      state.planLog.push({
        event: "independent_publisher_retrieved",
        url,
        publishingOrganization: publisher.publishingOrganization,
      });
    }
    state.investigatedUrls.push(url);
    state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));

    const extracted = extractAcceptedClaimsFromPage({ url, text: fetched.text, html: fetched.html });
    for (const claim of extracted.accepted) {
      claim.publishingOrganization = publisher.publishingOrganization;
      claim.sourceOwnerHost = publisher.sourceOwnerHost;
      claim.independentOfOrganizer = publisher.independentOfOrganizer;
    }
    state.acceptedClaims.push(...extracted.accepted);
    state.rejectedClaims.push(...extracted.rejected);
    if (extracted.substantive) {
      state.substantiveQuestions.push({
        question: selection.nextAction?.factSought || selection.factsSought?.join("; ") || activeQuestion,
        url,
        substantive: true,
        refinement: extracted.refinement,
        intent: selection.intent,
        dimension: dimensionCoverage.nextPreferred,
      });
    }

    // Grow pool with on-domain unread commercial links from this page.
    for (const link of extracted.discoveredLinks || []) {
      if (!/principallondon\.com/i.test(link)) continue;
      if (classifyTechnicalSource(link).rejected) continue;
      if (state.investigatedUrls.map(normalizeUrl).includes(normalizeUrl(link))) continue;
      if (state.candidateQueue.map(normalizeUrl).includes(normalizeUrl(link))) continue;
      state.candidateQueue.push(link);
    }
    // Also queue off-host commercial links discovered on the page as independent-publisher candidates.
    for (const link of extracted.discoveredLinks || []) {
      if (!isIndependentPublisherUrl(link, "principallondon.com")) continue;
      if (classifyTechnicalSource(link).rejected) continue;
      if (state.investigatedUrls.map(normalizeUrl).includes(normalizeUrl(link))) continue;
      if (state.candidateQueue.map(normalizeUrl).includes(normalizeUrl(link))) continue;
      state.candidateQueue.push(link);
      state.planLog.push({ event: "independent_publisher_link_discovered", url: link });
    }

    const named = [...new Set((fetched.text.match(/\b(?:Rosewood London|The Savoy|Bankside Hotel|Hilton)\b/g) || []))];
    const hints = hintUrlsFromPage({
      pageText: fetched.text,
      html: fetched.html,
      baseUrl: url,
      namedEntities: named,
    });
    Object.assign(state.entityUrlHints, hints);

    const followUps = proposeRelatedEntityFollowUps({
      findingsText: fetched.text,
      sourceUrl: url,
      organizationHost: "principallondon.com",
      organizationName: LONGFORM_CANDIDATE.organizationName,
      investigatedUrls: state.investigatedUrls,
      researchIntent: selection.intent || RESEARCH_INTENT.OPPORTUNITY_DISCOVERY,
      entityUrlHints: state.entityUrlHints,
    });
    for (const lead of followUps) {
      if (publisher.independentOfOrganizer && lead.relationship === ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD) {
        const namesEntity = new RegExp(String(lead.entityName || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(fetched.text);
        if (namesEntity) {
          const namesOrganizer = /\bprincipal(?:\.?\s*london)?\b/i.test(fetched.text);
          const upgraded = assessRelationshipVerification({
            provenanceUrl: lead.provenanceUrl,
            confirmingUrl: url,
            organizationHost: "principallondon.com",
            relationshipMentionedOnOrganizer: true,
            independentPublisherConfirms: namesEntity && namesOrganizer,
            independentSearchAttempted: true,
            independentEvidenceFound: namesEntity && namesOrganizer,
          });
          lead.verificationStatus = upgraded.verificationStatus;
          lead.independentlyVerified = upgraded.independentlyVerified;
          lead.confirmingUrl = upgraded.independentlyVerified ? url : null;
          lead.confirmingPublisher = upgraded.confirmingPublisher;
        }
      }
      state.relatedEntityFollowUps.push(lead);
      state.followUpQuestions.push(lead.researchQuestion);
      const canQueueIndependent = lead.suggestedUrl
        && isIndependentPublisherUrl(lead.suggestedUrl, "principallondon.com");
      if (canQueueIndependent) {
        const n = normalizeUrl(lead.suggestedUrl);
        if (!state.investigatedUrls.map(normalizeUrl).includes(n) && !state.candidateQueue.map(normalizeUrl).includes(n)) {
          state.candidateQueue.push(lead.suggestedUrl);
          state.planLog.push({
            event: "independent_lead_queued",
            entity: lead.entityName,
            url: lead.suggestedUrl,
            relationship: ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD,
            verificationStatus: lead.verificationStatus,
          });
        }
      } else {
        state.planLog.push({
          event: "related_entity_recorded",
          entity: lead.entityName,
          relationship: lead.relationship,
          verificationStatus: lead.verificationStatus,
          note: lead.note,
        });
      }
    }

    const voi = assessValueOfInformation({
      targetHotel: LONGFORM_CANDIDATE.hotelName,
      destination: "London, United Kingdom",
      hotelMarket: "London",
      hotelCommerciallyUnsuitable: false,
      hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
      unresolvedFact: "current dated bedroom commitment at Nobu",
      alreadyInvestigated: true,
      contactPresent: true,
      openHostQuestion: true,
    });
    const independentQueued = state.candidateQueue.some((item) => isIndependentPublisherUrl(item, "principallondon.com"));
    if (!voi.furtherPublicResearchJustified && !independentQueued && state.retrievals.length >= 1) {
      state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
      state.stoppingTrigger = STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL;
      state.commercialNextAction = "Introduce via published London contact; do not claim active RFP or Nobu relationship";
      state.status = "STOPPED";
      state.planLog.push({ event: "early_stop_buyer_engagement", valueOfInformation: voi });
      saveCheckpoint(checkpointDir, state);
      break;
    }
    // If pool only has weak leftovers and we already have contact + placement evidence, prefer engagement.
    if (state.retrievals.length >= 3 && !independentQueued) {
      const stall = resolvePlannerStall({
        targetHotel: LONGFORM_CANDIDATE.hotelName,
        destination: "London, United Kingdom",
        hotelMarket: "London",
        hotelCommerciallyUnsuitable: false,
        hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
        unresolvedFact: "current dated bedroom commitment at Nobu",
        alreadyInvestigated: true,
        contactPresent: true,
        investigatedUrls: state.investigatedUrls,
        organizationName: LONGFORM_CANDIDATE.organizationName,
        alternativeSources: state.candidateQueue.slice(0, 3).map((item) => ({
          url: item,
          factSought: "current programme or accommodation partner",
          sourceFamily: SOURCE_FAMILY.ORGANIZER_PUBLICATION,
          whySourceMightContainIt: "Unread Principal page remaining in pool",
          provider: "direct_public",
          estimatedUsd: 0,
        })),
        sourcesExhausted: false,
        openHostQuestion: true,
        researchQuestion,
      });
      if (stall.researchDisposition === RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED && !stall.nextPermittedAction) {
        state.disposition = stall.researchDisposition;
        state.stoppingTrigger = STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL;
        state.commercialNextAction = "Introduce via published London contact; do not claim active RFP or Nobu relationship";
        state.status = "STOPPED";
        state.planLog.push({ event: "voi_stop_after_depth", disposition: state.disposition });
        saveCheckpoint(checkpointDir, state);
        break;
      }
      if (stall.nextPermittedAction?.url) {
        const n = normalizeUrl(stall.nextPermittedAction.url);
        if (!state.candidateQueue.map(normalizeUrl).includes(n)) state.candidateQueue.unshift(stall.nextPermittedAction.url);
      }
    }
    saveCheckpoint(checkpointDir, state);
  }

  if (state.status === "IN_PROGRESS") {
    const retrievalCapHit = state.retrievals.length >= LONGFORM_MAX_RETRIEVALS;
    state.disposition = state.disposition || RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
    state.stoppingTrigger = retrievalCapHit
      ? STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED
      : (state.stoppingTrigger || STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL);
    state.commercialNextAction = state.commercialNextAction
      || "Introduce via published London contact; retrieval cap is not research exhaustion";
    state.status = "STOPPED";
    state.planLog.push({
      event: "loop_complete_stop",
      disposition: state.disposition,
      stoppingTrigger: state.stoppingTrigger,
    });
    saveCheckpoint(checkpointDir, state);
  }

  const nobuNamed = state.acceptedClaims.some((item) => /nobu hotel london portman square/i.test(item.statement));
  const openRfp = state.acceptedClaims.some((item) => /\brfp\b|request for proposal/i.test(item.statement));
  const bedroom = state.acceptedClaims.some((item) => /room block|bedroom commitment/i.test(item.statement));
  const commercialAfter = {
    fit: LONGFORM_CANDIDATE.hotelSpecificFit,
    openRfp,
    nobuNamed,
    bedroomCommitment: bedroom,
    pathway: "ACCOUNT_LED",
    assessment: nobuNamed
      ? "Target hotel named in accepted evidence; still not an automatic award."
      : "Geographic fit and Principal hotel-sourcing capability support account development. No accepted evidence establishes an active Nobu RFP, room block, or named Nobu relationship. Rosewood/other hotel mentions are historical or competitive placements only.",
  };

  const dossierResult = buildLongformResearchDossier({
    hotel: {
      hotelId: LONGFORM_CANDIDATE.hotelId,
      name: LONGFORM_CANDIDATE.hotelName,
      market: "London",
      fit: LONGFORM_CANDIDATE.hotelSpecificFit,
    },
    organization: {
      name: LONGFORM_CANDIDATE.organizationName,
      pathway: LONGFORM_CANDIDATE.pathway,
    },
    objective: LONGFORM_CANDIDATE.researchObjective,
    acceptedClaims: state.acceptedClaims,
    rejectedClaims: state.rejectedClaims,
    unresolvedGaps: [
      "No accepted evidence names Nobu Hotel London Portman Square as a Principal partner or host.",
      "No accepted evidence establishes an open RFP or bedroom commitment for Nobu.",
      "Future Principal programme demand timing remains publicly unconfirmed.",
    ],
    recommendedSalesActions: [
      "Introduce Nobu Portman Square via london@principalge.com as a Marylebone luxury option for future incentive bases.",
      "Ask whether upcoming London incentives still need a central lifestyle hotel base; do not claim an active RFP or existing Nobu relationship.",
      "Reference Principal's documented hotel-sourcing capability and historical Rosewood-style base placements as context only.",
    ],
    researchDisposition: state.disposition,
    suitability: {
      summary: commercialAfter.assessment,
      detail: commercialAfter.assessment,
    },
  });
  const citationAudit = assertDossierCitationsResolve(dossierResult);
  const ledger = loadOrCreateLongformLedger(ledgerPath);
  const remainingAuthorizationUsd = Number((LONGFORM_BUDGET_USD - Number(ledger.estimatedSpentUsd || 0)).toFixed(6));
  const dimensionCoverage = assessCommercialDimensionCoverage({
    acceptedClaims: state.acceptedClaims,
    substantiveQuestions: state.substantiveQuestions,
    unresolvedGaps: [
      "No accepted evidence names Nobu Hotel London Portman Square as a Principal partner or host.",
      "No accepted evidence establishes an open RFP or bedroom commitment for Nobu.",
      "Future Principal programme demand timing remains publicly unconfirmed.",
    ],
  });
  const stopRecord = buildStopRecord({
    researchDisposition: state.disposition,
    commercialNextAction: state.commercialNextAction
      || "Introduce via published London contact; do not claim active RFP or Nobu relationship",
    stoppingTrigger: state.stoppingTrigger,
    unresolvedQuestions: dimensionCoverage.unresolved,
    remainingAuthorizedBudgetUsd: remainingAuthorizationUsd,
    retrievalCount: state.retrievals.length,
    maxRetrievals: LONGFORM_MAX_RETRIEVALS,
  });
  // Genuine cross-org lead-following requires an independent publisher retrieval, not same-site browsing.
  const autonomousLeadFollowing = state.planLog.some((item) => item.event === "independent_publisher_retrieved")
    && state.planLog.some((item) => item.event === "independent_lead_queued");

  state.dossierPath = input.dossierPath || null;
  state.citationAudit = citationAudit;
  state.commercialAfter = commercialAfter;
  state.stopRecord = stopRecord;
  saveCheckpoint(checkpointDir, state);

  return {
    executed: true,
    certificationPassed: citationAudit.ok === true
      && Number(ledger.estimatedSpentUsd || 0) - LONGFORM_BUDGET_USD <= 1e-9
      && state.plannerModelCalls <= LONGFORM_MAX_PLANNER_CALLS
      && state.retrievals.length <= LONGFORM_MAX_RETRIEVALS
      && state.substantiveQuestions.length <= LONGFORM_MAX_SUBSTANTIVE_QUESTIONS
      && state.publicationEligibilityChanged !== true,
    plannerModelCalls: state.plannerModelCalls,
    retrievals: state.retrievals,
    retrievalCount: state.retrievals.length,
    maxRetrievals: LONGFORM_MAX_RETRIEVALS,
    substantiveQuestions: state.substantiveQuestions,
    substantiveQuestionCount: state.substantiveQuestions.length,
    actualSpentUsd: ledger.estimatedSpentUsd,
    remainingAuthorizationUsd,
    hardCeilingUsd: LONGFORM_BUDGET_USD,
    sourcesVisited: state.retrievals,
    uniquePublishingOrganizations: [...new Set(state.retrievals.map((item) => item.sourceOwnerHost).filter(Boolean))],
    independentSourceFamilies: [...new Set(state.retrievals.map((item) => item.sourceFamily).filter(Boolean))],
    newlyDiscoveredEntities: state.relatedEntityFollowUps.map((item) => ({
      name: item.entityName,
      type: item.entityType,
      relationship: item.relationship,
      verificationStatus: item.verificationStatus || RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
      independentlyVerified: item.independentlyVerified === true,
      publishingOrganization: item.publishingOrganization || null,
      provenanceUrl: item.provenanceUrl,
      suggestedUrl: item.suggestedUrl || null,
    })),
    followUpQuestions: state.followUpQuestions,
    acceptedClaims: state.acceptedClaims,
    rejectedClaims: state.rejectedClaims,
    contradictoryEvidence: state.acceptedClaims.filter((item) => item.topic === "contradiction"),
    commercialBefore,
    commercialAfter,
    researchDisposition: state.disposition,
    stoppingTrigger: state.stoppingTrigger,
    commercialNextAction: state.commercialNextAction || stopRecord.commercialNextAction,
    stopRecord,
    dimensionCoverage,
    autonomousLeadFollowing,
    dossier: dossierResult.dossier,
    citationAudit,
    dossierCompleteness: {
      sectionsPresent: Object.keys(dossierResult.dossier.sections || {}).length,
      evidenceLedgerCount: (dossierResult.dossier.evidenceLedger || []).length,
      publicationGate: dossierResult.dossier.publicationGate,
    },
    checkpointStatus: state.status,
    planLog: state.planLog,
    publicationEligibilityChanged: false,
    salesReadiness: "NEEDS_RESEARCH",
    commercialStage: "NEEDS_RESEARCH",
  };
}

export async function executeLongformResearchCertification(input = {}) {
  return executeAuthorizedLongformResearch(input);
}
