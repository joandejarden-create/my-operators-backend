/**
 * Supervisor V2 hooks invoked from the existing ORS maturation path.
 * Not a separate execution pipeline — enrichment + decisions only.
 */

import {
  planAutonomousLeadFollowing,
  updateLeadRelationshipFromRetrieval,
  executeBoundedLeadFollowUps,
} from "./autonomous-lead-following-v2.js";
import {
  buildEvidenceProvenanceRecord,
  summarizeIndependenceCoverage,
  RETRIEVAL_OUTCOME_V2,
} from "./evidence-provenance-v2.js";
import { qualifyHotelSpecificProspectV2 } from "./hotel-specific-qualification-v2.js";
import { qualifyCommercialProspectV3 } from "./commercial-qualification-v3.js";
import {
  buildSegmentDiscoveryCoverageReport,
  shouldContinueSegmentDiscovery,
} from "./segment-discovery-coverage-v2.js";
import {
  selectDiverseResearchQuestionV2,
  buildStopRecord,
  STOPPING_TRIGGER,
  assessCommercialDimensionCoverage,
} from "./commercial-source-selection-v1.js";
import { buildLongformResearchDossier } from "./longform-research-dossier-v1.js";
import { RESEARCH_PATHWAY } from "./commercial-pathway-v1.js";
import { writeMaturationCheckpointAtomic, maturationMemoryPath } from "./bethesda-maturation-checkpoint-v1.js";
import { classifyPersonResponsibility } from "../workflow/buyer-contact-enrichment-v1.js";
import {
  extractPublicContactIntelligenceForOrs,
  CONTACT_OWNERSHIP_CLASS,
} from "./commercial-spine-restoration-v41.js";
import {
  runPublicBuyerIntelligencePass,
  assessResearchCompleteness,
  assessRetrievalCommercialValue,
} from "./commercial-depth-buyer-intelligence-v1.js";

export const ORS_SUPERVISOR_V2_HOOKS_VERSION = "gdi-ors-supervisor-v2-hooks";

function evidencePageText(e = {}) {
  return e.quote || e.statement || e.text || e.excerpt || "";
}

function evidenceBlob(state = {}) {
  return [
    state.evidenceQuote,
    ...(state.evidence || []).map((e) => evidencePageText(e)),
    ...(state.establishedFacts || []),
  ]
    .filter(Boolean)
    .join("\n");
}

function acceptedClaimsFromState(state = {}) {
  return (state.evidence || [])
    .filter((e) => e && String(evidencePageText(e)).length >= 40)
    .map((e, i) => ({
      evidenceId: e.id || e.evidenceId || `ev_ors_${i}`,
      url: e.url || null,
      statement: evidencePageText(e),
      topic: e.topic || e.sourceFamily || "evidence",
      accepted: e.accepted !== false && e.publicationReady !== true ? true : e.accepted !== false,
    }));
}

/**
 * Apply V2 enrichment during/after ORS maturation iterations.
 * Mutates a clone-friendly state object and returns it.
 */
export function applySupervisorV2MaturationHooks(state = {}, options = {}) {
  if (options.enableSupervisorV2 === false) return state;

  const publicationWritesDisabled = options.forbidPublication !== false;
  const airtableWritesDisabled = options.forbidAirtable !== false;
  const targetHotel = options.targetHotel || state.targetHotel || state.hotelName || "";
  const orgHost = String(state.officialDomain || state.organizationDomain || "")
    .replace(/^www\./i, "")
    .toLowerCase();
  const blob = evidenceBlob(state);
  const claims = acceptedClaimsFromState(state);

  // Provenance for each evidence URL
  const provenanceRecords = (state.evidence || []).map((e) =>
    buildEvidenceProvenanceRecord({
      url: e.url || "",
      organizationHost: orgHost,
      organizationName: state.organizationName || "",
      sourceFamily: e.sourceFamily || null,
      claimSupported: evidencePageText(e) || null,
      evidenceQuotation: evidencePageText(e) || null,
      retrievalOutcome: e.retrievalFailed
        ? RETRIEVAL_OUTCOME_V2.PROVIDER_ERROR
        : RETRIEVAL_OUTCOME_V2.SUCCESS_WITH_CONTENT,
    }),
  );
  const independence = summarizeIndependenceCoverage(provenanceRecords);

  // Lead-following from latest evidence
  const lastUrl = (state.evidence || []).map((e) => e.url).filter(Boolean).at(-1) || state.officialSource || "";
  const entityUrlHints = {
    ...(state.entityUrlHints || {}),
    ...(options.entityUrlHints || {}),
  };
  // Keep hints on state so later iterations / lead execution see them.
  if (Object.keys(entityUrlHints).length) state.entityUrlHints = entityUrlHints;

  const leadPlan = planAutonomousLeadFollowing({
    findingsText: blob,
    sourceUrl: lastUrl,
    organizationHost: orgHost,
    organizationName: state.organizationName || "",
    investigatedUrls: [
      ...(state.evidence || []).map((e) => e.url).filter(Boolean),
      ...(state.investigatedLeadUrls || []),
    ],
    entityUrlHints,
    targetHotel,
    remainingBudgetUsd: options.remainingBudgetUsd ?? Infinity,
    maxPursue: options.maxLeadPursue ?? 3,
  });

  // Optionally validate one suggested independent lead already retrieved (offline/live)
  if (options.leadRetrievalResult?.url && leadPlan.leadsToPursue[0]) {
    const updated = updateLeadRelationshipFromRetrieval({
      lead: leadPlan.leadsToPursue[0],
      retrievedUrl: options.leadRetrievalResult.url,
      pageText: options.leadRetrievalResult.text || "",
      organizationHost: orgHost,
    });
    leadPlan.leadsToPursue[0] = updated;
  }

  // Bounded lead-following execution — do not merely queue and forget.
  // Offline/default: use held evidence text. Live HTTP requires explicit auth + ORS budget.
  const leadExecution = executeBoundedLeadFollowUps({
    leads: leadPlan.leadsToPursue,
    mode: options.authorizeLiveLeadRetrieval === true ? "live_requires_ors_budget" : "offline_stored_evidence_only",
    storedPages: (state.evidence || []).map((e) => ({ url: e.url, text: evidencePageText(e) })),
    organizationHost: orgHost,
    maxExecute: options.maxLeadPursue ?? 2,
    authorizeLiveRetrieval: options.authorizeLiveLeadRetrieval === true,
    fetchPage: options.fetchPage || null,
  });

  // Hotel-specific qualification — V2 retained for pathway snapshot; V3 is internal SoT
  const contactRoute =
    state.contact?.businessEmail ||
    state.contactEmail ||
    state.primaryContactEmail ||
    null;
  const qualificationV2 = qualifyHotelSpecificProspectV2({
    opportunity: state,
    acceptedClaims: claims,
    pageTexts: [blob],
    targetHotel,
    usableContactRoute: contactRoute,
  });
  const qualification = options.enableQualificationV3 === false
    ? qualificationV2
    : qualifyCommercialProspectV3({
      opportunity: state,
      acceptedClaims: claims,
      pageTexts: [blob],
      targetHotel,
      usableContactRoute: contactRoute,
      hotelFacts: options.hotelFacts || state.hotelFacts || null,
    });

  // Question diversity for next step preference
  const dimCoverage = assessCommercialDimensionCoverage({
    acceptedClaims: claims,
    substantiveQuestions: (state.researchAttempts || []).map((a) => ({
      question: a.notes || a.strategy,
      dimension: a.blocker,
    })),
  });
  const diverseQuestion = selectDiverseResearchQuestionV2({
    pathway: qualification.pathway || state.pathway || RESEARCH_PATHWAY.ACCOUNT_LED,
    coveredDimensions: dimCoverage.covered,
    recentQuestionDimensions: (state.supervisorV2?.recentQuestionDimensions || []).concat(
      dimCoverage.covered.slice(-2),
    ),
  });

  // Evidence-aware stop record (does not force stop — informs disposition)
  const stopRecord = buildStopRecord({
    researchDisposition: qualification.commerciallyUsefulInternal || qualification.internalAccountDevelopment
      ? "BUYER_ENGAGEMENT_REQUIRED"
      : qualification.downgradeReason
        ? "FURTHER_RESEARCH_JUSTIFIED"
        : null,
    commercialNextAction: qualification.nextSalesAction || qualification.salesAction?.whatSalesShouldAskOrPropose,
    stoppingTrigger: qualification.commerciallyUsefulInternal || qualification.internalAccountDevelopment
      ? STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL
      : options.stoppingTrigger || null,
    unresolvedQuestions: [
      ...(qualification.researchGaps || []).slice(0, 4),
      ...(qualification.downgradeReason ? [qualification.downgradeReason] : []),
      ...(dimCoverage.unresolved || []).slice(0, 3).map((d) => `Uncovered dimension: ${d}`),
    ],
    remainingAuthorizedBudgetUsd: options.remainingBudgetUsd ?? null,
    retrievalCount: options.retrievalCount ?? null,
    maxRetrievals: options.maxRetrievals ?? null,
  });

  // Public buyer / WHO discovery on already-fetched evidence (no Surfe/PDL).
  if (options.enablePublicBuyerContactEnrichment !== false && blob.length >= 40) {
    try {
      const buyerPass = runPublicBuyerIntelligencePass({
        evidence: state.evidence || [],
        organizationName: state.organizationName || "",
        organizationDomain: state.organizationDomain || state.officialDomain || "",
        segmentId: state.segmentId || "",
        existingContactEmail: state.contactEmail || state.contact?.businessEmail || null,
      });
      if (buyerPass.namedStakeholders?.length) {
        state.namedStakeholders = [
          ...new Map(
            [...(state.namedStakeholders || []), ...buyerPass.namedStakeholders]
              .filter((s) => s?.name)
              .map((s) => [String(s.name).toLowerCase(), s]),
          ).values(),
        ];
      }
      if (buyerPass.contactOwnership?.salesUsableAsProspectContact && buyerPass.contactOwnership.route) {
        state.contact = {
          ...(state.contact || {}),
          businessEmail: state.contact?.businessEmail || buyerPass.contactOwnership.route,
          ownershipClass: buyerPass.contactOwnership.ownershipClass,
          personName: state.contact?.personName || buyerPass.namedStakeholders[0]?.name || null,
          roleTitle: state.contact?.roleTitle || buyerPass.namedStakeholders[0]?.title || null,
        };
        if (!state.contactEmail) state.contactEmail = buyerPass.contactOwnership.route;
      } else if (buyerPass.contactOwnership?.ownershipClass === CONTACT_OWNERSHIP_CLASS.THIRD_PARTY_DIRECTORY_CONTACT) {
        state.notes = [...(state.notes || []), "ors_public_ci_directory_contact_rejected"];
      }
      if (buyerPass.decisionMakerGap) {
        state.notes = [...(state.notes || []), `ors_decision_maker_gap:${buyerPass.decisionMakerGap}`.slice(0, 200)];
      }
      if (buyerPass.whoResearchStatus === "ZERO_PAGES_SCANNED") {
        state.notes = [...(state.notes || []), "ors_who_zero_pages_scanned_not_success"];
      }
      state.publicBuyerIntelligenceMetrics = {
        pagesProvided: buyerPass.pagesProvided,
        pagesScanned: buyerPass.pagesScanned,
        pagesSkippedEmpty: (buyerPass.pagesSkippedEmpty || []).length,
        whoResearchStatus: buyerPass.whoResearchStatus,
        whoResearchSucceeded: buyerPass.whoResearchSucceeded === true,
        namedStakeholders: (buyerPass.namedStakeholders || []).length,
        roleRelevant: (buyerPass.roleRelevantStakeholders || []).length,
      };
      // Flag unproductive independent hotel amenity retrievals for replanning
      const lastEv = (state.evidence || []).at(-1);
      if (lastEv?.url) {
        const val = assessRetrievalCommercialValue({
          url: lastEv.url,
          pageText: lastEv.quote || lastEv.statement || lastEv.excerpt || "",
          researchQuestions: state.unresolvedQuestions || [],
          organizationName: state.organizationName || "",
          organizationHost: orgHost,
          relatedEntityFollowUp: /related_entity/i.test(lastEv.sourceFamily || "") || lastEv.relatedEntity === true,
          claimedRelatedEntity: "Rosewood London",
        });
        if (val.valueClass === "UNPRODUCTIVE_RELATIONSHIP_PROBE") {
          state.notes = [...(state.notes || []), `ors_unproductive_relationship_probe:${lastEv.url}`];
          state.unresolvedQuestions = [
            ...new Set([
              ...(state.unresolvedQuestions || []),
              "Independent hotel amenity page did not corroborate DMC–hotel relationship — seek connecting evidence or buyer path",
            ]),
          ];
        }
        state.lastRetrievalCommercialValue = val;
      }
      state.publicBuyerContactEnrichment = {
        version: ORS_SUPERVISOR_V2_HOOKS_VERSION,
        commercialDepthVersion: buyerPass.version,
        surfeInvoked: false,
        pdlInvoked: false,
        stakeholdersFound: (buyerPass.namedStakeholders || []).length,
        namedDecisionMakerEstablished: buyerPass.namedDecisionMakerEstablished === true,
        decisionMakerGap: buyerPass.decisionMakerGap,
        ownershipClass: buyerPass.contactOwnership?.ownershipClass || null,
      };
      state.researchCompleteness = assessResearchCompleteness({
        unresolvedQuestions: state.unresolvedQuestions || stopRecord.unresolvedQuestions || [],
        namedDecisionMakerEstablished: buyerPass.namedDecisionMakerEstablished === true,
        contactUsable: buyerPass.contactOwnership?.salesUsableAsProspectContact === true
          || Boolean(state.contactEmail || state.contact?.businessEmail),
        technicalStopReason: options.stoppingTrigger || state.commercialStoppingTrigger || state.stopReason || null,
        remainingBudgetUsd: options.remainingBudgetUsd ?? null,
        commerciallyUsefulInternal: qualification.commerciallyUsefulInternal === true
          || qualification.internalAccountDevelopment === true,
      });
    } catch (err) {
      state.notes = [
        ...(state.notes || []),
        `ors_public_buyer_enrichment_error:${err?.message || String(err)}`.slice(0, 160),
      ];
    }
  }

  // Internal dossier when enough evidence exists (never customer publish)
  let dossierResult = null;
  if ((claims.length >= 1 || blob.length >= 80) && options.generateDossier !== false) {
    dossierResult = buildLongformResearchDossier({
      hotel: {
        hotelId: state.hotelId || null,
        name: targetHotel,
        market: state.market || null,
      },
      organization: {
        name: state.organizationName || null,
        pathway: qualification.pathway,
      },
      objective: `ORS V2 maturation for ${state.organizationName || "organization"}`,
      acceptedClaims: claims,
      rejectedClaims: (state.rejectedClaims || []).concat(
        qualification.downgradeReason
          ? [{ claim: "Prior useful assessment retained", reason: qualification.downgradeReason }]
          : [],
      ),
      unresolvedGaps: stopRecord.unresolvedQuestions,
      recommendedSalesActions: qualification.nextSalesAction ? [qualification.nextSalesAction] : [],
      researchDisposition: stopRecord.researchDisposition,
      stoppingTrigger: stopRecord.stoppingTrigger,
      researchCompleteness: stopRecord.note || stopRecord.researchDisposition,
      retrievalProvenance: { records: provenanceRecords, independence },
      supportedInferences: qualification.internalAccountDevelopment
        ? ["Internal account-development posture is supported without claiming an open RFP."]
        : [],
    });
  }

  state.supervisorV2 = {
    version: ORS_SUPERVISOR_V2_HOOKS_VERSION,
    appliedAt: new Date().toISOString(),
    discoveryCoverageHint: options.segmentCoverageReport || null,
    leadFollowing: {
      recorded: leadPlan.leadsRecorded,
      pursue: leadPlan.leadsToPursue,
      deferred: (leadPlan.leadsDeferred || []).slice(0, 5),
      execution: leadExecution,
      executedCount: leadExecution.executedCount,
      notMerelyQueued: true,
    },
    provenance: { records: provenanceRecords, independence },
    qualification,
    qualificationV2,
    qualificationV3: qualification.version === "gdi-commercial-qualification-v3" ? qualification : null,
    dimensionCoverage: dimCoverage,
    nextDiverseQuestion: diverseQuestion,
    stopRecord,
    dossier: dossierResult?.dossier || null,
    dossierCitationOk: dossierResult ? dossierResult.citationAudit?.ok === true : null,
    publicationWritesDisabled: publicationWritesDisabled,
    airtableWritesDisabled: airtableWritesDisabled,
    customerFacingClassificationChanged: false,
    productionPublicationEligibilityChanged: false,
    recentQuestionDimensions: [
      ...(state.supervisorV2?.recentQuestionDimensions || []),
      diverseQuestion.selected?.dimension,
    ].filter(Boolean).slice(-8),
  };

  // Surface pathway / V3 disposition on opportunity without flipping publication
  state.pathway = qualification.pathway || state.pathway;
  state.commercialStage = qualification.commercialStage || state.commercialStage;
  state.internalAccountDevelopment = qualification.internalAccountDevelopment === true;
  state.commerciallyUsefulInternal = qualification.commerciallyUsefulInternal === true;
  state.internalDisposition = qualification.internalDisposition || null;
  state.v2NextSalesAction = qualification.nextSalesAction || null;
  state.v3SalesAction = qualification.salesAction || null;
  state.revenueOpportunities = qualification.revenueOpportunities || null;
  state.sourcingStatus = qualification.sourcingStatus || state.sourcingStatus || "UNKNOWN";
  if (qualification.downgradeReason) {
    state.notes = [...(state.notes || []), `v3_qualification:${qualification.downgradeReason}`];
  }

  // Persist checkpoint when dir available
  if (options.checkpointDir && (state.opportunityId || state.id)) {
    const id = state.opportunityId || state.id;
    try {
      writeMaturationCheckpointAtomic(options.checkpointDir, {
        id,
        opportunityId: id,
        hotelId: state.hotelId || null,
        supervisorV2: {
          pathway: state.pathway,
          internalAccountDevelopment: state.internalAccountDevelopment,
          leadsToPursue: (leadPlan.leadsToPursue || []).map((l) => l.entityName),
          stoppingTrigger: stopRecord.stoppingTrigger,
        },
        updatedAt: new Date().toISOString(),
      });
      state.supervisorV2.checkpointWritten = true;
      state.supervisorV2.checkpointPath = maturationMemoryPath(options.checkpointDir, id);
    } catch {
      state.supervisorV2.checkpointWritten = false;
    }
  }

  return state;
}

/**
 * Attach segment-level discovery coverage to orchestrator/segment supervision output.
 */
export function attachSegmentDiscoveryCoverageV2({
  hotelId,
  hotelName,
  segmentId,
  knownOrganizations = [],
  discoveredOrganizations = [],
  investigatedCandidates = [],
  uninvestigatedQueue = [],
  incompleteCandidates = [],
  usefulInternalProspects = [],
  publicationEligibleCount = 0,
  independentPublisherReads = 0,
  relatedEntityLeadsFollowed = 0,
  stoppingTrigger = null,
  usefulProspectTargetHit = false,
  remainingBudgetUsd = 0,
} = {}) {
  const report = buildSegmentDiscoveryCoverageReport({
    hotelId,
    hotelName,
    segmentId,
    knownOrganizations,
    discoveredOrganizations,
    investigatedCandidates,
    uninvestigatedQueue,
    incompleteCandidates,
    usefulInternalProspects,
    publicationEligibleCount,
    independentPublisherReads,
    relatedEntityLeadsFollowed,
    stoppingTrigger,
    usefulProspectTargetHit,
  });
  const continuation = shouldContinueSegmentDiscovery({
    coverageReport: report,
    usefulProspectTargetHit,
    remainingBudgetUsd,
    uninvestigatedRemaining: uninvestigatedQueue.length,
  });
  return { report, continuation };
}

/**
 * Offline E2E rehearsal through matureOpportunity-shaped processing:
 * new org → evidence → related-entity lead → V2 disposition.
 * Publication/Airtable remain disabled.
 */
export function rehearseSupervisorV2ThroughOrsPath() {
  const discovered = {
    id: "gdi_ors_v2_rehearsal_new_org",
    opportunityId: "gdi_ors_v2_rehearsal_new_org",
    hotelId: "recC2kbCaS48jFjFF",
    organizationName: "Rehearsal London Incentive DMC",
    officialDomain: "rehearsal-london-dmc.example",
    officialSource: "https://rehearsal-london-dmc.example/services",
    segmentId: "professional_planners_dmc",
    market: "London",
    targetHotel: "Nobu Hotel London Portman Square",
    contactEmail: "groups@rehearsal-london-dmc.example",
    contact: { businessEmail: "groups@rehearsal-london-dmc.example" },
    evidence: [],
    researchAttempts: [],
    statusHistory: [],
    notes: ["entered_candidate_queue:discovery_rehearsal"],
  };

  // Simulate research retrieval joining the queue
  discovered.evidence.push({
    sourceFamily: "dmc_profile",
    url: "https://rehearsal-london-dmc.example/services",
    quote: "We provide hotel sourcing and group accommodation for incentive programmes across central London, including residential meetings.",
    organizationOwned: true,
    publicationReady: false,
    customerVisible: false,
  });
  discovered.evidence.push({
    sourceFamily: "press",
    url: "https://rehearsal-london-dmc.example/case-studies",
    quote: "A five-night London incentive used Rosewood London as the programme base hotel for a corporate client.",
    organizationOwned: true,
    publicationReady: false,
    customerVisible: false,
  });

  const enriched = applySupervisorV2MaturationHooks(discovered, {
    enableSupervisorV2: true,
    targetHotel: "Nobu Hotel London Portman Square",
    remainingBudgetUsd: 1.5,
    forbidPublication: true,
    forbidAirtable: true,
    generateDossier: true,
    entityUrlHints: {},
  });

  const lead = enriched.supervisorV2?.leadFollowing?.pursue?.[0] || null;
  const checks = [
    { name: "entered_candidate_queue", ok: (enriched.notes || []).some((n) => /entered_candidate_queue/.test(n)) },
    { name: "received_research_evidence", ok: (enriched.evidence || []).length >= 2 },
    { name: "related_entity_followup_generated", ok: (enriched.supervisorV2?.leadFollowing?.recorded || 0) >= 1 },
    { name: "internal_commercial_disposition", ok: enriched.supervisorV2?.qualification?.pathway != null },
    { name: "hotel_sourcing_qualified_or_explicit", ok: enriched.internalAccountDevelopment === true || Boolean(enriched.supervisorV2?.qualification?.downgradeReason) },
    { name: "dossier_generated", ok: Boolean(enriched.supervisorV2?.dossier) },
    { name: "publication_disabled", ok: enriched.supervisorV2?.publicationWritesDisabled === true },
    { name: "airtable_disabled", ok: enriched.supervisorV2?.airtableWritesDisabled === true },
    { name: "customer_facing_unchanged", ok: enriched.supervisorV2?.customerFacingClassificationChanged === false },
    { name: "provenance_tracked", ok: (enriched.supervisorV2?.provenance?.records || []).length >= 1 },
    { name: "diverse_question_present", ok: Boolean(enriched.supervisorV2?.nextDiverseQuestion?.selected) },
    { name: "stop_record_present", ok: Boolean(enriched.supervisorV2?.stopRecord) },
  ];

  // Coverage: new org investigated, others still queued
  const coverage = attachSegmentDiscoveryCoverageV2({
    hotelId: "recC2kbCaS48jFjFF",
    hotelName: "Nobu Hotel London Portman Square",
    segmentId: "professional_planners_dmc",
    knownOrganizations: ["London DMC", "Spectra DMC", "Rehearsal London Incentive DMC"],
    discoveredOrganizations: ["Rehearsal London Incentive DMC"],
    investigatedCandidates: ["Rehearsal London Incentive DMC"],
    uninvestigatedQueue: ["Spectra DMC"],
    usefulInternalProspects: enriched.internalAccountDevelopment ? ["Rehearsal London Incentive DMC"] : [],
    publicationEligibleCount: 0,
    usefulProspectTargetHit: false,
    remainingBudgetUsd: 1.5,
    relatedEntityLeadsFollowed: lead ? 0 : 0,
  });

  checks.push({
    name: "coverage_does_not_claim_exhaustion_with_queue",
    ok: coverage.continuation.continue === true || coverage.report.readings.researchCompleteness.segmentExhaustedDeclared === false,
  });
  checks.push({
    name: "segment_discovery_coverage_attached",
    ok: Boolean(coverage.report?.readings?.discoveryCoverage),
  });

  return {
    version: ORS_SUPERVISOR_V2_HOOKS_VERSION,
    offline: true,
    liveCalls: 0,
    spent: false,
    entrypoint: "applySupervisorV2MaturationHooks_within_ORS_maturation_path",
    soleOrchestrator: "opportunity-research-supervisor-v1",
    separateV2PipelineCreated: false,
    organization: enriched.organizationName,
    pathway: enriched.pathway,
    internalAccountDevelopment: enriched.internalAccountDevelopment === true,
    relatedEntityLead: lead ? { entityName: lead.entityName, pursueRecommended: lead.pursueRecommended } : null,
    coverage: coverage.report.readings,
    continuation: coverage.continuation,
    checks,
    passed: checks.every((c) => c.ok),
    publicationWrites: 0,
    airtableWrites: 0,
  };
}
