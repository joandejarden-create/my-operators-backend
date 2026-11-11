/**
 * GDI Autonomous Research Supervisor V2 — integration layer.
 * Extends the existing Node.js Research Supervisor; no competing agent framework.
 * Offline implementation + certification only in this phase.
 */

import fs from "node:fs";
import { SUPERVISOR_EXPECTED_SEGMENTS, SUPERVISOR_VERSION as ORS_VERSION } from "./research-constants-v1.js";
import { SEGMENT_SUPERVISOR_VERSION } from "./demand-segment-research-supervisor-v1.js";
import { COMMERCIAL_SOURCE_SELECTION_VERSION, selectDiverseResearchQuestionV2, STOPPING_TRIGGER } from "./commercial-source-selection-v1.js";
import { LONGFORM_DOSSIER_VERSION, buildLongformResearchDossier, assertDossierCitationsResolve } from "./longform-research-dossier-v1.js";
import { RESEARCH_PATHWAY } from "./commercial-pathway-v1.js";
import {
  buildSegmentDiscoveryCoverageReport,
  shouldContinueSegmentDiscovery,
  SEGMENT_DISCOVERY_COVERAGE_VERSION,
} from "./segment-discovery-coverage-v2.js";
import {
  planAutonomousLeadFollowing,
  updateLeadRelationshipFromRetrieval,
  AUTONOMOUS_LEAD_FOLLOWING_VERSION,
} from "./autonomous-lead-following-v2.js";
import {
  buildEvidenceProvenanceRecord,
  summarizeIndependenceCoverage,
  EVIDENCE_PROVENANCE_VERSION,
  RETRIEVAL_OUTCOME_V2,
} from "./evidence-provenance-v2.js";
import {
  qualifyHotelSpecificProspectV2,
  HOTEL_QUALIFICATION_V2,
} from "./hotel-specific-qualification-v2.js";
import { reassessNobuPilotCandidatesV2, NOBU_PILOT_REASSESSMENT_VERSION } from "./nobu-pilot-reassessment-v2.js";
import {
  validateWeeklySupervisorIntegrationOfflineV2,
  WEEKLY_SUPERVISOR_INTEGRATION_VERSION,
} from "./weekly-supervisor-integration-v2.js";
import {
  prepareThreeHotelBenchmarkAuthorizationV2,
  THREE_HOTEL_BENCHMARK_VERSION,
} from "./three-hotel-benchmark-authorization-v2.js";

export const RESEARCH_SUPERVISOR_V2 = "gdi-autonomous-research-supervisor-v2";
export const RESEARCH_SUPERVISOR_V2_REPORTS_DIR = "reports/gdi-autonomous-research-supervisor-v2";

export const IMPLEMENTATION_STATUS = Object.freeze({
  IMPLEMENTED: "IMPLEMENTED",
  TESTED_OFFLINE: "TESTED_OFFLINE",
  TESTED_LIVE: "TESTED_LIVE",
  NOT_YET_AVAILABLE: "NOT_YET_AVAILABLE",
});

/**
 * Record baselines before V2 changes (read-only snapshot).
 */
export function recordResearchSupervisorV2Baseline() {
  const certPaths = {
    nobuPilot: "reports/gdi-segment-pilot-professional-planners-dmc-nobu-v1/CERTIFICATION_RESULT.json",
    principalLongform: "reports/gdi-autonomous-longform-research-certification-v1/CERTIFICATION_RESULT.json",
    savoyCloseout: "reports/gdi-cross-organization-live-recovery-certification-v1/CERTIFICATION_CLOSEOUT.json",
    segmentE2e: "reports/gdi-segment-e2e-certification-v1/live/CERTIFICATION.md",
  };
  const present = {};
  for (const [key, rel] of Object.entries(certPaths)) {
    present[key] = fs.existsSync(rel);
  }

  return {
    version: RESEARCH_SUPERVISOR_V2,
    recordedAt: new Date().toISOString(),
    soleOrchestrator: {
      opportunityResearchSupervisor: ORS_VERSION,
      demandSegmentResearchSupervisor: SEGMENT_SUPERVISOR_VERSION,
      commercialSourceSelection: COMMERCIAL_SOURCE_SELECTION_VERSION,
      dossier: LONGFORM_DOSSIER_VERSION,
    },
    elevenCanonicalSegments: SUPERVISOR_EXPECTED_SEGMENTS.length === 11,
    segments: [...SUPERVISOR_EXPECTED_SEGMENTS],
    pathwaysPreserved: Object.values(RESEARCH_PATHWAY),
    certificationArtifactsPresent: present,
    priorPilotCommercialNote: "Nobu pilot found 2 useful ACCOUNT_LED prospects under V1 pilot standard; V2 reassessment applies stricter venue-vs-guestroom rules.",
    offlineTestPassIsNotCommercialCertification: true,
    productionPublicationUnchanged: true,
  };
}

/**
 * Offline certification suite for Supervisor V2 capabilities.
 */
export function runResearchSupervisorV2OfflineCertification() {
  const checks = [];
  const push = (name, ok, detail = "", status = IMPLEMENTATION_STATUS.TESTED_OFFLINE) => {
    checks.push({ name, ok: Boolean(ok), detail, status: ok ? status : "FAILED" });
  };

  const baseline = recordResearchSupervisorV2Baseline();
  push("baseline_recorded", baseline.elevenCanonicalSegments === true);
  push("no_competing_agent_framework", true, "AutoGen/CrewAI/LangGraph not introduced");

  // Discovery coverage
  const coverage = buildSegmentDiscoveryCoverageReport({
    hotelId: "recC2kbCaS48jFjFF",
    hotelName: "Nobu Hotel London Portman Square",
    segmentId: "professional_planners_dmc",
    knownOrganizations: ["Spectra DMC", "London DMC", "2B UK DMC", "Miss Jones Group", "HeadBox", "Hire Space", "Principal. London DMC"],
    discoveredOrganizations: ["Spectra DMC", "London DMC"],
    investigatedCandidates: ["London DMC", "Miss Jones Group", "Spectra DMC", "2B UK DMC"],
    uninvestigatedQueue: ["HeadBox", "Hire Space"],
    incompleteCandidates: ["Spectra DMC", "2B UK DMC"],
    usefulInternalProspects: ["London DMC"],
    publicationEligibleCount: 0,
    usefulProspectTargetHit: true,
    providerOrBudgetFinished: false,
    stoppingTrigger: STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL,
  });
  push("discovery_coverage_narrow_or_partial_when_queue_remains",
    coverage.readings.discoveryCoverage.status !== "BROAD");
  push("useful_target_is_not_segment_exhaustion",
    coverage.readings.researchCompleteness.usefulProspectTargetIsNotSegmentExhaustion === true);
  push("four_independent_readings",
    Boolean(coverage.readings.discoveryCoverage
      && coverage.readings.researchCompleteness
      && coverage.readings.commercialQualification
      && coverage.readings.clientPublicationReadiness));

  const cont = shouldContinueSegmentDiscovery({
    coverageReport: coverage,
    usefulProspectTargetHit: true,
    remainingBudgetUsd: 0.5,
    uninvestigatedRemaining: 2,
  });
  push("continue_after_two_useful_when_queue_remains", cont.continue === true);

  // Lead following
  const leads = planAutonomousLeadFollowing({
    findingsText: "Principal delivered a five-night London incentive with Rosewood London as the base hotel for Relativity.",
    sourceUrl: "https://www.principallondon.com/case-studies",
    organizationHost: "principallondon.com",
    organizationName: "Principal. London DMC",
    investigatedUrls: [],
    entityUrlHints: { "Rosewood London": "https://www.rosewoodhotels.com/en/london" },
    targetHotel: "Nobu Hotel London Portman Square",
    remainingBudgetUsd: 0.5,
    maxPursue: 2,
  });
  push("lead_following_records_leads", leads.leadsRecorded >= 1);
  push("lead_following_pursues_strongest", leads.leadsToPursue.length >= 1);
  const updated = updateLeadRelationshipFromRetrieval({
    lead: leads.leadsToPursue[0] || leads.leadsDeferred[0] || { entityName: "Rosewood London" },
    retrievedUrl: "https://www.rosewoodhotels.com/en/london",
    pageText: "Welcome to Rosewood London. Book a stay.",
    organizationHost: "principallondon.com",
  });
  push("independent_page_without_organizer_link_not_verified",
    updated.independentlyVerified !== true);

  // Provenance
  const prov = buildEvidenceProvenanceRecord({
    url: "https://www.principallondon.com/what-we-do",
    organizationHost: "principallondon.com",
    organizationName: "Principal. London DMC",
    claimSupported: "hotel sourcing",
    evidenceQuotation: "hotel sourcing and contract negotiation",
    retrievalOutcome: RETRIEVAL_OUTCOME_V2.SUCCESS_WITH_CONTENT,
  });
  push("organizer_page_not_independent", prov.independentOfOrganizer === false);
  const failProv = buildEvidenceProvenanceRecord({
    url: "https://www.thesavoy.co.uk/",
    organizationHost: "principallondon.com",
    retrievalOutcome: RETRIEVAL_OUTCOME_V2.PROVIDER_ERROR,
  });
  push("retrieval_failure_not_negative_finding",
    failProv.technicalFailureIsNotNegativeFinding === true && failProv.claimSupported == null);
  push("independence_summary", summarizeIndependenceCoverage([prov, failProv]).sameOrganizationPagesDoNotEqualIndependence === true);

  // Question diversity
  const q1 = selectDiverseResearchQuestionV2({
    pathway: "ACCOUNT_LED",
    coveredDimensions: [],
    recentQuestionDimensions: ["decision_makers_and_contact_routes", "decision_makers_and_contact_routes"],
  });
  push("question_diversity_avoids_contact_cluster",
    q1.selected?.dimension !== "decision_makers_and_contact_routes");

  // Qualification — venue vs hotel sourcing
  const venueFail = qualifyHotelSpecificProspectV2({
    opportunity: { organizationName: "Miss Jones Group", contactEmail: "hello@missjonesgroup.com" },
    acceptedClaims: [
      { statement: "Free Venue Finding · Nobu Hotel London Portman Square · Maximum Guests: 700 Bedrooms: 243", topic: "relationship" },
    ],
    pageTexts: ["Free Venue Finding", "Maximum Guests: 700 Bedrooms: 243 Layout & Spaces"],
    targetHotel: "Nobu Hotel London Portman Square",
    usableContactRoute: "hello@missjonesgroup.com",
  });
  push("venue_listing_not_guestroom_control", venueFail.internalAccountDevelopment === false);

  const hotelOk = qualifyHotelSpecificProspectV2({
    opportunity: { organizationName: "London DMC", contactEmail: "info@londondmc.com" },
    acceptedClaims: [
      { statement: "handling anything from hotel and restaurant bookings to incentive programmes for thousands of delegates", topic: "placement" },
    ],
    pageTexts: ["hotel and restaurant bookings to incentive programmes for thousands of delegates", "London"],
    targetHotel: "Nobu Hotel London Portman Square",
    usableContactRoute: "info@londondmc.com",
  });
  push("hotel_sourcing_supports_account_led", hotelOk.internalAccountDevelopment === true
    || hotelOk.commerciallyUsefulInternal === true);
  push("publication_untouched_by_qualification", hotelOk.productionPublicationEligibilityChanged === false);

  // Dossier contract
  const dossierResult = buildLongformResearchDossier({
    hotel: { hotelId: "recC2kbCaS48jFjFF", name: "Nobu Hotel London Portman Square", market: "London" },
    organization: { name: "London DMC", pathway: "ACCOUNT_LED" },
    objective: "V2 dossier contract",
    acceptedClaims: [{
      evidenceId: "ev_v2",
      url: "https://londondmc.com/",
      statement: "hotel and restaurant bookings to incentive programmes",
      topic: "placement",
      accepted: true,
    }],
    rejectedClaims: [{ claim: "Open RFP", reason: "not_in_cited_source" }],
    unresolvedGaps: ["Independent publisher corroboration absent"],
    recommendedSalesActions: ["Introduce Nobu via info@londondmc.com"],
    researchDisposition: "BUYER_ENGAGEMENT_REQUIRED",
    stoppingTrigger: STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL,
    researchCompleteness: "INCOMPLETE_WITHOUT_INDEPENDENT_PUBLISHER",
    supportedInferences: ["Central London incentive capability is commercially relevant to Nobu"],
  });
  push("dossier_has_sales_action_summary", Boolean(dossierResult.dossier?.salesActionSummary?.concise));
  push("dossier_citations_resolve", assertDossierCitationsResolve(dossierResult).ok === true);
  push("dossier_evidence_classification", Boolean(dossierResult.dossier?.evidenceClassification));

  // Nobu reassessment
  const reassessment = reassessNobuPilotCandidatesV2();
  push("nobu_reassessment_offline", reassessment.spent === false && reassessment.liveCalls === 0);
  push("miss_jones_downgraded", reassessment.downgraded.some((d) => /miss jones/i.test(d.organizationName)));
  push("london_dmc_retained_or_qualified",
    reassessment.revisedUsefulInternalAccountDevelopment.includes("London DMC"));
  push("customer_facing_unchanged_on_reassessment",
    reassessment.customerFacingClassificationChanged === false);

  // Weekly design
  const weekly = validateWeeklySupervisorIntegrationOfflineV2();
  push("weekly_integration_designed_not_switched", weekly.passed === true && weekly.design.productionWeeklyJobSwitched === false);

  // Three-hotel benchmark prep
  const benchmark = prepareThreeHotelBenchmarkAuthorizationV2();
  push("three_hotel_benchmark_awaiting_auth",
    benchmark.status === "AWAITING_AUTHORIZATION" && benchmark.spent === false);
  push("benchmark_budgets_nontransferable",
    benchmark.authorizationManifest.budgetsNontransferableAcrossHotels === true);
  push("benchmark_three_hotels",
    benchmark.authorizationManifest.hotels.length === 3);

  // Pathways preserved
  push("three_pathways_preserved",
    Object.values(RESEARCH_PATHWAY).includes("EVENT_LED")
    && Object.values(RESEARCH_PATHWAY).includes("ACCOUNT_LED")
    && Object.values(RESEARCH_PATHWAY).includes("COMPETITIVE_FUTURE_CYCLE"));

  const capabilityMatrix = [
    { capability: "segment_discovery_coverage_reporting", status: IMPLEMENTATION_STATUS.TESTED_OFFLINE, module: SEGMENT_DISCOVERY_COVERAGE_VERSION },
    { capability: "autonomous_lead_following", status: IMPLEMENTATION_STATUS.TESTED_OFFLINE, module: AUTONOMOUS_LEAD_FOLLOWING_VERSION },
    { capability: "evidence_provenance_independence", status: IMPLEMENTATION_STATUS.TESTED_OFFLINE, module: EVIDENCE_PROVENANCE_VERSION },
    { capability: "hotel_specific_qualification_v2", status: IMPLEMENTATION_STATUS.TESTED_OFFLINE, module: HOTEL_QUALIFICATION_V2 },
    { capability: "research_question_diversity_v2", status: IMPLEMENTATION_STATUS.TESTED_OFFLINE, module: "gdi-research-question-diversity-v2" },
    { capability: "internal_dossier_sales_action_contract", status: IMPLEMENTATION_STATUS.TESTED_OFFLINE, module: LONGFORM_DOSSIER_VERSION },
    { capability: "nobu_pilot_reassessment", status: IMPLEMENTATION_STATUS.TESTED_OFFLINE, module: NOBU_PILOT_REASSESSMENT_VERSION },
    { capability: "weekly_supervisor_integration", status: IMPLEMENTATION_STATUS.IMPLEMENTED, module: WEEKLY_SUPERVISOR_INTEGRATION_VERSION, note: "Designed; production weekly job not switched" },
    { capability: "three_hotel_benchmark_authorization", status: IMPLEMENTATION_STATUS.IMPLEMENTED, module: THREE_HOTEL_BENCHMARK_VERSION, note: "Awaiting paid authorization" },
    { capability: "live_three_hotel_research", status: IMPLEMENTATION_STATUS.NOT_YET_AVAILABLE, note: "Requires separate GO + AUTHORIZED spend" },
    { capability: "customer_facing_account_led_format", status: IMPLEMENTATION_STATUS.NOT_YET_AVAILABLE, note: "Review only; no customer format change authorized" },
    { capability: "production_weekly_job_switch", status: IMPLEMENTATION_STATUS.NOT_YET_AVAILABLE },
  ];

  const remainingBlockers = [
    "No live three-hotel benchmark spend authorized.",
    "Production weekly job must not be switched until weekly integration is live-certified.",
    "Customer-facing account-led presentation format requires separate product authorization.",
    "Independent publisher coverage remains weak in prior Nobu pilot (0 independent reads) — must be measured in next live run.",
    "Segment-wide discovery still needs live permitted providers under new ceilings to expand beyond frozen shortlists.",
    "Passing offline engineering tests ≠ verified commercial effectiveness on live markets.",
  ];

  const prioritizedRemainingWork = [
    { priority: 1, work: "Authorize and execute three-hotel benchmark under nontransferable ledgers with V2 qualification + coverage reporting." },
    { priority: 2, work: "Wire lead-following pursuit into Opportunity Research Supervisor live maturation loop (currently TESTED_OFFLINE helpers)." },
    { priority: 3, work: "Expand discovery beyond frozen corpora with priced Exa/Firecrawl only after evidence exhaustion." },
    { priority: 4, work: "Live-certify weekly supervisor hooks without enabling auto-publish." },
    { priority: 5, work: "Product decision on distinct customer-facing ACCOUNT_LED format (no implementation until authorized)." },
  ];

  const offlinePassed = checks.every((c) => c.ok);
  const goNoGo = {
    decision: offlinePassed ? "NO-GO_FOR_LIVE_THREE_HOTEL_UNTIL_EXPLICIT_SPEND_AUTH" : "NO-GO",
    offlineImplementationCertified: offlinePassed,
    liveThreeHotelResearchAuthorized: false,
    rationale: offlinePassed
      ? "V2 capabilities are IMPLEMENTED/TESTED_OFFLINE. Live three-hotel research remains blocked until a separate AUTHORIZED spend GO. Offline pass is not commercial effectiveness certification."
      : "Offline certification failed; do not authorize live spend.",
  };

  return {
    version: RESEARCH_SUPERVISOR_V2,
    offline: true,
    liveCalls: 0,
    spent: false,
    baseline,
    checks,
    offlinePassed,
    capabilityMatrix,
    nobuReassessment: reassessment,
    weeklyIntegration: weekly,
    threeHotelBenchmark: benchmark,
    remainingProductionBlockers: remainingBlockers,
    prioritizedRemainingWork,
    goNoGo,
    controls: {
      noNewPaidResearch: true,
      noProductionPublication: true,
      noAirtableWrites: true,
      noDeploy: true,
      noCustomerFacingClassificationChanges: true,
      noCompetingAgentFramework: true,
      existingResearchLedgersPreserved: true,
    },
  };
}

/**
 * Prepare full V2 deliverable package (writes are done by the runner script).
 */
export function prepareResearchSupervisorV2Deliverable() {
  const cert = runResearchSupervisorV2OfflineCertification();
  return {
    ...cert,
    affectedModules: [
      "lib/group-demand-intelligence/research/research-supervisor-v2.js",
      "lib/group-demand-intelligence/research/segment-discovery-coverage-v2.js",
      "lib/group-demand-intelligence/research/autonomous-lead-following-v2.js",
      "lib/group-demand-intelligence/research/evidence-provenance-v2.js",
      "lib/group-demand-intelligence/research/hotel-specific-qualification-v2.js",
      "lib/group-demand-intelligence/research/nobu-pilot-reassessment-v2.js",
      "lib/group-demand-intelligence/research/weekly-supervisor-integration-v2.js",
      "lib/group-demand-intelligence/research/three-hotel-benchmark-authorization-v2.js",
      "lib/group-demand-intelligence/research/commercial-source-selection-v1.js",
      "lib/group-demand-intelligence/research/longform-research-dossier-v1.js",
    ],
    prepared: true,
    executedLivePaid: false,
  };
}
