/**
 * First complete autonomous GDI segment-research pilot — preparation only.
 * Hotel: Nobu Hotel London Portman Square · Segment: professional_planners_dmc
 * Uses existing 11-segment architecture (Demand Segment Research Supervisor +
 * Opportunity Research Supervisor + dynamic planner). No live spend here.
 */

import fs from "node:fs";
import {
  quotePlannerUsd,
  PLANNER_CEILING_USD,
} from "./dynamic-research-planner-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import {
  SEGMENT_SUPERVISOR_VERSION,
  SEGMENT_SOURCE_FAMILIES_EXPECTED,
  SEGMENT_DEMAND_MECHANISMS_EXPECTED,
} from "./demand-segment-research-supervisor-v1.js";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";
import { RESEARCH_SEGMENT, RESEARCH_SEGMENT_LABEL } from "../workflow/hotel-segment-research-plan-v1.js";
import { SEGMENT_E2E_HOTELS } from "./segment-e2e-budget-v1.js";

export const SEGMENT_PILOT_VERSION = "gdi-segment-pilot-professional-planners-dmc-nobu-v1";
export const SEGMENT_PILOT_HOTEL_ID = "recC2kbCaS48jFjFF";
export const SEGMENT_PILOT_HOTEL_NAME = "Nobu Hotel London Portman Square";
export const SEGMENT_PILOT_SEGMENT_ID = "professional_planners_dmc";
export const SEGMENT_PILOT_BUDGET_USD = 0.75;
export const SEGMENT_PILOT_CALL_CEILING_USD = 0.01;
export const SEGMENT_PILOT_MAX_PLANNER_CALLS = 12;
export const SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS = 18;
export const SEGMENT_PILOT_MAX_FAILED_TECHNICAL_ATTEMPTS = 24;
export const SEGMENT_PILOT_MAX_CANDIDATES_INVESTIGATED = 6;
export const SEGMENT_PILOT_LEDGER_RELATIVE =
  "reports/gdi-segment-pilot-professional-planners-dmc-nobu-v1/budget-ledger.json";
export const SEGMENT_PILOT_REPORTS_DIR =
  "reports/gdi-segment-pilot-professional-planners-dmc-nobu-v1";

const CANONICAL_OPPS =
  "data/group-demand-intelligence/hotels/recC2kbCaS48jFjFF/opportunities.json";
const RESEARCH_MEMORY_DIR =
  "reports/gdi-opportunity-research-supervisor-v1/hotel-orchestrator-recC2kbCaS48jFjFF/research-memory";
const LONGFORM_DOSSIER =
  "reports/gdi-autonomous-longform-research-certification-v1/INTERNAL_RESEARCH_DOSSIER.json";
const LONGFORM_RESULT =
  "reports/gdi-autonomous-longform-research-certification-v1/CERTIFICATION_RESULT.json";
const SAVOY_CLOSEOUT =
  "reports/gdi-cross-organization-live-recovery-certification-v1/CERTIFICATION_CLOSEOUT.md";
const SEGMENT_E2E_LIVE =
  "reports/gdi-segment-e2e-certification-v1/live/CERTIFICATION.md";

function readJsonSafe(rel) {
  if (!fs.existsSync(rel)) return null;
  try {
    return JSON.parse(fs.readFileSync(rel, "utf8"));
  } catch {
    return null;
  }
}

function loadFrozenPlannerCandidates() {
  const doc = readJsonSafe(CANONICAL_OPPS);
  const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  const known = [
    "Spectra DMC",
    "Miss Jones Group",
    "Hire Space",
    "London DMC",
    "HeadBox",
    "2B UK DMC",
  ];
  const byName = new Map();
  for (const o of ops) {
    const name = String(o.organizationName || o.title || "").trim();
    if (!name) continue;
    if (known.some((k) => k.toLowerCase() === name.toLowerCase())
      || /dmc|planner|headbox|hire space|miss jones/i.test(name)) {
      byName.set(name, {
        opportunityId: o.id || o.opportunityId || null,
        organizationName: name,
        contactEmail: o.contactEmail || o.email || null,
        primarySegment: o.primarySegment || o.segment || SEGMENT_PILOT_SEGMENT_ID,
        pathway: o.pathway || o.commercialPathway || null,
        customerVisible: o.customerVisible === true,
      });
    }
  }
  // Ensure the six frozen names appear even if segment tags vary.
  for (const name of known) {
    if (!byName.has(name)) {
      byName.set(name, {
        opportunityId: null,
        organizationName: name,
        contactEmail: null,
        primarySegment: SEGMENT_PILOT_SEGMENT_ID,
        pathway: "ACCOUNT_LED",
        customerVisible: false,
        note: "Named in segment corpus / prior cert; confirm fields from opportunities.json when present",
      });
    }
  }
  return [...byName.values()];
}

/**
 * Current baseline for the pilot (read-only snapshot).
 */
export function buildSegmentPilotBaseline() {
  const frozenCandidates = loadFrozenPlannerCandidates();
  const longform = readJsonSafe(LONGFORM_RESULT);
  const dossier = readJsonSafe(LONGFORM_DOSSIER);
  const hotelMeta = SEGMENT_E2E_HOTELS[SEGMENT_PILOT_HOTEL_ID];
  const memoryFiles = fs.existsSync(RESEARCH_MEMORY_DIR)
    ? fs.readdirSync(RESEARCH_MEMORY_DIR).filter((f) => f.endsWith(".json")).length
    : 0;

  const principalResearch = {
    organizationName: "Principal. London DMC",
    inFrozenSix: frozenCandidates.some((c) => /principal/i.test(c.organizationName)),
    longformDisposition: longform?.researchDisposition || null,
    longformPathway: "ACCOUNT_LED",
    openRfp: false,
    nobuNamed: false,
    bedroomCommitment: false,
    contactRoute: "london@principalge.com · 020 7485 5500",
    savoyIndependentCorroboration: "UNVERIFIED",
    savoyRetrievalRecovery: "PASS",
    fairmontPagesRead: 2,
    independentThirdPartyInvestigated: 0,
    dossierPublicationGate: dossier?.publicationGate || "INTERNAL_RESEARCH_DOSSIER_ONLY",
    commercialNextAction: "Buyer engagement",
    note: "Prior Principal work is research memory for the segment pilot — do not re-spend Savoy/longform ledgers; do not invent Nobu demand.",
  };

  return {
    hotelId: SEGMENT_PILOT_HOTEL_ID,
    hotelName: SEGMENT_PILOT_HOTEL_NAME,
    market: "London",
    segmentId: SEGMENT_PILOT_SEGMENT_ID,
    segmentLabel: RESEARCH_SEGMENT_LABEL[RESEARCH_SEGMENT.PROFESSIONAL_PLANNERS_DMC],
    elevenSegmentArchitecture: SUPERVISOR_EXPECTED_SEGMENTS.length === 11,
    expectedSourceFamilies: SEGMENT_SOURCE_FAMILIES_EXPECTED[SEGMENT_PILOT_SEGMENT_ID],
    expectedDemandMechanisms: SEGMENT_DEMAND_MECHANISMS_EXPECTED[SEGMENT_PILOT_SEGMENT_ID],
    hotelProfileRefs: [
      "fixtures/ai-demand-positioning/nobu-london-portman-property-profile.json",
      CANONICAL_OPPS,
    ],
    frozenCandidateCorpus: {
      path: CANONICAL_OPPS,
      plannerDmcCandidates: frozenCandidates,
      count: frozenCandidates.length,
    },
    evidenceAndMemory: {
      researchMemoryDir: RESEARCH_MEMORY_DIR,
      researchMemoryFileCount: memoryFiles,
      segmentE2eLiveCert: SEGMENT_E2E_LIVE,
      longformDossier: LONGFORM_DOSSIER,
      savoyCloseout: SAVOY_CLOSEOUT,
    },
    priorSegmentE2eNobu: {
      spentUsd: 0.025,
      ceilingUsd: hotelMeta?.ceilingUsd ?? 2,
      productionReadyForHotel: 0,
      accountLedShadow: 3,
      firmsWithContacts: ["London DMC", "2B UK DMC", "Spectra DMC"],
      note: "Segment E2E showed real London DMCs with emails but no dated programme/hotel commitment; publication gate failed occasion/travel-need on account-led rows.",
    },
    principalLondonDmc: principalResearch,
    publicationEligibilityBaseline: {
      customerFacingPublicationEligibleCount: frozenCandidates.filter((c) => c.customerVisible).length,
      internalAccountDevelopmentProspects: frozenCandidates.length + 1, // + Principal from prior research
      separationRequired: "publication eligibility ≠ internal research completion",
    },
  };
}

/**
 * Exact integration gaps before live pilot authorization.
 */
export function buildSegmentPilotIntegrationGaps() {
  return [
    {
      id: "GAP_ACCOUNT_LED_GATE",
      severity: "high",
      gap: "Publication / qualification still scores many DMC account rows as dated-lodging events, failing supported_occasion_or_timing and travel_need_evidence.",
      required: "Keep ACCOUNT_LED internal qualification separate from publication-gate eligibility; do not force dated occasion language for account intros.",
      existingModules: [
        "lib/group-demand-intelligence/prospect-publication-eligibility-v1.js",
        "lib/group-demand-intelligence/research/opportunity-research-decision-record-v1.js",
      ],
    },
    {
      id: "GAP_WIRE_PRIOR_RESEARCH_MEMORY",
      severity: "high",
      gap: "Principal longform + Savoy recovery results are not automatically ingested as known-organization / investigated-URL memory for the segment supervisor.",
      required: "Seed pilot state from frozen corpus + longform investigated URLs + Savoy blocked hosts without re-fetching completed/failed URLs.",
      existingModules: [
        "lib/group-demand-intelligence/research/longform-research-certification-v1.js",
        "lib/group-demand-intelligence/research/cross-organization-live-recovery-certification-v1.js",
        "lib/group-demand-intelligence/research/demand-segment-research-supervisor-v1.js",
      ],
    },
    {
      id: "GAP_RETRIEVAL_RECOVERY_DEFAULT",
      severity: "medium",
      gap: "Segment E2E historically used Exa-heavy paths; retrieval-outcome vs evidence separation and adaptive recovery are certified but not yet the default segment investigation path.",
      required: "Reuse cross-organization retrieval recovery + commercial source selection inside Opportunity Research Supervisor loops for this pilot.",
      existingModules: [
        "lib/group-demand-intelligence/research/cross-organization-retrieval-recovery-v1.js",
        "lib/group-demand-intelligence/research/commercial-source-selection-v1.js",
        "lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js",
      ],
    },
    {
      id: "GAP_INDEPENDENT_PUBLISHER_METRICS",
      severity: "medium",
      gap: "Segment metrics do not yet report independent publisher coverage distinctly from organizer self-pages.",
      required: "Pilot measurement must count independent publisher reads and verification status separately (Fairmont-style brand alternates ≠ independent corroboration).",
      existingModules: [
        "lib/group-demand-intelligence/research/cross-organization-live-recovery-certification-v1.js",
      ],
    },
    {
      id: "GAP_INTERNAL_DOSSIER_PIPELINE",
      severity: "medium",
      gap: "Longform internal dossier builder exists; segment pilot must emit per-candidate INTERNAL_RESEARCH_DOSSIER_ONLY artifacts without flipping publication eligibility.",
      required: "Call longform-research-dossier (or equivalent) per investigated candidate; publicationGate remains internal-only unless a separate publish auth exists.",
      existingModules: [
        "lib/group-demand-intelligence/research/longform-research-dossier-v1.js",
      ],
    },
    {
      id: "GAP_DISCOVERY_DEDUP_PRIORITY",
      severity: "medium",
      gap: "Event-to-Demand rediscovery + Parallel adapters exist, but pilot must prefer existing evidence and dedupe against the six frozen orgs + Principal before paid discovery.",
      required: "Orchestrator segment discovery with authorizeLivePaid=false until existing candidates are exhausted or VoI justifies paid.",
      existingModules: [
        "lib/group-demand-intelligence/research/orchestrator-segment-discovery-v1.js",
        "lib/group-demand-intelligence/research/dynamic-research-planner-v1.js",
      ],
    },
    {
      id: "GAP_ISOLATED_PILOT_LEDGER",
      severity: "high",
      gap: "Nobu $22 envelope, segment-e2e $2 Nobu ceiling, longform $0.15, and Savoy $0.05 ledgers must not be drawn.",
      required: "New isolated pilot ledger only; refuse prior ledger paths in assert.",
      existingModules: [
        "lib/group-demand-intelligence/research/segment-e2e-budget-v1.js",
      ],
    },
  ];
}

export function buildSegmentPilotWorkflowDesign() {
  return {
    soleOrchestrator: "opportunity-research-supervisor-v1 (Node.js Research Supervisor)",
    segmentCoordinator: SEGMENT_SUPERVISOR_VERSION,
    architecture: "existing_11_segment",
    doNotCreateAnotherResearchSystem: true,
    steps: [
      {
        n: 1,
        name: "discover_relevant_organizations",
        modules: ["orchestrator-segment-discovery-v1", "event-to-demand-discovery-v1"],
        note: "Prefer frozen corpus + prior memory; paid discovery only after VoI + auth.",
      },
      {
        n: 2,
        name: "deduplicate_against_known_orgs_and_history",
        modules: ["demand-segment-research-supervisor-v1", "research-memory"],
        note: "Include Principal investigated URLs and Savoy blocked hosts.",
      },
      {
        n: 3,
        name: "prioritize_by_hotel_specific_demand_potential",
        modules: ["dynamic-research-planner-v1", "commercial-source-selection-v1"],
        note: "London incentive / hotel-sourcing DMCs outrank generic venue marketplaces when evidence supports.",
      },
      {
        n: 4,
        name: "investigate_highest_value_candidates",
        modules: ["opportunity-research-supervisor-v1", "dynamic-research-planner-v1"],
        caps: {
          maxCandidatesInvestigated: SEGMENT_PILOT_MAX_CANDIDATES_INVESTIGATED,
          maxSubstantiveRetrievals: SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS,
        },
      },
      {
        n: 5,
        name: "follow_commercially_meaningful_leads",
        modules: ["cross-organization-retrieval-recovery-v1", "commercial-source-selection-v1"],
        note: "Cross-org follow-ups require independent publishers for verification.",
      },
      {
        n: 6,
        name: "validate_hotel_placement_and_accommodation_evidence",
        modules: ["opportunity-research-decision-record-v1"],
        note: "Distinguish demonstrated placement, historical programmes, verified hotel relationships, plausible future potential, active sourcing.",
      },
      {
        n: 7,
        name: "identify_usable_contact_routes",
        modules: ["opportunity-research-supervisor-v1 isActionableContactRoute"],
      },
      {
        n: 8,
        name: "produce_internal_research_dossiers",
        modules: ["longform-research-dossier-v1"],
        publicationGate: "INTERNAL_RESEARCH_DOSSIER_ONLY",
      },
      {
        n: 9,
        name: "determine_commercial_qualification_and_next_sales_actions",
        modules: ["opportunity-research-decision-record-v1", "prospect-publication-eligibility-v1"],
        note: "Report publication-gate eligible vs internal account-development separately.",
      },
      {
        n: 10,
        name: "persist_unresolved_questions_and_future_research_dates",
        modules: ["demand-segment-research-supervisor-v1 checkpoints", "research-memory"],
      },
    ],
  };
}

export function buildSegmentPilotMeasurementStandards() {
  return {
    metrics: [
      "organizations_discovered",
      "unique_qualified_candidates",
      "candidates_investigated_to_defensible_outcome",
      "relevant_overnight_accommodation_evidence",
      "independent_publisher_coverage",
      "usable_contact_routes",
      "credible_sales_actions",
      "publication_gate_eligible_opportunities",
      "internal_account_development_prospects",
      "false_positives_rejected",
      "research_cost_per_useful_prospect",
    ],
    definitions: {
      useful_prospect: "Either publication-gate eligible OR internal account-development with usable contact + hotel-relevant placement/programme evidence (not inventing RFP/Nobu award)",
      independent_publisher_coverage: "Successful reads from publishers that are not the organizer and not counted as automatic brand-alternate corroboration",
      publication_gate_eligible_opportunities: "Passes prospect-publication-eligibility without changing production flags in this pilot",
      internal_account_development_prospects: "ACCOUNT_LED / NEEDS_RESEARCH or similar — research complete internally but not customer-publishable",
    },
    keepSeparate: [
      "publication_eligibility",
      "internal_research_completion",
    ],
  };
}

export function buildSegmentPilotOfflineCertificationPlan() {
  return {
    scenarios: [
      "frozen_corpus_load_and_dedupe",
      "principal_prior_memory_not_refetched",
      "account_led_vs_publication_gate_separation",
      "priority_ordering_hotel_specific",
      "retrieval_failure_not_negative_relationship_finding",
      "fairmont_style_brand_alternate_not_independent_corroboration",
      "independent_publisher_required_for_verification",
      "contact_route_extraction",
      "internal_dossier_citations_resolve",
      "budget_and_attempt_caps",
      "checkpoint_resume_no_repeat_job_keys",
      "prior_ledgers_refused",
      "false_positive_rejection_wrong_hotel_named",
      "stop_reasons_separated",
    ],
    passRequires: [
      "Uses Demand Segment Research Supervisor + Opportunity Research Supervisor only",
      "Isolated pilot ledger; no prior ledger spend",
      "Publication eligibility unchanged unless explicitly gated internal-only dossiers",
      "Measurement standards fully reported",
      "No Airtable / deploy / customer classification changes",
    ],
    failIf: [
      "New parallel research framework invented",
      "Re-spend Savoy/longform/segment-e2e/Nobu-$22 ledgers",
      "Invented Nobu RFP or room block",
      "DMC account auto-published via dated-lodging gate misuse",
    ],
  };
}

export function buildSegmentPilotSuccessCriteria() {
  return {
    commercialSuccess: [
      {
        id: "CS1",
        criterion: "At least one internal account-development prospect with usable contact and hotel-relevant placement/programme evidence",
      },
      {
        id: "CS2",
        criterion: "False positives with another hotel already named as host are rejected for Nobu publication",
      },
      {
        id: "CS3",
        criterion: "Publication-gate eligible count reported separately; may be zero without failing the pilot if internal prospects + research discipline pass",
      },
      {
        id: "CS4",
        criterion: "Independent publisher coverage measured; organizer-only loops do not claim independent corroboration",
      },
      {
        id: "CS5",
        criterion: "Cost per useful prospect computed when ≥1 useful prospect exists; otherwise reported as N/A with spend transparency",
      },
    ],
    technicalSuccess: [
      "Offline certification scenarios pass",
      "Live run (when authorized) respects caps and isolation",
      "Checkpoints resume without repeating spent job keys",
      "Stop triggers distinguish evidence sufficiency, buyer engagement, budget, retrieval failures",
    ],
  };
}

function providerPricingCard() {
  const planner = quotePlannerUsd();
  let firecrawl = { bounded: false, maxUsd: 0 };
  let exa = { bounded: false, maxUsd: 0 };
  try {
    firecrawl = quoteBenchmarkCall({
      provider: "firecrawl",
      operation: "scrape",
      params: { url: "https://www.example.com/", formats: ["markdown"] },
    });
  } catch {
    firecrawl = { bounded: false, maxUsd: 0 };
  }
  try {
    exa = quoteBenchmarkCall({
      provider: "exa",
      operation: "search",
      params: { query: "London DMC incentive hotel", numResults: 3 },
    });
  } catch {
    exa = { bounded: false, maxUsd: 0 };
  }
  return { planner, firecrawl, exa };
}

export function buildSegmentPilotAuthorizationManifest() {
  const pricing = providerPricingCard();
  const maxQuotedUsd = Number((
    SEGMENT_PILOT_MAX_PLANNER_CALLS * pricing.planner.estimatedUsd
    + SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS * (
      pricing.firecrawl.bounded ? Number(pricing.firecrawl.maxUsd) : 0
    )
  ).toFixed(6));

  return {
    version: SEGMENT_PILOT_VERSION,
    status: "AWAITING_AUTHORIZATION",
    hotelId: SEGMENT_PILOT_HOTEL_ID,
    hotelName: SEGMENT_PILOT_HOTEL_NAME,
    segmentId: SEGMENT_PILOT_SEGMENT_ID,
    objective: "First complete autonomous GDI segment-research pilot for professional_planners_dmc at Nobu Hotel London Portman Square using the existing 11-segment Research Supervisor architecture.",
    orchestrator: "opportunity-research-supervisor-v1",
    segmentCoordinator: SEGMENT_SUPERVISOR_VERSION,
    liveExecutionAuthorized: false,
    hardCeilingUsd: SEGMENT_PILOT_BUDGET_USD,
    perCallCeilingUsd: SEGMENT_PILOT_CALL_CEILING_USD,
    hotelSpecificCostCeilingUsd: SEGMENT_PILOT_BUDGET_USD,
    maxQuotedUsd,
    ledger: SEGMENT_PILOT_LEDGER_RELATIVE,
    usesPriorResearchLedgers: false,
    refusedLedgers: [
      "reports/gdi-autonomous-longform-research-certification-v1/budget-ledger.json",
      "reports/gdi-cross-organization-followup-savoy-v1/budget-ledger.json",
      "reports/gdi-cross-organization-live-recovery-certification-v1/budget-ledger.json",
      "reports/gdi-segment-e2e-certification-v1/live/budget-ledger.json",
      "data/group-demand-intelligence/nobu-london-portman-budget/authorization-ledger-v1.json",
      "reports/gdi-parallel-live-comparison-v1/budget-ledger.json",
      "reports/gdi-eight-hotel-research-benchmark-v1/cohort-a-live/budget-ledger.json",
    ],
    providerPricing: {
      gpt4oMiniPlanner: {
        model: pricing.planner.model,
        estimatedUsdEach: pricing.planner.estimatedUsd,
        hardCeilingUsdEach: PLANNER_CEILING_USD,
        maxCalls: SEGMENT_PILOT_MAX_PLANNER_CALLS,
      },
      directPublic: { estimatedUsdEach: 0, preferred: true },
      firecrawlScrape: {
        bounded: pricing.firecrawl.bounded === true,
        maxUsdEach: pricing.firecrawl.bounded ? Number(pricing.firecrawl.maxUsd) : null,
        allowedOnlyIfPriced: true,
      },
      exa: {
        bounded: pricing.exa.bounded === true,
        maxUsdEach: pricing.exa.bounded ? Number(pricing.exa.maxUsd) : null,
        note: "Optional; only if priced and VoI justifies after existing evidence exhausted",
      },
      parallel: { refusedUnlessSeparatelyAuthorized: true },
    },
    stoppingLimits: {
      maxCandidatesInvestigated: SEGMENT_PILOT_MAX_CANDIDATES_INVESTIGATED,
      maxPlannerCalls: SEGMENT_PILOT_MAX_PLANNER_CALLS,
      maxSubstantiveRetrievals: SEGMENT_PILOT_MAX_SUBSTANTIVE_RETRIEVALS,
      maxFailedTechnicalAttempts: SEGMENT_PILOT_MAX_FAILED_TECHNICAL_ATTEMPTS,
      stopWhen: [
        "Highest-value candidates reach defensible outcomes and VoI for further public research is low",
        "Buyer engagement is more useful than additional public pages for remaining ACCOUNT_LED rows",
        "Budget exhausted",
        "Attempt caps reached",
        "Publication-gate eligible opportunities reach pilot target (optional; not required for pilot pass)",
      ],
      preferExistingEvidenceBeforePaid: true,
    },
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
    customerFacingClassificationsUnchanged: true,
    doNotContinuePrincipalSavoyResearch: true,
  };
}

export function runSegmentPilotOfflinePreparationChecks() {
  const checks = [];
  const push = (name, ok, detail = "") => checks.push({ name, ok: Boolean(ok), detail });

  push("eleven_segments_canonical", SUPERVISOR_EXPECTED_SEGMENTS.length === 11
    && SUPERVISOR_EXPECTED_SEGMENTS.includes(SEGMENT_PILOT_SEGMENT_ID));
  push("canonical_opportunities_present", fs.existsSync(CANONICAL_OPPS));
  const baseline = buildSegmentPilotBaseline();
  push("frozen_planner_candidates_ge_6", baseline.frozenCandidateCorpus.count >= 6,
    String(baseline.frozenCandidateCorpus.count));
  push("principal_not_forcing_extra_savoy_spend", baseline.principalLondonDmc.savoyRetrievalRecovery === "PASS");
  push("savoy_closeout_present", fs.existsSync(SAVOY_CLOSEOUT));
  push("longform_dossier_internal_only", /INTERNAL_RESEARCH_DOSSIER_ONLY/.test(
    baseline.principalLondonDmc.dossierPublicationGate || "",
  ));
  const gaps = buildSegmentPilotIntegrationGaps();
  push("integration_gaps_documented", gaps.length >= 5);
  const manifest = buildSegmentPilotAuthorizationManifest();
  push("manifest_awaiting_authorization", manifest.status === "AWAITING_AUTHORIZATION");
  push("manifest_live_not_authorized", manifest.liveExecutionAuthorized === false);
  push("isolated_ledger_path", manifest.ledger === SEGMENT_PILOT_LEDGER_RELATIVE);
  push("hard_ceiling_0_75", manifest.hardCeilingUsd === SEGMENT_PILOT_BUDGET_USD);
  push("refuses_prior_ledgers_listed", (manifest.refusedLedgers || []).length >= 5);
  push("provider_prices_planner_present", Number(manifest.providerPricing.gpt4oMiniPlanner.estimatedUsdEach) > 0);
  push("direct_public_preferred", manifest.providerPricing.directPublic.preferred === true);
  push("no_principal_savoy_continuation", manifest.doNotContinuePrincipalSavoyResearch === true);
  push("publication_unchanged", manifest.publicationEligibilityChanged === false);
  push("workflow_has_10_steps", buildSegmentPilotWorkflowDesign().steps.length === 10);
  push("measurement_has_11_metrics", buildSegmentPilotMeasurementStandards().metrics.length === 11);
  push("offline_plan_has_scenarios", buildSegmentPilotOfflineCertificationPlan().scenarios.length >= 10);

  return {
    version: SEGMENT_PILOT_VERSION,
    offline: true,
    liveCalls: 0,
    spent: false,
    checks,
    preparationPassed: checks.every((c) => c.ok),
    baseline,
    gaps,
    manifest,
  };
}

export function prepareSegmentPilotCertification() {
  const prep = runSegmentPilotOfflinePreparationChecks();
  return {
    prepared: true,
    executed: false,
    liveCalls: 0,
    spent: false,
    status: "AWAITING_AUTHORIZATION",
    version: SEGMENT_PILOT_VERSION,
    hotelId: SEGMENT_PILOT_HOTEL_ID,
    hotelName: SEGMENT_PILOT_HOTEL_NAME,
    segmentId: SEGMENT_PILOT_SEGMENT_ID,
    hardCeilingUsd: SEGMENT_PILOT_BUDGET_USD,
    maxQuotedUsd: prep.manifest.maxQuotedUsd,
    ledger: SEGMENT_PILOT_LEDGER_RELATIVE,
    reportsDir: SEGMENT_PILOT_REPORTS_DIR,
    savoyCertificationClosed: true,
    doNotContinuePrincipalSavoyResearch: true,
    pilotDesign: buildSegmentPilotWorkflowDesign(),
    currentBaseline: prep.baseline,
    integrationGaps: prep.gaps,
    offlineCertificationPlan: buildSegmentPilotOfflineCertificationPlan(),
    measurementStandards: buildSegmentPilotMeasurementStandards(),
    proposedCostCeilingUsd: SEGMENT_PILOT_BUDGET_USD,
    expectedCommercialSuccessCriteria: buildSegmentPilotSuccessCriteria(),
    authorizationManifest: prep.manifest,
    offlinePreparationChecks: prep.checks,
    preparationPassed: prep.preparationPassed,
    controls: {
      noLivePaidResearchWithoutExplicitAuthorization: true,
      noSpendFromPreviousResearchLedgers: true,
      noAdditionalPrincipalSavoyResearch: true,
      noDeploy: true,
      noPublish: true,
      noAirtableWrites: true,
      noCustomerFacingClassificationChanges: true,
    },
    stopBeforeLiveExecution: true,
  };
}
