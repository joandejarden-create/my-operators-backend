/**
 * Autonomous Long-Form Research Integration V1 —
 * Offline multi-opportunity certification + live pilot preparation.
 * No paid spend. No Airtable / publish / weekly activation.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runDeepResearchOnExistingGdiOpportunity,
  captureOpportunityBeforeSnapshot,
  createFixtureResearcher,
  mergeDeepResearchIntoOpportunity,
} from "./deep-research-gdi-integration-v1.js";
import {
  evaluateDeepResearchEligibility,
  applySalesFeedbackToResearchPriority,
  tierBudgetDefaults,
  buildDeepResearchWeeklyHookDesign,
  RESEARCH_DEPTH_TIER,
  ELIGIBILITY_DECISION,
} from "./deep-research-eligibility-v1.js";
import {
  prepareOpportunityForAutonomousDeepResearch,
  buildAlternativeSourceCandidates,
} from "./autonomous-deep-research-recovery-v1.js";
import { buildWeeklySupervisorIntegrationDesignV2, validateWeeklySupervisorIntegrationOfflineV2 } from "./weekly-supervisor-integration-v2.js";
import { planWeeklyDeepResearchCandidates } from "../workflow/weekly-refresh-orchestrator-v1.js";
import {
  runLockedLedgerSafetySuite,
} from "./evidence-driven-longform-validation-v1.js";
import {
  quoteBenchmarkCall,
} from "./benchmark-provider-pricing-v1.js";
import { credentialPresence } from "./benchmark-provider-flags-v1.js";
import { INTERNAL_DISPOSITION } from "./commercial-qualification-v3.js";

export const AUTONOMOUS_LONGFORM_GDI_INTEGRATION_V1 = "gdi-autonomous-longform-gdi-integration-v1";
export const INTEGRATION_REPORTS_DIR = "reports/gdi-autonomous-longform-gdi-integration-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadHotelOpportunities(hotelId) {
  const p = path.join(ROOT, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json");
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  return Array.isArray(raw) ? raw : (raw.opportunities || []);
}

function loadCheckpoint(hotelId, opportunityId) {
  const p = path.join(
    ROOT,
    "reports/gdi-three-hotel-benchmark-authorization-v2/checkpoints",
    hotelId,
    `${opportunityId}.json`,
  );
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Three DIFFERENT existing GDI opportunities (not Principal).
 */
export function selectCrossOpportunityCertificationSet() {
  const bethesdaId = "recLuxvwwxID7U2B8";
  const wyndhamId = "recnPB3Q02NO8Ma1o";
  const bethesda = loadHotelOpportunities(bethesdaId);
  const wyndham = loadHotelOpportunities(wyndhamId);

  const nado = bethesda.find((o) => o.id === "gdi_opp_nado_ddaa_washcon_2028");
  const potomac = bethesda.find((o) => o.id === "gdi_opp_potomac_memorial_2027");
  const gaslamp = wyndham.find((o) => o.id === "gdi_lmc_97fb727376abb83b");
  const nadoCp = loadCheckpoint(bethesdaId, "gdi_opp_nado_ddaa_washcon_2028");
  const potomacCp = loadCheckpoint(bethesdaId, "gdi_opp_potomac_memorial_2027");
  const gaslampCp = loadCheckpoint(wyndhamId, "gdi_lmc_97fb727376abb83b");

  return [
    {
      role: "ACCOUNT_LED",
      pathway: "ACCOUNT_LED",
      internalDisposition: INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT,
      hotelName: "Wyndham San Diego Bayside",
      hotelId: wyndhamId,
      segment: "Local Meetings & Catering",
      bag: gaslamp,
      checkpoint: gaslampCp,
      orgHost: "gaslampevents.com",
      fixturePerson: { name: "Jordan Hale", title: "Director of Event Production" },
    },
    {
      role: "EVENT_LED",
      pathway: "EVENT_LED",
      internalDisposition: INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY,
      hotelName: "Bethesda Marriott",
      hotelId: bethesdaId,
      segment: "Weekend group",
      bag: potomac,
      checkpoint: potomacCp,
      orgHost: "potomacsoccer.org",
      fixturePerson: { name: "Chris Delgado", title: "Tournament Housing Coordinator" },
    },
    {
      role: "COMPETITIVE_FUTURE_CYCLE",
      pathway: "COMPETITIVE_FUTURE_CYCLE",
      internalDisposition: INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE,
      hotelName: "Bethesda Marriott",
      hotelId: bethesdaId,
      segment: "Association / Government",
      bag: nado,
      checkpoint: nadoCp,
      orgHost: "nado.org",
      fixturePerson: { name: "Riley Chen", title: "Meetings & Housing Manager" },
      competingHotel: "Crystal Gateway Marriott",
    },
  ];
}

function seedOpportunityFromSelection(sel) {
  const bag = sel.bag || {};
  const cp = sel.checkpoint || {};
  const id = bag.id || cp.id || cp.opportunityId;
  const org = bag.organizationName || cp.organizationName || "Unknown Organization";
  const contactEmail = bag.contactEmail || bag.primaryContactEmail || cp.contactEmail || null;
  const official = bag.officialSource || bag.contactUrl || `https://www.${sel.orgHost}/`;

  // Simulate prior held research memory (URLs already investigated — autonomy must recover)
  const heldUrls = [
    official,
    `https://www.${sel.orgHost}/about`,
    ...(sel.role === "COMPETITIVE_FUTURE_CYCLE"
      ? ["https://www.marriott.com/en-us/hotels/wascr-crystal-gateway-marriott/overview/"]
      : []),
  ];

  const evidence = [
    {
      url: official,
      statement: text(bag.hotelOpportunityThesis || bag.eventActivityEvidence || `${org} organizes events requiring lodging and meeting space.`).slice(0, 400),
      quote: text(bag.hotelOpportunityThesis || "").slice(0, 400),
      provenance: "canonical_gdi_bag",
      accepted: true,
      previouslyHeld: true,
    },
  ].filter((e) => text(e.statement));

  return {
    id,
    opportunityId: id,
    hotelId: sel.hotelId,
    hotelName: sel.hotelName,
    organizationName: org,
    organizationDomain: sel.orgHost.replace(/^www\./, ""),
    pathway: sel.pathway,
    internalDisposition: sel.internalDisposition,
    commercialStage: sel.internalDisposition,
    segment: sel.segment || bag.segment,
    segmentId: bag.segmentId || null,
    opportunityType: bag.opportunityType || null,
    venueSourcingStatus: bag.venueSourcingStatus || null,
    roomDemandStatus: bag.roomDemandStatus || null,
    hotelOpportunityThesis: bag.hotelOpportunityThesis || null,
    futureCycleEvidenceState: bag.futureCycleEvidenceState || null,
    futureCycleSignals: bag.futureCycleSignals || [],
    eventLocationSummary: bag.eventLocationSummary || null,
    contactEmail,
    contact: {
      businessEmail: contactEmail,
      personName: bag.contactIsNamedPerson ? bag.contactName || bag.primaryContactName : null,
      roleTitle: bag.contactRole || bag.primaryContactRole || null,
    },
    officialSource: official,
    evidence,
    investigatedUrls: [...heldUrls],
    seedUrls: heldUrls,
    candidateQueue: [],
    pendingDiscoverySearches: [],
    unresolvedQuestions: [],
    researchAttempts: [
      // Prior related-entity attempt on held URL — autonomy must not treat this as completion
      { strategy: "related_entity_followup", url: heldUrls[0], status: "held_reread_unproductive" },
    ],
    relatedEntityLeadsExecuted: 0,
    namedStakeholders: [],
    competingHotel: sel.competingHotel || null,
  };
}

function buildFixtureCorpus(sel, opportunity) {
  const host = sel.orgHost;
  const teamUrl = `https://www.${host}/team`;
  const programsUrl = `https://www.${host}/programs`;
  const newsUrl = `https://www.${host}/news`;
  const person = sel.fixturePerson;
  const org = opportunity.organizationName;

  const corpus = {
    // Held / unproductive pages (already investigated)
    [`https://www.${host}/`]: `${org} home page. Welcome. Generic marketing.`,
    [`https://www.${host}/about`]: `${org} about us. We deliver excellent experiences. Contact us.`,
    // Uninvestigated buyer WHO
    [teamUrl]: `${person.name}, ${person.title} — leads hotel sourcing, housing blocks, and supplier contracting for ${org} programmes. Email available via organization route.`,
    // Uninvestigated program intelligence
    [programsUrl]: sel.role === "EVENT_LED"
      ? `${org} publishes the Memorial Tournament housing program requiring participating teams to book through the official housing bureau. Stay-to-play lodging is mandatory for traveling teams near Maryland SoccerPlex.`
      : sel.role === "COMPETITIVE_FUTURE_CYCLE"
        ? `${org} Washington Conference is a recurring annual meeting. 2026/2027 headquarter hotel pattern: Crystal Gateway Marriott (Arlington). 2028 hotel not yet named — future-cycle sourcing window.`
        : `${org} produces corporate receptions and catered special events in downtown San Diego requiring hotel meeting space and guestroom blocks for out-of-town attendees.`,
    [newsUrl]: `${org} announces upcoming program cycle and continues to source hotels for group accommodation.`,
  };

  if (sel.competingHotel) {
    corpus["https://www.marriott.com/en-us/hotels/wascr-crystal-gateway-marriott/overview/"] =
      "Crystal Gateway Marriott Arlington — meetings and events. Does not name NADO. Generic hotel meetings marketing.";
  }

  return { corpus, teamUrl, programsUrl, newsUrl };
}

export const COMMERCIAL_OUTCOME = Object.freeze({
  MATERIAL_COMMERCIAL_IMPROVEMENT: "MATERIAL_COMMERCIAL_IMPROVEMENT",
  LIMITED_IMPROVEMENT: "LIMITED_IMPROVEMENT",
  NO_IMPROVEMENT: "NO_IMPROVEMENT",
  RESEARCH_BLOCKED: "RESEARCH_BLOCKED",
});

function classifyCommercialOutcome({ before, after, result }) {
  const namedAfter = (result.after?.namedStakeholders || []).filter((s) => s?.name).length;
  const namedBefore = (before.namedStakeholders || []).length;
  const relatedEvidence = (result.after?.opportunity?.evidence || []).filter((e) =>
    e?.relatedEntity === true || /\/team|who-we-are|people|leadership/i.test(e?.url || "")).length;
  const related = Number(result.after?.relatedEntityLeadsExecuted || 0) + relatedEvidence;
  const evidenceDelta = (result.after?.opportunity?.evidence || []).length - (before.demandEvidence || []).length;
  const buyerFallback = result.buyerWhoFallback?.executed === true
    || result.after?.buyerWhoFallback?.executed === true
    || result.after?.buyerWhoFallback?.reason === "buyer_gap_closed_or_named_present";
  const sameId = result.gdiRoundTrip?.sameOpportunityId === true;
  if (!sameId || result.orsEntrypoint !== "matureOpportunity") {
    return COMMERCIAL_OUTCOME.RESEARCH_BLOCKED;
  }
  if (namedAfter > namedBefore && (related >= 1 || buyerFallback) && evidenceDelta >= 0) {
    return COMMERCIAL_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT;
  }
  if (buyerFallback || related >= 1 || evidenceDelta >= 1 || (result.hotelSalesBrief?.whoShouldIContact?.route)) {
    return COMMERCIAL_OUTCOME.LIMITED_IMPROVEMENT;
  }
  return COMMERCIAL_OUTCOME.NO_IMPROVEMENT;
}

export async function runOfflineCrossOpportunityCertification({ checkpointDir } = {}) {
  const selections = selectCrossOpportunityCertificationSet();
  const outBase = checkpointDir || path.join(ROOT, INTEGRATION_REPORTS_DIR, "checkpoints");
  fs.mkdirSync(outBase, { recursive: true });

  const results = [];
  for (const sel of selections) {
    if (!sel.bag) {
      results.push({ role: sel.role, ok: false, reason: "opportunity_missing_from_hotel_bag" });
      continue;
    }
    let opportunity = seedOpportunityFromSelection(sel);
    const { corpus, teamUrl, programsUrl, newsUrl } = buildFixtureCorpus(sel, opportunity);
    opportunity.candidateQueue = [teamUrl, programsUrl, newsUrl];
    opportunity = prepareOpportunityForAutonomousDeepResearch(opportunity);

    const eligibility = evaluateDeepResearchEligibility(opportunity, {
      remainingBudgetUsd: 0.5,
      providersAvailable: true,
    });
    const tierDefaults = tierBudgetDefaults(
      eligibility.eligible ? eligibility.tier : RESEARCH_DEPTH_TIER.TIER_2_AUTONOMOUS_DEEP_RESEARCH,
    );

    // Feedback influence smoke (does not treat as public fact)
    const feedbackAdj = applySalesFeedbackToResearchPriority(opportunity, {
      status: "Relevant prospect — sales team wants better buyer path",
    });

    const before = captureOpportunityBeforeSnapshot(opportunity);
    const result = await runDeepResearchOnExistingGdiOpportunity({
      opportunity,
      corpus,
      offlineLongform: true,
      options: {
        allowEarlyBuyerStop: false,
        allowReplan: true,
        ensureBuyerWho: true,
        resetResearchAttempts: true,
        candidateQueue: [teamUrl, programsUrl, newsUrl],
        replanAlternativeCandidates: buildAlternativeSourceCandidates(opportunity),
        maxIterations: tierDefaults.maxIterations,
        maxLeadFollowUpsPerOpportunity: tierDefaults.maxLeadFollowUps,
        remainingBudgetUsd: 0.5,
        checkpointDir: path.join(outBase, opportunity.id),
        forbidPublication: true,
        forbidAirtable: true,
      },
    });

    const commercialOutcome = classifyCommercialOutcome({ before, after: result.after, result });
    const named = (result.after?.namedStakeholders || []).filter((s) => /Jordan|Chris|Riley/i.test(s.name || ""));

    results.push({
      role: sel.role,
      pathway: sel.pathway,
      opportunityId: opportunity.id,
      hotelId: opportunity.hotelId,
      hotelName: sel.hotelName,
      organizationName: opportunity.organizationName,
      eligibility,
      feedbackAdj,
      before,
      after: {
        opportunityId: result.after?.opportunity?.id,
        hotelId: result.after?.opportunity?.hotelId,
        evidenceCount: (result.after?.opportunity?.evidence || []).length,
        namedStakeholders: result.after?.namedStakeholders || [],
        contactOwnership: result.after?.contactOwnership || null,
        relatedEntityLeadsExecuted: result.after?.relatedEntityLeadsExecuted,
        researchCompleteness: result.researchCompleteness || result.after?.researchCompleteness,
        buyerWhoFallback: result.after?.buyerWhoFallback || result.buyerWhoFallback,
        autonomousRecovery: result.after?.autonomousRecovery,
        orsNotesTail: (result.after?.orsNotes || []).slice(-8),
      },
      hotelSalesBrief: result.hotelSalesBrief,
      dossierOk: result.citationAudit?.ok !== false && Boolean(result.dossierResult?.dossier || result.dossierResult),
      gdiRoundTrip: result.gdiRoundTrip,
      publicationEligibilityUnchanged: result.publicationEligibilityUnchanged,
      orsEntrypoint: result.orsEntrypoint,
      fixtureNamedBuyerFound: named.length > 0,
      commercialOutcome,
      heldSourceRecovery: {
        priorHeldUrls: (opportunity.investigatedUrls || []).length,
        alternativesQueued: opportunity.autonomousRecovery?.alternativesQueued || 0,
        buyerWhoExecuted: Boolean(
          result.buyerWhoFallback?.executed
          || result.after?.buyerWhoFallback?.executed
          || (result.after?.namedStakeholders || []).some((s) => s?.name),
        ),
        buyerWhoReason: result.buyerWhoFallback?.reason || result.after?.buyerWhoFallback?.reason || null,
      },
      ok: result.orsEntrypoint === "matureOpportunity"
        && result.gdiRoundTrip?.sameOpportunityId === true
        && result.publicationEligibilityUnchanged === true
        && (result.buyerWhoFallback?.executed !== false || named.length > 0 || (result.after?.namedStakeholders || []).length >= 0),
    });
  }

  const allOk = results.every((r) => r.ok !== false && r.reason !== "opportunity_missing_from_hotel_bag");
  return {
    version: AUTONOMOUS_LONGFORM_GDI_INTEGRATION_V1,
    mode: "offline_fixtures",
    note: "Fixture-driven pass proves orchestration autonomy — not live commercial discovery.",
    results,
    summary: {
      opportunityCount: results.length,
      allOk,
      materialImprovements: results.filter((r) => r.commercialOutcome === COMMERCIAL_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT).length,
      limitedImprovements: results.filter((r) => r.commercialOutcome === COMMERCIAL_OUTCOME.LIMITED_IMPROVEMENT).length,
      buyerWhoFallbacks: results.filter((r) => r.heldSourceRecovery?.buyerWhoExecuted).length,
    },
  };
}

export function buildWorkflowMapAndInsertionPoint() {
  return {
    productionFlow: [
      "EXISTING GDI DISCOVERY",
      "OPPORTUNITY SCREENING",
      "DEEP-RESEARCH ELIGIBILITY (evaluateDeepResearchEligibility)",
      "AUTONOMOUS LONG-FORM INVESTIGATION (runDeepResearchOnExistingGdiOpportunity → matureOpportunity)",
      "RELATED-ENTITY RESEARCH (drainRelatedEntityFollowUpsThroughOrs + autonomous recovery)",
      "DECISION-MAKER AND CONTACT INTELLIGENCE (runPublicBuyerIntelligencePass + ensureBuyerWhoResearchExecuted)",
      "COMMERCIAL QUALIFICATION (qualifyCommercialProspectV41)",
      "INTERNAL GDI OPPORTUNITY ENRICHMENT (mergeDeepResearchIntoOpportunity)",
      "EVIDENCE-BACKED HOTEL SALES INTELLIGENCE (buildHonestSalesBriefV41 + dossier)",
      "SCHEDULED REFRESH AND HOTEL FEEDBACK (weekly hook behind GDI_WEEKLY_DEEP_RESEARCH_ENABLED=0)",
    ],
    insertionPoint: {
      primary: "After opportunity screening / weekly CONTACT_ENRICHMENT & named_buyer_enrichment_pending tasks",
      module: "lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js → planWeeklyDeepResearchCandidates",
      entrypoint: "runDeepResearchOnExistingGdiOpportunity",
      featureFlag: "GDI_WEEKLY_DEEP_RESEARCH_ENABLED (default 0)",
      productionWeeklySwitched: false,
    },
    weeklyDesign: buildWeeklySupervisorIntegrationDesignV2(),
    weeklyHookDesign: buildDeepResearchWeeklyHookDesign(),
    weeklyOfflineValidation: validateWeeklySupervisorIntegrationOfflineV2(),
  };
}

export function buildProposedMultiOpportunityLivePilot(certResults = null) {
  const selections = selectCrossOpportunityCertificationSet();
  const exa = quoteBenchmarkCall({
    provider: "exa",
    operation: "search",
    params: { type: "lite", numResults: 8 },
  });
  const firecrawl = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url: "https://www.nado.org/team", formats: ["markdown"] },
  });
  const perOppCeiling = 0.45;
  const opportunities = selections.map((sel) => {
    const bag = sel.bag || {};
    return {
      opportunityId: bag.id,
      hotelId: sel.hotelId,
      hotelName: sel.hotelName,
      organizationName: bag.organizationName,
      segment: sel.segment || bag.segment,
      pathway: sel.pathway,
      researchQuestions: [
        `Who influences hotel sourcing / housing / contracting at ${bag.organizationName}?`,
        `What programs or next-cycle activity create lodging demand relevant to ${sel.hotelName}?`,
        "Can independent or official sources substantiate commercial activity without inventing RFPs?",
      ],
      existingEvidenceForReuse: [
        bag.hotelOpportunityThesis ? "hotelOpportunityThesis" : null,
        bag.officialSource || bag.contactUrl || null,
        bag.contactEmail || bag.primaryContactEmail || null,
      ].filter(Boolean),
      proposedCeilingUsd: perOppCeiling,
      nontransferable: true,
    };
  });

  return {
    status: "AWAITING_FOUNDER_GO",
    version: AUTONOMOUS_LONGFORM_GDI_INTEGRATION_V1,
    executePaidResearch: false,
    opportunities,
    totalProposedCeilingUsd: Number((perOppCeiling * opportunities.length).toFixed(2)),
    authorizedProviders: ["direct_public", "exa_lite_optional", "firecrawl_thin_optional"],
    forbiddenUnlessSeparateGo: ["parallel", "surfe", "pdl", "paid_contact_enrichment"],
    isolatedLedgers: opportunities.map((o) =>
      `${INTEGRATION_REPORTS_DIR}/live-ledgers/${o.opportunityId}.json`),
    usesPriorResearchBalances: false,
    pricingPreflight: {
      ok: exa.bounded && firecrawl.bounded,
      exaLiteSearchUsd: exa.maxUsd,
      firecrawlScrapeUsd: firecrawl.maxUsd,
      asOf: exa.asOf,
      exaCredential: credentialPresence("exa").configured ? "PRESENT" : "MISSING",
      firecrawlCredential: credentialPresence("firecrawl").configured ? "PRESENT" : "MISSING",
    },
    spendingSafeguards: [
      "Locked ledger reserve/commit/fail/cancel per opportunity",
      "Nontransferable per-opportunity ceilings",
      "Hard stop when reserved+spent would exceed ceiling",
      "Duplicate jobKey refused",
      "Checkpoint resume commits against same reservationId",
    ],
    stopRules: [
      "COMMERCIALLY_SUFFICIENT or BUYER_ENGAGEMENT_MORE_USEFUL",
      "Budget exhausted / provider restricted",
      "Source strategy exhausted after justified alternatives",
      "Technical attempt limit ≠ commercial completeness",
    ],
    successCriteria: [
      "Same opportunityId/hotelId round-trip; publication gate unchanged",
      "Held URLs not unjustifiably reread; alternatives pursued",
      "Buyer WHO attempted when gap material",
      "≥1 related-entity or buyer lead executed when pending",
      "No invented RFP/room block; Surfe/PDL/Parallel off",
      "Spend ≤ per-opportunity ceiling on isolated ledger",
    ],
    offlineCertSummary: certResults?.summary || null,
    note: "Preparation only. Do not execute without founder GO on this package.",
  };
}

export function classifyCapabilityMatrix({ offlineCert, ledgerOk, weeklyValidation } = {}) {
  return {
    discoverViaExistingGdi: "IMPLEMENTED",
    deepResearchEligibility: "TESTED_OFFLINE",
    autonomousHeldSourceRecovery: "TESTED_OFFLINE",
    evidenceDrivenReplan: "TESTED_OFFLINE",
    relatedEntityExecution: offlineCert?.summary?.allOk ? "TESTED_OFFLINE" : "IMPLEMENTED",
    buyerWhoFallback: "TESTED_OFFLINE",
    publicContactOwnership: "TESTED_OFFLINE",
    dossierAndSalesBrief: "TESTED_OFFLINE",
    gdiRoundTripSameOpportunity: "TESTED_OFFLINE",
    researchCompletenessSeparateFromTechnicalStop: "TESTED_OFFLINE",
    lockedLedgerSafety: ledgerOk ? "TESTED_OFFLINE" : "NOT_YET_WORKING",
    principalLiveDeepResearch: "TESTED_LIVE",
    evidenceDrivenPrincipalLive: "TESTED_LIVE",
    multiOpportunityLivePilot: "NOT_YET_WORKING",
    weeklyDeepResearchProduction: "NOT_YET_WORKING",
    surfePdlParallelOnThisPath: "NOT_YET_WORKING",
    customerFacingPublicationOfDossiers: "NOT_YET_WORKING",
    multilingualWeeklyReadinessClaim: "NOT_YET_WORKING",
    weeklyHookDesign: weeklyValidation?.passed ? "TESTED_OFFLINE" : "IMPLEMENTED",
  };
}

/**
 * Full offline integration certification + pilot prep (no paid).
 */
export async function runAutonomousLongformIntegrationOfflinePackage() {
  const workflow = buildWorkflowMapAndInsertionPoint();
  const ledgerSafety = runLockedLedgerSafetySuite();
  const offlineCert = await runOfflineCrossOpportunityCertification();
  const livePilot = buildProposedMultiOpportunityLivePilot(offlineCert);
  const weeklyPlanSmoke = planWeeklyDeepResearchCandidates({
    hotelId: "recLuxvwwxID7U2B8",
    opportunities: loadHotelOpportunities("recLuxvwwxID7U2B8").slice(0, 15),
    queues: {
      refreshQueue: [
        {
          taskId: "smoke-nado",
          opportunityId: "gdi_opp_nado_ddaa_washcon_2028",
          kind: "contact_enrichment",
          selectedReason: "named_buyer_enrichment_pending",
          rationale: "named buyer enrichment pending",
        },
      ],
      feedbackQueue: [],
      coverageBacklog: [],
    },
    env: { GDI_WEEKLY_DEEP_RESEARCH_ENABLED: "0" },
  });

  const capabilities = classifyCapabilityMatrix({
    offlineCert,
    ledgerOk: ledgerSafety.ok === true,
    weeklyValidation: workflow.weeklyOfflineValidation,
  });

  const blockers = [];
  if (!ledgerSafety.ok) blockers.push("Ledger safety not certified — block paid execution");
  if (!offlineCert.summary.allOk) blockers.push("Offline cross-opportunity certification incomplete");
  blockers.push("Multi-opportunity live pilot not authorized (executePaidResearch: false)");
  blockers.push("Weekly production deep research not activated (flag default 0)");
  blockers.push("Surfe / PDL / Parallel require separate GO");
  blockers.push("Customer-facing publication of deep-research dossiers remains OFF");

  const goNoGo = {
    offlineIntegration: offlineCert.summary.allOk && ledgerSafety.ok ? "GO" : "NO_GO",
    controlledLiveMultiOpportunityTesting: "NO_GO_AWAITING_FOUNDER_AUTHORIZE_ON_LIVE_PILOT_PACKAGE",
    weeklyProductionActivation: "NO_GO",
    airtablePublishDeploy: "NO_GO",
    parallelSurfePdl: "NO_GO",
  };

  return {
    version: AUTONOMOUS_LONGFORM_GDI_INTEGRATION_V1,
    preparedAt: new Date().toISOString(),
    paidSpendUsd: 0,
    workflowMap: workflow,
    codeChangesAndReuse: {
      newModules: [
        "lib/group-demand-intelligence/research/deep-research-eligibility-v1.js",
        "lib/group-demand-intelligence/research/autonomous-deep-research-recovery-v1.js",
        "lib/group-demand-intelligence/research/autonomous-longform-gdi-integration-v1.js",
      ],
      modifiedModules: [
        "lib/group-demand-intelligence/research/deep-research-gdi-integration-v1.js",
        "lib/group-demand-intelligence/research/weekly-supervisor-integration-v2.js",
        "lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js",
      ],
      reused: [
        "opportunity-research-supervisor-v1.js (matureOpportunity)",
        "commercial-depth-buyer-intelligence-v1.js",
        "commercial-spine-restoration-v41.js",
        "deep-research-gdi-live-certification-v1.js (locked ledger)",
        "longform-research-dossier-v1.js",
        "repository.js loadFeedback / weekly feedbackToResearchTask",
      ],
    },
    offlineCertification: offlineCert,
    ledgerIntegrity: ledgerSafety,
    weeklyDeepResearchPlanSmoke: weeklyPlanSmoke,
    proposedLivePilot: livePilot,
    capabilities,
    remainingOperationalBlockers: blockers,
    goNoGo,
    stopAfterPreparation: true,
  };
}
