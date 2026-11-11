/**
 * Bethesda package writer + offline certification for Autonomous Demand Discovery V5.
 * No paid APIs · no Airtable · weekly production OFF.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AUTONOMOUS_DEMAND_DISCOVERY_V5,
  DISCOVERY_PATHWAY,
  DEMAND_MECHANISM_CLASS,
  PROPOSED_V5_STAGE_A_CEILING_USD,
  V5_INCREMENTAL_PRIORITY_SEGMENTS,
  generateTriPathDiscoveryStrategies,
  buildElevenSegmentStrategyMatrix,
  buildSourceFamilyStrategy,
  planWeeklyTriPathDiscoveryCycle,
  buildV5OfflineFixtureSuite,
  replayBethesdaStageAFalsePositives,
  buildV5DiscoveryFunnelReport,
  assessDemandMechanism,
} from "./autonomous-demand-discovery-v5.js";
import {
  loadHotelGdiConfig,
  assessCapturableHotelDemandFit,
  COMMERCIAL_DEMAND_DISCOVERY_V4,
} from "./commercial-demand-discovery-v4.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
  runBethesdaDatabaseReconciliationPreflight,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { buildBethesdaImmutableBaselineManifest } from "./bethesda-weekly-net-new-discovery-v1.js";
import { assertPaidContactProvidersDisabled } from "./commercial-spine-restoration-v41.js";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";

export const REPORTS_DIR = "reports/gdi-autonomous-demand-discovery-v5-bethesda";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function writeMd(p, md) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, md.endsWith("\n") ? md : `${md}\n`);
}

export function runOfflineV5Certifications(hotel = null) {
  const cfg = hotel || loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
  };
  const strategies = generateTriPathDiscoveryStrategies(cfg);
  const fixtures = buildV5OfflineFixtureSuite(cfg);
  const replay = replayBethesdaStageAFalsePositives(cfg);
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });
  const weeklyPlan = planWeeklyTriPathDiscoveryCycle(cfg);

  const checks = {
    allElevenSegmentsCovered: strategies.allElevenSegmentsCovered === true,
    hasOrganizationLed: strategies.byPathway[DISCOVERY_PATHWAY.ORGANIZATION_LED] > 0,
    hasTriggerLed: strategies.byPathway[DISCOVERY_PATHWAY.TRIGGER_LED] > 0,
    hasEventLed: strategies.byPathway[DISCOVERY_PATHWAY.EVENT_LED] > 0,
    fixtureSuitePassed: fixtures.allPassed === true,
    falsePositiveReplayPassed: replay.every((r) => r.ok),
    paidContactProvidersDisabled: paidGuard.ok === true,
    weeklyProductionDisabled: weeklyPlan.weeklyProductionEnabled === false,
    mpiPatternAccepted: replay.find((r) => r.id === "mpi_positive")?.accepted === true,
    housingPageRejected: replay.find((r) => r.id === "premier_cup_baseline" || r.id === "canopy_competitor_rfp")
      ? replay.filter((r) => r.id !== "mpi_positive").every((r) => r.accepted === false)
      : false,
  };

  return {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    ok: Object.values(checks).every(Boolean),
    checks,
    strategyCount: strategies.strategyCount,
    byPathway: strategies.byPathway,
    fixtures,
    replay,
    weeklyPlanMode: weeklyPlan.mode,
  };
}

export function writeAutonomousDemandDiscoveryV5BethesdaPackage() {
  const outDir = path.join(ROOT, REPORTS_DIR);
  fs.mkdirSync(outDir, { recursive: true });

  const hotel = loadHotelGdiConfig(BETHESDA_CANONICAL_HOTEL_ID) || {
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    displayName: BETHESDA_CANONICAL_HOTEL_NAME,
    city: "Bethesda",
    capabilityProfile: {
      totalGuestrooms: 407,
      meetingRoomCount: 27,
      largestBallroomSqFt: 4592,
    },
  };

  const recon = runBethesdaDatabaseReconciliationPreflight();
  const baseline = buildBethesdaImmutableBaselineManifest({ reconciliation: recon });
  const strategies = generateTriPathDiscoveryStrategies(hotel);
  const matrix = buildElevenSegmentStrategyMatrix(hotel);
  const sources = buildSourceFamilyStrategy();
  const weeklyPlan = planWeeklyTriPathDiscoveryCycle(hotel);
  const cert = runOfflineV5Certifications(hotel);
  const paidGuard = assertPaidContactProvidersDisabled({ authorizeSurfe: false, authorizePdl: false });

  const funnel = buildV5DiscoveryFunnelReport({
    hits: strategies.strategies.map((s) => ({ pathway: s.pathway, segment: s.segment })),
    qualifications: cert.fixtures.results.map((r) => ({
      pathway: DISCOVERY_PATHWAY.ORGANIZATION_LED,
      countsAsNetNewOpportunity: r.accepted && r.candidateKind === "OPPORTUNITY",
      mechanism: { mechanismClass: r.mechanismClass },
      fit: { fitClass: r.accepted ? "POTENTIALLY_CAPTURABLE_CORE" : "INSUFFICIENT_GEO_EVIDENCE" },
      deepResearchEligible: r.accepted,
      normalized: { discoveryId: r.id, organizationName: r.id, segment: "corporate_meetings_training" },
      candidateKind: r.candidateKind,
      segment: "corporate_meetings_training",
    })),
    pathwaySpendUsd: {
      [DISCOVERY_PATHWAY.ORGANIZATION_LED]: 0,
      [DISCOVERY_PATHWAY.TRIGGER_LED]: 0,
      [DISCOVERY_PATHWAY.EVENT_LED]: 0,
    },
    actionableNetNewIds: [],
  });

  writeJson(path.join(outDir, "01-existing-gdi-discovery-architecture-audit.json"), {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    reusedModules: [
      "commercial-demand-discovery-v4.js (strategies, fit, screen, normalize, dedupe, enqueue)",
      "opportunity-research-supervisor-v1.js / matureOpportunity (ORS sole orchestrator)",
      "deep-research-gdi-integration-v1.js (runDeepResearchOnExistingGdiOpportunity)",
      "commercial-spine-restoration-v41.js (contact ownership, paid guard)",
      "bethesda-weekly-net-new-discovery-v1.js (WEEKLY_FINDING_CLASS, baseline classify)",
      "bethesda-full-demand-database-reconciliation-v1.js (immutable baseline)",
      "commercial-qualification-v3.js (dispositions — via ORS path)",
      "buyer-intelligence / WHO enrichment modules (existing; Surfe gated)",
    ],
    extendedNotReplaced: {
      v4EventLed: "Preserved and tagged EVENT_LED with early RFP hygiene",
      newOrganizationLed: "generateOrganizationLedStrategies",
      newTriggerLed: "generateTriggerLedStrategies",
      demandMechanism: "assessDemandMechanism + acceptance standards",
    },
    latestBethesdaEvidence: {
      stageA: "208 hits → 11 NET_NEW → 3 Stage B eligible (post gate)",
      stageB: "MPI Potomac PRE_SOURCING; ICF EM RFP expired; USBC historical",
      selected26Concentration: "associations + medical + sports dominate",
      baselineDistinct: baseline.counts?.distinctCanonicalOpportunities ?? null,
    },
    missingCapabilityAddressed: [
      "Organization-first discovery pathway",
      "Trigger-led causal demand validation",
      "Demand-mechanism VERIFIED/INFERRED/UNSUPPORTED classes",
      "Account intelligence vs opportunity distinction",
      "Tri-path funnel economics reporting",
    ],
    parentVersions: {
      v4: COMMERCIAL_DEMAND_DISCOVERY_V4,
      v5: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    },
  });

  writeJson(path.join(outDir, "02-organization-led-discovery-implementation.json"), {
    entrypoints: [
      "generateOrganizationLedStrategies",
      "acceptOrganizationLedCandidate",
      "qualifyV5DiscoveryCandidate(pathway=ORGANIZATION_LED)",
    ],
    acceptanceStandard: [
      "real organization",
      "specific demand-generating activity or defensible recurring mechanism",
      "evidence supporting activity",
      "plausible Bethesda connection",
      "defined commercial pathway",
      "buyer function to investigate",
      "unanswered question or sales action",
    ],
    rejects: [
      "company directory / office address alone",
      "generic hiring announcement",
      "DMC landing without sourcing/program",
      "unsupported room-night estimates",
    ],
    strategyCount: strategies.strategies.filter((s) => s.pathway === DISCOVERY_PATHWAY.ORGANIZATION_LED).length,
    sampleStrategies: strategies.strategies
      .filter((s) => s.pathway === DISCOVERY_PATHWAY.ORGANIZATION_LED)
      .slice(0, 8),
  });

  writeJson(path.join(outDir, "03-trigger-led-discovery-implementation.json"), {
    entrypoints: [
      "generateTriggerLedStrategies",
      "acceptTriggerLedCandidate",
      "qualifyV5DiscoveryCandidate(pathway=TRIGGER_LED)",
    ],
    validation: [
      "trigger verified",
      "timing relevant",
      "plausible meeting/travel/catering mechanism",
      "Bethesda can compete",
      "not duplicate of baseline",
      "further research has commercial value",
    ],
    rejects: [
      "contract award without travel/training",
      "hiring without travel/training",
      "unsupported demand mechanism",
    ],
    strategyCount: strategies.strategies.filter((s) => s.pathway === DISCOVERY_PATHWAY.TRIGGER_LED).length,
    sampleStrategies: strategies.strategies
      .filter((s) => s.pathway === DISCOVERY_PATHWAY.TRIGGER_LED)
      .slice(0, 8),
  });

  writeJson(path.join(outDir, "04-event-led-integration-and-improvements.json"), {
    preservesV4: true,
    earlyChecks: [
      "event_dates_forward_looking",
      "rfp_deadline_not_expired",
      "respondent_class_matches_hotel_or_venue",
      "destination_eligibility",
      "existing_hotel_commitments",
      "reject_housing_tab_as_opportunity",
    ],
    strategyCount: strategies.strategies.filter((s) => s.pathway === DISCOVERY_PATHWAY.EVENT_LED).length,
    referencePositiveCase: {
      organization: "MPI Potomac Chapter",
      class: "PRE_SOURCING",
      note: "Preserve Stage B finding; not a confirmed room block; not the only pattern to optimize toward",
    },
  });

  writeJson(path.join(outDir, "05-eleven-segment-strategy-matrix.json"), matrix);
  writeJson(path.join(outDir, "06-source-family-and-research-query-strategy.json"), sources);

  writeJson(path.join(outDir, "07-hotel-fit-qualification-integration.json"), {
    reused: "assessCapturableHotelDemandFit + screenDiscoveryHitCommercialRelevance",
    exampleCore: assessCapturableHotelDemandFit({
      organizationName: "Local Corp",
      programName: "leadership retreat Bethesda",
      demandSignalSummary: "corporate offsite near NIH Rockville",
    }, hotel),
    distinctions: [
      "current capturable",
      "future capturable",
      "credible account development",
      "watchlist",
      "unsuitable",
    ],
    note: "Nearby organization alone is insufficient; active RFP not required for account-development",
  });

  writeJson(path.join(outDir, "08-demand-mechanism-evidence-rules.json"), {
    classes: DEMAND_MECHANISM_CLASS,
    assess: "assessDemandMechanism",
    examples: {
      verified: assessDemandMechanism({
        organizationName: "MPI Potomac",
        programName: "venue host RFP meeting space catering",
        demandSignalSummary: "in-kind hotel RFP speaker room",
      }),
      inferred: assessDemandMechanism({
        organizationName: "ScaleUp Bio",
        triggerSummary: "expansion with onboarding training offsite",
        programName: "facility expansion training offsite",
        demandSignalSummary: "new office workforce training lodging",
      }),
      unsupported: assessDemandMechanism({
        organizationName: "Acme",
        programName: "About us",
        demandSignalSummary: "office directory",
      }),
    },
    mapIntoCanonical: "signalKind + evidenceKind; do not invent public statuses that bypass publication gates",
  });

  writeJson(path.join(outDir, "09-canonical-net-new-reconciliation.json"), {
    baselineImmutable: baseline.immutable === true,
    baselineDistinctOpportunities: baseline.counts?.distinctCanonicalOpportunities,
    classes: [
      "NET_NEW",
      "REACTIVATED",
      "EXISTING_ENRICHED",
      "EXISTING_UNCHANGED",
      "DUPLICATE",
      "EXCLUDED",
    ],
    rules: [
      "new company name ≠ new opportunity",
      "known org + distinct demand mechanism may be NET_NEW",
      "research-memory rediscovery ≠ NET_NEW",
      "new contacts ≠ new opportunity",
      "account intelligence without mechanism ≠ NET_NEW opportunity count",
    ],
    classifier: "classifyFindingAgainstBethesdaBaseline",
  });

  writeJson(path.join(outDir, "10-ors-deep-research-integration.json"), {
    entrypoint: "runDeepResearchOnExistingGdiOpportunity → matureOpportunity",
    identityFirst: true,
    enqueueHelper: "enqueueV5IntoOrsQueue → enqueueDiscoveryIntoOrsQueue",
    requirements: [
      "commercially meaningful questions",
      "related-entity follow-ups",
      "buyer accessibility",
      "replan after unproductive sources",
      "checkpoints",
      "completeness evaluation",
      "cited intelligence",
    ],
    note: "Technically completed run ≠ commercially sufficient",
  });

  writeJson(path.join(outDir, "11-buyer-intelligence-and-contact-enrichment-integration.json"), {
    chain: "ORG → ACTIVITY → SOURCING RESPONSIBILITY → BUYER/PLANNER → CONTACT ROUTE → SALES ACTION",
    reused: [
      "public WHO research",
      "stakeholder classification",
      "contact ownership validation (V4.1)",
      "targeted enrichment request builders",
    ],
    namedPersonPreferredNotMandatory: true,
    functionalRouteOk: true,
    genericInboxInsufficient: true,
    surfePdlParallel: "SEPARATE_GO_REQUIRED",
    surfePersistence: "must not persist Surfe-derived contacts in permanent opportunity records",
    autoContactProspects: false,
    paidGuard,
  });

  writeJson(path.join(outDir, "12-weekly-plan-only-integration.json"), weeklyPlan);

  writeJson(path.join(outDir, "13-offline-regression-and-safety-results.json"), cert);

  writeJson(path.join(outDir, "14-previous-bethesda-false-positive-replay.json"), {
    replay: cert.replay,
    allNonMpiRejected: cert.replay.filter((r) => r.id !== "mpi_positive").every((r) => r.accepted === false),
    mpiAccepted: cert.replay.find((r) => r.id === "mpi_positive")?.accepted === true,
  });

  writeJson(path.join(outDir, "15-discovery-pathway-reporting-and-economics.json"), {
    funnelSpec: funnel,
    note: "Offline fixture illustration — live cost/yield requires authorized Stage A",
    zeroActionableRule: "costPerActionableNetNew = UNDEFINED_ZERO_ACTIONABLE_YIELD when yield is 0",
  });

  writeJson(path.join(outDir, "16-proposed-stage-a-live-pilot-package.json"), {
    authorizeRequired: true,
    ceilingUsd: PROPOSED_V5_STAGE_A_CEILING_USD,
    allocateAcross: Object.values(DISCOVERY_PATHWAY),
    reserveUnderrepresented: [...V5_INCREMENTAL_PRIORITY_SEGMENTS],
    isolatedLedgerRequired: true,
    reusePriorBalances: false,
    providers: ["exa", "firecrawl", "direct_public"],
    forbidden: ["surfe", "pdl", "parallel", "airtable", "publish", "weekly_production"],
    successMetric: "named organizations + demand mechanisms + Bethesda-fit — not raw hit count",
    doNotAutoAdvanceToStageB: true,
    comparePathways: true,
  });

  writeJson(path.join(outDir, "17-proposed-stage-b-deep-research-approach.json"), {
    authorizeRequired: true,
    afterStageAFounderReview: true,
    prioritize: [
      "high expected commercial value",
      "credible Bethesda demand",
      "sourcing accessibility",
      "contact intelligence potential",
      "researchable open questions",
    ],
    orsPath: "runDeepResearchOnExistingGdiOpportunity",
    surfePdlParallel: "separate explicit GO",
    preserveMpiAsReference: "PRE_SOURCING — not confirmed room block",
  });

  writeJson(path.join(outDir, "18-known-blockers.json"), {
    blockers: [
      "Corporate/local demand often unpublished — Exa lite yield remains event/RFP biased without org/trigger pathways (now implemented offline)",
      "ORS may under-credit functional RFP inboxes (Stage B MPI override pattern) — buyer ownership calibration still needed",
      "Paid Stage A/B not authorized in this package",
      "Weekly production remains disabled by design",
      "Surfe/PDL/Parallel remain separately gated",
    ],
  });

  writeJson(path.join(outDir, "19-explicit-go-nogo-decisions.json"), {
    GO: [
      "engineering implementation of V5 pathways",
      "integration with existing GDI/ORS",
      "saved-evidence analysis",
      "offline testing and certification",
      "controlled live pilot packaging (docs only)",
    ],
    NO_GO: [
      "new paid research",
      "Surfe/PDL/Parallel",
      "Airtable writes",
      "customer publication",
      "production deployment",
      "weekly activation",
      "reuse of prior budget balances",
    ],
    nextAuthorizationNeededFor: [
      `Stage A discovery-only pilot ≤ $${PROPOSED_V5_STAGE_A_CEILING_USD} across three pathways`,
      "Stage B deep research after Stage A founder review",
      "any contact enrichment provider",
    ],
  });

  writeJson(path.join(outDir, "SUMMARY.json"), {
    version: AUTONOMOUS_DEMAND_DISCOVERY_V5,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    offlineCertOk: cert.ok,
    strategyCount: strategies.strategyCount,
    byPathway: strategies.byPathway,
    incrementalPrioritySegments: V5_INCREMENTAL_PRIORITY_SEGMENTS,
    filesModified: [
      "lib/group-demand-intelligence/research/autonomous-demand-discovery-v5.js",
      "lib/group-demand-intelligence/research/autonomous-demand-discovery-v5-bethesda.js",
      "scripts/test-gdi-autonomous-demand-discovery-v5.mjs",
      "scripts/gdi-autonomous-demand-discovery-v5-bethesda.mjs",
      "package.json",
      `${REPORTS_DIR}/*`,
    ],
    functionsReused: [
      "generateHotelDemandDiscoveryStrategies",
      "assessCapturableHotelDemandFit",
      "screenDiscoveryHitCommercialRelevance",
      "normalizeDiscoveryHit",
      "dedupeDiscoveryAgainstMemory",
      "enqueueDiscoveryIntoOrsQueue",
      "classifyFindingAgainstBethesdaBaseline",
      "runDeepResearchOnExistingGdiOpportunity (integration contract)",
      "assertPaidContactProvidersDisabled",
    ],
    ambition: "≈10–15 credible contactable opportunities per hotel per week — commercial ambition, not quota; do not manufacture",
  });

  writeMd(path.join(outDir, "DELIVERABLES.md"), `# Autonomous Demand Discovery V5 — Bethesda

**Status:** Offline implementation + certification complete · **Paid research:** NO-GO  
**Hotel:** Bethesda Marriott (\`${BETHESDA_CANONICAL_HOTEL_ID}\`)  
**Offline cert:** ${cert.ok ? "PASS" : "FAIL"} · Strategies: ${strategies.strategyCount} (org ${strategies.byPathway.ORGANIZATION_LED} / trigger ${strategies.byPathway.TRIGGER_LED} / event ${strategies.byPathway.EVENT_LED})

## What changed

Three complementary pathways into the **same** GDI/ORS canonical path:

1. **ORGANIZATION_LED** — demand-capable organizations + required demand mechanism  
2. **TRIGGER_LED** — verified changes with causal meeting/travel/catering logic  
3. **EVENT_LED** — preserved V4 event/RFP discovery + early deadline/respondent hygiene  

Demand mechanisms classified as **VERIFIED / INFERRED / UNSUPPORTED**. Account intelligence ≠ NET_NEW opportunity inflation.

## Integration

ORS entrypoint unchanged: \`runDeepResearchOnExistingGdiOpportunity → matureOpportunity\`.  
Weekly workflow is **plan-only** (\`weeklyProductionEnabled: false\`).  
Surfe/PDL/Parallel / Airtable / publish remain gated.

## Proposed next authorization

- Stage A discovery-only ≤ **$${PROPOSED_V5_STAGE_A_CEILING_USD}** across all three pathways (isolated ledger).  
- Stage B only after founder review of named Stage A candidates.

## Files

See \`SUMMARY.json\` for modified paths and reused functions. Deliverables \`01\`–\`19\` in this folder.
`);

  return {
    outDir: outDir.replace(/\\/g, "/"),
    certOk: cert.ok,
    strategyCount: strategies.strategyCount,
    byPathway: strategies.byPathway,
  };
}
