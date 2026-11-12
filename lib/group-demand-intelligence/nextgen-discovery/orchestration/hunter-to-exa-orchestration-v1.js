/**
 * Hunter → shared screen → dedupe → L1/L2/L3 depth → ORS handoff orchestration.
 *
 * Paid Exa/Apify are planned only when founder-authorized; this module never
 * auto-executes paid retrieval. Synthetic fixtures stay out of customer inventory.
 */

import fs from "node:fs";
import path from "node:path";
import { runPublicSourceAdapter } from "../adapters/public-source-adapters-v1.js";
import { planExaEvidenceGapResearch } from "../adapters/exa-hunter-bridge-v1.js";
import { planApifyHunterRetrieval } from "../adapters/apify-hunter-bridge-v1.js";
import { normalizeHunterDiscoveries } from "./normalize-discovery-v1.js";
import {
  buildDedupeKeySets,
  dedupeNormalizedDiscoveries,
} from "./discovery-dedupe-v1.js";
import {
  selectResearchDepthBatch,
  HUNTER_RESEARCH_LEVEL,
} from "./research-depth-selector-v1.js";
import { buildHunterMeasurementReport } from "./hunter-measurement-v1.js";
import {
  screenHunterCandidate,
  materializeHunterDiscoveries,
  assertHunterRunSafe,
} from "../hunter-base-v1.js";
import { buildQualificationHandoffPacket } from "../shared/qualification-handoff-v1.js";
import {
  qualifyDiscoveryCandidates,
} from "../../research/discovery-to-qualification-handoff-v1.js";
import { getSpecialistHunterMeta } from "../specialist-hunter-contract-v1.js";
import { resolveHunterConfig } from "../shared/hunter-config-registry-v1.js";
import { DISCOVERY_EVIDENCE_CLASS } from "../shared/discovery-evidence-class-v1.js";

export const HUNTER_TO_EXA_ORCHESTRATION_V1 =
  "gdi-nextgen-hunter-to-exa-orchestration-v1";

/**
 * Full zero-paid-safe orchestration for one hunter + hotel.
 */
export async function runHunterToExaOrchestration({
  hunterId,
  hotelId,
  hotelName = "",
  territory = "",
  city = "",
  country = "US",
  commercialPriorities = null,
  hotelFeedback = null,
  existingHotelOpportunities = [],
  existingKeys = [],
  priorHunterDiscoveries = [],
  priorApifyExaKeys = [],
  rejectedOrMonitored = [],
  budgetUsd = 0,
  authorizePaid = false,
  dryRun = true,
  skipDirectPublicFetches = false,
  preferHistoricalOnly = false,
  attemptLivePublic = true,
  reportDir = "",
  rootDir = process.cwd(),
  inventoryRows = [],
} = {}) {
  const startedAt = new Date().toISOString();
  const meta = getSpecialistHunterMeta(hunterId);
  const hunterConfig = resolveHunterConfig(hunterId, { hotelId, budgetUsd });
  const safety = assertHunterRunSafe({
    hunterId,
    hotelId,
    budgetUsd,
    allowPaid: authorizePaid === true,
    authorizeMeasurement: authorizePaid === true,
  });

  // Nextgen wave remains fail-closed for paid — even if flags are passed.
  const paidAuthorized = false;

  const adapterResult = await runPublicSourceAdapter(hunterId, {
    hotelId,
    hotelName,
    territory,
    city: city || territory,
    country,
    commercialPriorities,
    skipDirectPublicFetches,
    preferHistoricalOnly,
    attemptLivePublic,
    budgetUsd: 0,
    rootDir,
  });

  let discoveries = normalizeHunterDiscoveries(adapterResult.discoveries || [], {
    hunterId,
    hotelId,
    discoveryEvidenceClass: adapterResult.meta?.discoveryEvidenceClass,
  });

  // Screen before deeper research.
  const screenedAccepted = [];
  const screenedRejected = [];
  for (const d of discoveries) {
    const screen = screenHunterCandidate(d);
    if (!screen.accepted) {
      screenedRejected.push({ ...d, rejectReason: screen.reason });
    } else {
      screenedAccepted.push(d);
    }
  }

  const keySets = buildDedupeKeySets({
    existingHotelOpportunities,
    priorHunterDiscoveries,
    priorApifyExaKeys,
    rejectedOrMonitored,
    existingKeys,
  });
  const deduped = dedupeNormalizedDiscoveries(screenedAccepted, keySets);

  const depth = selectResearchDepthBatch(deduped.incremental, {
    commercialPriorities,
    hotelFeedback,
    paidAuthorized,
    budgetUsd: 0,
  });

  // Paid plans only — never execute in this wave.
  const deferredForPaid = depth.decisions.filter((d) => d.deferredPaidPilot);
  const exaPlan = planExaEvidenceGapResearch({
    hunterId,
    discoveries: deduped.incremental.filter((d) =>
      deferredForPaid.some(
        (x) =>
          x.organizationName === d.organizationName &&
          x.activityTitle === d.activityTitle
      )
    ),
    budgetUsd: Number(budgetUsd) || 0,
    authorizePaid: false,
  });
  const apifyPlan = planApifyHunterRetrieval({
    hunterId,
    budgetUsd: 0,
    authorizePaid: false,
  });

  const materialized = materializeHunterDiscoveries({
    hunterId,
    hotelId,
    territory,
    discoveries: deduped.incremental,
    existingKeys: [...keySets.keys],
  });

  const qualificationHandoff = buildQualificationHandoffPacket({
    hunterId,
    hotelId,
    hypotheses: materialized.incremental,
    graph: materialized.graph,
    discoveryEvidenceClass: adapterResult.meta?.discoveryEvidenceClass,
    dryRun,
  });

  // Existing ORS / Phase-2 qualification (zero-cost dispositions).
  // persist:false + no discovery-state writes during orchestration dry runs.
  const orsQualification = qualifyDiscoveryCandidates(
    deduped.incremental.map((d) => ({
      organizationName: d.organizationName,
      activityTitle: d.activityTitle,
      evidenceUrls: d.evidenceUrls,
      sourceUrl: d.sourceUrl,
      hotelId,
      segmentId: meta?.workflowIds?.[0] || null,
      dates: d.activityDate,
      geography: d.geographicScope,
      hasSourceEvidence: (d.evidenceUrls || []).length > 0,
    })),
    {
      hotelId,
      hotelName,
      segmentId: meta?.workflowIds?.[0] || null,
      hunterId,
    },
    { inventoryRows, persist: false, persistDiscoveryState: false }
  );
  const orsDoc = orsQualification?.reconcileDoc || {};

  const evidenceGroundedTheses = materialized.incremental.filter(
    (h) => h.hasRealisticHotelThesis !== false && (h.evidenceUrls || []).length
  ).length;

  const measurement = buildHunterMeasurementReport({
    hunterId,
    hotelId,
    rawDiscoveries: discoveries.length,
    incrementalUnique: deduped.incremental.length,
    evidenceGroundedTheses,
    qualifiedForOrs: qualificationHandoff.summary.routedToOrs,
    usefulContacts: materialized.graph.stats.contacts || 0,
    rejected: screenedRejected.length + (orsDoc.rejected?.length || 0),
    duplicates: deduped.duplicates.length,
    previouslyRejected: deduped.previouslyRejected.length,
    researchCostUsd: 0,
    researchLevels: depth.summary,
    discoveryEvidenceClass: adapterResult.meta?.discoveryEvidenceClass,
    paidAuthorized: false,
  });

  const report = {
    schema: HUNTER_TO_EXA_ORCHESTRATION_V1,
    hunterId,
    label: meta?.label || hunterId,
    hotelId,
    hotelName: hotelName || null,
    startedAt,
    finishedAt: new Date().toISOString(),
    dryRun,
    paidAuthorized,
    safety,
    hunterConfig,
    adapterMeta: adapterResult.meta || {},
    screening: {
      accepted: screenedAccepted.length,
      rejected: screenedRejected.length,
      rejectedRows: screenedRejected,
    },
    dedupe: deduped.summary,
    researchDepth: depth,
    paidResearchPlans: {
      executed: false,
      exa: exaPlan,
      apify: apifyPlan,
      note: "Bounded paid pilots prepared as plans/quotes only — not executed.",
    },
    qualificationHandoff,
    orsQualification: {
      version: orsDoc.version || null,
      waterfall: orsDoc.waterfall || null,
      dispositions: (orsDoc.unique || []).map((r) => ({
        organizationName: r.organizationName,
        activityTitle: r.activityTitle,
        disposition: r.disposition,
        nextAction: r.nextAction || r.salesAction || null,
        customerPublicationReady: false,
      })),
      autoPublished: false,
      salesReady: 0,
    },
    measurement,
    ecosystemStats: materialized.graph.stats,
    incremental: materialized.incremental,
    publicationSafeguard: {
      autoPublish: false,
      salesReadyFromHunter: false,
      cmiIsNotDemandSegment: true,
      fourteenDemandSegmentsPreserved: true,
      syntheticFixturesNotCustomerInventory: true,
    },
    fourteenSegmentNote:
      "Hunters map into existing workflows/segments — CMI remains orthogonal intelligence, not a fifteenth segment.",
  };

  if (reportDir) {
    fs.mkdirSync(reportDir, { recursive: true });
    const outPath = path.join(
      reportDir,
      `${hunterId}-${hotelId}-orchestration.json`
    );
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
    report.reportPath = outPath;
  }

  return report;
}

/**
 * Prepare bounded paid research pilots for founder authorization — no execution.
 */
export function prepareBoundedPaidResearchPilots({
  hunterIds = [],
  hotelId,
  budgetUsdPerHunter = 3,
  discoveriesByHunter = {},
} = {}) {
  const pilots = (hunterIds || []).map((hunterId) => {
    const discoveries = discoveriesByHunter[hunterId] || [];
    const depth = selectResearchDepthBatch(discoveries, {
      paidAuthorized: true,
      budgetUsd: budgetUsdPerHunter,
    });
    const exa = planExaEvidenceGapResearch({
      hunterId,
      discoveries,
      budgetUsd: budgetUsdPerHunter,
      authorizePaid: true,
    });
    const apify = planApifyHunterRetrieval({
      hunterId,
      budgetUsd: budgetUsdPerHunter,
      authorizePaid: true,
    });
    return {
      hunterId,
      hotelId,
      budgetUsdCeiling: budgetUsdPerHunter,
      execute: false,
      awaitingFounderAuthorization: true,
      depthSummary: depth.summary,
      exaQuote: exa.quote,
      exaQuestions: exa.plannedQuestions,
      apifyPlan: {
        role: apify.providerPlanRole,
        actors: apify.actorsPlanned,
        validations: apify.validations,
      },
      gatesRequired: [
        "founder_budget_authorization",
        "GDI_EXA_AGENT_LIVE=1 for Exa",
        "GDI_APIFY_LIVE=1 + APIFY_TOKEN for Apify",
        "assertHunterRunSafe must be extended to allow paid (currently fail-closed)",
        "per-hunter cost ceiling",
        "dedupe against inventory before spend",
      ],
    };
  });

  return {
    schema: "gdi-nextgen-bounded-paid-research-pilots-v1",
    createdAt: new Date().toISOString(),
    execute: false,
    paidAuthorized: false,
    hotelId,
    pilots,
    totalPlannedCeilingUsd: pilots.reduce(
      (s, p) => s + (Number(p.budgetUsdCeiling) || 0),
      0
    ),
    note: "Pilots are prepared for founder authorization only — no paid calls executed.",
  };
}

export { HUNTER_RESEARCH_LEVEL, DISCOVERY_EVIDENCE_CLASS };
