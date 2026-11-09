#!/usr/bin/env node
/**
 * Prepare bounded $40 / 16-run Exa live discovery pilot plan.
 * Does NOT execute paid research.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveHotelGdiProfileForResearch } from "../lib/group-demand-intelligence/hotel-gdi-profile-resolution-v1.js";
import { buildSegmentDiscoveryQueries } from "../lib/group-demand-intelligence/research/segment-discovery-adapters-v1.js";
import {
  inspectExaLiveDiscoveryReadiness,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js";
import {
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";
import { buildDiscoveryResumePlan } from "../lib/group-demand-intelligence/research/full-e2e-fourteen-segment-discovery-state-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CEILING_USD = 40;
const CEILING_RUNS = 16;

const HOTELS = [
  { hotelId: "recCAGqkERSXgiku4", name: "Sheraton Guadalajara Expo" },
  { hotelId: "recTk5I8VOzsPhxDz", name: "voco Cancún Zona Hotelera" },
];
const SEGMENTS = [
  "associations_conferences_events",
  "corporate_accounts_scaling",
];

const effort = EXA_AGENT_EFFORT.MEDIUM;
const quote = quoteExaAgentRun({ effort });
const cells = [];

for (const h of HOTELS) {
  const resolved = await resolveHotelGdiProfileForResearch(h.hotelId, {
    skipHpc: true,
    preferLive: false,
  });
  const hotel = resolved.hotel || resolved.profile;
  for (const segmentId of SEGMENTS) {
    const built = buildSegmentDiscoveryQueries(segmentId, hotel);
    const readiness = inspectExaLiveDiscoveryReadiness({
      hotel: { ...hotel, hotelId: h.hotelId },
      segmentId,
      effort,
      authorizeExaAgentLive: false,
    });
    // Allocate up to 4 agent runs per cell (4 cells × 4 = 16).
    const maxRuns = 4;
    const plannedQueries = (built.queries || []).slice(0, maxRuns).map((q, i) => ({
      sequence: i + 1,
      queryId: q.id || `${segmentId}_q${i + 1}`,
      intent: q.intent || null,
      languageHint: /[áéíóúñ¿¡]|congreso|asociación|empresa/i.test(q.query)
        ? "es_primary_or_mixed"
        : "en_or_mixed",
      queryPreview: String(q.query || "").slice(0, 220),
      estimatedAgentRuns: 1,
      estimatedMaxUsd: quote.maxUsd,
    }));
    cells.push({
      hotelId: h.hotelId,
      hotelName: h.name,
      city: hotel?.city || null,
      country: hotel?.country || null,
      segmentId,
      builder: built.builder,
      queryFamilyCount: built.queries?.length || 0,
      plannedOperations: plannedQueries,
      allocatedRuns: plannedQueries.length,
      allocatedMaxUsd: Number(
        (plannedQueries.length * Number(quote.maxUsd || 0)).toFixed(6)
      ),
      readiness: {
        readyForPaidPilot: readiness.readyForPaidPilot,
        blockers: readiness.blockers,
        contractId: readiness.contractId,
        credentialPresent: readiness.credentialPresent,
        liveAuthorized: readiness.liveAuthorized,
      },
    });
  }
}

const resume = buildDiscoveryResumePlan({
  hotelIds: HOTELS.map((h) => h.hotelId),
  segmentIds: SEGMENTS,
});

const totalAllocatedRuns = cells.reduce((n, c) => n + c.allocatedRuns, 0);
const totalAllocatedMaxUsd = cells.reduce((n, c) => n + c.allocatedMaxUsd, 0);
const contingencyUsd = Number((CEILING_USD - totalAllocatedMaxUsd).toFixed(6));

const plan = {
  version: "gdi-live-discovery-pilot-plan-v1",
  at: new Date().toISOString(),
  authorizationStatus: "NOT_AUTHORIZED — plan only",
  paidExecution: false,
  ceilings: {
    maxUsd: CEILING_USD,
    maxAgentRuns: CEILING_RUNS,
    note: "Hard ceilings, not spending targets. Stop before exceeding.",
  },
  billingAssumptions: {
    provider: "exa_agent",
    effort,
    quote,
    sources: quote.sources || [
      "https://exa.ai/docs/reference/agent-api-guide",
    ],
    contactEnrichmentExcluded: true,
    note:
      "Fixed medium effort quoted at adapter FIXED_EFFORT_USD.medium. auto/max not used in pilot. Surfe/Firecrawl not included.",
  },
  safetyControls: [
    "Dual auth: authorizeExaAgentLive + GDI_EXA_AGENT_LIVE=1",
    "Separate founder authorization for this $40 pilot required before any live call",
    "Durable state skip-live on resume — completed provider runs not auto-repeated",
    "No Airtable production writes",
    "No customer publication",
    "No Railway deploy",
    "Stop if projected remaining budget < next quote.maxUsd",
    "Zero Sales Ready inflation — Stage A only",
  ],
  successMetrics: [
    "actual_provider_calls",
    "actual_provider_costs",
    "fresh_returned_sources",
    "evidence_backed_candidates",
    "valid_new_unique_demand_opportunities",
    "duplicates_of_historical_candidates",
    "hotel_specific_relevance",
    "commercial_research_gaps",
    "named_commercial_contacts_found",
    "durable_checkpoint_persistence",
    "successful_restart_resume",
    "accurate_powershell_status_reporting",
  ],
  cells,
  totals: {
    cells: cells.length,
    allocatedAgentRuns: totalAllocatedRuns,
    allocatedMaxUsd: Number(totalAllocatedMaxUsd.toFixed(6)),
    contingencyUsd,
    withinCeiling:
      totalAllocatedRuns <= CEILING_RUNS && totalAllocatedMaxUsd <= CEILING_USD,
  },
  resumePlan: resume,
  requiredAuthorization: {
    flags: [
      "FOUNDER_EXPLICIT_APPROVAL_FOR_GDI_LIVE_DISCOVERY_PILOT_V1",
      "GDI_EXA_AGENT_LIVE=1",
      "authorizeExaAgentLive=true / --authorize-paid-discovery",
      "EXA_API_KEY present in local env only",
    ],
    maxUsd: CEILING_USD,
    maxAgentRuns: CEILING_RUNS,
    hotels: HOTELS.map((h) => h.hotelId),
    segments: SEGMENTS,
    commandSketch:
      "powershell -File scripts/gdi-workflow.ps1 -Action FullE2E -HotelIds recCAGqkERSXgiku4,recTk5I8VOzsPhxDz  (plus paid flags once authorized — not enabled now)",
  },
};

if (!plan.totals.withinCeiling) {
  console.error("PILOT PLAN EXCEEDS CEILING — aborting plan write");
  process.exit(2);
}

const outDir = path.join(
  ROOT,
  "reports",
  "gdi-live-discovery-pilot-plan-v1"
);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "pilot-plan.json"),
  `${JSON.stringify(plan, null, 2)}\n`
);

console.log(JSON.stringify({
  authorizationStatus: plan.authorizationStatus,
  allocatedAgentRuns: plan.totals.allocatedAgentRuns,
  allocatedMaxUsd: plan.totals.allocatedMaxUsd,
  contingencyUsd: plan.totals.contingencyUsd,
  withinCeiling: plan.totals.withinCeiling,
  cells: plan.cells.map((c) => ({
    hotelId: c.hotelId,
    segmentId: c.segmentId,
    allocatedRuns: c.allocatedRuns,
    allocatedMaxUsd: c.allocatedMaxUsd,
    readyForPaidPilot: c.readiness.readyForPaidPilot,
    blockers: c.readiness.blockers,
  })),
}, null, 2));
console.log(`Wrote ${outDir}/pilot-plan.json`);
