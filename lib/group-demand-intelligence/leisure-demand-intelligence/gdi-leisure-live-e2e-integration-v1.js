/**
 * LDI → canonical Full E2E live path (mock providers / historical replay).
 *
 * Connects leisure modules to fourteen-segment discovery cells, shared
 * qualification/dedupe/contact gates, and a STAGED package *plan*
 * (never writes STAGED.json / Airtable / Findymail / Surfe without separate GO).
 *
 * Spent USD always 0 unless authorizePaidResearch + allowLivePilot + budget
 * (paid path remains fail-closed here — use Full E2E paid runner separately).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import {
  getLeisureModule,
  listLeisureModuleIds,
} from "./gdi-leisure-module-registry-v1.js";
import { selectLeisureModulesForHotel } from "./gdi-leisure-hotel-module-selection-v1.js";
import { buildLeisureOpportunityContract } from "./gdi-leisure-opportunity-contract-v1.js";
import { dedupeLeisureAgainstInventory } from "./gdi-leisure-cross-segment-dedupe-v1.js";
import { assertLeisureRunAuthorization } from "./gdi-leisure-module-runner-v1.js";
import { executeFourteenSegmentDiscoveryCell } from "../workflow/full-e2e-fourteen-segment-discovery-execution-v1.js";

export const GDI_LEISURE_LIVE_E2E_INTEGRATION_V1 =
  "gdi-leisure-live-e2e-integration-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const FIXTURE_DIR = path.join(
  ROOT,
  "fixtures/gdi-leisure-demand-intelligence-v1",
);

/** Design Hotels cohort for three-hotel Full E2E. */
export const DESIGN_HOTELS_LDI_COHORT = Object.freeze([
  Object.freeze({
    hotelId: "recaokE8psIbuVeuH",
    name: "LaLuna Boutique Hotels & Villas",
    priorityModules: Object.freeze([
      "leisure_distribution",
      "leisure_attractions",
      "leisure_events",
    ]),
  }),
  Object.freeze({
    hotelId: "rec1t35e3sPerzxYu",
    name: "Memmo Príncipe Real",
    priorityModules: Object.freeze([
      "leisure_distribution",
      "leisure_attractions",
      "leisure_events",
    ]),
  }),
  Object.freeze({
    hotelId: "rece0NdV9FCnaFlVS",
    name: "Condesa DF",
    priorityModules: Object.freeze([
      "leisure_events",
      "leisure_attractions",
      "leisure_distribution",
      "leisure_universities",
    ]),
  }),
]);

/**
 * Finalization gate shared with GDI Sales Ready / STAGED standards.
 * Leisure never auto-promotes Sales Ready; STAGED plan only includes
 * commercially actionable rows that pass this gate.
 */
export function evaluateLeisureFinalizationGate(contract = {}) {
  const reasons = [];
  const demand =
    String(contract.demandGenerator || contract.organization || "").trim();
  if (!demand) reasons.push("MISSING_DEMAND_GENERATOR");
  const sources = Array.isArray(contract.sourceUrls) ? contract.sourceUrls : [];
  if (!sources.length) reasons.push("MISSING_EVIDENCE_URLS");
  const mechanism = String(
    contract.roomNightDemandMechanism || contract.demandMechanism || "",
  ).trim();
  if (!mechanism) reasons.push("MISSING_ROOM_NIGHT_MECHANISM");
  const pathway = String(contract.leisureCommercialPathway || "").trim();
  if (!pathway) reasons.push("MISSING_COMMERCIAL_PATHWAY");
  const contact = contract.commercialDecisionMakerOrEntryPoint || {};
  const named =
    String(contact.name || contact.displayName || "").trim() ||
    String(contact.organizationEntry || contact.orgEntry || "").trim();
  const contactOk =
    Boolean(named) ||
    pathway === "CAMPAIGN_DRIVEN" ||
    pathway === "PARTNERSHIP_DRIVEN";
  if (!contactOk) reasons.push("CONTACT_RESEARCH_REQUIRED");
  const narrative = String(
    contract.recommendedSalesOrMarketingAction ||
      contract.customerNarrative ||
      "",
  ).trim();
  if (!narrative) reasons.push("MISSING_COMMERCIAL_ACTION");
  if (contract.salesReady === true && reasons.length) {
    reasons.push("SALES_READY_BLOCKED_BY_GATES");
  }
  return {
    finalizedReady: reasons.length === 0,
    reasons,
    contactResearchRequired: reasons.includes("CONTACT_RESEARCH_REQUIRED"),
    salesReady: false, // never auto
  };
}

function loadModuleFixtures(moduleId, hotelId) {
  const files = [
    path.join(FIXTURE_DIR, `${moduleId}.json`),
    path.join(FIXTURE_DIR, `${hotelId}-${moduleId}.json`),
  ];
  for (const f of files) {
    if (fs.existsSync(f)) {
      const raw = JSON.parse(fs.readFileSync(f, "utf8"));
      return Array.isArray(raw) ? raw : raw.opportunities || raw.candidates || [];
    }
  }
  return [];
}

/**
 * Mock provider discovery cell — simulates live segment discovery without Exa.
 * Uses leisure fixtures mapped onto the module's primary segments.
 */
export function buildMockLeisureSegmentFixtureExecutor(moduleId, hotelId) {
  const fixtures = loadModuleFixtures(moduleId, hotelId);
  return async function fixtureExecutor({ hotelId: hid, segmentId }) {
    const candidates = fixtures.map((row, i) => ({
      candidateId: `mock_${moduleId}_${segmentId}_${i}`,
      hotelId: hid,
      segmentId,
      organizationName: row.organization || row.orgName || row.demandGenerator,
      title: row.demandGenerator || row.title || row.name,
      sourceUrls: row.sourceUrls || (row.sourceUrl ? [row.sourceUrl] : []),
      startDate: row.startDate || row.dates || null,
      leisureModuleId: moduleId,
      mockProvider: true,
      raw: row,
    }));
    return {
      executorId: `mock_leisure_${moduleId}`,
      retrievalMode: "MOCK_PROVIDER",
      providerCallCount: 0,
      researchCostUsd: 0,
      rawResults: candidates.length,
      extractedCandidates: candidates.length,
      validatedCandidates: candidates.length,
      deduplicatedCandidates: candidates.length,
      candidates,
      stopReason: candidates.length ? "MOCK_COMPLETE" : "MOCK_EMPTY",
      ok: true,
    };
  };
}

function prioritizeModules(moduleIds, hotelId) {
  const cohort = DESIGN_HOTELS_LDI_COHORT.find((h) => h.hotelId === hotelId);
  if (!cohort) return moduleIds;
  const priority = cohort.priorityModules;
  const rest = moduleIds.filter((id) => !priority.includes(id));
  return [...priority.filter((id) => moduleIds.includes(id)), ...rest];
}

/**
 * Plan a STAGED publication package without writing bundled STAGED.json.
 */
export function planLeisureStagedPackage({
  hotelId,
  opportunities = [],
  schemaVersion = GDI_LEISURE_LIVE_E2E_INTEGRATION_V1,
} = {}) {
  const finalized = [];
  const held = [];
  for (const opp of opportunities) {
    const gate = evaluateLeisureFinalizationGate(opp);
    const row = { ...opp, finalizationGate: gate, salesReady: false };
    if (gate.finalizedReady) finalized.push(row);
    else held.push(row);
  }
  const contentFingerprintSha256 = createHash("sha256")
    .update(
      JSON.stringify({
        hotelId,
        ids: finalized.map((o) => o.opportunityId).sort(),
      }),
    )
    .digest("hex");

  return {
    schema: "gdi-publication-package-store-v1-plan",
    kind: "STAGED_PLAN",
    hotelId,
    writeApplied: false,
    writeForbiddenReason:
      "FOUNDER_PUBLICATION_GO_REQUIRED — plan only; no STAGED.json mutation",
    lifecycleState: "PLANNED_NOT_STAGED",
    opportunityCount: finalized.length,
    heldForResearchCount: held.length,
    contentFingerprintSha256,
    schemaVersion,
    opportunities: finalized,
    heldForResearch: held.map((o) => ({
      opportunityId: o.opportunityId,
      reasons: o.finalizationGate?.reasons || [],
    })),
  };
}

/**
 * Run leisure modules through the canonical live E2E path with mocked providers.
 *
 * @param {object} opts
 * @param {string} opts.hotelId
 * @param {"mock_live"|"live_replay"|"fixture"} [opts.researchMode]
 */
export async function runLeisureLiveE2eIntegration(opts = {}) {
  const auth = assertLeisureRunAuthorization({
    ...opts,
    // Force $0 for mock_live / live_replay certification
    budgetUsd:
      opts.researchMode === "mock_live" || opts.researchMode === "live_replay"
        ? 0
        : opts.budgetUsd,
    authorizePaidResearch:
      opts.researchMode === "mock_live" || opts.researchMode === "live_replay"
        ? false
        : opts.authorizePaidResearch,
  });
  if (!auth.ok) {
    return {
      ok: false,
      schema: GDI_LEISURE_LIVE_E2E_INTEGRATION_V1,
      error: auth.error,
      spentUsd: 0,
    };
  }

  const hotelId = String(opts.hotelId || "").trim();
  if (!hotelId) {
    return {
      ok: false,
      schema: GDI_LEISURE_LIVE_E2E_INTEGRATION_V1,
      error: "HOTEL_ID_REQUIRED",
      spentUsd: 0,
    };
  }

  const researchMode = String(opts.researchMode || "mock_live").toLowerCase();
  const hotelProfile = {
    hotelId,
    ...(opts.hotelProfile && typeof opts.hotelProfile === "object"
      ? opts.hotelProfile
      : {}),
  };

  const selection = selectLeisureModulesForHotel(hotelProfile, {
    forceModules: opts.modules || opts.forceModules,
    disableModules: opts.disableModules,
  });

  let moduleIds =
    Array.isArray(opts.modules) && opts.modules.length
      ? opts.modules.map((m) => getLeisureModule(m)?.id).filter(Boolean)
      : selection.moduleIds;
  moduleIds = prioritizeModules(moduleIds, hotelId);

  const existingInventory = Array.isArray(opts.existingInventory)
    ? opts.existingInventory
    : [];

  const allOpps = [];
  const moduleResults = [];
  let spentUsd = 0;
  const fourteenSegmentCalls = [];

  for (const moduleId of moduleIds) {
    const mod = getLeisureModule(moduleId);
    if (!mod) continue;

    const segments = [...(mod.primarySegments || [])];
    const segmentTraces = [];
    const rawCandidates = [];

    for (const segmentId of segments) {
      const cellOpts = {
        hotelId,
        segmentId,
        authorizePaidDiscovery: false,
        allowHistoricalReplay: researchMode === "live_replay",
      };
      if (researchMode === "mock_live" || researchMode === "fixture") {
        cellOpts.fixtureExecutor = buildMockLeisureSegmentFixtureExecutor(
          moduleId,
          hotelId,
        );
      }
      const trace = await executeFourteenSegmentDiscoveryCell(cellOpts);
      spentUsd += Number(trace.researchCostUsd || 0);
      segmentTraces.push({
        segmentId,
        ok: trace.ok !== false,
        retrievalMode: trace.retrievalMode,
        candidateCount: (trace.candidates || []).length,
        researchCostUsd: trace.researchCostUsd || 0,
        stopReason: trace.stopReason,
      });
      fourteenSegmentCalls.push({
        leisureModuleId: moduleId,
        segmentId,
        retrievalMode: trace.retrievalMode,
      });
      for (const c of trace.candidates || []) {
        rawCandidates.push({
          ...(c.raw || c),
          hotelId,
          leisureModuleId: moduleId,
          segmentId,
          demandGenerator: c.title || c.demandGenerator,
          organization: c.organizationName || c.organization,
          sourceUrls: c.sourceUrls || [],
          startDate: c.startDate,
        });
      }
    }

    // Deduplicate raw candidates within module before contract build
    const seen = new Set();
    const uniqueRaw = [];
    for (const row of rawCandidates) {
      const key = [
        String(row.organization || "").toLowerCase(),
        String(row.demandGenerator || "").toLowerCase(),
        String(row.startDate || ""),
      ].join("|");
      if (seen.has(key)) continue;
      seen.add(key);
      uniqueRaw.push(row);
    }

    const contracts = uniqueRaw.map((row) =>
      buildLeisureOpportunityContract(
        { ...row, hotelId, leisureModuleId: moduleId },
        { needPeriodContext: opts.needPeriodContext || null },
      ),
    );

    const deduped = dedupeLeisureAgainstInventory(contracts, [
      ...existingInventory,
      ...allOpps,
    ]);

    const gated = deduped.accepted.map((c) => {
      const gate = evaluateLeisureFinalizationGate(c);
      return { ...c, finalizationGate: gate, salesReady: false };
    });

    moduleResults.push({
      leisureModuleId: moduleId,
      label: mod.label,
      researchMode,
      primarySegments: segments,
      segmentTraces,
      discovered: uniqueRaw.length,
      acceptedCount: gated.length,
      suppressedCount: deduped.suppressedCount,
      finalizedReadyCount: gated.filter((g) => g.finalizationGate.finalizedReady)
        .length,
      contactResearchRequiredCount: gated.filter(
        (g) => g.finalizationGate.contactResearchRequired,
      ).length,
      spentUsd: 0,
      paidCalls: 0,
      status: "MOCK_LIVE_COMPLETE",
    });

    allOpps.push(...gated);
  }

  const stagedPlan = planLeisureStagedPackage({
    hotelId,
    opportunities: allOpps,
  });

  const outDir = opts.outDir
    ? path.resolve(opts.outDir)
    : path.join(
        ROOT,
        "reports/gdi-leisure-live-e2e-integration-v1",
        `run_${hotelId}_${Date.now()}`,
      );

  if (opts.writeArtifacts !== false) {
    fs.mkdirSync(outDir, { recursive: true });
    const artifact = {
      schema: GDI_LEISURE_LIVE_E2E_INTEGRATION_V1,
      hotelId,
      researchMode,
      selection,
      moduleIds,
      spentUsd,
      authorizePaidResearch: false,
      findymail: false,
      surfe: false,
      airtableOpportunityWrites: false,
      publication: false,
      fourteenSegmentCalls,
      moduleResults,
      opportunities: allOpps,
      stagedPlan,
      sharedGates: {
        budget: "SHARED_WITH_GDI_FULL_E2E",
        dedupe: "gdi-leisure-cross-segment-dedupe-v1",
        contactIntelligence: "pathway_gates_no_findymail_surfe",
        finalization: "evaluateLeisureFinalizationGate",
        fourteenSegmentsPreserved: true,
      },
    };
    fs.writeFileSync(
      path.join(outDir, "leisure-live-e2e.json"),
      JSON.stringify(artifact, null, 2),
    );
    fs.writeFileSync(
      path.join(outDir, "STAGED_PLAN.json"),
      JSON.stringify(stagedPlan, null, 2),
    );
  }

  return {
    ok: true,
    schema: GDI_LEISURE_LIVE_E2E_INTEGRATION_V1,
    hotelId,
    researchMode,
    selection,
    moduleIds,
    moduleResults,
    opportunities: allOpps,
    opportunityCount: allOpps.length,
    finalizedReadyCount: stagedPlan.opportunityCount,
    heldForResearchCount: stagedPlan.heldForResearchCount,
    stagedPlan,
    spentUsd,
    fourteenSegmentCalls,
    outDir: opts.writeArtifacts === false ? null : outDir,
    availableModules: listLeisureModuleIds(),
  };
}

/**
 * $0 certification across Design Hotels cohort.
 */
export async function certifyLeisureLiveE2eDesignHotels(opts = {}) {
  const hotels = opts.hotels || DESIGN_HOTELS_LDI_COHORT;
  const results = [];
  for (const h of hotels) {
    const run = await runLeisureLiveE2eIntegration({
      hotelId: h.hotelId,
      hotelProfile: { hotelId: h.hotelId, name: h.name },
      modules: h.priorityModules,
      researchMode: "mock_live",
      budgetUsd: 0,
      writeArtifacts: opts.writeArtifacts !== false,
      outDir: opts.outDir
        ? path.join(opts.outDir, h.hotelId)
        : undefined,
    });
    results.push({
      hotelId: h.hotelId,
      name: h.name,
      priorityModules: h.priorityModules,
      ok: run.ok === true,
      opportunityCount: run.opportunityCount || 0,
      finalizedReadyCount: run.finalizedReadyCount || 0,
      spentUsd: run.spentUsd || 0,
      stagedWriteApplied: run.stagedPlan?.writeApplied === true,
      error: run.error || null,
    });
  }

  const passed = results.every(
    (r) =>
      r.ok &&
      r.spentUsd === 0 &&
      r.stagedWriteApplied === false,
  );

  return {
    schema: `${GDI_LEISURE_LIVE_E2E_INTEGRATION_V1}/design-hotels-cert`,
    at: new Date().toISOString(),
    passed,
    spentUsdTotal: results.reduce((s, r) => s + (r.spentUsd || 0), 0),
    results,
    gates: {
      findymail: false,
      surfe: false,
      airtableOpportunityWrites: false,
      customerPublication: false,
      paidExa: false,
      fourteenSegmentsPreserved: true,
      leisureFeedsSameStagedInventory: true,
    },
  };
}

export default {
  GDI_LEISURE_LIVE_E2E_INTEGRATION_V1,
  DESIGN_HOTELS_LDI_COHORT,
  evaluateLeisureFinalizationGate,
  planLeisureStagedPackage,
  runLeisureLiveE2eIntegration,
  certifyLeisureLiveE2eDesignHotels,
};
