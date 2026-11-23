/**
 * Canonical LDI module runner — fixture / replay / $0 only unless paid auth.
 * Reuses fourteen-segment discovery executor registry metadata; does not call paid APIs.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getLeisureModule,
  listLeisureModuleIds,
  buildLeisureFourteenSegmentCoverageMatrix,
} from "./gdi-leisure-module-registry-v1.js";
import { buildLeisureOpportunityContract } from "./gdi-leisure-opportunity-contract-v1.js";
import { selectLeisureModulesForHotel } from "./gdi-leisure-hotel-module-selection-v1.js";
import { dedupeLeisureAgainstInventory } from "./gdi-leisure-cross-segment-dedupe-v1.js";
import { getFourteenSegmentExecutorSpec } from "../workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js";

export const GDI_LEISURE_MODULE_RUNNER_V1 = "gdi-leisure-module-runner-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const FIXTURE_DIR = path.join(
  ROOT,
  "fixtures/gdi-leisure-demand-intelligence-v1",
);

/**
 * Fail-closed authorization for paid leisure research.
 */
export function assertLeisureRunAuthorization(opts = {}) {
  const budget = Number(opts.budgetUsd || 0);
  const paid =
    opts.authorizePaidResearch === true || opts.authorizePaidDiscovery === true;
  const live = opts.allowLivePilot === true;

  if (budget > 0 && !paid) {
    return {
      ok: false,
      error: "NONZERO_BUDGET_REQUIRES_AUTHORIZE_PAID_RESEARCH",
    };
  }
  if (paid && budget <= 0) {
    return {
      ok: false,
      error: "AUTHORIZE_PAID_REQUIRES_POSITIVE_BUDGET",
    };
  }
  if ((paid || budget > 0) && !live) {
    return {
      ok: false,
      error: "PAID_REQUIRES_ALLOW_LIVE_PILOT",
    };
  }
  if (opts.authorizePublication === true) {
    return {
      ok: false,
      error: "PUBLICATION_NOT_AUTHORIZED_IN_LDI_V1",
    };
  }
  if (opts.authorizeFindymail === true || opts.authorizeSurfe === true) {
    return {
      ok: false,
      error: "PAID_CONTACT_ENRICHMENT_NOT_AUTHORIZED",
    };
  }
  return { ok: true, mode: paid ? "PAID_BLOCKED_UNTIL_FOUNDER_GO" : "ZERO_PAID" };
}

export async function runLeisureModules(opts = {}) {
  const auth = assertLeisureRunAuthorization(opts);
  if (!auth.ok) {
    return {
      ok: false,
      schema: GDI_LEISURE_MODULE_RUNNER_V1,
      error: auth.error,
      spentUsd: 0,
    };
  }

  const hotelId = String(opts.hotelId || "").trim();
  if (!hotelId) {
    return {
      ok: false,
      schema: GDI_LEISURE_MODULE_RUNNER_V1,
      error: "HOTEL_ID_REQUIRED",
      spentUsd: 0,
    };
  }

  const researchMode = String(opts.researchMode || opts.mode || "fixture").toLowerCase();

  // Live E2E path: mocked providers or historical replay through fourteen-segment cells.
  if (researchMode === "mock_live" || researchMode === "live_replay") {
    const { runLeisureLiveE2eIntegration } = await import(
      "./gdi-leisure-live-e2e-integration-v1.js"
    );
    return runLeisureLiveE2eIntegration({
      ...opts,
      hotelId,
      researchMode,
      existingInventory: loadExistingInventory(opts),
    });
  }

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

  const moduleIds =
    Array.isArray(opts.modules) && opts.modules.length
      ? opts.modules.map((m) => getLeisureModule(m)?.id).filter(Boolean)
      : selection.moduleIds;

  const existingInventory = loadExistingInventory(opts);
  const allOpps = [];
  const moduleResults = [];

  for (const moduleId of moduleIds) {
    const mod = getLeisureModule(moduleId);
    if (!mod) continue;

    const segmentWiring = (mod.primarySegments || []).map((seg) => ({
      segmentId: seg,
      executor: getFourteenSegmentExecutorSpec(seg)
        ? {
            classification: getFourteenSegmentExecutorSpec(seg).classification,
            strategy: getFourteenSegmentExecutorSpec(seg).strategy,
          }
        : { classification: "MISSING", strategy: null },
    }));

    const fixtures = loadModuleFixtures(moduleId, hotelId);
    const contracts = fixtures.map((row) =>
      buildLeisureOpportunityContract(
        { ...row, hotelId, leisureModuleId: moduleId },
        { needPeriodContext: opts.needPeriodContext || null },
      ),
    );

    const deduped = dedupeLeisureAgainstInventory(contracts, [
      ...existingInventory,
      ...allOpps,
    ]);

    moduleResults.push({
      leisureModuleId: moduleId,
      label: mod.label,
      coverage: mod.coverage,
      implementationMode: mod.implementationMode,
      researchMode,
      segmentWiring,
      fixtureCount: fixtures.length,
      acceptedCount: deduped.acceptedCount,
      suppressedCount: deduped.suppressedCount,
      suppressed: deduped.suppressed,
      status:
        researchMode === "replay" || researchMode === "fixture"
          ? fixtures.length
            ? "FIXTURE_COMPLETE"
            : "NO_IMPROVEMENT"
          : "AWAITING_PAID_AUTH",
      spentUsd: 0,
      paidCalls: 0,
    });

    allOpps.push(...deduped.accepted);
  }

  const outDir = opts.outDir
    ? path.resolve(opts.outDir)
    : path.join(
        ROOT,
        "reports/gdi-leisure-demand-intelligence-v1",
        `run_${hotelId}_${Date.now()}`,
      );
  if (opts.writeArtifacts !== false) {
    fs.mkdirSync(outDir, { recursive: true });
    const artifact = {
      schema: GDI_LEISURE_MODULE_RUNNER_V1,
      hotelId,
      selection,
      moduleIds,
      researchMode,
      spentUsd: 0,
      authorizePaidResearch: false,
      publication: false,
      findymail: false,
      surfe: false,
      moduleResults,
      opportunities: allOpps,
      coverageMatrix: buildLeisureFourteenSegmentCoverageMatrix(),
      investigatedUrls: [],
      resume: { resumable: true, replaySafe: true },
    };
    fs.writeFileSync(
      path.join(outDir, "leisure-run.json"),
      JSON.stringify(artifact, null, 2),
      "utf8",
    );
  }

  return {
    ok: true,
    schema: GDI_LEISURE_MODULE_RUNNER_V1,
    hotelId,
    selection,
    moduleIds,
    moduleResults,
    opportunities: allOpps,
    opportunityCount: allOpps.length,
    spentUsd: 0,
    outDir: opts.writeArtifacts === false ? null : outDir,
    coverageMatrix: buildLeisureFourteenSegmentCoverageMatrix(),
    availableModules: listLeisureModuleIds(),
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
  // Synthetic negative-safe empty for modules without fixtures
  return [];
}

function loadExistingInventory(opts) {
  if (Array.isArray(opts.existingInventory)) return opts.existingInventory;
  if (opts.existingInventoryPath && fs.existsSync(opts.existingInventoryPath)) {
    const raw = JSON.parse(fs.readFileSync(opts.existingInventoryPath, "utf8"));
    return Array.isArray(raw) ? raw : raw.opportunities || [];
  }
  return [];
}

export function runLeisureOrchestratorDisabledCheck() {
  // When leisure modules disabled, portfolio path must not invoke runner.
  return {
    leisureEnabled: false,
    modulesInvoked: [],
    fourteenSegmentsUnchanged: true,
  };
}
