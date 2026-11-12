#!/usr/bin/env node
/**
 * PowerShell FullE2E fourteen-segment orchestrator (accounting + discovery).
 *
 *   node scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs \
 *     --hotels recCAGqkERSXgiku4,recTk5I8VOzsPhxDz,recXh4NgF8sDbi9cQ \
 *     --dry-run
 *
 *   # Invoke registered discovery executors (zero-cost / historical replay):
 *   node scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs \
 *     --hotels … --execute-discovery --dry-run
 *
 * Without --execute-discovery this is ACCOUNTING ONLY (no provider calls).
 * Does not call paid providers unless --authorize-paid-discovery (founder).
 * Does not publish.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  FULL_E2E_FOURTEEN_SEGMENT_TRAVERSAL_V1,
  buildFourteenSegmentTraversalPlan,
  dedupeCrossSegmentOpportunities,
  evaluateFourteenSegmentHotelCompletion,
  nextSegmentsToExecute,
  assertNoSilentLeadCap,
  getCanonicalFourteenSegmentIds,
} from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-traversal-v1.js";
import {
  executeFourteenSegmentDiscoveryMatrix,
} from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-execution-v1.js";
import {
  assertFourteenSegmentExecutorArtifactsOnDisk,
  listFourteenSegmentExecutorSpecs,
} from "../lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js";
import {
  buildDiscoveryResumePlan,
} from "../lib/group-demand-intelligence/research/full-e2e-fourteen-segment-discovery-state-v1.js";
import {
  executeOriginalFullE2eResearchChain,
  buildThreeHotelFullE2eBudgetManifest,
} from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function has(flag) {
  return process.argv.includes(flag);
}

const hotelsRaw =
  arg("--hotels") ||
  "recCAGqkERSXgiku4,recTk5I8VOzsPhxDz,recXh4NgF8sDbi9cQ";
const hotelIds = hotelsRaw
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const dryRun = has("--dry-run") || !has("--apply");
const executeDiscovery = has("--execute-discovery");
const authorizePaidDiscovery = has("--authorize-paid-discovery");
const runOriginalChain = has("--run-original-full-e2e-chain");
const authorizePaidResearch = has("--authorize-paid-research");
const leadCap = arg("--discovery-lead-cap");
const chainCandidateLimit = arg("--chain-candidate-limit");
const outDir =
  arg("--out") ||
  path.join(
    ROOT,
    "reports",
    "gdi-full-e2e-fourteen-segment-orchestrator-v1"
  );

const artifactGate = assertFourteenSegmentExecutorArtifactsOnDisk();
if (!artifactGate.ok) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        code: "EXECUTOR_ARTIFACTS_MISSING",
        failures: artifactGate.failures,
      },
      null,
      2
    )
  );
  process.exit(2);
}

const capGate = assertNoSilentLeadCap({
  discoveryLeadCap: leadCap != null ? Number(leadCap) : null,
});
if (!capGate.ok) {
  console.error(JSON.stringify(capGate, null, 2));
  process.exit(2);
}

const priorPath = path.join(outDir, "traversal-state.json");
const auditMatrixPath = path.join(
  ROOT,
  "reports",
  "aimbridge-14-segment-discovery-coverage-audit-v1",
  "01-COVERAGE_MATRIX.json"
);
const prior = fs.existsSync(priorPath)
  ? JSON.parse(fs.readFileSync(priorPath, "utf8"))
  : { cells: [] };

// Prefer durable per-hotel fourteen-segment state, then traversal-state, then audit matrix.
function loadDurableSegmentCells(hotelIds) {
  const cells = [];
  for (const hotelId of hotelIds) {
    const dir = path.join(
      ROOT,
      "data",
      "group-demand-intelligence",
      "hotels",
      hotelId,
      "full-e2e-fourteen-segment-state"
    );
    if (!fs.existsSync(dir)) continue;
    for (const file of fs.readdirSync(dir)) {
      if (!file.endsWith(".json")) continue;
      const st = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
      cells.push({
        hotelId,
        segmentId: st.segmentId || file.replace(/\.json$/, ""),
        status: st.coverageStatus || st.status || "PARTIAL",
        reason: st.completionRationale || null,
        opportunityIds: st.candidateIds || [],
      });
    }
  }
  return cells;
}

let priorCells = loadDurableSegmentCells(hotelIds);
if (!priorCells.length) priorCells = prior.cells || [];
if (!priorCells.length && fs.existsSync(auditMatrixPath)) {
  const matrixDoc = JSON.parse(fs.readFileSync(auditMatrixPath, "utf8"));
  const post = matrixDoc?.post?.matrix || [];
  if (post.length) {
    priorCells = post.map((c) => ({
      hotelId: c.hotelId,
      segmentId: c.segmentId,
      status: c.status,
      reason: c.reason || null,
      opportunityIds: [],
    }));
  }
}

const plan = buildFourteenSegmentTraversalPlan({
  hotelIds,
  priorCells,
});

// Load any existing Aimbridge inventories for dedupe accounting
const inventoryFiles = {
  recCAGqkERSXgiku4:
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/sheraton-guadalajara-expo-inventory.json",
  recTk5I8VOzsPhxDz:
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/voco-cancun-zona-hotelera-inventory.json",
  recXh4NgF8sDbi9cQ:
    "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/wyndham-alltra-punta-cana-inventory.json",
};

const allRows = [];
for (const hotelId of hotelIds) {
  const rel = inventoryFiles[hotelId];
  if (!rel) continue;
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) continue;
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  for (const row of doc.rows || []) {
    allRows.push({ ...row, hotelId });
  }
}
const deduped = dedupeCrossSegmentOpportunities(allRows);

const queue = nextSegmentsToExecute(plan.cells);
const perHotel = hotelIds.map((hotelId) =>
  evaluateFourteenSegmentHotelCompletion(plan.cells, hotelId)
);

const resumePlan = buildDiscoveryResumePlan({
  hotelIds,
  segmentIds: getCanonicalFourteenSegmentIds(),
});

let discoveryExecution = null;
let originalChain = null;
const budgetManifest = buildThreeHotelFullE2eBudgetManifest({
  hotelIds,
  priorLiveSpendUsd: 0.4,
});

// Original Full E2E chain owns Discovery → Deep Research → Contacts → Qualification → Presentation.
// Standalone discovery matrix runs only when chain is not requested.
if (runOriginalChain) {
  const ceilingFromCli = arg("--budget-ceiling-usd");
  const ceilingFromEnv = process.env.GDI_FULL_E2E_BUDGET_USD;
  const budgetCeilingUsd =
    authorizePaidDiscovery || authorizePaidResearch
      ? Number(
          ceilingFromCli ||
            ceilingFromEnv ||
            budgetManifest.hardCeilingUsd ||
            0
        )
      : 0;
  originalChain = await executeOriginalFullE2eResearchChain({
    hotelIds,
    authorizePaidDiscovery,
    authorizePaidResearch,
    // Live Exa deep-research seed requires the same dual-auth as discovery.
    // Without these, --authorize-paid-research only runs offline ORS continuation.
    authorizeExaAgentLive: authorizePaidResearch === true,
    allowLiveExaDeepSeed: authorizePaidResearch === true,
    authorizationId:
      process.env.GDI_RESEARCH_AUTHORIZATION_ID ||
      "GDI_AIMBRIDGE_FULL_E2E_LIVE_V1",
    budgetCeilingUsd,
    useFixtureProvider: !authorizePaidDiscovery,
    allowHistoricalReplay: true,
    persistState: true,
    // Explicit null = no lead cap. Only apply when flag provided.
    maxCandidates:
      chainCandidateLimit != null ? Number(chainCandidateLimit) : null,
    priorLiveSpendUsd: Number(process.env.GDI_PRIOR_LIVE_SPEND_USD || 0.4),
    outDir: path.join(outDir, "original-full-e2e-chain"),
  });
  discoveryExecution = {
    summary: {
      cellCount: originalChain.discovery?.cellCount || 0,
      totalExtractedCandidates: originalChain.discovery?.extracted || 0,
      totalResearchCostUsd: originalChain.discovery?.costUsd || 0,
      freshRetrievalCells: originalChain.discovery?.freshRetrievalCells || 0,
    },
    cells: [],
  };
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "discovery-execution-traces.json"),
    `${JSON.stringify({ from: "original_full_e2e_chain", discovery: originalChain.discovery }, null, 2)}\n`
  );
} else if (executeDiscovery) {
  discoveryExecution = await executeFourteenSegmentDiscoveryMatrix({
    hotelIds,
    authorizePaidDiscovery,
    allowHistoricalReplay: true,
    persistState: true,
  });
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "discovery-execution-traces.json"),
    `${JSON.stringify(discoveryExecution, null, 2)}\n`
  );
  fs.writeFileSync(
    path.join(outDir, "discovery-resume-plan.json"),
    `${JSON.stringify(
      buildDiscoveryResumePlan({
        hotelIds,
        segmentIds: getCanonicalFourteenSegmentIds(),
      }),
      null,
      2
    )}\n`
  );
}

const executorSpecs = listFourteenSegmentExecutorSpecs().map((s) => ({
  segmentId: s.segmentId,
  classification: s.classification,
  hotelRunnerExists: s.hotelRunnerExists,
  strategy: s.strategy,
  paidProvider: s.paidProvider,
}));

const statusRollup = (() => {
  if (!discoveryExecution?.cells?.length) return null;
  const counts = Object.create(null);
  for (const cell of discoveryExecution.cells) {
    const st =
      cell.executionStatus ||
      cell.classification ||
      "UNKNOWN";
    counts[st] = (counts[st] || 0) + 1;
  }
  return {
    byExecutionStatus: counts,
    liveProviderCallCells: discoveryExecution.cells.filter(
      (c) => (c.providerCallCount || 0) > 0 || (c.freshRetrievals || 0) > 0
    ).length,
    historicalReplayCells: discoveryExecution.cells.filter((c) =>
      String(c.retrievalMode || "").includes("HISTORICAL")
    ).length,
    blockedPaidCells: discoveryExecution.cells.filter(
      (c) =>
        c.executionStatus === "EXECUTION_BLOCKED" ||
        String(c.stopReason || "").includes("PAID_PROVIDER")
    ).length,
    discoveryCompletedClaims: discoveryExecution.cells.filter(
      (c) => c.discoveryCompleted === true
    ).length,
    totalResearchCostUsd: discoveryExecution.cells.reduce(
      (n, c) => n + Number(c.researchCostUsd || 0),
      0
    ),
  };
})();

const report = {
  version: FULL_E2E_FOURTEEN_SEGMENT_TRAVERSAL_V1,
  at: new Date().toISOString(),
  dryRun,
  executeDiscovery,
  authorizePaidDiscovery,
  runOriginalChain,
  authorizePaidResearch,
  mode: runOriginalChain
    ? "ORIGINAL_FULL_E2E_CHAIN"
    : executeDiscovery
      ? "DISCOVERY_EXECUTION"
      : "ACCOUNTING_ONLY",
  budgetManifest,
  originalChainSummary: originalChain
    ? {
        retainedCandidates: originalChain.retainedCandidates,
        processedCount: originalChain.processedCount,
        withPresentation: originalChain.withPresentation,
        paidSpendUsd: originalChain.paidSpendUsd,
        fullE2eCompleteClaimAllowed: originalChain.fullE2eCompleteClaimAllowed,
      }
    : null,
  completionContract: {
    EXECUTOR_NOT_IMPLEMENTED: "registered adapter/query path missing",
    EXECUTOR_READY: "queries built; awaiting provider or historical/fixture",
    EXECUTION_BLOCKED: "paid provider gated or hotel unresolved",
    EXECUTION_PARTIAL: "fixture or historical Stage A replay only",
    DISCOVERY_EXECUTED: "live provider retrieval produced candidates",
    DISCOVERY_COMPLETED: "segment research stop conditions met with live evidence",
    QUALIFICATION_PARTIAL: "commercial qualification started (separate stage)",
    QUALIFICATION_COMPLETED: "buyer/contact commercial eligibility met",
    rule:
      "Traversal or historical replay alone cannot claim DISCOVERY_COMPLETED. Qualification is a separate stage from discovery.",
  },
  segmentIds: getCanonicalFourteenSegmentIds(),
  hotelIds,
  cellCount: plan.cellCount,
  queueRemaining: queue.map((c) => ({
    hotelId: c.hotelId,
    segmentId: c.segmentId,
    status: c.status,
  })),
  perHotel,
  opportunityAccounting: {
    rawRows: allRows.length,
    uniqueAfterCrossSegmentDedupe: deduped.length,
  },
  executorRegistry: executorSpecs,
  resumePlanBeforeExecution: resumePlan,
  discoveryExecutionSummary: discoveryExecution?.summary || null,
  discoveryStatusRollup: statusRollup,
  completeClaimsBlocked: perHotel.filter((h) => h.incompleteBecauseNotRun),
  discoveryCompleteClaimAllowed: false,
  note: runOriginalChain
    ? "Original Full E2E chain invoked (discovery → deep research → contacts → qualification → presentation). Live paid Exa requires --authorize-paid-discovery and --authorize-paid-research with budget ceiling. See original-full-e2e-chain/."
    : executeDiscovery
      ? "Discovery executors were invoked. See discovery-execution-traces.json. Historical replay is Stage A recovery — not a FullE2E discovery COMPLETE claim. Pass --run-original-full-e2e-chain to continue into deep research/contacts/qualification/presentation."
      : "ACCOUNTING ONLY — no discovery executors invoked. Pass --execute-discovery to run registered executors / historical Stage A replay. Do not treat this exit as discovery completion.",
};

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "orchestrator-report.json"),
  `${JSON.stringify(report, null, 2)}\n`
);
if (!dryRun) {
  fs.writeFileSync(
    priorPath,
    `${JSON.stringify({ cells: plan.cells, updatedAt: report.at }, null, 2)}\n`
  );
}

console.log(JSON.stringify(report, null, 2));

if (!executeDiscovery) {
  console.error(
    "FullE2E ACCOUNTING ONLY: 0 discovery executors invoked. Re-run with --execute-discovery."
  );
  if (has("--require-discovery-execution")) process.exit(4);
}

const anyNotRun = perHotel.some((h) => h.incompleteBecauseNotRun);
if (anyNotRun) {
  console.error(
    "FullE2E discovery COMPLETE claim blocked: applicable segments remain NOT_RUN."
  );
  if (has("--require-complete")) process.exit(3);
}

if (
  executeDiscovery &&
  discoveryExecution?.summary &&
  discoveryExecution.summary.freshRetrievalCells === 0 &&
  has("--require-fresh-retrieval")
) {
  console.error(
    "FullE2E fresh retrieval required but 0 cells performed provider calls (historical replay / stubs only)."
  );
  process.exit(5);
}

process.exit(0);
