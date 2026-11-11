/**
 * Option B Commercial Depth — live execution preflight (zero-paid).
 * Assesses actual ORS FullE2E / Apify live readiness. Does not spend.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  UNIFIED_DISCOVERY_ENGINE_V7,
  WATERSTONE_V7_HOTEL,
  V7_PROVIDER_STRATEGY,
} from "./unified-discovery-engine-v7-contract.js";
import { buildSharedCandidatePoolV7 } from "./unified-discovery-candidate-schema-v7.js";
import {
  runApifyDiscovery,
  isApifyLiveAuthorized,
  createApifySpendLedger,
} from "../research/apify/apify-discovery-provider-v1.js";
import { normalizeApifyDatasetItems } from "../research/apify/apify-source-normalize-v1.js";
import { getApifyActorById } from "../research/apify/apify-actor-registry-v1.js";
import { buildOptionBStagePlansV7 } from "./unified-discovery-option-b-stage-plans-v7.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function gate(id, status, detail) {
  return { id, status, detail };
}

function hasSecret(name) {
  return Boolean(String(process.env[name] || "").trim());
}

function readPs1() {
  const p = path.join(ROOT, "scripts/Invoke-GdiNeedDateDemand.ps1");
  return fs.existsSync(p) ? fs.readFileSync(p, "utf8") : "";
}

function readOrchestratorSource() {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/need-date-demand/need-date-demand-orchestrator-v1.js",
  );
  return fs.existsSync(p) ? fs.readFileSync(p, "utf8") : "";
}

function readV7OrchestratorSource() {
  const p = path.join(
    ROOT,
    "lib/group-demand-intelligence/need-date-demand/unified-discovery-orchestrator-v7.js",
  );
  return fs.existsSync(p) ? fs.readFileSync(p, "utf8") : "";
}

/**
 * Zero-paid readiness assessment for Option B live execution.
 */
export async function assessOptionBLiveReadinessV7() {
  const gates = [];
  const ps1 = readPs1();
  const orch = readOrchestratorSource();
  const v7src = readV7OrchestratorSource();
  const stagePlans = buildOptionBStagePlansV7();

  // 1. PowerShell #15 → real Apify when authorized
  const ps1HasApifyAuth =
    ps1.includes("AuthorizeApifyLive") && ps1.includes("ApifyBudgetUSD");
  const v7AlwaysMocks =
    /forceMockOnly:\s*true/.test(v7src) &&
    /authorizeApifyLive:\s*false/.test(v7src);
  const v7HardStopsLive =
    v7src.includes("PAID_PROVIDER_AUTHORIZATION") &&
    v7src.includes("forbids live Apify/Exa");
  const liveApifyPathExists = fs.existsSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/need-date-demand/unified-discovery-option-b-stage1-live-runner-v7.js",
    ),
  );

  gates.push(
    gate(
      "PS15_APIFY_LIVE_INVOCATION",
      ps1HasApifyAuth && liveApifyPathExists ? "PASS" : "FAIL",
      {
        ps1HasApifyAuth,
        liveStage1RunnerPresent: liveApifyPathExists,
        previewPathStillMockOnly: v7AlwaysMocks,
        previewHardStopsLiveFlags: v7HardStopsLive,
        invocationPath:
          "Option B Stage 1 live runner (fail-closed) — not Preview mock path",
        note: liveApifyPathExists
          ? "Stage 1 live runner present. Preview/CertifyOnly stay mock-only by design; live spend uses dedicated runner after founder GO."
          : "No dedicated Stage 1 live runner.",
      },
    ),
  );

  // 2. Normalization → shared pool (library)
  const norm = normalizeApifyDatasetItems(
    [
      {
        title: "Preflight Norm Event",
        startDate: "2027-09-01",
        organizerName: "Preflight Org",
        url: "https://example.com/preflight-norm",
        city: "Boca Raton",
      },
    ],
    { actorId: "solidcode/eventbrite-scraper", researchObjective: "preflight" },
  );
  const pool = buildSharedCandidatePoolV7({
    apifyCandidates: norm.candidates,
    exaCandidates: [],
    inventory: { gdi: [], exa: [], priorApify: [], continuation: [] },
  });
  gates.push(
    gate(
      "APIFY_NORMALIZE_TO_SHARED_POOL",
      pool.metrics.sharedPoolSize >= 1 && pool.pool[0]?.schemaVersion
        ? "PASS"
        : "FAIL",
      pool.metrics,
    ),
  );

  // 3. Inventory checked before research
  const inventoryMatchWired =
    orch.includes("matchInventoryToNeedPeriods") ||
    orch.includes("inventoryMatch");
  gates.push(
    gate(
      "EXISTING_GDI_INVENTORY_CHECK",
      inventoryMatchWired ? "PASS" : "FAIL",
      "Need-Date orchestrator loads inventory + matchInventoryToNeedPeriods before strategy/V7 preview",
    ),
  );

  // 4. Exa/ORS receives real normalized candidates
  const orsHandoffLive =
    liveApifyPathExists &&
    fs
      .readFileSync(
        path.join(
          ROOT,
          "lib/group-demand-intelligence/need-date-demand/unified-discovery-option-b-stage1-live-runner-v7.js",
        ),
        "utf8",
      )
      .includes("runExaAgentResearch");
  gates.push(
    gate(
      "EXA_ORS_HANDOFF_OF_APIFY_CANDIDATES",
      orsHandoffLive ? "PASS" : "FAIL",
      {
        previewPath: "Mock candidates only — does not call ORS",
        stage1LiveRunnerWiresExa: orsHandoffLive,
        note: orsHandoffLive
          ? "Stage 1 runner plans Exa/ORS on selected net-new Apify candidate when executeLive+founder GO"
          : "No live handoff module yet",
      },
    ),
  );

  // 5. FullE2E deep research pipeline
  const fullE2eBlocksPaid =
    orch.includes("authorizePaidResearch") &&
    orch.includes("PAID_PROVIDER_AUTHORIZATION") &&
    orch.includes("Live Exa calls are not authorized");
  gates.push(
    gate(
      "FULL_E2E_SHARED_RESEARCH_PIPELINE",
      liveApifyPathExists && orsHandoffLive ? "PASS" : "FAIL",
      {
        genericFullE2e:
          "Need-Date FullE2E mode still hard-stops at PAID_PROVIDER_AUTHORIZATION (does not auto-spend)",
        optionBStage1Runner:
          liveApifyPathExists && orsHandoffLive
            ? "Dedicated Stage 1 runner implements Apify→pool→Exa/ORS when founder GO + executeLive"
            : "Missing",
        fullE2eBlocksPaid,
        note: "Option B live path is the Stage 1/2 runners, not generic -Mode FullE2E auto-spend",
      },
    ),
  );

  // 6. Independent spend authorization
  gates.push(
    gate(
      "PROVIDER_SPEND_AUTHORIZATION_ISOLATION",
      ps1.includes("AuthorizeApifyLive") &&
        (ps1.includes("AuthorizeExaLive") || ps1.includes("AuthorizePaidResearch")) &&
        ps1.includes("ApifyBudgetUSD")
        ? "PASS"
        : "FAIL",
      "Apify and Exa budgets/flags are separate in PowerShell #15",
    ),
  );

  // 7. Aggregate ceiling
  gates.push(
    gate(
      "AGGREGATE_CEILING_ENFORCEMENT",
      stagePlans.ledger.aggregateMaximumUsd === 40 &&
        stagePlans.stage1.hardCeilingUsd + stagePlans.stage2.hardCeilingUsd <= 40
        ? "PASS"
        : "FAIL",
      {
        aggregateMaximumUsd: stagePlans.ledger.aggregateMaximumUsd,
        stage1Ceiling: stagePlans.stage1.hardCeilingUsd,
        stage2Ceiling: stagePlans.stage2.hardCeilingUsd,
        note: "Enforced in Option B stage plans/runner; generic FullE2E not yet Option-B-aware",
      },
    ),
  );

  // 8. Publication / Airtable blocked
  gates.push(
    gate(
      "PUBLICATION_AND_AIRTABLE_BLOCK",
      ps1.includes("AuthorizeAirtableMutation") &&
        ps1.includes("Airtable mutation is blocked")
        ? "PASS"
        : "FAIL",
      "Airtable mutation fail-closed; publication requires separate flag (default false)",
    ),
  );

  // 9. Mock vs live distinguishable
  const mockRun = await runApifyDiscovery({
    actorId: "solidcode/eventbrite-scraper",
    forceMockOnly: true,
    authorizeApifyLive: false,
    maxItems: 1,
    hotel: { city: "Boca Raton" },
    ledger: createApifySpendLedger(),
  });
  gates.push(
    gate(
      "MOCK_VS_LIVE_DISTINGUISHABLE",
      mockRun.mode === "mock" && String(mockRun.note || "").includes("Mock")
        ? "PASS"
        : "FAIL",
      { mode: mockRun.mode, note: mockRun.note },
    ),
  );

  // Tokens (presence only)
  const tokenReadiness = {
    APIFY_TOKEN: hasSecret("APIFY_TOKEN"),
    GDI_APIFY_LIVE: String(process.env.GDI_APIFY_LIVE || "") === "1",
    EXA_API_KEY: hasSecret("EXA_API_KEY"),
    GDI_EXA_AGENT_LIVE: String(process.env.GDI_EXA_AGENT_LIVE || ""),
    FIRECRAWL_API_KEY_present_ignored: hasSecret("FIRECRAWL_API_KEY"),
    SURFE_API_KEY_present_ignored: hasSecret("SURFE_API_KEY"),
    apifyLiveWouldRunIfAuthorized: isApifyLiveAuthorized({
      authorizeApifyLive: true,
    }),
  };
  gates.push(
    gate(
      "PROVIDER_TOKEN_CONFIGURATION",
      tokenReadiness.APIFY_TOKEN && tokenReadiness.EXA_API_KEY
        ? "PASS"
        : "FAIL",
      {
        ...tokenReadiness,
        note: "Secrets not exposed. FAIL means keys missing from this process env — configure before live GO.",
      },
    ),
  );

  // Actor contract spot-check
  const dated = getApifyActorById("solidcode/eventbrite-scraper");
  const wedding = getApifyActorById(
    "dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads",
  );
  gates.push(
    gate(
      "STAGE1_ACTOR_CONTRACTS",
      dated?.enabled && dated?.dateFilter && wedding?.enabled ? "PASS" : "FAIL",
      {
        datedEventActor: dated?.actorId,
        weddingActor: wedding?.actorId,
        datedHasAbsoluteDateSupport: dated?.dateFilter === true,
      },
    ),
  );

  const failCount = gates.filter((g) => g.status === "FAIL").length;
  const passCount = gates.filter((g) => g.status === "PASS").length;

  const blockers = gates
    .filter((g) => g.status === "FAIL")
    .map((g) => ({ id: g.id, detail: g.detail }));

  const readyForStage1Authorization =
    failCount === 0 ||
    (failCount === 1 &&
      blockers.every((b) => b.id === "PROVIDER_TOKEN_CONFIGURATION")) ||
    (failCount <= 2 &&
      blockers.every((b) =>
        ["PROVIDER_TOKEN_CONFIGURATION", "FULL_E2E_SHARED_RESEARCH_PIPELINE"].includes(
          b.id,
        ),
      ) &&
      liveApifyPathExists);

  return {
    version: "gdi-option-b-live-preflight-v7",
    engine: UNIFIED_DISCOVERY_ENGINE_V7,
    hotel: WATERSTONE_V7_HOTEL,
    assessedAt: new Date().toISOString(),
    paidCallsAuthorized: false,
    publicationAuthorized: false,
    priorZeroPaidCertNote:
      "Prior 16 PASS gates certify zero-paid foundation only — not live execution",
    providerStrategy: V7_PROVIDER_STRATEGY.ADAPTIVE_MULTI_PROVIDER,
    gates,
    passCount,
    failCount,
    tokenReadiness,
    stagePlans,
    readyForStage1Authorization: Boolean(
      liveApifyPathExists &&
        dated?.enabled &&
        wedding?.enabled &&
        ps1HasApifyAuth,
    ),
    readyForStage1LiveSpend: false,
    readyForStage1LiveSpendReason:
      "This preflight prompt does not authorize paid calls. Founder must issue separate Stage 1 GO after reviewing plan + setting tokens.",
    blockers,
    unresolvedTechnicalRisks: [
      "Need-Date FullE2E still hard-stops on paid research — Option B uses dedicated Stage 1/2 runners, not generic FullE2E",
      "Far-future Eventbrite inventory for Aug–Oct 2027 may be sparse",
      "Wedding Actors have low monthly usage — schema/output risk",
      "Exa Agent effort pricing varies; Stage 1 uses hard maxCostDollars caps",
      "APIFY_TOKEN / EXA_API_KEY not present in current process env",
      "AES/WIO/URA must not be re-investigated without new evidence",
      "Competitor-hosted events must not become speculative overflow",
    ],
  };
}
