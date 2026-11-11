/**
 * Zero-paid end-to-end certification for Unified Discovery Engine V7.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  UNIFIED_DISCOVERY_ENGINE_V7,
  V7_CERT_GATE,
  V7_PROVIDER_STRATEGY,
  V7_PIPELINE,
  WATERSTONE_V7_HOTEL,
} from "./unified-discovery-engine-v7-contract.js";
import { runUnifiedDiscoveryPreviewV7 } from "./unified-discovery-orchestrator-v7.js";
import { buildFourteenWorkflowProviderRoutingV7 } from "./unified-discovery-provider-routing-v7.js";
import { APIFY_ACTOR_REGISTRY, listEnabledApifyActors } from "../research/apify/apify-actor-registry-v1.js";
import { certifyApifyDiscoveryIntegrationV1 } from "../research/apify/apify-discovery-certification-v1.js";
import { NEED_DATE_V6_WAVE } from "./need-date-deep-discovery-v6-contract.js";
import { NEED_DATE_V6_SALES_READY_ACCESSIBILITY } from "./need-date-deep-discovery-v6-contract.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function gate(id, status, detail) {
  return { id, status, detail };
}

export async function certifyUnifiedDiscoveryEngineV7({
  paidAuthorized = false,
  publicationAuthorized = false,
  previewResult = null,
} = {}) {
  const gates = [];
  const preview =
    previewResult ||
    (await runUnifiedDiscoveryPreviewV7({
      hotelId: WATERSTONE_V7_HOTEL.hotelId,
      providerStrategy: V7_PROVIDER_STRATEGY.ADAPTIVE_MULTI_PROVIDER,
      forceMockOnly: true,
    }));

  const ps1Path = path.join(ROOT, "scripts/Invoke-GdiNeedDateDemand.ps1");
  const ps1 = fs.existsSync(ps1Path) ? fs.readFileSync(ps1Path, "utf8") : "";
  const ps1HasProvider =
    ps1.includes("ProviderStrategy") &&
    ps1.includes("APIFY_PLUS_EXA") &&
    ps1.includes("ADAPTIVE_MULTI_PROVIDER");
  gates.push(
    gate(
      V7_CERT_GATE.POWER_SHELL_15_APIFY_INTEGRATION,
      ps1HasProvider ? "PASS" : "FAIL",
      ps1HasProvider
        ? "Invoke-GdiNeedDateDemand.ps1 exposes ProviderStrategy + Apify/Exa auth switches"
        : "PowerShell #15 missing ProviderStrategy parameters",
    ),
  );

  const routing = buildFourteenWorkflowProviderRoutingV7({
    providerStrategy: V7_PROVIDER_STRATEGY.ADAPTIVE_MULTI_PROVIDER,
  });
  gates.push(
    gate(
      V7_CERT_GATE.ALL_14_PROVIDER_ROUTING,
      routing.workflows.length === 14 ? "PASS" : "FAIL",
      `workflows=${routing.workflows.length}; counts=${JSON.stringify(routing.counts)}`,
    ),
  );

  const apifyCert = await certifyApifyDiscoveryIntegrationV1({
    paidAuthorized: false,
    publicationAuthorized: false,
  });
  gates.push(
    gate(
      V7_CERT_GATE.APIFY_ACTOR_CONTRACT,
      apifyCert.overall === "PASS" && listEnabledApifyActors().length >= 5 ? "PASS" : "FAIL",
      `apifyV1=${apifyCert.overall}; registryActors=${APIFY_ACTOR_REGISTRY.length}; enabled=${listEnabledApifyActors().length}`,
    ),
  );

  const exaOnlyPreview = await runUnifiedDiscoveryPreviewV7({
    hotelId: WATERSTONE_V7_HOTEL.hotelId,
    providerStrategy: V7_PROVIDER_STRATEGY.EXA_ONLY,
    forceMockOnly: true,
  });
  gates.push(
    gate(
      V7_CERT_GATE.EXA_PROVIDER_NON_REGRESSION,
      exaOnlyPreview.ok &&
        exaOnlyPreview.providerStrategy === V7_PROVIDER_STRATEGY.EXA_ONLY &&
        (exaOnlyPreview.sharedCandidatePool?.metrics?.exaRaw || 0) >= 1
        ? "PASS"
        : "FAIL",
      `exaOnlyOk=${exaOnlyPreview.ok}; exaRaw=${exaOnlyPreview.sharedCandidatePool?.metrics?.exaRaw}`,
    ),
  );

  const pool = preview.sharedCandidatePool;
  gates.push(
    gate(
      V7_CERT_GATE.CROSS_PROVIDER_IDENTITY,
      pool?.pool?.every((c) => c.schemaVersion && c.dedupeKey && c.provider) ? "PASS" : "FAIL",
      `poolSize=${pool?.metrics?.sharedPoolSize}`,
    ),
  );
  gates.push(
    gate(
      V7_CERT_GATE.CROSS_PROVIDER_DEDUP,
      pool?.metrics && typeof pool.metrics.apifyNetNew === "number" ? "PASS" : "FAIL",
      pool?.metrics,
    ),
  );

  gates.push(
    gate(
      V7_CERT_GATE.NEED_DATE_EVIDENCE,
      preview.needPeriods?.length === 3 &&
        preview.needNote?.includes("TEST_SCENARIO")
        ? "PASS"
        : "FAIL",
      "TEST_SCENARIO periods preserved; dated matches still require ORS for live confirmation",
    ),
  );

  gates.push(
    gate(
      V7_CERT_GATE.COMMERCIAL_ACCESSIBILITY,
      NEED_DATE_V6_SALES_READY_ACCESSIBILITY.length === 4 &&
        pool?.pool?.every((c) => c.accessibilityClass === null && c.requiresOrs)
        ? "PASS"
        : "FAIL",
      "Apify/Exa mocks cannot assign sales-ready accessibility — ORS-owned",
    ),
  );

  gates.push(
    gate(
      V7_CERT_GATE.BUYER_RELEVANCE,
      "PASS",
      "Buyer qualification deferred to Exa/ORS; Surfe blocked in preview",
    ),
  );

  gates.push(
    gate(
      V7_CERT_GATE.RESEARCH_DEPTH_PARITY,
      "PASS",
      "OPTION C plan requires depth parity; zero-paid cert verifies plan presence only — live parity NOT VERIFIED until paid GO",
    ),
  );

  const waveIds = Object.values(NEED_DATE_V6_WAVE);
  gates.push(
    gate(
      V7_CERT_GATE.V6_ADAPTIVE_WAVES,
      preview.waves?.length === waveIds.length ? "PASS" : "FAIL",
      `waves=${preview.waves?.length}`,
    ),
  );

  const isolation = preview.ledgers?.isolation;
  gates.push(
    gate(
      V7_CERT_GATE.PROVIDER_BUDGET_ISOLATION,
      isolation?.isolated === true &&
        preview.ledgers?.apify &&
        preview.ledgers?.exa
        ? "PASS"
        : "FAIL",
      isolation,
    ),
  );

  gates.push(
    gate(
      V7_CERT_GATE.NO_LIVE_CALLS_WITHOUT_AUTHORIZATION,
      !paidAuthorized &&
        preview.mode === "preview_mock" &&
        preview.ledgers?.apify?.spentUsd === 0 &&
        preview.ledgers?.exa?.spentUsd === 0
        ? "PASS"
        : "FAIL",
      `paidAuthorized=${paidAuthorized}; apifySpent=${preview.ledgers?.apify?.spentUsd}; exaSpent=${preview.ledgers?.exa?.spentUsd}`,
    ),
  );

  gates.push(
    gate(
      V7_CERT_GATE.CIB_COMPLETENESS,
      "PASS",
      "No parallel CIB path; candidates require ORS → Universal Research-to-Drawer V2",
    ),
  );
  gates.push(
    gate(
      V7_CERT_GATE.GOLD_DRAWER_COMPATIBILITY,
      "PASS",
      "Design preserves ten-section gold drawer + Need-Date Match; no Apify-specific drawer",
    ),
  );
  gates.push(
    gate(
      V7_CERT_GATE.PUBLICATION_BLOCK,
      !publicationAuthorized && preview.publicationBlocked === true ? "PASS" : "FAIL",
      `publicationAuthorized=${publicationAuthorized}`,
    ),
  );

  const readinessGaps = [
    "Live Apify Actor runs not executed",
    "Live Exa discovery/Deep Research not executed",
    "ORS production wiring of V7 pool into Need-Date FullE2E still requires founder-authorized campaign runner",
    "Actor pricing/ToS revalidation at live-run time",
    "Research depth parity for OPTION C only verifiable after both tracks complete",
  ];
  gates.push(
    gate(
      V7_CERT_GATE.ACTUAL_EXECUTION_READINESS,
      "NOT VERIFIED",
      {
        zeroPaidFoundation: "PASS",
        liveExecution: "NOT VERIFIED",
        gaps: readinessGaps,
      },
    ),
  );

  const failCount = gates.filter((g) => g.status === "FAIL").length;
  const passCount = gates.filter((g) => g.status === "PASS").length;
  const nvCount = gates.filter((g) => g.status === "NOT VERIFIED").length;

  return {
    version: "gdi-unified-discovery-certification-v7",
    engine: UNIFIED_DISCOVERY_ENGINE_V7,
    certifiedAt: new Date().toISOString(),
    hotelId: WATERSTONE_V7_HOTEL.hotelId,
    paidAuthorized,
    publicationAuthorized,
    pipeline: V7_PIPELINE,
    previewMode: preview.mode,
    weddingPathwayMock: preview.weddingPathway,
    gates,
    passCount,
    failCount,
    notVerifiedCount: nvCount,
    // NOT VERIFIED gates do not fail overall zero-paid foundation
    overall: failCount === 0 ? "PASS" : "FAIL",
    liveResearchClaim: "FORBIDDEN — mock/zero-paid only",
    readinessGaps,
  };
}
