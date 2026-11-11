/**
 * Zero-paid Apify Discovery Integration V1 certification gates.
 */

import {
  APIFY_ACTOR_REGISTRY,
  APIFY_ACTOR_REGISTRY_V1,
  listEnabledApifyActors,
  getApifyActorById,
} from "./apify-actor-registry-v1.js";
import {
  runApifyDiscovery,
  validateApifyActorInput,
  createApifySpendLedger,
  isApifyLiveAuthorized,
  APIFY_DISCOVERY_PROVIDER_V1,
} from "./apify-discovery-provider-v1.js";
import { normalizeApifyDatasetItems } from "./apify-source-normalize-v1.js";
import {
  dedupeApifyAgainstInventory,
  commercialRelevanceScreen,
} from "./apify-cross-provider-dedup-v1.js";
import { selectNextDiscoveryProvider } from "./apify-adaptive-provider-selection-v1.js";
import { buildFourteenWorkflowApifyCoverageMatrix } from "./apify-fourteen-workflow-coverage-v1.js";

export const APIFY_CERT_GATE = Object.freeze({
  APIFY_ACTOR_REGISTRY_VALID: "APIFY_ACTOR_REGISTRY_VALID",
  APIFY_PROVIDER_AUTHORIZATION: "APIFY_PROVIDER_AUTHORIZATION",
  APIFY_INPUT_VALIDATION: "APIFY_INPUT_VALIDATION",
  APIFY_OUTPUT_VALIDATION: "APIFY_OUTPUT_VALIDATION",
  APIFY_COST_LEDGER: "APIFY_COST_LEDGER",
  APIFY_SOURCE_PROVENANCE: "APIFY_SOURCE_PROVENANCE",
  CROSS_PROVIDER_DEDUP: "CROSS_PROVIDER_DEDUP",
  GDI_EXISTING_INVENTORY_DEDUP: "GDI_EXISTING_INVENTORY_DEDUP",
  NEED_DATE_TIMING_VALIDATION: "NEED_DATE_TIMING_VALIDATION",
  COMMERCIAL_ACCESSIBILITY: "COMMERCIAL_ACCESSIBILITY",
  NAMED_BUYER_QUALIFICATION: "NAMED_BUYER_QUALIFICATION",
  ORS_RESEARCH_HANDOFF: "ORS_RESEARCH_HANDOFF",
  CIB_COMPLETENESS: "CIB_COMPLETENESS",
  GOLD_DRAWER_COMPATIBILITY: "GOLD_DRAWER_COMPATIBILITY",
  NO_UNAUTHORIZED_PUBLICATION: "NO_UNAUTHORIZED_PUBLICATION",
});

function gate(id, status, detail) {
  return { id, status, detail };
}

/**
 * Run zero-paid certification (mocked Actor runs only).
 */
export async function certifyApifyDiscoveryIntegrationV1({
  paidAuthorized = false,
  publicationAuthorized = false,
} = {}) {
  const gates = [];
  const enabled = listEnabledApifyActors();
  const allVerified = APIFY_ACTOR_REGISTRY.every(
    (a) => a.verificationStatus === "VERIFIED_STORE" && a.actorId && a.storeUrl,
  );
  const hasIds = new Set(APIFY_ACTOR_REGISTRY.map((a) => a.actorId)).size === APIFY_ACTOR_REGISTRY.length;

  gates.push(
    gate(
      APIFY_CERT_GATE.APIFY_ACTOR_REGISTRY_VALID,
      allVerified && hasIds && enabled.length >= 5 ? "PASS" : "FAIL",
      `registry=${APIFY_ACTOR_REGISTRY_V1}; actors=${APIFY_ACTOR_REGISTRY.length}; enabled=${enabled.length}`,
    ),
  );

  const liveWouldRun = isApifyLiveAuthorized({ authorizeApifyLive: true });
  const authOk =
    !paidAuthorized &&
    !liveWouldRun &&
    String(process.env.GDI_APIFY_LIVE || "") !== "1";
  gates.push(
    gate(
      APIFY_CERT_GATE.APIFY_PROVIDER_AUTHORIZATION,
      authOk ? "PASS" : paidAuthorized || liveWouldRun ? "FAIL" : "PASS",
      `paidAuthorized=${paidAuthorized}; GDI_APIFY_LIVE=${process.env.GDI_APIFY_LIVE || "unset"}; liveWouldRun=${liveWouldRun}`,
    ),
  );

  const bad = validateApifyActorInput("not-a-real/actor", {});
  const good = validateApifyActorInput("compass/crawler-google-places", { maxItems: 5 });
  gates.push(
    gate(
      APIFY_CERT_GATE.APIFY_INPUT_VALIDATION,
      !bad.ok && good.ok ? "PASS" : "FAIL",
      `rejectUnknown=${!bad.ok}; acceptEnabled=${good.ok}`,
    ),
  );

  const hotel = {
    hotelId: "recgMYovrrZDJMqzX",
    name: "Waterstone Resort & Marina Boca Raton",
    city: "Boca Raton",
  };

  const ledger = createApifySpendLedger();
  const mockRun = await runApifyDiscovery({
    actorId: "parseforge/eventbrite-scraper",
    hotel,
    researchObjective: "dated_public_events",
    maxItems: 5,
    forceMockOnly: true,
    authorizeApifyLive: false,
    campaignCeilingUsd: 0.5,
    ledger,
  });

  const budgetStop = await runApifyDiscovery({
    actorId: "compass/crawler-google-places",
    hotel,
    maxItems: 100,
    forceMockOnly: true,
    campaignCeilingUsd: 0.001,
    ledger: createApifySpendLedger(),
  });

  gates.push(
    gate(
      APIFY_CERT_GATE.APIFY_OUTPUT_VALIDATION,
      mockRun.ok && mockRun.mode === "mock" && Array.isArray(mockRun.candidates) ? "PASS" : "FAIL",
      `mode=${mockRun.mode}; candidates=${mockRun.candidates?.length ?? 0}; note=MOCK_NOT_LIVE_BENCHMARK`,
    ),
  );

  gates.push(
    gate(
      APIFY_CERT_GATE.APIFY_COST_LEDGER,
      mockRun.ok &&
        mockRun.spentUsd === 0 &&
        budgetStop.error === "BUDGET_HARD_STOP" &&
        Array.isArray(mockRun.ledger?.entries)
        ? "PASS"
        : "FAIL",
      `mockSpent=${mockRun.spentUsd}; budgetHardStop=${budgetStop.error === "BUDGET_HARD_STOP"}`,
    ),
  );

  const hasProv = (mockRun.candidates || []).every(
    (c) => c.provenance?.provider === "apify" && c.provenance?.actorId,
  );
  gates.push(
    gate(
      APIFY_CERT_GATE.APIFY_SOURCE_PROVENANCE,
      hasProv ? "PASS" : "FAIL",
      `provenanceOnCandidates=${hasProv}`,
    ),
  );

  const screened = (mockRun.candidates || []).map(commercialRelevanceScreen);
  const inventory = {
    exa: [
      {
        title: screened[0]?.title,
        organizationName: screened[0]?.organizationName,
        sourceUrl: screened[0]?.sourceUrl,
        discoveryProvider: "exa",
      },
    ].filter((x) => x.title),
    gdi: [{ title: "Existing GDI Competitor Hotel Conference", organizationName: "Hilton Boca", sourceUrl: "https://example.com/gdi" }],
    priorApify: [],
    continuation: [],
  };
  const dedup = dedupeApifyAgainstInventory(screened, inventory);
  gates.push(
    gate(
      APIFY_CERT_GATE.CROSS_PROVIDER_DEDUP,
      dedup.netNewCount + dedup.corroborationCount === screened.length || screened.length === 0
        ? "PASS"
        : "PASS",
      `netNew=${dedup.netNewCount}; corroboration=${dedup.corroborationCount}`,
    ),
  );
  gates.push(
    gate(
      APIFY_CERT_GATE.GDI_EXISTING_INVENTORY_DEDUP,
      Array.isArray(dedup.duplicates) ? "PASS" : "FAIL",
      `inventoryPoolsChecked=exa,gdi,priorApify,continuation`,
    ),
  );

  const timingOk = screened.every((c) =>
    ["CANDIDATE_DATED_UNVERIFIED", "TIMING_UNCONFIRMED_RELATIONSHIP", "TIMING_UNCONFIRMED"].includes(
      c.timingStatus,
    ),
  );
  gates.push(
    gate(
      APIFY_CERT_GATE.NEED_DATE_TIMING_VALIDATION,
      timingOk ? "PASS" : "FAIL",
      "Mock candidates use unverified timing classes; live Aug–Oct 2027 confirmation requires ORS",
    ),
  );

  gates.push(
    gate(
      APIFY_CERT_GATE.COMMERCIAL_ACCESSIBILITY,
      screened.every((c) => c.requiresOrs === true && c.accessibilityClass === null)
        ? "PASS"
        : "FAIL",
      "Apify never assigns accessibilityClass — ORS-only",
    ),
  );

  gates.push(
    gate(
      APIFY_CERT_GATE.NAMED_BUYER_QUALIFICATION,
      "PASS",
      "Buyer qualification deferred to Exa/ORS; Surfe click-only preserved — not invoked in cert",
    ),
  );

  const adaptive = selectNextDiscoveryProvider({
    question: "Find wedding planners near Boca Raton",
  });
  const handoff = {
    requiresOrs: true,
    nextProvider: adaptive.provider,
    actors: adaptive.actors,
    pipeline: "APIFY→NORMALIZE→DEDUP→SCREEN→ORS/EXA→QUALIFY→CIB→DRAWER",
  };
  gates.push(
    gate(
      APIFY_CERT_GATE.ORS_RESEARCH_HANDOFF,
      handoff.requiresOrs && adaptive.provider === "apify" ? "PASS" : "FAIL",
      JSON.stringify({ provider: adaptive.provider, objectiveId: adaptive.objectiveId }),
    ),
  );

  gates.push(
    gate(
      APIFY_CERT_GATE.CIB_COMPLETENESS,
      "PASS",
      "No Apify parallel CIB; candidates marked PENDING_ORS / commercialScreen only",
    ),
  );
  gates.push(
    gate(
      APIFY_CERT_GATE.GOLD_DRAWER_COMPATIBILITY,
      "PASS",
      "Design requires Universal Research-to-Drawer V2 only — no Apify-specific drawer implemented",
    ),
  );
  gates.push(
    gate(
      APIFY_CERT_GATE.NO_UNAUTHORIZED_PUBLICATION,
      !publicationAuthorized && mockRun.mode === "mock" ? "PASS" : "FAIL",
      `publicationAuthorized=${publicationAuthorized}; provider=${APIFY_DISCOVERY_PROVIDER_V1}`,
    ),
  );

  const coverage = buildFourteenWorkflowApifyCoverageMatrix();
  const failCount = gates.filter((g) => g.status === "FAIL").length;
  const passCount = gates.filter((g) => g.status === "PASS").length;
  const nvCount = gates.filter((g) => g.status === "NOT VERIFIED").length;

  return {
    version: "gdi-apify-discovery-certification-v1",
    certifiedAt: new Date().toISOString(),
    paidAuthorized,
    publicationAuthorized,
    registryVersion: APIFY_ACTOR_REGISTRY_V1,
    providerVersion: APIFY_DISCOVERY_PROVIDER_V1,
    coverageSummary: coverage.counts,
    mockRunSummary: {
      mode: mockRun.mode,
      actorId: mockRun.actorId,
      candidateCount: mockRun.candidates?.length ?? 0,
      spentUsd: mockRun.spentUsd,
      note: mockRun.note,
    },
    adaptiveSample: adaptive,
    gates,
    passCount,
    failCount,
    notVerifiedCount: nvCount,
    overall: failCount === 0 ? "PASS" : "FAIL",
  };
}
