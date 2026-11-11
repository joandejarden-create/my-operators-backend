/**
 * ADP_BPP_SOURCE_API_DOM_PARITY_V1
 *
 * Permanent Full E2E gate: certified BPP source pack ↔ staged payload ↔
 * review API ↔ rendered DOM must reconcile for the four primary KPIs and
 * peer ranking set. Blocks FULL_ADP_E2E_CERTIFIED_BPP_COMPLETE / CUSTOMER_READY
 * when any layer is blank/stale while an upstream layer is certified.
 */

export const ADP_BPP_SOURCE_API_DOM_PARITY_V1 = "ADP_BPP_SOURCE_API_DOM_PARITY_V1";

const PRIMARY_KPI_IDS = Object.freeze([
  "portfolioAiPresence",
  "portfolioRank",
  "numberOneAppearance",
  "top3Appearance",
]);

function kpiMap(bpp) {
  const out = {};
  for (const k of bpp?.kpis || []) {
    if (k?.id) out[k.id] = String(k.value ?? "").trim();
  }
  return out;
}

function rankingNames(bpp) {
  return (bpp?.ranking?.rows || [])
    .map((r) => String(r.name || "").trim())
    .filter(Boolean);
}

/**
 * Pure readiness acceptance used by SPA — mirrored for gate unit tests.
 */
export function bppComparisonCertifiedIsRenderable(bpp) {
  if (!bpp || bpp.status !== "READY") return false;
  const customer = bpp.bppCustomerState || bpp.customerReadyClass || "";
  const measurement = bpp.bppMeasurementState || "";
  const stagedCertified =
    customer === "BPP_COMPARISON_CERTIFIED" ||
    measurement === "BPP_COMPARISON_CERTIFIED" ||
    customer === "BPP_READY_POPULATED_FULL" ||
    customer === "BPP_READY_POPULATED_RANK_ONLY";
  const assuranceOk =
    bpp.assuranceStatus === "CUSTOMER_READY" ||
    bpp.assuranceStatus === "PERIOD_2_CERTIFIED_LOCAL" ||
    bpp.assuranceStatus === "STAGED_COMPARISON_CERTIFIED" ||
    bpp.controlledPeriod2LocalPreview === true ||
    stagedCertified;
  const hasKpis = Array.isArray(bpp.kpis) && bpp.kpis.length > 0;
  const hasRows =
    bpp.ranking && Array.isArray(bpp.ranking.rows) && bpp.ranking.rows.length > 0;
  return assuranceOk && hasKpis && hasRows && bpp.showAnalyticalScaffolding !== false;
}

/**
 * Compare source pack / staged / API KPI maps.
 */
export function reconcileBppKpiLayers({
  sourcePackBpp = null,
  stagedBpp = null,
  apiBpp = null,
  domKpiValues = null,
  domHasAwaitingMessage = null,
  domRankingNames = null,
} = {}) {
  const failures = [];
  const layers = {
    source: kpiMap(sourcePackBpp),
    staged: kpiMap(stagedBpp),
    api: kpiMap(apiBpp),
  };

  const certifiedUpstream =
    bppComparisonCertifiedIsRenderable(sourcePackBpp) ||
    bppComparisonCertifiedIsRenderable(stagedBpp) ||
    bppComparisonCertifiedIsRenderable(apiBpp);

  for (const id of PRIMARY_KPI_IDS) {
    const vals = [layers.source[id], layers.staged[id], layers.api[id]].filter(
      (v) => v && v !== "—" && v !== "-"
    );
    if (vals.length >= 2) {
      const head = vals[0];
      for (const v of vals.slice(1)) {
        if (v !== head) {
          failures.push(`KPI_MISMATCH_${id}:${head}_vs_${v}`);
        }
      }
    }
  }

  if (certifiedUpstream) {
    if (!bppComparisonCertifiedIsRenderable(apiBpp) && apiBpp) {
      // API may omit assurance but still carry certified state — check customer state
      const apiCert =
        apiBpp.bppCustomerState === "BPP_COMPARISON_CERTIFIED" ||
        apiBpp.bppMeasurementState === "BPP_COMPARISON_CERTIFIED";
      if (!apiCert) failures.push("API_NOT_COMPARISON_CERTIFIED");
    }
    if (domHasAwaitingMessage === true) {
      failures.push("DOM_AWAITING_DESPITE_CERTIFIED_UPSTREAM");
    }
    if (Array.isArray(domKpiValues)) {
      const blank = domKpiValues.filter((v) => !v || v === "—" || v === "-").length;
      if (blank > 0) failures.push(`DOM_BLANK_KPIS_${blank}`);
      const apiVals = PRIMARY_KPI_IDS.map((id) => layers.api[id] || layers.staged[id]).filter(
        Boolean
      );
      for (const v of apiVals) {
        if (v && v !== "—" && !domKpiValues.includes(v)) {
          failures.push(`DOM_MISSING_KPI_VALUE_${v}`);
        }
      }
    }
    const expectedNames = rankingNames(apiBpp || stagedBpp || sourcePackBpp);
    if (Array.isArray(domRankingNames) && expectedNames.length) {
      for (const name of expectedNames) {
        const hit = domRankingNames.some(
          (n) =>
            String(n).toLowerCase().includes(String(name).toLowerCase().slice(0, 18)) ||
            String(name).toLowerCase().includes(String(n).toLowerCase().slice(0, 18))
        );
        if (!hit) failures.push(`DOM_MISSING_PEER_${name}`);
      }
    }
  }

  // Negative: SPA readiness must accept staged comparison certification
  if (
    stagedBpp?.bppCustomerState === "BPP_COMPARISON_CERTIFIED" &&
    stagedBpp?.status === "READY" &&
    !bppComparisonCertifiedIsRenderable({
      ...stagedBpp,
      showAnalyticalScaffolding: true,
    })
  ) {
    failures.push("RENDERER_REJECTS_BPP_COMPARISON_CERTIFIED");
  }

  return {
    ok: failures.length === 0,
    contract: ADP_BPP_SOURCE_API_DOM_PARITY_V1,
    certifiedUpstream,
    failures,
    layers,
    blocksFullE2eComplete: failures.length > 0,
    blocksCustomerReady: failures.length > 0,
  };
}

export function permanentBppDeliveryParityNegativeFixtures() {
  const certified = {
    status: "READY",
    bppCustomerState: "BPP_COMPARISON_CERTIFIED",
    bppMeasurementState: "BPP_COMPARISON_CERTIFIED",
    showAnalyticalScaffolding: true,
    kpis: [
      { id: "portfolioAiPresence", value: "56.3%", available: true },
      { id: "portfolioRank", value: "#4 of 5", available: true },
      { id: "numberOneAppearance", value: "3.6%", available: true },
      { id: "top3Appearance", value: "89.3%", available: true },
    ],
    ranking: {
      rows: [
        { rank: 1, name: "Peer A" },
        { rank: 2, name: "Subject" },
      ],
    },
  };
  const blankDom = reconcileBppKpiLayers({
    stagedBpp: certified,
    apiBpp: certified,
    domKpiValues: ["—", "—", "—", "—"],
    domHasAwaitingMessage: true,
    domRankingNames: [],
  });
  const mismatch = reconcileBppKpiLayers({
    stagedBpp: certified,
    apiBpp: {
      ...certified,
      kpis: certified.kpis.map((k) =>
        k.id === "portfolioAiPresence" ? { ...k, value: "10.0%" } : k
      ),
    },
  });
  const legacyReject = !bppComparisonCertifiedIsRenderable({
    status: "READY",
    bppCustomerState: "BPP_COMPARISON_CERTIFIED",
    kpis: certified.kpis,
    ranking: certified.ranking,
    // no assuranceStatus — must still be renderable via customer state
    showAnalyticalScaffolding: true,
  });
  return [
    {
      id: "DOM_BLANK_WITH_CERTIFIED_API",
      pass: blankDom.failures.includes("DOM_AWAITING_DESPITE_CERTIFIED_UPSTREAM"),
    },
    {
      id: "API_STAGED_KPI_MISMATCH",
      pass: mismatch.failures.some((f) => f.startsWith("KPI_MISMATCH_")),
    },
    {
      id: "RENDERER_ACCEPTS_COMPARISON_CERTIFIED",
      pass: legacyReject === false,
    },
  ];
}
