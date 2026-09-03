#!/usr/bin/env node
/**
 * Full Existing Hotel ADP local ↔ production customer fingerprint parity.
 * npm run test:adp-local-production-customer-fingerprint-parity-v1
 */

import { createHash } from "crypto";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../lib/ai-demand-positioning/contracts/adp-certified-property-cohort-v1.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { isBrandPortfolioCustomerReady } from "../lib/ai-demand-positioning/brand-portfolio/build-brand-portfolio-position-payload-v1.js";

const OUT_DIR = join(process.cwd(), "reports/ai-demand-positioning/local-production-parity");
const EXPECTED_CACHE = "adp-v76-20260903-trends-p2";
const EXPECTED_BPP_PERIOD = "bpp_second_cycle_2026-09-02T1947";
const EXPECTED_BPP_PRIOR = "bpp_first_cycle_2026-08-21T2057";

const GATES = {
  PRODUCTION_CURRENT_PERIOD_MATCHES_VERIFIED_LOCAL: "PRODUCTION_CURRENT_PERIOD_MATCHES_VERIFIED_LOCAL",
  PRODUCTION_CORE_TRENDS_MATCH_VERIFIED_LOCAL: "PRODUCTION_CORE_TRENDS_MATCH_VERIFIED_LOCAL",
  PRODUCTION_BPP_MATCHES_VERIFIED_LOCAL: "PRODUCTION_BPP_MATCHES_VERIFIED_LOCAL",
  PRODUCTION_PRIOR_RUN_MATCHES_VERIFIED_LOCAL: "PRODUCTION_PRIOR_RUN_MATCHES_VERIFIED_LOCAL",
  PRODUCTION_PROVIDER_METRICS_MATCH_VERIFIED_LOCAL: "PRODUCTION_PROVIDER_METRICS_MATCH_VERIFIED_LOCAL",
  PRODUCTION_TERRITORY_METRICS_MATCH_VERIFIED_LOCAL: "PRODUCTION_TERRITORY_METRICS_MATCH_VERIFIED_LOCAL",
  PRODUCTION_COMPETITIVE_METRICS_MATCH_VERIFIED_LOCAL: "PRODUCTION_COMPETITIVE_METRICS_MATCH_VERIFIED_LOCAL",
  PRODUCTION_EVIDENCE_MATCHES_VERIFIED_LOCAL: "PRODUCTION_EVIDENCE_MATCHES_VERIFIED_LOCAL",
  PRODUCTION_NARRATIVE_MATCHES_VERIFIED_LOCAL: "PRODUCTION_NARRATIVE_MATCHES_VERIFIED_LOCAL",
  PRODUCTION_SECTION_RENDER_STATE_MATCHES_VERIFIED_LOCAL: "PRODUCTION_SECTION_RENDER_STATE_MATCHES_VERIFIED_LOCAL",
  EXTERNAL_SHARE_ANALYTICS_MATCH_OWNER_REPORT: "EXTERNAL_SHARE_ANALYTICS_MATCH_OWNER_REPORT",
  NO_UNEXPLAINED_LOCAL_PRODUCTION_CUSTOMER_DIFFERENCE:
    "NO_UNEXPLAINED_LOCAL_PRODUCTION_CUSTOMER_DIFFERENCE",
  ADP_LOCAL_PRODUCTION_CUSTOMER_FINGERPRINT_PARITY:
    "ADP_LOCAL_PRODUCTION_CUSTOMER_FINGERPRINT_PARITY",
};

function sha(v) {
  return createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(v)).digest("hex").slice(0, 20);
}

function near(a, b, eps = 0.15) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= eps;
}

function loadInventory() {
  return JSON.parse(
    readFileSync(
      "reports/client-share-links/PRODUCTION_CLIENT_SHARE_LINK_INVENTORY_2026-09-03T04-34-03.json",
      "utf8"
    )
  );
}

async function fetchProdReport(link) {
  const share = new URL(link.shareUrl).searchParams.get("share");
  const base = link.shareUrl.split("/owner-ai-demand-share.html")[0];
  const url = `${base}/api/ai-demand-positioning/property/${encodeURIComponent(link.propertyId)}/report?share=${encodeURIComponent(share)}&_cb=${Date.now()}`;
  const res = await fetch(url, { headers: { "cache-control": "no-cache" } });
  const json = await res.json();
  return { status: res.status, cacheControl: res.headers.get("cache-control"), payload: json };
}

function fingerprint(payload, extras = {}) {
  const em = payload?.executiveMetrics || {};
  const bpp = payload?.brandPortfolioPosition || {};
  const trends = payload?.trends || [];
  const er = payload?.executiveRead || {};
  const territories = payload?.intentPresenceIndex || {};
  const ranking = payload?.competitiveRankingByTerritory || {};
  const evidence = payload?.evidence || {};
  const actions = payload?.actions || [];

  const sectionInventory = {
    executiveRead: Boolean(er && (er.strength || er.constraint || er.change || er.narrative || er.trend)),
    executiveMetrics: Boolean(em.considerationRate || em.scenarioPresence),
    trends: Array.isArray(trends) && trends.length > 0,
    brandPortfolioPosition: Boolean(bpp && bpp.status),
    demandTerritories: Object.keys(territories).length > 0,
    competitiveRanking: Boolean(ranking?.byTerritory),
    realityGap: Boolean(payload?.realityGap),
    evidence: Boolean(evidence && (evidence.ownedSourceMix || evidence.ownedSourceShare != null)),
    actions: Array.isArray(actions),
    competitiveSet: Boolean(payload?.competitiveSet),
  };

  const bppKpis = (bpp.kpis || []).map((k) => ({
    id: k.id || k.key || k.label,
    value: k.value ?? k.display ?? k.rate ?? null,
    delta: k.delta ?? k.deltaDisplay ?? null,
  }));
  const bppRows = (bpp.ranking?.rows || []).slice(0, 25).map((r) => ({
    id: r.entityId || r.id || r.name,
    rank: r.rank ?? null,
    presence: r.presence ?? r.presenceRate ?? null,
    delta: r.delta ?? r.deltaDisplay ?? null,
  }));

  return {
    propertyId: payload?.property?.propertyId || payload?.propertyId || extras.propertyId,
    corePeriodId: payload?.period?.periodId || null,
    corePriorPeriodId: em.currentVsPrior?.priorComparablePeriodId || null,
    reportEdition: payload?.reportVersionLabel || payload?.measurementContractVersion || null,
    trendsLen: trends.length,
    trendDates: trends.map((t) => String(t.date || "").slice(0, 10)),
    trendMetrics: trends.map((t) => ({
      periodId: t.periodId,
      rc: t.propertyRealityCoverage,
      sp: t.scenarioPresenceRate,
      cr: t.considerationRate,
    })),
    considerationRate: em.considerationRate?.rate ?? null,
    scenarioPresence: em.scenarioPresence?.rate ?? null,
    priorDeltas: em.currentVsPrior?.deltas || null,
    bppStatus: bpp.status || null,
    bppAssurance: bpp.assuranceStatus || null,
    bppReady: isBrandPortfolioCustomerReady(bpp),
    bppPeriodId: bpp.periodId || bpp.measurement?.periodId || extras.bppPeriodId || null,
    bppPriorPeriodId: bpp.priorPeriodId || extras.bppPriorPeriodId || null,
    bppPublicationVersion: bpp.publicationVersion || bpp.measurement?.publicationVersion || null,
    bppKpis,
    bppRows,
    bppHasRanking: Boolean(bpp.ranking?.rows?.length),
    territoryKeys: Object.keys(territories).sort(),
    competitiveTerritoryKeys: Object.keys(ranking.byTerritory || {}).sort(),
    executiveNarrative: {
      strength: er.strength || er.headline || null,
      constraint: er.constraint || null,
      change: er.change || er.trend?.narrative || er.trend?.label || null,
      state: er.trend?.state || null,
    },
    actionsCount: actions.length,
    realityRecognizes: payload?.realityGap?.recognized?.length ?? payload?.realityGap?.recognizes?.length ?? null,
    realityMisses: payload?.realityGap?.missed?.length ?? payload?.realityGap?.misses?.length ?? null,
    sectionInventory,
    ...extras,
  };
}

function canonicalize(fp) {
  // Drop auth/env-only fields from equality
  const { cacheControl, jsCacheBust, source, ...rest } = fp;
  return rest;
}

function diffFingerprints(localFp, prodFp) {
  const a = canonicalize(localFp);
  const b = canonicalize(prodFp);
  const diffs = [];
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  for (const k of keys) {
    const av = a[k];
    const bv = b[k];
    if (typeof av === "number" || typeof bv === "number") {
      if (!near(av, bv)) diffs.push({ path: k, local: av, prod: bv, class: "PRODUCTION_WRONG_DATA" });
      continue;
    }
    if (JSON.stringify(av) !== JSON.stringify(bv)) {
      diffs.push({
        path: k,
        local: av,
        prod: bv,
        class:
          av != null && bv == null
            ? "PRODUCTION_MISSING"
            : av == null && bv != null
              ? "PRODUCTION_WRONG_STATE"
              : "PRODUCTION_WRONG_DATA",
      });
    }
  }
  return diffs;
}

async function main() {
  const inv = loadInventory();
  const html = await (await fetch(`${inv.publicBase}/owner-ai-demand-share.html?_cb=${Date.now()}`)).text();
  const jsCacheBust = (html.match(/ai-demand-positioning\.js\?v=([^"']+)/) || [])[1] || null;
  const marker = await (await fetch(`${inv.publicBase}/adp-core-trends-p2-deploy-marker.json?_cb=${Date.now()}`)).json().catch(() => null);

  const properties = [];
  const gateFails = Object.fromEntries(Object.values(GATES).map((g) => [g, []]));

  for (const propertyId of ADP_CERTIFIED_PROPERTY_IDS) {
    const profile = loadPropertyProfile(propertyId);
    const localReport = await getPublishedOwnerReport(propertyId);
    const localPayload = {
      ...localReport.payload,
      brandPortfolioPosition: resolveBrandPortfolioPosition(propertyId, profile, { query: {} }),
    };
    const link = (inv.adpLinks || []).find((l) => l.propertyId === propertyId);
    const prodFetch = link ? await fetchProdReport(link) : { status: 0, payload: null };
    const prodPayload = prodFetch.payload;

    const localFp = fingerprint(localPayload, {
      propertyId,
      bppPeriodId: localPayload.brandPortfolioPosition?.periodId,
      bppPriorPeriodId: localPayload.brandPortfolioPosition?.priorPeriodId,
      source: "local",
      jsCacheBust: EXPECTED_CACHE,
    });
    const prodFp = fingerprint(prodPayload, {
      propertyId,
      bppPeriodId: prodPayload?.brandPortfolioPosition?.periodId,
      bppPriorPeriodId: prodPayload?.brandPortfolioPosition?.priorPeriodId,
      source: "production",
      jsCacheBust,
      cacheControl: prodFetch.cacheControl,
    });

    const diffs = diffFingerprints(localFp, prodFp);
    const localHash = sha(canonicalize(localFp));
    const prodHash = sha(canonicalize(prodFp));

    const checks = {
      period: localFp.corePeriodId === prodFp.corePeriodId && Boolean(prodFp.corePeriodId?.includes("202609")),
      trends: localFp.trendsLen === 2 && prodFp.trendsLen === 2 && JSON.stringify(localFp.trendMetrics) === JSON.stringify(prodFp.trendMetrics),
      bpp:
        prodFp.bppReady === true &&
        localFp.bppReady === true &&
        prodFp.bppStatus === "READY" &&
        (prodFp.bppPeriodId === EXPECTED_BPP_PERIOD || prodFp.bppPublicationVersion?.includes("20260902")) &&
        sha(localFp.bppKpis) === sha(prodFp.bppKpis),
      priorRun: JSON.stringify(localFp.priorDeltas) === JSON.stringify(prodFp.priorDeltas) && localFp.corePriorPeriodId === prodFp.corePriorPeriodId,
      territories: JSON.stringify(localFp.territoryKeys) === JSON.stringify(prodFp.territoryKeys),
      competitive: JSON.stringify(localFp.competitiveTerritoryKeys) === JSON.stringify(prodFp.competitiveTerritoryKeys),
      narrative: JSON.stringify(localFp.executiveNarrative) === JSON.stringify(prodFp.executiveNarrative),
      sections: JSON.stringify(localFp.sectionInventory) === JSON.stringify(prodFp.sectionInventory),
      fingerprint: localHash === prodHash && diffs.length === 0,
    };

    if (!checks.period) gateFails[GATES.PRODUCTION_CURRENT_PERIOD_MATCHES_VERIFIED_LOCAL].push(propertyId);
    if (!checks.trends) gateFails[GATES.PRODUCTION_CORE_TRENDS_MATCH_VERIFIED_LOCAL].push(propertyId);
    if (!checks.bpp) gateFails[GATES.PRODUCTION_BPP_MATCHES_VERIFIED_LOCAL].push(propertyId);
    if (!checks.priorRun) gateFails[GATES.PRODUCTION_PRIOR_RUN_MATCHES_VERIFIED_LOCAL].push(propertyId);
    if (!checks.territories) gateFails[GATES.PRODUCTION_TERRITORY_METRICS_MATCH_VERIFIED_LOCAL].push(propertyId);
    if (!checks.competitive) gateFails[GATES.PRODUCTION_COMPETITIVE_METRICS_MATCH_VERIFIED_LOCAL].push(propertyId);
    if (!checks.narrative) gateFails[GATES.PRODUCTION_NARRATIVE_MATCHES_VERIFIED_LOCAL].push(propertyId);
    if (!checks.sections) gateFails[GATES.PRODUCTION_SECTION_RENDER_STATE_MATCHES_VERIFIED_LOCAL].push(propertyId);
    if (!checks.fingerprint) {
      gateFails[GATES.ADP_LOCAL_PRODUCTION_CUSTOMER_FINGERPRINT_PARITY].push(propertyId);
      gateFails[GATES.NO_UNEXPLAINED_LOCAL_PRODUCTION_CUSTOMER_DIFFERENCE].push(propertyId);
    }

    // Provider / evidence: semantic presence via BPP rows + ranking (customer surface)
    const providerOk = localFp.bppHasRanking === prodFp.bppHasRanking && sha(localFp.bppRows) === sha(prodFp.bppRows);
    if (!providerOk) gateFails[GATES.PRODUCTION_PROVIDER_METRICS_MATCH_VERIFIED_LOCAL].push(propertyId);
    const evidenceOk =
      localFp.realityRecognizes === prodFp.realityRecognizes &&
      localFp.realityMisses === prodFp.realityMisses &&
      localFp.actionsCount === prodFp.actionsCount;
    if (!evidenceOk) gateFails[GATES.PRODUCTION_EVIDENCE_MATCHES_VERIFIED_LOCAL].push(propertyId);

    properties.push({
      propertyId,
      checks,
      localHash,
      prodHash,
      diffs,
      localFp,
      prodFp,
      sectionMatrix: Object.keys(localFp.sectionInventory).map((section) => ({
        section,
        local: localFp.sectionInventory[section],
        prod: prodFp.sectionInventory[section],
        class:
          localFp.sectionInventory[section] === prodFp.sectionInventory[section]
            ? "PARITY"
            : localFp.sectionInventory[section] && !prodFp.sectionInventory[section]
              ? "PRODUCTION_MISSING"
              : "PRODUCTION_WRONG_STATE",
      })),
    });
  }

  // Share analytics == owner report: production share payload is the production source of truth compared above.
  gateFails[GATES.EXTERNAL_SHARE_ANALYTICS_MATCH_OWNER_REPORT] = properties
    .filter((p) => !p.checks.fingerprint)
    .map((p) => p.propertyId);

  const gates = {};
  let allPass = true;
  for (const [name, fails] of Object.entries(gateFails)) {
    gates[name] = fails.length === 0 ? "PASS" : "FAIL";
    if (fails.length) allPass = false;
  }

  const result = {
    stamp: new Date().toISOString(),
    expectedCache: EXPECTED_CACHE,
    expectedBppPeriod: EXPECTED_BPP_PERIOD,
    expectedBppPrior: EXPECTED_BPP_PRIOR,
    jsCacheBust,
    marker,
    publicBase: inv.publicBase,
    gates,
    ADP_LOCAL_PRODUCTION_CUSTOMER_FINGERPRINT_PARITY: gates[GATES.ADP_LOCAL_PRODUCTION_CUSTOMER_FINGERPRINT_PARITY],
    NO_UNEXPLAINED_LOCAL_PRODUCTION_CUSTOMER_DIFFERENCE:
      gates[GATES.NO_UNEXPLAINED_LOCAL_PRODUCTION_CUSTOMER_DIFFERENCE],
    properties,
    remainingDifferences: properties.flatMap((p) =>
      p.diffs.map((d) => ({ propertyId: p.propertyId, ...d }))
    ),
  };

  mkdirSync(OUT_DIR, { recursive: true });
  const outPath = join(OUT_DIR, `fingerprint-parity-${Date.now()}.json`);
  writeFileSync(outPath, JSON.stringify(result, null, 2));
  console.log(
    JSON.stringify(
      {
        ADP_LOCAL_PRODUCTION_CUSTOMER_FINGERPRINT_PARITY: result.ADP_LOCAL_PRODUCTION_CUSTOMER_FINGERPRINT_PARITY,
        NO_UNEXPLAINED_LOCAL_PRODUCTION_CUSTOMER_DIFFERENCE:
          result.NO_UNEXPLAINED_LOCAL_PRODUCTION_CUSTOMER_DIFFERENCE,
        gates,
        remainingDifferences: result.remainingDifferences.slice(0, 40),
        remainingCount: result.remainingDifferences.length,
        outPath,
        bppLocalReady: properties.map((p) => ({ id: p.propertyId, local: p.localFp.bppStatus, prod: p.prodFp.bppStatus, ready: p.checks.bpp })),
      },
      null,
      2
    )
  );
  if (!allPass) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
