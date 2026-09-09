#!/usr/bin/env node
/**
 * ADP BPP full-universe remediation V1
 * Populate-before-suppress across all published Existing Hotel ADP properties.
 *
 *   node scripts/run-adp-bpp-full-universe-remediation-v1.mjs
 *   node scripts/run-adp-bpp-full-universe-remediation-v1.mjs --apply
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * No new provider calls — observation reuse only.
 */

import "../load-env.js";
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { listPublishedPropertyIds, loadPublishedManifest, loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { loadPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { getPortfolioMapping } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-affiliation-mapping-v1.js";
import { getBrandPortfolioPeerSet } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-peer-set-v1.js";
import { getBppHierarchyExhaustion } from "../lib/ai-demand-positioning/brand-portfolio/bpp-hierarchy-exhaustion-ledger-v1.js";
import { computeBrandPortfolioMetricsV1 } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-first-cycle-metrics-v1.js";
import {
  buildBrandPortfolioPositionPayload,
  BRAND_PORTFOLIO_STATUS,
  isBrandPortfolioCustomerReady,
} from "../lib/ai-demand-positioning/brand-portfolio/build-brand-portfolio-position-payload-v1.js";
import {
  resolveBppPopulationAttempt,
  evaluateBppClientReadinessGate,
  customerFacingBppCopyIsClean,
  FORBIDDEN_CUSTOMER_PHRASES,
  BPP_CLIENT_READY_CLASS,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-client-readiness-doctrine-v1.js";
import {
  BPP_CUSTOMER_PUBLISHED_PACK,
  BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE,
  BPP_ASSET_CACHE_TOKEN,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import { loadCustomerPublishedBrandPortfolio } from "../api/ai-demand-positioning.js";

const APPLY = process.argv.includes("--apply");
const OUT_DIR = join(process.cwd(), "reports/ai-demand-positioning");
const CORRECTION_TAG = `bpp_universe_remediation_v1_${new Date().toISOString().replace(/[:.]/g, "").slice(0, 15)}`;

const POPULATE_IDS = ["adp_st_regis_mexico_city", "adp_jw_marriott_santo_domingo"];
const SUPPRESS_IDS = [
  "adp_st_regis_cap_cana",
  "adp_jw_marriott_monterrey_valle",
  "adp_westin_monterrey_valle",
  "adp_radisson_santo_domingo",
  "adp_faranda_collection_bogota",
  "adp_hotel_caribe_faranda_grand",
];

function sha(s) {
  return createHash("sha256").update(String(s || "")).digest("hex");
}

function loadPeriodObservations(propertyId, periodId) {
  // Prefer runtime period file; fall back to published evidence sidecar if needed
  try {
    const period = loadPeriod(periodId);
    if (period?.observations?.length) return period;
  } catch {
    /* continue */
  }
  const runtimePath = join(
    process.cwd(),
    "data/ai-demand-positioning/runtime",
    `${periodId}.json`
  );
  if (existsSync(runtimePath)) {
    return JSON.parse(readFileSync(runtimePath, "utf8"));
  }
  return null;
}

function buildReadyPayload(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const mapping = getPortfolioMapping(propertyId);
  const peerSet = getBrandPortfolioPeerSet(propertyId);
  const manifest = loadPublishedManifest(propertyId);
  const periodId = manifest?.latestPeriodId;
  if (!profile || !mapping || !peerSet || !periodId) {
    return { ok: false, propertyId, reason: "MISSING_PROFILE_MAPPING_PEERSET_OR_PERIOD" };
  }
  const period = loadPeriodObservations(propertyId, periodId);
  const observations = (period?.observations || []).filter((o) => !o?.error && o?.rawResponse);
  if (!observations.length) {
    return { ok: false, propertyId, reason: "NO_REUSABLE_OBSERVATIONS", periodId };
  }
  const lens = {
    lensId: mapping.defaultLensId,
    label: (mapping.lenses || []).find((l) => l.lensId === mapping.defaultLensId)?.label || mapping.defaultLensId,
  };
  const scenarios = buildScenarioUniverse(profile);
  const metrics = computeBrandPortfolioMetricsV1({
    profile,
    peerSet,
    scenarios,
    observations,
    lens,
  });
  const bppPayload = buildBrandPortfolioPositionPayload(profile, {
    certifiedMeasurement: true,
    kpis: metrics.kpis,
    ranking: { rows: metrics.tableRows, comparisonMode: "PUBLICATION_TIME" },
    hasPriorPeriod: false,
  });
  bppPayload.periodId = periodId;
  bppPayload.customerPublished = true;
  bppPayload.assuranceStatus = "CUSTOMER_READY";
  bppPayload.showAnalyticalScaffolding = true;
  bppPayload.comparisonMode = "PUBLICATION_TIME";
  bppPayload.hasPriorPeriod = false;
  bppPayload.measurement = {
    kpiContractVersion: metrics.kpiContractVersion,
    metricsVersion: metrics.metricsVersion,
    grain: "PROVIDER_OBSERVATION",
    periodId,
    priorPeriodId: null,
    comparisonMode: "PUBLICATION_TIME",
    publicationVersion: "bpp-customer-v1.1-20260902-p2",
    customerPublished: true,
    compatibleObservationReuse: "PERIOD_CORE_OBSERVATIONS_REUSED_FOR_PEER_RANKING",
  };
  bppPayload.peerSet = {
    peerSetId: metrics.peerSetId,
    peerSetVersion: metrics.peerSetVersion,
    adequacy: peerSet.adequacy?.status || null,
    peerCount: peerSet.peerCountExcludingSubject,
  };
  bppPayload.remediation = {
    tag: CORRECTION_TAG,
    class: BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED,
    newProviderCalls: 0,
  };
  return {
    ok: true,
    propertyId,
    periodId,
    observationCount: observations.length,
    bppPayload,
    metricsSummary: {
      kpiCount: metrics.kpis?.length || 0,
      rowCount: metrics.tableRows?.length || 0,
      adequacy: peerSet.adequacy?.status || null,
    },
  };
}

function buildSuppressedPayload(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const exhaustion = getBppHierarchyExhaustion(propertyId);
  if (!profile || !exhaustion) {
    return { ok: false, propertyId, reason: "MISSING_PROFILE_OR_EXHAUSTION_LEDGER" };
  }
  const bppPayload = buildBrandPortfolioPositionPayload(profile, {
    forceStatus: BRAND_PORTFOLIO_STATUS.EXCEPTION_SUPPRESSED,
  });
  bppPayload.customerPublished = true;
  bppPayload.assuranceStatus = "EXCEPTION_SUPPRESSED";
  bppPayload.showAnalyticalScaffolding = false;
  bppPayload.suppressionReason = exhaustion.suppressionReason;
  bppPayload.hierarchyExhaustion = {
    run: true,
    hierarchyExhausted: true,
    levelsAttempted: exhaustion.levelsAttempted,
    candidateCount: exhaustion.candidateCount,
    certifiedPeerCount: exhaustion.certifiedPeerCount,
    sourceRegistry: exhaustion.sourceRegistry,
  };
  bppPayload.remediation = {
    tag: CORRECTION_TAG,
    class: BPP_CLIENT_READY_CLASS.BPP_EXCEPTION_SUPPRESSED,
    newProviderCalls: 0,
  };
  return { ok: true, propertyId, bppPayload, exhaustion };
}

function scrubForbiddenInObject(node, hits = []) {
  if (node == null) return hits;
  if (typeof node === "string") {
    for (const p of FORBIDDEN_CUSTOMER_PHRASES) {
      if (node.toLowerCase().includes(p.toLowerCase())) hits.push(p);
    }
    return hits;
  }
  if (Array.isArray(node)) {
    for (const x of node) scrubForbiddenInObject(x, hits);
    return hits;
  }
  if (typeof node === "object") {
    for (const [k, v] of Object.entries(node)) {
      if (typeof v === "string") {
        for (const p of FORBIDDEN_CUSTOMER_PHRASES) {
          if (v.toLowerCase().includes(p.toLowerCase())) {
            hits.push(`${k}:${p}`);
            // Replace known stale setup copy with governed suppression language
            if (/portfolio monitoring not yet available/i.test(v) || /affiliation lens/i.test(v)) {
              if (/statusLabel|headline/i.test(k)) {
                node[k] = "Brand & Portfolio not shown";
              } else if (/body|emptyMessage|message/i.test(k)) {
                node[k] =
                  "Brand & Portfolio benchmarking is not shown for this period because the governed peer set does not yet meet the minimum comparability requirement.";
              }
            }
          }
        }
      } else {
        scrubForbiddenInObject(v, hits);
      }
    }
  }
  return hits;
}

function writePackPayloads(updates) {
  const written = [];
  for (const rel of [BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE, BPP_CUSTOMER_PUBLISHED_PACK]) {
    const abs = join(process.cwd(), rel);
    if (!existsSync(abs)) continue;
    const pack = JSON.parse(readFileSync(abs, "utf8"));
    pack.payloads = pack.payloads || {};
    for (const { propertyId, bppPayload } of updates) {
      pack.payloads[propertyId] = bppPayload;
    }
    pack.payloadHash = sha(JSON.stringify(pack.payloads));
    pack.publishedAt = new Date().toISOString();
    pack.assetCacheToken = BPP_ASSET_CACHE_TOKEN;
    pack.universeRemediationNote =
      "Full-universe BPP remediation V1: populate Mexico City + JW Santo Domingo; suppress Cap Cana / Monterrey / Choice thin markets after hierarchy exhaustion; scrub forbidden setup copy.";
    if (APPLY) writeFileSync(abs, JSON.stringify(pack, null, 2) + "\n");
    written.push({
      path: rel,
      propertyCount: Object.keys(pack.payloads).length,
      payloadHash: pack.payloadHash,
      applied: APPLY,
    });
  }
  return written;
}

function stampPublishedReport(propertyId, bppPayload) {
  const manifest = loadPublishedManifest(propertyId);
  const report = loadPublishedReport(propertyId);
  if (!manifest || !report) return { propertyId, ok: false, reason: "MISSING_PUBLISHED" };
  const dir = join(process.cwd(), "data/ai-demand-positioning/published", propertyId);
  const periodId = manifest.latestPeriodId;
  const reportFile = manifest.reportFile || `report-${periodId}.json`;
  const next = {
    ...report,
    brandPortfolioPosition: bppPayload,
    bppCorrection: {
      tag: CORRECTION_TAG,
      correctedAt: new Date().toISOString(),
      priorBppStatus: report.brandPortfolioPosition?.status || null,
      class: bppPayload.remediation?.class || bppPayload.status,
      immutableEditionNote:
        "Corrected current published snapshot BPP state — history preserved via bppCorrection lineage; current_published shares resolve this edition.",
    },
  };
  const forbiddenBefore = scrubForbiddenInObject(JSON.parse(JSON.stringify(report.brandPortfolioPosition || {})));
  scrubForbiddenInObject(next);
  if (APPLY) {
    writeFileSync(join(dir, reportFile), JSON.stringify(next, null, 2) + "\n");
    const nextManifest = {
      ...manifest,
      bppClientReadyClass: bppPayload.remediation?.class || bppPayload.status,
      bppStatus: bppPayload.status,
      bppCorrectionTag: CORRECTION_TAG,
      latestPublicationCompletedAt: new Date().toISOString(),
    };
    writeFileSync(join(dir, "manifest.json"), JSON.stringify(nextManifest, null, 2) + "\n");
  }
  return {
    propertyId,
    ok: true,
    periodId,
    reportFile,
    bppStatus: bppPayload.status,
    forbiddenScrubbed: forbiddenBefore,
    applied: APPLY,
  };
}

function main() {
  const populateResults = POPULATE_IDS.map(buildReadyPayload);
  const suppressResults = SUPPRESS_IDS.map(buildSuppressedPayload);
  const updates = [];
  for (const r of [...populateResults, ...suppressResults]) {
    if (r.ok && r.bppPayload) updates.push({ propertyId: r.propertyId, bppPayload: r.bppPayload });
  }

  const packWrites = writePackPayloads(updates);
  const stamped = updates.map(({ propertyId, bppPayload }) => stampPublishedReport(propertyId, bppPayload));

  // Scrub remaining published snapshots that still embed forbidden setup copy
  const scrubUniverse = [];
  for (const propertyId of listPublishedPropertyIds()) {
    const report = loadPublishedReport(propertyId);
    if (!report?.brandPortfolioPosition) continue;
    const clone = JSON.parse(JSON.stringify(report));
    const hits = scrubForbiddenInObject(clone.brandPortfolioPosition);
    if (!hits.length) continue;
    if (APPLY) {
      const manifest = loadPublishedManifest(propertyId);
      const dir = join(process.cwd(), "data/ai-demand-positioning/published", propertyId);
      const reportFile = manifest.reportFile;
      writeFileSync(join(dir, reportFile), JSON.stringify(clone, null, 2) + "\n");
    }
    scrubUniverse.push({ propertyId, hits, applied: APPLY });
  }

  // Post-state classification (uses updated pack when apply; otherwise planned payloads)
  const rows = [];
  for (const propertyId of listPublishedPropertyIds()) {
    const profile = loadPropertyProfile(propertyId);
    const planned = updates.find((u) => u.propertyId === propertyId)?.bppPayload;
    const live = APPLY ? loadCustomerPublishedBrandPortfolio(propertyId) : planned || loadCustomerPublishedBrandPortfolio(propertyId);
    const attempt = resolveBppPopulationAttempt({
      propertyId,
      profile,
      bppPayload: live,
    });
    const gate = evaluateBppClientReadinessGate(attempt);
    rows.push({
      propertyId,
      displayName: profile?.name || propertyId,
      brand: profile?.brand || null,
      parent: profile?.parentCompany || null,
      clientReadyClass: attempt.clientReadyClass,
      clientReady: gate.clientReady,
      bppStatus: live?.status || attempt.bppStatus,
      copyClean: customerFacingBppCopyIsClean(JSON.stringify(live || {})),
      peerCount: getBrandPortfolioPeerSet(propertyId)?.peerCountExcludingSubject ?? 0,
      hierarchyExhausted: attempt.discovery?.hierarchyExhausted === true,
      suppressionReason: attempt.suppressionReason || null,
    });
  }

  const summary = {
    BPP_READY_POPULATED: rows.filter((r) => r.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED).length,
    BPP_EXCEPTION_SUPPRESSED: rows.filter((r) => r.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_EXCEPTION_SUPPRESSED).length,
    BPP_INVALID_INCOMPLETE: rows.filter((r) => r.clientReady !== "YES").length,
    newProviderCalls: 0,
    estimatedCostUsd: 0,
    apply: APPLY,
  };

  const report = {
    title: "ADP_BPP_FULL_UNIVERSE_REMEDIATION_V1",
    doctrine: [
      "METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.",
      "BPP_POPULATION_IS_EXPECTED_FOR_AFFILIATED_HOTELS",
      "ADP_BPP_POPULATE_BEFORE_SUPPRESS",
      "ADP_NO_INTERNAL_CONFIGURATION_LANGUAGE_CUSTOMER_FACING",
    ],
    methodologyChanged: false,
    correctionTag: CORRECTION_TAG,
    populateResults,
    suppressResults,
    packWrites,
    stamped,
    scrubUniverse,
    rows,
    summary,
    placeholderBefore: [
      "adp_faranda_collection_bogota",
      "adp_hotel_caribe_faranda_grand",
      "adp_jw_marriott_monterrey_valle",
      "adp_jw_marriott_santo_domingo",
      "adp_radisson_santo_domingo",
      "adp_st_regis_cap_cana",
      "adp_st_regis_mexico_city",
      "adp_westin_monterrey_valle",
      "adp_waterstone_boca_raton (baked forbidden copy; pack READY overlays)",
    ],
  };

  mkdirSync(OUT_DIR, { recursive: true });
  const outPath = join(OUT_DIR, `adp-bpp-full-universe-remediation-v1-latest.json`);
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ outPath, summary, populateOk: populateResults.filter((r) => r.ok).length, suppressOk: suppressResults.filter((r) => r.ok).length }, null, 2));
  if (summary.BPP_INVALID_INCOMPLETE > 0 && APPLY) process.exitCode = 1;
}

main();
