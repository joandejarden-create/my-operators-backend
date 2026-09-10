/**
 * Build ADP_CUSTOMER_OUTPUT_FINGERPRINT_V1 + production baseline rows.
 */

import { createHash } from "crypto";
import { createRequire } from "module";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  ADP_CUSTOMER_OUTPUT_FINGERPRINT_V1,
  ADP_PRODUCTION_OUTPUT_BASELINE_V1,
  ADP_PRODUCTION_RELEASE_MANIFEST_V1,
  ADP_ATOMIC_CUSTOMER_RELEASE_BUNDLE,
  RENDERER_ASSET_VERSION,
  COMPOSITION_CONTRACT_VERSION,
  TERRITORY_CONTRACT_VERSION,
  EVIDENCE_CONTRACT_VERSION,
  FOUNDATION_PROPERTY_IDS,
} from "./adp-production-output-baseline-v1.js";
import { listPublishedPropertyIds, loadPublishedManifest, loadPublishedReport } from "../published-snapshot.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import { getBppHierarchyExhaustion } from "../brand-portfolio/bpp-hierarchy-exhaustion-ledger-v1.js";
import { BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE } from "../brand-portfolio/bpp-publication-meta-v1.js";

const require = createRequire(import.meta.url);

function sha(s) {
  return createHash("sha256").update(String(s || "")).digest("hex");
}

function stableStringify(value) {
  if (value == null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(",")}}`;
}

function unwrapPayload(report) {
  if (!report) return null;
  if (report.payload && typeof report.payload === "object") return report.payload;
  return report;
}

function executiveReadHash(er) {
  if (!er) return null;
  const v3 = er.compositionVersion === "ADP_EXECUTIVE_READ_COMPOSITION_V3" && er.sections
    ? er
    : er.compositionV3?.ok
      ? er.compositionV3
      : null;
  if (v3?.sections) {
    return sha(
      stableStringify({
        compositionVersion: "ADP_EXECUTIVE_READ_COMPOSITION_V3",
        sections: v3.sections,
        compositionHash: v3.compositionHash || null,
      })
    );
  }
  return sha(stableStringify({ writeup: er.writeup || null, narrative: er.narrative || null }));
}

function territoryHash(ipi) {
  if (!ipi || typeof ipi !== "object") return null;
  const rows = {};
  for (const [intent, row] of Object.entries(ipi)) {
    rows[intent] = {
      subjectRatePct: row?.subjectRatePct ?? row?.myRate ?? null,
      coreBenchmarkRatePct: row?.coreBenchmarkRatePct ?? null,
      index: row?.index ?? null,
      status: row?.status ?? null,
    };
  }
  return sha(stableStringify(rows));
}

function bppHash(propertyId, reportPayload) {
  const peerSet = getBrandPortfolioPeerSet(propertyId);
  const exhaustion = getBppHierarchyExhaustion(propertyId);
  const bpp = reportPayload?.brandPortfolioPosition || null;
  return sha(
    stableStringify({
      peerCount: peerSet?.peerCountExcludingSubject ?? null,
      adequacy: peerSet?.adequacy?.status ?? null,
      suppressed: Boolean(exhaustion),
      packStatus: bpp?.status || null,
      packClass: bpp?.remediation?.class || null,
    })
  );
}

function evidenceHash(reportPayload) {
  const ev = reportPayload?.evidence || reportPayload?.lostDemand || null;
  return sha(stableStringify(ev));
}

export function buildPropertyOutputFingerprint(propertyId, opts = {}) {
  const manifest = loadPublishedManifest(propertyId);
  const report = loadPublishedReport(propertyId);
  const payload = unwrapPayload(report);
  const er = payload?.executiveRead || report?.executiveRead || {};
  const ipi = payload?.intentPresenceIndex || {};
  const periodId = manifest?.latestPeriodId || payload?.period?.periodId || report?.periodId || null;
  const editionId =
    report?.executiveReadEditionId ||
    manifest?.executiveReadEditionId ||
    er?.compositionHash ||
    null;
  const compositionVersion =
    er.compositionVersion ||
    (er.compositionV3?.ok ? "ADP_EXECUTIVE_READ_COMPOSITION_V3" : er.compositionVersionPreview || null);
  const erHash = executiveReadHash(er);
  const tHash = territoryHash(ipi);
  const bHash = bppHash(propertyId, payload);
  const eHash = evidenceHash(payload);
  const analyticsHash = sha(
    stableStringify({
      realityGap: payload?.realityGap || null,
      demandCapture: payload?.demandCapture || null,
      executiveMetrics: payload?.executiveMetrics || null,
    })
  );
  const fingerprint = sha(
    [
      propertyId,
      periodId,
      editionId,
      analyticsHash,
      erHash,
      tHash,
      bHash,
      eHash,
      opts.rendererContractVersion || RENDERER_ASSET_VERSION,
    ].join("|")
  );

  return {
    object: ADP_CUSTOMER_OUTPUT_FINGERPRINT_V1,
    propertyId,
    periodId,
    editionId,
    snapshotHash: sha(stableStringify({ propertyId, periodId, reportFile: manifest?.reportFile })),
    compositionVersion,
    hasCompositionV3: Boolean(er.compositionV3?.ok && er.compositionV3?.sections),
    executiveReadHash: erHash,
    territoryMetricHash: tHash,
    bppHash: bHash,
    evidenceIndexHash: eHash,
    analyticsHash,
    rendererAssetVersion: opts.rendererContractVersion || RENDERER_ASSET_VERSION,
    printRendererVersion: opts.rendererContractVersion || RENDERER_ASSET_VERSION,
    fingerprint,
  };
}

export function buildProductionOutputBaselineV1(opts = {}) {
  const propertyIds = opts.propertyIds || listPublishedPropertyIds();
  const rows = propertyIds.map((id) => buildPropertyOutputFingerprint(id, opts));
  let bppPackVersion = null;
  try {
    const pack = require(`../../../${BPP_CUSTOMER_PUBLISHED_PACK_DEPLOYABLE}`);
    bppPackVersion = pack.publicationVersion || null;
  } catch {
    bppPackVersion = null;
  }
  return {
    object: ADP_PRODUCTION_OUTPUT_BASELINE_V1,
    version: "v1_20260910",
    createdAt: new Date().toISOString(),
    rendererAssetVersion: RENDERER_ASSET_VERSION,
    compositionContractVersion: COMPOSITION_CONTRACT_VERSION,
    territoryContractVersion: TERRITORY_CONTRACT_VERSION,
    evidenceContractVersion: EVIDENCE_CONTRACT_VERSION,
    bppPackVersion,
    publishedUniverseCount: rows.length,
    founderSeven: FOUNDATION_PROPERTY_IDS,
    rows,
    baselineHash: sha(stableStringify(rows.map((r) => ({ id: r.propertyId, fp: r.fingerprint })))),
    doctrine: [
      "APPROVED_CUSTOMER_OUTPUT_IS_A_VERSIONED_PRODUCTION CONTRACT",
      "REPORT_SNAPSHOT_VERSION_RENDERER_VERSION_DATA_CONTRACT_MUST_MOVE_TOGETHER",
      "SAME_PUBLISHED_SNAPSHOT_SAME_RENDERER_SAME_CUSTOMER_OUTPUT",
    ],
  };
}

export function classifyFingerprintDelta(priorFp, nextFp, declaredClass) {
  if (!priorFp || priorFp === nextFp) {
    return { changed: false, class: null, hardStop: false };
  }
  const cls = declaredClass || FINGERPRINT_CHANGE_CLASS_UNEXPECTED();
  return {
    changed: true,
    class: cls,
    hardStop: cls === "UNEXPECTED_REGRESSION",
  };
}

function FINGERPRINT_CHANGE_CLASS_UNEXPECTED() {
  return "UNEXPECTED_REGRESSION";
}

export function buildReleaseManifestV1({ gitCommit = null, deployTimestamp = null } = {}) {
  const baseline = buildProductionOutputBaselineV1();
  return {
    object: ADP_PRODUCTION_RELEASE_MANIFEST_V1,
    gitCommit,
    deployTimestamp: deployTimestamp || new Date().toISOString(),
    rendererAssetVersion: baseline.rendererAssetVersion,
    reportSnapshotSetHash: baseline.baselineHash,
    bppPackVersion: baseline.bppPackVersion,
    compositionContractVersion: baseline.compositionContractVersion,
    evidenceContractVersion: baseline.evidenceContractVersion,
    territoryContractVersion: baseline.territoryContractVersion,
    publishedUniverseCount: baseline.publishedUniverseCount,
    atomicBundle: ADP_ATOMIC_CUSTOMER_RELEASE_BUNDLE,
    requiredBundleParts: [
      "published_snapshots",
      "current_edition_indexes",
      "bpp_pack",
      "frontend_js",
      "frontend_css",
      "share_renderer",
      "print_renderer",
      "version_cache_bust",
      "release_manifest",
    ],
    baselineVersion: baseline.version,
    baselineHash: baseline.baselineHash,
  };
}
