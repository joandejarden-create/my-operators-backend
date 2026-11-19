/**
 * Extract and attach ADP methodology provenance from published/durable payloads
 * and monthly-review archive metadata.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  ADP_METHODOLOGY_CANONICAL_V1,
  ADP_METHODOLOGY_FINGERPRINT_CURRENT,
  buildAdminMethodologyProvenanceBlock,
} from "./adp-methodology-canonical-v1.js";

export { buildAdminMethodologyProvenanceBlock };
import { loadPublishedManifest, loadPublishedReport } from "../published-snapshot.js";
import { assessOwnerAppReportArtifacts } from "../share/adp-owner-app-admin-inventory-v1.js";
// Intentionally do not import archive-store here (archive-store attaches provenance on save).

function readJsonSafe(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Durable CURRENT snapshots wrap the analytic report under `.payload` and stamp
 * methodology fields on the outer envelope. Published snapshots may be flat.
 */
export function unwrapAdpReportLayers(raw) {
  if (!raw || typeof raw !== "object") {
    return { envelope: null, report: null };
  }
  const envelope = raw;
  let report = raw;
  if (raw.payload && typeof raw.payload === "object") {
    report = raw.payload;
  } else if (raw.report && typeof raw.report === "object") {
    report =
      raw.report.payload && typeof raw.report.payload === "object"
        ? raw.report.payload
        : raw.report;
  }
  return { envelope, report };
}

function firstPresenceMethodology(report) {
  const index = report?.intentPresenceIndex;
  if (!index || typeof index !== "object") return null;
  for (const v of Object.values(index)) {
    if (v && typeof v === "object" && v.methodology) return String(v.methodology);
  }
  return null;
}

function loadDurableReportPayload(propertyId, cwd = process.cwd()) {
  const art = assessOwnerAppReportArtifacts(propertyId, { cwd });
  const currentPath = join(
    cwd,
    "data/adp-durable-published/current",
    propertyId,
    "manifest.json"
  );
  const current = existsSync(currentPath) ? readJsonSafe(currentPath) : null;
  if (current?.reportFile) {
    const snapPath = join(
      cwd,
      "data/adp-durable-published/snapshots",
      propertyId,
      current.reportFile
    );
    if (existsSync(snapPath)) {
      return {
        payload: readJsonSafe(snapPath),
        periodId: current.latestPeriodId || current.periodId || art.durablePeriodId,
        source: "durable_snapshot",
        publishedAt: current.latestPublishedAt || null,
        sourceStagedVersionId: current.sourceStagedVersionId || null,
      };
    }
  }
  // Fallback via durable store assessment path (same CURRENT pointer).
  if (art.durablePeriodId && art.hasDurableSnapshotReport) {
    const snapPath = join(
      cwd,
      "data/adp-durable-published/snapshots",
      propertyId,
      `report-${art.durablePeriodId}.json`
    );
    if (existsSync(snapPath)) {
      return {
        payload: readJsonSafe(snapPath),
        periodId: art.durablePeriodId,
        source: "durable_snapshot",
        publishedAt: null,
        sourceStagedVersionId: art.sourceStagedVersionId || null,
      };
    }
  }
  const published = loadPublishedReport(propertyId);
  if (published) {
    return {
      payload: published.report || published.payload || published,
      periodId:
        published.periodId ||
        published.period?.periodId ||
        loadPublishedManifest(propertyId)?.latestPeriodId ||
        art.durablePeriodId,
      source: "published_snapshot",
      publishedAt: published.publishedAt || null,
      sourceStagedVersionId: null,
    };
  }
  return {
    payload: null,
    periodId: art.durablePeriodId || null,
    source: "none",
    publishedAt: null,
    sourceStagedVersionId: null,
  };
}

/**
 * @param {object|null} payload
 * @returns {object}
 */
export function extractMethodologyProvenanceFromPayload(payload) {
  const { envelope, report } = unwrapAdpReportLayers(payload);
  if (!report && !envelope) {
    return {
      productVersion: null,
      measurementContractVersion: null,
      appearanceMethodologyId: null,
      realityGapMethodologyId: null,
      presenceIndexMethodologyId: null,
      executiveCompositionVersion: null,
      bppMetricsVersion: null,
      reportTemplateVersion: null,
      reportContentVersion: null,
      presentationBundleVersion: null,
      hasDualRealityGapScores: false,
      realityGapRecognizedCount: null,
      realityGapTotalAttributes: null,
      scenarioTaxonomyHint: null,
    };
  }

  const env = envelope || {};
  const p = report || {};
  const rg = p.realityGap || env.realityGap || {};
  const em = p.executiveMetrics || {};
  const er = p.executiveRead || env.executiveRead || {};
  const consideration =
    em.considerationRate ||
    em.consideration ||
    em.aiConsideration ||
    {};
  const presence =
    em.governedPresenceIndex ||
    em.presenceIndex ||
    p.governedPresenceIndex ||
    {};
  const bpp = p.brandPortfolio || p.bpp || env.brandPortfolio || {};

  const appearanceMethodologyId =
    env.methodologyId ||
    consideration.methodologyId ||
    p.appearanceMethodologyId ||
    p.methodologyId ||
    p._certification?.appearanceMethodologyId ||
    null;

  const presenceIndexMethodologyId =
    presence.methodologyId ||
    presence.indexMethodologyId ||
    firstPresenceMethodology(p) ||
    p._certification?.presenceIndexMethodologyId ||
    null;

  return {
    productVersion:
      env.productVersion || p.productVersion || p.version || null,
    measurementContractVersion:
      env.measurementContractVersion || p.measurementContractVersion || null,
    appearanceMethodologyId,
    realityGapMethodologyId: rg.methodologyId || null,
    presenceIndexMethodologyId,
    executiveCompositionVersion: er.compositionVersion || null,
    bppMetricsVersion:
      bpp.metricsVersion || bpp.kpiVersion || bpp.publicationPackId || null,
    reportTemplateVersion:
      env.reportTemplateVersion || p.reportTemplateVersion || null,
    reportContentVersion:
      env.reportContentVersion || p.reportContentVersion || null,
    presentationBundleVersion:
      env.presentationBundleVersion || p.presentationBundleVersion || null,
    hasDualRealityGapScores:
      rg.gapScoreBinaryV1 != null || rg.gapScoreWeightedV2 != null,
    realityGapRecognizedCount: rg.recognizedCount ?? null,
    realityGapTotalAttributes: rg.totalAttributes ?? null,
    scenarioTaxonomyHint:
      Array.isArray(p.scenarios) && p.scenarios.length
        ? `scenarios_${p.scenarios.length}`
        : p.intentPresenceIndex
          ? `intentPresenceIndex_${Object.keys(p.intentPresenceIndex).length}`
          : null,
  };
}

/**
 * Full provenance pack for one property (research + review layers).
 */
export function resolveAdpMethodologyProvenance(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const loaded = loadDurableReportPayload(propertyId, cwd);
  const extracted = extractMethodologyProvenanceFromPayload(loaded.payload);
  const art = assessOwnerAppReportArtifacts(propertyId, { cwd });
  const reviewMeta = opts.reviewMeta || null;

  return {
    gate: ADP_METHODOLOGY_CANONICAL_V1,
    propertyId,
    researchPeriodId: loaded.periodId,
    researchPublishedAt: loaded.publishedAt,
    evidenceSource: loaded.source,
    sourceStagedVersionId: loaded.sourceStagedVersionId || null,
    hasDurableSnapshotReport: art.hasDurableSnapshotReport === true,
    hasCurrentPointer: art.hasCurrentPointer === true,
    evidenceItemCount: art.evidenceItemCount || 0,
    ...extracted,
    performanceReview: reviewMeta
      ? {
          reviewId: reviewMeta.reviewId || opts.reviewId || null,
          builderVersion: reviewMeta.builderVersion || null,
          reviewSchemaVersion: reviewMeta.reviewSchemaVersion || null,
          actionLibraryVersion: reviewMeta.actionLibraryVersion || null,
          versionLabel: reviewMeta.versionLabel || null,
          sourceCurrentPeriodId: reviewMeta.sourceCurrentPeriodId || null,
          sourcePriorPeriodId: reviewMeta.sourcePriorPeriodId || null,
          generatedAt: reviewMeta.generatedAt || null,
          pdfStatus: reviewMeta.pdfStatus || null,
        }
      : null,
    canonicalFingerprint: ADP_METHODOLOGY_FINGERPRINT_CURRENT,
  };
}

