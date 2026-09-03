/**
 * Freeze customer-published Brand & Portfolio Period 1 — no metric recompute.
 * BRAND_PORTFOLIO_PUBLISHED_SNAPSHOT_IMMUTABILITY
 * BRAND_PORTFOLIO_HISTORICAL_REPORT_REPRODUCTION_INTEGRITY
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { sha256OfPayload, stableStringify } from "../longitudinal/report-snapshot-v1.js";
import {
  MEASUREMENT_FAMILY,
  BPP_HISTORY_SCHEMA_VERSION,
  BPP_REPORT_EDITION,
  BPP_PUBLICATION_VERSION,
  BPP_PERIOD_ID,
  BPP_CALENDAR_WEEK_ID,
  BPP_ASSET_TOKEN_FROZEN,
  CORRECTION_LINEAGE_STAGES,
  CORRECTION_REASON,
  BPP_COMPARABILITY_POLICY_V1,
} from "./bpp-history-schema-v1.js";

export const BPP_HISTORY_FREEZE_VERSION = "bpp_history_freeze_v1";
export const BRAND_PORTFOLIO_PUBLISHED_SNAPSHOT_IMMUTABILITY =
  "BRAND_PORTFOLIO_PUBLISHED_SNAPSHOT_IMMUTABILITY";
export const BRAND_PORTFOLIO_HISTORICAL_REPORT_REPRODUCTION_INTEGRITY =
  "BRAND_PORTFOLIO_HISTORICAL_REPORT_REPRODUCTION_INTEGRITY";
export const MEASUREMENT_FAMILY_HISTORY_ISOLATION = "MEASUREMENT_FAMILY_HISTORY_ISOLATION";
export const PORTFOLIO_LONGITUDINAL_PEER_SET_INTEGRITY =
  "PORTFOLIO_LONGITUDINAL_PEER_SET_INTEGRITY";
export const HISTORY_PERSISTENCE_CURRENT_REPORT_ISOLATION =
  "HISTORY_PERSISTENCE_CURRENT_REPORT_ISOLATION";

export const CUSTOMER_PUBLISHED_PACK_PATH = join(
  process.cwd(),
  "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_CUSTOMER_PUBLISHED_V1.json"
);
/** Period-1 customer pack archive — used when live pack has advanced to a later period. */
export const CUSTOMER_PUBLISHED_PERIOD1_ARCHIVE_PATH = join(
  process.cwd(),
  "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_CUSTOMER_PUBLISHED_PERIOD1_ARCHIVE_V1.json"
);
export const OFFICIAL_BASELINE_PATH = join(
  process.cwd(),
  "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_OFFICIAL_BASELINE_V1.json"
);
export const CORRECTION_LINEAGE_PATH = join(
  process.cwd(),
  "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_CORRECTION_LINEAGE_V1_1.json"
);
export const PROMPT_MANIFEST_PATH = join(
  process.cwd(),
  "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_PROMPT_MANIFEST_V1.json"
);
export const ORIGINAL_CANDIDATE_PATH = join(
  process.cwd(),
  "reports/ai-demand-positioning/ADP_BRAND_PORTFOLIO_ORIGINAL_FIRST_CYCLE_CANDIDATE_SCENARIO_ANY_V1.json"
);

/** Immutable filesystem history SoT (not Live overlay). */
export const BPP_HISTORY_ROOT = join(
  process.cwd(),
  "data/ai-demand-positioning/brand-portfolio-history"
);

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function kpiById(kpis, id) {
  return (kpis || []).find((k) => k && k.id === id) || null;
}

function parsePresencePct(display) {
  if (display == null) return null;
  if (typeof display === "number") return display;
  const m = String(display).match(/([0-9.]+)/);
  return m ? Number(m[1]) : null;
}

/**
 * Build immutable BPP report snapshot from exact customer-published payload.
 * Does not recompute KPIs.
 */
export function buildBppPublishedSnapshotV1({
  propertyId,
  customerPayload,
  packMeta,
  baselineMeta,
  promptManifestHash,
  lineage,
}) {
  const generatedAt = packMeta.publishedAt || lineage?.customerPublication?.publishedAt || null;
  const customerVisibleContentHash = sha256OfPayload(customerPayload);
  const snapshotId = `bpp_snap_${propertyId}_${BPP_PERIOD_ID}_${customerVisibleContentHash.slice(0, 10)}`;

  const envelope = {
    schema: "ADP_BPP_REPORT_SNAPSHOT_V1",
    snapshotId,
    propertyId,
    monitoringPeriodId: BPP_CALENDAR_WEEK_ID,
    measurementPeriodId: BPP_PERIOD_ID,
    measurementFamily: MEASUREMENT_FAMILY.BRAND_PORTFOLIO,
    monitoringDate: "2026-08-21",
    generatedAt,
    certifiedAt: generatedAt,
    publishedAt: generatedAt,
    certificationStatus: "CUSTOMER_PUBLISHED",
    assuranceStatus: customerPayload.assuranceStatus || "CUSTOMER_READY",
    publicationVersion: packMeta.publicationVersion || BPP_PUBLICATION_VERSION,
    reportEdition: BPP_REPORT_EDITION,
    measurementContract: packMeta.kpiContractVersion || "ADP_BRAND_PORTFOLIO_KPI_CONTRACT_V1_1",
    metricsVersion: packMeta.metricsVersion || "ADP_BRAND_PORTFOLIO_METRICS_V1_1",
    lensId: customerPayload.lens?.lensId || null,
    lensLabel: customerPayload.lens?.label || null,
    affiliationSnapshot: customerPayload.affiliationSnapshot || null,
    peerSetId: baselineMeta?.peerSetId || null,
    peerSetVersion: baselineMeta?.peerSetVersion || null,
    peerSetHash: baselineMeta?.peerSetHash || null,
    promptManifestHash: promptManifestHash || baselineMeta?.promptManifestHash || null,
    providerCoverage: (customerPayload.providerPresence?.rows || []).map((r) => ({
      provider: r.provider,
      observations: r.observations,
      subjectHits: r.subjectHits,
      presenceRate: r.presenceRate,
    })),
    customerVisibleContentHash,
    assetCacheTokenAtPublication: BPP_ASSET_TOKEN_FROZEN,
    synthetic: false,
    versions: {
      bppHistorySchema: BPP_HISTORY_SCHEMA_VERSION,
      publicationVersion: packMeta.publicationVersion || BPP_PUBLICATION_VERSION,
      kpiContractVersion: packMeta.kpiContractVersion,
      metricsVersion: packMeta.metricsVersion,
      rendererVersion: BPP_ASSET_TOKEN_FROZEN,
      promptManifestHash: promptManifestHash || baselineMeta?.promptManifestHash || null,
    },
    disclosures: [
      {
        code: CORRECTION_REASON,
        note: "Original scenario-any candidate never customer-published; V1.1 observation-grain is the published baseline.",
      },
    ],
    correctionLineage: {
      stages: [
        CORRECTION_LINEAGE_STAGES.ORIGINAL_FIRST_CYCLE_CANDIDATE,
        CORRECTION_LINEAGE_STAGES.CORRECTED_FIRST_CYCLE_CANDIDATE,
        CORRECTION_LINEAGE_STAGES.CUSTOMER_PUBLISHED_BASELINE,
      ],
      reason: CORRECTION_REASON,
      originalNeverCustomerPublished: true,
      lineageRef: "ADP_BRAND_PORTFOLIO_CORRECTION_LINEAGE_V1_1.json",
    },
    comparability: {
      rankMovementState: "INITIAL",
      hasPriorComparablePeriod: false,
      policyVersion: BPP_COMPARABILITY_POLICY_V1.version,
    },
    // Exact customer-visible section — reproduction source of truth
    customerPayload,
  };

  const envelopeHash = createHash("sha256").update(stableStringify(envelope)).digest("hex");
  return { ...envelope, contentHash: customerVisibleContentHash, envelopeHash };
}

/**
 * Extract structured history fields from frozen published + certified baseline (no recompute).
 */
export function extractBppStructuredHistory({
  propertyId,
  customerPayload,
  baseline,
  snapshot,
}) {
  const kpis = customerPayload.kpis || [];
  const presence = kpiById(kpis, "portfolioAiPresence");
  const rank = kpiById(kpis, "portfolioRank");
  const bench = kpiById(kpis, "portfolioBenchmark");
  const index = kpiById(kpis, "portfolioPresenceIndex");
  const n1 = kpiById(kpis, "numberOneAppearance");
  const top3 = kpiById(kpis, "top3Appearance");

  const presenceMeta = String(presence?.meta || "").match(/(\d+)\s+of\s+(\d+)/i);
  const rankingUniverse = baseline?.metrics?.rankingUniverse || [];
  const byEntity = new Map(rankingUniverse.map((r) => [r.canonicalEntityId, r]));

  const overallRows = (customerPayload.ranking?.rows || []).map((row) => {
    const full = byEntity.get(row.canonicalEntityId) || {};
    return {
      canonicalEntityId: row.canonicalEntityId,
      displayName: row.name,
      brand: row.brand || full.brand || null,
      numerator: full.numerator ?? full.obsHits ?? null,
      denominator: full.denominator ?? full.obsDenom ?? null,
      presenceRate: full.presenceRate ?? parsePresencePct(row.presenceDisplay) / 100,
      presencePct: full.presencePct ?? parsePresencePct(row.presenceDisplay),
      rank: row.rank,
      tieState: full.tieState || null,
      isSubject: Boolean(row.isSubject),
      displacementDisplay: row.displacementDisplay ?? null,
      sharedDisplay: row.sharedDisplay ?? null,
      scope: "overall",
    };
  });

  const territories = [];
  for (const [territoryId, t] of Object.entries(customerPayload.byTerritory || {})) {
    territories.push({
      territoryId,
      name: territoryId,
      numerator: t.subjectHits ?? null,
      denominator: t.observations ?? null,
      aiPresence: t.presenceRate ?? null,
      rank: t.portfolioRank ?? null,
      portfolioRankOf: t.portfolioRankOf ?? null,
      benchmark: t.portfolioBenchmark ?? null,
      presenceIndex: t.portfolioPresenceIndex ?? null,
      scenarioCount: t.scenarios ?? null,
      peerSetVersion: baseline?.peerSetVersion || null,
      rankingUniverse: t.rankingUniverse || [],
    });
  }

  const providers = (customerPayload.providerPresence?.rows || []).map((r) => ({
    providerId: r.provider,
    name: r.label || r.provider,
    numerator: r.subjectHits ?? null,
    denominator: r.observations ?? null,
    presenceRate: r.presenceRate ?? null,
  }));

  const evidence = {
    positive: customerPayload.evidence?.positive || [],
    missing: customerPayload.evidence?.missing || [],
    displacement: customerPayload.evidence?.displacement || [],
  };

  const suppressed = {
    portfolioBenchmark: !bench,
    portfolioPresenceIndex: !index,
  };

  return {
    propertyId,
    monitoringPeriodId: snapshot.monitoringPeriodId,
    measurementPeriodId: snapshot.measurementPeriodId,
    measurementFamily: MEASUREMENT_FAMILY.BRAND_PORTFOLIO,
    publicationVersion: snapshot.publicationVersion,
    reportEdition: snapshot.reportEdition,
    measurementContract: snapshot.measurementContract,
    metricsVersion: snapshot.metricsVersion,
    lensId: snapshot.lensId,
    lensLabel: snapshot.lensLabel,
    affiliationSnapshot: snapshot.affiliationSnapshot,
    peerSetId: snapshot.peerSetId,
    peerSetVersion: snapshot.peerSetVersion,
    peerSetHash: snapshot.peerSetHash,
    promptManifestHash: snapshot.promptManifestHash,
    customerVisibleContentHash: snapshot.customerVisibleContentHash,
    assuranceStatus: snapshot.assuranceStatus,
    publicationTimestamp: snapshot.publishedAt,
    rankMovementState: "INITIAL",
    periodMetrics: {
      portfolioAiPresence: presence?.valueRaw ?? null,
      portfolioAiPresenceNumerator: presenceMeta ? Number(presenceMeta[1]) : null,
      portfolioAiPresenceDenominator: presenceMeta ? Number(presenceMeta[2]) : null,
      portfolioRank: rank?.valueRaw ?? null,
      portfolioRankOf: rankingUniverse.length || null,
      portfolioBenchmark: suppressed.portfolioBenchmark ? null : bench?.valueRaw ?? null,
      portfolioPresenceIndex: suppressed.portfolioPresenceIndex ? null : index?.valueRaw ?? null,
      numberOneAppearanceRate: n1?.valueRaw ?? null,
      topThreeAppearanceRate: top3?.valueRaw ?? null,
      // INTERNAL_DIAGNOSTIC only — never promote as customer KPI
      portfolioScenarioPresence: baseline?.metrics?.portfolioScenarioPresence ?? null,
      portfolioScenarioPresenceRole: "INTERNAL_DIAGNOSTIC",
      suppressedKpis: suppressed,
    },
    territories,
    providers,
    rankingOverall: overallRows,
    evidence,
    narrative: customerPayload.narrative || null,
    displacement: baseline?.metrics?.displacement || null,
    scenariosShared: baseline?.metrics?.scenariosShared || null,
  };
}

/**
 * Resolve the Period-1 customer pack source.
 * After Period-2+ promotion, live CUSTOMER_PUBLISHED_PACK is no longer Period-1 —
 * freeze must use the Period-1 archive (HISTORICAL_FREEZE_USES_PERIOD_ARCHIVE).
 */
export function resolvePeriod1CustomerPublishedPackPath() {
  const live = readJson(CUSTOMER_PUBLISHED_PACK_PATH);
  if (live.periodId === BPP_PERIOD_ID && live.publicationVersion === BPP_PUBLICATION_VERSION) {
    return CUSTOMER_PUBLISHED_PACK_PATH;
  }
  if (!existsSync(CUSTOMER_PUBLISHED_PERIOD1_ARCHIVE_PATH)) {
    throw new Error(
      "Period-1 freeze requires ADP_BRAND_PORTFOLIO_CUSTOMER_PUBLISHED_PERIOD1_ARCHIVE_V1.json when live pack is not Period 1"
    );
  }
  return CUSTOMER_PUBLISHED_PERIOD1_ARCHIVE_PATH;
}

export function freezeCustomerPublishedBppPeriod1({ writeFilesystem = false } = {}) {
  const packPath = resolvePeriod1CustomerPublishedPackPath();
  const pack = readJson(packPath);
  const baselinePack = readJson(OFFICIAL_BASELINE_PATH);
  const lineage = readJson(CORRECTION_LINEAGE_PATH);
  const manifest = readJson(PROMPT_MANIFEST_PATH);

  if (!pack.customerPublished) {
    throw new Error("Customer published pack is not marked customerPublished=true");
  }
  if (pack.publicationVersion !== BPP_PUBLICATION_VERSION) {
    throw new Error(`Unexpected publicationVersion ${pack.publicationVersion} in ${packPath}`);
  }
  if (pack.periodId !== BPP_PERIOD_ID) {
    throw new Error(`Unexpected periodId ${pack.periodId} in ${packPath}`);
  }

  const packPayloadHash = pack.payloadHash || sha256OfPayload(pack.payloads);
  const propertyIds = Object.keys(pack.payloads || {});
  const frozen = [];

  for (const propertyId of propertyIds) {
    const customerPayload = pack.payloads[propertyId];
    const baseline = baselinePack.baselines?.[propertyId] || null;
    const snapshot = buildBppPublishedSnapshotV1({
      propertyId,
      customerPayload,
      packMeta: {
        publicationVersion: pack.publicationVersion,
        kpiContractVersion: pack.kpiContractVersion,
        metricsVersion: pack.metricsVersion,
        publishedAt: lineage?.customerPublication?.publishedAt || pack.publishedAt || null,
      },
      baselineMeta: baseline,
      promptManifestHash: manifest.promptManifestHash || baseline?.promptManifestHash,
      lineage,
    });
    const structured = extractBppStructuredHistory({
      propertyId,
      customerPayload,
      baseline,
      snapshot,
    });
    frozen.push({ propertyId, snapshot, structured });
  }

  const freezeBundle = {
    freezeVersion: BPP_HISTORY_FREEZE_VERSION,
    measurementFamily: MEASUREMENT_FAMILY.BRAND_PORTFOLIO,
    monitoringPeriodId: BPP_CALENDAR_WEEK_ID,
    measurementPeriodId: BPP_PERIOD_ID,
    publicationVersion: BPP_PUBLICATION_VERSION,
    reportEdition: BPP_REPORT_EDITION,
    packPayloadHash,
    lineage: {
      reason: CORRECTION_REASON,
      stages: Object.values(CORRECTION_LINEAGE_STAGES),
      originalNeverCustomerPublished: true,
      originalArchive: "ADP_BRAND_PORTFOLIO_ORIGINAL_FIRST_CYCLE_CANDIDATE_SCENARIO_ANY_V1.json",
      correctedCandidate: "ADP_BRAND_PORTFOLIO_OFFICIAL_BASELINE_V1.json",
      customerPublished: "ADP_BRAND_PORTFOLIO_CUSTOMER_PUBLISHED_V1.json",
      lineageDoc: "ADP_BRAND_PORTFOLIO_CORRECTION_LINEAGE_V1_1.json",
    },
    comparabilityPolicy: BPP_COMPARABILITY_POLICY_V1,
    propertyCount: frozen.length,
    properties: frozen.map((f) => ({
      propertyId: f.propertyId,
      snapshotId: f.snapshot.snapshotId,
      contentHash: f.snapshot.contentHash,
      envelopeHash: f.snapshot.envelopeHash,
      peerSetHash: f.snapshot.peerSetHash,
      lensId: f.snapshot.lensId,
      rankingRowCount: f.structured.rankingOverall.length,
      territoryCount: f.structured.territories.length,
      providerCount: f.structured.providers.length,
      evidencePositive: f.structured.evidence.positive.length,
      evidenceMissing: f.structured.evidence.missing.length,
      evidenceDisplacement: f.structured.evidence.displacement.length,
      suppressedBenchmark: f.structured.periodMetrics.suppressedKpis.portfolioBenchmark,
      suppressedIndex: f.structured.periodMetrics.suppressedKpis.portfolioPresenceIndex,
    })),
    frozenAt: new Date().toISOString(),
    currentReportIsolation: {
      assetToken: BPP_ASSET_TOKEN_FROZEN,
      period1PackPath: packPath,
      liveCustomerPackPath: CUSTOMER_PUBLISHED_PACK_PATH,
      mustNotMutateLivePublishedReports: true,
      mustNotMutateCoreFilesystemSoT: true,
    },
  };

  if (writeFilesystem) {
    const periodRoot = join(BPP_HISTORY_ROOT, "periods", BPP_PERIOD_ID);
    mkdirSync(periodRoot, { recursive: true });
    writeFileSync(join(periodRoot, "freeze-index.json"), JSON.stringify(freezeBundle, null, 2) + "\n");
    for (const f of frozen) {
      const propDir = join(periodRoot, f.propertyId);
      mkdirSync(join(propDir, "evidence-blobs"), { recursive: true });
      writeFileSync(join(propDir, "snapshot.json"), JSON.stringify(f.snapshot, null, 2) + "\n");
      writeFileSync(join(propDir, "structured.json"), JSON.stringify(f.structured, null, 2) + "\n");
      // Evidence refs — store verbatim responses as immutable blobs (index only in Airtable)
      for (const lane of ["positive", "missing", "displacement"]) {
        for (const ev of f.structured.evidence[lane] || []) {
          const oid = ev.observationId || createHash("sha256").update(stableStringify(ev)).digest("hex").slice(0, 16);
          const ref = join(propDir, "evidence-blobs", `${oid}.txt`);
          if (!existsSync(ref) && (ev.aiResponse || ev.context)) {
            writeFileSync(ref, ev.aiResponse || ev.context || "", "utf8");
          }
        }
      }
    }
    freezeBundle.filesystemRoot = periodRoot;
  }

  return { ok: true, freezeBundle, frozen };
}

/**
 * Prove freeze did not alter customer pack / asset token (isolation).
 */
export function assertCurrentReportIsolation(beforeHash) {
  const pack = readJson(CUSTOMER_PUBLISHED_PACK_PATH);
  const afterHash = pack.payloadHash || sha256OfPayload(pack.payloads);
  const defects = [];
  if (beforeHash && beforeHash !== afterHash) {
    defects.push({
      code: HISTORY_PERSISTENCE_CURRENT_REPORT_ISOLATION,
      detail: "customer published pack hash changed during history freeze",
    });
  }
  // Live pack may be Period-2+; Period-1 freeze must not mutate current customer SoT.
  // Do not require live publicationVersion === Period-1 version after multi-period publication.
  return {
    gate: HISTORY_PERSISTENCE_CURRENT_REPORT_ISOLATION,
    pass: defects.length === 0,
    defects,
    packPayloadHash: afterHash,
    livePublicationVersion: pack.publicationVersion || null,
    livePeriodId: pack.periodId || null,
    assetToken: BPP_ASSET_TOKEN_FROZEN,
  };
}
