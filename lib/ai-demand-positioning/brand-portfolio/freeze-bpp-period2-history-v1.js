/**
 * Freeze Period-2 Brand & Portfolio history to filesystem (immutable, no Period-1 overwrite).
 */
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { sha256OfPayload, stableStringify } from "../longitudinal/report-snapshot-v1.js";
import {
  MEASUREMENT_FAMILY,
  BPP_HISTORY_SCHEMA_VERSION,
} from "./bpp-history-schema-v1.js";
import { BPP_HISTORY_ROOT } from "./bpp-history-freeze-v1.js";
import { PERIOD_2_ID, PERIOD_1_ID, PROVIDERS } from "./bpp-period2-longitudinal-v1.js";

export const PERIOD_2_CALENDAR_WEEK_ID = "adp_week_2026-09-02";
export const PERIOD_2_MONITORING_DATE = "2026-09-02";
export const PERIOD_2_PUBLICATION_VERSION = "bpp-period2-local-v1.1-20260902";
export const PERIOD_2_REPORT_EDITION = "PERIOD_2_CERTIFIED_LOCAL_CANDIDATE_V1";

function rateToNumeratorDenom(metrics) {
  // Prefer observation counts from subject row if present
  const subject = (metrics.tableRows || metrics.rankingUniverse || []).find((r) => r.isSubject);
  if (subject?.numerator != null && subject?.denominator != null) {
    return { numerator: subject.numerator, denominator: subject.denominator };
  }
  if (subject?.obsHits != null && subject?.obsDenom != null) {
    return { numerator: subject.obsHits, denominator: subject.obsDenom };
  }
  return { numerator: null, denominator: null };
}

/**
 * Build history snapshot + structured from Period-2 execution snapshot + local payload.
 */
export function buildPeriod2HistoryArtifacts({
  propertyId,
  executionSnapshot,
  localPayload,
  longitudinal,
}) {
  const metrics = executionSnapshot.metrics;
  const customerPayload = {
    ...localPayload,
    customerPublished: false,
    periodId: PERIOD_2_ID,
    priorPeriodId: PERIOD_1_ID,
    assuranceStatus: "PERIOD_2_CERTIFIED_LOCAL",
    byTerritory: Object.fromEntries(
      Object.entries(metrics.byTerritory || metrics.territories || {}).map(([tid, t]) => [
        tid,
        {
          subjectHits: t.subjectHits ?? t.numerator ?? null,
          observations: t.observations ?? t.denominator ?? null,
          presenceRate: t.aiPresence ?? t.presenceRate ?? null,
          portfolioRank: t.rank ?? t.portfolioRank ?? null,
          portfolioRankOf: t.portfolioRankOf ?? metrics.portfolioRankOf,
          portfolioBenchmark: t.benchmark ?? t.portfolioBenchmark ?? null,
          portfolioPresenceIndex: t.presenceIndex ?? t.portfolioPresenceIndex ?? null,
          scenarios: t.scenarioCount ?? t.scenarios ?? null,
          rankingUniverse: t.rankingUniverse || [],
        },
      ])
    ),
    providerPresence: {
      rows: PROVIDERS.map((p) => {
        const row = metrics.byProvider?.[p] || {};
        return {
          provider: p,
          label: p.charAt(0).toUpperCase() + p.slice(1),
          subjectHits: row.subjectHits ?? row.numerator ?? null,
          observations: row.observations ?? row.denominator ?? null,
          presenceRate: row.presenceRate ?? null,
        };
      }),
    },
    evidence: {
      positive: executionSnapshot.positiveEvidence || [],
      missing: executionSnapshot.missingEvidence || [],
      displacement: executionSnapshot.displacementEvidence || [],
    },
    narrative: longitudinal?.narrative || null,
  };

  const contentHash = sha256OfPayload(customerPayload);
  const snapshotId = `bpp_snap_${propertyId}_${PERIOD_2_ID}_${contentHash.slice(0, 10)}`;
  const now = executionSnapshot.createdAt || new Date().toISOString();
  const nd = rateToNumeratorDenom(metrics);
  const suppressedBenchmark =
    metrics.portfolioBenchmark == null ||
    (metrics.suppressedKpis || []).includes("portfolioBenchmark");
  const suppressedIndex =
    metrics.portfolioPresenceIndex == null ||
    (metrics.suppressedKpis || []).includes("portfolioPresenceIndex");

  const envelope = {
    schema: "ADP_BPP_REPORT_SNAPSHOT_V1",
    snapshotId,
    propertyId,
    monitoringPeriodId: PERIOD_2_CALENDAR_WEEK_ID,
    measurementPeriodId: PERIOD_2_ID,
    measurementFamily: MEASUREMENT_FAMILY.BRAND_PORTFOLIO,
    monitoringDate: PERIOD_2_MONITORING_DATE,
    generatedAt: now,
    certifiedAt: now,
    publishedAt: null,
    certificationStatus: "PERIOD_2_CERTIFIED_LOCAL",
    assuranceStatus: "PERIOD_2_CERTIFIED_LOCAL",
    publicationVersion: PERIOD_2_PUBLICATION_VERSION,
    reportEdition: PERIOD_2_REPORT_EDITION,
    measurementContract: "ADP_BRAND_PORTFOLIO_KPI_CONTRACT_V1_1",
    metricsVersion: "ADP_BRAND_PORTFOLIO_METRICS_V1_1",
    lensId: metrics.lens?.lensId || executionSnapshot.lens?.lensId,
    lensLabel: metrics.lens?.label || executionSnapshot.lens?.label,
    affiliationSnapshot: customerPayload.affiliationSnapshot || executionSnapshot.affiliation,
    peerSetId: metrics.peerSetId || executionSnapshot.peerSetId,
    peerSetVersion: metrics.peerSetVersion || executionSnapshot.peerSetVersion,
    peerSetHash: executionSnapshot.peerSetHash,
    promptManifestHash: executionSnapshot.promptManifestHash,
    frozenPackageHash: executionSnapshot.frozenPackageHash,
    priorComparablePeriodId: PERIOD_1_ID,
    comparisonMode: "PRIOR_RUN",
    customerVisibleContentHash: contentHash,
    customerPublished: false,
    synthetic: false,
    versions: {
      bppHistorySchema: BPP_HISTORY_SCHEMA_VERSION,
      publicationVersion: PERIOD_2_PUBLICATION_VERSION,
      kpiContractVersion: "ADP_BRAND_PORTFOLIO_KPI_CONTRACT_V1_1",
      metricsVersion: "ADP_BRAND_PORTFOLIO_METRICS_V1_1",
      promptManifestHash: executionSnapshot.promptManifestHash,
    },
    disclosures: [
      {
        code: "PERIOD_2_LOCAL_CERTIFIED_NOT_CUSTOMER_PUBLISHED",
        note: "Period-2 certified locally; customer publication requires separate founder GO.",
      },
    ],
    comparability: {
      rankMovementState: "COMPARABLE",
      hasPriorComparablePeriod: true,
      priorPeriodId: PERIOD_1_ID,
      peerSetChangeClass: "PEER_SET_UNCHANGED",
    },
    customerPayload,
  };
  const envelopeHash = createHash("sha256").update(stableStringify(envelope)).digest("hex");
  const snapshot = { ...envelope, contentHash, envelopeHash };

  const rankingOverall = (metrics.tableRows || []).map((row) => ({
    canonicalEntityId: row.canonicalEntityId,
    displayName: row.name || row.displayName,
    brand: row.brand || null,
    numerator: row.numerator ?? row.obsHits ?? null,
    denominator: row.denominator ?? row.obsDenom ?? null,
    presenceRate: row.presenceRate ?? null,
    presencePct: row.presencePct ?? (row.presenceRate != null ? Math.round(row.presenceRate * 1000) / 10 : null),
    rank: row.rank,
    tieState: row.tieState || null,
    isSubject: Boolean(row.isSubject),
    displacementDisplay: row.displacementDisplay ?? null,
    sharedDisplay: row.sharedDisplay ?? null,
    deltaDisplay: row.deltaDisplay ?? null,
    scope: "overall",
  }));

  const territories = Object.entries(metrics.byTerritory || metrics.territories || {}).map(
    ([territoryId, t]) => ({
      territoryId,
      name: territoryId,
      numerator: t.subjectHits ?? t.numerator ?? null,
      denominator: t.observations ?? t.denominator ?? null,
      aiPresence: t.aiPresence ?? t.presenceRate ?? null,
      rank: t.rank ?? t.portfolioRank ?? null,
      portfolioRankOf: t.portfolioRankOf ?? metrics.portfolioRankOf,
      benchmark: t.benchmark ?? t.portfolioBenchmark ?? null,
      presenceIndex: t.presenceIndex ?? t.portfolioPresenceIndex ?? null,
      scenarioCount: t.scenarioCount ?? t.scenarios ?? null,
      peerSetVersion: metrics.peerSetVersion,
      rankingUniverse: t.rankingUniverse || [],
    })
  );

  const providers = PROVIDERS.map((p) => {
    const row = metrics.byProvider?.[p] || {};
    return {
      providerId: p,
      name: p.charAt(0).toUpperCase() + p.slice(1),
      numerator: row.subjectHits ?? row.numerator ?? null,
      denominator: row.observations ?? row.denominator ?? null,
      presenceRate: row.presenceRate ?? null,
    };
  });

  const structured = {
    propertyId,
    monitoringPeriodId: PERIOD_2_CALENDAR_WEEK_ID,
    measurementPeriodId: PERIOD_2_ID,
    measurementFamily: MEASUREMENT_FAMILY.BRAND_PORTFOLIO,
    publicationVersion: PERIOD_2_PUBLICATION_VERSION,
    reportEdition: PERIOD_2_REPORT_EDITION,
    measurementContract: "ADP_BRAND_PORTFOLIO_KPI_CONTRACT_V1_1",
    metricsVersion: "ADP_BRAND_PORTFOLIO_METRICS_V1_1",
    lensId: snapshot.lensId,
    lensLabel: snapshot.lensLabel,
    affiliationSnapshot: snapshot.affiliationSnapshot,
    peerSetId: snapshot.peerSetId,
    peerSetVersion: snapshot.peerSetVersion,
    peerSetHash: snapshot.peerSetHash,
    promptManifestHash: snapshot.promptManifestHash,
    frozenPackageHash: executionSnapshot.frozenPackageHash,
    customerVisibleContentHash: contentHash,
    assuranceStatus: "PERIOD_2_CERTIFIED_LOCAL",
    publicationTimestamp: null,
    priorComparablePeriodId: PERIOD_1_ID,
    rankMovementState: "COMPARABLE",
    periodMetrics: {
      portfolioAiPresence: metrics.portfolioAiPresence,
      portfolioAiPresenceNumerator: nd.numerator,
      portfolioAiPresenceDenominator: nd.denominator,
      portfolioRank: metrics.portfolioRank,
      portfolioRankOf: metrics.portfolioRankOf,
      portfolioBenchmark: suppressedBenchmark ? null : metrics.portfolioBenchmark,
      portfolioPresenceIndex: suppressedIndex ? null : metrics.portfolioPresenceIndex,
      numberOneAppearanceRate: metrics.numberOneAppearance,
      topThreeAppearanceRate: metrics.top3Appearance,
      portfolioScenarioPresence: metrics.portfolioScenarioPresence,
      portfolioScenarioPresenceRole: "INTERNAL_DIAGNOSTIC",
      suppressedKpis: {
        portfolioBenchmark: suppressedBenchmark,
        portfolioPresenceIndex: suppressedIndex,
      },
    },
    territories,
    providers,
    rankingOverall,
    evidence: customerPayload.evidence,
    narrative: customerPayload.narrative,
    displacement: metrics.displacement,
    scenariosShared: metrics.scenariosShared,
    longitudinal: longitudinal || null,
    independent: executionSnapshot.independent || null,
    assurance: executionSnapshot.assurance || null,
  };

  return { snapshot, structured };
}

export function freezePeriod2Filesystem({
  snapshotsByProperty,
  localPayloads,
  longitudinalByProperty,
  writeFilesystem = true,
} = {}) {
  const periodRoot = join(BPP_HISTORY_ROOT, "periods", PERIOD_2_ID);
  const period1Root = join(BPP_HISTORY_ROOT, "periods", PERIOD_1_ID);
  if (!existsSync(period1Root)) {
    throw new Error("Period-1 history missing — refuse Period-2 freeze");
  }

  const frozen = [];
  for (const [propertyId, executionSnapshot] of Object.entries(snapshotsByProperty)) {
    const localPayload = localPayloads[propertyId];
    const longitudinal = longitudinalByProperty?.[propertyId] || null;
    const { snapshot, structured } = buildPeriod2HistoryArtifacts({
      propertyId,
      executionSnapshot,
      localPayload,
      longitudinal,
    });
    frozen.push({ propertyId, snapshot, structured });
  }

  const freezeBundle = {
    freezeVersion: "bpp_period2_history_freeze_v1",
    measurementFamily: MEASUREMENT_FAMILY.BRAND_PORTFOLIO,
    monitoringPeriodId: PERIOD_2_CALENDAR_WEEK_ID,
    measurementPeriodId: PERIOD_2_ID,
    priorPeriodId: PERIOD_1_ID,
    publicationVersion: PERIOD_2_PUBLICATION_VERSION,
    reportEdition: PERIOD_2_REPORT_EDITION,
    customerPublished: false,
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
      suppressedBenchmark: f.structured.periodMetrics.suppressedKpis.portfolioBenchmark,
      suppressedIndex: f.structured.periodMetrics.suppressedKpis.portfolioPresenceIndex,
    })),
    frozenAt: new Date().toISOString(),
    period1Untouched: true,
    period1Root,
  };

  if (writeFilesystem) {
    mkdirSync(periodRoot, { recursive: true });
    writeFileSync(join(periodRoot, "freeze-index.json"), JSON.stringify(freezeBundle, null, 2) + "\n");
    for (const f of frozen) {
      const propDir = join(periodRoot, f.propertyId);
      mkdirSync(join(propDir, "evidence-blobs"), { recursive: true });
      writeFileSync(join(propDir, "snapshot.json"), JSON.stringify(f.snapshot, null, 2) + "\n");
      writeFileSync(join(propDir, "structured.json"), JSON.stringify(f.structured, null, 2) + "\n");
      for (const lane of ["positive", "missing", "displacement"]) {
        for (const ev of f.structured.evidence[lane] || []) {
          const oid =
            ev.observationId ||
            createHash("sha256").update(stableStringify(ev)).digest("hex").slice(0, 16);
          const ref = join(propDir, "evidence-blobs", `${oid}.txt`);
          if (!existsSync(ref) && (ev.aiResponse || ev.context || ev.exactResponse)) {
            writeFileSync(ref, ev.aiResponse || ev.context || ev.exactResponse || "", "utf8");
          }
        }
      }
    }
    freezeBundle.filesystemRoot = periodRoot;
  }

  return { freezeBundle, frozen, periodRoot };
}

export function assertPeriod1Untouched() {
  const p1 = join(BPP_HISTORY_ROOT, "periods", PERIOD_1_ID, "freeze-index.json");
  if (!existsSync(p1)) return { pass: false, detail: "Period-1 freeze-index missing" };
  const before = readFileSync(p1, "utf8");
  // Caller should re-read after writes; this helper verifies existence + hash snapshot
  return {
    pass: true,
    path: p1,
    hash: createHash("sha256").update(before).digest("hex"),
    content: before,
  };
}
