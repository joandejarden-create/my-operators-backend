/**
 * Persist Brand & Portfolio historical period — mock/filesystem only by default.
 * Reuses MockAdpHistoryStore + HISTORY_TABLES with Measurement Family isolation.
 *
 * Production Airtable blocked unless founder activates ADP_HISTORY_WRITES_ENABLED
 * + ADP_HISTORY_AIRTABLE_WRITE_APPLY + sandbox base (same Core history gate).
 */

import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import {
  HISTORY_TABLES,
  PERSISTENCE_STATES,
  HISTORY_ENV,
  isAdpHistoryWritesEnabled,
} from "../longitudinal/airtable-history-schema-final-v1.js";
import {
  MockAdpHistoryStore,
  MOCK_HISTORY_ROOT,
  HISTORICAL_RECORD_NO_SILENT_OVERWRITE,
  DEFECT_HISTORICAL_RECORD_SILENT_OVERWRITE,
  REPORT_HISTORY_PERSISTENCE_COMPLETE,
} from "../longitudinal/persist-adp-historical-period-v1.js";
import { sha256OfPayload, stableStringify } from "../longitudinal/report-snapshot-v1.js";
import {
  MEASUREMENT_FAMILY,
  BPP_CALENDAR_WEEK_ID,
  BPP_PERIOD_ID,
  BPP_PUBLICATION_VERSION,
  CORRECTION_LINEAGE_STAGES,
  CORRECTION_REASON,
  BPP_IDEMPOTENCY_KEYS,
} from "./bpp-history-schema-v1.js";
import {
  freezeCustomerPublishedBppPeriod1,
  BRAND_PORTFOLIO_PUBLISHED_SNAPSHOT_IMMUTABILITY,
  BRAND_PORTFOLIO_HISTORICAL_REPORT_REPRODUCTION_INTEGRITY,
  MEASUREMENT_FAMILY_HISTORY_ISOLATION,
} from "./bpp-history-freeze-v1.js";

export const PERSIST_BPP_HISTORICAL_PERIOD_VERSION = "persist_bpp_historical_period_v1";
export const BRAND_PORTFOLIO_HISTORY_IDEMPOTENCY_INTEGRITY =
  "BRAND_PORTFOLIO_HISTORY_IDEMPOTENCY_INTEGRITY";
export const BRAND_PORTFOLIO_REAL_PERIOD_2_HISTORY_READY =
  "BRAND_PORTFOLIO_REAL_PERIOD_2_HISTORY_READY";
export const BRAND_PORTFOLIO_REAL_PERIOD_2_READY = "BRAND_PORTFOLIO_REAL_PERIOD_2_READY";

export const BPP_MOCK_HISTORY_ROOT = join(MOCK_HISTORY_ROOT, "brand-portfolio");

function hashShort(s) {
  return createHash("sha256").update(String(s)).digest("hex").slice(0, 16);
}

function familyKey(propertyId, measurementPeriodId, family) {
  return `${propertyId}|${measurementPeriodId}|${family}`;
}

/**
 * Build structured write manifest for one property from frozen snapshot + structured.
 */
export function buildBppHistoricalWriteManifest({
  snapshot,
  structured,
  historicalVersion = 1,
  synthetic = false,
}) {
  const defects = [];
  const propertyId = snapshot.propertyId;
  const calendarWeekId = snapshot.monitoringPeriodId || BPP_CALENDAR_WEEK_ID;
  const measurementPeriodId = snapshot.measurementPeriodId || BPP_PERIOD_ID;
  const family = MEASUREMENT_FAMILY.BRAND_PORTFOLIO;
  const version = historicalVersion;
  const fk = familyKey(propertyId, measurementPeriodId, family);
  const records = [];

  if (snapshot.measurementFamily !== family) {
    defects.push({
      code: MEASUREMENT_FAMILY_HISTORY_ISOLATION,
      detail: "snapshot family must be BRAND_PORTFOLIO",
    });
  }
  if (snapshot.contentHash !== sha256OfPayload(snapshot.customerPayload)) {
    defects.push({
      code: BRAND_PORTFOLIO_PUBLISHED_SNAPSHOT_IMMUTABILITY,
      detail: "contentHash !== customerPayload hash",
    });
  }

  // Shared monitoring period (calendar week) — families listed in JSON
  records.push({
    table: HISTORY_TABLES.MONITORING_PERIODS,
    idempotencyKey: `${propertyId}|${calendarWeekId}|v${version}`,
    fields: {
      "Period Key": `${propertyId}|${calendarWeekId}|v${version}`,
      "Property ID": propertyId,
      "Period ID": calendarWeekId,
      "Calendar Week ID": calendarWeekId,
      "Measurement Period IDs JSON": JSON.stringify([measurementPeriodId]),
      "Families Present JSON": JSON.stringify([family]),
      "Snapshot ID": snapshot.snapshotId,
      "Monitoring Date": snapshot.monitoringDate,
      "Certification Timestamp": snapshot.certifiedAt,
      "Publication Timestamp": snapshot.publishedAt,
      "Certification Status": snapshot.certificationStatus,
      "Report Schema Version": snapshot.schema,
      "Persistence State": PERSISTENCE_STATES.PENDING,
      "Content Hash": snapshot.contentHash,
      "Envelope Hash": snapshot.envelopeHash,
      "Historical Version": version,
      "Is Current Historical Version": true,
      "Corrects Period Key": "",
      Synthetic: Boolean(synthetic),
    },
  });

  const snapshotStoreRef = `bpp-snapshot-blobs/${propertyId}/${snapshot.snapshotId}.json`;
  records.push({
    table: HISTORY_TABLES.REPORT_SNAPSHOTS,
    idempotencyKey: `${snapshot.snapshotId}|${family}`,
    fields: {
      "Snapshot ID": snapshot.snapshotId,
      "Property ID": propertyId,
      "Period ID": measurementPeriodId,
      "Measurement Family": family,
      "Monitoring Date": snapshot.monitoringDate,
      "Schema Version": snapshot.schema,
      "Content Hash": snapshot.contentHash,
      "Envelope Hash": snapshot.envelopeHash,
      "Publication Version": snapshot.publicationVersion,
      "Report Edition": snapshot.reportEdition,
      "Portfolio Lens ID": snapshot.lensId,
      "Peer Set ID": snapshot.peerSetId,
      "Peer Set Version": snapshot.peerSetVersion,
      "Peer Set Hash": snapshot.peerSetHash,
      "Prompt Manifest Hash": snapshot.promptManifestHash,
      "Customer Visible Content Hash": snapshot.customerVisibleContentHash,
      "Publication Commit": snapshot.publicationVersion,
      "Renderer Version": snapshot.versions?.rendererVersion || null,
      "Measurement Contract Version": snapshot.measurementContract,
      "Certification Status": snapshot.certificationStatus,
      "Correction Version": version,
      "Is Current Historical Version": true,
      "Snapshot Store Ref": snapshotStoreRef,
      Synthetic: Boolean(synthetic),
    },
  });

  const pm = structured.periodMetrics;
  records.push({
    table: HISTORY_TABLES.PERIOD_METRICS,
    idempotencyKey: `${fk}|metrics|v${version}`,
    fields: {
      "Metrics Key": `${fk}|metrics|v${version}`,
      "Property ID": propertyId,
      "Period ID": measurementPeriodId,
      "Measurement Family": family,
      "Historical Version": version,
      "Portfolio Lens ID": structured.lensId,
      "Portfolio Lens Label": structured.lensLabel,
      "Peer Set ID": structured.peerSetId,
      "Peer Set Version": structured.peerSetVersion,
      "Peer Set Hash": structured.peerSetHash,
      "Portfolio AI Presence": pm.portfolioAiPresence,
      "Portfolio AI Presence Numerator": pm.portfolioAiPresenceNumerator,
      "Portfolio AI Presence Denominator": pm.portfolioAiPresenceDenominator,
      "Portfolio Rank": pm.portfolioRank,
      "Portfolio Rank Of": pm.portfolioRankOf,
      "Portfolio Benchmark": pm.portfolioBenchmark, // null when suppressed
      "Portfolio Presence Index": pm.portfolioPresenceIndex, // null when suppressed
      "Number One Appearance Rate": pm.numberOneAppearanceRate,
      "Top Three Appearance Rate": pm.topThreeAppearanceRate,
      "Portfolio Scenario Presence": pm.portfolioScenarioPresence,
      "Portfolio Scenario Presence Role": pm.portfolioScenarioPresenceRole,
      "Rank Movement State": structured.rankMovementState || "INITIAL",
      "Certification Status": snapshot.certificationStatus,
      "Disclosures JSON": JSON.stringify(snapshot.disclosures || []),
    },
  });

  for (const t of structured.territories || []) {
    records.push({
      table: HISTORY_TABLES.TERRITORY_METRICS,
      idempotencyKey: `${fk}|${t.territoryId}|v${version}`,
      fields: {
        "Territory Key": `${fk}|${t.territoryId}|v${version}`,
        "Property ID": propertyId,
        "Period ID": measurementPeriodId,
        "Measurement Family": family,
        "Territory ID": t.territoryId,
        "Territory Name": t.name || t.territoryId,
        Numerator: t.numerator,
        Denominator: t.denominator,
        "AI Presence Pct": t.aiPresence != null ? Math.round(t.aiPresence * 1000) / 10 : null,
        Benchmark: t.benchmark != null ? Math.round(t.benchmark * 1000) / 10 : null,
        "Presence Index": t.presenceIndex,
        Rank: t.rank,
        "Scenario Count": t.scenarioCount,
        "Peer Set Version": t.peerSetVersion,
        "Historical Version": version,
      },
    });

    // Full territory ranking universe
    for (const e of t.rankingUniverse || []) {
      const entityId = e.canonicalEntityId;
      if (!entityId) continue;
      records.push({
        table: HISTORY_TABLES.COMPETITIVE_RANKINGS,
        idempotencyKey: `${fk}|${t.territoryId}|${entityId}|v${version}`,
        fields: {
          "Rank Key": `${fk}|${t.territoryId}|${entityId}|v${version}`,
          "Property ID": propertyId,
          "Period ID": measurementPeriodId,
          "Measurement Family": family,
          Scope: t.territoryId,
          "Ranking Universe Type": "BRAND_PORTFOLIO",
          "Portfolio Lens": structured.lensLabel,
          "Portfolio Type": snapshot.customerPayload?.lens?.portfolioType || null,
          "Peer Set Version": structured.peerSetVersion,
          "Peer Set ID": structured.peerSetId,
          "Peer Set Hash": structured.peerSetHash,
          "Canonical Entity ID": entityId,
          "Display Name At That Time": e.name || entityId,
          Brand: e.brand || null,
          Numerator: e.numerator ?? e.obsHits ?? null,
          Denominator: e.denominator ?? e.obsDenom ?? null,
          "AI Presence Pct": e.presencePct ?? null,
          Rank: e.rank ?? null,
          "Tie State": e.tieState || null,
          "Is Subject": Boolean(e.isSubject),
          "Historical Version": version,
        },
      });
    }
  }

  for (const p of structured.providers || []) {
    records.push({
      table: HISTORY_TABLES.PROVIDER_METRICS,
      idempotencyKey: `${fk}|${p.providerId}|v${version}`,
      fields: {
        "Provider Key": `${fk}|${p.providerId}|v${version}`,
        "Property ID": propertyId,
        "Period ID": measurementPeriodId,
        "Measurement Family": family,
        "Provider ID": p.providerId,
        "Provider Name": p.name || p.providerId,
        Numerator: p.numerator,
        Denominator: p.denominator,
        "Presence Pct": p.presenceRate != null ? Math.round(p.presenceRate * 1000) / 10 : null,
        "Historical Version": version,
      },
    });
  }

  for (const e of structured.rankingOverall || []) {
    const entityId = e.canonicalEntityId;
    if (!entityId) continue;
    records.push({
      table: HISTORY_TABLES.COMPETITIVE_RANKINGS,
      idempotencyKey: `${fk}|overall|${entityId}|v${version}`,
      fields: {
        "Rank Key": `${fk}|overall|${entityId}|v${version}`,
        "Property ID": propertyId,
        "Period ID": measurementPeriodId,
        "Measurement Family": family,
        Scope: "overall",
        "Ranking Universe Type": "BRAND_PORTFOLIO",
        "Portfolio Lens": structured.lensLabel,
        "Peer Set Version": structured.peerSetVersion,
        "Peer Set ID": structured.peerSetId,
        "Peer Set Hash": structured.peerSetHash,
        "Canonical Entity ID": entityId,
        "Display Name At That Time": e.displayName || entityId,
        Brand: e.brand || null,
        Numerator: e.numerator,
        Denominator: e.denominator,
        "AI Presence Pct": e.presencePct,
        Rank: e.rank,
        "Tie State": e.tieState || null,
        "Is Subject": Boolean(e.isSubject),
        "Historical Version": version,
      },
    });
  }

  const evidenceBlobs = [];
  const lanes = [
    ["positive", structured.evidence?.positive || []],
    ["missing", structured.evidence?.missing || []],
    ["displacement", structured.evidence?.displacement || []],
  ];
  for (const [lane, items] of lanes) {
    for (const ev of items) {
      const observationId =
        ev.observationId || hashShort(`${lane}|${ev.provider}|${measurementPeriodId}`);
      const response = ev.aiResponse || "";
      const responseHash = hashShort(response);
      const storeRef = `bpp-evidence-blobs/${propertyId}/${measurementPeriodId}/${observationId}.txt`;
      evidenceBlobs.push({ storeRef, response, observationId });
      records.push({
        table: HISTORY_TABLES.EVIDENCE_INDEX,
        idempotencyKey: `${fk}|${observationId}`,
        fields: {
          "Evidence Key": `${fk}|${observationId}`,
          "Property ID": propertyId,
          "Period ID": measurementPeriodId,
          "Measurement Family": family,
          "Observation ID": observationId,
          "Evidence Lane": lane,
          Provider: ev.provider || null,
          Territory: ev.territory || null,
          "Subject Present": true,
          "Subject Rank": ev.position ?? null,
          "Response Store Ref": storeRef,
          "Response Length": response.length,
          "Response Content Hash": responseHash,
          "Mention Spans JSON": "[]",
          "Competitors JSON": "[]",
          "Citations JSON": "[]",
        },
      });

      // Prompt ledger index (exact prompt on disk / manifest — not recomputed)
      records.push({
        table: HISTORY_TABLES.PROMPT_LEDGER,
        idempotencyKey: `${fk}|prompt|${observationId}`,
        fields: {
          "Prompt Ledger Key": `${fk}|prompt|${observationId}`,
          "Observation ID": observationId,
          "Property ID": propertyId,
          "Period ID": measurementPeriodId,
          "Measurement Family": family,
          "Scenario ID": ev.scenarioId || null,
          Provider: ev.provider || null,
          Territory: ev.territory || null,
          "Scenario Class": "BRAND_PORTFOLIO_DEMAND",
          "Prompt Hash": snapshot.promptManifestHash,
          "Exact Prompt Preview": "",
          "Measurement Eligibility": true,
          "Integrity Classification": "FROZEN_FIRST_CYCLE",
          "Immutable Observation Ref": storeRef,
        },
      });
    }
  }

  // Correction lineage — original never published → corrected → customer published
  records.push({
    table: HISTORY_TABLES.REPORT_CORRECTIONS,
    idempotencyKey: `corr|bpp|${propertyId}|${CORRECTION_LINEAGE_STAGES.ORIGINAL_FIRST_CYCLE_CANDIDATE}|${CORRECTION_LINEAGE_STAGES.CORRECTED_FIRST_CYCLE_CANDIDATE}`,
    fields: {
      "Correction ID": `corr|bpp|${propertyId}|original_to_corrected`,
      "Property ID": propertyId,
      "Period ID": measurementPeriodId,
      "Measurement Family": family,
      "Lineage Stage": CORRECTION_LINEAGE_STAGES.CORRECTED_FIRST_CYCLE_CANDIDATE,
      "Original Snapshot ID": `${CORRECTION_LINEAGE_STAGES.ORIGINAL_FIRST_CYCLE_CANDIDATE}:${propertyId}`,
      "Corrected Snapshot ID": `${CORRECTION_LINEAGE_STAGES.CORRECTED_FIRST_CYCLE_CANDIDATE}:${propertyId}`,
      Reason: CORRECTION_REASON,
      "Changed Fields JSON": JSON.stringify([
        "portfolioAiPresence grain PROVIDER_OBSERVATION",
        "portfolioRank",
        "portfolioBenchmark",
        "portfolioPresenceIndex",
      ]),
      "Authorized By": "FOUNDER_METRIC_CONTRACT_V1_1",
      "Corrected At": snapshot.publishedAt,
      "Version Number": 1,
    },
  });
  records.push({
    table: HISTORY_TABLES.REPORT_CORRECTIONS,
    idempotencyKey: `corr|bpp|${propertyId}|${CORRECTION_LINEAGE_STAGES.CUSTOMER_PUBLISHED_BASELINE}`,
    fields: {
      "Correction ID": `corr|bpp|${propertyId}|customer_published`,
      "Property ID": propertyId,
      "Period ID": measurementPeriodId,
      "Measurement Family": family,
      "Lineage Stage": CORRECTION_LINEAGE_STAGES.CUSTOMER_PUBLISHED_BASELINE,
      "Original Snapshot ID": `${CORRECTION_LINEAGE_STAGES.CORRECTED_FIRST_CYCLE_CANDIDATE}:${propertyId}`,
      "Corrected Snapshot ID": snapshot.snapshotId,
      Reason: "CUSTOMER_PUBLICATION_OF_CORRECTED_BASELINE",
      "Changed Fields JSON": JSON.stringify(["customerPublished=true", "publicationVersion"]),
      "Authorized By": "FOUNDER_CUSTOMER_PUBLICATION_GO",
      "Corrected At": snapshot.publishedAt,
      "Version Number": 2,
    },
  });

  const byTable = {};
  for (const r of records) byTable[r.table] = (byTable[r.table] || 0) + 1;

  return {
    ok: defects.length === 0,
    defects,
    propertyId,
    calendarWeekId,
    measurementPeriodId,
    measurementFamily: family,
    snapshotId: snapshot.snapshotId,
    contentHash: snapshot.contentHash,
    peerSetHash: snapshot.peerSetHash,
    historicalVersion: version,
    records,
    evidenceBlobs,
    snapshotBlob: {
      storeRef: snapshotStoreRef,
      json: snapshot,
      contentHash: snapshot.contentHash,
    },
    counts: byTable,
    recordCount: records.length,
    estimatedAirtableBatches: Math.ceil(records.length / 10),
    idempotencyKeys: BPP_IDEMPOTENCY_KEYS,
    synthetic,
  };
}

function bppWritesAllowed({ allowProductionWrites = false, useMock = true } = {}) {
  if (useMock) return { allowed: true, mode: "MOCK" };
  if (!isAdpHistoryWritesEnabled()) {
    return { allowed: false, mode: "DISABLED", reason: "ADP_HISTORY_WRITES_ENABLED=false" };
  }
  if (process.env[HISTORY_ENV.ADP_HISTORY_AIRTABLE_WRITE_APPLY] !== "true") {
    return { allowed: false, mode: "DISABLED", reason: "ADP_HISTORY_AIRTABLE_WRITE_APPLY not true" };
  }
  if (!allowProductionWrites) {
    return { allowed: false, mode: "DISABLED", reason: "allowProductionWrites=false" };
  }
  if (process.env[HISTORY_ENV.ADP_HISTORY_AIRTABLE_BASE_ID]) {
    return { allowed: true, mode: "PRODUCTION" };
  }
  if (!process.env[HISTORY_ENV.ADP_HISTORY_SANDBOX_BASE_ID]) {
    return {
      allowed: false,
      mode: "DISABLED",
      reason: "No ADP_HISTORY_AIRTABLE_BASE_ID or ADP_HISTORY_SANDBOX_BASE_ID",
    };
  }
  return { allowed: true, mode: "SANDBOX" };
}

/**
 * Persist one BPP property period (dryRun | mock).
 */
export async function persistBppHistoricalPeriodV1(input = {}) {
  const {
    dryRun = true,
    useMock = true,
    allowProductionWrites = false,
    correctionMode = false,
    snapshot,
    structured,
    synthetic = false,
  } = input;

  const gate = bppWritesAllowed({ allowProductionWrites, useMock });
  const manifest = buildBppHistoricalWriteManifest({ snapshot, structured, synthetic });

  const result = {
    version: PERSIST_BPP_HISTORICAL_PERIOD_VERSION,
    dryRun,
    writeMode: dryRun ? "DRY_RUN" : gate.mode,
    productionWritesEnabled: false,
    ok: manifest.ok,
    defects: [...manifest.defects],
    manifest: {
      recordCount: manifest.recordCount,
      counts: manifest.counts,
      estimatedAirtableBatches: manifest.estimatedAirtableBatches,
      snapshotId: manifest.snapshotId,
      propertyId: manifest.propertyId,
      measurementPeriodId: manifest.measurementPeriodId,
      measurementFamily: manifest.measurementFamily,
      contentHash: manifest.contentHash,
      peerSetHash: manifest.peerSetHash,
    },
    persistenceState: PERSISTENCE_STATES.PENDING,
    recordsCreated: 0,
    idempotentSkips: 0,
    snapshotVerification: null,
    structuredHistoryVerification: null,
  };

  if (!manifest.ok) {
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    return result;
  }

  if (dryRun) {
    result.ok = true;
    result.dryRunManifest = manifest;
    return result;
  }

  if (!gate.allowed) {
    result.ok = false;
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    result.defects.push({
      code: REPORT_HISTORY_PERSISTENCE_COMPLETE,
      detail: `writes blocked: ${gate.reason}`,
    });
    return result;
  }

  result.persistenceState = PERSISTENCE_STATES.WRITING;
  const store = new MockAdpHistoryStore(BPP_MOCK_HISTORY_ROOT);
  const periodKeyV = `${familyKey(manifest.propertyId, manifest.measurementPeriodId, MEASUREMENT_FAMILY.BRAND_PORTFOLIO)}|v${manifest.historicalVersion}`;

  mkdirSync(join(BPP_MOCK_HISTORY_ROOT, "bpp-snapshot-blobs", manifest.propertyId), {
    recursive: true,
  });
  mkdirSync(
    join(BPP_MOCK_HISTORY_ROOT, "bpp-evidence-blobs", manifest.propertyId, manifest.measurementPeriodId),
    { recursive: true }
  );

  if (manifest.snapshotBlob?.json) {
    const path = join(BPP_MOCK_HISTORY_ROOT, manifest.snapshotBlob.storeRef);
    mkdirSync(join(path, ".."), { recursive: true });
    writeFileSync(path, JSON.stringify(manifest.snapshotBlob.json, null, 2) + "\n");
  }
  for (const blob of manifest.evidenceBlobs) {
    const path = join(BPP_MOCK_HISTORY_ROOT, blob.storeRef);
    mkdirSync(join(path, ".."), { recursive: true });
    writeFileSync(path, blob.response || "", "utf8");
  }

  const write = store.writeRecords(manifest.records, {
    correctionMode,
    periodKeyV,
    markComplete: false,
  });
  if (write.errors.length) {
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    result.ok = false;
    result.defects.push(...write.errors);
    return result;
  }

  result.recordsCreated = write.created.length;
  result.recordsUpdated = write.updated.length;
  result.idempotentSkips = write.skipped.length;
  result.persistenceState = PERSISTENCE_STATES.VERIFYING;

  const snapPath = join(BPP_MOCK_HISTORY_ROOT, manifest.snapshotBlob.storeRef);
  const loaded = JSON.parse(readFileSync(snapPath, "utf8"));
  const h = sha256OfPayload(loaded.customerPayload);
  const snapOk = h === manifest.contentHash;
  result.snapshotVerification = {
    ok: snapOk,
    contentHash: h,
    expected: manifest.contentHash,
    gate: BRAND_PORTFOLIO_HISTORICAL_REPORT_REPRODUCTION_INTEGRITY,
  };

  const stored = store.query(
    (r) =>
      r.fields["Property ID"] === manifest.propertyId &&
      (r.fields["Period ID"] === manifest.measurementPeriodId ||
        r.fields["Period ID"] === manifest.calendarWeekId) &&
      (r.fields["Measurement Family"] === MEASUREMENT_FAMILY.BRAND_PORTFOLIO ||
        r.table === HISTORY_TABLES.MONITORING_PERIODS)
  );
  result.structuredHistoryVerification = {
    ok: stored.length >= manifest.recordCount,
    storedCount: stored.length,
    expected: manifest.recordCount,
  };

  // Family isolation: no CORE collision on same keys
  const coreCollision = store.query(
    (r) =>
      r.fields["Measurement Family"] === MEASUREMENT_FAMILY.CORE &&
      r.fields["Property ID"] === manifest.propertyId &&
      r.fields["Period ID"] === manifest.measurementPeriodId
  );
  result.familyIsolationVerification = {
    ok: coreCollision.length === 0,
    gate: MEASUREMENT_FAMILY_HISTORY_ISOLATION,
  };

  if (
    result.snapshotVerification.ok &&
    result.structuredHistoryVerification.ok &&
    result.familyIsolationVerification.ok &&
    result.defects.length === 0
  ) {
    store.state.finalizedKeys[periodKeyV] = true;
    store._save();
    result.persistenceState = PERSISTENCE_STATES.COMPLETE;
    result.ok = true;
    result.reportHistoryPersistenceComplete = true;
  } else {
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    result.ok = false;
    result.reportHistoryPersistenceComplete = false;
  }

  return result;
}

/**
 * Dry-run all five published properties + optional mock persist for synthetic proof.
 */
export async function runBppPeriod1HistoryPersistence({
  dryRun = true,
  useMock = false,
  writeFilesystemFreeze = true,
  propertyIds = null,
} = {}) {
  const { freezeBundle, frozen } = freezeCustomerPublishedBppPeriod1({
    writeFilesystem: writeFilesystemFreeze,
  });
  const selected = propertyIds
    ? frozen.filter((f) => propertyIds.includes(f.propertyId))
    : frozen;

  const perProperty = [];
  for (const f of selected) {
    const res = await persistBppHistoricalPeriodV1({
      dryRun,
      useMock: useMock && !dryRun,
      snapshot: f.snapshot,
      structured: f.structured,
      synthetic: false,
    });
    perProperty.push(res);
  }

  const totals = {};
  for (const r of perProperty) {
    const counts = r.manifest?.counts || r.dryRunManifest?.counts || {};
    for (const [t, n] of Object.entries(counts)) totals[t] = (totals[t] || 0) + n;
  }

  return {
    ok: perProperty.every((r) => r.ok),
    freezeBundle,
    perProperty,
    totals,
    recordCount: perProperty.reduce((s, r) => s + (r.manifest?.recordCount || 0), 0),
    productionWriteAuthorized: false,
    productionWriteBlockedReason:
      "ADP_HISTORY_WRITES_ENABLED=false — founder GO required for Airtable history write",
  };
}

export function evaluateBppPeriod2HistoryReady(checks = {}) {
  const required = [
    "period1Frozen",
    "immutableSnapshotVerified",
    "structuredMetricsPersisted",
    "providerHistoryPersisted",
    "territoryHistoryPersisted",
    "completeRankHistoryPersisted",
    "peerSetHistoryPersisted",
    "promptEvidenceTraceability",
    "idempotencyPass",
    "noSilentOverwritePass",
    "currentReportIsolationPass",
    "productionHistoryPersistedOrExplicitlyDeferred",
  ];
  const blockers = required.filter((k) => !checks[k]);
  return {
    gate: BRAND_PORTFOLIO_REAL_PERIOD_2_HISTORY_READY,
    pass: blockers.length === 0,
    blockers,
    checks,
  };
}

export function evaluateBppPeriod2ExecutionReady(checks = {}) {
  const required = [
    "historyReady",
    "methodologyStillValid",
    "peerSetsRevalidatedForNewPeriod",
    "promptManifestReady",
    "providerRecoveryPathReady",
    "assuranceStackReady",
  ];
  const blockers = required.filter((k) => !checks[k]);
  return {
    gate: BRAND_PORTFOLIO_REAL_PERIOD_2_READY,
    pass: blockers.length === 0,
    blockers,
    checks,
    note: "Do not run Period 2 until PASS + founder execution GO",
  };
}

/** Re-export store helper for overwrite tests */
export {
  MockAdpHistoryStore,
  HISTORICAL_RECORD_NO_SILENT_OVERWRITE,
  DEFECT_HISTORICAL_RECORD_SILENT_OVERWRITE,
};
