/**
 * persistAdpHistoricalPeriodV1 — production-ready adapter, WRITES DISABLED by default.
 *
 * Does NOT publish reports.
 * Does NOT enable ADP_AIRTABLE_READ_LIVE.
 * Production Airtable mutation requires ADP_HISTORY_AIRTABLE_WRITE_APPLY=true
 * AND explicit allowProductionWrites (still blocked unless sandbox/history base).
 *
 * Large blobs (full snapshot JSON, verbatim LLM responses) → filesystem/object refs.
 * Airtable (future) → structured index rows only.
 */

import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import {
  ADP_HISTORY_WRITES_ENABLED,
  HISTORY_TABLES,
  PERSISTENCE_STATES,
  IDEMPOTENCY_KEYS,
  SNAPSHOT_STORAGE_STRATEGY,
  HISTORY_ENV,
  EXISTING_LIVE_PUBLISH,
} from "./airtable-history-schema-final-v1.js";
import { auditReportSnapshotCompleteness } from "./customer-surface-persistence-registry-v1.js";
import { sha256OfPayload, stableStringify } from "./report-snapshot-v1.js";

export const PERSIST_ADP_HISTORICAL_PERIOD_VERSION = "persist_adp_historical_period_v1";

export const REPORT_HISTORY_PERSISTENCE_COMPLETE = "REPORT_HISTORY_PERSISTENCE_COMPLETE";
export const HISTORICAL_RECORD_NO_SILENT_OVERWRITE = "HISTORICAL_RECORD_NO_SILENT_OVERWRITE";
export const HISTORY_SNAPSHOT_PERIOD_IDENTITY_INTEGRITY =
  "HISTORY_SNAPSHOT_PERIOD_IDENTITY_INTEGRITY";
export const AIRTABLE_HISTORY_IDEMPOTENCY_INTEGRITY = "AIRTABLE_HISTORY_IDEMPOTENCY_INTEGRITY";
export const AIRTABLE_HISTORY_SCHEMA_CAPABILITY_INTEGRITY =
  "AIRTABLE_HISTORY_SCHEMA_CAPABILITY_INTEGRITY";
export const REAL_SECOND_PERIOD_HISTORY_PERSISTENCE_READY =
  "REAL_SECOND_PERIOD_HISTORY_PERSISTENCE_READY";

export const DEFECT_PARTIAL_HISTORY_PERSISTENCE = "PARTIAL_HISTORY_PERSISTENCE";
export const DEFECT_HISTORICAL_RECORD_DUPLICATION = "HISTORICAL_RECORD_DUPLICATION";
export const DEFECT_HISTORICAL_RECORD_SILENT_OVERWRITE = "HISTORICAL_RECORD_SILENT_OVERWRITE";
export const DEFECT_SNAPSHOT_PERIOD_IDENTITY_MISMATCH = "SNAPSHOT_PERIOD_IDENTITY_MISMATCH";
export const DEFECT_AIRTABLE_HISTORY_SCHEMA_DRIFT = "AIRTABLE_HISTORY_SCHEMA_DRIFT";

/** Local mock / synthetic historical store (never production Airtable). */
export const MOCK_HISTORY_ROOT = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/longitudinal-synthetic/history-store"
);

export const EVIDENCE_BLOB_ROOT = join(MOCK_HISTORY_ROOT, "evidence-blobs");
export const SNAPSHOT_BLOB_ROOT = join(MOCK_HISTORY_ROOT, "snapshot-blobs");

function hashShort(s) {
  return createHash("sha256").update(String(s)).digest("hex").slice(0, 16);
}

function periodKey(propertyId, periodId) {
  return `${propertyId}|${periodId}`;
}

/**
 * Build intended structured records from certified inputs (no I/O mutation).
 */
export function buildHistoricalWriteManifest({
  propertyId,
  periodId,
  monitoringDate,
  certificationStatus,
  reportSnapshot,
  customerPayload,
  rankingPack = null,
  evidencePack = null,
  historicalVersion = 1,
  correctionMode = false,
  correctsSnapshotId = null,
  synthetic = true,
}) {
  const defects = [];
  const snap = reportSnapshot;
  const payload = customerPayload || snap?.customerPayload;
  if (!propertyId || !periodId) {
    defects.push({ code: HISTORY_SNAPSHOT_PERIOD_IDENTITY_INTEGRITY, detail: "missing property/period" });
  }
  if (snap && (snap.propertyId !== propertyId || snap.periodId !== periodId)) {
    defects.push({
      code: DEFECT_SNAPSHOT_PERIOD_IDENTITY_MISMATCH,
      detail: "snapshot identity !== period identity",
    });
  }

  const completeness = auditReportSnapshotCompleteness(payload || {});
  if (!completeness.pass) {
    defects.push(...completeness.defects);
  }

  const version = historicalVersion;
  const records = [];

  records.push({
    table: HISTORY_TABLES.MONITORING_PERIODS,
    idempotencyKey: periodKey(propertyId, periodId) + `|v${version}`,
    fields: {
      "Period Key": `${periodKey(propertyId, periodId)}|v${version}`,
      "Property ID": propertyId,
      "Period ID": periodId,
      "Snapshot ID": snap?.snapshotId || null,
      "Monitoring Date": monitoringDate,
      "Certification Timestamp": snap?.certifiedAt || null,
      "Publication Timestamp": snap?.publishedAt || null,
      "Certification Status": certificationStatus,
      "Report Schema Version": snap?.schema || snap?.versions?.reportVersion || null,
      "Persistence State": PERSISTENCE_STATES.PENDING,
      "Content Hash": snap?.contentHash || null,
      "Envelope Hash": snap?.envelopeHash || null,
      "Historical Version": version,
      "Is Current Historical Version": true,
      "Corrects Period Key": correctsSnapshotId || "",
      Synthetic: Boolean(synthetic),
    },
  });

  const snapshotStoreRef = snap?.snapshotId
    ? `snapshot-blobs/${propertyId}/${snap.snapshotId}.json`
    : null;

  records.push({
    table: HISTORY_TABLES.REPORT_SNAPSHOTS,
    idempotencyKey: snap?.snapshotId || `missing_snap_${periodId}`,
    fields: {
      "Snapshot ID": snap?.snapshotId,
      "Property ID": propertyId,
      "Period ID": periodId,
      "Monitoring Date": monitoringDate,
      "Schema Version": snap?.schema,
      "Content Hash": snap?.contentHash,
      "Envelope Hash": snap?.envelopeHash,
      "Publication Commit": snap?.versions?.publicationCommit || null,
      "Renderer Version": snap?.versions?.rendererVersion || null,
      "Measurement Contract Version": snap?.versions?.measurementContractVersion || null,
      "Parser Version": snap?.versions?.parserVersion || null,
      "Entity Resolver Version": snap?.versions?.entityResolverVersion || null,
      "Ranking Version": snap?.versions?.rankingVersion || null,
      "Evidence Version": snap?.versions?.evidenceVersion || null,
      "Assurance Version": snap?.versions?.assuranceVersion || null,
      "Certification Status": certificationStatus,
      "Correction Version": version,
      "Is Current Historical Version": true,
      "Snapshot Store Ref": snapshotStoreRef,
      Synthetic: Boolean(synthetic),
    },
  });

  const kpis = payload?.kpis || {};
  records.push({
    table: HISTORY_TABLES.PERIOD_METRICS,
    idempotencyKey: `${periodKey(propertyId, periodId)}|metrics|v${version}`,
    fields: {
      "Metrics Key": `${periodKey(propertyId, periodId)}|metrics|v${version}`,
      "Property ID": propertyId,
      "Period ID": periodId,
      "Historical Version": version,
      "Reality Coverage": kpis.realityCoverage ?? null,
      "Scenario Presence": kpis.scenarioPresence ?? null,
      "Scenario Presence Numerator": kpis.scenarioPresenceNumerator ?? null,
      "Scenario Presence Denominator": kpis.scenarioPresenceDenominator ?? null,
      "Demand Capture": kpis.demandCapture ?? null,
      "Consideration Rate": kpis.considerationRate ?? null,
      "Consideration Numerator": kpis.considerationNumerator ?? null,
      "Consideration Denominator": kpis.considerationDenominator ?? null,
      "Number One Appearance Rate": kpis.numberOneAppearanceRate ?? null,
      "Top Three Appearance Rate": kpis.topThreeAppearanceRate ?? null,
      "Presence Index": kpis.presenceIndex ?? null,
      "CORE Benchmark": kpis.coreBenchmark ?? null,
      "CORE Count": Array.isArray(kpis.coreComposition) ? kpis.coreComposition.length : null,
      "Certification Status": certificationStatus,
      "Disclosures JSON": JSON.stringify(snap?.disclosures || []),
    },
  });

  for (const t of payload?.territories || []) {
    records.push({
      table: HISTORY_TABLES.TERRITORY_METRICS,
      idempotencyKey: `${periodKey(propertyId, periodId)}|${t.territoryId}|v${version}`,
      fields: {
        "Territory Key": `${periodKey(propertyId, periodId)}|${t.territoryId}|v${version}`,
        "Property ID": propertyId,
        "Period ID": periodId,
        "Territory ID": t.territoryId,
        "Territory Name": t.name || t.territoryId,
        Numerator: t.numerator ?? null,
        Denominator: t.denominator ?? null,
        "AI Presence Pct": t.aiPresence ?? null,
        Benchmark: t.coreBenchmark ?? null,
        "Presence Index": t.presenceIndex ?? null,
        Rank: t.rank ?? null,
        "Missing Evidence Count": t.missingEvidenceCount ?? null,
        "Positive Evidence Eligible Count": t.positiveEvidenceEligibleCount ?? null,
        "Historical Version": version,
      },
    });
  }

  for (const p of payload?.providers || []) {
    records.push({
      table: HISTORY_TABLES.PROVIDER_METRICS,
      idempotencyKey: `${periodKey(propertyId, periodId)}|${p.providerId}|v${version}`,
      fields: {
        "Provider Key": `${periodKey(propertyId, periodId)}|${p.providerId}|v${version}`,
        "Property ID": propertyId,
        "Period ID": periodId,
        "Provider ID": p.providerId,
        "Provider Name": p.name || p.providerId,
        Numerator: p.numerator ?? null,
        Denominator: p.denominator ?? null,
        "Presence Pct": p.presenceRate ?? null,
        "Missing Count": p.missingCount ?? null,
        "Completeness State": p.completeness || null,
        "Recovery State": p.recoveryState || null,
        "Historical Version": version,
      },
    });
  }

  // Rankings: prefer rankingPack.byScope, else payload competitive overview (overall only)
  const scopes =
    rankingPack?.byScope ||
    (payload?.competitive?.overview
      ? {
          overall: {
            entities: (payload.competitive.overview.entities || []).map((e) => ({
              entityId: e.entityId,
              displayName: e.displayName,
              rank: e.rank,
              numerator: e.numerator,
              denominator: e.denominator,
              aiPresencePct: e.presencePct,
              tieState: e.tieState,
            })),
          },
        }
      : {});

  for (const [scope, scopeData] of Object.entries(scopes)) {
    const entities = scopeData.entities || scopeData.rows || [];
    for (const e of entities) {
      const entityId = e.entityId || e.canonicalEntityId;
      if (!entityId) continue;
      records.push({
        table: HISTORY_TABLES.COMPETITIVE_RANKINGS,
        idempotencyKey: `${periodKey(propertyId, periodId)}|${scope}|${entityId}|v${version}`,
        fields: {
          "Rank Key": `${periodKey(propertyId, periodId)}|${scope}|${entityId}|v${version}`,
          "Property ID": propertyId,
          "Period ID": periodId,
          Scope: scope,
          "Canonical Entity ID": entityId,
          "Display Name At That Time": e.displayName || e.name || entityId,
          Numerator: e.numerator ?? e.appearances ?? null,
          Denominator: e.denominator ?? null,
          "AI Presence Pct": e.aiPresencePct ?? e.presencePct ?? null,
          Rank: e.rank ?? e.currentRank ?? null,
          "Tie State": e.tieState || null,
          "Historical Version": version,
        },
      });
    }
  }

  const evidenceBlobs = [];
  const lanes = [
    ["positive", payload?.evidence?.positive || evidencePack?.positive || []],
    ["missing", payload?.evidence?.missing || evidencePack?.missing || []],
    ["displacement", payload?.evidence?.displacement || evidencePack?.displacement || []],
  ];
  for (const [lane, items] of lanes) {
    for (const ev of items) {
      const observationId = ev.observationId || hashShort(`${lane}|${ev.provider}|${periodId}`);
      const response = ev.aiResponse || ev.responseExcerpt || "";
      const responseHash = hashShort(response);
      const storeRef = `evidence-blobs/${propertyId}/${periodId}/${observationId}.txt`;
      evidenceBlobs.push({ storeRef, response, observationId });
      records.push({
        table: HISTORY_TABLES.EVIDENCE_INDEX,
        idempotencyKey: `${periodKey(propertyId, periodId)}|${observationId}`,
        fields: {
          "Evidence Key": `${periodKey(propertyId, periodId)}|${observationId}`,
          "Property ID": propertyId,
          "Period ID": periodId,
          "Observation ID": observationId,
          "Evidence Lane": lane,
          Provider: ev.provider || null,
          Territory: ev.territory || null,
          "Subject Present": Boolean(
            ev.subjectStatus === "Appeared" || ev.subjectPresent === true
          ),
          "Subject Rank": ev.rank ?? null,
          "Response Store Ref": storeRef,
          "Response Length": response.length,
          "Response Content Hash": responseHash,
          "Mention Spans JSON": JSON.stringify(ev.subjectMentionSpans || []),
          "Competitors JSON": JSON.stringify(ev.competitors || ev.competitorsAlongside || []),
          "Citations JSON": JSON.stringify(ev.citations || []),
        },
      });
    }
  }

  if (correctionMode && correctsSnapshotId && snap?.snapshotId) {
    records.push({
      table: HISTORY_TABLES.REPORT_CORRECTIONS,
      idempotencyKey: `corr|${correctsSnapshotId}|${snap.snapshotId}`,
      fields: {
        "Correction ID": `corr|${correctsSnapshotId}|${snap.snapshotId}`,
        "Property ID": propertyId,
        "Period ID": periodId,
        "Original Snapshot ID": correctsSnapshotId,
        "Corrected Snapshot ID": snap.snapshotId,
        Reason: snap.correctionReason || "CORRECTION",
        "Changed Fields JSON": JSON.stringify(snap.changedFields || []),
        "Authorized By": snap.correctionAuthorizedBy || "UNKNOWN",
        "Corrected At": snap.correctionAt || snap.generatedAt || null,
        "Version Number": version,
      },
    });
  }

  const byTable = {};
  for (const r of records) {
    byTable[r.table] = (byTable[r.table] || 0) + 1;
  }

  // Airtable batch estimate: 10 records per create request
  const estimatedBatches = Math.ceil(records.length / 10);

  return {
    ok: defects.length === 0,
    defects,
    propertyId,
    periodId,
    snapshotId: snap?.snapshotId || null,
    historicalVersion: version,
    correctionMode,
    completeness,
    records,
    evidenceBlobs,
    snapshotBlob: snap
      ? {
          storeRef: snapshotStoreRef,
          json: snap,
          contentHash: snap.contentHash,
        }
      : null,
    counts: byTable,
    recordCount: records.length,
    estimatedAirtableBatches: estimatedBatches,
    estimatedAirtableOperations: estimatedBatches + 2, // + verify/verify
    storageStrategy: SNAPSHOT_STORAGE_STRATEGY,
    idempotencyKeys: IDEMPOTENCY_KEYS,
    livePublishTableNote: EXISTING_LIVE_PUBLISH,
  };
}

/**
 * Mock historical store — filesystem JSON indexes for synthetic round-trip.
 */
export class MockAdpHistoryStore {
  constructor(root = MOCK_HISTORY_ROOT) {
    this.root = root;
    this.indexPath = join(root, "record-index.json");
    this.state = this._load();
  }

  _load() {
    if (!existsSync(this.indexPath)) {
      return { records: Object.create(null), finalizedKeys: Object.create(null) };
    }
    return JSON.parse(readFileSync(this.indexPath, "utf8"));
  }

  _save() {
    mkdirSync(this.root, { recursive: true });
    writeFileSync(this.indexPath, JSON.stringify(this.state, null, 2) + "\n");
  }

  hasFinalized(periodKeyV) {
    return Boolean(this.state.finalizedKeys[periodKeyV]);
  }

  getByKey(idempotencyKey) {
    return this.state.records[idempotencyKey] || null;
  }

  /**
   * Upsert with no-silent-overwrite for finalized periods outside correctionMode.
   */
  writeRecords(records, { correctionMode = false, periodKeyV = null, markComplete = false } = {}) {
    const created = [];
    const updated = [];
    const skipped = [];
    const errors = [];

    for (const rec of records) {
      const key = rec.idempotencyKey;
      const existing = this.state.records[key];
      if (existing) {
        if (
          this.state.finalizedKeys[periodKeyV] &&
          !correctionMode &&
          stableStringify(existing.fields) !== stableStringify(rec.fields)
        ) {
          errors.push({
            code: DEFECT_HISTORICAL_RECORD_SILENT_OVERWRITE,
            gate: HISTORICAL_RECORD_NO_SILENT_OVERWRITE,
            detail: `would mutate finalized ${key}`,
          });
          continue;
        }
        if (stableStringify(existing.fields) === stableStringify(rec.fields)) {
          skipped.push(key);
          continue;
        }
        if (!correctionMode && this.state.finalizedKeys[periodKeyV]) {
          errors.push({
            code: DEFECT_HISTORICAL_RECORD_SILENT_OVERWRITE,
            detail: key,
          });
          continue;
        }
        this.state.records[key] = { ...rec, updatedAt: new Date().toISOString() };
        updated.push(key);
      } else {
        this.state.records[key] = { ...rec, createdAt: new Date().toISOString() };
        created.push(key);
      }
    }

    if (markComplete && periodKeyV && errors.length === 0) {
      this.state.finalizedKeys[periodKeyV] = true;
    }
    this._save();
    return { created, updated, skipped, errors };
  }

  query(predicate) {
    return Object.values(this.state.records).filter(predicate);
  }

  listPeriods(propertyId) {
    return this.query(
      (r) =>
        r.table === HISTORY_TABLES.MONITORING_PERIODS && r.fields["Property ID"] === propertyId
    ).sort((a, b) =>
      String(a.fields["Monitoring Date"]).localeCompare(String(b.fields["Monitoring Date"]))
    );
  }

  getEntityRanks(propertyId, entityId, scope = "overall") {
    return this.query(
      (r) =>
        r.table === HISTORY_TABLES.COMPETITIVE_RANKINGS &&
        r.fields["Property ID"] === propertyId &&
        r.fields["Canonical Entity ID"] === entityId &&
        r.fields.Scope === scope
    ).sort((a, b) =>
      String(a.fields["Period ID"]).localeCompare(String(b.fields["Period ID"]))
    );
  }
}

function writesAllowed({ allowProductionWrites = false, useMock = true } = {}) {
  if (useMock) return { allowed: true, mode: "MOCK" };
  if (!ADP_HISTORY_WRITES_ENABLED) {
    return { allowed: false, mode: "DISABLED", reason: "ADP_HISTORY_WRITES_ENABLED=false" };
  }
  if (process.env[HISTORY_ENV.ADP_HISTORY_AIRTABLE_WRITE_APPLY] !== "true") {
    return {
      allowed: false,
      mode: "DISABLED",
      reason: "ADP_HISTORY_AIRTABLE_WRITE_APPLY not true",
    };
  }
  if (!allowProductionWrites) {
    return { allowed: false, mode: "DISABLED", reason: "allowProductionWrites=false" };
  }
  // Still refuse unless sandbox/history base explicitly set
  if (!process.env[HISTORY_ENV.ADP_HISTORY_SANDBOX_BASE_ID]) {
    return {
      allowed: false,
      mode: "DISABLED",
      reason: "No ADP_HISTORY_SANDBOX_BASE_ID — refuse production destination",
    };
  }
  return { allowed: true, mode: "SANDBOX" };
}

/**
 * Persist historical period — dryRun | mock | (future sandbox).
 */
export async function persistAdpHistoricalPeriodV1(input = {}) {
  const {
    dryRun = true,
    useMock = true,
    allowProductionWrites = false,
    correctionMode = false,
    ...manifestInput
  } = input;

  const gate = writesAllowed({ allowProductionWrites, useMock: useMock && !dryRun ? true : useMock });
  const manifest = buildHistoricalWriteManifest({ ...manifestInput, correctionMode });

  const result = {
    version: PERSIST_ADP_HISTORICAL_PERIOD_VERSION,
    dryRun,
    writeMode: dryRun ? "DRY_RUN" : gate.mode,
    productionWritesEnabled: false,
    publishPerformed: false,
    ok: manifest.ok,
    defects: [...manifest.defects],
    manifest: {
      recordCount: manifest.recordCount,
      counts: manifest.counts,
      estimatedAirtableBatches: manifest.estimatedAirtableBatches,
      estimatedAirtableOperations: manifest.estimatedAirtableOperations,
      snapshotId: manifest.snapshotId,
      periodId: manifest.periodId,
      propertyId: manifest.propertyId,
    },
    persistenceState: PERSISTENCE_STATES.PENDING,
    recordsAttempted: manifest.recordCount,
    recordsCreated: 0,
    recordsUpdated: 0,
    idempotentSkips: 0,
    snapshotVerification: null,
    structuredHistoryVerification: null,
  };

  if (!manifest.ok) {
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    result.defects.push({
      code: DEFECT_PARTIAL_HISTORY_PERSISTENCE,
      detail: "manifest invalid — refuse persist",
    });
    return result;
  }

  if (dryRun) {
    result.persistenceState = PERSISTENCE_STATES.PENDING;
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

  // MOCK path only in this task
  result.persistenceState = PERSISTENCE_STATES.WRITING;
  const store = new MockAdpHistoryStore();
  const periodKeyV = `${periodKey(manifest.propertyId, manifest.periodId)}|v${manifest.historicalVersion}`;

  // No silent overwrite if already finalized
  if (store.hasFinalized(periodKeyV) && !correctionMode) {
    // Idempotent re-run: rewrite same records should skip
  }

  // Persist blobs first
  mkdirSync(join(MOCK_HISTORY_ROOT, "snapshot-blobs", manifest.propertyId), { recursive: true });
  mkdirSync(join(EVIDENCE_BLOB_ROOT, manifest.propertyId, manifest.periodId), { recursive: true });
  if (manifest.snapshotBlob?.json) {
    const path = join(MOCK_HISTORY_ROOT, manifest.snapshotBlob.storeRef);
    mkdirSync(join(path, ".."), { recursive: true });
    if (!existsSync(path)) {
      writeFileSync(path, JSON.stringify(manifest.snapshotBlob.json, null, 2) + "\n");
    }
  }
  for (const blob of manifest.evidenceBlobs) {
    const path = join(MOCK_HISTORY_ROOT, blob.storeRef);
    mkdirSync(join(path, ".."), { recursive: true });
    writeFileSync(path, blob.response, "utf8");
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
    result.defects.push({
      code: DEFECT_PARTIAL_HISTORY_PERSISTENCE,
      detail: "write errors — not COMPLETE",
    });
    return result;
  }

  result.recordsCreated = write.created.length;
  result.recordsUpdated = write.updated.length;
  result.idempotentSkips = write.skipped.length;
  result.persistenceState = PERSISTENCE_STATES.VERIFYING;

  // Verify
  const snapPath = manifest.snapshotBlob
    ? join(MOCK_HISTORY_ROOT, manifest.snapshotBlob.storeRef)
    : null;
  let snapOk = false;
  if (snapPath && existsSync(snapPath)) {
    const loaded = JSON.parse(readFileSync(snapPath, "utf8"));
    const h = sha256OfPayload(loaded.customerPayload);
    snapOk = h === manifest.snapshotBlob.contentHash;
    result.snapshotVerification = {
      ok: snapOk,
      contentHash: h,
      expected: manifest.snapshotBlob.contentHash,
    };
    if (!snapOk) {
      result.defects.push({
        code: HISTORY_SNAPSHOT_PERIOD_IDENTITY_INTEGRITY,
        detail: "snapshot hash mismatch after persist",
      });
    }
  }

  const storedCount = store.query(
    (r) =>
      r.fields["Period ID"] === manifest.periodId &&
      r.fields["Property ID"] === manifest.propertyId
  ).length;
  result.structuredHistoryVerification = {
    ok: storedCount >= manifest.recordCount,
    storedCount,
    expected: manifest.recordCount,
  };

  if (
    result.snapshotVerification?.ok &&
    result.structuredHistoryVerification.ok &&
    result.defects.length === 0
  ) {
    store.writeRecords([], { periodKeyV, markComplete: true });
    // mark finalized
    store.state.finalizedKeys[periodKeyV] = true;
    store._save();
    result.persistenceState = PERSISTENCE_STATES.COMPLETE;
    result.ok = true;
    result.reportHistoryPersistenceComplete = true;
  } else {
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    result.ok = false;
    result.reportHistoryPersistenceComplete = false;
    result.defects.push({
      code: DEFECT_PARTIAL_HISTORY_PERSISTENCE,
      detail: "verification failed — not COMPLETE",
    });
  }

  return result;
}

/**
 * Evaluate REAL_SECOND_PERIOD_HISTORY_PERSISTENCE_READY from check results.
 */
export function evaluateRealSecondPeriodHistoryPersistenceReady(checks = {}) {
  const required = [
    "schemaFinalized",
    "fieldMapFinalized",
    "dryRunPass",
    "idempotencyPass",
    "noSilentOverwritePass",
    "snapshotCompletenessPass",
    "historicalReproductionPass",
    "structuredHistoryQueriesPass",
    "correctionVersioningPass",
    "syntheticLeakageZero",
    "persistenceFailureSemanticsProven",
    "productionDestinationReviewed",
  ];
  const blockers = [];
  for (const k of required) {
    if (!checks[k]) blockers.push(k);
  }
  // productionDestinationReviewed may be "reviewed_blocked_pending_founder"
  if (checks.productionDestinationReviewed === "reviewed_no_sandbox_blocked") {
    // still a blocker for LIVE writes, but readiness for MANUAL period-2 capture
    // can PASS if mock persistence proven and founder will activate later
  }
  return {
    gate: REAL_SECOND_PERIOD_HISTORY_PERSISTENCE_READY,
    pass: blockers.length === 0,
    blockers,
    checks,
    note: "UI comparison/history viewer and weekly automation are NOT blockers for Period 2 capture",
  };
}
