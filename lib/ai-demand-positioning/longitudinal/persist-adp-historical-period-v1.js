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
  isAdpHistoryWritesEnabled,
  HISTORY_TABLES,
  PERSISTENCE_STATES,
  IDEMPOTENCY_KEYS,
  SNAPSHOT_STORAGE_STRATEGY,
  HISTORY_ENV,
  EXISTING_LIVE_PUBLISH,
} from "./airtable-history-schema-final-v1.js";
import {
  ADP_GOVERNED_BASE_ID,
  resolveProductionHistoryDestination,
} from "./resolve-adp-history-destination-v1.js";
import { auditReportSnapshotCompleteness } from "./customer-surface-persistence-registry-v1.js";
import { sha256OfPayload, stableStringify } from "./report-snapshot-v1.js";
import { buildCorePromptLedgerRecordsFromPeriod } from "./adp-core-prompt-ledger-and-evidence-from-period-v1.js";
import {
  buildCurrentVersionDemotionPlan,
  applyDemotionPlanToMockStore,
  assertSingleCurrentPerProperty,
} from "./adp-current-historical-version-v1.js";
import {
  assessPersistenceCompleteness,
  countRecordsByTable,
} from "./adp-persistence-completeness-gate-v1.js";
import {
  ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1,
  PERSISTENCE_LIFECYCLE,
  PUBLICATION_LIFECYCLE,
} from "./adp-e2e-persistence-table-contract-v1.js";

export const PERSIST_ADP_HISTORICAL_PERIOD_VERSION = "persist_adp_historical_period_v1";
export { ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1 };

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
  /** Loaded Core period with observations — enables Prompt Ledger rows for Core. */
  period = null,
  /** When true, also expand Evidence Index from customer-visible cards with traceability meta. */
  includeCustomerVisibleEvidenceIndex = true,
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
          // Core independent-unprompted rankings are MARKET_CORE.
          // Do NOT invent Portfolio Lens / Portfolio Type / Peer Set Version (BPP-only).
          "Ranking Universe Type":
            e.rankingUniverseType || rankingPack?.rankingUniverseType || "MARKET_CORE",
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

  // Core Prompt Ledger — from authoritative period observations (not BPP-only / not UI-card-only)
  let promptLedgerMeta = null;
  if (period && Array.isArray(period.observations) && period.observations.length > 0) {
    const ledger = buildCorePromptLedgerRecordsFromPeriod({
      propertyId,
      periodId,
      period,
      historicalVersion: version,
    });
    promptLedgerMeta = {
      observationCount: ledger.observationCount,
      eligibleCount: ledger.eligibleCount,
      assistedOrIneligibleCount: ledger.assistedOrIneligibleCount,
      defects: ledger.defects?.length || 0,
    };
    for (const rec of ledger.records || []) {
      const { _meta, ...rest } = rec;
      void _meta;
      records.push(rest);
    }
    if (ledger.defects?.length) {
      defects.push(
        ...ledger.defects.slice(0, 20).map((d) => ({
          code: d.code || "PROMPT_LEDGER_DEFECT",
          detail: d.observationId || JSON.stringify(d),
        }))
      );
    }
  }

  void includeCustomerVisibleEvidenceIndex;

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
    promptLedgerMeta,
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

/**
 * Write authorization (fail-closed).
 * - MOCK: local fixture store only
 * - SANDBOX: ADP_HISTORY_SANDBOX_BASE_ID
 * - GOVERNED_PRODUCTION: governed ADP base appa2cE7FTRmIbB32 when
 *   ADP_HISTORY_WRITES_ENABLED + ADP_HISTORY_AIRTABLE_WRITE_APPLY +
 *   allowProductionWrites + allowGovernedProductionHistoryBase
 * Never treats Published Reports overlay as history destination.
 */
export function writesAllowed({
  allowProductionWrites = false,
  useMock = true,
  allowGovernedProductionHistoryBase = false,
} = {}) {
  if (useMock) return { allowed: true, mode: "MOCK" };
  if (!isAdpHistoryWritesEnabled()) {
    return { allowed: false, mode: "DISABLED", reason: "ADP_HISTORY_WRITES_ENABLED=false" };
  }
  const applyFlag = process.env[HISTORY_ENV.ADP_HISTORY_AIRTABLE_WRITE_APPLY];
  if (applyFlag !== "true" && applyFlag !== "1") {
    return {
      allowed: false,
      mode: "DISABLED",
      reason: "ADP_HISTORY_AIRTABLE_WRITE_APPLY not true",
    };
  }
  if (!allowProductionWrites) {
    return { allowed: false, mode: "DISABLED", reason: "allowProductionWrites=false" };
  }
  const sandboxId = (process.env[HISTORY_ENV.ADP_HISTORY_SANDBOX_BASE_ID] || "").trim();
  if (sandboxId) {
    return { allowed: true, mode: "SANDBOX", baseId: sandboxId };
  }
  if (allowGovernedProductionHistoryBase) {
    const dest = resolveProductionHistoryDestination({ confirmGovernedAdpBase: true });
    if (dest.baseId === ADP_GOVERNED_BASE_ID && dest.isolation?.liveOverlayTableUsedForHistory === false) {
      return {
        allowed: true,
        mode: "GOVERNED_PRODUCTION",
        baseId: dest.baseId,
        reason: "Founder-authorized governed ADP history base",
      };
    }
    return {
      allowed: false,
      mode: "DISABLED",
      reason: `Governed production base unresolved or isolation failed: ${dest.defects?.map((d) => d.code).join(",") || "unknown"}`,
    };
  }
  return {
    allowed: false,
    mode: "DISABLED",
    reason:
      "No ADP_HISTORY_SANDBOX_BASE_ID and allowGovernedProductionHistoryBase=false — refuse production destination",
  };
}

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

async function airtableListByFormula({ baseId, table, filterByFormula, fields = [] }) {
  const token = airtableToken();
  if (!token || !baseId) return { ok: false, records: [], error: "AIRTABLE_CONFIG_MISSING" };
  const q = new URLSearchParams({ pageSize: "100", filterByFormula });
  for (const f of fields) q.append("fields[]", f);
  const out = [];
  let offset = null;
  do {
    if (offset) q.set("offset", offset);
    else q.delete("offset");
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${q}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      return {
        ok: false,
        records: out,
        error: `Airtable ${res.status}: ${json?.error?.message || ""}`,
      };
    }
    out.push(...(json.records || []));
    offset = json.offset || null;
  } while (offset);
  return { ok: true, records: out };
}

async function airtableCreateRecords({ baseId, table, records }) {
  const token = airtableToken();
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`;
  const created = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10).map((r) => ({ fields: r.fields }));
    const res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ records: chunk, typecast: true }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      return {
        ok: false,
        created,
        error: `Airtable create ${res.status}: ${json?.error?.message || JSON.stringify(json)}`,
      };
    }
    created.push(...(json.records || []));
  }
  return { ok: true, created };
}

/**
 * Idempotent upsert plan for one history table using primary key field values.
 * Read-only when dryRun; live create only for missing keys (no silent overwrite).
 */
export async function applyHistoricalRecordsToAirtable({
  baseId,
  records,
  dryRun = true,
  primaryFieldByTable = {
    [HISTORY_TABLES.MONITORING_PERIODS]: "Period Key",
    [HISTORY_TABLES.REPORT_SNAPSHOTS]: "Snapshot ID",
    [HISTORY_TABLES.PERIOD_METRICS]: "Metrics Key",
    [HISTORY_TABLES.TERRITORY_METRICS]: "Territory Key",
    [HISTORY_TABLES.PROVIDER_METRICS]: "Provider Key",
    [HISTORY_TABLES.COMPETITIVE_RANKINGS]: "Rank Key",
    [HISTORY_TABLES.EVIDENCE_INDEX]: "Evidence Key",
    [HISTORY_TABLES.REPORT_CORRECTIONS]: "Correction ID",
    [HISTORY_TABLES.PROMPT_LEDGER]: "Prompt Ledger Key",
  },
} = {}) {
  const byTable = new Map();
  for (const r of records || []) {
    if (!byTable.has(r.table)) byTable.set(r.table, []);
    byTable.get(r.table).push(r);
  }
  const plan = [];
  const created = [];
  const skipped = [];
  const errors = [];

  for (const [table, rows] of byTable) {
    const primary = primaryFieldByTable[table];
    if (!primary) {
      errors.push({ table, error: "NO_PRIMARY_FIELD_MAPPING" });
      continue;
    }
    for (const row of rows) {
      const key = row.fields?.[primary] || row.idempotencyKey;
      if (!key) {
        errors.push({ table, error: "MISSING_IDEMPOTENCY_KEY" });
        continue;
      }
      const safe = String(key).replace(/'/g, "\\'");
      const lookup = await airtableListByFormula({
        baseId,
        table,
        filterByFormula: `{${primary}}='${safe}'`,
        fields: [primary],
      });
      if (!lookup.ok) {
        errors.push({ table, key, error: lookup.error });
        continue;
      }
      if (lookup.records.length > 0) {
        skipped.push({
          table,
          key,
          existingRecordId: lookup.records[0].id,
          action: "SKIP_IDEMPOTENT",
        });
        plan.push({
          table,
          key,
          action: "SKIP_IDEMPOTENT",
          existingRecordId: lookup.records[0].id,
          fields: row.fields,
        });
        continue;
      }
      plan.push({ table, key, action: "INSERT", fields: row.fields });
      if (!dryRun) {
        const write = await airtableCreateRecords({
          baseId,
          table,
          records: [{ fields: row.fields }],
        });
        if (!write.ok) {
          errors.push({ table, key, error: write.error });
        } else {
          created.push({
            table,
            key,
            recordId: write.created[0]?.id,
            action: "INSERT",
          });
        }
      }
    }
  }

  return {
    ok: errors.length === 0,
    dryRun,
    baseId,
    plan,
    created,
    skipped,
    errors,
    insertCount: plan.filter((p) => p.action === "INSERT").length,
    skipCount: skipped.length,
  };
}

/**
 * Persist historical period — dryRun | mock | (future sandbox).
 */
export async function persistAdpHistoricalPeriodV1(input = {}) {
  const {
    dryRun = true,
    useMock = true,
    allowProductionWrites = false,
    allowGovernedProductionHistoryBase = false,
    correctionMode = false,
    ...manifestInput
  } = input;

  const wantLiveAirtable =
    !dryRun && !useMock && (allowProductionWrites || allowGovernedProductionHistoryBase);
  const gate = writesAllowed({
    allowProductionWrites,
    useMock: wantLiveAirtable ? false : useMock,
    allowGovernedProductionHistoryBase,
  });
  const manifest = buildHistoricalWriteManifest({ ...manifestInput, correctionMode });

  const result = {
    version: PERSIST_ADP_HISTORICAL_PERIOD_VERSION,
    persistenceContract: ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1,
    dryRun,
    writeMode: dryRun ? "DRY_RUN" : gate.mode,
    productionWritesEnabled: false,
    publishPerformed: false,
    /** Distinct from publication — Airtable history index lifecycle */
    persistenceLifecycle: dryRun
      ? PERSISTENCE_LIFECYCLE.PLANNED_DRY_RUN
      : PERSISTENCE_LIFECYCLE.NOT_ATTEMPTED,
    /** Distinct from persistence — FS / Live overlay publication */
    publicationLifecycle: PUBLICATION_LIFECYCLE.NOT_PUBLISHED,
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
    completenessGate: null,
    currentVersionDemotion: null,
    airtableApply: null,
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
    result.persistenceLifecycle = PERSISTENCE_LIFECYCLE.PLANNED_DRY_RUN;
    result.ok = true;
    result.dryRunManifest = manifest;
    result.completenessGate = assessPersistenceCompleteness({
      manifestCounts: manifest.counts,
      requirePromptLedger: Boolean(manifest.promptLedgerMeta),
      requireEvidence: (manifest.counts?.[HISTORY_TABLES.EVIDENCE_INDEX] || 0) > 0,
      correctionMode,
      persistenceState: PERSISTENCE_LIFECYCLE.PLANNED_DRY_RUN,
      claimedComplete: false,
    });
    // Optional read-only Airtable plan against governed base (no writes)
    if (allowGovernedProductionHistoryBase || process.env.ADP_HISTORY_DRY_RUN_LIVE_LOOKUP === "1") {
      const dest = resolveProductionHistoryDestination({ confirmGovernedAdpBase: true });
      if (dest.baseId) {
        result.airtableApply = await applyHistoricalRecordsToAirtable({
          baseId: dest.baseId,
          records: manifest.records,
          dryRun: true,
        });
      } else {
        result.persistenceLifecycle = PERSISTENCE_LIFECYCLE.UNAVAILABLE;
        result.defects.push({
          code: "AIRTABLE_UNAVAILABLE",
          detail: "History destination unresolved — must not claim persistence succeeded",
        });
      }
    }
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

  // Live Airtable path (sandbox or founder-authorized governed base)
  if (wantLiveAirtable && (gate.mode === "SANDBOX" || gate.mode === "GOVERNED_PRODUCTION")) {
    result.persistenceState = PERSISTENCE_STATES.WRITING;
    result.productionWritesEnabled = gate.mode === "GOVERNED_PRODUCTION";
    const apply = await applyHistoricalRecordsToAirtable({
      baseId: gate.baseId,
      records: manifest.records,
      dryRun: false,
    });
    result.airtableApply = apply;
    result.recordsCreated = apply.created.length;
    result.idempotentSkips = apply.skipped.length;
    result.persistenceState = PERSISTENCE_STATES.VERIFYING;
    if (!apply.ok || apply.errors.length) {
      result.ok = false;
      result.persistenceState = PERSISTENCE_STATES.FAILED;
      result.defects.push({
        code: DEFECT_PARTIAL_HISTORY_PERSISTENCE,
        detail: "Airtable write/readback errors — not COMPLETE",
        errors: apply.errors,
      });
      result.reportHistoryPersistenceComplete = false;
      return result;
    }
    // Readback: every intended INSERT must resolve by primary key
    const missingReadback = apply.plan.filter((p) => p.action === "INSERT").filter((p) => {
      const hit = apply.created.find((c) => c.table === p.table && c.key === p.key);
      return !hit?.recordId;
    });
    if (missingReadback.length) {
      result.ok = false;
      result.persistenceState = PERSISTENCE_STATES.FAILED;
      result.defects.push({
        code: DEFECT_PARTIAL_HISTORY_PERSISTENCE,
        detail: `readback missing ${missingReadback.length} inserts`,
      });
      result.reportHistoryPersistenceComplete = false;
      return result;
    }

    // Field-level completeness: primary-key existence alone is NOT AIRTABLE_SYNC_COMPLETE
    const { assertFieldLevelReadback } = await import("./adp-history-field-completeness-v1.js");
    const fieldFailures = [];
    for (const p of apply.plan.filter((x) => x.action === "INSERT")) {
      const hit = apply.created.find((c) => c.table === p.table && c.key === p.key);
      if (!hit?.recordId) continue;
      const lookup = await airtableListByFormula({
        baseId: gate.baseId,
        table: p.table,
        filterByFormula: `RECORD_ID()='${hit.recordId}'`,
      });
      const actual = lookup.records?.[0]?.fields || {};
      const required = Object.keys(p.fields || {}).filter(
        (k) => p.fields[k] != null && p.fields[k] !== ""
      );
      const check = assertFieldLevelReadback({
        expected: p.fields,
        actual,
        requiredFields: required.slice(0, 24),
        allowBlankFields: ["Corrects Period Key", "Publication Timestamp"],
      });
      if (!check.ok) {
        fieldFailures.push({
          table: p.table,
          key: p.key,
          recordId: hit.recordId,
          failures: check.failures.slice(0, 12),
        });
      }
    }
    if (fieldFailures.length) {
      result.ok = false;
      result.persistenceState = PERSISTENCE_STATES.FAILED;
      result.fieldLevelReadback = { ok: false, fieldFailures };
      result.defects.push({
        code: "FIELD_LEVEL_READBACK_INCOMPLETE",
        detail: `${fieldFailures.length} inserts failed field-level readback — refuse AIRTABLE_SYNC_COMPLETE`,
      });
      result.reportHistoryPersistenceComplete = false;
      return result;
    }

    // Ranking universe consistency — Core rows must be MARKET_CORE
    const { assertRankingUniverseConsistency } = await import(
      "./adp-history-field-completeness-v1.js"
    );
    const rankingRows = apply.plan
      .filter((p) => p.table === HISTORY_TABLES.COMPETITIVE_RANKINGS && p.action === "INSERT")
      .map((p) => ({ fields: p.fields }));
    if (rankingRows.length) {
      const universeCheck = assertRankingUniverseConsistency({
        measurementFamily: "CORE",
        rankingRecords: rankingRows,
      });
      if (!universeCheck.ok) {
        result.ok = false;
        result.persistenceState = PERSISTENCE_STATES.FAILED;
        result.defects.push({
          code: "RANKING_UNIVERSE_INCONSISTENT",
          detail: `${universeCheck.failures.length} ranking-universe defect(s) — refuse AIRTABLE_SYNC_COMPLETE`,
          failures: universeCheck.failures.slice(0, 12),
        });
        result.reportHistoryPersistenceComplete = false;
        return result;
      }
    }

    result.structuredHistoryVerification = {
      ok: true,
      storedCount: apply.created.length + apply.skipped.length,
      expected: manifest.recordCount,
      mode: gate.mode,
      baseId: gate.baseId,
      fieldLevelReadbackOk: true,
      rankingUniverseOk: true,
    };
    result.fieldLevelReadback = { ok: true, checked: apply.created.length };
    result.persistenceState = PERSISTENCE_STATES.COMPLETE;
    result.ok = true;
    result.reportHistoryPersistenceComplete = true;
    return result;
  }

  // MOCK path
  result.persistenceState = PERSISTENCE_STATES.WRITING;
  result.persistenceLifecycle = PERSISTENCE_LIFECYCLE.WRITING;
  const store = new MockAdpHistoryStore();
  const periodKeyV = `${periodKey(manifest.propertyId, manifest.periodId)}|v${manifest.historicalVersion}`;

  // No silent overwrite if already finalized
  if (store.hasFinalized(periodKeyV) && !correctionMode) {
    // Idempotent re-run: rewrite same records should skip
  }

  // Demote prior current flags before installing new current (local mock only)
  const priorMonitoring = store.listPeriods(manifest.propertyId);
  const priorSnapshots = store.query(
    (r) =>
      r.table === HISTORY_TABLES.REPORT_SNAPSHOTS &&
      r.fields["Property ID"] === manifest.propertyId
  );
  const demotionPlan = buildCurrentVersionDemotionPlan({
    propertyId: manifest.propertyId,
    newPeriodId: manifest.periodId,
    newHistoricalVersion: manifest.historicalVersion,
    newPeriodKey: `${manifest.propertyId}|${manifest.periodId}|v${manifest.historicalVersion}`,
    newSnapshotId: manifest.snapshotId,
    existingMonitoringPeriodRows: priorMonitoring,
    existingReportSnapshotRows: priorSnapshots,
  });
  result.currentVersionDemotion = demotionPlan;
  applyDemotionPlanToMockStore(store, demotionPlan);

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
    result.persistenceLifecycle = PERSISTENCE_LIFECYCLE.FAILED;
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
  result.persistenceLifecycle = PERSISTENCE_LIFECYCLE.VERIFYING;

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
  } else if (!manifest.snapshotBlob) {
    // Allow metrics-only synthetic fixtures without full snapshot blob
    result.snapshotVerification = { ok: true, skipped: true };
    snapOk = true;
  }

  const storedForPeriod = store.query(
    (r) =>
      r.fields["Period ID"] === manifest.periodId &&
      r.fields["Property ID"] === manifest.propertyId
  );
  const storedCount = storedForPeriod.length;
  const storedCounts = countRecordsByTable(storedForPeriod, {
    propertyId: manifest.propertyId,
    periodId: manifest.periodId,
  });
  result.structuredHistoryVerification = {
    ok: storedCount >= manifest.recordCount,
    storedCount,
    expected: manifest.recordCount,
    byTable: storedCounts,
  };

  const singleCurrent = assertSingleCurrentPerProperty(
    store.listPeriods(manifest.propertyId),
    manifest.propertyId
  );
  if (!singleCurrent.ok) {
    result.defects.push(singleCurrent.defect);
  }

  result.completenessGate = assessPersistenceCompleteness({
    manifestCounts: manifest.counts,
    storedCounts,
    requirePromptLedger: Boolean(manifest.promptLedgerMeta),
    requireEvidence: (manifest.counts?.[HISTORY_TABLES.EVIDENCE_INDEX] || 0) > 0,
    correctionMode,
    persistenceState: PERSISTENCE_LIFECYCLE.VERIFYING,
    claimedComplete: false,
  });
  if (!result.completenessGate.ok) {
    result.defects.push(
      ...result.completenessGate.defects.filter((d) => d.severity !== "INFO")
    );
  }

  if (
    result.snapshotVerification?.ok &&
    result.structuredHistoryVerification.ok &&
    result.completenessGate.ok &&
    singleCurrent.ok &&
    result.defects.length === 0
  ) {
    store.writeRecords([], { periodKeyV, markComplete: true });
    store.state.finalizedKeys[periodKeyV] = true;
    store._save();
    result.persistenceState = PERSISTENCE_STATES.COMPLETE;
    result.persistenceLifecycle = PERSISTENCE_LIFECYCLE.COMPLETE;
    result.ok = true;
    result.reportHistoryPersistenceComplete = true;
    result.completenessGate = assessPersistenceCompleteness({
      manifestCounts: manifest.counts,
      storedCounts,
      requirePromptLedger: Boolean(manifest.promptLedgerMeta),
      requireEvidence: (manifest.counts?.[HISTORY_TABLES.EVIDENCE_INDEX] || 0) > 0,
      correctionMode,
      persistenceState: PERSISTENCE_LIFECYCLE.COMPLETE,
      claimedComplete: true,
    });
  } else {
    result.persistenceState = PERSISTENCE_STATES.FAILED;
    result.persistenceLifecycle = PERSISTENCE_LIFECYCLE.FAILED;
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
