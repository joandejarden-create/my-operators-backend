/**
 * Core ADP history writer — Airtable production persistence with Measurement Family=CORE.
 */

import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  HISTORY_TABLES,
  PERSISTENCE_STATES,
} from "./airtable-history-schema-final-v1.js";
import { MEASUREMENT_FAMILY } from "../brand-portfolio/bpp-history-schema-v1.js";
import {
  loadPublishedReport,
  loadPublishedManifest,
  loadPublishedEvidenceIndex,
} from "../published-snapshot.js";
import { loadPeriod } from "../data-model.js";
import { sha256OfPayload } from "./report-snapshot-v1.js";
import { assertProductionHistoryWriteAllowed } from "../brand-portfolio/airtable-bpp-history-sandbox-writer-v1.js";
import { MEASUREMENT_CONTRACT_V1_1 } from "../measurement-assurance/adp-measurement-contract-v1-1-candidate.js";

const PRIMARY_KEY_BY_TABLE = {
  [HISTORY_TABLES.MONITORING_PERIODS]: "Period Key",
  [HISTORY_TABLES.REPORT_SNAPSHOTS]: "Snapshot ID",
  [HISTORY_TABLES.PERIOD_METRICS]: "Metrics Key",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function airtableFetch(baseId, token, tableName, pathSuffix = "", init = {}) {
  const encoded = encodeURIComponent(tableName);
  const url = `https://api.airtable.com/v0/${baseId}/${encoded}${pathSuffix}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function escapeFormulaString(s) {
  return String(s).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

async function findByPrimaryKey(baseId, token, tableName, primaryField, keyVal) {
  const formula = `{${primaryField}}="${escapeFormulaString(keyVal)}"`;
  const { res, json } = await airtableFetch(
    baseId,
    token,
    tableName,
    `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=3`
  );
  return { ok: res.ok, records: json.records || [], error: res.ok ? null : json };
}

function sanitizeFieldsForAirtable(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (v === undefined || v === null) continue;
    if (typeof v === "number" && Number.isNaN(v)) continue;
    out[k] = v;
  }
  return out;
}

async function createRecordsBatch(baseId, token, tableName, records) {
  const body = {
    records: records.map((fields) => ({ fields: sanitizeFieldsForAirtable(fields) })),
    typecast: true,
  };
  const { res, json } = await airtableFetch(baseId, token, tableName, "", {
    method: "POST",
    body: JSON.stringify(body),
  });
  return { ok: res.ok, status: res.status, json };
}

export function buildCoreHistorySnapshot(propertyId, periodId, { calendarWeekId, monitoringDate }) {
  const manifest = loadPublishedManifest(propertyId);
  const payload = loadPublishedReport(propertyId);
  const evidence = loadPublishedEvidenceIndex(propertyId, periodId);
  const period = loadPeriod(periodId);
  const snapshotId = `core_snap_${propertyId}_${periodId}`;
  const contentHash = sha256OfPayload(payload || {});

  return {
    propertyId,
    periodId,
    measurementPeriodId: periodId,
    monitoringPeriodId: calendarWeekId,
    monitoringDate,
    measurementFamily: MEASUREMENT_FAMILY.CORE,
    snapshotId,
    schema: "adp_core_report_v1",
    contentHash,
    envelopeHash: contentHash,
    customerPayload: payload,
    evidenceIndex: evidence,
    certificationStatus: manifest?.certificationStatus || "CERTIFIED_WITH_DISCLOSURES",
    certifiedAt: period?.certifiedAt || new Date().toISOString(),
    publishedAt: manifest?.latestPublishedAt || new Date().toISOString(),
    measurementContract: MEASUREMENT_CONTRACT_V1_1,
    publicationVersion: manifest?.assetCacheToken || `core-${periodId}`,
    reportEdition: "CORRECTED_V1_1",
    periodMetrics: {
      demandCaptureRate: payload?.demandCapture?.overallRate ?? null,
      considerationRate: payload?.executiveMetrics?.considerationRate?.rate ?? null,
      scenarioPresence: payload?.executiveMetrics?.scenarioPresence?.rate ?? null,
    },
  };
}

export function buildCoreHistoricalWriteManifest(snapshot) {
  const family = MEASUREMENT_FAMILY.CORE;
  const records = [];
  const propertyId = snapshot.propertyId;
  const periodId = snapshot.periodId;
  const calendarWeekId = snapshot.monitoringPeriodId;

  const historicalVersion = 1;
  const periodKey = `${propertyId}|${calendarWeekId}|core-p2-v${historicalVersion}`;

  records.push({
    table: HISTORY_TABLES.MONITORING_PERIODS,
    fields: {
      "Period Key": periodKey,
      "Property ID": propertyId,
      "Period ID": calendarWeekId,
      "Calendar Week ID": calendarWeekId,
      "Measurement Period IDs JSON": JSON.stringify([periodId]),
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
      "Historical Version": historicalVersion,
      "Is Current Historical Version": true,
      "Corrects Period Key": "",
      Synthetic: false,
    },
  });

  records.push({
    table: HISTORY_TABLES.REPORT_SNAPSHOTS,
    fields: {
      "Snapshot ID": snapshot.snapshotId,
      "Property ID": propertyId,
      "Period ID": periodId,
      "Monitoring Date": snapshot.monitoringDate,
      "Schema Version": snapshot.schema,
      "Content Hash": snapshot.contentHash,
      "Envelope Hash": snapshot.envelopeHash,
      "Publication Version": snapshot.publicationVersion,
      "Report Edition": snapshot.reportEdition,
      "Measurement Contract Version": snapshot.measurementContract,
      "Certification Status": snapshot.certificationStatus,
      "Correction Version": historicalVersion,
      "Is Current Historical Version": true,
      "Snapshot Store Ref": `core-snapshot-blobs/${propertyId}/${snapshot.snapshotId}.json`,
      Synthetic: false,
    },
  });

  const pm = snapshot.periodMetrics || {};
  const metricsKey = `${propertyId}|${periodId}|metrics|v${historicalVersion}`;
  records.push({
    table: HISTORY_TABLES.PERIOD_METRICS,
    fields: {
      "Metrics Key": metricsKey,
      "Property ID": propertyId,
      "Period ID": periodId,
      "Historical Version": historicalVersion,
      "Demand Capture": pm.demandCaptureRate,
      "Consideration Rate": pm.considerationRate,
      "Scenario Presence": pm.scenarioPresence,
      "Certification Status": snapshot.certificationStatus,
    },
  });

  return { ok: true, records, snapshot, defects: [] };
}

export async function writeCoreHistoryPropertyToAirtable({
  propertyId,
  periodId,
  monitoringDate,
  calendarWeekId,
  dryRun = true,
  allowProductionWrites = false,
}) {
  const snapshot = buildCoreHistorySnapshot(propertyId, periodId, {
    calendarWeekId,
    monitoringDate,
  });
  const manifest = buildCoreHistoricalWriteManifest(snapshot);

  const blobRoot = join(process.cwd(), "data/ai-demand-positioning/core-history/snapshot-blobs", propertyId);
  mkdirSync(blobRoot, { recursive: true });
  const blobPath = join(blobRoot, `${snapshot.snapshotId}.json`);
  if (!existsSync(blobPath)) {
    writeFileSync(blobPath, JSON.stringify(snapshot, null, 2) + "\n");
  }

  const result = {
    propertyId,
    periodId,
    dryRun,
    ok: manifest.ok,
    recordCount: manifest.records.length,
    created: 0,
    skipped: 0,
    errors: [],
  };

  if (dryRun) return result;

  const gate = assertProductionHistoryWriteAllowed({ allowProductionWrites });
  if (!gate.allowed) {
    result.ok = false;
    result.errors.push({ code: "WRITE_BLOCKED", detail: gate.reason });
    return result;
  }

  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = gate.baseId;
  result.baseId = baseId;

  for (const rec of manifest.records) {
    const tableName = rec.table;
    const primary = PRIMARY_KEY_BY_TABLE[tableName];
    const keyVal = rec.fields[primary];
    const found = await findByPrimaryKey(baseId, token, tableName, primary, keyVal);
    await sleep(80);
    if (found.records?.length) {
      result.skipped += 1;
      continue;
    }
    const created = await createRecordsBatch(baseId, token, tableName, [rec.fields]);
    await sleep(220);
    if (!created.ok) {
      result.errors.push({ code: "CREATE_FAILED", table: tableName, detail: created.json });
    } else {
      result.created += 1;
    }
  }

  result.ok = result.errors.length === 0;
  result.persistenceState = result.ok ? PERSISTENCE_STATES.COMPLETE : PERSISTENCE_STATES.FAILED;
  return result;
}
