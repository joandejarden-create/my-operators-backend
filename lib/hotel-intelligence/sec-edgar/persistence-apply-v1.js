/**
 * Apply planned HI Graph Evidence rows with read-back.
 * Fail-closed without --apply. Idempotent upsert by Evidence ID.
 */

import {
  GRAPH_BASE_ID_CANONICAL,
  assertCanonicalGraphBase,
  resolveGraphBaseId,
  MAP_HI_GRAPH_EVIDENCE,
} from "../research-engine-v1/airtable-graph-contract-v1.js";
import tableIds from "../research-engine-v1/airtable-graph-table-ids.generated.json" with { type: "json" };
import { PERSISTENCE_CLASS } from "./contract.js";

export const PERSISTENCE_APPLY_VERSION = "sec-edgar-persistence-apply-v1";

function token() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

async function airtable(path, opts = {}) {
  const baseId = resolveGraphBaseId();
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) throw new Error(gate.error || "GRAPH_BASE_GATE");
  const url = path.startsWith("http")
    ? path
    : `https://api.airtable.com/v0/${baseId}/${path}`;
  const res = await fetch(url, {
    ...opts,
    headers: {
      Authorization: `Bearer ${token()}`,
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(`Airtable ${res.status}: ${json?.error?.message || res.status}`);
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

function evidenceTableId() {
  return tableIds?.tableIds?.["HI Graph Evidence"] || "tblc9JhKT1edr4grE";
}

async function findEvidenceById(evidenceId) {
  const formula = encodeURIComponent(`{${MAP_HI_GRAPH_EVIDENCE.evidenceId}}='${evidenceId}'`);
  const table = evidenceTableId();
  const j = await airtable(`${table}?filterByFormula=${formula}&maxRecords=1`);
  return (j.records || [])[0] || null;
}

async function upsertEvidenceRow(row) {
  const evidenceId = row.fields[MAP_HI_GRAPH_EVIDENCE.evidenceId];
  const existing = await findEvidenceById(evidenceId);
  const table = evidenceTableId();
  if (existing) {
    // Non-destructive: do not blank stronger values; patch notes/snippet when useful.
    const patched = await airtable(`${table}/${existing.id}`, {
      method: "PATCH",
      body: JSON.stringify({
        fields: {
          [MAP_HI_GRAPH_EVIDENCE.evidenceSnippet]:
            row.fields[MAP_HI_GRAPH_EVIDENCE.evidenceSnippet],
          [MAP_HI_GRAPH_EVIDENCE.sourceUrl]: row.fields[MAP_HI_GRAPH_EVIDENCE.sourceUrl],
          [MAP_HI_GRAPH_EVIDENCE.notes]: row.fields[MAP_HI_GRAPH_EVIDENCE.notes],
          [MAP_HI_GRAPH_EVIDENCE.retrievedAt]: row.fields[MAP_HI_GRAPH_EVIDENCE.retrievedAt],
        },
      }),
    });
    return { action: "PATCH", record: patched };
  }
  const created = await airtable(table, {
    method: "POST",
    body: JSON.stringify({ fields: row.fields, typecast: true }),
  });
  return { action: "CREATE", record: created };
}

async function readBackEvidence(evidenceId) {
  const rec = await findEvidenceById(evidenceId);
  if (!rec) return { ok: false, error: "READBACK_MISSING" };
  const f = rec.fields || {};
  return {
    ok: true,
    recordId: rec.id,
    evidenceId: f[MAP_HI_GRAPH_EVIDENCE.evidenceId],
    fieldName: f[MAP_HI_GRAPH_EVIDENCE.fieldName],
    valueObserved: f[MAP_HI_GRAPH_EVIDENCE.valueObserved],
    sourceUrl: f[MAP_HI_GRAPH_EVIDENCE.sourceUrl],
    hpcHotelId: f[MAP_HI_GRAPH_EVIDENCE.hpcHotelId],
  };
}

/**
 * @param {object} plan — from planContractPersistence
 * @param {{ apply?: boolean }} opts
 */
export async function applyContractPersistencePlan(plan, opts = {}) {
  const apply = opts.apply === true;
  const results = [];
  if (!apply) {
    return {
      version: PERSISTENCE_APPLY_VERSION,
      applied: false,
      dryRun: true,
      baseId: GRAPH_BASE_ID_CANONICAL,
      plannedEvidence: plan.counts?.evidence || 0,
      results: (plan.evidenceRows || []).map((r) => ({
        persistenceClass: PERSISTENCE_CLASS.PLANNED_DRY_RUN,
        dedupeKey: r.dedupeKey,
        findingRef: r.findingRef,
      })),
    };
  }

  if (!token()) {
    throw new Error("AIRTABLE_API_KEY/PAT required for --apply");
  }

  for (const row of plan.evidenceRows || []) {
    const upserted = await upsertEvidenceRow(row);
    const evidenceId = row.fields[MAP_HI_GRAPH_EVIDENCE.evidenceId];
    const readback = await readBackEvidence(evidenceId);
    const persisted =
      readback.ok &&
      String(readback.valueObserved || "") ===
        String(row.fields[MAP_HI_GRAPH_EVIDENCE.valueObserved] || "") &&
      String(readback.fieldName || "") ===
        String(row.fields[MAP_HI_GRAPH_EVIDENCE.fieldName] || "");
    results.push({
      persistenceClass: persisted
        ? PERSISTENCE_CLASS.PERSISTED_VERIFIED
        : PERSISTENCE_CLASS.QUARANTINED,
      upsertAction: upserted.action,
      evidenceId,
      readback,
      findingRef: row.findingRef,
    });
  }

  return {
    version: PERSISTENCE_APPLY_VERSION,
    applied: true,
    dryRun: false,
    baseId: resolveGraphBaseId(),
    persistedVerified: results.filter(
      (r) => r.persistenceClass === PERSISTENCE_CLASS.PERSISTED_VERIFIED,
    ).length,
    readbackFailures: results.filter((r) => r.persistenceClass !== PERSISTENCE_CLASS.PERSISTED_VERIFIED)
      .length,
    results,
  };
}
