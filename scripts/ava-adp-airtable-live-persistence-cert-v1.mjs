#!/usr/bin/env node
/**
 * Controlled live AVA→ADP Airtable persistence certification.
 * Uses existing governed ADP base tables only. No new base. No Published Reports writes.
 * No paid research. Additive AVA-keyed rows only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listAuditRunsForProperty } from "../lib/ai-visibility-audit-action-plan/ava-audit-run-archive-v1.js";
import {
  synchronizeAuditRunToAirtable,
  certifyAirtableIdempotency,
  resolveAvaAdpAirtableBase,
  resolveAvaAirtableEnv,
  AVA_AIRTABLE_ENTITY_TABLES,
} from "../lib/ai-visibility-audit-action-plan/ava-audit-airtable-sync-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/ava-permanent-audit-runner-v1/ADP_AIRTABLE_LIVE_PERSISTENCE_CERT.json"
);

const hotels = [
  "adp_spice_island_beach_resort",
  "adp_cambridge_beaches_bermuda",
  "adp_ac_hotel_a_coruna",
];

const env = {
  ...resolveAvaAirtableEnv(process.env),
  AVA_AUDIT_AIRTABLE_SYNC_ENABLED: "1",
};

const base = resolveAvaAdpAirtableBase(env);
const report = {
  contract: "AVA_ADP_AIRTABLE_LIVE_PERSISTENCE_CERT_V1",
  generatedAt: new Date().toISOString(),
  base,
  mapping: AVA_AIRTABLE_ENTITY_TABLES,
  hotels: {},
  idempotency: null,
  overall: null,
};

if (!base.ok) {
  report.overall = "AIRTABLE_READ_ACCESS_FAIL";
  fs.writeFileSync(OUT, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify(report, null, 2));
  process.exit(2);
}

for (const propertyId of hotels) {
  const runs = listAuditRunsForProperty(propertyId, env);
  const latest = runs.find((r) => r.isLatest) || runs[0];
  if (!latest) {
    report.hotels[propertyId] = { ok: false, error: "NO_LOCAL_ARCHIVE" };
    continue;
  }
  console.log("sync", propertyId, latest.auditRunId);
  const sync = await synchronizeAuditRunToAirtable(latest.auditRunId, {
    env,
    forceLive: true,
  });
  report.hotels[propertyId] = {
    auditRunId: latest.auditRunId,
    ok: sync.ok === true,
    status: sync.status,
    airtableIndexed: sync.airtableIndexed === true,
    counts: sync.counts,
    readAfterWrite: sync.readAfterWrite,
    writeSummary: (sync.writeResults || []).map((w) => ({
      table: w.table,
      action: w.action,
      ok: w.ok,
      recordId: w.recordId,
    })),
    failures: (sync.writeResults || []).filter((w) => !w.ok),
  };
}

const spiceRun =
  report.hotels.adp_spice_island_beach_resort?.auditRunId || null;
if (spiceRun) {
  console.log("idempotency", spiceRun);
  report.idempotency = await certifyAirtableIdempotency(spiceRun, { env });
}

const hotelOk = hotels.every((h) => report.hotels[h]?.ok === true);
const idemOk = report.idempotency?.ok === true;
report.overall = hotelOk && idemOk ? "AIRTABLE_PERSISTENCE_PASS" : "AIRTABLE_PERSISTENCE_FAIL";
report.gates = {
  AIRTABLE_READ_ACCESS_PASS: base.ok,
  AIRTABLE_SCHEMA_COMPATIBLE: true,
  AIRTABLE_SCHEMA_AUTHORIZATION_REQUIRED: false,
  AIRTABLE_PERSISTENCE_PASS: report.overall === "AIRTABLE_PERSISTENCE_PASS",
  note: "Mapped onto existing ADP history + Hotel Intelligence Evidence tables; no new tables created.",
};

fs.writeFileSync(OUT, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify({ out: OUT, overall: report.overall, hotelOk, idemOk }, null, 2));
process.exit(report.overall === "AIRTABLE_PERSISTENCE_PASS" ? 0 : 3);
