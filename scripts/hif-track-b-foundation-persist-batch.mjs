#!/usr/bin/env node
/**
 * Track B — Controlled Airtable persistence for historical foundation dossiers.
 * Default: --dry-run. Pass --apply for create/update + read-back.
 * CUSTOMER_BLOCKED. No HPC create. No schema mutation. Avoids Track A Lakeville IDs.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  buildDiscoveryInventoryFields,
  buildCommercialFindingFields,
  classifyWriteCategory,
  planFoundationPersistBatch,
  WRITE_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import { isTrackAReservedId } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}

const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inventoryTableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const findingsTableId = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-historical-foundation");
const batch = JSON.parse(fs.readFileSync(path.join(OUT, "HISTORICAL_RESEARCH_BATCH.json"), "utf8"));

async function findByFormula(tableId, formula) {
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "1" });
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.records?.[0] || null;
}

async function createRecord(tableId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  return { ok: res.ok, json };
}

async function getRecord(tableId, recordId) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  return { ok: res.ok, json };
}

const ledger = {
  asOf: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN",
  publicationState: "CUSTOMER_BLOCKED",
  stream: "TRACK_B",
  researchCostUsd: 0,
  operations: [],
  summary: { creates: 0, updates: 0, noops: 0, conflicts: 0, staged: 0, researchOnly: 0, skippedTrackA: 0 },
  schemaGaps: [],
  readBack: [],
};

const planItems = [];

for (const d of batch.dossiers) {
  if (isTrackAReservedId(d.discoveryPropertyId) || isTrackAReservedId(d.findingId)) {
    ledger.summary.skippedTrackA += 1;
    ledger.operations.push({ id: d.discoveryPropertyId, action: "SKIP_TRACK_A_RESERVED" });
    continue;
  }

  if (!d.persistEligible) {
    const item = {
      id: d.discoveryPropertyId,
      destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
      researchOnly: d.identityClass === "GENERIC_BRAND_OR_COMPANY",
      stagedForReview: true,
      schemaGap: d.persistReason?.includes("SCHEMA") || false,
      evidenceSupported: true,
      sourceUrl: d.historicalEvents?.[0]?.sourceUrl,
      verified: false,
      notes: d.persistReason,
    };
    planItems.push(item);
    ledger.operations.push({ id: d.discoveryPropertyId, action: "STAGED", reason: d.persistReason });
    continue;
  }

  const primarySource = d.historicalEvents?.[0]?.sourceUrl || d.historicalEvents?.slice(-1)[0]?.sourceUrl;
  const latestEvent = d.historicalEvents?.[d.historicalEvents.length - 1] || {};

  const hotelPayload = {
    discoveryPropertyId: d.discoveryPropertyId,
    hotelName: d.hotelName,
    hotelNameNormalized: d.hotelNameNormalized,
    city: d.city,
    stateProvince: d.stateProvince,
    physicalCountry: d.physicalCountry,
    address: d.address,
    currentBrand: d.currentBrand,
    historicalBrand: d.historicalBrand,
    identityClass: d.identityClass,
    evidenceConfidence: d.evidenceConfidence,
    hpcMatchStatus: d.hpcMatchStatus === "PENDING_HPC_SEARCH" ? "DISCOVERED_NOT_IN_HPC" : d.hpcMatchStatus,
    sourceUrl: primarySource,
    discoveryMethod: "TRACK_B_HISTORICAL_FOUNDATION",
    notes: `CUSTOMER_BLOCKED | Track B historical | status=${d.reconciliation.currentStatusClass} | events=${d.historicalEvents?.length || 0}`,
    verified: true,
    evidenceSupported: true,
    destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  };

  const existingInv = await findByFormula(
    inventoryTableId,
    `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${d.discoveryPropertyId}"`,
  );

  hotelPayload.existingAirtableId = existingInv?.id || null;
  hotelPayload.fieldsUnchanged = false;
  planItems.push(hotelPayload);

  const findingPayload = {
    findingId: d.findingId,
    commercialCategory: d.commercialCategory,
    commercialThesis: d.historicalEvents?.map((e) => e.label).join(" | "),
    commercialTrigger: latestEvent.label,
    decisionEvidence: d.historicalEvents
      ?.map((e) => `${e.effectiveDate || ""}: ${e.note || e.label}`)
      .join("\n")
      .slice(0, 5000),
    nextDecisionDate: latestEvent.amendedExpiration || latestEvent.originalExpiration || null,
    sourcePublicationDate: latestEvent.effectiveDate || null,
    sourceUrl: latestEvent.sourceUrl || primarySource,
    evidenceConfidence: d.evidenceConfidence,
    verificationStatus: "EVIDENCE_SUPPORTED",
    verified: true,
    evidenceSupported: true,
    destination: TABLE_HI_DISCOVERY_FINDINGS,
    notes: `CUSTOMER_BLOCKED | Track B | ${d.reconciliation.currentStatusClass} | agreementType=${latestEvent.agreementType || "UNRESOLVED"}`,
  };

  const existingFinding = await findByFormula(
    findingsTableId,
    `{${MAP_DISCOVERY_FINDINGS.findingId}}="${d.findingId}"`,
  );
  findingPayload.existingAirtableId = existingFinding?.id || null;
  findingPayload._hotelDiscoveryPropertyId = d.discoveryPropertyId;
  findingPayload._pendingLink = true;
  planItems.push(findingPayload);
}

const plan = planFoundationPersistBatch(planItems);
ledger.planCounts = plan.counts;

if (!APPLY) {
  ledger.summary.creates = plan.counts.creates;
  ledger.summary.staged = plan.counts.staged;
  ledger.summary.researchOnly = plan.counts.researchOnly;
  ledger.summary.conflicts = plan.counts.conflicts;
  ledger.summary.noops = plan.counts.noops;
  fs.writeFileSync(path.join(OUT, "PERSISTENCE_LEDGER_DRY_RUN.json"), JSON.stringify(ledger, null, 2));
  console.log(JSON.stringify({ ok: true, mode: "DRY_RUN", counts: plan.counts }, null, 2));
  process.exit(0);
}

/** APPLY path */
const invIdByDiscoveryPropertyId = new Map();

for (const item of plan.creates.concat(plan.updates)) {
  if (item.destination !== TABLE_HI_HOTEL_DISCOVERY_INVENTORY) continue;
  if (isTrackAReservedId(item.discoveryPropertyId)) continue;

  const fields = buildDiscoveryInventoryFields(item);
  if (item.existingAirtableId) {
    // Non-destructive: skip update if already present (idempotent noop for this batch)
    ledger.summary.noops += 1;
    ledger.operations.push({ id: item.discoveryPropertyId, action: "NOOP_EXISTING", airtableId: item.existingAirtableId });
    invIdByDiscoveryPropertyId.set(item.discoveryPropertyId, item.existingAirtableId);
    const rb = await getRecord(inventoryTableId, item.existingAirtableId);
    ledger.readBack.push({ table: "inventory", id: item.existingAirtableId, ok: rb.ok, fields: rb.json?.fields });
    continue;
  }

  const created = await createRecord(inventoryTableId, fields);
  if (!created.ok) {
    ledger.operations.push({ id: item.discoveryPropertyId, action: "CREATE_FAIL", error: created.json });
    continue;
  }
  ledger.summary.creates += 1;
  ledger.operations.push({ id: item.discoveryPropertyId, action: "CREATE", airtableId: created.json.id });
  invIdByDiscoveryPropertyId.set(item.discoveryPropertyId, created.json.id);
  const rb = await getRecord(inventoryTableId, created.json.id);
  ledger.readBack.push({
    table: "inventory",
    id: created.json.id,
    ok: rb.ok,
    discoveryPropertyId: rb.json?.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId],
    country: rb.json?.fields?.[MAP_DISCOVERY_INVENTORY.country],
    hotelName: rb.json?.fields?.[MAP_DISCOVERY_INVENTORY.hotelNameRaw],
  });
}

for (const item of plan.creates.concat(plan.updates)) {
  if (item.destination !== TABLE_HI_DISCOVERY_FINDINGS) continue;
  if (isTrackAReservedId(item.findingId)) continue;

  const linkId =
    invIdByDiscoveryPropertyId.get(item._hotelDiscoveryPropertyId) ||
    (
      await findByFormula(
        inventoryTableId,
        `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${item._hotelDiscoveryPropertyId}"`,
      )
    )?.id ||
    null;

  if (item.existingAirtableId) {
    ledger.summary.noops += 1;
    ledger.operations.push({ id: item.findingId, action: "NOOP_EXISTING", airtableId: item.existingAirtableId });
    continue;
  }

  const fields = buildCommercialFindingFields(item, linkId);
  const created = await createRecord(findingsTableId, fields);
  if (!created.ok) {
    ledger.operations.push({ id: item.findingId, action: "CREATE_FAIL", error: created.json });
    continue;
  }
  ledger.summary.creates += 1;
  ledger.operations.push({ id: item.findingId, action: "CREATE", airtableId: created.json.id, linkedInventory: linkId });
  const rb = await getRecord(findingsTableId, created.json.id);
  ledger.readBack.push({
    table: "findings",
    id: created.json.id,
    ok: rb.ok,
    findingId: rb.json?.fields?.[MAP_DISCOVERY_FINDINGS.findingId],
    findingType: rb.json?.fields?.[MAP_DISCOVERY_FINDINGS.findingType],
    sourceUrl: rb.json?.fields?.[MAP_DISCOVERY_FINDINGS.sourceUrl],
  });
}

ledger.summary.staged = plan.counts.staged;
ledger.summary.researchOnly = plan.counts.researchOnly;
ledger.summary.conflicts = plan.counts.conflicts;

fs.writeFileSync(path.join(OUT, "PERSISTENCE_LEDGER_APPLY.json"), JSON.stringify(ledger, null, 2));
console.log(JSON.stringify({ ok: true, mode: "APPLY", summary: ledger.summary }, null, 2));
