#!/usr/bin/env node
/**
 * Track A CB4 — Persist global commercial expansion batch.
 * Default: --dry-run. Pass --apply for create + read-back.
 * CUSTOMER_BLOCKED. No HPC create. No schema mutation.
 * Preserves prior CB1–CB3 Airtable IDs (NOOP on duplicates).
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
import {
  classifyAgreementVsOpportunity,
  qualifiesPrincipalFirst100,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-a-continuous-foundation-waves-v1.js";

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
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb4-global-commercial-expansion");
const batch = JSON.parse(fs.readFileSync(path.join(OUT, "RESEARCH_CANDIDATES.json"), "utf8"));

const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inventoryTableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const findingsTableId = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];

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
  stream: "TRACK_A",
  wave: batch.wave,
  researchCostUsd: 0,
  operations: [],
  summary: { creates: 0, updates: 0, noops: 0, conflicts: 0, staged: 0, researchOnly: 0, skipped: 0 },
  first100Checks: [],
  readBack: [],
  errors: [],
};

if (!token) {
  ledger.errors.push("NO_AIRTABLE_TOKEN");
  fs.writeFileSync(path.join(OUT, APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json"), JSON.stringify(ledger, null, 2));
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}

const planItems = [];
const persistCandidates = batch.candidates.filter((c) => c.persistEligible && c.discoveryPropertyId && c.findingId);

for (const c of persistCandidates) {
  const agreementOpp = classifyAgreementVsOpportunity(c);
  const first100 = qualifiesPrincipalFirst100(c);
  ledger.first100Checks.push({ id: c.id, first100, agreementOpp });

  const sourceUrl = c.sources?.[0]?.url || c.sourceUrl;
  const hotelPayload = {
    discoveryPropertyId: c.discoveryPropertyId,
    hotelName: c.hotelName,
    hotelNameNormalized: String(c.hotelName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim(),
    city: c.physicalLocation?.city || null,
    stateProvince: c.physicalLocation?.stateProvince || null,
    physicalCountry: c.physicalLocation?.country || null,
    address: c.physicalLocation?.address || null,
    currentBrand: c.currentBrand || null,
    historicalBrand: c.historicalBrand || null,
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    sourceUrl,
    discoveryMethod: "CURRENT_BACKWARD_PUBLIC_RESEARCH",
    notes: `Track A continuous waves | ${c.qualificationTier || ""} | ${c.currentDecisionStatus || ""}`,
    verified: true,
    evidenceSupported: true,
    destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  };

  const oppPayload = {
    findingId: c.findingId,
    id: c.findingId,
    commercialCategory: c.commercialCategory || c.opportunityType,
    commercialThesis: c.commercialTrigger,
    commercialTrigger: c.commercialTrigger,
    commercialRelevance: c.commercialRelevance,
    commercialTiming: c.commercialTiming,
    commercialAccessibility: c.commercialAccessibility,
    evidencePathway: c.evidencePathway,
    engagementRoute: c.engagementRoute,
    decisionParty: c.decisionParty || c.owner || null,
    decisionEvidence: c.decisionEvidence,
    nextDecisionDate: c.nextDecisionDate || null,
    sourcePublicationDate: c.sources?.[0]?.publicationDate || null,
    sourceUrl,
    sources: c.sources,
    priority: c.priority,
    countsTowardFirst100: c.countsTowardFirst100 === true,
    evidenceConfidence: "HIGH",
    verificationStatus: "EVIDENCE_SUPPORTED",
    verified: true,
    evidenceSupported: true,
    destination: TABLE_HI_DISCOVERY_FINDINGS,
    notes: `CUSTOMER_BLOCKED | agreementType=${agreementOpp.agreementType} | opportunityType=${agreementOpp.opportunityType} | ${c.notes || ""}`,
  };

  const existingInv = await findByFormula(
    inventoryTableId,
    `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${c.discoveryPropertyId}"`,
  );
  const existingFinding = await findByFormula(
    findingsTableId,
    `{${MAP_DISCOVERY_FINDINGS.findingId}}="${c.findingId}"`,
  );

  planItems.push({
    ...hotelPayload,
    existingAirtableId: existingInv?.id,
    fieldsUnchanged: Boolean(existingInv),
    _candidateId: c.id,
    _kind: "inventory",
    _existing: existingInv,
  });
  planItems.push({
    ...oppPayload,
    existingAirtableId: existingFinding?.id,
    fieldsUnchanged: Boolean(existingFinding),
    _candidateId: c.id,
    _kind: "finding",
    _existing: existingFinding,
    _hotelDiscoveryPropertyId: c.discoveryPropertyId,
  });
}

const plan = planFoundationPersistBatch(planItems);
ledger.planCounts = plan.counts;

for (const item of planItems) {
  const cls = classifyWriteCategory(item);
  if (cls.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
    ledger.operations.push({ id: item.discoveryPropertyId || item.findingId, action: "NOT_ELIGIBLE", reason: cls.reason });
    ledger.summary.staged += 1;
    continue;
  }

  if (!APPLY) {
    const action = item.existingAirtableId ? "NOOP_OR_UPDATE_DRY" : "CREATE_DRY";
    ledger.operations.push({
      id: item.discoveryPropertyId || item.findingId,
      kind: item._kind,
      action,
      existingId: item.existingAirtableId || null,
      fieldsPreview:
        item._kind === "inventory" ? buildDiscoveryInventoryFields(item) : buildCommercialFindingFields(item),
    });
    if (item.existingAirtableId) ledger.summary.noops += 1;
    else ledger.summary.creates += 1;
    continue;
  }

  if (item._kind === "inventory") {
    if (item.existingAirtableId) {
      ledger.operations.push({ id: item.discoveryPropertyId, action: "NOOP", airtableId: item.existingAirtableId });
      ledger.summary.noops += 1;
    } else {
      const fields = buildDiscoveryInventoryFields(item);
      const created = await createRecord(inventoryTableId, fields);
      if (!created.ok) {
        ledger.errors.push({ inventoryCreate: item.discoveryPropertyId, err: created.json });
      } else {
        ledger.operations.push({ id: item.discoveryPropertyId, action: "CREATE", airtableId: created.json.id });
        ledger.summary.creates += 1;
        item._createdId = created.json.id;
        await new Promise((r) => setTimeout(r, 250));
      }
    }
  }
}

if (APPLY) {
  for (const item of planItems.filter((p) => p._kind === "finding")) {
    const cls = classifyWriteCategory(item);
    if (cls.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) continue;

    if (item.existingAirtableId) {
      ledger.operations.push({ id: item.findingId, action: "NOOP", airtableId: item.existingAirtableId });
      ledger.summary.noops += 1;
      continue;
    }

    const invId =
      planItems.find((p) => p._kind === "inventory" && p.discoveryPropertyId === item._hotelDiscoveryPropertyId)
        ?._createdId ||
      planItems.find((p) => p._kind === "inventory" && p.discoveryPropertyId === item._hotelDiscoveryPropertyId)
        ?.existingAirtableId ||
      null;

    const fields = buildCommercialFindingFields(item, invId);
    const created = await createRecord(findingsTableId, fields);
    if (!created.ok) {
      ledger.errors.push({ findingCreate: item.findingId, err: created.json });
    } else {
      ledger.operations.push({ id: item.findingId, action: "CREATE", airtableId: created.json.id, linkedInventory: invId });
      ledger.summary.creates += 1;
      await new Promise((r) => setTimeout(r, 250));
      const rb = await getRecord(findingsTableId, created.json.id);
      if (rb.ok) {
        ledger.readBack.push({
          findingId: item.findingId,
          airtableId: created.json.id,
          publicationState: rb.json.fields?.[MAP_DISCOVERY_FINDINGS.notes] || null,
          findingType: rb.json.fields?.[MAP_DISCOVERY_FINDINGS.findingType] || null,
          fieldName: rb.json.fields?.[MAP_DISCOVERY_FINDINGS.fieldName] || null,
        });
      }
    }
  }
}

const outName = APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json";
fs.writeFileSync(path.join(OUT, outName), JSON.stringify(ledger, null, 2));
console.log(JSON.stringify({ mode: ledger.mode, summary: ledger.summary, errors: ledger.errors.length }, null, 2));
if (ledger.errors.length) process.exit(1);
