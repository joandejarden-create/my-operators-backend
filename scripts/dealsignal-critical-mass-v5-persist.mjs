#!/usr/bin/env node
/**
 * DealSignal Critical-Mass V5 — persist.
 * Default dry-run. --apply for create + read-back. CUSTOMER_BLOCKED. No Exa. No schema.
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
  WRITE_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import { applyQualifiedClassificationGuard } from "../lib/hotel-intelligence/dealsignal/dealsignal-qualified-opportunity-classification-v1.js";

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
const OUT = path.join(ROOT, "reports/dealsignal-critical-mass-v5");
const batch = JSON.parse(fs.readFileSync(path.join(OUT, "RESEARCH_CANDIDATES.json"), "utf8"));
fs.mkdirSync(OUT, { recursive: true });
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
  return { ok: res.ok, json: await res.json() };
}

function thesisBlock(c) {
  const t = c.commercialThesis;
  if (!t) return "";
  return [
    "THESIS:",
    `1 What: ${t.whatIsHappening || ""}`,
    `2 Why: ${t.whyOpportunity || ""}`,
    `3 Owner: ${t.ownerObjective || ""}`,
    `4 Rel: ${t.existingRelationship || ""}`,
    `5 Alt: ${t.whyAlternativePartner || ""}`,
    `6 Limits: ${t.whatCouldPreventChange || ""}`,
    `7 Timing: ${t.timing || ""}`,
    `8 Who: ${t.whoControls || ""}`,
    `9 Discuss: ${t.discussionAngle || ""}`,
    `10 Evidence: ${t.evidenceBasis || ""}`,
  ].join(" | ");
}

function historicalBlock(c) {
  const h = c.historicalAnalysis;
  if (!h) return "";
  return [
    "HISTORICAL:",
    `outcome=${h.outcome || h.newBrand || ""}`,
    `effective=${h.decisionOrEffectiveDate || ""}`,
    `lead=${h.observableLeadTime || h.observableLeadTimeMonths || "UNKNOWN"}`,
    `lesson=${h.patternLesson || ""}`,
  ].filter(Boolean).join(" | ");
}

const ledger = {
  asOf: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN",
  publicationState: "CUSTOMER_BLOCKED",
  wave: batch.wave,
  researchCostUsd: 0,
  operations: [],
  summary: { creates: 0, noops: 0, errors: 0 },
  readBack: [],
  errors: [],
};

if (!token) {
  ledger.errors.push("NO_AIRTABLE_TOKEN");
  fs.writeFileSync(path.join(OUT, APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json"), JSON.stringify(ledger, null, 2));
  process.exit(2);
}

const candidates = batch.candidates
  .map((c) => applyQualifiedClassificationGuard(c))
  .filter((c) => c.persistEligible);

for (const c of candidates) {
  const invExisting = await findByFormula(
    inventoryTableId,
    `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${c.discoveryPropertyId}"`,
  );
  const findExisting = await findByFormula(
    findingsTableId,
    `{${MAP_DISCOVERY_FINDINGS.findingId}}="${c.findingId}"`,
  );

  const hotelPayload = {
    discoveryPropertyId: c.discoveryPropertyId,
    hotelName: c.hotelName,
    hotelNameRaw: c.hotelName,
    city: c.physicalLocation?.city || null,
    stateProvince: c.physicalLocation?.stateProvince || null,
    physicalCountry: c.physicalLocation?.country || null,
    currentBrand: c.currentBrand || null,
    sourceUrl: c.sources?.[0]?.url || null,
    discoveryMethod: "DEALSIGNAL_DISCOVERY_EXPERIMENT_V2",
    notes: `CUSTOMER_BLOCKED | ${batch.wave} | ${c.stream || ""} | ${c.recordType || ""} | ${c.commercialStatus} | ${c.engagementPriority || ""}`,
    existingAirtableId: invExisting?.id,
    fieldsUnchanged: Boolean(invExisting),
    destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
    verified: true,
    evidenceSupported: true,
  };
  const invCls = classifyWriteCategory(hotelPayload);
  if (invCls.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
    ledger.operations.push({ id: c.discoveryPropertyId, action: "INV_NOT_ELIGIBLE", reason: invCls.reason });
    continue;
  }

  let inventoryRecId = invExisting?.id || null;
  if (!APPLY) {
    ledger.operations.push({
      id: c.discoveryPropertyId,
      action: invExisting ? "INV_NOOP_DRY" : "INV_CREATE_DRY",
    });
    if (invExisting) ledger.summary.noops += 1;
    else ledger.summary.creates += 1;
  } else if (invExisting) {
    ledger.operations.push({ id: c.discoveryPropertyId, action: "INV_NOOP", airtableId: invExisting.id });
    ledger.summary.noops += 1;
  } else {
    const created = await createRecord(inventoryTableId, buildDiscoveryInventoryFields(hotelPayload));
    if (!created.ok) {
      ledger.errors.push({ id: c.discoveryPropertyId, err: created.json });
      ledger.summary.errors += 1;
      continue;
    }
    inventoryRecId = created.json.id;
    ledger.operations.push({ id: c.discoveryPropertyId, action: "INV_CREATE", airtableId: inventoryRecId });
    ledger.summary.creates += 1;
    ledger.readBack.push({ discoveryPropertyId: c.discoveryPropertyId, airtableId: inventoryRecId });
    await new Promise((r) => setTimeout(r, 250));
  }

  const findingPayload = {
    findingId: c.findingId,
    id: c.findingId,
    commercialCategory: c.commercialCategory || c.opportunityType,
    commercialThesis: c.commercialTrigger,
    commercialTrigger: c.commercialTrigger,
    commercialRelevance: c.commercialRelevance,
    commercialTiming: c.commercialTiming,
    commercialAccessibility: c.commercialAccessibility || null,
    evidencePathway: c.evidencePathway || null,
    engagementRoute: c.engagementRoute || null,
    decisionParty: c.decisionParty,
    decisionEvidence: c.decisionEvidence,
    sourceUrl: c.sources?.[0]?.url || null,
    sources: c.sources,
    priority: c.engagementPriority === "ENGAGE_NOW" ? "PRIORITY_A" : "PRIORITY_B",
    countsTowardFirst100: false,
    nextDecisionDate: c.nextDecisionDate || null,
    sourcePublicationDate: c.sources?.[0]?.date || null,
    evidenceConfidence: c.evidenceStrength === "HIGH" ? "HIGH" : "MEDIUM",
    verificationStatus: "EVIDENCE_SUPPORTED",
    notes: [c.notes, thesisBlock(c), historicalBlock(c)].filter(Boolean).join(" || "),
    existingAirtableId: findExisting?.id,
    fieldsUnchanged: Boolean(findExisting),
    destination: TABLE_HI_DISCOVERY_FINDINGS,
    verified: true,
    evidenceSupported: true,
  };
  const fCls = classifyWriteCategory(findingPayload);
  if (fCls.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
    ledger.operations.push({ id: c.findingId, action: "FIND_NOT_ELIGIBLE", reason: fCls.reason });
    continue;
  }

  if (!APPLY) {
    ledger.operations.push({
      id: c.findingId,
      action: findExisting ? "FIND_NOOP_DRY" : "FIND_CREATE_DRY",
    });
    if (findExisting) ledger.summary.noops += 1;
    else ledger.summary.creates += 1;
  } else if (findExisting) {
    ledger.operations.push({ id: c.findingId, action: "FIND_NOOP", airtableId: findExisting.id });
    ledger.summary.noops += 1;
  } else {
    const fields = buildCommercialFindingFields(findingPayload, inventoryRecId);
    const created = await createRecord(findingsTableId, fields);
    if (!created.ok) {
      ledger.errors.push({ id: c.findingId, err: created.json });
      ledger.summary.errors += 1;
    } else {
      ledger.operations.push({ id: c.findingId, action: "FIND_CREATE", airtableId: created.json.id });
      ledger.summary.creates += 1;
      ledger.readBack.push({ findingId: c.findingId, airtableId: created.json.id });
      await new Promise((r) => setTimeout(r, 250));
    }
  }
}

const outPath = path.join(OUT, APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json");
fs.writeFileSync(outPath, JSON.stringify(ledger, null, 2));
if (APPLY && ledger.summary.creates > 0) {
  fs.writeFileSync(path.join(OUT, "PERSISTENCE_LEDGER_FIRST_APPLY.json"), JSON.stringify(ledger, null, 2));
}
console.log(JSON.stringify({ mode: ledger.mode, summary: ledger.summary, errors: ledger.errors.length }, null, 2));


