#!/usr/bin/env node
/**
 * Persist CB-1 Lakeville Inn commercial opportunity to existing HI destinations.
 * Default: --dry-run. Pass --apply for controlled write + read-back.
 * CUSTOMER_BLOCKED. No HPC create. No schema mutation.
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
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inventoryTableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const findingsTableId = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];

const DISCOVERY_PROPERTY_ID = "disc_cb1_lakeville_inn_mn_2026";
const FINDING_ID = "find_cb1_lakeville_bct_brand_search_2026";

const hotel = {
  discoveryPropertyId: DISCOVERY_PROPERTY_ID,
  hotelName: "Lakeville Inn",
  hotelNameNormalized: "lakeville inn",
  city: "Lakeville",
  stateProvince: "MN",
  physicalCountry: "US",
  address: "20851 Keokuk Ave",
  currentBrand: "Independent",
  historicalBrand: "Hampton Inn",
  identityClass: "VERIFIED_PROPERTY",
  evidenceConfidence: "HIGH",
  sourceUrl: "https://www.startribune.com/hotel-sold-at-discount-lost-hampton-inn-flag-during-ice-surge/601664022",
  discoveryMethod: "CURRENT_BACKWARD_PUBLIC_RESEARCH",
  notes: "CB-1 foundation persist | ex-Hampton | JAMP/Elevate ownership seeking brand by end-2026",
  verified: true,
  evidenceSupported: true,
  destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
};

const opportunity = {
  findingId: FINDING_ID,
  id: FINDING_ID,
  commercialCategory: "BRAND_CONVERSION_TARGET",
  commercialThesis:
    "New ownership (Elevate Hospitality / JAMP Hotels) publicly states goal to secure a new brand agreement by end of 2026 after Hilton Hampton franchise termination.",
  commercialTrigger: "Franchise termination + ownership change + owner-stated brand search",
  commercialRelevance: "VERIFIED_RELEVANT",
  commercialTiming: "0_6_MONTHS",
  commercialAccessibility: "ACCESSIBLE_PROBABLE",
  evidencePathway: "OWNER_LED_PRIVATE_DECISION",
  engagementRoute: "DIRECT_OWNER_ENGAGEMENT",
  decisionParty: "Jaymin Patel / Elevate Hospitality LLC / JAMP Hotels",
  decisionEvidence:
    "Star Tribune 2026-04-09: Patel exploring options with goal of new brand agreement by year-end. Chamber 2026-04-13 confirms Lakeville Inn under Elevate/JAMP.",
  nextDecisionDate: "2026-12-31",
  sourcePublicationDate: "2026-04-09",
  sourceUrl: hotel.sourceUrl,
  sources: [
    { url: hotel.sourceUrl, publicationDate: "2026-04-09" },
    { url: "https://lakevillechamber.org/member-news-lakeville-inn/", publicationDate: "2026-04-13" },
  ],
  priority: "PRIORITY_A",
  countsTowardFirst100: true,
  evidenceConfidence: "HIGH",
  verificationStatus: "EVIDENCE_SUPPORTED",
  verified: true,
  evidenceSupported: true,
  destination: TABLE_HI_DISCOVERY_FINDINGS,
};

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

const inventoryFields = buildDiscoveryInventoryFields(hotel);
const findingFieldsPreview = buildCommercialFindingFields(opportunity);

const hotelClass = classifyWriteCategory(hotel);
const oppClass = classifyWriteCategory(opportunity);

const existingInv = token
  ? await findByFormula(inventoryTableId, `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${DISCOVERY_PROPERTY_ID}"`)
  : null;
const existingFinding = token
  ? await findByFormula(findingsTableId, `{${MAP_DISCOVERY_FINDINGS.findingId}}="${FINDING_ID}"`)
  : null;

const planItems = [
  {
    ...hotel,
    existingAirtableId: existingInv?.id,
    fieldsUnchanged: Boolean(existingInv),
  },
  {
    ...opportunity,
    existingAirtableId: existingFinding?.id,
    fieldsUnchanged: Boolean(existingFinding),
  },
];
const plan = planFoundationPersistBatch(planItems);

const ledger = {
  asOf: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN",
  publicationState: "CUSTOMER_BLOCKED",
  baseId: GRAPH_BASE_ID_CANONICAL,
  hotelWriteClass: hotelClass,
  opportunityWriteClass: oppClass,
  planCounts: plan.counts,
  proposed: {
    inventoryFields,
    findingFields: findingFieldsPreview,
  },
  existing: {
    inventoryId: existingInv?.id || null,
    findingId: existingFinding?.id || null,
  },
  applied: null,
  readBack: null,
  errors: [],
};

if (!token) {
  ledger.errors.push("NO_AIRTABLE_TOKEN");
} else if (!APPLY) {
  ledger.note = "Dry-run only — re-run with --apply to persist AUTO_PERSIST_ELIGIBLE rows";
} else {
  if (hotelClass.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
    ledger.errors.push(`HOTEL_NOT_ELIGIBLE:${hotelClass.reason}`);
  }
  if (oppClass.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
    ledger.errors.push(`OPP_NOT_ELIGIBLE:${oppClass.reason}`);
  }

  const applied = { inventory: null, finding: null };
  let discoveryRecId = existingInv?.id || null;

  if (!existingInv && !ledger.errors.length) {
    const created = await createRecord(inventoryTableId, inventoryFields);
    if (!created.ok) ledger.errors.push({ inventoryCreate: created.json });
    else {
      applied.inventory = { action: "CREATE", id: created.json.id };
      discoveryRecId = created.json.id;
    }
  } else if (existingInv) {
    applied.inventory = { action: "NOOP", id: existingInv.id };
  }

  if (!existingFinding && !ledger.errors.length) {
    const findingFields = buildCommercialFindingFields(opportunity, discoveryRecId);
    const created = await createRecord(findingsTableId, findingFields);
    if (!created.ok) ledger.errors.push({ findingCreate: created.json });
    else applied.finding = { action: "CREATE", id: created.json.id };
  } else if (existingFinding) {
    applied.finding = { action: "NOOP", id: existingFinding.id };
  }

  await new Promise((r) => setTimeout(r, 300));
  const rbInv = await findByFormula(
    inventoryTableId,
    `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${DISCOVERY_PROPERTY_ID}"`,
  );
  const rbFind = await findByFormula(findingsTableId, `{${MAP_DISCOVERY_FINDINGS.findingId}}="${FINDING_ID}"`);
  ledger.applied = applied;
  ledger.readBack = {
    inventoryOk: Boolean(rbInv),
    inventoryId: rbInv?.id || null,
    inventoryPublicationState: rbInv?.fields?.["Publication State"] || null,
    findingOk: Boolean(rbFind),
    findingId: rbFind?.id || null,
    findingType: rbFind?.fields?.["Finding Type"] || null,
    findingFieldName: rbFind?.fields?.["Field Name"] || null,
  };
}

const outDir = path.join(ROOT, "reports/sec-edgar-hif-foundation-persistence");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, APPLY ? "LAKEVILLE_PERSIST_APPLY.json" : "LAKEVILLE_PERSIST_DRY_RUN.json");
fs.writeFileSync(outPath, JSON.stringify(ledger, null, 2));
console.log(JSON.stringify({ ok: ledger.errors.length === 0, mode: ledger.mode, planCounts: plan.counts, applied: ledger.applied, readBack: ledger.readBack, outPath }, null, 2));
if (ledger.errors.length) process.exitCode = 1;
