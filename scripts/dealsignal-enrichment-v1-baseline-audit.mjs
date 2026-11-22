#!/usr/bin/env node
/**
 * DealSignal Enrichment V1 — live Airtable baseline audit.
 * CUSTOMER_BLOCKED. Read-only.
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

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const OUT = path.join(ROOT, "reports/dealsignal-enrichment-v1");
fs.mkdirSync(OUT, { recursive: true });

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const invTable = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const findTable = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];

const QUALIFIED_FINDING_IDS = [
  "find_cb1_lakeville_bct_brand_search_2026",
  "find_cb2b_shorebreak_huntington_bct_2026",
  "find_cb2b_courtyard_denver_bct_2026",
  "find_cfa_hilton_trinidad_mct_hma_sale_2026",
  "find_cb3_embassy_one_bengaluru_mct_2026",
  "find_cb4_ahip_aimbridge_mct_2026",
];

const QUALIFIED_DISC_IDS = [
  "disc_cb1_lakeville_inn_mn_2026",
  "disc_cb2b_shorebreak_huntington_2026",
  "disc_cb2b_courtyard_denver_downtown_2026",
  "disc_cfa_hilton_trinidad_pos_2026",
  "disc_cb3_embassy_one_bengaluru_2026",
  "disc_cb4_ahip_remaining_portfolio_mct_2026",
];

async function listAll(tableId) {
  const records = [];
  let offset = null;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset || null;
    await new Promise((r) => setTimeout(r, 200));
  } while (offset);
  return records;
}

async function findBy(tableId, field, value) {
  const qs = new URLSearchParams({
    filterByFormula: `{${field}}="${value}"`,
    maxRecords: "1",
  });
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.records?.[0] || null;
}

if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}

const findings = await listAll(findTable);
const inventory = await listAll(invTable);

const commercialFindings = findings.filter((r) => {
  const field = String(r.fields[MAP_DISCOVERY_FINDINGS.fieldName] || "");
  const notes = String(r.fields[MAP_DISCOVERY_FINDINGS.notes] || "");
  const val = String(r.fields[MAP_DISCOVERY_FINDINGS.valueObserved] || "");
  return (
    field.includes("COMMERCIAL_OPPORTUNITY") ||
    notes.includes("first100") ||
    notes.includes("CUSTOMER_BLOCKED") ||
    /BCT|MCT|CONCESSION|OPERATOR|FRANCHISE/i.test(val) ||
    /PRIORITY_A|PRIORITY_B|FIRST_100/i.test(notes)
  );
});

const qualified = [];
for (const fid of QUALIFIED_FINDING_IDS) {
  const rec = findings.find((r) => r.fields[MAP_DISCOVERY_FINDINGS.findingId] === fid) || (await findBy(findTable, MAP_DISCOVERY_FINDINGS.findingId, fid));
  if (rec) {
    qualified.push({
      airtableId: rec.id,
      findingId: fid,
      findingType: rec.fields[MAP_DISCOVERY_FINDINGS.findingType] || null,
      fieldName: rec.fields[MAP_DISCOVERY_FINDINGS.fieldName] || null,
      sourceUrl: rec.fields[MAP_DISCOVERY_FINDINGS.sourceUrl] || null,
      valueObserved: rec.fields[MAP_DISCOVERY_FINDINGS.valueObserved] || null,
      evidenceSnippet: rec.fields[MAP_DISCOVERY_FINDINGS.evidenceSnippet] || null,
      confidence: rec.fields[MAP_DISCOVERY_FINDINGS.confidence] || null,
      verificationStatus: rec.fields[MAP_DISCOVERY_FINDINGS.verificationStatus] || null,
      notes: rec.fields[MAP_DISCOVERY_FINDINGS.notes] || null,
      relatedOrgName: rec.fields[MAP_DISCOVERY_FINDINGS.relatedOrgName] || null,
      effectiveDate: rec.fields[MAP_DISCOVERY_FINDINGS.effectiveDate] || null,
      reportingDate: rec.fields[MAP_DISCOVERY_FINDINGS.reportingDate] || null,
    });
  } else {
    qualified.push({ findingId: fid, missing: true });
  }
  await new Promise((r) => setTimeout(r, 150));
}

const qualifiedInv = [];
for (const did of QUALIFIED_DISC_IDS) {
  const rec =
    inventory.find((r) => r.fields[MAP_DISCOVERY_INVENTORY.discoveryPropertyId] === did) ||
    (await findBy(invTable, MAP_DISCOVERY_INVENTORY.discoveryPropertyId, did));
  if (rec) {
    qualifiedInv.push({
      airtableId: rec.id,
      discoveryPropertyId: did,
      hotelName: rec.fields[MAP_DISCOVERY_INVENTORY.hotelName] || null,
      city: rec.fields[MAP_DISCOVERY_INVENTORY.city] || null,
      physicalCountry: rec.fields[MAP_DISCOVERY_INVENTORY.physicalCountry] || null,
      currentBrand: rec.fields[MAP_DISCOVERY_INVENTORY.currentBrand] || null,
      sourceUrl: rec.fields[MAP_DISCOVERY_INVENTORY.sourceUrl] || null,
      notes: rec.fields[MAP_DISCOVERY_INVENTORY.notes] || null,
    });
  } else {
    qualifiedInv.push({ discoveryPropertyId: did, missing: true });
  }
  await new Promise((r) => setTimeout(r, 150));
}

const emergingPool = commercialFindings
  .filter((r) => !QUALIFIED_FINDING_IDS.includes(r.fields[MAP_DISCOVERY_FINDINGS.findingId]))
  .map((r) => {
    const notes = String(r.fields[MAP_DISCOVERY_FINDINGS.notes] || "");
    const val = String(r.fields[MAP_DISCOVERY_FINDINGS.valueObserved] || "");
    const field = String(r.fields[MAP_DISCOVERY_FINDINGS.fieldName] || "");
    let rank = 0;
    if (/PRIORITY_A|FIRST_100|OPEN_|PRIVATE_OPPORTUNITY|EMERGING/i.test(notes + val)) rank += 3;
    if (/PRIORITY_B|ADJACENT|AWARD/i.test(notes + val)) rank += 2;
    if (r.fields[MAP_DISCOVERY_FINDINGS.sourceUrl]) rank += 1;
    if (/COMMERCIAL_OPPORTUNITY/i.test(field)) rank += 2;
    if (/COMMITTED|COMPLETED|HISTORICAL|AWARD_PENDING/i.test(notes + val)) rank -= 2;
    return {
      airtableId: r.id,
      findingId: r.fields[MAP_DISCOVERY_FINDINGS.findingId] || null,
      findingType: r.fields[MAP_DISCOVERY_FINDINGS.findingType] || null,
      fieldName: field,
      sourceUrl: r.fields[MAP_DISCOVERY_FINDINGS.sourceUrl] || null,
      valueObserved: val.slice(0, 400),
      notes: notes.slice(0, 300),
      confidence: r.fields[MAP_DISCOVERY_FINDINGS.confidence] || null,
      rank,
    };
  })
  .sort((a, b) => b.rank - a.rank);

const topEmerging = emergingPool.slice(0, 25);

const baseline = {
  asOf: new Date().toISOString(),
  publicationState: "CUSTOMER_BLOCKED",
  wave: "DEALSIGNAL_ENRICHMENT_V1",
  branch: "feat/hif-v6-track-a-cb5-trigger-first-portfolio",
  headSha: "39b2e9aff9b10086eef0441e92a00769c72e1dd7",
  counts: {
    inventoryRecords: inventory.length,
    findingsRecords: findings.length,
    commercialFindingsHeuristic: commercialFindings.length,
    qualifiedFindingIdsRequested: QUALIFIED_FINDING_IDS.length,
    qualifiedFindingsFound: qualified.filter((q) => !q.missing).length,
    qualifiedInventoryFound: qualifiedInv.filter((q) => !q.missing).length,
    emergingPoolSize: emergingPool.length,
    topEmergingSelected: topEmerging.length,
  },
  qualifiedFindings: qualified,
  qualifiedInventory: qualifiedInv,
  topEmergingCandidates: topEmerging,
  gapsObserved: {
    outreachReadyContacts: "Not assessed in this baseline — prior DealSignal baseline reported 0",
    namedDecisionMakersInFindings: qualified.filter((q) => q.relatedOrgName).length,
    sourceUrlPresentQualified: qualified.filter((q) => q.sourceUrl).length,
  },
};

fs.writeFileSync(path.join(OUT, "BASELINE_AIRTABLE_AUDIT.json"), JSON.stringify(baseline, null, 2));
console.log(
  JSON.stringify(
    {
      inventory: inventory.length,
      findings: findings.length,
      commercialHeuristic: commercialFindings.length,
      qualifiedFound: baseline.counts.qualifiedFindingsFound,
      emergingPool: emergingPool.length,
      topEmerging: topEmerging.length,
    },
    null,
    2,
  ),
);
