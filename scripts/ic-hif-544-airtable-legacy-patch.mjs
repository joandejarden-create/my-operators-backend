#!/usr/bin/env node
/**
 * IC PR #544 — Non-destructive Airtable PATCH for Legacy Green Bay finding.
 * Preserves prior Value Observed / Notes in history fields. No CREATE. CUSTOMER_BLOCKED.
 * Default: dry-run. Pass --apply to write.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb2-commercial-discovery");
const FINDING_ID = "recAVM0frtNV6tzLJ";
const INV_ID = "rec3XVviNYdBxEBIN";

const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const findingsTable = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];
const invTable = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];

async function get(tableId, rid) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${rid}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  return { ok: res.ok, json };
}

async function patch(tableId, rid, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${rid}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  return { ok: res.ok, json };
}

const ledger = {
  asOf: new Date().toISOString(),
  mode: APPLY ? "APPLY_PATCH" : "DRY_RUN_PATCH",
  publicationState: "CUSTOMER_BLOCKED",
  pr: 544,
  creates: 0,
  duplicatesAvoided: true,
  operations: [],
  errors: [],
};

if (!token) {
  ledger.errors.push("NO_AIRTABLE_TOKEN");
  fs.writeFileSync(path.join(OUT, "PERSISTENCE_LEDGER_IC_QUALIFICATION_PATCH.json"), JSON.stringify(ledger, null, 2));
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}

const finding = await get(findingsTable, FINDING_ID);
const inv = await get(invTable, INV_ID);
if (!finding.ok || !inv.ok) {
  ledger.errors.push({ finding: finding.json, inv: inv.json });
  fs.writeFileSync(path.join(OUT, "PERSISTENCE_LEDGER_IC_QUALIFICATION_PATCH.json"), JSON.stringify(ledger, null, 2));
  console.error("READ_FAILED");
  process.exit(1);
}

const priorValue = finding.json.fields["Value Observed"] || "";
const priorNotes = finding.json.fields["Notes"] || "";
const priorInvNotes = inv.json.fields["Notes / Quarantine Reason"] || "";

const newValue =
  "IC#544 CORRECTED: Preferred Marriott soft-brand family / intra-family collection selection — NOT open competitive BCT | accessibility=ACCESS_LIMITED_PREFERRED_BRAND_FAMILY | first100=NO | relevance=VERIFIED_RELEVANT_CONVERSION_INTELLIGENCE | pathway=OWNER_LED_PRIVATE_DECISION | brandFamily=Marriott | collection=UNRESOLVED | MCT=FILLED_BY_DREAMSCAPE";

const newNotes = [
  "CUSTOMER_BLOCKED | IC#544 qualification correction 2026-11-21",
  "brandFamilyCommitmentClass=PREFERRED_BRAND_FAMILY_WITHOUT_BINDING_COMMITMENT",
  "collectionSelectionClass=INTRA_BRAND_FAMILY_COLLECTION_SELECTION",
  "openCompetitiveBrandSelection=false",
  "opportunityType=BCT_INTELLIGENCE (retained; not first-100 accessible numerator)",
  "Sources: Hotel Management 2026-10-22; Hospitality Net 2026-10-26; Pass Brief 2026-10-23",
  "--- PRIOR CLASSIFICATION (preserved) ---",
  priorNotes,
  "--- PRIOR VALUE OBSERVED (preserved) ---",
  priorValue,
].join(" | ");

const newInvNotes = [
  "Track A CB2 | IC#544 CORRECTED | EXPANDED_PIPELINE_CONVERSION_INTELLIGENCE | PREFERRED_MARRIOTT_SOFT_BRAND_COLLECTION_UNRESOLVED | not FIRST_100",
  "--- PRIOR ---",
  priorInvNotes,
].join(" | ");

const findingFields = {
  "Value Observed": newValue.slice(0, 100000),
  Notes: newNotes.slice(0, 100000),
  "Field Name": "COMMERCIAL_OPPORTUNITY_BCT_INTELLIGENCE",
  "Evidence Snippet":
    "IC review of Hotel Management / Hospitality Net / Pass Brief: ownership plans Marriott soft brand; collection unnamed = intra-family selection, not open competitive brand selection. Prior ACCESSIBLE_PROBABLE/first100=YES preserved in Notes history.",
};

const invFields = {
  "Notes / Quarantine Reason": newInvNotes.slice(0, 100000),
};

ledger.operations.push({
  kind: "finding",
  id: FINDING_ID,
  action: APPLY ? "PATCH" : "PATCH_DRY",
  fieldsPreview: findingFields,
  priorValuePreview: priorValue.slice(0, 400),
});
ledger.operations.push({
  kind: "inventory",
  id: INV_ID,
  action: APPLY ? "PATCH" : "PATCH_DRY",
  fieldsPreview: invFields,
  priorNotesPreview: priorInvNotes.slice(0, 400),
});

if (APPLY) {
  const fRes = await patch(findingsTable, FINDING_ID, findingFields);
  const iRes = await patch(invTable, INV_ID, invFields);
  ledger.operations[0].ok = fRes.ok;
  ledger.operations[1].ok = iRes.ok;
  if (!fRes.ok) ledger.errors.push({ findingPatch: fRes.json });
  if (!iRes.ok) ledger.errors.push({ invPatch: iRes.json });
  // read-back
  const f2 = await get(findingsTable, FINDING_ID);
  const i2 = await get(invTable, INV_ID);
  ledger.readBack = {
    findingValue: f2.json.fields?.["Value Observed"]?.slice(0, 300),
    findingNotesHasPrior: String(f2.json.fields?.Notes || "").includes("PRIOR CLASSIFICATION"),
    invNotesHasPrior: String(i2.json.fields?.["Notes / Quarantine Reason"] || "").includes("PRIOR"),
  };
}

fs.writeFileSync(path.join(OUT, "PERSISTENCE_LEDGER_IC_QUALIFICATION_PATCH.json"), JSON.stringify(ledger, null, 2));
console.log(JSON.stringify({ mode: ledger.mode, errors: ledger.errors.length, ops: ledger.operations.length, readBack: ledger.readBack }, null, 2));
if (ledger.errors.length) process.exit(1);
