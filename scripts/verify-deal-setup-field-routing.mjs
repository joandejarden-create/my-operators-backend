/**
 * Inventory: every #dealForm name= field → Airtable table (same logic as classifyDealSetupFormField).
 * Run: node scripts/verify-deal-setup-field-routing.mjs
 * Fails if any form field appears in more than one routing set (should never happen).
 */
import fs from "fs";
import {
  LOCATION_FORM_FIELDS,
  MARKET_PERFORMANCE_FIELD_NAMES,
  STRATEGIC_INTENT_FORM_FIELDS,
  CONTACT_UPLOADS_FORM_FIELDS,
  LEASE_STRUCTURE_FORM_FIELDS,
  classifyDealSetupFormField,
  DEAL_SETUP_AIRTABLE_TABLE_NAMES,
} from "../api/schemas/deal-setup-fields.js";

function extractDealFormNames() {
  const full = fs.readFileSync(new URL("../public/new-deal-setup.html", import.meta.url), "utf8");
  const start = full.indexOf('<form id="dealForm"');
  const end = full.indexOf("</form>", start);
  if (start < 0 || end <= start) throw new Error("dealForm not found");
  const h = full.slice(start, end);
  const re = /name="([^"]+)"/g;
  const s = new Set();
  let m;
  while ((m = re.exec(h))) s.add(m[1]);
  return [...s].sort();
}

const formNames = extractDealFormNames();
const sets = {
  [DEAL_SETUP_AIRTABLE_TABLE_NAMES.LOCATION]: new Set(LOCATION_FORM_FIELDS),
  [DEAL_SETUP_AIRTABLE_TABLE_NAMES.MARKET_PERFORMANCE]: MARKET_PERFORMANCE_FIELD_NAMES,
  [DEAL_SETUP_AIRTABLE_TABLE_NAMES.STRATEGIC_INTENT]: new Set(STRATEGIC_INTENT_FORM_FIELDS),
  [DEAL_SETUP_AIRTABLE_TABLE_NAMES.CONTACT_UPLOADS]: new Set(CONTACT_UPLOADS_FORM_FIELDS),
  [DEAL_SETUP_AIRTABLE_TABLE_NAMES.LEASE]: new Set(LEASE_STRUCTURE_FORM_FIELDS),
};

const conflicts = [];
for (const k of formNames) {
  const hits = [];
  for (const [label, set] of Object.entries(sets)) {
    if (set.has(k)) hits.push(label);
  }
  if (hits.length > 1) conflicts.push({ field: k, tables: hits });
}

if (conflicts.length) {
  console.error("Routing conflicts (field in more than one linked-table set):");
  for (const c of conflicts) console.error(JSON.stringify(c));
  process.exit(1);
}

const byTable = new Map();
for (const k of formNames) {
  const t = classifyDealSetupFormField(k);
  if (!byTable.has(t)) byTable.set(t, []);
  byTable.get(t).push(k);
}

for (const t of Object.values(DEAL_SETUP_AIRTABLE_TABLE_NAMES)) {
  const list = byTable.get(t) || [];
  console.log(`\n## ${t} (${list.length} fields)\n`);
  for (const k of list.sort()) console.log(k);
}

console.log(`\n--- Total form fields: ${formNames.length}`);
