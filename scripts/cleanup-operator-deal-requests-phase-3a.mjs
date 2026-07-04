#!/usr/bin/env node
/**
 * Phase 3A — archive duplicate validation ODR rows for a deal.
 * Keeps the newest row per deal+company; archives the rest.
 *
 * Run: node scripts/cleanup-operator-deal-requests-phase-3a.mjs
 * Apply: node scripts/cleanup-operator-deal-requests-phase-3a.mjs --apply
 */
import "../load-env.js";
import {
  findOdrRowsForDeal,
  pickLatestOdrForDealAndCompany,
} from "../lib/dealality/odr-owner-create.js";
import { MAP_ODR_AIRTABLE } from "../api/operator-deal-requests-fields.js";
import { getOdrAirtableBase } from "../lib/dealality/odr-owner-create.js";

const DEAL_ID = process.env.ODR_CLEANUP_DEAL_ID || "rec6JMTqtSUn1ygtd";
const COMPANY =
  process.env.ODR_CLEANUP_COMPANY || "GHL Hoteles (GHL Holding)";
const apply = process.argv.includes("--apply");

const base = getOdrAirtableBase();
const rows = await findOdrRowsForDeal(base, DEAL_ID);
const ghlRows = rows.filter((r) => {
  const name = String(r.fields?.[MAP_ODR_AIRTABLE.operatingCompanyName] || "").trim();
  return name.toLowerCase() === COMPANY.toLowerCase();
});

if (!ghlRows.length) {
  console.log("No ODR rows to clean for", DEAL_ID, COMPANY);
  process.exit(0);
}

const keep = pickLatestOdrForDealAndCompany(ghlRows, DEAL_ID, COMPANY);
const archiveIds = ghlRows.filter((r) => r.id !== keep.id).map((r) => r.id);

console.log(`Deal ${DEAL_ID} / ${COMPANY}: ${ghlRows.length} row(s)`);
console.log(`Keep: ${keep.id} (status: ${keep.fields?.[MAP_ODR_AIRTABLE.status] || "—"})`);
console.log(`Archive candidates: ${archiveIds.length}`);

if (!archiveIds.length) {
  console.log("Nothing to archive.");
  process.exit(0);
}

if (!apply) {
  console.log("Dry run — pass --apply to set Status=Archived on duplicates.");
  archiveIds.forEach((id) => console.log("  would archive:", id));
  process.exit(0);
}

for (let i = 0; i < archiveIds.length; i += 10) {
  const batch = archiveIds.slice(i, i + 10).map((id) => ({
    id,
    fields: { [MAP_ODR_AIRTABLE.status]: "Archived" },
  }));
  await base(MAP_ODR_AIRTABLE.table).update(batch);
}

console.log(`Archived ${archiveIds.length} duplicate row(s).`);
