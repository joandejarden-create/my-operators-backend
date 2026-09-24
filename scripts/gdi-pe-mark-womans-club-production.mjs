/**
 * Ensure Woman's Club production PE opp has isTestData=false.
 */
import "../load-env.js";
import { escapeAirtableFormulaValue } from "../lib/airtable-utils.js";
import {
  GDI_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_OPPORTUNITY as F,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { getPeBase } from "../lib/group-demand-intelligence/private-events/airtable-client.js";

const OPP = "gdi_pe_781f12393f8117e7";
const base = getPeBase();
const rows = await base(GDI_OPPORTUNITIES_TABLE_NAME)
  .select({
    filterByFormula: `{opportunityId}='${escapeAirtableFormulaValue(OPP)}'`,
    maxRecords: 1,
  })
  .firstPage();
const rec = rows[0];
if (!rec) {
  console.error("NOT_FOUND");
  process.exit(1);
}
const f = rec.fields || {};
let payload = {};
try {
  payload = JSON.parse(f[F.opportunityPayloadJson] || "null") || {};
} catch {
  payload = {};
}
payload.isTestData = false;
payload.customerVisible = true;

const patch = {
  [F.isTestData]: false,
  [F.opportunityPayloadJson]: JSON.stringify(payload),
};
await base(GDI_OPPORTUNITIES_TABLE_NAME).update(rec.id, patch);
console.log(
  JSON.stringify(
    {
      recordId: rec.id,
      opportunityId: OPP,
      isTestData: false,
      customerVisible: true,
      updated: true,
    },
    null,
    2
  )
);
