#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getGdiOpportunitiesAirtableBaseId } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { GDI_OPPORTUNITIES_TABLE_NAME } from "../lib/group-demand-intelligence/opportunity-field-map.js";

const OUT = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../reports/gdi-local-storage-parity"
);
fs.mkdirSync(OUT, { recursive: true });

const baseId = getGdiOpportunitiesAirtableBaseId();
const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const meta = await fetch(
  `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
  { headers: { Authorization: `Bearer ${key}` } }
);
const body = await meta.json();
if (!meta.ok) {
  console.error(body);
  process.exit(1);
}
const t = (body.tables || []).find((x) => x.name === GDI_OPPORTUNITIES_TABLE_NAME);
const result = {
  baseId,
  tableName: GDI_OPPORTUNITIES_TABLE_NAME,
  tableId: t?.id || null,
  fieldCount: t?.fields?.length || null,
  hasPayload: !!(t?.fields || []).find((f) => f.name === "opportunityPayloadJson"),
  hasHotelId: !!(t?.fields || []).find((f) => f.name === "hotelId"),
  hasOpportunityId: !!(t?.fields || []).find((f) => f.name === "opportunityId"),
  linkageFields: (t?.fields || [])
    .filter((f) =>
      /peVenue|peSignal|hotelVenueFit|decisionId|primaryContact|opportunityPayload/.test(
        f.name
      )
    )
    .map((f) => ({ name: f.name, type: f.type })),
  note:
    "Reviewed prospects store contacts/sources/evidence inside opportunityPayloadJson; PE/Decision columns are optional linkage, not required for reviewed-package publication.",
};
fs.writeFileSync(
  path.join(OUT, "airtable-schema-probe.json"),
  JSON.stringify(result, null, 2) + "\n"
);
console.log(JSON.stringify(result, null, 2));
