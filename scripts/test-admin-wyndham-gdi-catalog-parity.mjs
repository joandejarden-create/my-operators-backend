/**
 * Local Admin GDI catalog parity for Wyndham San Diego Bayside.
 * Filesystem mirror of the published Airtable set — no Airtable writes.
 *
 *   node scripts/test-admin-wyndham-gdi-catalog-parity.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

process.env.GDI_OPPORTUNITIES_PERSISTENCE = "filesystem";
process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 = "1";
delete process.env.GDI_DATA_ROOT;

const { getGdiPdfReportData } = await import(
  "../lib/group-demand-intelligence/reports/gdi-pdf-report-data-v1.js"
);
const { deriveGdiReportStatus } = await import(
  "../lib/admin/gdi-admin-report-row-v1.js"
);

const hotelId = "recnPB3Q02NO8Ma1o";
const westinId = "recdmo0CnhIZAwHGC";
const file = path.resolve(
  "data/group-demand-intelligence/hotels",
  hotelId,
  "opportunities.json"
);
const doc = JSON.parse(fs.readFileSync(file, "utf8"));
const facing = (doc.opportunities || []).filter(
  (o) => o.prospectHotelFacing === true
);

assert.equal(doc.hotelId, hotelId);
assert.equal(doc.source, "airtable");
assert.equal(doc.persistence, "filesystem");
assert.equal((doc.opportunities || []).length, 82);
assert.equal(facing.length, 51);

const data = await getGdiPdfReportData(hotelId);
assert.equal(data.available, true);
assert.equal(data.executiveSummary?.customerReadyCount, 5);
assert.equal(data.executiveSummary?.futureWatchCount, 11);

const status = deriveGdiReportStatus({
  available: data.available,
  reason: data.reason || null,
  ready: data.executiveSummary.customerReadyCount,
  watch: data.executiveSummary.futureWatchCount,
});
assert.equal(status, "READY");

const westinFile = path.resolve(
  "data/group-demand-intelligence/hotels",
  westinId,
  "opportunities.json"
);
assert.equal(
  fs.existsSync(westinFile),
  false,
  "Westin Tampa Waterside stays at zero facing and is not mirrored"
);

console.log(
  JSON.stringify({
    ok: true,
    hotelId,
    facing: facing.length,
    ready: data.executiveSummary.customerReadyCount,
    watch: data.executiveSummary.futureWatchCount,
    reportStatus: status,
  })
);
