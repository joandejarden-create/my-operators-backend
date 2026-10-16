/**
 * Admin GDI catalog/report-data follows the configured canonical source.
 * Isolated fixtures only — no production hotel counts.
 *
 *   node scripts/test-admin-gdi-catalog-canonical-source.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-admin-canonical-"));
process.env.GDI_DATA_ROOT = root;
process.env.GDI_OPPORTUNITIES_READ_FS_FALLBACK = "1";
process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 = "0";

const { getGdiPdfReportData } = await import(
  "../lib/group-demand-intelligence/reports/gdi-pdf-report-data-v1.js"
);
const { deriveGdiReportStatus, normalizeGdiAdminReportRow } = await import(
  "../lib/admin/gdi-admin-report-row-v1.js"
);
const { invalidateGdiHotelReadCache } = await import(
  "../lib/group-demand-intelligence/read-cache.js"
);

function hotelConfig(hotelId) {
  return {
    hotelId,
    displayName: "Fixture Hotel",
    geo: { city: "Fixture City" },
    commercialPriorities: {},
    capabilityProfile: {},
  };
}

function watchOpp(id) {
  return {
    id,
    title: "Fixture Watch Cycle",
    organizationName: "Fixture Watch Org",
    prospectHotelFacing: false,
    customerFacingState: "FUTURE_WATCH",
    eventStartDate: "2027-06-01",
    officialSource: "https://example.com/watch",
    sources: [{ url: "https://example.com/watch" }],
  };
}

function facingOpp(id) {
  return {
    id,
    title: "Fixture Facing Prospect",
    organizationName: "Fixture Facing Org",
    prospectHotelFacing: true,
    customerFacingState: "ACTIVE",
    eventStartDate: "2027-07-01",
    officialSource: "https://example.com/facing",
    sources: [{ url: "https://example.com/facing" }],
  };
}

function storedOpp(id) {
  return {
    id,
    title: "Fixture Stored Record",
    organizationName: "Fixture Stored Org",
    prospectHotelFacing: false,
    customerFacingState: "ACTIVE",
    eventStartDate: "2027-08-01",
    officialSource: "https://example.com/stored",
    sources: [{ url: "https://example.com/stored" }],
  };
}

function writeSnapshot(hotelId, opportunities) {
  const dir = path.join(root, "hotels", hotelId);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    path.join(dir, "opportunities.json"),
    JSON.stringify({ hotelId, opportunities, persistence: "filesystem" })
  );
}

function statusFor(data) {
  return deriveGdiReportStatus({
    available: data.available,
    reason: data.reason,
    ready: data.executiveSummary?.customerReadyCount ?? data.customerReadyCount ?? 0,
    watch: data.executiveSummary?.futureWatchCount ?? data.futureWatchCount ?? 0,
  });
}

async function loadReport(hotelId, loadAirtable) {
  invalidateGdiHotelReadCache(hotelId);
  return getGdiPdfReportData(hotelId, {
    hotelConfig: hotelConfig(hotelId),
    loadAirtable,
    nowDate: "2026-10-17",
  });
}

process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
const airtableHotel = "fixture_airtable_no_file";
const airtableFile = path.join(root, "hotels", airtableHotel, "opportunities.json");
let airtableCalls = 0;
const airtableRows = [facingOpp("facing-1"), watchOpp("watch-1")];
const airtableReport = await loadReport(airtableHotel, async () => {
  airtableCalls += 1;
  return airtableRows;
});
assert.equal(fs.existsSync(airtableFile), false);
assert.equal(airtableCalls, 1);
assert.equal(airtableReport.persistence, "airtable");
assert.equal(airtableReport.readFailed, false);
assert.equal(airtableReport.customerFacingProspectCount, 1);
assert.equal(airtableReport.executiveSummary.customerFacingProspectCount, 1);
assert.equal(airtableReport.executiveSummary.customerReadyCount, 0);
assert.equal(airtableReport.executiveSummary.futureWatchCount, 1);
assert.notEqual(statusFor(airtableReport), "READY");

const refreshHotel = "fixture_refresh";
let refreshGeneration = 0;
const firstRefresh = await loadReport(refreshHotel, async () => {
  refreshGeneration += 1;
  return refreshGeneration === 1
    ? [facingOpp("facing-a")]
    : [facingOpp("facing-a"), facingOpp("facing-b"), watchOpp("watch-b")];
});
const secondRefresh = await loadReport(refreshHotel, async () => {
  refreshGeneration += 1;
  return refreshGeneration === 1
    ? [facingOpp("facing-a")]
    : [facingOpp("facing-a"), facingOpp("facing-b"), watchOpp("watch-b")];
});
assert.equal(firstRefresh.customerFacingProspectCount, 1);
assert.equal(secondRefresh.customerFacingProspectCount, 2);
assert.equal(secondRefresh.executiveSummary.futureWatchCount, 1);
assert.equal(secondRefresh.executiveSummary.customerReadyCount, 0);
assert.notEqual(statusFor(secondRefresh), "READY");

process.env.GDI_OPPORTUNITIES_PERSISTENCE = "filesystem";
const fsHotel = "fixture_filesystem";
writeSnapshot(fsHotel, [facingOpp("fs-facing"), storedOpp("fs-stored")]);
let fsAirtableCalls = 0;
const fsReport = await loadReport(fsHotel, async () => {
  fsAirtableCalls += 1;
  return [facingOpp("should-not-read"), watchOpp("should-not-read")];
});
assert.equal(fsAirtableCalls, 0);
assert.equal(fsReport.persistence, "filesystem");
assert.equal(fsReport.customerFacingProspectCount, 1);
assert.equal(fsReport.customerReadyCount ?? fsReport.executiveSummary?.customerReadyCount ?? 0, 0);

writeSnapshot(fsHotel, [facingOpp("fs-1"), facingOpp("fs-2"), facingOpp("fs-3")]);
const fsRefresh = await loadReport(fsHotel, async () => {
  throw new Error("filesystem mode must not call Airtable");
});
assert.equal(fsRefresh.persistence, "filesystem");
assert.equal(fsRefresh.customerFacingProspectCount, 3);

process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
const failHotel = "fixture_read_failure";
writeSnapshot(failHotel, [facingOpp("stale-facing")]);
const failed = await loadReport(failHotel, async () => {
  throw new Error("airtable unavailable");
});
assert.equal(failed.readFailed, true);
assert.equal(failed.reason, "canonical_opportunity_read_failed");
assert.equal(failed.customerFacingProspectCount, null);
assert.equal(failed.available, false);
assert.notEqual(statusFor(failed), "READY");

const emptyHotel = "fixture_empty";
writeSnapshot(emptyHotel, [facingOpp("ignored-facing")]);
const empty = await loadReport(emptyHotel, async () => []);
assert.equal(empty.readFailed, false);
assert.equal(empty.customerFacingProspectCount, 0);
assert.match(empty.reason, /No customer-ready/);
assert.notEqual(empty.reason, "canonical_opportunity_read_failed");
assert.equal(statusFor(empty), "BLOCKED");

const zeroHotel = "fixture_zero_facing";
const zero = await loadReport(zeroHotel, async () => [
  storedOpp("stored-1"),
  storedOpp("stored-2"),
]);
assert.equal(zero.readFailed, false);
assert.equal(zero.customerFacingProspectCount, 0);
assert.equal(zero.customerReadyCount ?? zero.executiveSummary?.customerReadyCount ?? 0, 0);
assert.notEqual(statusFor(zero), "READY");

const pdfRow = normalizeGdiAdminReportRow({
  hotelId: "fixture_pdf",
  displayName: "Fixture Hotel",
  available: false,
  readFailed: false,
  reason: empty.reason,
  ready: 0,
  watch: 0,
  customerFacingCount: 0,
  pdfReady: true,
  pdfStatus: "READY",
  lastGeneratedAt: "2026-10-13T00:00:00.000Z",
});
assert.equal(pdfRow.customerFacingCount, 0);
assert.equal(pdfRow.readyCount, 0);
assert.equal(pdfRow.reportStatus, "BLOCKED");
assert.equal(pdfRow.pdfStatus, "READY");
assert.equal(pdfRow.lastGeneratedAt, "2026-10-13T00:00:00.000Z");

const failedRow = normalizeGdiAdminReportRow({
  hotelId: failHotel,
  available: false,
  readFailed: true,
  reason: failed.reason,
  customerFacingCount: null,
  pdfReady: false,
});
assert.equal(failedRow.readFailed, true);
assert.equal(failedRow.customerFacingCount, null);
assert.equal(failedRow.pdfStatus, "MISSING");

console.log(
  JSON.stringify({
    ok: true,
    airtableFacing: airtableReport.customerFacingProspectCount,
    airtableReady: airtableReport.executiveSummary.customerReadyCount,
    airtableWatch: airtableReport.executiveSummary.futureWatchCount,
    refreshedFacing: secondRefresh.customerFacingProspectCount,
    filesystemFacing: fsRefresh.customerFacingProspectCount,
    readFailed: failed.reason,
    emptyReason: empty.reason,
    zeroFacing: zero.customerFacingProspectCount,
  })
);
