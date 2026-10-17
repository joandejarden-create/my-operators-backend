/**
 * Admin GDI client inventory vs strict-ready, and row action visibility.
 * Isolated fixtures. Does not print share bearer URLs.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { applyContactPackageToOpportunity } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { stampLegacyVisibilityPreserved } from "../lib/group-demand-intelligence/legacy-visibility-compatibility-v1.js";
import { REVIEWED_PROSPECT_PUBLICATION_BASIS } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import { getAdpPropertyIdForHpc } from "../lib/admin/report-external-client-links-v1.js";
import { normalizeGdiAdminReportRow } from "../lib/admin/gdi-admin-report-row-v1.js";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-admin-client-"));
process.env.GDI_DATA_ROOT = root;
process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
process.env.GDI_OPPORTUNITIES_READ_FS_FALLBACK = "0";
process.env.GDI_REVIEWED_PROSPECT_PUBLICATION_V1 = "1";

const { getGdiPdfReportData } = await import(
  "../lib/group-demand-intelligence/reports/gdi-pdf-report-data-v1.js"
);
const { invalidateGdiHotelReadCache } = await import(
  "../lib/group-demand-intelligence/read-cache.js"
);

const HOTEL = "recG66DQJKP2c0UNh";

function reviewedNotReady(id) {
  let opp = {
    id,
    hotelId: HOTEL,
    title: "Example Advocacy 2027",
    organizationName: "Example Association " + id,
    fitExplanation: "Downtown fit for association lodging",
    hotelFitScore: 60,
    hotelFitScoreBasis: "gdi-hotel-fit-v1",
    hotelOpportunityThesis:
      "Downtown lodging for association attendees traveling for the multi-day conference",
    travelEvidenceNote:
      "Delegates travel overnight for the multi-day advocacy conference; room block expected",
    roomDemandStatus: "REQUIRED",
    officialSource: "https://example.org/events/2027",
    sources: [{ url: "https://example.org/events/2027" }],
    timing: "Verified: April 1–2, 2027",
    eventStartDate: "2027-04-01",
    prospectPublicationBasis: REVIEWED_PROSPECT_PUBLICATION_BASIS,
    prospectReviewSection: "PRIORITY",
    recommendedAction: "Call the published route",
    priority: "MEDIUM_PRIORITY",
  };
  opp = applyContactPackageToOpportunity(opp, {
    person: "Jane Smith",
    title: "Director of Events",
    contactMethod: "jane@example.org",
    businessPhone: "202-555-0100",
    phoneType: "direct",
    bookingResponsibility: "POTENTIAL_RELEVANT_CONTACT",
    sourceUrl: "https://example.org/staff",
    verificationDate: "2026-10-14",
  });
  opp.prospectHotelFacing = true;
  return opp;
}

const reviewed = reviewedNotReady("reviewed-1");
const legacy = stampLegacyVisibilityPreserved(reviewedNotReady("legacy-1"), {
  reason: "post_enrich_not_ready",
});
const disqualified = {
  ...reviewedNotReady("dq-1"),
  priority: "DISQUALIFIED",
};

const clientList = filterCustomerFacingOpportunities(
  filterSalespersonView([reviewed, legacy, disqualified]),
  { hotelId: HOTEL }
);
assert.equal(isGdiCustomerOpportunityReady(reviewed).ok, false);
assert.equal(isGdiCustomerOpportunityReady(legacy).ok, false);
assert.equal(clientList.length, 2, "reviewed + legacy stay on the client list");
assert.ok(!clientList.some((row) => row.id === "dq-1"));
assert.ok(clientList.length > clientList.filter((row) => isGdiCustomerOpportunityReady(row).ok).length);

async function loadReport(hotelId, rows, loader) {
  invalidateGdiHotelReadCache(hotelId);
  return getGdiPdfReportData(hotelId, {
    hotelConfig: {
      hotelId,
      displayName: "Fixture Hotel",
      geo: { city: "Fixture City" },
      commercialPriorities: {},
      capabilityProfile: {},
    },
    loadAirtable: loader || (async () => rows),
    nowDate: "2026-10-17",
  });
}

const counted = await loadReport("fixture_client_gt_ready", [reviewed, legacy, disqualified]);
assert.equal(counted.readFailed, false);
assert.equal(counted.clientOpportunityCount, 2);
assert.equal(counted.customerReadyCount, 0);
assert.ok(counted.clientOpportunityCount > counted.customerReadyCount);

const watches = Array.from({ length: 15 }, (_, i) => ({
  id: "watch-" + i,
  title: "Fixture Watch " + i,
  organizationName: "Watch Org " + i,
  prospectHotelFacing: false,
  customerFacingState: "FUTURE_WATCH",
  eventStartDate: "2027-06-01",
  officialSource: "https://example.com/watch-" + i,
  sources: [{ url: "https://example.com/watch-" + i }],
}));
const sampled = await loadReport("fixture_watch_sample", watches);
assert.equal(sampled.executiveSummary.futureWatchCount, 12);
assert.equal(sampled.executiveSummary.futureWatchTotal, 15);
assert.equal(sampled.executiveSummary.watchSampleCapped, true);
assert.equal(sampled.executiveSummary.clientOpportunityCount, 0);

const failed = await loadReport("fixture_read_fail", [], async () => {
  const err = new Error("airtable unavailable");
  err.code = "canonical_opportunity_read_failed";
  throw err;
});
assert.equal(failed.readFailed, true);
assert.equal(failed.clientOpportunityCount, null);
assert.equal(failed.customerReadyCount, null);

const genuineZero = await loadReport("fixture_genuine_zero", [
  {
    id: "stored-only",
    title: "Stored record",
    organizationName: "Stored Org",
    prospectHotelFacing: false,
    customerFacingState: "ACTIVE",
    priority: "MEDIUM_PRIORITY",
  },
]);
assert.equal(genuineZero.readFailed, false);
assert.equal(genuineZero.clientOpportunityCount, 0);

const failedRow = normalizeGdiAdminReportRow({
  hotelId: "fixture_read_fail",
  available: false,
  readFailed: true,
  reason: "canonical_opportunity_read_failed",
  ready: 0,
  watch: 0,
  clientOpportunityCount: null,
  pdfReady: true,
});
assert.equal(failedRow.clientOpportunityCount, null);
assert.equal(failedRow.readyCount, null);
assert.equal(failedRow.pdfStatus, "READY");

const emptyPdfRow = normalizeGdiAdminReportRow({
  hotelId: "fixture_empty_pdf",
  available: true,
  readFailed: false,
  ready: 0,
  actionSet: 0,
  watch: 12,
  watchTotal: 12,
  clientOpportunityCount: 0,
  pdfReady: true,
  gdiShareAvailable: true,
});
assert.equal(emptyPdfRow.clientOpportunityCount, 0);
assert.equal(emptyPdfRow.readyCount, 0);
assert.equal(emptyPdfRow.pdfReady, true);
assert.equal(emptyPdfRow.gdiShareAvailable, true);

assert.equal(getAdpPropertyIdForHpc("recG66DQJKP2c0UNh"), "adp_renaissance_times_square");
assert.equal(getAdpPropertyIdForHpc("recdmo0CnhIZAwHGC"), "adp_westin_tampa_waterside");
assert.equal(getAdpPropertyIdForHpc("recnPB3Q02NO8Ma1o"), "adp_wyndham_san_diego_bayside");
assert.notEqual(
  getAdpPropertyIdForHpc("recdmo0CnhIZAwHGC"),
  "adp_tampa_airport_marriott"
);

const tableJs = fs.readFileSync(
  path.join(process.cwd(), "public/js/admin-ai-demand-table.js"),
  "utf8"
);
const gdiJs = fs.readFileSync(
  path.join(process.cwd(), "public/js/admin-gdi-reports.js"),
  "utf8"
);
const reviewsJs = fs.readFileSync(
  path.join(process.cwd(), "public/js/admin-ai-demand-reviews.js"),
  "utf8"
);
const ids = {};
function el(id) {
  return {
    id,
    value: "",
    tagName: "DIV",
    type: "",
    style: {},
    disabled: false,
    checked: false,
    dataset: {},
    classList: { add() {}, remove() {}, toggle() {} },
    _listeners: null,
    addEventListener(type, fn) {
      this._listeners = this._listeners || {};
      this._listeners[type] = this._listeners[type] || [];
      this._listeners[type].push(fn);
    },
    removeEventListener() {},
    setAttribute() {},
    getAttribute() {
      return null;
    },
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    },
    closest() {
      return null;
    },
    appendChild() {},
    insertAdjacentHTML() {},
    set innerHTML(v) {
      this._html = v;
    },
    get innerHTML() {
      return this._html || "";
    },
  };
}
const documentStub = {
  readyState: "complete",
  getElementById(id) {
    if (!ids[id]) ids[id] = el(id);
    return ids[id];
  },
  addEventListener() {},
  createElement() {
    return el("created");
  },
  body: el("body"),
};
const context = {
  document: documentStub,
  console,
  setTimeout,
  clearTimeout,
  URL,
  innerWidth: 1280,
  innerHeight: 800,
};
context.window = context;
context.globalThis = context;
context.addEventListener = function () {};
context.removeEventListener = function () {};
const fetched = [];
const opened = [];
const copied = [];
context.fetch = async (url) => {
  fetched.push(String(url));
  const u = String(url);
  if (u.includes("external-client-links")) {
    const hotel = decodeURIComponent(u.split("/hotels/")[1].split("/")[0]);
    return {
      ok: true,
      json: async () => ({
        ok: true,
        gdi: { available: true, url: "https://client.example.test/gdi/" + hotel },
        adp: { available: true, url: "https://client.example.test/adp/" + hotel },
      }),
    };
  }
  if (u.includes("/report-pdf")) {
    return {
      ok: true,
      headers: { get: () => "Dealality_GDI_Report.pdf" },
      blob: async () => ({ type: "application/pdf" }),
      json: async () => ({}),
    };
  }
  return { ok: true, json: async () => ({ ok: true, hotels: [] }) };
};
context.open = (url) => {
  opened.push(url);
};
context.navigator = {
  clipboard: {
    writeText: async (text) => {
      copied.push(text);
    },
  },
};
context.URL.createObjectURL = () => "blob:row-pdf";
vm.createContext(context);
vm.runInContext(tableJs, context);
vm.runInContext(gdiJs, context);
const api = context.DealalityGdiAdminReports;
assert.ok(api && typeof api.buildActions === "function");

const withShare = api.buildActions({
  hotelId: HOTEL,
  available: true,
  readFailed: false,
  readyCount: 0,
  clientOpportunityCount: 44,
  pdfReady: true,
  pdfStatus: "READY",
  gdiShareAvailable: true,
  adpShareAvailable: true,
});
const primaryActs = withShare.primary.map((a) => a.act);
assert.ok(primaryActs.includes("view-pdf"));
assert.ok(primaryActs.includes("open-gdi"));
assert.ok(primaryActs.includes("copy-gdi"));
assert.equal(
  withShare.primary.find((a) => a.act === "open-gdi").label,
  "Open Client Link"
);
assert.equal(
  withShare.primary.find((a) => a.act === "copy-gdi").label,
  "Copy Client Link"
);

const emptyShare = api.buildActions({
  hotelId: "recdmo0CnhIZAwHGC",
  available: true,
  readFailed: false,
  readyCount: 0,
  clientOpportunityCount: 0,
  pdfReady: true,
  pdfStatus: "READY",
  gdiShareAvailable: true,
});
assert.ok(emptyShare.primary.some((a) => a.act === "open-gdi" && !a.disabled));
assert.match(
  emptyShare.primary.find((a) => a.act === "open-gdi").title,
  /No published opportunities/
);

const noShare = api.buildActions({
  hotelId: HOTEL,
  available: true,
  readyCount: 2,
  clientOpportunityCount: 2,
  pdfReady: true,
  pdfStatus: "READY",
  gdiShareAvailable: false,
  gdiShareReason: "GDI share secret not configured in this environment.",
});
assert.ok(!noShare.primary.some((a) => a.act === "open-gdi"));
assert.equal(
  noShare.more.find((a) => a.act === "open-gdi").title,
  "GDI share secret not configured in this environment."
);

assert.match(gdiJs, /encodeURIComponent\(hotelId\)/);
assert.match(gdiJs, /\/report-pdf/);
assert.match(reviewsJs, /resolveCoverageReviewId/);
assert.match(reviewsJs, /Never current-report web-print/);
assert.match(gdiJs, /open-gdi/);
assert.match(gdiJs, /copy-gdi/);
assert.doesNotMatch(gdiJs, /readyCount > 0 && gdiAvail/);

const html = context.DealalityAiAdminTable.actionsHtml(withShare, {
  hotel: HOTEL,
  name: "Renaissance",
});
assert.match(html, /aria-label="Open Client Link"/);
assert.match(html, /aria-label="Copy Client Link"/);
assert.match(html, /aria-label="View PDF"/);
assert.match(html, /data-act="download-pdf"/);

function fire(act, hotelId) {
  const target = {
    disabled: false,
    getAttribute(name) {
      if (name === "data-act") return act;
      if (name === "data-hotel") return hotelId;
      if (name === "data-name") return "Hotel";
      return null;
    },
    closest(sel) {
      if (sel === "button[data-act]" || sel === "[data-hotel]") return this;
      return null;
    },
  };
  const clicks = ids.gdiRptTableBody._listeners.click || [];
  assert.equal(clicks.length, 1);
  clicks[0]({ target });
}

fire("open-gdi", "recG66DQJKP2c0UNh");
fire("copy-gdi", "recnPB3Q02NO8Ma1o");
fire("view-pdf", "recdmo0CnhIZAwHGC");
fire("download-pdf", "recG66DQJKP2c0UNh");
await new Promise((resolve) => setTimeout(resolve, 40));
assert.deepEqual(opened.filter((u) => String(u).startsWith("https://")), [
  "https://client.example.test/gdi/recG66DQJKP2c0UNh",
]);
assert.deepEqual(copied, ["https://client.example.test/gdi/recnPB3Q02NO8Ma1o"]);
assert.ok(
  fetched.some(
    (u) => u.includes("/hotels/recdmo0CnhIZAwHGC/report-pdf?generate=1")
  )
);
assert.ok(
  fetched.some(
    (u) => u.includes("/hotels/recG66DQJKP2c0UNh/report-pdf?download=1")
  )
);
assert.ok(!fetched.some((u) => u.includes("adp_tampa_airport_marriott")));

console.log(
  JSON.stringify({
    ok: true,
    clientList: clientList.length,
    ready: counted.customerReadyCount,
    watchSample: sampled.executiveSummary.futureWatchCount,
    watchTotal: sampled.executiveSummary.futureWatchTotal,
    readFailedNull: failed.clientOpportunityCount,
    genuineZero: genuineZero.clientOpportunityCount,
  })
);
