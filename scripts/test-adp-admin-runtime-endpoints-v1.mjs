/**
 * test:adp-admin-runtime-endpoints-v1
 * Hits handler functions (same contracts as HTTP admin APIs).
 */
import assert from "node:assert/strict";
import { getAdminMonthlyReviews } from "../api/admin-adp-monthly-reviews.js";
import { getAdminActionPlanCatalog } from "../api/admin-adp-monthly-reviews.js";
import { getAdminAvaHotels } from "../api/admin-ai-visibility-audit.js";
import { getAdminReportArchiveCatalog } from "../api/admin-report-archive.js";

const ID = "adp_radisson_blu_toronto_downtown";

function capture(handler, req = {}) {
  return new Promise((resolve) => {
    const chunks = [];
    const res = {
      statusCode: 200,
      setHeader() {},
      status(code) { this.statusCode = code; return this; },
      json(body) { resolve({ status: this.statusCode, body }); },
      end(s) { chunks.push(String(s || "")); try { resolve({ status: this.statusCode, body: JSON.parse(chunks.join("") || "{}") }); } catch { resolve({ status: this.statusCode, body: chunks.join("") }); } },
      write(s) { chunks.push(String(s)); },
      send(s) { this.end(s); },
    };
    const out = handler(req, res);
    if (out && typeof out.then === "function") {
      out.then(() => {
        if (chunks.length) {
          try { resolve({ status: res.statusCode, body: JSON.parse(chunks.join("") || "{}") }); }
          catch { resolve({ status: res.statusCode, body: chunks.join("") }); }
        }
      }).catch((e) => resolve({ status: 500, body: { error: String(e) } }));
    }
  });
}

const reviews = await capture(getAdminMonthlyReviews, { query: {} });
assert.equal(reviews.body?.ok, true);
const reviewRows =
  reviews.body.properties || reviews.body.reviews || reviews.body.items || [];
const torReview = reviewRows.find((r) => r.propertyId === ID);
assert.ok(torReview, "Toronto in monthly-reviews coverage");
assert.notEqual(torReview.coverageStatus, "BLOCKED");
assert.ok(!(torReview.blockers || []).includes("published_manifest_missing"));
assert.equal(torReview.hasPdf, true);

const catalog = await capture(getAdminActionPlanCatalog, { query: {} });
assert.equal(catalog.body?.ok, true);
const torCat = (catalog.body.properties || []).find((p) => p.propertyId === ID);
assert.ok(torCat);
assert.equal(torCat.error, null);
assert.equal(torCat.ok, true);

const ava = await capture(getAdminAvaHotels, {});
const torAva = (ava.body.hotels || []).find((h) => h.propertyId === ID);
assert.ok(torAva);
assert.equal(torAva.auditStatus, "FOUNDER_REVIEWED_STAGED");

const archive = await capture(getAdminReportArchiveCatalog, { query: {} });
assert.equal(archive.body?.ok ?? archive.body?.success ?? true, true);
const entries =
  archive.body.entries ||
  archive.body.items ||
  archive.body.rows ||
  archive.body.archives ||
  [];
const torArch = entries.filter((e) => e.propertyId === ID);
assert.ok(torArch.length >= 1, "Toronto archive entries");

console.log("PASS test:adp-admin-runtime-endpoints-v1");
