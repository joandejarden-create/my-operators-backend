#!/usr/bin/env node
/**
 * Gate: AI Admin shared table headers align with coverage body columns,
 * sortable headers expose data-sort + SVG indicators, nonsortable columns stay inert.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const html = readFileSync(
  join(root, "public/app/admin/ai-demand-admin.html"),
  "utf8"
);
const tableJs = readFileSync(
  join(root, "public/js/admin-ai-demand-table.js"),
  "utf8"
);
const reviewsJs = readFileSync(
  join(root, "public/js/admin-ai-demand-reviews.js"),
  "utf8"
);

assert.match(tableJs, /paintSortState/);
assert.match(tableJs, /ada-sort-svg/);
assert.match(tableJs, /ada-sort-chevron--up/);
assert.match(reviewsJs, /paintAdrSortHeaders|paintSortState/);
assert.match(reviewsJs, /compactCoverageActions/);
assert.match(reviewsJs, /view-pdf/);
assert.match(reviewsJs, /download-pdf/);
assert.match(reviewsJs, /copy-adp-client/);

function theadBlock(id) {
  const re = new RegExp(
    `id="${id}"[\\s\\S]*?<tr>([\\s\\S]*?)</tr>[\\s\\S]*?</thead>`,
    "i"
  );
  const m = html.match(re);
  assert.ok(m, `missing thead ${id}`);
  return m[1];
}

const adr = theadBlock("adrTableHead");
const expectedSort = [
  "propertyName",
  "market",
  "reportingMonth",
  "currentMonitoringDate",
  "priorRunDate",
  "versionLabel",
  "reviewStatus",
  "generationStatus",
  "coverageStatus",
  "clientIssued",
  "hasPdf",
  "actionCount",
  "generatedAt",
];
for (const key of expectedSort) {
  assert.match(adr, new RegExp(`data-sort="${key}"`));
}
assert.match(adr, /ada-col-actions no-sort/);
assert.match(adr, />\s*Actions\s*</);
assert.ok(!/publishedMonthYear|newDraftLabel|customerAccessLabel|pdfLabel/.test(adr));

// Header count ≈ body coverage columns (checkbox + 13 data + actions = 15)
const thCount = (adr.match(/<th\b/g) || []).length;
assert.equal(thCount, 15, `expected 15 Reviews header cells, got ${thCount}`);

// Coverage body still renders Location / Research Date peers
assert.match(reviewsJs, /ada-cell-loc/);
assert.match(reviewsJs, /reportingMonth/);
assert.match(reviewsJs, /currentMonitoringDate/);
assert.match(reviewsJs, /coverageStatus/);
assert.match(reviewsJs, /colspan="15"/);

const ava = html.match(/id="avaTable"[\s\S]*?<thead>([\s\S]*?)<\/thead>/i);
assert.ok(ava);
assert.match(ava[1], /data-sort="name"/);
assert.match(ava[1], /data-sort="auditStatus"/);
assert.match(ava[1], /ada-col-actions no-sort/);

const gdi = theadBlock("gdiRptTableHead");
assert.match(gdi, /data-sort="displayName"/);
assert.match(gdi, /ada-col-actions no-sort/);

const rar = theadBlock("rarTableHead");
assert.match(rar, /data-sort="archivedAt"/);
assert.match(rar, /no-sort"><span class="ada-th-label">Snapshot/);

const gra = theadBlock("graTableHead");
assert.match(gra, /data-sort="startedAt"/);

const aap = theadBlock("aapTableHead");
assert.match(aap, /data-sort="priority"/);
assert.match(aap, /no-sort"><span class="ada-th-label">Observed Issue/);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-admin-table-headers-sort-v1",
      reviewsHeaderCells: thCount,
      sortableReviewsKeys: expectedSort.length,
      sharedPaint: true,
      svgSortIndicator: true,
    },
    null,
    2
  )
);
