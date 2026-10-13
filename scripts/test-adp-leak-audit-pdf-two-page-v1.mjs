#!/usr/bin/env node
/**
 * Deprecated: 2-page PDF gate replaced by 3-page.
 * npm run test:adp-leak-audit-pdf-two-page-v1
 * Prefer: npm run test:adp-leak-audit-pdf-three-page-v1
 */
import assert from "node:assert/strict";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const sample = loadLeakAuditSampleReport();
const html = readFileSync(
  join(process.cwd(), "public/adp-leak-audit-report.html"),
  "utf8"
);

assert.equal(sample.layoutMode, "three_page_v1");
assert.match(html, /data-ala-layout="three_page_v1"/);
assert.match(html, /data-ala-page="cover"/);
assert.match(html, /data-ala-page="executive"/);
assert.match(html, /data-ala-page="actions"/);
assert.equal(/data-ala-page="report"/.test(html), false);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_PDF_TWO_PAGE_V1",
    deprecated: true,
    successor: "test:adp-leak-audit-pdf-three-page-v1",
    layoutMode: sample.layoutMode,
  })
);
