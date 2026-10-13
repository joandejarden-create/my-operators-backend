#!/usr/bin/env node
/**
 * PDF contrast / readable text gate for Leak Audit navy pages.
 * npm run test:adp-leak-audit-pdf-contrast-v1
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const shared = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const sample = loadLeakAuditSampleReport();

const printBlock = css.slice(css.lastIndexOf("@media print"));
assert.ok(printBlock.length > 200, "print block missing");

// Cover fills full page — no white sheet gap
assert.match(printBlock, /hid-cover-page\.bas-cover-page[\s\S]{0,400}height:\s*297mm\s*!important/);
assert.match(printBlock, /bas-cover-geometric[\s\S]{0,120}height:\s*100%\s*!important/);
assert.match(printBlock, /hid-cover-page__foot[\s\S]{0,120}display:\s*flex\s*!important/);

// Body pages navy with forced light ink
assert.match(printBlock, /\.ala-adp-report-page[\s\S]{0,300}background:\s*#080f25\s*!important/);
assert.match(printBlock, /color:\s*#d1dbf9\s*!important/);
assert.match(printBlock, /color:\s*#ffffff\s*!important/);
assert.match(printBlock, /opacity:\s*1\s*!important/);

// Must not force white body behind navy pages in print
assert.equal(
  /body\.ala-adp-host\s*\{\s*background:\s*#fff\s*!important/.test(printBlock),
  false,
  "print body must not be white behind navy report pages"
);

// Screen overrides kill DRS dark ink on navy cards
assert.match(css, /Force readable ink on navy/);
assert.match(css, /PDF-safe contrast/);
assert.ok(css.includes("color: #d1dbf9 !important"));
assert.ok(css.includes("color: #ffffff !important"));
assert.ok(css.includes(".drs-action__field p"));
assert.ok(css.includes(".drs-action__title"));

// Labels
assert.match(shared, /Dealality can help prepare/);
assert.equal(/DEALALITY_PREPARES_LABEL\s*=\s*"Dealality prepares"/.test(shared), false);
assert.match(shared, /Hotel \/ Agency publishes/);
assert.equal(/Hotel\/operator or agency publishes/.test(shared), false);

assert.match(html, /Need help reading this\? Use the How to Read guide in the web report/);
assert.match(html, /alaScenariosMonitored/);

const who = sample.whoDoesTheWork;
assert.deepEqual(who.hotelOrAgencyPublishes, [
  "Website",
  "OTAs",
  "Google Business Profile",
  "TripAdvisor",
]);
assert.match(who.hotelOrAgencyPublishesShort || "", /GBP/);
assert.match(JSON.stringify(sample.scenariosMonitored), /15/);
assert.match(sample.scenariosMonitored.value || "", /15\s*×\s*4/);
assert.equal(
  /4 Providers/.test(sample.scenariosMonitored.value || ""),
  false,
  "value should be compact 15 × 4; observations is sub-label"
);
assert.equal(sample.scenariosMonitored.valueSubLabel, "60 observations");

const blob = JSON.stringify(sample) + shared + html;
assert.equal(/\bDealality prepares\b/.test(blob.replace(/Dealality can help prepare/g, "X")), false);
assert.equal(/Hotel\/operator or agency publishes/.test(blob), false);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_PDF_CONTRAST_V1",
    coverFullPage: true,
    lightInkForced: true,
    scenariosMonitored: true,
  })
);
