#!/usr/bin/env node
/**
 * Leak Audit Who Does the Work / partnership process polish gate.
 * npm run test:adp-leak-audit-who-does-work-v1
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const shared = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const sample = loadLeakAuditSampleReport();

assert.match(shared, /WHO_SECTION_TITLE/);
assert.match(shared, /How We Partner on These Improvements/);
assert.match(shared, /YOUR TEAM/);
assert.match(shared, /PUBLISHES/);
assert.match(shared, /DEALALITY/);
assert.match(shared, /PREPARES/);
assert.match(shared, /YOU/);
assert.match(shared, /CONFIRM/);
assert.match(shared, /FOLLOWS THROUGH/);
assert.match(shared, /ala-who__step/);
assert.match(shared, /Dealality can help prepare/);
assert.equal(/DEALALITY_PREPARES_LABEL\s*=\s*"Dealality prepares"/.test(shared), false);

assert.match(shared, /Your team or agency publishes on Website, OTAs, GBP, and TripAdvisor/);
assert.match(shared, /We assemble audits, draft copy, and checklists/);
assert.match(shared, /You approve facts, claims, and final wording/);
assert.match(shared, /We re-check signals, competitors/);

const who = sample.whoDoesTheWork;
assert.equal(
  who.hotelOrAgencyPublishesShort,
  "Your team or agency publishes on Website, OTAs, GBP, and TripAdvisor"
);
assert.ok(!/\n/.test(who.hotelOrAgencyPublishesShort || ""));
assert.ok((who.hotelOrAgencyPublishesShort || "").length <= 72);
assert.ok(!/Google Business Profile/.test(who.hotelOrAgencyPublishesShort));
assert.match(who.title || "", /Partner/i);

assert.match(html, /alaWhoHeading/);
assert.match(html, /How We Partner on These Improvements/);
assert.match(css, /ala-who__step/);
assert.match(css, /ala-who__card--publish[\s\S]{0,200}text-overflow:\s*unset/);
assert.match(css, /white-space:\s*pre-line\s*!important/);
assert.match(
  css,
  /\.ala-adp-report-page \.aiv-theme-group \.aiv-kpi \.aiv-value[\s\S]{0,120}margin-top:\s*26px/
);
assert.equal(
  /\.ala-who__card--publish h3\s*\{[^}]*text-overflow:\s*ellipsis/.test(css),
  false,
  "publish title must not ellipsize"
);

assert.match(shared, /ala-adp-action__why/);
assert.match(css, /ala-adp-action__why[\s\S]{0,80}#fdb52a/);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_WHO_DOES_WORK_V1",
    partnershipProcess: true,
    gbpBody: true,
    kpiValueSpacing: true,
    whyItMattersGold: true,
  })
);
