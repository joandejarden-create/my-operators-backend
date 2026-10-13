#!/usr/bin/env node
/**
 * Template lock gate for AI Demand Leak Audit 3-page PDF.
 * npm run test:adp-leak-audit-template-lock-v1
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { loadLeakAuditSampleReport } from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const sample = loadLeakAuditSampleReport();
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const reportJs = readFileSync(join(root, "public/js/adp-leak-audit-report.js"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const shared = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
const lockDoc = join(
  root,
  "docs/ai-demand-positioning/adp-leak-audit-demo-pack/TEMPLATE_LOCK_V1.md"
);

assert.ok(existsSync(lockDoc), "TEMPLATE_LOCK_V1.md missing");
assert.match(readFileSync(lockDoc, "utf8"), /LOCKED for Phase 2A/i);
assert.equal(sample.layoutMode, "three_page_v1");
assert.equal((html.match(/data-ala-page="/g) || []).length, 3);
assert.match(html, /bas-cover-page|hid-cover-page/);
assert.match(html, /Executive Summary/);
assert.match(html, /Priority AI Demand Improvements/);
assert.match(html, /Book Your ADP Walkthrough/);

assert.match(reportJs, /setCoverTitle|ala-cover-title-word/);
assert.match(css, /ala-cover-title-space|letter-spacing:\s*0\s*!important/);

const execBlob = JSON.stringify(sample.executiveSummary || {});
assert.match(execBlob, /Dealality should help prepare/);
assert.equal(/Dealality should prepare source/.test(execBlob), false);
assert.match(sample.recommendedNextStep || "", /If useful, the next step is to schedule/);
assert.equal(/Reply now/.test(sample.recommendedNextStep || ""), false);

assert.match(html, /adpHowToReadReport/);
assert.match(shared, /infoIconHtml|aiv-col-info/);
assert.match(shared, /View example/);
assert.equal(/data-ala-evidence/.test(shared) && /aiv-kpi/.test(shared), true);

const blob = JSON.stringify(sample);
assert.equal(/fullPromptText|"promptText"/.test(blob), false);
assert.equal(/\brec[A-Za-z0-9]{14}\b/.test(blob), false);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_TEMPLATE_LOCK_V1",
    pages: 3,
    helpPrepare: true,
    noReplyNow: true,
    coverTitleSpaces: true,
  })
);
