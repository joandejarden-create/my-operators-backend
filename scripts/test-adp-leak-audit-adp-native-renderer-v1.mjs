#!/usr/bin/env node
/**
 * Sample + live share ADP-native report shell / renderer.
 * npm run test:adp-leak-audit-adp-native-renderer-v1
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const root = process.cwd();
const reportHtml = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const sampleHtml = readFileSync(join(root, "public/adp-leak-audit-sample.html"), "utf8");
const reportJs = readFileSync(join(root, "public/js/adp-leak-audit-report.js"), "utf8");
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
const server = readFileSync(join(root, "server.js"), "utf8");

// Unified shell for sample + live routes
assert.match(server, /\/adp-leak-audit\/sample[\s\S]*adp-leak-audit-report\.html/);
assert.match(server, /\/adp-leak-audit\/:reportId[\s\S]*adp-leak-audit-report\.html/);
assert.match(sampleHtml, /\/adp-leak-audit\/sample|adp-leak-audit-report/);

assert.match(reportHtml, /brand-alignment-snapshot\.css/);
assert.match(reportHtml, /dealality-report-system-v1\.css/);
assert.match(reportHtml, /adp-monthly-review-report-v1\.css/);
assert.match(reportHtml, /adp-leak-audit-report-v2\.css/);
assert.match(reportHtml, /adp-guided-report-tour/);
assert.match(reportHtml, /adp-leak-audit-report\.js/);
assert.match(reportHtml, /Dealality AI Demand Positioning/);
assert.match(reportHtml, /ala-adp-report-page/);
assert.match(reportHtml, /data-ala-layout="three_page_v1"/);
assert.match(reportHtml, /data-ala-page="cover"/);
assert.match(reportHtml, /data-ala-page="executive"/);
assert.match(reportHtml, /data-ala-page="actions"/);
assert.match(reportHtml, /ala-adp-report-host/);
assert.match(reportHtml, /ai-visibility-shared\.css/);
assert.match(reportHtml, /alaScenariosMonitored|Scenarios Monitored/);
const sharedUi = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");
assert.match(sharedUi, /Dealality can help prepare/);
assert.equal(/\bDealality prepares\b/.test(reportHtml + sharedUi), false);

// Navy ADP surface, not white paper body as primary
assert.match(css, /#080f25|#0c142c|#101935/);
assert.match(css, /\.ala-adp-report-page[\s\S]*background:\s*(var\(--neutral--800|#080f25|linear-gradient)/);
assert.equal(/background:\s*#fff;\s*\n\s*color:\s*var\(--drs-ink/.test(css), false);

// Shared renderer detects sample vs live
assert.match(reportJs, /isSamplePath|sample-report/);
assert.match(reportJs, /\/api\/adp-leak-audit\/sample-report/);
assert.match(reportJs, /\/api\/adp-leak-audit\/report\//);
assert.match(reportJs, /executiveSummary/);
assert.match(reportJs, /renderEvidenceBody|AdpLeakAuditSharedUi/);
assert.match(reportJs, /AdpLeakAuditHowToReadGuide/);

// Web guide present; PDF suppresses it
assert.match(reportHtml, /How to [Rr]ead [Tt]his [Rr]eport/);
assert.match(css, /ala-adp-web-chrome[\s\S]*display:\s*none|ala-no-print[\s\S]*display:\s*none/);

// Evidence: web drawer ADP classes; PDF refs not buttons
assert.match(reportHtml, /alaEvidenceDrawer/);
assert.match(reportHtml, /ala-adp-evidence|aiv-evidence|adp-leak-audit-shared-ui/);
assert.match(reportJs, /supportingEvidence|alaSupportingEvidence/);
assert.match(reportHtml, /Supporting Evidence/);
assert.equal(/webEvidenceBtn\(kpi\.evidenceIds/.test(reportJs), false);
assert.match(css, /ala-evidence-links[\s\S]*display:\s*none|button\.aiv-btn-text\.aiv-link[\s\S]*display:\s*none|ala-web-ev[\s\S]*display:\s*none/);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_ADP_NATIVE_RENDERER_V1",
    shell: "public/adp-leak-audit-report.html",
    css: "public/css/adp-leak-audit-report-v2.css",
    routes: ["sample", ":reportId"],
  })
);
