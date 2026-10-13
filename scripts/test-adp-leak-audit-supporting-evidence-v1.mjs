#!/usr/bin/env node
/**
 * Leak Audit supporting evidence polish gate.
 * npm run test:adp-leak-audit-supporting-evidence-v1
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

const examples = sample.supportingEvidence?.examples || [];
assert.equal(examples.length, 3);
assert.ok(examples.some((e) => /positive signal/i.test(e.label)));
assert.ok(examples.some((e) => /competitor example/i.test(e.label)));
assert.ok(examples.some((e) => /source \/ attribute/i.test(e.label)));

assert.match(html, /alaSupportingEvidence/);
assert.match(html, /alaScenariosMonitored/);
assert.match(shared, /POSITIVE/);
assert.match(shared, /SIGNAL/);
assert.match(shared, /SCENARIOS/);
assert.match(shared, /MONITORED/);
assert.match(shared, /ala-scenarios-monitored__sub/);
assert.match(shared, /valueSubLabel|60 observations/);

assert.match(shared, /ids\[0\]/);
assert.match(shared, /One View example control per evidence card/);
assert.match(shared, /ala-evidence-links ala-web-ev ala-no-print/);

assert.match(css, /grid-template-columns:\s*repeat\(4/);
assert.match(css, /display:\s*contents/);
assert.match(css, /\.ala-evidence-links[\s\S]{0,80}margin-top:\s*auto/);
assert.match(css, /ala-scenarios-monitored__value[\s\S]{0,80}font-size:\s*18px/);

const print = css.slice(css.lastIndexOf("@media print"));
assert.match(print, /\.ala-evidence-links/);
assert.match(print, /display:\s*none\s*!important/);

const scenarios = sample.scenariosMonitored || {};
assert.equal(scenarios.value, "15 × 4");
assert.equal(scenarios.valueSubLabel, "60 observations");
assert.match(scenarios.description || "", /15 traveler scenarios across/);

console.log(
  JSON.stringify({
    pass: true,
    gate: "ADP_LEAK_AUDIT_SUPPORTING_EVIDENCE_V1",
    evidenceCards: examples.length,
    scenariosValue: scenarios.value,
    fourColRow: true,
    oneViewExample: true,
  })
);
