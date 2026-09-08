#!/usr/bin/env node
/**
 * Evidence links + client-safe drawer gate.
 * Evidence belongs in Supporting Evidence / competitor example area — not KPI cards.
 * npm run test:adp-leak-audit-evidence-links-v1
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  loadLeakAuditSampleReport,
  assertClientReportSafety,
  assertNoFullPromptInClientPayload,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const sample = loadLeakAuditSampleReport();
const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const js = readFileSync(join(root, "public/js/adp-leak-audit-report.js"), "utf8");
const shared = readFileSync(join(root, "public/js/adp-leak-audit-shared-ui.js"), "utf8");

assert.ok(Array.isArray(sample.evidenceLibrary));
assert.ok(sample.evidenceLibrary.length >= 4);

const byType = Object.fromEntries(
  sample.evidenceLibrary.map((e) => [e.evidenceType, e])
);
assert.ok(byType.positive_inclusion);
assert.ok(byType.competitor_displacement);
assert.ok(byType.attribute_gap);
assert.ok(byType.source_signal);

assert.match(byType.positive_inclusion.excerpt, /cottage-colony|romantic/i);
assert.match(byType.competitor_displacement.excerpt, /Rosewood Bermuda/);
assert.match(byType.attribute_gap.excerpt, /7\.0%/);
assert.match(byType.source_signal.excerpt, /84\.4%/);

for (const ev of sample.evidenceLibrary) {
  assert.ok(ev.id);
  assert.ok(ev.evidenceTypeLabel);
  assert.ok(ev.demandSegment);
  assert.ok(ev.provider);
  assert.ok(ev.excerpt);
  assert.ok(ev.whyThisMatters);
  assert.ok(ev.managementReview);
  assert.equal(Object.prototype.hasOwnProperty.call(ev, "fullPromptText"), false);
  assert.equal(Object.prototype.hasOwnProperty.call(ev, "promptId"), false);
  assert.equal(Object.prototype.hasOwnProperty.call(ev, "propertyId"), false);
  assert.equal(Object.prototype.hasOwnProperty.call(ev, "airtableId"), false);
}

assert.ok(sample.executiveSignals.every((s) => !s.evidenceIds && !s.evidenceLinkLabel));
assert.ok(sample.supportingEvidence?.examples?.length >= 3);
assert.match(
  sample.competitorDisplacementRank.evidenceLinkLabel || "",
  /View supporting examples/i
);
assert.ok(sample.competitorDisplacementRank.evidenceIds?.length);

assert.match(html, /alaEvidenceDrawer/);
assert.match(html, /Supporting Evidence|alaSupportingEvidence/);
assert.match(html, /<dialog[^>]*alaEvidenceDrawer|role="dialog"/);
assert.match(html, /does not prove causation/i);
assert.match(html, /ala-adp-evidence|aiv-evidence/);
assert.match(js, /function openEvidence/);
assert.match(js, /data-ala-evidence/);
assert.match(js, /alaSupportingEvidence|supportingEvidence/);
assert.match(js, /View supporting examples|View example/);
assert.equal(/webEvidenceBtn\(kpi\.evidenceIds/.test(js), false);
assert.match(js, /fullPromptText/);
assert.match(js, /promptId/);
assert.match(js, /renderEvidenceBody|AdpLeakAuditSharedUi/);
assert.match(shared, /aiv-evidence/);
assert.match(shared, /ala-adp-evidence/);
assert.match(shared, /View example/);
assert.match(shared, /ids\[0\]/);
assert.equal(/evidenceLinksHtml\(kpi\.evidenceIds/.test(shared), false);
assert.equal(/Hotel\/operator or agency publishes/.test(shared), false);
assert.match(shared, /Hotel \/ Agency publishes/);
assert.match(shared, /Dealality can help prepare/);
assert.match(shared, /renderScenariosMonitored/);
assert.match(html, /alaScenariosMonitored/);
assert.match(js, /renderScenariosMonitored/);

const libBlob = JSON.stringify(sample.evidenceLibrary);
assert.equal(/fullPromptText/i.test(libBlob), false);
assert.equal(/promptId/i.test(libBlob), false);
assert.equal(/\brec[a-z0-9]{14}\b/i.test(libBlob), false);
assert.equal(/\badp_[a-z0-9_]+/i.test(libBlob), false);
assert.equal(/\bthis proves\b/i.test(libBlob), false);

assert.equal(assertClientReportSafety(sample).ok, true);
assert.equal(assertNoFullPromptInClientPayload(sample).ok, true);

console.log(
  JSON.stringify(
    {
      pass: true,
      gate: "ADP_LEAK_AUDIT_EVIDENCE_LINKS_V1",
      evidenceCount: sample.evidenceLibrary.length,
      supportingExamples: sample.supportingEvidence.examples.length,
      types: Object.keys(byType),
    },
    null,
    2
  )
);
