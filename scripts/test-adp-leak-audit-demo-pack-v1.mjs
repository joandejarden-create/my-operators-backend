#!/usr/bin/env node
/**
 * Phase 1.5+ demo pack gate — plain-language ADP diagnostic + bridge.
 * npm run test:adp-leak-audit-demo-pack-v1
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  loadLeakAuditSampleReport,
  assertClientReportSafety,
  assertNoFullPromptInClientPayload,
  createLeakAuditStore,
  PRIORITY_SOURCES,
} from "../lib/ai-demand-positioning/leak-audit/index.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";

const root = process.cwd();
const requiredDocs = [
  "docs/ai-demand-positioning/adp-leak-audit-demo-pack/README.md",
  "public/adp-leak-audit-sample.html",
  "public/adp-leak-audit-sample-portfolio.html",
  "public/css/adp-leak-audit-sample.css",
  "public/js/adp-leak-audit-sample-report.js",
  "public/js/adp-leak-audit-sample-portfolio.js",
  "fixtures/ai-demand-positioning/leak-audit-sample-report-v1.json",
  "fixtures/ai-demand-positioning/leak-audit-portfolio-sample-v1.json",
];

for (const rel of requiredDocs) {
  assert.ok(existsSync(join(root, rel)), `missing:${rel}`);
}

const sample = loadLeakAuditSampleReport();
assert.equal(sample.sample, true);
assert.equal(sample.layoutMode, "three_page_v1");
assert.equal(sample.prioritySource, PRIORITY_SOURCES.INFERRED_FROM_RESULTS);
assert.ok(sample.demandAreaToReview || sample.inferredDemandLeak);
assert.match(
  (sample.demandAreaToReview || sample.inferredDemandLeak).summary,
  /Meetings & Groups appears to be the primary area to review/i
);
assert.match(
  (sample.demandAreaToReview || sample.inferredDemandLeak).summary,
  /leisure and romantic demand/i
);

const blob = JSON.stringify(sample);
assert.equal(/Inferred Demand Leak/.test(blob), false);
assert.equal(/observed displacement signal/i.test(blob), false);
assert.equal(/Priority Demand Watch/.test(blob), false);
assert.equal(/Competitor Displacement Rank/.test(blob), false);
assert.equal(/stated priority/i.test(blob), false);
assert.equal(/Based on your selected demand priority/i.test(blob), false);
assert.equal(/Because you told us/i.test(blob), false);
assert.equal(/Your strategy is/i.test(blob), false);

assert.ok(sample.executiveSummary?.paragraphs?.length >= 4);
const execBlob = sample.executiveSummary.paragraphs.join(" ");
assert.match(execBlob, /AI Consideration of 71\.3%/i);
assert.match(execBlob, /Scenario Presence of 95\.0%/i);
assert.match(execBlob, /Reality Coverage is 44\.4%/i);
assert.match(execBlob, /7\.3x/i);
assert.match(execBlob, /Rosewood Bermuda appeared in 3/i);
assert.match(execBlob, /The Reefs Resort & Club appeared in 2/i);
assert.match(execBlob, /Management priority/i);
assert.match(execBlob, /does not prove/i);

assert.match(blob, /AI Consideration/);
assert.match(blob, /Demand Area to Review/);
assert.match(blob, /Competitors Showing Up Instead/);
assert.match(blob, /Supporting Evidence/);
assert.match(blob, /Reality Coverage/);
assert.match(sample.howToRead.adpBridgeNote, /The full ADP pilot uses the same reading logic/);
assert.equal(sample.howToRead.mode, "guided_modal");
assert.equal(sample.howToRead.steps?.length, 8);

const signals = Object.fromEntries(
  (sample.executiveSignals || []).map((s) => [s.id, s.value])
);
assert.equal(signals.ai_consideration, "71.3%");
assert.equal(signals.scenario_presence, "95.0%");
assert.equal(signals.reality_coverage, "44.4%");
assert.equal(signals.leisure_advantage, "7.3x");
assert.equal(signals.primary_area_to_review, "Meetings & Groups");
assert.ok(sample.executiveSignals.every((s) => !s.evidenceIds));

const rosewood = sample.competitorDisplacement.find((c) => c.competitorName === "Rosewood Bermuda");
const reefs = sample.competitorDisplacement.find((c) => c.competitorName === "The Reefs Resort & Club");
assert.equal(rosewood.displacementCount, 3);
assert.equal(reefs.displacementCount, 2);

assert.ok(sample.competitorDisplacementRank);
assert.match(sample.competitorDisplacementRank.note, /not a full market ranking/i);
assert.equal(sample.competitorDisplacementRank.rows[0].competitorName, "Rosewood Bermuda");
assert.equal(sample.competitorDisplacementRank.rows[0].displacementCount, 3);
assert.equal(sample.competitorDisplacementRank.rows[1].competitorName, "The Reefs Resort & Club");
assert.equal(sample.competitorDisplacementRank.rows[1].displacementCount, 2);
assert.ok(sample.competitorDisplacementRank.rows[0].whatThisMayMean);
assert.equal(/#1 observed/.test(JSON.stringify(sample.competitorDisplacementRank)), false);

assert.ok(Array.isArray(sample.evidenceLibrary));
assert.ok(sample.evidenceLibrary.length >= 4);
assert.ok(sample.supportingEvidence?.examples?.length >= 3);
const types = new Set(sample.evidenceLibrary.map((e) => e.evidenceType));
assert.ok(types.has("positive_inclusion"));
assert.ok(types.has("competitor_displacement"));
assert.ok(types.has("attribute_gap"));
assert.ok(types.has("source_signal"));

assert.ok(sample.whoDoesTheWork);
assert.equal(sample.fixes.length, 3);

assert.equal(assertClientReportSafety(sample).ok, true);
assert.equal(assertNoFullPromptInClientPayload(sample).ok, true);
assert.equal(/\bthis proves\b/i.test(blob), false);
assert.equal(/\bai is penalizing\b/i.test(blob), false);
assert.equal(/\btop competitor\b/i.test(blob), false);
assert.equal(/\bmarket leader\b/i.test(blob), false);
assert.equal(/\badp_[a-z0-9_]+/i.test(blob), false);
assert.equal(/\brec[a-z0-9]{14}\b/i.test(blob), false);

const tmp = mkdtempSync(join(tmpdir(), "leak-intake-"));
const store = createLeakAuditStore({ root: tmp });
const req = store.createRequest({ hotelName: "Test Hotel Only" });
assert.equal(req.prioritySource, "inferred_from_results");
rmSync(tmp, { recursive: true, force: true });

const html = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
assert.match(html, /ala-adp-report-host|adp-mr-pdf-host/);
assert.match(html, /brand-alignment-snapshot\.css/);
assert.match(html, /dealality-report-system-v1\.css/);
assert.match(html, /bas-cover-page/);
assert.match(html, /alaKpis|ala-kpi-row/);
assert.match(html, /alaEvidenceDrawer/);
assert.match(html, /Competitors Showing Up Instead|alaCompetitors/);
assert.match(html, /Demand Area to Review|alaDemandArea/);
assert.match(html, /Supporting Evidence|alaSupportingEvidence/);
assert.match(html, /Executive Summary/);
assert.equal(/Inferred Demand Leak/.test(html), false);
assert.equal(/Competitor Displacement Rank/.test(html), false);
assert.match(html, /adp-leak-audit-report-v2\.css/);

const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
assert.match(css, /#080f25|#0c142c/);
assert.match(css, /ala-drawer|aiv-drawer|aiv-evidence/);
assert.match(css, /break-after:\s*page|page-break-after:\s*always/);
assert.match(html, /adp-leak-audit-shared-ui\.js/);
assert.match(html, /Recommended Dealality Action Items|alaActionsHeading/);

const js = readFileSync(join(root, "public/js/adp-leak-audit-report.js"), "utf8");
assert.match(js, /openEvidence|data-ala-evidence/);
assert.match(js, /alaEvidenceDrawer/);
assert.match(js, /executiveSummary|AdpLeakAuditSharedUi/);
assert.match(js, /supportingEvidence|alaSupportingEvidence/);
assert.equal(/webEvidenceBtn\(kpi\.evidenceIds/.test(js), false);
assert.ok(Array.isArray(sample.pdfEvidenceRefs) && sample.pdfEvidenceRefs.length >= 3);

console.log(
  JSON.stringify(
    {
      pass: true,
      gate: "ADP_LEAK_AUDIT_DEMO_PACK_V1",
      sampleReportId: sample.reportId,
      layoutMode: sample.layoutMode,
      evidenceCount: sample.evidenceLibrary.length,
      sampleUrl: "/adp-leak-audit/sample",
    },
    null,
    2
  )
);
