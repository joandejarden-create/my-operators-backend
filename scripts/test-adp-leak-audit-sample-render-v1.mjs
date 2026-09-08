#!/usr/bin/env node
/**
 * Smoke gate: sample API payload must include fields required by the browser renderer.
 * Fails if the client would show "Sample unavailable".
 * npm run test:adp-leak-audit-sample-render-v1
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  loadLeakAuditSampleReport,
  loadLeakAuditPortfolioSampleReport,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
const sample = loadLeakAuditSampleReport();
const portfolio = loadLeakAuditPortfolioSampleReport();
const sampleHtml = readFileSync(join(root, "public/adp-leak-audit-report.html"), "utf8");
const sampleJs = readFileSync(join(root, "public/js/adp-leak-audit-report.js"), "utf8");
const portfolioHtml = readFileSync(
  join(root, "public/adp-leak-audit-sample-portfolio.html"),
  "utf8"
);

function assertRenderableSingle(report) {
  const missing = [];
  if (!report.whoDoesTheWork) missing.push("whoDoesTheWork");
  if (!report.executiveSignals?.length) missing.push("executiveSignals");
  if (!report.howToRead) missing.push("howToRead");
  if (!report.inferredDemandLeak) missing.push("inferredDemandLeak");
  if (!report.evidenceLibrary?.length) missing.push("evidenceLibrary");
  if (!report.competitorDisplacementRank?.rows?.length) {
    missing.push("competitorDisplacementRank");
  }
  if (!report.fixes || report.fixes.length < 3) missing.push("fixes");
  if (!report.executiveSummary?.paragraphs?.length) missing.push("executiveSummary");
  if (!report.supportingEvidence?.examples?.length) missing.push("supportingEvidence");
  if (!report.demandAreaToReview && !report.inferredDemandLeak) {
    missing.push("demandAreaToReview");
  }
  assert.equal(
    missing.length,
    0,
    `Sample would show unavailable. Missing: ${missing.join(", ")}`
  );
  assert.equal(report.layoutMode, "three_page_v1");
}

assertRenderableSingle(sample);

assert.ok(portfolio.hotelsNeedingAttention?.length);
assert.ok(portfolio.inferredDemandSegmentsLeaking?.length);
assert.ok(portfolio.competitorsBenefiting?.length);
assert.ok(portfolio.portfolioActions?.length);
assert.ok(portfolio.executiveSignals?.length);
assert.match(portfolio.productFamily || "", /Dealality AI Demand Positioning \(ADP\)/);

assert.match(sampleHtml, /ala-rpt--branded|ala-adp-report-host|adp-mr-pdf-host/);
assert.match(sampleHtml, /bas-cover-page|bas-cover-geometric/);
assert.match(sampleJs, /evidenceLibrary|executiveSummary/);
assert.match(sampleJs, /competitorDisplacementRank|alaRankList/);
assert.match(sampleJs, /Report is missing|Sample report is missing|executive signals/);
assert.equal(/Sample unavailable/.test(sample.bottomLineSummary || ""), false);
assert.match(JSON.stringify(sample), /Book your live ADP walkthrough this week/);
assert.match(JSON.stringify(sample), /If useful, the next step is to schedule/);
assert.match(JSON.stringify(sample), /Dealality should help prepare/);
assert.equal(/Reply now/.test(JSON.stringify(sample)), false);
assert.equal(/Dealality should prepare source/.test(JSON.stringify(sample)), false);
assert.equal(/\u2014/.test(sample.recommendedNextStep || ""), false);
assert.match(JSON.stringify(sample), /do not run a pilot on every request/);
assert.match(JSON.stringify(sample), /Paid ADP pilot spots are limited/);
assert.match(
  sample.competitorDisplacementRank.rows[0].whatThisMayMean || "",
  /retreats|meeting space|luxury group/i
);
assert.equal(/observed displacement signal/i.test(JSON.stringify(sample)), false);

assert.match(portfolioHtml, /ala-rpt--branded|bas-cover-page/);
assert.match(portfolioHtml, /bas-cover-page/);
assert.match(portfolioHtml, /ala-kpi-grid--portfolio|drs-table|alaKpis/);
assert.match(JSON.stringify(portfolio), /Book your live ADP walkthrough this week/);

// Print CSS present to reduce blank trailing pages / card splits
const css = readFileSync(join(root, "public/css/adp-leak-audit-report-v2.css"), "utf8");
assert.match(css, /@media print/);
assert.match(css, /page-break-inside:\s*avoid|break-inside:\s*avoid/);
assert.match(css, /297mm|#080f25/);

console.log(
  JSON.stringify(
    {
      pass: true,
      gate: "ADP_LEAK_AUDIT_SAMPLE_RENDER_V1",
      sampleReportId: sample.reportId,
      hasEvidenceLibrary: sample.evidenceLibrary.length,
      hasRank: sample.competitorDisplacementRank.rows.length,
      portfolioHotels: portfolio.hotelsNeedingAttention.length,
    },
    null,
    2
  )
);
