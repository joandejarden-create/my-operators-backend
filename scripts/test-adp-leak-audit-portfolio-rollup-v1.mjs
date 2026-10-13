#!/usr/bin/env node
/**
 * Portfolio roll-up sample gate.
 * npm run test:adp-leak-audit-portfolio-rollup-v1
 */
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  loadLeakAuditPortfolioSampleReport,
  assertClientReportSafety,
  assertNoFullPromptInClientPayload,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const root = process.cwd();
assert.ok(existsSync(join(root, "public/adp-leak-audit-sample-portfolio.html")));
assert.ok(existsSync(join(root, "fixtures/ai-demand-positioning/leak-audit-portfolio-sample-v1.json")));
assert.ok(existsSync(join(root, "public/js/adp-leak-audit-sample-portfolio.js")));

const sample = loadLeakAuditPortfolioSampleReport();
assert.equal(sample.sample, true);
assert.equal(sample.reportMode, "portfolio_rollup");
assert.equal(sample.prioritySource, "inferred_from_results");
assert.ok(sample.portfolioAuditContext);
assert.match(sample.portfolioAuditContext.body, /inferred demand leaks/i);
assert.match(sample.portfolioAuditContext.body, /does not assume/i);
assert.ok(sample.executiveSignals?.length >= 5);
assert.ok(sample.hotelsNeedingAttention?.length >= 3);
assert.ok(sample.inferredDemandSegmentsLeaking?.length >= 3);
assert.ok(sample.competitorsBenefiting?.length >= 1);
assert.ok(sample.portfolioActions?.length >= 3);
assert.ok(sample.howToUseThis);
assert.match(sample.howToUseThis.body, /triage tool/i);
assert.match(sample.recommendedNextStep, /paid ADP pilot/i);

const blob = JSON.stringify(sample);
assert.equal(/stated priority/i.test(blob), false);
assert.equal(/\bthis proves\b/i.test(blob), false);
assert.equal(/\badp_[a-z0-9_]+/i.test(blob), false);
assert.equal(/\brec[a-z0-9]{14}\b/i.test(blob), false);
assert.equal(/fullPromptText/i.test(blob), false);
assert.equal(assertClientReportSafety(sample).ok, true, JSON.stringify(assertClientReportSafety(sample).failures));
assert.equal(assertNoFullPromptInClientPayload(sample).ok, true);

const html = readFileSync(join(root, "public/adp-leak-audit-sample-portfolio.html"), "utf8");
assert.match(html, /Hotels Needing Attention/);
assert.match(html, /Inferred Demand Segments Leaking Most Often/);
assert.match(html, /Competitors Benefiting Most Often/);
assert.match(html, /Priority AI Demand Improvements|First 3 Portfolio Actions/);

const server = readFileSync(join(root, "server.js"), "utf8");
assert.match(server, /\/adp-leak-audit\/sample-portfolio/);
assert.match(server, /sample-portfolio-report/);
assert.match(
  server,
  /\/adp-leak-audit\/portfolio\/:portfolioGroupId|\/adp-leak-audit\/:reportId/
);

console.log(
  JSON.stringify(
    {
      pass: true,
      gate: "ADP_LEAK_AUDIT_PORTFOLIO_ROLLUP_V1",
      reportId: sample.reportId,
      hotels: sample.hotelsNeedingAttention.length,
      segments: sample.inferredDemandSegmentsLeaking.length,
      competitors: sample.competitorsBenefiting.length,
      actions: sample.portfolioActions.length,
      url: "/adp-leak-audit/sample-portfolio",
    },
    null,
    2
  )
);
