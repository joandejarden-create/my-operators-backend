#!/usr/bin/env node
/**
 * Competitive Market Scope UI + recovery + completion gate regression.
 * DEALALITY_ADDITIVE_NON_REGRESSION_V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import {
  buildCompetitiveResearchCompletionRecord,
  COMPETITIVE_RESEARCH_EXECUTION_STATE,
} from "../lib/group-demand-intelligence/workflow/gdi-competitive-research-completion-gate-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

// Market Scope visible labels: All / Hotel / Competitive + counts
const ui = read("public/js/group-demand-intelligence/dealality-gdi-ui.js");
assert.match(ui, /shortLabel:\s*"All"/);
assert.match(ui, /shortLabel:\s*"Hotel"/);
assert.match(ui, /shortLabel:\s*"Competitive"/);
assert.match(ui, /gdi-market-scope__btn--competitive/);
assert.match(ui, /o\.shortLabel \+ countText/);
assert.match(ui, /Visible labels: All \/ Hotel \/ Competitive/);

const css = read("public/css/group-demand-intelligence.css");
assert.match(css, /grid-template-columns:\s*minmax\(72px/);
assert.match(css, /gdi-market-scope__btn--competitive/);
assert.match(css, /white-space:\s*nowrap/);
// Opportunity Type remains fixed width
assert.match(css, /\.gdi-page \.gdi-opportunity-type-filter \{[\s\S]*?width:\s*220px/);

// Completion gate distinctions
const notExec = buildCompetitiveResearchCompletionRecord({
  competitiveResearchExecuted: false,
  competitiveOpportunitiesStaged: 0,
});
assert.equal(notExec.executionState, COMPETITIVE_RESEARCH_EXECUTION_STATE.NOT_EXECUTED);
assert.equal(notExec.mustNotReportAsSuccessfulZero, true);
assert.equal(notExec.zeroQualifiedIsAcceptable, false);

const zeroOk = buildCompetitiveResearchCompletionRecord({
  competitiveResearchExecuted: true,
  competitiveCandidatesDiscovered: 0,
  competitiveOpportunitiesQualified: 0,
  competitiveOpportunitiesStaged: 0,
});
assert.equal(
  zeroOk.executionState,
  COMPETITIVE_RESEARCH_EXECUTION_STATE.EXECUTED_ZERO_QUALIFIED,
);
assert.equal(zeroOk.zeroQualifiedIsAcceptable, true);

const candidatesOnly = buildCompetitiveResearchCompletionRecord({
  competitiveResearchExecuted: true,
  competitiveCandidatesDiscovered: 15,
  competitiveOpportunitiesQualified: 0,
  competitiveOpportunitiesStaged: 0,
});
assert.equal(
  candidatesOnly.executionState,
  COMPETITIVE_RESEARCH_EXECUTION_STATE.CANDIDATES_DISCOVERED_NOT_QUALIFIED,
);

// Wyndham STAGED competitive recovery
const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer("recnPB3Q02NO8Ma1o", "STAGED");
assert.ok(ptr?.publicationVersion);
const loaded = store.loadPackage("recnPB3Q02NO8Ma1o", ptr.publicationVersion);
assert.equal(loaded.ok, true);
assert.equal(loaded.package.opportunities.length, 26);
const competitive = loaded.package.opportunities.filter(
  (o) => o.competitiveMarketScopeEligible === true,
);
assert.ok(
  competitive.length >= 3,
  `expected >=3 recovered competitive, got ${competitive.length}`,
);
const ids = new Set(competitive.map((o) => o.opportunityId || o.id));
assert.ok(ids.has("gdi_opp_prospect_mv8p5ddh_7edb8e7c"));
assert.ok(ids.has("gdi_opp_prospect_mv8p5ddj_cf54ecae"));
assert.ok(ids.has("gdi_wyndham_live_associatio_a790d46ad631"));

const counts = loaded.package.inventoryCounts || loaded.package.meta?.inventoryCounts;
if (counts) {
  assert.equal(counts.all, 26);
  assert.ok(counts.competitive >= 3);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      wyndhamStaged: ptr.publicationVersion,
      competitiveRecovered: competitive.length,
      competitiveIds: [...ids],
      labels: ["All", "Hotel", "Competitive"],
    },
    null,
    2,
  ),
);
