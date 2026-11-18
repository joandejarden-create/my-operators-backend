#!/usr/bin/env node
/**
 * Deep-research selection register tests (deterministic, no paid).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  scoreSelectionDimensions,
  selectCandidatesForDeepResearch,
  SELECTION_DECISION,
  SELECTION_DIMENSION_WEIGHTS,
} from "../lib/group-demand-intelligence/research/gdi-deep-research-selection-register-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

function cand(partial) {
  return {
    seedUrls: ["https://example.com/x"],
    hotelOpportunityThesis:
      "Corporate training and project lodging near Bethesda with guestroom and meeting demand",
    ...partial,
  };
}

check("weights_sum_to_one", () => {
  const sum = Object.values(SELECTION_DIMENSION_WEIGHTS).reduce((a, b) => a + b, 0);
  assert.ok(Math.abs(sum - 1) < 1e-9);
});

check("transparent_dimensions_present", () => {
  const dim = scoreSelectionDimensions(
    cand({
      id: "1",
      organizationName: "Amentum",
      researchSegment: "corporate_accounts_scaling",
      city: "Bethesda",
    }),
    { hotelProfile: { city: "Bethesda" } },
  );
  assert.ok(dim.combinedScore > 0);
  assert.ok(dim.dimensions.hotelSpecificFit > 0.3);
  assert.equal(Object.keys(dim.weights).length, 7);
});

check("selection_prefers_hotel_fit_over_out_of_market", () => {
  const pool = [
    cand({
      id: "weak",
      organizationName: "Dallas Expo Org",
      city: "Dallas",
      researchSegment: "associations_conferences_events",
      geoFit: { geoFitClass: "OUT_OF_MARKET" },
      hotelOpportunityThesis: "National expo in Dallas",
      preDeepScreen: { outcome: "GLOBAL_INTELLIGENCE_ONLY", eligibleForDeep: true },
    }),
    cand({
      id: "strong",
      organizationName: "Amentum Holdings",
      city: "Bethesda",
      researchSegment: "corporate_accounts_scaling",
      hotelOpportunityThesis:
        "Defense services expansion in Bethesda/Rockville with training cohorts needing guestrooms",
      preDeepScreen: { outcome: "ELIGIBLE_FOR_DEEP_RESEARCH", eligibleForDeep: true },
    }),
  ];
  // Even if both somehow eligible, strong should rank higher
  const sel = selectCandidatesForDeepResearch(pool, {
    maxCandidates: 1,
    fairMinPerSegment: 0,
    hotelProfile: { city: "Bethesda" },
  });
  assert.equal(sel.selectedCount, 1);
  assert.equal(sel.selected[0].id, "strong");
  assert.equal(
    sel.register.find((r) => r.candidateId === "strong").selectionDecision,
    SELECTION_DECISION.SELECTED_FOR_DEEP,
  );
});

check("fair_segment_floor_then_commercial_fill", () => {
  const pool = [];
  for (let i = 0; i < 3; i++) {
    pool.push(
      cand({
        id: `corp_${i}`,
        organizationName: `Corp ${i}`,
        researchSegment: "corporate_accounts_scaling",
        hotelOpportunityThesis: `Strong corporate thesis ${i} with lodging near Bethesda`,
      }),
    );
  }
  pool.push(
    cand({
      id: "gov_1",
      organizationName: "Local Gov Program",
      researchSegment: "government_public_sector",
      hotelOpportunityThesis: "Montgomery County public program meetings at Bethesda hotels",
    }),
  );
  const sel = selectCandidatesForDeepResearch(pool, {
    maxCandidates: 2,
    fairMinPerSegment: 1,
    hotelProfile: { city: "Bethesda" },
  });
  assert.equal(sel.selectedCount, 2);
  const segs = new Set(sel.selected.map((s) => s.researchSegment));
  assert.ok(segs.has("government_public_sector"));
  assert.ok(segs.has("corporate_accounts_scaling"));
});

check("register_explains_deferrals", () => {
  const pool = Array.from({ length: 5 }, (_, i) =>
    cand({
      id: `c${i}`,
      organizationName: `Org ${i}`,
      researchSegment: "corporate_meetings_incentives",
      hotelOpportunityThesis: `Meeting demand ${i} for Bethesda area teams`,
    }),
  );
  const sel = selectCandidatesForDeepResearch(pool, {
    maxCandidates: 2,
    fairMinPerSegment: 0,
  });
  assert.equal(sel.selectedCount, 2);
  assert.equal(sel.deferredCount, 3);
  const deferred = sel.register.filter(
    (r) => r.selectionDecision === SELECTION_DECISION.DEFERRED_LOWER_RANK,
  );
  assert.ok(deferred.length >= 3);
  assert.ok(deferred.every((r) => r.decisionReason));
});

check("budget_aware_selection", () => {
  const pool = Array.from({ length: 5 }, (_, i) =>
    cand({
      id: `b${i}`,
      organizationName: `Budget Org ${i}`,
      researchSegment: "projects_infrastructure",
      hotelOpportunityThesis: `Project workforce lodging ${i} near Bethesda`,
    }),
  );
  const sel = selectCandidatesForDeepResearch(pool, {
    maxCandidates: 10,
    fairMinPerSegment: 0,
    remainingBudgetUsd: 0.25, // ~2 medium calls
  });
  assert.ok(sel.selectedCount <= 3);
  assert.ok(sel.expectedCommittedUsd <= 0.25 + 1e-9);
});

check("chain_wires_selection_register", () => {
  const src = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js",
    ),
    "utf8",
  );
  assert.ok(src.includes("selectCandidatesForDeepResearch"));
  assert.ok(src.includes("deep-research-selection-register.json"));
});

check("bethesda_fingerprint_untouched", () => {
  const pkg = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "data/group-demand-intelligence/publication-packages/recLuxvwwxID7U2B8/gdi_pub_recLuxvwwxID7U2B8_20261113_reconciliation_v1/package.json",
      ),
      "utf8",
    ),
  );
  assert.equal(
    pkg.contentFingerprintSha256,
    "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f",
  );
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-deep-research-selection-register-v1",
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      safety: { paidExa: false, airtableWrites: false, publicationChanges: false },
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
