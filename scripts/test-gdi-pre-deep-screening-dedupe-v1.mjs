#!/usr/bin/env node
/**
 * Pre-deep screening + published inventory dedupe tests (no paid).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadPublishedPackageInventory,
  evaluateAgainstPublishedInventory,
} from "../lib/group-demand-intelligence/research/gdi-published-inventory-dedupe-v1.js";
import {
  screenCandidatePreDeep,
  screenAllCandidatesPreDeep,
  PRE_DEEP_SCREEN_OUTCOME,
} from "../lib/group-demand-intelligence/research/gdi-pre-deep-candidate-screening-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL = "recLuxvwwxID7U2B8";
const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

check("loads_bethesda_67_published_package", () => {
  const inv = loadPublishedPackageInventory(HOTEL);
  assert.equal(inv.found, true);
  assert.ok(inv.opportunityCount >= 67);
  assert.ok(inv.orgKeys.size > 0);
  assert.ok(
    inv.packageMeta?.contentFingerprintSha256?.startsWith("441773211494"),
  );
});

check("rock_creek_mansion_is_duplicate_no_deep", () => {
  const inv = loadPublishedPackageInventory(HOTEL);
  const evald = evaluateAgainstPublishedInventory(
    {
      organizationName: "Rock Creek Mansion",
      title: "Preferred lodging partnership",
      researchSegment: "preferred_lodging_partnerships",
    },
    inv,
  );
  assert.equal(evald.decision, "DUPLICATE_NO_NEW_EVIDENCE");
  const screen = screenCandidatePreDeep(
    {
      id: "cand_rcm",
      organizationName: "Rock Creek Mansion",
      title: "Preferred lodging",
      seedUrls: ["https://example.com/rcm"],
      researchSegment: "preferred_lodging_partnerships",
    },
    { hotelId: HOTEL, publishedInventory: inv, hotelProfile: { city: "Bethesda" } },
  );
  assert.equal(screen.outcome, PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE);
  assert.equal(screen.eligibleForDeep, false);
});

check("known_org_new_occurrence_can_enrich", () => {
  const inv = loadPublishedPackageInventory(HOTEL);
  // AHIMA is in published 67 — new training occurrence should enrich, not hard-block
  const evald = evaluateAgainstPublishedInventory(
    {
      organizationName: "AHIMA",
      title: "AHIMA Regional Training Cohort 2027",
      opportunityType: "TRAINING",
      hotelOpportunityThesis:
        "Future cycle training cohort for regional coding staff near Bethesda campus",
      futureCycleSupported: true,
      seedUrls: ["https://example.com/ahima-training"],
    },
    inv,
  );
  assert.equal(evald.decision, "EXISTING_OPPORTUNITY_ENRICHMENT");
  assert.equal(evald.allowDeepResearch, true);
});

check("chicago_out_of_market_is_global_only", () => {
  const screen = screenCandidatePreDeep(
    {
      id: "cand_chi",
      organizationName: "Chicago Widget Association",
      title: "Chicago Annual 2027",
      city: "Chicago",
      seedUrls: ["https://example.com/chi"],
      geoFit: { geoFitClass: "OUT_OF_MARKET" },
      researchSegment: "associations_conferences_events",
    },
    { hotelId: HOTEL, hotelProfile: { city: "Bethesda" } },
  );
  assert.equal(screen.outcome, PRE_DEEP_SCREEN_OUTCOME.GLOBAL_INTELLIGENCE_ONLY);
  assert.equal(screen.eligibleForDeep, false);
  assert.equal(screen.outOfMarketForCurrentHotel, true);
});

check("screen_all_covers_every_candidate", () => {
  const candidates = [
    {
      id: "a",
      organizationName: "Net New Bethesda Corp",
      title: "Office expansion Rockville",
      city: "Rockville",
      seedUrls: ["https://example.com/n"],
      hotelOpportunityThesis:
        "Corporate expansion into Rockville with training and project team lodging needs near Bethesda Marriott",
      researchSegment: "corporate_accounts_scaling",
    },
    {
      id: "b",
      organizationName: "Rock Creek Mansion",
      title: "Venue partnership",
      seedUrls: ["https://example.com/r"],
      researchSegment: "preferred_lodging_partnerships",
    },
    {
      id: "c",
      organizationName: "National Expo",
      title: "Dallas Expo 2027",
      city: "Dallas",
      seedUrls: ["https://example.com/d"],
      geoFit: { geoFitClass: "OUT_OF_MARKET" },
      researchSegment: "associations_conferences_events",
    },
    {
      id: "d",
      organizationName: "?",
      unsupportedNameOnly: true,
    },
  ];
  const ledger = screenAllCandidatesPreDeep(candidates, {
    hotelId: HOTEL,
    hotelProfile: { city: "Bethesda" },
    runId: "test_pre_deep",
  });
  assert.equal(ledger.screenedCount, 4);
  assert.equal(ledger.candidateCount, 4);
  assert.ok(ledger.outcomeCounts[PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE] >= 1);
  assert.ok(ledger.outcomeCounts[PRE_DEEP_SCREEN_OUTCOME.GLOBAL_INTELLIGENCE_ONLY] >= 1);
  assert.ok(ledger.eligibleForDeepCount >= 1);
  assert.ok(
    ledger.eligibleCandidates.every((c) => c.preDeepScreen?.eligibleForDeep === true),
  );
});

check("chain_screens_before_cap", () => {
  const src = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js",
    ),
    "utf8",
  );
  assert.ok(src.includes("screenAllCandidatesPreDeep"));
  assert.ok(src.includes("pre-deep-candidate-screening.json"));
  const screenIdx = src.indexOf("screenAllCandidatesPreDeep");
  const capIdx = src.indexOf("candidateCapApplied");
  assert.ok(screenIdx > 0 && capIdx > screenIdx);
});

check("published_fingerprint_unchanged", () => {
  const inv = loadPublishedPackageInventory(HOTEL);
  assert.equal(
    inv.packageMeta.contentFingerprintSha256,
    "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f",
  );
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-pre-deep-screening-dedupe-v1",
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
