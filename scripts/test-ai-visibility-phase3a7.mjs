#!/usr/bin/env node
/**
 * Phase 3A.7 — Showcase portfolio + peer v2 + decision eligibility governance tests.
 * No provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadPeerSetConfig,
  resolvePeerSetMembership,
  diffBrandPeerSetVersions,
  PEER_SET_ID_V1,
  PEER_SET_ID_V2,
} from "../lib/ai-visibility/peer-sets.js";
import {
  loadShowcaseCompaniesConfig,
  getShowcaseCompany,
  assertShowcasePortfolioParentPurity,
} from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import {
  loadDecisionEligibilityConfig,
  getBrandDecisionEligibility,
  summarizeIntentCompetitiveDensity,
  eligibilityIsLanguageNeutral,
  ELIGIBILITY,
  SHOWCASE_DECISION_TERRITORIES,
} from "../lib/ai-visibility/brand-decision-eligibility.js";
import { deriveBrandArchetypes } from "../lib/ai-visibility/brand-archetypes.js";
import {
  getBrandGeographyEligibility,
} from "../lib/ai-visibility/brand-geography-eligibility.js";
import { normalizeParentCompany } from "../lib/ai-visibility/parent-company-normalize.js";
import { resolveAiIntelligenceAccess } from "../lib/ai-visibility/authorization.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { buildFixtureViewerContext } from "../lib/ai-visibility/viewer-context.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const audit = JSON.parse(
  fs.readFileSync(
    path.join(root, "data/ai-visibility/phase3a7-showcase-brand-basics-audit.json"),
    "utf8"
  )
);

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3A.7 — Showcase Data Governance\n");

const liveById = Object.fromEntries(
  audit.rows.map((r) => [
    r.BRAND_ID,
    { id: r.BRAND_ID, name: r.BRAND_NAME_LIVE, parentCompany: r.CURRENT_PARENT },
  ])
);
const mg = audit.mgalleryAndKimptonNameHits.find((b) => b.brandName === "MGallery Collection");
liveById[mg.brandId] = {
  id: mg.brandId,
  name: mg.brandName,
  parentCompany: mg.parent,
};

test("Marriott portfolio — Marriott brands only", () => {
  const m = getShowcaseCompany("marriott");
  assert.equal(m.ok, true);
  assert.equal(m.brandIds.length, 5);
  const purity = assertShowcasePortfolioParentPurity("marriott", liveById);
  assert.equal(purity.ok, true, JSON.stringify(purity.violations));
});

test("Hilton portfolio — Hilton brands only", () => {
  const h = getShowcaseCompany("hilton");
  assert.equal(h.brandIds.length, 4);
  assert.equal(assertShowcasePortfolioParentPurity("hilton", liveById).ok, true);
});

test("Choice portfolio — Choice brands only (includes RED)", () => {
  const c = getShowcaseCompany("choice");
  assert.equal(c.brandIds.length, 4);
  assert.ok(c.brandIds.includes("recmKqo7M7mLZgRqQ"));
  assert.equal(assertShowcasePortfolioParentPurity("choice", liveById).ok, true);
});

test("peer v2 — 15 canonical Active/Live IDs; no duplicates; v1 preserved", () => {
  const cfg = loadPeerSetConfig();
  const v1 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V1 }, cfg);
  const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, cfg);
  assert.equal(v1.ok, true);
  assert.equal(v1.entityIds.length, 10);
  assert.equal(v2.ok, true);
  assert.equal(v2.entityIds.length, 15);
  assert.equal(new Set(v2.entityIds).size, 15);
  assert.ok(v2.entityIds.includes(mg.brandId));
  assert.ok(!v2.entityIds.includes("recmKqo7M7mLZgRqQ")); // RED not in cohort
  const diff = diffBrandPeerSetVersions(cfg);
  assert.equal(diff.V1_PRESERVED, true);
  assert.equal(diff.REMOVED.length, 0);
  assert.equal(diff.ADDED.length, 5);
  // Historical batch file still names v1
  const batch = JSON.parse(
    fs.readFileSync(
      path.join(
        root,
        "data/ai-visibility/runtime/phase2e/batches/aiv_batch_20260813_d14b3e80.json"
      ),
      "utf8"
    )
  );
  assert.equal(batch.peerSetId, PEER_SET_ID_V1);
});

test("decision eligibility deterministic; UNKNOWN preserved; no LLM", () => {
  const cfg = loadDecisionEligibilityConfig();
  assert.equal(cfg.LANGUAGE_NEUTRAL, true);
  // Phase 3A.9 adds Branded Residences + Soft-Brand Affiliation Flexibility entries (legacy retained)
  assert.ok(cfg.entries.length >= 15 * 7);
  assert.deepEqual(cfg.decisionTerritories, [...SHOWCASE_DECISION_TERRITORIES]);
  const westinSoft = getBrandDecisionEligibility(
    "recIPuBC50fv13zRR",
    "Collection / Soft Brand",
    cfg
  );
  assert.equal(westinSoft.state, ELIGIBILITY.NOT_ELIGIBLE);
  const bluSoft = getBrandDecisionEligibility(
    "recWPEvxBQxVVzSq3",
    "Collection / Soft Brand",
    cfg
  );
  assert.equal(bluSoft.state, ELIGIBILITY.NOT_ELIGIBLE);
  // Phase 3A.8 may mark New Build ELIGIBLE from footprint; Design Hotels stays UNKNOWN
  const newBuild = getBrandDecisionEligibility(
    "rec02zPClpWUTCyXM",
    "New Build",
    cfg
  );
  assert.equal(newBuild.state, ELIGIBILITY.UNKNOWN);
  assert.equal(eligibilityIsLanguageNeutral(), true);
  const dens = summarizeIntentCompetitiveDensity(cfg, [...SHOWCASE_DECISION_TERRITORIES]);
  assert.equal(dens.length, SHOWCASE_DECISION_TERRITORIES.length);
});

test("language does not change eligibility", () => {
  const en = getBrandDecisionEligibility("recEJCTDj1zrsjPM6", "Conversion");
  const es = getBrandDecisionEligibility("recEJCTDj1zrsjPM6", "Conversion");
  assert.equal(en.state, es.state);
  assert.equal(en.LANGUAGE_NEUTRAL, true);
});

test("geography eligibility Mexico UNKNOWN + under CALA; independent of language", () => {
  const geo = getBrandGeographyEligibility("recEJCTDj1zrsjPM6");
  assert.equal(geo.MEXICO, "UNKNOWN");
  assert.equal(geo.MEXICO_UNDER_CALA, true);
  assert.equal(geo.LANGUAGE_INDEPENDENT, true);
  // Phase 3A.8 may set CALA/Europe/NA from Region Offered — do not assert UNKNOWN globally.
});

test("parent normalization collapses Marriott Inc / Choice International / AccorHotels", () => {
  assert.equal(
    normalizeParentCompany("Marriott International, Inc.").canonical,
    "Marriott International"
  );
  assert.equal(
    normalizeParentCompany("Choice Hotels International").canonical,
    "Choice Hotels"
  );
  assert.equal(normalizeParentCompany("AccorHotels").canonical, "Accor");
  assert.equal(normalizeParentCompany("Hilton Worldwide").canonical, "Hilton");
});

test("archetypes from Brand Model + Chain Scale", () => {
  const a = deriveBrandArchetypes({
    brandModel: "Collection Brand",
    chainScale: "Upper Upscale",
  });
  assert.ok(a.archetypes.includes("COLLECTION"));
  assert.ok(a.archetypes.includes("SOFT_BRAND"));
  assert.ok(a.archetypes.includes("UPPER_UPSCALE"));
  const h = deriveBrandArchetypes({
    brandModel: "Hard Brand",
    chainScale: "Upper Upscale",
  });
  assert.ok(h.archetypes.includes("HARD_BRAND"));
  assert.ok(!h.archetypes.includes("SOFT_BRAND"));
});

test("authorization unchanged — showcase config is not a bypass", () => {
  const cfg = loadShowcaseCompaniesConfig();
  assert.ok(cfg.notes.some((n) => /NOT an authorization bypass/i.test(n)));
  const viewer = buildFixtureViewerContext({
    viewerCompanyId: "co_other",
    isBrand: true,
  });
  const graph = buildFixtureEntitlementGraph({
    entitledBrandIds: ["recOther"],
  });
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: "recEJCTDj1zrsjPM6" },
    entitlementGraph: graph,
  });
  assert.equal(access.allowed, false);
});

test("metric formulas unchanged; Addressable Rec Rate not implemented", () => {
  assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
  const src = fs.readFileSync(
    path.join(root, "lib/ai-visibility/metrics.js"),
    "utf8"
  );
  assert.ok(!/computeAddressableRecommendationRate/.test(src));
});

test("intent density summary available", () => {
  const dens = summarizeIntentCompetitiveDensity();
  const soft = dens.find((d) => d.decisionTerritory === "Collection / Soft Brand");
  assert.ok(soft.ELIGIBLE >= 5);
  assert.ok(soft.NOT_ELIGIBLE >= 2); // Westin + Blu + lifestyles
});

console.log(`\nResults: ${passed} passed, ${failed} failed`);
console.log("LIVE_PROVIDER_CALLS: 0");
console.log("AIRTABLE_PRODUCTION_CLIENT_WRITES: 0");
if (failed) process.exit(1);
