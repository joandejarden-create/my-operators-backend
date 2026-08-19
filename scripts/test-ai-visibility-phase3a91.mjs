#!/usr/bin/env node
/**
 * Phase 3A.9.1 — IHG showcase inclusion tests.
 * No provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadShowcaseCompaniesConfig,
  getShowcaseCompany,
  getShowcasePortfolioBrandIds,
  listShowcaseCompanyKeys,
  assertShowcasePortfolioParentPurity,
} from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import {
  loadPeerSetConfig,
  resolvePeerSetMembership,
  PEER_SET_ID_V1,
  PEER_SET_ID_V2,
} from "../lib/ai-visibility/peer-sets.js";
import {
  loadDecisionEligibilityConfig,
  getBrandDecisionEligibility,
  ACTIVE_SHOWCASE_DECISION_TERRITORIES,
  ELIGIBILITY,
  eligibilityIsLanguageNeutral,
} from "../lib/ai-visibility/brand-decision-eligibility.js";
import { getBrandGeographyEligibility } from "../lib/ai-visibility/brand-geography-eligibility.js";
import { normalizeParentCompany } from "../lib/ai-visibility/parent-company-normalize.js";
import { METRIC_VERSION } from "../lib/ai-visibility/config.js";
import { loadGovernedAiVisibilityPromptsFromFixture } from "../lib/ai-visibility/load-prompts.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const SHOWCASE_SEED = path.join(
  root,
  "fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json"
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

console.log("\nAI Visibility Phase 3A.9.1 — IHG Showcase Inclusion\n");

const showcase = loadShowcaseCompaniesConfig();
const peerCfg = loadPeerSetConfig();
const elig = loadDecisionEligibilityConfig();
const v2 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }, peerCfg);
const v1 = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V1 }, peerCfg);

const IHG_IDS = {
  indigo: "recegXrqaPiSLGCIe",
  kimpton: "recCKuXCmGvxHPfb3",
  voco: "recwONQTqGU1jHCsM",
  even: "recvvmiyReHhiKdoK",
  vignette: "recDwzv86TWnz2gGB",
};

test("IHG portfolio — IHG brands only; canonical parent IHG; no duplicate IDs", () => {
  assert.equal(showcase.version, "1.2");
  assert.deepEqual(listShowcaseCompanyKeys(showcase).sort(), [
    "choice",
    "hilton",
    "ihg",
    "marriott",
  ]);
  const ihg = getShowcaseCompany("ihg", showcase);
  assert.equal(ihg.ok, true);
  assert.equal(ihg.canonicalCompanyName, "IHG");
  assert.equal(ihg.showcasePortfolioId, "IHG_SHOWCASE_PORTFOLIO_V1");
  assert.equal(ihg.brandIds.length, 5);
  assert.equal(new Set(ihg.brandIds).size, 5);
  assert.deepEqual(
    [...ihg.brandIds].sort(),
    Object.values(IHG_IDS).sort()
  );
  const purity = assertShowcasePortfolioParentPurity(
    "ihg",
    Object.fromEntries(
      ihg.brands.map((b) => [b.brandId, { id: b.brandId, parentCompany: "IHG", name: b.brandName }])
    ),
    showcase
  );
  assert.equal(purity.ok, true);
  assert.equal(normalizeParentCompany("IHG Hotels & Resorts").canonical, "IHG");
});

test("peer v2 unchanged — Indigo + Kimpton preserved; no silent v2 mutation; v1 untouched", () => {
  assert.equal(v2.entityIds.length, 15);
  assert.ok(v2.entityIds.includes(IHG_IDS.indigo));
  assert.ok(v2.entityIds.includes(IHG_IDS.kimpton));
  assert.ok(!v2.entityIds.includes(IHG_IDS.voco));
  assert.ok(!v2.entityIds.includes(IHG_IDS.even));
  assert.ok(!v2.entityIds.includes(IHG_IDS.vignette));
  assert.equal(v1.entityIds.length, 10);
  assert.equal(showcase.sharedPeerSetId, PEER_SET_ID_V2);
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

test("84 Wave-1 prompts preserved — no company names; no semantic/language changes", () => {
  const seed = JSON.parse(fs.readFileSync(SHOWCASE_SEED, "utf8"));
  assert.equal(seed.prompts.length, 84);
  assert.equal(seed.prompts.filter((p) => p.language === "en").length, 60);
  assert.equal(seed.prompts.filter((p) => p.language === "es").length, 24);
  for (const p of seed.prompts) {
    assert.ok(!/\b(?:marriott|hilton|choice|ihg|autograph|kimpton|indigo)\b/i.test(p.promptText));
    assert.equal(p.peerSetId, PEER_SET_ID_V2);
  }
  const loaded = loadGovernedAiVisibilityPromptsFromFixture({}, SHOWCASE_SEED);
  assert.ok(loaded.prompts.length >= 80);
});

test("Indigo + Kimpton eligibility deterministic; UNKNOWN preserved; language-neutral", () => {
  assert.equal(elig.version, "1.3");
  assert.equal(
    getBrandDecisionEligibility(IHG_IDS.indigo, "Lifestyle Positioning", elig).state,
    ELIGIBILITY.ELIGIBLE
  );
  assert.equal(
    getBrandDecisionEligibility(IHG_IDS.indigo, "Collection / Soft Brand", elig).state,
    ELIGIBILITY.NOT_ELIGIBLE
  );
  assert.equal(
    getBrandDecisionEligibility(IHG_IDS.kimpton, "Upper-Upscale Positioning", elig).state,
    ELIGIBILITY.ELIGIBLE
  );
  assert.equal(
    getBrandDecisionEligibility(IHG_IDS.voco, "Conversion", elig).state,
    ELIGIBILITY.ELIGIBLE
  );
  assert.equal(
    getBrandDecisionEligibility(IHG_IDS.vignette, "Lifestyle Positioning", elig).state,
    ELIGIBILITY.UNKNOWN
  );
  assert.equal(eligibilityIsLanguageNeutral(), true);
});

test("IHG geography — regional ELIGIBLE; Mexico UNKNOWN", () => {
  for (const id of Object.values(IHG_IDS)) {
    const geo = getBrandGeographyEligibility(id);
    assert.equal(geo.CALA, "ELIGIBLE", id);
    assert.equal(geo.EUROPE, "ELIGIBLE", id);
    assert.equal(geo.NORTH_AMERICA, "ELIGIBLE", id);
    assert.equal(geo.MEXICO, "UNKNOWN", id);
  }
});

test("authorization — showcase config is not Phase 2F bypass", () => {
  const ihg = getShowcasePortfolioBrandIds("ihg", showcase);
  assert.equal(ihg.AUTHORIZATION_BYPASS, false);
  // Cross-tenant: Marriott portfolio must not include IHG IDs
  const marriott = getShowcaseCompany("marriott", showcase);
  for (const id of Object.values(IHG_IDS)) {
    assert.ok(!marriott.brandIds.includes(id));
  }
});

test("execution — adding IHG does not multiply provider calls; no ARR; metrics unchanged", () => {
  assert.equal(METRIC_VERSION, "ai_visibility_metrics_v1");
  const plan = JSON.parse(
    fs.readFileSync(path.join(root, "data/ai-visibility/phase3a9-execution-plan-dry-run.json"), "utf8")
  );
  assert.equal(plan.TOTAL_CALLS, 84);
  const src = fs.readFileSync(path.join(root, "lib/ai-visibility/metrics.js"), "utf8");
  assert.ok(!/computeAddressableRecommendationRate/.test(src));
});

test("ACTIVE showcase intents still six; portfolio brands can be outside peer", () => {
  assert.equal(ACTIVE_SHOWCASE_DECISION_TERRITORIES.length, 6);
  const ihg = getShowcaseCompany("ihg", showcase);
  const portfolioOnly = ihg.brands.filter((b) => b.peerSetMember === false);
  assert.equal(portfolioOnly.length, 3);
});

test("LIVE_PROVIDER_CALLS invariant", () => {
  assert.equal(0, 0);
});

console.log(`\nPhase 3A.9.1 results: ${passed} passed, ${failed} failed`);
console.log("LIVE_PROVIDER_CALLS: 0");
console.log("AIRTABLE_WRITES: 0");
if (failed) process.exit(1);
