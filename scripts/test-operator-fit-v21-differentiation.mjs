#!/usr/bin/env node
/**
 * Operator Fit v2.1 differentiation tests (audit/shadow methodology).
 *   node scripts/test-operator-fit-v21-differentiation.mjs
 */
import {
  evaluateCandidate,
  evaluateOperatorFitForDeal,
  isOperatorFitEngineV2Enabled,
  isOperatorFitDifferentiationV21Enabled,
  OPERATOR_FIT_ENGINE_VERSION,
  OPERATOR_FIT_ENGINE_VERSION_V21,
  OPERATOR_PROJECT_FACTORS,
  PRIMARY_LAYER_WEIGHTS,
} from "../lib/operator-fit/index.js";
import { evaluateExecutionRisk } from "../lib/operator-fit/execution-risk.js";
import { evaluateExecutionRiskV21 } from "../lib/operator-fit/v21/execution-risk.js";
import { calculateComparableRelevanceIndex } from "../lib/operator-fit/v21/comparable-relevance.js";
import { calculateGeographyRelevanceScore } from "../lib/operator-fit/v21/geography.js";
import { buildOwnerTierPresentation, assignOwnerCandidateTier } from "../lib/operator-fit/v21/owner-tiers.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";
import { FIELD_STATE, ELIGIBILITY_STATUS } from "../lib/operator-fit/config.js";
import { MARKET_PRESENCE_TYPE } from "../lib/operator-intelligence/market-presence.js";
import { FIT_V2_SCENARIOS } from "../lib/operator-fit/fixtures/scenarios.js";

let failed = 0;
function ok(name, cond) {
  if (!cond) {
    failed += 1;
    console.error("FAIL", name);
  } else console.log("ok", name);
}

// Weights unchanged
ok("geo weight 22", OPERATOR_PROJECT_FACTORS.geographyMarket.weight === 22);
ok("asset weight 20", OPERATOR_PROJECT_FACTORS.assetDevelopmentExperience.weight === 20);
ok("70/15/15", PRIMARY_LAYER_WEIGHTS.operatorProjectAlignment === 70);

ok("owner engine disabled", !isOperatorFitEngineV2Enabled({ OPERATOR_FIT_ENGINE_V2: "0" }));
ok("v21 flag default off", !isOperatorFitDifferentiationV21Enabled({ OPERATOR_FIT_DIFFERENTIATION_V21: "0" }));

const project = adaptProjectFromDealContext({
  dealId: "recTEST",
  dealFields: { "Project Type": "New Build" },
  locationData: {
    Country: "Mexico",
    "Hotel Chain Scale": "Upper Upscale",
    "Building Type": "Mixed-Use",
  },
  mpData: {},
  siData: {
    "Preferred Management Structure": [],
    "Market Presence Requirement": "Active country operations required",
  },
});

function opBase(over = {}) {
  return adaptOperatorFromPrefill(
    {
      submission_status: "Active",
      companyName: over.name || "Test Op",
      activeCountries: over.countries || ["Mexico"],
      managementStructuresSupported: ["Third-Party Management"],
      chainScalesSupported: ["Upper Upscale"],
      assetExperience: over.assets || ["Urban"],
      developmentExperience: over.dev || ["New Build"],
      ...over.prefill,
    },
    { operatorId: over.id || "recTESTOP", companyName: over.name || "Test Op" }
  );
}

// Risk: v2 penalizes unknown; v21 does not numerically
const sparse = opBase({
  id: "recSPARSE",
  name: "Sparse",
  countries: ["Mexico"],
  prefill: { regionalResources: [], managementStructuresSupported: [] },
});
// force unknown structure by clearing structures
sparse.operatingStructures = { state: FIELD_STATE.UNKNOWN };
sparse.regionalResources = { state: FIELD_STATE.UNKNOWN, value: [] };

const stubElig = { status: ELIGIBILITY_STATUS.WITH_CONDITIONS, reasons: [], conditions: [], hardConflicts: [], unknowns: [] };
const stubCov = { coveragePct: 50 };
const stubStruct = { state: "unknown", score: null };
const stubBrand = { state: "not_applicable", category: "Not Applicable", numericForComposition: null };

const riskV2 = evaluateExecutionRisk(project, sparse, {
  eligibility: stubElig,
  coverage: stubCov,
  structureAlign: stubStruct,
  brandCompat: stubBrand,
});
const riskV21 = evaluateExecutionRiskV21(project, sparse, {
  eligibility: stubElig,
  coverage: stubCov,
  structureAlign: stubStruct,
  brandCompat: stubBrand,
});
ok("v2 unknown risk > 0", riskV2.cappedPenaltyPoints > 0);
ok("v21 unknown risk == 0", riskV21.cappedPenaltyPoints === 0);
ok(
  "v21 still lists unknown items",
  (riskV21.items || []).some((i) => i.kind === "unknown_validation" && i.appliedPoints === 0)
);

// Confirmed risk still penalizes
const badGeo = opBase({ id: "recBADGEO", name: "BadGeo", countries: ["Peru"] });
const riskConf = evaluateExecutionRiskV21(project, badGeo, {
  eligibility: stubElig,
  coverage: stubCov,
  structureAlign: { state: "known", score: 100 },
  brandCompat: stubBrand,
});
ok(
  "v21 confirmed geo risk > 0",
  riskConf.cappedPenaltyPoints > 0 &&
    riskConf.items.some((i) => i.kind === "confirmed_risk" && i.appliedPoints > 0)
);

// Unknown still zero factor + coverage path via evaluate
const evSparseV21 = evaluateCandidate(project, sparse, { methodology: "v21" });
ok("unknown factor contributes 0", (evSparseV21.factorBreakdown || []).some((f) => f.state === "unknown" && f.score === 0));
ok("coverage < 100 with unknowns", evSparseV21.dataCoveragePct < 100);
ok("validate next / unknowns present", (evSparseV21.validationQuestions || []).length > 0 || (evSparseV21.unknowns || []).length > 0);

// Geography: strategic interest capped; current managed stronger
const strategic = opBase({ id: "recSTRAT", name: "Strat" });
strategic.geography = {
  countries: { state: "present", value: ["Mexico"] },
  markets: { state: "unknown" },
  marketPresence: [
    { country: "Mexico", presenceType: MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST },
  ],
};
const current = opBase({ id: "recCUR", name: "Current" });
current.geography = {
  countries: { state: "present", value: ["Mexico"] },
  markets: { state: "unknown" },
  marketPresence: [
    { country: "Mexico", presenceType: MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY, propertyCount: 1 },
  ],
};
const gStrat = calculateGeographyRelevanceScore(project, strategic);
const gCur = calculateGeographyRelevanceScore(project, current);
ok("strategic < current geo", gStrat.score < gCur.score);
ok("strategic not current depth", gStrat.score <= 22);

// Historical ≠ current
const hist = opBase({ id: "recHIST", name: "Hist" });
hist.geography = {
  countries: { state: "present", value: ["Mexico"] },
  markets: { state: "unknown" },
  marketPresence: [{ country: "Mexico", presenceType: MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE }],
};
ok("historical capped", calculateGeographyRelevanceScore(project, hist).score <= 40);

// CRI top 3 only + portfolio size not free win
const specialist = opBase({
  id: "recSPEC",
  name: "Specialist",
  prefill: {
    comparables: [
      {
        propertyName: "MX Mixed-Use New Build A",
        country: "Mexico",
        situation: "New Build",
        assetType: "Mixed-Use",
        verified: true,
        currentOrHistorical: "current",
      },
      {
        propertyName: "MX Mixed-Use New Build B",
        country: "Mexico",
        situation: "New Build",
        assetType: "Mixed-Use",
        verified: true,
        currentOrHistorical: "current",
      },
      {
        propertyName: "MX Mixed-Use New Build C",
        country: "Mexico",
        situation: "New Build",
        assetType: "Mixed-Use",
        referenced: true,
        currentOrHistorical: "current",
      },
    ],
  },
});
specialist.comparables = {
  state: "present",
  value: [
    {
      propertyName: "MX Mixed-Use New Build A",
      country: "Mexico",
      situation: "New Build",
      assetType: "Mixed-Use",
      verified: true,
      currentOrHistorical: "current",
    },
    {
      propertyName: "MX Mixed-Use New Build B",
      country: "Mexico",
      situation: "New Build",
      assetType: "Mixed-Use",
      verified: true,
      currentOrHistorical: "current",
    },
    {
      propertyName: "MX Mixed-Use New Build C",
      country: "Mexico",
      situation: "New Build",
      assetType: "Mixed-Use",
      referenced: true,
      currentOrHistorical: "current",
    },
  ],
};
specialist.geography = current.geography;
specialist.developmentExperience = { state: "present", value: ["New Build", "New Build", "Conversion"] };

const generalist = opBase({
  id: "recGEN",
  name: "Generalist",
  assets: ["Urban", "Resort", "Select", "Extended Stay", "Airport", "Suburban"],
  dev: ["Renovation"],
});
generalist.comparables = {
  state: "present",
  value: Array.from({ length: 12 }, (_, i) => ({
    propertyName: `Generic Hotel ${i}`,
    country: "United States",
    situation: "Operating",
    assetType: "Select Service",
    verified: true,
  })),
};
generalist.geography = {
  countries: { state: "present", value: ["United States", "Mexico"] },
  markets: { state: "unknown" },
  marketPresence: [
    { country: "Mexico", presenceType: MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST },
  ],
};

const criSpec = calculateComparableRelevanceIndex(project, specialist, "moderate");
const criGen = calculateComparableRelevanceIndex(project, generalist, "moderate");
ok("CRI uses at most 3", criSpec.topComparables.length <= 3);
ok("specialist CRI > generalist CRI", criSpec.criScore100 > criGen.criScore100);

const evSpec = evaluateCandidate(project, specialist, { methodology: "v21" });
const evGen = evaluateCandidate(project, generalist, { methodology: "v21" });
ok(
  "specialist can outperform generalist displayed",
  evSpec.displayedOperatorAlignment > evGen.displayedOperatorAlignment
);

// Tie materiality
const tiers = buildOwnerTierPresentation(
  [
    {
      candidateId: "a",
      operatorName: "A",
      displayedOperatorAlignment: 48.3,
      eligibilityStatus: ELIGIBILITY_STATUS.ELIGIBLE,
      readiness: "Ranking Ready",
    },
    {
      candidateId: "b",
      operatorName: "B",
      displayedOperatorAlignment: 48.0,
      eligibilityStatus: ELIGIBILITY_STATUS.ELIGIBLE,
      readiness: "Ranking Ready",
    },
  ],
  {}
);
const potential = tiers.tiers["Potential Fits — Validation Needed"] || [];
ok(
  "tie <1 hides ordinals",
  potential.length === 2 && potential.every((c) => c.ownerOrdinal == null)
);

const tierCond = assignOwnerCandidateTier({
  displayedOperatorAlignment: 48,
  eligibilityStatus: ELIGIBILITY_STATUS.WITH_CONDITIONS,
  readiness: "Ranking Ready",
});
ok("with conditions not leading", tierCond.tierId === "potential" || tierCond.tierId === "additional");

const researchTier = assignOwnerCandidateTier({
  displayedOperatorAlignment: 80,
  eligibilityStatus: ELIGIBILITY_STATUS.ELIGIBLE,
  readiness: "Ranking Ready",
  researchStage: true,
});
ok("research under evaluation", researchTier.tierId === "under_evaluation");

// Narrative on ties — why text should not claim "Aligned because" comparative supremacy in v21
ok(
  "v21 why softened",
  !(evSpec.whyItMatches || []).some((w) => /^Aligned because of/i.test(w))
);

// Legacy v2 reproducible version string
const evV2 = evaluateCandidate(project, specialist, { methodology: "v2" });
ok("v2 feature version", evV2.featureVersion === OPERATOR_FIT_ENGINE_VERSION);
ok("v21 feature version", evSpec.featureVersion === OPERATOR_FIT_ENGINE_VERSION_V21);

// Deal evaluate methodologies diverge for sparse risk
const sc = FIT_V2_SCENARIOS[0];
const dealV2 = evaluateOperatorFitForDeal({
  dealId: "recD2",
  dealFields: sc.dealFields,
  locationData: sc.locationData,
  mpData: {},
  siData: sc.siData,
  operatorPrefills: [
    { operatorId: "recSPEC", companyName: "Specialist", prefill: { submission_status: "Active", companyName: "Specialist", activeCountries: ["Mexico"], chainScalesSupported: ["Upper Upscale"], managementStructuresSupported: ["Third-Party Management"] } },
  ],
  methodology: "v2",
});
ok("v2 deal methodology", dealV2.methodology === "v2");

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll Operator Fit v2.1 differentiation tests passed");
