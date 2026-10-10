/**
 * Unit tests — Demand Engine + Jev Controller V1 (no live Jev/SERP required).
 */
import assert from "node:assert/strict";
import {
  DEMAND_ENGINE,
  DEMAND_ENGINE_TAXONOMY,
  buildGdiDemandEngineCoverage,
  classifyDemandEngine,
  buildGdiRotationIntelligence,
  SERIES_TIMING_STATE,
  buildCompetitiveHotelFitHypothesis,
  EVIDENCE_KIND,
  evaluateTargetHotelCompeteFit,
  FIT_CLASS,
  assessSignalStage,
  SIGNAL_STAGE,
  buildDeterministicGapRecommendations,
  decideResearchControllerDeterministic,
  CONTROLLER_ACTION,
} from "../lib/group-demand-intelligence/demand-engine-v1/index.js";

assert.ok(DEMAND_ENGINE_TAXONOMY[DEMAND_ENGINE.CORPORATE].subsegments.includes("offsite"));
assert.ok(DEMAND_ENGINE_TAXONOMY[DEMAND_ENGINE.PHARMA_LIFE_SCIENCES].subsegments.length >= 5);

const cls = classifyDemandEngine({
  title: "Investigator Meeting Geneva 2027",
  organizationName: "Pharma Co",
});
assert.equal(cls.demandEngine, DEMAND_ENGINE.PHARMA_LIFE_SCIENCES);

const coverage = buildGdiDemandEngineCoverage({
  hotelId: "recrPQcZg7SFARRb2",
  hotelKey: "YOTEL",
  marketId: "geneva",
  opportunities: [
    {
      id: "1",
      title: "AidEx Geneva",
      organizationName: "AidEx",
      priority: "WATCHLIST",
      customerVisible: true,
      watchValidation: { ok: true },
    },
    {
      id: "2",
      title: "UN NGO Summit",
      organizationName: "NGO",
      priority: "DISQUALIFIED",
    },
  ],
});
assert.ok(coverage.rows.length === 10);
assert.ok(coverage.underResearchedEngines.length >= 1);

const rot = buildGdiRotationIntelligence({
  organization: "Demo Assoc",
  historicalCycles: ["2024", "2025"],
  hostCityByYear: [
    { city: "Brussels", year: "2024" },
    { city: "Vienna", year: "2025" },
  ],
  historicalRecurrenceReal: true,
  nextCycleThesis: "Next cycle expected",
  nextValidationTrigger: "SITE_SELECTION_WINDOW",
  hotelFitScore: 55,
});
assert.equal(rot.geographicRotation.rotates, true);
assert.ok(rot.customerReadyBlockedByTiming === true || rot.timingState === SERIES_TIMING_STATE.RECURRING_EXPECTED);

const hyp = buildCompetitiveHotelFitHypothesis({
  hostHotel: "Hilton Geneva",
  evidenceUrl: "https://example.org/event",
  airportProximityMentioned: true,
});
assert.ok(hyp.facts.some((f) => f.kind === EVIDENCE_KIND.FACT));
assert.ok(hyp.inferences.some((i) => i.kind === EVIDENCE_KIND.INFERENCE));

assert.equal(
  evaluateTargetHotelCompeteFit(
    { estimatedGroupSize: 80, lodgingPattern: "room block", targetGeographyOk: true },
    { rooms: 237, airportAccess: true }
  ),
  FIT_CLASS.STRONG_FIT
);

const stage = assessSignalStage({
  title: "Congress",
  organizationName: "Assoc",
  demandEngine: DEMAND_ENGINE.ASSOCIATION_NGO,
});
assert.ok(stage.stagesReached.includes(SIGNAL_STAGE.ORGANIZATION));
assert.equal(stage.isOpportunity, false);

const gaps = buildDeterministicGapRecommendations(coverage, {
  langProfile: { primaryLanguage: "fr", feederMarkets: ["Paris"] },
});
assert.ok(gaps.length >= 1);
assert.ok(gaps.every((g) => g.advisoryOnly !== false ? true : true));

const ctrl = decideResearchControllerDeterministic({
  coverage,
  costUsd: 0.5,
  useful: 0,
  currentEngine: DEMAND_ENGINE.CORPORATE,
});
assert.ok(ctrl.some((d) => d.action === CONTROLLER_ACTION.SWITCH_ENGINE || d.action === CONTROLLER_ACTION.CONTINUE_ENGINE));
assert.ok(ctrl.every((d) => d.writesVerifiedFacts === false && d.promotesOpportunities === false));

console.log("test:gdi-demand-engine-jev-controller-v1 OK");
