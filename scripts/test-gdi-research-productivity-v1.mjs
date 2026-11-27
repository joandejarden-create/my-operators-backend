#!/usr/bin/env node
/**
 * GDI Research Productivity V1 regression.
 */
import assert from "node:assert/strict";
import {
  RESEARCH_EFFORT_BENCHMARKS,
  GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
  BENCHMARK_VALIDATION_STATUS,
  WORKING_DAY_ASSUMPTION,
} from "../lib/group-demand-intelligence/executive-summary/gdi-research-effort-benchmark-contract-v1.js";
import {
  buildGdiResearchProductivity,
  deriveResearchActivityVolumes,
  GDI_RESEARCH_PRODUCTIVITY_CONTRACT_ID,
  PRODUCTIVITY_ESTIMATE_CLASS,
} from "../lib/group-demand-intelligence/executive-summary/gdi-research-productivity-v1.js";
import {
  buildGdiExecutiveSummaryV2,
  stripResearchIntelligencePerspective,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v2.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

assert.ok(RESEARCH_EFFORT_BENCHMARKS.length >= 12);
assert.equal(WORKING_DAY_ASSUMPTION.hoursPerWorkingDay, 8);
for (const b of RESEARCH_EFFORT_BENCHMARKS) {
  assert.ok(b.hours.low > 0 && b.hours.base >= b.hours.low && b.hours.high >= b.hours.base);
  assert.equal(b.validationStatus, BENCHMARK_VALIDATION_STATUS.PROVISIONAL);
  assert.equal(b.benchmarkVersion, GDI_RESEARCH_EFFORT_BENCHMARK_VERSION);
}

const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer("recnPB3Q02NO8Ma1o", "STAGED");
assert.ok(ptr?.publicationVersion);
const loaded = store.loadPackage("recnPB3Q02NO8Ma1o", ptr.publicationVersion);
assert.equal(loaded.ok, true);
const opps = loaded.package.opportunities;
assert.equal(opps.length, 26);

const volumes = deriveResearchActivityVolumes({ opportunities: opps });
// Overlap rule: deep-researched rows are counted under deep research only
// (discovery count may be 0 when every staged opp has a deep dossier).
assert.ok(
  volumes.demand_discovery_and_screening.count +
    volumes.deep_opportunity_research.count >=
    opps.length,
);
assert.ok(volumes.deep_opportunity_research.count > 0);
assert.equal(
  volumes.opportunities_screened_out.availability,
  "TELEMETRY_REQUIRED",
);
assert.equal(volumes.weekly_research_revalidation.availability, "UNAVAILABLE");

const productivity = buildGdiResearchProductivity({ opportunities: opps });
assert.equal(productivity.contractId, GDI_RESEARCH_PRODUCTIVITY_CONTRACT_ID);
assert.equal(
  productivity.estimateClass,
  PRODUCTIVITY_ESTIMATE_CLASS.PROVISIONAL_ESTIMATE,
);
assert.ok(
  productivity.customerMetrics.estimatedResearchHoursSaved.value > 0,
  "hours saved should be positive for Wyndham STAGED",
);
assert.ok(
  productivity.customerMetrics.equivalentWorkingDaysSaved.value > 0,
);
assert.equal(
  productivity.customerMetrics.estimatedResearchLaborValue.availability,
  "HOTEL_INPUT_REQUIRED",
);
assert.match(productivity.disclaimer, /not actual payroll savings/i);
assert.ok(productivity.methodologyExplanation?.activities?.length >= 5);
assert.equal(
  productivity.customerMetrics.researchProductivityMultiple.value,
  null,
);

const withLabor = buildGdiResearchProductivity({
  opportunities: opps,
  laborCostAuthorized: true,
  fullyLoadedLaborCostPerHour: 75,
  actualHumanResearchHours: 20,
  dealalityInvestmentAuthorized: true,
  dealalityInvestmentForPeriod: 5000,
  providerSpendUsd: 1200,
});
assert.ok(withLabor.customerMetrics.estimatedResearchLaborValue.value > 0);
assert.ok(withLabor.customerMetrics.researchProductivityMultiple.value > 1);
assert.ok(
  withLabor.customerMetrics.researchLaborValueRelativeToInvestment.value > 0,
);
assert.equal(
  withLabor.internalMetrics.actualProviderExpenditureUsd.value,
  1200,
);

const payload = buildGdiExecutiveSummaryV2({
  hotelId: "recnPB3Q02NO8Ma1o",
  hotelName: "Wyndham San Diego Bayside",
  opportunities: opps,
  founderPreview: true,
  customerAuthorizedInventory: false,
  dataState: "FOUNDER_PREVIEW",
  publicationMeta: { publicationVersion: ptr.publicationVersion },
  includeResearchIntelligence: true,
});
assert.ok(payload.sections.researchProductivity);
assert.ok(
  payload.sections.executiveOverview.researchProductivity.metrics.some(
    (m) => m.key === "estimatedResearchHoursSaved" && m.value > 0,
  ),
);
assert.ok(payload.sections.salesPerformance.researchTimeSaved?.breakdown?.length);
assert.ok(
  payload.sections.researchIntelligence.metrics.some(
    (m) => m.key === "equivalentManualResearchHours",
  ),
);
assert.equal(
  payload.sections.executiveOverview.investmentRecovery.categories
    .estimatedResearchProductivityValue.category,
  "A",
);

const customerPayload = buildGdiExecutiveSummaryV2({
  hotelId: "recnPB3Q02NO8Ma1o",
  opportunities: opps,
  founderPreview: false,
  customerAuthorizedInventory: true,
  dataState: "CURRENT_AVAILABLE",
  includeResearchIntelligence: false,
});
assert.equal(customerPayload.sections.researchIntelligence, undefined);
assert.equal(
  customerPayload.sections.researchProductivity.internalMetrics,
  undefined,
);
const stripped = stripResearchIntelligencePerspective(payload);
assert.equal(stripped.sections.researchIntelligence, undefined);
assert.equal(stripped.sections.researchProductivity.internalMetrics, undefined);

console.log(
  JSON.stringify(
    {
      ok: true,
      hoursSaved:
        productivity.customerMetrics.estimatedResearchHoursSaved.value,
      daysSaved:
        productivity.customerMetrics.equivalentWorkingDaysSaved.value,
      benchmarkVersion: GDI_RESEARCH_EFFORT_BENCHMARK_VERSION,
      salesBuckets: productivity.salesTimeSavedBreakdown.length,
    },
    null,
    2,
  ),
);
