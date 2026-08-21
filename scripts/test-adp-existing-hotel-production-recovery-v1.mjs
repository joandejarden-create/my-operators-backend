#!/usr/bin/env node
/**
 * Existing Hotel ADP production recovery regressions (correctness only).
 *   npm run test:adp-existing-hotel-production-recovery-v1
 */

import assert from "assert";
import { readFileSync, readdirSync } from "fs";
import { join } from "path";
import { resolveCustomerFacingEntity } from "../lib/ai-demand-positioning/customer/customer-entity-resolution-v1.js";
import { computeCompetitiveSet } from "../lib/ai-demand-positioning/intelligence/competitive-set.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import {
  filterCustomerTrendPeriods,
  isCustomerTrendEligible,
  isOfficialProductionPeriod,
} from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { OFFICIAL_BASELINE_PERIOD_MARKER } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const PORTFOLIO = [
  "adp_waterstone_boca_raton",
  "adp_cambridge_beaches_bermuda",
  "adp_renaissance_times_square",
  "adp_now_now_noho",
];

function main() {
  const ui = readFileSync(UI, "utf8");

  // Trend: one-period baseline still uses the chart; no fabricated delta / no chart removal.
  assert.ok(ui.includes("singlePeriod"), "one-period trend branch");
  assert.ok(ui.includes("Awaiting next comparable period") || ui.includes("Awaiting next comparable monitoring period"), "awaiting next period copy");
  assert.ok(ui.includes("showLine"), "single-point chart preserves series without fake lines");
  assert.ok(!ui.includes("chartWrap.hidden = true;\n      return;"), "one-period path must not hide the chart and return");

  // Evidence empty state must be explicit.
  assert.ok(ui.includes("Evidence unavailable for this observation"), "explicit evidence empty state");

  // Entity resolution: Eau / Boca aliases collapse for Waterstone.
  const waterstone = loadPropertyProfile("adp_waterstone_boca_raton");
  const eau1 = resolveCustomerFacingEntity("Eau Palm Beach Resort", waterstone);
  const eau2 = resolveCustomerFacingEntity("Eau Palm Beach Resort & Spa", waterstone);
  assert.equal(eau1.ok, true);
  assert.equal(eau2.ok, true);
  assert.equal(eau1.mergeKey, eau2.mergeKey, "Eau Palm Beach aliases share merge key");
  assert.equal(eau1.displayName, eau2.displayName, "Eau Palm Beach display canonical");

  const boca1 = resolveCustomerFacingEntity("Boca Raton Resort", waterstone);
  const boca2 = resolveCustomerFacingEntity("Boca Raton Resort & Club, A Waldorf Astoria Resort", waterstone);
  const boca3 = resolveCustomerFacingEntity("The Boca Raton", waterstone);
  assert.equal(boca1.mergeKey, boca2.mergeKey);
  assert.equal(boca2.mergeKey, boca3.mergeKey);
  assert.equal(boca1.displayName, "The Boca Raton");

  assert.equal(resolveCustomerFacingEntity("This hotel", waterstone).rejected, true);
  assert.equal(resolveCustomerFacingEntity("Many suites", waterstone).rejected, true);
  assert.equal(resolveCustomerFacingEntity("Located in Tucker's Point, this resort", waterstone).rejected, true);

  const comps = computeCompetitiveSet(
    [
      {
        scenarioId: "s1",
        parsed: true,
        competitorsMentioned: [
          "Eau Palm Beach Resort",
          "Eau Palm Beach Resort & Spa",
          "This hotel",
          "Boca Raton Resort",
        ],
      },
    ],
    waterstone
  );
  const names = comps.observed.map((o) => o.name);
  assert.ok(names.includes("Eau Palm Beach Resort & Spa") || names.includes("Eau Palm Beach Resort"));
  assert.equal(names.filter((n) => /eau palm beach/i.test(n)).length, 1, "single Eau row");
  assert.equal(names.filter((n) => /this hotel/i.test(n)).length, 0, "junk suppressed");

  // Actions: no unsupported numeric impact claims on rebuild.
  const period = loadAllPeriods("adp_waterstone_boca_raton").find((p) =>
    String(p.baselineMarker || "").includes("OFFICIAL_BASELINE")
  );
  assert.ok(period, "official baseline period present");
  const scenarios = buildScenarioUniverse(waterstone);
  const payload = buildOwnerPayload(period, scenarios, waterstone);
  assert.equal(payload.ok, true);
  for (const action of payload.actions || []) {
    const impact = String(action.expectedImpact || "");
    assert.ok(
      !/could improve capture|new demand scenarios captured|reduce displacement in \d+/i.test(impact),
      "no unsupported numeric impact: " + impact
    );
    assert.equal(action.expectedImpact, null);
    assert.ok(
      !/would increase demand capture/i.test(String(action.description || "")),
      "no causal uplift language in action description"
    );
  }

  // Baseline inventory: exactly one active official baseline marker per portfolio property.
  for (const propertyId of PORTFOLIO) {
    const periods = loadAllPeriods(propertyId);
    const official = periods.filter(
      (p) =>
        isOfficialProductionPeriod(p) &&
        (p.baselineMarker === OFFICIAL_BASELINE_PERIOD_MARKER || p.baselinePeriod === true)
    );
    const trendEligible = filterCustomerTrendPeriods(periods);
    assert.ok(trendEligible.length >= 1, propertyId + " has trend-eligible period");
    assert.equal(
      trendEligible.filter((p) => p.baselineMarker === OFFICIAL_BASELINE_PERIOD_MARKER).length,
      1,
      propertyId + " single official baseline in trend set"
    );
    for (const p of periods) {
      if (!isCustomerTrendEligible(p)) continue;
      assert.equal(isOfficialProductionPeriod(p), true, "trend-eligible must be official production");
    }
    // published still Live
    const pub = loadPublishedReport(propertyId);
    assert.ok(pub?.period?.periodId || pub?.ok !== false, propertyId + " published payload loads");
  }

  // Phillips standalone not in portfolio official four.
  const phillipsPeriods = loadAllPeriods("adp_hotel_phillips_kansas_city");
  assert.ok(phillipsPeriods.length >= 1);
  assert.ok(
    !phillipsPeriods.some((p) => p.baselineMarker === OFFICIAL_BASELINE_PERIOD_MARKER),
    "Phillips not portfolio official baseline marker"
  );

  console.log("test:adp-existing-hotel-production-recovery-v1 OK");
}

main();
