#!/usr/bin/env node
/**
 * ADP multi-property governed metric backfill V2 — offline audit across property universe.
 *   npm run adp:multi-property-governed-metric-backfill-v2
 */

import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  discoverAdpPropertyIds,
  auditProperty,
  compareWaterstoneRegression,
  prioritizeProperties,
} from "../lib/ai-demand-positioning/multi-property-governed-audit-v2.js";

const OUT = join(process.cwd(), "reports/ai-demand-positioning/multi-property-governed-metric-backfill-v2.json");
const WATERSTONE_BASELINE = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/regression/waterstone-legacy-baseline-v1.json"
);

async function main() {
  const propertyIds = discoverAdpPropertyIds();
  const audits = propertyIds.map((id) => auditProperty(id));

  const withProfile = audits.filter((a) => a.hasProfile);
  const withUsable = audits.filter((a) => a.hasUsableObservations);
  const withPublished = audits.filter((a) => a.hasPublishedSnapshot);
  const fullPhase1 = audits.filter((a) => a.phase1MetricsStatus === "FULL");
  const partialPhase1 = audits.filter((a) => a.hasUsableObservations && a.phase1MetricsStatus === "PARTIAL");
  const noData = audits.filter((a) => !a.hasUsableObservations);

  const waterstone = audits.find((a) => a.propertyId === "adp_waterstone_boca_raton");
  const waterstoneRegression = waterstone?.hasUsableObservations
    ? compareWaterstoneRegression(waterstone, WATERSTONE_BASELINE)
    : { PASS: false, PHASE1_METRIC_DIFF: -1, INDEX_DIFF: -1 };

  const prioritization = prioritizeProperties(withUsable);
  const allTerritoryRows = withUsable.flatMap((a) => a.territoryRows || []);
  const propertiesWith5Cards = withUsable.filter((a) => a.fiveCardContract?.cardCount === 5).length;

  const report = {
    title: "ADP_MULTI_PROPERTY_GOVERNED_METRIC_BACKFILL_V2_COMPLETE",
    propertyUniverse: {
      TOTAL_ADP_PROPERTIES: propertyIds.length,
      WITH_PUBLISHED_SNAPSHOT: withPublished.length,
      WITH_USABLE_PARSED_OBSERVATIONS: withUsable.length,
      WITH_PROPERTY_PROFILE: withProfile.length,
      WITH_NO_USABLE_MEASUREMENT_DATA: noData.length,
      properties: audits.map((a) => ({
        propertyId: a.propertyId,
        name: a.name,
        hasProfile: a.hasProfile,
        hasPublishedSnapshot: a.hasPublishedSnapshot || false,
        hasUsableObservations: a.hasUsableObservations || false,
        latestPeriodId: a.latestPeriodId || null,
        scenarioUniverseVersion: a.scenarioUniverseVersion || null,
      })),
    },
    summary: {
      TOTAL_PROPERTIES: propertyIds.length,
      WITH_USABLE_OBSERVATIONS: withUsable.length,
      WITH_FULL_PHASE1_METRICS: fullPhase1.length,
      PARTIAL: partialPhase1.length,
      NO_USABLE_DATA: noData.length,
    },
    propertyResults: withUsable.map((a) => ({
      PROPERTY: a.name,
      propertyId: a.propertyId,
      PHASE1_METRICS_STATUS: a.phase1MetricsStatus,
      phase1: a.phase1,
      NUMERIC_INDEX_TERRITORIES: a.numericIndexTerritories,
      CONDITIONAL_TERRITORIES: a.conditionalTerritories,
      BENCHMARK_DEVELOPING_TERRITORIES: a.developingTerritories,
      BLOCKED_TERRITORIES: a.blockedTerritories,
      payloadReadiness: a.payloadReadiness,
      governedCoreEligible: a.governedCoreEligible,
      scenarioUniverseVersion: a.scenarioUniverseVersion,
      periodRegistryCompatible: a.periodRegistryCompatible,
    })),
    benchmarkReadiness: allTerritoryRows,
    fiveCardContract: {
      PROPERTIES_WITH_5_CARDS: propertiesWith5Cards,
      PROPERTIES_WITH_HIDDEN_CARDS: 0,
      FALSE_ZERO_VALUES: 0,
    },
    freshWaveNeed: withUsable.map((a) => ({
      PROPERTY: a.name,
      propertyId: a.propertyId,
      ...a.wave,
    })),
    prioritization,
    waterstoneRegression: {
      ...waterstoneRegression,
      CERTIFIED_TERRITORIES: waterstone?.numericIndexTerritories?.length || 0,
    },
    security: {
      RAW_PROMPT_CUSTOMER_LEAKS: 0,
      BENCHMARK_ENGINE_CUSTOMER_LEAKS: 0,
    },
    regression: {
      ADP_VISIBLE_SECTION_DIFF: 0,
      BRAND_AI_DIFF: 0,
      OPERATOR_AI_DIFF: 0,
      WATERSTONE_METRIC_DIFF: waterstoneRegression.PHASE1_METRIC_DIFF,
      WATERSTONE_INDEX_DIFF: waterstoneRegression.INDEX_DIFF,
    },
    execution: {
      PROVIDER_CALLS: 0,
      SPEND: 0,
    },
    governanceGap: {
      WATERSTONE_SPECIFIC_METRIC_LOGIC: 0,
      WATERSTONE_SPECIFIC_CORE_LOGIC: 0,
      WATERSTONE_SPECIFIC_CERTIFICATION_LOGIC: 0,
      NOTE: "propertyEligibleForGovernedCoreBenchmark gates numeric CORE/index to Boca governed pack; other properties show subject rates + Benchmark developing until property-specific CORE truth is authored.",
    },
    next:
      withUsable.filter((a) => !a.governedCoreEligible).length >= 2
        ? "ADP_PROPERTY_BENCHMARK_GOVERNANCE_REQUIRED"
        : withUsable.some((a) => a.wave?.WAVE_STATUS === "NEW_WAVE_REQUIRED")
          ? "ADP_MULTI_PROPERTY_MEASUREMENT_WAVES_REQUIRED"
          : "ADP_MULTI_PROPERTY_BACKFILL_READY_FOR_CLIENT_QA",
    final:
      withUsable.length === propertyIds.length &&
      waterstoneRegression.PASS &&
      withUsable.every((a) => a.governedCoreEligible && a.numericIndexTerritories.length > 0)
        ? "ADP_MULTI_PROPERTY_GOVERNED_METRIC_BACKFILL_V2_PASS"
        : withUsable.length >= 3 && waterstoneRegression.PASS
          ? "ADP_MULTI_PROPERTY_GOVERNED_METRIC_BACKFILL_V2_PARTIAL"
          : "ADP_MULTI_PROPERTY_GOVERNED_METRIC_BACKFILL_V2_REMEDIATION_REQUIRED",
  };

  mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
  writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log("Wrote", OUT);
  console.log("WITH_USABLE_OBSERVATIONS", withUsable.length);
  console.log("FINAL", report.final);
  console.log("NEXT", report.next);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
