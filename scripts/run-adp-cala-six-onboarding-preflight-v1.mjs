#!/usr/bin/env node
/**
 * CALA six-hotel ADP onboarding — zero-cost preflight.
 *
 * Usage:
 *   node scripts/run-adp-cala-six-onboarding-preflight-v1.mjs
 *   node scripts/run-adp-cala-six-onboarding-preflight-v1.mjs --property adp_st_regis_mexico_city
 *
 * Does NOT call providers. Does NOT publish. Does NOT issue share tokens.
 */

import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, listPropertyProfiles, PROVIDERS } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { estimateCost } from "../lib/ai-demand-positioning/execution/multi-provider-runner.js";
import {
  propertyCoreGovernanceReady,
  stabilizedCoreIdsForProperty,
} from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";
import { getEntityRegistryForProperty } from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";
import { CALA_SIX_PROPERTY_IDS } from "../lib/ai-demand-positioning/metrics/cala-six-core-governance.js";
import { TRAVELER_INTENTS } from "../lib/ai-demand-positioning/prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "../lib/ai-demand-positioning/metrics/intent-territory-labels.js";
import { getCensusLinkEntry } from "../lib/ai-demand-positioning/census-link-registry.js";

const COST_CAP_USD = 12;
const PROPERTY_IDS = [...CALA_SIX_PROPERTY_IDS];

const args = process.argv.slice(2);
const onlyIdx = args.indexOf("--property");
const only =
  onlyIdx >= 0 && args[onlyIdx + 1]
    ? args[onlyIdx + 1]
    : args.find((a) => a.startsWith("--property="))?.slice("--property=".length) || null;

function auditProperty(propertyId) {
  const blockers = [];
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    return {
      propertyId,
      PREFLIGHT: "FAIL",
      blockers: ["PROPERTY_PROFILE_MISSING"],
    };
  }

  const scenarios = buildScenarioUniverse(profile);
  const std = scenarios.filter((s) => s.source === "standard").length;
  const spec = scenarios.filter((s) => s.source === "property_specific").length;
  if (!profile?.sourceGovernance?.propertySourceTruthComplete) {
    blockers.push("PROPERTY_SOURCE_TRUTH_INCOMPLETE");
  }
  if (std < 40) blockers.push("STANDARD_SCENARIO_PACK_TOO_SMALL");
  if (spec < 12) blockers.push("PROPERTY_SCENARIOS_TOO_SMALL");
  if (scenarios.length < 60 || scenarios.length > 66) {
    blockers.push(`SCENARIO_TOTAL_OUT_OF_TOLERANCE_${scenarios.length}`);
  }
  if (!propertyCoreGovernanceReady(propertyId)) {
    blockers.push("CORE_GOVERNANCE_NOT_READY");
  }
  if (!getEntityRegistryForProperty(propertyId)) {
    blockers.push("ENTITY_REGISTRY_MISSING");
  }
  if (profile.customerDropdownVisible === true) {
    blockers.push("DROPDOWN_VISIBLE_BEFORE_CERTIFICATION");
  }

  const census = getCensusLinkEntry(propertyId);
  if (!census?.censusRecordId) blockers.push("CENSUS_LINK_MISSING");

  const cost = estimateCost(scenarios.length || 0);
  const calls = scenarios.length * PROVIDERS.length;
  const roundedTotal = Math.round(cost.total * 100) / 100;
  if (roundedTotal > COST_CAP_USD) blockers.push("COST_CAP_EXCEEDED");

  const visibleLeak = listPropertyProfiles().some((p) => p.propertyId === propertyId);
  if (visibleLeak) blockers.push("DROPDOWN_LIST_LEAK_BEFORE_CERTIFICATION");

  const coreRows = Object.values(TRAVELER_INTENTS).map((intent) => {
    const core = stabilizedCoreIdsForProperty(propertyId, intent);
    return {
      TERRITORY: territoryLabelForIntent(intent),
      intent,
      CORE_COUNT: core.length,
      CORE_HOTELS: core,
      CERTIFICATION_READY: core.length >= 4,
      BLOCKER:
        core.length >= 4
          ? null
          : core.length
            ? "BELOW_MIN_CORE_4_FOR_NUMERIC_BENCHMARK"
            : "NO_CORE_PEERS",
    };
  });

  const byIntent = {};
  for (const s of scenarios) byIntent[s.intent] = (byIntent[s.intent] || 0) + 1;

  return {
    propertyId,
    CANONICAL_NAME: profile.name,
    ROOMS: profile.rooms ?? null,
    ROOM_COUNT_CONFIDENCE: profile.roomCountConfidence || null,
    MANAGEMENT_COMPANY: profile.managementCompany || profile.sourceGovernance?.managementCompany || null,
    OFFICIAL_PROPERTY_PAGE: profile.officialPropertyPageUrl || null,
    CENSUS_RECORD_ID: census?.censusRecordId || null,
    PROPERTY_SOURCE_TRUTH_COMPLETE: profile.sourceGovernance?.propertySourceTruthComplete ? "YES" : "NO",
    STANDARD_SCENARIOS: std,
    PROPERTY_SPECIFIC_SCENARIOS: spec,
    TOTAL_SCENARIOS: scenarios.length,
    SCENARIOS_BY_INTENT: byIntent,
    TOTAL_PLANNED_CALLS: calls,
    ESTIMATED_COST_BY_PROVIDER: cost.byProvider,
    TOTAL_ESTIMATED_COST: roundedTotal,
    COST_CAP_USD,
    CORE: coreRows,
    blockers,
    PREFLIGHT: blockers.length === 0 ? "PASS" : "FAIL",
  };
}

const targets = only ? [only] : PROPERTY_IDS;
const rows = targets.map(auditProperty);
const allPass = rows.every((r) => r.PREFLIGHT === "PASS");
const totalCost = Math.round(rows.reduce((n, r) => n + (r.TOTAL_ESTIMATED_COST || 0), 0) * 100) / 100;

const out = {
  gate: "ADP_CALA_SIX_ONBOARDING_PREFLIGHT_V1",
  baseCheckpoint: "98a4e32",
  PREFLIGHT: allPass ? "PASS" : "FAIL",
  PROPERTY_COUNT: rows.length,
  TOTAL_ESTIMATED_COST_COHORT: totalCost,
  COST_NOTE: "Estimate only — founder approval required before --apply provider runs (~$8–12/hotel)",
  NEXT: allPass
    ? "AWAITING_FOUNDER_COST_APPROVAL_THEN_DRY_RUN"
    : "FIX_BLOCKERS_BEFORE_MEASUREMENT",
  rows,
};

const outDir = join(process.cwd(), "reports/ai-demand-positioning");
mkdirSync(outDir, { recursive: true });
const outPath = join(outDir, "adp-cala-six-onboarding-preflight-v1.json");
writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");

console.log(JSON.stringify(out, null, 2));
console.log(`\nWrote ${outPath}`);
process.exit(allPass ? 0 : 2);
