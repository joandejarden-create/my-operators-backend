#!/usr/bin/env node
/**
 * Generate / upsert CALA Radar Build Plans from country configs + live counts.
 *
 *   node scripts/generate-cala-radar-build-plans.mjs
 *   node scripts/generate-cala-radar-build-plans.mjs --apply
 *   node scripts/generate-cala-radar-build-plans.mjs --country "Dominican Republic"
 *   node scripts/generate-cala-radar-build-plans.mjs --tier "Tier 1"
 *   node scripts/generate-cala-radar-build-plans.mjs --strategy CORRIDOR_BASED
 *   node scripts/generate-cala-radar-build-plans.mjs --apply --force --verbose
 */
import "../load-env.js";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { listCountryConfigs, getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import { fetchLiveCountsByCountry } from "../lib/radar-buildout/live-country-counts.js";
import { buildCountryPlanPayload } from "../lib/radar-buildout/build-plan-generator.js";
import {
  fetchRadarBuildPlans,
  buildRadarBuildPlanAirtableFields,
  mergeBuildPlanWithExisting,
} from "../lib/radar-buildout/airtable-radar-build-plans-io.js";
import {
  getRadarBuildPlansAirtableConfig,
  resolveRadarBuildPlansTableName,
} from "../lib/radar-buildout/radar-build-plans-base.js";
import { RADAR_BUILD_PLANS_FIELDS as F } from "../lib/radar-buildout/airtable-radar-build-plans-fields.js";
import { filterFieldsToAirtableSchema } from "../lib/third-party-operator-basics-airtable-column-aliases.js";
import { fetchAirtableTableFieldNameSet } from "../lib/third-party-operator-basics-airtable-column-aliases.js";

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const VERBOSE = process.argv.includes("--verbose");

function parseArg(flag) {
  const idx = process.argv.indexOf(flag);
  return idx >= 0 ? process.argv[idx + 1] : null;
}

const COUNTRY_FILTER = parseArg("--country");
const TIER_FILTER = parseArg("--tier");
const STRATEGY_FILTER = parseArg("--strategy");

const STRATEGY_MAP = {
  ISLAND_COUNTRYWIDE: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  CORRIDOR_BASED: BUILD_STRATEGY_TYPES.CORRIDOR_BASED,
  MARKET_BY_MARKET: BUILD_STRATEGY_TYPES.MARKET_BY_MARKET,
};

const AIRTABLE_DELAY = Number(process.env.AIRTABLE_WRITE_DELAY_MS) || 220;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function selectConfigs() {
  if (COUNTRY_FILTER) {
    const c = getCountryConfig(COUNTRY_FILTER);
    if (!c) throw new Error(`Unknown country config: ${COUNTRY_FILTER}`);
    return [{ country: COUNTRY_FILTER, ...c }];
  }
  const strategyLabel = STRATEGY_FILTER ? STRATEGY_MAP[STRATEGY_FILTER] || STRATEGY_FILTER : null;
  return listCountryConfigs({ tier: TIER_FILTER || undefined, strategy: strategyLabel || undefined });
}

async function main() {
  console.log(APPLY ? "=== APPLY ===" : "=== DRY RUN ===");
  const configs = selectConfigs();
  console.log(`Countries to process: ${configs.length}`);

  const liveByCountry = await fetchLiveCountsByCountry();
  const existingResult = await fetchRadarBuildPlans();
  const existingByCountry = {};
  if (!existingResult.error && existingResult.plans) {
    for (const p of existingResult.plans) existingByCountry[p.country] = p;
  } else if (existingResult.error) {
    console.warn("WARN: Could not load stored build plans:", existingResult.error);
    if (existingResult.error === "radar_build_plans_table_missing") {
      console.warn("  Run: npm run ensure:radar-build-plans-schema:apply");
    }
  }

  const generated = [];
  for (const entry of configs) {
    const { country, ...config } = entry;
    const live = liveByCountry[country] || {
      demandAnchors: [],
      travelInfrastructure: [],
      summary: {
        demandAnchors: 0,
        travelInfrastructure: 0,
        totalRadarPoints: 0,
        sourceCoveragePct: 0,
        coordinateCoveragePct: 0,
        dataConfidenceMix: {},
        submarkets: {},
      },
    };
    const payload = buildCountryPlanPayload(country, config, live);
    const merged = mergeBuildPlanWithExisting(payload, {
      force: FORCE,
      existingRecord: existingByCountry[country],
    });
    generated.push(merged);

    console.log(
      `\n${country} | ${merged.buildStatus} | DA ${merged.current.demandAnchors}/${merged.targets.demandAnchors} | TI ${merged.current.travelInfrastructure}/${merged.targets.travelInfrastructure} | Total ${merged.current.totalRadarPoints}/${merged.targets.totalRadarPoints}`
    );
    if (VERBOSE) console.log(" ", merged.evaluationReason, "→", merged.nextRecommendedAction);
  }

  if (!APPLY) {
    console.log("\nDry run complete. Re-run with --apply to upsert Airtable build plans.");
    return;
  }

  const cfg = getRadarBuildPlansAirtableConfig();
  if (!cfg) throw new Error("Airtable config missing");
  const tableName = await resolveRadarBuildPlansTableName(cfg.baseId, cfg.apiKey);
  const schema = await fetchAirtableTableFieldNameSet(cfg.baseId, cfg.apiKey, tableName);

  let created = 0;
  let updated = 0;
  let failed = 0;

  for (const payload of generated) {
    const fields = filterFieldsToAirtableSchema(buildRadarBuildPlanAirtableFields(payload), schema);
    const existing = existingByCountry[payload.country];
    try {
      if (existing?.id) {
        const updateFields = { ...fields };
        if (!FORCE && existing.notes) delete updateFields[F.notes];
        await cfg.base(tableName).update(existing.id, updateFields, { typecast: true });
        updated += 1;
        if (VERBOSE) console.log("UPDATED", payload.country);
      } else {
        await cfg.base(tableName).create(fields, { typecast: true });
        created += 1;
        if (VERBOSE) console.log("CREATED", payload.country);
      }
    } catch (err) {
      failed += 1;
      console.error("FAIL", payload.country, err?.message || err);
    }
    await sleep(AIRTABLE_DELAY);
  }

  console.log(`\nApply complete: created=${created} updated=${updated} failed=${failed}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
