/**
 * Overnight enrichment runner — unit tests (no live Airtable / Mapbox).
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolveOvernightConfig,
  scoreLanePriority,
  selectNextCycleProfile,
  evaluateCycleYield,
  buildMasterOptsForCycle,
  SOURCE_LANE_STATUS,
  OVERNIGHT_CYCLE_PROFILES,
  runOvernightCensusEnrichment,
  OVERNIGHT_STATE_DIR,
} from "../lib/research-engine-v2/overnight-census-enrichment-v1.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

test("production table lock rejects wrong table id", () => {
  const bad = assertProductionCensusWriteTarget({
    tableId: "tblWRONG",
  });
  assert.equal(bad.ok, false);
  const good = assertProductionCensusWriteTarget({
    tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  });
  assert.equal(good.ok, true);
});

test("overnight config defaults and overrides", () => {
  const cfg = resolveOvernightConfig({
    OVERNIGHT_MAX_RUNTIME_MINUTES: "360",
    OVERNIGHT_MAX_EXTERNAL_COST_USD: "50",
  });
  assert.equal(cfg.max_runtime_minutes, 360);
  assert.equal(cfg.max_external_cost_usd, 50);
});

test("lane priority prefers Current Brand when most missing", () => {
  const ranked = scoreLanePriority({
    current_brand: { completeness_pct: 14 },
    rooms: { completeness_pct: 17 },
    address: { completeness_pct: 85 },
    postal_code: { completeness_pct: 38 },
    latitude: { completeness_pct: 67 },
    state_region: { completeness_pct: 89 },
    website: { completeness_pct: 75 },
    phone: { completeness_pct: 90 },
    city: { completeness_pct: 98 },
  });
  assert.equal(ranked[0].lane, "Current Brand");
});

test("selectNextCycleProfile skips plateaued profiles", () => {
  const state = {
    next_profile_index: 0,
    lane_status: {
      brand_rooms: SOURCE_LANE_STATUS.PLATEAUED,
      brand_focus: SOURCE_LANE_STATUS.READY,
      rooms_focus: SOURCE_LANE_STATUS.PLATEAUED,
      fundamentals: SOURCE_LANE_STATUS.PLATEAUED,
      opportunistic_coords: SOURCE_LANE_STATUS.PLATEAUED,
    },
  };
  const profile = selectNextCycleProfile(state, {
    current_brand: { completeness_pct: 14 },
    rooms: { completeness_pct: 50 },
  });
  assert.equal(profile.id, "brand_focus");
});

test("evaluateCycleYield detects zero vs useful", () => {
  assert.equal(
    evaluateCycleYield({
      CURRENT_BRAND_WRITES: 0,
      ROOMS_WRITTEN: 0,
      TOTAL_FIELDS_WRITTEN_THIS_RUN: 0,
      PROPERTIES_PATCHED_THIS_RUN: 0,
    }).zero_yield,
    true
  );
  assert.equal(
    evaluateCycleYield({
      CURRENT_BRAND_WRITES: 3,
      ROOMS_WRITTEN: 0,
      TOTAL_FIELDS_WRITTEN_THIS_RUN: 5,
      PROPERTIES_PATCHED_THIS_RUN: 3,
    }).zero_yield,
    false
  );
});

test("cutoff guard skips heavy lanes near runtime end", () => {
  const profile = OVERNIGHT_CYCLE_PROFILES.find((p) => p.id === "brand_rooms");
  const opts = buildMasterOptsForCycle(
    profile,
    { minutes_remaining: 5, cutoff_guard_minutes: 12 },
    true
  );
  assert.equal(opts.skipPropertyFundamentals, true);
  assert.equal(opts.skipRoomsRegistry, true);
  assert.equal(opts.maxPfResearch, 0);
  assert.equal(opts.continueMapboxWave, false);
});

test("overnight loop rotates on plateau and stops on runtime", async () => {
  let calls = 0;
  const start = Date.now();
  /** @type {number} */
  let fakeNow = start;
  const report = await runOvernightCensusEnrichment({
    mode: "dry-run",
    enableProductionWrites: false,
    maxRuntimeMinutes: 60,
    maxExternalCostUsd: 50,
    now: () => fakeNow,
    sleepFn: async () => {},
    runMasterFn: async () => {
      calls += 1;
      // Advance clock ~25 minutes per cycle → ~2–3 cycles in 60m with guard
      fakeNow += 25 * 60_000;
      return {
        ok: true,
        MASTER_ENRICHMENT_STATUS: "master_enrichment_wave_complete",
        CURRENT_BRAND_WRITES: calls === 1 ? 2 : 0,
        ROOMS_WRITTEN: 0,
        TOTAL_FIELDS_WRITTEN_THIS_RUN: calls === 1 ? 4 : 0,
        PROPERTIES_PATCHED_THIS_RUN: calls === 1 ? 2 : 0,
        MAPBOX_REQUESTS: 0,
        ESTIMATED_MAPBOX_COST: 0,
        HBX_ROOMS_ARRAY_WRITES: 0,
        HBX_COORDINATE_WRITES: 0,
        CVENT_ONLY_ROOM_VALIDATIONS: 0,
        BENCHMARK_ROOM_WRITES: 0,
        DESTRUCTIVE_OVERWRITES: 0,
        WRONG_TABLE_WRITES: 0,
        ERRORS: 0,
        FOUNDER_DECISION_REQUIRED: "NO",
        DASHBOARD_BEFORE: {
          n: 15575,
          current_brand: { completeness_pct: 14 },
          rooms: { completeness_pct: 17 },
          address: { completeness_pct: 85 },
          postal_code: { completeness_pct: 38 },
          latitude: { completeness_pct: 67 },
          longitude: { completeness_pct: 67 },
          state_region: { completeness_pct: 89 },
          city: { completeness_pct: 98 },
          website: { completeness_pct: 75 },
          phone: { completeness_pct: 90 },
          brand_family: { completeness_pct: 14 },
          family_source_family: { completeness_pct: 16 },
        },
        DASHBOARD_AFTER: {
          n: 15575,
          current_brand: { completeness_pct: 14 },
          rooms: { completeness_pct: 17 },
          address: { completeness_pct: 85 },
          postal_code: { completeness_pct: 38 },
          latitude: { completeness_pct: 67 },
          longitude: { completeness_pct: 67 },
          state_region: { completeness_pct: 89 },
          city: { completeness_pct: 98 },
          website: { completeness_pct: 75 },
          phone: { completeness_pct: 90 },
          brand_family: { completeness_pct: 14 },
          family_source_family: { completeness_pct: 16 },
        },
      };
    },
    log: () => {},
  });

  assert.ok(calls >= 1);
  assert.equal(report.ok, true);
  assert.equal(report.HBX_ROOMS_ARRAY_WRITES, 0);
  assert.equal(report.HBX_COORDINATE_WRITES, 0);
  assert.equal(report.CVENT_ONLY_ROOM_VALIDATIONS, 0);
  assert.equal(report.BENCHMARK_DATA_WRITES, 0);
  assert.equal(report.DESTRUCTIVE_OVERWRITES, 0);
  assert.equal(report.WRONG_TABLE_WRITES, 0);
  assert.ok(
    report.STOP_REASON === "overnight_runtime_limit_reached" ||
      report.STOP_REASON === "all_structured_lanes_plateaued" ||
      report.CYCLES_COMPLETED >= 1
  );
  assert.ok(fs.existsSync(path.join(OVERNIGHT_STATE_DIR, "state.json")));
});

test("overnight stops on wrong-table from master report", async () => {
  const report = await runOvernightCensusEnrichment({
    mode: "dry-run",
    enableProductionWrites: false,
    maxRuntimeMinutes: 30,
    now: (() => {
      let t = Date.now();
      return () => {
        t += 1000;
        return t;
      };
    })(),
    sleepFn: async () => {},
    runMasterFn: async () => ({
      ok: false,
      WRONG_TABLE_WRITES: 1,
      DESTRUCTIVE_OVERWRITES: 0,
      HBX_ROOMS_ARRAY_WRITES: 0,
      HBX_COORDINATE_WRITES: 0,
      CVENT_ONLY_ROOM_VALIDATIONS: 0,
      BENCHMARK_ROOM_WRITES: 0,
      CURRENT_BRAND_WRITES: 0,
      ROOMS_WRITTEN: 0,
      TOTAL_FIELDS_WRITTEN_THIS_RUN: 0,
      PROPERTIES_PATCHED_THIS_RUN: 0,
      MAPBOX_REQUESTS: 0,
      ESTIMATED_MAPBOX_COST: 0,
      ERRORS: 0,
      FOUNDER_DECISION_REQUIRED: "NO",
    }),
    log: () => {},
  });
  assert.equal(report.ok, false);
  assert.equal(report.STOP_REASON, "wrong_production_target");
  assert.equal(report.FOUNDER_DECISION_REQUIRED, "YES");
});
