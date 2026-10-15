#!/usr/bin/env node
/**
 * Weekly GDI refresh within the research-to-page coordinator family.
 *
 *   node scripts/gdi-weekly-refresh-coordinator-v1.mjs --hotel=rec0eTEnCqlsGJJt9
 *   node scripts/gdi-weekly-refresh-coordinator-v1.mjs --hotel=recnPB3Q02NO8Ma1o --cycle=2
 *   node scripts/gdi-weekly-refresh-coordinator-v1.mjs --hotels=mayflower,san_diego --simulate-weeks=1
 *
 * Auto-publish OFF. Paid spend not authorized.
 * Does not replace gdi-weekly-refresh.mjs (legacy Bethesda delta) or future-watch scheduler.
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  runWeeklyRefreshCycle,
  buildWeeklyWorkQueues,
} from "../lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js";
import { RUN_TRIGGER } from "../lib/group-demand-intelligence/workflow/weekly-refresh-contract-v1.js";
import { loadWeeklyRefreshState } from "../lib/group-demand-intelligence/workflow/weekly-refresh-state-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-weekly-refresh");

const HOTEL_ALIASES = {
  mayflower: "rec0eTEnCqlsGJJt9",
  may: "rec0eTEnCqlsGJJt9",
  san_diego: "recnPB3Q02NO8Ma1o",
  sd: "recnPB3Q02NO8Ma1o",
};

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(`--${name}=`.length) : fallback;
}

function resolveHotels() {
  const multi = arg("hotels");
  const single = arg("hotel");
  const raw = multi || single || "";
  if (!raw) {
    console.error("Required: --hotel=<id> or --hotels=mayflower,san_diego");
    process.exit(2);
  }
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => HOTEL_ALIASES[s.toLowerCase()] || s);
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const hotels = resolveHotels();
  const simulateWeeks = Number(arg("simulate-weeks", "0") || 0);
  const cycle = Number(arg("cycle", "1") || 1);
  process.env.GDI_WEEKLY_REFRESH_PERSIST_STATE_ON_DRY_RUN = "1";

  // Isolated data root for deterministic proof when requested
  const isolated = arg("data-root");
  if (isolated) {
    process.env.GDI_DATA_ROOT = path.resolve(ROOT, isolated);
    fs.mkdirSync(process.env.GDI_DATA_ROOT, { recursive: true });
  }

  const results = [];
  for (const hotelId of hotels) {
    let opps = [];
    try {
      opps = await listOpportunitiesForHotel(hotelId);
    } catch (err) {
      console.warn(hotelId, "opp_load_failed", err?.message || err);
    }

    const now =
      simulateWeeks > 0
        ? new Date(Date.now() + simulateWeeks * 7 * 86400000)
        : new Date();

    const queuesPreview = buildWeeklyWorkQueues({
      hotelId,
      opportunities: opps,
      now,
    });

    const cycle1 = await runWeeklyRefreshCycle({
      hotelId,
      opportunities: opps,
      trigger: simulateWeeks > 0 ? RUN_TRIGGER.SIMULATED : RUN_TRIGGER.MANUAL,
      now: cycle === 1 ? now : new Date(),
      dryRun: true,
      clockLabel:
        simulateWeeks > 0 ? `simulated_plus_${simulateWeeks}_weeks` : "live_now",
    });

    // Second cycle immediately (dedupe proof) — same clock unless simulate
    const cycle2Now =
      simulateWeeks > 0
        ? now
        : new Date(Date.now() + 1000);
    const cycle2 = await runWeeklyRefreshCycle({
      hotelId,
      opportunities: opps,
      candidates: opps.map((o) => ({ ...o })), // unchanged → NO_MATERIAL_CHANGE
      trigger: RUN_TRIGGER.RETRY,
      now: cycle2Now,
      dryRun: true,
      clockLabel: "second_cycle_resume",
    });

    const state = loadWeeklyRefreshState(hotelId);

    const hotelOut = {
      hotelId,
      queuesPreview: {
        runMode: queuesPreview.runMode,
        counts: queuesPreview.counts,
        coverageBacklog: queuesPreview.coverageBacklog.map((t) => t.segmentId),
        deferredCount: queuesPreview.deferred.length,
      },
      cycle1: {
        runId: cycle1.runId,
        status: cycle1.status,
        runMode: cycle1.runMode,
        summary: cycle1.summary,
        changeSummary: cycle1.changes?.summary,
      },
      cycle2: {
        runId: cycle2.runId,
        status: cycle2.status,
        runMode: cycle2.runMode,
        summary: cycle2.summary,
        changeSummary: cycle2.changes?.summary,
        note: "Second cycle should not create NEW_PROSPECT from unchanged evidence",
      },
      stateAfter: {
        lastSuccessfulRunAt: state.lastSuccessfulRunAt,
        nextEligibleAt: state.nextEligibleAt,
        pendingGaps: (state.pendingGaps || []).length,
        pendingRetries: (state.pendingRetries || []).length,
        killSwitch: state.killSwitch,
        enabled: state.enabled,
      },
    };

    const p = path.join(OUT, `two-cycle-${hotelId}.json`);
    fs.writeFileSync(p, JSON.stringify(hotelOut, null, 2) + "\n");
    results.push(hotelOut);
    console.log(
      JSON.stringify(
        {
          hotelId,
          cycle1New: cycle1.changes?.summary?.newProspect,
          cycle1Unchanged: cycle1.changes?.summary?.noMaterialChange,
          cycle2New: cycle2.changes?.summary?.newProspect,
          cycle2Unchanged: cycle2.changes?.summary?.noMaterialChange,
          coverageGaps: cycle1.summary?.segmentCoverageGaps,
          path: p,
        },
        null,
        2
      )
    );
  }

  fs.writeFileSync(
    path.join(OUT, "two-cycle-rollup.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        autoPublish: false,
        paidSpendUsd: 0,
        results,
      },
      null,
      2
    ) + "\n"
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
