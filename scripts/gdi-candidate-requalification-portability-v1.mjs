#!/usr/bin/env node
/**
 * GDI Candidate / Watch Requalification + Cross-Hotel Portability V1
 *
 * Usage:
 *   node scripts/gdi-candidate-requalification-portability-v1.mjs --mode snapshot
 *   node scripts/gdi-candidate-requalification-portability-v1.mjs --mode dry-run
 *   node scripts/gdi-candidate-requalification-portability-v1.mjs --mode apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listOpportunitiesForHotel, upsertOpportunity } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { buildHotelGeographyProfile } from "../lib/group-demand-intelligence/market-opportunity-graph/hotel-geography-profile-v1.js";
import {
  buildHiEnrichedHotelProfile,
  proposeCapabilityConfigPatch,
} from "../lib/group-demand-intelligence/requalification/hi-enriched-hotel-profile-v1.js";
import {
  requalifyHotelCorpus,
  summarizeHotelCorpus,
} from "../lib/group-demand-intelligence/requalification/requalify-hotel-corpus-v1.js";
import {
  runRenaissanceToHiltonPortability,
  runReverseNycPortability,
  PORTABILITY_CLASS,
} from "../lib/group-demand-intelligence/requalification/portability-eval-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isCustomerFacingOpportunity } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "candidate-requalification-portability-v1"
);

const ALL_HOTELS = [
  "recLuxvwwxID7U2B8",
  "recgMYovrrZDJMqzX",
  "recG66DQJKP2c0UNh",
  "recGkME49yYuxQl0u",
  "rec8hHupaSwiWI3r7",
  "recIwaP1etgx2g9nA",
  "recsn3BUKJ9PNfeZW",
  "recRXmrakhSAuctwz",
  "recN76iEE6yAaPh8H",
  "recESHsNsWUFYZrxR",
  "recCEpdskZeUBvQwG",
  "recD17Kxn6BcJjGFh",
  "recUOyzOXn2Zdp98I",
  "recjDsNzu93CFfe87",
  "rec9Tp0WBb2uk6w3u",
  "rece0or38cxo3Fymb",
  "rec35fExUxCClpOP6",
  "rec2PVBDavppGpenm",
  "recKRJjcPnb4tVDDS",
];

const PRIORITY_COHORT = [
  "recUOyzOXn2Zdp98I",
  "recCEpdskZeUBvQwG",
  "recjDsNzu93CFfe87",
];

const REN = "recG66DQJKP2c0UNh";
const HILTON = "rec35fExUxCClpOP6";
const NOW_NOW = "recGkME49yYuxQl0u";

function parseArgs(argv) {
  const out = { mode: "dry-run", skipPortability: false };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--mode") out.mode = argv[++i];
    if (argv[i] === "--skip-portability") out.skipPortability = true;
  }
  return out;
}

function writeJson(name, data) {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const p = path.join(OUT_DIR, name);
  fs.writeFileSync(p, JSON.stringify(data, null, 2) + "\n", "utf8");
  return p;
}

async function loadHotelBundle(hotelId) {
  const opps = await listOpportunitiesForHotel(hotelId);
  const beforeHi = buildHotelGeographyProfile(hotelId);
  const afterHi = await buildHiEnrichedHotelProfile(hotelId);
  return { hotelId, opps, beforeHi, afterHi };
}

function syncCapabilityConfig(hotelId, hiOverlay, mode) {
  const cfgPath = path.join(
    ROOT,
    "config",
    "group-demand-intelligence",
    "hotels",
    `${hotelId}.json`
  );
  if (!fs.existsSync(cfgPath)) return { skipped: true, reason: "no_config" };
  const cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
  const { capabilityProfile, changes } = proposeCapabilityConfigPatch(
    cfg.capabilityProfile || {},
    hiOverlay || {}
  );
  if (!changes.length) return { skipped: true, reason: "no_changes", changes: [] };
  if (mode === "apply") {
    cfg.capabilityProfile = capabilityProfile;
    cfg.capabilityProfileSource = {
      ...(cfg.capabilityProfileSource || {}),
      hiRequalV1At: new Date().toISOString(),
      note: "Filled null/zero capability fields from Hotel Intelligence Airtable",
    };
    fs.writeFileSync(cfgPath, JSON.stringify(cfg, null, 2) + "\n", "utf8");
  }
  return { skipped: false, changes, applied: mode === "apply" };
}

async function applyFitUpdates(hotelId, evaluations, mode, fullOpps = []) {
  if (mode !== "apply") return { updated: 0, created: 0 };
  const byId = new Map(
    (fullOpps || []).map((o) => [String(o.id || o.opportunityId), o])
  );
  let updated = 0;
  for (const ev of evaluations) {
    if (!ev.hiRecovered && !(ev.fitDelta != null && Math.abs(ev.fitDelta) >= 5)) continue;
    if (ev.newFit == null) continue;
    const oppId = String(ev.snapshot?.id || "");
    if (!oppId) continue;
    const full = byId.get(oppId);
    if (!full) continue;
    const next = {
      ...full,
      id: oppId,
      opportunityId: oppId,
      hotelId,
      hotelFitScore: ev.newFit,
      fitExplanation: (ev.reasons || []).slice(0, 6).join("; ") || full.fitExplanation,
      notes: [
        full.notes,
        `hi_requal_portability_v1 fit ${ev.oldFit ?? "null"}→${ev.newFit}`,
        ev.hiFactThatChanged ? `hi:${ev.hiFactThatChanged}` : null,
        `blocker ${ev.oldBlocker}→${ev.newBlocker}`,
      ]
        .filter(Boolean)
        .join(" | "),
      hiRequalV1: {
        at: new Date().toISOString(),
        oldFit: ev.oldFit,
        newFit: ev.newFit,
        oldBlocker: ev.oldBlocker,
        newBlocker: ev.newBlocker,
        hiFactThatChanged: ev.hiFactThatChanged,
      },
    };
    try {
      await upsertOpportunity(next, { source: "hi_requal_portability_v1" });
      updated += 1;
    } catch (err) {
      console.error("upsert_fit_failed", oppId, err.message || err);
    }
  }
  return { updated, created: 0 };
}

async function applyPortabilityCreates(portabilityResults, hiltonExisting, mode) {
  if (mode !== "apply") return { created: 0, skippedExisting: 0, readyCreated: 0 };
  const existingMarketIds = new Set(
    (hiltonExisting || [])
      .map((o) => o.marketOpportunityId)
      .filter(Boolean)
  );
  const existingTitles = new Set(
    (hiltonExisting || []).map((o) => String(o.title || "").toLowerCase().trim())
  );
  let created = 0;
  let skippedExisting = 0;
  let readyCreated = 0;

  for (const r of portabilityResults || []) {
    if (!r.built?.ok || !r.built?.opportunityId) continue;
    if (
      r.portability?.portabilityClass !== PORTABILITY_CLASS.MARKET_PORTABLE &&
      r.portability?.portabilityClass !== PORTABILITY_CLASS.CONDITIONAL_PORTABLE
    ) {
      continue;
    }
    if (r.marketOpportunityId && existingMarketIds.has(r.marketOpportunityId)) {
      skippedExisting += 1;
      continue;
    }
    // Need full opportunity object — rebuild from stored build is only lite; skip if we don't have full
    // The evaluate function only stored lite built. Re-fetch from result isn't enough.
    // We'll only count in report for dry diagnostics; apply path re-builds below.
  }
  return { created, skippedExisting, readyCreated, note: "apply_handled_in_orchestrator" };
}

async function main() {
  const args = parseArgs(process.argv);
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const nowDate = new Date().toISOString().slice(0, 10);
  const ledger = {
    jevCalls: 0,
    jevUseful: 0,
    uniqueBlockersResolved: 0,
    factsAdded: 0,
    candidatesStateChanged: 0,
    hotelsImproved: 0,
    noOp: 0,
    wrongRoute: 0,
    configPatches: [],
    fitUpdates: 0,
    portabilityCreates: 0,
    portabilityReadyCreates: 0,
  };

  // -------- PHASE 0 SNAPSHOT --------
  process.stderr.write("Phase 0 snapshot...\n");
  const bundles = {};
  const snapshotHotels = [];
  for (const id of ALL_HOTELS) {
    process.stderr.write(`  load ${id}\n`);
    const b = await loadHotelBundle(id);
    bundles[id] = b;
    const sum = summarizeHotelCorpus(b.opps);
    snapshotHotels.push({
      hpcHotelId: id,
      hotel: b.afterHi.displayName || loadHotelDemandConfig(id)?.displayName || id,
      counts: sum.counts,
      ids: sum.ids,
      hi: {
        rooms: b.afterHi.rooms,
        meetingSqFt: b.afterHi.meetingSqFt,
        meetingRoomCount: b.afterHi.meetingRoomCount,
        eventStatus: b.afterHi.eventSpaceDomainStatus,
        hiEnriched: b.afterHi.hiEnriched,
        beforeMeetingSqFt: b.beforeHi.meetingSqFt,
        beforeRooms: b.beforeHi.rooms,
      },
    });
  }
  const snapshotPath = writeJson("PHASE0_SNAPSHOT.json", {
    generatedAt: new Date().toISOString(),
    hotels: snapshotHotels,
    totals: {
      strictReady: snapshotHotels.reduce((a, h) => a + h.counts.strictReady, 0),
      customerVisible: snapshotHotels.reduce((a, h) => a + h.counts.customerVisible, 0),
      watch: snapshotHotels.reduce((a, h) => a + h.counts.watch, 0),
      futureWatch: snapshotHotels.reduce((a, h) => a + h.counts.futureWatch, 0),
      held: snapshotHotels.reduce((a, h) => a + h.counts.held, 0),
      total: snapshotHotels.reduce((a, h) => a + h.counts.total, 0),
    },
  });
  if (args.mode === "snapshot") {
    console.log(JSON.stringify({ snapshotPath, totals: snapshotHotels.length }, null, 2));
    return;
  }

  // Sync HI capability into configs (fill nulls)
  for (const id of ALL_HOTELS) {
    const patch = syncCapabilityConfig(id, bundles[id].afterHi.hiOverlay, args.mode);
    if (!patch.skipped) ledger.configPatches.push({ hotelId: id, ...patch });
    if (patch.changes?.length) ledger.factsAdded += patch.changes.length;
  }
  // Reload enriched profiles after config sync for accurate after-state
  for (const id of ALL_HOTELS) {
    bundles[id].afterHi = await buildHiEnrichedHotelProfile(id);
  }

  // -------- PHASE priority cohort --------
  process.stderr.write("Priority cohort requal...\n");
  const cohortResults = [];
  for (const id of PRIORITY_COHORT) {
    const b = bundles[id];
    const requal = requalifyHotelCorpus({
      hotelId: id,
      opportunities: b.opps,
      hotelProfile: b.afterHi,
      hotelProfileBeforeHi: b.beforeHi,
    });
    for (const ev of requal.hiRecovered) {
      if (ev.jev?.action) {
        ledger.jevCalls += 1;
        if (ev.jev.action !== "STOP_NO_FURTHER_EVIDENCE") ledger.jevUseful += 1;
        else ledger.noOp += 1;
      }
    }
    const fitApply = await applyFitUpdates(
      id,
      requal.evaluations,
      args.mode,
      b.opps
    );
    ledger.fitUpdates += fitApply.updated;
    if (requal.hiRecoveredCount > 0) ledger.hotelsImproved += 1;
    cohortResults.push(requal);
    writeJson(`COHORT_${id}.json`, requal);
  }
  const cohortPass = cohortResults.every((r) => r.evaluations && !r.error);
  writeJson("PRIORITY_COHORT.json", { pass: cohortPass, results: cohortResults });
  if (!cohortPass) {
    console.error("PRIORITY COHORT FAILED — STOP");
    process.exit(3);
  }

  // -------- All-19 requal --------
  process.stderr.write("All-19 requal...\n");
  const allRequal = [];
  for (const id of ALL_HOTELS) {
    if (PRIORITY_COHORT.includes(id)) {
      allRequal.push(cohortResults.find((r) => r.hotelId === id));
      continue;
    }
    const b = bundles[id];
    const requal = requalifyHotelCorpus({
      hotelId: id,
      opportunities: b.opps,
      hotelProfile: b.afterHi,
      hotelProfileBeforeHi: b.beforeHi,
    });
    for (const ev of requal.evaluations) {
      if (ev.shouldContinueResearch && ev.jev?.action) {
        ledger.jevCalls += 1;
        if (ev.jev.action !== "STOP_NO_FURTHER_EVIDENCE") ledger.jevUseful += 1;
      }
      if (ev.blockerChanged || (ev.fitDelta != null && ev.fitDelta !== 0)) {
        ledger.candidatesStateChanged += 1;
      }
      if (
        ev.hiRecovered &&
        ev.oldBlocker !== ev.newBlocker &&
        ev.hiRelatedBefore
      ) {
        ledger.uniqueBlockersResolved += 1;
      }
    }
    const fitApply = await applyFitUpdates(
      id,
      requal.evaluations,
      args.mode,
      b.opps
    );
    ledger.fitUpdates += fitApply.updated;
    if (requal.hiRecoveredCount > 0) ledger.hotelsImproved += 1;
    allRequal.push(requal);
  }
  writeJson("ALL19_REQUAL.json", { results: allRequal });

  // -------- Portability --------
  process.stderr.write("Renaissance ↔ Hilton portability...\n");
  const renBundle = bundles[REN];
  const hiltonBundle = bundles[HILTON];
  const nowBundle = bundles[NOW_NOW];
  const renToHilton = runRenaissanceToHiltonPortability({
    renaissanceOpps: renBundle.opps,
    renaissanceProfile: renBundle.afterHi,
    hiltonProfile: hiltonBundle.afterHi,
    nowDate,
  });

  // Apply portable Hilton creates when build.ok
  if (args.mode === "apply") {
    const { buildHotelOpportunityFromMarketPacket } = await import(
      "../lib/group-demand-intelligence/market-opportunity-graph/build-hotel-opportunity-from-market-v1.js"
    );
    const { extractMarketOpportunityPacket } = await import(
      "../lib/group-demand-intelligence/market-opportunity-graph/market-opportunity-packet-v1.js"
    );
    const existingMarket = new Set(
      hiltonBundle.opps.map((o) => o.marketOpportunityId).filter(Boolean)
    );
    const existingTitles = new Set(
      hiltonBundle.opps.map((o) => String(o.title || "").toLowerCase().trim())
    );
    for (const r of renToHilton.results) {
      if (r.portability?.portabilityClass !== PORTABILITY_CLASS.MARKET_PORTABLE) {
        // CONDITIONAL / NOT_PORTABLE: do not auto-create hotel rows this pass
        continue;
      }
      const seed = renBundle.opps.find(
        (o) => (o.id || o.opportunityId) === r.sourceOpportunityId
      );
      if (!seed) continue;
      const marketPacket = extractMarketOpportunityPacket(seed, REN);
      if (marketPacket.marketOpportunityId && existingMarket.has(marketPacket.marketOpportunityId)) {
        continue;
      }
      const built = buildHotelOpportunityFromMarketPacket({
        marketPacket,
        seedOpp: seed,
        targetHotelProfile: hiltonBundle.afterHi,
        nowDate,
        requireStrictReady: false,
      });
      if (!built.ok || !built.opportunity) continue;
      const titleKey = String(built.opportunity.title || "").toLowerCase().trim();
      if (existingTitles.has(titleKey)) continue;
      // Never force ACTIVE/ACTIONABLE_NOW — only persist hotel-specific evaluation.
      // Strict readiness remains governed by isGdiCustomerOpportunityReady.
      if (!built.opportunity.customerFacingState || built.opportunity.customerFacingState === "ACTIVE") {
        built.opportunity.customerFacingState = built.readiness?.ok
          ? built.opportunity.customerFacingState || "WATCH"
          : "WATCH";
      }
      if (!built.readiness?.ok && built.opportunity.customerFacingState === "ACTIONABLE_NOW") {
        built.opportunity.customerFacingState = "WATCH";
      }
      built.opportunity.notes = [
        built.opportunity.notes,
        "created_via_ren_hilton_portability_v1",
        `portability=${r.portability.portabilityClass}`,
      ]
        .filter(Boolean)
        .join(" | ");
      try {
        await upsertOpportunity(built.opportunity, {
          source: "gdi_portability_v1",
        });
        ledger.portabilityCreates += 1;
        if (isGdiCustomerOpportunityReady(built.opportunity).ok) {
          ledger.portabilityReadyCreates += 1;
        }
        existingTitles.add(titleKey);
        if (marketPacket.marketOpportunityId) {
          existingMarket.add(marketPacket.marketOpportunityId);
        }
        if (built.jev?.action) {
          ledger.jevCalls += 1;
          if (built.jev.action !== "STOP_NO_FURTHER_EVIDENCE") ledger.jevUseful += 1;
        }
      } catch (err) {
        console.error("portability_upsert_failed", err.message || err);
      }
    }
  }

  const reverse = runReverseNycPortability({
    hiltonOpps: hiltonBundle.opps,
    hiltonProfile: hiltonBundle.afterHi,
    targetProfiles: [renBundle.afterHi, nowBundle.afterHi],
    nowDate,
  });

  writeJson("PORTABILITY_REN_TO_HILTON.json", renToHilton);
  writeJson("PORTABILITY_REVERSE_NYC.json", reverse);

  // -------- After snapshot --------
  process.stderr.write("After snapshot...\n");
  const afterHotels = [];
  for (const id of ALL_HOTELS) {
    const opps =
      args.mode === "apply" ? await listOpportunitiesForHotel(id) : bundles[id].opps;
    // For dry-run after counts, simulate fit-only (same rows)
    const sum = summarizeHotelCorpus(opps);
    const before = snapshotHotels.find((h) => h.hpcHotelId === id);
    afterHotels.push({
      hpcHotelId: id,
      hotel: before?.hotel,
      before: before?.counts,
      after: sum.counts,
      hiRecovered:
        allRequal.find((r) => r.hotelId === id)?.hiRecoveredCount || 0,
      deltaStrictReady: sum.counts.strictReady - (before?.counts.strictReady || 0),
      deltaVisible: sum.counts.customerVisible - (before?.counts.customerVisible || 0),
    });
  }

  const totalsBefore = {
    strictReady: snapshotHotels.reduce((a, h) => a + h.counts.strictReady, 0),
    visible: snapshotHotels.reduce((a, h) => a + h.counts.customerVisible, 0),
  };
  const totalsAfter = {
    strictReady: afterHotels.reduce((a, h) => a + h.after.strictReady, 0),
    visible: afterHotels.reduce((a, h) => a + h.after.customerVisible, 0),
  };

  const hiRecoveredAll = allRequal.flatMap((r) =>
    (r.hiRecovered || []).map((e) => ({
      hotelId: r.hotelId,
      hotel: r.hotelName,
      ...e,
    }))
  );

  writeJson("AFTER_SNAPSHOT.json", { hotels: afterHotels, totalsBefore, totalsAfter });
  writeJson("HI_RECOVERED_CANDIDATES.json", { count: hiRecoveredAll.length, rows: hiRecoveredAll });
  writeJson("LEDGER.json", ledger);

  const summary = {
    mode: args.mode,
    cohortPass,
    hotels: ALL_HOTELS.length,
    totalsBefore,
    totalsAfter,
    netStrictReady: totalsAfter.strictReady - totalsBefore.strictReady,
    netVisible: totalsAfter.visible - totalsBefore.visible,
    hiRecovered: hiRecoveredAll.length,
    portability: renToHilton.summary,
    reverse: {
      demandEntitiesTested: reverse.demandEntitiesTested,
      byTarget: reverse.byTarget.map((t) => ({
        target: t.targetName,
        accepted: t.accepted,
        held: t.held,
        rejected: t.rejected,
      })),
    },
    ledger,
    hiltonBefore: snapshotHotels.find((h) => h.hpcHotelId === HILTON)?.counts,
    hiltonAfter: afterHotels.find((h) => h.hpcHotelId === HILTON)?.after,
    renaissanceBefore: snapshotHotels.find((h) => h.hpcHotelId === REN)?.counts,
    renaissanceAfter: afterHotels.find((h) => h.hpcHotelId === REN)?.after,
  };
  writeJson("RUN_SUMMARY.json", summary);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
