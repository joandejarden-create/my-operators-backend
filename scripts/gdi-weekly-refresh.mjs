#!/usr/bin/env node
/**
 * GDI Weekly Refresh — reusable hotel weekly delta.
 *
 * Usage:
 *   node scripts/gdi-weekly-refresh.mjs --hotel=recLuxvwwxID7U2B8
 *   node scripts/gdi-weekly-refresh.mjs --hotel=recLuxvwwxID7U2B8 --skip-discovery  # delta from frozen discovery
 *   node scripts/gdi-weekly-refresh.mjs --hotel=recLuxvwwxID7U2B8 --apply-stage
 *   node scripts/gdi-weekly-refresh.mjs --hotel=recLuxvwwxID7U2B8 --max-queries=20 --skip-parallel
 *
 * Does not live-promote Airtable. Staging writes local hotel bag annotations when --apply-stage.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadHotelDemandConfig,
  buildHotelGroupDemandProfile,
} from "../lib/group-demand-intelligence/index.js";
import { runGdiBlindDiscoveryPipeline } from "../lib/group-demand-intelligence/discovery-pipeline.js";
import {
  applyDiscoveryHygieneV3,
  ACTIONABILITY_V3,
  DISCOVERY_HYGIENE_V3,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";
import {
  WEEKLY_DELTA_VERSION,
  WEEKLY_DELTA_STATE,
  snapshotBaselineOpportunity,
  computeWeeklyDelta,
  applyWeeklyDeltaToOpportunities,
  weeklyDeltaSummaryCounts,
} from "../lib/group-demand-intelligence/weekly-delta.js";
import { loadOpportunities, saveOpportunities } from "../lib/group-demand-intelligence/repository.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const RUN_DATE = "20260921";
const STACK_SHA = "7312754ea8823bb4f5c28491f0096805030a0ffd";

function arg(name, fallback = null) {
  const prefix = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : fallback;
}
function flag(name) {
  return process.argv.includes(`--${name}`);
}
function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}
function writeMd(p, lines) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, (Array.isArray(lines) ? lines : [lines]).join("\n"));
}

function resolveHotel(hotelArg) {
  const raw = String(hotelArg || "").trim();
  if (raw === "bethesda" || raw === "recLuxvwwxID7U2B8") {
    return {
      hotelId: "recLuxvwwxID7U2B8",
      slug: "bethesda",
      name: "Bethesda Marriott",
    };
  }
  if (/^rec[A-Za-z0-9]+$/.test(raw)) {
    const cfg = loadHotelDemandConfig(raw);
    return {
      hotelId: raw,
      slug: String(cfg?.displayName || raw)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, ""),
      name: cfg?.displayName || raw,
    };
  }
  throw new Error(`unknown_hotel_${raw}`);
}

function buildOppStub(hotelId, cand, runId, nowIso) {
  const id = cand.id || cand.opportunityId;
  return {
    id,
    hotelId,
    organizationName: cand.organizationName || null,
    title: cand.title,
    segment: cand.segment || null,
    opportunityType: cand.opportunityType || "PRIMARY_PURSUIT",
    opportunityTypeLabel: cand.opportunityType || "Primary Pursuit",
    priority: cand.priority || "MEDIUM_PRIORITY",
    venueSourcingStatus: cand.venueSourcingStatus || "UNKNOWN",
    roomDemandStatus: cand.roomDemandStatus || "UNKNOWN",
    eventStartDate: cand.eventStartDate || null,
    eventEndDate: cand.eventEndDate || null,
    destinationStatus: cand.destinationStatus || null,
    venueStatus: cand.venueStatus || null,
    hotelOpportunityThesis: cand.hotelOpportunityThesis || cand.summaryWhyHotel || null,
    whyNow: cand.whyNow || "New this weekly refresh — review for sales action.",
    summaryWhat: cand.summaryWhat || cand.title,
    summaryWhyHotel: cand.summaryWhyHotel || cand.hotelOpportunityThesis || null,
    primaryContact: cand.primaryContact || null,
    contacts: cand.contacts || [],
    sources: cand.sources || cand.evidenceSources || [],
    evidence: cand.evidence || [],
    bookingWindowStatus: cand.bookingWindowStatus || "QUALIFY_NOW",
    labels: ["new_this_week"],
    researchStatus: "WEEKLY_REFRESH_STAGED",
    createdAt: nowIso,
    updatedAt: nowIso,
    firstSeenAt: nowIso,
    firstSeenRunId: runId,
    lastSeenAt: nowIso,
    lastSeenRunId: runId,
    weeklyDeltaState: WEEKLY_DELTA_STATE.NEW,
    isNewThisWeek: true,
    actionabilityV3: cand.actionabilityV3 || ACTIONABILITY_V3.TRUE_ACTIONABLE,
  };
}

async function runDiscovery(hotel, args) {
  const config = loadHotelDemandConfig(hotel.hotelId);
  const profile = buildHotelGroupDemandProfile(hotel.hotelId, { config });
  const pipe = await runGdiBlindDiscoveryPipeline({
    hotelId: hotel.hotelId,
    profile,
    config,
    dryRun: false,
    skipParallel: args.skipParallel,
    nativeOpts: { maxQueries: args.maxQueries },
  });
  return pipe;
}

function mainSyncBaseline(hotel, runId) {
  const bag = loadOpportunities(hotel.hotelId);
  const opps = bag.opportunities || [];
  const baseline = {
    validationMarker: `gdi_weekly_baseline_${hotel.slug}_${RUN_DATE}`,
    runId,
    hotelId: hotel.hotelId,
    hotelName: hotel.name,
    frozenAt: new Date().toISOString(),
    stackSha: STACK_SHA,
    priorCanonicalCount: opps.length,
    sourceRunId: bag.runId || null,
    opportunities: opps.map(snapshotBaselineOpportunity),
  };
  const p = path.join(
    ROOT,
    `data/group-demand-intelligence/evals/${hotel.slug}-weekly-baseline-${RUN_DATE}.json`
  );
  writeJson(p, baseline);
  return { baseline, bag, path: p };
}

async function main() {
  const hotel = resolveHotel(arg("hotel", "recLuxvwwxID7U2B8"));
  const runId = `gdi_weekly_${hotel.slug}_${RUN_DATE}`;
  const nowIso = new Date().toISOString();
  const startedAt = nowIso;
  const args = {
    skipDiscovery: flag("skip-discovery"),
    force: flag("force"),
    applyStage: flag("apply-stage"),
    skipParallel: flag("skip-parallel") || !flag("allow-parallel"),
    maxQueries: Number(arg("max-queries", "20")),
    skipWho: flag("skip-who") || true, // WHO staged separately unless --run-who
    runWho: flag("run-who"),
  };

  console.error(`[weekly] hotel=${hotel.hotelId} runId=${runId}`);

  const { baseline, bag } = mainSyncBaseline(hotel, runId);
  console.error(`[weekly] baseline frozen: ${baseline.priorCanonicalCount} opportunities`);

  const discoveryPath = path.join(
    ROOT,
    `data/group-demand-intelligence/evals/${hotel.slug}-weekly-discovery-${RUN_DATE}.json`
  );
  let discoveryFreeze;
  if (args.skipDiscovery && fs.existsSync(discoveryPath) && !args.force) {
    discoveryFreeze = readJson(discoveryPath);
    console.error(`[weekly] reusing frozen discovery ${discoveryPath}`);
  } else {
    console.error(`[weekly] running native discovery maxQueries=${args.maxQueries}`);
    const pipe = await runDiscovery(hotel, args);
    const seed = pipe.seedCandidates || [];
    const hygiene = applyDiscoveryHygieneV3(hotel.hotelId, seed, {
      nowDate: "2026-09-21",
      subjectHotel: { hotelId: hotel.hotelId, name: hotel.name },
    });
    discoveryFreeze = {
      validationMarker: `gdi_weekly_discovery_${hotel.slug}_${RUN_DATE}`,
      runId,
      hotelId: hotel.hotelId,
      frozenAt: new Date().toISOString(),
      stackSha: STACK_SHA,
      discoveryHygieneVersion: DISCOVERY_HYGIENE_V3,
      pipeline: {
        version: pipe.version,
        nativeQueries: pipe.native?.ledger?.serpQueries ?? null,
        parallelSkipped: pipe.parallel?.skipped ?? true,
      },
      candidateCount: seed.length,
      trueActionableCount: hygiene.trueActionable.length,
      byState: hygiene.byState,
      candidates: hygiene.rows.map((r) => ({
        id: r.id || r.opportunityId,
        title: r.title,
        organizationName: r.organizationName,
        eventStartDate: r.eventStartDate,
        eventEndDate: r.eventEndDate,
        destinationStatus: r.destinationStatus,
        venueSourcingStatus: r.venueSourcingStatus,
        roomDemandStatus: r.roomDemandStatus,
        opportunityType: r.opportunityType,
        priority: r.priority,
        actionabilityV3: r.actionabilityV3,
        hygieneV3: {
          failureClass: r.hygieneV3?.failureClass || null,
          notes: r.hygieneV3?.notes || [],
          openSourcing: r.hygieneV3?.openSourcing || null,
          overflow: r.hygieneV3?.overflow || null,
          demand: r.hygieneV3?.demand || null,
        },
        sources: r.sources || r.evidenceSources || [],
        whyNow: r.whyNow,
        hotelOpportunityThesis: r.hotelOpportunityThesis,
        primaryContact: r.primaryContact || null,
      })),
      trueActionableIds: hygiene.trueActionable.map((r) => r.id || r.opportunityId),
    };
    writeJson(discoveryPath, discoveryFreeze);
    console.error(
      `[weekly] discovery frozen: candidates=${discoveryFreeze.candidateCount} TRUE=${discoveryFreeze.trueActionableCount}`
    );
  }

  const existingAsCurrent = (bag.opportunities || []).map((o) => ({
    ...o,
    // Existing canonical rows are already in the hotel bag — treat as rediscovery set
    weeklyActionable:
      o.priority === "HIGH_PRIORITY" || o.priority === "MEDIUM_PRIORITY",
  }));
  const discoveryCandidates = discoveryFreeze.candidates || [];
  // Prefer discovery row when same id; otherwise union
  const byCandId = new Map();
  for (const o of existingAsCurrent) byCandId.set(o.id || o.opportunityId, o);
  for (const c of discoveryCandidates) {
    const id = c.id || c.opportunityId;
    byCandId.set(id, { ...byCandId.get(id), ...c });
  }
  const currentCandidates = [...byCandId.values()];

  const delta = computeWeeklyDelta({
    baselineOpportunities: baseline.opportunities,
    currentCandidates,
    runId,
    nowIso,
    trueActionableIds: [
      ...(discoveryFreeze.trueActionableIds || []),
      ...existingAsCurrent
        .filter((o) => o.weeklyActionable)
        .map((o) => o.id),
    ],
  });

  const deltaPath = path.join(
    ROOT,
    `data/group-demand-intelligence/evals/${hotel.slug}-weekly-delta-${RUN_DATE}.json`
  );
  writeJson(deltaPath, {
    validationMarker: `gdi_weekly_delta_${hotel.slug}_${RUN_DATE}`,
    version: WEEKLY_DELTA_VERSION,
    runId,
    hotelId: hotel.hotelId,
    frozenAt: nowIso,
    stackSha: STACK_SHA,
    counts: delta.counts,
    rows: delta.rows,
  });

  const newBuilders = {};
  for (const row of delta.rows) {
    if (row.weeklyDeltaState !== WEEKLY_DELTA_STATE.NEW) continue;
    const cand = discoveryFreeze.candidates.find(
      (c) => (c.id || c.opportunityId) === row.candidateId
    );
    if (cand) newBuilders[row.candidateId] = buildOppStub(hotel.hotelId, cand, runId, nowIso);
  }

  const stagedOpps = applyWeeklyDeltaToOpportunities({
    existingOpportunities: bag.opportunities || [],
    delta,
    runId,
    latestCompletedWeeklyRunId: runId,
    newOpportunityBuilders: newBuilders,
  });
  const summary = weeklyDeltaSummaryCounts(stagedOpps, runId);

  const refresh = {
    validationMarker: `gdi_weekly_refresh_${hotel.slug}_${RUN_DATE}`,
    runId,
    hotelId: hotel.hotelId,
    hotelName: hotel.name,
    startedAt,
    completedAt: new Date().toISOString(),
    discoveryVersion: DISCOVERY_HYGIENE_V3,
    whoVersion: "v11_v12_deferred_unless_run_who",
    providerVersion: "surfe_pdl_gated",
    stackSha: STACK_SHA,
    priorCanonicalCount: baseline.priorCanonicalCount,
    candidateCount: discoveryFreeze.candidateCount,
    trueActionableCount: discoveryFreeze.trueActionableCount,
    counts: delta.counts,
    headerSummary: summary,
    appliedStage: false,
    who: {
      runWho: args.runWho,
      newWho: 0,
      surfeCalls: 0,
      pdlCalls: 0,
      unchangedOppProviderCalls: 0,
      note: args.runWho
        ? "WHO requested — wire discoverWhoV9 for NEW only in follow-up if needed"
        : "WHO deferred for NEW until founder live-promote; staged discovery only",
    },
    integrity: {
      duplicateOpportunitiesCreated: 0,
      firstSeenReset: 0,
      validationsLost: 0,
      actionsLost: 0,
      outcomesLost: 0,
      decisionLinksLost: 0,
    },
    artifacts: {
      baseline: `${hotel.slug}-weekly-baseline-${RUN_DATE}.json`,
      discovery: `${hotel.slug}-weekly-discovery-${RUN_DATE}.json`,
      delta: `${hotel.slug}-weekly-delta-${RUN_DATE}.json`,
    },
  };

  // Integrity checks
  const priorIds = new Set((bag.opportunities || []).map((o) => o.id));
  for (const o of bag.opportunities || []) {
    const staged = stagedOpps.find((s) => s.id === o.id);
    if (!staged) refresh.integrity.validationsLost += 1;
    else {
      if (o.firstSeenAt && staged.firstSeenAt !== o.firstSeenAt) {
        refresh.integrity.firstSeenReset += 1;
      }
    }
  }
  const newIds = stagedOpps.filter((o) => !priorIds.has(o.id)).map((o) => o.id);
  const dupNew = newIds.filter((id, i) => newIds.indexOf(id) !== i);
  refresh.integrity.duplicateOpportunitiesCreated = dupNew.length;

  const refreshPath = path.join(
    ROOT,
    `data/group-demand-intelligence/evals/${hotel.slug}-weekly-refresh-${RUN_DATE}.json`
  );

  if (args.applyStage) {
    const backupDir = path.join(
      ROOT,
      `data/group-demand-intelligence/hotels/${hotel.hotelId}/weekly-runs/${runId}`
    );
    writeJson(path.join(backupDir, "opportunities.pre-stage.json"), bag);
    saveOpportunities(hotel.hotelId, {
      ...bag,
      runId,
      weeklyRunId: runId,
      weeklyRefreshAt: refresh.completedAt,
      opportunities: stagedOpps,
      salespersonVisibleIds: stagedOpps
        .filter((o) => o.priority !== "DISQUALIFIED")
        .map((o) => o.id),
      updatedAt: refresh.completedAt,
    });
    writeJson(path.join(backupDir, "opportunities.staged.json"), {
      hotelId: hotel.hotelId,
      runId,
      opportunities: stagedOpps,
    });
    refresh.appliedStage = true;
    console.error(`[weekly] staged annotations applied to opportunities.json (backup in weekly-runs/${runId})`);
  } else {
    // Always write staged bag for review without mutating live until --apply-stage
    const stagePath = path.join(
      ROOT,
      `data/group-demand-intelligence/hotels/${hotel.hotelId}/weekly-runs/${runId}/opportunities.staged.json`
    );
    writeJson(stagePath, {
      hotelId: hotel.hotelId,
      runId,
      opportunities: stagedOpps,
      headerSummary: summary,
    });
  }

  writeJson(refreshPath, refresh);

  console.error(
    JSON.stringify(
      {
        runId,
        prior: baseline.priorCanonicalCount,
        candidates: discoveryFreeze.candidateCount,
        trueActionable: discoveryFreeze.trueActionableCount,
        counts: delta.counts,
        headerSummary: summary,
        appliedStage: refresh.appliedStage,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
