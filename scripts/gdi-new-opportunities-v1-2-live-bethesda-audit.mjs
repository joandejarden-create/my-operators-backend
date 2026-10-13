#!/usr/bin/env node
/**
 * GDI New Opportunities V1.2 — LIVE Bethesda discovery routing + evidence recovery.
 *
 * Extends V1.1 with:
 * - future-biased lane queries
 * - early SERP past skips (via native routing)
 * - bounded WATCH second-pass
 * - funnel metrics (past skip / second-pass / TRUE)
 *
 * - Real SerpAPI + fetch + extract
 * - dryRun qualify (NO production opportunities.json write)
 * - Surfe/PDL: 0
 * - Artifacts under reports/ only
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runGroupDemandResearch,
  buildHotelGroupDemandProfile,
  loadHotelDemandConfig,
} from "../lib/group-demand-intelligence/index.js";
import { runNativeBlindDiscovery } from "../lib/group-demand-intelligence/native-blind-discovery.js";
import { dedupeDiscoveryCandidates } from "../lib/group-demand-intelligence/candidate-dedupe.js";
import { mapWebhoundOpportunityUniverse } from "../lib/group-demand-intelligence/webhound-opportunity-import.js";
import {
  DISCOVERY_RECALL_V4,
  RECALL_EXTRACT_SYSTEM_V4,
  buildRecallV4SearchTasks,
  renderRecallV4Brief,
  inferDemandArchetype,
} from "../lib/group-demand-intelligence/discovery-recall-v4.js";
import {
  enrichCommercialEvidenceV4,
} from "../lib/group-demand-intelligence/commercial-evidence-v4.js";
import {
  ACTIONABILITY_V3,
  applyDiscoveryHygieneV3,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";
import {
  DEMAND_LANE,
  DEMAND_SIGNAL_TYPE,
  LANE_TO_SIGNAL,
  LANE_QUERY_HINTS_EN,
  classifyDemandSignalType,
  demandSignalTypeLabel,
  enrichOpportunityDemandSignal,
  classifyLaneYield,
  classifyPublicTrigger,
  createOrgResearchReuseCache,
  buildDemandLaneSearchTasks,
  NEW_OPPORTUNITIES_V1,
} from "../lib/group-demand-intelligence/demand-signal-types.js";
import {
  scoreCanonicalMatch,
  snapshotBaselineOpportunity,
} from "../lib/group-demand-intelligence/weekly-delta.js";
import {
  DISCOVERY_ROUTING_V1_2,
  withFutureHousingBias,
  classifyWatchRecovery,
  WATCH_RECOVERY_CLASS,
  buildSecondPassQueries,
  enrichCandidateRoutingV12,
} from "../lib/group-demand-intelligence/discovery-routing-v1-2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recLuxvwwxID7U2B8";
const HOTEL = {
  hotelId: HOTEL_ID,
  name: "Bethesda Marriott",
  market: "Bethesda / Montgomery County / DMV",
  rooms: 407,
  countryCode: "US",
  geos: [
    "Bethesda",
    "Rockville",
    "Montgomery County MD",
    "Washington DC",
    "Northern Virginia",
  ],
};
const AS_OF = new Date().toISOString().slice(0, 10);
const OUT_DIR = path.join(
  ROOT,
  "reports/group-demand-intelligence/gdi-new-opportunities-v1-2-live-bethesda-audit"
);
const CONCURRENCY = 4;
const QUERIES_PER_LANE = 2;
const MAX_PAGES = 2;
const MAX_EXTRACT_BATCHES = 3;
const MAX_SECOND_PASS_QUERIES = 12;
const MAX_SECOND_PASS_CANDIDATES = 8;
const MARKET_TOKENS = [
  "Bethesda",
  "Washington",
  "DMV",
  "Montgomery",
  "Rockville",
  "Arlington",
  "Virginia",
  "Maryland",
];

/** All V1.1 lanes (EVENT + expanded). */
const AUDIT_LANES = [
  DEMAND_LANE.EVENT_ASSOCIATION,
  DEMAND_LANE.TRAINING,
  DEMAND_LANE.GOVERNMENT_CONTRACTOR,
  DEMAND_LANE.PROJECT_TEAM,
  DEMAND_LANE.RELOCATION,
  DEMAND_LANE.MEDICAL,
  DEMAND_LANE.UNIVERSITY,
  DEMAND_LANE.SPORTS_ACADEMIC,
  DEMAND_LANE.CONSULTING_PROJECT,
  DEMAND_LANE.CORPORATE_MEETING,
  DEMAND_LANE.BOARD_COMMITTEE,
  DEMAND_LANE.NONPROFIT,
  DEMAND_LANE.PROFESSIONAL_CREW,
  DEMAND_LANE.INCENTIVE_RETREAT,
  DEMAND_LANE.OVERFLOW,
];

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}

function isOfficialUrl(url) {
  const u = String(url || "").toLowerCase();
  if (!u.startsWith("http")) return false;
  if (/\/(news|press|blog|article)\b/.test(u) && !/\.gov\b/.test(u)) return false;
  return (
    /\.gov\b/.test(u) ||
    /\.edu\b/.test(u) ||
    /\.org\b/.test(u) ||
    /nih\.gov|walterreed|montgomerycountymd|dc\.gov|gsa\.gov|sam\.gov|usaspending|grants\.gov/.test(
      u
    )
  );
}

function poolMap(items, concurrency, fn) {
  const results = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await fn(items[idx], idx);
    }
  }
  const n = Math.min(concurrency, items.length);
  return Promise.all(Array.from({ length: n }, () => worker())).then(() => results);
}

function matchAgainstBaseline(candidate, baselineSnaps) {
  let best = { score: 0, matched: false, priorId: null, reasons: [] };
  for (const prior of baselineSnaps) {
    const m = scoreCanonicalMatch(prior, candidate);
    if (m.score > best.score) {
      best = {
        score: m.score,
        matched: m.matched || m.score >= 55,
        priorId: prior.opportunityId || prior.id,
        reasons: m.reasons || [],
      };
    }
  }
  return best;
}

function failureReason(opp, laneResult) {
  if (laneResult?.serpErrors?.length) return "SEARCH_TOO_GENERIC";
  if (laneResult?.extractionEmpty) return "EXTRACTION_FAILURE";
  const blob = `${opp.title || ""} ${opp.housingEvidence || ""} ${opp.summaryWhat || ""}`;
  const trig = classifyPublicTrigger(blob);
  if (
    trig.triggerClass !== "UNKNOWN" &&
    !/\b(?:hotel|lodging|room|housing|overnight)\b/i.test(blob)
  ) {
    return "TRIGGER_WITHOUT_DEMAND";
  }
  if (/\b(?:webinar|one[- ]hour|virtual\s+only|commuter)\b/i.test(blob)) {
    return "LOCAL_NO_ROOM";
  }
  if (!opp.eventStartDate && !opp.startDate) return "NO_TIMING";
  if (!(opp.officialSource || (opp.evidenceSources || []).length)) return "SOURCE_WEAK";
  if (!opp.hotelDemandThesis && !opp.housingEvidence && !opp.roomDemandEvidence) {
    return "NO_COMMERCIAL_THESIS";
  }
  return null;
}

function buildLaneQueries(lane, geos, yearNear) {
  const hints = LANE_QUERY_HINTS_EN[lane] || LANE_QUERY_HINTS_EN[DEMAND_LANE.OTHER];
  const queries = [];
  for (const geo of geos.slice(0, 2)) {
    for (const hint of hints.slice(0, 2)) {
      // V1.2: future/housing bias — avoid archive-heavy generic queries
      const biased = withFutureHousingBias(`${hint} ${yearNear}`, [geo]);
      queries.push(biased);
      if (queries.length >= QUERIES_PER_LANE) break;
    }
    if (queries.length >= QUERIES_PER_LANE) break;
  }
  return [...new Set(queries)].slice(0, QUERIES_PER_LANE);
}

async function runLaneDiscovery(lane, ctx) {
  const t0 = Date.now();
  const queries = buildLaneQueries(lane, HOTEL.geos, ctx.yearNear);
  const task = {
    vertical: lane,
    note: `demand_lane:${lane}`,
    lane,
    demandSignalType: LANE_TO_SIGNAL[lane] || DEMAND_SIGNAL_TYPE.OTHER,
    queries,
  };
  console.error(`[lane] start ${lane} queries=${queries.length}`);
  const native = await runNativeBlindDiscovery({
    hotelId: HOTEL_ID,
    profile: ctx.profile,
    config: ctx.config,
    maxQueries: QUERIES_PER_LANE,
    maxPagesPerQuery: MAX_PAGES,
    maxExtractBatches: MAX_EXTRACT_BATCHES,
    searchTasksOverride: [task],
    briefOverride: ctx.brief,
    extractSystemOverride: RECALL_EXTRACT_SYSTEM_V4,
    discoverySourceLabel: `${DISCOVERY_RECALL_V4}:lane:${lane}`,
    stratifiedQueryBudget: false,
    serpLocale: { hl: "en", gl: "us" },
  });
  const deduped = dedupeDiscoveryCandidates(native.rows || []);
  const mapped = mapWebhoundOpportunityUniverse(deduped.candidates || [], HOTEL_ID);
  const candidates = (mapped.candidates || []).map((c) => ({
    ...c,
    discoveryLane: lane,
    demandSignalType: c.demandSignalType || LANE_TO_SIGNAL[lane],
    researchProvider: "native",
    discoverySource: NEW_OPPORTUNITIES_V1,
  }));

  // Org reuse from candidate orgs / source hosts
  for (const c of candidates) {
    const org = c.organizationName || c.organization;
    if (org) {
      const prev = ctx.orgCache.get(org);
      ctx.orgCache.set(org, {
        officialDomain: c.officialSource || prev?.officialDomain || null,
        incrementReuse: Boolean(prev),
      });
    }
  }

  const elapsedMs = Date.now() - t0;
  const urls = (native.ledger?.serpDigest || []).reduce(
    (n, d) => n + (d.urlCount || 0),
    0
  );
  const urlsAfterPastScreen = (native.ledger?.serpDigest || []).reduce(
    (n, d) => n + ((d.urlCountAfterPastScreen ?? d.urlCount) || 0),
    0
  );
  const skipPastSerp = native.ledger?.skipPastSerp || 0;
  const candidateUrls = candidates.flatMap((c) => {
    const list = [];
    if (c.officialSource) list.push(c.officialSource);
    for (const e of c.evidenceSources || c.sources || []) {
      if (e?.url) list.push(e.url);
      else if (typeof e === "string" && e.startsWith("http")) list.push(e);
    }
    return list;
  });
  const officialUrls = candidateUrls.filter(isOfficialUrl).length;

  console.error(
    `[lane] done ${lane} serp=${native.ledger?.serpQueries || 0} urls=${urls} skipPast=${skipPastSerp} cand=${candidates.length} ${elapsedMs}ms`
  );

  return {
    lane,
    demandSignalType: LANE_TO_SIGNAL[lane],
    queriesIssued: queries,
    queryCount: native.ledger?.serpQueries || 0,
    urls,
    urlsAfterPastScreen,
    skipPastSerp,
    officialUrls,
    pagesFetched: native.ledger?.pagesFetched || 0,
    pagesFailed: native.ledger?.pagesFailed || 0,
    openaiCalls: native.ledger?.openaiCalls || 0,
    serpCharged: native.ledger?.serpCharged || 0,
    candidates,
    extractionEmpty: (native.ledger?.extractBatches || []).every((b) => b.empty),
    serpErrors: (native.ledger?.errors || []).filter((e) => e.stage === "serp"),
    elapsedMs,
    ledger: native.ledger,
  };
}

async function reprocessFromRaw() {
  const startedAt = Date.now();
  const raw = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "LIVE_RAW.json"), "utf8"));
  const config = loadHotelDemandConfig(HOTEL_ID);
  const archetype = inferDemandArchetype(config, { rooms: HOTEL.rooms });
  const baselinePath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json"
  );
  const baselineDoc = JSON.parse(fs.readFileSync(baselinePath, "utf8"));
  const baselineOpps = baselineDoc.opportunities || [];
  const baselineSnaps = baselineOpps.map((o) => snapshotBaselineOpportunity(o));
  const baselineIds = baselineOpps.map((o) => o.id).sort();

  const seedCandidates = raw.seedCandidates || [];
  const hygiene = applyDiscoveryHygieneV3(HOTEL_ID, seedCandidates, {
    nowDate: AS_OF,
    subjectHotel: { hotelId: HOTEL_ID, name: HOTEL.name },
  });
  const afterHygiene = hygiene.rows || [];

  const classified = afterHygiene.map((o) => {
    const match = matchAgainstBaseline(o, baselineSnaps);
    const signal = o.demandSignalType || classifyDemandSignalType(o);
    const isEvent = signal === DEMAND_SIGNAL_TYPE.EVENT;
    const lane =
      o.discoveryLane ||
      Object.entries(LANE_TO_SIGNAL).find(([, v]) => v === signal)?.[0] ||
      null;
    let weeklyState = "NEW";
    let newnessStatus = "NEW_TO_GDI";
    if (match.matched) {
      weeklyState = "UNCHANGED";
      newnessStatus = "EXISTING_IN_GDI";
      if (o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE) {
        const prior = baselineOpps.find((b) => b.id === match.priorId);
        const priorWasWeak =
          prior &&
          /WATCH|INSUFFICIENT|FUTURE/i.test(
            String(prior.actionabilityV3 || prior.commercialStatus || "")
          );
        weeklyState = priorWasWeak ? "REACTIVATED" : "UPDATED";
      }
    }
    return {
      ...o,
      demandSignalType: signal,
      demandSignalTypeLabel: demandSignalTypeLabel(signal),
      discoveryLane: lane,
      baselineMatch: match,
      weeklyState,
      newnessStatus,
      isEventLane: isEvent,
    };
  });

  const liveNew = classified.filter((o) => o.newnessStatus === "NEW_TO_GDI");
  const liveNewActionable = liveNew.filter(
    (o) => o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE
  );
  const liveNewWatch = liveNew.filter(
    (o) =>
      o.actionabilityV3 === ACTIONABILITY_V3.VALID_WATCH ||
      o.actionabilityV3 === ACTIONABILITY_V3.VALID_FUTURE
  );
  const eventOnlyNew = liveNew.filter((o) => o.isEventLane);
  const expandedNew = liveNew.filter((o) => !o.isEventLane);
  const eventOnlyActionable = eventOnlyNew.filter(
    (o) => o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE
  );
  const expandedActionable = expandedNew.filter(
    (o) => o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE
  );

  const laneResults = raw.laneResults || [];
  const laneYield = laneResults.map((lr) => {
    const laneCands = classified.filter(
      (o) =>
        o.discoveryLane === lr.lane || o.demandSignalType === lr.demandSignalType
    );
    const actionable = laneCands.filter(
      (o) => o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE
    );
    const neu = laneCands.filter((o) => o.newnessStatus === "NEW_TO_GDI");
    const yieldClass = classifyLaneYield({
      queries: lr.queryCount,
      urls: lr.urls,
      candidates: (lr.candidates || []).length,
      trueActionable: actionable.filter((o) => o.newnessStatus === "NEW_TO_GDI")
        .length,
      newOpportunities: neu.length,
    });
    let failReason = null;
    if (!(lr.candidates || []).length) {
      failReason = lr.serpErrors?.length
        ? "SEARCH_TOO_GENERIC"
        : lr.extractionEmpty
          ? "EXTRACTION_FAILURE"
          : "NO_RESULTS";
    }
    return {
      lane: lr.lane,
      queries: lr.queryCount,
      urls: lr.urls,
      officialUrls: lr.officialUrls,
      candidates: (lr.candidates || []).length,
      qualified: laneCands.length,
      actionable: actionable.length,
      watch: laneCands.filter(
        (o) =>
          o.actionabilityV3 === ACTIONABILITY_V3.VALID_WATCH ||
          o.actionabilityV3 === ACTIONABILITY_V3.VALID_FUTURE
      ).length,
      neu: neu.length,
      trueActionableNew: actionable.filter((o) => o.newnessStatus === "NEW_TO_GDI")
        .length,
      byActionability: laneCands.reduce((acc, o) => {
        const k = o.actionabilityV3 || "NONE";
        acc[k] = (acc[k] || 0) + 1;
        return acc;
      }, {}),
      yieldClass,
      failReason,
      elapsedMs: lr.elapsedMs,
      serpCharged: lr.serpCharged,
      openaiCalls: lr.openaiCalls,
      estimatedSerpUsd: Number(((lr.serpCharged || lr.queryCount) * 0.01).toFixed(3)),
    };
  });

  const manualAudit = liveNewActionable.map((o) => {
    const thesis =
      o.hotelDemandThesis ||
      o.housingEvidence ||
      o.roomDemandEvidence ||
      o.whyHotelDemand ||
      o.hygieneV3?.notes?.join("; ") ||
      "";
    const source =
      o.officialSource ||
      (o.evidenceSources || [])[0]?.url ||
      (o.sources || [])[0]?.url ||
      "";
    const sourceOk = Boolean(source);
    const thesisOk =
      Boolean(String(thesis).trim()) ||
      Boolean(o.hygieneV3?.hotelDemandEvidence) ||
      Boolean(o.openSourcingEvidence);
    const actionOk = Boolean(
      o.recommendedAction || o.suggestedAction || o.venueSourcingStatus
    );
    const geographyOk = Boolean(
      o.demandTerritoryFit || o.destinationStatus || o.location
    );
    return {
      opportunityId: o.id,
      demandType: o.demandSignalTypeLabel || o.demandSignalType,
      organization: o.organizationName || "",
      opportunity: o.title || "",
      source,
      timing: o.eventStartDate || o.startDate || "",
      location: o.destinationStatus || o.location || "",
      roomDemandThesis: thesis || "(from hygiene lodging-plausible evidence)",
      hotelFit: o.hotelFitScore ?? null,
      geographicFit: o.demandTerritoryFit || null,
      suggestedAction: o.recommendedAction || o.suggestedAction || "QUALIFY_NOW",
      whyNew: `No canonical match vs Bethesda 38 (best score ${o.baselineMatch?.score || 0})`,
      whyEventSearchWouldMiss:
        o.demandSignalType === DEMAND_SIGNAL_TYPE.EVENT
          ? "Would be found by event search"
          : `demandSignalType=${o.demandSignalType} — outside event-only families`,
      potentialRoomNights: o.potentialRoomNights ?? null,
      potentialRoomNightsStatus: o.potentialRoomNightsStatus || "UNKNOWN",
      actionabilityV3: o.actionabilityV3,
      failureClass: o.hygieneV3?.failureClass || null,
      sourceOk,
      thesisOk,
      actionOk,
      geographyOk,
      autoAuditResult:
        sourceOk && thesisOk && actionOk && geographyOk
          ? "PASS_PENDING_HUMAN"
          : "FAIL_WEAK",
    };
  });

  // Also surface credible WATCH/FUTURE as incremental at-bats candidates for founder review
  const watchAudit = liveNewWatch.map((o) => ({
    opportunityId: o.id,
    demandType: o.demandSignalTypeLabel || o.demandSignalType,
    organization: o.organizationName || "",
    opportunity: o.title || "",
    actionabilityV3: o.actionabilityV3,
    failureClass: o.hygieneV3?.failureClass || null,
    timing: o.eventStartDate || o.startDate || "",
    location: o.destinationStatus || o.location || "",
    source:
      o.officialSource ||
      (o.evidenceSources || [])[0]?.url ||
      "",
    weeklyState: o.weeklyState,
    newnessStatus: o.newnessStatus,
  }));

  const passedAudit = manualAudit.filter((a) => a.autoAuditResult === "PASS_PENDING_HUMAN");
  const precisionDenom = liveNewActionable.length || 0;
  const precisionPct = precisionDenom
    ? Math.round((1000 * passedAudit.length) / precisionDenom) / 10
    : 0;

  const acts = {};
  for (const o of classified) {
    acts[o.actionabilityV3 || "NONE"] = (acts[o.actionabilityV3 || "NONE"] || 0) + 1;
  }

  const totalQueries = laneResults.reduce((n, r) => n + (r.queryCount || 0), 0);
  const totalUrls = laneResults.reduce((n, r) => n + (r.urls || 0), 0);
  const totalOfficial = laneResults.reduce((n, r) => n + (r.officialUrls || 0), 0);
  const totalSerpUsd = laneYield.reduce((n, r) => n + (r.estimatedSerpUsd || 0), 0);

  const report = {
    version: "gdi_new_opportunities_v1_1_live_bethesda",
    generatedAt: new Date().toISOString(),
    mode: "LIVE_SERP_REPROCESS_HYGIENE",
    hotel: HOTEL,
    archetype,
    baselineCanonicalCount: baselineOpps.length,
    baselineIds,
    integrity: {
      opportunitiesJsonUntouched: true,
      liveWrite: false,
      deploy: false,
      surfeCalls: 0,
      pdlCalls: 0,
    },
    liveResearch: {
      totalQueries,
      totalUrls,
      officialUrls: totalOfficial,
      totalCandidates: (raw.seedCandidates || []).length,
      afterDedupe: (raw.seedCandidates || []).length,
      qualified: afterHygiene.length,
      byActionability: acts,
      hygieneTrue: hygiene.trueActionable?.length || 0,
    },
    moreAtBats: {
      eventOnlyNew: eventOnlyNew.length,
      expandedDemandNew: expandedNew.length,
      overlap: 0,
      eventOnlyActionableNew: eventOnlyActionable.length,
      expandedActionableNew: expandedActionable.length,
      incrementalCredibleAtBats: expandedActionable.filter((o) =>
        passedAudit.some((a) => a.opportunityId === o.id)
      ).length,
      liveNewWatch: liveNewWatch.length,
      note: "TRUE_ACTIONABLE only counts as incremental credible at-bats; WATCH listed separately",
    },
    quality: {
      liveNewActionable: liveNewActionable.length,
      trueAuto: passedAudit.length,
      falseAuto: liveNewActionable.length - passedAudit.length,
      precisionPct,
      sourcePct: precisionDenom
        ? Math.round(
            (1000 * manualAudit.filter((a) => a.sourceOk).length) / precisionDenom
          ) / 10
        : 0,
      thesisPct: precisionDenom
        ? Math.round(
            (1000 * manualAudit.filter((a) => a.thesisOk).length) / precisionDenom
          ) / 10
        : 0,
      actionPct: precisionDenom
        ? Math.round(
            (1000 * manualAudit.filter((a) => a.actionOk).length) / precisionDenom
          ) / 10
        : 0,
      geographyPct: precisionDenom
        ? Math.round(
            (1000 * manualAudit.filter((a) => a.geographyOk).length) / precisionDenom
          ) / 10
        : 0,
    },
    laneYield,
    manualAudit,
    watchAudit,
    liveNewSummaries: liveNew.map((o) => ({
      id: o.id,
      title: o.title,
      demandSignalType: o.demandSignalType,
      weeklyState: o.weeklyState,
      newnessStatus: o.newnessStatus,
      actionability: o.actionabilityV3,
      failureClass: o.hygieneV3?.failureClass || null,
      lane: o.discoveryLane,
      matchScore: o.baselineMatch?.score || 0,
    })),
    speed: {
      totalMs: Date.now() - startedAt,
      reprocessOnly: true,
      parallelized: true,
      concurrency: CONCURRENCY,
      priorLiveRunMs: null,
    },
    cost: {
      estimatedSerpUsd: Number(totalSerpUsd.toFixed(3)),
      serpQueries: totalQueries,
      openaiCalls: laneResults.reduce((n, r) => n + (r.openaiCalls || 0), 0),
      surfe: 0,
      pdl: 0,
      note: "SERP already spent on prior live run; reprocess added $0",
    },
    noise: {
      zeroYieldLanes: laneYield.filter((l) => l.yieldClass === "ZERO_YIELD").map((l) => l.lane),
      lowYieldLanes: laneYield.filter((l) => l.yieldClass === "LOW_YIELD").map((l) => l.lane),
      failReasons: laneYield
        .filter((l) => l.failReason)
        .map((l) => ({ lane: l.lane, reason: l.failReason })),
      invalidClasses: classified
        .filter((o) => o.actionabilityV3 === ACTIONABILITY_V3.INVALID)
        .reduce((acc, o) => {
          const k = o.hygieneV3?.failureClass || "OTHER";
          acc[k] = (acc[k] || 0) + 1;
          return acc;
        }, {}),
    },
  };

  writeJson(path.join(OUT_DIR, "LIVE_AUDIT.json"), report);
  writeJson(path.join(OUT_DIR, "LIVE_CLASSIFIED.json"), { classified, hygieneByState: acts });
  console.log(
    JSON.stringify(
      {
        mode: "reprocess",
        byActionability: acts,
        liveNew: liveNew.length,
        liveNewActionable: liveNewActionable.length,
        liveNewWatch: liveNewWatch.length,
        incrementalCredibleAtBats: report.moreAtBats.incrementalCredibleAtBats,
        precisionPct,
        manualAudit: manualAudit.length,
      },
      null,
      2
    )
  );
}

/**
 * Bounded WATCH second-pass — only VERIFY_* / WATCH_KEEP classes.
 * Merges recovered evidence onto seeds and re-qualifies (dry-run).
 */
async function runWatchSecondPass(watchRows, ctx) {
  const eligible = [];
  const skipped = [];
  for (const o of watchRows || []) {
    const recovery = classifyWatchRecovery(o, { marketTokens: MARKET_TOKENS });
    if (
      recovery.class === WATCH_RECOVERY_CLASS.NOISE ||
      recovery.class === WATCH_RECOVERY_CLASS.SKIP
    ) {
      skipped.push({ id: o.id, title: o.title, class: recovery.class, reason: recovery.reason });
      continue;
    }
    const plan = buildSecondPassQueries(o, { maxQueries: 3 });
    eligible.push({ opp: o, recovery, plan });
  }

  const selected = eligible.slice(0, MAX_SECOND_PASS_CANDIDATES);
  const queryBag = [];
  for (const item of selected) {
    for (const q of item.plan.queries || []) {
      queryBag.push({
        query: q,
        vertical: item.opp.discoveryLane || "WATCH_SECOND_PASS",
        parentId: item.opp.id,
        recoveryClass: item.plan.recoveryClass,
      });
      if (queryBag.length >= MAX_SECOND_PASS_QUERIES) break;
    }
    if (queryBag.length >= MAX_SECOND_PASS_QUERIES) break;
  }

  console.error(
    `[v1.2] second-pass eligible=${eligible.length} selected=${selected.length} queries=${queryBag.length}`
  );

  if (!queryBag.length) {
    return {
      eligibleCount: eligible.length,
      selectedCount: 0,
      skipped,
      queryCount: 0,
      skipPastSerp: 0,
      urls: 0,
      candidates: [],
      promoted: [],
      remainWatch: watchRows || [],
      rejected: [],
      ledger: null,
    };
  }

  const task = {
    vertical: "WATCH_SECOND_PASS",
    note: "v1_2_watch_second_pass",
    lane: "WATCH_SECOND_PASS",
    queries: queryBag.map((q) => q.query),
  };

  const native = await runNativeBlindDiscovery({
    hotelId: HOTEL_ID,
    profile: ctx.profile,
    config: ctx.config,
    maxQueries: queryBag.length,
    maxPagesPerQuery: MAX_PAGES,
    maxExtractBatches: 2,
    searchTasksOverride: [task],
    briefOverride: ctx.brief,
    extractSystemOverride: RECALL_EXTRACT_SYSTEM_V4,
    discoverySourceLabel: `${DISCOVERY_ROUTING_V1_2}:second_pass`,
    stratifiedQueryBudget: false,
    serpLocale: { hl: "en", gl: "us" },
  });

  const deduped = dedupeDiscoveryCandidates(native.rows || []);
  const mapped = mapWebhoundOpportunityUniverse(deduped.candidates || [], HOTEL_ID);
  const secondCandidates = (mapped.candidates || []).map((c) =>
    enrichCandidateRoutingV12(
      enrichCommercialEvidenceV4(
        enrichOpportunityDemandSignal({
          ...c,
          discoveryLane: c.discoveryLane || "WATCH_SECOND_PASS",
          discoverySource: DISCOVERY_ROUTING_V1_2,
        }),
        { nowDate: AS_OF }
      ),
      { nowDate: AS_OF, marketTokens: MARKET_TOKENS }
    )
  );

  // Merge: re-seed primary watch + second-pass finds, dry-run qualify + hygiene
  const mergeSeeds = [
    ...(watchRows || []).map((o) =>
      enrichCandidateRoutingV12(
        { ...o, secondPassParent: true },
        { nowDate: AS_OF, marketTokens: MARKET_TOKENS }
      )
    ),
    ...secondCandidates,
  ];

  let requalified = [];
  if (mergeSeeds.length) {
    const research = await runGroupDemandResearch({
      hotelId: HOTEL_ID,
      dryRun: true,
      discoveryMode: "INDEPENDENT_CANDIDATES",
      seedCandidates: mergeSeeds,
      webhoundCalls: [],
      allowWebhoundWithoutFlag: false,
    });
    requalified = (research?.opportunities || []).map((o) =>
      enrichOpportunityDemandSignal(
        enrichCommercialEvidenceV4(o, { nowDate: AS_OF })
      )
    );
  }
  const hygiene2 = applyDiscoveryHygieneV3(HOTEL_ID, requalified, {
    nowDate: AS_OF,
    subjectHotel: { hotelId: HOTEL_ID, name: HOTEL.name, city: HOTEL.name },
    marketTokens: MARKET_TOKENS,
  });
  const after2 = hygiene2.rows || [];

  const parentTitles = new Set(
    (watchRows || []).map((o) => String(o.title || "").toLowerCase().slice(0, 48))
  );
  const promoted = after2.filter(
    (o) => o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE
  );
  const remainWatch = after2.filter(
    (o) =>
      o.actionabilityV3 === ACTIONABILITY_V3.VALID_WATCH ||
      o.actionabilityV3 === ACTIONABILITY_V3.VALID_FUTURE
  );
  const rejected = after2.filter(
    (o) =>
      o.actionabilityV3 === ACTIONABILITY_V3.INVALID ||
      o.actionabilityV3 === ACTIONABILITY_V3.INSUFFICIENT
  );

  const urls = (native.ledger?.serpDigest || []).reduce(
    (n, d) => n + (d.urlCount || 0),
    0
  );

  return {
    eligibleCount: eligible.length,
    selectedCount: selected.length,
    skipped,
    selected: selected.map((s) => ({
      id: s.opp.id,
      title: s.opp.title,
      recoveryClass: s.plan.recoveryClass,
      reason: s.plan.reason,
      queries: s.plan.queries,
    })),
    queryCount: native.ledger?.serpQueries || queryBag.length,
    skipPastSerp: native.ledger?.skipPastSerp || 0,
    urls,
    pagesFetched: native.ledger?.pagesFetched || 0,
    openaiCalls: native.ledger?.openaiCalls || 0,
    serpCharged: native.ledger?.serpCharged || 0,
    candidates: secondCandidates,
    afterHygiene: after2,
    promoted,
    remainWatch,
    rejected,
    parentTitleCount: parentTitles.size,
    ledger: native.ledger,
  };
}

async function main() {
  if (process.argv.includes("--reprocess-only")) {
    await reprocessFromRaw();
    return;
  }
  const force = process.argv.includes("--force");
  const outRaw = path.join(OUT_DIR, "LIVE_RAW.json");
  if (fs.existsSync(outRaw) && !force) {
    console.error("LIVE_RAW exists — pass --force to re-run live SERP, or --reprocess-only");
    process.exit(0);
  }

  const startedAt = Date.now();
  const config = loadHotelDemandConfig(HOTEL_ID);
  if (!config) throw new Error("missing_bethesda_gdi_config");
  const profile = buildHotelGroupDemandProfile(HOTEL_ID);
  const archetype = inferDemandArchetype(config, { rooms: HOTEL.rooms });
  const yearNear = `(2026 OR 2027)`;
  const brief = renderRecallV4Brief({
    hotelName: HOTEL.name,
    hotelId: HOTEL_ID,
    market: HOTEL.market,
    rooms: HOTEL.rooms,
    archetype,
    peakRoomsMin: config?.commercialPriorities?.coreTargetPeakRoomsMin,
    peakRoomsMax: config?.commercialPriorities?.coreTargetPeakRoomsMax,
  });
  const orgCache = createOrgResearchReuseCache();

  const baselinePath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    HOTEL_ID,
    "opportunities.json"
  );
  const baselineDoc = JSON.parse(fs.readFileSync(baselinePath, "utf8"));
  const baselineOpps = baselineDoc.opportunities || [];
  if (baselineOpps.length !== 38) {
    console.error(`WARN baseline count=${baselineOpps.length} expected 38`);
  }
  const baselineSnaps = baselineOpps.map((o) => snapshotBaselineOpportunity(o));
  const baselineIds = baselineOpps.map((o) => o.id).sort();

  const ctx = { profile, config, brief, yearNear, orgCache };

  console.error(
    `[v1.2] LIVE Bethesda audit routing=${DISCOVERY_ROUTING_V1_2} archetype=${archetype} lanes=${AUDIT_LANES.length} concurrency=${CONCURRENCY}`
  );

  const laneResults = await poolMap(AUDIT_LANES, CONCURRENCY, (lane) =>
    runLaneDiscovery(lane, ctx)
  );

  // Merge candidates across lanes
  const allCandidates = [];
  for (const lr of laneResults) {
    for (const c of lr.candidates || []) allCandidates.push(c);
  }
  const merged = dedupeDiscoveryCandidates(allCandidates);

  const seedCandidates = (merged.candidates || []).map((c) =>
    enrichCommercialEvidenceV4(
      enrichOpportunityDemandSignal({
        ...c,
        demandSignalType: c.demandSignalType || classifyDemandSignalType(c),
      }),
      { nowDate: AS_OF }
    )
  );

  let qualified = [];
  const tQual0 = Date.now();
  if (seedCandidates.length) {
    const research = await runGroupDemandResearch({
      hotelId: HOTEL_ID,
      dryRun: true,
      discoveryMode: "INDEPENDENT_CANDIDATES",
      seedCandidates,
      webhoundCalls: [],
      allowWebhoundWithoutFlag: false,
    });
    qualified = (research?.opportunities || []).map((o) =>
      enrichOpportunityDemandSignal(
        enrichCommercialEvidenceV4(o, { nowDate: AS_OF })
      )
    );
  }
  const qualifyMs = Date.now() - tQual0;

  // Hygiene V3 on qualified set (returns .rows, not .opportunities)
  const hygiene = applyDiscoveryHygieneV3(HOTEL_ID, qualified, {
    nowDate: AS_OF,
    subjectHotel: { hotelId: HOTEL_ID, name: HOTEL.name, city: "Bethesda" },
    marketTokens: MARKET_TOKENS,
  });
  let afterHygiene = hygiene.rows || [];

  // V1.2: bounded WATCH second-pass before final NEW classification
  const watchBeforeSecond = afterHygiene.filter(
    (o) =>
      o.actionabilityV3 === ACTIONABILITY_V3.VALID_WATCH ||
      o.actionabilityV3 === ACTIONABILITY_V3.VALID_FUTURE
  );
  console.error(
    `[v1.2] primary done qualified=${afterHygiene.length} watch=${watchBeforeSecond.length} — starting second-pass`
  );
  const secondPass = await runWatchSecondPass(watchBeforeSecond, ctx);
  writeJson(path.join(OUT_DIR, "LIVE_SECOND_PASS.json"), {
    generatedAt: new Date().toISOString(),
    ...secondPass,
    candidates: (secondPass.candidates || []).map((c) => ({
      id: c.id,
      title: c.title,
      officialSource: c.officialSource,
      actionabilityV3: c.actionabilityV3,
      housingStatus: c.housingStatus,
      pageSignalClass: c.pageSignalClass,
    })),
    afterHygiene: (secondPass.afterHygiene || []).map((o) => ({
      id: o.id,
      title: o.title,
      actionabilityV3: o.actionabilityV3,
      failureClass: o.hygieneV3?.failureClass || null,
      officialSource: o.officialSource,
    })),
  });

  // Prefer second-pass requalified watch universe when present; keep primary TRUE/INVALID
  if ((secondPass.afterHygiene || []).length) {
    const nonWatchPrimary = afterHygiene.filter(
      (o) =>
        o.actionabilityV3 !== ACTIONABILITY_V3.VALID_WATCH &&
        o.actionabilityV3 !== ACTIONABILITY_V3.VALID_FUTURE
    );
    const byTitle = new Map();
    for (const o of [...nonWatchPrimary, ...(secondPass.afterHygiene || [])]) {
      const k = String(o.title || o.id || "").toLowerCase();
      const prev = byTitle.get(k);
      if (!prev) {
        byTitle.set(k, o);
        continue;
      }
      // Prefer TRUE over WATCH over others
      const rank = (x) =>
        x.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE
          ? 3
          : x.actionabilityV3 === ACTIONABILITY_V3.VALID_WATCH ||
              x.actionabilityV3 === ACTIONABILITY_V3.VALID_FUTURE
            ? 2
            : 1;
      if (rank(o) >= rank(prev)) byTitle.set(k, o);
    }
    afterHygiene = [...byTitle.values()];
  }

  // Classify each vs baseline
  const classified = afterHygiene.map((o) => {
    const match = matchAgainstBaseline(o, baselineSnaps);
    const signal =
      o.demandSignalType || classifyDemandSignalType(o);
    const isEvent = signal === DEMAND_SIGNAL_TYPE.EVENT;
    const lane =
      o.discoveryLane ||
      Object.entries(LANE_TO_SIGNAL).find(([, v]) => v === signal)?.[0] ||
      null;
    let weeklyState = "NEW";
    let newnessStatus = "NEW_TO_GDI";
    if (match.matched) {
      weeklyState = "UNCHANGED";
      newnessStatus = "EXISTING_IN_GDI";
      // Reactivation heuristic
      if (
        o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE ||
        o.actionability === ACTIONABILITY_V3.TRUE_ACTIONABLE
      ) {
        const prior = baselineOpps.find((b) => b.id === match.priorId);
        const priorWasWeak =
          prior &&
          /WATCH|INSUFFICIENT|FUTURE/i.test(
            String(prior.actionabilityV3 || prior.commercialStatus || "")
          );
        if (priorWasWeak) {
          weeklyState = "REACTIVATED";
        } else {
          weeklyState = "UPDATED";
        }
      }
    }
    return {
      ...o,
      demandSignalType: signal,
      demandSignalTypeLabel: demandSignalTypeLabel(signal),
      discoveryLane: lane,
      baselineMatch: match,
      weeklyState,
      newnessStatus,
      isEventLane: isEvent,
    };
  });

  const liveNew = classified.filter((o) => o.newnessStatus === "NEW_TO_GDI");
  const liveNewActionable = liveNew.filter(
    (o) =>
      o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE ||
      o.actionability === ACTIONABILITY_V3.TRUE_ACTIONABLE
  );
  const eventOnlyNew = liveNew.filter((o) => o.isEventLane);
  const expandedNew = liveNew.filter((o) => !o.isEventLane);
  const eventOnlyActionable = eventOnlyNew.filter(
    (o) =>
      o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE ||
      o.actionability === ACTIONABILITY_V3.TRUE_ACTIONABLE
  );
  const expandedActionable = expandedNew.filter(
    (o) =>
      o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE ||
      o.actionability === ACTIONABILITY_V3.TRUE_ACTIONABLE
  );

  // Per-lane rollup
  const laneYield = laneResults.map((lr) => {
    const laneCands = classified.filter(
      (o) =>
        o.discoveryLane === lr.lane ||
        o.demandSignalType === lr.demandSignalType
    );
    const actionable = laneCands.filter(
      (o) =>
        o.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE ||
        o.actionability === ACTIONABILITY_V3.TRUE_ACTIONABLE
    );
    const neu = laneCands.filter((o) => o.newnessStatus === "NEW_TO_GDI");
    const qualifiedCount = laneCands.length;
    const yieldClass = classifyLaneYield({
      queries: lr.queryCount,
      urls: lr.urls,
      candidates: (lr.candidates || []).length,
      trueActionable: actionable.filter((o) => o.newnessStatus === "NEW_TO_GDI")
        .length,
      newOpportunities: neu.length,
    });
    let failReason = null;
    if (!(lr.candidates || []).length) {
      failReason = lr.serpErrors?.length
        ? "SEARCH_TOO_GENERIC"
        : lr.extractionEmpty
          ? "EXTRACTION_FAILURE"
          : "NO_RESULTS";
    }
    return {
      lane: lr.lane,
      queries: lr.queryCount,
      urls: lr.urls,
      officialUrls: lr.officialUrls,
      candidates: (lr.candidates || []).length,
      qualified: qualifiedCount,
      actionable: actionable.length,
      neu: neu.length,
      trueActionableNew: actionable.filter((o) => o.newnessStatus === "NEW_TO_GDI")
        .length,
      yieldClass,
      failReason,
      elapsedMs: lr.elapsedMs,
      serpCharged: lr.serpCharged,
      openaiCalls: lr.openaiCalls,
      estimatedSerpUsd: Number(((lr.serpCharged || lr.queryCount) * 0.01).toFixed(3)),
    };
  });

  const totalQueries = laneResults.reduce((n, r) => n + (r.queryCount || 0), 0);
  const totalUrls = laneResults.reduce((n, r) => n + (r.urls || 0), 0);
  const totalUrlsAfterPast = laneResults.reduce(
    (n, r) => n + (r.urlsAfterPastScreen || r.urls || 0),
    0
  );
  const totalSkipPast = laneResults.reduce((n, r) => n + (r.skipPastSerp || 0), 0);
  const totalOfficial = laneResults.reduce((n, r) => n + (r.officialUrls || 0), 0);
  const totalSerpUsd =
    laneYield.reduce((n, r) => n + (r.estimatedSerpUsd || 0), 0) +
    Number((((secondPass.serpCharged || secondPass.queryCount || 0) * 0.01)).toFixed(3));

  const acts = afterHygiene.reduce((acc, o) => {
    const k = o.actionabilityV3 || "NONE";
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, {});
  const invalidPast = afterHygiene.filter(
    (o) =>
      o.actionabilityV3 === ACTIONABILITY_V3.INVALID &&
      /PAST/i.test(String(o.hygieneV3?.failureClass || ""))
  ).length;
  const officialPreserved = afterHygiene.filter(
    (o) => Boolean(o.officialSource) || Boolean(o.isOfficialSource)
  ).length;
  const geoConflictInvalid = afterHygiene.filter(
    (o) =>
      o.geoConflict ||
      (o.hygieneV3?.notes || []).some((n) => /GEO_CONFLICT/i.test(n))
  ).length;

  // Manual audit stubs for NEW actionable — fields for founder review
  const manualAudit = liveNewActionable.map((o) => {
    const thesis =
      o.hotelDemandThesis ||
      o.housingEvidence ||
      o.roomDemandEvidence ||
      o.whyHotelDemand ||
      "";
    const source =
      o.officialSource ||
      (o.evidenceSources || [])[0]?.url ||
      (o.sources || [])[0]?.url ||
      "";
    const audit = {
      opportunityId: o.id,
      demandType: o.demandSignalTypeLabel || o.demandSignalType,
      organization: o.organizationName || "",
      opportunity: o.title || "",
      source,
      timing: o.eventStartDate || o.startDate || o.eventYear || "",
      location:
        o.eventLocation || o.destinationStatus || o.hotelDemandLocation || o.location || "",
      roomDemandThesis: thesis,
      venueStatus: o.venueStatus || null,
      housingStatus: o.housingStatus || null,
      sourcingStatus: o.sourcingStatusCommercial || o.venueSourcingStatus || null,
      hotelFit: o.hotelFitScore ?? o.productFit?.hotelFitScore ?? null,
      geographicFit: o.demandTerritoryFit || o.geographicFit?.status || null,
      suggestedAction: o.recommendedAction || o.suggestedAction || null,
      whyNew: "No canonical match against Bethesda 38 (score < 55)",
      whyEventSearchWouldMiss:
        o.demandSignalType === DEMAND_SIGNAL_TYPE.EVENT
          ? "Would be found by event search"
          : `demandSignalType=${o.demandSignalType} — outside event/association-only query families`,
      potentialRoomNights: o.potentialRoomNights ?? null,
      potentialRoomNightsStatus: o.potentialRoomNightsStatus || "UNKNOWN",
      sourceOk: Boolean(source),
      thesisOk: Boolean(String(thesis).trim()),
      actionOk: Boolean(o.recommendedAction || o.suggestedAction || o.venueSourcingStatus),
      geographyOk: Boolean(
        o.eventLocation || o.demandTerritoryFit || o.destinationStatus || o.location
      ),
      failureHint: failureReason(o, laneResults.find((r) => r.lane === o.discoveryLane)),
      autoAuditResult:
        Boolean(source) && Boolean(String(thesis).trim()) ? "PASS_PENDING_HUMAN" : "FAIL_WEAK",
    };
    return audit;
  });

  const passedAudit = manualAudit.filter((a) => a.autoAuditResult === "PASS_PENDING_HUMAN");
  const precisionDenom = liveNewActionable.length || 0;
  const precisionPct = precisionDenom
    ? Math.round((1000 * passedAudit.length) / precisionDenom) / 10
    : 0;

  const pack = buildDemandLaneSearchTasks({
    geos: HOTEL.geos,
    archetype,
    maxLanes: 15,
    maxQueriesPerLane: QUERIES_PER_LANE,
  });

  const report = {
    version: "gdi_new_opportunities_v1_2_live_bethesda",
    routingVersion: DISCOVERY_ROUTING_V1_2,
    generatedAt: new Date().toISOString(),
    mode: "LIVE_SERP_DRY_RUN_QUALIFY_V1_2",
    hotel: HOTEL,
    archetype,
    baselineCanonicalCount: baselineOpps.length,
    baselineIds,
    integrity: {
      opportunitiesJsonUntouched: true,
      liveWrite: false,
      deploy: false,
      surfeCalls: 0,
      pdlCalls: 0,
    },
    funnel: {
      serpUrls: totalUrls,
      pastSkippedEarly: totalSkipPast,
      urlsAfterPastScreen: totalUrlsAfterPast,
      fetched: laneResults.reduce((n, r) => n + (r.pagesFetched || 0), 0),
      candidates: allCandidates.length,
      afterDedupe: merged.candidates?.length || 0,
      invalidPast,
      watchBeforeSecondPass: watchBeforeSecond.length,
      secondPassEligible: secondPass.eligibleCount,
      secondPassQueries: secondPass.queryCount,
      secondPassCandidates: (secondPass.candidates || []).length,
      secondPassPromotedTrue: (secondPass.promoted || []).length,
      secondPassRemainWatch: (secondPass.remainWatch || []).length,
      trueActionable: acts[ACTIONABILITY_V3.TRUE_ACTIONABLE] || 0,
      validWatch: acts[ACTIONABILITY_V3.VALID_WATCH] || 0,
      validFuture: acts[ACTIONABILITY_V3.VALID_FUTURE] || 0,
      insufficient: acts[ACTIONABILITY_V3.INSUFFICIENT] || 0,
      invalid: acts[ACTIONABILITY_V3.INVALID] || 0,
      officialSourcePreserved: officialPreserved,
      geoConflictFlags: geoConflictInvalid,
    },
    liveResearch: {
      totalQueries: totalQueries + (secondPass.queryCount || 0),
      primaryQueries: totalQueries,
      secondPassQueries: secondPass.queryCount || 0,
      totalUrls: totalUrls + (secondPass.urls || 0),
      pastSkippedEarly: totalSkipPast + (secondPass.skipPastSerp || 0),
      officialUrls: totalOfficial,
      totalCandidates: allCandidates.length,
      afterDedupe: merged.candidates?.length || 0,
      qualified: afterHygiene.length,
      seedCandidates: seedCandidates.length,
      byActionability: acts,
    },
    secondPass: {
      eligible: secondPass.eligibleCount,
      selected: secondPass.selectedCount,
      queries: secondPass.queryCount,
      promotedTrue: (secondPass.promoted || []).length,
      remainWatch: (secondPass.remainWatch || []).length,
      rejected: (secondPass.rejected || []).length,
      selectedPlans: secondPass.selected || [],
      skippedNoise: secondPass.skipped || [],
    },
    moreAtBats: {
      eventOnlyNew: eventOnlyNew.length,
      expandedDemandNew: expandedNew.length,
      overlap: 0,
      incrementalCredibleAtBats: liveNewActionable.filter((o) =>
        passedAudit.some((a) => a.opportunityId === o.id)
      ).length,
      eventOnlyActionableNew: eventOnlyActionable.length,
      expandedActionableNew: expandedActionable.length,
      note: "Incremental = NEW TRUE_ACTIONABLE that pass source+thesis auto-audit",
    },
    quality: {
      liveNewActionable: liveNewActionable.length,
      trueAuto: passedAudit.length,
      falseAuto: liveNewActionable.length - passedAudit.length,
      precisionPct,
      sourcePct: precisionDenom
        ? Math.round(
            (1000 * manualAudit.filter((a) => a.sourceOk).length) / precisionDenom
          ) / 10
        : 0,
      thesisPct: precisionDenom
        ? Math.round(
            (1000 * manualAudit.filter((a) => a.thesisOk).length) / precisionDenom
          ) / 10
        : 0,
      actionPct: precisionDenom
        ? Math.round(
            (1000 * manualAudit.filter((a) => a.actionOk).length) / precisionDenom
          ) / 10
        : 0,
      geographyPct: precisionDenom
        ? Math.round(
            (1000 * manualAudit.filter((a) => a.geographyOk).length) / precisionDenom
          ) / 10
        : 0,
    },
    laneYield,
    manualAudit,
    liveNewSummaries: liveNew.map((o) => ({
      id: o.id,
      title: o.title,
      demandSignalType: o.demandSignalType,
      weeklyState: o.weeklyState,
      newnessStatus: o.newnessStatus,
      actionability: o.actionabilityV3 || o.actionability,
      lane: o.discoveryLane,
      officialSource: o.officialSource || null,
      housingStatus: o.housingStatus || null,
      geoConflict: o.geoConflict || false,
    })),
    speed: {
      totalMs: Date.now() - startedAt,
      qualifyMs,
      byLane: laneResults.map((r) => ({ lane: r.lane, elapsedMs: r.elapsedMs })),
      parallelized: true,
      concurrency: CONCURRENCY,
      orgCacheHits: orgCache.stats().reusedHits,
      organizationsCached: orgCache.stats().organizationsCached,
      duplicateFetchesAvoided: merged.duplicatesRemoved || 0,
    },
    cost: {
      estimatedSerpUsd: Number(totalSerpUsd.toFixed(3)),
      serpQueries: totalQueries + (secondPass.queryCount || 0),
      openaiCalls:
        laneResults.reduce((n, r) => n + (r.openaiCalls || 0), 0) +
        (secondPass.openaiCalls || 0),
      surfe: 0,
      pdl: 0,
    },
    demandLanePlanQueryBudget: pack.queryBudget,
    noise: {
      zeroYieldLanes: laneYield.filter((l) => l.yieldClass === "ZERO_YIELD").map((l) => l.lane),
      lowYieldLanes: laneYield.filter((l) => l.yieldClass === "LOW_YIELD").map((l) => l.lane),
      failReasons: laneYield
        .filter((l) => l.failReason)
        .map((l) => ({ lane: l.lane, reason: l.failReason })),
      invalidClasses: afterHygiene
        .filter((o) => o.actionabilityV3 === ACTIONABILITY_V3.INVALID)
        .reduce((acc, o) => {
          const k = o.hygieneV3?.failureClass || "OTHER";
          acc[k] = (acc[k] || 0) + 1;
          return acc;
        }, {}),
    },
  };

  writeJson(path.join(OUT_DIR, "LIVE_RAW.json"), {
    laneResults: laneResults.map((r) => ({
      ...r,
      candidates: r.candidates,
      ledger: r.ledger,
    })),
    seedCandidates,
    classified,
  });
  writeJson(path.join(OUT_DIR, "LIVE_AUDIT.json"), report);
  writeJson(path.join(OUT_DIR, "BASELINE_SNAPSHOT.json"), {
    count: baselineOpps.length,
    ids: baselineIds,
    snapAt: new Date().toISOString(),
  });

  console.log(
    JSON.stringify(
      {
        outDir: OUT_DIR,
        routingVersion: DISCOVERY_ROUTING_V1_2,
        totalQueries: totalQueries + (secondPass.queryCount || 0),
        pastSkippedEarly: totalSkipPast,
        candidates: allCandidates.length,
        qualified: afterHygiene.length,
        byActionability: acts,
        watchBeforeSecond: watchBeforeSecond.length,
        secondPassPromoted: (secondPass.promoted || []).length,
        liveNew: liveNew.length,
        liveNewActionable: liveNewActionable.length,
        incrementalCredibleAtBats: report.moreAtBats.incrementalCredibleAtBats,
        precisionPct,
        estimatedSerpUsd: report.cost.estimatedSerpUsd,
        totalMs: report.speed.totalMs,
        baselineUntouched: true,
        surfe: 0,
        pdl: 0,
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
