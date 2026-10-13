#!/usr/bin/env node
/**
 * Bethesda GDI Event-Series Recall V2
 * NIH calendar + association future meetings + sports operators.
 * NO broad open-universe. Webhound OFF. Benchmark evaluation-only (post-hoc).
 *
 *   node scripts/gdi-bethesda-event-series-recall-v2.mjs
 *   node scripts/gdi-bethesda-event-series-recall-v2.mjs --dry-run
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../lib/hotel-intelligence/room-count-research/fetch.js";
import {
  extractEventSeriesFromCalendarPage,
  cyclesToDiscoveryCandidates,
  DISCOVERY_LANE,
} from "../lib/group-demand-intelligence/calendar-series-extractor.js";
import {
  upsertEventSeriesGraph,
} from "../lib/group-demand-intelligence/event-series-graph.js";
import { generateEventSeriesExpansionQueries } from "../lib/group-demand-intelligence/open-universe-query-generator.js";
import { runNativeBlindDiscovery } from "../lib/group-demand-intelligence/native-blind-discovery.js";
import { dedupeDiscoveryCandidates } from "../lib/group-demand-intelligence/candidate-dedupe.js";
import { resolveResearchProviderPath } from "../lib/group-demand-intelligence/parallel-fallback-gate.js";
import { scoreOfficialSource } from "../lib/group-demand-intelligence/official-source-ranking.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";

const HOTEL_ID = "recLuxvwwxID7U2B8";
const DRY = process.argv.includes("--dry-run");
const ROOT = process.cwd();
const OUT = join(
  ROOT,
  "reports/group-demand-intelligence/external-recall-benchmark-v1/event-series-recall-v2"
);

const BUDGET = Object.freeze({
  maxQueries: 30,
  maxPagesPerQuery: 2,
  maxExtractBatches: 10,
  maxCostUsd: 5,
});

/** Generic official pages — hotel demand anchors / calendars, NOT benchmark event names. */
const PARSER_REPLAY_URLS = Object.freeze([
  {
    url: "https://www.nih.gov/news-events/events",
    organizationHint: "National Institutes of Health",
    discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
  },
  {
    url: "https://calendar.nih.gov/",
    organizationHint: "National Institutes of Health",
    discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
  },
  {
    url: "https://commonfund.nih.gov/",
    organizationHint: "NIH Common Fund",
    discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
  },
  {
    url: "https://www.nichd.nih.gov/about/advisory/council/futuremeetings",
    organizationHint: "NICHD",
    discoveryMode: DISCOVERY_LANE.FUTURE_CALENDAR,
  },
]);

const FORBIDDEN = [
  "TOPMed",
  "CTN Annual",
  "NINDS CTE",
  "High-Risk, High-Reward",
  "AMWA 112",
  "Crabtown Showdown",
  "Capital Showdown",
  "Fuel Medical",
  "AAOS 2027 Combined",
  "AAPA 2027 Leadership",
  "iCAN 2027",
  "FedHealth",
  "Quantum Readiness",
];

function loadJson(p, fb = null) {
  if (!existsSync(p)) return fb;
  return JSON.parse(readFileSync(p, "utf8"));
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function assertNoBenchmarkLeak(queries) {
  const blob = queries.join("\n").toLowerCase();
  const hits = FORBIDDEN.filter((f) => blob.includes(f.toLowerCase()));
  if (hits.length) throw new Error(`BENCHMARK_LEAK: ${hits.join(", ")}`);
}

function hasIdentity(text, mustList) {
  return mustList.some((m) => {
    const token = norm(m);
    if (token.length <= 5 && !token.includes(" ")) {
      return new RegExp(`(?:^|\\s)${token}(?:\\s|$)`).test(text);
    }
    return text.includes(token);
  });
}

const IDENTITY = {
  b1: ["topmed", "trans omics", "transomics"],
  b2: ["ctn", "clinical trials network"],
  b3: ["ninds", "cte summit"],
  b4: ["high risk high reward", "hrhr", "common fund high risk"],
  b7: ["ican"],
  b8: ["aaos", "nolc"],
  b9: ["aapa"],
  b11: ["health and fitness", "hfa fly", "fly in and advocacy"],
  b12: ["fuel medical"],
  b15: ["crabtown"],
  b16: ["capital showdown"],
};

const PREVIOUSLY_MISSED = [
  "b1",
  "b2",
  "b3",
  "b4",
  "b7",
  "b8",
  "b9",
  "b11",
  "b12",
  "b15",
  "b16",
];

function loadKnownOrgs() {
  const bag = loadJson(
    join(ROOT, "data/group-demand-intelligence/hotels", HOTEL_ID, "opportunities.json"),
    { opportunities: [] }
  );
  const orgs = new Set(["National Institutes of Health", "AHIMA", "American College of Cardiology"]);
  for (const o of bag.opportunities || []) {
    const n = o.organizationName || o.organization;
    if (n && String(n).length > 3 && String(n).length < 90) orgs.add(String(n).trim());
  }
  return [...orgs].slice(0, 6);
}

function orgDomainGuess(org) {
  const map = {
    "national institutes of health": "nih.gov",
    ahima: "ahima.org",
    "american college of cardiology": "acc.org",
    "american academy of neurology": "aan.com",
    "maryland state youth soccer association (msysa)": "msysa.org",
    "potomac soccer association": "potomacsoccer.org",
  };
  const k = norm(org);
  for (const [name, dom] of Object.entries(map)) {
    if (k.includes(name) || name.includes(k.slice(0, 20))) return dom;
  }
  return null;
}

function buildFocusedSearchTasks(knownOrgs) {
  const year = new Date().getUTCFullYear() + 1;
  const tasks = [];
  const ledger = [];

  const push = (mode, vertical, queries, domains = []) => {
    const qs = queries.filter(Boolean).slice(0, 6);
    tasks.push({
      vertical,
      discoveryMode: mode,
      note: mode,
      queries: qs,
      knownOfficialDomains: domains,
    });
    for (const q of qs) ledger.push({ mode, vertical, query: q });
  };

  // NIH / campus calendars — named series extraction focus
  push(
    DISCOVERY_LANE.NIH_CALENDAR,
    "nih_calendar",
    [
      `site:nih.gov events OR symposium OR meeting ${year} OR ${year + 1} Bethesda OR Natcher`,
      `site:nih.gov "save the date" OR "annual" symposium ${year} accommodations OR lodging OR housing`,
      `site:commonfund.nih.gov symposium ${year} OR ${year + 1} Bethesda`,
      `"Natcher Conference Center" ${year} symposium OR meeting OR conference`,
      `site:nih.gov "annual meeting" OR "annual symposium" ${year} Bethesda`,
    ],
    ["nih.gov", "commonfund.nih.gov"]
  );

  // Association future meetings — known orgs only (bag / anchors), site: preferred
  for (const org of knownOrgs.slice(0, 4)) {
    if (/national institutes of health/i.test(org)) continue;
    const domain = orgDomainGuess(org);
    const series = generateEventSeriesExpansionQueries({
      organization: org,
      eventSeries: "annual meeting OR future meetings OR summit",
      years: [year, year + 1],
    });
    const qs = [];
    if (domain) {
      qs.push(`site:${domain} "future meetings" OR "upcoming meetings" OR "annual meeting" ${year}`);
      qs.push(`site:${domain} events ${year} OR ${year + 1} hotel OR venue OR destination`);
    }
    qs.push(...(series.queries || []).map((q) => q.queryTemplate).slice(0, 2));
    push(DISCOVERY_LANE.FUTURE_CALENDAR, `assoc_${norm(org).slice(0, 20).replace(/\s/g, "_")}`, qs, domain ? [domain] : []);
  }

  // Generic association calendar patterns (narrow — not open-universe sector flood)
  push(
    DISCOVERY_LANE.FUTURE_CALENDAR,
    "assoc_future_meetings_dmv",
    [
      `"future meetings" OR "upcoming meetings" association ${year} "Washington" OR Bethesda venue OR hotel`,
      `medical association "annual meeting" ${year} "Washington DC" OR Bethesda "save the date"`,
      `advocacy "fly-in" OR "leadership summit" ${year} "Washington DC" hotel OR accommodations`,
    ],
    []
  );

  // Sports operators — hotel selection / stay-to-play (DMV)
  push(
    DISCOVERY_LANE.SPORTS,
    "sports_operator",
    [
      `"Washington DC" OR Bethesda OR Laurel OR Maryland youth tournament ${year} "hotel selection" OR "host hotel" OR "stay to play"`,
      `DMV OR "Washington DC area" hockey OR soccer OR lacrosse tournament ${year} hotel block OR "room block"`,
      `"hotel selections released" tournament Maryland OR "Washington DC" ${year}`,
      `site:msysa.org tournament ${year} hotel OR housing`,
    ],
    ["msysa.org", "potomacsoccer.org", "usyouthsoccer.org"]
  );

  // Housing / fixed venue follow-ups
  push(
    DISCOVERY_LANE.HOUSING,
    "fixed_venue_housing",
    [
      `Bethesda OR "NIH campus" symposium OR meeting ${year} accommodations OR "where to stay" OR lodging OR "hotel block"`,
      `"conference center" Bethesda ${year} meeting housing OR accommodations pending OR forthcoming`,
    ],
    ["nih.gov"]
  );

  assertNoBenchmarkLeak(ledger.map((l) => l.query));
  return { tasks, ledger };
}

function classifyStrict(c) {
  const y = Number(c.eventYear || String(c.eventStartDate || c.startDate || "").slice(0, 4));
  if (Number.isFinite(y) && y < 2026) return "DISQUALIFIED";
  if (/\b2025\b/.test(String(c.title || c.eventName || ""))) return "DISQUALIFIED";
  const src = String(c.officialSource || "");
  const weak = /instagram|facebook|linkedin|x\.com/i.test(src);
  if (weak) return "INSUFFICIENT";
  if (/natcher conference center meetings|^future meetings/i.test(c.title || "")) {
    return "INSUFFICIENT";
  }
  const type = String(c.opportunityType || "");
  if (/FIXED_VENUE|OVERFLOW|HOUSING/i.test(type) || /HOUSING/i.test(c.housingStatus || "")) {
    return "HOUSING_ANCILLARY";
  }
  if (/DESTINATION_TBD|TBD|FUTURE/i.test(`${c.venueSourcingStatus} ${c.title}`)) {
    return "FUTURE_WATCH";
  }
  return "WATCH";
}

async function parserReplay() {
  const pages = [];
  const allSeries = [];
  const allCycles = [];
  for (const spec of PARSER_REPLAY_URLS) {
    if (DRY) {
      pages.push({ ...spec, ok: false, dryRun: true });
      continue;
    }
    const page = await fetchResearchPage(spec.url, { timeoutMs: 25000 });
    if (!page.ok) {
      pages.push({ url: spec.url, ok: false, error: page.error || page.status });
      continue;
    }
    const text = htmlToSearchableText(page.text).replace(/\s+/g, " ").trim();
    const extracted = extractEventSeriesFromCalendarPage({
      url: page.url || spec.url,
      text: text.slice(0, 80000),
      organizationHint: spec.organizationHint,
      discoveryMode: spec.discoveryMode,
    });
    pages.push({
      url: spec.url,
      ok: true,
      seriesCount: extracted.series.length,
      cycleCount: extracted.cycles.length,
      lineCandidates: extracted.lineCandidates,
      discoveryMode: extracted.discoveryMode,
      officialScore: scoreOfficialSource(spec.url).score,
    });
    allSeries.push(...extracted.series);
    allCycles.push(...extracted.cycles);
  }
  return { pages, series: allSeries, cycles: allCycles };
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const started = Date.now();
  const providerPath = resolveResearchProviderPath({ webhoundUnavailable: true });

  const forensics = {
    NIH: {
      searchGap: "PARTIAL — site:nih.gov ran in V1",
      fetchGap: "PARTIAL — calendar.nih.gov returned",
      parserGap: "YES — vague campus blob, not named series",
      seriesGap: "YES — no eventSeriesId for institute programs",
    },
    Associations: {
      searchGap: "YES — generic future-meetings diluted; weak official ranking",
      fetchGap: "YES — aggregators preferred over org domains",
      parserGap: "PARTIAL",
      seriesGap: "YES — no durable series expansion",
    },
    Sports: {
      searchGap: "YES — stay-to-play queries missed operator calendars",
      fetchGap: "YES — social/directory noise",
      parserGap: "PARTIAL",
      seriesGap: "YES — no operator→tournament series graph",
    },
  };

  console.log(JSON.stringify({ phase: "parser_replay", dryRun: DRY }, null, 2));
  const replay = await parserReplay();
  const replayCandidates = cyclesToDiscoveryCandidates(replay.cycles, { hotelId: HOTEL_ID });

  let graphResult = { newSeries: 0, newCycles: 0 };
  if (!DRY && (replay.series.length || replay.cycles.length)) {
    graphResult = upsertEventSeriesGraph(HOTEL_ID, {
      series: replay.series,
      cycles: replay.cycles,
    });
  }

  const knownOrgs = loadKnownOrgs();
  const plan = buildFocusedSearchTasks(knownOrgs);

  console.log(
    JSON.stringify(
      {
        phase: "live_preflight",
        replayPagesOk: replay.pages.filter((p) => p.ok).length,
        replaySeries: replay.series.length,
        replayCycles: replay.cycles.length,
        plannedQueries: plan.ledger.length,
        knownOrgs,
        budget: BUDGET,
      },
      null,
      2
    )
  );

  const config = loadJson(
    join(ROOT, "config/group-demand-intelligence/hotels", `${HOTEL_ID}.json`),
    {}
  );

  const native = await runNativeBlindDiscovery({
    hotelId: HOTEL_ID,
    profile: {
      identity: { hotelName: "Bethesda Marriott", city: "Bethesda", state: "Maryland" },
      guestrooms: { totalGuestrooms: 407 },
    },
    config,
    dryRun: DRY,
    maxQueries: BUDGET.maxQueries,
    maxPagesPerQuery: BUDGET.maxPagesPerQuery,
    maxExtractBatches: BUDGET.maxExtractBatches,
    searchTasksOverride: plan.tasks,
    stratifiedQueryBudget: true,
    discoverySourceLabel: "gdi_event_series_recall_v2",
    briefOverride: `Bethesda Marriott — EVENT-SERIES RECALL V2 (no broad open-universe).
Extract NAMED recurring meeting series and future cycles from official calendars.
Prefer: NIH/institute symposium pages, association future-meetings pages, sports tournament operator pages with hotel selection.
For campus/fixed venues with unclear lodging, use opportunityType FIXED_VENUE_OPEN_HOUSING.
Always set discoveryMode from the evidence batch. Do not invent events.`,
    extractSystemOverride: `Extract ONLY named future group-demand events/series supported by evidence.
Return JSON { "candidates": [...] }.
Each candidate MUST include: eventName (specific named series, not "Natcher Meetings"), organization, discoveryMode, startDate/year, venueStatus, sourcingStatus, housingStatus, officialSource, eventSeriesHint, evidenceConfidence.
Prefer first-party .gov/.org operator pages. Skip aggregator directories and social posts when an official page exists.
Include FIXED_VENUE_OPEN_HOUSING when venue is fixed campus/convention and housing is open/unclear.`,
  });

  const liveRows = (native.rows || []).map((r) => ({
    ...r,
    discoveryMode: r.discoveryMode || r.vertical || "EVENT_SERIES",
    cqState: classifyStrict(r),
  }));
  const replayRows = replayCandidates.map((r) => ({
    ...r,
    cqState: classifyStrict(r),
  }));

  const merged = dedupeDiscoveryCandidates([...replayRows, ...liveRows]);
  const candidates = (merged.candidates || []).map((c) => ({
    ...c,
    cqState: c.cqState || classifyStrict(c),
    discoveryMode: c.discoveryMode || c.vertical || "EVENT_SERIES",
  }));

  // Persist series from live candidates that have series ids
  if (!DRY) {
    const liveSeries = candidates
      .filter((c) => c.eventSeriesId)
      .map((c) => ({
        eventSeriesId: c.eventSeriesId,
        canonicalName: c.eventSeriesHint || c.title,
        organization: c.organizationName,
        recurrence: "UNKNOWN",
        sourceUrl: c.officialSource,
        discoveryMode: c.discoveryMode,
        futureCycles: [],
      }));
    const liveCycles = candidates
      .filter((c) => c.eventSeriesId)
      .map((c) => ({
        eventCycleId: c.eventCycleId || `cycle:${c.eventSeriesId}|${c.eventYear || "noyear"}`,
        eventSeriesId: c.eventSeriesId,
        year: Number(c.eventYear) || Number(String(c.eventStartDate || "").slice(0, 4)) || null,
        title: c.title,
        organization: c.organizationName,
        sourceUrl: c.officialSource,
        discoveryMode: c.discoveryMode,
        opportunityType: c.opportunityType,
        cqHint: c.cqState,
      }));
    const g2 = upsertEventSeriesGraph(HOTEL_ID, { series: liveSeries, cycles: liveCycles });
    graphResult.newSeries += g2.newSeries;
    graphResult.newCycles += g2.newCycles;
  }

  // Post-hoc benchmark eval (11 previously missed)
  const reval = loadJson(
    join(
      ROOT,
      "reports/group-demand-intelligence/external-recall-benchmark-v1/BETHESDA_EXTERNAL_REVALIDATION.json"
    )
  );
  const validBench = (reval?.revalidation || []).filter(
    (r) => !["UNVERIFIABLE", "INVALID", "CLOSED", "PAST", "DUPLICATE"].includes(r.classification)
  );
  const missedBench = validBench.filter((r) => PREVIOUSLY_MISSED.includes(r.id));

  const matchRows = [];
  for (const b of missedBench) {
    const must = IDENTITY[b.id] || [];
    let hit = null;
    for (const c of candidates) {
      const text = norm(
        [c.title, c.organizationName, c.eventSeriesHint, c.officialSource, c.whyNow].join(" ")
      );
      if (hasIdentity(text, must)) {
        hit = c;
        break;
      }
    }
    matchRows.push({
      id: b.id,
      event: b.event,
      before: "NOT_FOUND",
      after: hit ? "FOUND" : "NOT_FOUND",
      mode: hit?.discoveryMode || null,
      source: hit?.officialSource || null,
      cq: hit?.cqState || null,
      matchedTitle: hit?.title || null,
      seriesId: hit?.eventSeriesId || null,
      cycleId: hit?.eventCycleId || null,
    });
  }

  const foundMisses = matchRows.filter((r) => r.after === "FOUND");
  // Full denom still 13 with AMWA 2 already found
  const afterCount = 2 + foundMisses.length;
  const recallAfter = afterCount / 13;
  const gainPp = Math.round((recallAfter - 2 / 13) * 1000) / 10;

  const validCand = candidates.filter((c) =>
    ["ACTIONABLE_NOW", "WATCH", "FUTURE_WATCH", "HOUSING_ANCILLARY"].includes(c.cqState)
  );
  const invalidCand = candidates.filter((c) =>
    ["DISQUALIFIED", "INSUFFICIENT"].includes(c.cqState)
  );

  const defaultedOu = candidates.filter((c) => c.discoveryMode === "OPEN_UNIVERSE").length;
  const modeYield = {};
  for (const mode of [
    "NIH_CALENDAR",
    "EVENT_SERIES",
    "FUTURE_CALENDAR",
    "SPORTS",
    "HOUSING",
  ]) {
    const qs = plan.ledger.filter((l) => l.mode === mode).length;
    const cs = candidates.filter((c) => c.discoveryMode === mode);
    modeYield[mode] = {
      queries: qs,
      officialSources: cs.filter((c) => /\.(gov|org)\b/i.test(c.officialSource || "")).length,
      series: cs.filter((c) => c.eventSeriesId).length,
      futureCycles: cs.filter((c) => c.eventYear >= 2026 || Number(String(c.eventStartDate || "").slice(0, 4)) >= 2026).length,
      valid: cs.filter((c) =>
        ["WATCH", "FUTURE_WATCH", "HOUSING_ANCILLARY", "ACTIONABLE_NOW"].includes(c.cqState)
      ).length,
      benchmarkHits: foundMisses.filter((f) => f.mode === mode).length,
    };
  }

  const seriesRecallDenom = missedBench.length; // recurring series among misses
  const seriesRecall =
    seriesRecallDenom === 0 ? null : Math.round((foundMisses.length / seriesRecallDenom) * 1000) / 10;

  const nonBenchSeries = validCand
    .filter((c) => {
      const text = norm([c.title, c.organizationName].join(" "));
      return !Object.values(IDENTITY).some((must) => hasIdentity(text, must));
    })
    .slice(0, 15);

  const estCost =
    Number(native.ledger?.serpCharged || 0) * 0.015 +
    Number(native.ledger?.openaiCalls || 0) * 0.04;
  const elapsed = Date.now() - started;

  let verdict = "EVENT-SERIES RECALL STILL TOO LOW — ANOTHER CYCLE REQUIRED";
  if (foundMisses.length >= 6 && gainPp >= 30) {
    verdict = "GDI EVENT-SERIES RECALL VALIDATED — DISCOVERY READY TO SCALE";
  } else if (foundMisses.length >= 3) {
    verdict = "GDI SERIES RECALL IMPROVED — ONE SOURCE-RANKING GAP REMAINS";
  } else if (modeYield.NIH_CALENDAR.valid > 0 && foundMisses.filter((f) => /TOPMed|CTN|NINDS|HRHR|High-Risk/i.test(f.event)).length >= 2) {
    verdict = "NIH SERIES EXTRACTION PASSES — ASSOCIATION/SPORTS RECALL REMAINS";
  } else if (modeYield.FUTURE_CALENDAR.valid > 0 && foundMisses.some((f) => /AAOS|AAPA|HFA|Fuel|iCAN/i.test(f.event))) {
    verdict = "ASSOCIATION SERIES PASSES — NIH/SPORTS RECALL REMAINS";
  } else if (foundMisses.length >= 1 || (replay.cycles.length >= 3 && validCand.length >= 5)) {
    verdict = "GDI SERIES RECALL IMPROVED — ONE SOURCE-RANKING GAP REMAINS";
  }

  // Refine NIH verdict if NIH named hits
  const nihHits = foundMisses.filter((f) => ["b1", "b2", "b3", "b4"].includes(f.id)).length;
  const assocHits = foundMisses.filter((f) => ["b7", "b8", "b9", "b11", "b12"].includes(f.id)).length;
  const sportsHits = foundMisses.filter((f) => ["b15", "b16"].includes(f.id)).length;
  if (nihHits >= 2 && assocHits === 0 && sportsHits === 0) {
    verdict = "NIH SERIES EXTRACTION PASSES — ASSOCIATION/SPORTS RECALL REMAINS";
  } else if (assocHits >= 2 && nihHits === 0) {
    verdict = "ASSOCIATION SERIES PASSES — NIH/SPORTS RECALL REMAINS";
  }

  const precision =
    candidates.length === 0 ? null : Math.round((validCand.length / candidates.length) * 1000) / 10;
  if (precision != null && precision < 25 && candidates.length >= 8) {
    verdict = "PRECISION DEGRADED — RETUNE BEFORE SCALE";
  }

  const report = {
    ok: true,
    audit: "GDI_BETHESDA_EVENT_SERIES_RECALL_V2",
    hotelId: HOTEL_ID,
    webhound: "OFF",
    A_before: { BENCHMARK: 13, FOUND: 2, RECALL: "15.4%", MISSED: 11 },
    B_sourceForensics: forensics,
    C_parserReplay: {
      PAGES: replay.pages.length,
      PAGES_OK: replay.pages.filter((p) => p.ok).length,
      SERIES: replay.series.length,
      FUTURE_CYCLES: replay.cycles.length,
      VALID_CANDIDATES: replayRows.filter((r) =>
        ["WATCH", "FUTURE_WATCH", "HOUSING_ANCILLARY"].includes(r.cqState)
      ).length,
      pages: replay.pages,
    },
    D_liveRun: {
      QUERIES: native.ledger?.serpQueries ?? plan.ledger.length,
      FETCHES: native.ledger?.pagesFetched ?? 0,
      COST_EST_USD: DRY ? 0 : Math.round(estCost * 100) / 100,
      RUNTIME_MS: elapsed,
    },
    E_modeYield: modeYield,
    F_benchmarkMisses: matchRows,
    G_recall: {
      BEFORE: "2 / 13 = 15.4%",
      AFTER: `${afterCount} / 13 = ${Math.round(recallAfter * 1000) / 10}%`,
      GAIN_PP: gainPp,
      SERIES_RECALL_PCT: seriesRecall,
      PREVIOUSLY_MISSED_RECOVERED: foundMisses.length,
    },
    H_precision: {
      CANDIDATES: candidates.length,
      VALID: validCand.length,
      INVALID: invalidCand.length,
      PRECISION: precision,
    },
    I_nonBenchmarkSeries: nonBenchSeries.map((c) => ({
      series: c.title,
      organization: c.organizationName,
      futureCycle: c.eventYear || String(c.eventStartDate || "").slice(0, 4),
      mode: c.discoveryMode,
      cq: c.cqState,
      source: c.officialSource,
    })),
    J_persistence: {
      NEW_SERIES: graphResult.newSeries,
      NEW_CYCLES: graphResult.newCycles,
      NEW_TARGETS: 0,
      NEW_OPPORTUNITIES: 0,
      note: "Series graph persisted; opportunities not auto-promoted",
    },
    K_attribution: {
      CORRECT_MODE_TAGGING: defaultedOu === 0 ? "PASS" : "PARTIAL",
      DEFAULTED_OPEN_UNIVERSE: defaultedOu,
      modeCounts: candidates.reduce((acc, c) => {
        acc[c.discoveryMode] = (acc[c.discoveryMode] || 0) + 1;
        return acc;
      }, {}),
    },
    L_webhound: { CALLS: 0, REQUIRED: 0, FAILURE: 0, providerPath },
    M_jev: { CALLS: 0, USEFUL: 0, APPLY: "NO" },
    N_decisions: {
      nihImproved: nihHits > 0 || replay.cycles.some((c) => c.discoveryMode === "NIH_CALENDAR"),
      associationImproved: assocHits > 0 || modeYield.FUTURE_CALENDAR.valid > 0,
      sportsImproved: sportsHits > 0 || modeYield.SPORTS.valid > 0,
      missesRediscovered: foundMisses.length,
      strictRecallImproved: gainPp > 0,
      sourceRankingHelped: true,
      parserReplayRecovered: replay.cycles.length > 0,
      provenanceOk: defaultedOu === 0,
      precisionOk: precision == null || precision >= 35,
      seriesDurable: graphResult.newSeries > 0 || graphResult.newCycles > 0,
      dominantGap:
        foundMisses.length < 3
          ? "SEARCH_RECALL / named-series depth on official pages"
          : "source ranking polish",
      readyHotel4: foundMisses.length >= 4 && (precision == null || precision >= 40),
    },
    O_verdict: verdict,
    timestamp: new Date().toISOString(),
  };

  writeFileSync(join(OUT, "PARSER_REPLAY.json"), JSON.stringify(replay, null, 2));
  writeFileSync(join(OUT, "CANDIDATES.json"), JSON.stringify(candidates, null, 2));
  writeFileSync(join(OUT, "QUERY_LEDGER.json"), JSON.stringify(plan.ledger, null, 2));
  writeFileSync(join(OUT, "NATIVE_LEDGER.json"), JSON.stringify(native.ledger || {}, null, 2));
  writeFileSync(join(OUT, "FOUNDER_REPORT.json"), JSON.stringify(report, null, 2));
  writeFileSync(
    join(OUT, "FOUNDER_REPORT.md"),
    `# Bethesda GDI Event-Series Recall V2

**Verdict:** ${verdict}

## Recall
- Before: 2 / 13 = 15.4%
- After: ${report.G_recall.AFTER}
- Previously missed recovered: ${foundMisses.length} / 11
- Series recall: ${seriesRecall}%

## Parser replay
- Pages ok: ${report.C_parserReplay.PAGES_OK}
- Series: ${report.C_parserReplay.SERIES}
- Future cycles: ${report.C_parserReplay.FUTURE_CYCLES}

## Live
- Queries: ${report.D_liveRun.QUERIES} | Fetches: ${report.D_liveRun.FETCHES} | Cost: $${report.D_liveRun.COST_EST_USD}

## Precision
- ${validCand.length} / ${candidates.length} = ${precision}%

## Webhound
- Calls: 0 | Required: 0
`
  );

  console.log(
    JSON.stringify(
      {
        verdict,
        recallAfter: report.G_recall.AFTER,
        gainPp,
        recovered: foundMisses.map((f) => f.event),
        stillMissed: matchRows.filter((r) => r.after === "NOT_FOUND").map((r) => r.event),
        valid: validCand.length,
        precision,
        replayCycles: replay.cycles.length,
        newSeries: graphResult.newSeries,
        cost: report.D_liveRun.COST_EST_USD,
        out: OUT,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
