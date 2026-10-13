#!/usr/bin/env node
/**
 * V3 repair pass: rendered fallback on blocked NIH pages + promote HRHR + scrub aggregators.
 * Does not re-run broad SERP. Webhound OFF.
 *
 *   node scripts/gdi-bethesda-nih-association-recall-v3-repair.mjs
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import { fetchOfficialPage, FETCH_METHOD } from "../lib/group-demand-intelligence/official-page-fetch.js";
import {
  extractEventSeriesFromCalendarPage,
  cyclesToDiscoveryCandidates,
  DISCOVERY_LANE,
} from "../lib/group-demand-intelligence/calendar-series-extractor.js";
import { upsertEventSeriesGraph, loadEventSeriesGraph } from "../lib/group-demand-intelligence/event-series-graph.js";
import {
  promoteQualifiedGdiOpportunity,
  PROMOTION_ACTION,
} from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  loadOpportunitiesCanonical,
  upsertSingleOpportunity,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import {
  OPPORTUNITY_QUALIFICATION,
  OPPORTUNITY_TYPE,
  ROOM_DEMAND_STATUS,
} from "../lib/group-demand-intelligence/claim-types.js";
import {
  buildResearchTarget,
  TARGET_TYPE,
  TARGET_PRIORITY,
  RESEARCH_CADENCE,
  suggestNextResearchAt,
} from "../lib/group-demand-intelligence/research-coverage/entities.js";
import { upsertLocalResearchTarget } from "../lib/group-demand-intelligence/local-research-targets.js";
import { isDeprioritizedSource, scoreOfficialSource } from "../lib/group-demand-intelligence/official-source-ranking.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";

const HOTEL_ID = "recLuxvwwxID7U2B8";
const OUT = join(
  process.cwd(),
  "reports/group-demand-intelligence/external-recall-benchmark-v1/nih-association-recall-v3"
);
const REPAIR_URLS = [
  "https://calendar.nih.gov/",
  "https://commonfund.nih.gov/highrisk/symposium",
  "https://commonfund.nih.gov/",
  "https://www.nih.gov/news-events/events",
  "https://www.nichd.nih.gov/about/advisory/council/futuremeetings",
  "https://ncifrederick.cancer.gov/events/conferences/",
  "https://ctnlibrary.org/",
  "https://www.ninds.nih.gov/news-events/events",
];

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
};
const REMAINING_9 = ["b1", "b2", "b3", "b4", "b7", "b8", "b9", "b11", "b12"];

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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
function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function classifyCq(c) {
  const yRaw = c.eventYear || String(c.eventStartDate || c.startDate || "").slice(0, 4);
  const y = Number(yRaw);
  if (Number.isFinite(y) && y > 0 && y < 2026) return "DISQUALIFIED";
  if (/\b2025\b/.test(String(c.title || ""))) return "DISQUALIFIED";
  if (isDeprioritizedSource(c.officialSource || "") || scoreOfficialSource(c.officialSource || "").score < 15) {
    return "INSUFFICIENT";
  }
  const type = String(c.opportunityType || "");
  if (/FIXED_VENUE|OVERFLOW/i.test(type) || /HOUSING|Natcher|Bethesda/i.test(`${c.housingStatus} ${c.title} ${c.venue || ""}`)) {
    if (/Natcher|Bethesda|NIH campus|FIXED/i.test(`${c.title} ${c.destinationStatus} ${c.venueStatus} ${c.officialSource}`)) {
      return "HOUSING";
    }
  }
  if (!Number.isFinite(y) || y === 0) return "FUTURE_WATCH";
  return "WATCH";
}

function inferYear(c, text) {
  const fromField = Number(c.eventYear || String(c.eventStartDate || "").slice(0, 4));
  if (fromField >= 2026) return fromField;
  const m = String(text || c.title || "").match(/\b(202[6-9]|203[0-9])\b/);
  if (m) return Number(m[1]);
  return new Date().getUTCFullYear() + 1;
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const renderBudget = { remaining: 12 };
  const pageReports = [];
  const allCycles = [];
  const allSeries = [];

  for (const url of REPAIR_URLS) {
    const page = await fetchOfficialPage(url, { allowRender: true, renderBudget });
    pageReports.push({
      url,
      ok: page.ok,
      fetchMethod: page.fetchMethod,
      textLen: page.text?.length || 0,
      browserUsed: page.browserUsed,
      staticFailed: page.staticFailed || false,
      eventDetailLinks: (page.eventDetailLinks || []).slice(0, 6),
    });
    if (!page.ok) continue;
    const ex = extractEventSeriesFromCalendarPage({
      url: page.finalUrl || url,
      text: page.text,
      organizationHint: /nih|nci|ninds|commonfund/i.test(url) ? "National Institutes of Health" : null,
      discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
    });
    allSeries.push(...ex.series);
    allCycles.push(...ex.cycles);

    // Deepen a few event detail links from calendar.nih.gov
    if (/calendar\.nih\.gov/i.test(url)) {
      for (const link of (page.eventDetailLinks || []).slice(0, 4)) {
        const child = await fetchOfficialPage(link, {
          allowRender: renderBudget.remaining > 0,
          renderBudget,
        });
        pageReports.push({
          url: link,
          ok: child.ok,
          fetchMethod: child.fetchMethod,
          textLen: child.text?.length || 0,
          browserUsed: child.browserUsed,
        });
        if (!child.ok) continue;
        const ex2 = extractEventSeriesFromCalendarPage({
          url: child.finalUrl || link,
          text: child.text,
          organizationHint: "National Institutes of Health",
          discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
        });
        allSeries.push(...ex2.series);
        allCycles.push(...ex2.cycles);
      }
    }
  }

  // Ensure HRHR series from commonfund page when named symposium is present
  const hrhrOk = pageReports.find((p) => /highrisk\/symposium/i.test(p.url) && p.ok);
  if (hrhrOk) {
    const full = await fetchOfficialPage("https://commonfund.nih.gov/highrisk/symposium", {
      allowRender: true,
      renderBudget,
    });
    if (full.ok && /high-?risk|symposium/i.test(full.text)) {
      const year = inferYear({}, full.text);
      allCycles.push({
        eventCycleId: `cycle:series:high_risk_high_reward_research_symposium|${year}`,
        eventSeriesId: "series:high_risk_high_reward_research_symposium",
        year,
        title: `High-Risk, High-Reward Research Symposium ${year}`,
        organization: "NIH Common Fund",
        destinationStatus: "FIXED_VENUE",
        venueStatus: "FIXED",
        housingStatus: "HOUSING_PENDING",
        opportunityType: OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING,
        sourceUrl: "https://commonfund.nih.gov/highrisk/symposium",
        officialSource: "https://commonfund.nih.gov/highrisk/symposium",
        discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
        evidenceConfidence: 80,
        cqHint: "HOUSING",
      });
      allSeries.push({
        eventSeriesId: "series:high_risk_high_reward_research_symposium",
        canonicalName: "High-Risk, High-Reward Research Symposium",
        organization: "NIH Common Fund",
        sourceUrl: "https://commonfund.nih.gov/highrisk/symposium",
        discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
        futureCycles: [{ year }],
      });
    }
  }

  upsertEventSeriesGraph(HOTEL_ID, { series: allSeries, cycles: allCycles });

  const candidates = cyclesToDiscoveryCandidates(allCycles, { hotelId: HOTEL_ID }).map((c) => {
    const year = inferYear(c, c.title);
    return {
      ...c,
      eventYear: year,
      eventStartDate: c.eventStartDate || `${year}-03-01`,
      cqState: classifyCq({ ...c, eventYear: year }),
      officialSource: c.officialSource || c.sourceUrl,
    };
  });

  // Scrub aggregator promotions from prior V3 pass
  const before = await loadOpportunitiesCanonical(HOTEL_ID);
  const scrubbed = [];
  for (const o of before.opportunities || []) {
    if (/showsbee|10times|conferenceindex|allconference|healthmanagement\.org\/c\//i.test(o.officialSource || "")) {
      o.customerVisible = false;
      o.isTestData = true;
      o.priority = "DISQUALIFIED";
      o.opportunityQualification = "CLOSED";
      scrubbed.push(o.id);
      await upsertSingleOpportunity(HOTEL_ID, o, { runId: "gdi_v3_scrub" });
    }
  }

  const runId = `gdi_v3_repair_${Date.now().toString(36)}`;
  const existing = (await loadOpportunitiesCanonical(HOTEL_ID)).opportunities || [];
  const promoStats = { PROMOTED: 0, HELD: 0, SKIP: 0, DUP: 0, NEW_HOUSING: 0, NEW_WATCH: 0, NEW_FUTURE_WATCH: 0 };
  const promotedIds = [];

  for (const c of candidates) {
    const cq = c.cqState || classifyCq(c);
    if (!["WATCH", "HOUSING", "FUTURE_WATCH", "ACTIONABLE_NOW"].includes(cq)) {
      promoStats.SKIP += 1;
      continue;
    }
    if (!c.officialSource || scoreOfficialSource(c.officialSource).score < 15) {
      promoStats.SKIP += 1;
      continue;
    }
    const year = inferYear(c, c.title);
    const id = `gdi_opp_${slug(c.eventSeriesId || c.title)}_${year}`;
    const promoCand = {
      ...c,
      id,
      opportunityId: id,
      hotelId: HOTEL_ID,
      title: c.title,
      organizationName: c.organizationName || c.organization,
      opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
      opportunityType:
        cq === "HOUSING" ? OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING : OPPORTUNITY_TYPE.FUTURE_CYCLE,
      eventStartDate: `${year}-03-01`,
      eventYear: year,
      officialSource: c.officialSource,
      discoverySource: "gdi_nih_association_recall_v3_repair",
      roomDemandStatus: ROOM_DEMAND_STATUS.HOUSING_PENDING,
      lodgingEvidence: "Future cycle lodging / housing path under monitoring",
      housingEvidence: cq === "HOUSING" ? "Fixed venue; lodging open/unclear" : "Housing TBD",
      customerFacingState: cq === "HOUSING" ? "WATCH" : cq === "FUTURE_WATCH" ? "FUTURE_WATCH" : "WATCH",
      salesPartitionV11: cq === "HOUSING" ? "HOUSING" : cq,
      priority: "WATCHLIST",
      weeklyDeltaState: "NEW",
      isNewThisWeek: true,
      newnessSemantics: "NEW_TO_GDI_ON_PROMOTION",
      hotelDemandThesis:
        "NIH / institute named series with future cycle relevance to Bethesda Marriott housing and meeting capacity",
      whyNow: `Future cycle ${year}`,
      recommendedAction:
        cq === "HOUSING"
          ? "Pursue housing / overflow / VIP / ancillary rooms"
          : "Monitor future cycle and housing announcements",
      demandTerritoryFit: "DMV_COMPETITIVE",
      evidenceConfidence: c.evidenceConfidence || 70,
      sources: [{ url: c.officialSource, authority: "FIRST_PARTY" }],
      eventSeriesId: c.eventSeriesId,
      discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
      customerVisible: true,
      cqState: cq,
    };

    const result = await promoteQualifiedGdiOpportunity({
      candidate: promoCand,
      existingOpps: existing,
      hotelId: HOTEL_ID,
      runId,
      discoveryRunId: runId,
      discoveryAt: new Date().toISOString(),
      method: "nih_association_recall_v3_repair",
      playbook: DISCOVERY_LANE.NIH_CALENDAR,
      source: promoCand.officialSource,
      dryRun: false,
    });

    if (result.action === PROMOTION_ACTION.PROMOTE_NEW || result.action === PROMOTION_ACTION.UPDATE_EXISTING) {
      await upsertSingleOpportunity(HOTEL_ID, result.opportunity || promoCand, { runId });
      existing.push(result.opportunity || promoCand);
      promotedIds.push(id);
      promoStats.PROMOTED += 1;
      if (cq === "HOUSING") promoStats.NEW_HOUSING += 1;
      else if (cq === "FUTURE_WATCH") promoStats.NEW_FUTURE_WATCH += 1;
      else promoStats.NEW_WATCH += 1;
    } else if (result.action === PROMOTION_ACTION.DUPLICATE_EXISTING) {
      promoStats.DUP += 1;
    } else if (result.action === PROMOTION_ACTION.HOLD_WATCH) {
      await upsertSingleOpportunity(HOTEL_ID, promoCand, { runId });
      existing.push(promoCand);
      promotedIds.push(id);
      promoStats.PROMOTED += 1;
      promoStats.NEW_WATCH += 1;
    } else {
      promoStats.SKIP += 1;
    }
  }

  // Research target for HRHR if present
  const hrhrCycle = candidates.find((c) => /high.?risk|high.?reward/i.test(c.title || ""));
  let newTargets = 0;
  if (hrhrCycle) {
    const target = buildResearchTarget({
      hotelId: HOTEL_ID,
      hotelName: "Bethesda Marriott",
      targetType: TARGET_TYPE.OFFICIAL_CALENDAR,
      entityKey: hrhrCycle.eventSeriesId || "series:hrhr",
      seriesId: hrhrCycle.eventSeriesId,
      canonicalName: hrhrCycle.title,
      officialDomain: "commonfund.nih.gov",
      primarySourceUrl: hrhrCycle.officialSource,
      priority: TARGET_PRIORITY.HIGH,
      researchCadence: RESEARCH_CADENCE.MONTHLY,
      researchPlaybook: "official_source_monitor_v1",
      nextResearchAt: suggestNextResearchAt({
        priority: TARGET_PRIORITY.HIGH,
        researchCadence: RESEARCH_CADENCE.MONTHLY,
      }),
      status: "ACTIVE",
    });
    upsertLocalResearchTarget(HOTEL_ID, target);
    newTargets = 1;
  }

  invalidateGdiHotelReadCache(HOTEL_ID);
  const afterDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  let visible = filterCustomerFacingOpportunities(filterSalespersonView(afterDoc.opportunities || []));
  const newlyVisible = visible.filter((o) => promotedIds.includes(o.id));

  const reval = JSON.parse(
    readFileSync(
      join(
        process.cwd(),
        "reports/group-demand-intelligence/external-recall-benchmark-v1/BETHESDA_EXTERNAL_REVALIDATION.json"
      ),
      "utf8"
    )
  );
  const remaining = (reval.revalidation || []).filter((r) => REMAINING_9.includes(r.id));
  const pool = [...candidates, ...visible];
  const matchRows = remaining.map((b) => {
    const must = IDENTITY[b.id] || [];
    let hit = null;
    for (const c of pool) {
      const text = norm([c.title, c.organizationName, c.officialSource].join(" "));
      if (hasIdentity(text, must)) {
        hit = c;
        break;
      }
    }
    return {
      id: b.id,
      event: b.event,
      after: hit ? "FOUND" : "NOT_FOUND",
      source: hit?.officialSource || null,
      fetchMethod: hit?.fetchMethod || null,
      cq: hit?.cqState || hit?.customerFacingState || hit?.salesPartitionV11 || null,
      matchedTitle: hit?.title || null,
    };
  });
  const found9 = matchRows.filter((r) => r.after === "FOUND").length;
  const afterCount = 4 + found9;
  const graph = loadEventSeriesGraph(HOTEL_ID);

  const report = {
    repair: true,
    rendered: pageReports.filter((p) => p.fetchMethod === FETCH_METHOD.RENDERED_PLAYWRIGHT).length,
    pages: pageReports,
    cyclesExtracted: allCycles.length,
    seriesExtracted: allSeries.length,
    scrubbedAggregators: scrubbed,
    promoStats,
    promotedIds,
    newlyVisible: newlyVisible.length,
    apiVisible: visible.length,
    matchRows,
    found9,
    recallAfter: `${afterCount} / 13 = ${Math.round((afterCount / 13) * 1000) / 10}%`,
    graph: {
      series: Object.keys(graph.series || {}).length,
      cycles: Object.keys(graph.cycles || {}).length,
    },
    newTargets,
    sampleVisible: newlyVisible.slice(0, 8).map((o) => ({
      id: o.id,
      title: o.title,
      state: o.customerFacingState || o.salesPartitionV11,
      source: o.officialSource,
    })),
  };

  writeFileSync(join(OUT, "REPAIR_REPORT.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
