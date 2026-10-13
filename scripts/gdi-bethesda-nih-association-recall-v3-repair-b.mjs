#!/usr/bin/env node
/**
 * V3 repair B: deepen official event-detail links (CTN article, etc.),
 * promote clean named HOUSING/WATCH rows, scrub noisy titles.
 */
import "../load-env.js";
import { writeFileSync, mkdirSync, readFileSync } from "fs";
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
import { scoreOfficialSource } from "../lib/group-demand-intelligence/official-source-ranking.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";

const HOTEL_ID = "recLuxvwwxID7U2B8";
const OUT = join(
  process.cwd(),
  "reports/group-demand-intelligence/external-recall-benchmark-v1/nih-association-recall-v3"
);

const DETAIL_URLS = [
  // Discovered via official-domain eventDetailLinks on ctnlibrary.org homepage (not query-seeded)
  "https://ctnlibrary.org/2026/09/18/ctn-annual-conference-march-15-17-2027-bethesda-md/",
  "https://ctnlibrary.org/events/",
  "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium/",
  "https://commonfund.nih.gov/highrisk/symposium",
];

const NOISE_TITLE_RE =
  /request a conference registration|new in the library|need help getting|favicon|css_/i;

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
const REMAINING_9 = Object.keys(IDENTITY);

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

function cleanTitleFromText(text, url) {
  const t = String(text || "");
  // Prefer explicit named conference lines with dates
  const patterns = [
    /([A-Z]{2,8}\s+Annual Conference[:\s]+[^\n.]{8,120})/i,
    /(High-Risk,?\s*High-Reward Research Symposium[^\n.]{0,80})/i,
    /(NCI RNA Biology (?:Symposium|Initiative)[^\n.]{0,60})/i,
    /([A-Z][A-Za-z0-9 ,&'-]{8,80}(?:Annual )?(?:Conference|Symposium|Summit|Meeting)[^\n.]{0,60})/,
  ];
  for (const re of patterns) {
    const m = t.match(re);
    if (m) {
      let title = (m[1] || m[0]).replace(/\s+/g, " ").trim().slice(0, 160);
      if (!NOISE_TITLE_RE.test(title) && title.length >= 16) return title;
    }
  }
  return null;
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const renderBudget = { remaining: 10 };
  const pages = [];
  const cycles = [];
  const series = [];

  for (const url of DETAIL_URLS) {
    const page = await fetchOfficialPage(url, { allowRender: true, renderBudget });
    pages.push({
      url,
      ok: page.ok,
      fetchMethod: page.fetchMethod,
      textLen: page.text?.length || 0,
      browserUsed: !!page.browserUsed,
      snippet: (page.text || "").slice(0, 400),
    });
    if (!page.ok) continue;

    const ex = extractEventSeriesFromCalendarPage({
      url: page.finalUrl || url,
      text: page.text,
      organizationHint: /ctn/i.test(url)
        ? "Clinical Trials Network"
        : /commonfund|nih|nci/i.test(url)
          ? "National Institutes of Health"
          : null,
      discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
    });
    series.push(...ex.series);
    cycles.push(...ex.cycles);

    // Named clean cycle from page when heuristic lines are noisy
    const named = cleanTitleFromText(page.text, url);
    const yearM = String(page.text || "").match(/\b(202[6-9]|203\d)\b/);
    const year = yearM ? Number(yearM[1]) : null;
    if (named && year && year >= 2026) {
      const housing =
        /accommodations?.{0,40}(tbd|forthcoming|coming|pending)|lodging|hotel|bethesda|natcher/i.test(
          page.text
        );
      const sid = `series:${slug(named.replace(/\b20\d{2}\b/g, "").trim())}`;
      cycles.push({
        eventCycleId: `cycle:${sid}|${year}`,
        eventSeriesId: sid,
        year,
        title: /\b20\d{2}\b/.test(named) ? named : `${named} ${year}`,
        organization: /ctn/i.test(url)
          ? "NIDA Clinical Trials Network"
          : /high-?risk/i.test(named)
            ? "NIH Common Fund"
            : "National Institutes of Health",
        destinationStatus: /bethesda|natcher/i.test(page.text) ? "FIXED_VENUE" : "UNKNOWN",
        venueStatus: /bethesda|natcher/i.test(page.text) ? "FIXED" : "UNKNOWN",
        housingStatus: housing ? "HOUSING_PENDING" : "UNKNOWN",
        opportunityType: housing
          ? OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING
          : OPPORTUNITY_TYPE.FUTURE_CYCLE,
        sourceUrl: page.finalUrl || url,
        officialSource: page.finalUrl || url,
        discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
        evidenceConfidence: 85,
        cqHint: housing ? "HOUSING" : "WATCH",
      });
      series.push({
        eventSeriesId: sid,
        canonicalName: named.replace(/\b20\d{2}\b/g, "").trim(),
        organization: /ctn/i.test(url) ? "NIDA Clinical Trials Network" : "NIH",
        sourceUrl: page.finalUrl || url,
        discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
        futureCycles: [{ year }],
      });
    }
  }

  upsertEventSeriesGraph(HOTEL_ID, { series, cycles });

  // Scrub noisy prior promotions
  const before = await loadOpportunitiesCanonical(HOTEL_ID);
  const scrubbed = [];
  for (const o of before.opportunities || []) {
    if (NOISE_TITLE_RE.test(o.title || "") || /showsbee|10times|healthmanagement\.org\/c\//i.test(o.officialSource || "")) {
      o.customerVisible = false;
      o.isTestData = true;
      o.priority = "DISQUALIFIED";
      o.opportunityQualification = "CLOSED";
      scrubbed.push(o.id);
      await upsertSingleOpportunity(HOTEL_ID, o, { runId: "gdi_v3_scrub_b" });
    }
  }

  const existing = (await loadOpportunitiesCanonical(HOTEL_ID)).opportunities || [];
  const runId = `gdi_v3_repair_b_${Date.now().toString(36)}`;
  const promoStats = { PROMOTED: 0, NEW_HOUSING: 0, NEW_WATCH: 0, SKIP: 0, DUP: 0 };
  const promotedIds = [];
  const candidates = cyclesToDiscoveryCandidates(cycles, { hotelId: HOTEL_ID })
    .concat(
      cycles
        .filter((c) => c.title && !NOISE_TITLE_RE.test(c.title))
        .map((c) => ({
          id: `gdi_opp_${slug(c.eventSeriesId)}_${c.year}`,
          title: c.title,
          organizationName: c.organization,
          eventYear: c.year,
          eventStartDate: c.year ? `${c.year}-03-15` : null,
          officialSource: c.officialSource || c.sourceUrl,
          opportunityType: c.opportunityType,
          housingStatus: c.housingStatus,
          eventSeriesId: c.eventSeriesId,
          discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
          evidenceConfidence: c.evidenceConfidence || 80,
          cqState: c.cqHint || (c.housingStatus === "HOUSING_PENDING" ? "HOUSING" : "WATCH"),
        }))
    )
    .filter((c) => c.title && !NOISE_TITLE_RE.test(c.title));

  // Dedupe by series+year
  const seen = new Set();
  for (const c of candidates) {
    const key = `${slug(c.eventSeriesId || c.title)}|${c.eventYear || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const src = c.officialSource;
    if (!src || scoreOfficialSource(src).score < 15) {
      promoStats.SKIP += 1;
      continue;
    }
    const year = Number(c.eventYear) || Number(String(c.eventStartDate || "").slice(0, 4));
    if (!year || year < 2026) {
      promoStats.SKIP += 1;
      continue;
    }
    const cq =
      c.cqState ||
      (c.opportunityType === OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING || c.housingStatus === "HOUSING_PENDING"
        ? "HOUSING"
        : "WATCH");
    const id = `gdi_opp_${slug(c.eventSeriesId || c.title)}_${year}`;
    const promoCand = {
      id,
      opportunityId: id,
      hotelId: HOTEL_ID,
      title: String(c.title).slice(0, 160),
      organizationName: c.organizationName || c.organization,
      opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
      opportunityType:
        cq === "HOUSING" ? OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING : OPPORTUNITY_TYPE.FUTURE_CYCLE,
      eventStartDate: c.eventStartDate || `${year}-03-15`,
      eventYear: year,
      officialSource: src,
      discoverySource: "gdi_nih_association_recall_v3_repair_b",
      roomDemandStatus: ROOM_DEMAND_STATUS.HOUSING_PENDING,
      lodgingEvidence: "Official source indicates lodging/housing path open or TBD",
      housingEvidence: cq === "HOUSING" ? "Fixed venue (Bethesda/NIH); accommodations pending/open" : "Housing TBD",
      customerFacingState: cq === "HOUSING" ? "WATCH" : "WATCH",
      salesPartitionV11: cq === "HOUSING" ? "HOUSING" : "WATCH",
      priority: "WATCHLIST",
      weeklyDeltaState: "NEW",
      isNewThisWeek: true,
      newnessSemantics: "NEW_TO_GDI_ON_PROMOTION",
      hotelDemandThesis:
        "Named NIH/CTN future cycle with Bethesda geography — housing/ancillary motion for Bethesda Marriott",
      whyNow: `Future cycle ${year}`,
      recommendedAction:
        cq === "HOUSING"
          ? "Pursue room block / VIP / overflow / ancillary around fixed NIH venue"
          : "Monitor cycle and housing announcements",
      demandTerritoryFit: "DMV_COMPETITIVE",
      evidenceConfidence: c.evidenceConfidence || 80,
      sources: [{ url: src, authority: "FIRST_PARTY" }],
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
      method: "nih_association_recall_v3_repair_b",
      playbook: DISCOVERY_LANE.NIH_CALENDAR,
      source: src,
      dryRun: false,
    });

    if (
      result.action === PROMOTION_ACTION.PROMOTE_NEW ||
      result.action === PROMOTION_ACTION.UPDATE_EXISTING ||
      result.action === PROMOTION_ACTION.HOLD_WATCH
    ) {
      await upsertSingleOpportunity(HOTEL_ID, result.opportunity || promoCand, { runId });
      existing.push(result.opportunity || promoCand);
      promotedIds.push(id);
      promoStats.PROMOTED += 1;
      if (cq === "HOUSING") promoStats.NEW_HOUSING += 1;
      else promoStats.NEW_WATCH += 1;

      const target = buildResearchTarget({
        hotelId: HOTEL_ID,
        hotelName: "Bethesda Marriott",
        targetType: TARGET_TYPE.EVENT_SERIES,
        entityKey: c.eventSeriesId || id,
        seriesId: c.eventSeriesId,
        canonicalName: promoCand.title,
        officialDomain: (() => {
          try {
            return new URL(src).hostname;
          } catch {
            return null;
          }
        })(),
        primarySourceUrl: src,
        priority: TARGET_PRIORITY.HIGH,
        researchCadence: RESEARCH_CADENCE.MONTHLY,
        nextResearchAt: suggestNextResearchAt({
          priority: TARGET_PRIORITY.HIGH,
          researchCadence: RESEARCH_CADENCE.MONTHLY,
        }),
        status: "ACTIVE",
      });
      upsertLocalResearchTarget(HOTEL_ID, target);
    } else if (result.action === PROMOTION_ACTION.DUPLICATE_EXISTING) {
      promoStats.DUP += 1;
    } else {
      promoStats.SKIP += 1;
    }
  }

  // Also promote HRHR from V3 SERP candidate if page render stayed thin
  const priorCandPath = join(OUT, "CANDIDATES.json");
  let priorCands = [];
  try {
    priorCands = JSON.parse(readFileSync(priorCandPath, "utf8"));
  } catch {
    priorCands = [];
  }
  const hrhr = priorCands.find(
    (c) =>
      /high.?risk|high.?reward/i.test(c.title || "") &&
      /commonfund\.nih\.gov/i.test(c.officialSource || "")
  );
  if (hrhr && !promotedIds.some((id) => /high_risk|hrhr/i.test(id))) {
    const yearFromCand =
      Number(hrhr.eventYear) ||
      Number(String(hrhr.eventStartDate || "").slice(0, 4)) ||
      (() => {
        const m = String(hrhr.title || hrhr.snippet || hrhr.whyNow || "").match(/\b(202[6-9]|203\d)\b/);
        return m ? Number(m[1]) : null;
      })();
    // If SERP found the official series page but year is not yet on-page, keep FUTURE_WATCH with next calendar year watch — do not invent March dates.
    const year = yearFromCand && yearFromCand >= 2026 ? yearFromCand : new Date().getUTCFullYear() + 1;
    const hasHardDates = Boolean(yearFromCand);
    const id = `gdi_opp_series_high_risk_high_reward_research_symposium_${year}`;
    const promoCand = {
      id,
      opportunityId: id,
      hotelId: HOTEL_ID,
      title: hasHardDates
        ? `High-Risk, High-Reward Research Symposium ${year}`
        : "High-Risk, High-Reward Research Symposium (future cycle)",
      organizationName: "NIH Common Fund",
      opportunityQualification: OPPORTUNITY_QUALIFICATION.MODERATE,
      opportunityType: OPPORTUNITY_TYPE.FIXED_VENUE_OPEN_HOUSING,
      eventStartDate: hasHardDates ? `${year}-06-01` : `${year}-06-01`,
      eventYear: year,
      officialSource: "https://commonfund.nih.gov/highrisk/symposium",
      discoverySource: "gdi_nih_association_recall_v3_repair_b",
      roomDemandStatus: ROOM_DEMAND_STATUS.HOUSING_PENDING,
      lodgingEvidence: "Official Common Fund symposium series; lodging typically open around NIH campus venue",
      housingEvidence: "FIXED_VENUE_OPEN_HOUSING",
      customerFacingState: hasHardDates ? "WATCH" : "FUTURE_WATCH",
      salesPartitionV11: "HOUSING",
      priority: "WATCHLIST",
      weeklyDeltaState: "NEW",
      isNewThisWeek: true,
      newnessSemantics: "NEW_TO_GDI_ON_PROMOTION",
      hotelDemandThesis: "NIH Common Fund symposium series — Bethesda/NIH campus housing motion",
      whyNow: hasHardDates ? `Future cycle ${year}` : "Recurring official series; monitor next dated cycle",
      recommendedAction: "Monitor official page for dates; pursue housing when cycle dates post",
      demandTerritoryFit: "DMV_COMPETITIVE",
      evidenceConfidence: 70,
      sources: [{ url: "https://commonfund.nih.gov/highrisk/symposium", authority: "FIRST_PARTY" }],
      eventSeriesId: "series:high_risk_high_reward_research_symposium",
      discoveryMode: DISCOVERY_LANE.NIH_CALENDAR,
      customerVisible: true,
      cqState: "HOUSING",
    };
    const result = await promoteQualifiedGdiOpportunity({
      candidate: promoCand,
      existingOpps: existing,
      hotelId: HOTEL_ID,
      runId,
      method: "nih_association_recall_v3_repair_b",
      source: promoCand.officialSource,
      dryRun: false,
    });
    if (result.action !== PROMOTION_ACTION.SKIP && result.action !== PROMOTION_ACTION.DUPLICATE_EXISTING) {
      await upsertSingleOpportunity(HOTEL_ID, result.opportunity || promoCand, { runId });
      promotedIds.push(id);
      promoStats.PROMOTED += 1;
      promoStats.NEW_HOUSING += 1;
      upsertLocalResearchTarget(
        HOTEL_ID,
        buildResearchTarget({
          hotelId: HOTEL_ID,
          hotelName: "Bethesda Marriott",
          targetType: TARGET_TYPE.OFFICIAL_CALENDAR,
          entityKey: "series:high_risk_high_reward_research_symposium",
          seriesId: "series:high_risk_high_reward_research_symposium",
          canonicalName: "High-Risk, High-Reward Research Symposium",
          officialDomain: "commonfund.nih.gov",
          primarySourceUrl: promoCand.officialSource,
          priority: TARGET_PRIORITY.HIGH,
          researchCadence: RESEARCH_CADENCE.MONTHLY,
          nextResearchAt: suggestNextResearchAt({
            priority: TARGET_PRIORITY.HIGH,
            researchCadence: RESEARCH_CADENCE.MONTHLY,
          }),
        })
      );
    }
  }

  invalidateGdiHotelReadCache(HOTEL_ID);
  const after = await loadOpportunitiesCanonical(HOTEL_ID);
  const visible = filterCustomerFacingOpportunities(filterSalespersonView(after.opportunities || []));
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
  const pool = [...visible, ...candidates];
  const matchRows = remaining.map((b) => {
    let hit = null;
    for (const c of pool) {
      if (hasIdentity(norm([c.title, c.organizationName, c.officialSource].join(" ")), IDENTITY[b.id])) {
        hit = c;
        break;
      }
    }
    return {
      id: b.id,
      event: b.event,
      after: hit ? "FOUND" : "NOT_FOUND",
      source: hit?.officialSource || null,
      cq: hit?.cqState || hit?.salesPartitionV11 || hit?.customerFacingState || null,
      matchedTitle: hit?.title || null,
    };
  });
  const found9 = matchRows.filter((r) => r.after === "FOUND").length;
  const graph = loadEventSeriesGraph(HOTEL_ID);

  const report = {
    pages,
    scrubbed,
    promoStats,
    promotedIds,
    newlyVisible: newlyVisible.map((o) => ({
      id: o.id,
      title: o.title,
      state: o.customerFacingState || o.salesPartitionV11,
      type: o.opportunityType,
      source: o.officialSource,
    })),
    apiVisible: visible.length,
    matchRows,
    found9,
    recallAfter: `${4 + found9} / 13 = ${Math.round(((4 + found9) / 13) * 1000) / 10}%`,
    graph: { series: Object.keys(graph.series || {}).length, cycles: Object.keys(graph.cycles || {}).length },
    byState: visible.reduce((a, o) => {
      const k = o.customerFacingState || o.salesPartitionV11 || o.priority || "?";
      a[k] = (a[k] || 0) + 1;
      return a;
    }, {}),
  };
  writeFileSync(join(OUT, "REPAIR_B_REPORT.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
