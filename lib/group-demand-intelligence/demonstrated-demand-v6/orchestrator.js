/**
 * Demonstrated-Demand Discovery V6 orchestrator.
 * Comp-set mining → demonstrated-demand gate → success-pattern admission → Jev (admitted only) → classify.
 * Depth-3 not default. Thresholds unchanged.
 */

import { buildGdiSuccessfulOpportunityPattern } from "../opportunity-discovery-v5/success-pattern.js";
import {
  COMP_SET_TARGET_HOTELS,
  runCompSetDemandMiningForHotel,
  buildTargetHotelThesis,
  resolveBuyerForPattern,
  FIT_CLASS,
  EVIDENCE_CLASS,
} from "../comp-set-demand-mining-v1/index.js";
import { isGdiDemonstratedDemandLead } from "./demonstrated-demand-gate.js";
import {
  successfulPatternMatchScore,
  buildSuccessPatternModelDoc,
  SUCCESS_MATCH,
} from "./success-pattern-admission.js";
import { researchAdmittedLead, JEV_ACTION } from "./jev-admitted.js";
import {
  buildRepeatBuyerPatterns,
  buildRepeatAgencyPatterns,
  buildRepeatMarketPatterns,
} from "./repeat-buyer-agency.js";
import { buildCompetitorWinBackAngles } from "./win-back-angles.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";

function traceToLeadShape(trace = {}, hotel = {}, repeat = null, thesis = null, buyer = null) {
  return {
    id: trace.traceId || `dd_${hotel.hotelKey}_${String(trace.organization || "").slice(0, 24)}`,
    title: trace.eventProgram || trace.organization,
    organizationName: trace.organization,
    organization: trace.organization,
    eventProgram: trace.eventProgram,
    eventSeriesId: trace.eventSeriesId,
    eventCycleId: trace.eventCycleId,
    eventYear: trace.eventYear,
    eventStartDate: trace.eventDate,
    groupType: trace.groupType,
    groupMotion: trace.groupType,
    demandEngine: trace.demandEngine,
    market: trace.market || hotel.market,
    lodgingMarket: hotel.market,
    officialSource: trace.source,
    source: trace.source,
    competitorHotel: trace.competitorHotel,
    competitorHotelId: trace.competitorHotelId,
    evidenceClass: trace.evidenceClass,
    feedsDeeperResearch: trace.feedsDeeperResearch,
    fact: trace.fact,
    inference: trace.inference,
    unknown: trace.unknown,
    lodgingEvidence: trace.lodgingEvidence,
    organizationContactUrl: trace.organizationContactUrl || buyer?.publicContactPath,
    functionalContactEmail: trace.functionalContactEmail,
    buyerEntity: buyer?.buyerEntity,
    buyerType: buyer?.buyerType,
    buyerRole: buyer?.buyerRole,
    organizer: buyer?.organizer,
    agency: buyer?.agency,
    housingPartner: buyer?.housingPartner,
    publicContactPath: buyer?.publicContactPath,
    repeatCadence: repeat?.cadence,
    nextKnownCycle: repeat?.nextKnownCycle,
    nextExpectedCycle: repeat?.nextExpectedCycle,
    nextDecisionWindow: repeat?.nextDecisionWindow,
    historicHotels: repeat?.historicHotels
      ? String(repeat.historicHotels).split("|").filter(Boolean)
      : [trace.competitorHotel].filter(Boolean),
    hotelOpportunityThesis: thesis
      ? `${thesis.whyRelevant} ${thesis.whatTargetCouldWin} Fit:${thesis.fitClass}.`
      : `Competitor demand at ${trace.competitorHotel} for ${trace.organization}.`,
    hotelFitScore: hotel.defaultFitScore ?? 48,
    defaultFitScore: hotel.defaultFitScore ?? 48,
    hotelKey: hotel.hotelKey,
    hotelId: hotel.hotelId,
    opportunityType: "OVERFLOW_HOUSING",
    gdiDiscoveryVersion: "demonstrated_demand_v6",
    pivotId: trace.pivotId,
    pivotType: trace.pivotType,
  };
}

/**
 * Run V6 for one hotel.
 */
export async function runDemonstratedDemandV6ForHotel(hotel = {}, opts = {}) {
  const budget = {
    queriesLeft: opts.maxJevQueries ?? 6,
    queriesRun: 0,
    costUsd: 0,
    errors: [],
  };

  // Phase 3–6: reuse comp-set mining (includes page validation)
  const comp = await runCompSetDemandMiningForHotel(hotel, {
    nowDate: opts.nowDate || "2026-10-03",
    maxCompetitors: opts.maxCompetitors ?? 4,
    maxQueries: opts.maxCompQueries ?? 12,
    maxPivotsPerCompetitor: 4,
    maxPagesPerCompetitor: 2,
    resolveIdentity: true,
  });
  budget.costUsd += comp.costUsd || 0;

  const deepTraces = (comp.traces || []).filter(
    (t) =>
      t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED ||
      t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION
  );

  const demonstratedLeads = [];
  const admissionRows = [];
  const thesisRows = [];
  const buyerRows = [];
  const winBackRows = [];
  const jevRows = [];
  const depthRows = [];
  const admitted = [];
  const researched = [];
  const customerReady = [];
  const futureWatch = [];
  const rejected = [];

  // Index repeats by org
  const repeatByOrg = new Map();
  for (const r of comp.repeatPatterns || []) {
    repeatByOrg.set(String(r.organization || "").toLowerCase(), r);
  }

  for (const trace of deepTraces) {
    const repeat = repeatByOrg.get(String(trace.organization || "").toLowerCase()) || null;
    const patternLite = {
      organization: trace.organization,
      eventProgram: trace.eventProgram,
      demandEngine: trace.demandEngine,
      historicHotels: [trace.competitorHotel].filter(Boolean),
      repeatCadence: repeat?.cadence,
      nextKnownCycle: repeat?.nextKnownCycle,
      nextExpectedCycle: repeat?.nextExpectedCycle,
      lodgingPattern: trace.lodgingEvidence ? "LODGING_EVIDENCE_PRESENT" : "UNKNOWN",
      evidenceSet: trace.evidenceClass,
      sources: trace.source,
    };
    const buyer = resolveBuyerForPattern(patternLite, trace);
    const thesis = buildTargetHotelThesis(
      {
        ...patternLite,
        patternId: trace.traceId,
        nextDecisionWindow: repeat?.nextDecisionWindow,
      },
      { ...hotel, rooms: hotel.rooms, productFit: hotel.productFit, targetRooms: hotel.rooms }
    );

    const lead = traceToLeadShape(trace, hotel, repeat, thesis, buyer);
    const demo = isGdiDemonstratedDemandLead(lead, {
      geoTokens: hotel.geoTokens || String(hotel.market || "").split(/[\/,]/).map((s) => s.trim()),
    });
    lead.demonstratedDemand = demo.ok;
    lead.demonstratedClass = demo.class;
    lead.supportSignals = demo.supportSignals;
    lead.supportCount = demo.supportCount;

    buyerRows.push({
      leadId: lead.id,
      hotelKey: hotel.hotelKey,
      ...buyer,
    });
    thesisRows.push({ ...thesis, leadId: lead.id });

    if (!demo.ok) {
      rejected.push({
        id: lead.id,
        hotelKey: hotel.hotelKey,
        title: lead.title,
        reason: (demo.reasons || []).join("|"),
        stage: "NOT_DEMONSTRATED",
      });
      continue;
    }

    demonstratedLeads.push(lead);

    const match = successfulPatternMatchScore(lead, {
      demonstratedDemand: true,
      geoTokens: hotel.geoTokens || [],
    });
    admissionRows.push({
      leadId: lead.id,
      hotelKey: hotel.hotelKey,
      organization: lead.organizationName,
      competitorHotel: lead.competitorHotel,
      demonstrated: true,
      successMatch: match.match,
      score: match.score,
      maxScore: match.maxScore,
      admit: match.admit,
      traitsPresent: match.traitsPresent.join("|"),
      traitsMissing: match.traitsMissing.join("|"),
      admissionClass: match.admissionClass,
    });

    if (!match.admit) {
      rejected.push({
        id: lead.id,
        hotelKey: hotel.hotelKey,
        title: lead.title,
        reason: `SUCCESS_PATTERN_${match.match}`,
        stage: "DEMONSTRATED_NOT_ADMITTED",
      });
      continue;
    }

    admitted.push({ ...lead, successMatch: match.match, successScore: match.score });
    winBackRows.push(...buildCompetitorWinBackAngles(lead, thesis, hotel));

    // Jev only on admitted — depth default 2
    const beforeClass = "ADMITTED";
    const research = await researchAdmittedLead(lead, hotel, budget, {
      maxDepth: 2,
      allowThirdIfStrong: match.match === SUCCESS_MATCH.STRONG,
      successMatch: match.match,
      startingDepth: 1,
    });
    for (const j of research.jevLog) jevRows.push(j);

    const enriched = research.lead;
    researched.push(enriched);

    const ready = isGdiCustomerOpportunityReady(enriched, {
      nowDate: opts.nowDate || "2026-10-03",
    });
    const watch = isValidFutureWatch(enriched, { nowDate: opts.nowDate || "2026-10-03" });
    const watchOk =
      watch?.ok === true || watch?.valid === true || watch?.class === "VALID_FUTURE_WATCH";

    let afterClass = "RESEARCHED_NOT_USEFUL";
    if (ready.ok) {
      afterClass = "CUSTOMER_READY";
      customerReady.push(enriched);
    } else if (watchOk) {
      afterClass = "VALID_FUTURE_WATCH";
      futureWatch.push(enriched);
    } else {
      rejected.push({
        id: enriched.id,
        hotelKey: hotel.hotelKey,
        title: enriched.title,
        reason: (ready.failed || []).slice(0, 5).join("|") || "GATES_FAILED",
        stage: "RESEARCHED_REJECTED",
      });
    }

    depthRows.push({
      leadId: enriched.id,
      hotelKey: hotel.hotelKey,
      depth: research.depth,
      blockersResolved: research.blockersResolved,
      classificationBefore: beforeClass,
      classificationAfter: afterClass,
      classificationChanged: beforeClass !== afterClass && afterClass !== "RESEARCHED_NOT_USEFUL",
      useful: ready.ok || watchOk,
      costUsd: "", // filled in aggregate
    });
  }

  // Also reject WEAK/DISCOVERY traces as non-demonstrated (count only)
  const weakCount = (comp.traces || []).length - deepTraces.length;

  const buyerByLead = new Map(buyerRows.map((b) => [b.leadId, b]));
  const repeatBuyers = buildRepeatBuyerPatterns(demonstratedLeads, deepTraces);
  const repeatAgencies = buildRepeatAgencyPatterns(
    demonstratedLeads.map((l) => ({
      ...l,
      agency: buyerByLead.get(l.id)?.agency,
      housingPartner: buyerByLead.get(l.id)?.housingPartner,
    }))
  );
  const repeatMarkets = buildRepeatMarketPatterns(demonstratedLeads);

  return {
    hotelKey: hotel.hotelKey,
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    competitors: comp.competitors,
    pivots: comp.pivots,
    phoneHits: comp.phoneHits,
    pageRows: comp.pageRows,
    traces: comp.traces,
    deepTraces,
    repeatPatterns: comp.repeatPatterns,
    demonstratedLeads,
    admissionRows,
    thesisRows,
    buyerRows,
    winBackRows,
    jevRows,
    depthRows,
    admitted,
    researched,
    customerReady,
    futureWatch,
    rejected,
    repeatBuyers,
    repeatAgencies,
    repeatMarkets,
    weakTraceCount: weakCount,
    counts: {
      competitors: (comp.competitors || []).length,
      rawHits: (comp.phoneHits || []).length + (comp.pivots || []).length,
      validatedTraces: (comp.traces || []).length,
      directConfirmed: (comp.traces || []).filter(
        (t) => t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED
      ).length,
      strongAssociation: (comp.traces || []).filter(
        (t) => t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION
      ).length,
      demonstrated: demonstratedLeads.length,
      admitted: admitted.length,
      jevResearched: researched.length,
      customerReady: customerReady.length,
      futureWatch: futureWatch.length,
      rejected: rejected.length,
    },
    costUsd: budget.costUsd,
    queriesRun: (comp.queriesRun || 0) + budget.queriesRun,
    jevQueriesRun: budget.queriesRun,
    errors: [...(comp.errors || []), ...budget.errors],
  };
}

export async function loadSuccessControlForV6(opts = {}) {
  const pattern = await buildGdiSuccessfulOpportunityPattern(opts);
  const model = buildSuccessPatternModelDoc(pattern);
  return { pattern, model };
}

export {
  COMP_SET_TARGET_HOTELS,
  SUCCESS_MATCH,
  FIT_CLASS,
  JEV_ACTION,
};
