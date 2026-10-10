/**
 * Complete Demand Packet V8 — hotel-level controlled run.
 * Comp-set mining + Apify identity pivots + packet classify + Jev on qualified only.
 */

import {
  COMP_SET_TARGET_HOTELS,
  runCompSetDemandMiningForHotel,
  applyPublicIdentity,
  EVIDENCE_CLASS,
} from "../comp-set-demand-mining-v1/index.js";
import {
  buildApifyActorInventory,
  enrichCompHotelsViaTripadvisor,
  apifyResultsToSignals,
} from "./apify-contribution.js";
import {
  evaluateCompleteDemandPacket,
  PACKET_QUALITY,
  isQualifiedForExpensiveCompletion,
  highPotentialPartial,
} from "./packet-schema.js";
import { successfulPacketPatternMatch } from "./success-calibration.js";
import { completeDemandPacket } from "./packet-completion.js";

function traceToRecord(trace, hotel) {
  return {
    id: trace.traceId,
    hotelKey: hotel.hotelKey,
    hotelId: hotel.hotelId,
    title: trace.eventProgram || trace.organization,
    organizationName: trace.organization,
    organization: trace.organization,
    officialSource: trace.source,
    source: trace.source,
    competitorHotel: trace.competitorHotel,
    evidenceClass: trace.evidenceClass,
    feedsDeeperResearch: trace.feedsDeeperResearch,
    eventYear: trace.eventYear,
    eventStartDate: trace.eventDate,
    groupType: trace.groupType,
    groupMotion: trace.groupType,
    demandEngine: trace.demandEngine,
    market: trace.market || hotel.market,
    lodgingMarket: hotel.market,
    lodgingEvidence:
      trace.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED ||
      trace.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION
        ? { housingPageFound: true, status: "WEAK" }
        : null,
    hotelFitScore: hotel.defaultFitScore ?? hotel.rooms ? 50 : 48,
    defaultFitScore: hotel.defaultFitScore ?? 48,
    signalType: "COMP_TRACE",
    sourceFamily: "COMP_SET",
    language: hotel.languages?.[0] || "en",
    fact: trace.fact,
  };
}

/**
 * Run V8 discovery + packet completion for one hotel.
 */
export async function runCompleteDemandPacketV8ForHotel(hotel = {}, opts = {}) {
  const budget = {
    queriesLeft: opts.maxCompletionQueries ?? 8,
    queriesRun: 0,
    costUsd: 0,
    errors: [],
  };

  // Comp-set mining
  const comp = await runCompSetDemandMiningForHotel(hotel, {
    nowDate: opts.nowDate || "2026-10-04",
    maxCompetitors: opts.maxCompetitors ?? 4,
    maxQueries: opts.maxCompQueries ?? 10,
    maxPivotsPerCompetitor: 3,
    maxPagesPerCompetitor: 2,
    resolveIdentity: true,
  });
  budget.costUsd += comp.costUsd || 0;

  // Apify Tripadvisor identity enrichment (bounded)
  let apify = { enabled: false, results: [], costUsd: 0 };
  if (opts.enableApify !== false) {
    apify = await enrichCompHotelsViaTripadvisor(comp.competitors || [], {
      maxComps: opts.maxApifyComps ?? 2,
    });
    budget.costUsd += apify.costUsd || 0;
    // Merge phones/domains into competitors for reporting
    for (const row of apify.results || []) {
      if (!row.ok) continue;
      const idx = (comp.competitors || []).findIndex(
        (c) => c.competitorHotelId === row.competitorHotelId
      );
      if (idx >= 0) {
        comp.competitors[idx] = applyPublicIdentity(comp.competitors[idx], {
          publicPhone: row.publicPhone,
          address: row.address,
          domain: row.domain,
          currentWebsite: row.currentWebsite,
        });
      }
    }
  }

  const apifySignals = apifyResultsToSignals(apify, hotel);

  // Build records from deep traces + apify signals
  const records = [];
  for (const t of comp.traces || []) {
    records.push(traceToRecord(t, hotel));
  }
  for (const s of apifySignals) records.push(s);

  // Feeder / multilingual motion tags from pivots (already in traces via origin)
  const classified = [];
  const completePackets = [];
  const partialPackets = [];
  const signalOnly = [];
  const rejected = [];

  for (const r of records) {
    const ev = evaluateCompleteDemandPacket(r, {
      defaultFitScore: hotel.defaultFitScore ?? 48,
      geoOk: true,
    });
    const row = {
      ...r,
      quality: ev.quality,
      strongCount: ev.strongCount,
      presentOrStrongCount: ev.presentOrStrongCount,
      missingPillars: (ev.missingPillars || []).join("|"),
      packet: ev.packet,
    };
    classified.push(row);
    if (
      ev.quality === PACKET_QUALITY.COMPLETE_STRONG ||
      ev.quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
    ) {
      completePackets.push(row);
    } else if (ev.quality === PACKET_QUALITY.PARTIAL_PACKET) {
      partialPackets.push(row);
    } else if (ev.quality === PACKET_QUALITY.REJECTED) {
      rejected.push(row);
    } else {
      signalOnly.push(row);
    }
  }

  // Completion only for qualified (+ high-potential partial with success match)
  const toComplete = [];
  for (const row of [...completePackets, ...partialPackets]) {
    const ev = evaluateCompleteDemandPacket(row, { geoOk: true, defaultFitScore: hotel.defaultFitScore });
    const match = successfulPacketPatternMatch(ev, { allowHighPotentialPartial: true });
    if (match.admit) toComplete.push({ row, match, ev });
  }

  const completions = [];
  const jevRows = [];
  const theses = [];
  const buyers = [];
  const customerReady = [];
  const futureWatch = [];

  for (const { row, match } of toComplete.slice(0, opts.maxPacketsToComplete ?? 6)) {
    const result = await completeDemandPacket(row, hotel, budget, {
      nowDate: opts.nowDate || "2026-10-04",
    });
    completions.push({
      packetId: row.id,
      hotelKey: hotel.hotelKey,
      qualityBefore: row.quality,
      qualityAfter: result.quality,
      successMatch: match.match,
      depth: result.depth,
      pillarsResolved: result.pillarsResolved,
      customerReady: result.customerReady,
      validFutureWatch: result.validFutureWatch,
      organization: row.organizationName,
    });
    for (const j of result.jevLog || []) jevRows.push(j);
    if (result.thesis) {
      theses.push({
        hotelKey: hotel.hotelKey,
        packetId: row.id,
        organization: row.organizationName,
        ...result.thesis,
      });
    }
    if (result.buyer) {
      buyers.push({
        hotelKey: hotel.hotelKey,
        packetId: row.id,
        ...result.buyer,
      });
    }
    if (result.customerReady) customerReady.push(result.record);
    else if (result.validFutureWatch) futureWatch.push(result.record);
  }

  return {
    hotelKey: hotel.hotelKey,
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    competitors: comp.competitors,
    pivots: comp.pivots,
    phoneHits: comp.phoneHits,
    pageRows: comp.pageRows,
    traces: comp.traces,
    repeatPatterns: comp.repeatPatterns,
    apifyInventory: buildApifyActorInventory(),
    apify,
    apifySignals,
    classified,
    completePackets,
    partialPackets,
    signalOnly,
    rejected,
    completions,
    jevRows,
    theses,
    buyers,
    customerReady,
    futureWatch,
    counts: {
      rawSignals: (comp.pivots || []).length + (apifySignals || []).length,
      validatedTraces: (comp.traces || []).length,
      directConfirmed: (comp.traces || []).filter(
        (t) => t.evidenceClass === EVIDENCE_CLASS.DIRECT_CONFIRMED
      ).length,
      strongAssociation: (comp.traces || []).filter(
        (t) => t.evidenceClass === EVIDENCE_CLASS.STRONG_ASSOCIATION
      ).length,
      completeStrong: completePackets.filter((p) => p.quality === PACKET_QUALITY.COMPLETE_STRONG)
        .length,
      completePlausible: completePackets.filter(
        (p) => p.quality === PACKET_QUALITY.COMPLETE_PLAUSIBLE
      ).length,
      completePackets: completePackets.length,
      partialPackets: partialPackets.length,
      signalOnly: signalOnly.length,
      rejected: rejected.length,
      customerReady: customerReady.length,
      futureWatch: futureWatch.length,
      apifyCompletePackets: completePackets.filter((p) => p.sourceFamily === "APIFY_TRIPADVISOR")
        .length,
      compCompletePackets: completePackets.filter((p) => p.sourceFamily === "COMP_SET").length,
    },
    costUsd: budget.costUsd,
    queriesRun: (comp.queriesRun || 0) + budget.queriesRun,
    errors: [...(comp.errors || []), ...budget.errors],
  };
}

export { COMP_SET_TARGET_HOTELS, buildApifyActorInventory };
