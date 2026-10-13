/**
 * Cross-competitor Competitive Demand Pattern mining.
 */

import { buildCompSetRepeatPattern } from "./repeat-pattern.js";

function orgKey(t) {
  return String(t.organization || t.eventSeriesId || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 60);
}

/**
 * Group traces by organization across competitors → Competitive Demand Pattern rows.
 */
export function buildCrossCompetitorPatterns(traces = [], targetHotel = {}) {
  const byOrg = new Map();
  for (const t of traces) {
    if (!t.feedsDeeperResearch) continue;
    const k = orgKey(t);
    if (!k || k.length < 4) continue;
    if (!byOrg.has(k)) byOrg.set(k, []);
    byOrg.get(k).push(t);
  }

  const patterns = [];
  for (const [, group] of byOrg) {
    const repeat = buildCompSetRepeatPattern(group);
    const hotels = [...new Set(group.map((t) => t.competitorHotel).filter(Boolean))];
    const markets = [...new Set(group.map((t) => t.market).filter(Boolean))];
    const cycles = [
      ...new Set(group.map((t) => t.eventCycleId || t.eventYear).filter(Boolean)),
    ].sort();

    patterns.push({
      patternId: `cdp_${targetHotel.hotelKey || "h"}_${orgKey(group[0])}`.slice(0, 80),
      organization: group[0].organization,
      eventSeries: group[0].eventSeriesId,
      historicHotels: hotels,
      historicMarkets: markets,
      historicCycles: cycles,
      demandEngine: group[0].demandEngine,
      groupSizeRange: null,
      lodgingPattern: group.some((t) => t.lodgingEvidence) ? "LODGING_EVIDENCE_PRESENT" : "UNKNOWN",
      repeatCadence: repeat?.cadence || "UNKNOWN",
      agency: null,
      organizer: group[0].organization,
      buyerEntity: group[0].organization,
      nextExpectedCycle: repeat?.nextExpectedCycle || null,
      evidenceSet: group.map((t) => t.evidenceClass).join("|"),
      crossCompetitor: hotels.length > 1,
      traceCount: group.length,
      targetHotelKey: targetHotel.hotelKey || targetHotel.targetHotelKey,
      sources: group.map((t) => t.source).join("|"),
    });
  }

  return patterns.sort((a, b) => b.traceCount - a.traceCount || (b.crossCompetitor ? 1 : 0));
}
