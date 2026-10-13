/**
 * Success control calibration — Bethesda/NYC ready-time pillars.
 */

import { buildGdiSuccessfulOpportunityPattern } from "../opportunity-discovery-v5/success-pattern.js";
import { evaluateCompleteDemandPacket, PACKET_QUALITY } from "./packet-schema.js";

export const PACKET_MATCH = Object.freeze({
  STRONG: "STRONG",
  PLAUSIBLE: "PLAUSIBLE",
  WEAK: "WEAK",
});

export async function buildSuccessControlPackets(opts = {}) {
  const pattern = await buildGdiSuccessfulOpportunityPattern(opts);
  const rows = [];
  for (const c of pattern.controls || []) {
    const record = {
      id: c.opportunityId,
      hotelKey: c.hotelKey,
      organizationName: c.organizationName,
      title: c.title,
      opportunityType: c.opportunityType || c.groupMotion,
      groupMotion: c.groupMotion,
      eventStartDate: c.eventStartDate,
      lodgingEvidence:
        c.lodgingEvidence && c.lodgingEvidence !== "WEAK"
          ? { housingPageFound: true, status: "CREDIBLE" }
          : null,
      organizationContactUrl: c.contactPath ? "PATH_PRESENT" : null,
      hotelFitScore: c.hotelFit === "PASS" ? 55 : 40,
      officialSource: c.officialSource,
      hotelOpportunityThesis: c.thesisSnippet,
      demandEngine: c.demandEngine,
      sourceFamily: c.howDiscovered,
    };
    const ev = evaluateCompleteDemandPacket(record, { defaultFitScore: 55, geoOk: true });
    rows.push({
      ...c,
      packetQuality: ev.quality,
      pillarA: ev.pillars.A_NAMED_DEMAND_ENTITY?.strength,
      pillarB: ev.pillars.B_DEFINED_GROUP_MOTION?.strength,
      pillarC: ev.pillars.C_BUYER_ORGANIZER_PATH?.strength,
      pillarD: ev.pillars.D_FUTURE_DECISION_POINT?.strength,
      pillarE: ev.pillars.E_HOTEL_LODGING_EVIDENCE?.strength,
      pillarF: ev.pillars.F_TARGET_HOTEL_FIT?.strength,
      strongCount: ev.strongCount,
      presentOrStrongCount: ev.presentOrStrongCount,
    });
  }
  return { pattern, controlPackets: rows };
}

/**
 * successfulPacketPatternMatch — research admission only.
 */
export function successfulPacketPatternMatch(evalResult = {}, opts = {}) {
  const n = evalResult.presentOrStrongCount || 0;
  const s = evalResult.strongCount || 0;
  const q = evalResult.quality;
  let match = PACKET_MATCH.WEAK;
  if (
    q === PACKET_QUALITY.COMPLETE_STRONG ||
    (n >= 6 && s >= 3)
  ) {
    match = PACKET_MATCH.STRONG;
  } else if (
    q === PACKET_QUALITY.COMPLETE_PLAUSIBLE ||
    (n >= 5 && s >= 2)
  ) {
    match = PACKET_MATCH.PLAUSIBLE;
  }
  const admit =
    (q === PACKET_QUALITY.COMPLETE_STRONG ||
      q === PACKET_QUALITY.COMPLETE_PLAUSIBLE ||
      (opts.allowHighPotentialPartial &&
        q === PACKET_QUALITY.PARTIAL_PACKET &&
        n >= 4)) &&
    (match === PACKET_MATCH.STRONG || match === PACKET_MATCH.PLAUSIBLE);

  return { match, admit, presentOrStrongCount: n, strongCount: s, quality: q };
}
