/**
 * Packet completion + Jev — only for COMPLETE_STRONG / COMPLETE_PLAUSIBLE
 * or high-potential PARTIAL (4/6) with success match.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  validateResearchLeadPage,
  applyPageEvidenceToLead,
} from "../opportunity-discovery-v5/page-validation.js";
import { resolveGdiDemandBuyer } from "../opportunity-discovery-v5/buyer-resolution.js";
import { buildTargetHotelThesis, FIT_CLASS } from "../comp-set-demand-mining-v1/target-thesis.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../future-watch/is-valid-future-watch-v1.js";
import {
  evaluateCompleteDemandPacket,
  PACKET_PILLAR,
  PACKET_QUALITY,
  isQualifiedForExpensiveCompletion,
  highPotentialPartial,
} from "./packet-schema.js";
import { successfulPacketPatternMatch, PACKET_MATCH } from "./success-calibration.js";

function hasSerp() {
  return Boolean(String(process.env.SERPAPI_KEY || process.env.SERPAPI_API_KEY || "").trim());
}

const PILLAR_QUERY = {
  [PACKET_PILLAR.C_BUYER_ORGANIZER_PATH]: (p, hotel) =>
    `"${p.organization}" (organizer OR secretariat OR "housing bureau" OR contact OR DMC) ${hotel.market || ""}`,
  [PACKET_PILLAR.D_FUTURE_DECISION_POINT]: (p, hotel) =>
    `"${p.organization}" (2026 OR 2027 OR 2028 OR "next edition" OR "site selection" OR RFP OR "housing opening") ${hotel.market || ""}`,
  [PACKET_PILLAR.E_HOTEL_LODGING_EVIDENCE]: (p, hotel) =>
    `"${p.organization}" ("hotel block" OR "official hotel" OR housing OR accommodation OR "room block") ${hotel.market || ""}`,
  [PACKET_PILLAR.B_DEFINED_GROUP_MOTION]: (p, hotel) =>
    `"${p.organization}" (congress OR conference OR meeting OR delegation OR kickoff) ${hotel.market || ""}`,
};

export function jevAdvisePacket(packetEval = {}, ctx = {}) {
  const q = packetEval.quality;
  const match = ctx.successMatch;
  const allowed =
    isQualifiedForExpensiveCompletion(q) ||
    (highPotentialPartial(packetEval) &&
      (match === PACKET_MATCH.STRONG || match === PACKET_MATCH.PLAUSIBLE));

  if (!allowed || ctx.admitted !== true) {
    return {
      issued: false,
      action: "STOP",
      reason: "PACKET_NOT_QUALIFIED_FOR_JEV",
      wroteFacts: false,
      promoted: false,
    };
  }

  const missing = packetEval.missingPillars || [];
  const pillar =
    missing.find((m) =>
      [
        PACKET_PILLAR.E_HOTEL_LODGING_EVIDENCE,
        PACKET_PILLAR.D_FUTURE_DECISION_POINT,
        PACKET_PILLAR.C_BUYER_ORGANIZER_PATH,
      ].includes(m)
    ) || missing[0];

  const depth = ctx.depth || 0;
  if (depth >= 2) {
    return {
      issued: true,
      action: "STOP_LOW_INFORMATION_GAIN",
      stopContinue: "STOP",
      nextPillar: pillar || null,
      oneMoreStepWorthwhile: false,
      wroteFacts: false,
      promoted: false,
      note: "Default max 2 steps; third exceptional only.",
    };
  }

  return {
    issued: true,
    action: depth === 0 ? "CONTINUE" : "SWITCH_SOURCE",
    stopContinue: "CONTINUE",
    nextPillar: pillar || PACKET_PILLAR.C_BUYER_ORGANIZER_PATH,
    exactQuestion: pillar
      ? `Resolve missing pillar ${pillar}`
      : "Confirm lodging + buyer + future decision",
    oneMoreStepWorthwhile: Boolean(pillar),
    wroteFacts: false,
    promoted: false,
  };
}

/**
 * Complete one qualified packet with ≤2 targeted steps.
 */
export async function completeDemandPacket(record = {}, hotel = {}, budget = {}, opts = {}) {
  let current = { ...record };
  let ev = evaluateCompleteDemandPacket(current, {
    defaultFitScore: hotel.defaultFitScore,
    geoOk: true,
  });
  const match = successfulPacketPatternMatch(ev, {
    allowHighPotentialPartial: true,
  });

  if (!match.admit) {
    return {
      packet: ev.packet,
      quality: ev.quality,
      successMatch: match.match,
      admitted: false,
      jevLog: [],
      depth: 0,
      customerReady: false,
      validFutureWatch: false,
      thesis: null,
      buyer: null,
    };
  }

  const jevLog = [];
  let depth = 0;
  let pillarsResolved = 0;

  // Buyer resolution (deterministic)
  const buyer = resolveGdiDemandBuyer(current, {});
  current = {
    ...current,
    buyerEntity: buyer.buyerEntity,
    buyerType: buyer.buyerType,
    organizer: buyer.organizer,
    agency: buyer.agency,
    publicContactPath: buyer.publicContactPath,
    organizationContactUrl: current.organizationContactUrl || buyer.publicContactPath,
  };

  while (depth < 2) {
    ev = evaluateCompleteDemandPacket(current, {
      defaultFitScore: hotel.defaultFitScore,
      geoOk: true,
    });
    const advice = jevAdvisePacket(ev, {
      admitted: true,
      successMatch: match.match,
      depth,
    });
    jevLog.push({
      packetId: current.id || current.packetId,
      hotelKey: hotel.hotelKey,
      depth: depth + 1,
      ...advice,
    });
    if (!advice.oneMoreStepWorthwhile || advice.stopContinue === "STOP") break;
    if (!hasSerp() || budget.queriesLeft <= 0) break;

    const qFn = PILLAR_QUERY[advice.nextPillar];
    const query = qFn
      ? qFn(
          {
            organization: current.organizationName || current.organization || current.title,
          },
          hotel
        )
      : null;
    if (!query) break;

    budget.queriesLeft -= 1;
    budget.queriesRun += 1;
    let hit = null;
    try {
      const serp = await serpapiSearch({
        engine: "google",
        q: query,
        num: 3,
        hl: hotel.serpHl || "en",
        gl: hotel.serpGl || "us",
      });
      budget.costUsd += 0.05;
      hit = (serp?.data?.organic_results || [])[0] || null;
    } catch (err) {
      budget.errors.push(String(err?.message || err).slice(0, 160));
      budget.costUsd += 0.05;
      break;
    }

    if (hit?.link) {
      const before = evaluateCompleteDemandPacket(current, { geoOk: true });
      const pageEv = await validateResearchLeadPage(
        { ...current, officialSource: hit.link },
        {}
      );
      budget.costUsd += 0.01;
      current = applyPageEvidenceToLead(
        {
          ...current,
          sources: [
            ...(current.sources || []),
            { url: hit.link, kind: "complete_demand_packet_v8", pillar: advice.nextPillar },
          ],
        },
        pageEv
      );
      const after = evaluateCompleteDemandPacket(current, { geoOk: true });
      if (after.presentOrStrongCount > before.presentOrStrongCount) pillarsResolved += 1;
    }
    depth += 1;
  }

  ev = evaluateCompleteDemandPacket(current, {
    defaultFitScore: hotel.defaultFitScore,
    geoOk: true,
  });

  const thesis = buildTargetHotelThesis(
    {
      organization: current.organizationName || current.organization,
      eventProgram: current.title,
      demandEngine: current.demandEngine,
      historicHotels: current.competitorHotel ? [current.competitorHotel] : [],
      repeatCadence: current.repeatCadence,
      nextKnownCycle: current.eventStartDate || current.eventYear,
      nextExpectedCycle: current.nextExpectedCycle,
      lodgingPattern: current.lodgingEvidence ? "LODGING_EVIDENCE_PRESENT" : "UNKNOWN",
      evidenceSet: current.evidenceClass,
      sources: current.officialSource,
      patternId: current.id,
    },
    hotel
  );

  current.hotelOpportunityThesis = [
    thesis.whyRelevant,
    thesis.whatTargetCouldWin,
    `Fit:${thesis.fitClass}`,
    thesis.fact,
  ].join(" ");
  current.hotelFitScore =
    thesis.fitClass === FIT_CLASS.STRONG_FIT
      ? Math.max(hotel.defaultFitScore || 50, 55)
      : hotel.defaultFitScore || 48;

  const ready = isGdiCustomerOpportunityReady(current, {
    nowDate: opts.nowDate || "2026-10-04",
  });
  const watch = isValidFutureWatch(current, { nowDate: opts.nowDate || "2026-10-04" });
  const watchOk =
    watch?.ok === true || watch?.valid === true || watch?.class === "VALID_FUTURE_WATCH";

  return {
    packet: ev.packet,
    quality: ev.quality,
    successMatch: match.match,
    admitted: true,
    record: current,
    jevLog,
    depth,
    pillarsResolved,
    classificationChanged:
      ev.quality !== evaluateCompleteDemandPacket(record, { geoOk: true }).quality,
    customerReady: ready.ok === true,
    validFutureWatch: watchOk && !ready.ok,
    thesis,
    buyer,
    readyFailed: ready.failed || [],
  };
}

export { PACKET_QUALITY, PACKET_MATCH };
