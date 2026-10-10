/**
 * Canonical convergence — all spines merge into one opportunity packet.
 * Same pillars / packet qualities / maturity rules. Multi-path provenance preserved.
 */

import { DISCOVERY_SPINE, IDV2_VERSION, PACKET_PILLAR_ID } from "./constants.js";
import { assessPacketPillars, computeNextBlocker } from "./next-blocker-engine.js";
import { customerSafeControllerCopy } from "./demand-controller-model.js";

function normName(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * Merge key for dedupe across spines.
 */
export function opportunityMergeKey(opp = {}) {
  const org = normName(opp.organizationName || opp.accountName || opp.namedEntity);
  const campaign = normName(opp.campaignId || opp.campaignName || opp.eventName || opp.title);
  const hotel = normName(opp.hotelId || opp.targetHotelId);
  const motion = normName(opp.participationRole || opp.demandType || opp.groupMotion || "");
  return [hotel, campaign, org, motion].filter(Boolean).join("::") || null;
}

/**
 * Merge multiple spine-produced candidates into one canonical packet draft.
 * Does NOT auto-promote Ready.
 */
export function convergeSpineResultsToPacket({
  existingOpportunity = null,
  spineResults = [],
  hotelId,
  marketProfile = {},
} = {}) {
  const provenance = [];
  let packet = existingOpportunity ? { ...existingOpportunity } : {};
  packet.hotelId = packet.hotelId || hotelId;
  packet.marketProfileVersion = marketProfile.marketProfileVersion || packet.marketProfileVersion;
  packet.idv2Version = IDV2_VERSION;

  for (const result of spineResults) {
    const spine = result.spine || result.discoverySpine;
    if (!spine) continue;

    if (spine === DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST) {
      for (const c of result.controllers || []) {
        provenance.push({
          discoverySpine: spine,
          baseOfDemand: c.baseOfDemand,
          demandControllerId: c.demandController?.demandControllerId,
          evidenceRoute: c.evidenceRoute,
          historicalVsCurrent: c.historicalVsCurrent,
        });
        if (!packet.demandControllerId && c.demandController) {
          packet.demandControllerId = c.demandController.demandControllerId;
          packet.demandController = c.demandController;
          packet.buyerEntity = packet.buyerEntity || c.demandController.controllerName;
          packet.contactPath = packet.contactPath || c.demandController.publicContactPath;
          const copy = customerSafeControllerCopy(c.demandController);
          if (copy) {
            packet.customerSafeController = copy;
            packet.lodgingDecisionLine = copy.lodgingDecisionLine;
            packet.bestRouteIn = copy.bestRouteIn;
          }
        }
      }
    }

    if (spine === DISCOVERY_SPINE.ACCOUNT_FIRST) {
      for (const a of result.accounts || []) {
        if (a.rejected) continue;
        provenance.push({
          discoverySpine: spine,
          baseOfDemand: a.baseOfDemand,
          organizationName: a.organizationName,
          evidenceRoute: a.evidenceRoute,
          historicalVsCurrent: a.historicalVsCurrent,
        });
        if (!packet.organizationName) {
          packet.organizationName = a.organizationName;
          packet.triggerFamily = a.triggerFamily;
          packet.evidenceUrl = a.evidenceUrl;
          packet.relationshipGraph = a.relationshipGraph;
        }
      }
    }

    if (spine === DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST) {
      for (const p of result.patterns || []) {
        provenance.push({
          discoverySpine: spine,
          baseOfDemand: p.baseOfDemand,
          evidenceRoute: p.evidenceRoute,
          historicalVsCurrent: "HISTORICAL",
          canEstablishCurrentHotelSelection: false,
        });
        packet.historicalProcess = packet.historicalProcess || [];
        packet.historicalProcess.push(p);
        // Process intelligence only — seed plausible controller type hint, not current placement
        if (!packet.demandControllerId && p.historicalPco) {
          packet.processControllerHint = {
            name: p.historicalPco,
            role: "HISTORICAL_PCO",
            requiresCurrentEvidence: true,
          };
        }
      }
    }

    if (spine === DISCOVERY_SPINE.HOTEL_HISTORY_FIRST) {
      for (const r of result.rows || []) {
        if (r.rejected) continue;
        provenance.push({
          discoverySpine: spine,
          baseOfDemand: r.baseOfDemand,
          evidenceRoute: r.evidenceRoute,
          hotelSuppliedEvidence: true,
          lookalikeConfirmedDemand: false,
        });
        packet.hotelSuppliedEvidence = packet.hotelSuppliedEvidence || [];
        packet.hotelSuppliedEvidence.push(r);
        if (!packet.organizationName && r.knownAccount) {
          packet.organizationName = r.knownAccount;
          packet.lookalikeFromHotelHistory = true;
          packet.lookalikeConfirmedDemand = false;
        }
      }
    }

    if (spine === DISCOVERY_SPINE.PARTICIPANT_FIRST && result.opportunity) {
      provenance.push({
        discoverySpine: spine,
        baseOfDemand: result.baseOfDemand,
        evidenceRoute: result.evidenceRoute || "OFFICIAL_PUBLIC",
        historicalVsCurrent: "CURRENT",
      });
      packet = { ...result.opportunity, ...packet, ...result.opportunity };
    }
  }

  packet.multiPathProvenance = [...(packet.multiPathProvenance || []), ...provenance];
  packet.discoverySpines = [
    ...new Set([
      ...(packet.discoverySpines || []),
      ...provenance.map((p) => p.discoverySpine).filter(Boolean),
    ]),
  ];
  packet.discoverySpine = packet.discoverySpines[packet.discoverySpines.length - 1] || null;

  const pillars = assessPacketPillars(packet);
  const blocker = computeNextBlocker(packet, {
    hotelId,
    market: marketProfile.market,
    country: marketProfile.country,
    marketProfile,
    hotelSuppliedEvidenceCount: (packet.hotelSuppliedEvidence || []).length,
  });
  packet.nextBlocker = blocker.nextBlocker;
  packet.nextBestPath = blocker.bestDiscoveryPath;
  packet.nextBlockerDetail = blocker;
  packet.pillarsPresent = pillars;
  packet.mergeKey = opportunityMergeKey(packet);

  // Never auto-promote
  packet.autoPromotedFromIdv2 = false;
  packet.maturityUnchangedByConvergence = true;

  return {
    packet,
    pillars,
    blocker,
    provenance,
    mergeKey: packet.mergeKey,
  };
}

/**
 * Merge duplicate opportunities that share merge key; preserve multi-path provenance.
 */
export function mergeDuplicateOpportunities(opportunities = []) {
  const map = new Map();
  for (const opp of opportunities) {
    const key = opportunityMergeKey(opp) || opp.opportunityId || opp.id;
    if (!key) continue;
    if (!map.has(key)) {
      map.set(key, { ...opp, multiPathProvenance: [...(opp.multiPathProvenance || [])] });
      continue;
    }
    const prev = map.get(key);
    const merged = {
      ...prev,
      ...opp,
      multiPathProvenance: [
        ...(prev.multiPathProvenance || []),
        ...(opp.multiPathProvenance || []),
        { mergedFrom: opp.opportunityId || opp.id, discoverySpine: opp.discoverySpine },
      ],
      discoverySpines: [
        ...new Set([...(prev.discoverySpines || []), ...(opp.discoverySpines || []), opp.discoverySpine].filter(Boolean)),
      ],
      demandControllerId: prev.demandControllerId || opp.demandControllerId,
      demandController: prev.demandController || opp.demandController,
    };
    map.set(key, merged);
  }
  return [...map.values()];
}

export { PACKET_PILLAR_ID };
