/**
 * Cross-hotel portability evaluation (Renaissance ↔ Hilton / NYC).
 * Does not copy hotel-specific fit/priority/why-hotel.
 */

import { extractMarketOpportunityPacket } from "../market-opportunity-graph/market-opportunity-packet-v1.js";
import { buildHotelOpportunityFromMarketPacket } from "../market-opportunity-graph/build-hotel-opportunity-from-market-v1.js";
import { evaluateHotelGeographicApplicability } from "../market-opportunity-graph/geographic-applicability-v1.js";
import { evaluateCrossHotelFit } from "../market-opportunity-graph/cross-hotel-fit-v1.js";
import { decideSupportingDataNextAction } from "../market-opportunity-graph/jev-supporting-data-router-v1.js";
import { isCustomerFacingOpportunity, isGdiTestOrFixtureOpportunity } from "../customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "../customer-readiness-gate-v1.js";
import { classifyDominantBlocker, isHardTerminalBlocker } from "./blocker-taxonomy-v1.js";

export const PORTABILITY_CLASS = Object.freeze({
  HOTEL_SPECIFIC: "HOTEL_SPECIFIC",
  MARKET_PORTABLE: "MARKET_PORTABLE",
  CONDITIONAL_PORTABLE: "CONDITIONAL_PORTABLE",
  NOT_PORTABLE: "NOT_PORTABLE",
});

/**
 * Classify whether a source-hotel opportunity is portable to a peer.
 */
export function classifyPortability(seedOpp, sourceProfile, targetProfile) {
  const title = `${seedOpp.title || ""} ${seedOpp.summaryWhyHotel || ""} ${seedOpp.hotelOpportunityThesis || ""}`;
  const sourceName = sourceProfile?.displayName || "";
  const targetMeet = targetProfile?.meetingSqFt ?? 0;
  const sourceMeet = sourceProfile?.meetingSqFt ?? 0;
  const needsMeeting = /\b(meeting|conference|symposium|convention|ballroom|plenary|banquet)\b/i.test(
    `${seedOpp.title || ""} ${seedOpp.summaryWhat || ""} ${seedOpp.opportunityType || ""}`
  );
  const isOverflow = /overflow|housing|room.?block|exhibitor|vip housing/i.test(
    `${seedOpp.title || ""} ${seedOpp.opportunityType || ""}`
  );

  // Explicit hotel-name lock in thesis
  if (
    sourceName &&
    new RegExp(sourceName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(title) &&
    /exclusive|only at|host hotel is|headquarters hotel/i.test(title)
  ) {
    return {
      portabilityClass: PORTABILITY_CLASS.HOTEL_SPECIFIC,
      reason: "Source hotel named as exclusive/host hotel constraint",
    };
  }

  if (/exclusive host hotel|official headquarters hotel only/i.test(title)) {
    return {
      portabilityClass: PORTABILITY_CLASS.HOTEL_SPECIFIC,
      reason: "Exclusive host-hotel language",
    };
  }

  // Meeting-led demand vs tiny meeting inventory
  if (needsMeeting && !isOverflow && targetMeet != null && targetMeet > 0 && targetMeet < 1000 && sourceMeet >= 2000) {
    return {
      portabilityClass: PORTABILITY_CLASS.CONDITIONAL_PORTABLE,
      reason: `Target meeting inventory low (${targetMeet} sq ft) vs meeting-led demand; verify lodging-only path`,
    };
  }

  if (needsMeeting && !isOverflow && (targetMeet == null || targetMeet === 0)) {
    return {
      portabilityClass: PORTABILITY_CLASS.CONDITIONAL_PORTABLE,
      reason: "Target meeting capability unknown/empty — verify whether onsite meetings required",
    };
  }

  // Same metro / TS cluster → default market-portable for lodging/overflow
  if (
    sourceProfile?.metro &&
    targetProfile?.metro &&
    sourceProfile.metro === targetProfile.metro
  ) {
    if (isOverflow || /exhibitor|sponsor|housing/i.test(String(seedOpp.opportunityType || ""))) {
      return {
        portabilityClass: PORTABILITY_CLASS.MARKET_PORTABLE,
        reason: "Same metro lodging/overflow/exhibitor motion",
      };
    }
    return {
      portabilityClass: PORTABILITY_CLASS.MARKET_PORTABLE,
      reason: "Same metro market demand; hotel-specific fit still required",
    };
  }

  return {
    portabilityClass: PORTABILITY_CLASS.NOT_PORTABLE,
    reason: "Metro/geography mismatch or insufficient shared market signal",
  };
}

/**
 * Evaluate one source opportunity against a target hotel without copying fit.
 */
export function evaluatePortabilityCandidate({
  seedOpp,
  sourceProfile,
  targetProfile,
  nowDate = new Date().toISOString().slice(0, 10),
  requireStrictReady = false,
} = {}) {
  if (isGdiTestOrFixtureOpportunity(seedOpp)) {
    return { skipped: true, reason: "test_or_fixture" };
  }

  const portability = classifyPortability(seedOpp, sourceProfile, targetProfile);
  if (
    portability.portabilityClass === PORTABILITY_CLASS.HOTEL_SPECIFIC ||
    portability.portabilityClass === PORTABILITY_CLASS.NOT_PORTABLE
  ) {
    return {
      skipped: false,
      sourceOpportunityId: seedOpp.id || seedOpp.opportunityId,
      title: seedOpp.title,
      portability,
      hiltonFit: null,
      hiltonBlocker: portability.portabilityClass,
      built: null,
      jev: null,
    };
  }

  const marketPacket = extractMarketOpportunityPacket(
    seedOpp,
    sourceProfile?.hotelId
  );
  const geo = evaluateHotelGeographicApplicability(seedOpp, targetProfile);
  const fit = evaluateCrossHotelFit({
    marketPacket,
    hotelProfile: targetProfile,
    geographicApplicability: geo,
    seedOpp,
  });
  const blocker = classifyDominantBlocker({
    opp: { ...seedOpp, hotelFitScore: fit.hotelFitScore },
    hotelProfile: targetProfile,
    readiness: { ok: false, failed: [] },
    fit,
  });
  const jev = decideSupportingDataNextAction({
    marketPacket,
    geographicApplicability: geo,
    hotelFit: fit,
  });

  const built = buildHotelOpportunityFromMarketPacket({
    marketPacket,
    seedOpp,
    targetHotelProfile: targetProfile,
    nowDate,
    requireStrictReady,
  });

  return {
    skipped: false,
    sourceOpportunityId: seedOpp.id || seedOpp.opportunityId,
    title: seedOpp.title,
    marketOpportunityId: marketPacket.marketOpportunityId,
    portability,
    targetFitScore: fit.hotelFitScore,
    targetFinalState: fit.finalState,
    targetBlocker: blocker.blocker,
    targetReasons: fit.reasons || [],
    jev,
    built: built
      ? {
          ok: built.ok,
          reason: built.reason || null,
          finalState: built.finalState || null,
          opportunityId: built.opportunity?.id || built.opportunity?.opportunityId || null,
          hotelFitScore: built.opportunity?.hotelFitScore ?? null,
          customerFacingState: built.opportunity?.customerFacingState || null,
          readinessOk: built.opportunity
            ? isGdiCustomerOpportunityReady(built.opportunity).ok
            : false,
          whyHotel: built.opportunity?.summaryWhyHotel || null,
        }
      : null,
  };
}

/**
 * Renaissance → Hilton portability pass over visible/ready Renaissance rows.
 */
export function runRenaissanceToHiltonPortability({
  renaissanceOpps = [],
  renaissanceProfile,
  hiltonProfile,
  nowDate,
} = {}) {
  const sourceRows = renaissanceOpps.filter(
    (o) =>
      !isGdiTestOrFixtureOpportunity(o) &&
      (isCustomerFacingOpportunity(o) || isGdiCustomerOpportunityReady(o).ok)
  );

  const results = [];
  for (const opp of sourceRows) {
    results.push(
      evaluatePortabilityCandidate({
        seedOpp: opp,
        sourceProfile: renaissanceProfile,
        targetProfile: hiltonProfile,
        nowDate,
        requireStrictReady: false,
      })
    );
  }

  const summary = {
    renaissanceReviewed: sourceRows.length,
    marketPortable: results.filter(
      (r) => r.portability?.portabilityClass === PORTABILITY_CLASS.MARKET_PORTABLE
    ).length,
    conditional: results.filter(
      (r) => r.portability?.portabilityClass === PORTABILITY_CLASS.CONDITIONAL_PORTABLE
    ).length,
    notPortable: results.filter(
      (r) =>
        r.portability?.portabilityClass === PORTABILITY_CLASS.NOT_PORTABLE ||
        r.portability?.portabilityClass === PORTABILITY_CLASS.HOTEL_SPECIFIC
    ).length,
    hiltonBuildOk: results.filter((r) => r.built?.ok).length,
    hiltonReady: results.filter((r) => r.built?.readinessOk).length,
    hiltonHeld: results.filter(
      (r) =>
        r.built?.ok &&
        !r.built?.readinessOk &&
        !isHardTerminalBlocker(r.targetBlocker)
    ).length,
    hiltonRejected: results.filter(
      (r) =>
        !r.built?.ok ||
        isHardTerminalBlocker(r.targetBlocker) ||
        r.portability?.portabilityClass === PORTABILITY_CLASS.NOT_PORTABLE ||
        r.portability?.portabilityClass === PORTABILITY_CLASS.HOTEL_SPECIFIC
    ).length,
  };

  return { results, summary, sourceRows };
}

/**
 * Reverse: Hilton → Renaissance (and optionally NOW NOW).
 */
export function runReverseNycPortability({
  hiltonOpps = [],
  hiltonProfile,
  targetProfiles = [],
  nowDate,
} = {}) {
  const sourceRows = hiltonOpps.filter(
    (o) =>
      !isGdiTestOrFixtureOpportunity(o) &&
      (isCustomerFacingOpportunity(o) || isGdiCustomerOpportunityReady(o).ok)
  );
  const byTarget = [];
  for (const targetProfile of targetProfiles) {
    const results = sourceRows.map((opp) =>
      evaluatePortabilityCandidate({
        seedOpp: opp,
        sourceProfile: hiltonProfile,
        targetProfile,
        nowDate,
        requireStrictReady: false,
      })
    );
    byTarget.push({
      targetHotelId: targetProfile.hotelId,
      targetName: targetProfile.displayName,
      results,
      accepted: results.filter((r) => r.built?.ok).length,
      held: results.filter((r) => r.built?.ok && !r.built?.readinessOk).length,
      rejected: results.filter((r) => !r.built?.ok).length,
    });
  }
  return {
    demandEntitiesTested: sourceRows.length,
    byTarget,
  };
}
