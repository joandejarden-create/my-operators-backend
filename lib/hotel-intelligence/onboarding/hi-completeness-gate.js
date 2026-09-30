/**
 * Hotel Intelligence completeness gate — ADP/GDI onboarding invariant.
 */

import { buildHotelIntelligenceProfile } from "../adp-attributes/build-hotel-intelligence-profile.js";
import {
  evaluateHotelDomainStatuses,
  loadDomainStatusLedger,
} from "./domain-status-store.js";
import {
  BLOCKING_DOMAIN_STATUSES,
  HI_DOMAIN_STATUS,
  OVERALL_HI_STATUS,
  REQUIRED_HI_DOMAINS,
  isDomainResolved,
} from "./domain-status-v1.js";
import { countActiveAdpAttributes } from "./adp-attribute-counts.js";

/**
 * @param {string} hpcHotelId
 * @param {{ profile?: object, adpAttributeActiveCount?: number, skipLiveHpc?: boolean }} [opts]
 */
export async function isHotelIntelligenceComplete(hpcHotelId, opts = {}) {
  const profile =
    opts.profile ||
    (await buildHotelIntelligenceProfile(hpcHotelId, {
      skipLiveHpc: opts.skipLiveHpc === true,
    }));
  if (!profile?.ok) {
    return {
      ok: false,
      complete: false,
      overallStatus: OVERALL_HI_STATUS.HI_ERROR,
      error: profile?.error || "profile_failed",
      hotelId: hpcHotelId,
      blockingDomains: [...REQUIRED_HI_DOMAINS],
    };
  }

  let adpCount = opts.adpAttributeActiveCount;
  if (adpCount == null && opts.skipAdpCount !== true) {
    try {
      adpCount = await countActiveAdpAttributes(profile.identity?.hpcHotelId || hpcHotelId);
    } catch {
      adpCount = loadDomainStatusLedger(hpcHotelId).domains?.ADP_ATTRIBUTES?.rowCount ?? 0;
    }
  }

  const evaluated = evaluateHotelDomainStatuses(profile.identity.hpcHotelId, profile, {
    adpAttributeActiveCount: adpCount,
  });

  const blocking = REQUIRED_HI_DOMAINS.filter((d) =>
    BLOCKING_DOMAIN_STATUSES.includes(evaluated.domains[d]?.domainStatus)
  ).map((d) => ({
    domain: d,
    status: evaluated.domains[d]?.domainStatus || HI_DOMAIN_STATUS.NOT_RESEARCHED,
    blocker: evaluated.domains[d]?.blocker || null,
    notes: evaluated.domains[d]?.notes || null,
  }));

  return {
    ok: true,
    complete: evaluated.overallStatus === OVERALL_HI_STATUS.HI_COMPLETE,
    overallStatus: evaluated.overallStatus,
    coverage: evaluated.coverage,
    domains: evaluated.domains,
    blockingDomains: blocking,
    hotelId: profile.identity.hpcHotelId,
    hotelName: profile.identity.hotelName,
    adpPropertyId: profile.identity.adpPropertyId,
    evaluatedAt: evaluated.evaluatedAt,
  };
}

/**
 * GDI hotel-fit contract: Event Spaces must not be treated as "no capability"
 * when status is NOT_RESEARCHED.
 */
export function gdiEventSpaceCapabilitySemantics(domainStatus) {
  const s = domainStatus || HI_DOMAIN_STATUS.NOT_RESEARCHED;
  if (s === HI_DOMAIN_STATUS.NOT_RESEARCHED) {
    return {
      mayTreatAsNoCapability: false,
      mayEnterFitWithKnownMeetingState: false,
      reason: "EVENT_SPACES_NOT_RESEARCHED",
      meetingCapabilityState: "UNKNOWN_UNRESEARCHED",
    };
  }
  if (s === HI_DOMAIN_STATUS.RESEARCHED_EMPTY) {
    return {
      mayTreatAsNoCapability: true,
      mayEnterFitWithKnownMeetingState: true,
      reason: "EVENT_SPACES_RESEARCHED_EMPTY",
      meetingCapabilityState: "KNOWN_EMPTY",
    };
  }
  if (s === HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING) {
    return {
      mayTreatAsNoCapability: false,
      mayEnterFitWithKnownMeetingState: true,
      reason: "EVENT_SPACES_PUBLIC_DATA_CEILING",
      meetingCapabilityState: "CEILING_UNKNOWN",
    };
  }
  if (s === HI_DOMAIN_STATUS.POPULATED) {
    return {
      mayTreatAsNoCapability: false,
      mayEnterFitWithKnownMeetingState: true,
      reason: "EVENT_SPACES_POPULATED",
      meetingCapabilityState: "KNOWN_POPULATED",
    };
  }
  return {
    mayTreatAsNoCapability: false,
    mayEnterFitWithKnownMeetingState: false,
    reason: `EVENT_SPACES_${s}`,
    meetingCapabilityState: "BLOCKED",
  };
}

export function assertNoNotResearchedForOnboard(gateResult) {
  if (!gateResult?.complete) {
    return {
      allowed: false,
      reason: "HI_INCOMPLETE",
      blockingDomains: gateResult?.blockingDomains || [],
    };
  }
  return { allowed: true, reason: "HI_COMPLETE" };
}

export { isDomainResolved, REQUIRED_HI_DOMAINS, HI_DOMAIN_STATUS };
