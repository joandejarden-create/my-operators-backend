/**
 * Second-source corroboration + correction confidence gates (V1.1).
 */

import { brandLabelsAlign } from "./match-confidence.js";
import { canonicalizeObservedBrand } from "./brand-family.js";
import { parseLooseDate, resolveTemporalConflict, sourcePriorityRank } from "./source-hierarchy.js";

export const CORROBORATION_CONFIG_V1_1 = Object.freeze({
  version: "contradiction-first-v1.1",
  pipelineToOpen: {
    primaryRequired: ["official_brand_directory", "official_hotel_website", "official_parent_page"],
    secondaryAccepted: [
      "official_opening_announcement",
      "official_parent_page",
      "official_hotel_website",
      "reputable_trade_press",
      "booking_availability",
    ],
  },
  reflag: {
    requirePropertyLevelBrand: true,
    preferSecondSource: true,
  },
});

/**
 * @param {object} observation - adapter observation
 * @param {object} match - assessEntityMatch result
 * @param {{ currentStatus: string, observedStatus: string }} status
 */
export function assessStatusCorroboration(observation, match, status) {
  const primaryOk =
    observation?.hotelFound &&
    ["official_brand_directory", "official_hotel_website", "official_parent_page"].includes(
      observation.sourceType
    ) &&
    (observation.bookable === true ||
      observation.rawSignals?.hasBookNow === true ||
      observation.rawSignals?.newHotelBanner === true ||
      /open/i.test(String(observation.operatingStatus || "")));

  const secondarySignals = [];
  if (observation.rawSignals?.newHotelBanner) secondarySignals.push("new_hotel_banner");
  if (observation.rawSignals?.hasBookNow && observation.rawSignals?.newHotelBanner) {
    secondarySignals.push("book_now_plus_new_hotel");
  }
  // V1.1: directory bookable + New Hotel banner counts as dual signal on same primary page
  const dualOnPrimary =
    primaryOk &&
    observation.rawSignals?.hasBookNow &&
    observation.rawSignals?.newHotelBanner;

  const singlePrimary = primaryOk && !dualOnPrimary && secondarySignals.length < 2;

  if (!match.allowMaterialCorrection) {
    return {
      tier: "Review",
      recommended_action: "Review",
      confidenceBand: "Medium",
      reason: `Entity match ${match.level} — material status change blocked`,
      primaryOk,
      secondarySignals,
      corroboration: "insufficient_match",
    };
  }

  if (status.currentStatus === "Pipeline" && status.observedStatus === "Open") {
    if (dualOnPrimary || (primaryOk && secondarySignals.length >= 1)) {
      return {
        tier: "High Confidence Proposed Update",
        recommended_action: "Proposed Status Change",
        confidenceBand: "High",
        reason: "Exact/High match + official bookable primary with corroborating signal",
        primaryOk,
        secondarySignals: dualOnPrimary ? ["bookable", "new_hotel_banner"] : secondarySignals,
        corroboration: dualOnPrimary ? "dual_primary_page_signals" : "primary_plus_secondary",
      };
    }
    if (primaryOk) {
      return {
        tier: "Proposed Update — Single Primary Source",
        recommended_action: "Proposed Status Change",
        confidenceBand: "Medium",
        reason: "Exact/High match + single official bookable source",
        primaryOk,
        secondarySignals,
        corroboration: "single_primary",
      };
    }
    return {
      tier: "Review",
      recommended_action: "Review",
      confidenceBand: "Low",
      reason: "Insufficient official bookable evidence",
      primaryOk,
      secondarySignals,
      corroboration: "weak",
    };
  }

  return {
    tier: "Review",
    recommended_action: "Review",
    confidenceBand: "Medium",
    reason: "Status pattern not a high-impact Pipeline→Open case",
    primaryOk,
    secondarySignals,
    corroboration: "n/a",
  };
}

/**
 * Brand change / reflag gate — parent membership is never enough.
 * @param {object} hotel
 * @param {object} observation
 * @param {object} match
 */
export function assessReflagCorroboration(hotel, observation, match) {
  const current = canonicalizeObservedBrand(hotel.currentBrand || hotel.affiliation || "");
  const observed = canonicalizeObservedBrand(observation.brand || "");
  const brandConf = brandLabelsAlign(current, observed);

  const propertyLevel =
    Boolean(observation.officialUrl) &&
    Boolean(observed) &&
    observation.hotelFound === true &&
    !/ihg\.com\/?$/i.test(observation.officialUrl) &&
    !/marriott\.com\/?$/i.test(observation.officialUrl);

  const brandConfidence =
    brandConf === "align"
      ? "aligned"
      : propertyLevel && observed
        ? "property_level_label"
        : "weak";

  if (!observed || brandConf !== "conflict") {
    return {
      tier: "No Action",
      recommended_action: "No Change",
      confidenceBand: "High",
      reason: "No property-level brand conflict",
      brandConfidence,
      propertyLevel,
      currentBrand: current,
      observedBrand: observed,
    };
  }

  if (!match.allowMaterialCorrection) {
    return {
      tier: match.allowReviewOnly ? "Medium Confidence Review" : "Low Confidence / No Action",
      recommended_action: match.allowReviewOnly ? "Review" : "Insufficient Evidence",
      confidenceBand: match.level === "Medium" ? "Medium" : "Low",
      reason: `Reflag blocked — entity match ${match.level}`,
      brandConfidence,
      propertyLevel,
      currentBrand: current,
      observedBrand: observed,
    };
  }

  if (!propertyLevel) {
    return {
      tier: "Low Confidence / No Action",
      recommended_action: "Insufficient Evidence",
      confidenceBand: "Low",
      reason: "Parent/domain membership is not property-level brand evidence",
      brandConfidence: "parent_only",
      propertyLevel: false,
      currentBrand: current,
      observedBrand: observed,
    };
  }

  // Single official directory page with Exact/High match → Medium reflag review or proposed with medium band
  return {
    tier: "Proposed Update — Single Primary Source",
    recommended_action: "Proposed Reflag",
    confidenceBand: "Medium",
    reason: "Property-level official brand label differs from Dealality with Exact/High entity match",
    brandConfidence,
    propertyLevel,
    currentBrand: current,
    observedBrand: observed,
    evidence: {
      officialUrl: observation.officialUrl,
      sourceType: observation.sourceType,
      evidenceDate: observation.sourceDate || observation.evidenceTimestamp,
      parent: observation.parent,
    },
  };
}

/**
 * Apply temporal supersession labeling for status claims.
 */
export function applyTemporalStatusResolution(hotel, observation, claimType = "OPERATING_STATUS") {
  const current = hotel.currentStatus;
  const observed = observation.operatingStatus;
  if (!current || !observed || current === observed) {
    return { claimStatus: current && observed ? "Confirmed" : "Unknown", winningValue: observed || current };
  }

  const older = {
    value: current,
    sourceType: "secondary",
    sourceDate: hotel.dealalityLastVerified || null,
    evidenceRetrievalDate: hotel.dealalityLastVerified || null,
  };
  const newer = {
    value: observed,
    sourceType: observation.sourceType || "official_brand_directory",
    sourceDate: observation.sourceDate || null,
    evidenceRetrievalDate: observation.evidenceTimestamp || new Date().toISOString(),
  };

  const temporal = resolveTemporalConflict(older, newer, claimType);
  if (current === "Pipeline" && observed === "Open" && temporal.claimStatus === "Conflicting Evidence") {
    // Prefer Superseded when official directory is clearly newer retrieval than unknown Dealality verify date
    if (!hotel.dealalityLastVerified && sourcePriorityRank(claimType, newer.sourceType) <= 1) {
      return {
        claimStatus: "Superseded",
        winningValue: observed,
        reason: "Official current directory bookable evidence supersedes undated Pipeline value",
        temporal,
        dates: {
          dealalityLastVerified: hotel.dealalityLastVerified || null,
          sourcePublicationDate: observation.sourceDate || null,
          evidenceRetrievalDate: observation.evidenceTimestamp || null,
          eventDate: observation.rawSignals?.openDate || null,
        },
      };
    }
  }
  return {
    claimStatus: temporal.claimStatus,
    winningValue: temporal.winningValue || observed,
    reason: temporal.reason,
    temporal,
    dates: {
      dealalityLastVerified: hotel.dealalityLastVerified || null,
      sourcePublicationDate: observation.sourceDate || null,
      evidenceRetrievalDate: observation.evidenceTimestamp || null,
      eventDate: observation.rawSignals?.openDate || null,
    },
  };
}

/**
 * Final correction gate — maps corroboration + match into queue tier.
 */
export function gateCorrection({ match, corroboration, field }) {
  if (!corroboration) {
    return { queue: "none", recommended_action: "Insufficient Evidence", confidenceBand: "Low" };
  }
  if (corroboration.tier === "Low Confidence / No Action" || corroboration.recommended_action === "Insufficient Evidence") {
    return {
      queue: "research_history_only",
      recommended_action: "Insufficient Evidence",
      confidenceBand: "Low",
      preserveEvidence: true,
    };
  }
  if (corroboration.tier === "Medium Confidence Review" || corroboration.recommended_action === "Review") {
    return {
      queue: "review",
      recommended_action: "Review",
      confidenceBand: corroboration.confidenceBand || "Medium",
      preserveEvidence: true,
    };
  }
  if (
    corroboration.tier === "High Confidence Proposed Update" ||
    corroboration.tier === "Proposed Update — Single Primary Source"
  ) {
    if (!match.allowMaterialCorrection) {
      return {
        queue: "review",
        recommended_action: "Review",
        confidenceBand: "Medium",
        preserveEvidence: true,
        reason: "Downgraded — match gate",
      };
    }
    return {
      queue: corroboration.confidenceBand === "High" ? "proposed_high" : "proposed_medium",
      recommended_action: corroboration.recommended_action,
      confidenceBand: corroboration.confidenceBand,
      preserveEvidence: true,
      field,
    };
  }
  return {
    queue: "none",
    recommended_action: corroboration.recommended_action || "No Change",
    confidenceBand: corroboration.confidenceBand || "High",
  };
}

export { parseLooseDate };
