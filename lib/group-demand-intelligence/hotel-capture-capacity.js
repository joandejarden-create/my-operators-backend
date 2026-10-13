/**
 * Hotel-relative group capture capacity model.
 * REUSABLE_PRODUCT_LOGIC — hotel keys/constraints come from profile/config (HOTEL_SPECIFIC_DATA).
 *
 * Waterstone A3: market-wide peak rooms must not be scored as if the hotel must host the entire event.
 */

import { OPPORTUNITY_TYPE } from "./claim-types.js";

export const CAPTURE_CAPACITY_STATE = Object.freeze({
  IDEAL_CAPTURE_RANGE: "IDEAL_CAPTURE_RANGE",
  PLAUSIBLE_CAPTURE: "PLAUSIBLE_CAPTURE",
  STRETCH: "STRETCH",
  OVERFLOW_ONLY: "OVERFLOW_ONLY",
  TOO_LARGE_FOR_PRIMARY: "TOO_LARGE_FOR_PRIMARY",
  TOO_SMALL_LOW_VALUE: "TOO_SMALL_LOW_VALUE",
  UNKNOWN: "UNKNOWN",
});

export const CAPTURE_CAPACITY_STATE_LABEL = Object.freeze({
  IDEAL_CAPTURE_RANGE: "Ideal capture range",
  PLAUSIBLE_CAPTURE: "Plausible capture",
  STRETCH: "Stretch",
  OVERFLOW_ONLY: "Overflow only",
  TOO_LARGE_FOR_PRIMARY: "Too large for primary",
  TOO_SMALL_LOW_VALUE: "Too small / low value",
  UNKNOWN: "Unknown",
});

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/**
 * Realistic sellable group inventory for one hotel night peak.
 * Defaults: ~70% of keys for full-service; clamp with commercialPriorities band when present.
 */
export function resolveRealisticGroupCaptureCapacity(hotelContext = {}) {
  const keys =
    num(hotelContext.totalGuestrooms) ||
    num(hotelContext.rooms) ||
    num(hotelContext.guestrooms?.totalGuestrooms) ||
    num(hotelContext.capabilityProfile?.totalGuestrooms);

  if (!keys) {
    return {
      realisticGroupCaptureCapacity: null,
      totalGuestrooms: null,
      method: "unknown_keys",
    };
  }

  const minBand = num(hotelContext.commercialPriorities?.coreTargetPeakRoomsMin);
  const maxBand = num(hotelContext.commercialPriorities?.coreTargetPeakRoomsMax);
  const defaultMax = Math.max(20, Math.min(Math.round(keys * 0.7), keys - 5));
  const defaultMin = Math.max(10, Math.round(keys * 0.12));

  const capacityMax = maxBand || defaultMax;
  const capacityMin = minBand || defaultMin;
  const mid = Math.round((capacityMin + capacityMax) / 2);

  return {
    totalGuestrooms: keys,
    realisticGroupCaptureCapacity: mid,
    captureCapacityMin: capacityMin,
    captureCapacityMax: capacityMax,
    method: maxBand ? "commercial_priorities_band" : "keys_fraction_default",
  };
}

/**
 * Estimate hotel's realistic share of a larger housing program / citywide demand.
 */
export function estimateCaptureShareRooms({
  peakRoomDemand,
  captureCapacityMax,
  opportunityType,
  hasHousingAccess,
  hasOverflowThesis,
} = {}) {
  const peak = num(peakRoomDemand);
  const cap = num(captureCapacityMax);
  if (!peak || !cap) return null;

  if (opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING || hasOverflowThesis) {
    // Overflow: hotel captures a slice, not the program
    const share = hasHousingAccess ? Math.min(cap, Math.round(peak * 0.15)) : Math.min(cap, Math.round(peak * 0.08));
    return Math.max(15, share);
  }

  if (peak <= cap) return peak;
  // Primary pursuit of oversized demand without overflow path → cannot capture full peak
  return hasHousingAccess ? Math.min(cap, Math.round(peak * 0.25)) : null;
}

/**
 * @param {object} input
 * @param {number} [input.peakRoomDemand] market or program peak rooms
 * @param {object} [input.hotelContext] totalGuestrooms / commercialPriorities / capabilityProfile
 * @param {string} [input.opportunityType]
 * @param {boolean} [input.hasHousingAccess] hotel list / TTS / official housing path
 * @param {boolean} [input.hasOverflowThesis]
 * @param {boolean} [input.independentBookingPattern]
 * @returns {object}
 */
export function calculateHotelGroupCaptureCapacity(input = {}) {
  const {
    peakRoomDemand = null,
    hotelContext = {},
    opportunityType = null,
    hasHousingAccess = false,
    hasOverflowThesis = false,
    independentBookingPattern = false,
    roomDemandStatus = null,
  } = input;

  const base = resolveRealisticGroupCaptureCapacity(hotelContext);
  const peak = num(peakRoomDemand);

  if (!base.realisticGroupCaptureCapacity) {
    return {
      ...base,
      peakRoomDemand: peak,
      estimatedCaptureRooms: null,
      captureRatio: null,
      captureCapacityState: CAPTURE_CAPACITY_STATE.UNKNOWN,
      captureCapacityStateLabel: CAPTURE_CAPACITY_STATE_LABEL.UNKNOWN,
      physicalFitScore: 50,
      rationale: "Hotel keys unknown — capacity-relative fit cannot be computed.",
    };
  }

  if (!peak) {
    return {
      ...base,
      peakRoomDemand: null,
      estimatedCaptureRooms: null,
      captureRatio: null,
      captureCapacityState: CAPTURE_CAPACITY_STATE.UNKNOWN,
      captureCapacityStateLabel: CAPTURE_CAPACITY_STATE_LABEL.UNKNOWN,
      physicalFitScore: 55,
      rationale: "Peak room demand unknown — neutral physical fit pending evidence.",
    };
  }

  const isOverflowType =
    opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING || hasOverflowThesis;
  const accessible =
    hasHousingAccess ||
    independentBookingPattern ||
    isOverflowType ||
    /VERIFIED_HOUSING|STRONG_ROOM|OVERFLOW/i.test(String(roomDemandStatus || ""));

  const estimatedCaptureRooms = estimateCaptureShareRooms({
    peakRoomDemand: peak,
    captureCapacityMax: base.captureCapacityMax,
    opportunityType,
    hasHousingAccess: accessible,
    hasOverflowThesis: isOverflowType,
  });

  const ratio =
    estimatedCaptureRooms != null
      ? estimatedCaptureRooms / base.realisticGroupCaptureCapacity
      : peak / base.realisticGroupCaptureCapacity;

  let state = CAPTURE_CAPACITY_STATE.UNKNOWN;
  let physicalFitScore = 50;
  let rationale = "";

  const min = base.captureCapacityMin;
  const max = base.captureCapacityMax;

  // Peak fits inside hotel band → score as primary capture range even if commercial
  // posture is overflow/housing (overflow is a sales path, not a size signal).
  const peakFitsBand = peak >= min * 0.5 && peak <= max;

  if (peak < min * 0.5 && !isOverflowType) {
    state = CAPTURE_CAPACITY_STATE.TOO_SMALL_LOW_VALUE;
    physicalFitScore = 35;
    rationale = `Peak demand (~${peak}) is below meaningful group value for a ${base.totalGuestrooms}-key hotel.`;
  } else if (peakFitsBand) {
    if (peak >= min && peak <= max) {
      state = isOverflowType
        ? CAPTURE_CAPACITY_STATE.PLAUSIBLE_CAPTURE
        : CAPTURE_CAPACITY_STATE.IDEAL_CAPTURE_RANGE;
      physicalFitScore = isOverflowType ? 86 : 92;
      rationale = isOverflowType
        ? `Overflow/housing posture with peak (~${peak}) inside hotel capture band (${min}–${max}) — realistic share, not market-wide demand.`
        : `Peak (~${peak}) sits inside hotel capture band (${min}–${max}) for a ${base.totalGuestrooms}-key property.`;
    } else {
      state = CAPTURE_CAPACITY_STATE.PLAUSIBLE_CAPTURE;
      physicalFitScore = 78;
      rationale = `Peak (~${peak}) is below core band floor (${min}) but still commercially plausible.`;
    }
  } else if (isOverflowType || (peak > max && accessible)) {
    if (!accessible && peak > max * 1.5) {
      state = CAPTURE_CAPACITY_STATE.TOO_LARGE_FOR_PRIMARY;
      physicalFitScore = 28;
      rationale = `Market peak (~${peak}) exceeds hotel capture band (${min}–${max}) with no accessible housing/overflow path.`;
    } else {
      state = CAPTURE_CAPACITY_STATE.OVERFLOW_ONLY;
      const capture = estimatedCaptureRooms || Math.min(max, Math.round(peak * 0.12));
      const captureRatio = capture / base.realisticGroupCaptureCapacity;
      physicalFitScore =
        captureRatio >= 0.5 && captureRatio <= 1.15 ? 88 : captureRatio >= 0.25 ? 78 : 68;
      rationale = `Large program (~${peak} peak) — evaluate hotel share (~${capture} rooms) within ${min}–${max} band, not full market demand.`;
      return {
        ...base,
        peakRoomDemand: peak,
        estimatedCaptureRooms: capture,
        captureRatio: Math.round(captureRatio * 100) / 100,
        captureCapacityState: state,
        captureCapacityStateLabel: CAPTURE_CAPACITY_STATE_LABEL[state],
        physicalFitScore,
        rationale,
        housingAccessAssumed: accessible,
      };
    }
  } else if (peak > max * 1.25 && !accessible) {
    state = CAPTURE_CAPACITY_STATE.TOO_LARGE_FOR_PRIMARY;
    physicalFitScore = 30;
    rationale = `Peak (~${peak}) exceeds primary capture band (${min}–${max}) without housing accessibility evidence.`;
  } else if (peak > max) {
    state = CAPTURE_CAPACITY_STATE.STRETCH;
    physicalFitScore = 58;
    rationale = `Peak (~${peak}) stretches above ideal band (${min}–${max}) — possible with displacement management.`;
  } else if (peak >= min && peak <= max) {
    state = CAPTURE_CAPACITY_STATE.IDEAL_CAPTURE_RANGE;
    physicalFitScore = 92;
    rationale = `Peak (~${peak}) sits inside hotel capture band (${min}–${max}) for a ${base.totalGuestrooms}-key property.`;
  } else if (peak >= min * 0.5 && peak < min) {
    state = CAPTURE_CAPACITY_STATE.PLAUSIBLE_CAPTURE;
    physicalFitScore = 78;
    rationale = `Peak (~${peak}) is below core band floor (${min}) but still commercially plausible.`;
  } else {
    state = CAPTURE_CAPACITY_STATE.PLAUSIBLE_CAPTURE;
    physicalFitScore = 70;
    rationale = `Peak (~${peak}) vs band (${min}–${max}) is plausible with monitoring.`;
  }

  return {
    ...base,
    peakRoomDemand: peak,
    estimatedCaptureRooms: estimatedCaptureRooms ?? (peak <= max ? peak : null),
    captureRatio: Math.round(ratio * 100) / 100,
    captureCapacityState: state,
    captureCapacityStateLabel: CAPTURE_CAPACITY_STATE_LABEL[state],
    physicalFitScore,
    rationale,
    housingAccessAssumed: accessible,
  };
}

/**
 * Build hotelContext from GDI hotel profile / demand config.
 */
export function hotelContextFromProfile(profile = {}, demandConfig = null) {
  return {
    totalGuestrooms:
      profile.guestrooms?.totalGuestrooms ||
      profile.capabilityProfile?.totalGuestrooms ||
      demandConfig?.capabilityProfile?.totalGuestrooms ||
      null,
    commercialPriorities:
      profile.commercialPriorityConfiguration || demandConfig?.commercialPriorities || null,
    capabilityProfile: profile.capabilityProfile || demandConfig?.capabilityProfile || null,
    guestrooms: profile.guestrooms || null,
  };
}
