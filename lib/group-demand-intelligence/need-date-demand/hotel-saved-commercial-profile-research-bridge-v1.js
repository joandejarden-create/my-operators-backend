/**
 * Bridge: hotel-confirmed Tab 2 commercial profile → research soft signals.
 *
 * Saved profiles are soft preferences only. They must never invent demand,
 * elevate Sales Ready, force Account Development, or rewrite verified research.
 *
 * Config commercialPriorities remain fallback suggestions when no hotel-
 * confirmed profile exists.
 */

import { loadCommercialProfile } from "./hotel-commercial-profile-store-v1.js";

export const HOTEL_SAVED_COMMERCIAL_PROFILE_RESEARCH_BRIDGE_V1 =
  "gdi-hotel-saved-commercial-profile-research-bridge-v1";

function text(v) {
  return String(v == null ? "" : v).trim();
}

function list(v) {
  return (Array.isArray(v) ? v : [])
    .map((x) => {
      if (x == null) return "";
      if (typeof x === "string" || typeof x === "number") return text(x);
      return text(x.id || x.label || x.name || x.segmentId || "");
    })
    .filter(Boolean);
}

/**
 * Map a saved commercial profile into the commercialPriorities shape used by
 * hotel-segment-research-plan / research path selection (soft signals only).
 */
export function mapSavedProfileToCommercialPriorities(profile = null) {
  if (!profile || typeof profile !== "object") return null;
  const revenue = list(profile.revenuePriorities);
  const segments = list(
    profile.prioritySegments ||
      profile.targetSegments ||
      profile.priorityDemandSegments
  );
  const excluded = list(
    profile.excludedSegments || profile.lowerPrioritySegments
  );
  const day = text(profile.dayPreference || "").toLowerCase();
  const weekendFocus =
    profile.weekendFocus === true ||
    day === "weekends" ||
    day === "weekend" ||
    (Array.isArray(profile.specificDays) &&
      profile.specificDays.some((d) =>
        /sat|sun/i.test(String(d || ""))
      ) &&
      !profile.specificDays.some((d) =>
        /mon|tue|wed|thu|fri/i.test(String(d || ""))
      ));
  const weekdayFocus =
    day === "weekdays" ||
    day === "weekday" ||
    (Array.isArray(profile.specificDays) &&
      profile.specificDays.some((d) =>
        /mon|tue|wed|thu|fri/i.test(String(d || ""))
      ));

  return {
    source: "HOTEL_CONFIRMED_TAB2_PROFILE",
    provenance: profile.provenance || "HOTEL_CONFIRMED",
    softSignalOnly: true,
    targetSegments: segments,
    excludedSegments: excluded,
    demandAnchorFocus: list(profile.demandGeneratorCategories),
    notes: text(profile.restrictionNotes || profile.notes) || null,
    weekendFocus: weekendFocus === true,
    weekdayFocus: weekdayFocus === true,
    dayPreference: text(profile.dayPreference) || null,
    peakRoomsPreset: text(profile.peakRoomsPreset) || null,
    bookingWindow: text(profile.bookingWindow) || null,
    meetingSpaceNeed: text(profile.meetingSpaceNeed) || null,
    stayDuration: text(profile.stayDuration) || null,
    revenuePriorities: revenue,
    knownAccounts: list(
      (profile.knownAccounts || []).map((a) =>
        typeof a === "string" ? a : a?.name || a?.label
      )
    ),
    businessToAvoid: list(profile.businessToAvoid),
    hotelConsiderations: list(profile.hotelConsiderations),
  };
}

/**
 * Load hotel-confirmed profile (if any) and merge into hotelConfig for research.
 * Does not write Airtable. Does not invent a profile when none exists.
 */
export async function resolveHotelConfigWithSavedCommercialProfile(
  hotelId,
  hotelConfig = null,
  opts = {}
) {
  const cfg =
    hotelConfig && typeof hotelConfig === "object" ? { ...hotelConfig } : {};
  const id = text(hotelId);
  if (!id) {
    return {
      hotelConfig: cfg,
      savedProfileApplied: false,
      commercialProfile: null,
    };
  }

  let profile = opts.commercialProfile || null;
  if (!profile && opts.skipLoad !== true) {
    try {
      profile = await loadCommercialProfile(id, {
        useAirtable: opts.useAirtable,
        env: opts.env,
        allowLocalFallback: opts.allowLocalFallback !== false,
      });
    } catch {
      profile = null;
    }
  }

  const hasConfirmedContent =
    profile &&
    (Boolean(profile.profileId) ||
      list(profile.revenuePriorities).length > 0 ||
      list(profile.prioritySegments).length > 0 ||
      list(profile.demandGeneratorCategories).length > 0 ||
      Boolean(text(profile.dayPreference)) ||
      Boolean(text(profile.lastConfirmedAt)));

  const mapped = hasConfirmedContent
    ? mapSavedProfileToCommercialPriorities(profile)
    : null;
  if (!mapped) {
    return {
      hotelConfig: cfg,
      savedProfileApplied: false,
      commercialProfile: profile,
    };
  }

  const configCp =
    cfg.commercialPriorities && typeof cfg.commercialPriorities === "object"
      ? cfg.commercialPriorities
      : {};

  // Hotel-confirmed fields win for soft planning; config fills gaps only.
  cfg.commercialPriorities = {
    ...configCp,
    ...mapped,
    // Keep any config-only research toggles unless explicitly set on profile.
    scalingCompaniesResearch:
      mapped.scalingCompaniesResearch ?? configCp.scalingCompaniesResearch,
    _bridge: HOTEL_SAVED_COMMERCIAL_PROFILE_RESEARCH_BRIDGE_V1,
  };

  return {
    hotelConfig: cfg,
    savedProfileApplied: true,
    commercialProfile: profile,
    mappedCommercialPriorities: mapped,
  };
}
