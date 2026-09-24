/**
 * Pilot / reference-hotel path quarantine for GDI.
 *
 * Bethesda pilot seeds, DMV deepen, and pilot-only contact packs must not run
 * for arbitrary hotels unless explicitly enabled.
 */

import { PILOT_HOTEL_ID, loadHotelDemandConfig } from "./hotel-profile.js";

export function isPilotReferenceHotel(hotelId) {
  return String(hotelId || "").trim() === PILOT_HOTEL_ID;
}

/**
 * Whether Bethesda legacy seed / DMV expansion may run.
 * Default: only for pilot hotel when config does not forbid it.
 */
export function isPilotReferenceLogicEnabled(hotelId, opts = {}, env = process.env) {
  if (opts.forceEnablePilotReferenceLogic === true) return true;
  if (opts.forceDisablePilotReferenceLogic === true) return false;

  const globalOff =
    String(env.GDI_ENABLE_PILOT_REFERENCE_LOGIC || "")
      .trim()
      .toLowerCase() === "0" ||
    String(env.GDI_ENABLE_PILOT_REFERENCE_LOGIC || "")
      .trim()
      .toLowerCase() === "false";
  if (globalOff) return false;

  const globalOn =
    String(env.GDI_ENABLE_PILOT_REFERENCE_LOGIC || "").trim() === "1" ||
    String(env.GDI_ENABLE_PILOT_REFERENCE_LOGIC || "")
      .trim()
      .toLowerCase() === "true";

  const cfg = loadHotelDemandConfig(hotelId) || {};
  const onboarding = cfg.gdiOnboarding || {};

  if (onboarding.bethesdaSeedPathAllowed === false) return false;
  if (onboarding.dmvExpansionAllowed === false && opts.forDmvExpansion) return false;

  if (!isPilotReferenceHotel(hotelId) && !globalOn) return false;

  if (isPilotReferenceHotel(hotelId)) {
    if (onboarding.bethesdaSeedPathAllowed === true) return true;
    // Pilot hotel defaults to reference logic unless explicitly disabled
    return onboarding.bethesdaSeedPathAllowed !== false;
  }

  return false;
}

export function assertNoPilotBleedForHotel(hotelId, context = "gdi") {
  if (isPilotReferenceLogicEnabled(hotelId)) return;
  // Soft assert used by seed/tests — callers decide throw vs skip
  return {
    ok: true,
    pilotLogic: false,
    hotelId,
    context,
  };
}
