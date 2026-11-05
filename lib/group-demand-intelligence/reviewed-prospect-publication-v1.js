/**
 * Scoped reviewed-prospect publication path (Bethesda + Mayflower first).
 *
 * Surfaces explicitly reviewed, contact-ready sales prospects on the hotel GDI
 * page without lowering Ready/ACTIONABLE or inventing housing-control evidence.
 *
 * Pattern mirrors W Rome pilot visibility: checked before surface-active traps
 * that reject association organizers lacking housing-control depth.
 */

import {
  evaluateProspectPublicationEligibility,
  PROSPECT_ELIGIBILITY,
} from "./prospect-publication-eligibility-v1.js";
import { isGroupDemandIntelligenceLocalDev } from "./feature-flag.js";

export const REVIEWED_PROSPECT_PUBLICATION_BASIS = "REVIEWED_PACKAGE_V1";
export const REVIEWED_PROSPECT_PUBLICATION_ENV = "GDI_REVIEWED_PROSPECT_PUBLICATION_V1";
export const REVIEWED_PROSPECT_PACKAGE_VERSION = "hotel-review-contact-packages-2026-10-14";

/**
 * Publication allowlist freeze: founder-approved commissioned ADP portfolio
 * (GDI_33_HOTEL_PUBLICATION_ALLOWLIST — 2026-10-27).
 *
 * Source: reports/gdi-universal-adp-hotel-onboarding-v1/
 *         FOUNDER_PUBLICATION_ALLOWLIST_CHANGE_MANIFEST.md
 *
 * New ADP hotels after this freeze enter PROFILE onboarding via the universal
 * roster automatically, but must NOT gain PUBLICATION_AUTHORIZED without a
 * separate founder GO (do not auto-append here from ADP membership alone).
 */
export const REVIEWED_PROSPECT_PUBLICATION_ALLOWLIST_RELEASE =
  "gdi-adp-33-hotel-publication-allowlist-release-v1";

export const REVIEWED_PROSPECT_HOTEL_IDS = Object.freeze([
  // --- previously authorized (preserved) ---
  "recLuxvwwxID7U2B8", // Bethesda Marriott
  "rec0eTEnCqlsGJJt9", // The Mayflower Hotel, Autograph Collection
  "recnPB3Q02NO8Ma1o", // Wyndham San Diego Bayside
  "recG66DQJKP2c0UNh", // Renaissance New York Times Square Hotel
  "recC2kbCaS48jFjFF", // Nobu Hotel London Portman Square
  "recdmo0CnhIZAwHGC", // The Westin Tampa Waterside
  "recgMYovrrZDJMqzX", // Waterstone Resort & Marina
  "recIEcSWuhSCoNLFN", // Fiesta Americana Condesa Cancún All Inclusive
  "recterKmNPdntCm0v", // Grand Fiesta Americana Chapultepec
  "recSM9ARs8U9UZ3AZ", // Tampa Airport Marriott
  // --- founder GO 2026-10-27: remaining commissioned ADP hotels ---
  "rec2PVBDavppGpenm", // AC Hotel A Coruña
  "recIwaP1etgx2g9nA", // Cambridge Beaches Resort & Spa
  "recjDsNzu93CFfe87", // Casas del XVI
  "rec82D9zpB8fede1I", // Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca
  "recbRIFhro6tLmSrO", // Charlotte Marriott City Center
  "recRkGGNKslJptFIL", // Falls Church Marriott Fairview Park
  "rec9Tp0WBb2uk6w3u", // Faranda Collection Bogotá
  "rec35fExUxCClpOP6", // Hilton New York Times Square
  "recCEpdskZeUBvQwG", // Hotel Caribe by Faranda Grand
  "rec8hHupaSwiWI3r7", // Hotel Phillips Kansas City, Curio Collection by Hilton
  "recsn3BUKJ9PNfeZW", // JW Marriott Hotel Monterrey Valle
  "recESHsNsWUFYZrxR", // JW Marriott Hotel Santo Domingo
  "recGkME49yYuxQl0u", // NOW NOW NOHO
  "recUOyzOXn2Zdp98I", // Radisson Hotel Santo Domingo
  "recyZciNlXq5KulnD", // Renaissance Arlington Capital View Hotel
  "recjhQdAUiSyxfCqE", // Sheraton Mallorca Arabella Golf Hotel
  "recKRJjcPnb4tVDDS", // Spice Island Beach Resort
  "recN76iEE6yAaPh8H", // The St. Regis Cap Cana Resort
  "recRXmrakhSAuctwz", // The St. Regis Mexico City
  "recFaxTEFF9ILHWC9", // The Westin Grand München
  "recD17Kxn6BcJjGFh", // The Westin Monterrey Valle
  "rece0or38cxo3Fymb", // W Rome
  "recrPQcZg7SFARRb2", // YOTEL Geneva Lake
]);

function truthy(raw) {
  const v = String(raw || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function explicitlyFalse(raw) {
  if (raw == null || String(raw).trim() === "") return false;
  const v = String(raw).trim().toLowerCase();
  return v === "0" || v === "false" || v === "no" || v === "off";
}

export function isReviewedProspectPublicationEnabled(env = process.env) {
  if (explicitlyFalse(env[REVIEWED_PROSPECT_PUBLICATION_ENV])) return false;
  if (truthy(env[REVIEWED_PROSPECT_PUBLICATION_ENV])) return true;
  // Local / non-Railway: on by default so localhost proves the path
  return isGroupDemandIntelligenceLocalDev(env);
}

export function parseReviewedProspectHotelAllowlist(env = process.env) {
  return String(env.GDI_REVIEWED_PROSPECT_HOTEL_IDS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

export function isReviewedProspectHotelEnabled(hotelId = "", env = process.env) {
  if (!isReviewedProspectPublicationEnabled(env)) return false;
  const id = String(hotelId || "").trim();
  const allow = parseReviewedProspectHotelAllowlist(env);
  const set = allow.length ? allow : REVIEWED_PROSPECT_HOTEL_IDS;
  return set.includes(id);
}

/**
 * Fail-closed preflight for customer-share list visibility.
 *
 * Airtable upsert can succeed while production share list omits REVIEWED_PACKAGE
 * rows when Railway `GDI_REVIEWED_PROSPECT_HOTEL_IDS` excludes the hotel.
 * Built-in local allowlist alone is not proof of production list eligibility.
 *
 * When `requireExplicitEnvAllowlist` is true (Full E2E + live share probe),
 * the hotel MUST appear in the explicit env allowlist string — never rely solely
 * on the built-in fallback for production-facing publication claims.
 */
export function evaluateReviewedProspectProductionListPreflight(
  hotelId = "",
  {
    env = process.env,
    requireExplicitEnvAllowlist = false,
    targetEnvironmentLabel = "production",
  } = {},
) {
  const id = String(hotelId || "").trim();
  const publicationEnabled = isReviewedProspectPublicationEnabled(env);
  const explicit = parseReviewedProspectHotelAllowlist(env);
  const usingBuiltInFallback = explicit.length === 0;
  const inExplicitEnv = explicit.includes(id);
  const enabled = isReviewedProspectHotelEnabled(id, env);

  const requiredAction = !publicationEnabled
    ? `Set GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1 on ${targetEnvironmentLabel}, then add ${id} to GDI_REVIEWED_PROSPECT_HOTEL_IDS (preserve existing IDs).`
    : usingBuiltInFallback && requireExplicitEnvAllowlist
      ? `Set GDI_REVIEWED_PROSPECT_HOTEL_IDS on ${targetEnvironmentLabel} to an explicit CSV that includes ${id} (do not rely on built-in fallback for production share list).`
      : !inExplicitEnv && requireExplicitEnvAllowlist
        ? `Append ${id} to ${targetEnvironmentLabel} GDI_REVIEWED_PROSPECT_HOTEL_IDS while preserving every existing hotel ID.`
        : !enabled
          ? `Enable hotel ${id} for reviewed-prospect publication (GDI_REVIEWED_PROSPECT_PUBLICATION_V1 + allowlist).`
          : null;

  const ok =
    enabled === true &&
    (!requireExplicitEnvAllowlist || (explicit.length > 0 && inExplicitEnv));

  return {
    ok,
    hotelId: id,
    publicationEnabled,
    enabled,
    usingBuiltInFallback,
    inExplicitEnvAllowlist: inExplicitEnv,
    explicitAllowlistCount: explicit.length,
    requireExplicitEnvAllowlist: requireExplicitEnvAllowlist === true,
    blockerCode: ok
      ? null
      : !publicationEnabled
        ? "REVIEWED_PROSPECT_PUBLICATION_DISABLED"
        : usingBuiltInFallback && requireExplicitEnvAllowlist
          ? "REVIEWED_PROSPECT_EXPLICIT_ALLOWLIST_REQUIRED_FOR_PRODUCTION_SHARE"
          : !inExplicitEnv && requireExplicitEnvAllowlist
            ? "HOTEL_ABSENT_FROM_REVIEWED_PROSPECT_ALLOWLIST_ENV"
            : "HOTEL_NOT_ON_REVIEWED_PROSPECT_ALLOWLIST",
    requiredAction,
    note:
      "Production share list uses Railway env allowlist. Local built-in REVIEWED_PROSPECT_HOTEL_IDS does not update Railway.",
  };
}

function isSuppressed(opp = {}) {
  if (opp.customerVisible === false) return true;
  if (opp.prospectSuppressed === true) return true;
  if (opp.archived === true || opp.isArchived === true) return true;
  const pri = String(opp.priority || "").toUpperCase();
  if (pri === "DISQUALIFIED") return true;
  const disp = String(opp.customerSurfaceDisposition || "").toUpperCase();
  if (/ARCHIVE|REJECT|DISMISS|SUPPRESS|REMOVE/.test(disp)) return true;
  if (opp.hotelValidationDismissed === true) return true;
  return false;
}

/**
 * Customer-list visibility for reviewed package prospects.
 * Does NOT mark Ready/ACTIONABLE. Does NOT invent housing control.
 */
export function isReviewedProspectCustomerVisible(opp = {}, opts = {}) {
  const env = opts.env || process.env;
  const hotelId = String(opp.hotelId || opts.hotelId || "").trim();
  if (!isReviewedProspectHotelEnabled(hotelId, env)) return false;
  if (opp.isTestData === true) return false;
  if (isSuppressed(opp)) return false;
  if (String(opp.prospectPublicationBasis || "") !== REVIEWED_PROSPECT_PUBLICATION_BASIS) {
    return false;
  }
  if (opp.prospectHotelFacing !== true) return false;

  const elig = evaluateProspectPublicationEligibility(opp, opts);
  if (!elig.hotelFacing) return false;

  // Minimal date activation: reject clearly past occasions when a start date exists
  const start = String(opp.eventStartDate || "").slice(0, 10);
  const now = opts.nowDate || new Date().toISOString().slice(0, 10);
  if (start && /^\d{4}-\d{2}-\d{2}$/.test(start) && start < now) {
    const future = String(opp.futureCycleEvidenceState || "").toUpperCase();
    if (!/FUTURE_CYCLE|CURRENT_FUTURE/.test(future)) return false;
  }

  return true;
}

export function buildReviewedProspectDiagnostics(opp = {}, opts = {}) {
  const elig = evaluateProspectPublicationEligibility(opp, opts);
  const visible = isReviewedProspectCustomerVisible(opp, opts);
  return {
    visible,
    publicationBasis: opp.prospectPublicationBasis || null,
    eligibility: elig.eligibility,
    hotelFacing: elig.hotelFacing === true,
    enrichmentQueue: elig.enrichmentQueue === true,
    contactCompletenessGroup: elig.contactCompletenessGroup,
    routeReason: elig.route?.reason || null,
    readyContactClass: elig.readyContactClass,
    readyContactEligible: elig.readyContactEligible,
    reasons: elig.reasons || [],
    nextTask: elig.nextTask || null,
  };
}

export function customerFacingLabelForProspect(opp = {}) {
  const section = String(opp.prospectReviewSection || "").toUpperCase();
  if (section === "EXPLORATORY") return "Potential opportunity";
  if (section === "PRIORITY") return "Core prospect";
  return "Prospect to qualify";
}

export function contactResponsibilityLabel(raw = "") {
  const v = String(raw || "").toUpperCase();
  if (v === "CONFIRMED_BOOKING_RESPONSIBILITY") return "Confirmed booking contact";
  if (v === "POTENTIAL_RELEVANT_CONTACT") return "Potential contact";
  if (v === "REFERRAL_OR_ORG_ENTRY_POINT") return "Organizational contact";
  return "Contact route";
}

export function phoneTypeCustomerLabel(raw = "") {
  const v = String(raw || "").toLowerCase();
  if (v === "direct") return "Direct line";
  if (v === "department") return "Department line";
  if (v === "agency") return "Agency line";
  if (v === "organization_helpline" || v === "helpline") return "Organization helpline";
  if (v === "company_switchboard" || v === "switchboard") return "Company switchboard";
  return raw ? String(raw) : null;
}
