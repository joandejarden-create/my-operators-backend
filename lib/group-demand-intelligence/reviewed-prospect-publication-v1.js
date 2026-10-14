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

/** Initial enablement — config/data, not architecture hardcodes forever. */
export const REVIEWED_PROSPECT_HOTEL_IDS = Object.freeze([
  "recLuxvwwxID7U2B8", // Bethesda Marriott
  "rec0eTEnCqlsGJJt9", // The Mayflower Hotel, Autograph Collection
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

export function isReviewedProspectHotelEnabled(hotelId = "", env = process.env) {
  if (!isReviewedProspectPublicationEnabled(env)) return false;
  const id = String(hotelId || "").trim();
  const allow = String(env.GDI_REVIEWED_PROSPECT_HOTEL_IDS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const set = allow.length ? allow : REVIEWED_PROSPECT_HOTEL_IDS;
  return set.includes(id);
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
  if (section === "EXPLORATORY") return "Exploratory account";
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
