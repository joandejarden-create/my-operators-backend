/**
 * Commercial disposition for Event-to-Demand buyer prospects.
 * Classification only — does not grant Ready or customer publication.
 *
 * Distinguishes hotel sales prospects, strategic partnerships,
 * market-intelligence signals, and legitimate rejections.
 */

import {
  ACTIONABLE_EVENT_MECHANISMS,
  EVENT_DEMAND_MECHANISM,
} from "./event-to-demand-constants-v1.js";
import { EVENT_DEMAND_CLASS } from "./event-demand-class-v1.js";
import { DEMAND_OWNER_ROLE } from "./event-demand-ownership-v1.js";

export const COMMERCIAL_DISPOSITION = Object.freeze({
  HOTEL_SALES_PROSPECT: "HOTEL_SALES_PROSPECT",
  STRATEGIC_PARTNERSHIP: "STRATEGIC_PARTNERSHIP",
  MARKET_INTELLIGENCE: "MARKET_INTELLIGENCE",
  LEGITIMATELY_REJECTED: "LEGITIMATELY_REJECTED",
});

export const DEMAND_EVIDENCE_LABEL = Object.freeze({
  CONFIRMED_BUYING: "CONFIRMED_BUYING",
  INFERRED_DEMAND: "INFERRED_DEMAND",
  PARTNERSHIP_WITHOUT_LODGING: "PARTNERSHIP_WITHOUT_LODGING",
  SIGNAL_ONLY: "SIGNAL_ONLY",
  UNSUPPORTED: "UNSUPPORTED",
});

/** Forbidden customer-facing phrases unless demandEvidenceLabel is CONFIRMED_BUYING. */
const CONFIRMED_DEMAND_COPY_RE =
  /\b(?:confirmed demand|open room block|actively sourcing|confirmed buying|confirmed lodging buyer)\b/i;

/**
 * Card / drawer / action copy must not claim confirmed buying for inferred prospects.
 */
export function assertInferredDemandCopyIntegrity({
  textBlob = "",
  demandEvidenceLabel = "",
  confirmedBuying = false,
} = {}) {
  const label = String(demandEvidenceLabel || "").toUpperCase();
  if (label === DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING || confirmedBuying === true) {
    return { ok: true, violations: [] };
  }
  const blob = String(textBlob || "");
  const violations = [];
  if (CONFIRMED_DEMAND_COPY_RE.test(blob)) {
    violations.push("confirmed_demand_language_without_confirmed_label");
  }
  return { ok: violations.length === 0, violations };
}

/**
 * Sanitize presentation blobs for inferred / partnership / signal labels.
 */
export function sanitizeInferredDemandCopy(textBlob = "", demandEvidenceLabel = "") {
  const label = String(demandEvidenceLabel || "").toUpperCase();
  if (label === DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING) return String(textBlob || "");
  return String(textBlob || "")
    .replace(/\bconfirmed demand\b/gi, "inferred demand opportunity")
    .replace(/\bopen room block\b/gi, "no verified room block")
    .replace(/\bactively sourcing\b/gi, "hotel sourcing unconfirmed")
    .replace(/\bconfirmed buying\b/gi, "inferred buying interest")
    .replace(/\bconfirmed lodging buyer\b/gi, "inferred lodging prospect");
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function mechanismOf(row = {}) {
  return String(
    row.mechanism ||
      row.eventDemandMechanism ||
      row.eventHotelFitClass ||
      row.proposedHotelUse ||
      ""
  ).toUpperCase();
}

function demandClassOf(row = {}) {
  return String(
    row.eventDemandClass ||
      row.demandClass ||
      row.commercialDispositionDemandClass ||
      ""
  ).toUpperCase();
}

function isPlaceManagementOrEstate(org = "", role = "") {
  const o = text(org);
  const r = String(role || "").toUpperCase();
  if (/howard de walden|marylebone village|estate|bid\b|business improvement/i.test(o)) {
    return true;
  }
  if (r === DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY) {
    if (/howard de walden|estate|village|district|place.?brand/i.test(o)) {
      return true;
    }
  }
  return false;
}

function hasFutureOrRecurringActivity(row = {}) {
  if (row.futureDemand?.supported === true) return true;
  if (/\b20\d{2}\b/.test(text(row.futureDemand?.dateWindow || row.eventStartDate || row.timing))) {
    return true;
  }
  if (row.recurring === true || /annual|recurring|open programmes?/i.test(text(row.title || row.eventName))) {
    return true;
  }
  return false;
}

function hasHotelFit(row = {}) {
  return Boolean(
    text(row.hotelFitRationale || row.fitExplanation || row.whyThisHotel) ||
      (row.hotelFitScore != null && Number.isFinite(Number(row.hotelFitScore)))
  );
}

/**
 * Classify commercial disposition from a buyer / demand-owner row + prospect bag.
 */
export function classifyCommercialDisposition({
  opportunity = {},
  demandOwner = null,
  buyer = null,
} = {}) {
  const row = {
    ...opportunity,
    ...(demandOwner || {}),
    ...(buyer || {}),
    organizationName:
      demandOwner?.organizationName ||
      buyer?.organizationName ||
      opportunity.organizationName,
    role: demandOwner?.role || buyer?.role,
    mechanism: mechanismOf({ ...opportunity, ...demandOwner, ...buyer }),
  };

  const org = text(row.organizationName);
  const mech = mechanismOf(row);
  const cls = demandClassOf(row) || EVENT_DEMAND_CLASS.INFERRED;
  const role = String(row.role || "").toUpperCase();
  const reasons = [];

  if (!org) {
    return {
      disposition: COMMERCIAL_DISPOSITION.LEGITIMATELY_REJECTED,
      demandEvidenceLabel: DEMAND_EVIDENCE_LABEL.UNSUPPORTED,
      reasons: ["missing_organization"],
      publicationImplication: "Does not grant Ready or hotel-facing eligibility",
    };
  }

  // Place-management / estate → partnership, not accommodation buyer
  if (isPlaceManagementOrEstate(org, role)) {
    reasons.push("place_management_or_estate_intermediary");
    return {
      disposition: COMMERCIAL_DISPOSITION.STRATEGIC_PARTNERSHIP,
      demandEvidenceLabel: DEMAND_EVIDENCE_LABEL.PARTNERSHIP_WITHOUT_LODGING,
      reasons,
      partnershipMechanisms: [
        "district_private_events",
        "referral_relationship",
        "local_hospitality_introductions",
      ],
      note:
        "Do not represent as confirmed lodging demand; estate/place brands are partnership routes unless separate lodging-buyer evidence exists.",
      publicationImplication: "Does not grant Ready or hotel-facing eligibility",
    };
  }

  if (mech === EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL || cls === EVENT_DEMAND_CLASS.SPECULATIVE) {
    return {
      disposition: COMMERCIAL_DISPOSITION.MARKET_INTELLIGENCE,
      demandEvidenceLabel: DEMAND_EVIDENCE_LABEL.SIGNAL_ONLY,
      reasons: ["speculative_or_signal_mechanism"],
      publicationImplication: "Does not grant Ready or hotel-facing eligibility",
    };
  }

  const actionable = ACTIONABLE_EVENT_MECHANISMS.includes(mech);
  const futureOk = hasFutureOrRecurringActivity(row);
  const fitOk = hasHotelFit(row);
  const researchBlob = text(
    `${row.localConnection || ""} ${row.hotelFitRationale || ""} ${row.evidenceBasis || ""}`
  );
  const territory = String(
    row.eventTerritoryClass || row.hotelFit?.territory || ""
  ).toUpperCase();
  const stretchSpilloverOnly =
    (territory === "TERRITORY_STRETCH" || /stretch vs/i.test(researchBlob)) &&
    /spillover|proportionate .+ mechanism/i.test(researchBlob) &&
    !/travelling participants|housing page|official.?housing|delegate.?hotel/i.test(
      researchBlob.replace(
        /\b(?:rather than|not|no|without|exclude|excluding|not mass)[^.!?]{0,100}\b(?:room\s*blocks?|lodging|accommodation|housing)\b/gi,
        " "
      )
    );

  if (stretchSpilloverOnly) {
    return {
      disposition: COMMERCIAL_DISPOSITION.MARKET_INTELLIGENCE,
      demandEvidenceLabel: DEMAND_EVIDENCE_LABEL.SIGNAL_ONLY,
      reasons: ["stretch_venue_spillover_without_org_lodging_evidence"],
      publicationImplication: "Does not grant Ready or hotel-facing eligibility",
    };
  }

  const exhibitorWithoutLodgingClaim =
    (role === DEMAND_OWNER_ROLE.EXHIBITOR ||
      mech === EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING) &&
    !/overnight|lodging|housing|room.?block|travel|delegate/i.test(researchBlob);

  if (exhibitorWithoutLodgingClaim && !futureOk) {
    return {
      disposition: COMMERCIAL_DISPOSITION.MARKET_INTELLIGENCE,
      demandEvidenceLabel: DEMAND_EVIDENCE_LABEL.SIGNAL_ONLY,
      reasons: ["exhibitor_participation_without_supported_lodging_path"],
      publicationImplication: "Does not grant Ready or hotel-facing eligibility",
    };
  }

  if (actionable && futureOk && fitOk) {
    const confirmed = cls === EVENT_DEMAND_CLASS.CONFIRMED;
    return {
      disposition: COMMERCIAL_DISPOSITION.HOTEL_SALES_PROSPECT,
      demandEvidenceLabel: confirmed
        ? DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING
        : DEMAND_EVIDENCE_LABEL.INFERRED_DEMAND,
      reasons: [
        confirmed ? "confirmed_demand_class" : "inferred_demand_with_future_activity",
        "actionable_mechanism",
        "hotel_fit_present",
      ],
      note: confirmed
        ? "Confirmed demand language requires source-backed lodging evidence."
        : "Inferred demand — not confirmed open buying or room block.",
      publicationImplication: "Does not grant Ready or hotel-facing eligibility",
    };
  }

  if (actionable && (!futureOk || !fitOk)) {
    reasons.push(!futureOk ? "missing_future_occasion" : "missing_hotel_fit");
    return {
      disposition: COMMERCIAL_DISPOSITION.MARKET_INTELLIGENCE,
      demandEvidenceLabel: DEMAND_EVIDENCE_LABEL.SIGNAL_ONLY,
      reasons,
      publicationImplication: "Does not grant Ready or hotel-facing eligibility",
    };
  }

  if (role === DEMAND_OWNER_ROLE.EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY) {
    return {
      disposition: COMMERCIAL_DISPOSITION.STRATEGIC_PARTNERSHIP,
      demandEvidenceLabel: DEMAND_EVIDENCE_LABEL.PARTNERSHIP_WITHOUT_LODGING,
      reasons: ["intermediary_agency_route"],
      publicationImplication: "Does not grant Ready or hotel-facing eligibility",
    };
  }

  return {
    disposition: COMMERCIAL_DISPOSITION.LEGITIMATELY_REJECTED,
    demandEvidenceLabel: DEMAND_EVIDENCE_LABEL.UNSUPPORTED,
    reasons: ["no_credible_commercial_path"],
    publicationImplication: "Does not grant Ready or hotel-facing eligibility",
  };
}

/**
 * Diagnose why a usable sales route did not become hotel-facing eligible.
 */
export function diagnoseConversionBlocker({
  opportunity = {},
  eligibility = {},
  disposition = null,
  contactEvidence = null,
} = {}) {
  const failed = [].concat(eligibility.reasons || eligibility.suitability?.failed || []);
  const routeOk = eligibility.usableContactRoute !== false;
  const facing = eligibility.hotelFacing === true || eligibility.eligibility === "HOTEL_FACING";

  if (facing) {
    return {
      primaryBlocker: "none_eligibility_passed",
      category: "publication_path",
      detail: "HOTEL_FACING — remaining gap is Ready / reviewed package / canonical write / visibility",
      researchCanResolve: true,
    };
  }

  if (
    disposition?.disposition === COMMERCIAL_DISPOSITION.STRATEGIC_PARTNERSHIP ||
    failed.includes("partnership_not_lodging_sales")
  ) {
    return {
      primaryBlocker: "partnership_not_lodging_sales",
      category: "commercial_path",
      detail:
        "Strategic partnership / place brand — use Private Events or referral pathway; not accommodation HOTEL_FACING",
      researchCanResolve: true,
    };
  }

  if (failed.includes("no_usable_route") || failed.includes("homepage_or_vague_only")) {
    return {
      primaryBlocker: "missing_usable_contact_route",
      category: "missing_usable_contact_route",
      detail: failed.join(","),
      researchCanResolve: true,
    };
  }
  if (failed.includes("travel_need_evidence")) {
    return {
      primaryBlocker: "missing_travel_need_evidence",
      category: "incorrect_eligibility_mapping",
      detail:
        "Dated-lodging suitability requires travel/overnight/lodging language on research fields",
      researchCanResolve: true,
    };
  }
  if (
    disposition?.disposition === COMMERCIAL_DISPOSITION.STRATEGIC_PARTNERSHIP
  ) {
    return {
      primaryBlocker: "partnership_not_lodging_buyer",
      category: "incorrect_demand_mechanism_classification",
      detail: "Strategic partnership — do not force accommodation-buyer eligibility",
      researchCanResolve: false,
    };
  }
  if (disposition?.disposition === COMMERCIAL_DISPOSITION.MARKET_INTELLIGENCE) {
    return {
      primaryBlocker: "market_intelligence_only",
      category: "missing_commercial_relevance",
      detail: (disposition.reasons || []).join(","),
      researchCanResolve: Boolean(disposition.reasons?.includes("missing_future_occasion")),
    };
  }
  if (contactEvidence?.tier === "NO_USABLE_ROUTE") {
    return {
      primaryBlocker: "missing_usable_contact_route",
      category: "missing_usable_contact_route",
      detail: contactEvidence.note || "",
      researchCanResolve: true,
    };
  }
  if (!routeOk && failed.length === 0) {
    return {
      primaryBlocker: "missing_usable_contact_route",
      category: "missing_usable_contact_route",
      detail: "route_not_ok",
      researchCanResolve: true,
    };
  }
  if (failed.length) {
    return {
      primaryBlocker: failed[0],
      category: "missing_evidence",
      detail: failed.join(","),
      researchCanResolve: true,
    };
  }
  return {
    primaryBlocker: "unclassified",
    category: "missing_evidence",
    detail: "",
    researchCanResolve: true,
  };
}
