/**
 * Professional Event Planner commercial eligibility (zero invention).
 * At least one credible hotel revenue mechanism required — guestrooms OR
 * banquets are not universally mandatory.
 */
import {
  PLANNER_EXCLUDED_VENDOR_CLASSES,
  PLANNER_PARTNERSHIP_POTENTIAL,
  text,
} from "./professional-event-planner-contract-v1.js";
import { PLANNER_GEO_SCOPE } from "./planner-service-area-v1.js";

export const PROFESSIONAL_EVENT_PLANNER_ELIGIBILITY_V1 =
  "gdi-professional-event-planner-eligibility-v1";

function hasNamedPerson(contact) {
  const name = text(contact?.name || contact?.personName || contact?.fullName);
  if (!name || name.length < 4) return false;
  if (/^(info|events|sales|contact|planner|the\s+)/i.test(name)) return false;
  if (!/\s/.test(name) && name.length < 8) return false;
  const tokens = name.split(/\s+/);
  return tokens.length >= 2 && tokens.every((t) => t.length >= 2);
}

function isExcludedVendorClass(planner = {}) {
  const cls = text(planner.vendorClass || planner.companyType || "").toLowerCase();
  const blob = [
    planner.specialization,
    planner.summary,
    planner.title,
    planner.organizationName,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();
  return PLANNER_EXCLUDED_VENDOR_CLASSES.some((v) => {
    const needle = v.replace(/_/g, " ");
    return cls.includes(needle) || blob.includes(needle);
  });
}

export function evaluateProfessionalEventPlannerEligibility({
  planner = {},
  serviceArea = {},
  contacts = [],
  eventActivityVerified = false,
  revenueMechanisms = [],
  hotelInfluenceEvidence = false,
  knownBarrier = null,
  alreadyClassifiedElsewhere = false,
} = {}) {
  const reasons = [];
  const gaps = [];

  const plannerName = text(
    planner.name || planner.companyName || planner.organizationName,
  );
  if (!plannerName) reasons.push("missing_planner_identity");

  if (isExcludedVendorClass(planner) && !hotelInfluenceEvidence) {
    reasons.push("excluded_vendor_class_without_hotel_influence");
  }

  if (!eventActivityVerified) {
    gaps.push("planner_activity_not_verified");
  }

  const mechanisms = (revenueMechanisms || [])
    .map((m) => text(m))
    .filter(Boolean);
  if (!mechanisms.length) {
    gaps.push("no_credible_hotel_revenue_mechanism");
  }

  const geo = serviceArea.geoScope || PLANNER_GEO_SCOPE.UNKNOWN;
  if (
    geo === PLANNER_GEO_SCOPE.OUT_OF_MARKET ||
    geo === PLANNER_GEO_SCOPE.UNKNOWN
  ) {
    gaps.push("service_area_not_verified_for_hotel_destination");
  }
  if (geo === PLANNER_GEO_SCOPE.OFFICE_ONLY_UNVERIFIED_DESTINATION) {
    gaps.push("destination_production_not_verified");
  }

  const named = (contacts || []).find(hasNamedPerson);
  const hasNamedBuyer = Boolean(named);
  if (!hasNamedBuyer) {
    gaps.push("named_commercially_relevant_person_missing");
  }

  if (!hotelInfluenceEvidence) {
    gaps.push("hotel_buying_influence_not_evidenced");
  }

  if (alreadyClassifiedElsewhere) {
    reasons.push("already_represented_under_another_subtype_or_segment");
  }

  if (knownBarrier) {
    reasons.push(`barrier:${text(knownBarrier)}`);
  }

  const hardFail = reasons.some((r) =>
    /missing_planner_identity|excluded_vendor|already_represented|barrier:/i.test(
      r,
    ),
  );

  const salesReady =
    !hardFail &&
    plannerName &&
    eventActivityVerified &&
    mechanisms.length > 0 &&
    hasNamedBuyer &&
    hotelInfluenceEvidence &&
    (geo === PLANNER_GEO_SCOPE.BOTH ||
      geo === PLANNER_GEO_SCOPE.PRODUCES_IN_DESTINATION ||
      geo === PLANNER_GEO_SCOPE.OFFICE_IN_MARKET ||
      (geo === PLANNER_GEO_SCOPE.OFFICE_ONLY_UNVERIFIED_DESTINATION &&
        eventActivityVerified));

  let partnershipPotential = PLANNER_PARTNERSHIP_POTENTIAL.UNKNOWN;
  if (hardFail) {
    partnershipPotential = PLANNER_PARTNERSHIP_POTENTIAL.NOT_QUALIFIED;
  } else if (
    salesReady &&
    eventActivityVerified &&
    (planner.multipleEventEvidence || planner.recurringProgrammeEvidence)
  ) {
    partnershipPotential =
      PLANNER_PARTNERSHIP_POTENTIAL.ESTABLISHED_EVENT_PRODUCER;
  } else if (eventActivityVerified && mechanisms.length > 0) {
    partnershipPotential = PLANNER_PARTNERSHIP_POTENTIAL.RELEVANT_SPECIALIST;
  } else if (plannerName && !hardFail) {
    partnershipPotential =
      PLANNER_PARTNERSHIP_POTENTIAL.EMERGING_RESEARCH_CONTINUATION;
  }

  const researchContinuation =
    !hardFail && plannerName && (!salesReady || gaps.length > 0);

  return {
    version: PROFESSIONAL_EVENT_PLANNER_ELIGIBILITY_V1,
    salesReady,
    researchContinuation,
    disposition: salesReady
      ? "ASSIGN_FOR_DISCOVERY"
      : researchContinuation
        ? "WATCHLIST"
        : "DO_NOT_PURSUE",
    partnershipPotential,
    hasNamedBuyer,
    namedContactName: named?.name || null,
    revenueMechanisms: mechanisms,
    gaps,
    hardFailReasons: reasons.filter((r) =>
      /missing_planner|excluded_vendor|already_represented|barrier:/i.test(r),
    ),
    notes: [
      "Confirmed RFPs and room-night totals are not required.",
      "At least one credible hotel revenue type is required (guestrooms or banquets are not both mandatory).",
      "Do not invent annual wedding counts, contracts, or production volume.",
      "Do not auto-mark partnership as agreed from research alone.",
      "General inbox alone does not meet the named-buyer requirement.",
    ],
  };
}
