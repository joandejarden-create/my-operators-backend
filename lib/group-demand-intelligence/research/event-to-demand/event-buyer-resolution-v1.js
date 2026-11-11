/**
 * Buyer-resolution stage for Event-to-Demand Opportunity Research Supervisor.
 * Aligns contact-evidence tiers with existing BUYER_CONFIDENCE + CONTACT_ROUTE_QUALITY.
 * Does not invent emails, phones, or purchasing authority from titles alone.
 */

import {
  BUYER_CONFIDENCE,
  BUYER_FUNCTION,
  classifyBuyerConfidence,
  buyerFunctionForRole,
} from "./event-demand-ownership-v1.js";
import {
  CONTACT_ROUTE_QUALITY,
  classifyContactRouteQuality,
  isUsableOrganizationSalesRoute,
} from "./event-demand-class-v1.js";
import {
  extractContactSurface,
  hasUsableContactRoute,
} from "../../prospect-publication-eligibility-v1.js";

/** Contact evidence tiers — diagnostic labels mapped onto existing enums. */
export const CONTACT_EVIDENCE_TIER = Object.freeze({
  VERIFIED_NAMED_DECISION_MAKER: "VERIFIED_NAMED_DECISION_MAKER",
  VERIFIED_ROLE_RELEVANT_PERSON: "VERIFIED_ROLE_RELEVANT_PERSON",
  FUNCTIONAL_DEPARTMENT_ROUTE: "FUNCTIONAL_DEPARTMENT_ROUTE",
  GENERIC_ORGANIZATION_ROUTE: "GENERIC_ORGANIZATION_ROUTE",
  INTERMEDIARY_ROUTE: "INTERMEDIARY_ROUTE",
  UNVERIFIED_CONTACT: "UNVERIFIED_CONTACT",
  NO_USABLE_ROUTE: "NO_USABLE_ROUTE",
});

export const BUYER_RESOLUTION_BLOCKER = Object.freeze({
  BUYER_NOT_IDENTIFIED: "BUYER_NOT_IDENTIFIED",
  FUNCTION_KNOWN_PERSON_UNKNOWN: "FUNCTION_KNOWN_PERSON_UNKNOWN",
  PERSON_ROLE_UNVERIFIED: "PERSON_ROLE_UNVERIFIED",
  PERSON_ROUTE_MISSING: "PERSON_ROUTE_MISSING",
  ORG_ROUTE_NAMED_ENRICHMENT_INCOMPLETE: "ORG_ROUTE_NAMED_ENRICHMENT_INCOMPLETE",
  HOTEL_DEMAND_MECHANISM_MISSING: "HOTEL_DEMAND_MECHANISM_MISSING",
  PARTNERSHIP_WITHOUT_LODGING: "PARTNERSHIP_WITHOUT_LODGING",
  EXPLORATORY_PROSPECT: "EXPLORATORY_PROSPECT",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
  PAID_RESEARCH_NOT_JUSTIFIED: "PAID_RESEARCH_NOT_JUSTIFIED",
  NONE: "NONE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isPersonName(v) {
  const n = text(v);
  if (!n || /^not\s*established$/i.test(n) || n === "UNKNOWN") return false;
  if (/^(staff|team|office|desk|info|contact)\b/i.test(n)) return false;
  return n.split(/\s+/).length >= 2 || /^[A-Z][a-z]+ [A-Z]/.test(n);
}

/**
 * Map contact evidence tier → existing BUYER_CONFIDENCE (no conflicting statuses).
 */
export function buyerConfidenceFromContactEvidenceTier(tier) {
  switch (tier) {
    case CONTACT_EVIDENCE_TIER.VERIFIED_NAMED_DECISION_MAKER:
      return BUYER_CONFIDENCE.CONFIRMED_BUYER;
    case CONTACT_EVIDENCE_TIER.VERIFIED_ROLE_RELEVANT_PERSON:
      return BUYER_CONFIDENCE.LIKELY_BUYER_FUNCTION;
    case CONTACT_EVIDENCE_TIER.FUNCTIONAL_DEPARTMENT_ROUTE:
    case CONTACT_EVIDENCE_TIER.GENERIC_ORGANIZATION_ROUTE:
    case CONTACT_EVIDENCE_TIER.INTERMEDIARY_ROUTE:
      return BUYER_CONFIDENCE.GENERIC_ORG_CONTACT;
    default:
      return BUYER_CONFIDENCE.NONE;
  }
}

/**
 * Classify contact evidence from opportunity + optional named-person package.
 * Never upgrades a generic org email to VERIFIED_NAMED_DECISION_MAKER.
 */
export function classifyContactEvidence(opportunity = {}, opts = {}) {
  const surface = extractContactSurface(opportunity);
  const nested =
    opportunity.contact && typeof opportunity.contact === "object"
      ? opportunity.contact
      : {};
  const route = hasUsableContactRoute(opportunity);
  const contactBag = {
    ...nested,
    personName: surface.person || nested.personName,
    role: surface.title || nested.role || nested.title,
    businessEmail: surface.email || nested.businessEmail,
    businessPhone: surface.phone || nested.businessPhone,
    contactFormUrl: surface.contactUrl || nested.contactFormUrl,
  };
  const routeQuality =
    nested.contactRouteQuality ||
    classifyContactRouteQuality(contactBag);
  const named = isPersonName(surface.person || nested.personName);
  const roleText = text(surface.title || nested.role || nested.title);
  const roleVerified = opts.roleRelevanceVerified === true;
  const decisionMakerVerified = opts.decisionMakerVerified === true;

  if (!route.ok) {
    if (named && !roleVerified) {
      return {
        tier: CONTACT_EVIDENCE_TIER.UNVERIFIED_CONTACT,
        buyerConfidence: BUYER_CONFIDENCE.NONE,
        contactRouteQuality: routeQuality,
        usableRoute: false,
        routeReason: route.reason,
        note: "Named person without usable contact route",
        surface,
      };
    }
    return {
      tier: CONTACT_EVIDENCE_TIER.NO_USABLE_ROUTE,
      buyerConfidence: BUYER_CONFIDENCE.NONE,
      contactRouteQuality: routeQuality || CONTACT_ROUTE_QUALITY.NONE,
      usableRoute: false,
      routeReason: route.reason,
      note: "No usable contact route",
      surface,
    };
  }

  if (named && decisionMakerVerified && roleVerified) {
    return {
      tier: CONTACT_EVIDENCE_TIER.VERIFIED_NAMED_DECISION_MAKER,
      buyerConfidence: BUYER_CONFIDENCE.CONFIRMED_BUYER,
      contactRouteQuality: CONTACT_ROUTE_QUALITY.DIRECT_SALES,
      usableRoute: true,
      routeReason: route.reason,
      note: "Verified named decision-maker with usable route",
      surface,
    };
  }

  if (named && roleVerified) {
    return {
      tier: CONTACT_EVIDENCE_TIER.VERIFIED_ROLE_RELEVANT_PERSON,
      buyerConfidence: BUYER_CONFIDENCE.LIKELY_BUYER_FUNCTION,
      contactRouteQuality:
        routeQuality === CONTACT_ROUTE_QUALITY.NONE
          ? CONTACT_ROUTE_QUALITY.DIRECT_SALES
          : routeQuality,
      usableRoute: true,
      routeReason: route.reason,
      note: "Role-relevant person verified; purchasing authority not confirmed",
      surface,
    };
  }

  if (named && !roleVerified) {
    return {
      tier: CONTACT_EVIDENCE_TIER.UNVERIFIED_CONTACT,
      buyerConfidence: classifyBuyerConfidence(contactBag),
      contactRouteQuality: routeQuality,
      usableRoute: true,
      routeReason: route.reason,
      note: "Person identified but role relevance unverified — not a verified buyer",
      surface,
    };
  }

  if (route.reason === "intermediary") {
    return {
      tier: CONTACT_EVIDENCE_TIER.INTERMEDIARY_ROUTE,
      buyerConfidence: BUYER_CONFIDENCE.GENERIC_ORG_CONTACT,
      contactRouteQuality: CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES,
      usableRoute: true,
      routeReason: route.reason,
      note: "Intermediary route — not a verified named buyer",
      surface,
    };
  }

  const deptHint =
    /meetings|events|conferences|housing|groups|executive.?education|hospitality|sponsorship|exhibitor/i.test(
      roleText + " " + text(nested.buyerFunctionHint)
    );
  if (deptHint || route.reason === "functional_contact_url") {
    return {
      tier: CONTACT_EVIDENCE_TIER.FUNCTIONAL_DEPARTMENT_ROUTE,
      buyerConfidence: BUYER_CONFIDENCE.GENERIC_ORG_CONTACT,
      contactRouteQuality:
        routeQuality === CONTACT_ROUTE_QUALITY.DIRECT_SALES ||
        routeQuality === CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES
          ? routeQuality
          : CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES,
      usableRoute: true,
      routeReason: route.reason,
      note: "Functional department / org sales route — not a verified named buyer",
      surface,
    };
  }

  if (surface.email || route.reason === "email" || route.reason === "phone") {
    return {
      tier: CONTACT_EVIDENCE_TIER.GENERIC_ORGANIZATION_ROUTE,
      buyerConfidence: BUYER_CONFIDENCE.GENERIC_ORG_CONTACT,
      contactRouteQuality:
        routeQuality === CONTACT_ROUTE_QUALITY.NONE
          ? CONTACT_ROUTE_QUALITY.DIRECT_SALES
          : routeQuality,
      usableRoute: true,
      routeReason: route.reason,
      note: "Generic organization route — usable outreach, not verified buyer",
      surface,
    };
  }

  return {
    tier: CONTACT_EVIDENCE_TIER.UNVERIFIED_CONTACT,
    buyerConfidence: BUYER_CONFIDENCE.NONE,
    contactRouteQuality: routeQuality,
    usableRoute: route.ok,
    routeReason: route.reason,
    note: "Contact present but unverified",
    surface,
  };
}

/**
 * Determine the next buyer-resolution blocker (continuation driver).
 */
export function classifyBuyerResolutionBlocker({
  opportunity = {},
  contactEvidence = null,
  disposition = null,
  researchExhausted = false,
  paidJustified = false,
} = {}) {
  const evidence = contactEvidence || classifyContactEvidence(opportunity);
  const mech = text(
    opportunity.eventDemandMechanism || opportunity.proposedHotelUse
  );
  if (disposition?.disposition === "STRATEGIC_PARTNERSHIP") {
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.PARTNERSHIP_WITHOUT_LODGING,
      continueResearch: false,
      nextAction: "Route as partnership / private-events relationship — do not force lodging buyer hunt",
    };
  }
  if (!mech) {
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.HOTEL_DEMAND_MECHANISM_MISSING,
      continueResearch: true,
      nextAction: "Establish evidence-backed demand mechanism before buyer hunt",
    };
  }
  if (researchExhausted) {
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.RESEARCH_EXHAUSTED,
      continueResearch: false,
      nextAction: "Stop — preserve evidence; do not re-query equivalent sources",
    };
  }
  if (evidence.tier === CONTACT_EVIDENCE_TIER.VERIFIED_NAMED_DECISION_MAKER) {
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.NONE,
      continueResearch: false,
      nextAction: "Proceed eligibility → Ready → publication path",
    };
  }
  if (evidence.tier === CONTACT_EVIDENCE_TIER.VERIFIED_ROLE_RELEVANT_PERSON) {
    if (!evidence.usableRoute) {
      return {
        blocker: BUYER_RESOLUTION_BLOCKER.PERSON_ROUTE_MISSING,
        continueResearch: true,
        nextAction: "Locate usable route for verified role-relevant person",
      };
    }
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.ORG_ROUTE_NAMED_ENRICHMENT_INCOMPLETE,
      continueResearch: paidJustified,
      nextAction: paidJustified
        ? "Optional named-person enrichment within authorization"
        : "Org/person route usable — named enrichment optional, not a close reason",
    };
  }
  if (evidence.tier === CONTACT_EVIDENCE_TIER.UNVERIFIED_CONTACT && isPersonName(evidence.surface?.person)) {
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.PERSON_ROLE_UNVERIFIED,
      continueResearch: true,
      nextAction: "Verify role relevance against demand mechanism before treating as buyer",
    };
  }
  if (
    evidence.tier === CONTACT_EVIDENCE_TIER.FUNCTIONAL_DEPARTMENT_ROUTE ||
    evidence.tier === CONTACT_EVIDENCE_TIER.GENERIC_ORGANIZATION_ROUTE ||
    evidence.tier === CONTACT_EVIDENCE_TIER.INTERMEDIARY_ROUTE
  ) {
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.ORG_ROUTE_NAMED_ENRICHMENT_INCOMPLETE,
      continueResearch: false,
      nextAction:
        "Do not close for lack of named person — usable org route is valid exploratory outreach; named enrichment optional",
    };
  }
  if (evidence.tier === CONTACT_EVIDENCE_TIER.NO_USABLE_ROUTE) {
    const fn = opportunity.buyerFunction || opportunity.contact?.buyerFunctionHint;
    if (fn && fn !== BUYER_FUNCTION.UNKNOWN) {
      return {
        blocker: BUYER_RESOLUTION_BLOCKER.FUNCTION_KNOWN_PERSON_UNKNOWN,
        continueResearch: true,
        nextAction: `Buyer function ${fn} known — locate department route or role-relevant person`,
      };
    }
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.BUYER_NOT_IDENTIFIED,
      continueResearch: true,
      nextAction: "Identify buyer function then org sales route before paid providers",
    };
  }
  if (!paidJustified) {
    return {
      blocker: BUYER_RESOLUTION_BLOCKER.PAID_RESEARCH_NOT_JUSTIFIED,
      continueResearch: false,
      nextAction: "Public research incomplete or EV insufficient for paid spend",
    };
  }
  return {
    blocker: BUYER_RESOLUTION_BLOCKER.EXPLORATORY_PROSPECT,
    continueResearch: true,
    nextAction: "Continue economically justified public research",
  };
}

/**
 * Full buyer-resolution snapshot for one prospect / demand-owner.
 */
export function resolveBuyerForProspect({
  opportunity = {},
  demandOwner = null,
  roleRelevanceVerified = false,
  decisionMakerVerified = false,
  researchExhausted = false,
  paidJustified = false,
  disposition = null,
} = {}) {
  const bag = {
    ...opportunity,
    organizationName:
      demandOwner?.organizationName || opportunity.organizationName,
    contact: opportunity.contact,
  };
  if (demandOwner?.buyerFunction) {
    bag.buyerFunction = demandOwner.buyerFunction;
  } else if (demandOwner?.role) {
    bag.buyerFunction = buyerFunctionForRole(
      demandOwner.role,
      demandOwner.mechanism || opportunity.eventDemandMechanism
    );
  }

  const contactEvidence = classifyContactEvidence(bag, {
    roleRelevanceVerified,
    decisionMakerVerified,
  });
  const resolutionBlocker = classifyBuyerResolutionBlocker({
    opportunity: bag,
    contactEvidence,
    disposition,
    researchExhausted,
    paidJustified,
  });

  return {
    version: "gdi-e2d-buyer-resolution-v1",
    organizationName: bag.organizationName || null,
    buyerFunction:
      bag.buyerFunction ||
      contactEvidence.surface?.title ||
      BUYER_FUNCTION.UNKNOWN,
    namedPerson: isPersonName(contactEvidence.surface?.person)
      ? contactEvidence.surface.person
      : null,
    namedPersonStatus: isPersonName(contactEvidence.surface?.person)
      ? roleRelevanceVerified
        ? "role_relevant_verified"
        : "unverified_role"
      : "explicitly_unknown",
    contactEvidence,
    buyerConfidence: contactEvidence.buyerConfidence,
    contactRouteQuality: contactEvidence.contactRouteQuality,
    usableSalesRoute:
      contactEvidence.usableRoute &&
      isUsableOrganizationSalesRoute({
        businessEmail: contactEvidence.surface?.email,
        businessPhone: contactEvidence.surface?.phone,
        contactFormUrl: contactEvidence.surface?.contactUrl,
        contactRouteQuality: contactEvidence.contactRouteQuality,
      }),
    resolutionBlocker,
    note:
      "Generic organization routes are usable outreach — never label as verified buyers.",
  };
}
