/**
 * Commercial accessibility + organizational route assessment V1.
 *
 * Distinguishes organizer / PCO / housing / sponsorship / functional routes.
 * Does not invent people, emails, or open room blocks.
 * Scaling Companies must receive corporate-travel actions — never Association
 * PCO / host-overflow procurement templates.
 */
import { isScalingCompaniesSegment } from "../scaling-drawer-presentation-contract-v1.js";
import {
  filterPressPublisherOrgRoute,
  isPressPublisherNonCompanyRoute,
} from "../press-publisher-entity-v1.js";

export const COMMERCIAL_ACCESSIBILITY_AND_ORG_ROUTE_V1 =
  "gdi-commercial-accessibility-and-org-route-v1";

export const ORG_ROLE_CLASS = Object.freeze({
  EVENT_OWNING_ASSOCIATION: "EVENT_OWNING_ASSOCIATION",
  EVENT_ORGANIZER: "EVENT_ORGANIZER",
  PCO_AMC: "PCO_AMC",
  HOUSING_PROVIDER: "HOUSING_PROVIDER",
  SPONSORSHIP_EXHIBITION: "SPONSORSHIP_EXHIBITION",
  MEETINGS_VENUE_SOURCING: "MEETINGS_VENUE_SOURCING",
  CORPORATE_TRAVEL_MEETINGS: "CORPORATE_TRAVEL_MEETINGS",
  FUNCTIONAL_INQUIRY_ROUTE: "FUNCTIONAL_INQUIRY_ROUTE",
  UNKNOWN_ORG_ROLE: "UNKNOWN_ORG_ROLE",
});

export const SOURCING_ACCESSIBILITY_CLASS = Object.freeze({
  VERIFIED_SOURCING_ARRANGEMENT: "VERIFIED_SOURCING_ARRANGEMENT",
  EVIDENCE_SUPPORTED_HYPOTHESIS: "EVIDENCE_SUPPORTED_HYPOTHESIS",
  UNKNOWN_SOURCING_STATUS: "UNKNOWN_SOURCING_STATUS",
  CLEARLY_INACCESSIBLE: "CLEARLY_INACCESSIBLE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function evidenceBlob(opportunity = {}, extra = "") {
  const g = opportunity.associationEventsGoldBrief || opportunity.salesBrief || {};
  return text(
    [
      opportunity.title,
      opportunity.organizationName,
      opportunity.eventName,
      opportunity.summaryWhat,
      opportunity.whatIsHappening,
      opportunity.whyThisContact,
      opportunity.buyingStatus,
      opportunity.sourcingStatus,
      g.buyingAndSourcingStatus,
      g.eventHistoryAndActivity,
      g.whoToContact?.primary?.title,
      g.whoToContact?.primary?.role,
      ...(opportunity.sources || []).map((s) => `${s.title || ""} ${s.excerpt || ""}`),
      extra,
    ]
      .filter(Boolean)
      .join("\n"),
  ).slice(0, 24000);
}

/**
 * Classify organizational role from evidence (not interchangeable).
 */
export function classifyOrgRoleFromEvidence(opportunity = {}, { evidenceText = "" } = {}) {
  const blob = evidenceText || evidenceBlob(opportunity);
  const roleHint = text(
    opportunity.contactRole ||
      opportunity.primaryContact?.title ||
      opportunity.primaryContact?.role ||
      "",
  );
  const pack = `${blob}\n${roleHint}`;

  // Housing control beats generic PCO/AMC labels when both appear (e.g. Spargo housing desk).
  if (/housing bureau|stay[- ]?to[- ]?play|hoteles oficiales|alojamiento oficial|hotel reservation|passkey|onpeak|bloque de habitaciones oficial/i.test(pack)) {
    return {
      orgRoleClass: ORG_ROLE_CLASS.HOUSING_PROVIDER,
      rationale: "Evidence points to housing / official accommodation control",
    };
  }
  if (/spargo|pco|professional conference organizer|association management|AMC\b|congresista profesional|organizador profesional de congresos/i.test(pack)) {
    return {
      orgRoleClass: ORG_ROLE_CLASS.PCO_AMC,
      rationale: "Evidence points to a PCO / association-management company role",
    };
  }
  if (/sponsor|sponsorship|exhibition sales|exhibitor services|prospectus.*partner|partner contact/i.test(pack) &&
      !/housing|hospedaje|meetings manager|venue sourcing/i.test(pack)) {
    return {
      orgRoleClass: ORG_ROLE_CLASS.SPONSORSHIP_EXHIBITION,
      rationale: "Evidence points to sponsorship / exhibition — referral route only unless lodging ownership is also shown",
    };
  }
  if (/meetings manager|director of meetings|venue sourcing|hotel sourcing|group sales|director de eventos|director de congresos|coordinador de hospedaje/i.test(pack)) {
    return {
      orgRoleClass: ORG_ROLE_CLASS.MEETINGS_VENUE_SOURCING,
      rationale: "Evidence points to meetings / venue / hotel sourcing responsibility",
    };
  }
  // Only for Scaling segment — do not match association sources that merely say "meetings and events".
  if (
    isScalingCompaniesSegment(opportunity) ||
    /scaling/i.test(String(opportunity.researchSegment || opportunity.segment || ""))
  ) {
    return {
      orgRoleClass: ORG_ROLE_CLASS.CORPORATE_TRAVEL_MEETINGS,
      rationale: "Scaling / corporate demand — travel, meetings, or workplace functions may own lodging",
    };
  }
  if (/comit[eé] organizador|event organizer|organizador del (evento|congreso)|secretariat|secretar[ií]a t[eé]cnica/i.test(pack)) {
    return {
      orgRoleClass: ORG_ROLE_CLASS.EVENT_ORGANIZER,
      rationale: "Evidence points to event organizer / technical secretariat",
    };
  }
  if (text(opportunity.organizationName || opportunity.organizer)) {
    return {
      orgRoleClass: ORG_ROLE_CLASS.EVENT_OWNING_ASSOCIATION,
      rationale: "Default to event-owning association / company until a more specific role is evidenced",
    };
  }
  return {
    orgRoleClass: ORG_ROLE_CLASS.UNKNOWN_ORG_ROLE,
    rationale: "Organizational role not yet evidenced",
  };
}

/**
 * Assess whether the hotel has a realistic sourcing access path.
 */
export function assessCommercialAccessibilityFromEvidence(
  opportunity = {},
  { evidenceText = "", hotelName = "this hotel" } = {},
) {
  const blob = evidenceText || evidenceBlob(opportunity);
  const pathClass = opportunity.lodgingPathClass || null;
  const scaling = isScalingCompaniesSegment(opportunity);

  if (scaling) {
    return {
      sourcingAccessibilityClass: SOURCING_ACCESSIBILITY_CLASS.EVIDENCE_SUPPORTED_HYPOTHESIS,
      verifiedArrangement: false,
      hypothesis: true,
      summary: `Corporate travel / visiting-staff lodging accessibility for ${hotelName} is a hypothesis until a travel, HR, facilities, or meetings owner confirms preferred-hotel or project-stay patterns`,
      askNext: [
        "Who owns visiting-staff lodging, training stays, and local meetings?",
        "Is there a preferred-hotel, travel-management, or project-team lodging pattern?",
        "Is the local site still an active operating location?",
      ],
    };
  }

  if (/already contracted exclusive host|exclusive headquarters hotel|no overflow|closed block|sold out with no overflow/i.test(blob) &&
      !/overflow|recommended hotel|still seeking|busca hotel|alojamiento disponible/i.test(blob)) {
    return {
      sourcingAccessibilityClass: SOURCING_ACCESSIBILITY_CLASS.CLEARLY_INACCESSIBLE,
      verifiedArrangement: true,
      hypothesis: false,
      summary: `Evidence suggests lodging is already locked without an overflow path — ${hotelName} should not be pitched as primary host without new cycle evidence`,
      askNext: [
        "Confirm whether any overflow / recommended list still opens",
        "Ask whether the next edition's hotel selection window has dates",
      ],
    };
  }

  if (/official hotel|hotel sede|host hotel confirmed|hoteles oficiales|stay[- ]?to[- ]?play|housing bureau/i.test(blob)) {
    return {
      sourcingAccessibilityClass: SOURCING_ACCESSIBILITY_CLASS.VERIFIED_SOURCING_ARRANGEMENT,
      verifiedArrangement: true,
      hypothesis: false,
      summary: `A sourcing arrangement (host / official / housing) is evidenced — ${hotelName} must pursue inclusion, overflow, or preferred rates rather than inventing an open RFP`,
      askNext: [
        "Who controls the official housing list?",
        "Is overflow or preferred-hotel addition still possible?",
      ],
    };
  }

  if (
    pathClass === "OVERFLOW_OR_RECOMMENDED" ||
    pathClass === "LODGING_SIGNAL_UNSTRUCTURED" ||
    /hospedaje|alojamiento|room block|hotel|venue selection|sourcing/i.test(blob)
  ) {
    return {
      sourcingAccessibilityClass: SOURCING_ACCESSIBILITY_CLASS.EVIDENCE_SUPPORTED_HYPOTHESIS,
      verifiedArrangement: false,
      hypothesis: true,
      summary: `Lodging/venue language supports a sourcing hypothesis for ${hotelName}, but exclusive host status and decision owner are not fully confirmed`,
      askNext: [
        "Is lodging centralized (association/PCO/housing) or individual?",
        "Has a host hotel already been contracted for this edition?",
        "Who owns hotel selection and what is the timetable?",
      ],
    };
  }

  return {
    sourcingAccessibilityClass: SOURCING_ACCESSIBILITY_CLASS.UNKNOWN_SOURCING_STATUS,
    verifiedArrangement: false,
    hypothesis: false,
    summary: `Sourcing accessibility for ${hotelName} remains unknown — do not assume an open room block or that the hotel cannot compete`,
    askNext: [
      "Who organizes the activity and who sources hotels?",
      "Is there a PCO, housing bureau, or association meetings owner?",
      "What is the next edition's decision window?",
    ],
  };
}

/**
 * Build a sales action that prefers verified functional routes over generic Identify copy.
 * Never invents named buyers. Sponsorship contacts stay referral-only.
 */
function buildScalingCorporateSalesAction({
  opportunity = {},
  hotelName = "this hotel",
  accessibility = null,
  namedBuyer = null,
  orgRouteUrl = null,
} = {}) {
  const org =
    text(
      opportunity.displayCompanyName ||
        opportunity.organizationName ||
        opportunity.company ||
        opportunity.companyName,
    ) || "this company";
  const city =
    text(opportunity.eventCity || opportunity.city || opportunity.market || opportunity.hotelCity) ||
    "the local market";
  const ask = (accessibility?.askNext || []).slice(0, 2).join(" ");
  const safeRoute = filterPressPublisherOrgRoute(orgRouteUrl);
  const routeNote = safeRoute ? ` Public company route on file: ${safeRoute}.` : "";

  // Stage F: preserve sales-leader editorial next step when present
  const candidateNext = text(
    opportunity.salesActionRecommended ||
      opportunity.commercialIntelligenceBrief?.recommendedSalesApproach?.nextAction ||
      "",
  );
  const looksEditorial =
    opportunity.stageFEditorial === true ||
    opportunity.stageFEditorialVersion ||
    (/Goal:\s*identify/i.test(candidateNext) &&
      !/association\/PCO|host-overflow|Selection signal/i.test(candidateNext));
  if (
    looksEditorial &&
    candidateNext &&
    !/association\/PCO|host-overflow|Selection signal/i.test(candidateNext)
  ) {
    return {
      recommendedAction: candidateNext,
      contactPromotion: namedBuyer?.name
        ? "NAMED_RELEVANT_ROUTE"
        : safeRoute
          ? "FUNCTIONAL_ORG_ROUTE"
          : "GENERIC_IDENTIFY",
      referralOnly: false,
    };
  }

  if (namedBuyer?.name) {
    return {
      recommendedAction: (
        `Contact ${namedBuyer.name}${namedBuyer.title ? ` (${namedBuyer.title})` : ""} at ${org} ` +
        `to introduce ${hotelName} and ask who coordinates overnight stays and small meetings for visitors in ${city}. ` +
        `Goal: identify the travel or meetings owner and whether a preferred-hotel pattern already exists.`
      )
        .replace(/\s+/g, " ")
        .trim(),
      contactPromotion: "NAMED_RELEVANT_ROUTE",
      referralOnly: false,
    };
  }

  return {
    recommendedAction: (
      `Identify who owns corporate travel, visiting-staff lodging, training, and local meetings for ${org} in ${city}. ` +
      `Introduce ${hotelName} and ask whether a preferred-hotel or travel-management pattern already exists.` +
      `${routeNote}`
    )
      .replace(/\s+/g, " ")
      .trim(),
    contactPromotion: safeRoute ? "FUNCTIONAL_ORG_ROUTE" : "GENERIC_IDENTIFY",
    referralOnly: false,
  };
}

export function buildOrgRouteSalesAction({
  opportunity = {},
  hotelName = "this hotel",
  orgRole = null,
  accessibility = null,
  namedBuyer = null,
  orgRouteUrl = null,
} = {}) {
  const eventName =
    text(opportunity.eventName || opportunity.title || "this opportunity");
  const org = text(opportunity.organizationName || opportunity.organizer || "the organizing body");
  const role = orgRole?.orgRoleClass || ORG_ROLE_CLASS.UNKNOWN_ORG_ROLE;
  const access =
    accessibility?.sourcingAccessibilityClass ||
    SOURCING_ACCESSIBILITY_CLASS.UNKNOWN_SOURCING_STATUS;
  const ask = (accessibility?.askNext || []).slice(0, 2).join(" ");
  const safeOrgRoute = filterPressPublisherOrgRoute(orgRouteUrl);

  // Scaling / corporate-travel: never emit Association PCO / host-overflow templates.
  if (
    role === ORG_ROLE_CLASS.CORPORATE_TRAVEL_MEETINGS ||
    isScalingCompaniesSegment(opportunity)
  ) {
    return buildScalingCorporateSalesAction({
      opportunity,
      hotelName,
      accessibility,
      namedBuyer,
      orgRouteUrl: safeOrgRoute,
    });
  }

  if (namedBuyer?.name && role !== ORG_ROLE_CLASS.SPONSORSHIP_EXHIBITION) {
    return {
      recommendedAction: `Contact ${namedBuyer.name}${namedBuyer.title ? ` (${namedBuyer.title})` : ""} at ${org} regarding ${eventName}: introduce ${hotelName}, ask how lodging/meetings are sourced, and whether hotel selection remains open. ${ask}`.trim(),
      contactPromotion: "NAMED_RELEVANT_ROUTE",
      referralOnly: false,
    };
  }

  if (role === ORG_ROLE_CLASS.SPONSORSHIP_EXHIBITION && namedBuyer?.name) {
    return {
      recommendedAction: `Use ${namedBuyer.name} (${namedBuyer.title || "sponsorship/exhibition"}) only as a referral into meetings/housing ownership for ${eventName} — do not treat sponsorship as lodging procurement authority. Ask who owns hotel sourcing for ${org}.`,
      contactPromotion: "REFERRAL_ONLY",
      referralOnly: true,
    };
  }

  if (role === ORG_ROLE_CLASS.HOUSING_PROVIDER || role === ORG_ROLE_CLASS.PCO_AMC) {
    return {
      recommendedAction: `Approach the ${role === ORG_ROLE_CLASS.PCO_AMC ? "PCO / association-management" : "housing"} route for ${org} (${eventName})${safeOrgRoute ? ` via ${safeOrgRoute}` : ""}: ask how official hotels / overflow are selected and whether ${hotelName} can be considered. ${ask}`.trim(),
      contactPromotion: "FUNCTIONAL_ORG_ROUTE",
      referralOnly: false,
    };
  }

  if (safeOrgRoute || org) {
    return {
      recommendedAction: `Open with ${org}'s meetings / congress / operations route for ${eventName}${safeOrgRoute ? ` (${safeOrgRoute})` : ""}: introduce ${hotelName}, ask who owns hotel sourcing (association staff vs PCO vs housing), and whether a host/overflow decision is still open. Accessibility: ${access}. ${ask}`.trim(),
      contactPromotion: "FUNCTIONAL_ORG_ROUTE",
      referralOnly: false,
    };
  }

  return {
    recommendedAction: `Identify the meetings / congress / housing owner for ${eventName} (not sponsorship alone) and introduce ${hotelName}; confirm sourcing timetable and whether hotel selection remains open.`,
    contactPromotion: "GENERIC_IDENTIFY",
    referralOnly: false,
  };
}

/**
 * Full assessment pack for gold-brief / remediation integration.
 */
export function assessBuyerOrgAndAccessibility(opportunity = {}, opts = {}) {
  const hotelName =
    text(opts.hotelName) ||
    text(opportunity.hotelDisplayName || opportunity.hotelName) ||
    "this hotel";
  const evidenceText = opts.evidenceText || evidenceBlob(opportunity);
  const orgRole = classifyOrgRoleFromEvidence(opportunity, { evidenceText });
  const accessibility = assessCommercialAccessibilityFromEvidence(opportunity, {
    evidenceText,
    hotelName,
  });
  const gold = opportunity.associationEventsGoldBrief || opportunity.salesBrief || {};
  const named =
    (opportunity.hasVerifiedNamedRelevantContact &&
      (opportunity.contactName || opportunity.primaryContact?.name) && {
        name: opportunity.contactName || opportunity.primaryContact?.name,
        title: opportunity.contactRole || opportunity.primaryContact?.title,
      }) ||
    null;
  const rawOrgRouteUrl =
    gold.whoToContact?.routes?.url ||
    opportunity.organizationRouteUrl ||
    opportunity.contactUrl ||
    null;
  const orgRouteUrl = filterPressPublisherOrgRoute(rawOrgRouteUrl);
  const publisherRouteRejected =
    Boolean(rawOrgRouteUrl) &&
    !orgRouteUrl &&
    isPressPublisherNonCompanyRoute(rawOrgRouteUrl);
  const action = buildOrgRouteSalesAction({
    opportunity,
    hotelName,
    orgRole,
    accessibility,
    namedBuyer: named,
    orgRouteUrl,
  });

  return {
    version: COMMERCIAL_ACCESSIBILITY_AND_ORG_ROUTE_V1,
    at: new Date().toISOString(),
    orgRoleClass: orgRole.orgRoleClass,
    orgRoleRationale: orgRole.rationale,
    sourcingAccessibilityClass: accessibility.sourcingAccessibilityClass,
    accessibilitySummary: accessibility.summary,
    askNext: accessibility.askNext,
    recommendedAction: action.recommendedAction,
    contactPromotion: action.contactPromotion,
    referralOnly: action.referralOnly === true,
    orgRouteUrl,
    publisherRouteRejected: publisherRouteRejected || undefined,
    rejectedPublisherRouteUrl: publisherRouteRejected ? rawOrgRouteUrl : undefined,
    namedBuyer: named,
  };
}

/**
 * Apply assessment onto opportunity + gold brief fields (no invented people).
 */
export function applyBuyerOrgAccessibilityToOpportunity(opportunity = {}, assessment = {}, opts = {}) {
  if (!assessment?.version) return opportunity;
  const next = { ...opportunity };
  next.orgRoleClass = assessment.orgRoleClass;
  next.sourcingAccessibilityClass = assessment.sourcingAccessibilityClass;
  next.commercialAccessibilitySummary = assessment.accessibilitySummary;
  next.buyerOrgAccessibility = assessment;
  next.recommendedAction = assessment.recommendedAction;
  next.salesActionRecommended = assessment.recommendedAction;
  next.buyingStatus = assessment.accessibilitySummary;
  next.sourcingStatus = assessment.sourcingAccessibilityClass;

  const briefKey = isScalingCompaniesSegment(next)
    ? "commercialIntelligenceBrief"
    : "associationEventsGoldBrief";
  if (next[briefKey]) {
    const brief = { ...next[briefKey] };
    brief.buyingAndSourcingStatus = assessment.accessibilitySummary;
    // Preserve structured Scaling approach objects when present; only overwrite
    // string approaches / Association contamination paths with the segment action.
    const priorApproach = brief.recommendedSalesApproach;
    if (
      priorApproach &&
      typeof priorApproach === "object" &&
      isScalingCompaniesSegment(next)
    ) {
      brief.recommendedSalesApproach = {
        ...priorApproach,
        nextAction: assessment.recommendedAction,
        timingRationale: priorApproach.timingRationale || null,
      };
    } else {
      brief.recommendedSalesApproach = assessment.recommendedAction;
    }
    brief.orgRoleClass = assessment.orgRoleClass;
    brief.sourcingAccessibilityClass = assessment.sourcingAccessibilityClass;
    const priorRoute = brief.whoToContact?.routes?.url || null;
    const safePrior = filterPressPublisherOrgRoute(priorRoute);
    brief.whoToContact = {
      ...(brief.whoToContact || {}),
      orgRoleClass: assessment.orgRoleClass,
      sourcingAccessibilityClass: assessment.sourcingAccessibilityClass,
      routes: {
        ...(brief.whoToContact?.routes || {}),
        url: assessment.orgRouteUrl || safePrior || null,
      },
      referralOnly: assessment.referralOnly === true,
      contactPromotion: assessment.contactPromotion,
    };
    if (assessment.referralOnly && brief.whoToContact?.primary) {
      brief.whoToContact.primary = {
        ...brief.whoToContact.primary,
        isRelevantCommercialEntryPoint: false,
        contactEligibilityNote: "REFERRAL_ONLY_NOT_LODGING_BUYER",
      };
    }
    next[briefKey] = brief;
  }

  // Top-level action fields used by drawer
  if (!opts.skipSuggestedAction) {
    next.suggestedAction = assessment.recommendedAction;
  }
  return next;
}
