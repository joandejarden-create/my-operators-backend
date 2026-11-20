/**
 * Commercial Contact Intelligence — evidence-gated classification.
 *
 * Title alone is never enough for DIRECT_HOTEL_BUYER or
 * VERIFIED_MEETINGS_OR_TRAVEL_BUYER. Scores cannot override these gates.
 */
export const GDI_COMMERCIAL_CONTACT_CLASSIFICATION_V1 =
  "gdi-commercial-contact-classification-v1";

export const COMMERCIAL_CONTACT_CLASS = Object.freeze({
  DIRECT_HOTEL_BUYER: "DIRECT_HOTEL_BUYER",
  VERIFIED_MEETINGS_OR_TRAVEL_BUYER: "VERIFIED_MEETINGS_OR_TRAVEL_BUYER",
  RELEVANT_COMMERCIAL_ENTRY_POINT: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE:
    "ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE",
  GENERAL_BUSINESS_ROUTE: "GENERAL_BUSINESS_ROUTE",
  AUTHORITY_UNVERIFIED: "AUTHORITY_UNVERIFIED",
});

export const COMMERCIAL_CONTACT_CLASS_LABEL = Object.freeze({
  [COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER]:
    "Verified direct hotel buyer",
  [COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER]:
    "Verified meetings / travel buyer",
  [COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT]:
    "Relevant commercial entry point",
  [COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE]:
    "Organizational housing / meetings route",
  [COMMERCIAL_CONTACT_CLASS.GENERAL_BUSINESS_ROUTE]: "General business route",
  [COMMERCIAL_CONTACT_CLASS.AUTHORITY_UNVERIFIED]: "Authority unverified",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function bool(v) {
  return v === true;
}

/**
 * Evidence flags — each must be independently source-supported when true.
 * Callers supply evidence; this module never invents authority from title.
 */
export function buildCommercialContactEvidence(input = {}) {
  return {
    connectedToDemandActivity: bool(input.connectedToDemandActivity),
    confirmedHotelSourcingResponsibility: bool(
      input.confirmedHotelSourcingResponsibility,
    ),
    confirmedMeetingOrVenueBuyingResponsibility: bool(
      input.confirmedMeetingOrVenueBuyingResponsibility,
    ),
    confirmedTravelProcurementResponsibility: bool(
      input.confirmedTravelProcurementResponsibility,
    ),
    confirmedHousingAccommodationResponsibility: bool(
      input.confirmedHousingAccommodationResponsibility,
    ),
    relevantOperationalOrCommercialInfluence: bool(
      input.relevantOperationalOrCommercialInfluence,
    ),
    credibleReferralRoute: bool(input.credibleReferralRoute),
    currentOrganizationAndRole: bool(input.currentOrganizationAndRole),
    supportingSourceUrls: Array.isArray(input.supportingSourceUrls)
      ? input.supportingSourceUrls.filter(Boolean).slice(0, 12)
      : [],
    evidenceNotes: text(input.evidenceNotes) || null,
    unverifiedResponsibilityGaps: Array.isArray(
      input.unverifiedResponsibilityGaps,
    )
      ? input.unverifiedResponsibilityGaps.map(text).filter(Boolean)
      : [],
  };
}

/**
 * Infer *candidate* evidence hints from text (not authority certification).
 * Used only to propose research questions / gaps — never to promote to DIRECT.
 */
export function inferEvidenceHintsFromText(person = {}, opportunity = {}) {
  const title = text(person.title || person.role);
  const why = text(
    person.whyRelevant ||
      person.whyThisContact ||
      person.relationshipToOpportunity ||
      "",
  );
  const blob = `${title} ${why} ${text(opportunity.title)} ${text(opportunity.segment)}`.toLowerCase();
  const hints = {
    titleSuggestsSales: /sales manager|account manager|commercial/i.test(title),
    titleSuggestsHousing:
      /housing|accommodation|room block|hotel partner|lodging/i.test(blob),
    titleSuggestsMeetings:
      /meeting.?planner|event.?manager|congress|conference.?manager/i.test(
        blob,
      ),
    titleSuggestsTravel:
      /travel.?manager|travel.?procurement|corporate.?travel|tmc/i.test(blob),
    titleSuggestsProcurement:
      /procurement|adquisicion|recursos materiales|purchasing/i.test(blob),
    governmentOrPublicSector:
      /gobierno|secretar[ií]a|municipal|jalisco|siop|public/i.test(blob),
    eventOrganizerSales:
      /exhibition|expo|ecomondo|igeco|messe|fair organizer/i.test(blob),
  };
  const gaps = [];
  if (hints.titleSuggestsSales || hints.eventOrganizerSales) {
    gaps.push("hotel_accommodation_responsibility_not_confirmed");
  }
  if (hints.titleSuggestsProcurement || hints.governmentOrPublicSector) {
    gaps.push("hotel_sourcing_authority_not_inferred_from_procurement_title");
  }
  if (hints.titleSuggestsHousing) {
    gaps.push("confirm_named_housing_coordinator_vs_org_route");
  }
  return { hints, suggestedGaps: gaps };
}

/**
 * Classify with hard evidence gates. Title-only → never DIRECT / VERIFIED buyer.
 */
export function classifyCommercialContact(person = {}, opts = {}) {
  const evidence = buildCommercialContactEvidence(
    opts.evidence || person.commercialEvidence || {},
  );
  const name = text(person.name || person.fullName);
  const title = text(person.title || person.role);
  const org = text(
    person.organization ||
      person.organizationName ||
      opts.organizationName ||
      "",
  );
  const isNamed = Boolean(name);
  const isOrgRouteOnly = !isNamed && Boolean(opts.organizationalRoute);

  // Hard gates for buyer classes
  if (
    evidence.confirmedHotelSourcingResponsibility &&
    evidence.connectedToDemandActivity &&
    evidence.currentOrganizationAndRole &&
    isNamed
  ) {
    return finalize(
      COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER,
      evidence,
      person,
      opts,
    );
  }

  if (
    (evidence.confirmedMeetingOrVenueBuyingResponsibility ||
      evidence.confirmedTravelProcurementResponsibility ||
      evidence.confirmedHousingAccommodationResponsibility) &&
    evidence.connectedToDemandActivity &&
    evidence.currentOrganizationAndRole &&
    isNamed
  ) {
    return finalize(
      COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER,
      evidence,
      person,
      opts,
    );
  }

  if (isOrgRouteOnly || opts.forceOrganizationalRoute === true) {
    if (
      evidence.confirmedHousingAccommodationResponsibility ||
      evidence.confirmedMeetingOrVenueBuyingResponsibility ||
      /housing|accommodation|meetings|congress|hotel/i.test(
        `${title} ${text(opts.routeLabel)}`,
      )
    ) {
      return finalize(
        COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE,
        evidence,
        person,
        opts,
      );
    }
    return finalize(
      COMMERCIAL_CONTACT_CLASS.GENERAL_BUSINESS_ROUTE,
      evidence,
      person,
      opts,
    );
  }

  if (
    isNamed &&
    (evidence.relevantOperationalOrCommercialInfluence ||
      evidence.credibleReferralRoute ||
      evidence.connectedToDemandActivity)
  ) {
    // Explicit prior class from PCD 2.0 / research may already say entry point
    const prior = text(
      person.contactClass || person.contactClassification || "",
    ).toUpperCase();
    if (
      prior === COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT ||
      /RELEVANT|ENTRY_POINT|INFLUENCER|REFERRAL/i.test(prior) ||
      evidence.relevantOperationalOrCommercialInfluence ||
      evidence.credibleReferralRoute
    ) {
      return finalize(
        COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
        evidence,
        person,
        {
          ...opts,
          ensureGap:
            "hotel_accommodation_responsibility_not_yet_confirmed",
        },
      );
    }
  }

  if (isNamed && evidence.currentOrganizationAndRole) {
    return finalize(
      COMMERCIAL_CONTACT_CLASS.AUTHORITY_UNVERIFIED,
      evidence,
      person,
      opts,
    );
  }

  if (isNamed) {
    return finalize(
      COMMERCIAL_CONTACT_CLASS.AUTHORITY_UNVERIFIED,
      evidence,
      person,
      opts,
    );
  }

  return finalize(
    COMMERCIAL_CONTACT_CLASS.GENERAL_BUSINESS_ROUTE,
    evidence,
    person,
    opts,
  );
}

function finalize(klass, evidence, person, opts = {}) {
  const gaps = [...(evidence.unverifiedResponsibilityGaps || [])];
  if (
    opts.ensureGap &&
    !gaps.includes(opts.ensureGap) &&
    klass !== COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER &&
    klass !== COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER
  ) {
    gaps.push(opts.ensureGap);
  }
  // Reject buyer promotion when title-only / no buying evidence
  const buyerPromotedWithoutEvidence =
    (klass === COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER ||
      klass === COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER) &&
    !evidence.confirmedHotelSourcingResponsibility &&
    !evidence.confirmedMeetingOrVenueBuyingResponsibility &&
    !evidence.confirmedTravelProcurementResponsibility &&
    !evidence.confirmedHousingAccommodationResponsibility;

  if (buyerPromotedWithoutEvidence) {
    return finalize(
      COMMERCIAL_CONTACT_CLASS.AUTHORITY_UNVERIFIED,
      {
        ...evidence,
        unverifiedResponsibilityGaps: [
          ...gaps,
          "buyer_class_rejected_insufficient_evidence",
        ],
      },
      person,
      {},
    );
  }

  return {
    version: GDI_COMMERCIAL_CONTACT_CLASSIFICATION_V1,
    class: klass,
    label: COMMERCIAL_CONTACT_CLASS_LABEL[klass],
    personName: text(person.name || person.fullName) || null,
    title: text(person.title || person.role) || null,
    organization: text(person.organization || opts.organizationName) || null,
    evidence: {
      ...evidence,
      unverifiedResponsibilityGaps: gaps,
    },
    buyerAuthorityClaimed:
      klass === COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER ||
      klass === COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER,
    salesFriendlyWhy: buildSalesFriendlyWhy(klass, person, evidence, gaps),
  };
}

/**
 * Customer-facing explanation — no internal score dump.
 */
export function buildSalesFriendlyWhy(klass, person = {}, evidence = {}, gaps = []) {
  const name = text(person.name) || "This contact";
  const title = text(person.title || person.role);
  const roleBit = title ? ` (${title})` : "";

  switch (klass) {
    case COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER:
      return `${name}${roleBit} — verified hotel sourcing responsibility for this demand activity.`;
    case COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER:
      return `${name}${roleBit} — verified meetings / travel / housing buying responsibility; appropriate first contact for room blocks or travel procurement.`;
    case COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT:
      return gaps.includes("hotel_accommodation_responsibility_not_yet_confirmed") ||
        gaps.includes("hotel_accommodation_responsibility_not_confirmed")
        ? `Best available organizer / commercial contact${roleBit}. Hotel accommodation responsibility not yet confirmed.`
        : `${name}${roleBit} — relevant commercial entry point / referral route for this opportunity.`;
    case COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE:
      return `Organizational housing / meetings route${title ? ` (${title})` : ""}. Use when a named buyer is not yet identified.`;
    case COMMERCIAL_CONTACT_CLASS.GENERAL_BUSINESS_ROUTE:
      return `General published business route${title ? ` (${title})` : ""}. Qualify who owns hotel sourcing before treating as a buyer.`;
    default:
      return `${name}${roleBit} — role and buying authority for hotel demand remain unverified.`;
  }
}

export function rejectBuyerPromotionWithoutEvidence(classification) {
  if (!classification?.buyerAuthorityClaimed) return classification;
  const e = classification.evidence || {};
  const ok =
    e.confirmedHotelSourcingResponsibility ||
    e.confirmedMeetingOrVenueBuyingResponsibility ||
    e.confirmedTravelProcurementResponsibility ||
    e.confirmedHousingAccommodationResponsibility;
  if (ok) return classification;
  return classifyCommercialContact(
    { name: classification.personName, title: classification.title },
    {
      evidence: {
        ...e,
        unverifiedResponsibilityGaps: [
          ...(e.unverifiedResponsibilityGaps || []),
          "false_positive_buyer_promotion_rejected",
        ],
      },
    },
  );
}
