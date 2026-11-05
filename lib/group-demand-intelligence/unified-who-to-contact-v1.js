/**
 * Unified Who to Contact V1 / V2 — shared contact DTO hydration for ALL GDI segments.
 *
 * Presentation reuses Scaling gold-standard two-contact slots
 * (`scalingTwoContactPresentation` → drawer `twoContactSlotHtml`).
 * Segment-specific buyer relevance lives in role/why fields only.
 *
 * V2: force hydration for every demand segment (not Events/Scaling only).
 * Never invents emails/phones. Never auto-triggers Surfe.
 */
import { applyScalingTwoContactStandard } from "./scaling-two-contact-standard-v1.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import { isGenericOrganizationEmail } from "./scaling-companies-sales-presentation-v1.js";
import { isAssociationConferenceEventsOpportunity } from "./events-opportunity-overview-v1.js";
import { isScalingCompaniesSegment } from "./scaling-drawer-presentation-contract-v1.js";
import { attachUniversalTileContactPreview } from "./universal-tile-contact-preview-v1.js";

export const UNIFIED_WHO_TO_CONTACT_V1 = "gdi-unified-who-to-contact-v1";
/** Platform-wide contact standard — same renderer for all 12 segments + PLP/PEP. */
export const UNIVERSAL_WHO_TO_CONTACT_V2 = "gdi-universal-who-to-contact-v2";

/** Customer-facing primary route labels (never raw enums). */
export const CUSTOMER_CONTACT_ROUTE_LABEL = Object.freeze({
  NAMED_BUYER: "Named buyer / sourcing contact",
  NAMED_EVENT_ORGANIZER: "Named event organizer",
  NAMED_VENUE_PLANNER: "Named venue or planner contact",
  NAMED_OPERATIONAL: "Named operational influencer",
  NAMED_REFERRAL: "Named person — professional profile / referral route",
  ORGANIZATION_PATH: "Organization contact path",
  PROCUREMENT_PATH: "Procurement contact path",
  RESEARCH_GAP: "Research continuation — contact not verified",
});

export const EVENTS_CONTACT_ROLE_CLASS = Object.freeze({
  DECISION_MAKER: "Decision-Maker",
  EVENT_ORGANIZER: "Event Organizer",
  MEETING_PLANNER: "Meeting Planner",
  HOTEL_SOURCING: "Hotel Sourcing Contact",
  RELEVANT_INFLUENCER: "Relevant Influencer",
  REFERRAL: "Referral Contact",
  /** Customer-facing label when role evidence is insufficient (never show as sales-ready buyer). */
  UNSPECIFIED: "Role Unverified",
});

/** Internal eligibility code — not for customer UI. */
export const EVENTS_CONTACT_BUYER_GATE = Object.freeze({
  INELIGIBLE_UNSPECIFIED: "INELIGIBLE_UNSPECIFIED",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isEventsSegment(o = {}) {
  return (
    isAssociationConferenceEventsOpportunity(o) ||
    o.researchSegment === "associations_government" ||
    /association|conference|events/i.test(String(o.customerSegmentUmbrella || ""))
  );
}

/** Collapse repeated hotel-sourcing / authority caveats into one sentence. */
export function dedupeAuthorityDisclaimer(raw) {
  let s = text(raw);
  if (!s) return "";
  s = s
    .replace(/\bCONTACT_RELEVANT\b\.?\s*/gi, "")
    .replace(/\bINELIGIBLE_UNSPECIFIED\b\.?\s*/gi, "")
    .replace(/\bCONTRACTING_AUTHORITY_VERIFIED\b/gi, "contracting authority")
    .replace(/\s{2,}/g, " ")
    .trim();
  const caveatRe =
    /(?:confirm|not\s+(?:yet\s+)?(?:verified|confirmed|published)|unverified)[^.]*?(?:hotel[- ]?sourc(?:ing|e)|housing|signing\s+authority|buyer\s+authority)[^.]*\.?/gi;
  const caveats = s.match(caveatRe) || [];
  if (caveats.length > 1) {
    s = s.replace(caveatRe, "").replace(/\s{2,}/g, " ").trim();
    const keep = text(caveats[caveats.length - 1]).replace(/\.*$/, ".");
    s = `${s.replace(/[.;:\s]+$/, "")}. ${keep}`.replace(/\s{2,}/g, " ").trim();
  }
  // Identical consecutive sentences
  const parts = s
    .split(/(?<=\.)\s+/)
    .map((p) => text(p))
    .filter(Boolean);
  const out = [];
  for (const p of parts) {
    if (out.some((x) => x.toLowerCase() === p.toLowerCase())) continue;
    out.push(p);
  }
  return out.join(" ").trim();
}

function buildDefaultWhyRelevant({
  title,
  org,
  roleDisplay,
  opportunity,
  buyerEligible,
  isEvents,
} = {}) {
  const programme =
    text(opportunity.eventName || opportunity.programmeName || opportunity.title) ||
    "this opportunity";
  const orgBit = org ? ` at ${org}` : "";
  if (isEvents) {
    if (title) {
      return `${title}${orgBit}${roleDisplay ? ` — ${roleDisplay}` : ""} for ${programme}. Confirm hotel-sourcing / housing authority before pursuit.`;
    }
    if (roleDisplay) {
      return `${roleDisplay} for ${programme}; hotel-sourcing authority unverified.`;
    }
    return `Named contact for ${programme}; confirm commercial relevance and hotel-sourcing authority before outreach.`;
  }
  if (title && org) {
    return buyerEligible
      ? `${title} at ${org}, associated with ${programme}. Relevant commercial outreach route; confirm lodging-sourcing ownership before pursuit.`
      : `${title} at ${org}, associated with ${programme}. Relevant as an operational or referral route; responsibility for hotel sourcing is not yet verified.`;
  }
  if (title) {
    return `${title} associated with ${programme}; hotel-sourcing authority unverified.`;
  }
  return `Named contact associated with ${programme}; confirm commercial relevance before outreach.`;
}

export function resolveCustomerContactRouteLabel({
  raw = {},
  roleMeta = {},
  title = "",
  buyerEligible = false,
  opportunity = {},
} = {}) {
  const existing = text(
    raw.commercialContactPathLabel || raw.customerRouteLabel || "",
  );
  if (
    existing &&
    !/^[A-Z0-9_]+$/.test(existing) &&
    !/NAMED_DIRECT|ORGANIZATION_PATH|NO_CONTACT/i.test(existing)
  ) {
    return existing;
  }
  const blob = [roleMeta.roleLabel, title, raw.roleClass, raw.role]
    .map(text)
    .join(" ")
    .toLowerCase();
  if (buyerEligible && /sourc|housing|room.?block|procurement|buyer/i.test(blob)) {
    return CUSTOMER_CONTACT_ROUTE_LABEL.NAMED_BUYER;
  }
  if (/event organizer|congress|meeting.?planner|pco/i.test(blob)) {
    return CUSTOMER_CONTACT_ROUTE_LABEL.NAMED_EVENT_ORGANIZER;
  }
  if (/venue|planner|dmc|preferred.?lodging|event.?executive/i.test(blob)) {
    return CUSTOMER_CONTACT_ROUTE_LABEL.NAMED_VENUE_PLANNER;
  }
  if (buyerEligible) return CUSTOMER_CONTACT_ROUTE_LABEL.NAMED_BUYER;
  if (/referral/i.test(blob)) return CUSTOMER_CONTACT_ROUTE_LABEL.NAMED_REFERRAL;
  if (/influencer|operations|workplace|routing|confirmation/i.test(blob)) {
    return CUSTOMER_CONTACT_ROUTE_LABEL.NAMED_OPERATIONAL;
  }
  if (isEventsSegment(opportunity)) {
    return CUSTOMER_CONTACT_ROUTE_LABEL.NAMED_REFERRAL;
  }
  return CUSTOMER_CONTACT_ROUTE_LABEL.NAMED_REFERRAL;
}

/**
 * Non-Events segments: infer buyer eligibility from title/role evidence.
 * Routing / confirmation / influencer titles are not automatic buyers.
 */
export function classifyGenericContactRole(contact = {}, opportunity = {}) {
  const blob = [
    contact.roleClass,
    contact.role,
    contact.title,
    contact.whyThisContact,
    contact.whyRelevant,
    contact.commercialRelevance,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();
  const explicit = contact.isNamedCommerciallyRelevant;
  if (explicit === false) {
    return {
      roleLabel: text(contact.contactClassificationLabel || contact.title) || null,
      buyerEligible: false,
      reason: "explicitly_not_commercially_relevant",
    };
  }
  if (
    /\b(routing|confirmation|event executive|coordinator|operations|workplace|influencer|referral)\b/i.test(
      blob,
    ) &&
    !/\b(buyer|procurement|hotel.?sourc|housing|travel.?manager|decision)\b/i.test(blob)
  ) {
    return {
      roleLabel: text(contact.contactClassificationLabel || contact.title) || null,
      buyerEligible: false,
      reason: "operational_or_routing_not_buyer",
    };
  }
  if (/\b(buyer|procurement|hotel.?sourc|housing|travel.?manager|meeting.?planner)\b/i.test(blob)) {
    return {
      roleLabel: text(contact.contactClassificationLabel || contact.title) || null,
      buyerEligible: true,
      reason: "buyer_or_sourcing_signal",
    };
  }
  return {
    roleLabel: text(contact.contactClassificationLabel || contact.roleClass || contact.title) || null,
    buyerEligible: explicit === true,
    reason: explicit === true ? "explicitly_relevant" : "buyer_unverified",
  };
}

/**
 * Map free-text / Exa roleClass → customer-facing Events role label.
 * Speakers/authors/attendees → influencer or unspecified — never Decision-Maker.
 */
export function classifyEventsContactRole(contact = {}, opportunity = {}) {
  const blob = [
    contact.roleClass,
    contact.role,
    contact.title,
    contact.whyThisContact,
    contact.commercialRelevance,
    contact.relationshipToOpportunity,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();

  if (!text(contact.name) || !looksLikePersonName(contact.name)) {
    return {
      roleLabel: EVENTS_CONTACT_ROLE_CLASS.UNSPECIFIED,
      buyerEligible: false,
      reason: "missing_or_non_person_name",
    };
  }

  if (
    /\b(speaker|keynote|panelist|author|attendee|delegate|participant|journalist|reporter)\b/i.test(
      blob,
    ) &&
    !/\b(organizer|planner|housing|sourcing|director of|meetings|events manager|pco)\b/i.test(
      blob,
    )
  ) {
    return {
      roleLabel: EVENTS_CONTACT_ROLE_CLASS.RELEVANT_INFLUENCER,
      buyerEligible: false,
      reason: "speaker_author_attendee_not_buyer",
    };
  }

  if (
    /\b(hotel.?sourc|housing|room.?block|accommodation|venue.?sourc|host.?hotel)\b/i.test(
      blob,
    )
  ) {
    return {
      roleLabel: EVENTS_CONTACT_ROLE_CLASS.HOTEL_SOURCING,
      buyerEligible: true,
      reason: "hotel_sourcing_role",
    };
  }
  if (/\b(meeting.?planner|congress.?manager|pco|destination.?manager)\b/i.test(blob)) {
    return {
      roleLabel: EVENTS_CONTACT_ROLE_CLASS.MEETING_PLANNER,
      buyerEligible: true,
      reason: "meeting_planner_role",
    };
  }
  if (
    /\b(organizer|event.?director|congress.?chair|executive.?director|secretary.?general)\b/i.test(
      blob,
    )
  ) {
    return {
      roleLabel: EVENTS_CONTACT_ROLE_CLASS.EVENT_ORGANIZER,
      buyerEligible: true,
      reason: "event_organizer_role",
    };
  }
  if (
    /\b(decision.?maker|procurement|travel.?manager|incentive|recognition.?trip|c-suite|ceo|coo|president)\b/i.test(
      blob,
    )
  ) {
    return {
      roleLabel: EVENTS_CONTACT_ROLE_CLASS.DECISION_MAKER,
      buyerEligible: true,
      reason: "decision_maker_role",
    };
  }
  if (/\b(referral|partner|sponsor.?liaison|influencer)\b/i.test(blob)) {
    return {
      roleLabel: EVENTS_CONTACT_ROLE_CLASS.REFERRAL,
      buyerEligible: false,
      reason: "referral_not_sourcing_owner",
    };
  }

  // Named with a professional title but no clear buyer signal → displayable influencer, not Ready buyer
  if (text(contact.title || contact.role)) {
    return {
      roleLabel: EVENTS_CONTACT_ROLE_CLASS.RELEVANT_INFLUENCER,
      buyerEligible: false,
      reason: "named_with_title_buyer_unverified",
    };
  }

  return {
    roleLabel: EVENTS_CONTACT_ROLE_CLASS.UNSPECIFIED,
    buyerEligible: false,
    reason: "insufficient_role_evidence",
  };
}

function pickWhoBlock(opportunity = {}) {
  return (
    opportunity.commercialIntelligenceBrief?.whoToContact ||
    opportunity.associationEventsGoldBrief?.whoToContact ||
    opportunity.salesBrief?.whoToContact ||
    null
  );
}

function normalizeContactPerson(raw = {}, opportunity = {}, opts = {}) {
  if (!raw || typeof raw !== "object") return null;
  const name = text(raw.name);
  if (!name || !looksLikePersonName(name)) return null;

  let email = text(raw.email || raw.workEmail || "");
  let orgEmail = text(raw.organizationEmail || raw.organizationContactEmail || "");
  if (email && isGenericOrganizationEmail(email)) {
    if (!orgEmail) orgEmail = email;
    email = "";
  }

  const profileUrl = text(
    raw.profileUrl || raw.linkedinUrl || raw.linkedInUrl || raw.sourceUrl || "",
  );
  const linkedInUrl = /linkedin\.com/i.test(profileUrl)
    ? profileUrl
    : text(raw.linkedinUrl || raw.linkedInUrl || "");

  const roleMeta = isEventsSegment(opportunity)
    ? classifyEventsContactRole(raw, opportunity)
    : classifyGenericContactRole(raw, opportunity);

  const org =
    text(raw.organization) ||
    text(opportunity.organizationName) ||
    text(opportunity.companyName) ||
    text(opportunity.displayCompanyName) ||
    null;

  const title = text(raw.title || raw.role || "");
  const roleDisplay =
    roleMeta.roleLabel === EVENTS_CONTACT_ROLE_CLASS.UNSPECIFIED
      ? null
      : roleMeta.roleLabel;
  // CIB / Exa often store relevance as `whyRelevant` — must not be dropped.
  const whyRaw = text(
    raw.whyRelevant ||
      raw.whyThisContact ||
      raw.whyRelevantToHotelDemand ||
      raw.commercialRelevance ||
      raw.relationshipToOpportunity ||
      "",
  );
  const why = dedupeAuthorityDisclaimer(
    whyRaw ||
      buildDefaultWhyRelevant({
        title,
        org,
        roleDisplay,
        opportunity,
        buyerEligible: roleMeta.buyerEligible === true,
        isEvents: isEventsSegment(opportunity),
      }),
  );

  const routeLabel = resolveCustomerContactRouteLabel({
    raw,
    roleMeta,
    title,
    buyerEligible: roleMeta.buyerEligible === true,
    opportunity,
  });

  return {
    name,
    title: title || null,
    role: title || null,
    organization: org,
    email: email || null,
    phone: text(raw.phone || raw.directPhone || "") || null,
    companyPhone: text(raw.companyPhone || raw.organizationPhone || "") || null,
    organizationEmail: orgEmail || null,
    organizationContactRoute: orgEmail || null,
    profileUrl: /^https?:/i.test(profileUrl) ? profileUrl : null,
    linkedinUrl: /^https?:/i.test(linkedInUrl) ? linkedInUrl : null,
    sourceUrl: text(raw.sourceUrl) || null,
    whyThisContact: why,
    whyRelevantToHotelDemand: why,
    whyRelevant: why,
    commercialContactPathLabel: routeLabel,
    contactClassificationLabel: roleMeta.roleLabel,
    roleClass: raw.roleClass || roleMeta.roleLabel,
    isNamedCommerciallyRelevant: roleMeta.buyerEligible === true,
    buyerEligible: roleMeta.buyerEligible === true,
    buyerEligibilityReason: roleMeta.reason,
    contactRelevanceLabel: roleMeta.buyerEligible
      ? "CONTACT_RELEVANT"
      : roleMeta.roleLabel === EVENTS_CONTACT_ROLE_CLASS.REFERRAL
        ? "REFERRAL"
        : "INFLUENCER",
    evidenceUrls: Array.isArray(raw.evidenceUrls)
      ? raw.evidenceUrls
      : raw.sourceUrl
        ? [raw.sourceUrl]
        : [],
    slotHint: opts.slot || null,
  };
}

/**
 * Hydrate primaryContact / secondaryContact / contactName from CIB whoToContact
 * and namedContacts when top-level contact fields are thin.
 * Does not invent people or channels.
 */
export function hydrateOpportunityContactsFromCib(opportunity = {}) {
  const o =
    opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const who = pickWhoBlock(o);
  const named = Array.isArray(o.namedContacts) ? o.namedContacts : [];

  // Prefer CIB primary (including buyer-demoted but display-eligible people).
  const primaryRaw =
    who?.primary ||
    (Array.isArray(who?.additional) ? who.additional[0] : null) ||
    named[0] ||
    o.primaryContact ||
    null;
  const secondaryRaw =
    who?.secondary ||
    (Array.isArray(who?.additional) ? who.additional[1] : null) ||
    named[1] ||
    o.secondaryContact ||
    null;

  const primary = normalizeContactPerson(primaryRaw, o, { slot: "PRIMARY" });
  let secondary = normalizeContactPerson(secondaryRaw, o, { slot: "SECONDARY" });
  if (
    primary &&
    secondary &&
    text(primary.name).toLowerCase() === text(secondary.name).toLowerCase()
  ) {
    secondary = null;
  }

  const next = { ...o };
  if (primary) {
    if (!looksLikePersonName(next.contactName)) next.contactName = primary.name;
    if (!next.contactRole) next.contactRole = primary.title;
    if (!next.whoPrimaryReason) next.whoPrimaryReason = primary.whyThisContact;
    next.primaryContact = {
      ...(next.primaryContact && typeof next.primaryContact === "object"
        ? next.primaryContact
        : {}),
      ...primary,
      // Prefer hydrated email/phone only when existing empty
      email: text(next.primaryContact?.email) || primary.email,
      phone: text(next.primaryContact?.phone) || primary.phone,
    };
    if (primary.email && !next.contactEmail) next.contactEmail = primary.email;
    if (primary.phone && !next.contactPhone) next.contactPhone = primary.phone;
    if (primary.organizationEmail && !next.organizationContactEmail) {
      next.organizationContactEmail = primary.organizationEmail;
    }
  }
  if (secondary) {
    next.secondaryContact = {
      ...(next.secondaryContact && typeof next.secondaryContact === "object"
        ? next.secondaryContact
        : {}),
      ...secondary,
      organizationRoutingAlternative:
        secondary.organizationEmail ||
        next.secondaryContact?.organizationRoutingAlternative ||
        null,
    };
  }

  next.unifiedWhoToContactHydration = {
    version: UNIFIED_WHO_TO_CONTACT_V1,
    hydratedPrimary: Boolean(primary),
    hydratedSecondary: Boolean(secondary),
    primaryBuyerEligible: primary?.buyerEligible === true,
    secondaryBuyerEligible: secondary?.buyerEligible === true,
  };
  return next;
}

/**
 * Apply shared gold-standard two-contact presentation for every GDI segment.
 * V2 defaults force=true so education/government/social/catering/etc. hydrate.
 */
export function applyUnifiedWhoToContactStandard(opportunity = {}, opts = {}) {
  const hydrated = hydrateOpportunityContactsFromCib(opportunity);
  const force = opts.force !== false; // V2: force unless explicitly disabled
  const withTwo = applyScalingTwoContactStandard(hydrated, { ...opts, force });
  const presentation = withTwo.scalingTwoContactPresentation;
  if (presentation?.primary?.customerFacingNamed) {
    const route =
      text(hydrated.primaryContact?.commercialContactPathLabel) ||
      text(presentation.primary.commercialContactPathLabel) ||
      null;
    withTwo.scalingTwoContactPresentation = {
      ...presentation,
      version: UNIVERSAL_WHO_TO_CONTACT_V2,
      primaryRouteTypeLabel: route,
      primary: {
        ...presentation.primary,
        commercialContactPathLabel:
          route || presentation.primary.commercialContactPathLabel || null,
        whyContact: dedupeAuthorityDisclaimer(presentation.primary.whyContact),
      },
      secondary: presentation.secondary
        ? {
            ...presentation.secondary,
            whyContact: dedupeAuthorityDisclaimer(
              presentation.secondary.whyContact,
            ),
          }
        : presentation.secondary,
    };
    withTwo.universalWhoToContactVersion = UNIVERSAL_WHO_TO_CONTACT_V2;
    withTwo.primaryRouteTypeLabel =
      withTwo.scalingTwoContactPresentation.primaryRouteTypeLabel;
  }
  return attachUniversalTileContactPreview(withTwo);
}

/**
 * Reconciliation row for reporting — does not mutate readiness.
 */
export function reconcileOpportunityContactSurface(opportunity = {}) {
  const applied = applyUnifiedWhoToContactStandard(opportunity);
  const p = applied.scalingTwoContactPresentation?.primary || null;
  const s = applied.scalingTwoContactPresentation?.secondary || null;
  const primaryBuyer =
    applied.primaryContact?.buyerEligible === true ||
    applied.primaryContact?.isNamedCommerciallyRelevant === true;
  const secondaryBuyer =
    applied.secondaryContact?.buyerEligible === true ||
    applied.secondaryContact?.isNamedCommerciallyRelevant === true;
  const referralOnly =
    p?.customerFacingNamed &&
    !primaryBuyer &&
    /referral|influencer/i.test(
      String(
        applied.primaryContact?.contactClassificationLabel ||
          applied.primaryContact?.contactRelevanceLabel ||
          "",
      ),
    );
  return {
    opportunityId: applied.id || applied.opportunityId || null,
    hotelId: applied.hotelId || null,
    title: text(applied.eventName || applied.companyName || applied.title),
    segment: isEventsSegment(applied) ? "events" : "scaling",
    hasTwoContactPresentation: Boolean(applied.scalingTwoContactPresentation?.primary),
    primaryName: p?.name || null,
    primaryNamed: p?.customerFacingNamed === true,
    secondaryName: s?.name || null,
    secondaryNamed: s?.customerFacingNamed === true,
    primaryBuyerEligible: primaryBuyer,
    secondaryBuyerEligible: secondaryBuyer,
    referralOnly: Boolean(referralOnly),
    hasLinkedIn: Boolean(p?.linkedInUrl || s?.linkedInUrl),
    hasPublicEmail: Boolean(p?.email || s?.email),
    hasPublicPhone: Boolean(p?.phone || s?.phone),
    surfeEligiblePrimary: p?.surfeEnrichmentEligible === true,
    coverage: applied.scalingContactCoverage || null,
    buyerResearchRequired:
      !primaryBuyer && !(p?.customerFacingNamed && primaryBuyer),
  };
}
