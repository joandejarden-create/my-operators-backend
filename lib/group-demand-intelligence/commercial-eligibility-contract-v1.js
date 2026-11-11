/**
 * GDI 2.0 shared commercial eligibility contract (Phase 2A).
 *
 * Composes existing Scaling + Association + prospect validators.
 * Does NOT replace them. Does NOT mutate opportunities.
 * Does NOT authorize publication or change customer visibility by itself.
 *
 * GDI is a proactive demand discovery platform: credible future hotel demand
 * may qualify before a confirmed booking requirement or formal RFP exists.
 *
 * Sales-ready requires:
 *   1. Credible organization / activity
 *   2. Evidence-grounded guestroom and/or local catering / meetings / banquet /
 *      private-dining / event thesis for the hotel
 *      (CONFIRMED_DEMAND or RELIABLE_INFERRED_DEMAND — not SPECULATIVE).
 *      Guestrooms are NOT mandatory (see GDI_MULTI_REVENUE_QUALIFICATION:
 *      GUESTROOMS_ONLY | EVENT_FNB_ONLY | COMBINED_GUESTROOMS_EVENT_FNB).
 *   3. ≥1 verified named DIRECT_BUYER or RELEVANT_COMMERCIAL_ENTRY_POINT
 *   4. Contact affiliation / commercial connection supported (unknown role ≠ auto-pass)
 *   5. Usable contact route (org route OK if labeled for named person)
 *   6. Traceable, consistent supporting sources
 *   7. Credible connection between potential demand and the specific hotel
 *
 * Contact philosophy (Scaling Companies FullE2E): prioritize the hotel-booking
 * decision-maker, but direct booking authority is NOT universally required.
 * Relevant commercial entry points may qualify when evidenced and defensible.
 *
 * Explicitly NOT required: confirmed room-night demand, confirmed group blocks,
 * published RFP, hotel booking announcement, active hotel sourcing confirmation,
 * purchase intent, direct hotel-booking authority, second named buyer/contact,
 * or person-direct email when a legitimate org route can reach the named person.
 *
 * SPECULATIVE_DEMAND must remain RESEARCH_CONTINUATION.
 */

import {
  evaluateScalingPublicationQualification,
  isCommerciallyRelevantScalingBuyer,
  evaluateRealisticHotelDemandThesis,
  detectInvalidBuyerContactAttribution,
  SCALING_PUBLICATION_STATUS,
} from "./scaling-publication-qualification-v1.js";
import {
  evaluateAssociationEventsPublicationQualification,
  ASSOCIATION_EVENTS_PUBLICATION_STATUS,
} from "./research/association-events-qualification-v1.js";
import {
  evaluateProspectPublicationEligibility,
  hasUsableContactRoute,
  classifyProspectUseCase,
  PROSPECT_USE_CASE,
} from "./prospect-publication-eligibility-v1.js";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";
import {
  classifyHotelCommercialRemit,
  REMIT_CLASS,
} from "./hotel-commercial-responsibility-v1.js";
import {
  DEMAND_THESIS_NOT_REQUIRED,
  DEMAND_THESIS_STANDARD,
  SALES_READY_REQUIRES,
  classifyCommercialDemandThesis,
} from "./commercial-demand-thesis-standard-v1.js";
import {
  classifyContactEligibility,
  buildContactEligibilityBriefFields,
  CONTACT_ELIGIBILITY_CLASS,
  CONTACT_ELIGIBILITY_NOT_REQUIRED,
} from "./contact-eligibility-standard-v1.js";
import { normalizeOpportunityForCommercialEligibilityEvaluation } from "./commercial-eligibility-field-normalization-v1.js";

export const COMMERCIAL_ELIGIBILITY_CONTRACT_V1 =
  "gdi-commercial-eligibility-contract-v1";

export const COMMERCIAL_ELIGIBILITY_STATUS = Object.freeze({
  SALES_READY: "SALES_READY",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  COMMERCIALLY_INELIGIBLE: "COMMERCIALLY_INELIGIBLE",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export const ELIGIBILITY_REASON = Object.freeze({
  CREDIBLE_ORG_ACTIVITY: "CREDIBLE_ORG_ACTIVITY",
  MISSING_CREDIBLE_ORG_ACTIVITY: "MISSING_CREDIBLE_ORG_ACTIVITY",
  REALISTIC_HOTEL_DEMAND: "REALISTIC_HOTEL_DEMAND",
  MISSING_HOTEL_DEMAND_THESIS: "MISSING_HOTEL_DEMAND_THESIS",
  NAMED_RELEVANT_BUYER: "NAMED_RELEVANT_BUYER",
  MISSING_NAMED_RELEVANT_BUYER: "MISSING_NAMED_RELEVANT_BUYER",
  BUYER_RESPONSIBILITY_SUPPORTED: "BUYER_RESPONSIBILITY_SUPPORTED",
  BUYER_RESPONSIBILITY_UNKNOWN: "BUYER_RESPONSIBILITY_UNKNOWN",
  BUYER_ROLE_IRRELEVANT: "BUYER_ROLE_IRRELEVANT",
  BUYER_AFFILIATION_MISMATCH: "BUYER_AFFILIATION_MISMATCH",
  USABLE_CONTACT_ROUTE: "USABLE_CONTACT_ROUTE",
  MISSING_CONTACT_ROUTE: "MISSING_CONTACT_ROUTE",
  SOURCES_TRACEABLE: "SOURCES_TRACEABLE",
  SOURCES_INSUFFICIENT: "SOURCES_INSUFFICIENT",
  SEGMENT_ENGINE_EXCLUSION: "SEGMENT_ENGINE_EXCLUSION",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const FUNCTIONAL_ORG_MAILBOX_RE =
  /^(info|contact|hello|press|pr|media|sales|support|admin|office|enquiries|inquiries|newbiz|newbusiness|hq|careers|recruiting|recruitment|team|general|customerservice|customer\.?service|help|noreply|no-reply|meetings?|events?|eventos?|congress|congresos?|convention|reservations?|reservas|booking|groups?|grupos?|catering|banquets?|hospitality|lodging|housing|hospedaje|alojamiento|reuniones?|tournament|showcase|collegeshowcase|registration|registrations|staytoplay|roomblock|hotelblock|grouphousing)@/i;

function isFunctionalOrgMailbox(email) {
  return FUNCTIONAL_ORG_MAILBOX_RE.test(text(email));
}

/**
 * Org route is explicitly allowed when it can reach the named buyer.
 * Does not invent person-direct attribution for functional mailboxes.
 */
export function resolveUsableContactRouteForEligibility(opportunity = {}, buyer = {}) {
  const base = hasUsableContactRoute(opportunity);
  const personEmail = text(
    buyer?.email ||
      opportunity.primaryContact?.email ||
      opportunity.contactEmail ||
      "",
  );
  const orgEmail = text(
    opportunity.organizationContactEmail ||
      opportunity.functionalContactEmail ||
      opportunity.companyEmail ||
      "",
  );
  const orgPhone = text(opportunity.companyPhone || opportunity.organizationPhone || "");
  const orgUrl = text(
    opportunity.organizationContactUrl ||
      opportunity.contactUrl ||
      opportunity.publicContactPath ||
      "",
  );

  if (personEmail.includes("@") && !isFunctionalOrgMailbox(personEmail)) {
    return {
      ok: true,
      reason: "person_direct_or_attributed_email",
      labeling: "person_or_org",
      surface: base.surface || null,
    };
  }
  if (personEmail.includes("@") && isFunctionalOrgMailbox(personEmail)) {
    return {
      ok: true,
      reason: "organizational_functional_mailbox_ask_for_named_person",
      labeling: "must_not_present_as_person_direct",
      surface: base.surface || null,
    };
  }
  if (orgEmail.includes("@")) {
    return {
      ok: true,
      reason: "organizational_route_ask_for_named_person",
      labeling: "must_not_present_as_person_direct",
      surface: base.surface || null,
    };
  }
  if (orgPhone || /^https?:\/\//i.test(orgUrl)) {
    // Prefer functional contact URLs; also accept source-established routes that
    // field-normalization lifted from evidence (never narrative-only emails).
    const sourceEstablished =
      opportunity.commercialEligibilityFieldNormalization?.routeDerivation
        ?.reason === "source_establishes_contact_route" &&
      /^https?:\/\//i.test(orgUrl) &&
      !/linkedin\.com\/in\//i.test(orgUrl);
    if (base?.ok === true || sourceEstablished || orgPhone) {
      return {
        ok: true,
        reason: sourceEstablished
          ? "source_established_organization_contact_url"
          : "organizational_phone_or_contact_url",
        labeling: "must_not_present_as_person_direct",
        surface: base.surface || null,
      };
    }
  }
  if (base?.ok === true) {
    return { ok: true, reason: base.reason || "usable_contact_route", surface: base.surface };
  }
  return { ok: false, reason: base?.reason || "no_usable_route", surface: base?.surface || null };
}

/**
 * Soften Scaling attribution when the only "mismatch" is a functional org mailbox
 * used as an ask-for route for a named buyer (allowed by contract).
 */
export function normalizeAttributionForSharedEligibility(attribution = {}, opportunity = {}) {
  if (!attribution?.invalid) return { ...attribution, invalid: false };
  const email = text(
    attribution.email ||
      opportunity.contactEmail ||
      opportunity.organizationContactEmail ||
      opportunity.functionalContactEmail ||
      "",
  );
  const orgRouteCodes = new Set([
    "EMAIL_LOCAL_PART_DOES_NOT_MATCH_NAMED_PERSON",
    "ORG_ROUTE_EMAIL_BELONGS_TO_DIFFERENT_IDENTITY",
  ]);
  if (orgRouteCodes.has(attribution.code) && isFunctionalOrgMailbox(email)) {
    return {
      invalid: false,
      code: "ORGANIZATIONAL_FUNCTIONAL_MAILBOX",
      name: attribution.name || null,
      email,
      labeling: "must_not_present_as_person_direct",
      note: "Functional org mailbox is a usable ask-for route for the named buyer — not person-direct attribution.",
    };
  }
  return attribution;
}

function detectSegmentFamily(opportunity = {}, opts = {}) {
  if (opts.segmentFamily) return String(opts.segmentFamily);
  const useCase = classifyProspectUseCase(opportunity);
  if (
    useCase === PROSPECT_USE_CASE.SCALING_CORPORATE ||
    opportunity.researchSegment === "scaling_companies" ||
    /scaling/i.test(String(opportunity.segment || ""))
  ) {
    return "scaling_companies";
  }
  if (
    opportunity.researchSegment === "associations_government" ||
    opportunity.associationEvents === true ||
    opportunity.eventName ||
    /association|event|congress|conference/i.test(
      String(opportunity.segment || opportunity.opportunityType || ""),
    )
  ) {
    return "associations_events";
  }
  return "generic";
}

/**
 * Deterministic buyer role extraction across Scaling / Association / DTO shapes.
 * Fixes false UNKNOWN when role lives on contactRole / responsibility fields.
 */
export function extractBuyerRoleFromOpportunity(person = {}, opportunity = {}) {
  const candidates = [
    person.role,
    person.title,
    person.relevantResponsibility,
    person.jobTitle,
    opportunity.primaryContact?.role,
    opportunity.primaryContact?.title,
    opportunity.contactRole,
    opportunity.contactResponsibility,
    opportunity.buyerRole,
    opportunity.primaryContactRole,
    opportunity.salesBrief?.primaryContactRole,
    opportunity.salesBrief?.buyerRole,
    opportunity.commercialIntelligenceBrief?.primaryBuyerRole,
  ];
  for (const c of candidates) {
    const t = text(c);
    if (t) return t;
  }
  return "";
}

/**
 * Unknown commercial responsibility must NOT auto-satisfy named-buyer.
 * Empty / unspecified role → unresolved (research continuation), not sales-ready.
 *
 * Shared sales-ready contact gate (Scaling Companies philosophy restored):
 *   DIRECT_BUYER or RELEVANT_COMMERCIAL_ENTRY_POINT may satisfy responsibility.
 *   Bare leadership without evidenced demand-activity connection does not.
 *   Speakers / marketing / recruiting / unrelated roles do not.
 * Scaling soft-pass (isCommerciallyRelevantScalingBuyer) remains for legacy
 * hotelFacingEligible paths — this function is the shared sales-ready gate.
 */
export function evaluateBuyerCommercialResponsibility(person = {}, opportunity = {}, opts = {}) {
  const name = text(person.name || person.person || "");
  if (!name || !looksLikePersonName(name)) {
    return {
      ok: false,
      code: ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER,
      reason: "not_a_person_name",
    };
  }

  const role = extractBuyerRoleFromOpportunity(person, opportunity);
  const personWithRole = role && !text(person.role || person.title)
    ? { ...person, role, title: person.title || role }
    : { ...person, role: text(person.role || person.title) || role };

  const scalingEv = isCommerciallyRelevantScalingBuyer(personWithRole, opportunity);
  if (!scalingEv.ok) {
    if (scalingEv.reason === "employer_affiliation_mismatch") {
      return {
        ok: false,
        code: ELIGIBILITY_REASON.BUYER_AFFILIATION_MISMATCH,
        reason: scalingEv.reason,
        detail: scalingEv,
      };
    }
    if (scalingEv.reason === "irrelevant_role_media_pr_ir") {
      return {
        ok: false,
        code: ELIGIBILITY_REASON.BUYER_ROLE_IRRELEVANT,
        reason: scalingEv.reason,
        detail: scalingEv,
      };
    }
    return {
      ok: false,
      code: ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER,
      reason: scalingEv.reason,
      detail: scalingEv,
    };
  }

  // Shared contract tightens Scaling soft-pass for empty role.
  if (
    scalingEv.reason === "named_person_role_unspecified_qualify_on_outreach" ||
    !role
  ) {
    return {
      ok: false,
      code: ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN,
      reason: "named_person_commercial_responsibility_unresolved",
      note:
        "Named person identified but commercial responsibility not yet supported — research continuation, not sales-ready.",
      detail: scalingEv,
      extractedRole: role || null,
    };
  }

  // Contact eligibility standard (default on): DIRECT_BUYER or ENTRY_POINT.
  // opts.hotelCommercialAlignment=false restores legacy Scaling soft-pass only.
  const enforceAlignment = opts.hotelCommercialAlignment !== false;
  const contactEligibility = classifyContactEligibility(personWithRole, opportunity, opts);
  const briefFields = buildContactEligibilityBriefFields(contactEligibility);

  if (enforceAlignment) {
    if (!contactEligibility.salesReadyContactOk) {
      const remit = classifyHotelCommercialRemit(role);
      let code = ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN;
      if (
        contactEligibility.class === CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_IRRELEVANT ||
        contactEligibility.class === CONTACT_ELIGIBILITY_CLASS.UNRELATED_CONTACT ||
        remit.class === REMIT_CLASS.NON_HOTEL_COMMERCIAL
      ) {
        code = ELIGIBILITY_REASON.BUYER_ROLE_IRRELEVANT;
      } else if (
        contactEligibility.class ===
          CONTACT_ELIGIBILITY_CLASS.INELIGIBLE_AFFILIATION_UNSUPPORTED ||
        contactEligibility.reason === "employer_affiliation_mismatch"
      ) {
        code = ELIGIBILITY_REASON.BUYER_AFFILIATION_MISMATCH;
      } else if (contactEligibility.class === CONTACT_ELIGIBILITY_CLASS.RESEARCH_CANDIDATE) {
        code = ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN;
      }
      return {
        ok: false,
        code,
        reason: contactEligibility.reason,
        note: contactEligibility.label,
        detail: { scalingEv, contactEligibility, remit },
        extractedRole: role,
        hotelCommercialAlignment: true,
        contactEligibility,
        ...briefFields,
      };
    }

    return {
      ok: true,
      code: ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_SUPPORTED,
      reason: contactEligibility.reason,
      detail: { scalingEv, contactEligibility },
      extractedRole: role,
      hotelCommercialAlignment: true,
      contactEligibility,
      ...briefFields,
    };
  }

  return {
    ok: true,
    code: ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_SUPPORTED,
    reason: scalingEv.reason,
    detail: { scalingEv, contactEligibility },
    extractedRole: role,
    hotelCommercialAlignment: false,
    contactEligibility,
    ...briefFields,
  };
}

export function evaluateSourceTraceability(opportunity = {}) {
  const sources = Array.isArray(opportunity.sources) ? opportunity.sources : [];
  const evidence = Array.isArray(opportunity.evidence) ? opportunity.evidence : [];
  const urls = [
    ...sources.map((s) => text(s?.url || s)),
    ...evidence.map((e) => text(e?.url || e?.sourceUrl || "")),
    text(opportunity.officialSource),
    text(opportunity.sourceUrl),
    text(opportunity.primarySourceUrl),
  ].filter((u) => /^https?:\/\//i.test(u));

  const quotes = [
    text(opportunity.evidenceQuote),
    ...evidence.map((e) => text(e?.quote || e?.evidenceQuote || "")),
  ].filter(Boolean);

  const ok = urls.length > 0 || quotes.length > 0;
  return {
    ok,
    code: ok ? ELIGIBILITY_REASON.SOURCES_TRACEABLE : ELIGIBILITY_REASON.SOURCES_INSUFFICIENT,
    urlCount: urls.length,
    quoteCount: quotes.length,
    sampleUrls: urls.slice(0, 3),
  };
}

function buildGaps(gates, responsibility = null) {
  const gaps = [];
  if (!gates.credibleOrganizationActivity) gaps.push(ELIGIBILITY_REASON.MISSING_CREDIBLE_ORG_ACTIVITY);
  if (!gates.realisticHotelDemand) gaps.push(ELIGIBILITY_REASON.MISSING_HOTEL_DEMAND_THESIS);
  if (!gates.namedRelevantBuyer) gaps.push(ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER);
  if (!gates.buyerResponsibilitySupported) {
    if (!gaps.includes(ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER)) {
      const code = responsibility?.code;
      if (
        code === ELIGIBILITY_REASON.BUYER_ROLE_IRRELEVANT ||
        code === ELIGIBILITY_REASON.BUYER_AFFILIATION_MISMATCH ||
        code === ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN
      ) {
        gaps.push(code);
      } else {
        gaps.push(ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN);
      }
    }
  }
  if (!gates.usableContactRoute) gaps.push(ELIGIBILITY_REASON.MISSING_CONTACT_ROUTE);
  if (!gates.sourcesTraceable) gaps.push(ELIGIBILITY_REASON.SOURCES_INSUFFICIENT);
  return gaps;
}

function nextActionsForGaps(gaps = []) {
  const actions = [];
  for (const g of gaps) {
    if (g === ELIGIBILITY_REASON.MISSING_CREDIBLE_ORG_ACTIVITY) {
      actions.push("Continue organization/activity verification; retain opportunity.");
    } else if (g === ELIGIBILITY_REASON.MISSING_HOTEL_DEMAND_THESIS) {
      actions.push("Strengthen evidence-grounded guestroom/meetings/catering thesis for this hotel.");
    } else if (
      g === ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER ||
      g === ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_UNKNOWN
    ) {
      actions.push(
        "Continue named-buyer investigation until a commercially relevant role/affiliation is supported.",
      );
    } else if (g === ELIGIBILITY_REASON.MISSING_CONTACT_ROUTE) {
      actions.push(
        "Recover a usable person-direct or organizational ask-for route for the named buyer.",
      );
    } else if (g === ELIGIBILITY_REASON.SOURCES_INSUFFICIENT) {
      actions.push("Attach traceable source URLs or evidence quotes; do not invent claims.");
    }
  }
  if (!actions.length) {
    actions.push("Eligible under shared commercial contract — publication still requires separate authorization.");
  }
  return [...new Set(actions)];
}

/**
 * Universal commercial eligibility evaluation.
 * @param {object} opportunity
 * @param {{ hotel?: object, segmentFamily?: string, requireScalingSegment?: boolean }} [opts]
 */
export function evaluateSharedCommercialEligibility(opportunity = {}, opts = {}) {
  const hotel = opts.hotel || {};
  // Field recognition only — maps equivalent evidence into canonical evaluator fields.
  // Does not invent facts or weaken Sales Ready criteria.
  const normalized = normalizeOpportunityForCommercialEligibilityEvaluation(
    opportunity,
    opts,
  );
  const bag = normalized.opportunity || opportunity;
  const segmentFamily = detectSegmentFamily(bag, opts);
  const reasons = [];
  const evidenceRefs = [];
  let engineResult = null;

  if (segmentFamily === "scaling_companies") {
    engineResult = evaluateScalingPublicationQualification(bag, {
      requireScalingSegment: opts.requireScalingSegment,
    });
  } else if (segmentFamily === "associations_events") {
    engineResult = evaluateAssociationEventsPublicationQualification(
      bag,
      hotel,
    );
  } else {
    // Generic path: reuse prospect eligibility + Scaling demand/buyer helpers.
    engineResult = {
      status: "GENERIC",
      publicationQualified: false,
      researchContinuation: true,
      gates: {},
      prospect: evaluateProspectPublicationEligibility(bag, opts),
    };
  }

  const demand =
    segmentFamily === "associations_events"
      ? {
          ok: engineResult?.gates?.hotelDemandThesis === true,
          reason: engineResult?.demandThesis
            ? "association_demand_thesis"
            : "missing_association_demand",
          thesis: engineResult?.demandThesis || null,
        }
      : evaluateRealisticHotelDemandThesis(bag);

  // Commercial thesis standard annotation. Sales-ready demand gate of record remains
  // evaluateRealisticHotelDemandThesis / association hotelDemandThesis (proactive:
  // CONFIRMED or RELIABLE_INFERRED — never requires RFP / confirmed room nights).
  const demandThesis = classifyCommercialDemandThesis(bag, opts.extraction || {});

  const sources = evaluateSourceTraceability(bag);
  const rawAttribution = detectInvalidBuyerContactAttribution(bag);
  const attribution = normalizeAttributionForSharedEligibility(
    rawAttribution,
    bag,
  );

  let buyerPerson =
    engineResult?.relevantBuyer ||
    (Array.isArray(engineResult?.namedBuyers) && engineResult.namedBuyers[0]) ||
    null;
  if (!buyerPerson) {
    const name = text(
      bag.primaryContact?.name ||
        bag.contactName ||
        bag.primaryContactName ||
        "",
    );
    if (name) {
      buyerPerson = {
        name,
        role:
          bag.primaryContact?.role ||
          bag.primaryContact?.title ||
          bag.contactResponsibility ||
          bag.buyerRole ||
          "",
        title:
          bag.primaryContact?.title ||
          bag.contactResponsibility ||
          "",
      };
    }
  }
  // Prefer the richest job-title / remit label across engine + primaryContact shapes.
  // Association namedBuyers previously kept short labels like "Meetings / housing"
  // and discarded "Senior Manager, Events and Member Services".
  if (buyerPerson) {
    const extractedRole = extractBuyerRoleFromOpportunity(buyerPerson, bag);
    const candidates = [
      buyerPerson.role,
      buyerPerson.title,
      bag.primaryContact?.role,
      bag.primaryContact?.title,
      bag.contactRole,
      extractedRole,
    ]
      .map(text)
      .filter(Boolean);
    const scored = candidates
      .map((role) => {
        let score = role.length >= 24 ? 1 : 0;
        if (/\b(director|manager|coordinator|planner|officer|head|lead|vp|vice president|gerente|chief)\b/i.test(role)) {
          score += 3;
        }
        if (/\b(meeting|meetings|event|events|housing|hotel|lodging|conference|procurement|travel)\b/i.test(role)) {
          score += 2;
        }
        // Penalize OCR/scraped residue: "Tournament Director, Dave Bucciero, at".
        if (/,\s*[A-Z][a-z]+/.test(role) || /\bat\s*$/i.test(role) || /@/.test(role)) {
          score -= 4;
        }
        return { role, score };
      })
      .sort((a, b) => b.score - a.score || b.role.length - a.role.length);
    const bestRole = scored[0]?.role || extractedRole || "";
    if (bestRole) {
      buyerPerson = {
        ...buyerPerson,
        role: bestRole,
        title: bestRole,
      };
    }
  }

  const responsibility = buyerPerson
    ? evaluateBuyerCommercialResponsibility(buyerPerson, bag, {
        hotelCommercialAlignment: opts.hotelCommercialAlignment,
      })
    : {
        ok: false,
        code: ELIGIBILITY_REASON.MISSING_NAMED_RELEVANT_BUYER,
        reason: "no_named_buyer",
      };

  const route = resolveUsableContactRouteForEligibility(bag, buyerPerson || {});

  const credibleActivity =
    segmentFamily === "scaling_companies"
      ? engineResult?.gates?.verifiedCompanyActivity === true
      : segmentFamily === "associations_events"
        ? engineResult?.gates?.verifiedEventOrOrganizer === true
        : Boolean(
            text(bag.organizationName) &&
              (text(bag.whatIsHappening) ||
                text(bag.activityHeadline) ||
                text(bag.eventName) ||
                sources.ok),
          );

  // Engine exclusion (wrong fit / duplicate) → commercially ineligible, not continuation
  const engineExcluded =
    engineResult?.status === ASSOCIATION_EVENTS_PUBLICATION_STATUS.EXCLUDE_WRONG_FIT ||
    engineResult?.status === ASSOCIATION_EVENTS_PUBLICATION_STATUS.EXCLUDE_DUPLICATE ||
    engineResult?.status === SCALING_PUBLICATION_STATUS.COMMERCIALLY_INELIGIBLE;

  const gates = {
    credibleOrganizationActivity: credibleActivity,
    realisticHotelDemand: demand.ok === true,
    namedRelevantBuyer: Boolean(buyerPerson && looksLikePersonName(buyerPerson.name)),
    buyerResponsibilitySupported: responsibility.ok === true && !attribution.invalid,
    usableContactRoute: route.ok === true,
    sourcesTraceable: sources.ok === true,
  };

  if (gates.credibleOrganizationActivity) {
    reasons.push(ELIGIBILITY_REASON.CREDIBLE_ORG_ACTIVITY);
  }
  if (gates.realisticHotelDemand) {
    reasons.push(ELIGIBILITY_REASON.REALISTIC_HOTEL_DEMAND);
    if (demand.reason) evidenceRefs.push({ kind: "demand", ref: demand.reason });
  }
  if (gates.namedRelevantBuyer && gates.buyerResponsibilitySupported) {
    reasons.push(ELIGIBILITY_REASON.NAMED_RELEVANT_BUYER);
    reasons.push(ELIGIBILITY_REASON.BUYER_RESPONSIBILITY_SUPPORTED);
  } else if (responsibility.code) {
    reasons.push(responsibility.code);
  }
  if (gates.usableContactRoute) {
    reasons.push(ELIGIBILITY_REASON.USABLE_CONTACT_ROUTE);
  }
  if (gates.sourcesTraceable) {
    reasons.push(ELIGIBILITY_REASON.SOURCES_TRACEABLE);
    evidenceRefs.push({ kind: "sources", urls: sources.sampleUrls });
  }

  const gaps = engineExcluded
    ? [ELIGIBILITY_REASON.SEGMENT_ENGINE_EXCLUSION, ...(engineResult?.reasons || [])]
    : buildGaps(gates, responsibility);

  if (attribution.invalid) {
    gaps.push("INVALID_BUYER_CONTACT_ATTRIBUTION");
  }

  const thesisBlocksSalesReady = demandThesis.mustRemainResearchContinuation === true;
  const salesReady =
    !thesisBlocksSalesReady &&
    !engineExcluded &&
    gaps.length === 0 &&
    gates.credibleOrganizationActivity &&
    gates.realisticHotelDemand &&
    gates.namedRelevantBuyer &&
    gates.buyerResponsibilitySupported &&
    gates.usableContactRoute &&
    gates.sourcesTraceable;

  let status = COMMERCIAL_ELIGIBILITY_STATUS.RESEARCH_CONTINUATION;
  if (salesReady) status = COMMERCIAL_ELIGIBILITY_STATUS.SALES_READY;
  else if (engineExcluded) status = COMMERCIAL_ELIGIBILITY_STATUS.COMMERCIALLY_INELIGIBLE;
  else if (
    !gates.credibleOrganizationActivity &&
    !gates.realisticHotelDemand &&
    !gates.namedRelevantBuyer &&
    !text(bag.organizationName)
  ) {
    status = COMMERCIAL_ELIGIBILITY_STATUS.COMMERCIALLY_INELIGIBLE;
  }

  const unresolvedResearchGaps = gaps.filter(
    (g) =>
      g !== ELIGIBILITY_REASON.SEGMENT_ENGINE_EXCLUSION &&
      status === COMMERCIAL_ELIGIBILITY_STATUS.RESEARCH_CONTINUATION,
  );

  return {
    version: COMMERCIAL_ELIGIBILITY_CONTRACT_V1,
    segmentFamily,
    status,
    commercialEligible: salesReady,
    salesReady,
    fieldNormalization: {
      applied: normalized.applied === true,
      patches: normalized.patches || {},
      notes: normalized.notes || [],
    },
    researchContinuation:
      status === COMMERCIAL_ELIGIBILITY_STATUS.RESEARCH_CONTINUATION,
    preserveOpportunity: true,
    doNotSilentWithdraw: true,
    doNotTreatAsExhausted:
      status === COMMERCIAL_ELIGIBILITY_STATUS.RESEARCH_CONTINUATION,
    gates,
    gaps,
    unresolvedResearchGaps,
    reasons: [...new Set(reasons)],
    evidenceRefs,
    nextActions: nextActionsForGaps(gaps),
    notRequired: [
      ...new Set([
        ...DEMAND_THESIS_NOT_REQUIRED,
        ...CONTACT_ELIGIBILITY_NOT_REQUIRED,
        "confirmed_room_night_volume",
        "immediate_rfp",
      ]),
    ],
    salesReadyRequires: [
      ...SALES_READY_REQUIRES.filter(
        (r) => r !== "verified_named_commercially_relevant_buyer",
      ),
      "verified_named_direct_buyer_or_relevant_commercial_entry_point",
      "credible_approach_or_reach_path_for_named_contact",
    ],
    buyer: {
      person: buyerPerson,
      responsibility,
      attribution,
      contactEligibility: responsibility.contactEligibility || null,
      contactEligibilityClass: responsibility.contactEligibilityClass || null,
      contactTypeForBrief: responsibility.contactTypeForBrief || null,
      isDirectBuyer: responsibility.isDirectBuyer === true,
      isRelevantCommercialEntryPoint:
        responsibility.isRelevantCommercialEntryPoint === true,
    },
    demand: {
      ...demand,
      thesisStandard: demandThesis.thesisStandard,
      mayQualifySalesReadyDemandGate: demandThesis.mayQualifySalesReadyDemandGate,
      mustRemainResearchContinuation: demandThesis.mustRemainResearchContinuation,
    },
    demandThesisStandard: demandThesis,
    route,
    sources,
    engineResult,
    note:
      "Shared GDI 2.0 commercial eligibility. CONFIRMED and RELIABLE_INFERRED demand may qualify; SPECULATIVE stays in research continuation. Visibility and publication authorization are separate decisions.",
  };
}
