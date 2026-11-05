/**
 * Hotel-facing GDI sales-prospect eligibility (integration / publication prep).
 *
 * Separates the intended hotel sales-prospect standard from strict Ready/ACTIONABLE.
 * Does NOT lower isGdiCustomerOpportunityReady / maturity ACTIONABLE.
 *
 * Usable contact route (minimum for hotel-facing action list):
 *   - named or functional email, OR
 *   - relevant telephone, OR
 *   - specific contact-form / functional contact URL, OR
 *   - documented intermediary with reachable contact
 * Homepage-only / vague “meetings team” / identify-buyer alone → fail.
 *
 * Named person + direct phone are enrichment targets, not absolute blockers
 * when a relevant functional route exists.
 */

import { classifyBuyerContactPath } from "./buyer-contact-path-taxonomy-v1.js";
import { parseCrediblePhone } from "./evidence-contact-extraction-v1.js";
import { isResearchLayerPublishableDisposition } from "./workflow/candidate-dispositions-load-v1.js";

export const PROSPECT_ELIGIBILITY = Object.freeze({
  HOTEL_FACING: "HOTEL_FACING",
  ENRICHMENT_QUEUE: "ENRICHMENT_QUEUE",
  NOT_A_PROSPECT: "NOT_A_PROSPECT",
});

export const CONTACT_COMPLETENESS_GROUP = Object.freeze({
  PERSON_METHOD_PHONE: 1,
  PERSON_METHOD_NO_PHONE: 2,
  FUNCTIONAL_ORG_ONLY: 3,
  NO_USABLE_ROUTE: 4,
});

const HOMEPAGE_ONLY_RE =
  /^https?:\/\/[^/]+\/?(?:en\/|es\/|de\/)?(?:home\/?)?(?:index\.(?:html?|php))?$/i;

/** Discovery / about pages — not a usable hotel-facing contact route by themselves. */
const DISCOVERY_ONLY_URL_RE =
  /linkedin\.com\/(?:company|school)\//i;

const CAREERS_OR_ABOUT_URL_RE =
  /\/(?:careers?|jobs?|about(?:-us)?|our-story|team)(?:\/|$|\?|#)/i;

// Exact path segments only — `/event-tech-…` slug pages are listings, not sales desks.
// Plurals (accommodations) and sponsors/exhibitors contact pages are functional routes.
const FUNCTIONAL_CONTACT_PATH_RE =
  /\/(?:contact(?:-us)?|contacts|housing|hotel|accommodations?|meetings?|reservations?|exhibit(?:ors?)?|sponsors?(?:-exhibitors?)?|rfp|planner|plan-an-event|plan-your-trip|general|(?:wedding-)?request-form|enquir(?:y|ies)|inquir(?:y|ies))(?:\/|$|\?|#)|\/events?(?:\/|$|\?|#)/i;

/** Venue calendars / fair landings — research evidence, not sales desks. */
const EVENT_LISTING_CONTACT_URL_RE =
  /\/(whats-on|whats_on|calendar|things-to-do|fairs?\/)/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isDiscoveryOnlyUrl(url) {
  const u = text(url);
  if (!u || !/^https?:\/\//i.test(u)) return false;
  if (DISCOVERY_ONLY_URL_RE.test(u)) return true;
  if (CAREERS_OR_ABOUT_URL_RE.test(u) && !FUNCTIONAL_CONTACT_PATH_RE.test(u)) return true;
  return false;
}

function isFunctionalContactUrl(url) {
  const u = text(url);
  if (!u || !/^https?:\/\//i.test(u) || HOMEPAGE_ONLY_RE.test(u)) return false;
  if (isDiscoveryOnlyUrl(u)) return false;
  // ExCeL / Olympia what's-on and fair detail pages are listings, not org sales routes.
  if (EVENT_LISTING_CONTACT_URL_RE.test(u)) return false;
  return (
    /(?:onpeak|passkey|housing\s*bureau|official\s*housing)/i.test(u) ||
    FUNCTIONAL_CONTACT_PATH_RE.test(u) ||
    /[?&](?:type|page|form|view)=(?:contact|inquiry|enquiry|rfp|events?)/i.test(u)
  );
}

/**
 * Prefer functional contact-form URLs over discovery sources (LinkedIn company,
 * careers, about). contactOfficialUrl / contactSourceUrl are often discovery
 * provenance and must not shadow an established organization contact route.
 */
export function pickPreferredContactUrl(candidates = []) {
  const urls = []
    .concat(candidates || [])
    .map((u) => text(u))
    .filter((u) => /^https?:\/\//i.test(u));
  const functional = urls.filter((u) => isFunctionalContactUrl(u));
  if (functional.length) return functional[0];
  const nonDiscovery = urls.filter(
    (u) => !isDiscoveryOnlyUrl(u) && !HOMEPAGE_ONLY_RE.test(u)
  );
  if (nonDiscovery.length) return nonDiscovery[0];
  return urls[0] || "";
}

function isEmail(v) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text(v));
}

function isPhone(v) {
  return Boolean(parseCrediblePhone(v));
}

function isPersonName(v) {
  const n = text(v);
  if (!n || /^not\s*established$/i.test(n) || n === "UNKNOWN") return false;
  if (/^(staff|team|office|desk|info|contact)\b/i.test(n)) return false;
  return n.split(/\s+/).length >= 2 || /^[A-Z][a-z]+ [A-Z]/.test(n);
}

/**
 * Extract contact fields from opportunity bag or hotel-review contact package.
 */
export function extractContactSurface(opp = {}) {
  const pkg = opp.contactPackage || {};
  // Supervisor / E2D research-memory stores routes under nested `contact`.
  // Without reading it, usable org emails/forms falsely fail eligibility.
  const nested =
    opp.contact && typeof opp.contact === "object" ? opp.contact : {};
  const nestedIntermediary =
    nested.intermediary && typeof nested.intermediary === "object"
      ? nested.intermediary
      : null;
  const person =
    text(pkg.person) ||
    text(opp.primaryContact?.name) ||
    text(opp.primaryContactName) ||
    text(opp.contactName) ||
    text(opp.namedBuyer) ||
    text(nested.personName) ||
    text(nested.name);
  const title =
    text(pkg.title) ||
    text(opp.primaryContactRole) ||
    text(opp.primaryContact?.role) ||
    text(opp.buyerRole) ||
    text(nested.role) ||
    text(nested.title);
  // buyerFunctionHint is a diagnostic function enum — never a person title.
  const email =
    text(pkg.contactMethod).match(/^[^\s@]+@[^\s@]+/)?.[0] ||
    text(opp.primaryContact?.email) ||
    text(opp.primaryContactEmail) ||
    text(opp.functionalContactEmail) ||
    text(opp.contactEmail) ||
    text(nested.businessEmail) ||
    text(nested.email);
  const phone =
    text(pkg.businessPhone) ||
    text(opp.primaryContact?.phone) ||
    text(opp.primaryContactPhone) ||
    text(opp.contactPhone) ||
    text(nested.businessPhone) ||
    text(nested.phone);
  const phoneType = text(
    pkg.phoneType ||
      opp.primaryContactPhoneType ||
      opp.contactPhoneType ||
      nested.phoneType
  );
  const methodRaw = text(pkg.contactMethod || opp.contactMethod);
  const contactUrl = pickPreferredContactUrl([
    pkg.contactPageUrl,
    opp.publicContactPath,
    opp.organizationContactUrl,
    opp.primaryContact?.organizationContactRoute,
    opp.contactChannelSeparation?.organizationRoute,
    opp.officialContactPath,
    opp.contactUrl,
    nested.contactFormUrl,
    nested.organizationRouteUrl,
    nestedIntermediary?.url,
    // Discovery provenance last — LinkedIn company / careers must not win
    opp.contactOfficialUrl,
    opp.contactSourceUrl,
    /^https?:\/\//i.test(methodRaw) ? methodRaw : "",
  ]);
  const methodType = text(pkg.contactMethodType || opp.contactMethodType);
  const intermediary =
    pkg.bookingIntermediary && typeof pkg.bookingIntermediary === "object"
      ? pkg.bookingIntermediary
      : nestedIntermediary;
  const intermediaryContact =
    text(intermediary?.contact) ||
    text(intermediary?.email) ||
    (typeof pkg.bookingIntermediary === "string" &&
    isEmail(pkg.bookingIntermediary)
      ? pkg.bookingIntermediary
      : "");
  const intermediaryPhone = text(intermediary?.phone);

  return {
    person,
    title,
    email,
    phone: /^not\s*established$/i.test(phone) ? "" : phone,
    phoneType,
    contactUrl,
    methodType,
    intermediaryContact,
    intermediaryPhone,
    methodRaw,
  };
}

/**
 * Usable contact route for hotel-facing action list (intended standard).
 */
export function hasUsableContactRoute(opp = {}) {
  const c = extractContactSurface(opp);
  const method = text(c.methodRaw);

  if (/^not\s*established$/i.test(method) && !c.email && !c.phone && !c.contactUrl) {
    return { ok: false, reason: "not_established", surface: c };
  }

  if (isEmail(c.email) || isEmail(method)) {
    return { ok: true, reason: "email", surface: c };
  }
  if (isPhone(c.phone) || isPhone(method)) {
    return { ok: true, reason: "phone", surface: c };
  }
  if (isEmail(c.intermediaryContact) || isPhone(c.intermediaryPhone)) {
    return { ok: true, reason: "intermediary", surface: c };
  }
  const urlCandidate = c.contactUrl || (/^https?:\/\//i.test(method) ? method : "");
  if (isFunctionalContactUrl(urlCandidate)) {
    return { ok: true, reason: "functional_contact_url", surface: c };
  }
  if (/contact\s*form|mailto:|@/.test(method) && !/homepage|home\s*page|meetings?\s*team/i.test(method)) {
    if (isEmail(method) || /form/i.test(method)) {
      return { ok: true, reason: "method_string", surface: c };
    }
  }

  // Homepage / vague instruction alone
  if (
    !c.email &&
    !c.phone &&
    (HOMEPAGE_ONLY_RE.test(c.contactUrl) ||
      /homepage|meetings?\s*team|identify\s+the\s+buyer|contact\s+the\s+team/i.test(method || c.contactUrl))
  ) {
    return { ok: false, reason: "homepage_or_vague_only", surface: c };
  }

  return { ok: false, reason: "no_usable_route", surface: c };
}

export function classifyContactCompletenessGroup(opp = {}) {
  const route = hasUsableContactRoute(opp);
  const c = route.surface;
  const personOk = isPersonName(c.person);
  const phoneOk = isPhone(c.phone) || isPhone(c.intermediaryPhone);

  if (!route.ok) return CONTACT_COMPLETENESS_GROUP.NO_USABLE_ROUTE;
  if (personOk && phoneOk) return CONTACT_COMPLETENESS_GROUP.PERSON_METHOD_PHONE;
  if (personOk) return CONTACT_COMPLETENESS_GROUP.PERSON_METHOD_NO_PHONE;
  return CONTACT_COMPLETENESS_GROUP.FUNCTIONAL_ORG_ONLY;
}

/**
 * Commercial use-case classes for evidence-based suitability.
 * Presentation fields (summaryWhat / summaryWhyMatters / title polish) are NOT
 * evidence. Changing presentation must not change eligibility when research is unchanged.
 */
export const PROSPECT_USE_CASE = Object.freeze({
  DATED_LODGING: "dated_lodging",
  PREFERRED_LODGING_REFERRAL: "preferred_lodging_referral",
  LOCAL_MEETINGS_CATERING: "local_meetings_catering",
  PROFESSIONAL_PLANNER: "professional_planner",
  SCALING_CORPORATE: "scaling_corporate",
});

function sourcesList(opp = {}) {
  return Array.isArray(opp.sources) ? opp.sources : [];
}

function hasResearchSource(opp = {}) {
  return (
    Boolean(text(opp.officialSource || opp.discoverySource || opp.sourceUrl)) ||
    sourcesList(opp).some((s) => text(s?.url || s?.sourceUrl || s))
  );
}

/**
 * Hotel-fit evidence from research fields — not from customer summary prose alone.
 */
function hasHotelFitEvidence(opp = {}) {
  if (opp.hotelFitScore != null && Number.isFinite(Number(opp.hotelFitScore))) {
    return true;
  }
  if (
    text(
      opp.fitExplanation ||
        opp.hotelFitRationale ||
        opp.hotelOpportunityThesis ||
        opp.whyThisHotel
    )
  ) {
    return true;
  }
  const profile = opp.hotelFitAssessmentProfile;
  if (profile && typeof profile === "object") {
    if (profile.overallScore != null || profile.assessedDimensionCount > 0) {
      return true;
    }
  }
  return false;
}

function blobFromResearchFields(opp = {}, keys = []) {
  return keys.map((k) => text(opp[k])).filter(Boolean).join(" \n ");
}

function sourceEvidenceBlob(opp = {}) {
  return sourcesList(opp)
    .map((s) =>
      [s?.type, s?.title, s?.name, s?.note, s?.authority, s?.url]
        .map((x) => text(x))
        .filter(Boolean)
        .join(" ")
    )
    .join(" \n ");
}

function hasVenueActivityEvidence(opp = {}) {
  if (
    text(
      opp.venueActivityEvidence ||
        opp.eventActivityEvidence ||
        opp.meetingCateringNeeds ||
        opp.localConnection
    )
  ) {
    return true;
  }
  const facts = Array.isArray(opp.establishedFacts) ? opp.establishedFacts : [];
  if (facts.some((f) => text(f).length >= 24)) return true;
  const src = sourceEvidenceBlob(opp);
  if (
    /official_venue|official_private_event|wedding|reception|private.?event|gala|celebration|charter|yacht|event.?space|host.?an.?event/i.test(
      src
    )
  ) {
    return true;
  }
  if (opp.peVenueId || opp.peSignalId) {
    // Venue identity alone is not activity — require source or activity field above.
    return Boolean(src);
  }
  return false;
}

/**
 * Plausible accommodation / referral use case for outside venues.
 * Proximity / "no on-site lodging" alone is insufficient.
 */
function hasLodgingReferralUseCase(opp = {}) {
  const research = blobFromResearchFields(opp, [
    "lodgingOrReferralRationale",
    "fitExplanation",
    "hotelFitRationale",
    "hotelOpportunityThesis",
    "whyThisHotel",
    "recommendedAction",
    "proposedHotelUse",
  ]);
  const useCases = []
    .concat(opp.demandUseCases || [])
    .map((u) => String(u || "").toUpperCase());
  if (useCases.includes("VENUE_REFERRAL_PARTNERSHIP")) return true;

  // Positive lodging/referral language (research fields only — not summaryWhat).
  // Strip negation clauses so "rather than … room blocks" cannot count as referral demand.
  const researchPositive = research.replace(
    /\b(?:rather than|not|no|without|exclude|excluding)[^.!?]{0,80}\b(?:room\s*blocks?|lodging|accommodation|housing)\b/gi,
    " "
  );
  const positiveReferral =
    /accommodation[\s-]+referrals?|accommodation[\s-]+recommendations?|accommodation[\s-]?partnership|lodging[\s-]+referrals?|lodging[\s-]?partnership|lodging\s+option|preferred[\s-]?lodging|guest\s+accommodation|room\s*blocks?|overflow\s+stay|overnight\s+guests?|venue-to-hotel|complementary[^.]{0,40}accommodation|lodging\s+or\s+hospitality\s+partnerships?/i.test(
      researchPositive
    );

  // Negations / proximity-only / hospitality-potential alone must not count.
  const proximityOnly =
    /nearby|proximity|close to|walking distance|no on-?site lodging|without (?:on-?site )?lodging|hospitality potential/i.test(
      research
    ) && !positiveReferral;
  if (proximityOnly) return false;

  return positiveReferral;
}

function hasMeetingOrCateringActivity(opp = {}) {
  if (
    text(
      opp.eventActivityEvidence ||
        opp.meetingCateringNeeds ||
        opp.functionType ||
        opp.recurrenceNote ||
        opp.localConnection
    )
  ) {
    return true;
  }
  const facts = Array.isArray(opp.establishedFacts) ? opp.establishedFacts : [];
  if (facts.some((f) => /meeting|cater|banquet|gala|luncheon|workshop|training|symposium/i.test(text(f)))) {
    return true;
  }
  return /meeting|cater|banquet|gala|luncheon|workshop|training|awards|fundraiser/i.test(
    sourceEvidenceBlob(opp)
  );
}

function hasPlannerActivityEvidence(opp = {}) {
  if (
    text(
      opp.eventActivityEvidence ||
        opp.localMarketEvidence ||
        opp.localConnection ||
        opp.plannerSpecialties
    )
  ) {
    return true;
  }
  const facts = Array.isArray(opp.establishedFacts) ? opp.establishedFacts : [];
  if (facts.some((f) => text(f).length >= 20)) return true;
  return /planner|dmc|destination.?management|event.?product|portfolio|wedding|mitzvah|gala/i.test(
    sourceEvidenceBlob(opp)
  );
}

function hasDatedLodgingOccasion(opp = {}) {
  if (text(opp.timing) || text(opp.eventStartDate) || text(opp.eventDateDisplay)) {
    return true;
  }
  // Supervisor / maturation futureDemand window is source-backed timing evidence
  const dateWindow = text(
    opp.futureDemand?.dateWindow ||
      opp.futureDemand?.date ||
      opp.demandTiming ||
      opp.dateWindow
  );
  if (/\b20\d{2}\b/.test(dateWindow)) {
    return true;
  }
  if (opp.relationshipGrain === "DATED_OCCASION") return true;
  const title = text(opp.title || opp.opportunityName);
  if (/\b20\d{2}\b|annual meeting|conference|summit|congress/i.test(title)) {
    return true;
  }
  const facts = Array.isArray(opp.establishedFacts) ? opp.establishedFacts : [];
  return facts.some((f) =>
    /\b20\d{2}\b|dated|cycle|annual meeting|conference/i.test(text(f))
  );
}

/**
 * Grade travel-need language on research fields.
 * SPECULATIVE_SPILLOVER (stretch venue + spillover-only prose) is not sufficient
 * for dated-lodging HOTEL_FACING — keep as market intelligence.
 */
export function gradeTravelNeedEvidence(opp = {}) {
  if (opp.travelEvidence === true) {
    return { ok: true, grade: "STRUCTURED", note: "travelEvidence=true" };
  }
  if (text(opp.travelEvidenceNote || opp.relevantTravelers)) {
    return { ok: true, grade: "STRUCTURED", note: "travelEvidenceNote" };
  }
  const room = String(opp.roomDemandStatus || opp.guestroomStatus || "").toUpperCase();
  if (/REQUIRED|EVIDENCED|CONFIRMED/.test(room)) {
    return { ok: true, grade: "STRUCTURED", note: "roomDemandStatus" };
  }
  if (text(opp.housingStatus) && !/unknown|not.?established/i.test(text(opp.housingStatus))) {
    return { ok: true, grade: "STRUCTURED", note: "housingStatus" };
  }

  const research = blobFromResearchFields(opp, [
    "lodgingOrReferralRationale",
    "hotelOpportunityThesis",
    "establishedFacts",
    "localConnection",
    "hotelFitRationale",
  ]);
  // proposedHotelUse enum names alone (e.g. SMALL_GROUP_LODGING) must not satisfy travel need.
  const enumOnly = blobFromResearchFields(opp, ["proposedHotelUse"]);
  const factsOk =
    Array.isArray(opp.establishedFacts) &&
    opp.establishedFacts.some((f) =>
      /travel|attendee|room.?block|overnight|lodging|housing|delegate/i.test(text(f))
    );
  const proseOk =
    factsOk ||
    /travel|attendee|room.?block|overnight|lodging|housing|delegate/i.test(research);

  if (!proseOk) {
    // Enum name containing "lodging" is not travel evidence.
    if (/lodging/i.test(enumOnly) && !proseOk) {
      return { ok: false, grade: "ENUM_ONLY", note: "proposedHotelUse_enum_not_evidence" };
    }
    return { ok: false, grade: "NONE", note: "no_travel_language" };
  }

  const territory = String(opp.eventTerritoryClass || opp.hotelFit?.territory || "").toUpperCase();
  const stretch = territory === "TERRITORY_STRETCH" || /stretch vs/i.test(research);
  // Strip negation so "not mass … room blocks" cannot count as lodging proof.
  const researchPositive = research.replace(
    /\b(?:rather than|not|no|without|exclude|excluding|not mass)[^.!?]{0,100}\b(?:room\s*blocks?|lodging|accommodation|housing)\b/gi,
    " "
  );
  const orgLodgingProof =
    /travelling participants|travelling .+ stay|room.?block|housing page|official.?housing|delegate.?hotel/i.test(
      researchPositive
    );
  const spilloverOnly =
    /spillover|proportionate .+ mechanism|hospitality potential/i.test(research) &&
    !orgLodgingProof;
  if (stretch && spilloverOnly) {
    return {
      ok: false,
      grade: "SPECULATIVE_SPILLOVER",
      note: "stretch_venue_spillover_without_org_lodging_evidence",
    };
  }

  return {
    ok: true,
    grade: /room.?block|housing page|official.?housing/i.test(researchPositive)
      ? "STRONG_INFERRED"
      : "INFERRED",
    note: "research_field_travel_language",
  };
}

function hasTravelNeedEvidence(opp = {}) {
  return gradeTravelNeedEvidence(opp).ok === true;
}

function hasScalingLocalConnection(opp = {}) {
  return Boolean(text(opp.localConnection || opp.localPresence));
}

function hasScalingGrowthEvidence(opp = {}) {
  if (text(opp.datedGrowthEvidence) || text(opp.growthEvidenceDate)) return true;
  if (opp.operatingExpansionEvidence === true) return true;
  const signals = [].concat(opp.growthSignals || []);
  if (
    signals.some((s) =>
      /OFFICE|EXPANSION|HIRING|ONBOARDING|GATHERING|TRAINING|CUSTOMER_IMPLEMENTATION/i.test(
        String(s || "")
      )
    )
  ) {
    return true;
  }
  // Funding-only is insufficient
  if (signals.length === 1 && /FUNDING/i.test(String(signals[0] || ""))) {
    return false;
  }
  return false;
}

/**
 * Strategic partnerships / place brands are not accommodation HOTEL_FACING
 * unless separate lodging-buyer / referral evidence is stamped.
 */
function isPartnershipWithoutLodgingSales(opp = {}) {
  const disposition = String(
    opp.commercialDisposition || opp.disposition || ""
  ).toUpperCase();
  const label = String(
    opp.demandEvidenceLabel || opp.commercialDemandEvidenceLabel || ""
  ).toUpperCase();
  if (disposition === "STRATEGIC_PARTNERSHIP") return true;
  if (label === "PARTNERSHIP_WITHOUT_LODGING") return true;
  return false;
}

/**
 * Classify commercial use case from research identity — not from summary prose.
 */
export function classifyProspectUseCase(opp = {}) {
  const type = String(opp.opportunityType || "").toUpperCase();
  const seg = String(opp.researchSegment || opp.segment || "").toLowerCase();
  const grain = String(opp.relationshipGrain || "").toUpperCase();
  const lmcSub = String(opp.lmcSubsegment || "").toLowerCase();
  const useCases = []
    .concat(opp.demandUseCases || [])
    .map((u) => String(u || "").toUpperCase());
  const id = String(opp.id || "");

  // Place-management / estate partners are not lodging-referral sales by segment alone.
  if (
    isPartnershipWithoutLodgingSales(opp) ||
    /howard de walden|marylebone village/i.test(
      text(opp.organizationName || opp.company || opp.account || "")
    )
  ) {
    // Fall through to dated lodging only when separate lodging-referral evidence exists;
    // otherwise suitability will fail travel_need / lodging_referral as appropriate.
    if (
      useCases.includes("VENUE_REFERRAL_PARTNERSHIP") &&
      hasLodgingReferralUseCase(opp)
    ) {
      return PROSPECT_USE_CASE.PREFERRED_LODGING_REFERRAL;
    }
    // Keep as dated lodging so partnership gate (not weddings_social auto-referral) applies.
  } else if (
    type === "VENUE_PARTNERSHIP" ||
    grain === "VENUE_REFERRAL" ||
    Boolean(opp.peVenueId) ||
    useCases.includes("VENUE_REFERRAL_PARTNERSHIP") ||
    /private.?event|wedding|preferred.?lodging/i.test(seg)
  ) {
    return PROSPECT_USE_CASE.PREFERRED_LODGING_REFERRAL;
  }
  if (
    lmcSub === "professional_planners_intermediaries" ||
    opp.entityKind === "PLANNER" ||
    opp.candidateKind === "planner" ||
    Array.isArray(opp.plannerSpecialties)
  ) {
    return PROSPECT_USE_CASE.PROFESSIONAL_PLANNER;
  }
  if (
    seg.includes("scaling") ||
    /^gdi_sc_/i.test(id) ||
    /scaling corporate/i.test(String(opp.prospectCommercialLabel || ""))
  ) {
    return PROSPECT_USE_CASE.SCALING_CORPORATE;
  }
  if (
    seg.includes("local_meetings") ||
    /^gdi_lmc_/i.test(id) ||
    opp.guestroomStatus === "NOT_REQUIRED" ||
    useCases.includes("MEETINGS_AND_CATERING")
  ) {
    return PROSPECT_USE_CASE.LOCAL_MEETINGS_CATERING;
  }
  return PROSPECT_USE_CASE.DATED_LODGING;
}

/**
 * Prospect suitability (commercial credibility) — evidence-based and use-case-specific.
 * Independent of contact completeness and of confirmed buying / open sourcing.
 *
 * Intentionally ignores customer presentation fields such as summaryWhat /
 * summaryWhyMatters as evidence of travel, venue activity, or growth.
 */
export function evaluateProspectSuitability(opp = {}) {
  const failed = [];
  const useCase = classifyProspectUseCase(opp);
  const org = text(opp.organizationName || opp.company || opp.account || opp.venueName);
  const title = text(opp.title || opp.opportunityName);
  if (!org) failed.push("organization");

  if (useCase === PROSPECT_USE_CASE.PREFERRED_LODGING_REFERRAL) {
    if (!title && !org) failed.push("venue_or_title");
    if (!hasVenueActivityEvidence(opp)) failed.push("venue_activity_evidence");
    if (!hasLodgingReferralUseCase(opp)) {
      failed.push("lodging_referral_use_case");
    }
    if (!hasHotelFitEvidence(opp)) failed.push("hotel_fit");
    if (!hasResearchSource(opp)) failed.push("source");
  } else if (useCase === PROSPECT_USE_CASE.LOCAL_MEETINGS_CATERING) {
    if (!title && !org) failed.push("occasion_or_title");
    if (!hasMeetingOrCateringActivity(opp)) {
      failed.push("meeting_or_catering_activity");
    }
    if (!hasHotelFitEvidence(opp)) failed.push("hotel_fit");
    if (!hasResearchSource(opp)) failed.push("source");
    // Guestrooms / traveling attendees are NOT required.
  } else if (useCase === PROSPECT_USE_CASE.PROFESSIONAL_PLANNER) {
    if (!title && !org) failed.push("planner_or_title");
    if (!hasPlannerActivityEvidence(opp)) {
      failed.push("planner_local_activity");
    }
    if (!hasHotelFitEvidence(opp)) failed.push("hotel_fit");
    if (!hasResearchSource(opp)) failed.push("source");
    // Do not require confirmed purchasing authority or an invented forthcoming date.
  } else if (useCase === PROSPECT_USE_CASE.SCALING_CORPORATE) {
    if (!title && !org) failed.push("company_or_title");
    if (!hasScalingLocalConnection(opp)) failed.push("local_connection");
    if (!hasScalingGrowthEvidence(opp)) {
      failed.push("dated_growth_or_expansion_evidence");
    }
    if (
      !hasHotelFitEvidence(opp) &&
      !text(opp.proposedHotelUse)
    ) {
      failed.push("hotel_use_or_fit");
    }
    if (!hasResearchSource(opp)) failed.push("source");
  } else {
    // Dated lodging prospect
    if (!title && !text(opp.timing)) failed.push("occasion_or_title");
    if (!hasDatedLodgingOccasion(opp)) failed.push("supported_occasion_or_timing");
    if (!hasTravelNeedEvidence(opp)) failed.push("travel_need_evidence");
    if (!hasHotelFitEvidence(opp)) failed.push("hotel_fit");
    if (!hasResearchSource(opp)) failed.push("source");
  }

  return {
    ok: failed.length === 0,
    failed,
    useCase,
    note:
      "Presentation fields (summaryWhat/summaryWhyMatters) are not suitability evidence.",
  };
}

export const REVIEWED_PUBLICATION_BASIS = "REVIEWED_PACKAGE_V1";

export const PUBLICATION_EVENT_TYPE = Object.freeze({
  AUTHORIZED_PROMOTION: "AUTHORIZED_PUBLICATION_PROMOTION",
  BASIS_REPAIR: "PUBLICATION_BASIS_REPAIR",
  WITHDRAWAL: "AUTHORIZED_PUBLICATION_WITHDRAWAL",
});

/**
 * Preserve publication / identity metadata across partial enrichment writes.
 * Never invent a publication basis; only keep an existing one (or explicit next).
 * Does not promote internal→published. Does not revive explicit withdrawals.
 */
export function preservePublicationMetadata(prior = {}, next = {}) {
  const out = { ...next };
  const priorBasis = text(prior.prospectPublicationBasis);
  const nextBasis = text(next.prospectPublicationBasis);
  if (priorBasis && !nextBasis) {
    out.prospectPublicationBasis = prior.prospectPublicationBasis;
  }
  // Explicit withdrawal must survive enrichment / reimport
  if (
    prior.prospectPublicationWithdrawn === true ||
    prior.prospectSuppressed === true
  ) {
    out.prospectPublicationWithdrawn =
      prior.prospectPublicationWithdrawn === true
        ? true
        : out.prospectPublicationWithdrawn;
    out.prospectSuppressed =
      prior.prospectSuppressed === true ? true : out.prospectSuppressed;
    if (prior.prospectPublicationWithdrawn === true) {
      out.prospectHotelFacing = false;
      out.customerVisible = false;
      if (!nextBasis && !priorBasis) {
        out.prospectPublicationBasis = null;
      }
    }
  } else {
    if (prior.prospectHotelFacing === true && next.prospectHotelFacing == null) {
      out.prospectHotelFacing = true;
    }
    if (prior.customerVisible === true && next.customerVisible == null) {
      out.customerVisible = true;
    }
  }
  for (const key of [
    "id",
    "opportunityId",
    "hotelId",
    "peVenueId",
    "peSignalId",
    "dgGeneratorId",
    "hotelGeneratorFitId",
    "decisionId",
    "firstSeenAt",
    "prospectReviewSection",
    "prospectCommercialLabel",
    "customerFeedback",
    "hotelFeedback",
    "salesActions",
    "decisionOutcomes",
    "activityHistory",
    "publicationEvents",
    "prospectPublicationWithdrawnAt",
    "prospectPublicationWithdrawnReason",
  ]) {
    if (prior[key] != null && (next[key] == null || next[key] === "")) {
      out[key] = prior[key];
    }
  }
  return out;
}

function buildPublicationEvent({
  type,
  reason,
  authorizedBy,
  priorBasis,
  nextBasis,
  eligibility,
  opportunityId,
}) {
  return {
    type,
    at: new Date().toISOString(),
    opportunityId: opportunityId || null,
    reason: reason || null,
    authorizedBy: authorizedBy || "explicit_authorizePublication",
    priorBasis: priorBasis || null,
    nextBasis: nextBasis || null,
    eligibility: eligibility || null,
  };
}

/**
 * Explicitly authorized internal → published (or basis repair) transition.
 *
 * - Requires opts.authorizePublication === true (never inferred from facing flags).
 * - Re-evaluates eligibility; refuses when not HOTEL_FACING.
 * - Writes REVIEWED_PACKAGE_V1 basis + durable publication event.
 * - preservePublicationMetadata alone cannot substitute for this step.
 */
export function authorizeProspectPublicationPromotion(opp = {}, opts = {}) {
  if (opts.authorizePublication !== true) {
    return {
      ok: false,
      reason: "authorizePublication_required",
      patched: opp,
      event: null,
    };
  }
  if (opp.prospectPublicationWithdrawn === true || opp.prospectSuppressed === true) {
    return {
      ok: false,
      reason: "withdrawn_or_suppressed",
      patched: opp,
      event: null,
    };
  }

  const elig = evaluateProspectPublicationEligibility(opp, opts);
  if (!elig.hotelFacing) {
    return {
      ok: false,
      reason: "eligibility_not_hotel_facing",
      eligibility: elig,
      patched: opp,
      event: null,
    };
  }

  const priorBasis = text(opp.prospectPublicationBasis) || null;
  const alreadyPublished =
    priorBasis === REVIEWED_PUBLICATION_BASIS &&
    opp.prospectHotelFacing === true &&
    opp.customerVisible !== false;

  const eventType = priorBasis
    ? priorBasis === REVIEWED_PUBLICATION_BASIS
      ? PUBLICATION_EVENT_TYPE.AUTHORIZED_PROMOTION
      : PUBLICATION_EVENT_TYPE.BASIS_REPAIR
    : PUBLICATION_EVENT_TYPE.AUTHORIZED_PROMOTION;

  // Missing basis with facing already true = basis repair (Wisedocs/Shipyard pattern)
  const effectiveType =
    !priorBasis &&
    (opp.prospectHotelFacing === true || opp.customerVisible === true)
      ? PUBLICATION_EVENT_TYPE.BASIS_REPAIR
      : eventType;

  const event = buildPublicationEvent({
    type: effectiveType,
    reason:
      opts.reason ||
      (effectiveType === PUBLICATION_EVENT_TYPE.BASIS_REPAIR
        ? "authorized_basis_repair_after_facing_without_basis"
        : "authorized_internal_to_published_promotion"),
    authorizedBy: opts.authorizedBy || "explicit_authorizePublication",
    priorBasis,
    nextBasis: REVIEWED_PUBLICATION_BASIS,
    eligibility: elig.eligibility,
    opportunityId: opp.id || opp.opportunityId,
  });

  if (alreadyPublished && opts.force !== true) {
    // Idempotent: still append event only when requested; otherwise no-op success
    return {
      ok: true,
      idempotent: true,
      reason: "already_published_with_basis",
      eligibility: elig,
      patched: opp,
      event: null,
    };
  }

  const publicationEvents = [
    ...(Array.isArray(opp.publicationEvents) ? opp.publicationEvents : []),
    event,
  ];
  const activityHistory = [
    ...(Array.isArray(opp.activityHistory) ? opp.activityHistory : []),
    {
      kind: "publication",
      ...event,
    },
  ];

  const patched = {
    ...opp,
    prospectPublicationBasis: REVIEWED_PUBLICATION_BASIS,
    prospectHotelFacing: true,
    customerVisible: true,
    prospectPublicationWithdrawn: false,
    prospectSuppressed: false,
    prospectPublishedAt: opp.prospectPublishedAt || event.at,
    prospectPublicationLastEventAt: event.at,
    publicationEvents,
    activityHistory,
  };

  return {
    ok: true,
    idempotent: false,
    reason: event.reason,
    eligibility: elig,
    patched,
    event,
  };
}

/**
 * Explicit withdrawal — remains withdrawn through enrichment / reimport.
 */
export function authorizeProspectPublicationWithdrawal(opp = {}, opts = {}) {
  if (opts.authorizeWithdrawal !== true) {
    return {
      ok: false,
      reason: "authorizeWithdrawal_required",
      patched: opp,
      event: null,
    };
  }
  const event = buildPublicationEvent({
    type: PUBLICATION_EVENT_TYPE.WITHDRAWAL,
    reason: opts.reason || "authorized_publication_withdrawal",
    authorizedBy: opts.authorizedBy || "explicit_authorizeWithdrawal",
    priorBasis: text(opp.prospectPublicationBasis) || null,
    nextBasis: null,
    eligibility: null,
    opportunityId: opp.id || opp.opportunityId,
  });
  const patched = {
    ...opp,
    prospectHotelFacing: false,
    customerVisible: false,
    prospectPublicationWithdrawn: true,
    prospectSuppressed: true,
    prospectPublicationWithdrawnAt: event.at,
    prospectPublicationWithdrawnReason: event.reason,
    publicationEvents: [
      ...(Array.isArray(opp.publicationEvents) ? opp.publicationEvents : []),
      event,
    ],
    activityHistory: [
      ...(Array.isArray(opp.activityHistory) ? opp.activityHistory : []),
      { kind: "publication", ...event },
    ],
  };
  return { ok: true, patched, event };
}

/**
 * Agreed hotel-facing publication eligibility for sales-prospect action lists.
 * Unknown hotel sourcing / unknown rooms / TBA do NOT fail this gate.
 * Confirmed buying is a separate commercial label.
 */
export function evaluateProspectPublicationEligibility(opp = {}, opts = {}) {
  const suitability = evaluateProspectSuitability(opp);
  const route = hasUsableContactRoute(opp);
  const group = classifyContactCompletenessGroup(opp);
  // Stamp nested contact onto flat fields so Ready taxonomy sees the same surface.
  const s = extractContactSurface(opp);
  const readyContact = classifyBuyerContactPath({
    ...opp,
    publicContactPath: opp.publicContactPath || s.contactUrl || undefined,
    organizationContactUrl: opp.organizationContactUrl || s.contactUrl || undefined,
    primaryContactEmail: opp.primaryContactEmail || s.email || undefined,
    primaryContact: {
      ...(opp.primaryContact || {}),
      email: opp.primaryContact?.email || s.email || undefined,
      phone: opp.primaryContact?.phone || s.phone || undefined,
      name: opp.primaryContact?.name || s.person || undefined,
      organizationContactRoute:
        opp.primaryContact?.organizationContactRoute || s.contactUrl || undefined,
    },
  });
  const travelGrade = gradeTravelNeedEvidence(opp);

  if (opp.isTestData === true || text(opp.priority).toUpperCase() === "DISQUALIFIED") {
    return {
      eligibility: PROSPECT_ELIGIBILITY.NOT_A_PROSPECT,
      hotelFacing: false,
      enrichmentQueue: false,
      suitability,
      route,
      contactCompletenessGroup: group,
      readyContactClass: readyContact.class,
      readyContactEligible: readyContact.readyEligible === true,
      travelNeedGrade: travelGrade,
      reasons: ["disqualified_or_test"],
    };
  }

  // Paid MS research-layer quarantine/reject must not become HOTEL_FACING via packet prose.
  const researchLayer =
    opp.researchLayerDisposition || opp.disposition || null;
  if (
    researchLayer &&
    !isResearchLayerPublishableDisposition(researchLayer)
  ) {
    return {
      eligibility: PROSPECT_ELIGIBILITY.NOT_A_PROSPECT,
      hotelFacing: false,
      enrichmentQueue: false,
      suitability,
      route,
      contactCompletenessGroup: group,
      readyContactClass: readyContact.class,
      readyContactEligible: readyContact.readyEligible === true,
      travelNeedGrade: travelGrade,
      reasons: [`research_layer_${String(researchLayer).toLowerCase()}`],
    };
  }

  // Partnership / place-management is a separate commercial path — not lodging sales.
  if (isPartnershipWithoutLodgingSales(opp) && !hasLodgingReferralUseCase(opp)) {
    return {
      eligibility: PROSPECT_ELIGIBILITY.ENRICHMENT_QUEUE,
      hotelFacing: false,
      enrichmentQueue: true,
      suitability,
      route,
      contactCompletenessGroup: group,
      readyContactClass: readyContact.class,
      readyContactEligible: readyContact.readyEligible === true,
      travelNeedGrade: travelGrade,
      reasons: ["partnership_not_lodging_sales"],
      nextTask:
        "Route via Private Events / referral / partnership path — do not treat as hotel accommodation sales eligibility.",
      commercialPath: "STRATEGIC_PARTNERSHIP",
    };
  }

  if (!suitability.ok) {
    return {
      eligibility: PROSPECT_ELIGIBILITY.ENRICHMENT_QUEUE,
      hotelFacing: false,
      enrichmentQueue: true,
      suitability,
      route,
      contactCompletenessGroup: group,
      readyContactClass: readyContact.class,
      readyContactEligible: readyContact.readyEligible === true,
      travelNeedGrade: travelGrade,
      reasons: suitability.failed,
      nextTask: `Resolve suitability gaps: ${suitability.failed.join(", ")}`,
    };
  }

  if (!route.ok) {
    return {
      eligibility: PROSPECT_ELIGIBILITY.ENRICHMENT_QUEUE,
      hotelFacing: false,
      enrichmentQueue: true,
      suitability,
      route,
      contactCompletenessGroup: group,
      readyContactClass: readyContact.class,
      readyContactEligible: readyContact.readyEligible === true,
      travelNeedGrade: travelGrade,
      reasons: [route.reason],
      nextTask: "Establish source-supported usable contact route (email, phone, form, or intermediary).",
    };
  }

  return {
    eligibility: PROSPECT_ELIGIBILITY.HOTEL_FACING,
    hotelFacing: true,
    enrichmentQueue: false,
    suitability,
    route,
    contactCompletenessGroup: group,
    readyContactClass: readyContact.class,
    readyContactEligible: readyContact.readyEligible === true,
    travelNeedGrade: travelGrade,
    reasons: [],
    note:
      readyContact.readyEligible === true
        ? "Also satisfies strict Ready contact taxonomy"
        : "Hotel-facing via usable route; may still fail strict Ready/ACTIONABLE until full packet fields stamp",
  };
}

/**
 * Map a hotel-review contact package + register row into opportunity-shaped fields
 * so Ready contact taxonomy and UI serializers see the same surface.
 */
function cleanPersonName(raw = "") {
  // Strip parenthetical aliases so isLikelyPersonName / Ready contact taxonomy accept the name
  return text(raw)
    .replace(/\s*\([^)]*\)\s*/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function applyContactPackageToOpportunity(opp = {}, pkg = {}) {
  const next = { ...opp, contactPackage: pkg };
  const person = cleanPersonName(pkg.person);
  const email = isEmail(pkg.contactMethod)
    ? text(pkg.contactMethod)
    : text(pkg.contactMethod).match(/^[^\s@]+@[^\s@]+\.[^\s@]+/)?.[0] || "";
  const phone = /^not\s*established$/i.test(text(pkg.businessPhone))
    ? ""
    : text(pkg.businessPhone);

  if (isPersonName(person)) {
    next.primaryContact = {
      ...(typeof next.primaryContact === "object" ? next.primaryContact : {}),
      name: person,
      role: text(pkg.title),
      email: email || undefined,
      phone: phone || undefined,
      phoneType: text(pkg.phoneType) || undefined,
      sourceUrl: text(pkg.sourceUrl) || undefined,
      verificationDate: text(pkg.verificationDate) || undefined,
    };
    next.primaryContactName = person;
    next.primaryContactRole = text(pkg.title);
    next.primaryContactEmail = email || next.primaryContactEmail;
    next.primaryContactPhone = phone || next.primaryContactPhone;
    next.primaryContactPhoneType = text(pkg.phoneType) || next.primaryContactPhoneType;
  } else if (email) {
    next.functionalContactEmail = email;
    next.primaryContactEmail = email;
  }

  if (phone && !next.primaryContactPhone) next.primaryContactPhone = phone;

  if (pkg.bookingIntermediary && typeof pkg.bookingIntermediary === "object") {
    next.housingIntermediaryName = pkg.bookingIntermediary.name;
    next.housingIntermediaryEmail = pkg.bookingIntermediary.contact;
    next.housingIntermediaryPhone = pkg.bookingIntermediary.phone;
    if (isEmail(pkg.bookingIntermediary.contact) && !next.functionalContactEmail) {
      next.functionalContactEmail = pkg.bookingIntermediary.contact;
    }
  }

  next.contactResearchAttempted = true;
  next.whoResearchAttempted = true;
  next.contactVerificationDate = text(pkg.verificationDate) || next.contactVerificationDate;
  next.bookingResponsibilityClass = text(pkg.bookingResponsibility) || next.bookingResponsibilityClass;
  next.contactCompletenessGroup = pkg.completenessGroup ?? classifyContactCompletenessGroup(next);
  next.officialSource = next.officialSource || text(pkg.sourceUrl);
  return next;
}
