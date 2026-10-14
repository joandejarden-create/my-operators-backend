/**
 * Shared drawer-content contract for reviewed (and other) GDI prospects.
 *
 * Separately inspectable from:
 *   - commercial / publication eligibility
 *   - contact completeness grading
 *
 * Unknown room volume or unconfirmed sourcing alone must not exclude a prospect.
 * Generic repeated filler must not satisfy content requirements.
 * Numeric fit scores only when explainable (fitRationale / fitExplanation present).
 */

import { isGdiCustomerDrawerReady } from "../enrich-gdi-opportunity-for-customer.js";
import {
  evaluateProspectPublicationEligibility,
  classifyContactCompletenessGroup,
  hasUsableContactRoute,
  extractContactSurface,
} from "../prospect-publication-eligibility-v1.js";

export const DRAWER_CONTENT_CONTRACT_VERSION = "gdi-drawer-content-contract-v3";

const GENERIC_FILLER_RE =
  /\b(reach out to discuss|synerg(?:y|ies)|leverage our (?:full-service|premier)|world[- ]class (?:hospitality|experience)|perfect (?:venue|fit) for your (?:next )?event|contact us (?:today|for more)|we look forward to|please (?:let us know|contact)|general inquiries)\b/i;

/** Short commercial labels that are NOT hotel-specific theses. */
const GENERIC_THESIS_RE =
  /^(prospect to qualify|exploratory account|exploratory\b)([^a-z]|$)/i;

const GENERIC_ACTION_RE =
  /^(ask whether hotel selection\s*\/\s*lodging plans are open for this occasion|contact the published route|reach out to discuss|follow up as appropriate)\b/i;

const HOTEL_SPECIFIC_TOKEN_RE =
  /\b(mayflower|bethesda|marriott|wyndham|renaissance|waterstone|autograph|connecticut|harbor|bayside|downtown|overflow|corridor|soccerplex|hbc|national mall|grand hyatt|crystal gateway|omni shoreham|hilton)\b/i;

const UNSUPPORTED_VERIFIED_RE =
  /\bverified(?:\s+via|\s*:)?\b/i;

const HOMEPAGE_ONLY_RE =
  /^https?:\/\/[^/]+\/?(?:en\/|es\/|de\/)?(?:home\/?)?(?:index\.(?:html?|php))?$/i;

/** Event/conference microsite roots are material occasion evidence, not org homepages. */
const EVENT_MICROSITE_HOST_RE =
  /(?:conference|congress|summit|symposium|washcon|annual-?meeting|event)/i;

const STAFF_ONLY_SOURCE_RE =
  /\/(?:nado-)?staff\/?$|\/(?:our-)?(?:team|people|meet-our-staff)\/?$/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hasUsefulText(v, min = 24) {
  const t = text(v);
  return t.length >= min && !GENERIC_FILLER_RE.test(t) && !GENERIC_THESIS_RE.test(t);
}

function isGenericThesis(v) {
  const t = text(v);
  if (!t) return true;
  if (GENERIC_THESIS_RE.test(t)) return true;
  if (GENERIC_FILLER_RE.test(t)) return true;
  if (/^prospect to qualify\s*[—–-]\s*hotel sourcing unconfirmed\.?$/i.test(t)) {
    return true;
  }
  // Browse-template filler used when commercialStatus was mistaken for thesis
  if (/group lodging prospect\.?$/i.test(t) && t.length < 120) {
    return true;
  }
  return false;
}

function sourceUrls(opp = {}) {
  const urls = [];
  if (text(opp.officialSource)) urls.push(text(opp.officialSource));
  for (const s of opp.sources || []) {
    const u = text(s?.url || s);
    if (u) urls.push(u);
  }
  for (const u of opp.evidenceLinks || []) {
    if (text(u)) urls.push(text(u));
  }
  if (text(opp.discoverySource)) urls.push(text(opp.discoverySource));
  return [...new Set(urls)];
}

function isHomepageOnlyUrl(u) {
  const url = text(u);
  if (!url) return true;
  if (!HOMEPAGE_ONLY_RE.test(url)) return false;
  try {
    const host = new URL(url).hostname || "";
    if (EVENT_MICROSITE_HOST_RE.test(host)) return false;
  } catch {
    /* keep homepage classification */
  }
  return true;
}

function evidencePresent(opp = {}) {
  return sourceUrls(opp).some((u) => u && !isHomepageOnlyUrl(u));
}

function hasEventSupportingSource(opp = {}) {
  const urls = sourceUrls(opp);
  if (!urls.length) return false;
  // Staff/team pages support contact facts only — not event dates/lodging.
  const nonStaff = urls.filter((u) => !STAFF_ONLY_SOURCE_RE.test(u));
  return nonStaff.length > 0;
}

function contactRouteSummary(opp = {}) {
  // Same usable-route standard as publication eligibility (email / phone /
  // functional contact URL / intermediary) — not email/phone/form only.
  const route = hasUsableContactRoute(opp);
  const surface = route.surface || extractContactSurface(opp);
  const name = text(surface.person);
  const email = text(surface.email);
  const phone = text(surface.phone);
  const form =
    text(opp.functionalContactUrl || opp.contactFormUrl) ||
    (route.ok && route.reason === "functional_contact_url"
      ? text(surface.contactUrl || surface.methodRaw)
      : "");
  const namedPerson = Boolean(name && /\s/.test(name) && !/^(staff|team|office|desk)\b/i.test(name));
  return {
    hasRoute: route.ok === true,
    namedPerson,
    routingOnly: route.ok === true && !namedPerson,
    email: Boolean(email),
    phone: Boolean(phone),
    form: Boolean(form),
    relevance: text(
      opp.primaryContact?.relevanceEvidence ||
        opp.primaryContact?.whyThisContact ||
        opp.contactRelevance ||
        opp.whyThisContact
    ),
    responsibilityClass: text(
      opp.bookingResponsibilityClass ||
        opp.bookingResponsibilityLabel ||
        opp.primaryContact?.relationshipToOpportunity
    ),
  };
}

function notEstablishedNote(field, opp = {}) {
  const map = {
    roomDemand:
      opp.roomDemandStatusNote ||
      "Not established — peak rooms / attendance not published for this occasion.",
    venueHistory:
      opp.venueHistoryNote ||
      "Not researched — prior host hotels for this occasion were not captured in this package.",
    reactivation:
      opp.reactivationNote ||
      "Not applicable — not framed as a reactivation of a prior hotel relationship.",
    competitive:
      opp.competitiveContextNote ||
      "Not researched — STR / group alternatives were not assessed for this reviewed package.",
    opportunityType:
      "Not established — opportunity type was not assigned in the reviewed package.",
  };
  return map[field] || "Not established.";
}

/**
 * Evaluate drawer sales-content completeness for one opportunity/card.
 */
export function evaluateDrawerContentCompleteness(opportunity = {}, opts = {}) {
  const failed = [];
  const warnings = [];
  const notes = {};

  const account = text(
    opportunity.organizationName || opportunity.company || opportunity.account
  );
  const occasion = text(
    opportunity.title || opportunity.opportunityName || opportunity.occasion
  );
  const cohort = text(
    opportunity.travelingCohort ||
      opportunity.relevantTravelers ||
      opportunity.cohortDescription
  );
  const timing =
    text(opportunity.eventStartDate) ||
    text(opportunity.timing?.start) ||
    text(opportunity.timingNote) ||
    text(opportunity.timing);
  const location = text(
    opportunity.destinationStatus ||
      opportunity.eventCity ||
      opportunity.venueCity ||
      opportunity.city ||
      opportunity.venue ||
      opportunity.location ||
      opportunity.eventLocationLabel
  );
  const timingEvidenceStatus = text(
    opportunity.timingEvidenceStatus ||
      (hasEventSupportingSource(opportunity) ? "source_linked" : "unverified_or_staff_only")
  );
  const opportunityType = text(opportunity.opportunityType);
  const thesis = text(
    opportunity.hotelOpportunityThesis ||
      opportunity.whyWyndhamFits ||
      opportunity.summaryWhyHotel ||
      opportunity.fitExplanation
  );
  const fitRationale = text(
    opportunity.fitRationale ||
      opportunity.hotelFitRationale ||
      opportunity.fitExplanation ||
      opportunity.summaryWhyHotel
  );
  const demand = text(
    opportunity.summaryWhat || opportunity.demandSignal || opportunity.whyNow
  );
  const whyNow = text(
    opportunity.whyNow || opportunity.summaryWhyNow || opportunity.cardWhyNowLine
  );
  const sourcing = text(
    opportunity.venueSourcingStatusLabel ||
      opportunity.sourcingStatus ||
      opportunity.venueSourcingStatus ||
      opportunity.venueSourcingRationale
  );
  const nextAction = text(
    opportunity.recommendedAction ||
      opportunity.recommendedFirstAction ||
      opportunity.recommendedNextStep ||
      opportunity.nextAction
  );
  const contact = contactRouteSummary(opportunity);
  const fitScore = opportunity.hotelFitScore;
  const hasCanonicalFitBasis = Boolean(
    opportunity.hotelFitScoreBasis ||
      opportunity.fitScoreMethod ||
      opportunity.fitScoreVersion
  );
  const hasExplainableFitScore =
    fitScore != null &&
    Number.isFinite(Number(fitScore)) &&
    hasCanonicalFitBasis &&
    hasUsefulText(fitRationale, 40) &&
    !isGenericThesis(fitRationale);

  // Required contract fields
  if (!account) failed.push("account");
  if (!occasion && !cohort) failed.push("occasion_or_cohort");
  if (!timing) failed.push("timing");
  if (!location) failed.push("location");
  if (!opportunityType) {
    failed.push("opportunity_type");
    notes.opportunityType = notEstablishedNote("opportunityType", opportunity);
  }
  if (!sourcing) failed.push("commercial_sourcing_status");
  if (!hasUsefulText(thesis, 40) || isGenericThesis(thesis)) {
    failed.push("hotel_specific_thesis");
  }
  if (!hasUsefulText(fitRationale, 40) || isGenericThesis(fitRationale)) {
    failed.push("hotel_fit_rationale");
  }
  if (
    fitScore != null &&
    Number.isFinite(Number(fitScore)) &&
    !hasExplainableFitScore
  ) {
    failed.push("fit_score_without_explainable_basis");
  }
  if (!hasUsefulText(whyNow, 16)) failed.push("why_consider_now");
  if (!contact.hasRoute) failed.push("usable_contact_route");
  if (contact.hasRoute && !text(contact.relevance) && contact.namedPerson) {
    failed.push("contact_relevance");
  }
  if (contact.hasRoute && contact.routingOnly) {
    warnings.push("routing_only_not_verified_named_contact");
  }
  if (!evidencePresent(opportunity)) failed.push("sources_for_material_claims");
  if (timing && location && !hasEventSupportingSource(opportunity)) {
    failed.push("event_evidence_beyond_staff_page");
    notes.eventEvidence =
      "Contact staff pages support contact facts only — event dates/location need an occasion source.";
  }
  if (
    !hasUsefulText(nextAction, 20) ||
    /contact the published route/i.test(nextAction) ||
    GENERIC_ACTION_RE.test(nextAction)
  ) {
    failed.push("concrete_next_sales_action");
  }

  // Substantive quality — text presence alone is not enough
  const substantiveFailed = [];
  const normativeBlobs = [thesis, fitRationale, demand, whyNow, nextAction]
    .map((t) => text(t).toLowerCase())
    .filter(Boolean);
  const uniqueBlobs = new Set(normativeBlobs);
  if (normativeBlobs.length >= 3 && uniqueBlobs.size === 1) {
    substantiveFailed.push("repeated_identical_section_prose");
  }
  if (
    hasUsefulText(thesis, 40) &&
    !HOTEL_SPECIFIC_TOKEN_RE.test(thesis) &&
    !HOTEL_SPECIFIC_TOKEN_RE.test(fitRationale)
  ) {
    substantiveFailed.push("missing_hotel_specific_rationale");
  }
  const claimSurfaces = [whyNow, demand, thesis, text(opportunity.timingNote)];
  const hasVerifiedClaim = claimSurfaces.some((t) => UNSUPPORTED_VERIFIED_RE.test(t));
  if (hasVerifiedClaim && !evidencePresent(opportunity)) {
    substantiveFailed.push("unsupported_verified_claim");
  }
  if (
    contact.hasRoute &&
    contact.namedPerson &&
    !text(contact.relevance) &&
    !text(opportunity.contactRelevance)
  ) {
    substantiveFailed.push("contact_relevance_missing");
  }
  // Implementation notes must not masquerade as customer fit assessment
  if (
    /reviewed package did not run canonical|canonical (?:fit )?engine|canonical scoring|implementation note/i.test(
      text(opportunity.hotelFitScoreNote) + " " + thesis + " " + fitRationale
    )
  ) {
    substantiveFailed.push("customer_facing_implementation_note");
  }
  for (const f of substantiveFailed) failed.push(f);

  // Optional sections — populate or explicit not-established
  const optional = {
    roomDemand: {
      value:
        opportunity.estimatedPeakRooms ??
        opportunity.peakRooms ??
        opportunity.peakRoomEstimate ??
        null,
      status: text(opportunity.roomDemandStatus) || "UNKNOWN",
      note:
        opportunity.roomDemandNote ||
        opportunity.roomDemandStatusNote ||
        notEstablishedNote("roomDemand", opportunity),
    },
    venueHistory: {
      value: opportunity.venueHistory || opportunity.meetingVenueHistory || null,
      note: opportunity.venueHistoryNote || notEstablishedNote("venueHistory", opportunity),
    },
    reactivation: {
      value: opportunity.reactivationSignals || null,
      note: opportunity.reactivationNote || notEstablishedNote("reactivation", opportunity),
    },
    competitive: {
      value: opportunity.competitiveContext || null,
      note:
        opportunity.competitiveContextNote ||
        notEstablishedNote("competitive", opportunity),
    },
  };

  if (
    optional.roomDemand.value == null ||
    /unknown/i.test(String(optional.roomDemand.status))
  ) {
    warnings.push("unknown_room_volume_allowed");
  }
  if (/unconfirmed|unknown|not established|hotel sourcing unconfirmed/i.test(sourcing)) {
    warnings.push("unconfirmed_sourcing_allowed");
  }

  if (GENERIC_FILLER_RE.test(thesis) || GENERIC_FILLER_RE.test(demand)) {
    failed.push("generic_filler");
  }

  const legacyDrawer = isGdiCustomerDrawerReady(opportunity);
  const elig = evaluateProspectPublicationEligibility(opportunity);
  const contactGroup =
    opportunity.contactCompletenessGroup ??
    classifyContactCompletenessGroup(opportunity);

  return {
    version: DRAWER_CONTENT_CONTRACT_VERSION,
    ok: failed.length === 0,
    failed,
    warnings,
    notes,
    optional,
    layers: {
      qualification: {
        eligibility: elig.eligibility || null,
        hotelFacing: elig.hotelFacing === true,
        enrichmentQueue: elig.enrichmentQueue === true,
        reasons: elig.reasons || [],
      },
      contactCompleteness: {
        group: contactGroup,
        route: contact,
      },
      drawerContent: {
        ok: failed.length === 0,
        failed,
      },
      drawerQuality: {
        ok: substantiveFailed.length === 0,
        failed: substantiveFailed,
        uniqueSectionProse: uniqueBlobs.size,
        hotelSpecificTokenPresent: HOTEL_SPECIFIC_TOKEN_RE.test(
          `${thesis} ${fitRationale}`
        ),
      },
      legacyDrawerReady: legacyDrawer,
    },
    fieldsPresent: {
      account: Boolean(account),
      occasion: Boolean(occasion),
      cohort: Boolean(cohort),
      timing: Boolean(timing),
      location: Boolean(location),
      opportunityType: Boolean(opportunityType),
      thesis: Boolean(thesis) && !isGenericThesis(thesis),
      fitRationale: Boolean(fitRationale) && !isGenericThesis(fitRationale),
      explainableFitScore: hasExplainableFitScore,
      whyNow: Boolean(whyNow),
      sourcing: Boolean(sourcing),
      nextAction: Boolean(nextAction),
      evidence: evidencePresent(opportunity),
      eventEvidenceBeyondStaff: hasEventSupportingSource(opportunity),
      contactRoute: contact.hasRoute,
      contactRelevance: Boolean(contact.relevance),
      timingEvidenceStatus,
    },
    note:
      opts.note ||
      "Qualification, contact completeness, drawer content, and drawer quality are reported separately.",
  };
}

export function reportDrawerCompleteness(opportunities = [], opts = {}) {
  const results = (opportunities || []).map((o) => ({
    id: o.id || o.opportunityId || o.cardId || null,
    organizationName: o.organizationName || o.company || o.account || null,
    title: o.title || o.opportunityName || null,
    prospectReviewSection: o.prospectReviewSection || null,
    prospectHotelFacing: o.prospectHotelFacing === true,
    evaluation: evaluateDrawerContentCompleteness(o, opts),
  }));
  const facing = opts.facingOnly
    ? results.filter((r) => r.prospectHotelFacing !== false)
    : results;
  const pass = facing.filter((r) => r.evaluation.ok).length;
  const fail = facing.length - pass;
  return {
    version: DRAWER_CONTENT_CONTRACT_VERSION,
    hotelId: opts.hotelId || null,
    total: facing.length,
    pass,
    fail,
    results: facing,
  };
}

/**
 * Map a Parallel lab San Diego card into a shape the drawer contract can score.
 */
export function mapLabCardToDrawerShape(card = {}, hotelId) {
  const c = card.contact || {};
  return {
    id: card.cardId || card.dedupeKey,
    hotelId,
    organizationName: card.account,
    title: text(card.timing?.raw) || card.account,
    travelingCohort: card.travelingCohort || null,
    eventStartDate: card.timing?.start || null,
    eventEndDate: card.timing?.end || null,
    eventCity: null,
    destinationStatus: null,
    demandSignal: card.demandSignal,
    summaryWhat: card.demandSignal,
    hotelOpportunityThesis: card.whyWyndhamFits,
    fitRationale: card.whyWyndhamFits,
    fitExplanation: card.whyWyndhamFits,
    sourcingStatus: card.sourcingStatus,
    venueSourcingStatusLabel: /open|housing|block|rfp/i.test(
      String(card.sourcingStatus || "")
    )
      ? "Housing channel observed — supplier openness unconfirmed"
      : "Hotel sourcing unconfirmed",
    materialLimitations: card.materialLimitations || [],
    recommendedFirstAction: card.recommendedFirstAction,
    recommendedAction: card.recommendedFirstAction,
    evidenceLinks: card.evidenceLinks || [],
    officialSource: (card.evidenceLinks || [])[0] || null,
    sources: (card.evidenceLinks || []).map((url) => ({
      url,
      verificationDate: card.completedAt || null,
    })),
    primaryContact: {
      name: c.name || null,
      email: c.email || c.functionalEmail || null,
      phone: c.phone || c.switchboard || null,
      role: c.role || null,
      relevanceEvidence: c.relevanceEvidence || c.role || null,
    },
    functionalContactEmail: c.functionalEmail || null,
    noveltyVsBaseline: card.noveltyVsBaseline || "UNKNOWN",
    peakRoomEstimate: null,
    roomDemandStatus: "UNKNOWN",
    opportunityQualification: "QUALIFIED",
    opportunityType: "OVERFLOW_HOUSING",
    // Do not invent explainable scores from lab
    hotelFitScore: null,
    whyNow: card.demandSignal,
    customerEnrichmentVersion: "gdi_customer_enrichment_v1",
    discoverySource: "gdi_parallel_lab_replay_v1",
    venueHistoryNote: "Not researched in lab card freeze.",
    competitiveContextNote: "Not researched in lab card freeze.",
    reactivationNote: "Not applicable for this lab card.",
  };
}

export {
  isGenericThesis,
  hasEventSupportingSource,
  sourceUrls,
  GENERIC_THESIS_RE,
  STAFF_ONLY_SOURCE_RE,
};
