/**
 * Events canonical drawer model V1 — sections 2–10 match Scaling gold structure.
 *
 * Section 1 (Opportunity overview) remains the structured Events seven-field grid
 * rendered separately in the UI — never replaced by Scaling narrative overview.
 *
 * Presentation only. Does not invent dates, attendance, RFPs, or paid research.
 */
import {
  SCALING_CANONICAL_SECTION_IDS,
  SCALING_CANONICAL_SECTION_TITLES,
} from "./scaling-drawer-normalized-model-v1.js";
import { isAssociationConferenceEventsOpportunity } from "./events-opportunity-overview-v1.js";
import { isScalingCompaniesSegment } from "./scaling-drawer-presentation-contract-v1.js";

export const EVENTS_DRAWER_NORMALIZED_MODEL_V1 =
  "gdi-events-drawer-normalized-model-v1";

export const EVENTS_CANONICAL_SECTION_IDS = SCALING_CANONICAL_SECTION_IDS;
export const EVENTS_CANONICAL_SECTION_TITLES = SCALING_CANONICAL_SECTION_TITLES;

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanList(items) {
  if (!Array.isArray(items)) return [];
  return items
    .map((t) => text(t))
    .filter(Boolean)
    .filter((t) => !/^—+$/.test(t) && t !== "-" && t !== "•");
}

function approachText(brief) {
  if (!brief || brief.recommendedSalesApproach == null) return "";
  const a = brief.recommendedSalesApproach;
  if (typeof a === "string") return text(a);
  if (typeof a === "object") {
    return text(a.nextAction || a.openingApproach || "");
  }
  return "";
}

function qualificationQuestions(brief, opportunity = {}) {
  const a = brief?.recommendedSalesApproach;
  let qs = [];
  if (Array.isArray(brief?.qualificationQuestions)) {
    qs = cleanList(brief.qualificationQuestions);
  }
  if (!qs.length && a && typeof a === "object" && Array.isArray(a.qualificationQuestions)) {
    qs = cleanList(a.qualificationQuestions);
  }
  if (!qs.length && Array.isArray(opportunity.qualificationQuestions)) {
    qs = cleanList(opportunity.qualificationQuestions);
  }
  if (
    !qs.length &&
    Array.isArray(opportunity.suggestedQualificationQuestions)
  ) {
    qs = cleanList(opportunity.suggestedQualificationQuestions);
  }
  if (!qs.length) {
    const event =
      text(opportunity.eventName || opportunity.eventDisplayName || opportunity.title) ||
      "this edition";
    const org =
      text(
        opportunity.displayCompanyName ||
          opportunity.organizationName ||
          opportunity.companyName,
      ) || "the organizer";
    qs = [
      `Who owns hotel / housing sourcing for ${event} — association staff, a PCO, or a housing bureau?`,
      `Is any room block or overflow still open for ${org}, or is lodging already fully committed elsewhere?`,
    ];
  }
  return qs.slice(0, 4);
}

function dedupeSources(sources) {
  const seen = new Set();
  const out = [];
  for (const s of Array.isArray(sources) ? sources : []) {
    const url = text(typeof s === "string" ? s : s?.url || s?.href);
    if (!url) continue;
    const key = url.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) continue;
    seen.add(key);
    const obj = typeof s === "object" && s ? s : {};
    out.push({
      url,
      title: text(obj.title || obj.sourceTitle) || null,
      publisher: text(obj.publisher || obj.entityAttributed) || null,
      publicationDate: obj.publicationDate || obj.publishedDate || null,
      establishes: text(obj.establishes || obj.relevance || obj.claim) || null,
    });
  }
  return out;
}

function hotelContext(opportunity = {}, opts = {}) {
  const hotelId = text(opportunity.hotelId || opts.hotelId);
  const name =
    text(
      opts.hotelDisplayName ||
        opportunity.hotelDisplayName ||
        opportunity.hotelName ||
        opportunity.targetHotelName,
    ) || "this hotel";
  const isCondesa = hotelId === "recIEcSWuhSCoNLFN" || /condesa/i.test(name);
  const isChapultepec =
    hotelId === "recterKmNPdntCm0v" || /chapultepec/i.test(name);
  return { hotelId, name, isCondesa, isChapultepec };
}

/**
 * Evidence-bounded advantages / limitations when CIB lists are thin.
 * Never invents travel minutes, exclusive contracts, or room-night volumes.
 */
export function deriveEventsWhyThisHotelLists(opportunity = {}, opts = {}) {
  const cib = opportunity.commercialIntelligenceBrief || {};
  const aegb = opportunity.associationEventsGoldBrief || {};
  const peakLabel = text(
    opportunity.eventsOpportunityOverview?.peakRooms ||
      opportunity.peakRoomsLabel ||
      opportunity.peakRoomsStatus ||
      ""
  );
  // Empty / unpublished / unknown ⇒ treat as not published (scrub size signals)
  const peakPublished =
    Boolean(peakLabel) &&
    !/not yet published|unpublished|unknown|tbd|not established/i.test(peakLabel);
  const scrubSizeSignal = (list) =>
    cleanList(list)
      .map((t) =>
        String(t || "")
          .replace(/\bSize signal:[^.]*\.?/gi, "")
          .replace(/\bgdi_hotel_[a-z0-9_]+\b/gi, "this hotel")
          .replace(/\brec[A-Za-z0-9]{10,}\b/g, "this hotel")
          .replace(/\s{2,}/g, " ")
          .trim()
      )
      .filter(Boolean)
      // Drop unpublished peak-room numeric size signals entirely
      .filter((t) => peakPublished || !/\b\d+\s*peak rooms\b/i.test(t));

  let advantages = scrubSizeSignal(
    cib.whyThisHotelAdvantages ||
      opportunity.whyThisHotelAdvantages ||
      aegb.whyThisHotelAdvantages,
  );
  let limitations = scrubSizeSignal(
    cib.whyThisHotelLimitations ||
      opportunity.whyThisHotelLimitations ||
      aegb.whyThisHotelLimitations,
  );
  if (advantages.length || limitations.length) {
    return { advantages, limitations };
  }

  const hx = hotelContext(opportunity, opts);
  const overview = opportunity.eventsOpportunityOverview || {};
  const sourcing = text(
    overview.hotelSourcing ||
      opportunity.hotelSourcingStatusLabel ||
      opportunity.venueSourcingStatusLabel,
  );
  const loc = text(overview.location || opportunity.eventLocationSummary || "");
  const venue = text(
    opportunity.verifiedVenue ||
      opportunity.incumbentVenue ||
      overview.venueConfirmationNote ||
      "",
  );

  if (hx.isChapultepec) {
    advantages.push(
      `${hx.name} sits in the Chapultepec / Reforma / Polanco meetings corridor of Mexico City — relevant when the edition’s attendees or programme create lodging or meetings demand in that geography.`,
    );
    advantages.push(
      "Urban meetings hotel product with published guestroom and meeting inventory suitable for delegation overflow or compact programme blocks when sourcing is still open.",
    );
  } else if (hx.isCondesa) {
    advantages.push(
      `${hx.name} is an oceanfront Cancún Hotel Zone all-inclusive resort with meetings capacity — aligned to incentive, congress-delegate, and resort-format programmes when organizers shortlist that product.`,
    );
  } else {
    advantages.push(
      `${hx.name} may compete for overnight or meeting needs tied to this programme if lodging geography and product fit are confirmed with the organizer.`,
    );
  }

  if (/Venue Confirmed/i.test(sourcing)) {
    limitations.push(
      "A venue is already confirmed elsewhere — treat the present edition as competitive capture / overflow / ancillary lodging unless a room block or housing RFP is separately evidenced as open.",
    );
  } else if (/Historical Venue Only/i.test(sourcing)) {
    limitations.push(
      "Only historical venue pattern is evidenced — do not pitch the completed edition as an open booking; focus on the next cycle once announced.",
    );
  } else if (/Hotel Sourcing Unconfirmed/i.test(sourcing)) {
    limitations.push(
      "Hotel sourcing status is unconfirmed — a future edition does not prove an open RFP or room-block opportunity until the organizer confirms accessibility.",
    );
  }

  if (hx.isCondesa) {
    limitations.push(
      "All-inclusive resort format may be a mismatch for organizers seeking European-plan urban hotels or convention-center-adjacent inventory — confirm product preference before pitching.",
    );
  }
  if (hx.isChapultepec && /Cancún|Cancun|Riviera Maya|Quintana Roo/i.test(loc)) {
    limitations.push(
      "Event geography is outside Mexico City — Chapultepec is not a destination-fit property for this edition unless a distinct CDMX programme element is evidenced.",
    );
  }
  if (venue && !/unconfirmed|tbd|not yet/i.test(venue)) {
    limitations.push(
      `Incumbent or published venue context: ${venue.slice(0, 160)}${venue.length > 160 ? "…" : ""} — use as competitive context, not as proof ${hx.name} is selected.`,
    );
  }
  if (!limitations.length) {
    limitations.push(
      "Exact room-block size, decision owner, and contracting window remain unverified from public research.",
    );
  }

  return { advantages: advantages.slice(0, 5), limitations: limitations.slice(0, 5) };
}

function enrichPotentialBusiness(opportunity = {}, body = "") {
  let pot = text(body);
  // Repair truncated template lead before accepting source body.
  if (/^sized\s+group\s+opportunity\.\s*/i.test(pot)) {
    pot = pot.replace(/^sized\s+group\s+opportunity\.\s*/i, "").trim();
    if (pot) pot = pot.charAt(0).toUpperCase() + pot.slice(1);
  }
  const genericOnly =
    !pot ||
    /^potential guestrooms and\/?or meeting/i.test(pot) ||
    pot.length < 80;
  if (!genericOnly) return pot;

  const event =
    text(opportunity.eventName || opportunity.eventDisplayName || opportunity.title) ||
    "this event";
  const org =
    text(
      opportunity.displayCompanyName ||
        opportunity.organizationName ||
        opportunity.companyName,
    ) || "the organizer";
  const sourcing = text(
    opportunity.eventsOpportunityOverview?.hotelSourcing ||
      opportunity.hotelSourcingStatusLabel,
  );
  const attendance = text(opportunity.eventsOpportunityOverview?.attendance);
  const peak = text(opportunity.eventsOpportunityOverview?.peakRooms);

  const bits = [
    `Plausible hotel business tied to ${event} includes guestrooms and/or group room blocks for delegates, speakers, organizers and exhibitors; meetings or catering only when the programme still needs off-site or overflow space.`,
  ];
  if (/Venue Confirmed/i.test(sourcing)) {
    bits.push(
      "Because a venue is already confirmed elsewhere, the realistic pathway is lodging overflow, faculty/housing packages, or relationship-building for a future edition — not claiming the contracted venue is available to rebook.",
    );
  } else if (/Historical Venue Only/i.test(sourcing)) {
    bits.push(
      "Historical hosting pattern supports a future competitive-capture conversation once the next edition’s sourcing window opens; it does not create a live room-night claim for a completed edition.",
    );
  } else {
    bits.push(
      `Confirm with ${org} whether lodging is centralized (association / PCO / housing bureau) or individual booking before proposing a contracted block.`,
    );
  }
  if (/not yet published/i.test(attendance) || !attendance) {
    bits.push(
      "Attendance is not yet published for this edition — do not treat historical headcount as confirmed future volume.",
    );
  }
  if (/not yet published/i.test(peak) || !peak) {
    bits.push(
      "Peak rooms are not published — any room-night estimate remains a hypothesis until the organizer releases housing figures.",
    );
  }
  return bits.join(" ");
}

/**
 * Build Events drawer model (same section IDs as Scaling). Section 1 body is a
 * pointer only — UI must render structured Events overview HTML instead.
 */
export function buildEventsDrawerNormalizedModel(opportunity = {}, opts = {}) {
  if (
    !isAssociationConferenceEventsOpportunity(opportunity) &&
    !opts.force
  ) {
    return null;
  }
  if (isScalingCompaniesSegment(opportunity) && !opts.force) {
    return null;
  }

  const cib = opportunity.commercialIntelligenceBrief || {};
  const aegb = opportunity.associationEventsGoldBrief || {};
  const brief = cib;
  const hx = hotelContext(opportunity, opts);
  const overview = opportunity.eventsOpportunityOverview || {};
  const org =
    text(
      overview.organizerName ||
        opportunity.displayCompanyName ||
        opportunity.organizationName ||
        opportunity.companyName,
    ) || "Organizer";
  const eventName =
    text(
      overview.eventName ||
        opportunity.eventDisplayName ||
        opportunity.eventName ||
        opportunity.title,
    ) || "Event";

  let whatHappening = text(
    cib.whatIsHappening ||
      aegb.eventHistoryAndActivity ||
      opportunity.whatIsHappening ||
      opportunity.summaryWhat,
  );
  // Title / pipeline stubs are not executive narratives.
  if (
    !whatHappening ||
    whatHappening.length < 80 ||
    /^.{0,120}—\s*EXHIBITOR BLOCK\s*$/i.test(whatHappening) ||
    whatHappening.toLowerCase() === text(opportunity.title || eventName).toLowerCase()
  ) {
    whatHappening = `${eventName} is tracked as an association / conference / events opportunity for ${org}; verify edition status, destination and sourcing before treating lodging demand as confirmed.`;
  }

  const activityDate = text(
    cib.evidenceAndUncertainty?.originalActivityDate ||
      opportunity.growthEvidenceDate ||
      opportunity.sourcePublicationDate ||
      opportunity.activityChronology?.originalEventDate ||
      "",
  );

  let potential = enrichPotentialBusiness(
    opportunity,
    cib.potentialBusinessForThisHotel ||
      cib.potentialHotelBusiness ||
      aegb.potentialBusinessForHotel ||
      opportunity.potentialHotelBusiness ||
      opportunity.whyHotelDemand ||
      opportunity.summaryWhyMatters,
  );

  let whyHotel = text(
    cib.whyThisHotel || aegb.whyThisHotel || opportunity.whyThisHotel || opportunity.summaryWhyHotel,
  );
  whyHotel = whyHotel
    .replace(/\bSize signal:[^.]*\.?/gi, "")
    .replace(/\bgdi_hotel_[a-z0-9_]+\b/gi, "this hotel")
    .replace(/\brec[A-Za-z0-9]{10,}\b/g, "this hotel")
    .replace(/\s{2,}/g, " ")
    .trim();
  const action = text(
    opportunity.recommendedAction || opportunity.suggestedAction || ""
  );
  const priority = text(opportunity.priority || opportunity.cardPriority || "");
  const attendanceUnpublished = /not yet published|unpublished|unknown/i.test(
    text(opportunity.eventsOpportunityOverview?.attendance || opportunity.attendanceStatus || "")
  ) || !text(opportunity.eventsOpportunityOverview?.attendance || "");
  if (
    (/disqualified/i.test(priority) ||
      /Past cycle\s*[—–-]\s*do not pursue/i.test(action) ||
      /wrong[- ]destination|Close as wrong-destination/i.test(action) ||
      attendanceUnpublished) &&
    /\b(?:attracts?\s+over\s+\d+|large\s+number\s+of\s+attendees|significant\s+(?:room\s+)?demand|draw\s+a\s+large\s+number|expected\s+to\s+attract\s+a\s+large)\b/i.test(
      whyHotel
    )
  ) {
    whyHotel =
      "Property fit is relationship / next-cycle context only — do not treat unpublished attendance or room-demand claims as an active chase for this hotel.";
  }
  const { advantages, limitations } = deriveEventsWhyThisHotelLists(
    opportunity,
    opts,
  );
  // Distinct section purpose: if Why This Hotel duplicates Potential Business,
  // prefer advantage/limitation lists and clear the duplicate body.
  if (
    potential &&
    whyHotel &&
    potential.toLowerCase() === whyHotel.toLowerCase()
  ) {
    if (advantages.length || limitations.length) {
      whyHotel = "";
    }
  }
  if (!whyHotel && (advantages.length || limitations.length)) {
    whyHotel = "";
  }
  if (!whyHotel && !advantages.length) {
    whyHotel = `Property-specific fit for ${hx.name} depends on verified event geography and sourcing accessibility — confirm before pitching contracted space.`;
  }

  let whyActNow = text(
    cib.whyActNow ||
      opportunity.whyEstablishContactNow ||
      opportunity.whyNow ||
      opportunity.whyActNow ||
      aegb.whyDealalitySelected ||
      opportunity.summaryWhyNow,
  );
  // Why Act Now must not duplicate Potential Business verbatim.
  if (
    whyActNow &&
    potential &&
    whyActNow.toLowerCase() === potential.toLowerCase()
  ) {
    whyActNow = "";
  }
  if (!whyActNow) {
    const sourcing = text(overview.hotelSourcing);
    if (/Venue Confirmed|Host hotel contracted|Competitor venue/i.test(sourcing)) {
      whyActNow =
        "Venue is already committed elsewhere — act only if overflow lodging or a next-edition relationship is the goal; do not invent urgency for an unavailable contracted venue.";
    } else if (/Historical Venue Only/i.test(sourcing)) {
      whyActNow =
        "No immediate open sourcing window is evidenced for a completed or historical edition. Monitor for the next announced cycle before aggressive pursuit.";
    } else if (/Monitor|Past cycle|Watch/i.test(text(opportunity.recommendedAction))) {
      whyActNow =
        "Timing is watch-only until a confirmed lodging path or open sourcing signal appears — do not treat potential demand as an active chase.";
    } else {
      whyActNow =
        "Confirm whether the organizer’s planning / housing window is open for this edition. No time-critical trigger is assumed without a published RFP or housing deadline.";
    }
  }

  let facts = cleanList(
    cib.establishedFacts ||
      cib.establishedFactsAndUncertainties?.facts ||
      cib.evidenceAndUncertainty?.establishedFacts ||
      cib.evidenceAndUncertainty?.facts ||
      opportunity.establishedFacts,
  );
  let uncertainties = cleanList(
    cib.establishedFactsAndUncertainties?.uncertainties ||
      cib.evidenceAndUncertainty?.uncertainties ||
      opportunity.uncertainties ||
      opportunity.constraints,
  );
  facts = facts.filter((f) => f.length > 2);
  uncertainties = uncertainties.filter((u) => u.length > 2);
  if (!uncertainties.length) {
    uncertainties = [
      "Whether hotel / housing sourcing is still accessible for this edition",
      "Named buyer with hotel-sourcing or room-block authority",
      "Confirmed attendance and peak-room figures for the upcoming edition",
    ];
  }

  // Prefer published/certified recommendedNextStep over invented association
  // procurement boilerplate (share/auth customer parity).
  const certifiedNext = text(
    opportunity.recommendedNextStep ||
      cib.recommendedNextStep ||
      opportunity.recommendedAction ||
      opportunity.recommendedNextAction,
  );
  let nextStep =
    certifiedNext ||
    approachText(brief) ||
    text(
      typeof aegb.recommendedSalesApproach === "string"
        ? aegb.recommendedSalesApproach
        : aegb.recommendedSalesApproach?.nextAction,
    );
  const nextIsGenericAssoc =
    /meetings\s*\/\s*congress\s*\/\s*operations route|association staff\s*vs\s*PCO|Organizer TBD/i.test(
      nextStep,
    );
  if (certifiedNext && nextIsGenericAssoc) {
    nextStep = certifiedNext;
  }
  if (!nextStep || /contact organizer to discuss opportunity/i.test(nextStep)) {
    nextStep = `Open with ${org}'s meetings / congress / operations route for ${eventName}: introduce ${hx.name}, ask who owns hotel sourcing (association staff vs PCO vs housing), and whether a host or overflow decision is still open. Do not imply a contracted booking requirement that is not evidenced.`;
  }
  const questions = qualificationQuestions(brief, opportunity);

  const drawerEvidence = opportunity.supportingEvidenceDrawer || null;
  const sources = dedupeSources(
    drawerEvidence?.sources?.length
      ? drawerEvidence.sources
      : brief.evidenceAndUncertainty?.sources?.length
        ? brief.evidenceAndUncertainty.sources
        : opportunity.primaryEvidenceSources?.length
          ? opportunity.primaryEvidenceSources
          : opportunity.sources,
  );
  const sourcesFull = dedupeSources(
    brief.evidenceAndUncertainty?.sourcesFull ||
      opportunity.sourcesFull ||
      sources,
  );
  const primarySources = sources.slice(0, 8);

  const contactPathLabel =
    text(opportunity.commercialContactPathLabel) ||
    text(opportunity.scalingTwoContactPresentation?.primary?.commercialContactPathLabel) ||
    null;

  return {
    version: EVENTS_DRAWER_NORMALIZED_MODEL_V1,
    overviewMode: "events_structured_grid_v1",
    sectionIds: [...EVENTS_CANONICAL_SECTION_IDS],
    sectionTitles: { ...EVENTS_CANONICAL_SECTION_TITLES },
    organizationName: org,
    eventName,
    opportunityTypeLabel:
      text(opportunity.eventTypeLabel || opportunity.opportunityTypeLabel) ||
      "Association / Conference / Events",
    title: text(opportunity.displayTitle || opportunity.title) || eventName,
    sections: {
      opportunity_overview: {
        id: "opportunity_overview",
        title: EVENTS_CANONICAL_SECTION_TITLES.opportunity_overview,
        mode: "events_structured_grid_v1",
        // UI must render eventsOpportunityOverview HTML — not this narrative body
        identityLine: text(overview.identityLine) || `${org} — ${eventName}`,
        useStructuredEventsOverview: true,
      },
      what_is_happening: {
        id: "what_is_happening",
        title: EVENTS_CANONICAL_SECTION_TITLES.what_is_happening,
        body: whatHappening,
        activityDate: activityDate || null,
      },
      potential_business: {
        id: "potential_business",
        title: EVENTS_CANONICAL_SECTION_TITLES.potential_business,
        body: potential,
        inferred: cleanList(opportunity.inferredHotelDemand),
      },
      why_this_hotel: {
        id: "why_this_hotel",
        title: EVENTS_CANONICAL_SECTION_TITLES.why_this_hotel,
        body: whyHotel,
        advantages,
        limitations,
      },
      why_act_now: {
        id: "why_act_now",
        title: EVENTS_CANONICAL_SECTION_TITLES.why_act_now,
        body: whyActNow,
      },
      facts_and_uncertainties: {
        id: "facts_and_uncertainties",
        title: EVENTS_CANONICAL_SECTION_TITLES.facts_and_uncertainties,
        facts,
        uncertainties,
        factsEmptyNote:
          facts.length === 0
            ? "No separately listed established facts beyond the activity narrative above."
            : null,
        uncEmptyNote: null,
      },
      who_to_contact: {
        id: "who_to_contact",
        title: EVENTS_CANONICAL_SECTION_TITLES.who_to_contact,
        contactPathLabel,
      },
      recommended_next_step: {
        id: "recommended_next_step",
        title: EVENTS_CANONICAL_SECTION_TITLES.recommended_next_step,
        body: nextStep,
        qualificationQuestions: questions,
      },
      supporting_evidence: {
        id: "supporting_evidence",
        title: EVENTS_CANONICAL_SECTION_TITLES.supporting_evidence,
        sources: primarySources.map((s) => ({
          ...s,
          establishes:
            s.establishes && s.establishes.length <= 120 ? s.establishes : null,
        })),
        sourcesFull,
        evidenceGroups: [],
        activityDate: activityDate || null,
        researchVerificationDate:
          text(
            opportunity.evidenceFreshnessAt ||
              opportunity.activityChronology?.researchVerificationDate ||
              cib.evidenceAndUncertainty?.researchVerificationDate,
          ) || null,
      },
      hotel_validation: {
        id: "hotel_validation",
        title: EVENTS_CANONICAL_SECTION_TITLES.hotel_validation,
      },
    },
  };
}

export function extractEventsSectionIdsFromHtml(html) {
  const ids = [];
  const re = /data-section-id=["']([a-z_]+)["']/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    ids.push(m[1]);
  }
  return ids;
}

export function assertEventsCanonicalSectionOrder(sectionIds) {
  const expected = EVENTS_CANONICAL_SECTION_IDS;
  const got = Array.isArray(sectionIds) ? sectionIds : [];
  if (got.length !== expected.length) {
    return {
      ok: false,
      reason: `expected ${expected.length} sections, got ${got.length}`,
    };
  }
  for (let i = 0; i < expected.length; i++) {
    if (got[i] !== expected[i]) {
      return {
        ok: false,
        reason: `section[${i}] expected ${expected[i]}, got ${got[i]}`,
      };
    }
  }
  return { ok: true };
}
