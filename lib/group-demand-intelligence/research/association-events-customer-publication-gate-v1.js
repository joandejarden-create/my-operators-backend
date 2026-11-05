/**
 * Association / Conference / Events — customer publication / local staging gate V1.
 *
 * Rejects generic hotel/venue pages, placeholder titles, truncated discovery-only
 * snippets, and records lacking verified event identity + hotel-demand evidence.
 *
 * Zero paid. Used before customer-facing local shadow staging and FullE2E cert.
 */
export const ASSOCIATION_EVENTS_CUSTOMER_PUBLICATION_GATE_V1 =
  "association-events-customer-publication-gate-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

const HOTEL_VENUE_NAME_RE =
  /\b(hotel|novotel|hilton|marriott|ihg|hyatt|ibis|radisson|sheraton|westin|intercontinental|de vere|may ?fair|bankside|accommodation suggestions?|hotel list|rooms?\s*&\s*event|event venue)\b/i;

const PLACEHOLDER_TITLE_RE =
  /^(travel|home|members[’']?\s+conference\s+accommodation|hotel list\b|2026 conference|contrata\b|hubspot testimonial|company incentive trips|pco\b|conferences in\b)/i;

const GENERIC_SNIPPET_RE =
  /\bis a Hospitality company\b|Welcome to one of the London|Discover the contemporary elegance|Best price guarantee|Customer review rating/i;

const DIRECTORY_PAGE_RE =
  /\b(london convention bureau|conferences in .+,\s*uk\s+20\d{2}|page \d+)\b/i;

/**
 * @returns {{ ok: boolean, code: string|null, reasons: string[], defects: string[] }}
 */
export function evaluateAssociationEventsCustomerPublicationGate(opp = {}, opts = {}) {
  const reasons = [];
  const defects = [];
  const title = text(
    opp.displayTitle || opp.eventName || opp.title || opp.organizationName || "",
  );
  const org = text(opp.organizationName || opp.organizer || "");
  const what = text(opp.whatIsHappening || opp.summaryWhat || "");
  const demand = text(
    opp.proposedHotelUse ||
      opp.hotelOpportunityThesis ||
      opp.commercialIntelligenceBrief?.potentialBusinessForThisHotel ||
      "",
  );
  const fit = text(
    opp.hotelFitRationale ||
      opp.whyThisHotel ||
      opp.commercialIntelligenceBrief?.whyThisHotel ||
      "",
  );
  const sourceUrl = text(opp.sourceUrl || opp.sources?.[0]?.url || "");
  const eventIdentity = text(
    opp.verifiedEventName ||
      opp.eventIdentity ||
      opp.eventDisplayName ||
      opp.programmeName ||
      "",
  );
  const contactName = text(
    opp.contactName || opp.primaryContact?.name || opp.primaryContact?.fullName || "",
  );
  const deepComplete =
    opp.deepResearchComplete === true ||
    opp.deepResearchStatus === "completed" ||
    opts.deepResearchComplete === true;

  if (!title || title.length < 8) {
    defects.push("missing_or_thin_title");
    reasons.push("Missing substantive event or programme title.");
  }

  if (PLACEHOLDER_TITLE_RE.test(title) || PLACEHOLDER_TITLE_RE.test(org)) {
    defects.push("placeholder_event_title");
    reasons.push("Title looks like a placeholder, directory page, or marketing headline.");
  }

  // Hotel/venue page used as the opportunity identity (not a named congress/event).
  const identityIsVenue =
    HOTEL_VENUE_NAME_RE.test(title) &&
    !/\b(congress|conference|summit|symposium|forum|annual meeting|world congress|wlic|eurochamp)\b/i.test(
      title,
    );
  if (identityIsVenue) {
    defects.push("venue_or_hotel_as_event_identity");
    reasons.push(
      "Hotel or venue marketing identity was used as the event name — not a verified group demand opportunity.",
    );
  }

  if (DIRECTORY_PAGE_RE.test(title) || DIRECTORY_PAGE_RE.test(sourceUrl)) {
    defects.push("directory_or_bureau_page");
    reasons.push("Source appears to be a destination directory / bureau listing, not a specific event.");
  }

  if (!eventIdentity && identityIsVenue) {
    defects.push("missing_verified_event_identity");
    reasons.push("No verified event/programme identity distinct from the venue page.");
  }

  if (what && GENERIC_SNIPPET_RE.test(what)) {
    defects.push("generic_hotel_marketing_snippet");
    reasons.push("What is happening is a hotel marketing snippet, not event demand evidence.");
  }

  if (!what || what.length < 80) {
    defects.push("sparse_what_is_happening");
    reasons.push("What is happening is missing or too sparse for customer delivery.");
  }

  if (!demand || demand.length < 60) {
    defects.push("missing_hotel_demand_thesis");
    reasons.push("No credible hotel-specific demand mechanism for customer delivery.");
  } else if (/programme existence ≠ contracted hotel demand/i.test(demand) && demand.length < 180) {
    defects.push("boilerplate_only_hotel_demand");
    reasons.push("Hotel demand text is boilerplate without event-specific mechanism.");
  }

  if (!fit || fit.length < 40) {
    defects.push("missing_hotel_fit");
    reasons.push("No genuine hotel-specific fit thesis.");
  }

  // Existing hotel named on a conference site ≠ automatic room-block opportunity.
  if (
    /hotel list|accommodation suggestions?|members[’']?\s+conference\s+accommodation|contrata\b/i.test(
      title,
    )
  ) {
    defects.push("hotel_list_page_not_room_block");
    reasons.push(
      "Accommodation/hotel-list pages do not automatically create a room-block opportunity for this hotel.",
    );
  }

  if (opts.requireNamedContact === true && !contactName) {
    defects.push("missing_named_contact");
    reasons.push("No relevant verified named contact for buyer-eligible staging.");
  }

  if (opts.requireDeepResearch === true && !deepComplete) {
    defects.push("deep_research_missing");
    reasons.push("Deep Research incomplete — discovery-only rows cannot certify Ready.");
  }

  if (opp.providerArtifactTruncated === true || opp.researchArtifactIncomplete === true) {
    defects.push("truncated_or_incomplete_provider_artifact");
    reasons.push("Provider artifact marked truncated/incomplete.");
  }

  // Founder-rejected campaign stamp — never re-stage to customer facing.
  if (
    opp.founderCustomerVisibilityDisable === true ||
    opp.campaignCustomerFacingDisabled === true ||
    text(opp.founderVisibilityDisableReason) === "FOUNDER_REJECTED_SECOND_WAVE_QUALITY"
  ) {
    defects.push("founder_rejected_campaign_quality");
    reasons.push("Founder rejected this campaign staging (FOUNDER_REJECTED_SECOND_WAVE_QUALITY).");
  }

  const ok = defects.length === 0;
  return {
    version: ASSOCIATION_EVENTS_CUSTOMER_PUBLICATION_GATE_V1,
    ok,
    code: ok ? null : defects[0],
    reasons,
    defects,
    title: title || null,
    eventIdentity: eventIdentity || null,
  };
}

export function assertAssociationEventsCustomerPublicationGate(opp = {}, opts = {}) {
  const result = evaluateAssociationEventsCustomerPublicationGate(opp, opts);
  if (!result.ok) {
    const err = new Error(
      `association_events_customer_publication_gate_failed:${result.code}`,
    );
    err.code = "association_events_customer_publication_gate_failed";
    err.gate = result;
    throw err;
  }
  return result;
}
