/**
 * Demand class + contact-route quality for Event-to-Demand conversion.
 * Never invents lodging, buyers, or attendance.
 */

export const EVENT_DEMAND_CLASS = Object.freeze({
  CONFIRMED: "CONFIRMED", // source-supported lodging/housing or contracted rooms
  INFERRED: "INFERRED", // verified event + org + mechanism rationale; no room block
  SPECULATIVE: "SPECULATIVE", // weak / market signal only
});

export const CONTACT_ROUTE_QUALITY = Object.freeze({
  DIRECT_SALES: "DIRECT_SALES", // business email / phone / named buyer with route
  ORGANIZATION_SALES: "ORGANIZATION_SALES", // meetings/contact/enquiry/exhibitor form path
  EVENT_LISTING_ONLY: "EVENT_LISTING_ONLY", // calendar / what's-on / fair landing — not sales
  NONE: "NONE",
});

const SALES_PATH_RE =
  /\/(contact|contacts|enquiry|inquiry|get-in-touch|meetings?|events?(?:-team)?|exhibit(?:ors?)?|sponsor|sponsorship|groups?|rfp|book(?:ing)?|reservations?|plan-an-event)(?:\/|$|\?|#)/i;

const LISTING_PATH_RE =
  /\/(whats-on|whats_on|calendar|events?\/(?:list|archive)|things-to-do|fairs?\/[^/]+\/?$)(?:\/|$|\?|#)?/i;

const HOMEPAGE_RE =
  /^https?:\/\/[^/]+\/?(?:en\/|es\/)?(?:home\/?)?(?:index\.(?:html?|php))?$/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Classify a URL as sales vs listing vs homepage.
 */
export function classifyContactUrlQuality(url = "") {
  const u = text(url);
  if (!u || !/^https?:\/\//i.test(u)) return CONTACT_ROUTE_QUALITY.NONE;
  if (HOMEPAGE_RE.test(u)) return CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY;
  if (/excel\.london\/whats-on|olympia\.london\/whats-on|visitlondon\.com\/things-to-do/i.test(u)) {
    return CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY;
  }
  if (SALES_PATH_RE.test(u)) return CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES;
  // Fair detail pages (frieze.com/fairs/...) are event evidence, not sales desks
  if (/\/fairs?\//i.test(u) && !SALES_PATH_RE.test(u)) {
    return CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY;
  }
  if (LISTING_PATH_RE.test(u)) return CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY;
  // Deep non-homepage org pages without sales keywords stay listing-grade
  return CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY;
}

/**
 * Classify contact object quality for conversion / publication.
 */
export function classifyContactRouteQuality(contact = {}) {
  const c = contact || {};
  if (c.businessEmail || c.namedBuyerEmail || c.businessPhone || c.namedBuyerPhone) {
    return CONTACT_ROUTE_QUALITY.DIRECT_SALES;
  }
  if (c.personName && (c.role || c.title) && (c.contactFormUrl || c.businessEmail)) {
    return CONTACT_ROUTE_QUALITY.DIRECT_SALES;
  }
  const urls = [
    c.contactFormUrl,
    c.organizationRouteUrl,
    c.intermediary?.url,
  ].filter(Boolean);
  for (const u of urls) {
    if (classifyContactUrlQuality(u) === CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES) {
      return CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES;
    }
  }
  if (c.intermediary?.type === "organization_sales_route" && c.contactFormUrl) {
    const q = classifyContactUrlQuality(c.contactFormUrl);
    if (q === CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES) return q;
  }
  if (urls.length) return CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY;
  return CONTACT_ROUTE_QUALITY.NONE;
}

/**
 * Usable sales route for publication / Ready path — excludes event listings.
 */
export function isUsableOrganizationSalesRoute(contact = {}) {
  const q = classifyContactRouteQuality(contact);
  return (
    q === CONTACT_ROUTE_QUALITY.DIRECT_SALES ||
    q === CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES
  );
}

/**
 * Research-lead route: verified event page evidence (not a sales desk).
 */
export function isEventListingResearchLead(contact = {}) {
  return classifyContactRouteQuality(contact) === CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY;
}

/**
 * Classify commercial demand certainty from prospect fields.
 */
export function classifyEventDemandClass(opportunity = {}) {
  const scale = opportunity.demandScale || {};
  const housing =
    scale.confirmedRoomBlock === true ||
    scale.basis === "source_supported_lodging_or_attendance" ||
    /official\s+hotel|room\s+block|housing\s+bureau/i.test(
      text(opportunity.housingArrangements || opportunity.localConnection)
    );
  if (scale.supported === true && (housing || Number(scale.estimatedPeakRooms) > 0)) {
    return EVENT_DEMAND_CLASS.CONFIRMED;
  }
  if (
    opportunity.futureDemand?.supported === true &&
    opportunity.hotelFit?.supported === true &&
    opportunity.eventDemandMechanism &&
    opportunity.eventDemandMechanism !== "SPECULATIVE_MARKET_SIGNAL" &&
    text(opportunity.localConnection || opportunity.hotelFitRationale)
  ) {
    return EVENT_DEMAND_CLASS.INFERRED;
  }
  return EVENT_DEMAND_CLASS.SPECULATIVE;
}

/**
 * Suggest next sales action from demand class + contact quality (no invented claims).
 */
export function suggestNextSalesAction({
  demandClass,
  contactQuality,
  mechanism,
  organizationName,
  eventName,
} = {}) {
  const org = text(organizationName) || "the organizer";
  const event = text(eventName) || "the event";
  const mech = text(mechanism) || "lodging";

  if (demandClass === EVENT_DEMAND_CLASS.SPECULATIVE) {
    return {
      action: "INTERNAL_MONITOR",
      detail: `Keep ${event} as an internal market signal until an organization and mechanism are evidence-backed.`,
    };
  }
  if (contactQuality === CONTACT_ROUTE_QUALITY.DIRECT_SALES) {
    return {
      action: "OUTREACH_NAMED_OR_DIRECT",
      detail: `Contact the established route for ${org} about ${mech} around ${event}; confirm dates and room need without inventing a block.`,
    };
  }
  if (contactQuality === CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES) {
    return {
      action: "OUTREACH_ORG_SALES_DESK",
      detail: `Use the verified organization meetings/events contact path for ${org}; position Nobu Portman Square for ${mech} related to ${event}.`,
    };
  }
  if (contactQuality === CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY) {
    return {
      action: "FIND_ORG_SALES_ROUTE",
      detail: `Event listing is evidence only — locate ${org} meetings/exhibitor/sponsorship contact page or email before sales outreach on ${event}.`,
    };
  }
  return {
    action: "SOURCE_CONTACT",
    detail: `Identify a usable sales route for ${org} before hotel-facing publication.`,
  };
}

/**
 * Nobu Portman Square fit narrative from territory + mechanism (evidence-based).
 */
export function buildNobuFitRationale({
  hotel = {},
  territory,
  mechanism,
  venue,
  demandClass,
} = {}) {
  const rooms = Number(hotel.capabilityProfile?.totalGuestrooms);
  const inventory = Number.isFinite(rooms) && rooms > 0 ? `${rooms} keys` : "unverified guestroom inventory";
  const parts = [];
  if (territory === "TERRITORY_CORE" || territory === "TERRITORY_NEARBY") {
    parts.push(
      `Venue catchment (${venue || territory}) is within Nobu Portman Square's Marylebone / West End walking demand territory.`
    );
  } else if (territory === "TERRITORY_STRETCH") {
    parts.push(
      `Primary venue (${venue || "stretch venue"}) is stretch vs Marylebone; fit rests on ${mechanism} spillover overnight — not mass show lodging.`
    );
  } else {
    parts.push(
      `Citywide / competitive London positioning; credible only for compact ${mechanism} demand, not mass blocks.`
    );
  }
  if (/VIP|EXECUTIVE|SMALL_GROUP|OFFSITE/i.test(String(mechanism || ""))) {
    parts.push(
      `Luxury lifestyle inventory (${inventory}) and private dining / boardroom stock align with ${mechanism} rather than large exhibition room blocks.`
    );
  }
  if (demandClass === EVENT_DEMAND_CLASS.INFERRED) {
    parts.push(
      "Demand is inferred from verified future activity and mechanism rationale; no confirmed room block is claimed."
    );
  } else if (demandClass === EVENT_DEMAND_CLASS.CONFIRMED) {
    parts.push("Confirmed lodging/housing evidence is source-supported.");
  }
  return parts.join(" ");
}
