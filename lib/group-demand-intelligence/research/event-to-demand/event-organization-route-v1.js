/**
 * Organization / buyer contact routes for event-derived opportunities.
 * Homepage alone is never enough; meetings/events/contact paths are.
 * Official event / calendar listing pages are research evidence — not sales desks.
 * Never invents named people or personal contact data.
 */

import {
  CONTACT_ROUTE_QUALITY,
  classifyContactUrlQuality,
} from "./event-demand-class-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Build a usable organization sales / meetings route from event evidence.
 * Prefer explicit organizer contact URLs; else official event page as
 * intermediary organization route.
 *
 * Bare marketing homepages are normally rejected. For a *verified* event
 * organizer, the official event/org URL is treated as an organization sales
 * route (meetings outreach via published channels) — still not a named buyer.
 */
export function buildOrganizationContactRoute({
  event = {},
  stakeholder = null,
  organizationName = null,
  allowVerifiedOrganizerSite = true,
} = {}) {
  const explicit =
    text(stakeholder?.contact?.contactFormUrl) ||
    text(stakeholder?.contact?.organizationRouteUrl) ||
    text(stakeholder?.contactUrl) ||
    text(event.organizerContactUrl) ||
    text(event.meetingsContactUrl) ||
    text(event.housingContactUrl) ||
    "";

  const official = text(event.officialUrl || event.sources?.[0]?.url || "");
  const routeUrl = explicit || official;
  if (!routeUrl) {
    return {
      contact: {},
      actionable: false,
      reason: "no_organization_route_url",
    };
  }

  const urlQuality = classifyContactUrlQuality(explicit || routeUrl);
  const isBareHomepage =
    !explicit && /^https?:\/\/[^/]+\/?$/i.test(routeUrl);

  const verifiedOrganizerSite =
    allowVerifiedOrganizerSite &&
    event.verified !== false &&
    Boolean(event.eventName || event.organizer || organizationName) &&
    Boolean(official || explicit);

  // Explicit sales/meetings contact path → usable org sales route
  if (urlQuality === CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES || explicit) {
    const salesUrl = explicit || routeUrl;
    const contact = {
      organizationName: text(
        organizationName || stakeholder?.organizationName || event.organizer
      ),
      organizationRouteUrl: salesUrl,
      contactFormUrl: salesUrl,
      intermediary: {
        type: "organization_sales_route",
        label: "Organizer / meetings contact route",
        url: salesUrl,
      },
      homepageOnly: false,
      contactRouteQuality: CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES,
      buyerFunctionHint:
        stakeholder?.buyerFunctionHint ||
        event.buyerFunctionHint ||
        "meetings_events_or_corporate_hospitality",
      ...(stakeholder?.contact && typeof stakeholder.contact === "object"
        ? Object.fromEntries(
            Object.entries(stakeholder.contact).filter(
              ([k, v]) =>
                v != null &&
                v !== "" &&
                !["homepageOnly", "organizationHomepage"].includes(k)
            )
          )
        : {}),
    };
    return {
      contact,
      actionable: true,
      reason: "organization_sales_route",
      contactRouteQuality: CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES,
    };
  }

  // Event listing / fair page / bare homepage: research-lead evidence only
  if (
    urlQuality === CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY ||
    isBareHomepage ||
    verifiedOrganizerSite
  ) {
    return {
      contact: {
        organizationName: text(
          organizationName || stakeholder?.organizationName || event.organizer
        ),
        organizationRouteUrl: routeUrl,
        organizationHomepage: isBareHomepage ? routeUrl : undefined,
        eventListingUrl: routeUrl,
        intermediary: {
          type: "event_listing_evidence",
          label: "Verified event / organizer page (research lead — not a sales desk)",
          url: routeUrl,
        },
        homepageOnly: isBareHomepage === true,
        contactRouteQuality: CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY,
        buyerFunctionHint:
          stakeholder?.buyerFunctionHint ||
          event.buyerFunctionHint ||
          "meetings_events_or_corporate_hospitality",
      },
      actionable: false,
      reason: "event_listing_research_lead_not_sales_route",
      contactRouteQuality: CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY,
    };
  }

  return {
    contact: {},
    actionable: false,
    reason: "no_organization_route_url",
    contactRouteQuality: CONTACT_ROUTE_QUALITY.NONE,
  };
}

/**
 * Mechanisms where confirmed room-block evidence is NOT required for
 * exploratory hotel-facing prospecting (still required for Ready/confirmed).
 */
export const EXPLORATORY_WITHOUT_ROOM_BLOCK_MECHANISMS = Object.freeze([
  "EXECUTIVE_DELEGATION",
  "VIP_HOSPITALITY",
  "SMALL_GROUP_LODGING",
  "OFFSITE_MEETING",
  "ANCILLARY_EVENT_BUSINESS",
]);

export function isExploratoryWithoutRoomBlock(mechanism) {
  return EXPLORATORY_WITHOUT_ROOM_BLOCK_MECHANISMS.includes(
    String(mechanism || "").toUpperCase()
  );
}
