/**
 * Apify contribution for Complete Demand Packet V8.
 * Only uses actors already present in the Dealality repo.
 * Apify output = SIGNAL until independently validated. Never verified truth alone.
 */

import { getApifyToken, runApifyActor } from "../../hotel-intelligence/apify/local-client.js";
import { DEFAULT_TRIPADVISOR_ACTOR } from "../../hotel-intelligence/apify-usage/constants.js";

/**
 * Inventory of repo-known Apify actors mapped to GDI packet use.
 * Does NOT invent Eventbrite / LinkedIn Events / Meetup actor IDs.
 */
export function buildApifyActorInventory() {
  return {
    status: "REPO_INVENTORY_ONLY",
    note:
      "No Eventbrite, LinkedIn Events, Meetup, or Google Maps Places actors are referenced in this repo. Do not invent actor IDs.",
    actors: [
      {
        actorRef: "maxcopell~tripadvisor",
        actorName: DEFAULT_TRIPADVISOR_ACTOR.actor_name,
        gdiUse: "COMP_HOTEL_IDENTITY",
        pillarsHelped: ["E_HOTEL_LODGING_EVIDENCE_PIVOT", "COMP_SET_PHONE_ADDRESS_DOMAIN"],
        fillsOrganization: false,
        fillsOrganizer: false,
        fillsFutureDate: false,
        fillsBuyerPath: false,
        usedInV8: true,
        truthPolicy: "SIGNAL_UNTIL_PAGE_VALIDATED",
      },
      {
        actorRef: "axlymxp~ihg-hotel-scraper",
        gdiUse: "BRAND_HOTEL_IDENTITY",
        pillarsHelped: ["COMP_SET_IDENTITY"],
        usedInV8: false,
        truthPolicy: "SIGNAL_UNTIL_PAGE_VALIDATED",
      },
      {
        actorRef: "dataquarry~hotels-lodging",
        gdiUse: "OSM_HOTEL_POI_CANDIDATE_ONLY",
        pillarsHelped: ["WEAK_VENUE_GEO"],
        usedInV8: false,
        truthPolicy: "CANDIDATE_ONLY_NOT_INVOKED",
      },
    ],
    notInRepo: [
      "Eventbrite actor",
      "LinkedIn Events actor",
      "Meetup actor",
      "Google Maps Places actor",
      "Generic web-scraper actor ID for congress pages",
    ],
    recommendedNext:
      "Add Store actors for Eventbrite/LinkedIn Events only after founder approval — map to organization+date+organizer pillars; keep SIGNAL until page validation.",
  };
}

/**
 * Bounded Tripadvisor enrichment for competitor identity (phone/address/website).
 * Does not create demand packets by itself.
 */
export async function enrichCompHotelsViaTripadvisor(competitors = [], opts = {}) {
  const token = getApifyToken();
  const results = [];
  if (!token) {
    return {
      enabled: false,
      reason: "APIFY_TOKEN_MISSING",
      results,
      costUsd: 0,
    };
  }

  const max = opts.maxComps ?? 3;
  let costUsd = 0;
  for (const comp of competitors.slice(0, max)) {
    const search = `${comp.canonicalName} ${comp.market || ""}`.trim();
    try {
      const run = await runApifyActor({
        actorRef: "maxcopell~tripadvisor",
        input: {
          searchString: search,
          maxItemsPerQuery: 1,
          includeAttractions: false,
          includeRestaurants: false,
          includeHotels: true,
        },
        waitSecs: 90,
        maxTotalChargeUsd: opts.maxChargePerRun ?? 0.25,
      });
      costUsd += Number(run?.usage_total_usd || 0.05) || 0.05;
      const item = (run.items || [])[0] || null;
      const phone = item?.phone || item?.telephone || null;
      const address = item?.address || item?.addressObj?.address || null;
      const website = item?.website || item?.webUrl || null;
      results.push({
        competitorHotelId: comp.competitorHotelId,
        canonicalName: comp.canonicalName,
        apifyActor: "maxcopell~tripadvisor",
        ok: Boolean(item),
        publicPhone: phone,
        address,
        domain: website
          ? (() => {
              try {
                return new URL(website).hostname.replace(/^www\./, "");
              } catch {
                return null;
              }
            })()
          : null,
        currentWebsite: website,
        signalOnly: true,
        verifiedTruth: false,
        rawName: item?.name || null,
      });
    } catch (err) {
      results.push({
        competitorHotelId: comp.competitorHotelId,
        canonicalName: comp.canonicalName,
        apifyActor: "maxcopell~tripadvisor",
        ok: false,
        error: String(err?.message || err).slice(0, 160),
        signalOnly: true,
        verifiedTruth: false,
      });
    }
  }

  return { enabled: true, results, costUsd };
}

/**
 * Convert Apify enrichment rows into packet-adjacent signals (never complete packets alone).
 */
export function apifyResultsToSignals(apifyResults = [], hotel = {}) {
  return (apifyResults.results || [])
    .filter((r) => r.ok)
    .map((r) => ({
      hotelKey: hotel.hotelKey,
      id: `apify_comp_${r.competitorHotelId}`,
      title: `Apify identity pivot: ${r.canonicalName}`,
      organizationName: null,
      competitorHotel: r.canonicalName,
      officialSource: r.currentWebsite,
      signalType: "APIFY_COMP_IDENTITY",
      sourceFamily: "APIFY_TRIPADVISOR",
      artifact: "APIFY_V8",
      publicPhone: r.publicPhone,
      // Identity pivot only — lodging pillar NOT granted without group/event page
      evidenceClass: "DISCOVERY_ONLY",
      apifySignal: true,
      verifiedTruth: false,
    }));
}
