/**
 * V4.1 Commercial Spine Restoration — reuse-first identity, geography,
 * contact ownership, public CI bridge, sales-brief honesty, lead-drain helpers.
 *
 * Does NOT invent architecture. Wires existing entity-truth, CI extract,
 * buyer taxonomy, and lead-following into the ORS commercial path.
 * No Surfe/PDL execution. No publication / Airtable changes.
 */

import { cleanEntityDisplayName, isV3QueueNoise } from "../hidden-demand/v3-entity-clean.js";
import { classifyEntityTruth, ENTITY_CLASS } from "../entity-truth-gate-v1.js";
import { extractContactIntelligenceFromSource } from "../extract-contact-intelligence-from-source.js";
import { BUYER_CONTACT_PATH_CLASS } from "../buyer-contact-path-taxonomy-v1.js";
import { classifyPersonResponsibility } from "../workflow/buyer-contact-enrichment-v1.js";
import { namedCompetitorHost } from "./commercial-pathway-v1.js";
import {
  INTERNAL_DISPOSITION,
  REVENUE_TYPE,
  qualifyCommercialProspectV3,
} from "./commercial-qualification-v3.js";
import {
  planAutonomousLeadFollowing,
  selectNextAuthorizedLeadFollowUp,
  scoreRelatedEntityLead,
} from "./autonomous-lead-following-v2.js";

export const COMMERCIAL_SPINE_RESTORATION_V41 = "gdi-commercial-spine-restoration-v41";

/** Contact ownership classes for sales-usable routes. */
export const CONTACT_OWNERSHIP_CLASS = Object.freeze({
  VERIFIED_DECISION_MAKER: "VERIFIED_DECISION_MAKER",
  POTENTIAL_DECISION_MAKER: "POTENTIAL_DECISION_MAKER",
  INFLUENCER_OR_INTERMEDIARY: "INFLUENCER_OR_INTERMEDIARY",
  FUNCTIONAL_DEPARTMENT: "FUNCTIONAL_DEPARTMENT",
  GENERAL_ORGANIZATION_CONTACT: "GENERAL_ORGANIZATION_CONTACT",
  THIRD_PARTY_DIRECTORY_CONTACT: "THIRD_PARTY_DIRECTORY_CONTACT",
  UNVERIFIED_CONTACT_ROUTE: "UNVERIFIED_CONTACT_ROUTE",
  MISSING: "MISSING",
});

export const PUBLISHER_KIND = Object.freeze({
  ORGANIZATION_OWNED: "ORGANIZATION_OWNED",
  DIRECTORY_LISTING: "DIRECTORY_LISTING",
  VENUE_OR_HOTEL_MARKETING: "VENUE_OR_HOTEL_MARKETING",
  CVB_OR_DESTINATION: "CVB_OR_DESTINATION",
  HOUSING_BUREAU_OR_AMC: "HOUSING_BUREAU_OR_AMC",
  NEWS_OR_INDEPENDENT: "NEWS_OR_INDEPENDENT",
  UNKNOWN: "UNKNOWN",
});

export const GEO_FIT_CLASS = Object.freeze({
  TARGET_MARKET_MATCH: "TARGET_MARKET_MATCH",
  EVENT_DESTINATION_MATCH: "EVENT_DESTINATION_MATCH",
  NAMED_COMPETITOR_HOST_ELSEWHERE: "NAMED_COMPETITOR_HOST_ELSEWHERE",
  ORG_HQ_ELSEWHERE_BUT_NATIONAL_OK: "ORG_HQ_ELSEWHERE_BUT_NATIONAL_OK",
  FUTURE_CYCLE_DESTINATION_UNKNOWN: "FUTURE_CYCLE_DESTINATION_UNKNOWN",
  PLAUSIBLE_UNVERIFIED: "PLAUSIBLE_UNVERIFIED",
  MISMATCH_EXCLUDE_EVENT: "MISMATCH_EXCLUDE_EVENT",
  INSUFFICIENT: "INSUFFICIENT",
});

const DIRECTORY_HOSTS = /\b(dmc\.travel|conferencehotelgroup|simpleviewinc|goeshow|cvent\.com|passkey|a2zinc|expolink|easyhotelrfp|venuefinder|tenokas|meetingpackage)\b/i;
const HOTEL_BRAND_HOSTS = /\b(marriott\.com|hilton\.com|hyatt\.com|ihg\.com|wyndhamhotels\.com|nobuhotels|accor\.com|radissonhotels)\b/i;
const CVB_HOSTS = /\b(visit|cvb|sandiego\.org|londonandpartners|destination)\b/i;
const HOUSING_INTERMEDIARY_EMAIL = /\b(mci-agency|onpeak|orchid\.events|c2presearch|passkey|experient|conferencedirect|helmsbriscoe)\b/i;
const FUNCTIONAL_LOCAL = /^(events?|meetings?|conference|housing|registration|groupsales|group\.?sales|sales|banquets?|planning|planner|meetings)@/i;
const GENERIC_LOCAL = /^(info|contact|office|hello|admin|support|mail|custservice|customerservice)@/i;
const JUNK_ORG_RE = /^(list|hotels?|accommodation|venues?|headquarters|map|and\s+travel|and\s+room\s+blocks?|travel|housing|home|about|contact|planner\s+guide|2027|&\s*travel|,?\s*&?\s*visas?|-+\s*hotels?)\b/i;
const TITLE_ORDINAL_NOISE = /\b\d{1,3}(?:st|nd|rd|th)\b/i;

/** Known publisher hosts → demand organization (housing pages are evidence, not the org identity). */
export const KNOWN_ORG_HOST_ALIASES = Object.freeze({
  "ausa.org": "Association of the United States Army",
  "meetings.ausa.org": "Association of the United States Army",
  "aacpdm.org": "American Academy for Cerebral Palsy and Developmental Medicine",
  "dri.org": "DRI — The Voice of the Defense Bar",
  "napex.org": "National Philatelic Exhibitions of Washington, D.C., Inc.",
  "visaa.org": "Virginia Independent Schools Athletic Association",
  "roseliassociates.com": "Roseli Associates",
  "event.roseliassociates.com": "Roseli Associates",
  "aoc2026.org": "Association of Old Crows",
  "crows.org": "Association of Old Crows",
  "nih.gov": "National Institutes of Health",
  "cc.nih.gov": "NIH Clinical Center",
  "csr.nih.gov": "NIH Center for Scientific Review",
  "commonfund.nih.gov": "NIH Common Fund",
});

/** Official housing / hotel-travel page paths — lodging evidence, not org names. */
export const OFFICIAL_HOUSING_PAGE_RE = /\/(?:hotel(?:s)?(?:-travel)?|housing|hoteltravel|exhibitor[_-]?housing)(?:\/|\.cfm|\.html?)?(?=$|[?#\s"'<>]|Hotel|Travel|&)|hotel[-_]?list|housing-and-travel|hotel_availability|hotel-map|\/hotels\.pdf|AM\d+-HOTEL-LIST|hotel[-_]?flyer|\/hotel(?:\/|$|[?#\s])/i;

export function resolveOrgNameFromPublisherHost(host = "") {
  const h = String(host || "").replace(/^www\./i, "").toLowerCase();
  if (!h) return null;
  if (KNOWN_ORG_HOST_ALIASES[h]) return KNOWN_ORG_HOST_ALIASES[h];
  const parts = h.split(".").filter(Boolean);
  if (parts.length >= 2) {
    const registrable = parts.slice(-2).join(".");
    if (KNOWN_ORG_HOST_ALIASES[registrable]) return KNOWN_ORG_HOST_ALIASES[registrable];
  }
  return null;
}

export function isOfficialHousingOrHotelListPage(url = "", title = "") {
  return OFFICIAL_HOUSING_PAGE_RE.test(`${url} ${title}`);
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hostnameOf(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function emailDomain(email) {
  const m = String(email || "").toLowerCase().match(/@([^>\s]+)/);
  return m ? m[1].replace(/^www\./, "") : "";
}

function registrableBase(host) {
  const parts = String(host || "").toLowerCase().split(".").filter(Boolean);
  if (parts.length < 2) return host || "";
  return parts.slice(-2).join(".");
}

/**
 * Classify the publisher of an evidence URL (not the claimed prospect).
 */
export function classifyPublisherKind(url = "", title = "") {
  const host = hostnameOf(url);
  const blob = `${host} ${title}`;
  if (!host) return PUBLISHER_KIND.UNKNOWN;
  if (DIRECTORY_HOSTS.test(host) || /\/listing\//i.test(String(url))) return PUBLISHER_KIND.DIRECTORY_LISTING;
  if (HOTEL_BRAND_HOSTS.test(host)) return PUBLISHER_KIND.VENUE_OR_HOTEL_MARKETING;
  if (CVB_HOSTS.test(blob)) return PUBLISHER_KIND.CVB_OR_DESTINATION;
  if (HOUSING_INTERMEDIARY_EMAIL.test(host) || /\b(onpeak|orchid|passkey)\b/i.test(host)) {
    return PUBLISHER_KIND.HOUSING_BUREAU_OR_AMC;
  }
  if (/\b(news|press|times|post|journal|reuters|bloomberg)\b/i.test(host)) {
    return PUBLISHER_KIND.NEWS_OR_INDEPENDENT;
  }
  return PUBLISHER_KIND.ORGANIZATION_OWNED;
}

/**
 * Resolve actual organization vs publisher / directory / venue / junk title.
 * Unresolved identities become research tasks — never invented outreach targets.
 */
export function resolveOrganizationIdentity({
  claimedOrganizationName = "",
  title = "",
  url = "",
  excerpt = "",
  memoryOrganizations = [],
} = {}) {
  const publisherKind = classifyPublisherKind(url, title);
  const host = hostnameOf(url);
  const claimed = cleanEntityDisplayName(claimedOrganizationName || "");
  const titleHead = cleanEntityDisplayName((title || "").split(/\s+[|\u2013\u2014:]\s+/)[0] || title);
  const strippedTitle = titleHead
    .replace(TITLE_ORDINAL_NOISE, " ")
    .replace(/\b(20\d{2}|annual|conference|meeting|awards?|banquet|hotel|housing|travel|room\s*blocks?|list)\b/gi, " ")
    .replace(/^[\s\-–—|&]+/, "")
    .replace(/\s+/g, " ")
    .trim();

  // Housing / hotel-list pages are lodging evidence — not organization names by themselves.
  const hotelListOnlyPage = isOfficialHousingOrHotelListPage(url, title);
  const hostAliasOrg = resolveOrgNameFromPublisherHost(host);

  let candidateName = claimed || strippedTitle;
  if (!candidateName || JUNK_ORG_RE.test(candidateName) || candidateName.length < 3) {
    candidateName = strippedTitle;
  }
  if (!candidateName || JUNK_ORG_RE.test(candidateName) || isV3QueueNoise({ entityName: candidateName })) {
    if (hostAliasOrg) {
      candidateName = hostAliasOrg;
    } else if (publisherKind === PUBLISHER_KIND.ORGANIZATION_OWNED && host && !hotelListOnlyPage) {
      // Prefer domain-derived org only when publisher is organization-owned AND not a bare hotel-list artifact
      const base = host.split(".")[0].replace(/-/g, " ");
      if (base.length >= 3 && !/^(bit|pages|events|event|annualmeeting|www)$/i.test(base)) {
        candidateName = base.replace(/\b\w/g, (c) => c.toUpperCase());
      } else {
        candidateName = "";
      }
    } else {
      candidateName = "";
    }
  }

  // Housing index with unresolved host: research task — do not invent list-title entities.
  // When host alias is known, treat as org-level candidate with housingPageEvidenceOnly.
  if (hotelListOnlyPage && (JUNK_ORG_RE.test(claimed) || JUNK_ORG_RE.test(titleHead) || !claimed || isV3QueueNoise({ entityName: claimed || "" }))) {
    if (hostAliasOrg || (publisherKind === PUBLISHER_KIND.ORGANIZATION_OWNED && candidateName && !JUNK_ORG_RE.test(candidateName))) {
      const orgFromHost = hostAliasOrg || candidateName;
      return {
        version: COMMERCIAL_SPINE_RESTORATION_V41,
        actionableCandidate: true,
        rejectReason: null,
        organizationName: orgFromHost,
        listedEntityName: orgFromHost,
        publisherKind,
        publisherHost: host || null,
        entityTruth: null,
        housingPageEvidenceOnly: true,
        unresolvedIdentityTask: "Official housing/hotel page evidences lodging arrangements for this organization — investigate program destination, open sourcing vs committed host, and buyer/housing contact. Do not treat the housing URL as an open Bethesda RFP.",
        duplicateOfMemory: false,
      };
    }
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      actionableCandidate: false,
      rejectReason: "HOTEL_LIST_OR_HOUSING_INDEX_NOT_ORGANIZATION",
      organizationName: null,
      listedEntityName: host ? host.split(".")[0] : null,
      publisherKind,
      publisherHost: host || null,
      entityTruth: null,
      housingPageEvidenceOnly: true,
      unresolvedIdentityTask: "Hotel list/housing index page — resolve the hosting association as a separate org research task; do not invent list-title entities.",
      duplicateOfMemory: false,
    };
  }

  // Directory listing: listed entity may be in path/title, but publisher ≠ prospect contact owner
  let listedEntityName = null;
  if (publisherKind === PUBLISHER_KIND.DIRECTORY_LISTING) {
    const pathMatch = String(url).match(/\/listing\/([^/?#]+)/i);
    if (pathMatch) {
      listedEntityName = pathMatch[1].replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    }
    if (/eureka/i.test(`${title} ${excerpt} ${claimed}`)) listedEntityName = "Eureka DMC";
    candidateName = listedEntityName || candidateName || null;
  }

  if (publisherKind === PUBLISHER_KIND.VENUE_OR_HOTEL_MARKETING) {
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      actionableCandidate: false,
      rejectReason: "VENUE_OR_HOTEL_MARKETING_PAGE",
      organizationName: null,
      listedEntityName: candidateName || null,
      publisherKind,
      publisherHost: host || null,
      entityTruth: null,
      unresolvedIdentityTask: "Investigate demand organization behind venue marketing page — do not treat hotel brand page as prospect org.",
      duplicateOfMemory: false,
    };
  }

  if (!candidateName || JUNK_ORG_RE.test(candidateName) || isV3QueueNoise({ entityName: candidateName })) {
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      actionableCandidate: false,
      rejectReason: "JUNK_OR_UNRESOLVED_ORGANIZATION_IDENTITY",
      organizationName: null,
      listedEntityName: null,
      publisherKind,
      publisherHost: host || null,
      entityTruth: null,
      unresolvedIdentityTask: "Resolve organization identity from publisher domain / about page before commercial qualification.",
      duplicateOfMemory: false,
    };
  }

  const truth = classifyEntityTruth({
    organizationName: candidateName,
    title: title || candidateName,
    sources: url ? [{ url }] : [],
  });
  if (truth.validEntity === false && (truth.entityClass === ENTITY_CLASS.DIRECTORY_TITLE
    || truth.entityClass === ENTITY_CLASS.UI_CHROME
    || truth.entityClass === ENTITY_CLASS.EVENT_TITLE_ONLY
    || truth.entityClass === ENTITY_CLASS.VENUE_NAME_ONLY
    || truth.entityClass === ENTITY_CLASS.UNKNOWN_INVALID
    || truth.entityClass === ENTITY_CLASS.CATEGORY_LABEL
    || truth.entityClass === ENTITY_CLASS.NAVIGATION_TEXT)) {
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      actionableCandidate: false,
      rejectReason: `ENTITY_TRUTH_${truth.entityClass}`,
      organizationName: null,
      listedEntityName: candidateName,
      publisherKind,
      publisherHost: host || null,
      entityTruth: truth,
      unresolvedIdentityTask: "Entity truth gate rejected name as non-organization; research task only.",
      duplicateOfMemory: false,
    };
  }

  const norm = candidateName.toLowerCase();
  const duplicateOfMemory = (memoryOrganizations || []).some((o) => String(o || "").toLowerCase() === norm);
  const housingPageEvidenceOnly = hotelListOnlyPage === true;

  return {
    version: COMMERCIAL_SPINE_RESTORATION_V41,
    actionableCandidate: true,
    rejectReason: null,
    organizationName: hostAliasOrg && (JUNK_ORG_RE.test(claimed) || !claimed) ? hostAliasOrg : candidateName,
    listedEntityName: listedEntityName || candidateName,
    publisherKind,
    publisherHost: host || null,
    entityTruth: truth,
    housingPageEvidenceOnly,
    unresolvedIdentityTask: publisherKind === PUBLISHER_KIND.DIRECTORY_LISTING
      ? "Confirm organization-owned domain and contact; directory listing is not a verified contact owner."
      : housingPageEvidenceOnly
        ? "Housing/hotel page evidences lodging arrangements — confirm destination fit, open sourcing vs committed host, and buyer route before hotel-facing pitch."
        : null,
    duplicateOfMemory,
    directoryListingNotContactOwner: publisherKind === PUBLISHER_KIND.DIRECTORY_LISTING,
  };
}

/**
 * Contact ownership — directory publisher inbox ≠ prospect verified contact.
 */
/** Consumer / free-mail domains — never treat as organization-owned contact routes. */
export const CONSUMER_EMAIL_DOMAINS = /\b(gmail|googlemail|yahoo|ymail|hotmail|outlook|live|msn|aol|icloud|me|mac|protonmail|proton|gmx|mail\.com|hey\.com)\b/i;

export function isConsumerEmailDomain(domainOrEmail = "") {
  const raw = String(domainOrEmail || "").trim().toLowerCase();
  const d = raw.includes("@") ? emailDomain(raw) : raw.replace(/^www\./, "");
  if (!d) return false;
  const base = d.split(".").slice(-2).join(".");
  return CONSUMER_EMAIL_DOMAINS.test(base) || CONSUMER_EMAIL_DOMAINS.test(d.split(".")[0] || "");
}

export function classifyContactOwnership({
  email = null,
  pageUrl = "",
  organizationName = "",
  organizationDomain = "",
  personName = null,
  roleTitle = null,
  publisherKind = null,
} = {}) {
  if (!email || !/@/.test(String(email))) {
    return {
      route: null,
      ownershipClass: CONTACT_OWNERSHIP_CLASS.MISSING,
      salesUsableAsProspectContact: false,
      note: "No email route",
    };
  }
  const em = String(email).trim().toLowerCase();
  const ed = emailDomain(em);
  const pageHost = hostnameOf(pageUrl);
  const pub = publisherKind || classifyPublisherKind(pageUrl);
  const orgDom = String(organizationDomain || "").replace(/^www\./, "").toLowerCase()
    || (pub === PUBLISHER_KIND.ORGANIZATION_OWNED ? pageHost : "");

  // Consumer-domain emails are never sales-usable org contacts (even if listed on an org site).
  if (isConsumerEmailDomain(ed)) {
    return {
      route: em,
      ownershipClass: CONTACT_OWNERSHIP_CLASS.UNVERIFIED_CONTACT_ROUTE,
      salesUsableAsProspectContact: false,
      note: "Consumer-domain email (e.g. gmail/yahoo/hotmail) — unsupported organization ownership; do not promote as verified buyer contact.",
      personName: personName || null,
      roleTitle: roleTitle || null,
      consumerDomain: true,
    };
  }

  if (pub === PUBLISHER_KIND.DIRECTORY_LISTING || DIRECTORY_HOSTS.test(ed) || DIRECTORY_HOSTS.test(pageHost)) {
    // Directory email matching directory host is publisher contact, not listed org
    if (!orgDom || registrableBase(ed) === registrableBase(pageHost) || DIRECTORY_HOSTS.test(ed)) {
      return {
        route: em,
        ownershipClass: CONTACT_OWNERSHIP_CLASS.THIRD_PARTY_DIRECTORY_CONTACT,
        salesUsableAsProspectContact: false,
        note: "Directory/publisher inbox — not the listed organization's verified business contact.",
        personName: personName || null,
        roleTitle: roleTitle || null,
      };
    }
  }

  if (HOUSING_INTERMEDIARY_EMAIL.test(ed)) {
    return {
      route: em,
      ownershipClass: CONTACT_OWNERSHIP_CLASS.INFLUENCER_OR_INTERMEDIARY,
      salesUsableAsProspectContact: true,
      note: "Housing bureau / AMC intermediary — authorized intermediary, not buyer staff.",
      personName: personName || null,
      roleTitle: roleTitle || null,
    };
  }

  const orgMatch = orgDom && registrableBase(ed) === registrableBase(orgDom);
  const pageMatch = pageHost && registrableBase(ed) === registrableBase(pageHost)
    && pub === PUBLISHER_KIND.ORGANIZATION_OWNED;

  // Functional entities / intermediaries (e.g. "Maritz Global Events") must never be
  // classified as POTENTIAL_DECISION_MAKER people — fall through to department/general route.
  const personLooksHuman = Boolean(personName)
    && /^[A-Z][a-z]+(?:\s+[A-Z][a-z.'-]+)+$/.test(String(personName).trim())
    && !/association|academy|events|housing|provider|channel|tournaments|inc\.|llc|global|maritz|staff|tbd|meetings\s*\/\s*housing/i.test(
      String(personName),
    );

  if (personLooksHuman && roleTitle) {
    const resp = classifyPersonResponsibility(
      { name: personName, title: roleTitle, email: em, relevanceEvidence: roleTitle },
      { organizationName, organizationDomain: orgDom },
      {},
    );
    const decisionLike = /decision|buyer|planner|housing|meeting|event|procurement|sourcing|confirmed_relevant|booking/i.test(
      `${roleTitle} ${resp?.class || ""}`,
    );
    if ((orgMatch || pageMatch) && decisionLike) {
      return {
        route: em,
        ownershipClass: CONTACT_OWNERSHIP_CLASS.POTENTIAL_DECISION_MAKER,
        salesUsableAsProspectContact: true,
        note: "Named person with role-relevant title on org-owned evidence — not Surfe-verified.",
        personName,
        roleTitle,
        responsibilityClass: resp?.class || null,
      };
    }
  }

  // Intermediary org named with housing/registration role → influencer/intermediary ownership
  if (personName && !personLooksHuman
    && /housing|registration|eventshq|maritz|bureau|amc|intermediary/i.test(`${personName} ${roleTitle || ""}`)
    && (orgMatch || pageMatch || FUNCTIONAL_LOCAL.test(em))) {
    return {
      route: em,
      ownershipClass: CONTACT_OWNERSHIP_CLASS.INFLUENCER_OR_INTERMEDIARY,
      salesUsableAsProspectContact: true,
      note: "Functional housing/registration intermediary — usable org route, not a named individual decision-maker.",
      personName: null,
      roleTitle: roleTitle || null,
      intermediaryLabel: personName,
    };
  }

  if (FUNCTIONAL_LOCAL.test(em) && (orgMatch || pageMatch)) {
    return {
      route: em,
      ownershipClass: CONTACT_OWNERSHIP_CLASS.FUNCTIONAL_DEPARTMENT,
      salesUsableAsProspectContact: true,
      note: "Functional department inbox on organization-owned domain.",
      personName: personName || null,
      roleTitle: roleTitle || null,
    };
  }

  if (GENERIC_LOCAL.test(em) && (orgMatch || pageMatch)) {
    return {
      route: em,
      ownershipClass: CONTACT_OWNERSHIP_CLASS.GENERAL_ORGANIZATION_CONTACT,
      salesUsableAsProspectContact: true,
      note: "General organization inbox on organization-owned domain.",
      personName: personName || null,
      roleTitle: roleTitle || null,
    };
  }

  if (orgMatch || pageMatch) {
    return {
      route: em,
      ownershipClass: personName
        ? CONTACT_OWNERSHIP_CLASS.POTENTIAL_DECISION_MAKER
        : CONTACT_OWNERSHIP_CLASS.GENERAL_ORGANIZATION_CONTACT,
      salesUsableAsProspectContact: true,
      note: "Email domain matches organization/publisher host.",
      personName: personName || null,
      roleTitle: roleTitle || null,
    };
  }

  return {
    route: em,
    ownershipClass: CONTACT_OWNERSHIP_CLASS.UNVERIFIED_CONTACT_ROUTE,
    salesUsableAsProspectContact: false,
    note: "Email domain does not match resolved organization domain — unverified ownership.",
    personName: personName || null,
    roleTitle: roleTitle || null,
  };
}

/**
 * Parse named host hotel + city signals from evidence (no invention).
 */
export function extractNamedHostAndDestination(blob = "") {
  const textBlob = text(blob);
  const hostPatterns = [
    [/\bPhiladelphia Marriott Downtown\b/i, "Philadelphia Marriott Downtown", "Philadelphia"],
    [/\bCrystal Gateway Marriott\b/i, "Crystal Gateway Marriott", "Crystal City"],
    [/\bHilton San Diego Bayfront\b/i, "Hilton San Diego Bayfront", "San Diego"],
    [/\bBethesda North Marriott\b/i, "Bethesda North Marriott", "Bethesda"],
    [/\bHyatt Regency Washington\b/i, "Hyatt Regency Washington", "Washington"],
  ];
  for (const [re, hotel, city] of hostPatterns) {
    if (re.test(textBlob)) return { namedHostHotel: hotel, eventDestinationCity: city };
  }
  const cityMatch = textBlob.match(
    /\b(?:in|at|hosted\s+in|takes?\s+place\s+in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b/,
  );
  return {
    namedHostHotel: namedCompetitorHost(textBlob, "") || null,
    eventDestinationCity: cityMatch ? cityMatch[1] : null,
  };
}

/**
 * Hotel-specific geographic fit — separates HQ, event destination, contracted host, future cycle.
 */
export function assessHotelGeographicFit({
  targetHotel = "",
  targetCity = "",
  evidenceBlob = "",
  namedHostHotel = null,
  organizationLocation = null,
  eventDestination = null,
  nationalOrganization = false,
} = {}) {
  const hostInfo = extractNamedHostAndDestination(evidenceBlob);
  const host = namedHostHotel || hostInfo.namedHostHotel;
  const dest = eventDestination || hostInfo.eventDestinationCity;
  const target = text(targetHotel);
  const city = text(targetCity)
    || (/\bBethesda\b/i.test(target) ? "Bethesda"
      : /\bLondon\b/i.test(target) ? "London"
        : /\bSan Diego\b/i.test(target) ? "San Diego" : "");

  const hostIsTarget = host && target && new RegExp(
    target.split(/\s+/).slice(0, 2).join("\\s+"),
    "i",
  ).test(host);

  if (host && !hostIsTarget) {
    const destMismatch = dest && city && !new RegExp(city, "i").test(dest)
      && !new RegExp(city, "i").test(host);
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      geoFitClass: GEO_FIT_CLASS.NAMED_COMPETITOR_HOST_ELSEWHERE,
      eventQualifiedForTargetHotel: false,
      organizationStillAccountEligible: true,
      namedHostHotel: host,
      eventDestinationCity: dest,
      targetCity: city || null,
      note: destMismatch
        ? `Named host ${host} (${dest}) is not ${target} — do not qualify this event as target-hotel demand; organization may remain future-cycle / account-development without inventing destination.`
        : `Named competitor host ${host} — preserve host; do not invent open host selection at ${target}.`,
      inventFutureHotelLocation: false,
    };
  }

  if (dest && city && new RegExp(city, "i").test(dest)) {
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      geoFitClass: GEO_FIT_CLASS.EVENT_DESTINATION_MATCH,
      eventQualifiedForTargetHotel: true,
      organizationStillAccountEligible: true,
      namedHostHotel: host,
      eventDestinationCity: dest,
      targetCity: city,
      note: "Event destination matches target hotel market.",
      inventFutureHotelLocation: false,
    };
  }

  if (organizationLocation && city && !new RegExp(city, "i").test(String(organizationLocation)) && nationalOrganization) {
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      geoFitClass: GEO_FIT_CLASS.ORG_HQ_ELSEWHERE_BUT_NATIONAL_OK,
      eventQualifiedForTargetHotel: Boolean(dest && new RegExp(city, "i").test(dest)),
      organizationStillAccountEligible: true,
      namedHostHotel: host,
      eventDestinationCity: dest,
      targetCity: city,
      note: "National organization HQ elsewhere does not disqualify account development.",
      inventFutureHotelLocation: false,
    };
  }

  if (!host && !dest) {
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      geoFitClass: city && new RegExp(city, "i").test(evidenceBlob)
        ? GEO_FIT_CLASS.PLAUSIBLE_UNVERIFIED
        : GEO_FIT_CLASS.FUTURE_CYCLE_DESTINATION_UNKNOWN,
      eventQualifiedForTargetHotel: false,
      organizationStillAccountEligible: true,
      namedHostHotel: null,
      eventDestinationCity: null,
      targetCity: city || null,
      note: "No named host/destination — plausible unverified or future-cycle only; do not invent location.",
      inventFutureHotelLocation: false,
    };
  }

  return {
    version: COMMERCIAL_SPINE_RESTORATION_V41,
    geoFitClass: GEO_FIT_CLASS.INSUFFICIENT,
    eventQualifiedForTargetHotel: false,
    organizationStillAccountEligible: true,
    namedHostHotel: host,
    eventDestinationCity: dest,
    targetCity: city || null,
    note: "Insufficient geography evidence for event-level qualification.",
    inventFutureHotelLocation: false,
  };
}

/**
 * Public CI extract bridge for ORS — no Surfe/PDL.
 */
export function extractPublicContactIntelligenceForOrs({
  html = "",
  textContent = "",
  url = "",
  organizationName = "",
  authorizeSurfe = false,
  authorizePdl = false,
} = {}) {
  if (authorizeSurfe === true || authorizePdl === true) {
    throw new Error("V4.1 ORS public CI bridge forbids Surfe/PDL authorization on this path");
  }
  const pageContent = html || textContent || "";
  const extraction = extractContactIntelligenceFromSource({
    source: { url },
    pageContent,
    opportunityContext: { organizationName },
    organizationContext: { name: organizationName },
  });
  const people = (extraction?.people || extraction?.namedPeople || extraction?.who || []).slice(0, 5);
  const functional = extraction?.functional || extraction?.functionalContacts || [];
  const emails = [
    ...functional.map((f) => f.email).filter(Boolean),
    ...(extraction?.emails || []).map((e) => (typeof e === "string" ? e : e?.email)).filter(Boolean),
    ...people.map((p) => p.email).filter(Boolean),
  ];
  const primaryEmail = emails[0] || null;
  const primaryPerson = people[0] || null;
  const personNameOf = (p) => p?.personName || p?.name || null;
  const roleOf = (p) => p?.title || p?.role || null;
  const ownership = classifyContactOwnership({
    email: primaryEmail,
    pageUrl: url,
    organizationName,
    personName: personNameOf(primaryPerson),
    roleTitle: roleOf(primaryPerson),
    publisherKind: classifyPublisherKind(url),
  });
  // Filter non-person labels (e.g. "Meetings Content Managers") before WHO pass.
  let looksLikePersonNameFn = null;
  try {
    // Lazy import avoided for CJS interop — use inline plural/role guard mirroring secondary-named-contact.
    looksLikePersonNameFn = (name) => {
      const n = String(name || "").replace(/\s+/g, " ").trim();
      if (!n || n === "UNKNOWN") return false;
      if (
        /\b(managers?|directors?|officers?|associates?|specialists?|coordinators?|meetings?|content|team|staff|office|desk|services?)\b/i.test(
          n,
        )
      ) {
        return false;
      }
      return /^[\p{Lu}][\p{L}'.-]+(?:\s+[\p{Lu}][\p{L}'.-]+){1,3}$/u.test(n);
    };
  } catch {
    looksLikePersonNameFn = (name) => Boolean(String(name || "").trim());
  }
  return {
    version: COMMERCIAL_SPINE_RESTORATION_V41,
    provider: "public_source_only",
    surfeInvoked: false,
    pdlInvoked: false,
    extraction,
    ownership,
    namedStakeholdersFound: people
      .filter((p) => personNameOf(p) && looksLikePersonNameFn(personNameOf(p)))
      .map((p) => ({
      name: personNameOf(p),
      title: roleOf(p),
      email: p.email || null,
    })),
    buyerContactPathClass: ownership.ownershipClass === CONTACT_OWNERSHIP_CLASS.FUNCTIONAL_DEPARTMENT
      ? BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT
      : ownership.ownershipClass === CONTACT_OWNERSHIP_CLASS.POTENTIAL_DECISION_MAKER
        || ownership.ownershipClass === CONTACT_OWNERSHIP_CLASS.VERIFIED_DECISION_MAKER
        ? BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_PERSON
        : ownership.ownershipClass === CONTACT_OWNERSHIP_CLASS.GENERAL_ORGANIZATION_CONTACT
          ? BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT
          : BUYER_CONTACT_PATH_CLASS.NO_CONTACT,
  };
}

/**
 * Honest sales brief — target hotel + ownership + verified vs inferred.
 */
export function buildHonestSalesBriefV41({
  targetHotel = "",
  organizationName = "",
  activity = "",
  revenueTypes = [],
  disposition = null,
  contactOwnership = null,
  geoFit = null,
  sourcingStatus = "UNKNOWN",
  verifiedFacts = [],
  inferredOpportunities = [],
  reasonToContactNow = null,
  recommendedAsk = null,
} = {}) {
  const ownership = contactOwnership?.ownershipClass || CONTACT_OWNERSHIP_CLASS.MISSING;
  const contactRoute = contactOwnership?.salesUsableAsProspectContact
    ? contactOwnership.route
    : null;
  const hotelFit = geoFit?.eventQualifiedForTargetHotel
    ? `Event/programme destination supports ${targetHotel} market.`
    : geoFit?.geoFitClass === GEO_FIT_CLASS.NAMED_COMPETITOR_HOST_ELSEWHERE
      ? `Do not pitch ${targetHotel} as host for this contracted event (${geoFit.namedHostHotel}). Organization-level future-cycle outreach only.`
      : geoFit?.organizationStillAccountEligible
        ? `${targetHotel} may be relevant for future cycles; destination not verified — do not invent location.`
        : `Hotel fit not evidenced for ${targetHotel}.`;

  const ask = recommendedAsk
    || (geoFit?.geoFitClass === GEO_FIT_CLASS.NAMED_COMPETITOR_HOST_ELSEWHERE
      ? `Ask ${contactRoute || "the contact"} about future meeting cycles and hotel shortlist criteria. Preserve ${geoFit.namedHostHotel} as the current host; do not imply ${targetHotel} is hosting the current event.`
      : disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY
        || disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
        ? `Introduce ${targetHotel} to ${contactRoute || "the contact"} for future programmes they source. Ask how they shortlist hotels. Do not invent an RFP or room block.`
        : `Do not pitch yet — close research gaps first.`);

  return {
    version: COMMERCIAL_SPINE_RESTORATION_V41,
    targetHotel: targetHotel || null,
    prospectiveOrganization: organizationName || null,
    demandGeneratingActivity: activity || null,
    revenueTypes: revenueTypes.length ? revenueTypes : [],
    commercialStage: disposition || null,
    contact: {
      route: contactRoute,
      ownershipClass: ownership,
      personName: contactOwnership?.personName || null,
      roleTitle: contactOwnership?.roleTitle || null,
      directoryRejected: ownership === CONTACT_OWNERSHIP_CLASS.THIRD_PARTY_DIRECTORY_CONTACT,
    },
    evidenceBackedHotelFit: hotelFit,
    sourcingStatus,
    reasonToContactNow: reasonToContactNow
      || (disposition === INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY
        ? "Active sourcing evidenced."
        : disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY
          ? "Prospective programme evidenced; sourcing still UNKNOWN."
          : disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
            || disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
            ? "Account / future-cycle introduction — no invented urgency."
            : "Not sales-ready."),
    recommendedQuestionOrPitch: ask,
    verifiedFacts: verifiedFacts.slice(0, 8),
    inferredOpportunities: inferredOpportunities.slice(0, 8),
    crossHotelCopyContamination: false,
    actionable: Boolean(
      organizationName
      && targetHotel
      && contactRoute
      && [
        INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY,
        INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY,
        INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT,
        INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE,
      ].includes(disposition)
      && geoFit?.geoFitClass !== GEO_FIT_CLASS.MISMATCH_EXCLUDE_EVENT,
    ),
  };
}

/**
 * Qualify with V4.1 identity/geo/contact gates layered on V3 (publication unchanged).
 */
export function qualifyCommercialProspectV41({
  opportunity = {},
  acceptedClaims = [],
  pageTexts = [],
  targetHotel = "",
  usableContactRoute = null,
  hotelFacts = null,
  pageUrl = null,
} = {}) {
  const blob = [
    ...acceptedClaims.map((c) => c.statement || c.excerpt || ""),
    ...pageTexts,
    opportunity.summaryWhyHotel,
    opportunity.evidenceQuote,
  ].join("\n");
  const url = pageUrl || opportunity.officialSource || opportunity.sourceUrl
    || (opportunity.evidenceUrls || [])[0] || "";

  const identity = resolveOrganizationIdentity({
    claimedOrganizationName: opportunity.organizationName,
    title: opportunity.programName || opportunity.eventName || "",
    url,
    excerpt: blob,
  });

  if (!identity.actionableCandidate && !opportunity.allowUnresolvedIdentityQualification) {
    return {
      version: COMMERCIAL_SPINE_RESTORATION_V41,
      v3: null,
      identity,
      geoFit: null,
      contactOwnership: classifyContactOwnership({ email: usableContactRoute, pageUrl: url }),
      internalDisposition: INTERNAL_DISPOSITION.EXCLUDE,
      commerciallyUsefulInternal: false,
      salesBrief: null,
      rejectReason: identity.rejectReason,
      publicationEligibilityUnchanged: true,
    };
  }

  const orgName = identity.organizationName || opportunity.organizationName;
  const housingPageEvidenceOnly = identity.housingPageEvidenceOnly === true
    || isOfficialHousingOrHotelListPage(url, opportunity.programName || opportunity.eventName || "");

  // Housing pages prove lodging arrangements exist — inject evidence language so V3 can
  // score VERIFIED_ACCOMMODATION_DEMAND. They do NOT prove open Bethesda sourcing.
  const pageTextsForV3 = housingPageEvidenceOnly
    ? [
      ...pageTexts,
      "Official conference housing / hotel accommodations page for attendees and exhibitors.",
      "Room block and delegate housing information published by the organizing association.",
    ]
    : pageTexts;

  const blobForGeo = [
    blob,
    ...(housingPageEvidenceOnly ? pageTextsForV3.slice(-2) : []),
  ].join("\n");

  const geoFit = assessHotelGeographicFit({
    targetHotel,
    evidenceBlob: blobForGeo,
    namedHostHotel: opportunity.namedHostHotel || null,
    nationalOrganization: true,
  });

  const contactOwnership = classifyContactOwnership({
    email: usableContactRoute || opportunity.contactEmail || opportunity.contact?.businessEmail,
    pageUrl: url,
    organizationName: orgName,
    organizationDomain: identity.publisherKind === PUBLISHER_KIND.ORGANIZATION_OWNED
      ? identity.publisherHost : opportunity.organizationDomain,
    personName: opportunity.contact?.personName || null,
    roleTitle: opportunity.contact?.roleTitle || null,
    publisherKind: identity.publisherKind,
  });

  // Directory contacts are not usable for commerciallyUsefulInternal.
  // A housing page URL alone is NOT a sales-usable contact route.
  const routeForV3 = contactOwnership.salesUsableAsProspectContact
    ? contactOwnership.route
    : null;

  const oppForV3 = {
    ...opportunity,
    organizationName: orgName,
    namedHostHotel: geoFit.namedHostHotel || opportunity.namedHostHotel || null,
    // Event-level guestroom fit blocked when competitor host elsewhere
    hotelCatchmentMismatch: geoFit.geoFitClass === GEO_FIT_CLASS.NAMED_COMPETITOR_HOST_ELSEWHERE
      && !opportunity.overflowEvidenced,
    housingPageEvidenceOnly,
    // Housing page ≠ currently actionable buying; keep identifiable program if named
    currentlySourcing: housingPageEvidenceOnly ? false : opportunity.currentlySourcing,
    activeRfp: housingPageEvidenceOnly ? false : opportunity.activeRfp,
  };

  const v3 = qualifyCommercialProspectV3({
    opportunity: oppForV3,
    acceptedClaims,
    pageTexts: pageTextsForV3,
    targetHotel,
    usableContactRoute: routeForV3,
    hotelFacts,
  });

  let disposition = v3.internalDisposition;
  let useful = v3.commerciallyUsefulInternal === true;

  // Housing evidence without ownership-valid contact → WATCHLIST (research), not ACTIVE/EXCLUDE-as-noise.
  // With ownership-valid contact → ACCOUNT_DEVELOPMENT at best (never invent ACTIVE from housing page alone).
  if (housingPageEvidenceOnly) {
    if (disposition === INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY
      || disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY) {
      disposition = geoFit.geoFitClass === GEO_FIT_CLASS.NAMED_COMPETITOR_HOST_ELSEWHERE
        || opportunity.futureCycleSupported
        ? INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
        : INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT;
    }
    if (!routeForV3) {
      useful = false;
      if (disposition === INTERNAL_DISPOSITION.EXCLUDE) {
        disposition = INTERNAL_DISPOSITION.WATCHLIST_RESEARCH;
      } else if (
        disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
        || disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
        || disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY
      ) {
        disposition = INTERNAL_DISPOSITION.WATCHLIST_RESEARCH;
      }
    } else {
      useful = disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
        || disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE;
    }
  }

  // Event with competitor host elsewhere: demote event-level ACTIVE/PRE_SOURCING;
  // keep ACCOUNT_DEVELOPMENT / COMPETITIVE_FUTURE_CYCLE for the organization.
  if (geoFit.geoFitClass === GEO_FIT_CLASS.NAMED_COMPETITOR_HOST_ELSEWHERE) {
    if (disposition === INTERNAL_DISPOSITION.ACTIVE_OPPORTUNITY
      || disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY) {
      disposition = opportunity.futureCycleSupported || v3.competitiveFutureCycleProspect
        ? INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE
        : INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT;
    }
    // ACCOUNT_DEVELOPMENT OK at org level; not "Bethesda hosts this Philadelphia meeting"
    if (!routeForV3) useful = false;
    else if (disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
      || disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE) {
      useful = Boolean(routeForV3);
    } else {
      useful = false;
    }
  }

  if (!routeForV3) {
    useful = false;
    if (disposition !== INTERNAL_DISPOSITION.EXCLUDE
      && disposition !== INTERNAL_DISPOSITION.WATCHLIST_RESEARCH) {
      // Keep stage but not useful without ownership-valid contact
    }
  }

  if (identity.directoryListingNotContactOwner && !routeForV3) {
    useful = false;
    if (disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY
      || disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT) {
      disposition = INTERNAL_DISPOSITION.WATCHLIST_RESEARCH;
    }
  }

  const revenueTypes = Object.entries(v3.revenueOpportunities || {})
    .filter(([, r]) => r?.commerciallyRelevant)
    .map(([k]) => k);

  const salesBrief = buildHonestSalesBriefV41({
    targetHotel,
    organizationName: orgName,
    activity: opportunity.programName || opportunity.eventName || opportunity.demandSignalSummary || "",
    revenueTypes,
    disposition,
    contactOwnership,
    geoFit,
    sourcingStatus: v3.sourcingStatus || "UNKNOWN",
    verifiedFacts: [
      geoFit.namedHostHotel ? `Named host: ${geoFit.namedHostHotel}` : null,
      contactOwnership.route && contactOwnership.salesUsableAsProspectContact
        ? `Contact route (${contactOwnership.ownershipClass}): ${contactOwnership.route}`
        : null,
      ...acceptedClaims.slice(0, 3).map((c) => text(c.statement).slice(0, 160)),
    ].filter(Boolean),
    inferredOpportunities: revenueTypes.includes(REVENUE_TYPE.GUESTROOMS)
      && v3.revenueOpportunities?.[REVENUE_TYPE.GUESTROOMS]?.inferredOnly
      ? ["Inferred lodging potential — not a confirmed room block"]
      : [],
  });

  return {
    version: COMMERCIAL_SPINE_RESTORATION_V41,
    v3: { ...v3, internalDisposition: disposition, commerciallyUsefulInternal: useful },
    identity,
    geoFit,
    contactOwnership,
    internalDisposition: disposition,
    commerciallyUsefulInternal: useful,
    salesBrief,
    rejectReason: null,
    publicationEligibilityUnchanged: true,
  };
}

/**
 * Drain helper: select next lead including pendingDiscoverySearches URLs.
 */
export function buildLeadPursueQueueFromState(state = {}, { targetHotel = "", remainingBudgetUsd = Infinity } = {}) {
  const blob = [
    ...(state.evidence || []).map((e) => e.quote || e.excerpt || e.statement || ""),
    state.demandSignalSummary,
  ].join("\n");
  const planned = planAutonomousLeadFollowing({
    findingsText: blob,
    sourceUrl: (state.evidence || []).map((e) => e.url).filter(Boolean).at(-1) || state.officialSource || "",
    organizationHost: String(state.officialDomain || state.organizationDomain || "").replace(/^www\./i, ""),
    organizationName: state.organizationName || "",
    investigatedUrls: [
      ...(state.evidence || []).map((e) => e.url).filter(Boolean),
      ...(state.investigatedLeadUrls || []),
    ],
    targetHotel,
    remainingBudgetUsd,
    maxPursue: 5,
  });

  const pending = [
    ...(state.pendingDiscoverySearches || []),
    ...(state.discoveryExpansionV4?.executableNow || []),
  ];
  const extra = [];
  for (const item of pending) {
    const url = item?.url || item?.suggestedUrl || (typeof item === "string" && /^https?:/i.test(item) ? item : null);
    const name = item?.entityName || item?.organizationName || item?.question || "pending_discovery_lead";
    if (!url) continue;
    const lead = {
      entityName: name,
      entityType: item?.entityType || "programme",
      suggestedUrl: url,
      suggestedUrlIndependentPublisher: item?.independentPublisher === true,
      note: item?.limitation || item?.note || "from_pending_discovery_searches",
      researchQuestion: item?.researchQuestion || item?.question || `Follow related entity / source: ${name}`,
    };
    const ranking = scoreRelatedEntityLead(lead, { targetHotel, remainingBudgetUsd });
    extra.push({
      ...lead,
      commercialValueScore: ranking.score,
      pursueRecommended: ranking.pursue,
      rankingReasons: ranking.reasons,
    });
  }

  const merged = [...(planned.pursueQueue || planned.leadsToPursue || []), ...extra.filter((l) => l.pursueRecommended)]
    .sort((a, b) => (b.commercialValueScore || 0) - (a.commercialValueScore || 0));

  // Dedupe by URL
  const seen = new Set();
  const pursue = [];
  for (const lead of merged) {
    const key = String(lead.suggestedUrl || "");
    if (!key || seen.has(key)) continue;
    seen.add(key);
    pursue.push(lead);
  }
  return { version: COMMERCIAL_SPINE_RESTORATION_V41, pursue, planned, pendingInjected: extra.length };
}

/**
 * Execute bounded lead drain through an ORS-compatible researcher (mocked or live).
 * Separates provider restriction from commercial exhaustion.
 */
export async function drainRelatedEntityFollowUpsThroughOrs({
  state = {},
  researcher = null,
  targetHotel = "",
  remainingBudgetUsd = Infinity,
  maxFollowUps = 3,
  paidBlocked = true,
  checkpoint = null,
} = {}) {
  const started = { ...(state || {}) };
  started.investigatedLeadUrls = [...(started.investigatedLeadUrls || [])];
  started.researchAttempts = [...(started.researchAttempts || [])];
  started.notes = [...(started.notes || [])];
  started.evidence = [...(started.evidence || [])];

  const queuePack = buildLeadPursueQueueFromState(started, { targetHotel, remainingBudgetUsd });
  let followed = (started.researchAttempts || []).filter((a) => a.strategy === "related_entity_followup").length;
  const executed = [];
  const skipped = [];

  while (followed < maxFollowUps) {
    if (checkpoint?.shouldStop?.()) {
      skipped.push({ reason: "checkpoint_interrupt" });
      break;
    }
    const pick = selectNextAuthorizedLeadFollowUp({
      pursueQueue: queuePack.pursue,
      investigatedUrls: [
        ...(started.evidence || []).map((e) => e.url).filter(Boolean),
        ...(started.investigatedLeadUrls || []),
      ],
      alreadyFollowedCount: followed,
      maxFollowUps,
      remainingBudgetUsd,
      commerciallyUsefulInternal: started.commerciallyUsefulInternal === true,
    });
    if (!pick.lead) {
      skipped.push({ reason: pick.skipReason || "no_leads" });
      break;
    }
    if (!researcher?.execute) {
      skipped.push({
        reason: paidBlocked ? "provider_or_researcher_unavailable_not_exhaustion" : "researcher_missing",
        lead: pick.lead.entityName,
        url: pick.lead.suggestedUrl,
      });
      started.notes.push(`ors_lead_followup_unexecuted:${pick.skipReason || "no_researcher"}:${pick.lead.entityName}`);
      break;
    }
    const leadResult = await researcher.execute({
      opportunity: started,
      action: {
        strategy: "related_entity_followup",
        queries: [pick.lead.suggestedUrl],
        provider: "direct_public",
        fingerprint: pick.fingerprint,
        paid: false,
        lead: pick.lead,
      },
      options: { paidBlocked, remainingBudgetUsd },
    });
    started.investigatedLeadUrls.push(pick.lead.suggestedUrl);
    started.researchAttempts.push({
      at: new Date().toISOString(),
      strategy: "related_entity_followup",
      fingerprint: pick.fingerprint,
      notes: `related_entity_followup:${pick.lead.entityName}`,
      newMaterialEvidence: Boolean(leadResult?.newMaterialEvidence),
      evidenceIds: (leadResult?.evidence || []).map((e) => e.url).filter(Boolean),
    });
    if (leadResult?.evidence?.length) {
      started.evidence.push(...leadResult.evidence);
    }
    started.notes.push(`ors_executed_related_entity_followup:${pick.lead.entityName}`);
    executed.push({
      entityName: pick.lead.entityName,
      url: pick.lead.suggestedUrl,
      executed: true,
    });
    followed += 1;
    checkpoint?.persist?.(started);
  }

  const stillPending = selectNextAuthorizedLeadFollowUp({
    pursueQueue: queuePack.pursue,
    investigatedUrls: [
      ...(started.evidence || []).map((e) => e.url).filter(Boolean),
      ...(started.investigatedLeadUrls || []),
    ],
    alreadyFollowedCount: followed,
    maxFollowUps,
    remainingBudgetUsd,
    commerciallyUsefulInternal: started.commerciallyUsefulInternal === true,
  });

  return {
    version: COMMERCIAL_SPINE_RESTORATION_V41,
    state: started,
    executed,
    skipped,
    relatedEntityLeadsFollowed: executed.length,
    unexecutedRemaining: stillPending.lead
      ? [{ entityName: stillPending.lead.entityName, url: stillPending.lead.suggestedUrl, reason: "authorization_budget_or_cap" }]
      : [],
    stopIsNotExhaustion: skipped.some((s) => /provider|researcher|authorization|budget/i.test(s.reason || "")),
    maxIterationsIsNotExhaustion: true,
  };
}

/**
 * Regression guards — disabled paid contact providers must not run.
 */
export function assertPaidContactProvidersDisabled({
  authorizeSurfe = false,
  authorizePdl = false,
  surfeCalls = 0,
  pdlCalls = 0,
} = {}) {
  if (authorizeSurfe === true) {
    return { ok: false, error: "Surfe requires explicit separate authorization — blocked on V4.1 spine default" };
  }
  if (authorizePdl === true) {
    return { ok: false, error: "PDL disabled until pricing/permissions/spend authorized" };
  }
  if (surfeCalls > 0 || pdlCalls > 0) {
    return { ok: false, error: "Paid contact provider calls detected without authorization" };
  }
  return { ok: true, surfeNonPersistencePreserved: true, pdlDisabled: true };
}

export {
  planAutonomousLeadFollowing,
  selectNextAuthorizedLeadFollowUp,
};
