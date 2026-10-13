/**
 * Targeted second-pass research for venue partnership qualification (V1.4).
 * No broad market rediscovery — venue/domain-scoped queries only.
 */

import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../hotel-intelligence/room-count-research/fetch.js";
import { ON_SITE_LODGING_STATUS, PARTNER_STATUS } from "./constants.js";
import {
  classifyEventActivityEvidence,
  extractActivityEvidenceFromPage,
  classifyPartnerStatus,
  isDirectoryUrl,
} from "./activity-evidence.js";
import { classifyBuyerPath } from "./lodging-capture.js";
import { qualifyVenuePartnership } from "./qualify.js";
import { buildHotelVenueRelationship } from "./hotel-context.js";

function clean(s) {
  return String(s || "").trim();
}

function domainOf(venue = {}) {
  const raw = clean(venue.officialDomain || venue.website);
  return raw
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .split("/")[0]
    .toLowerCase();
}

/**
 * Build venue-scoped search families (no hotel/city hardcodes).
 */
export function buildPartnershipSecondPassQueries(venue = {}) {
  const name = clean(venue.venueName);
  const domain = domainOf(venue);
  const qs = [];
  if (domain) {
    for (const term of [
      "wedding",
      "private events",
      "events",
      "event packages",
      "accommodations",
      "hotels",
      "preferred hotel",
      "wedding gallery",
    ]) {
      qs.push({ family: "site", q: `site:${domain} ${term}` });
    }
  }
  if (name) {
    for (const term of [
      "weddings",
      "wedding planner",
      "wedding 2025",
      "wedding 2026",
      "event director",
      "preferred hotel",
      "accommodations",
    ]) {
      qs.push({ family: "name", q: `"${name}" ${term}` });
    }
  }
  return qs;
}

async function serpOrganic(query, { num = 6 } = {}) {
  const result = await serpapiSearch({
    engine: "google",
    q: query,
    num,
    hl: "en",
    gl: "us",
  });
  if (!result.ok) {
    return { ok: false, error: result.error, organic: [], charged: 0 };
  }
  const organic = (result.data?.organic_results || []).map((r) => ({
    title: r.title || null,
    url: r.link || r.url || null,
    snippet: r.snippet || null,
  }));
  return { ok: true, organic, charged: result.charged ?? 1 };
}

async function fetchPage(url, { maxChars = 7000 } = {}) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, url, error: "bad_url" };
  }
  const page = await fetchResearchPage(url, { timeoutMs: 20000 });
  if (!page.ok) {
    return { ok: false, url, error: page.error || `status_${page.status}` };
  }
  const text = htmlToSearchableText(page.text).replace(/\s+/g, " ").trim();
  return {
    ok: true,
    url: page.url || url,
    text: text.slice(0, maxChars),
    directory: isDirectoryUrl(page.url || url),
  };
}

function inferLodgingFromBlob(blob, prior) {
  if (prior && prior !== ON_SITE_LODGING_STATUS.UNKNOWN) return prior;
  if (
    /\bno\s+(?:on[- ]?site\s+)?(?:hotel|lodging|guestrooms?|overnight)\b|\bno\s+overnight\s+accommodations?\b/i.test(
      blob
    )
  ) {
    return ON_SITE_LODGING_STATUS.NO_LODGING;
  }
  if (
    /\blimited\s+(?:on[- ]?site\s+)?(?:rooms?|lodging|guestrooms?)\b|\bfew\s+guestrooms?\b/i.test(
      blob
    )
  ) {
    return ON_SITE_LODGING_STATUS.LIMITED_LODGING;
  }
  if (
    /\b(?:\d{2,3})\s+(?:guest\s*)?rooms?\b|\bon[- ]?site\s+(?:hotel|lodging|accommodations?)\b/i.test(
      blob
    )
  ) {
    return ON_SITE_LODGING_STATUS.ADEQUATE_LODGING;
  }
  // Clubs / mansions / gardens without lodging language → NO_LODGING when event venue
  if (
    /\b(?:country\s+club|mansion|garden|sanctuary|museum)\b/i.test(blob) &&
    !/\bhotel\s+rooms?|guestrooms?|overnight\b/i.test(blob)
  ) {
    return ON_SITE_LODGING_STATUS.NO_LODGING;
  }
  return prior || ON_SITE_LODGING_STATUS.UNKNOWN;
}

function inferPartnerFromBlob(blob, venue) {
  if (/exclusive\s+(?:hotel|lodging)\s+partner|sole\s+(?:hotel|lodging)\s+partner/i.test(blob)) {
    return {
      exclusiveHotelRelationship: true,
      partnerStatus: PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND,
      partnerResearchComplete: true,
    };
  }
  if (/preferred\s+hotel|hotel\s+partner|partner\s+hotel|room\s+block\s+partner/i.test(blob)) {
    return {
      preferredHotelListed: true,
      partnerStatus: PARTNER_STATUS.PREFERRED_PARTNER_FOUND,
      partnerResearchComplete: true,
    };
  }
  return {
    partnerResearchComplete: true,
    partnerStatus:
      venue.partnerStatus === PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND
        ? PARTNER_STATUS.EXCLUSIVE_PARTNER_FOUND
        : PARTNER_STATUS.NO_PUBLIC_PARTNER_FOUND,
  };
}

function inferCommercialPath(blob, venue) {
  const roleMatch = blob.match(
    /(?:contact|email|call)\s+(?:our\s+)?(director\s+of\s+events|event\s+director|events?\s+manager|venue\s+sales|catering\s+director|event\s+coordinator)/i
  );
  const hasInquiry =
    /\b(?:request\s+(?:a\s+)?(?:tour|proposal)|inquire|inquiry\s+form|book\s+(?:a\s+)?tour|contact\s+us\s+to\s+(?:book|plan))\b/i.test(
      blob
    );
  const out = { ...venue };
  if (roleMatch) {
    out.eventContactRole = roleMatch[1];
    out.eventContact = out.eventContact || "events team";
  }
  if (hasInquiry) {
    out.hasPublicInquiryForm = true;
    out.publicBookingLanguage = true;
    if (!out.eventContactRole && !out.eventContact) {
      out.organizationPath = "Public inquiry / booking form on venue site";
      out.commercialContactPath = "ORGANIZATION_PATH";
    }
  }
  return out;
}

/**
 * Second-pass research for one venue against one hotel context.
 */
export async function researchVenuePartnershipSecondPass({
  venue,
  hotelCtx,
  relationship: priorRel = null,
  maxQueries = 10,
  maxFetches = 8,
} = {}) {
  const ledger = {
    serpQueries: 0,
    serpCharged: 0,
    fetches: 0,
    errors: [],
  };
  const queries = buildPartnershipSecondPassQueries(venue).slice(0, maxQueries);
  const organic = [];
  for (const item of queries) {
    const serp = await serpOrganic(item.q, { num: 5 });
    ledger.serpQueries += 1;
    ledger.serpCharged += serp.charged || 0;
    if (!serp.ok) {
      ledger.errors.push({ stage: "serp", q: item.q, error: serp.error });
      continue;
    }
    for (const row of serp.organic || []) {
      if (row.url) organic.push({ ...row, query: item.q, family: item.family });
    }
  }

  // Prefer official domain URLs, then unique paths
  const domain = domainOf(venue);
  const ranked = [...organic].sort((a, b) => {
    const aOff = domain && hostIncludes(a.url, domain) ? 0 : 1;
    const bOff = domain && hostIncludes(b.url, domain) ? 0 : 1;
    if (aOff !== bOff) return aOff - bOff;
    return 0;
  });

  const seenUrl = new Set();
  const evidenceRaw = [];
  let blobAll = "";
  let partnerHints = {};
  let lodgingStatus = venue.onSiteLodgingStatus;
  let enrichedVenue = { ...venue };

  for (const row of ranked) {
    if (ledger.fetches >= maxFetches) break;
    const url = row.url;
    if (!url || seenUrl.has(url)) continue;
    seenUrl.add(url);
    // Skip pure couple directory deep-links
    if (/theknot\.com\/us\/wedding|weddingwire\.com\/weddings\//i.test(url)) continue;

    const page = await fetchPage(url);
    ledger.fetches += 1;
    if (!page.ok) {
      ledger.errors.push({ stage: "fetch", url, error: page.error });
      // Still use SERP snippet
      evidenceRaw.push(
        ...extractActivityEvidenceFromPage({
          url,
          text: "",
          venue,
          title: row.title,
          snippet: row.snippet,
        })
      );
      continue;
    }
    blobAll += `\n${page.text}`;
    evidenceRaw.push(
      ...extractActivityEvidenceFromPage({
        url: page.url,
        text: page.text,
        venue,
        title: row.title,
        snippet: row.snippet,
      })
    );
    lodgingStatus = inferLodgingFromBlob(page.text, lodgingStatus);
    partnerHints = {
      ...partnerHints,
      ...inferPartnerFromBlob(page.text, enrichedVenue),
    };
    enrichedVenue = inferCommercialPath(page.text, enrichedVenue);
  }

  // Also seed from known website if not fetched
  if (venue.website && !seenUrl.has(venue.website) && ledger.fetches < maxFetches) {
    const page = await fetchPage(venue.website);
    ledger.fetches += 1;
    if (page.ok) {
      blobAll += `\n${page.text}`;
      evidenceRaw.push(
        ...extractActivityEvidenceFromPage({
          url: page.url,
          text: page.text,
          venue,
        })
      );
      lodgingStatus = inferLodgingFromBlob(page.text, lodgingStatus);
      partnerHints = {
        ...partnerHints,
        ...inferPartnerFromBlob(page.text, enrichedVenue),
      };
      enrichedVenue = inferCommercialPath(page.text, enrichedVenue);
    }
  }

  if (
    /wedding/i.test(blobAll) ||
    venue.weddingsAdvertised ||
    evidenceRaw.some((e) => e.evidenceType === "OFFICIAL_WEDDING_PAGE")
  ) {
    enrichedVenue.weddingsAdvertised = true;
  }
  if (
    /private\s+events?|special\s+events?/i.test(blobAll) ||
    venue.privateEventsAdvertised
  ) {
    enrichedVenue.privateEventsAdvertised = true;
  }

  enrichedVenue = {
    ...enrichedVenue,
    onSiteLodgingStatus: lodgingStatus,
    ...partnerHints,
    activityEvidence: [
      ...(venue.activityEvidence || []),
      ...evidenceRaw,
    ],
  };

  // If partner research ran and no exclusive/preferred found → NO_PUBLIC_PARTNER_FOUND
  if (
    partnerHints.partnerResearchComplete &&
    !partnerHints.exclusiveHotelRelationship &&
    !partnerHints.preferredHotelListed
  ) {
    enrichedVenue.partnerStatus = PARTNER_STATUS.NO_PUBLIC_PARTNER_FOUND;
    enrichedVenue.partnerResearchComplete = true;
  }

  const activity = classifyEventActivityEvidence(
    enrichedVenue,
    enrichedVenue.activityEvidence
  );
  enrichedVenue.eventActivityEvidenceStatus = activity.eventActivityEvidenceStatus;
  enrichedVenue.activityEvidence = activity.activityEvidence;
  enrichedVenue.activityDecisionReason = activity.decisionReason;

  const partner = classifyPartnerStatus(enrichedVenue);
  enrichedVenue.partnerStatus = partner.partnerStatus;

  const buyerPath = classifyBuyerPath(enrichedVenue);
  const relationship =
    priorRel ||
    buildHotelVenueRelationship(hotelCtx, enrichedVenue);

  const qualification = qualifyVenuePartnership({
    venue: enrichedVenue,
    hotelCtx,
    relationship,
  });

  return {
    venue: enrichedVenue,
    relationship,
    activity,
    partner,
    buyerPath,
    qualification,
    ledger,
    evidenceMatrix: (activity.activityEvidence || []).map((e) => ({
      evidenceType: e.evidenceType,
      source: e.sourceUrl,
      date: e.sourceDate || e.eventDate || null,
      official: Boolean(e.official),
      unique: true,
    })),
    duplicatesRemoved: activity.duplicatesRemoved || 0,
  };
}

function hostIncludes(url, domain) {
  try {
    const h = new URL(url).hostname.toLowerCase();
    return h === domain || h.endsWith(`.${domain}`);
  } catch {
    return false;
  }
}
