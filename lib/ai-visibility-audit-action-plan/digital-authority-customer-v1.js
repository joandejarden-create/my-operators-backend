/**
 * Customer-facing Digital Authority normalization for AVA V4.
 * Consolidates duplicate host outreach; classifies destination health;
 * keeps Observation / Commercial relevance / Action as distinct fields.
 */
import {
  customerVisibleText,
  extractHttpUrl,
} from "./customer-facing-serialize-v1.js";

export const AVA_DIGITAL_AUTHORITY_CUSTOMER_V1 =
  "AVA_DIGITAL_AUTHORITY_CUSTOMER_V1";

export const SOURCE_TYPES = Object.freeze({
  CVB: "CONVENTION_AND_VISITORS_BUREAU",
  DESTINATION_TOURISM: "OFFICIAL_DESTINATION_TOURISM",
  TRAVEL_PUBLICATION: "TRAVEL_PUBLICATION",
  LEISURE_GUIDE: "LEISURE_GUIDE",
  WEDDING_DIRECTORY: "WEDDING_DIRECTORY",
  GOLF_DIRECTORY: "GOLF_DIRECTORY",
  BOOKING_PLATFORM: "HOTEL_BOOKING_PLATFORM",
  ASSOCIATION: "ASSOCIATION_WEBSITE",
  COMMERCIAL_PARTNER: "COMMERCIAL_PARTNER",
  OTHER: "OTHER_REFERRING_SOURCE",
});

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Classify referring source from host + labels — never guess CVB when evidence is a leisure guide.
 */
export function classifyReferringSourceType({ host = "", name = "", url = "", note = "" }) {
  const blob = `${host} ${name} ${url} ${note}`.toLowerCase();
  if (/weddingwire|theknot|wedding/i.test(blob)) return SOURCE_TYPES.WEDDING_DIRECTORY;
  if (/golfbreaks|golf\.|arabellagolf|golf director/i.test(blob)) return SOURCE_TYPES.GOLF_DIRECTORY;
  if (/booking\.com|expedia|hotels\.com|tripadvisor/i.test(blob)) return SOURCE_TYPES.BOOKING_PLATFORM;
  if (
    /convention.?bureau|cvb|acorunacb|mallorcaconvention|visitadores|mice/i.test(blob) ||
    /\/ficha\/|espacios-mice/i.test(blob)
  ) {
    return SOURCE_TYPES.CVB;
  }
  if (
    /visit-?palma|visit[a-z-]*\.(com|org)|tourism|tourismus|official destination/i.test(blob)
  ) {
    return SOURCE_TYPES.DESTINATION_TOURISM;
  }
  if (/greatlocations|timeout|lonelyplanet|tripadvisor.?blog|travel.?mag/i.test(blob)) {
    return SOURCE_TYPES.LEISURE_GUIDE;
  }
  if (/things-to-do|this-summer|leisure|local guide|city guide/i.test(blob)) {
    return SOURCE_TYPES.LEISURE_GUIDE;
  }
  if (/infogal|paxinasgalegas|directory|comercial\.es|association|chamber/i.test(blob)) {
    return SOURCE_TYPES.ASSOCIATION;
  }
  if (/partner|marina|dock|sponsor/i.test(blob)) return SOURCE_TYPES.COMMERCIAL_PARTNER;
  if (/magazine|journal|news|press|blog/i.test(blob)) return SOURCE_TYPES.TRAVEL_PUBLICATION;
  return SOURCE_TYPES.OTHER;
}

function sourceTypeLabel(type) {
  switch (type) {
    case SOURCE_TYPES.CVB:
      return "convention and visitors bureau profile";
    case SOURCE_TYPES.DESTINATION_TOURISM:
      return "official destination / tourism page";
    case SOURCE_TYPES.TRAVEL_PUBLICATION:
      return "travel publication";
    case SOURCE_TYPES.LEISURE_GUIDE:
      return "leisure / local activities guide";
    case SOURCE_TYPES.WEDDING_DIRECTORY:
      return "wedding directory listing";
    case SOURCE_TYPES.GOLF_DIRECTORY:
      return "golf holiday / golf directory listing";
    case SOURCE_TYPES.BOOKING_PLATFORM:
      return "booking platform";
    case SOURCE_TYPES.ASSOCIATION:
      return "business / destination association listing";
    case SOURCE_TYPES.COMMERCIAL_PARTNER:
      return "commercial partner page";
    default:
      return "referring website";
  }
}

function classifyDestination(destUrl, officialUrls = []) {
  if (!destUrl) {
    return {
      status: "UNLINKED_OR_UNKNOWN",
      note: "No property-specific hyperlink confirmed.",
    };
  }
  const h = hostOf(destUrl);
  const officialHosts = officialUrls.map(hostOf).filter(Boolean);
  if (officialHosts.some((o) => h === o || (o && h.endsWith(o)) || destUrl.includes(o))) {
    return {
      status: "CURRENT_CANONICAL_OR_OFFICIAL",
      note: "Destination matches a verified hotel/brand URL.",
    };
  }
  if (/hotelacacoruna|waterstoneboca|castillosonvida(?!mallorca)/i.test(h)) {
    return {
      status: "CURRENT_CANONICAL_OR_OFFICIAL",
      note: "Destination matches a hotel-operated property domain.",
    };
  }
  if (/marriott\.com|hilton\.com|hyatt\.com|ihg\.com/i.test(h)) {
    return {
      status: "CURRENT_BRAND_PROPERTY",
      note: "Destination is a live brand property URL.",
    };
  }
  if (/castillosonvidamallorca|waterstoneresort\.com/i.test(h)) {
    return {
      status: "HISTORICAL_OR_LEGACY_DOMAIN",
      note: "Destination uses a historical/independent property domain — verify redirect health before requesting a change.",
    };
  }
  return {
    status: "THIRD_PARTY_OR_UNRESOLVED",
    note: "Destination is a third-party or unresolved URL — confirm before prescribing an update.",
  };
}

function officialUrlCandidates(payload) {
  const out = [];
  const push = (u) => {
    const x = extractHttpUrl(u);
    if (x) out.push(x);
  };
  push(payload?.property?.website);
  push(payload?.digitalAuthority?.officialWebsite);
  for (const a of payload?.platformListingDiscoveryV3?.assessments || []) {
    if (/BRAND|HOTEL_OPERATED|WEBSITE/i.test(a.category || a.platform || "")) {
      push(a.url);
    }
  }
  return [...new Set(out)];
}

function hotelShortName(payload) {
  return customerVisibleText(
    payload?.property?.name || payload?.digitalAuthority?.hotelName || "the hotel",
    "the hotel"
  );
}

function buildDistinctCopy({
  kind,
  sourceName,
  sourceType,
  hotelName,
  pageCount,
  notes,
  destClass,
  primaryDest,
}) {
  const typeLabel = sourceTypeLabel(sourceType);
  const note = notes.filter(Boolean)[0] || "";
  let observation;
  let why;
  let action;

  if (kind.startsWith("Verified healthy")) {
    observation = `${sourceName} (${typeLabel}) includes a verified property-specific hyperlink to ${
      primaryDest || "the hotel’s approved brand or hotel-operated URL"
    }.`;
    why = `This ${typeLabel} already provides a working referral path for travelers who discover ${hotelName} through that channel.`;
    action =
      "No correction required. Periodically confirm the live destination still resolves to the hotel-approved URL.";
  } else if (/historical/i.test(kind)) {
    observation = `${sourceName} (${typeLabel}) links to a historical property domain${
      primaryDest ? ` (${primaryDest})` : ""
    } rather than the current brand/hotel-operated URL.`;
    why = `Legacy destinations can still convert if they redirect correctly; changing them without cause can break established referral paths for ${hotelName}.`;
    action =
      "Verify whether the historical destination still redirects to the approved hotel/brand URL. Request an update only if it is broken, misleading, or no longer intended — do not change a healthy legacy redirect solely because a newer brand URL exists.";
  } else if (/Unlinked/i.test(kind)) {
    observation =
      note && !/strengthen|destination discovery|digital authority/i.test(note)
        ? note
        : `${sourceName} (${typeLabel}) references ${hotelName}${
            pageCount > 1 ? ` across ${pageCount} pages` : ""
          } without a confirmed property-specific hyperlink to the hotel’s official website.`;
    if (sourceType === SOURCE_TYPES.LEISURE_GUIDE) {
      why = `Readers using this ${typeLabel} are already researching local leisure activities near ${hotelName}; a correct hotel link can turn that interest into a direct visit or inquiry.`;
      action = `Contact the publisher of ${sourceName} to request an appropriate property-specific hyperlink to the hotel’s approved official website, and confirm the hotel name and location are accurate.`;
    } else if (sourceType === SOURCE_TYPES.CVB) {
      why = `Meeting and leisure planners commonly use this ${typeLabel} when shortlisting hotels; an accurate official URL reduces friction between discovery and booking.`;
      action = `Request that the CVB update the hotel profile with the approved official website URL and verify meeting/capacity facts against the hotel source of truth.`;
    } else if (sourceType === SOURCE_TYPES.DESTINATION_TOURISM) {
      why = `Official destination pages influence how visitors evaluate lodging options in this market.`;
      action = `Ask the destination organization to add or correct the property-specific hyperlink to the hotel-approved URL.`;
    } else if (sourceType === SOURCE_TYPES.WEDDING_DIRECTORY) {
      why = `Wedding planners discover venues on this ${typeLabel}; the official URL affects lead quality.`;
      action = `Confirm the listing contents in a browser, then submit the hotel-approved official website as the primary property URL if missing or incorrect.`;
    } else if (sourceType === SOURCE_TYPES.GOLF_DIRECTORY) {
      why = `Golf travelers use this ${typeLabel} when packaging stays with tee times; a correct hotel link supports that journey.`;
      action = `Request a property-specific hyperlink to the hotel-approved URL on the golf listing.`;
    } else {
      why = `This ${typeLabel} already mentions ${hotelName}; adding the correct official URL makes the reference actionable.`;
      action = `Contact ${sourceName} to request a property-specific hyperlink to the hotel’s approved official website where the publisher allows it.`;
    }
  } else {
    const destPhrase =
      destClass.status === "THIRD_PARTY_OR_UNRESOLVED"
        ? "the destination URL is third-party or unresolved and needs hotel confirmation"
        : destClass.status === "HISTORICAL_OR_LEGACY_DOMAIN"
          ? "the destination uses a historical/legacy domain that should be verified"
          : destClass.status === "CURRENT_BRAND_PROPERTY"
            ? "the destination is a live brand property URL"
            : destClass.status === "CURRENT_CANONICAL_OR_OFFICIAL"
              ? "the destination matches an approved hotel/brand URL"
              : "the destination status needs hotel confirmation";
    observation = `${sourceName} (${typeLabel}) cites ${hotelName}; ${destPhrase}.`;
    why = `Incorrect destinations on referring sites can send guests to outdated or unrelated pages.`;
    action =
      "Review the referring page and destination with hotel marketing; update only if the hotel confirms the destination is incorrect.";
  }

  // Guarantee Observation ≠ Why (no copy-paste)
  if (normEq(observation, why)) {
    why = `Commercially, this ${typeLabel} is relevant to how potential guests discover ${hotelName} online.`;
  }

  return { observation, why, action, sourceTypeLabel: typeLabel };
}

function normEq(a, b) {
  const n = (s) =>
    String(s || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  return n(a) === n(b) || (n(a).length > 40 && (n(b).includes(n(a)) || n(a).includes(n(b))));
}

/**
 * Build customer DA cards from sources + opportunities (deduped by host+objective).
 */
export function buildDigitalAuthorityCustomerCards(payload) {
  const da = payload?.digitalAuthority || {};
  const official = officialUrlCandidates(payload);
  const hotelName = hotelShortName(payload);
  const cards = [];
  const hostBuckets = new Map();

  const addSource = (s) => {
    const url =
      extractHttpUrl(s.linkingPageUrl) ||
      extractHttpUrl(s.evidenceUrl) ||
      extractHttpUrl(s.referringUrl);
    if (!url) return;
    const host = hostOf(url) || customerVisibleText(s.referringOrganization || "source");
    const dest = extractHttpUrl(s.hotelDestinationUrl);
    const linked = s.hyperlinkToOfficialSite === true || Boolean(dest);
    const destClass = classifyDestination(dest, official);
    const sourceType = classifyReferringSourceType({
      host,
      name: s.referringOrganization || "",
      url,
      note: s.note || "",
    });
    const kind = linked
      ? destClass.status === "HISTORICAL_OR_LEGACY_DOMAIN"
        ? "Verified backlink — historical destination"
        : destClass.status === "CURRENT_CANONICAL_OR_OFFICIAL" ||
            destClass.status === "CURRENT_BRAND_PROPERTY"
          ? "Verified healthy backlink"
          : "Verified backlink — review destination"
      : "Unlinked mention";

    if (!hostBuckets.has(host)) {
      hostBuckets.set(host, {
        host,
        kind,
        sourceName: customerVisibleText(s.referringOrganization || host, host),
        sourceType,
        pages: [],
        linked,
        destinations: new Set(),
        destStatuses: new Set(),
        notes: [],
      });
    }
    const b = hostBuckets.get(host);
    b.pages.push(url);
    if (dest) b.destinations.add(dest);
    b.destStatuses.add(destClass.status);
    if (s.note) b.notes.push(s.note);
    // Prefer more specific source type over OTHER
    if (b.sourceType === SOURCE_TYPES.OTHER && sourceType !== SOURCE_TYPES.OTHER) {
      b.sourceType = sourceType;
    }
    const rank = (k) =>
      /healthy/i.test(k)
        ? 0
        : /historical/i.test(k)
          ? 1
          : /review/i.test(k)
            ? 2
            : /Unlinked/i.test(k)
              ? 3
              : 4;
    if (rank(kind) < rank(b.kind)) b.kind = kind;
    if (linked) b.linked = true;
  };

  for (const s of da.sources || []) addSource(s);
  for (const b of da.verifiedBacklinks || da.backlinks || []) {
    addSource({
      referringOrganization: b.source || b.domain,
      linkingPageUrl: b.referringUrl || b.url || b.linkingPageUrl,
      hotelDestinationUrl: b.hotelDestinationUrl || b.destinationUrl,
      hyperlinkToOfficialSite: true,
      note: b.note,
      evidenceUrl: b.evidenceUrl,
    });
  }

  for (const b of hostBuckets.values()) {
    const destList = [...b.destinations];
    const primaryDest = destList[0] || null;
    const destClass = classifyDestination(primaryDest, official);
    const uniquePages = [...new Set(b.pages)];
    const copy = buildDistinctCopy({
      kind: b.kind,
      sourceName: b.sourceName,
      sourceType: b.sourceType,
      hotelName,
      pageCount: uniquePages.length,
      notes: b.notes,
      destClass,
      primaryDest,
    });

    cards.push({
      kind: b.kind,
      source: b.sourceName,
      sourceType: b.sourceType,
      sourceTypeLabel: copy.sourceTypeLabel,
      detail: copy.observation,
      observation: copy.observation,
      why: copy.why,
      action: copy.action,
      owner: "Hotel Marketing / Partnerships",
      url: uniquePages[0],
      evidenceUrls: uniquePages,
      destinationUrl: primaryDest,
      destinationStatus: destClass.status,
      destinationNote: destClass.note,
      hyperlinkExists: b.linked,
      researchLimitations: null,
      consolidatedPageCount: uniquePages.length,
    });
  }

  for (const o of da.opportunities || da.v3Extras?.opportunities || []) {
    const url =
      extractHttpUrl(o.targetOrganizationUrl) ||
      extractHttpUrl(o.url) ||
      extractHttpUrl(o.evidenceUrl) ||
      extractHttpUrl(o.evidenceLinks?.[0]);
    const host = hostOf(url);
    const sourceType = classifyReferringSourceType({
      host,
      name: `${o.organization || ""} ${o.title || ""}`,
      url,
      note: o.whyItMatters || "",
    });
    if (
      host &&
      cards.some(
        (c) => hostOf(c.url) === host && /Unlinked|Opportunity|backlink/i.test(c.kind)
      )
    ) {
      if (
        !/weddingwire|wedding/i.test(`${o.organization} ${o.title}`) ||
        cards.some((c) => /weddingwire/i.test(c.source + c.url))
      ) {
        if (!/weddingwire|wedding/i.test(`${o.organization} ${o.title}`)) continue;
      }
    }
    const restricted = /ACCESS_RESTRICTED|restricted/i.test(
      `${o.accessStatus || ""} ${o.recommendedAction || ""}`
    );
    const sourceName = customerVisibleText(o.organization || o.title || "Source");
    const typeLabel = sourceTypeLabel(sourceType);
    const observation = restricted
      ? `${sourceName} (${typeLabel}) appears relevant, but listing contents could not be fully verified because access was restricted during research.`
      : customerVisibleText(
          o.title || o.summary ||
            `${sourceName} (${typeLabel}) is a candidate distribution opportunity for ${hotelName}.`
        );
    const why = customerVisibleText(
      o.whyItMatters ||
        `This ${typeLabel} reaches audiences that overlap with ${hotelName}’s demand mix.`
    );
    let action = customerVisibleText(
      o.recommendedAction ||
        `Confirm the live listing, then submit the hotel-approved official URL if the publisher allows property links.`
    );
    // Strip incorrect CVB phrasing when source is not a CVB
    if (
      sourceType !== SOURCE_TYPES.CVB &&
      /CVB\/destination profile|convention bureau/i.test(action)
    ) {
      action = `Contact ${sourceName} to request a property-specific hyperlink to the hotel’s approved official website where the publisher allows it.`;
    }

    cards.push({
      kind: restricted
        ? "Opportunity — confirm before prescribing copy changes"
        : "Distribution opportunity",
      source: sourceName,
      sourceType,
      sourceTypeLabel: typeLabel,
      detail: observation,
      observation,
      why: normEq(observation, why)
        ? `Commercially relevant ${typeLabel} for ${hotelName} discovery.`
        : why,
      action,
      owner: "Hotel Marketing / Partnerships",
      url,
      evidenceUrls: url ? [url] : [],
      destinationUrl: extractHttpUrl(o.evidenceLinks?.[0]) || null,
      destinationStatus: restricted ? "ACCESS_RESTRICTED" : "OPPORTUNITY",
      destinationNote: restricted
        ? "Listing access was restricted — confirm contents before claiming specific copy defects."
        : null,
      hyperlinkExists: null,
      researchLimitations: restricted ? "Access restricted during research." : null,
      consolidatedPageCount: 1,
    });
  }

  if (!cards.length) {
    cards.push({
      kind: "Summary",
      source: "Research record",
      sourceType: SOURCE_TYPES.OTHER,
      detail: da.executiveFinding || "Authority coverage is retained in the AVA research record.",
      observation:
        da.executiveFinding || "Authority coverage is retained in the AVA research record.",
      why: "Prioritize high-trust destination and partner sources already identified for this hotel.",
      action: "Review the internal AVA authority inventory with marketing before launching outreach.",
      owner: "Hotel Marketing",
      url: null,
      evidenceUrls: [],
      destinationUrl: null,
      destinationStatus: "NONE",
      hyperlinkExists: null,
      researchLimitations: null,
      consolidatedPageCount: 0,
    });
  }

  return {
    contract: AVA_DIGITAL_AUTHORITY_CUSTOMER_V1,
    summary: customerVisibleText(
      da.executiveFinding ||
        da.executiveSummary ||
        da.headline ||
        "Focus on a few high-trust referring domains already verified for this hotel rather than raw link volume."
    ),
    cards: cards.slice(0, 6),
    officialUrls: official,
  };
}
