/**
 * Reusable PE adapter: provider / disposition / nested package →
 * canonical venue-partnership package for persistVenuePartnershipPackages.
 *
 * Handles schema variants explicitly. Malformed/ambiguous rows enter a
 * review queue — never silently coerced into qualified publishable data.
 *
 * Distinct venues may share a domain/operator; venue identity and hotel fit
 * remain separate. Signals require actual evidence (no placeholder rows).
 */

import { createHash } from "node:crypto";

export const PE_PROVIDER_TO_CANONICAL_ADAPTER_VERSION =
  "gdi-pe-provider-to-canonical-adapter-v1";

export const PE_ADAPTER_STATUS = Object.freeze({
  CANONICAL_READY: "canonical_ready",
  NEEDS_REVIEW: "needs_review",
  REJECTED: "rejected",
  INSUFFICIENT_EVIDENCE: "insufficient_evidence",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function asArray(v) {
  if (Array.isArray(v)) return v;
  if (v == null || v === "") return [];
  return [v];
}

function domainFromUrl(urlOrDomain) {
  const raw = text(urlOrDomain);
  if (!raw) return null;
  try {
    const withProto = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    return new URL(withProto).hostname.replace(/^www\./i, "").toLowerCase() || null;
  } catch {
    return raw
      .replace(/^https?:\/\//i, "")
      .replace(/^www\./i, "")
      .split("/")[0]
      .toLowerCase() || null;
  }
}

function websiteFrom(domainOrUrl) {
  const d = text(domainOrUrl);
  if (!d) return null;
  if (/^https?:\/\//i.test(d)) return d;
  return `https://${d}`;
}

function inferCityRegion(location = "") {
  const loc = text(location);
  if (/san\s*diego/i.test(loc)) return { city: "San Diego", region: "CA" };
  if (/washington|dc\b|dupont|embassy|georgetown|capitol/i.test(loc)) {
    return { city: "Washington", region: "DC" };
  }
  if (/,\s*CA\b/i.test(loc)) return { city: null, region: "CA" };
  if (/,\s*DC\b/i.test(loc)) return { city: "Washington", region: "DC" };
  return { city: null, region: null };
}

function matchNameReFrom(venueName) {
  const tokens = text(venueName)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter((t) => t.length > 2 && !["the", "and", "for", "inc", "llc"].includes(t))
    .slice(0, 3);
  if (!tokens.length) {
    return text(venueName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&") || null;
  }
  return tokens.join(".*");
}

/**
 * Normalize one PE input into a working draft (all schema variants).
 */
export function normalizePeProviderCandidate(raw = {}, opts = {}) {
  const hotelId =
    opts.hotelId ||
    raw.hotelId ||
    raw.hotel?.hotelId ||
    raw.hotel?.id ||
    raw.package?.hotelId ||
    null;

  // Variant A: already canonical (venue.venueName)
  if (raw.venue?.venueName && raw.hotel) {
    return {
      variant: "canonical",
      hotelId: raw.hotel.hotelId || raw.hotel.id || hotelId,
      venueName: text(raw.venue.venueName),
      website: raw.venue.website || null,
      officialDomain:
        raw.venue.officialDomain || domainFromUrl(raw.venue.website) || null,
      location: raw.venue.address || raw.venue.location || null,
      city: raw.venue.city || null,
      region: raw.venue.region || null,
      category: raw.venue.category || raw.venue.venueType || null,
      contactEmail: raw.venue.eventContactEmail || raw.contactEmail || null,
      contactPhone: raw.venue.eventContactPhone || raw.contactPhone || null,
      contactUrl:
        raw.venue.organizationPath ||
        raw.contactUrl ||
        raw.venue.website ||
        null,
      eventActivityEvidence:
        raw.venue.activityEvidence?.[0]?.evidenceText ||
        raw.signals?.[0]?.evidenceText ||
        null,
      lodgingOrReferralRationale:
        raw.whyThisMatters || raw.lodgingOrReferralRationale || null,
      whyRelevantToHotel: raw.whyThisMatters || raw.whyRelevantToHotel || null,
      recommendedFirstAction:
        raw.recommendedAction || raw.recommendedFirstAction || null,
      sourceUrls: [
        ...asArray(raw.venue.sourceUrls),
        ...asArray(raw.signals).flatMap((s) => asArray(s?.sourceUrl)),
      ].filter(Boolean),
      publish: raw.publish !== false,
      existingOpportunityId: raw.existingOpportunityId || null,
      signals: asArray(raw.signals),
      raw,
    };
  }

  // Variant B: nested Parallel package (venue.name + signal + contact)
  if (raw.venue?.name || (raw.venue && raw.signal)) {
    const loc = raw.venue.location || raw.location || null;
    const geo = inferCityRegion(loc);
    return {
      variant: "nested_parallel_package",
      hotelId: hotelId,
      venueName: text(raw.venue?.name || raw.venueName),
      website: raw.venue?.website || null,
      officialDomain:
        domainFromUrl(raw.venue?.website) ||
        domainFromUrl(raw.officialDomain) ||
        null,
      location: loc,
      city: geo.city,
      region: geo.region || "US",
      category: raw.venue?.category || raw.category || null,
      contactEmail: raw.contact?.email || raw.contactEmail || null,
      contactPhone: raw.contact?.phone || raw.contactPhone || null,
      contactUrl: raw.contact?.url || raw.contactUrl || raw.venue?.website || null,
      eventActivityEvidence:
        raw.signal?.eventActivityEvidence || raw.eventActivityEvidence || null,
      lodgingOrReferralRationale:
        raw.signal?.lodgingOrReferralRationale ||
        raw.lodgingOrReferralRationale ||
        null,
      whyRelevantToHotel: raw.whyRelevantToHotel || null,
      recommendedFirstAction: raw.recommendedFirstAction || null,
      sourceUrls: asArray(raw.signal?.sourceUrls || raw.sourceUrls),
      publish: raw.publish !== false,
      existingOpportunityId: raw.existingOpportunityId || null,
      signals: [],
      raw,
    };
  }

  // Variant C: disposition / flat package from paid-multisegment
  const pkg = raw.package || raw;
  const venueName = text(pkg.venueName || pkg.organizationName || raw.venueName);
  const loc = pkg.location || raw.location || null;
  const geo = inferCityRegion(loc);
  return {
    variant: "flat_disposition",
    hotelId: hotelId || pkg.hotelId,
    venueName,
    website: websiteFrom(pkg.officialDomain || pkg.website || pkg.contactUrl),
    officialDomain:
      domainFromUrl(pkg.officialDomain) ||
      domainFromUrl(pkg.website) ||
      domainFromUrl(pkg.contactUrl) ||
      null,
    location: loc,
    city: geo.city,
    region: geo.region,
    category: pkg.category || null,
    contactEmail: pkg.contactEmail || raw.contactEmail || null,
    contactPhone: pkg.contactPhone || raw.contactPhone || null,
    contactUrl: pkg.contactUrl || raw.contactUrl || null,
    eventActivityEvidence: pkg.eventActivityEvidence || null,
    lodgingOrReferralRationale: pkg.lodgingOrReferralRationale || null,
    whyRelevantToHotel: pkg.whyRelevantToHotel || raw.whyRelevantToHotel || null,
    recommendedFirstAction:
      pkg.recommendedFirstAction || raw.recommendedFirstAction || null,
    sourceUrls: asArray(pkg.sourceUrls || raw.sourceUrls),
    publish: pkg.publish !== false && raw.publish !== false,
    existingOpportunityId: pkg.existingOpportunityId || null,
    signals: [],
    raw,
  };
}

function validateDraft(draft, hotelConfig = null) {
  const reasons = [];
  if (!draft.hotelId) reasons.push("missing_hotel_id");
  if (!draft.venueName) reasons.push("missing_venue_name");
  if (!draft.location && !draft.city) reasons.push("missing_location");
  if (!draft.eventActivityEvidence) reasons.push("missing_event_activity_evidence");
  if (!draft.lodgingOrReferralRationale && !draft.whyRelevantToHotel) {
    reasons.push("missing_lodging_or_referral_rationale");
  }

  // Wrong-market gate (lightweight; hotelConfig optional)
  const market = text(hotelConfig?.city || hotelConfig?.locale?.city || "");
  const locBlob = `${draft.location || ""} ${draft.city || ""}`;
  if (market && /san diego/i.test(market) && /washington|,\s*dc\b/i.test(locBlob) && !/san diego/i.test(locBlob)) {
    reasons.push("wrong_market_location");
  }
  if (
    market &&
    /washington|mayflower/i.test(`${market} ${hotelConfig?.displayName || ""}`) &&
    /san diego/i.test(locBlob) &&
    !/washington|dc\b/i.test(locBlob)
  ) {
    reasons.push("wrong_market_location");
  }

  const hasContact = Boolean(
    text(draft.contactEmail) || text(draft.contactPhone) || text(draft.contactUrl)
  );

  if (reasons.includes("wrong_market_location")) {
    return {
      status: PE_ADAPTER_STATUS.REJECTED,
      reasons,
      publishable: false,
    };
  }
  if (reasons.length) {
    return {
      status: PE_ADAPTER_STATUS.INSUFFICIENT_EVIDENCE,
      reasons,
      publishable: false,
    };
  }
  if (!hasContact) {
    return {
      status: PE_ADAPTER_STATUS.NEEDS_REVIEW,
      reasons: ["missing_usable_contact_route"],
      publishable: false,
    };
  }
  return {
    status: PE_ADAPTER_STATUS.CANONICAL_READY,
    reasons: [],
    publishable: draft.publish !== false,
  };
}

function buildSignalsFromDraft(draft) {
  const evidence = text(draft.eventActivityEvidence);
  if (!evidence) return [];
  const sourceUrl =
    draft.sourceUrls?.[0] || draft.contactUrl || draft.website || null;
  return [
    {
      eventName: `${draft.venueName} — published private-event activity`,
      evidenceText: evidence,
      evidenceType: "OFFICIAL_PRIVATE_EVENT_PAGE",
      sourceUrl,
      // Never mark artificial
      artificial: false,
    },
  ];
}

/**
 * Convert one PE candidate into a canonical persist package or review row.
 */
export function adaptPeCandidateToCanonicalPackage(raw = {}, opts = {}) {
  const draft = normalizePeProviderCandidate(raw, opts);
  const hotelConfig = opts.hotelConfig || null;
  const hotelDisplayName =
    opts.hotelDisplayName ||
    hotelConfig?.displayName ||
    hotelConfig?.name ||
    (draft.hotelId === "recnPB3Q02NO8Ma1o"
      ? "Wyndham San Diego Bayside"
      : draft.hotelId === "rec0eTEnCqlsGJJt9"
        ? "The Mayflower Hotel, Autograph Collection"
        : null);

  const validation = validateDraft(draft, hotelConfig);
  const reviewId = createHash("sha256")
    .update(
      `${draft.hotelId || ""}|${draft.venueName || ""}|${draft.officialDomain || ""}`
    )
    .digest("hex")
    .slice(0, 12);

  if (validation.status !== PE_ADAPTER_STATUS.CANONICAL_READY) {
    return {
      version: PE_PROVIDER_TO_CANONICAL_ADAPTER_VERSION,
      status: validation.status,
      reviewId,
      reasons: validation.reasons,
      draft,
      package: null,
      publishable: false,
    };
  }

  const website = draft.website || websiteFrom(draft.officialDomain);
  const signals =
    draft.signals?.length > 0 ? draft.signals : buildSignalsFromDraft(draft);

  const pkg = {
    hotel: {
      hotelId: draft.hotelId,
      displayName: hotelDisplayName,
    },
    publish: validation.publishable,
    researchVersion: opts.researchVersion || PE_PROVIDER_TO_CANONICAL_ADAPTER_VERSION,
    title: `${draft.venueName} — Preferred Lodging Partnership`,
    matchNameRe: matchNameReFrom(draft.venueName),
    whyThisMatters:
      draft.whyRelevantToHotel || draft.lodgingOrReferralRationale || null,
    recommendedAction: draft.recommendedFirstAction || null,
    contactUrl: draft.contactUrl || website,
    existingOpportunityId: draft.existingOpportunityId || null,
    venue: {
      venueName: draft.venueName,
      website,
      officialDomain: draft.officialDomain || domainFromUrl(website),
      city: draft.city,
      region: draft.region || "US",
      country: "US",
      address: text(draft.location)?.split(";")[0]?.trim() || draft.location,
      onSiteGuestrooms: 0,
      onSiteLodgingStatus: "NO_LODGING",
      privateEventsAdvertised: true,
      eventContactEmail: draft.contactEmail || null,
      eventContactPhone: draft.contactPhone || null,
      organizationPath: draft.contactUrl || website,
      commercialContactPath: draft.contactEmail
        ? "FUNCTIONAL_EMAIL"
        : draft.contactPhone
          ? "FUNCTIONAL_PHONE"
          : "ORGANIZATION_PATH",
      sourceUrls: draft.sourceUrls || [],
      activityEvidence: draft.eventActivityEvidence
        ? [
            {
              evidenceType: "OFFICIAL_PRIVATE_EVENT_PAGE",
              evidenceText: draft.eventActivityEvidence,
              sourceUrl: draft.sourceUrls?.[0] || draft.contactUrl || website,
            },
          ]
        : [],
      // Shared-domain note for operator groups (e.g. City Experiences)
      sharedDomainOperatorHint: draft.officialDomain || null,
    },
    signals,
  };

  return {
    version: PE_PROVIDER_TO_CANONICAL_ADAPTER_VERSION,
    status: PE_ADAPTER_STATUS.CANONICAL_READY,
    reviewId,
    reasons: [],
    draft,
    package: pkg,
    publishable: validation.publishable,
  };
}

/**
 * Batch adapt PE candidates. Returns packages + review queue.
 * Shared domains do NOT collapse distinct venueNames.
 */
export function adaptPeCandidatesBatch(candidates = [], opts = {}) {
  const packages = [];
  const reviewQueue = [];
  const byDomain = new Map();

  for (const raw of candidates) {
    const adapted = adaptPeCandidateToCanonicalPackage(raw, opts);
    if (adapted.status === PE_ADAPTER_STATUS.CANONICAL_READY && adapted.package) {
      const domain = adapted.package.venue.officialDomain || "unknown";
      if (!byDomain.has(domain)) byDomain.set(domain, []);
      byDomain.get(domain).push(adapted.package.venue.venueName);
      packages.push(adapted.package);
    } else {
      reviewQueue.push({
        reviewId: adapted.reviewId,
        status: adapted.status,
        reasons: adapted.reasons,
        venueName: adapted.draft?.venueName || null,
        hotelId: adapted.draft?.hotelId || opts.hotelId || null,
        officialDomain: adapted.draft?.officialDomain || null,
        variant: adapted.draft?.variant || null,
      });
    }
  }

  const sharedDomainVenues = [...byDomain.entries()]
    .filter(([, names]) => names.length > 1)
    .map(([domain, venueNames]) => ({ domain, venueNames }));

  return {
    version: PE_PROVIDER_TO_CANONICAL_ADAPTER_VERSION,
    generatedAt: new Date().toISOString(),
    hotelId: opts.hotelId || null,
    packageCount: packages.length,
    reviewCount: reviewQueue.length,
    packages,
    reviewQueue,
    sharedDomainVenues,
    note:
      "Distinct venues sharing a domain remain separate packages; identity is venueName + domain, not domain alone.",
  };
}

export default {
  PE_PROVIDER_TO_CANONICAL_ADAPTER_VERSION,
  PE_ADAPTER_STATUS,
  normalizePeProviderCandidate,
  adaptPeCandidateToCanonicalPackage,
  adaptPeCandidatesBatch,
};
