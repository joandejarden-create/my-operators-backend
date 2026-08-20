/**
 * ADP Owned Source Classification V1 — governed customer-safe ownership.
 *
 * OWNED_SOURCE = source controlled by (A) subject property OR (B) parent brand/operator
 * when the page directly represents the subject property.
 * UNKNOWN must not silently become OWNED or EXTERNAL in internal tallies.
 */

import { roundAdpPercent } from "../format-percent.js";

export const OWNED_SOURCE_DEFINITION_V1 =
  "OWNED_SOURCE means a source directly controlled by (A) the subject hotel/property OR (B) the hotel's official parent brand/operator when the page directly represents the specific property. Brand corporate pages that do not represent the subject property are not owned. UNKNOWN is retained separately and must not silently become OWNED or EXTERNAL.";

export const OWNED_SOURCE_CLASSIFICATION_VERSION = "adp_owned_source_classification_v1";

const OTA = new Set(["booking.com", "expedia.com", "hotels.com", "kayak.com", "hotwire.com", "agoda.com"]);
const REVIEW = new Set(["tripadvisor.com", "yelp.com"]);
const EDITORIAL_HINTS = ["forbes", "cntraveler", "travelandleisure", "condenast", "lonelyplanet"];
const SOCIAL = new Set(["facebook.com", "instagram.com", "x.com", "twitter.com", "linkedin.com", "reddit.com", "tiktok.com"]);

function domainOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

function pathOf(url) {
  try {
    return new URL(url).pathname.toLowerCase();
  } catch {
    return "";
  }
}

export function ownedDomainSet(propertyProfile) {
  const set = new Set();
  const brandDomain = String(propertyProfile?.officialBrandDomain || "")
    .replace(/^www\./i, "")
    .toLowerCase();
  const add = (d, { allowBrandHost = false } = {}) => {
    if (!d) return;
    String(d)
      .replace(/^https?:\/\//i, "")
      .replace(/^www\./i, "")
      .split("/")[0]
      .trim()
      .toLowerCase()
      .split(/\s*,\s*/)
      .forEach((x) => {
        if (!x) return;
        // Brand corporate hosts require property-page path match — do not treat whole brand domain as owned.
        if (!allowBrandHost && brandDomain && x === brandDomain) return;
        set.add(x);
      });
  };
  add(propertyProfile?.canonicalPropertyDomain, { allowBrandHost: true });
  (propertyProfile?.ownedDomains || []).forEach((d) => add(d, { allowBrandHost: true }));
  (propertyProfile?.additionalApprovedOwnedDomains || []).forEach((d) => add(d, { allowBrandHost: true }));
  // Standalone property websites only (not brand.com property-page URLs).
  add(propertyProfile?.website, { allowBrandHost: false });
  add(propertyProfile?.officialWebsite, { allowBrandHost: false });
  return set;
}

export function brandPropertyPageHints(propertyProfile) {
  return (propertyProfile?.brandPropertyPathHints || []).map((h) => String(h).toLowerCase()).filter(Boolean);
}

/**
 * Classify a single URL for a property.
 * @returns {{ class: string, rollup: 'OWNED'|'EXTERNAL'|'UNKNOWN' }}
 */
export function classifySourceUrl(url, propertyProfile) {
  const domain = domainOf(url);
  if (!domain) return { class: "UNKNOWN", rollup: "UNKNOWN" };

  const owned = ownedDomainSet(propertyProfile);
  if (owned.has(domain)) return { class: "OWNED_PROPERTY", rollup: "OWNED" };

  const brandDomain = String(propertyProfile?.officialBrandDomain || "")
    .replace(/^www\./i, "")
    .toLowerCase();
  if (brandDomain && domain === brandDomain) {
    const path = pathOf(url);
    const hints = brandPropertyPageHints(propertyProfile);
    if (hints.some((h) => path.includes(h))) {
      return { class: "OWNED_BRAND_PROPERTY_PAGE", rollup: "OWNED" };
    }
    return { class: "OTHER_EXTERNAL", rollup: "EXTERNAL", note: "BRAND_DOMAIN_WITHOUT_PROPERTY_PAGE_MATCH" };
  }

  if (OTA.has(domain) || [...OTA].some((d) => domain.endsWith(`.${d}`))) return { class: "OTA", rollup: "EXTERNAL" };
  if (REVIEW.has(domain) || [...REVIEW].some((d) => domain.endsWith(`.${d}`))) {
    return { class: "REVIEW_PLATFORM", rollup: "EXTERNAL" };
  }
  if (domain === "google.com" || domain.endsWith(".google.com")) return { class: "MAP_LOCAL_LISTING", rollup: "EXTERNAL" };
  if (EDITORIAL_HINTS.some((h) => domain.includes(h))) return { class: "TRAVEL_EDITORIAL", rollup: "EXTERNAL" };
  if (SOCIAL.has(domain) || [...SOCIAL].some((d) => domain.endsWith(`.${d}`))) return { class: "SOCIAL", rollup: "EXTERNAL" };
  if (domain.endsWith("wikipedia.org")) return { class: "WIKIPEDIA_REFERENCE", rollup: "EXTERNAL" };
  if (domain.endsWith(".gov") || domain.includes("visit") || domain.includes("tourism")) {
    return { class: "TOURISM_BOARD", rollup: "EXTERNAL" };
  }
  return { class: "OTHER_EXTERNAL", rollup: "EXTERNAL" };
}

/**
 * Grain: unique domain per observation (response). Owned if any cited URL in that response is OWNED.
 */
export function computeOwnedExternalSourceMix(observations, propertyProfile) {
  const domainsConfigured = ownedDomainSet(propertyProfile).size > 0 || Boolean(propertyProfile?.officialBrandDomain);
  let withCitations = 0;
  let ownedResponses = 0;
  let externalOnlyResponses = 0;
  let unknownOnlyResponses = 0;
  let mixedUnknownExternal = 0;

  for (const obs of observations || []) {
    const urls = [];
    if (obs.sourcesCited?.length) {
      for (const s of obs.sourcesCited) if (s?.url) urls.push(s.url);
    } else if (obs.providerCitations?.length) {
      urls.push(...obs.providerCitations.filter(Boolean));
    }
    if (!urls.length) continue;
    withCitations += 1;
    let hasOwned = false;
    let hasExternal = false;
    let hasUnknown = false;
    const seen = new Set();
    for (const url of urls) {
      const d = domainOf(url);
      if (!d || seen.has(d)) continue;
      seen.add(d);
      const c = classifySourceUrl(url, propertyProfile);
      if (c.rollup === "OWNED") hasOwned = true;
      else if (c.rollup === "EXTERNAL") hasExternal = true;
      else hasUnknown = true;
    }
    if (hasOwned) ownedResponses += 1;
    else if (hasUnknown && !hasExternal) unknownOnlyResponses += 1;
    else if (hasUnknown && hasExternal) mixedUnknownExternal += 1;
    else if (hasExternal) externalOnlyResponses += 1;
    else unknownOnlyResponses += 1;
  }

  const ownedShare =
    withCitations > 0 ? roundAdpPercent((ownedResponses / withCitations) * 100) : domainsConfigured ? 0 : null;
  const externalShare =
    withCitations > 0
      ? roundAdpPercent(((externalOnlyResponses + mixedUnknownExternal) / withCitations) * 100)
      : domainsConfigured
        ? 0
        : null;
  const unknownShare =
    withCitations > 0 ? roundAdpPercent((unknownOnlyResponses / withCitations) * 100) : domainsConfigured ? 0 : null;

  return {
    version: OWNED_SOURCE_CLASSIFICATION_VERSION,
    domainsConfigured,
    ownedSourceDefinition: OWNED_SOURCE_DEFINITION_V1,
    responsesWithCitations: withCitations,
    ownedResponses,
    externalOnlyResponses,
    unknownOnlyResponses,
    ownedShare,
    externalShare,
    unknownShare,
    grain: "RESPONSES_WITH_CITATIONS_UNIQUE_DOMAIN_PER_RESPONSE",
    customerNote:
      "Owned / External shares are observed citation presence among responses that included sources — not AI trust, preference, or influence.",
  };
}
