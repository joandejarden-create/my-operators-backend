/**
 * Global ADP source attribution taxonomy + label contract (Parts 17–20).
 */

import { classifySourceUrl } from "../metrics/owned-source-classification-v1.js";
import {
  normalizeDomainHost,
  resolveDomainOwnershipForProperty,
} from "./adp-domain-ownership-registry-v1.js";

export const ADP_SOURCE_ATTRIBUTION_CONTRACT_VERSION = "adp_source_attribution_contract_v1";

export const SOURCE_ATTRIBUTION_TAXONOMY = Object.freeze({
  PROPERTY_OWNED: "PROPERTY_OWNED",
  BRAND_OWNED: "BRAND_OWNED",
  PROPERTY_SPECIFIC_EXTERNAL: "PROPERTY_SPECIFIC_EXTERNAL",
  COMPETITOR_OWNED: "COMPETITOR_OWNED",
  COMPETITOR_EXTERNAL: "COMPETITOR_EXTERNAL",
  GENERAL_MARKET: "GENERAL_MARKET",
  UNKNOWN: "UNKNOWN",
});

export const SOURCE_LABEL_METRICS = Object.freeze({
  TOP_SOURCE_SUPPORTING_THIS_PROPERTY: "Top Source Supporting This Property",
  TOP_OWNED_BRAND_SOURCE: "Top Owned/Brand Source",
  TOP_EXTERNAL_PROPERTY_SOURCE: "Top External Property Source",
  TOP_COMPETITIVE_UNIVERSE_SOURCE: "Top Competitive-Universe Source",
});

function urlsFromObservation(obs) {
  const urls = [];
  if (obs?.sourcesCited?.length) {
    for (const s of obs.sourcesCited) if (s?.url) urls.push(s.url);
  } else if (obs?.providerCitations?.length) {
    urls.push(...obs.providerCitations.filter(Boolean));
  }
  return urls;
}

export function classifyCitationForProperty(url, propertyProfile) {
  const ownership = resolveDomainOwnershipForProperty(url, propertyProfile);
  const legacy = classifySourceUrl(url, propertyProfile);

  let taxonomy = SOURCE_ATTRIBUTION_TAXONOMY.UNKNOWN;
  if (ownership.ownershipType === "PROPERTY_OWNED") {
    taxonomy = SOURCE_ATTRIBUTION_TAXONOMY.PROPERTY_OWNED;
  } else if (ownership.ownershipType === "BRAND_OWNED") {
    taxonomy = SOURCE_ATTRIBUTION_TAXONOMY.BRAND_OWNED;
  } else if (ownership.ownershipType === "COMPETITOR_OWNED") {
    taxonomy = SOURCE_ATTRIBUTION_TAXONOMY.COMPETITOR_OWNED;
  } else if (legacy.rollup === "EXTERNAL") {
    taxonomy = SOURCE_ATTRIBUTION_TAXONOMY.PROPERTY_SPECIFIC_EXTERNAL;
  } else if (ownership.ownershipType === "GENERAL_MARKET") {
    taxonomy = SOURCE_ATTRIBUTION_TAXONOMY.GENERAL_MARKET;
  }

  return {
    url,
    domain: ownership.domain || normalizeDomainHost(url),
    taxonomy,
    ownershipType: ownership.ownershipType,
    brand: ownership.brand,
    legacyClass: legacy.class,
    legacyRollup: legacy.rollup,
  };
}

/**
 * Audit source labels — competitor-owned must not be labeled as property Top Source.
 */
export function auditSourceAttributionLabels(period, propertyProfile, options = {}) {
  const hardFailures = [];
  const reviewFlags = [];
  const domainCounts = new Map();

  for (const obs of period?.observations || []) {
    for (const url of urlsFromObservation(obs)) {
      const c = classifyCitationForProperty(url, propertyProfile);
      if (!c.domain) continue;
      const prev = domainCounts.get(c.domain) || {
        domain: c.domain,
        count: 0,
        taxonomy: c.taxonomy,
        ownershipType: c.ownershipType,
        brand: c.brand,
      };
      prev.count += 1;
      domainCounts.set(c.domain, prev);
    }
  }

  const ranked = [...domainCounts.values()].sort((a, b) => b.count - a.count);
  const supporting = ranked.filter(
    (d) =>
      d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.PROPERTY_OWNED ||
      d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.BRAND_OWNED ||
      d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.PROPERTY_SPECIFIC_EXTERNAL
  );
  const ownedBrand = ranked.filter(
    (d) =>
      d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.PROPERTY_OWNED ||
      d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.BRAND_OWNED
  );
  const externalProperty = ranked.filter(
    (d) => d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.PROPERTY_SPECIFIC_EXTERNAL
  );
  const competitive = ranked.filter(
    (d) =>
      d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.COMPETITOR_OWNED ||
      d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.COMPETITOR_EXTERNAL
  );

  const metrics = {
    topSourceSupportingThisProperty: supporting[0] || null,
    topOwnedBrandSource: ownedBrand[0] || null,
    topExternalPropertySource: externalProperty[0] || null,
    topCompetitiveUniverseSource: competitive[0] || null,
  };

  // Detect mislabel: if report claims a competitor domain as property top source
  const claimedTop = options.claimedTopSourceDomain
    ? normalizeDomainHost(options.claimedTopSourceDomain)
    : null;
  if (claimedTop) {
    const claimed = classifyCitationForProperty(`https://${claimedTop}/`, propertyProfile);
    if (
      claimed.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.COMPETITOR_OWNED ||
      claimed.ownershipType === "COMPETITOR_OWNED"
    ) {
      hardFailures.push({
        code: "COMPETITOR_SOURCE_MISLABELED_AS_PROPERTY_TOP_SOURCE",
        domain: claimedTop,
        taxonomy: claimed.taxonomy,
      });
    }
  }

  // Owned-source share zero despite *property-page* owned citations.
  // Brand corporate hosts without property-path match (classifySourceUrl EXTERNAL) must not
  // trigger this — registry BRAND_OWNED ≠ owned-source rollup OWNED.
  const brandDomain = normalizeDomainHost(propertyProfile?.officialBrandDomain);
  const propertyOwnedHits = ranked.filter(
    (d) => d.taxonomy === SOURCE_ATTRIBUTION_TAXONOMY.PROPERTY_OWNED
  );
  const brandOwnedPropertyPageHits = ranked.filter((d) => {
    if (d.taxonomy !== SOURCE_ATTRIBUTION_TAXONOMY.BRAND_OWNED) return false;
    const legacy = classifySourceUrl(`https://${d.domain}/`, propertyProfile);
    return legacy.rollup === "OWNED";
  });
  const ownedHitsForShare = [...propertyOwnedHits, ...brandOwnedPropertyPageHits];
  const storedOwnedShare = options.storedOwnedSourceShare;
  if (
    ownedHitsForShare.length > 0 &&
    storedOwnedShare != null &&
    Number(storedOwnedShare) === 0
  ) {
    reviewFlags.push({
      code: "OWNED_SOURCE_SHARE_ZERO_DESPITE_OWNED_CITATIONS",
      ownedDomainsCited: ownedHitsForShare.map((d) => d.domain),
      storedOwnedSourceShare: storedOwnedShare,
    });
  }

  // Hilton lesson: marriott.com must be competitor-universe for Hilton subject
  if (brandDomain === "hilton.com") {
    const marriott = ranked.find((d) => d.domain === "marriott.com" || d.domain?.endsWith(".marriott.com"));
    if (marriott && marriott.taxonomy !== SOURCE_ATTRIBUTION_TAXONOMY.COMPETITOR_OWNED) {
      hardFailures.push({
        code: "HILTON_MARRIOTT_NOT_COMPETITOR_UNIVERSE",
        domain: marriott.domain,
        taxonomy: marriott.taxonomy,
      });
    }
  }

  return {
    contractVersion: ADP_SOURCE_ATTRIBUTION_CONTRACT_VERSION,
    taxonomy: SOURCE_ATTRIBUTION_TAXONOMY,
    labelMetrics: SOURCE_LABEL_METRICS,
    metrics,
    rankedDomains: ranked.slice(0, 25),
    hardFailures,
    reviewFlags,
  };
}
