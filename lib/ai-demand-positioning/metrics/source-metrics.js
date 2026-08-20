/**
 * Source citation metrics — presence only, not influence.
 */

import { roundAdpPercent } from "../format-percent.js";
import { filterComparableObservations } from "./grain-governance.js";
import {
  classifySourceUrl,
  computeOwnedExternalSourceMix,
  OWNED_SOURCE_DEFINITION_V1,
} from "./owned-source-classification-v1.js";

const DOMAIN_TYPE_MAP = Object.freeze({
  "marriott.com": "OTA_OR_BRAND",
  "hilton.com": "OTA_OR_BRAND",
  "hyatt.com": "OTA_OR_BRAND",
  "ihg.com": "OTA_OR_BRAND",
  "booking.com": "OTA",
  "expedia.com": "OTA",
  "hotels.com": "OTA",
  "tripadvisor.com": "REVIEW_PLATFORM",
  "kayak.com": "OTA",
  "google.com": "MAP_LOCAL_LISTING",
});

function classifyDomain(domain, propertyProfile = null) {
  if (!domain) return "OTHER";
  const base = domain.replace(/^www\./, "").toLowerCase();
  if (propertyProfile) {
    const governed = classifySourceUrl(`https://${base}/`, propertyProfile);
    if (governed.rollup === "OWNED") return "OWNED";
    if (governed.class === "REVIEW_PLATFORM") return "REVIEW_PLATFORM";
    if (governed.class === "OTA") return "OTA";
  }
  if (DOMAIN_TYPE_MAP[base]) return DOMAIN_TYPE_MAP[base];
  if (base.endsWith(".gov") || base.includes("visit") || base.includes("tourism") || base.includes("destination")) {
    return "EDITORIAL_OR_DESTINATION";
  }
  if (
    base.includes("travel") ||
    base.includes("mag") ||
    base.includes("journal") ||
    base.includes("forbes") ||
    base.includes("cntraveler")
  ) {
    return "EDITORIAL";
  }
  return "OTHER";
}

export function computeSourceMetrics(observations, propertyProfile) {
  const comparable = filterComparableObservations(observations);
  const domainCounts = {};
  const categoryCounts = {};
  let withCitations = 0;

  for (const obs of comparable) {
    const sources = obs.sourcesCited?.length
      ? obs.sourcesCited
      : (obs.providerCitations || []).map((url) => ({ url }));
    if (!sources.length) continue;
    withCitations += 1;
    const seen = new Set();
    for (const src of sources) {
      const url = src.url || "";
      try {
        const domain = new URL(url).hostname.replace(/^www\./, "");
        if (seen.has(domain)) continue;
        seen.add(domain);
        domainCounts[domain] = (domainCounts[domain] || 0) + 1;
        const cat = classifyDomain(domain, propertyProfile);
        categoryCounts[cat] = (categoryCounts[cat] || 0) + 1;
      } catch (_) {}
    }
  }

  const citationRate =
    comparable.length > 0 ? roundAdpPercent((withCitations / comparable.length) * 100) : null;

  const topDomains = Object.entries(domainCounts)
    .map(([domain, count]) => ({
      domain,
      count,
      citationShare: withCitations > 0 ? roundAdpPercent((count / withCitations) * 100) : 0,
      category: classifyDomain(domain, propertyProfile),
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 15);

  const uniqueDomains = Object.keys(domainCounts).length;
  const uniqueCategories = Object.keys(categoryCounts).length;
  const topDomainShare =
    topDomains[0] && withCitations > 0
      ? roundAdpPercent((topDomains[0].count / withCitations) * 100)
      : null;

  const concentrationScore =
    uniqueDomains > 0
      ? Math.max(0, Math.min(100, Math.round((uniqueDomains / Math.max(withCitations, 1)) * 100)))
      : null;

  const ownedMix = computeOwnedExternalSourceMix(comparable, propertyProfile);

  return {
    citationRate,
    responsesWithCitations: withCitations,
    totalComparableObservations: comparable.length,
    sourceCitationShareReady: withCitations >= 5,
    sourceDiversityReady: uniqueDomains >= 3,
    uniqueDomains,
    uniqueSourceCategories: uniqueCategories,
    topDomainConcentration: topDomainShare,
    sourceDiversityScore: concentrationScore,
    categoryBreakdown: categoryCounts,
    topDomains,
    ownedSourceMix: ownedMix,
    ownedSourceDefinition: OWNED_SOURCE_DEFINITION_V1,
    customerNote:
      "Source Citation Share measures citation presence in grounded responses, not LLM weighting or influence.",
  };
}
