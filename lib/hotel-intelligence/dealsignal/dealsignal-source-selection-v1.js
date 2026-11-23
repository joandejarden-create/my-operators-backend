/**
 * DealSignal — select source families for a commercial research question.
 * Reuses dealsignal-source-registry-v1; does not invent scrapers or schedulers.
 */
import {
  DEALSIGNAL_SOURCE_REGISTRY,
  INTEGRATION_READINESS,
  SOURCE_CATEGORY,
  listSourcesByRegion,
} from "./dealsignal-source-registry-v1.js";
import { buildMultilingualQuery, QUERY_CONCEPTS } from "./dealsignal-multilingual-query-lexicon-v1.js";
import { isCalaCountry, normalizeCountry } from "./dealsignal-cala-research-priority-v1.js";

export const DEALSIGNAL_SOURCE_SELECTION_VERSION = "dealsignal-source-selection-v1";

const SIGNAL_TO_CONCEPT = Object.freeze({
  franchise_expiration: "FRANCHISE",
  brand_conversion: "BRAND_CONVERSION",
  hma_expiration: "HOTEL_MANAGEMENT_AGREEMENT",
  hma_internalization: "INTERNALIZATION",
  lease_vs_manage: "LEASE_MODEL",
  hotel_concession: "TENDER_CONCESSION",
  hotel_ops_services: "TENDER_CONCESSION",
  contract_expiration: "CONTRACT_EXPIRATION",
});

/**
 * @param {object} q
 * @param {string} [q.hotelCountry]
 * @param {string} [q.opportunityType] BCT|MCT|OTHER
 * @param {string[]} [q.commercialSignals]
 * @param {boolean} [q.preferCala]
 * @param {boolean} [q.includeRestricted]
 */
export function selectSourcesForQuestion(q = {}) {
  const country = normalizeCountry(q.hotelCountry || q.country || "");
  const cala = isCalaCountry(country);
  const mexico = country === "Mexico";
  const signals = new Set((q.commercialSignals || []).map(String));
  const oppType = String(q.opportunityType || q.kind || "").toUpperCase();

  if (oppType === "BCT" || oppType.includes("BRAND")) {
    signals.add("franchise_expiration");
    signals.add("brand_conversion");
  }
  if (oppType === "MCT" || oppType.includes("MANAGEMENT")) {
    signals.add("hma_expiration");
    signals.add("hma_internalization");
  }

  const regionHints = [];
  if (mexico) regionHints.push("Mexico");
  else if (country) regionHints.push(country);
  if (cala) regionHints.push("Dominican", "Colombia", "Brazil", "Caribbean");

  let pool = [...DEALSIGNAL_SOURCE_REGISTRY];
  if (regionHints.length) {
    const regional = new Set();
    for (const hint of regionHints) {
      for (const s of listSourcesByRegion(hint)) regional.add(s.sourceId);
    }
    // Always keep globally integrated contractual sources
    for (const s of DEALSIGNAL_SOURCE_REGISTRY) {
      if (
        s.integrationReadiness === INTEGRATION_READINESS.EXISTING_INTEGRATED ||
        s.countryRegionCoverage.includes("GLOBAL") ||
        s.countryRegionCoverage.includes("GLOBAL_ISSUERS")
      ) {
        regional.add(s.sourceId);
      }
    }
    pool = pool.filter((s) => regional.has(s.sourceId));
  }

  if (!q.includeRestricted) {
    pool = pool.filter((s) => s.integrationReadiness !== INTEGRATION_READINESS.RESTRICTED_OR_UNAVAILABLE);
  }

  const scored = pool.map((s) => {
    let score = 0;
    if (s.testedStatus === "TESTED_PRODUCTIVE") score += 40;
    else if (String(s.testedStatus).startsWith("TESTED")) score += 20;
    if (s.integrationReadiness === INTEGRATION_READINESS.EXISTING_INTEGRATED) score += 25;
    else if (s.integrationReadiness === INTEGRATION_READINESS.ACCESSIBLE_VIA_CURRENT_TOOLING) score += 15;
    else if (s.integrationReadiness === INTEGRATION_READINESS.MANUALLY_SEARCHABLE) score += 10;
    if (mexico && s.countryRegionCoverage.includes("Mexico")) score += 30;
    else if (cala && s.countryRegionCoverage.some((c) => isCalaCountry(c))) score += 18;
    for (const sig of signals) {
      if ((s.commercialSignals || []).includes(sig)) score += 12;
    }
    if (oppType === "BCT" && s.sourceCategory === SOURCE_CATEGORY.CONTRACT_INTELLIGENCE) score += 8;
    if (oppType === "MCT" && s.sourceCategory === SOURCE_CATEGORY.OWNER_CORPORATE_DISCLOSURE) score += 6;
    return { ...s, selectionScore: score };
  });

  scored.sort((a, b) => b.selectionScore - a.selectionScore);

  const langs = mexico || cala ? ["en", "es", "pt"] : ["en"];
  const queryConcepts = [];
  for (const sig of signals) {
    const concept = SIGNAL_TO_CONCEPT[sig];
    if (concept && QUERY_CONCEPTS[concept]) {
      queryConcepts.push({
        concept,
        queryFragment: buildMultilingualQuery(concept, langs),
      });
    }
  }

  return {
    version: DEALSIGNAL_SOURCE_SELECTION_VERSION,
    hotelCountry: country || null,
    cala,
    mexico,
    selected: scored.filter((s) => s.selectionScore > 0).slice(0, 12),
    deferredTechIntegration: scored.filter(
      (s) => s.integrationReadiness === INTEGRATION_READINESS.CANDIDATE_REQUIRES_TECH_INTEGRATION,
    ),
    queryConcepts,
    notes: [
      "CALA preference affects research queue order, not commercial classification or confidence.",
      "Machine translation of a hit is not independent corroboration.",
      "Related references must not be treated as direct claim support.",
    ],
  };
}

export function selectSourcesForMexicoBct() {
  return selectSourcesForQuestion({
    hotelCountry: "Mexico",
    opportunityType: "BCT",
    commercialSignals: ["franchise_expiration", "brand_conversion", "hma_internalization"],
    preferCala: true,
  });
}
