/**
 * Competitor narrative comparison (V1).
 * Comparable prompt / scenario / provider / geography where possible.
 * Not Recommendation Share / Win-Loss.
 */

import { PEER_SET_ID_V2, loadPeerSetConfig, resolvePeerSetMembership } from "./peer-sets.js";
import { aggregateBrandNarratives, extractAllNarrativeObservations } from "./narrative-intelligence.js";

export const NARRATIVE_COMPETITOR_COMPARISON_VERSION =
  "ai_visibility_narrative_competitor_comparison_v1";

function comparableKey(obs) {
  return `${obs.scenarioId || "none"}|${obs.language}|${obs.geography}|${obs.provider || "any"}`;
}

/**
 * Compare subject vs competitor narrative coverage within comparable slices.
 */
export function buildCompetitorNarrativeComparisons(args = {}) {
  const {
    evidence = [],
    subjectBrandId,
    subjectBrandName,
    competitorBrandIds = [],
    stabilityReport = null,
    familyPrecision = {},
  } = args;

  const observations = extractAllNarrativeObservations(evidence, args);
  const comparisons = [];

  for (const competitorBrandId of competitorBrandIds) {
    const competitorName =
      args.competitorNames?.[competitorBrandId] ||
      competitorBrandId;

    const subjectObs = observations.filter((o) => o.brandId === subjectBrandId);
    const competitorObs = observations.filter((o) => o.brandId === competitorBrandId);

    const sliceKeys = new Set([
      ...subjectObs.map(comparableKey),
      ...competitorObs.map(comparableKey),
    ]);

    for (const slice of sliceKeys) {
      const [scenarioId, language, geography] = slice.split("|");
      const subSlice = subjectObs.filter((o) => comparableKey(o) === slice);
      const compSlice = competitorObs.filter((o) => comparableKey(o) === slice);

      const families = new Set([
        ...subSlice.map((o) => o.narrativeFamily),
        ...compSlice.map((o) => o.narrativeFamily),
      ]);

      for (const narrativeFamily of families) {
        const subRows = subSlice.filter((o) => o.narrativeFamily === narrativeFamily);
        const compRows = compSlice.filter((o) => o.narrativeFamily === narrativeFamily);
        const subResponses = new Set(subRows.map((o) => o.responseId)).size;
        const compResponses = new Set(compRows.map((o) => o.responseId)).size;

        let comparison = "INSUFFICIENT_COMPARISON";
        if (subResponses === 0 && compResponses === 0) comparison = "BOTH_ABSENT";
        else if (subResponses > 0 && compResponses > 0) {
          if (subResponses > compResponses) comparison = "SUBJECT_STRONGER";
          else if (compResponses > subResponses) comparison = "COMPETITOR_STRONGER";
          else comparison = "BOTH_PRESENT";
        } else if (subResponses > 0 && compResponses === 0) comparison = "SUBJECT_STRONGER";
        else if (compResponses > 0 && subResponses === 0) comparison = "COMPETITOR_STRONGER";

        if (comparison === "INSUFFICIENT_COMPARISON") continue;

        comparisons.push({
          subjectBrand: subjectBrandName,
          subjectBrandId,
          competitor: competitorName,
          competitorBrandId,
          scenario: scenarioId === "none" ? null : scenarioId,
          language,
          geography,
          narrative: narrativeFamily,
          subjectResponses: subResponses,
          competitorResponses: compResponses,
          subjectState: subResponses > 0 ? "PRESENT" : "ABSENT",
          competitorState: compResponses > 0 ? "PRESENT" : "ABSENT",
          comparison,
        });
      }
    }
  }

  return comparisons.sort(
    (a, b) =>
      (b.subjectResponses + b.competitorResponses) - (a.subjectResponses + a.competitorResponses)
  );
}

/**
 * Detect contradictory narrative polarities for same brand/family.
 */
export function detectNarrativeTensions(narrativesByBrand = {}) {
  const tensions = [];
  for (const [brandName, narratives] of Object.entries(narrativesByBrand)) {
    const byFamily = new Map();
    for (const n of narratives) {
      if (!byFamily.has(n.narrativeFamily)) byFamily.set(n.narrativeFamily, []);
      byFamily.get(n.narrativeFamily).push(n);
    }
    for (const [family, rows] of byFamily.entries()) {
      const polarities = new Set(rows.map((r) => r.polarity));
      if (polarities.has("POSITIVE") && polarities.has("NEGATIVE")) {
        const pos = rows.find((r) => r.polarity === "POSITIVE");
        const neg = rows.find((r) => r.polarity === "NEGATIVE");
        tensions.push({
          brand: brandName,
          narrativeA: { family, polarity: "POSITIVE", label: pos?.narrativeLabel },
          narrativeB: { family, polarity: "NEGATIVE", label: neg?.narrativeLabel },
          evidence: {
            positiveResponses: pos?.comparableResponseCount || 0,
            negativeResponses: neg?.comparableResponseCount || 0,
          },
          providers: [...new Set([...(pos?.providers || []), ...(neg?.providers || [])])],
          disposition: "REVIEW_REQUIRED",
        });
      }
    }
  }
  return tensions;
}

/**
 * Provider-level narrative emphasis variation for one brand.
 */
export function detectProviderNarrativeVariation(narrativesByBrand = {}, observations = []) {
  const variations = [];
  for (const [brandName, brandId] of Object.entries(
    Object.fromEntries(
      Object.entries(narrativesByBrand).map(([name, rows]) => [name, rows[0]?.brandId])
    )
  )) {
    if (!brandId) continue;
    const brandObs = observations.filter((o) => o.brandId === brandId);
    const byProvider = new Map();
    for (const o of brandObs) {
      const p = String(o.provider || "unknown").toLowerCase();
      if (!byProvider.has(p)) byProvider.set(p, new Map());
      const famMap = byProvider.get(p);
      famMap.set(o.narrativeFamily, (famMap.get(o.narrativeFamily) || 0) + 1);
    }
    const providerTop = {};
    for (const [provider, famMap] of byProvider.entries()) {
      const top = [...famMap.entries()].sort((a, b) => b[1] - a[1])[0];
      providerTop[provider] = top ? { family: top[0], count: top[1] } : null;
    }
    const families = new Set(Object.values(providerTop).map((v) => v?.family).filter(Boolean));
    if (families.size >= 2) {
      variations.push({
        brand: brandName,
        scenario: "portfolio_baseline",
        openai: providerTop.openai?.family || null,
        gemini: providerTop.gemini?.family || null,
        perplexity: providerTop.perplexity?.family || null,
        claude: providerTop.claude?.family || null,
        state: "PROVIDER_NARRATIVE_VARIATION",
      });
    }
  }
  return variations;
}

export function defaultCompetitorIdsForSubject(subjectBrandId, commercialRegion = "CALA") {
  const cfg = loadPeerSetConfig();
  const membership = resolvePeerSetMembership(
    { peerSetId: PEER_SET_ID_V2, commercialRegion },
    cfg
  );
  return (membership.entityIds || []).filter((id) => id !== subjectBrandId);
}
