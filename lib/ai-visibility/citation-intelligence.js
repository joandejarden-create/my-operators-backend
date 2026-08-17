/**
 * Citation / Source Intelligence — factual response-level citation rates.
 * No influence claims. No arbitrary citation score.
 */

import { parseDomain } from "./extract-citations.js";
import {
  buildCitedSourceIntelligence,
  classifySourceOwnership,
  compareSourceCitationFrequency,
  SOURCE_CITATION_FREQUENCY_DEFINITION,
  SOURCE_CITATION_FREQUENCY_DENOMINATOR,
  MULTIPLE_CITATIONS_SAME_RESPONSE,
  TOP_CITED_SOURCE_SORT,
} from "./cited-source-intelligence.js";
import { hostnameMatchesOwnedDomain } from "./owned-domain-resolution.js";
import { AVAILABILITY } from "./availability-states.js";

export const CITATION_INTELLIGENCE_VERSION =
  "ai_visibility_citation_intelligence_v1_2";

export const SOURCE_MIX_DEFINITION =
  "How monitored responses divide between owned-only, mixed owned/external, external-only, and no-citation answers.";

export {
  SOURCE_CITATION_FREQUENCY_DEFINITION,
  SOURCE_CITATION_FREQUENCY_DENOMINATOR,
  MULTIPLE_CITATIONS_SAME_RESPONSE,
};

function rateShell(numerator, denominator) {
  if (!denominator || denominator <= 0) {
    return {
      value: null,
      numerator: 0,
      denominator: 0,
      display: "Not Monitored",
      availability: AVAILABILITY.NOT_MONITORED,
    };
  }
  const value = numerator / denominator;
  const pct = Math.round(value * 1000) / 10;
  return {
    value,
    numerator,
    denominator,
    display: `${pct.toFixed(1)}%`,
    availability:
      value === 0 ? AVAILABILITY.NO_PRESENCE_OBSERVED : AVAILABILITY.OBSERVED,
    ZERO_STATE_INTEGRITY: true,
  };
}

/**
 * Canonical citation read path for metrics:
 *   Observation.citations
 *     ← buildObservationFromExtracted({ citations })
 *     ← evidence.payload.citations  (primary persisted cohort path)
 *     ← response.citations          (adapter persist; same governed rows)
 *
 * Raw provider url_citation / grounding / top-level lists are normalized at
 * adapter time into those persisted arrays. Associated searchResults alone
 * are NOT citations and must not appear here.
 */
export const CANONICAL_CITATION_READ_PATH =
  "Observation.citations ← evidence.payload.citations | response.citations (adapter-normalized; not searchResults)";

function responseCitations(obs) {
  if (!obs || typeof obs !== "object") return [];
  if (Array.isArray(obs.citations)) return obs.citations;
  if (Array.isArray(obs.payload?.citations)) return obs.payload.citations;
  if (Array.isArray(obs.extractedCitations)) return obs.extractedCitations;
  return [];
}

function responseDomainSet(obs) {
  const set = new Set();
  for (const c of responseCitations(obs)) {
    const d = (c.domain || parseDomain(c.url || c.sourceUrl) || "").toLowerCase();
    if (d) set.add(d);
  }
  return set;
}

/**
 * Response-level citation incidence (not entity-associated citation rate).
 * CITATION_RATE = responses with ≥1 citation / comparable successful responses
 */
export function computeResponseCitationRates(observations = [], opts = {}) {
  const ownedDomains = (opts.ownedDomains || [])
    .map((d) => {
      if (!d) return null;
      if (typeof d === "string") return d;
      if (typeof d === "object") return d;
      return String(d);
    })
    .filter(Boolean);
  const ownedConfigured = ownedDomains.length > 0;
  const relevant = (observations || []).filter((o) => o && o.success !== false);
  const denom = relevant.length;

  let withCitation = 0;
  let withOwned = 0;
  let withThirdParty = 0;
  const citedDomains = new Set();
  const ownedCited = new Set();
  const thirdPartyCited = new Set();

  for (const obs of relevant) {
    const domains = responseDomainSet(obs);
    if (!domains.size) continue;
    withCitation += 1;
    let hitOwned = false;
    let hitThird = false;
    for (const d of domains) {
      citedDomains.add(d);
      const ownership = classifySourceOwnership(d, ownedDomains);
      if (ownership.type === "OWNED") {
        hitOwned = true;
        ownedCited.add(d);
      } else if (ownership.type === "THIRD_PARTY") {
        hitThird = true;
        thirdPartyCited.add(d);
      } else {
        // UNKNOWN ownership — count toward third-party incidence only when owned list ready
        if (ownedConfigured) {
          hitThird = true;
          thirdPartyCited.add(d);
        }
      }
    }
    if (hitOwned) withOwned += 1;
    if (hitThird) withThirdParty += 1;
  }

  const citationRate = rateShell(withCitation, denom);
  const ownedRate = ownedConfigured
    ? rateShell(withOwned, denom)
    : {
        value: null,
        numerator: null,
        denominator: denom,
        display: "Owned domains not configured",
        availability: AVAILABILITY.UNAVAILABLE,
        OWNED_SOURCE_CLASSIFICATION_READY: false,
      };
  const thirdRate = ownedConfigured
    ? rateShell(withThirdParty, denom)
    : {
        value: null,
        numerator: null,
        denominator: denom,
        display: "Owned domains not configured",
        availability: AVAILABILITY.UNAVAILABLE,
        OWNED_SOURCE_CLASSIFICATION_READY: false,
      };

  return {
    version: CITATION_INTELLIGENCE_VERSION,
    CITATION_RATE: citationRate,
    OWNED_SOURCE_CITATION_RATE: ownedRate,
    THIRD_PARTY_CITATION_RATE: thirdRate,
    EXTERNAL_SOURCE_CITATION_RATE: thirdRate,
    RESPONSES_WITH_CITATIONS: withCitation,
    COMPARABLE_RESPONSES: denom,
    CITED_DOMAINS: [...citedDomains].sort(),
    OWNED_DOMAINS_CITED: [...ownedCited].sort(),
    THIRD_PARTY_DOMAINS_CITED: [...thirdPartyCited].sort(),
    OWNED_SOURCE_CLASSIFICATION_READY: ownedConfigured,
    RATES_MAY_OVERLAP: true,
    NOTE: "Owned and third-party/external rates may both apply to the same response; they do not sum to 100%.",
    CAUSAL_LANGUAGE_USED: false,
    ARBITRARY_CITATION_SCORE: false,
  };
}

/**
 * Mutually exclusive response-level Source Mix (owned vs external composition).
 * Categories sum to successful comparable responses. Not a score.
 */
export function computeSourceMix(observations = [], opts = {}) {
  const ownedDomains = (opts.ownedDomains || [])
    .map((d) => {
      if (!d) return null;
      if (typeof d === "string") return d;
      if (typeof d === "object") return d;
      return String(d);
    })
    .filter(Boolean);
  const ownedConfigured = ownedDomains.length > 0;
  const relevant = (observations || []).filter((o) => o && o.success !== false);
  const denom = relevant.length;

  if (!ownedConfigured) {
    return {
      READY: false,
      OWNED_SOURCE_CLASSIFICATION_READY: false,
      SUCCESSFUL_COMPARABLE_RESPONSES: denom,
      OWNED_ONLY_N: null,
      OWNED_ONLY_RATE: null,
      MIXED_SOURCES_N: null,
      MIXED_SOURCES_RATE: null,
      EXTERNAL_ONLY_N: null,
      EXTERNAL_ONLY_RATE: null,
      NO_CITATIONS_N: null,
      NO_CITATIONS_RATE: null,
      MUTUALLY_EXCLUSIVE: true,
      SUMS_TO_DENOMINATOR: null,
      SOURCE_MIX_DEFINITION,
      display: "Owned domains not configured",
      availability: AVAILABILITY.UNAVAILABLE,
      CAUSAL_LANGUAGE_USED: false,
      ARBITRARY_SOURCE_SCORE: false,
    };
  }

  let ownedOnly = 0;
  let mixed = 0;
  let externalOnly = 0;
  let noCitations = 0;

  for (const obs of relevant) {
    const domains = responseDomainSet(obs);
    let hitOwned = false;
    let hitExternal = false;
    for (const d of domains) {
      const ownership = classifySourceOwnership(d, ownedDomains);
      if (ownership.type === "OWNED") hitOwned = true;
      else hitExternal = true; // THIRD_PARTY or UNKNOWN with owned configured
    }
    if (!hitOwned && !hitExternal) noCitations += 1;
    else if (hitOwned && hitExternal) mixed += 1;
    else if (hitOwned) ownedOnly += 1;
    else externalOnly += 1;
  }

  const sum = ownedOnly + mixed + externalOnly + noCitations;
  const shell = (n) => rateShell(n, denom);

  return {
    READY: denom > 0,
    OWNED_SOURCE_CLASSIFICATION_READY: true,
    SUCCESSFUL_COMPARABLE_RESPONSES: denom,
    OWNED_ONLY_N: ownedOnly,
    OWNED_ONLY_RATE: shell(ownedOnly),
    MIXED_SOURCES_N: mixed,
    MIXED_SOURCES_RATE: shell(mixed),
    EXTERNAL_ONLY_N: externalOnly,
    EXTERNAL_ONLY_RATE: shell(externalOnly),
    NO_CITATIONS_N: noCitations,
    NO_CITATIONS_RATE: shell(noCitations),
    MUTUALLY_EXCLUSIVE: true,
    SUMS_TO_DENOMINATOR: sum === denom,
    SOURCE_MIX_DEFINITION,
    CAUSAL_LANGUAGE_USED: false,
    ARBITRARY_SOURCE_SCORE: false,
  };
}

/**
 * Short deterministic Source Mix interpretation (composition only; no causality).
 */
export function interpretSourceMix(mix) {
  if (!mix || mix.READY !== true) return null;
  const candidates = [
    {
      key: "MIXED_SOURCES",
      n: Number(mix.MIXED_SOURCES_N) || 0,
      statement:
        "AI answers commonly combine official portfolio sources with external sources.",
    },
    {
      key: "OWNED_ONLY",
      n: Number(mix.OWNED_ONLY_N) || 0,
      statement:
        "Most cited answers relied only on governed official sources.",
    },
    {
      key: "EXTERNAL_ONLY",
      n: Number(mix.EXTERNAL_ONLY_N) || 0,
      statement:
        "Most cited answers relied on external sources without citing governed official sources.",
    },
    {
      key: "NO_CITATIONS",
      n: Number(mix.NO_CITATIONS_N) || 0,
      statement:
        "Some monitored answers were returned without explicit citations.",
    },
  ];
  let best = candidates[0];
  for (const c of candidates) {
    if (c.n > best.n) best = c;
  }
  if (best.key === "NO_CITATIONS" && best.n === (mix.SUCCESSFUL_COMPARABLE_RESPONSES || 0)) {
    return {
      dominant: best.key,
      statement:
        "Monitored answers in this cohort were returned without explicit citations.",
      CAUSAL_LANGUAGE_USED: false,
    };
  }
  return {
    dominant: best.key,
    statement: best.statement,
    CAUSAL_LANGUAGE_USED: false,
  };
}

function mapTopSourceCard(source) {
  if (!source) return null;
  const responses =
    source.RESPONSES_CITING_SOURCE ?? source.responsesAppearingIn ?? 0;
  const denom = source.COMPARABLE_RESPONSES ?? null;
  return {
    domain: source.domain,
    responsesAppearingIn: responses,
    RESPONSES_CITING_SOURCE: responses,
    COMPARABLE_RESPONSES: denom,
    SOURCE_CITATION_FREQUENCY: source.SOURCE_CITATION_FREQUENCY ?? null,
    SOURCE_CITATION_FREQUENCY_DISPLAY:
      source.SOURCE_CITATION_FREQUENCY_DISPLAY ?? null,
    CITATION_OCCURRENCES: source.CITATION_OCCURRENCES ?? source.citationCount ?? 0,
    SOURCE_TYPE: source.SOURCE_TYPE || null,
    sourceType: source.sourceType || null,
    PROMPT_FAMILIES_CITING_SOURCE: source.PROMPT_FAMILIES_CITING_SOURCE || [],
    responsesCitingDisplay:
      denom != null ? `${responses} of ${denom}` : String(responses),
  };
}

function mapDomainFrequencyRow(s) {
  return {
    domain: s.domain,
    responsesAppearingIn: s.RESPONSES_CITING_SOURCE ?? s.responsesAppearingIn,
    citationCount: s.CITATION_OCCURRENCES ?? s.citationCount,
    sourceType: s.sourceType,
    SOURCE_TYPE: s.SOURCE_TYPE,
    RESPONSES_CITING_SOURCE: s.RESPONSES_CITING_SOURCE,
    COMPARABLE_RESPONSES: s.COMPARABLE_RESPONSES,
    SOURCE_CITATION_FREQUENCY: s.SOURCE_CITATION_FREQUENCY,
    SOURCE_CITATION_FREQUENCY_DISPLAY: s.SOURCE_CITATION_FREQUENCY_DISPLAY,
    CITATION_OCCURRENCES: s.CITATION_OCCURRENCES,
    PROMPT_FAMILIES_CITING_SOURCE: s.PROMPT_FAMILIES_CITING_SOURCE || [],
    PROVIDERS_CITING_SOURCE: s.PROVIDERS_CITING_SOURCE || [],
    GEOGRAPHIES_CITING_SOURCE: s.GEOGRAPHIES_CITING_SOURCE || [],
    LANGUAGES_CITING_SOURCE: s.LANGUAGES_CITING_SOURCE || [],
    responsesCitingDisplay:
      s.COMPARABLE_RESPONSES != null
        ? `${s.RESPONSES_CITING_SOURCE} of ${s.COMPARABLE_RESPONSES}`
        : String(s.RESPONSES_CITING_SOURCE ?? ""),
  };
}

/**
 * Build executive Source Intelligence panel payload.
 */
export function buildSourceExecutivePanel(observations = [], opts = {}) {
  const rates = computeResponseCitationRates(observations, opts);
  const citationCapability = opts.citationCapability || null;

  if (citationCapability === "unsupported") {
    return {
      version: CITATION_INTELLIGENCE_VERSION,
      READY: false,
      CITATION_SUPPORT: "NOT_SUPPORTED",
      CITATION_RATE: {
        value: null,
        numerator: null,
        denominator: 0,
        display: "Not Supported",
        availability: AVAILABILITY.UNAVAILABLE,
      },
      OWNED_SOURCE_CITATION_RATE: null,
      THIRD_PARTY_CITATION_RATE: null,
      EXTERNAL_SOURCE_CITATION_RATE: null,
      RESPONSES_WITH_CITATIONS: null,
      TOP_OWNED_DOMAIN: null,
      TOP_THIRD_PARTY_DOMAIN: null,
      TOP_EXTERNAL_DOMAIN: null,
      DOMAIN_FREQUENCY: [],
      SOURCE_MIX: null,
      SOURCE_MIX_INTERPRETATION: null,
      SOURCE_MIX_DEFINITION,
      SOURCE_CITATION_FREQUENCY_DEFINITION,
      SOURCE_CITATION_FREQUENCY_DENOMINATOR,
      MULTIPLE_CITATIONS_SAME_RESPONSE,
      TOP_SOURCE_SORT: TOP_CITED_SOURCE_SORT,
      CAUSAL_LANGUAGE_USED: false,
      INFLUENCE_SCORE_CREATED: false,
      AUTHORITY_SCORE_CREATED: false,
    };
  }

  const successful = (observations || []).filter((o) => o && o.success !== false);
  const rows = successful.map((o) => ({
    responseId: o.responseId || o.observationId || o.evidenceId || null,
    observationId: o.observationId || o.evidenceId || null,
    promptId: o.promptId || null,
    intent: o.intentTerritory || o.intent || null,
    promptFamily: o.promptFamily || o.intentTerritory || o.intent || null,
    geographyKey: o.geographyKey || o.geography || null,
    language: o.language || null,
    provider: o.provider || null,
    success: true,
    citations: responseCitations(o),
  }));
  const intel = buildCitedSourceIntelligence(rows, {
    ownedDomains: opts.ownedDomains || [],
    comparableResponses: rates.COMPARABLE_RESPONSES,
    citationCapability,
  });
  const sourceMix = computeSourceMix(observations, opts);
  const sourceMixInterpretation = interpretSourceMix(sourceMix);

  const ranked = [...(intel.TOP_CITED_SOURCES || [])].sort(
    compareSourceCitationFrequency
  );
  const topOwned =
    ranked.find((s) => s.SOURCE_TYPE === "OWNED" || s.sourceType === "OWNED") ||
    null;
  const topExternal =
    ranked.find(
      (s) =>
        s.SOURCE_TYPE === "EXTERNAL" ||
        s.sourceType === "THIRD_PARTY" ||
        s.sourceType === "UNKNOWN"
    ) || null;

  const byProvider = intel.BY_PROVIDER || {};
  let providerHighestOwned = null;
  if (rates.OWNED_SOURCE_CLASSIFICATION_READY) {
    let best = null;
    for (const [provider, domains] of Object.entries(byProvider)) {
      const ownedCount = (domains || []).filter((d) =>
        (opts.ownedDomains || []).some((o) => hostnameMatchesOwnedDomain(d, o))
      ).length;
      if (!best || ownedCount > best.count) {
        best = { provider, count: ownedCount };
      }
    }
    providerHighestOwned = best;
  }

  return {
    version: CITATION_INTELLIGENCE_VERSION,
    READY: rates.COMPARABLE_RESPONSES > 0,
    CITATION_SUPPORT: intel.CITATION_SUPPORT || "SUPPORTED",
    CITATION_RATE: rates.CITATION_RATE,
    OWNED_SOURCE_CITATION_RATE: rates.OWNED_SOURCE_CITATION_RATE,
    THIRD_PARTY_CITATION_RATE: rates.THIRD_PARTY_CITATION_RATE,
    EXTERNAL_SOURCE_CITATION_RATE: rates.THIRD_PARTY_CITATION_RATE,
    RESPONSES_WITH_CITATIONS: rates.RESPONSES_WITH_CITATIONS,
    COMPARABLE_RESPONSES: rates.COMPARABLE_RESPONSES,
    TOP_OWNED_DOMAIN: mapTopSourceCard(topOwned),
    TOP_THIRD_PARTY_DOMAIN: mapTopSourceCard(topExternal),
    TOP_EXTERNAL_DOMAIN: mapTopSourceCard(topExternal),
    TOP_SOURCE_SORT: TOP_CITED_SOURCE_SORT,
    PROVIDER_HIGHEST_OWNED_CITATION_COVERAGE: providerHighestOwned,
    CITED_VS_ASSOCIATED: {
      CITED_DOMAINS: rates.CITED_DOMAINS,
      ASSOCIATED_DOMAINS: rates.CITED_DOMAINS,
      DISTINCTION:
        "Cited = appears as a citation in the response. Associated = linked to the brand observation context without claiming influence.",
    },
    DOMAIN_FREQUENCY: ranked.slice(0, 20).map(mapDomainFrequencyRow),
    SOURCE_MIX: sourceMix,
    SOURCE_MIX_INTERPRETATION: sourceMixInterpretation,
    SOURCE_MIX_DEFINITION,
    SOURCE_CITATION_FREQUENCY_DEFINITION,
    SOURCE_CITATION_FREQUENCY_DENOMINATOR,
    MULTIPLE_CITATIONS_SAME_RESPONSE,
    DOMAINS_BY_PROVIDER: intel.BY_PROVIDER,
    DOMAINS_BY_PROMPT_FAMILY: intel.BY_OWNER_DECISION,
    SOURCE_OVERLAP_BY_PROVIDER: null,
    SOURCE_UNIQUENESS: null,
    citedSourceIntelligence: intel,
    CAUSAL_LANGUAGE_USED: false,
    INFLUENCE_SCORE_CREATED: false,
    AUTHORITY_SCORE_CREATED: false,
  };
}

/**
 * Descriptive Presence × Owned-citation relationship (no causality).
 */
export function describePresenceCitationRelationship(presenceRate, ownedCitationRate) {
  if (
    typeof presenceRate !== "number" ||
    typeof ownedCitationRate !== "number" ||
    !Number.isFinite(presenceRate) ||
    !Number.isFinite(ownedCitationRate)
  ) {
    return null;
  }
  const highP = presenceRate >= 0.4;
  const highO = ownedCitationRate >= 0.25;
  let pattern;
  if (highP && !highO) pattern = "HIGH_PRESENCE_LOW_OWNED_CITATION";
  else if (!highP && highO) pattern = "LOW_PRESENCE_HIGH_OWNED_CITATION";
  else if (highP && highO) pattern = "HIGH_PRESENCE_HIGH_OWNED_CITATION";
  else pattern = "LOW_PRESENCE_LOW_OWNED_CITATION";

  const copy = {
    HIGH_PRESENCE_LOW_OWNED_CITATION:
      "Brand Presence is high in this cohort, while official brand domains are cited in relatively few monitored responses.",
    LOW_PRESENCE_HIGH_OWNED_CITATION:
      "Official brand domains appear among citations more often than the brand appears in monitored answers.",
    HIGH_PRESENCE_HIGH_OWNED_CITATION:
      "Brand Presence is high and official brand domains are also cited in a substantial share of monitored responses.",
    LOW_PRESENCE_LOW_OWNED_CITATION:
      "Brand Presence is limited and official brand domains are cited in relatively few monitored responses.",
  };

  return {
    pattern,
    statement: copy[pattern],
    CAUSAL_LANGUAGE_USED: false,
  };
}
