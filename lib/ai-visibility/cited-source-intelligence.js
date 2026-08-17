/**
 * Cited Source Intelligence — provider-pure deterministic aggregations (Phase 3B.4).
 * No authority/trust/impact scores. No All AI Sources. No causal claims.
 *
 * SOURCE_CITATION_FREQUENCY (cohort-scoped):
 *   unique successful comparable responses citing the domain
 *   ÷ successful comparable responses in the selected cohort
 * Multiple citations of the same domain within one response count once for frequency;
 * CITATION_OCCURRENCES remains the raw citation-row count.
 */

import { parseDomain } from "./extract-citations.js";
import { resolveEvidenceAssociationLevel } from "./evidence-footprint.js";
import { hostnameMatchesOwnedDomain } from "./owned-domain-resolution.js";

export const CITED_SOURCE_INTELLIGENCE_VERSION =
  "ai_visibility_cited_source_intelligence_v1_1";

export const SOURCE_CITATION_FREQUENCY_DEFINITION =
  "Share of successful monitored responses in this cohort that cited this domain.";

export const SOURCE_CITATION_FREQUENCY_DENOMINATOR =
  "Successful comparable monitored responses in the selected provider / geography / language / monitoring cohort.";

export const MULTIPLE_CITATIONS_SAME_RESPONSE = "COUNT_ONCE_FOR_FREQUENCY";

export const LONGITUDINAL_SOURCE_STATUS = Object.freeze({
  NEW_SOURCES: "NOT_YET_AVAILABLE",
  DISAPPEARING_SOURCES: "NOT_YET_AVAILABLE",
  SOURCE_FREQUENCY_MOVEMENT: "NOT_YET_AVAILABLE",
  PERSISTENT_SOURCES_ACROSS_PERIODS: "NOT_YET_AVAILABLE",
});

export const READERSHIP_DATA_STATUS = Object.freeze({
  STATUS: "FUTURE_EXTERNAL_DATA_DEPENDENCY",
  FUTURE_EXTERNAL_DATA_DEPENDENCY: true,
});

/**
 * Top Cited Sources / Top Owned / Top External sort:
 * 1) RESPONSES_CITING_SOURCE (distinct monitored responses)
 * 2) CITATION_OCCURRENCES
 * 3) deterministic domain name
 */
export const TOP_CITED_SOURCE_SORT = Object.freeze({
  primary: "RESPONSES_CITING_SOURCE",
  secondary: "CITATION_OCCURRENCES",
  tertiary: "domain_asc",
  RULE:
    "Sort by distinct responses citing the domain, then citation occurrences, then domain name ascending.",
});

function formatFrequencyPct(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return `${(value * 100).toFixed(1)}%`;
}

/**
 * Owned vs third-party — only if deterministic domain map is provided.
 * Do not AI-infer ownership.
 */
export function classifySourceOwnership(domain, ownedDomains = []) {
  if (!ownedDomains || !ownedDomains.length) {
    return { type: "UNKNOWN", OWNED_SOURCE_CLASSIFICATION_READY: false };
  }
  const owned = ownedDomains.some((o) => hostnameMatchesOwnedDomain(domain, o));
  return {
    type: owned ? "OWNED" : "THIRD_PARTY",
    OWNED_SOURCE_CLASSIFICATION_READY: true,
  };
}

function toSourceType(ownershipType) {
  if (ownershipType === "OWNED") return "OWNED";
  if (ownershipType === "THIRD_PARTY") return "EXTERNAL";
  return "EXTERNAL";
}

/**
 * Compare two source rows for ranking (desc responses, desc occurrences, asc domain).
 */
export function compareSourceCitationFrequency(a, b) {
  const ra = a?.RESPONSES_CITING_SOURCE ?? a?.responsesAppearingIn ?? 0;
  const rb = b?.RESPONSES_CITING_SOURCE ?? b?.responsesAppearingIn ?? 0;
  if (rb !== ra) return rb - ra;
  const oa = a?.CITATION_OCCURRENCES ?? a?.citationCount ?? 0;
  const ob = b?.CITATION_OCCURRENCES ?? b?.citationCount ?? 0;
  if (ob !== oa) return ob - oa;
  return String(a?.domain || "").localeCompare(String(b?.domain || ""));
}

/**
 * @param {Array<object>} rows — response-level citation rows (cited sources only)
 * @param {object} [opts]
 * @param {Array<string|object>} [opts.ownedDomains]
 * @param {number} [opts.comparableResponses] — successful comparable denominator
 * @param {"supported"|"unsupported"|"unavailable"|"partial"|null} [opts.citationCapability]
 */
export function buildCitedSourceIntelligence(rows = [], opts = {}) {
  const ownedDomains = opts.ownedDomains || [];
  const citationCapability = opts.citationCapability || null;

  if (citationCapability === "unsupported") {
    return {
      version: CITED_SOURCE_INTELLIGENCE_VERSION,
      TOP_CITED_SOURCES: [],
      SORT_METHOD: TOP_CITED_SOURCE_SORT,
      BY_OWNER_DECISION: {},
      BY_GEOGRAPHY: {},
      BY_LANGUAGE: {},
      BY_PROVIDER: {},
      OWNED_SOURCE_CLASSIFICATION_READY: ownedDomains.length > 0 ? "YES" : "NO",
      OWNED_METHOD: ownedDomains.length
        ? "deterministic_owned_domain_list"
        : "not_available_no_guesses",
      LONGITUDINAL: { ...LONGITUDINAL_SOURCE_STATUS },
      READERSHIP: { ...READERSHIP_DATA_STATUS },
      ALL_AI_SOURCES: "NOT_IMPLEMENTED",
      SOURCE_DIVERGENCE: "NOT_IMPLEMENTED",
      READY: false,
      CITATION_SUPPORT: "NOT_SUPPORTED",
      COMPARABLE_RESPONSES: 0,
      SOURCE_CITATION_FREQUENCY_DEFINITION,
      SOURCE_CITATION_FREQUENCY_DENOMINATOR,
      MULTIPLE_CITATIONS_SAME_RESPONSE,
      CAUSAL_LANGUAGE_USED: false,
      INFLUENCE_SCORE_CREATED: false,
      AUTHORITY_SCORE_CREATED: false,
    };
  }

  const byDomain = new Map();
  const successfulRows = (rows || []).filter((r) => r && r.success !== false);

  for (const row of successfulRows) {
    const responseId =
      row.responseId || row.observationId || row.runId || row.evidenceId || null;
    const promptId = row.promptId || null;
    const intent =
      row.promptFamily ||
      row.intent ||
      row.intentTerritory ||
      null;
    const geography = row.geographyKey || row.geography || null;
    const language = row.language || null;
    const provider = row.provider || null;
    // Cited only — never searchResults / associated-only lists.
    const citations = Array.isArray(row.citations) ? row.citations : [];

    /** @type {Map<string, { occurrences: number, urls: Set<string> }>} */
    const domainsInResponse = new Map();
    for (const c of citations) {
      const url = c.url || c.sourceUrl || null;
      const domain = (c.domain || parseDomain(url) || "").toLowerCase();
      if (!domain) continue;
      if (!domainsInResponse.has(domain)) {
        domainsInResponse.set(domain, { occurrences: 0, urls: new Set() });
      }
      const hit = domainsInResponse.get(domain);
      hit.occurrences += 1;
      if (url) hit.urls.add(url);
    }

    for (const [domain, hit] of domainsInResponse) {
      if (!byDomain.has(domain)) {
        byDomain.set(domain, {
          domain,
          citationCount: 0,
          responseIds: new Set(),
          promptIds: new Set(),
          intents: new Set(),
          geographies: new Set(),
          languages: new Set(),
          providers: new Set(),
          urls: new Set(),
        });
      }
      const entry = byDomain.get(domain);
      entry.citationCount += hit.occurrences;
      if (responseId) entry.responseIds.add(String(responseId));
      if (promptId) entry.promptIds.add(promptId);
      if (intent) entry.intents.add(intent);
      if (geography) entry.geographies.add(geography);
      if (language) entry.languages.add(language);
      if (provider) entry.providers.add(provider);
      for (const u of hit.urls) entry.urls.add(u);
    }
  }

  const comparableResponses =
    typeof opts.comparableResponses === "number" && opts.comparableResponses >= 0
      ? opts.comparableResponses
      : successfulRows.length;

  const sources = [...byDomain.values()]
    .map((s) => {
      const ownership = classifySourceOwnership(s.domain, ownedDomains);
      const responsesCiting = s.responseIds.size;
      const frequency =
        comparableResponses > 0 ? responsesCiting / comparableResponses : null;
      const promptFamilies = [...s.intents].sort();
      return {
        domain: s.domain,
        label: "Appearing in monitored responses",
        responsesAppearingIn: responsesCiting,
        citationCount: s.citationCount,
        uniquePromptCount: s.promptIds.size,
        ownerDecisions: promptFamilies,
        markets: [...s.geographies].sort(),
        languages: [...s.languages].sort(),
        providers: [...s.providers].sort(),
        uniqueUrls: s.urls.size,
        sourceType: ownership.type,
        SOURCE_TYPE: toSourceType(ownership.type),
        RESPONSES_CITING_SOURCE: responsesCiting,
        COMPARABLE_RESPONSES: comparableResponses,
        SOURCE_CITATION_FREQUENCY: frequency,
        SOURCE_CITATION_FREQUENCY_DISPLAY: formatFrequencyPct(frequency),
        CITATION_OCCURRENCES: s.citationCount,
        PROMPT_FAMILIES_CITING_SOURCE: promptFamilies,
        PROVIDERS_CITING_SOURCE: [...s.providers].sort(),
        GEOGRAPHIES_CITING_SOURCE: [...s.geographies].sort(),
        LANGUAGES_CITING_SOURCE: [...s.languages].sort(),
        CAUSAL_LANGUAGE_USED: false,
        INFLUENCE_SCORE_CREATED: false,
        AUTHORITY_SCORE_CREATED: false,
      };
    })
    .filter((s) => s.RESPONSES_CITING_SOURCE > 0)
    .sort(compareSourceCitationFrequency);

  return {
    version: CITED_SOURCE_INTELLIGENCE_VERSION,
    TOP_CITED_SOURCES: sources,
    SORT_METHOD: TOP_CITED_SOURCE_SORT,
    BY_OWNER_DECISION: groupBy(sources, "ownerDecisions"),
    BY_GEOGRAPHY: groupBy(sources, "markets"),
    BY_LANGUAGE: groupBy(sources, "languages"),
    BY_PROVIDER: groupBy(sources, "providers"),
    OWNED_SOURCE_CLASSIFICATION_READY: ownedDomains.length > 0 ? "YES" : "NO",
    OWNED_METHOD: ownedDomains.length
      ? "deterministic_owned_domain_list"
      : "not_available_no_guesses",
    LONGITUDINAL: { ...LONGITUDINAL_SOURCE_STATUS },
    READERSHIP: { ...READERSHIP_DATA_STATUS },
    ALL_AI_SOURCES: "NOT_IMPLEMENTED",
    SOURCE_DIVERGENCE: "NOT_IMPLEMENTED",
    READY: true,
    CITATION_SUPPORT: "SUPPORTED",
    COMPARABLE_RESPONSES: comparableResponses,
    SOURCE_CITATION_FREQUENCY_DEFINITION,
    SOURCE_CITATION_FREQUENCY_DENOMINATOR,
    MULTIPLE_CITATIONS_SAME_RESPONSE,
    CAUSAL_LANGUAGE_USED: false,
    INFLUENCE_SCORE_CREATED: false,
    AUTHORITY_SCORE_CREATED: false,
  };
}

function groupBy(sources, field) {
  const map = {};
  for (const s of sources) {
    const vals = Array.isArray(s[field]) ? s[field] : [s[field]];
    for (const v of vals) {
      if (!v) continue;
      if (!map[v]) map[v] = [];
      map[v].push(s.domain);
    }
  }
  return map;
}

/**
 * Build matched four-provider prompt groups (foundation only — no consensus metrics).
 */
export function buildMatchedPromptGroups(providerObservations = {}) {
  const providers = Object.keys(providerObservations);
  const byKey = new Map();

  for (const provider of providers) {
    for (const obs of providerObservations[provider] || []) {
      const key = [
        obs.promptId,
        obs.promptVersion || obs.version || "",
        obs.promptFamily || "",
        obs.geographyKey || obs.geography || "",
        obs.language || "",
        obs.intent || obs.intentTerritory || "",
        obs.peerSetId || "",
        obs.peerSetVersion || "2",
        obs.metricVersion || "",
      ].join("|");
      if (!byKey.has(key)) byKey.set(key, { key, providers: {}, promptId: obs.promptId });
      byKey.get(key).providers[provider] = {
        responseId: obs.responseId,
        model: obs.model || null,
        success: obs.success !== false,
      };
    }
  }

  const fullyMatched = [...byKey.values()].filter((g) =>
    providers.every((p) => g.providers[p])
  );

  return {
    FULL_BASELINE_PROVIDERS: providers,
    MATCHED_PROMPT_GROUPS: byKey.size,
    FULLY_MATCHED_PROMPT_COUNT: fullyMatched.length,
    READY: fullyMatched.length > 0,
    CONSENSUS_METRICS: "NOT_IMPLEMENTED",
  };
}

export function providerEvidenceAssociationMap() {
  return {
    OPENAI: resolveEvidenceAssociationLevel("openai"),
    GEMINI: resolveEvidenceAssociationLevel("gemini"),
    PERPLEXITY: resolveEvidenceAssociationLevel("perplexity"),
    CLAUDE: resolveEvidenceAssociationLevel("claude"),
  };
}

/**
 * Filter citation rows by monitoring period (Phase 3B.6).
 */
export function filterSourceRowsByPeriod(rows = [], periodId) {
  if (!periodId) return rows;
  return rows.filter(
    (r) =>
      r.periodId === periodId ||
      r.monitoringPeriodId === periodId ||
      r.batchId === periodId
  );
}

/**
 * Build Cited Source Intelligence scoped to a monitoring period.
 */
export function buildCitedSourceIntelligenceForPeriod(rows = [], opts = {}) {
  const scoped = filterSourceRowsByPeriod(rows, opts.periodId);
  const intel = buildCitedSourceIntelligence(scoped, opts);
  return {
    ...intel,
    periodId: opts.periodId || null,
    SOURCE_PERIOD_FILTER_READY: "YES",
    scopedRowCount: scoped.length,
    longitudinal: { ...LONGITUDINAL_SOURCE_STATUS },
  };
}
