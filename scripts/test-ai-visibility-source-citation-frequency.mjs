#!/usr/bin/env node
/**
 * Source citation frequency — factual cohort-scoped measures only.
 * No influence/authority scores. No provider calls. No evidence mutation.
 */
import assert from "node:assert/strict";
import {
  buildCitedSourceIntelligence,
  compareSourceCitationFrequency,
  SOURCE_CITATION_FREQUENCY_DEFINITION,
  MULTIPLE_CITATIONS_SAME_RESPONSE,
} from "../lib/ai-visibility/cited-source-intelligence.js";
import {
  buildSourceExecutivePanel,
  computeResponseCitationRates,
} from "../lib/ai-visibility/citation-intelligence.js";
import { buildObservationFromExtracted } from "../lib/ai-visibility/metrics.js";

const OWNED = ["hotel-development.marriott.com", "marriott.com"];

let passed = 0;
let failed = 0;

function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err?.stack || err);
  }
}

function obs(partial) {
  return buildObservationFromExtracted({
    observationId: partial.observationId || partial.responseId,
    promptId: partial.promptId || "p1",
    provider: partial.provider || "openai",
    success: partial.success !== false,
    mentions: partial.mentions || [],
    citations: partial.citations || [],
    intentTerritory: partial.promptFamily || partial.intentTerritory || null,
    geography: partial.geography || "CALA",
    ...partial,
  });
}

check("definition_documented", () => {
  assert.match(SOURCE_CITATION_FREQUENCY_DEFINITION, /Share of successful monitored responses/i);
  assert.equal(MULTIPLE_CITATIONS_SAME_RESPONSE, "COUNT_ONCE_FOR_FREQUENCY");
});

check("multiple_citations_same_response_count_once_for_frequency", () => {
  const rows = [
    {
      responseId: "r1",
      promptId: "p1",
      promptFamily: "Conversion",
      provider: "openai",
      success: true,
      citations: [
        { domain: "stories.hilton.com", url: "https://stories.hilton.com/a" },
        { domain: "stories.hilton.com", url: "https://stories.hilton.com/b" },
        { domain: "stories.hilton.com", url: "https://stories.hilton.com/c" },
      ],
    },
  ];
  const intel = buildCitedSourceIntelligence(rows, {
    ownedDomains: OWNED,
    comparableResponses: 12,
  });
  const s = intel.TOP_CITED_SOURCES.find((x) => x.domain === "stories.hilton.com");
  assert.equal(s.RESPONSES_CITING_SOURCE, 1);
  assert.equal(s.CITATION_OCCURRENCES, 3);
  assert.equal(s.SOURCE_CITATION_FREQUENCY, 1 / 12);
  assert.equal(s.SOURCE_CITATION_FREQUENCY_DISPLAY, "8.3%");
});

check("same_domain_across_multiple_responses", () => {
  const rows = [
    {
      responseId: "r1",
      promptFamily: "Conversion",
      success: true,
      citations: [{ domain: "stories.hilton.com" }],
    },
    {
      responseId: "r2",
      promptFamily: "Lifestyle Positioning",
      success: true,
      citations: [{ domain: "stories.hilton.com" }],
    },
    {
      responseId: "r3",
      promptFamily: "Conversion",
      success: true,
      citations: [{ domain: "newsroom.hyatt.com" }],
    },
  ];
  const intel = buildCitedSourceIntelligence(rows, {
    ownedDomains: OWNED,
    comparableResponses: 3,
  });
  const hilton = intel.TOP_CITED_SOURCES.find((x) => x.domain === "stories.hilton.com");
  assert.equal(hilton.RESPONSES_CITING_SOURCE, 2);
  assert.equal(hilton.SOURCE_CITATION_FREQUENCY, 2 / 3);
  assert.deepEqual(hilton.PROMPT_FAMILIES_CITING_SOURCE, [
    "Conversion",
    "Lifestyle Positioning",
  ]);
});

check("citation_occurrence_count_remains_separate", () => {
  const rows = [
    {
      responseId: "r1",
      success: true,
      citations: [
        { domain: "a.example" },
        { domain: "a.example" },
      ],
    },
    {
      responseId: "r2",
      success: true,
      citations: [{ domain: "a.example" }],
    },
  ];
  const intel = buildCitedSourceIntelligence(rows, { comparableResponses: 2 });
  const s = intel.TOP_CITED_SOURCES[0];
  assert.equal(s.RESPONSES_CITING_SOURCE, 2);
  assert.equal(s.CITATION_OCCURRENCES, 3);
  assert.notEqual(s.SOURCE_CITATION_FREQUENCY, 3 / 2);
  assert.equal(s.SOURCE_CITATION_FREQUENCY, 1);
});

check("provider_specific_denominator", () => {
  const observations = [
    obs({
      observationId: "o1",
      provider: "openai",
      citations: [{ domain: "stories.hilton.com" }],
    }),
    obs({
      observationId: "o2",
      provider: "openai",
      citations: [],
    }),
  ];
  const panel = buildSourceExecutivePanel(observations, { ownedDomains: OWNED });
  assert.equal(panel.COMPARABLE_RESPONSES, 2);
  const row = panel.DOMAIN_FREQUENCY.find((d) => d.domain === "stories.hilton.com");
  assert.equal(row.COMPARABLE_RESPONSES, 2);
  assert.equal(row.RESPONSES_CITING_SOURCE, 1);
  assert.equal(row.SOURCE_CITATION_FREQUENCY, 0.5);
});

check("failed_response_excluded", () => {
  const observations = [
    obs({
      observationId: "ok",
      success: true,
      citations: [{ domain: "stories.hilton.com" }],
    }),
    obs({
      observationId: "fail",
      success: false,
      citations: [{ domain: "stories.hilton.com" }],
    }),
  ];
  const panel = buildSourceExecutivePanel(observations, { ownedDomains: OWNED });
  assert.equal(panel.COMPARABLE_RESPONSES, 1);
  const row = panel.DOMAIN_FREQUENCY.find((d) => d.domain === "stories.hilton.com");
  assert.equal(row.RESPONSES_CITING_SOURCE, 1);
  assert.equal(row.SOURCE_CITATION_FREQUENCY, 1);
});

check("prompt_family_distinct_count", () => {
  const rows = [
    {
      responseId: "r1",
      promptFamily: "Conversion",
      success: true,
      citations: [{ domain: "hotel-development.marriott.com" }],
    },
    {
      responseId: "r2",
      promptFamily: "Conversion",
      success: true,
      citations: [{ domain: "hotel-development.marriott.com" }],
    },
    {
      responseId: "r3",
      promptFamily: "Branded Residences",
      success: true,
      citations: [{ domain: "hotel-development.marriott.com" }],
    },
  ];
  const intel = buildCitedSourceIntelligence(rows, {
    ownedDomains: OWNED,
    comparableResponses: 3,
  });
  const s = intel.TOP_CITED_SOURCES[0];
  assert.deepEqual(s.PROMPT_FAMILIES_CITING_SOURCE, [
    "Branded Residences",
    "Conversion",
  ]);
  assert.equal(s.PROMPT_FAMILIES_CITING_SOURCE.length, 2);
});

check("owned_and_external_source_ranking", () => {
  const observations = [
    obs({
      observationId: "a",
      promptFamily: "Conversion",
      citations: [{ domain: "hotel-development.marriott.com" }],
    }),
    obs({
      observationId: "b",
      promptFamily: "Conversion",
      citations: [
        { domain: "hotel-development.marriott.com" },
        { domain: "stories.hilton.com" },
      ],
    }),
    obs({
      observationId: "c",
      promptFamily: "Soft Brand",
      citations: [{ domain: "stories.hilton.com" }],
    }),
  ];
  const panel = buildSourceExecutivePanel(observations, { ownedDomains: OWNED });
  assert.equal(panel.TOP_OWNED_DOMAIN.domain, "hotel-development.marriott.com");
  assert.equal(panel.TOP_OWNED_DOMAIN.RESPONSES_CITING_SOURCE, 2);
  assert.equal(panel.TOP_OWNED_DOMAIN.SOURCE_CITATION_FREQUENCY_DISPLAY, "66.7%");
  assert.equal(panel.TOP_EXTERNAL_DOMAIN.domain, "stories.hilton.com");
  assert.equal(panel.TOP_EXTERNAL_DOMAIN.RESPONSES_CITING_SOURCE, 2);
  assert.equal(panel.TOP_OWNED_DOMAIN.SOURCE_TYPE, "OWNED");
  assert.equal(panel.TOP_EXTERNAL_DOMAIN.SOURCE_TYPE, "EXTERNAL");
});

check("deterministic_domain_tie_break", () => {
  const a = {
    domain: "zeta.example",
    RESPONSES_CITING_SOURCE: 2,
    CITATION_OCCURRENCES: 2,
  };
  const b = {
    domain: "alpha.example",
    RESPONSES_CITING_SOURCE: 2,
    CITATION_OCCURRENCES: 2,
  };
  assert.ok(compareSourceCitationFrequency(b, a) < 0);
  const rows = [
    {
      responseId: "r1",
      success: true,
      citations: [{ domain: "zeta.example" }, { domain: "alpha.example" }],
    },
    {
      responseId: "r2",
      success: true,
      citations: [{ domain: "zeta.example" }, { domain: "alpha.example" }],
    },
  ];
  const intel = buildCitedSourceIntelligence(rows, { comparableResponses: 2 });
  assert.equal(intel.TOP_CITED_SOURCES[0].domain, "alpha.example");
  assert.equal(intel.TOP_CITED_SOURCES[1].domain, "zeta.example");
});

check("associated_only_source_excluded", () => {
  const rows = [
    {
      responseId: "r1",
      success: true,
      citations: [{ domain: "cited.example" }],
      searchResults: [{ domain: "associated-only.example" }],
      associatedSources: [{ domain: "associated-only.example" }],
    },
  ];
  const intel = buildCitedSourceIntelligence(rows, { comparableResponses: 1 });
  assert.ok(intel.TOP_CITED_SOURCES.some((s) => s.domain === "cited.example"));
  assert.ok(
    !intel.TOP_CITED_SOURCES.some((s) => s.domain === "associated-only.example")
  );
});

check("unsupported_citation_state_not_converted_to_zero", () => {
  const observations = [
    obs({
      observationId: "o1",
      citations: [],
    }),
  ];
  const panel = buildSourceExecutivePanel(observations, {
    ownedDomains: OWNED,
    citationCapability: "unsupported",
  });
  assert.equal(panel.CITATION_SUPPORT, "NOT_SUPPORTED");
  assert.equal(panel.CITATION_RATE.display, "Not Supported");
  assert.equal(panel.CITATION_RATE.value, null);
  assert.notEqual(panel.CITATION_RATE.display, "0%");
  assert.deepEqual(panel.DOMAIN_FREQUENCY, []);
  assert.equal(panel.TOP_OWNED_DOMAIN, null);
});

check("portfolio_citation_rates_unchanged_contract", () => {
  const observations = [
    obs({
      observationId: "o1",
      citations: [
        { domain: "hotel-development.marriott.com" },
        { domain: "stories.hilton.com" },
      ],
    }),
    obs({
      observationId: "o2",
      citations: [{ domain: "stories.hilton.com" }],
    }),
  ];
  const rates = computeResponseCitationRates(observations, { ownedDomains: OWNED });
  assert.equal(rates.CITATION_RATE.numerator, 2);
  assert.equal(rates.OWNED_SOURCE_CITATION_RATE.numerator, 1);
  assert.equal(rates.EXTERNAL_SOURCE_CITATION_RATE.numerator, 2);
  assert.equal(rates.RESPONSES_WITH_CITATIONS, 2);
  assert.equal(rates.ARBITRARY_CITATION_SCORE, false);
});

check("zero_citing_domains_omitted_from_ranking", () => {
  const intel = buildCitedSourceIntelligence(
    [{ responseId: "r1", success: true, citations: [] }],
    { comparableResponses: 1 }
  );
  assert.equal(intel.TOP_CITED_SOURCES.length, 0);
});

console.log(JSON.stringify({ TOTAL: passed + failed, PASS: passed, FAIL: failed }, null, 2));
if (failed > 0) process.exit(1);
