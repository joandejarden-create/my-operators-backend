#!/usr/bin/env node
/**
 * Source Mix + executive citation reframe tests.
 * No provider calls. No evidence mutation. No metric contract changes.
 */
import assert from "node:assert/strict";
import {
  computeSourceMix,
  interpretSourceMix,
  computeResponseCitationRates,
  buildSourceExecutivePanel,
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
    mentions: [],
    citations: partial.citations || [],
    ...partial,
  });
}

check("owned_only_response", () => {
  const mix = computeSourceMix(
    [
      obs({
        observationId: "a",
        citations: [{ domain: "hotel-development.marriott.com" }],
      }),
    ],
    { ownedDomains: OWNED }
  );
  assert.equal(mix.OWNED_ONLY_N, 1);
  assert.equal(mix.MIXED_SOURCES_N, 0);
  assert.equal(mix.EXTERNAL_ONLY_N, 0);
  assert.equal(mix.NO_CITATIONS_N, 0);
});

check("mixed_response", () => {
  const mix = computeSourceMix(
    [
      obs({
        observationId: "a",
        citations: [
          { domain: "hotel-development.marriott.com" },
          { domain: "stories.hilton.com" },
        ],
      }),
    ],
    { ownedDomains: OWNED }
  );
  assert.equal(mix.MIXED_SOURCES_N, 1);
  assert.equal(mix.OWNED_ONLY_N, 0);
});

check("external_only_response", () => {
  const mix = computeSourceMix(
    [
      obs({
        observationId: "a",
        citations: [{ domain: "stories.hilton.com" }],
      }),
    ],
    { ownedDomains: OWNED }
  );
  assert.equal(mix.EXTERNAL_ONLY_N, 1);
});

check("no_citation_response", () => {
  const mix = computeSourceMix(
    [obs({ observationId: "a", citations: [] })],
    { ownedDomains: OWNED }
  );
  assert.equal(mix.NO_CITATIONS_N, 1);
});

check("categories_mutually_exclusive_and_sum", () => {
  const observations = [
    obs({
      observationId: "owned",
      citations: [{ domain: "marriott.com" }, { domain: "marriott.com" }],
    }),
    obs({
      observationId: "mixed",
      citations: [
        { domain: "hotel-development.marriott.com" },
        { domain: "stories.hilton.com" },
        { domain: "newsroom.hyatt.com" },
      ],
    }),
    obs({
      observationId: "external",
      citations: [{ domain: "stories.hilton.com" }],
    }),
    obs({ observationId: "none", citations: [] }),
  ];
  const mix = computeSourceMix(observations, { ownedDomains: OWNED });
  assert.equal(mix.MUTUALLY_EXCLUSIVE, true);
  assert.equal(mix.SUMS_TO_DENOMINATOR, true);
  assert.equal(mix.SUCCESSFUL_COMPARABLE_RESPONSES, 4);
  assert.equal(
    mix.OWNED_ONLY_N +
      mix.MIXED_SOURCES_N +
      mix.EXTERNAL_ONLY_N +
      mix.NO_CITATIONS_N,
    4
  );
  const pctSum =
    mix.OWNED_ONLY_RATE.value +
    mix.MIXED_SOURCES_RATE.value +
    mix.EXTERNAL_ONLY_RATE.value +
    mix.NO_CITATIONS_RATE.value;
  assert.ok(Math.abs(pctSum - 1) < 1e-9);
});

check("multiple_owned_citations_still_one_owned_only", () => {
  const mix = computeSourceMix(
    [
      obs({
        observationId: "a",
        citations: [
          { domain: "marriott.com" },
          { domain: "hotel-development.marriott.com" },
        ],
      }),
    ],
    { ownedDomains: OWNED }
  );
  assert.equal(mix.OWNED_ONLY_N, 1);
  assert.equal(mix.MIXED_SOURCES_N, 0);
});

check("owned_plus_multiple_external_is_mixed", () => {
  const mix = computeSourceMix(
    [
      obs({
        observationId: "a",
        citations: [
          { domain: "marriott.com" },
          { domain: "a.example" },
          { domain: "b.example" },
        ],
      }),
    ],
    { ownedDomains: OWNED }
  );
  assert.equal(mix.MIXED_SOURCES_N, 1);
});

check("failed_response_excluded", () => {
  const mix = computeSourceMix(
    [
      obs({
        observationId: "ok",
        citations: [{ domain: "stories.hilton.com" }],
      }),
      obs({
        observationId: "fail",
        success: false,
        citations: [{ domain: "marriott.com" }],
      }),
    ],
    { ownedDomains: OWNED }
  );
  assert.equal(mix.SUCCESSFUL_COMPARABLE_RESPONSES, 1);
  assert.equal(mix.EXTERNAL_ONLY_N, 1);
  assert.equal(mix.OWNED_ONLY_N, 0);
});

check("existing_citation_rates_unchanged_contract", () => {
  const observations = [
    obs({
      observationId: "m",
      citations: [
        { domain: "hotel-development.marriott.com" },
        { domain: "stories.hilton.com" },
      ],
    }),
  ];
  const rates = computeResponseCitationRates(observations, {
    ownedDomains: OWNED,
  });
  assert.equal(rates.CITATION_RATE.display, "100%");
  assert.equal(rates.OWNED_SOURCE_CITATION_RATE.display, "100%");
  assert.equal(rates.EXTERNAL_SOURCE_CITATION_RATE.display, "100%");
  assert.equal(rates.RATES_MAY_OVERLAP, true);
  assert.equal(rates.ARBITRARY_CITATION_SCORE, false);
});

check("source_frequency_ranking_unaffected", () => {
  const panel = buildSourceExecutivePanel(
    [
      obs({
        observationId: "a",
        citations: [
          { domain: "hotel-development.marriott.com" },
          { domain: "stories.hilton.com" },
        ],
      }),
      obs({
        observationId: "b",
        citations: [{ domain: "stories.hilton.com" }],
      }),
    ],
    { ownedDomains: OWNED }
  );
  assert.equal(panel.TOP_EXTERNAL_DOMAIN.domain, "stories.hilton.com");
  assert.equal(panel.TOP_EXTERNAL_DOMAIN.RESPONSES_CITING_SOURCE, 2);
  assert.equal(panel.TOP_OWNED_DOMAIN.domain, "hotel-development.marriott.com");
  assert.ok(panel.SOURCE_MIX);
  assert.equal(panel.SOURCE_MIX.MIXED_SOURCES_N, 1);
  assert.equal(panel.SOURCE_MIX.EXTERNAL_ONLY_N, 1);
  assert.equal(panel.SOURCE_MIX.SUMS_TO_DENOMINATOR, true);
});

check("interpret_mixed_dominant", () => {
  const out = interpretSourceMix({
    READY: true,
    MIXED_SOURCES_N: 8,
    OWNED_ONLY_N: 2,
    EXTERNAL_ONLY_N: 1,
    NO_CITATIONS_N: 1,
    SUCCESSFUL_COMPARABLE_RESPONSES: 12,
  });
  assert.equal(out.dominant, "MIXED_SOURCES");
  assert.match(out.statement, /combine official portfolio sources/i);
  assert.equal(out.CAUSAL_LANGUAGE_USED, false);
});

check("percentages_display_sum_near_100", () => {
  const mix = computeSourceMix(
    Array.from({ length: 3 }, (_, i) =>
      obs({
        observationId: "o" + i,
        citations: [{ domain: "marriott.com" }],
      })
    ).concat([
      obs({
        observationId: "e",
        citations: [{ domain: "stories.hilton.com" }],
      }),
    ]),
    { ownedDomains: OWNED }
  );
  const displays = [
    mix.OWNED_ONLY_RATE.display,
    mix.MIXED_SOURCES_RATE.display,
    mix.EXTERNAL_ONLY_RATE.display,
    mix.NO_CITATIONS_RATE.display,
  ];
  const sum = displays.reduce(
    (acc, d) => acc + parseFloat(String(d).replace("%", "")),
    0
  );
  assert.ok(Math.abs(sum - 100) < 0.15, `display sum ${sum}`);
});

console.log(JSON.stringify({ TOTAL: passed + failed, PASS: passed, FAIL: failed }, null, 2));
if (failed > 0) process.exit(1);
