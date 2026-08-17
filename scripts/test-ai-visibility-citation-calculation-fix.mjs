#!/usr/bin/env node
/**
 * Citation metric calculation regression — Observation.citations wiring.
 * No Presence changes. No provider calls. No evidence mutation.
 */
import assert from "node:assert/strict";
import {
  computeResponseCitationRates,
  CANONICAL_CITATION_READ_PATH,
} from "../lib/ai-visibility/citation-intelligence.js";
import { buildObservationFromExtracted } from "../lib/ai-visibility/metrics.js";

const OWNED = ["autograph-hotels.marriott.com"];

function rates(observations, ownedDomains = OWNED) {
  return computeResponseCitationRates(observations, { ownedDomains });
}

function pct(rate) {
  return rate?.display ?? null;
}

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

check("canonical_read_path_documented", () => {
  assert.match(CANONICAL_CITATION_READ_PATH, /Observation\.citations/);
  assert.match(CANONICAL_CITATION_READ_PATH, /evidence\.payload\.citations/);
  assert.match(CANONICAL_CITATION_READ_PATH, /response\.citations/);
  assert.match(CANONICAL_CITATION_READ_PATH, /not searchResults/);
});

check("buildObservation_attaches_citations_array", () => {
  const obs = buildObservationFromExtracted({
    observationId: "ev_1",
    promptId: "p1",
    provider: "openai",
    success: true,
    mentions: [],
    citations: [
      {
        url: "https://stories.hilton.com/x",
        domain: "stories.hilton.com",
      },
    ],
  });
  assert.equal(obs.citations.length, 1);
  assert.equal(obs.citations[0].domain, "stories.hilton.com");
});

check("response_with_citations", () => {
  const obs = buildObservationFromExtracted({
    observationId: "ev_cite",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [{ domain: "stories.hilton.com", url: "https://stories.hilton.com/a" }],
  });
  const r = rates([obs]);
  assert.equal(r.RESPONSES_WITH_CITATIONS, 1);
  assert.equal(pct(r.CITATION_RATE), "100%");
  assert.equal(r.CITATION_RATE.numerator, 1);
  assert.equal(r.CITATION_RATE.denominator, 1);
});

check("response_without_citations", () => {
  const obs = buildObservationFromExtracted({
    observationId: "ev_none",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [],
  });
  const r = rates([obs]);
  assert.equal(r.RESPONSES_WITH_CITATIONS, 0);
  assert.equal(pct(r.CITATION_RATE), "0%");
});

check("owned_citation_only", () => {
  const obs = buildObservationFromExtracted({
    observationId: "ev_owned",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [
      {
        domain: "autograph-hotels.marriott.com",
        url: "https://autograph-hotels.marriott.com/",
      },
    ],
  });
  const r = rates([obs]);
  assert.equal(pct(r.CITATION_RATE), "100%");
  assert.equal(pct(r.OWNED_SOURCE_CITATION_RATE), "100%");
  assert.equal(pct(r.THIRD_PARTY_CITATION_RATE), "0%");
});

check("third_party_citation_only", () => {
  const obs = buildObservationFromExtracted({
    observationId: "ev_tp",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [
      {
        domain: "hotel-development.marriott.com",
        url: "https://hotel-development.marriott.com/",
      },
    ],
  });
  const r = rates([obs], OWNED);
  assert.equal(pct(r.CITATION_RATE), "100%");
  assert.equal(pct(r.OWNED_SOURCE_CITATION_RATE), "0%");
  assert.equal(pct(r.THIRD_PARTY_CITATION_RATE), "100%");
});

check("both_owned_and_third_party", () => {
  const obs = buildObservationFromExtracted({
    observationId: "ev_both",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [
      {
        domain: "autograph-hotels.marriott.com",
        url: "https://autograph-hotels.marriott.com/",
      },
      { domain: "stories.hilton.com", url: "https://stories.hilton.com/a" },
    ],
  });
  const r = rates([obs]);
  assert.equal(pct(r.CITATION_RATE), "100%");
  assert.equal(pct(r.OWNED_SOURCE_CITATION_RATE), "100%");
  assert.equal(pct(r.THIRD_PARTY_CITATION_RATE), "100%");
  assert.equal(r.RATES_MAY_OVERLAP, true);
});

check("successful_denominator_only", () => {
  const ok = buildObservationFromExtracted({
    observationId: "ev_ok",
    promptId: "p1",
    provider: "openai",
    success: true,
    citations: [{ domain: "stories.hilton.com", url: "https://stories.hilton.com/a" }],
  });
  const bad = buildObservationFromExtracted({
    observationId: "ev_fail",
    promptId: "p2",
    provider: "openai",
    success: false,
    citations: [{ domain: "stories.hilton.com", url: "https://stories.hilton.com/b" }],
  });
  const r = rates([ok, bad]);
  assert.equal(r.COMPARABLE_RESPONSES, 1);
  assert.equal(r.CITATION_RATE.denominator, 1);
  assert.equal(r.RESPONSES_WITH_CITATIONS, 1);
});

check("failed_response_excluded", () => {
  const bad = buildObservationFromExtracted({
    observationId: "ev_fail_only",
    promptId: "p",
    provider: "openai",
    success: false,
    citations: [{ domain: "stories.hilton.com", url: "https://stories.hilton.com/a" }],
  });
  const r = rates([bad]);
  assert.equal(r.COMPARABLE_RESPONSES, 0);
  assert.equal(r.CITATION_RATE.value, null);
  assert.equal(r.CITATION_RATE.display, "Not Monitored");
});

check("missing_owned_domain_config_null_owned_rate", () => {
  const obs = buildObservationFromExtracted({
    observationId: "ev_cfg",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [{ domain: "stories.hilton.com", url: "https://stories.hilton.com/a" }],
  });
  const r = rates([obs], []);
  assert.equal(pct(r.CITATION_RATE), "100%");
  assert.equal(r.OWNED_SOURCE_CITATION_RATE.value, null);
  assert.match(String(r.OWNED_SOURCE_CITATION_RATE.display), /Owned domains not configured/i);
  assert.equal(r.OWNED_SOURCE_CLASSIFICATION_READY, false);
});

check("associated_source_alone_does_not_count", () => {
  // searchResults / associated evidence must not inflate Citation Rate
  const obs = buildObservationFromExtracted({
    observationId: "ev_assoc",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [],
  });
  obs.searchResults = [
    { domain: "stories.hilton.com", url: "https://stories.hilton.com/a" },
  ];
  obs.associatedSources = [
    { domain: "hotel-development.marriott.com", url: "https://hotel-development.marriott.com/" },
  ];
  const r = rates([obs]);
  assert.equal(r.RESPONSES_WITH_CITATIONS, 0);
  assert.equal(pct(r.CITATION_RATE), "0%");
});

check("openai_evidence_payload_citations_path_via_builder", () => {
  // Cohort loader passes evidence.payload.citations into the builder
  const evidencePayloadCitations = [
    {
      citationId: "cit_1",
      url: "https://newsroom.hyatt.com/x",
      domain: "newsroom.hyatt.com",
      providerSupplied: true,
    },
  ];
  const obs = buildObservationFromExtracted({
    observationId: "ev_openai_payload",
    promptId: "p_cala_x",
    provider: "openai",
    success: true,
    citations: evidencePayloadCitations,
  });
  const r = rates([obs]);
  assert.equal(obs.citations.length, 1);
  assert.equal(pct(r.CITATION_RATE), "100%");
});

check("response_citations_path_direct_on_observation", () => {
  // Alternate shape: response.citations already on observation-like row
  const obs = {
    observationId: "resp_path",
    promptId: "p",
    provider: "openai",
    success: true,
    citations: [
      {
        url: "https://group.accor.com/x",
        domain: "group.accor.com",
      },
    ],
  };
  const r = rates([obs]);
  assert.equal(pct(r.CITATION_RATE), "100%");
  assert.equal(pct(r.THIRD_PARTY_CITATION_RATE), "100%");
});

check("payload_citations_fallback_still_works", () => {
  const obs = {
    observationId: "legacy_payload",
    promptId: "p",
    provider: "perplexity",
    success: true,
    payload: {
      citations: [{ domain: "ihgplc.com", url: "https://www.ihgplc.com/" }],
    },
  };
  const r = rates([obs]);
  assert.equal(pct(r.CITATION_RATE), "100%");
});

console.log(
  JSON.stringify(
    {
      TOTAL: passed + failed,
      PASS: passed,
      FAIL: failed,
      CANONICAL_CITATION_READ_PATH,
    },
    null,
    2
  )
);

if (failed > 0) process.exit(1);
