#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildOfflineCorpusFromOpportunity,
  buildOfflineCorpusWithProvenance,
  mapCandidateToOpportunity,
  collectRetainedCandidatesFromDiscovery,
} from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";
import { DISCOVERY_YIELD_LABEL } from "../lib/group-demand-intelligence/research/gdi-discovery-intake-disposition-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const chainSrc = fs.readFileSync(
  path.join(
    ROOT,
    "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js",
  ),
  "utf8",
);
assert.equal(
  chainSrc.includes(
    "Hotel lodging housing meetings catering sourcing planner contact",
  ),
  false,
  "lodging-keyword padding must be removed from offline corpus builder",
);

const real = buildOfflineCorpusWithProvenance({
  id: "opp_real",
  organizationName: "National Institute Demo",
  title: "Annual Symposium 2027",
  evidence: [
    {
      url: "https://example.org/events/symposium-2027",
      statement: "Registration opens for the 2027 symposium in Bethesda.",
    },
  ],
});
assert.equal(real.synthetic, false);
assert.equal(real.paddedKeywords, false);
assert.ok(
  !JSON.stringify(real.corpus).includes(
    "Hotel lodging housing meetings catering sourcing planner contact",
  ),
);
assert.equal(
  buildOfflineCorpusFromOpportunity({
    id: "opp_real",
    organizationName: "National Institute Demo",
    title: "Annual Symposium 2027",
    evidence: real.corpus["https://example.org/events/symposium-2027"]
      ? [
          {
            url: "https://example.org/events/symposium-2027",
            statement: "Registration opens for the 2027 symposium in Bethesda.",
          },
        ]
      : [
          {
            url: "https://example.org/events/symposium-2027",
            statement: "Registration opens.",
          },
        ],
  })["https://example.org/events/symposium-2027"].includes("lodging housing"),
  false,
);

const synth = buildOfflineCorpusWithProvenance({
  id: "opp_synth",
  organizationName: "Unknown Org",
  title: "Untitled",
  evidence: [],
  acceptedClaims: [],
});
assert.equal(synth.synthetic, true);
assert.ok(synth.syntheticUrls.some((u) => /example\.invalid/i.test(u)));
assert.ok(
  Object.values(synth.corpus).every((body) =>
    /synthetic placeholder/i.test(body),
  ),
);

const mapped = mapCandidateToOpportunity(
  {
    candidateId: "cand_hist_1",
    organizationName: "Hist Org",
    title: "Hist Event",
    discoveryYieldLabel: DISCOVERY_YIELD_LABEL.HISTORICAL_REPLAY,
    claimsGenuineNewDiscovery: false,
    historical: true,
    url: "https://example.org/hist",
  },
  { hotelId: "recLuxvwwxID7U2B8", hotelName: "Bethesda Marriott", segmentId: "government_public_sector" },
);
assert.equal(mapped.discoveryYieldLabel, DISCOVERY_YIELD_LABEL.HISTORICAL_REPLAY);
assert.equal(mapped.claimsGenuineNewDiscovery, false);

const retained = collectRetainedCandidatesFromDiscovery(
  {
    cells: [
      {
        hotelId: "recLuxvwwxID7U2B8",
        hotelName: "Bethesda Marriott",
        segmentId: "corporate_accounts_scaling",
        retrievalMode: "FIXTURE",
        freshRetrievals: 0,
        candidates: [
          {
            candidateId: "fix_cand_1",
            organizationName: "Fixture Scaling Co",
            title: "Expansion Watch 2027",
            fixtureOnly: true,
            hasSourceEvidence: true,
            url: "https://example.invalid/fixture/1",
          },
        ],
      },
    ],
  },
  { includeFixtureCandidates: true },
);
assert.ok(retained.length >= 1);
assert.equal(
  retained.every((r) => r.discoveryYieldLabel !== DISCOVERY_YIELD_LABEL.LIVE_NEW),
  true,
);
assert.equal(
  retained.every((r) => r.claimsGenuineNewDiscovery !== true),
  true,
);

const seeded = collectRetainedCandidatesFromDiscovery(
  { cells: [] },
  {
    seedOpportunities: [
      {
        id: "seed_opp_1",
        hotelId: "recLuxvwwxID7U2B8",
        organizationName: "Seed Org",
        title: "Seed Program",
        segmentId: "projects_infrastructure",
        url: "https://example.org/seed",
      },
    ],
  },
);
assert.equal(seeded[0].discoveryYieldLabel, DISCOVERY_YIELD_LABEL.INVENTORY_REOPEN);
assert.equal(seeded[0].claimsGenuineNewDiscovery, false);

console.log("test-gdi-offline-corpus-honesty-v1: PASS");
