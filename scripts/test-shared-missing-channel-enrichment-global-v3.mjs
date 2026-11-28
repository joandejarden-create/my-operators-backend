#!/usr/bin/env node
/**
 * GLOBAL missing-channel + Surfe include regression (HE + GDI shared).
 * Synthetic fixtures only — no hotel IDs, person names, or real PII hardcodes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
  surfeIncludeForChannel,
  resolveEnrichmentChannelRequest,
  MISSING_CHANNEL_ENRICHMENT_V3,
} from "../lib/shared-contact-intelligence/missing-channel-enrichment-v3.js";
import { buildSurfeEnrichPersonInput } from "../lib/surfe/client.js";
import { surfeIncludeForChannel as gdiSurfeInclude } from "../lib/group-demand-intelligence/gdi-surfe-contact-provider-v1.js";
import { revealGdiContactOnDemand, createMockGdiContactRevealProvider } from "../lib/group-demand-intelligence/gdi-contact-on-demand-reveal-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

/** Synthetic multi-market cohort — placeholders only. */
const COHORT = [
  {
    market: "north_america_a",
    hotelKey: "hotel_fixture_alpha",
    people: [
      {
        id: "p_complete",
        email: "complete@example.test",
        phone: "+10000000001",
        linkedinUrl: "https://www.linkedin.com/in/fixture-complete/",
      },
      {
        id: "p_email_only",
        email: "emailonly@example.test",
        phone: null,
        linkedinUrl: "https://www.linkedin.com/in/fixture-email-only/",
      },
      {
        id: "p_phone_only",
        email: null,
        phone: "+10000000002",
        linkedinUrl: null,
      },
      {
        id: "p_both_missing_li",
        email: null,
        phone: null,
        linkedinUrl: "https://www.linkedin.com/in/fixture-both-missing/",
      },
      {
        id: "p_both_missing_no_li",
        email: null,
        phone: null,
        linkedinUrl: null,
      },
    ],
  },
  {
    market: "latam_b",
    hotelKey: "hotel_fixture_bravo",
    people: [
      {
        id: "p_email_only_b",
        email: "latam@example.test",
        phone: null,
        linkedinUrl: "https://www.linkedin.com/in/fixture-latam/",
      },
      {
        id: "p_both_missing_b",
        email: null,
        phone: null,
        linkedinUrl: "https://www.linkedin.com/in/fixture-latam-gap/",
      },
    ],
  },
  {
    market: "europe_c",
    hotelKey: "hotel_fixture_charlie",
    people: [
      {
        id: "p_phone_only_c",
        email: null,
        phone: "+34000000003",
        linkedinUrl: "https://www.linkedin.com/in/fixture-eu/",
      },
    ],
  },
];

const GATES = {
  SHARED_MODULE: "CONTACT_SHARED_MISSING_CHANNEL_MODULE",
  HE_GDI_PARITY: "CONTACT_HE_GDI_SURFE_INCLUDE_PARITY",
  MATRIX: "CONTACT_GLOBAL_CHANNEL_MATRIX",
  LINKEDIN_IDENTITY: "CONTACT_VERIFIED_LINKEDIN_IDENTITY_INPUT",
  ASYNC_PARTIAL: "CONTACT_ASYNC_PARTIAL_AND_SKIP",
  NO_HARDCODES: "CONTACT_NO_PRODUCTION_PII_HARDCODES",
  MULTI_MARKET: "CONTACT_MULTI_MARKET_REPRESENTATIVE",
};

const results = {};
function pass(g, d = {}) {
  results[g] = { status: "PASS", ...d };
}

console.log("Shared missing-channel enrichment GLOBAL V3…");

// Shared module identity
assert.equal(MISSING_CHANNEL_ENRICHMENT_V3, "missing-channel-enrichment-v3");
pass(GATES.SHARED_MODULE, { version: MISSING_CHANNEL_ENRICHMENT_V3 });

// HE/GDI Surfe include parity for channel strings
for (const ch of ["email", "phone", "both", "none"]) {
  const a = surfeIncludeForChannel(ch);
  const b = gdiSurfeInclude(ch);
  assert.equal(a.email, b.email, `email parity ${ch}`);
  assert.equal(a.mobile, b.mobile, `mobile parity ${ch}`);
}
pass(GATES.HE_GDI_PARITY, { channels: ["email", "phone", "both", "none"] });

// Full matrix across cohort
const matrix = [];
for (const hotel of COHORT) {
  for (const p of hotel.people) {
    const state = computeMissingContactChannels(p);
    const include = surfeIncludeFromMissingState(state);
    const resolved = resolveEnrichmentChannelRequest({ person: p, channelHint: "both" });
    matrix.push({
      market: hotel.market,
      hotelKey: hotel.hotelKey,
      personId: p.id,
      requestChannel: state.requestChannel,
      surfeInclude: { email: include.email, mobile: include.mobile },
      source: resolved.source,
    });
    if (state.requestChannel === "none") {
      assert.equal(include.email, false);
      assert.equal(include.mobile, false);
    }
    if (state.requestChannel === "email") {
      assert.equal(include.email, true);
      assert.equal(include.mobile, false);
    }
    if (state.requestChannel === "phone") {
      assert.equal(include.email, false);
      assert.equal(include.mobile, true);
    }
    if (state.requestChannel === "both") {
      assert.equal(include.email, true);
      assert.equal(include.mobile, true);
    }
    // LinkedIn never fills email/phone
    if (p.linkedinUrl && !p.email && !p.phone) {
      assert.equal(state.needsEmail, true);
      assert.equal(state.needsPhone, true);
      assert.equal(state.hasVerifiedLinkedIn, true);
    }
  }
}
assert.ok(matrix.some((r) => r.requestChannel === "none"));
assert.ok(matrix.some((r) => r.requestChannel === "email"));
assert.ok(matrix.some((r) => r.requestChannel === "phone"));
assert.ok(matrix.some((r) => r.requestChannel === "both"));
pass(GATES.MATRIX, { rows: matrix.length });

// LinkedIn identity input for Surfe person builder (synthetic slug)
const personBody = buildSurfeEnrichPersonInput({
  personName: "Fixture Person Name",
  companyName: "Fixture Org",
  linkedinUrl: "https://www.linkedin.com/in/fixture-complete/",
});
assert.equal(personBody.linkedinUrl, "https://www.linkedin.com/in/fixture-complete");
assert.ok(!("linkedInUrl" in personBody));
pass(GATES.LINKEDIN_IDENTITY, { field: "linkedinUrl" });

// GDI reveal skip when complete; mock partial; pending not NOT_FOUND
const skip = await revealGdiContactOnDemand({
  channel: "both",
  contact: {
    name: "Fixture Complete Person",
    email: "complete@example.test",
    phone: "+10000000001",
  },
  organization: { name: "Fixture Org" },
  provider: createMockGdiContactRevealProvider("hit"),
});
assert.equal(skip.skipped, true);
assert.equal(skip.persisted, false);
assert.equal(skip.providerCalls == null || skip.providerCalls === 0 || skip.ok === true, true);

const mockPending = createMockGdiContactRevealProvider("miss");
const miss = await revealGdiContactOnDemand({
  channel: "email",
  contact: { name: "Fixture Gap Person", linkedinUrl: "https://www.linkedin.com/in/fixture-gap/" },
  organization: { name: "Fixture Org" },
  provider: mockPending,
});
assert.equal(miss.persisted, false);
assert.equal(miss.ephemeral, true);
pass(GATES.ASYNC_PARTIAL, { skipComplete: true, missEphemeral: true });

// No production hardcodes of golden PII in shared module / GDI provider / HE API reveal mapper
const sharedSrc = fs.readFileSync(
  path.join(ROOT, "lib/shared-contact-intelligence/missing-channel-enrichment-v3.js"),
  "utf8",
);
const gdiProv = fs.readFileSync(
  path.join(ROOT, "lib/group-demand-intelligence/gdi-surfe-contact-provider-v1.js"),
  "utf8",
);
const heApi = fs.readFileSync(path.join(ROOT, "api/contact-intelligence.js"), "utf8");
assert.doesNotMatch(sharedSrc, /meinrad|marriott\.com|recLuxvwwxID7U2B8|\+13019082451/i);
assert.doesNotMatch(gdiProv, /meinrad|marriott\.com|recLuxvwwxID7U2B8|\+13019082451/i);
assert.doesNotMatch(heApi, /meinrad\.lang@marriott\.com|\+13019082451/i);
assert.match(heApi, /resolveCanonicalPersonChannels|computeMissingContactChannels/);
assert.match(gdiProv, /missing-channel-enrichment-v3|resolveEnrichmentChannelRequest/);
pass(GATES.NO_HARDCODES, { scanned: ["shared", "gdi-provider", "he-api"] });

const markets = [...new Set(matrix.map((r) => r.market))];
assert.ok(markets.length >= 3);
pass(GATES.MULTI_MARKET, { markets, hotels: COHORT.length, people: matrix.length });

const outDir = path.join(
  ROOT,
  "reports/shared-contact-intelligence/missing-channel-enrichment-global-v3",
);
fs.mkdirSync(outDir, { recursive: true });
const report = {
  at: new Date().toISOString(),
  version: MISSING_CHANNEL_ENRICHMENT_V3,
  scope: "GLOBAL_HE_AND_GDI",
  gates: results,
  matrixSummary: {
    markets: markets.length,
    hotels: COHORT.length,
    people: matrix.length,
    byRequestChannel: matrix.reduce((acc, r) => {
      acc[r.requestChannel] = (acc[r.requestChannel] || 0) + 1;
      return acc;
    }, {}),
  },
};
fs.writeFileSync(path.join(outDir, "GLOBAL_GATES.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
console.log("PASS — Shared missing-channel enrichment GLOBAL V3");
