#!/usr/bin/env node
/**
 * PE provider → canonical adapter regressions.
 *   node scripts/test-gdi-pe-provider-adapter-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  adaptPeCandidateToCanonicalPackage,
  adaptPeCandidatesBatch,
  PE_ADAPTER_STATUS,
} from "../lib/group-demand-intelligence/workflow/pe-provider-to-canonical-adapter-v1.js";
import { loadVenuePartnershipPackagesFromFile } from "../lib/group-demand-intelligence/private-events/canonical-persist-and-link-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PE_RAW = path.join(
  ROOT,
  "reports/gdi-paid-multisegment-validation/pe-venue-packages.json"
);

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// 1. Nested Parallel package → venue.venueName
{
  const nested = {
    hotel: { hotelId: "recnPB3Q02NO8Ma1o" },
    venue: {
      name: "deck655",
      website: "https://deck655.com",
      location: "655 W. Broadway, San Diego, CA 92101",
      category: "Wedding venue",
    },
    signal: {
      eventActivityEvidence: "Official site offers wedding receptions.",
      lodgingOrReferralRationale: "Referral partner hypothesis.",
      sourceUrls: ["https://deck655.com/"],
    },
    contact: {
      email: "events@deck655.com",
      phone: "+1-619-702-0655",
      url: "https://deck655.com/contact/",
    },
    publish: true,
    whyRelevantToHotel: "Downtown waterfront territory fit.",
  };
  const adapted = adaptPeCandidateToCanonicalPackage(nested);
  assert.equal(adapted.status, PE_ADAPTER_STATUS.CANONICAL_READY);
  assert.equal(adapted.package.venue.venueName, "deck655");
  assert.equal(adapted.package.hotel.hotelId, "recnPB3Q02NO8Ma1o");
  assert.ok(adapted.package.signals.length >= 1);
  assert.equal(adapted.package.signals[0].artificial, false);
  ok("nested_parallel_to_venueName");
}

// 2. Shared domain keeps distinct venues
{
  const batch = adaptPeCandidatesBatch([
    {
      hotel: { hotelId: "recnPB3Q02NO8Ma1o" },
      venue: {
        name: "Nolita Hall",
        website: "https://nolitahall.com",
        location: "2305 India Street, San Diego, CA 92101",
      },
      signal: {
        eventActivityEvidence: "Private events up to 300.",
        lodgingOrReferralRationale: "Referral partner.",
        sourceUrls: ["https://www.nolitahall.com/nolita-hall-events"],
      },
      contact: { phone: "(619) 255-8000", url: "https://www.nolitahall.com/nolita-hall-events" },
      publish: true,
      whyRelevantToHotel: "Little Italy catchment.",
    },
    {
      hotel: { hotelId: "recnPB3Q02NO8Ma1o" },
      venue: {
        name: "Nolita House",
        website: "https://nolitahall.com",
        location: "Little Italy, San Diego, CA 92101",
      },
      signal: {
        eventActivityEvidence: "Intimate lounge for 50 guests.",
        lodgingOrReferralRationale: "Separate small-event referral.",
        sourceUrls: ["https://www.nolitahall.com/nolita-house-events"],
      },
      contact: { phone: "(619) 255-8000", url: "https://www.nolitahall.com/nolita-house-events" },
      publish: true,
      whyRelevantToHotel: "Distinct sister venue.",
    },
  ]);
  assert.equal(batch.packageCount, 2);
  assert.equal(batch.packages[0].venue.venueName, "Nolita Hall");
  assert.equal(batch.packages[1].venue.venueName, "Nolita House");
  assert.ok(
    batch.sharedDomainVenues.some(
      (s) =>
        s.domain === "nolitahall.com" &&
        s.venueNames.includes("Nolita Hall") &&
        s.venueNames.includes("Nolita House")
    )
  );
  ok("shared_domain_distinct_venues");
}

// 3. Missing evidence → review / insufficient (not silently coerce)
{
  const bad = adaptPeCandidateToCanonicalPackage({
    hotel: { hotelId: "recnPB3Q02NO8Ma1o" },
    venue: { name: "Vague Venue", website: "https://example.com" },
    signal: {},
    contact: {},
    publish: true,
  });
  assert.notEqual(bad.status, PE_ADAPTER_STATUS.CANONICAL_READY);
  assert.equal(bad.package, null);
  ok("malformed_enters_review_not_coerced");
}

// 4. Loader adapts stored nested pe-venue-packages.json (optional local fixture)
{
  if (!fs.existsSync(PE_RAW)) {
    ok("loader_adapts_stored_nested_packages skipped (no local PE fixture)");
  } else {
    const pkgs = loadVenuePartnershipPackagesFromFile(PE_RAW, {
      hotelId: "recnPB3Q02NO8Ma1o",
    });
    assert.ok(Array.isArray(pkgs));
    assert.ok(pkgs.length >= 5, `expected ≥5 adapted packages, got ${pkgs.length}`);
    assert.ok(pkgs.every((p) => p.venue?.venueName));
    const names = pkgs.map((p) => p.venue.venueName);
    assert.ok(names.includes("deck655"));
    // Shared-domain Nolita family remains distinct if present
    const nolita = names.filter((n) => /nolita|house next door/i.test(n));
    assert.ok(nolita.length >= 2, `expected distinct Nolita venues, got ${nolita}`);
    ok("loader_adapts_stored_nested_packages");
  }
}

// 5. Wrong-market rejected
{
  const wrong = adaptPeCandidateToCanonicalPackage(
    {
      hotel: { hotelId: "rec0eTEnCqlsGJJt9" },
      venue: {
        name: "San Diego Only Venue",
        website: "https://example-sd.com",
        location: "San Diego, CA 92101",
      },
      signal: {
        eventActivityEvidence: "Events advertised.",
        lodgingOrReferralRationale: "Referral.",
      },
      contact: { phone: "619-555-0100", url: "https://example-sd.com" },
      publish: true,
      whyRelevantToHotel: "Would be wrong market for Mayflower.",
    },
    {
      hotelId: "rec0eTEnCqlsGJJt9",
      hotelConfig: { city: "Washington", displayName: "The Mayflower Hotel" },
    }
  );
  assert.equal(wrong.status, PE_ADAPTER_STATUS.REJECTED);
  assert.ok(wrong.reasons.includes("wrong_market_location"));
  ok("wrong_market_rejected");
}

console.log(`\n${passed} passed`);
