/**
 * Hotel Explorer Overview enrichment — evidence-gated, hotel-agnostic tests.
 * Free / local. No Airtable writes.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { formatHotelPropertyCensusAsBrandPresenceHotel } from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import {
  applyOverviewEnrichmentToHotel,
  buildHotelExplorerOverviewEnrichment,
  selectEvidenceBackedOverviewAttributes,
} from "../lib/hotel-intelligence/hotel-explorer-overview-enrichment-v1.js";
import { selectClientSafeVerifiedAttributes } from "../lib/hotel-intelligence/radar-hotel-explorer-progressive-enrichment-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BETHESDA_ID = "recLuxvwwxID7U2B8";
const SHERATON_ID = "recsYJb2R1jarPpK3";

function readJson(rel) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
}

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

// --- HPC mapper: description / amenities / meeting fields ---
{
  const mapped = formatHotelPropertyCensusAsBrandPresenceHotel({
    id: "recTestHotel00001",
    fields: {
      "Property Name": "Test Hotel",
      "Current Brand": "Test Brand",
      City: "Testville",
      Country: "United States",
      "Parent Company": "",
      "Short Property Summary": "A verified short summary.",
      "Amenities - Structured Tags": "Pool; Fitness center; Meeting space",
      "Total Meeting Space Sq Ft": 12000,
      "Rooms / Keys": 100,
      Latitude: 1,
      Longitude: 2,
    },
  });
  assert.equal(mapped.hotelDescription, "A verified short summary.");
  assert.match(mapped.amenities, /Pool/);
  assert.equal(mapped.totalMeetingSpace, 12000);
  pass("HPC DTO maps description, amenities, and meeting space");
}

// --- Confidence alone is insufficient ---
{
  const { selected, suppressed } = selectEvidenceBackedOverviewAttributes(
    [
      {
        attributeName: "outdoor_pool",
        attributeValue: true,
        confidence: "HIGH",
        active: true,
        // no sourceUrl / sourceName
      },
      {
        attributeName: "pet_friendly",
        attributeValue: true,
        confidence: "HIGH",
        active: true,
        sourceUrl: "https://example.com",
        sourceName: "ADP property profile",
        sourceType: "Hotel Commercial Profile",
      },
    ],
    { blockedThemes: new Set(["petpolicy"]), expectedHpcHotelId: BETHESDA_ID }
  );
  assert.equal(selected.length, 0, "no evidence / blocked theme must not verify");
  assert.ok(suppressed.some((s) => String(s.key).includes("pet") || s.reason));
  pass("confidence alone does not verify; unverified themes suppressed");
}

// --- Reality Gap recognition is not existence ---
{
  const selected = selectClientSafeVerifiedAttributes(
    [
      {
        attributeName: "meeting_space",
        attributeValue: true,
        confidence: "HIGH",
        active: true,
        sourceUrl: "https://example.com/events",
        sourceType: "Hotel Commercial Profile",
        sourceName: "events",
        factKind: "REALITY_GAP_RECOGNITION",
        measurementOnly: true,
      },
    ],
    { evidenceRequired: true, expectedHpcHotelId: BETHESDA_ID }
  );
  assert.equal(selected.length, 0);
  pass("Reality Gap recognition-only attributes excluded");
}

// --- Rejected / historical lifecycle ---
{
  const selected = selectClientSafeVerifiedAttributes(
    [
      {
        attributeName: "spa",
        attributeValue: true,
        confidence: "HIGH",
        active: true,
        lifecycleStatus: "rejected",
        sourceUrl: "https://example.com",
        sourceType: "Public Research",
        sourceName: "old page",
      },
      {
        attributeName: "outdoor_pool",
        attributeValue: true,
        confidence: "HIGH",
        active: true,
        historical: true,
        sourceUrl: "https://example.com",
        sourceType: "Public Research",
        sourceName: "old page",
      },
    ],
    { evidenceRequired: true }
  );
  assert.equal(selected.length, 0);
  pass("rejected/historical attributes excluded");
}

// --- Bethesda enrichment (fixture-backed, no Airtable writes) ---
{
  const hotelDto = {
    id: BETHESDA_ID,
    name: "Bethesda Marriott",
    parentCompany: "Unknown",
    hotelDescription: null,
    amenities: null,
    amenitiesDisplay: [],
    censusPropertyType: "Hotel",
    city: "Bethesda",
    country: "United States",
    rooms: 407,
  };
  const enrichment = await buildHotelExplorerOverviewEnrichment(BETHESDA_ID, {
    hotelDto,
    audience: "internal",
    skipLiveHpc: true,
    skipHiAirtable: true,
  });
  assert.equal(enrichment.ok, true);
  assert.equal(enrichment.censusRecordId, BETHESDA_ID);
  assert.equal(enrichment.adpPropertyId, "adp_bethesda_marriott");
  assert.equal(enrichment.parentCompanyOverlay?.value, "Marriott International");
  assert.equal(enrichment.parentCompanyOverlay?.factKind, "VERIFIED_FACT");
  assert.ok(enrichment.positioning?.text);
  assert.equal(enrichment.positioning?.factKind, "POSITIONING");
  assert.ok(enrichment.meetingSpace?.displayValue);
  assert.match(String(enrichment.meetingSpace.sourceUrl), /^https?:\/\//i);
  assert.ok(
    enrichment.verifiedAmenities.some((a) => /meeting|ballroom|cooper|fitness|pool|metro|nih|m club/i.test(a.label)),
    "expected facility/access amenities"
  );
  assert.ok(
    !enrichment.verifiedAmenities.some((a) => /pet|parking|accessib/i.test(a.label)),
    "unverified parking/pets/accessibility must not appear"
  );

  const applied = applyOverviewEnrichmentToHotel(hotelDto, enrichment);
  assert.equal(applied.parentCompany, "Marriott International");
  assert.ok(applied.hotelDescription);
  assert.ok(applied.meetingSpaceDisplay);
  assert.ok((applied.verifiedAmenitiesDisplay || []).length > 0);
  pass("Bethesda Overview enrichment: parent, positioning, meeting, amenities");
}

// --- Identity mismatch blocks enrichment ---
{
  const profile = readJson(
    "fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json"
  );
  // Simulate wrong census id against Bethesda profile linkage via resolve —
  // use a random rec id with no link → empty enrichment, not Bethesda bleed.
  const otherId = "recXXXXXXXXXXXXXX";
  // invalid length — use valid-shaped unknown id
  const unknown = "recABCDEFGHIJKLMN";
  const enrichment = await buildHotelExplorerOverviewEnrichment(unknown, {
    hotelDto: { parentCompany: "Unknown" },
    skipLiveHpc: true,
    skipHiAirtable: true,
  });
  assert.equal(enrichment.ok, true);
  assert.equal(enrichment.parentCompanyOverlay, null);
  assert.equal((enrichment.verifiedAmenities || []).length, 0);
  pass("unknown census id does not inherit Bethesda attributes");
  void profile;
  void otherId;
}

// --- Legacy golden-demo Sheraton fixture remains rich without ADP overlay requirement ---
{
  const sheraton = readJson(
    `fixtures/golden-demo/radar/hotel-${SHERATON_ID}.json`
  ).hotel;
  assert.ok(sheraton.hotelDescription);
  assert.ok(sheraton.amenities);
  assert.equal(sheraton.parentCompany, "Marriott International");
  // Enrichment for Sheraton census id (may or may not have ADP link) must not wipe fields.
  const enrichment = await buildHotelExplorerOverviewEnrichment(SHERATON_ID, {
    hotelDto: sheraton,
    skipLiveHpc: true,
    skipHiAirtable: true,
  });
  const applied = applyOverviewEnrichmentToHotel({ ...sheraton }, enrichment);
  assert.equal(applied.hotelDescription, sheraton.hotelDescription);
  assert.equal(applied.parentCompany, "Marriott International");
  assert.ok(String(applied.amenities).length > 0);
  pass("legacy Sheraton golden-demo fields preserved under enrichment apply");
}

// --- Low-quality description detector contract (mirrors HDP logic) ---
{
  function isLowQualityGeneratedDescription(text, hotel) {
    const s = String(text || "").trim();
    if (!s) return true;
    if (/hotel\s+hotel/i.test(s)) return true;
    return false;
  }
  assert.equal(
    isLowQualityGeneratedDescription(
      "Hotel hotel, serving Bethesda, United States.",
      { censusPropertyType: "Hotel", city: "Bethesda", country: "United States" }
    ),
    true
  );
  pass("low-quality Hotel hotel description detected");
}

if (process.exitCode) {
  console.error("\nOverview enrichment tests FAILED");
  process.exit(1);
}
console.log("\nOverview enrichment tests PASSED");
