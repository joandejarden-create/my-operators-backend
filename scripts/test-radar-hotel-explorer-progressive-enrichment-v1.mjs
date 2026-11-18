#!/usr/bin/env node
/**
 * Progressive enrichment contracts for Radar ↔ Hotel Explorer Phase 1.
 * Free / local — no paid APIs, no Airtable writes, no visibility changes.
 */
import assert from "node:assert/strict";
import {
  PRESENTATION_COMPLETENESS,
  resolveHotelCensusIdentity,
  selectClientSafeVerifiedAttributes,
  resolveFieldPrecedence,
  describeSourceConflict,
  evaluatePresentationCompleteness,
  isInternalHotelExplorerPreviewEligible,
  isBethesdaMarriottCensusId,
  RADAR_HE_PROGRESSIVE_ENRICHMENT_VERSION,
} from "../lib/hotel-intelligence/radar-hotel-explorer-progressive-enrichment-v1.js";

const BETHESDA = "recLuxvwwxID7U2B8";
const GOLDEN = "recUNycnMwOVFX0hc"; // KGPV

// --- Identity ---
{
  const ok = resolveHotelCensusIdentity({ id: BETHESDA, name: "Bethesda Marriott" });
  assert.equal(ok.ok, true);
  assert.equal(ok.censusRecordId, BETHESDA);

  const byName = resolveHotelCensusIdentity({ name: "Bethesda Marriott" });
  assert.equal(byName.ok, false);
  assert.equal(byName.reason, "name_alone_insufficient");

  const missing = resolveHotelCensusIdentity(null);
  assert.equal(missing.ok, false);
  console.log("PASS identity: census rec id required; name alone insufficient");
}

// --- Baseline-only hotel ---
{
  const hotel = { id: GOLDEN, name: "Krystal Grand", rooms: 500, market: "Puerto Vallarta" };
  const eval_ = evaluatePresentationCompleteness({ hotel, audience: "internal" });
  assert.equal(eval_.level, PRESENTATION_COMPLETENESS.RADAR_BASELINE);
  assert.equal(eval_.radarBaselineAvailable, true);
  assert.equal(eval_.customerSafeEligible, false);
  assert.equal(isInternalHotelExplorerPreviewEligible(GOLDEN), true);
  console.log("PASS baseline-only hotel → RADAR_BASELINE");
}

// --- Enriched hotel ---
{
  const hotel = { id: BETHESDA, name: "Bethesda Marriott" };
  const attrs = [
    {
      attributeKey: "rooms",
      attributeName: "Rooms",
      attributeValue: 407,
      confidence: "HIGH",
      active: true,
    },
  ];
  const eval_ = evaluatePresentationCompleteness({
    hotel,
    verifiedAttributes: attrs,
    audience: "internal",
  });
  assert.equal(eval_.level, PRESENTATION_COMPLETENESS.ENRICHED);
  assert.equal(eval_.verifiedAttributeCount, 1);
  assert.equal(isBethesdaMarriottCensusId(BETHESDA), true);
  console.log("PASS enriched hotel → ENRICHED (Bethesda internal eligible)");
}

// --- Full explorer hotel ---
{
  const hotel = { id: GOLDEN, name: "KGPV" };
  const eval_ = evaluatePresentationCompleteness({
    hotel,
    verifiedAttributes: [
      { attributeName: "Rooms", attributeValue: 250, confidence: "HIGH", active: true },
    ],
    ownership: { ok: true, propCo: "Demo PropCo" },
    hiSectionsComplete: true,
    audience: "internal",
  });
  assert.equal(eval_.level, PRESENTATION_COMPLETENESS.FULL);
  console.log("PASS full hotel → FULL");
}

// --- Unresolved identity ---
{
  const eval_ = evaluatePresentationCompleteness({
    hotel: { name: "Mystery Hotel", lat: 1, lng: 2 },
    verifiedAttributes: [
      { attributeName: "Rooms", attributeValue: 100, confidence: "HIGH", active: true },
    ],
  });
  assert.equal(eval_.level, PRESENTATION_COMPLETENESS.IDENTITY_UNRESOLVED);
  assert.equal(eval_.customerSafeEligible, false);
  console.log("PASS unresolved identity blocks verified attribution");
}

// --- Conflicting sources ---
{
  const conflict = describeSourceConflict({
    fieldKey: "rooms",
    censusValue: 400,
    verifiedValue: 407,
  });
  assert.equal(conflict.conflict, true);
  assert.equal(conflict.resolution, "display_verified_with_census_provenance");

  const prec = resolveFieldPrecedence({
    fieldKey: "rooms",
    censusValue: 400,
    verifiedAttribute: {
      attributeName: "Rooms",
      attributeValue: 407,
      confidence: "HIGH",
      active: true,
    },
    audience: "internal",
  });
  assert.equal(prec.value, 407);
  assert.equal(prec.source, "verified_hotel_attribute");
  assert.equal(prec.overridden, true);

  const idPrec = resolveFieldPrecedence({
    fieldKey: "id",
    censusValue: BETHESDA,
    verifiedAttribute: {
      attributeName: "id",
      attributeValue: "recFAKE0000000000",
      confidence: "HIGH",
      active: true,
    },
  });
  assert.equal(idPrec.value, BETHESDA);
  assert.equal(idPrec.source, "hotel_census");
  console.log("PASS conflict: verified overlays rooms; census keeps identity");
}

// --- Customer visibility restrictions ---
{
  const internalOnly = selectClientSafeVerifiedAttributes(
    [
      {
        attributeName: "Win Thesis",
        attributeValue: "secret",
        confidence: "HIGH",
        active: true,
        internalOnly: true,
      },
      {
        attributeName: "Rooms",
        attributeValue: 407,
        confidence: "HIGH",
        active: true,
      },
    ],
    { audience: "share" }
  );
  assert.equal(internalOnly.length, 1);
  assert.equal(internalOnly[0].name, "Rooms");

  const discovery = selectClientSafeVerifiedAttributes(
    [
      {
        attributeName: "Meeting Sq Ft",
        attributeValue: 999,
        confidence: "LOW",
        active: true,
        sourceRole: "DISCOVERY_ONLY",
      },
    ],
    { audience: "customer" }
  );
  assert.equal(discovery.length, 0);

  const shareEval = evaluatePresentationCompleteness({
    hotel: { id: BETHESDA, name: "Bethesda Marriott" },
    verifiedAttributes: [
      { attributeName: "Rooms", attributeValue: 407, confidence: "HIGH", active: true },
    ],
    audience: "share",
  });
  // Completeness may be ENRICHED; customerSafeEligible requires share audience + safe attrs
  assert.equal(shareEval.level, PRESENTATION_COMPLETENESS.ENRICHED);
  assert.equal(shareEval.customerSafeEligible, true);
  console.log("PASS customer/share visibility strips internal-only and unverified");
}

// --- Unverified → unavailable (no invention) ---
{
  const empty = resolveFieldPrecedence({
    fieldKey: "meeting_sq_ft",
    censusValue: null,
    verifiedAttribute: {
      attributeName: "Meeting Sq Ft",
      attributeValue: 12000,
      confidence: "LOW",
      active: true,
    },
  });
  assert.equal(empty.value, null);
  assert.equal(empty.reason, "unavailable_or_unverified");
  console.log("PASS unverified attributes do not invent display values");
}

console.log(
  JSON.stringify({
    ok: true,
    version: RADAR_HE_PROGRESSIVE_ENRICHMENT_VERSION,
    gate: "test:radar-hotel-explorer-progressive-enrichment-v1",
  })
);
