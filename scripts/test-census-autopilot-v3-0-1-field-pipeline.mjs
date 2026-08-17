/**
 * V3.0.1 writer contract + claim-level rights regression tests.
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  resolveBestEligibleClaim,
  assertOfficialBeatsBlockedSerpApi,
  mergeClaimStores,
  createClaimStore,
  upsertClaim,
} from "../lib/research-engine-v2/census-autopilot-v3/claim-store.js";
import {
  classifyFieldWrites,
  WRITER_CONTRACT_FIELDS,
} from "../lib/research-engine-v2/census-autopilot-v3/dry-run.js";
import { resolveDealalityGeography } from "../lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js";
import { WRITE_CLASS } from "../lib/research-engine-v2/census-autopilot-v3/constants.js";
import { resolveStateRegion } from "../lib/research-engine-v2/census-autopilot-v3/state-region-pipeline.js";

test("official Latitude beats blocked SerpApi claim", () => {
  const r = assertOfficialBeatsBlockedSerpApi("Latitude", 20.1, 20.2);
  assert.equal(r.pass, true);
});

test("official Longitude / Phone / Address beat SerpApi", () => {
  for (const [f, o, s] of [
    ["Longitude", -86.8, -86.7],
    ["Phone", "+52999111", "+52111222"],
    ["Address", "Av Oficial 1", "Serp Street"],
  ]) {
    assert.equal(assertOfficialBeatsBlockedSerpApi(f, o, s).pass, true, f);
  }
});

test("mergeClaimStores does not erase prior verified claims", () => {
  const a = createClaimStore();
  upsertClaim(a, "pid1", "Address", {
    value: "Calle A",
    source_type: "official_property_page",
    confidence: "High",
  });
  const b = createClaimStore();
  upsertClaim(b, "pid1", "City", {
    value: "Cancún",
    source_type: "official_brand_directory",
    confidence: "High",
  });
  const m = mergeClaimStores(a, b);
  assert.equal(m.properties.pid1.Address[0].value, "Calle A");
  assert.equal(m.properties.pid1.City[0].value, "Cancún");
});

test("State / Region resolves Cancún → Quintana Roo", () => {
  const r = resolveStateRegion({ country: "Mexico", city: "Cancún" });
  assert.equal(r.ok, true);
  assert.equal(r.normalized_state_region, "Quintana Roo");
});

test("writer contract: eligible Golden fields reach proposed writes", () => {
  const geo = resolveDealalityGeography({
    name: "Test Hotel",
    country: "Mexico",
    city: "Cancún",
  });
  const pilot = {
    property_identity_key: "ind_test_x",
    name: "Test Hotel",
    brand: "Test",
    family: "Hilton",
    country: "Mexico",
    city: "Cancún",
    official_url: "https://www.hilton.com/test",
    source_type: "official_brand_directory",
    match_class: "NEW_INSERT",
    verified_state: "VERIFIED — ROOMS PENDING",
    geography: geo,
    latitude: 21.16,
    longitude: -86.85,
    address: "Blvd. Kukulcan Km 14.5",
    phone: "+529998813300",
  };
  const { proposed } = classifyFieldWrites(pilot, {}, "test_run");
  const fields = new Set(proposed.map((p) => p.field));
  for (const f of [
    "State / Region",
    "Address",
    "Submarket",
    "Latitude",
    "Longitude",
    "Phone",
  ]) {
    assert.ok(fields.has(f), `missing writer path for ${f}`);
  }
  assert.ok(WRITER_CONTRACT_FIELDS.length >= 6);
  const lat = proposed.find((p) => p.field === "Latitude");
  assert.equal(lat.write_class, WRITE_CLASS.CORROBORATED_WRITE);
  assert.equal(lat.provenance.serpapi_used, false);
});

test("SerpApi-only Latitude is blocked when no official claim", () => {
  const sel = resolveBestEligibleClaim([
    {
      value: 20.2,
      source_type: "serpapi_google_hotels",
      serpapi_used: true,
      confidence: "High",
      match_confidence: "High",
    },
  ]);
  assert.equal(sel.selected_claim, null);
  assert.ok(sel.rejected_claims_with_reason.length >= 1);
});
