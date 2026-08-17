/**
 * City semantic + Marriott title city regression for incident closure.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { inferCityFromMarriottTitle } from "../lib/research-engine-v2/clean-census/marriott-mexico-discovery.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";
import {
  validateCitySemantics,
  CITY_STATUS,
} from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";

test("Adults Only marketing fragments are descriptor cities", () => {
  assert.equal(isDescriptorCity("An - Adults Only"), true);
  assert.equal(isDescriptorCity("- Adults Only"), true);
  assert.equal(validateCitySemantics("An - Adults Only", "Jamaica").status, CITY_STATUS.INVALID);
});

test("Marriott title no longer returns Adults Only as city", () => {
  const city = inferCityFromMarriottTitle(
    "Grand Lido Negril Au-Naturel, An Autograph Collection All-Inclusive Resort - Adults Only"
  );
  assert.equal(city, "Negril");
  assert.equal(validateCitySemantics(city, "Jamaica").ok, true);
});

test("canonical geography rejects descriptor city input", () => {
  const geo = resolveCanonicalGeography({
    country: "Jamaica",
    city: "An - Adults Only",
    name: "Grand Lido Negril Au-Naturel, An Autograph Collection All-Inclusive Resort - Adults Only",
  });
  assert.notEqual(geo.city, "An - Adults Only");
});

test("country-as-city rejected", () => {
  assert.equal(validateCitySemantics("Mexico", "Mexico").ok, false);
  assert.equal(validateCitySemantics("Barbados", "Barbados").ok, false);
});
