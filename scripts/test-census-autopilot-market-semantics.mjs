/**
 * Market ≠ Country + no country fallback regressions.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { resolveDealalityMarket } from "../lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js";
import {
  resolveDealalityMarketStrict,
  assertMarketWriteGate,
  assertSubmarketWriteGate,
  classifyProductionMarket,
  MARKET_CLASS,
  isSingleMarketCountry,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import { resolveCanonicalGeography } from "../lib/research-engine-v2/census-autopilot-v3/geography/canonical-geography.js";

test("Cancún resolves to Cancún / Riviera Maya — not Mexico", () => {
  assert.equal(resolveDealalityMarket("Mexico", "Cancún"), "Cancún / Riviera Maya");
  assert.notEqual(resolveDealalityMarket("Mexico", "Cancún"), "Mexico");
});

test("Unknown Mexico city does NOT fall back to Country", () => {
  assert.equal(resolveDealalityMarket("Mexico", "UnknownTownXYZ123"), null);
  assert.equal(resolveDealalityMarketStrict("Mexico", "UnknownTownXYZ123").ok, false);
});

test("Country-as-Market fails semantic gate for Mexico", () => {
  const g = assertMarketWriteGate({
    country: "Mexico",
    market: "Mexico",
    city: "Cancún",
  });
  assert.equal(g.pass, false);
  assert.ok(g.failures.includes("COUNTRY_AS_MARKET_FORBIDDEN"));
});

test("Barbados single-market allowlist may equal Country", () => {
  assert.equal(isSingleMarketCountry("Barbados"), true);
  assert.equal(resolveDealalityMarket("Barbados", "Bridgetown"), "Barbados");
  const cls = classifyProductionMarket({
    country: "Barbados",
    market: "Barbados",
    city: "Bridgetown",
  });
  assert.equal(cls.class, MARKET_CLASS.VALID_MARKET);
});

test("Submarket write blocked when Market is Country-as-Market (Mexico)", () => {
  const g = assertSubmarketWriteGate({
    country: "Mexico",
    market: "Mexico",
    submarket: "Tulum",
    status: "MATCHED",
  });
  assert.equal(g.write_allowed, false);
});

test("canonical geography does not set Market = Country for Mexico secondary city", () => {
  const geo = resolveCanonicalGeography({
    country: "Mexico",
    city: "UnknownTownXYZ123",
  });
  assert.notEqual(geo.market, "Mexico");
  assert.equal(geo.market == null || geo.market !== "Mexico", true);
});

test("São Paulo → São Paulo market", () => {
  assert.equal(resolveDealalityMarket("Brazil", "Sao Paulo"), "São Paulo");
});

test("explicit State→Market when city Unknown (Brazil São Paulo)", () => {
  const r = resolveDealalityMarketStrict("Brazil", "Unknown", {
    state: "São Paulo",
  });
  assert.equal(r.ok, true);
  assert.equal(r.market, "São Paulo");
  assert.equal(r.method, "explicit_state_to_market_registry");
});

test("coordinate centroid may resolve weak city; never Country", () => {
  const r = resolveDealalityMarketStrict("Mexico", "Unknown", {
    latitude: 19.43,
    longitude: -99.13,
  });
  assert.equal(r.ok, true);
  assert.equal(r.market, "Mexico City");
  assert.equal(r.method, "coordinate_market_centroid");
  assert.notEqual(r.market, "Mexico");
});

test("Jalisco state alone does NOT auto-map (ambiguous GDL vs PV)", () => {
  const r = resolveDealalityMarketStrict("Mexico", "Unknown", { state: "Jalisco" });
  assert.equal(r.ok, false);
});
