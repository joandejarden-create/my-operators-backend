#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildUniversalCompanyRegistryV24,
  listUniversalCompaniesV24,
  UNIVERSAL_WAVE_ID,
} from "../lib/hotel-intelligence/sec-edgar/cik-registry-v24.js";

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

test("registry_size_in_target_band", () => {
  const reg = buildUniversalCompanyRegistryV24();
  assert.ok(reg.companyCount >= 25, `expected >=25 got ${reg.companyCount}`);
  assert.ok(reg.companyCount <= 45, `expected <=45 got ${reg.companyCount}`);
  assert.equal(reg.waveId, UNIVERSAL_WAVE_ID);
});

test("no_duplicate_slugs_or_ciks_in_static_registry", () => {
  const reg = buildUniversalCompanyRegistryV24();
  const hardCollisions = (reg.collisions || []).filter((c) => c.type === "duplicate_cik" || c.type === "duplicate_slug");
  assert.equal(hardCollisions.length, 0, JSON.stringify(hardCollisions));
  const ciks = reg.companies.map((c) => c.cik).filter(Boolean);
  assert.equal(new Set(ciks).size, ciks.length);
});

test("not_restricted_to_marriott_hilton", () => {
  const rows = listUniversalCompaniesV24({ maxPriority: 3 });
  assert.ok(rows.some((c) => c.slug === "hyatt-hotels"));
  assert.ok(rows.some((c) => c.slug === "choice-hotels"));
  assert.ok(rows.some((c) => c.slug === "ryman-hospitality"));
  assert.ok(rows.some((c) => c.slug === "mgm-resorts"));
  assert.ok(rows.some((c) => (c.cohorts || []).includes("HISTORICAL")));
});

test("sotherly_does_not_share_ashford_cik", () => {
  const reg = buildUniversalCompanyRegistryV24();
  const aht = reg.companies.find((c) => c.slug === "ashford-hospitality-trust");
  const soho = reg.companies.find((c) => c.slug === "sotherly-hotels");
  assert.ok(aht?.cik);
  assert.ok(soho);
  assert.notEqual(soho.cik, aht.cik);
});

test("guarantees_present", () => {
  const reg = buildUniversalCompanyRegistryV24();
  assert.ok(reg.guarantees.some((g) => /not restricted to Marriott or Hilton/i.test(g)));
});

console.log(`${passed} tests passed`);
