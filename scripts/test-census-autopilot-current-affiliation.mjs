/**
 * Current Brand / affiliation gate regression tests (Choice + cross-family).
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  AFFILIATION_STATUS,
  assertNoFamilyDefaultCurrentBrand,
  evaluateCurrentAffiliationGate,
  inferChoiceBrandFromOfficialPropertyUrl,
  isParentCompanyAsCurrentBrand,
  runParentVsBrandRegressionMatrix,
  validateCurrentBrandSemantics,
  classifyBrandCorrection,
  BRAND_CORRECTION_CLASS,
} from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";
import { classifyFieldWrites } from "../lib/research-engine-v2/census-autopilot-v3/dry-run.js";
import { resolveDealalityGeography } from "../lib/research-engine-v2/census-autopilot-v2-2/geography-expansion.js";

test("Choice URL slug → Sleep Inn (not Choice)", () => {
  const brand = inferChoiceBrandFromOfficialPropertyUrl(
    "https://www.choicehotels.com/sao-paulo/guarulhos/sleep-inn-hotels/br129"
  );
  assert.equal(brand, "Sleep Inn");
});

test("Choice URL slug → Ascend Hotel Collection", () => {
  const brand = inferChoiceBrandFromOfficialPropertyUrl(
    "https://www.choicehotels.com/nuevo-leon/monterrey/ascend-hotels/mx001"
  );
  assert.equal(brand, "Ascend Hotel Collection");
});

test("Parent company labels rejected as Current Brand", () => {
  for (const v of [
    "Choice",
    "Choice Hotels",
    "Choice Hotels International",
    "Marriott International",
    "Hilton Worldwide",
    "IHG",
    "Accor",
    "Wyndham Hotels & Resorts",
    "Hyatt Hotels Corporation",
    "Minor Hotels",
  ]) {
    assert.equal(isParentCompanyAsCurrentBrand(v), true, v);
    assert.equal(validateCurrentBrandSemantics(v).ok, false, v);
  }
});

test("Property-level brands accepted", () => {
  for (const v of ["Sleep Inn", "Comfort Inn", "Radisson Blu", "Holiday Inn Express"]) {
    assert.equal(isParentCompanyAsCurrentBrand(v), false, v);
    assert.equal(validateCurrentBrandSemantics(v).ok, true, v);
  }
});

test("source_family=Choice must NOT default Current Brand to Choice", () => {
  const r = assertNoFamilyDefaultCurrentBrand("Choice", "Choice");
  assert.equal(r.pass, false);
});

test("gate: Choice URL + Exact identity → CONFIRMED Sleep Inn auto-write", () => {
  const g = evaluateCurrentAffiliationGate({
    official_property_url:
      "https://www.choicehotels.com/sao-paulo/guarulhos/sleep-inn-hotels/br129",
    family: "Choice",
    source_family: "Choice",
    brand: "Choice", // contaminated production/staging value
    identity_confidence: "High",
    match_class: "NEW_INSERT",
  });
  assert.equal(g.gate, AFFILIATION_STATUS.CONFIRMED);
  assert.equal(g.brand, "Sleep Inn");
  assert.equal(g.auto_write_allowed, true);
  assert.equal(g.parent_company, "Choice Hotels International");
});

test("gate: medium identity cannot write Current Brand", () => {
  const g = evaluateCurrentAffiliationGate({
    official_property_url:
      "https://www.choicehotels.com/sao-paulo/guarulhos/sleep-inn-hotels/br129",
    family: "Choice",
    match_confidence: "Medium",
    match_class: "PROBABLE_MATCH",
  });
  assert.equal(g.auto_write_allowed, false);
  assert.equal(g.write_policy, "staging_review_only");
});

test("gate: no property evidence → UNKNOWN leave blank (no family default)", () => {
  const g = evaluateCurrentAffiliationGate({
    brand: "Choice",
    family: "Choice",
    source_family: "Choice",
    identity_confidence: "High",
    match_class: "NEW_INSERT",
  });
  assert.equal(g.brand, null);
  assert.equal(g.gate, AFFILIATION_STATUS.UNKNOWN);
  assert.equal(g.auto_write_allowed, false);
});

test("dry-run does not propose Current Brand=Choice from family default", () => {
  const geo = resolveDealalityGeography({
    name: "Sleep Inn Guarulhos",
    country: "Brazil",
    city: "Guarulhos",
  });
  const pilot = {
    property_identity_key: "ind_choice_br_br129",
    name: "Sleep Inn Guarulhos",
    brand: "Choice",
    family: "Choice",
    country: "Brazil",
    city: "Guarulhos",
    official_url:
      "https://www.choicehotels.com/sao-paulo/guarulhos/sleep-inn-hotels/br129",
    source_type: "official_brand_directory",
    match_class: "NEW_INSERT",
    verified_state: "VERIFIED — ROOMS PENDING",
    geography: geo,
    identity_confidence: "High",
  };
  const { proposed } = classifyFieldWrites(pilot, {}, "test_run");
  const brandWrite = proposed.find((p) => p.field === "Current Brand");
  assert.ok(brandWrite, "Current Brand should be proposed from URL evidence");
  assert.equal(brandWrite.value, "Sleep Inn");
  assert.notEqual(brandWrite.value, "Choice");
});

test("dry-run leaves Current Brand blank when no property brand evidence", () => {
  const geo = resolveDealalityGeography({
    name: "Mystery Hotel",
    country: "Mexico",
    city: "Cancún",
  });
  const pilot = {
    property_identity_key: "ind_choice_mx_x",
    name: "Mystery Hotel",
    brand: "Choice",
    family: "Choice",
    country: "Mexico",
    city: "Cancún",
    official_url: "https://www.example.com/mystery",
    source_type: "official_brand_directory",
    match_class: "NEW_INSERT",
    verified_state: "VERIFIED — ROOMS PENDING",
    geography: geo,
    identity_confidence: "High",
  };
  const { proposed } = classifyFieldWrites(pilot, {}, "test_run");
  const brandWrite = proposed.find((p) => p.field === "Current Brand");
  assert.equal(brandWrite, undefined);
});

test("cross-family parent-vs-brand regression matrix passes", () => {
  const r = runParentVsBrandRegressionMatrix();
  assert.equal(r.pass, true);
});

test("SAFE_BRAND_CORRECTION when replacing Choice parent contamination", () => {
  const c = classifyBrandCorrection("Choice", "Sleep Inn");
  assert.equal(c.class, BRAND_CORRECTION_CLASS.SAFE_BRAND_CORRECTION);
});
