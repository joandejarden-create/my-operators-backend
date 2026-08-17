import test from "node:test";
import assert from "node:assert/strict";
import {
  extractPostalFromAddress,
  normalizePostalCode,
  isPostalPlaceholder,
  isValidPostalForCountry,
  comparePostalIdentitySignal,
  applyPostalToIdentityScore,
} from "../lib/research-engine-v2/census-postal-code-v1.js";
import { classifyNullFill, FILL_CLASS } from "../lib/research-engine-v2/property-fundamentals-enrichment-v1.js";

test("preserves leading zeros for Puerto Rico", () => {
  const r = extractPostalFromAddress(
    "6500 Av. Isla Verde, Carolina, 00979",
    "Puerto Rico"
  );
  assert.equal(r.ok, true);
  assert.equal(r.postal_code, "00979");
});

test("extracts Mexico 5-digit CP from address", () => {
  const r = extractPostalFromAddress(
    "Blvd. Agua Caliente 10488, Aviacion, 22014 Tijuana, B.C.",
    "Mexico"
  );
  assert.equal(r.ok, true);
  assert.equal(r.postal_code, "22014");
});

test("normalizes Brazil CEP with hyphen", () => {
  assert.equal(normalizePostalCode("01310100", "Brazil"), "01310-100");
  assert.equal(isValidPostalForCountry("01310-100", "Brazil"), true);
});

test("extracts Argentina CPA letter+digits", () => {
  const r = extractPostalFromAddress(
    "Sarmiento 43, M5509 Luján de Cuyo, Mendoza",
    "Argentina"
  );
  assert.equal(r.ok, true);
  assert.equal(r.postal_code, "M5509");
});

test("rejects placeholders", () => {
  assert.equal(isPostalPlaceholder("00000"), true);
  assert.equal(isPostalPlaceholder("N/A"), true);
  assert.equal(isPostalPlaceholder("77500"), false);
});

test("NULL_FILL does not overwrite existing postal", () => {
  const nf = classifyNullFill("77500", "99999");
  assert.equal(nf.class, FILL_CLASS.CONFLICT_REVIEW);
  assert.equal(nf.write, false);
});

test("postal identity signal boosts and mismatches", () => {
  assert.equal(comparePostalIdentitySignal("00979", "00979", "Puerto Rico"), "match");
  assert.equal(comparePostalIdentitySignal("00979", "00980", "Puerto Rico"), "mismatch");
  assert.equal(applyPostalToIdentityScore(70, "match"), 82);
  assert.equal(applyPostalToIdentityScore(70, "mismatch"), 52);
});

test("does not invent postal when absent", () => {
  const r = extractPostalFromAddress("Grand Anse Main Rd.", "Grenada");
  assert.equal(r.ok, false);
});
