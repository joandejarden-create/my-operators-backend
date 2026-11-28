#!/usr/bin/env node
/**
 * Permanent regression: DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CONTRACT_ID,
  LIBRARY_VERSION,
  formatDealalityPhone,
} from "../lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports",
  "dealality-shared-international-phone-formatting-v1",
);
fs.mkdirSync(OUT, { recursive: true });

const results = [];
function check(id, fn) {
  try {
    fn();
    results.push({ id, ok: true });
  } catch (err) {
    results.push({
      id,
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

check("CONTRACT_PRESENT", () => {
  assert.equal(CONTRACT_ID, "DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1");
  assert.equal(LIBRARY_VERSION, "1.13.17");
});

check("DEALALITY_PHONE_COUNTRY_FORMAT_VALIDATED", () => {
  const cases = [
    ["+12674754267", "+1 (267) 475-4267"],
    ["+19722335600", "+1 (972) 233-5600"],
    ["+34612345678", "+34 612 34 56 78"], // library international (not invented grouping)
    ["+442079460958", "+44 20 7946 0958"],
    ["+33612345678", "+33 6 12 34 56 78"],
    ["+4915123456789", "+49 1512 3456789"],
    ["+351912345678", "+351 912 345 678"],
    ["+525512345678", "+52 55 1234 5678"],
    ["+18095550123", "+1 (809) 555-0123"], // DO NANP
    ["+14165550100", "+1 (416) 555-0100"], // CA NANP
    ["+15145550123", "+1 (514) 555-0123"], // CA Montreal
    ["+50622221234", "+506 2222 1234"],
    ["+5511987654321", "+55 11 98765 4321"],
    ["+41441234567", "+41 44 123 45 67"],
  ];
  for (const [raw, expected] of cases) {
    const r = formatDealalityPhone(raw);
    assert.equal(r.displayValue, expected, `${raw} → ${r.displayValue} ≠ ${expected}`);
    assert.equal(r.formattingOutcome, "FORMATTED");
    assert.equal(r.originalRaw, raw);
  }
});

check("DEALALITY_PHONE_E164_LINK_VALIDATED", () => {
  const r = formatDealalityPhone("+12674754267");
  assert.equal(r.e164, "+12674754267");
  assert.equal(r.telHref, "tel:+12674754267");
  const r2 = formatDealalityPhone("+19722335600");
  assert.equal(r2.telHref, "tel:+19722335600");
});

check("DEALALITY_PHONE_RAW_VALUE_PRESERVED", () => {
  const raw = "+1 (267) 475-4267";
  const r = formatDealalityPhone(raw);
  assert.equal(r.originalRaw, raw);
  assert.equal(r.e164, "+12674754267");
  // Re-format deterministic
  const rAgain = formatDealalityPhone(raw);
  assert.equal(rAgain.displayValue, r.displayValue);
});

check("DEALALITY_PHONE_INVALID_FALLBACK", () => {
  for (const bad of ["", null, undefined, "   ", "not-a-phone", "123", "+999999"]) {
    const r = formatDealalityPhone(bad);
    if (!String(bad || "").trim()) {
      assert.equal(r.formattingOutcome, "EMPTY");
      assert.equal(r.telHref, null);
    } else {
      assert.equal(r.displayValue, String(bad).trim());
      assert.notEqual(r.formattingOutcome, "FORMATTED");
      // Must not invent a dial link for garbage
      assert.equal(r.telHref, null);
    }
  }
});

check("DEALALITY_PHONE_NO_AIRTABLE_MUTATION", () => {
  // Pure function — no network / no fs writes beyond this test's report.
  const input = { phone: "+12674754267" };
  const before = JSON.stringify(input);
  formatDealalityPhone(input.phone);
  assert.equal(JSON.stringify(input), before);
});

check("DEALALITY_PHONE_EXTENSION_PRESERVED", () => {
  const r = formatDealalityPhone("+12133734253 ext. 1234");
  assert.ok(r.isValid || r.isPossible);
  assert.equal(r.extension, "1234");
  assert.match(r.displayValue, /ext\.\s*1234/i);
  assert.ok(r.telHref && r.telHref.includes("tel:+12133734253"));
  assert.ok(r.telHref.includes("ext=1234") || r.telHref.includes(";ext=1234"));
});

check("DEALALITY_PHONE_NO_HOTEL_COUNTRY_INFERENCE", () => {
  // Explicit US number must stay US even if a bogus contactCountry is passed.
  const r = formatDealalityPhone("+12674754267", { contactCountry: "PT" });
  assert.equal(r.country, "US");
  assert.equal(r.displayValue, "+1 (267) 475-4267");
  // National without + and without reliable country → preserve
  const nat = formatDealalityPhone("2674754267");
  assert.equal(nat.formattingOutcome, "PRESERVED_ORIGINAL");
  assert.equal(nat.displayValue, "2674754267");
  assert.equal(nat.telHref, null);
  // National with verified contact country only
  const withCtx = formatDealalityPhone("2674754267", { contactCountry: "US" });
  assert.equal(withCtx.displayValue, "+1 (267) 475-4267");
  assert.equal(withCtx.e164, "+12674754267");
});

check("BETHESDA_GOLDEN_REFERENCE", () => {
  const a = formatDealalityPhone("+12674754267");
  const b = formatDealalityPhone("+19722335600");
  assert.equal(a.displayValue, "+1 (267) 475-4267");
  assert.equal(b.displayValue, "+1 (972) 233-5600");
  assert.equal(a.telHref, "tel:+12674754267");
  assert.equal(b.telHref, "tel:+19722335600");
});

const failed = results.filter((r) => !r.ok);
const summary = {
  contract: CONTRACT_ID,
  libraryVersion: LIBRARY_VERSION,
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
  generatedAt: new Date().toISOString(),
};
fs.writeFileSync(
  path.join(OUT, "UNIT_RESULTS.json"),
  JSON.stringify(summary, null, 2),
);
console.log(JSON.stringify({ ok: summary.ok, passed: summary.passed, failed: summary.failed }, null, 2));
if (!summary.ok) {
  console.error(failed);
  process.exit(1);
}
