import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizePhoneDigits,
  isTollFreePhone,
  isLikelyMobilePhone,
  phonesMatch,
  inferBusinessPhoneTier,
  inferMobilePhoneTier,
  resolveContactPhoneFields,
  pickPrimaryOutreachPhone,
  isVerifiedPersonPhoneTier,
} from "../lib/gtm-owner-target/registry-phone-verification.js";

test("normalizePhoneDigits strips formatting", () => {
  assert.equal(normalizePhoneDigits("+52 55 5249 8050"), "525552498050");
});

test("isTollFreePhone detects US and MX toll-free", () => {
  assert.equal(isTollFreePhone("00 1 866-211-6223"), true);
  assert.equal(isTollFreePhone("+52 55 5249 8050"), false);
});

test("isLikelyMobilePhone detects Mexico mobile", () => {
  assert.equal(isLikelyMobilePhone("+52 1 55 1234 5678", "Mexico"), true);
  assert.equal(isLikelyMobilePhone("+52 55 5249 8050", "Mexico"), false);
});

test("phonesMatch compares trailing digits", () => {
  assert.equal(phonesMatch("00 52 55 5249 8050", "+52 55 5249 8050"), true);
});

test("inferBusinessPhoneTier marks entity switchboard as VP3", () => {
  assert.equal(
    inferBusinessPhoneTier("+52 55 5249 8050", {
      entitySwitchboardPhone: "+52 55 5249 8050",
      verificationUrl: "https://norte19.com",
    }),
    "VP3"
  );
});

test("inferBusinessPhoneTier without switchboard reference stays VP3", () => {
  assert.equal(
    inferBusinessPhoneTier("+52 55 5262 6214", {
      verificationUrl: "https://caminoreal.com",
      country: "Mexico",
    }),
    "VP3"
  );
});

test("inferBusinessPhoneTier marks direct line as VP1", () => {
  assert.equal(
    inferBusinessPhoneTier("+52 55 5262 6214", {
      entitySwitchboardPhone: "+52 55 5262 6262",
      verificationUrl: "https://caminoreal.com",
      country: "Mexico",
    }),
    "VP1"
  );
});

test("inferMobilePhoneTier requires mobile pattern and proof", () => {
  assert.equal(
    inferMobilePhoneTier("+52 1 55 9876 5432", {
      verificationUrl: "https://example.com/team",
      country: "Mexico",
    }),
    "VP2"
  );
  assert.equal(
    inferMobilePhoneTier("+52 55 5249 8050", {
      verificationUrl: "https://example.com",
      country: "Mexico",
    }),
    null
  );
});

test("resolveContactPhoneFields splits legacy phone and sets tiers", () => {
  const resolved = resolveContactPhoneFields({
    phone: "+52 55 5262 6214",
    verificationUrl: "https://caminoreal.com",
    entitySwitchboardPhone: "+52 55 5262 6262",
    country: "Mexico",
    name: "Leandro Trejo",
  });
  assert.equal(resolved.businessPhoneTier, "VP1");
  assert.equal(resolved.phoneVerificationTier, "VP1");
  assert.equal(isVerifiedPersonPhoneTier(resolved.phoneVerificationTier), true);
});

test("pickPrimaryOutreachPhone prefers VP2 mobile over VP1 business", () => {
  assert.equal(
    pickPrimaryOutreachPhone({
      mobilePhone: "+52 1 55 1111 2222",
      mobileTier: "VP2",
      businessPhone: "+52 55 5262 6214",
      businessTier: "VP1",
    }),
    "+52 1 55 1111 2222"
  );
});
