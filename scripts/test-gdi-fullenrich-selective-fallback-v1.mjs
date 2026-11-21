#!/usr/bin/env node
/**
 * GDI FullEnrich selective fallback V1 — fixture tests only (no paid enrichment).
 *   node scripts/test-gdi-fullenrich-selective-fallback-v1.mjs
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  evaluateFullEnrichFallbackEligibility,
  assertFullEnrichSpendAllowed,
  readFullEnrichFallbackFlags,
  resetFullEnrichSpendBudgetForTests,
  reserveFullEnrichSpend,
  reconcileFullEnrichSpend,
  CONTACT_PROVIDER_SURFE,
  CONTACT_PROVIDER_FULLENRICH,
} from "../lib/group-demand-intelligence/gdi-fullenrich-fallback-v1.js";
import { FE_JOB_STATUS } from "../lib/group-demand-intelligence/gdi-fullenrich-spend-ledger-v1.js";
import {
  createMockGdiFullEnrichContactRevealProvider,
} from "../lib/group-demand-intelligence/gdi-fullenrich-contact-provider-v1.js";
import {
  beginFullEnrichInFlight,
  endFullEnrichInFlight,
  putFullEnrichEphemeral,
  getFullEnrichEphemeral,
  deleteFullEnrichEphemeral,
  clearFullEnrichEphemeralStoreForTests,
  fullEnrichEphemeralKey,
} from "../lib/group-demand-intelligence/gdi-fullenrich-ephemeral-store-v1.js";
import {
  appendSurfeLiveAccounting,
  summarizeSurfeLiveAccounting,
} from "../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js";

const ROOT = process.cwd();
let failed = 0;

function check(name, fn) {
  try {
    fn();
    console.log(`PASS  ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL  ${name}`);
    console.error(err);
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    console.log(`PASS  ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL  ${name}`);
    console.error(err);
  }
}

const enabledEnv = {
  GDI_FULLENRICH_FALLBACK_ENABLED: "1",
  GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED: "1",
  GDI_FULLENRICH_CREDIT_RESERVE: "20",
  GDI_FULLENRICH_SPEND_CAP: "10",
  GDI_FULLENRICH_CUSTOMER_DISPLAY: "0",
  FULL_ENRICH_API_KEY: "fixture-key-not-real",
};

check("flags default off (no spend)", () => {
  const f = readFullEnrichFallbackFlags({});
  assert.equal(f.enabled, false);
  assert.equal(f.spendAuthorized, false);
  assert.equal(f.phoneEnabled, false);
  assert.equal(f.customerDisplay, false);
});

check("Surfe email success → no fallback", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: { email: "a@example.com", phone: null },
    channel: "email",
    surfeAttempted: true,
    surfeMiss: false,
    env: enabledEnv,
  });
  assert.equal(r.eligible, false);
  assert.equal(r.reason, "EMAIL_ALREADY_AVAILABLE");
});

check("Surfe both available → no fallback", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: { email: "a@example.com", phone: "+15551212" },
    channel: "both",
    surfeAttempted: true,
    env: enabledEnv,
  });
  assert.equal(r.eligible, false);
  assert.equal(r.reason, "CHANNELS_ALREADY_AVAILABLE");
});

check("Surfe email miss → FullEnrich email eligible", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: null,
    channel: "email",
    surfeAttempted: true,
    surfeMiss: true,
    env: enabledEnv,
  });
  assert.equal(r.eligible, true);
  assert.equal(r.channel, "email");
  assert.equal(r.reason, "SURFE_EMAIL_MISS");
  assert.equal(r.autoExecute, false);
  assert.equal(r.confirmationRequired, true);
});

check("Surfe phone success, email miss → email fallback eligible", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: { phone: "+15551212" },
    channel: "both",
    surfeAttempted: true,
    surfeMiss: false,
    env: enabledEnv,
  });
  assert.equal(r.eligible, true);
  assert.equal(r.channel, "email");
  assert.match(r.reason, /SURFE_EMAIL_MISS|EMAIL_MISSING/);
});

check("Surfe complete miss → email fallback eligible (prioritize email)", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: null,
    channel: "both",
    surfeAttempted: true,
    surfeMiss: true,
    env: enabledEnv,
  });
  assert.equal(r.eligible, true);
  assert.equal(r.channel, "email");
});

check("phone-only gap → phone fallback disabled V1", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: { email: "a@example.com" },
    channel: "phone",
    surfeAttempted: true,
    env: enabledEnv,
  });
  assert.equal(r.eligible, false);
  assert.equal(r.reason, "PHONE_FALLBACK_DISABLED_V1");
});

check("feature flag off → not eligible", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: null,
    surfeAttempted: true,
    surfeMiss: true,
    env: { ...enabledEnv, GDI_FULLENRICH_FALLBACK_ENABLED: "0" },
  });
  assert.equal(r.eligible, false);
  assert.equal(r.reason, "FEATURE_FLAG_OFF");
});

check("surfe not attempted → not eligible", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: null,
    surfeAttempted: false,
    env: enabledEnv,
  });
  assert.equal(r.eligible, false);
  assert.equal(r.reason, "SURFE_NOT_ATTEMPTED");
});

check("spend gate: insufficient reserve", () => {
  const dir = mkdtempSync(join(tmpdir(), "gdi-fe-gate-"));
  const env = { ...enabledEnv, GDI_FULLENRICH_SPEND_DIR: dir, GDI_FULLENRICH_SPEND_CAP: "10" };
  try {
    resetFullEnrichSpendBudgetForTests(env);
    const g = assertFullEnrichSpendAllowed({
      flags: readFullEnrichFallbackFlags(env),
      balance: 20,
      env,
    });
    assert.equal(g.ok, false);
    assert.equal(g.error, "INSUFFICIENT_CREDITS_RESERVE");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

check("spend gate: spend not authorized", () => {
  const env = { ...enabledEnv, GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED: "0" };
  const g = assertFullEnrichSpendAllowed({
    flags: readFullEnrichFallbackFlags(env),
    balance: 36,
    env,
  });
  assert.equal(g.ok, false);
  assert.equal(g.error, "SPEND_NOT_AUTHORIZED");
});

check("spend gate: durable spend cap reached", () => {
  const dir = mkdtempSync(join(tmpdir(), "gdi-fe-cap-"));
  const env = { ...enabledEnv, GDI_FULLENRICH_SPEND_DIR: dir, GDI_FULLENRICH_SPEND_CAP: "1" };
  try {
    resetFullEnrichSpendBudgetForTests(env);
    const r = reserveFullEnrichSpend(
      { clientRequestId: "cap1", creditsReserved: 1 },
      env,
    );
    reconcileFullEnrichSpend(
      {
        jobId: r.jobId,
        providerStatus: FE_JOB_STATUS.FINISHED,
        creditCertainty: "estimated",
        creditsCharged: 1,
        incrementalEmailFound: true,
      },
      env,
    );
    const g = assertFullEnrichSpendAllowed({
      flags: readFullEnrichFallbackFlags(env),
      balance: 36,
      env,
    });
    assert.equal(g.ok, false);
    assert.equal(g.error, "SPEND_CAP_REACHED");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

check("spend gate: ok with balance above reserve", () => {
  const dir = mkdtempSync(join(tmpdir(), "gdi-fe-ok-"));
  const env = { ...enabledEnv, GDI_FULLENRICH_SPEND_DIR: dir, GDI_FULLENRICH_SPEND_CAP: "10" };
  try {
    resetFullEnrichSpendBudgetForTests(env);
    const g = assertFullEnrichSpendAllowed({
      flags: readFullEnrichFallbackFlags(env),
      balance: 36,
      env,
    });
    assert.equal(g.ok, true);
    assert.equal(g.balance, 36);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

await checkAsync("FullEnrich mock success", async () => {
  const p = createMockGdiFullEnrichContactRevealProvider("hit");
  const r = await p.reveal({ channel: "email", contact: { name: "Ada" } });
  assert.equal(r.ok, true);
  assert.equal(r.contactDetails?.email, "recovered.buyer@fixture.example");
  assert.equal(r.contactProvider, CONTACT_PROVIDER_FULLENRICH);
  assert.equal(r.incrementalEmailFound, true);
});

await checkAsync("FullEnrich mock no match", async () => {
  const p = createMockGdiFullEnrichContactRevealProvider("miss");
  const r = await p.reveal({});
  assert.equal(r.ok, true);
  assert.equal(r.miss, true);
  assert.equal(r.contactDetails, null);
});

await checkAsync("FullEnrich mock timeout", async () => {
  const p = createMockGdiFullEnrichContactRevealProvider("timeout");
  const r = await p.reveal({});
  assert.equal(r.pending, true);
  assert.equal(r.error, "FULLENRICH_TIMEOUT");
});

await checkAsync("FullEnrich mock insufficient credits", async () => {
  const p = createMockGdiFullEnrichContactRevealProvider("insufficient");
  const r = await p.reveal({});
  assert.equal(r.error, "INSUFFICIENT_CREDITS");
});

await checkAsync("FullEnrich mock provider error", async () => {
  const p = createMockGdiFullEnrichContactRevealProvider("error");
  await assert.rejects(() => p.reveal({}), /mock_fullenrich_down/);
});

check("duplicate in-flight protection", () => {
  clearFullEnrichEphemeralStoreForTests();
  const key = fullEnrichEphemeralKey({
    hotelId: "h1",
    opportunityId: "o1",
    contactName: "Ada Lovelace",
    channel: "email",
  });
  const a = beginFullEnrichInFlight(key);
  assert.equal(a.ok, true);
  const b = beginFullEnrichInFlight(key);
  assert.equal(b.ok, false);
  assert.equal(b.error, "LOOKUP_IN_FLIGHT");
  endFullEnrichInFlight(key);
  const c = beginFullEnrichInFlight(key);
  assert.equal(c.ok, true);
  endFullEnrichInFlight(key);
});

check("ephemeral store TTL + delete (no persistent PII)", () => {
  clearFullEnrichEphemeralStoreForTests();
  const key = fullEnrichEphemeralKey({
    hotelId: "h1",
    opportunityId: "o1",
    contactName: "Ada",
  });
  putFullEnrichEphemeral(key, { contactDetails: { email: "x@y.z" } }, 60_000);
  const got = getFullEnrichEphemeral(key, { consume: false });
  assert.equal(got.contactDetails.email, "x@y.z");
  deleteFullEnrichEphemeral(key);
  assert.equal(getFullEnrichEphemeral(key), null);
});

await checkAsync("phone fallback hard-disabled on live adapter", async () => {
  const { createGdiFullEnrichContactRevealProvider } = await import(
    "../lib/group-demand-intelligence/gdi-fullenrich-contact-provider-v1.js"
  );
  const live = createGdiFullEnrichContactRevealProvider({});
  const phone = await live.reveal({ channel: "phone", contact: { name: "Ada" } });
  assert.equal(phone.error, "PHONE_FALLBACK_DISABLED_V1");
});

check("non-PII usage accounting distinguishes SURFE vs FULLENRICH", () => {
  const dir = mkdtempSync(join(tmpdir(), "gdi-fe-acct-"));
  const env = {
    GDI_SURFE_ACCOUNTING_DIR: dir,
    NODE_ENV: "test",
  };
  try {
    appendSurfeLiveAccounting(
      {
        hotelId: "hotel_fe_test",
        opportunityId: "opp_fe_test",
        identityKey: "hotel_fe_test|opp_fe_test|ada|email",
        channel: "email",
        channelRequested: "email",
        ok: true,
        miss: false,
        emailReturned: true,
        phoneReturned: false,
        surfeAttempted: true,
        fullenrichAttempted: false,
        contactProvider: CONTACT_PROVIDER_SURFE,
        surface: "gdi_drawer",
        usageClass: "FIXTURE_TEST",
        dataEnvironment: "DEVELOPMENT",
      },
      env,
    );
    appendSurfeLiveAccounting(
      {
        hotelId: "hotel_fe_test",
        opportunityId: "opp_fe_test",
        identityKey: "hotel_fe_test|opp_fe_test|ada|email|fe",
        channel: "email",
        channelRequested: "email",
        ok: true,
        miss: false,
        emailReturned: false,
        phoneReturned: false,
        surfeAttempted: false,
        fullenrichAttempted: true,
        contactProvider: CONTACT_PROVIDER_FULLENRICH,
        fallbackReason: "SURFE_EMAIL_MISS",
        incrementalEmailFound: true,
        providerCreditsCharged: 1,
        surface: "gdi_drawer",
        usageClass: "FIXTURE_TEST",
        dataEnvironment: "DEVELOPMENT",
      },
      env,
    );
    const summary = summarizeSurfeLiveAccounting({}, env);
    assert.ok(summary.totals.surfeLookupCount >= 1);
    assert.ok(summary.totals.fullenrichLookupCount >= 1);
    assert.ok(summary.totals.fullenrichIncrementalEmailCount >= 1);
    // Surfe credit pools must not absorb FE credits; ledger must stay non-PII
    const raw = readFileSync(join(dir, "gdi-live-accounting-v1.json"), "utf8");
    assert.equal(/@y\.z|recovered\.buyer|fixture\.example/i.test(raw), false);
    assert.match(raw, /FULLENRICH/);
    assert.match(raw, /SURFE/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

check("client wires FullEnrich confirm CTA (no auto-execute)", () => {
  const src = readFileSync(
    join(ROOT, "public/js/group-demand-intelligence/gdi-contact-reveal-client-v1.js"),
    "utf8",
  );
  assert.match(src, /authorizeFullEnrichFallback/);
  assert.match(src, /fullEnrichFallbackOnly/);
  assert.match(src, /data-gdi-fullenrich-confirm/);
  assert.match(src, /MSG_FE_CONFIRM/);
  assert.match(src, /root\.confirm/);
  assert.equal(/authorizeFullEnrichFallback:\s*true,\s*\n\s*fullEnrichFallbackOnly:\s*false/.test(src), false);
});

check("API route imports FullEnrich fallback modules", () => {
  const api = readFileSync(join(ROOT, "api/group-demand-intelligence.js"), "utf8");
  assert.match(api, /gdi-fullenrich-fallback-v1/);
  assert.match(api, /fullEnrichFallbackOnly/);
  assert.match(api, /evaluateFullEnrichFallbackEligibility/);
  assert.match(api, /fullenrich_ephemeral_redacted|customerDisplayBlocked/);
  assert.match(api, /CONTACT_PROVIDER_FULLENRICH/);
});

check("fullenrich client reused (no duplicate HTTP client)", () => {
  const provider = readFileSync(
    join(ROOT, "lib/group-demand-intelligence/gdi-fullenrich-contact-provider-v1.js"),
    "utf8",
  );
  assert.match(provider, /from \"\.\.\/fullenrich\/client\.js\"/);
  assert.match(provider, /startFullEnrichBulkWorkEmailsOnly/);
  assert.match(provider, /pollFullEnrichBulkUntilDone/);
});

if (failed) {
  console.error(`\n${failed} test(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI FullEnrich selective fallback V1 fixture tests passed.");
