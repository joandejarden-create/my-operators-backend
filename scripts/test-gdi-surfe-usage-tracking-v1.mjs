#!/usr/bin/env node
/**
 * GDI Surfe Usage Tracking V1 — permanent regression gates (no paid Surfe calls).
 *   node scripts/test-gdi-surfe-usage-tracking-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const {
  appendSurfeLiveAccounting,
  appendSurfeLiveAccountingSafe,
  summarizeSurfeLiveAccounting,
  deriveSurfeCreditsAndCost,
  classifySurfeLookupStatus,
  FORBIDDEN_PII_KEYS,
  GDI_SURFE_USAGE_TRACKING_V1,
  beginSurfeInFlight,
  endSurfeInFlight,
} = await import("../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js");

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-surfe-usage-"));
const accountingFile = path.join(tmpRoot, "accounting.json");
const env = { GDI_SURFE_ACCOUNTING_PATH: accountingFile };

function readDoc() {
  return JSON.parse(fs.readFileSync(accountingFile, "utf8"));
}

// --- Credit semantics: miss does not invent charges ---
{
  const miss = deriveSurfeCreditsAndCost({
    channel: "both",
    emailReturned: false,
    phoneReturned: false,
  });
  assert.equal(miss.creditsCharged, 0);
  assert.equal(miss.costEstimateUsd, 0);
  assert.match(miss.costEstimateSource, /no_charge|miss/i);
}

{
  const hit = deriveSurfeCreditsAndCost({
    channel: "email",
    emailReturned: true,
    phoneReturned: false,
    creditsBefore: { totalEmail: 10, totalMobile: 5 },
    creditsAfter: { totalEmail: 9, totalMobile: 5 },
  });
  assert.equal(hit.creditsCharged, 1);
  assert.equal(hit.costEstimateSource, "balance_delta");
  assert.ok(hit.costEstimateUsd > 0);
}

assert.equal(
  classifySurfeLookupStatus({ emailReturned: true, phoneReturned: true }),
  "success",
);
assert.equal(
  classifySurfeLookupStatus({ emailReturned: true, phoneReturned: false }),
  "partial",
);
assert.equal(classifySurfeLookupStatus({ miss: true }), "no_match");
assert.equal(classifySurfeLookupStatus({ error: "TIMEOUT" }), "error");

// --- Persist non-PII only; strip emails/phones/raw ---
{
  const r = appendSurfeLiveAccounting(
    {
      hotelId: "hotel_a",
      opportunityId: "opp_1",
      identityKey: "idkey_a",
      channel: "both",
      lookupType: "both",
      emailReturned: true,
      phoneReturned: false,
      ok: true,
      email: "secret@example.com",
      phone: "+15551234567",
      contactDetails: { email: "secret@example.com" },
      raw: { payload: "LEAK" },
      apiKey: "sk-leak",
      personName: "Jane Doe",
      creditsBefore: { totalEmail: 3, totalMobile: 2 },
      creditsAfter: { totalEmail: 2, totalMobile: 2 },
      providerLatencyMs: 120,
      userId: "member_abc",
      portfolioId: "port_1",
      enrichmentId: "enr_unique_1",
      surfeAttempted: true,
    },
    env,
  );
  assert.equal(r.deduplicated, false);
  const entry = r.entry;
  assert.equal(entry.schema, GDI_SURFE_USAGE_TRACKING_V1);
  assert.ok(entry.lookupEventId);
  assert.equal(entry.emailFound, true);
  assert.equal(entry.phoneFound, false);
  assert.equal(entry.creditsCharged, 1);
  assert.ok(entry.requestingUserHash);
  assert.equal(entry.requestingUserHash.includes("member"), false);
  for (const k of FORBIDDEN_PII_KEYS) {
    assert.equal(k in entry, false, `forbidden key leaked: ${k}`);
  }
  const blob = JSON.stringify(entry);
  assert.equal(blob.includes("secret@example.com"), false);
  assert.equal(blob.includes("+15551234567"), false);
  assert.equal(blob.includes("sk-leak"), false);
  assert.equal(blob.includes("Jane Doe"), false);
}

// --- Dedup enrichmentId ---
{
  const r2 = appendSurfeLiveAccounting(
    {
      hotelId: "hotel_a",
      opportunityId: "opp_1",
      identityKey: "idkey_a",
      channel: "both",
      enrichmentId: "enr_unique_1",
      emailReturned: true,
      surfeAttempted: true,
    },
    env,
  );
  assert.equal(r2.deduplicated, true);
  assert.equal(readDoc().entries.length, 1);
}

// --- Safe wrapper never throws ---
{
  const badEnv = { GDI_SURFE_ACCOUNTING_PATH: path.join(tmpRoot, "no-such", "x.json") };
  // Make parent a file so mkdir/write fails
  const blocker = path.join(tmpRoot, "no-such");
  fs.writeFileSync(blocker, "not-a-dir");
  const safe = appendSurfeLiveAccountingSafe(
    { hotelId: "h", channel: "email", emailReturned: true },
    badEnv,
  );
  assert.equal(safe.error, "ACCOUNTING_WRITE_FAILED");
}

// --- In-flight dedupe ---
{
  const k = "flight_test_key";
  assert.equal(beginSurfeInFlight(k).ok, true);
  assert.equal(beginSurfeInFlight(k).error, "LOOKUP_ALREADY_IN_PROGRESS");
  endSurfeInFlight(k);
  assert.equal(beginSurfeInFlight(k).ok, true);
  endSurfeInFlight(k);
}

// --- Second event + summarize KPIs ---
{
  appendSurfeLiveAccounting(
    {
      hotelId: "hotel_b",
      opportunityId: "opp_2",
      identityKey: "idkey_b",
      channel: "phone",
      lookupType: "phone",
      miss: true,
      ok: false,
      emailReturned: false,
      phoneReturned: false,
      portfolioId: "port_2",
      enrichmentId: "enr_unique_2",
      surfeAttempted: true,
      at: new Date().toISOString(),
    },
    env,
  );
  const summary = summarizeSurfeLiveAccounting(
    { hotelNames: { hotel_a: "Hotel A", hotel_b: "Hotel B" } },
    env,
  );
  assert.equal(summary.totals.totalLookups, 2);
  assert.equal(summary.totals.successfulLookups + summary.totals.partialLookups, 1);
  assert.equal(summary.totals.noMatchLookups, 1);
  assert.ok(summary.totals.successRate >= 0);
  assert.ok(Array.isArray(summary.trends.daily));
  assert.ok(summary.hotels.length >= 2);
  assert.ok(summary.trackingStartedNote);
  const dump = JSON.stringify(summary);
  assert.equal(dump.includes("@"), false);
  assert.equal(dump.includes("secret"), false);
}

// --- Admin API + UI + server wiring (structural) ---
{
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.match(server, /\/api\/admin\/surfe-usage\/summary/);
  assert.match(server, /\/api\/admin\/surfe-usage\/events/);
  assert.match(server, /getAdminSurfeUsageSummary/);
  assert.match(server, /adminAuth/);

  const appJs = fs.readFileSync(path.join(ROOT, "public/app.js"), "utf8");
  assert.match(appJs, /\/admin\/surfe-usage/);
  assert.match(appJs, /Surfe Usage/);
  assert.match(appJs, /adminResources/);

  const html = fs.readFileSync(
    path.join(ROOT, "public/app/admin/surfe-usage.html"),
    "utf8",
  );
  assert.match(html, /admin-surfe-usage\.js/);
  assert.match(html, /support-admin-gate/);
  assert.match(html, /asuKpis/);

  const css = fs.readFileSync(
    path.join(ROOT, "public/css/admin-surfe-usage.css"),
    "utf8",
  );
  assert.match(css, /\.asu-page/);

  const client = fs.readFileSync(
    path.join(ROOT, "public/js/admin-surfe-usage.js"),
    "utf8",
  );
  assert.match(client, /\/api\/admin\/surfe-usage\/summary/);
  assert.doesNotMatch(client, /contactDetails/);
  assert.doesNotMatch(client, /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);

  const api = fs.readFileSync(path.join(ROOT, "api/admin-surfe-usage.js"), "utf8");
  assert.match(api, /includesPersonalContactDetails:\s*false/);
  assert.match(api, /summarizeSurfeLiveAccounting/);

  const gdiApi = fs.readFileSync(
    path.join(ROOT, "api/group-demand-intelligence.js"),
    "utf8",
  );
  assert.match(gdiApi, /appendSurfeLiveAccountingSafe/);
  assert.match(gdiApi, /providerLatencyMs/);
  // Must not write Surfe PII into accounting from reveal path
  assert.equal(/appendSurfeLiveAccountingSafe\(\{[\s\S]*?email:\s*result/.test(gdiApi), false);
}

console.log("PASS test-gdi-surfe-usage-tracking-v1");
