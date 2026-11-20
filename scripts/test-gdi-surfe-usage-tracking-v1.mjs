#!/usr/bin/env node
/**
 * GDI Surfe Usage Tracking V1.1 — permanent regression gates (no paid Surfe calls).
 * Covers durability, concurrency, credit certainty, idempotency, failures, privacy.
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
  recordSurfeAccountingFailure,
  summarizeSurfeLiveAccounting,
  deriveSurfeCreditsAndCost,
  classifySurfeLookupStatus,
  FORBIDDEN_PII_KEYS,
  GDI_SURFE_USAGE_TRACKING_V1,
  GDI_SURFE_USAGE_TRACKING_V1_1,
  beginSurfeInFlight,
  endSurfeInFlight,
  eventsJsonlPath,
  accountingPath,
} = await import("../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js");

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-surfe-usage-"));
const accountingFile = path.join(tmpRoot, "accounting.json");
const env = { GDI_SURFE_ACCOUNTING_PATH: accountingFile };

function readDoc() {
  return JSON.parse(fs.readFileSync(accountingFile, "utf8"));
}

// --- Credit certainty: miss is UNKNOWN (not assumed zero) ---
{
  const miss = deriveSurfeCreditsAndCost({
    channel: "both",
    emailReturned: false,
    phoneReturned: false,
  });
  assert.equal(miss.creditsCharged, null);
  assert.equal(miss.costEstimateUsd, null);
  assert.equal(miss.creditCertainty, "unknown");
  assert.equal(miss.costCertainty, "unknown");
  assert.match(miss.costEstimateSource, /unknown/i);
}

// --- Shared balance delta is ESTIMATED (never confirmed) ---
{
  const hit = deriveSurfeCreditsAndCost({
    channel: "email",
    emailReturned: true,
    phoneReturned: false,
    creditsBefore: { totalEmail: 10, totalMobile: 5 },
    creditsAfter: { totalEmail: 9, totalMobile: 5 },
  });
  assert.equal(hit.creditsCharged, 1);
  assert.equal(hit.creditCertainty, "estimated");
  assert.equal(hit.costCertainty, "estimated");
  assert.equal(hit.costEstimateSource, "shared_balance_delta_estimated");
  assert.equal(hit.attributionWarning, "SHARED_ACCOUNT_BALANCE_DELTA");
  assert.equal(hit.creditsConfirmed, null);
  assert.equal(hit.creditsEstimated, 1);
}

// --- Provider-confirmed path ---
{
  const confirmed = deriveSurfeCreditsAndCost({
    channel: "email",
    emailReturned: true,
    providerCreditsCharged: 2,
  });
  assert.equal(confirmed.creditCertainty, "confirmed");
  assert.equal(confirmed.creditsConfirmed, 2);
  assert.equal(confirmed.creditsCharged, 2);
  assert.equal(confirmed.costCertainty, "estimated");
}

// --- Success without balance: estimated ---
{
  const est = deriveSurfeCreditsAndCost({
    channel: "email",
    emailReturned: true,
    phoneReturned: false,
  });
  assert.equal(est.creditCertainty, "estimated");
  assert.equal(est.costEstimateSource, "estimate_on_success");
  assert.ok(est.creditsCharged > 0);
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
      clientRequestId: "cr_unique_1",
      surfeAttempted: true,
    },
    env,
  );
  assert.equal(r.deduplicated, false);
  const entry = r.entry;
  assert.equal(entry.schema, GDI_SURFE_USAGE_TRACKING_V1_1);
  assert.ok(entry.lookupEventId);
  assert.equal(entry.emailFound, true);
  assert.equal(entry.phoneFound, false);
  assert.equal(entry.creditsCharged, 1);
  assert.equal(entry.creditCertainty, "estimated");
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
  assert.equal(r2.dedupeReason, "enrichmentId");
  assert.equal(readDoc().entries.length, 1);
}

// --- Dedup clientRequestId (transport retry) ---
{
  const r3 = appendSurfeLiveAccounting(
    {
      hotelId: "hotel_a",
      opportunityId: "opp_1",
      clientRequestId: "cr_unique_1",
      channel: "both",
      emailReturned: true,
      surfeAttempted: true,
      enrichmentId: "enr_other",
    },
    env,
  );
  assert.equal(r3.deduplicated, true);
  assert.equal(r3.dedupeReason, "clientRequestId");
  assert.equal(readDoc().entries.length, 1);
}

// --- Legitimate later repeat for same person IS counted (no 15s identity suppress) ---
{
  const r4 = appendSurfeLiveAccounting(
    {
      hotelId: "hotel_a",
      opportunityId: "opp_1",
      identityKey: "idkey_a",
      channel: "both",
      clientRequestId: "cr_legitimate_repeat",
      enrichmentId: "enr_unique_repeat",
      emailReturned: true,
      phoneReturned: false,
      surfeAttempted: true,
    },
    env,
  );
  assert.equal(r4.deduplicated, false);
  assert.equal(readDoc().entries.length, 2);
}

// --- Safe wrapper never throws; records failure ---
{
  const failDir = path.join(tmpRoot, "fail-case");
  fs.mkdirSync(failDir, { recursive: true });
  const badFile = path.join(failDir, "blocked.json");
  // Create a directory where a file should be — rename will fail oddly;
  // use lock contention via broken parent: make accounting path a directory
  // that cannot be written as a file.
  fs.mkdirSync(badFile, { recursive: true });
  const badEnv = {
    GDI_SURFE_ACCOUNTING_PATH: badFile,
    GDI_SURFE_ACCOUNTING_FAILURES_PATH: path.join(failDir, "failures.jsonl"),
  };
  const safe = appendSurfeLiveAccountingSafe(
    {
      hotelId: "h",
      channel: "email",
      emailReturned: true,
      clientRequestId: "cr_fail_1",
    },
    badEnv,
  );
  assert.equal(safe.error, "ACCOUNTING_WRITE_FAILED");
  assert.ok(safe.failure);
  assert.ok(fs.existsSync(badEnv.GDI_SURFE_ACCOUNTING_FAILURES_PATH));
}

// --- Explicit failure recorder ---
{
  const f = recordSurfeAccountingFailure(
    {
      category: "LOCK_TIMEOUT",
      message: "test failure",
      hotelId: "hotel_z",
      opportunityId: "opp_z",
    },
    env,
  );
  assert.equal(f.category, "LOCK_TIMEOUT");
  assert.equal("email" in f, false);
  const summaryFail = summarizeSurfeLiveAccounting({}, env);
  assert.ok(summaryFail.totals.accountingFailureCount >= 1);
  assert.equal(summaryFail.totals.accountingComplete, false);
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

// --- Concurrent parallel appends do not lose rows ---
{
  const parallelDir = path.join(tmpRoot, "parallel");
  fs.mkdirSync(parallelDir, { recursive: true });
  const pEnv = {
    GDI_SURFE_ACCOUNTING_PATH: path.join(parallelDir, "accounting.json"),
  };
  const N = 24;
  const results = await Promise.all(
    Array.from({ length: N }, (_, i) =>
      Promise.resolve().then(() =>
        appendSurfeLiveAccounting(
          {
            hotelId: "hotel_parallel",
            opportunityId: `opp_p_${i}`,
            clientRequestId: `cr_parallel_${i}`,
            enrichmentId: `enr_parallel_${i}`,
            channel: "email",
            emailReturned: i % 2 === 0,
            miss: i % 2 === 1,
            surfeAttempted: true,
          },
          pEnv,
        ),
      ),
    ),
  );
  assert.equal(
    results.filter((r) => r && r.deduplicated !== true).length,
    N,
  );
  const doc = JSON.parse(
    fs.readFileSync(pEnv.GDI_SURFE_ACCOUNTING_PATH, "utf8"),
  );
  const ids = new Set(doc.entries.map((e) => e.clientRequestId));
  assert.equal(ids.size, N, "parallel appends must keep unique clientRequestIds");
  assert.equal(doc.entries.length, N, "parallel appends must not drop or duplicate rows");
  const eventsPath = eventsJsonlPath(pEnv);
  assert.ok(fs.existsSync(eventsPath));
  assert.equal(path.dirname(eventsPath), path.dirname(pEnv.GDI_SURFE_ACCOUNTING_PATH));
  const lines = fs
    .readFileSync(eventsPath, "utf8")
    .split(/\r?\n/)
    .filter(Boolean);
  assert.ok(lines.length >= N);
}

// --- Restart recovery from JSONL when primary JSON is corrupt ---
{
  const recoverDir = path.join(tmpRoot, "recover");
  fs.mkdirSync(recoverDir, { recursive: true });
  const rEnv = {
    GDI_SURFE_ACCOUNTING_PATH: path.join(recoverDir, "accounting.json"),
    GDI_SURFE_ACCOUNTING_EVENTS_PATH: path.join(recoverDir, "events.jsonl"),
  };
  appendSurfeLiveAccounting(
    {
      hotelId: "hotel_r",
      opportunityId: "opp_r1",
      clientRequestId: "cr_r1",
      enrichmentId: "enr_r1",
      channel: "email",
      emailReturned: true,
      surfeAttempted: true,
    },
    rEnv,
  );
  appendSurfeLiveAccounting(
    {
      hotelId: "hotel_r",
      opportunityId: "opp_r2",
      clientRequestId: "cr_r2",
      enrichmentId: "enr_r2",
      channel: "phone",
      miss: true,
      surfeAttempted: true,
    },
    rEnv,
  );
  // Corrupt primary document
  fs.writeFileSync(rEnv.GDI_SURFE_ACCOUNTING_PATH, "{not-json");
  const summary = summarizeSurfeLiveAccounting(
    { hotelNames: { hotel_r: "Recover Hotel" } },
    rEnv,
  );
  assert.equal(summary.totals.totalLookups, 2);
  assert.equal(summary.totals.creditsUnknownCount >= 1, true);
}

// --- Summarize certainty totals ---
{
  const summary = summarizeSurfeLiveAccounting(
    { hotelNames: { hotel_a: "Hotel A", hotel_b: "Hotel B" } },
    env,
  );
  assert.ok(summary.totals.totalLookups >= 2);
  assert.ok(typeof summary.totals.creditsEstimated === "number");
  assert.ok(typeof summary.totals.creditsConfirmed === "number");
  assert.ok(typeof summary.totals.creditsUnknownCount === "number");
  assert.ok(summary.durability);
  assert.match(summary.durability.mechanism, /lock|atomic|jsonl/i);
  assert.ok(summary.trackingStartedNote);
  const dump = JSON.stringify(summary);
  assert.equal(dump.includes("secret@example.com"), false);
  assert.equal(dump.includes("+15551234567"), false);
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
  assert.match(appJs, /navId:\s*'surfe-usage'/);
  // Admin-visible (not runbook-gated) — founders/admins must see the menu item
  assert.match(
    appJs,
    /label:\s*'Surfe Usage'[\s\S]*?internalRunbookOnly:\s*false/,
  );
  assert.match(appJs, /adminResources/);
  assert.match(appJs, /normalized === '\/admin\/surfe-usage'/);

  const html = fs.readFileSync(
    path.join(ROOT, "public/app/admin/surfe-usage.html"),
    "utf8",
  );
  assert.match(html, /admin-surfe-usage\.js/);
  assert.match(html, /support-admin-gate/);
  assert.match(html, /asuKpis/);
  assert.match(html, /Monitor on-demand contact enrichment/);
  assert.match(html, /support-runbook\.css/);
  assert.match(html, /admin-ai-demand-reviews\.css/);
  assert.match(html, /asuOutcome/);
  assert.match(html, /asuClearFilters/);
  assert.match(html, /asuAccountingFailures/);

  const css = fs.readFileSync(
    path.join(ROOT, "public/css/admin-surfe-usage.css"),
    "utf8",
  );
  assert.match(css, /\.asu-page/);
  assert.match(css, /--runbook-/);

  const client = fs.readFileSync(
    path.join(ROOT, "public/js/admin-surfe-usage.js"),
    "utf8",
  );
  assert.match(client, /\/api\/admin\/surfe-usage\/summary/);
  assert.match(client, /Remaining Surfe Credits/);
  assert.match(client, /Email Contacts Found/);
  assert.match(client, /creditCertainty|Credits \(estimated\)|Credits unknown/i);
  assert.match(client, /accountingFailure|Accounting failures/i);
  assert.doesNotMatch(client, /contactDetails/);
  assert.doesNotMatch(client, /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);

  const api = fs.readFileSync(path.join(ROOT, "api/admin-surfe-usage.js"), "utf8");
  assert.match(api, /includesPersonalContactDetails:\s*false/);
  assert.match(api, /summarizeSurfeLiveAccounting/);
  assert.match(api, /creditCertainty/);

  const gdiApi = fs.readFileSync(
    path.join(ROOT, "api/group-demand-intelligence.js"),
    "utf8",
  );
  assert.match(gdiApi, /appendSurfeLiveAccountingSafe/);
  assert.match(gdiApi, /providerLatencyMs/);
  assert.match(gdiApi, /clientRequestId/);
  assert.equal(
    /appendSurfeLiveAccountingSafe\(\{[\s\S]*?email:\s*result/.test(gdiApi),
    false,
  );

  const revealClient = fs.readFileSync(
    path.join(
      ROOT,
      "public/js/group-demand-intelligence/gdi-contact-reveal-client-v1.js",
    ),
    "utf8",
  );
  assert.match(revealClient, /clientRequestId/);
}

// GDI_SURFE_USAGE_TRACKING_V1 kept for back-compat export
assert.ok(GDI_SURFE_USAGE_TRACKING_V1);
assert.ok(accountingPath(env));

console.log("PASS test-gdi-surfe-usage-tracking-v1");
