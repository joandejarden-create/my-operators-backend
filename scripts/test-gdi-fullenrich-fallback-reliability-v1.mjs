#!/usr/bin/env node
/**
 * GDI FullEnrich fallback reliability hardening — durable spend + job recovery.
 * No paid enrichment calls.
 *   node scripts/test-gdi-fullenrich-fallback-reliability-v1.mjs
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  evaluateFullEnrichFallbackEligibility,
  assertFullEnrichSpendAllowed,
  readFullEnrichFallbackFlags,
} from "../lib/group-demand-intelligence/gdi-fullenrich-fallback-v1.js";
import {
  resetFullEnrichSpendLedgerForTests,
  reserveFullEnrichSpend,
  markFullEnrichJobSubmitted,
  reconcileFullEnrichSpend,
  prepareFullEnrichJobRecovery,
  listPendingFullEnrichJobs,
  getFullEnrichSpendBudgetSnapshot,
  summarizeFullEnrichSpendMetrics,
  markSpendAccountingUnhealthy,
  getFullEnrichJob,
  FE_JOB_STATUS,
  FE_CHARGE_STATUS,
  FORBIDDEN_PII_KEYS,
  fullEnrichSpendLedgerPath,
} from "../lib/group-demand-intelligence/gdi-fullenrich-spend-ledger-v1.js";
import { createMockGdiFullEnrichContactRevealProvider } from "../lib/group-demand-intelligence/gdi-fullenrich-contact-provider-v1.js";

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

function tempEnv(extra = {}) {
  const dir = mkdtempSync(join(tmpdir(), "gdi-fe-spend-"));
  return {
    env: {
      GDI_FULLENRICH_SPEND_DIR: dir,
      GDI_FULLENRICH_SPEND_CAP: "3",
      GDI_FULLENRICH_FALLBACK_ENABLED: "1",
      GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED: "1",
      GDI_FULLENRICH_CREDIT_RESERVE: "20",
      FULL_ENRICH_API_KEY: "fixture-key-not-real",
      ...extra,
    },
    dir,
    cleanup() {
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

check("missing email with existing phone → email fallback eligible", () => {
  const r = evaluateFullEnrichFallbackEligibility({
    contactDetails: { phone: "+15551212" },
    channel: "both",
    surfeAttempted: true,
    env: {
      GDI_FULLENRICH_FALLBACK_ENABLED: "1",
      FULL_ENRICH_API_KEY: "fixture-key-not-real",
    },
  });
  assert.equal(r.eligible, true);
  assert.equal(r.channel, "email");
  assert.match(r.reason, /SURFE_EMAIL_MISS|EMAIL_MISSING/);
});

check("customer-display flag blocks spend", () => {
  const env = {
    GDI_FULLENRICH_FALLBACK_ENABLED: "1",
    GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED: "1",
    GDI_FULLENRICH_CUSTOMER_DISPLAY: "1",
    FULL_ENRICH_API_KEY: "fixture-key-not-real",
  };
  const g = assertFullEnrichSpendAllowed({
    flags: readFullEnrichFallbackFlags(env),
    balance: 36,
    env,
  });
  assert.equal(g.ok, false);
  assert.equal(g.error, "CUSTOMER_DISPLAY_BLOCKED");
});

check("durable reserve survives process restart (re-read ledger)", () => {
  const t = tempEnv();
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const r1 = reserveFullEnrichSpend(
      {
        hotelId: "h1",
        opportunityId: "o1",
        clientRequestId: "cr_restart_1",
        creditsReserved: 1,
      },
      t.env,
    );
    assert.equal(r1.ok, true);
    assert.equal(r1.duplicate, false);
    // Simulate restart: new snapshot from disk
    const snap = getFullEnrichSpendBudgetSnapshot(t.env);
    assert.equal(snap.reservedOpen, 1);
    assert.equal(snap.committed, 1);
    assert.equal(snap.remainingBudget, 2);
    const pending = listPendingFullEnrichJobs(t.env);
    assert.equal(pending.length, 1);
    assert.equal(pending[0].lookupRequestId, r1.lookupRequestId);
    assert.equal(pending[0].hotelId, "h1");
    assert.equal(pending[0].opportunityId, "o1");
    // No PII keys in job
    for (const k of FORBIDDEN_PII_KEYS) {
      assert.equal(k in pending[0], false);
    }
  } finally {
    t.cleanup();
  }
});

check("server restart during pending enrichment → recover without duplicate paid submit", () => {
  const t = tempEnv();
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const r1 = reserveFullEnrichSpend(
      {
        hotelId: "h1",
        opportunityId: "o1",
        clientRequestId: "cr_pending_1",
        creditsReserved: 1,
      },
      t.env,
    );
    markFullEnrichJobSubmitted(
      {
        jobId: r1.jobId,
        enrichmentId: "fe_job_abc",
      },
      t.env,
    );
    // Restart: same clientRequestId must not create a second paid reservation
    const r2 = reserveFullEnrichSpend(
      {
        hotelId: "h1",
        opportunityId: "o1",
        clientRequestId: "cr_pending_1",
        creditsReserved: 1,
      },
      t.env,
    );
    assert.equal(r2.ok, true);
    assert.equal(r2.duplicate, true);
    assert.equal(r2.enrichmentId, "fe_job_abc");
    const prep = prepareFullEnrichJobRecovery(
      { enrichmentId: "fe_job_abc", clientRequestId: "cr_pending_1" },
      t.env,
    );
    assert.equal(prep.ok, true);
    assert.equal(prep.enrichmentId, "fe_job_abc");
    assert.match(prep.message, /do not POST/i);
    const snap = getFullEnrichSpendBudgetSnapshot(t.env);
    assert.equal(snap.totals.duplicatePaidPrevented >= 1, true);
    assert.equal(snap.reservedOpen, 1); // still open until reconcile
  } finally {
    t.cleanup();
  }
});

check("concurrent paid lookup requests cannot exceed durable cap", () => {
  const t = tempEnv({ GDI_FULLENRICH_SPEND_CAP: "2" });
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const a = reserveFullEnrichSpend(
      { clientRequestId: "cr_a", creditsReserved: 1, hotelId: "h", opportunityId: "o" },
      t.env,
    );
    const b = reserveFullEnrichSpend(
      { clientRequestId: "cr_b", creditsReserved: 1, hotelId: "h", opportunityId: "o" },
      t.env,
    );
    const c = reserveFullEnrichSpend(
      { clientRequestId: "cr_c", creditsReserved: 1, hotelId: "h", opportunityId: "o" },
      t.env,
    );
    assert.equal(a.ok, true);
    assert.equal(b.ok, true);
    assert.equal(c.ok, false);
    assert.equal(c.error, "SPEND_CAP_REACHED");
  } finally {
    t.cleanup();
  }
});

check("spending-cap exhaustion after reconcile (UNKNOWN still counts)", () => {
  const t = tempEnv({ GDI_FULLENRICH_SPEND_CAP: "1" });
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const r = reserveFullEnrichSpend(
      { clientRequestId: "cr_to", creditsReserved: 1, hotelId: "h", opportunityId: "o" },
      t.env,
    );
    markFullEnrichJobSubmitted(
      { jobId: r.jobId, enrichmentId: "fe_to" },
      t.env,
    );
    const rec = reconcileFullEnrichSpend(
      {
        jobId: r.jobId,
        providerStatus: FE_JOB_STATUS.TIMEOUT,
        creditCertainty: "unknown",
        creditsCharged: 1,
      },
      t.env,
    );
    assert.equal(rec.ok, true);
    assert.equal(rec.chargeStatus, FE_CHARGE_STATUS.UNKNOWN);
    const next = reserveFullEnrichSpend(
      { clientRequestId: "cr_to_2", creditsReserved: 1 },
      t.env,
    );
    assert.equal(next.ok, false);
    assert.equal(next.error, "SPEND_CAP_REACHED");
  } finally {
    t.cleanup();
  }
});

check("insufficient account credits gate (balance vs reserve)", () => {
  const t = tempEnv();
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const g = assertFullEnrichSpendAllowed({
      flags: readFullEnrichFallbackFlags(t.env),
      balance: 20,
      env: t.env,
    });
    assert.equal(g.ok, false);
    assert.equal(g.error, "INSUFFICIENT_CREDITS_RESERVE");
  } finally {
    t.cleanup();
  }
});

check("duplicate client requests prevent second paid reservation", () => {
  const t = tempEnv();
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const a = reserveFullEnrichSpend(
      { clientRequestId: "same", creditsReserved: 1, hotelId: "h1", opportunityId: "o1" },
      t.env,
    );
    const b = reserveFullEnrichSpend(
      { clientRequestId: "same", creditsReserved: 1, hotelId: "h1", opportunityId: "o1" },
      t.env,
    );
    assert.equal(a.ok && b.ok, true);
    assert.equal(b.duplicate, true);
    assert.equal(a.reservationId, b.reservationId);
  } finally {
    t.cleanup();
  }
});

check("timeout followed by retry recovers existing enrichmentId", () => {
  const t = tempEnv();
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const r = reserveFullEnrichSpend(
      { clientRequestId: "cr_retry", creditsReserved: 1, hotelId: "h", opportunityId: "o" },
      t.env,
    );
    markFullEnrichJobSubmitted(
      { jobId: r.jobId, enrichmentId: "fe_retry_1" },
      t.env,
    );
    reconcileFullEnrichSpend(
      {
        jobId: r.jobId,
        providerStatus: FE_JOB_STATUS.TIMEOUT,
        creditCertainty: "unknown",
        creditsCharged: 1,
      },
      t.env,
    );
    // Retry with same client id → duplicate, exposes enrichmentId for recovery poll
    const again = reserveFullEnrichSpend(
      { clientRequestId: "cr_retry", creditsReserved: 1 },
      t.env,
    );
    assert.equal(again.duplicate, true);
    assert.equal(again.enrichmentId, "fe_retry_1");
    const prep = prepareFullEnrichJobRecovery(
      { enrichmentId: "fe_retry_1" },
      t.env,
    );
    assert.equal(prep.ok, true);
  } finally {
    t.cleanup();
  }
});

await checkAsync("provider job recovery via mock recoverEnrichmentId", async () => {
  const p = createMockGdiFullEnrichContactRevealProvider("hit");
  const r = await p.reveal({
    recoverEnrichmentId: "fe_existing_99",
    onSubmitted: async ({ enrichmentId, recoveredExistingJob }) => {
      assert.equal(enrichmentId, "fe_existing_99");
      assert.equal(recoveredExistingJob, true);
    },
  });
  assert.equal(r.recoveredExistingJob, true);
  assert.equal(r.providerStatus, FE_JOB_STATUS.RECOVERED);
  assert.equal(r.incrementalEmailFound, true);
});

check("no-match billing uncertainty preserved as UNKNOWN", () => {
  const t = tempEnv();
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const r = reserveFullEnrichSpend(
      { clientRequestId: "cr_miss", creditsReserved: 1 },
      t.env,
    );
    markFullEnrichJobSubmitted(
      { jobId: r.jobId, enrichmentId: "fe_miss" },
      t.env,
    );
    const rec = reconcileFullEnrichSpend(
      {
        jobId: r.jobId,
        providerStatus: FE_JOB_STATUS.NO_MATCH,
        creditCertainty: "unknown",
        incrementalEmailFound: false,
      },
      t.env,
    );
    assert.equal(rec.chargeStatus, FE_CHARGE_STATUS.UNKNOWN);
    assert.equal(rec.creditCertainty, "unknown");
    const snap = getFullEnrichSpendBudgetSnapshot(t.env);
    assert.equal(snap.chargedUnknown >= 1, true);
  } finally {
    t.cleanup();
  }
});

check("failed accounting blocks new paid FullEnrich requests", () => {
  const t = tempEnv();
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    markSpendAccountingUnhealthy(
      { code: "TEST_FAIL", message: "forced unhealthy" },
      t.env,
    );
    const g = assertFullEnrichSpendAllowed({
      flags: readFullEnrichFallbackFlags(t.env),
      balance: 36,
      env: t.env,
    });
    assert.equal(g.ok, false);
    assert.equal(g.error, "SPEND_ACCOUNTING_UNSAFE");
    const r = reserveFullEnrichSpend(
      { clientRequestId: "blocked", creditsReserved: 1 },
      t.env,
    );
    assert.equal(r.ok, false);
    assert.equal(r.error, "SPEND_ACCOUNTING_UNSAFE");
  } finally {
    t.cleanup();
  }
});

check("privacy: ledger never stores email/phone/raw payload", () => {
  const t = tempEnv();
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    const r = reserveFullEnrichSpend(
      {
        clientRequestId: "cr_priv",
        hotelId: "hotel_x",
        opportunityId: "opp_y",
        creditsReserved: 1,
        email: "should-not-persist@example.com",
        contactDetails: { email: "nope@example.com", phone: "+1" },
        payload: { huge: true },
      },
      t.env,
    );
    reconcileFullEnrichSpend(
      {
        jobId: r.jobId,
        providerStatus: FE_JOB_STATUS.FINISHED,
        creditCertainty: "estimated",
        creditsCharged: 1,
        incrementalEmailFound: true,
        email: "also-no@example.com",
      },
      t.env,
    );
    const raw = readFileSync(fullEnrichSpendLedgerPath(t.env), "utf8");
    assert.equal(/should-not-persist|nope@|also-no@|\+1/.test(raw), false);
    assert.match(raw, /hotel_x/);
    assert.match(raw, /opp_y/);
    assert.match(raw, /FULLENRICH|FINISHED|estimated/i);
    const job = getFullEnrichJob({ jobId: r.jobId }, t.env);
    assert.equal(job.incrementalEmailFound, true);
    assert.equal(job.distinguishedFromPublicSource, true);
    assert.equal(job.email, undefined);
  } finally {
    t.cleanup();
  }
});

check("provider usage metrics: recovery rate + cost per incremental email", () => {
  const t = tempEnv({ GDI_FULLENRICH_SPEND_CAP: "10" });
  try {
    resetFullEnrichSpendLedgerForTests(t.env);
    for (const id of ["m1", "m2", "m3"]) {
      const r = reserveFullEnrichSpend(
        { clientRequestId: id, creditsReserved: 1, hotelId: "h", opportunityId: "o" },
        t.env,
      );
      markFullEnrichJobSubmitted(
        { jobId: r.jobId, enrichmentId: `fe_${id}` },
        t.env,
      );
      reconcileFullEnrichSpend(
        {
          jobId: r.jobId,
          providerStatus:
            id === "m3" ? FE_JOB_STATUS.NO_MATCH : FE_JOB_STATUS.FINISHED,
          creditCertainty: id === "m3" ? "unknown" : "estimated",
          creditsCharged: 1,
          incrementalEmailFound: id !== "m3",
        },
        t.env,
      );
    }
    const summary = summarizeFullEnrichSpendMetrics(t.env);
    assert.equal(summary.metrics.confirmedPaidSubmissions, 3);
    assert.equal(summary.metrics.successfulWorkEmailRecoveries, 2);
    assert.equal(summary.metrics.noMatches, 1);
    assert.equal(summary.metrics.creditsEstimated, 2);
    assert.equal(summary.metrics.creditsUnknown, 1);
    assert.equal(summary.metrics.incrementalRecoveryRate, 66.7);
    assert.equal(summary.metrics.costPerIncrementalSuccessfulWorkEmail, 1.5);
    assert.equal(summary.metrics.distinguishedFromPublicSourceGdiContact, true);
  } finally {
    t.cleanup();
  }
});

check("flags remain off by default (no paid activation)", () => {
  const f = readFullEnrichFallbackFlags({});
  assert.equal(f.enabled, false);
  assert.equal(f.spendAuthorized, false);
  assert.equal(f.customerDisplay, false);
  assert.equal(f.phoneEnabled, false);
});

check("API + client still redact FE PII / require confirm", () => {
  const api = readFileSync(join(ROOT, "api/group-demand-intelligence.js"), "utf8");
  assert.match(api, /reserveFullEnrichSpend/);
  assert.match(api, /reconcileFullEnrichSpend/);
  assert.match(api, /prepareFullEnrichJobRecovery/);
  assert.match(api, /customerDisplayBlocked/);
  assert.match(api, /fullenrich_ephemeral_redacted/);
  const client = readFileSync(
    join(ROOT, "public/js/group-demand-intelligence/gdi-contact-reveal-client-v1.js"),
    "utf8",
  );
  assert.match(client, /authorizeFullEnrichFallback/);
  assert.match(client, /root\.confirm/);
});

check("ledger module exists and is durable-path based", () => {
  assert.equal(
    existsSync(
      join(ROOT, "lib/group-demand-intelligence/gdi-fullenrich-spend-ledger-v1.js"),
    ),
    true,
  );
});

if (failed) {
  console.error(`\n${failed} reliability test(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI FullEnrich fallback reliability V1 tests passed.");
