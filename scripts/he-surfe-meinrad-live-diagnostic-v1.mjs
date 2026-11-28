#!/usr/bin/env node
/**
 * Credit-guarded live Surfe diagnostic for Meinrad Lang III (Bethesda Marriott).
 *
 * Explicit opt-in only:
 *   node scripts/he-surfe-meinrad-live-diagnostic-v1.mjs --confirm-live-spend
 *
 * Never prints full email/phone. Never writes Airtable. Max 1 enrich job.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getSurfeApiKeyFromEnv,
  describeSurfeKeyPresence,
  buildSurfeEnrichPersonInput,
  normalizeSurfePerson,
  pickSurfePhoneNumber,
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  SURFE_CREDITS_URL,
} from "../lib/surfe/client.js";
import { maskEmail, maskPhone } from "../lib/market-alerts-contact/safe-log.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-explorer/he-surfe-contact-parity-v1"
);
const CONFIRM = process.argv.includes("--confirm-live-spend");
const MAX_EMAIL_CREDITS = Number(process.env.HE_SURFE_DIAG_MAX_EMAIL_CREDITS || 5);
const MAX_MOBILE_CREDITS = Number(process.env.HE_SURFE_DIAG_MAX_MOBILE_CREDITS || 3);

const SUBJECT = {
  personName: "Meinrad Lang III",
  companyName: "Marriott International",
  jobTitle: "General Manager",
  linkedinUrl: "https://www.linkedin.com/in/meinradlang/",
  hotelId: "recLuxvwwxID7U2B8",
};

function maskDiag(personNorm) {
  const email = personNorm?.emails?.[0]?.email || null;
  const picked = pickSurfePhoneNumber(personNorm || {});
  return {
    hasEmail: Boolean(email),
    emailMasked: email ? maskEmail(email) : null,
    hasPhone: Boolean(picked.phone),
    phoneMasked: picked.phone ? maskPhone(picked.phone) : null,
    phoneKind: picked.phoneKind,
    providerPhoneField: picked.providerPhoneField,
    linkedin_url: personNorm?.linkedin_url || null,
    found: personNorm?.found,
    status: personNorm?.status,
    emailCount: personNorm?.emails?.length || 0,
    mobileCount: personNorm?.mobile_phones?.length || 0,
    landlineCount: personNorm?.landlines?.length || 0,
  };
}

async function fetchCredits(apiKey) {
  const res = await fetch(SURFE_CREDITS_URL, {
    headers: { Authorization: `Bearer ${apiKey}`, Accept: "application/json" },
  });
  const text = await res.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = { raw_preview: text.slice(0, 200) };
  }
  return { ok: res.ok, http: res.status, payload };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const keyInfo = describeSurfeKeyPresence();
  const personInput = buildSurfeEnrichPersonInput(SUBJECT);

  const baseReport = {
    at: new Date().toISOString(),
    subject: {
      hotelId: SUBJECT.hotelId,
      personName: SUBJECT.personName,
      companyName: SUBJECT.companyName,
      linkedinUrl: personInput.linkedinUrl,
      firstName: personInput.firstName,
      lastName: personInput.lastName,
    },
    requestField: "linkedinUrl",
    linkedinFieldCorrect: personInput.linkedinUrl === "https://www.linkedin.com/in/meinradlang",
    keyPresent: keyInfo.present,
    confirmLiveSpend: CONFIRM,
  };

  if (!CONFIRM) {
    const report = {
      ...baseReport,
      status: "DRY_RUN_NO_SPEND",
      message: "Pass --confirm-live-spend to run one guarded Surfe enrich.",
    };
    fs.writeFileSync(
      path.join(OUT_DIR, "MEINRAD_LIVE_DIAGNOSTIC.json"),
      JSON.stringify(report, null, 2)
    );
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  const apiKey = getSurfeApiKeyFromEnv();
  if (!apiKey) {
    const report = { ...baseReport, status: "BLOCKED", error: "MISSING_SURFE_API_KEY" };
    fs.writeFileSync(
      path.join(OUT_DIR, "MEINRAD_LIVE_DIAGNOSTIC.json"),
      JSON.stringify(report, null, 2)
    );
    console.error(JSON.stringify(report, null, 2));
    process.exitCode = 2;
    return;
  }

  const creditsBefore = await fetchCredits(apiKey);
  const emailCredits = Number(
    creditsBefore.payload?.totalEmail ??
      creditsBefore.payload?.emailCredits ??
      creditsBefore.payload?.email ??
      NaN
  );
  const mobileCredits = Number(
    creditsBefore.payload?.totalMobile ??
      creditsBefore.payload?.mobileCredits ??
      creditsBefore.payload?.mobile ??
      NaN
  );

  if (creditsBefore.ok && Number.isFinite(emailCredits) && emailCredits < 1) {
    const report = {
      ...baseReport,
      status: "BLOCKED_INSUFFICIENT_CREDITS",
      creditsBefore: { emailCredits, mobileCredits, http: creditsBefore.http },
    };
    fs.writeFileSync(
      path.join(OUT_DIR, "MEINRAD_LIVE_DIAGNOSTIC.json"),
      JSON.stringify(report, null, 2)
    );
    console.error(JSON.stringify(report, null, 2));
    process.exitCode = 3;
    return;
  }

  // One email enrich; mobile only if HE_SURFE_REQUEST_MOBILE=true and credits allow.
  const includeMobile =
    String(process.env.HE_SURFE_REQUEST_MOBILE || "").toLowerCase() === "true" &&
    (!Number.isFinite(mobileCredits) || mobileCredits >= 1) &&
    (!Number.isFinite(mobileCredits) || mobileCredits <= MAX_MOBILE_CREDITS * 1000);

  const start = await startSurfePeopleEnrichment({
    people: [personInput],
    include: {
      email: true,
      mobile: includeMobile,
      linkedInUrl: true,
    },
  });

  if (!start.ok) {
    const report = {
      ...baseReport,
      status: "ENRICH_START_FAILED",
      http: start.http_status,
      error: start.error || start.payload?.message || "start_failed",
      insufficient_credits: Boolean(start.insufficient_credits),
      creditsBefore: { emailCredits, mobileCredits },
      includeMobile,
      creditsConsumedEstimate: 0,
    };
    fs.writeFileSync(
      path.join(OUT_DIR, "MEINRAD_LIVE_DIAGNOSTIC.json"),
      JSON.stringify(report, null, 2)
    );
    console.error(JSON.stringify(report, null, 2));
    process.exitCode = 4;
    return;
  }

  const enrichmentId =
    start.payload?.enrichmentID || start.payload?.enrichmentId || start.payload?.id || null;

  let finalPayload = start.payload;
  let timedOut = false;
  if (enrichmentId) {
    const polled = await pollSurfePeopleEnrichment(enrichmentId, { maxWaitMs: 90000 });
    timedOut = Boolean(polled.timed_out);
    if (polled.ok || polled.payload) finalPayload = polled.payload || finalPayload;
  }

  const people = Array.isArray(finalPayload?.people) ? finalPayload.people : [];
  const first = people[0] || null;
  const norm = first ? normalizeSurfePerson(first) : null;
  const masked = norm ? maskDiag(norm) : null;
  const creditsAfter = await fetchCredits(apiKey);
  const emailAfter = Number(
    creditsAfter.payload?.totalEmail ??
      creditsAfter.payload?.emailCredits ??
      creditsAfter.payload?.email ??
      NaN
  );
  const mobileAfter = Number(
    creditsAfter.payload?.totalMobile ??
      creditsAfter.payload?.mobileCredits ??
      creditsAfter.payload?.mobile ??
      NaN
  );

  const jobStatus = String(finalPayload?.status || "").toUpperCase();
  const report = {
    ...baseReport,
    status: timedOut
      ? "PENDING_TIMEOUT"
      : jobStatus === "FAILED"
        ? "FAILED"
        : masked && (masked.hasEmail || masked.hasPhone)
          ? "PARTIAL_OR_FULL_SUCCESS"
          : jobStatus === "COMPLETED" || jobStatus === ""
            ? "COMPLETED_NO_CHANNELS"
            : jobStatus || "UNKNOWN",
    enrichmentId,
    jobStatus: jobStatus || null,
    timedOut,
    includeMobile,
    result: masked,
    creditsBefore: { emailCredits, mobileCredits, http: creditsBefore.http },
    creditsAfter: { emailCredits: emailAfter, mobileCredits: mobileAfter, http: creditsAfter.http },
    creditsConsumedEstimate: {
      email:
        Number.isFinite(emailCredits) && Number.isFinite(emailAfter)
          ? Math.max(0, emailCredits - emailAfter)
          : masked?.hasEmail
            ? 1
            : null,
      mobile:
        Number.isFinite(mobileCredits) && Number.isFinite(mobileAfter)
          ? Math.max(0, mobileCredits - mobileAfter)
          : null,
    },
    persistence: "NOT_WRITTEN",
    note: "Comparison screenshot values are not asserted or hardcoded.",
  };

  fs.writeFileSync(
    path.join(OUT_DIR, "MEINRAD_LIVE_DIAGNOSTIC.json"),
    JSON.stringify(report, null, 2)
  );
  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      status: "THROW",
      error: String(err?.code || err?.message || err).slice(0, 200),
    })
  );
  process.exitCode = 1;
});
