/**
 * Live Surfe gate for GDI contact reveal — credit check + in-flight dedupe +
 * non-PII usage accounting for Admin → Resources → Surfe Usage.
 *
 * Persistence: filesystem ledger under data/group-demand-intelligence/surfe-usage/
 * (same class of durable store as other GDI ledgers). Exclusive file lock +
 * atomic rename + append-only JSONL event log. Multi-replica deployments need a
 * shared volume; Airtable was not selected for V1 (rate/cost/schema overhead).
 *
 * Never logs or stores returned emails, phones, raw Surfe payloads, or API keys.
 */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getSurfeCredits, SURFE_CREDIT_COSTS } from "../surfe/client.js";
import { surfeIncludeForChannel } from "./gdi-surfe-contact-provider-v1.js";
import {
  SURFE_EMAIL_CREDIT_USD_ESTIMATE,
  SURFE_MOBILE_CREDIT_USD_ESTIMATE,
} from "./workflow/paid-multisegment-research-v1.js";
import {
  classifyUsageClass,
  resolveDataEnvironment,
  enrichSurfeUsageContext,
  GDI_SURFE_USAGE_TRACKING_V1_2 as CONTEXT_V1_2,
} from "./gdi-surfe-usage-context-v1.js";
import { airtableSyncHealthFromLedger } from "./gdi-surfe-usage-airtable-sync-v1.js";

export const GDI_SURFE_LIVE_GATE_V1 = "gdi-surfe-live-gate-v1";
export const GDI_SURFE_USAGE_TRACKING_V1 = "gdi-surfe-usage-tracking-v1";
export const GDI_SURFE_USAGE_TRACKING_V1_1 = "gdi-surfe-usage-tracking-v1.1";
export const GDI_SURFE_USAGE_TRACKING_V1_2 = CONTEXT_V1_2;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const inFlight = new Set();
const STALE_LOCK_MS = 30_000;

/** Fields that must never appear on persisted usage rows. */
const FORBIDDEN_PII_KEYS = Object.freeze([
  "email",
  "phone",
  "mobile",
  "contactDetails",
  "personName",
  "contactName",
  "raw",
  "payload",
  "response",
  "apiKey",
  "authorization",
  "acceptedFieldValues",
]);

function text(v) {
  return String(v == null ? "" : v).trim();
}

function accountingDir(env = process.env) {
  if (env.GDI_SURFE_ACCOUNTING_DIR) return path.resolve(env.GDI_SURFE_ACCOUNTING_DIR);
  // Keep events/failures colocated when only the JSON path is overridden (tests / custom mounts).
  if (env.GDI_SURFE_ACCOUNTING_PATH) {
    return path.dirname(path.resolve(env.GDI_SURFE_ACCOUNTING_PATH));
  }
  return path.join(ROOT, "data/group-demand-intelligence/surfe-usage");
}

function accountingPath(env = process.env) {
  if (env.GDI_SURFE_ACCOUNTING_PATH) return path.resolve(env.GDI_SURFE_ACCOUNTING_PATH);
  return path.join(accountingDir(env), "gdi-live-accounting-v1.json");
}

function sleepSync(ms) {
  try {
    const sab = new SharedArrayBuffer(4);
    const ia = new Int32Array(sab);
    Atomics.wait(ia, 0, 0, ms);
  } catch {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      /* spin fallback */
    }
  }
}

function eventsJsonlPath(env = process.env) {
  if (env.GDI_SURFE_ACCOUNTING_EVENTS_PATH) {
    return path.resolve(env.GDI_SURFE_ACCOUNTING_EVENTS_PATH);
  }
  return path.join(accountingDir(env), "gdi-live-accounting-events-v1.jsonl");
}

function failuresJsonlPath(env = process.env) {
  if (env.GDI_SURFE_ACCOUNTING_FAILURES_PATH) {
    return path.resolve(env.GDI_SURFE_ACCOUNTING_FAILURES_PATH);
  }
  return path.join(accountingDir(env), "gdi-live-accounting-failures-v1.jsonl");
}

function lockPathFor(file) {
  return `${file}.lock`;
}

function pidAlive(pid) {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Exclusive filesystem lock (same pattern as HE durable budget ledger).
 */
export function acquireSurfeAccountingLock(
  lockPath,
  { retries = 80, delayMs = 25, now = Date.now } = {},
) {
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });
  for (let i = 0; i < retries; i += 1) {
    try {
      const fd = fs.openSync(lockPath, "wx");
      fs.writeFileSync(
        fd,
        JSON.stringify({ pid: process.pid, at: now() }),
      );
      return {
        release() {
          try {
            fs.closeSync(fd);
          } catch {
            /* ignore */
          }
          try {
            fs.unlinkSync(lockPath);
          } catch {
            /* ignore */
          }
        },
      };
    } catch {
      try {
        const raw = fs.readFileSync(lockPath, "utf8");
        let meta = null;
        try {
          meta = JSON.parse(raw);
        } catch {
          meta = null;
        }
        const st = fs.statSync(lockPath);
        const age = now() - Number(st.mtimeMs || 0);
        const ownerPid = Number(meta?.pid);
        const dead = meta && !pidAlive(ownerPid);
        const aged = age > STALE_LOCK_MS;
        if ((dead && aged) || (!meta && aged)) {
          try {
            fs.unlinkSync(lockPath);
            continue;
          } catch {
            /* race */
          }
        }
      } catch {
        /* disappeared */
      }
      sleepSync(delayMs);
    }
  }
  const err = new Error("SURFE_ACCOUNTING_LOCK_UNSAFE");
  err.code = "SURFE_ACCOUNTING_LOCK_UNSAFE";
  throw err;
}

function atomicWriteJson(file, doc) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(doc, null, 2)}\n`);
  fs.renameSync(tmp, file);
}

function appendJsonl(file, row) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.appendFileSync(file, `${JSON.stringify(row)}\n`);
}

function hashUserId(userId) {
  const id = text(userId);
  if (!id) return null;
  return createHash("sha256").update(`gdi-surfe-user|${id}`).digest("hex").slice(0, 16);
}

export function contactRevealIdentityKey({ hotelId, opportunityId, contactName, channel }) {
  return createHash("sha256")
    .update(
      [
        String(hotelId || ""),
        String(opportunityId || ""),
        String(contactName || "").toLowerCase().trim(),
        String(channel || "both"),
      ].join("|"),
    )
    .digest("hex")
    .slice(0, 24);
}

export async function preflightSurfeCredits(channel = "both") {
  const include = surfeIncludeForChannel(channel);
  const credits = await getSurfeCredits();
  if (!credits.ok) {
    return {
      ok: false,
      error: "SURFE_CREDITS_UNAVAILABLE",
      message: "Could not read Surfe credit balance.",
      credits: null,
      include,
    };
  }
  const email = Number(credits.payload?.totalEmail ?? 0);
  const mobile = Number(credits.payload?.totalMobile ?? 0);
  if (include.email && email < 1) {
    return {
      ok: false,
      error: "INSUFFICIENT_CREDITS",
      message: "Insufficient Surfe email credits for this lookup.",
      credits: { totalEmail: email, totalMobile: mobile },
      include,
    };
  }
  if (include.mobile && mobile < 1) {
    return {
      ok: false,
      error: "INSUFFICIENT_CREDITS",
      message: "Insufficient Surfe mobile credits for this lookup.",
      credits: { totalEmail: email, totalMobile: mobile },
      include,
    };
  }
  return {
    ok: true,
    credits: { totalEmail: email, totalMobile: mobile },
    include,
  };
}

export function beginSurfeInFlight(key) {
  if (inFlight.has(key)) {
    return {
      ok: false,
      error: "LOOKUP_ALREADY_IN_PROGRESS",
      message: "A contact lookup for this person is already in progress.",
    };
  }
  inFlight.add(key);
  return { ok: true };
}

export function endSurfeInFlight(key) {
  inFlight.delete(key);
}

function creditDelta(before, after, field) {
  const b = Number(before?.[field]);
  const a = Number(after?.[field]);
  if (!Number.isFinite(b) || !Number.isFinite(a)) return null;
  const d = b - a;
  return d >= 0 ? d : null;
}

/**
 * Credit/cost derivation with explicit certainty.
 *
 * Surfe does not expose per-request billed credits on enrich responses today.
 * Shared-account balance deltas are never treated as confirmed for one lookup
 * (concurrent requests can attribute foreign consumption).
 * Misses are UNKNOWN — not assumed zero — unless provider-confirmed.
 */
export function deriveSurfeCreditsAndCost(entry = {}) {
  const channel = String(entry.channel || "both").toLowerCase();
  const emailReturned = entry.emailReturned === true;
  const phoneReturned = entry.phoneReturned === true;

  // Provider-confirmed path (future-ready / explicit inject for tests)
  const providerCredits = Number(entry.providerCreditsCharged);
  if (Number.isFinite(providerCredits) && providerCredits >= 0) {
    const cost =
      providerCredits *
      ((SURFE_EMAIL_CREDIT_USD_ESTIMATE + SURFE_MOBILE_CREDIT_USD_ESTIMATE) / 2);
    return {
      emailCreditsCharged: null,
      mobileCreditsCharged: null,
      creditsCharged: providerCredits,
      creditsConfirmed: providerCredits,
      creditsEstimated: null,
      creditCertainty: "confirmed",
      costEstimateUsd: Math.round(cost * 10000) / 10000,
      costConfirmedUsd: null,
      costEstimatedUsd: Math.round(cost * 10000) / 10000,
      costCertainty: "estimated",
      costEstimateSource: "provider_confirmed_credits",
      attributionWarning: null,
      emailCreditUsdEstimate: SURFE_EMAIL_CREDIT_USD_ESTIMATE,
      mobileCreditUsdEstimate: SURFE_MOBILE_CREDIT_USD_ESTIMATE,
    };
  }

  const emailDelta = creditDelta(entry.creditsBefore, entry.creditsAfter, "totalEmail");
  const mobileDelta = creditDelta(entry.creditsBefore, entry.creditsAfter, "totalMobile");

  if (emailDelta != null || mobileDelta != null) {
    const emailCredits = emailDelta == null ? 0 : emailDelta;
    const mobileCredits = mobileDelta == null ? 0 : mobileDelta;
    const creditsCharged = emailCredits + mobileCredits;
    const costEstimateUsd =
      emailCredits * SURFE_EMAIL_CREDIT_USD_ESTIMATE +
      mobileCredits * SURFE_MOBILE_CREDIT_USD_ESTIMATE;
    return {
      emailCreditsCharged: emailCredits,
      mobileCreditsCharged: mobileCredits,
      creditsCharged,
      creditsConfirmed: null,
      creditsEstimated: creditsCharged,
      creditCertainty: "estimated",
      costEstimateUsd: Math.round(costEstimateUsd * 10000) / 10000,
      costConfirmedUsd: null,
      costEstimatedUsd: Math.round(costEstimateUsd * 10000) / 10000,
      costCertainty: "estimated",
      costEstimateSource: "shared_balance_delta_estimated",
      attributionWarning: "SHARED_ACCOUNT_BALANCE_DELTA",
      emailCreditUsdEstimate: SURFE_EMAIL_CREDIT_USD_ESTIMATE,
      mobileCreditUsdEstimate: SURFE_MOBILE_CREDIT_USD_ESTIMATE,
    };
  }

  if (emailReturned || phoneReturned) {
    const emailCredits =
      emailReturned && (channel === "email" || channel === "both")
        ? SURFE_CREDIT_COSTS.email_enrichment_per_person_worst_case
        : 0;
    const mobileCredits =
      phoneReturned &&
      (channel === "phone" || channel === "mobile" || channel === "both")
        ? SURFE_CREDIT_COSTS.mobile_enrichment_per_person_worst_case
        : 0;
    const creditsCharged = emailCredits + mobileCredits;
    const costEstimateUsd =
      emailCredits * SURFE_EMAIL_CREDIT_USD_ESTIMATE +
      mobileCredits * SURFE_MOBILE_CREDIT_USD_ESTIMATE;
    return {
      emailCreditsCharged: emailCredits,
      mobileCreditsCharged: mobileCredits,
      creditsCharged,
      creditsConfirmed: null,
      creditsEstimated: creditsCharged,
      creditCertainty: "estimated",
      costEstimateUsd: Math.round(costEstimateUsd * 10000) / 10000,
      costConfirmedUsd: null,
      costEstimatedUsd: Math.round(costEstimateUsd * 10000) / 10000,
      costCertainty: "estimated",
      costEstimateSource: "estimate_on_success",
      attributionWarning: null,
      emailCreditUsdEstimate: SURFE_EMAIL_CREDIT_USD_ESTIMATE,
      mobileCreditUsdEstimate: SURFE_MOBILE_CREDIT_USD_ESTIMATE,
    };
  }

  return {
    emailCreditsCharged: null,
    mobileCreditsCharged: null,
    creditsCharged: null,
    creditsConfirmed: null,
    creditsEstimated: null,
    creditCertainty: "unknown",
    costEstimateUsd: null,
    costConfirmedUsd: null,
    costEstimatedUsd: null,
    costCertainty: "unknown",
    costEstimateSource: "unknown_no_provider_confirmation",
    attributionWarning: null,
    emailCreditUsdEstimate: SURFE_EMAIL_CREDIT_USD_ESTIMATE,
    mobileCreditUsdEstimate: SURFE_MOBILE_CREDIT_USD_ESTIMATE,
  };
}

export function classifySurfeLookupStatus(entry = {}) {
  if (entry.error) {
    const err = String(entry.error).toUpperCase();
    if (/TIMEOUT|TIMED_OUT/.test(err)) return "error";
    if (/INSUFFICIENT|CREDITS/.test(err)) return "error";
    if (/UNAVAILABLE|MISSING_SURFE|PROVIDER/.test(err)) return "error";
    if (/NO_MATCH|MISS|UNRESOLVED|CHANNELS_UNRESOLVED/.test(err)) return "no_match";
    return "error";
  }
  if (entry.miss === true) return "no_match";
  const email = entry.emailReturned === true;
  const phone = entry.phoneReturned === true;
  if (email && phone) return "success";
  if (email || phone) return "partial";
  if (entry.ok === true) return "no_match";
  return "error";
}

function stripForbidden(entry = {}) {
  const out = { ...entry };
  for (const k of FORBIDDEN_PII_KEYS) {
    if (k in out) delete out[k];
  }
  for (const [k, v] of Object.entries(out)) {
    if (typeof v !== "string") continue;
    if (/@/.test(v) && /\./.test(v)) delete out[k];
    if (/^\+?[\d\s().-]{8,}$/.test(v) && /\d{5,}/.test(v.replace(/\D/g, ""))) {
      delete out[k];
    }
  }
  return out;
}

function emptyDoc() {
  return {
    version: GDI_SURFE_LIVE_GATE_V1,
    trackingVersion: GDI_SURFE_USAGE_TRACKING_V1_1,
    durability: {
      store: "filesystem",
      mechanism: "exclusive_lock_atomic_json_plus_jsonl",
      multiReplica: "REQUIRES_SHARED_VOLUME",
      note:
        "Survives process restart on the same host/volume. Multiple app instances need a shared persistent volume; Airtable was not selected for V1.",
    },
    entries: [],
    accountingFailures: [],
    historicalNote:
      "Only events recorded after Surfe Usage Tracking V1 are complete operational telemetry. Do not invent pre-tracking history.",
  };
}

function rebuildEntriesFromJsonl(eventsPath) {
  if (!fs.existsSync(eventsPath)) return [];
  const lines = fs.readFileSync(eventsPath, "utf8").split(/\r?\n/).filter(Boolean);
  const byId = new Map();
  for (const line of lines) {
    try {
      const row = JSON.parse(line);
      if (!row || typeof row !== "object") continue;
      if (row._type === "failure") continue;
      const id = row.lookupEventId || row.clientRequestId || row.enrichmentId;
      if (!id) continue;
      byId.set(String(id), row);
    } catch {
      /* skip corrupt line */
    }
  }
  return [...byId.values()].sort((a, b) =>
    String(a.at || "").localeCompare(String(b.at || "")),
  );
}

function readAccountingDoc(env = process.env) {
  const file = accountingPath(env);
  const eventsPath = eventsJsonlPath(env);
  let doc = emptyDoc();
  if (fs.existsSync(file)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
      if (parsed && typeof parsed === "object") {
        doc = {
          ...doc,
          ...parsed,
          entries: Array.isArray(parsed.entries) ? parsed.entries : [],
          accountingFailures: Array.isArray(parsed.accountingFailures)
            ? parsed.accountingFailures
            : [],
        };
      }
    } catch {
      doc.entries = rebuildEntriesFromJsonl(eventsPath);
      doc.rebuiltFromJsonl = true;
    }
  } else if (fs.existsSync(eventsPath)) {
    doc.entries = rebuildEntriesFromJsonl(eventsPath);
    doc.rebuiltFromJsonl = true;
  }
  return { file, eventsPath, failuresPath: failuresJsonlPath(env), doc };
}

function findDuplicate(doc, cleaned) {
  const entries = Array.isArray(doc.entries) ? doc.entries : [];
  if (cleaned.enrichmentId) {
    const byEnrich = entries.find(
      (e) => e && e.enrichmentId && e.enrichmentId === cleaned.enrichmentId,
    );
    if (byEnrich) {
      return { entry: byEnrich, reason: "enrichmentId" };
    }
  }
  const clientRequestId = text(cleaned.clientRequestId || cleaned.idempotencyKey);
  if (clientRequestId) {
    const byClient = entries.find(
      (e) => e && e.clientRequestId && e.clientRequestId === clientRequestId,
    );
    if (byClient) {
      return { entry: byClient, reason: "clientRequestId" };
    }
  }
  return null;
}

/**
 * Append non-PII accounting row under exclusive lock.
 * Dedupes provider enrichmentId and clientRequestId (transport retries).
 * Does NOT suppress legitimate later lookups for the same person.
 */
export function appendSurfeLiveAccounting(entry = {}, env = process.env) {
  const { file, eventsPath, doc } = readAccountingDoc(env);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const lock = acquireSurfeAccountingLock(lockPathFor(file));
  try {
    // Re-read under lock
    const live = readAccountingDoc(env);
    const cleaned = stripForbidden(entry);
    const billing = deriveSurfeCreditsAndCost(cleaned);
    const status = classifySurfeLookupStatus(cleaned);
    const lookupEventId = text(cleaned.lookupEventId) || randomUUID();
    const clientRequestId =
      text(cleaned.clientRequestId || cleaned.idempotencyKey) || null;
    const at = text(cleaned.at) || new Date().toISOString();

    const dup = findDuplicate(live.doc, { ...cleaned, clientRequestId });
    if (dup) {
      return {
        path: live.file,
        entry: dup.entry,
        deduplicated: true,
        dedupeReason: dup.reason,
      };
    }

    const ctx =
      cleaned._enrichedContext && typeof cleaned._enrichedContext === "object"
        ? cleaned._enrichedContext
        : enrichSurfeUsageContext({
            hotelId: cleaned.hotelId,
            opportunityId: cleaned.opportunityId,
            opportunity: cleaned.opportunity || null,
            dealalityUser: cleaned.dealalityUser || null,
            body: cleaned,
            env,
          });

    const dataEnvironment =
      text(cleaned.dataEnvironment) || ctx.dataEnvironment || resolveDataEnvironment(env);
    const usageClass =
      text(cleaned.usageClass) ||
      classifyUsageClass(
        {
          ...cleaned,
          ...ctx,
          dataEnvironment,
        },
        env,
      );

    const safe = {
      schema: GDI_SURFE_USAGE_TRACKING_V1_2,
      lookupEventId,
      clientRequestId,
      at,
      hotelId: cleaned.hotelId || ctx.hotelId || null,
      hotelName: text(cleaned.hotelName) || ctx.hotelName || null,
      hotelCity: text(cleaned.hotelCity) || ctx.hotelCity || null,
      hotelMarket: text(cleaned.hotelMarket) || ctx.hotelMarket || null,
      hotelCountry: text(cleaned.hotelCountry) || ctx.hotelCountry || null,
      hotelLocationLine:
        text(cleaned.hotelLocationLine) || ctx.hotelLocationLine || null,
      hotelResolution: ctx.hotelResolution || null,
      hotelIdentityInvestigationFlag:
        cleaned.hotelIdentityInvestigationFlag === true ||
        ctx.hotelIdentityInvestigationFlag === true,
      portfolioId: cleaned.portfolioId || cleaned.customerId || null,
      opportunityId: cleaned.opportunityId || ctx.opportunityId || null,
      opportunityTitle:
        text(cleaned.opportunityTitle) || ctx.opportunityTitle || null,
      opportunityOrganizationName:
        text(cleaned.opportunityOrganizationName) ||
        ctx.opportunityOrganizationName ||
        null,
      opportunityCategory:
        text(cleaned.opportunityCategory) || ctx.opportunityCategory || null,
      opportunityDrawerHref:
        text(cleaned.opportunityDrawerHref) || ctx.opportunityDrawerHref || null,
      opportunityResolution: ctx.opportunityResolution || null,
      opportunityIdentityInvestigationFlag:
        cleaned.opportunityIdentityInvestigationFlag === true ||
        ctx.opportunityIdentityInvestigationFlag === true,
      requestingPortfolioId:
        text(cleaned.requestingPortfolioId) ||
        ctx.requestingPortfolioId ||
        null,
      requestingHotelOrOfficeId:
        text(cleaned.requestingHotelOrOfficeId) ||
        ctx.requestingHotelOrOfficeId ||
        null,
      requestingOrganizationName:
        text(cleaned.requestingOrganizationName) ||
        ctx.requestingOrganizationName ||
        null,
      requestingOfficeMarket:
        text(cleaned.requestingOfficeMarket) ||
        ctx.requestingOfficeMarket ||
        null,
      requestingOfficeCity:
        text(cleaned.requestingOfficeCity) ||
        ctx.requestingOfficeCity ||
        null,
      originResolution: ctx.originResolution || null,
      requestingUserHash: cleaned.requestingUserHash || hashUserId(cleaned.userId) || null,
      identityKey: cleaned.identityKey || null,
      lookupType: cleaned.lookupType || cleaned.channel || null,
      channel: cleaned.channel || null,
      status,
      emailFound: cleaned.emailReturned === true,
      phoneFound: cleaned.phoneReturned === true,
      emailReturned: cleaned.emailReturned === true,
      phoneReturned: cleaned.phoneReturned === true,
      creditsCharged: billing.creditsCharged,
      creditsConfirmed: billing.creditsConfirmed,
      creditsEstimated: billing.creditsEstimated,
      creditCertainty: billing.creditCertainty,
      emailCreditsCharged: billing.emailCreditsCharged,
      mobileCreditsCharged: billing.mobileCreditsCharged,
      costEstimateUsd: billing.costEstimateUsd,
      costConfirmedUsd: billing.costConfirmedUsd,
      costEstimatedUsd: billing.costEstimatedUsd,
      costCertainty: billing.costCertainty,
      costEstimateSource: billing.costEstimateSource,
      attributionWarning: billing.attributionWarning,
      providerLatencyMs: Number.isFinite(Number(cleaned.providerLatencyMs))
        ? Number(cleaned.providerLatencyMs)
        : null,
      errorCategory: cleaned.error || cleaned.errorCategory || null,
      enrichmentId: cleaned.enrichmentId || null,
      providerHttpStatus: cleaned.providerHttpStatus ?? null,
      miss: cleaned.miss === true,
      ok: cleaned.ok === true,
      error: cleaned.error || null,
      creditsBefore: cleaned.creditsBefore || null,
      creditsAfter: cleaned.creditsAfter || null,
      surfeAttempted: cleaned.surfeAttempted !== false,
      surface: cleaned.surface || "gdi_drawer",
      resolvedVia: cleaned.resolvedVia || null,
      dataEnvironment,
      usageClass,
      airtableSyncStatus: text(cleaned.airtableSyncStatus) || "pending",
      schemaVersion: GDI_SURFE_USAGE_TRACKING_V1_2,
    };

    live.doc.entries = Array.isArray(live.doc.entries) ? live.doc.entries : [];
    live.doc.entries.push(safe);
    live.doc.updatedAt = at;
    live.doc.trackingVersion = GDI_SURFE_USAGE_TRACKING_V1_2;
    live.doc.durability = emptyDoc().durability;
    atomicWriteJson(live.file, live.doc);
    appendJsonl(eventsPath, { _type: "event", ...safe });
    return {
      path: live.file,
      entry: safe,
      deduplicated: false,
      eventsPath,
    };
  } finally {
    lock.release();
  }
}

export function recordSurfeAccountingFailure(failure = {}, env = process.env) {
  const row = stripForbidden({
    _type: "failure",
    at: new Date().toISOString(),
    category: text(failure.category || failure.error || "ACCOUNTING_WRITE_FAILED"),
    message: text(failure.message).slice(0, 160),
    hotelId: failure.hotelId || null,
    opportunityId: failure.opportunityId || null,
    clientRequestId: failure.clientRequestId || null,
    enrichmentId: failure.enrichmentId || null,
  });
  const failuresPath = failuresJsonlPath(env);
  const file = accountingPath(env);
  try {
    appendJsonl(failuresPath, row);
  } catch {
    /* last resort */
  }
  try {
    const lock = acquireSurfeAccountingLock(lockPathFor(file));
    try {
      const live = readAccountingDoc(env);
      live.doc.accountingFailures = Array.isArray(live.doc.accountingFailures)
        ? live.doc.accountingFailures
        : [];
      live.doc.accountingFailures.push(row);
      if (live.doc.accountingFailures.length > 200) {
        live.doc.accountingFailures = live.doc.accountingFailures.slice(-200);
      }
      live.doc.updatedAt = row.at;
      atomicWriteJson(live.file, live.doc);
    } finally {
      lock.release();
    }
  } catch {
    /* already have jsonl */
  }
  try {
    console.warn(
      "[gdi-surfe-usage] accounting_failure",
      JSON.stringify({
        category: row.category,
        hotelId: row.hotelId,
        opportunityId: row.opportunityId,
        clientRequestId: row.clientRequestId,
      }),
    );
  } catch {
    /* ignore */
  }
  return row;
}

/** Non-blocking wrapper — never throws into the customer reveal path. */
export function appendSurfeLiveAccountingSafe(entry = {}, env = process.env) {
  try {
    return appendSurfeLiveAccounting(entry, env);
  } catch (err) {
    const failure = recordSurfeAccountingFailure(
      {
        category: err?.code || "ACCOUNTING_WRITE_FAILED",
        message: text(err?.message || err).slice(0, 160),
        hotelId: entry.hotelId,
        opportunityId: entry.opportunityId,
        clientRequestId: entry.clientRequestId || entry.idempotencyKey,
        enrichmentId: entry.enrichmentId,
      },
      env,
    );
    return {
      ok: false,
      error: "ACCOUNTING_WRITE_FAILED",
      message: failure.message,
      failure,
    };
  }
}

export function readSurfeLiveAccounting(env = process.env) {
  return readAccountingDoc(env);
}

function dayKey(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function weekKey(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  const tmp = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = tmp.getUTCDay() || 7;
  tmp.setUTCDate(tmp.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(tmp.getUTCFullYear(), 0, 1));
  const week = Math.ceil(((tmp - yearStart) / 86400000 + 1) / 7);
  return `${tmp.getUTCFullYear()}-W${String(week).padStart(2, "0")}`;
}

function monthKey(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 7);
}

function creditForTotals(e) {
  if (e.creditCertainty === "unknown" || e.creditsCharged == null) {
    return { add: 0, unknown: true, estimated: 0, confirmed: 0 };
  }
  const n = Number(e.creditsCharged) || 0;
  if (e.creditCertainty === "confirmed") {
    return { add: n, unknown: false, estimated: 0, confirmed: n };
  }
  return { add: n, unknown: false, estimated: n, confirmed: 0 };
}

function costForTotals(e) {
  if (e.costCertainty === "unknown" || e.costEstimateUsd == null) {
    return { add: 0, unknown: true };
  }
  return { add: Number(e.costEstimateUsd) || 0, unknown: false };
}

/**
 * Aggregate non-PII Surfe usage for admin dashboard.
 */
export function summarizeSurfeLiveAccounting(options = {}, env = process.env) {
  const { doc, file, failuresPath } = readAccountingDoc(env);
  let entries = Array.isArray(doc.entries) ? doc.entries.slice() : [];

  const hotelId = text(options.hotelId) || null;
  const portfolioId = text(options.portfolioId) || null;
  const statusFilter = text(options.status).toLowerCase() || null;
  const opportunityIdFilter = text(options.opportunityId) || null;
  const requestingOrgFilter = text(options.requestingOrganization).toLowerCase() || null;
  const environmentFilter = text(options.environment || options.dataEnvironment).toLowerCase() || null;
  const usageClassFilter = text(options.usageClass) || null;
  const includeFixtures = options.includeFixtures === true;
  const fromMs = options.from ? Date.parse(options.from) : null;
  const toMs = options.to ? Date.parse(options.to) : null;

  entries = entries.filter((e) => {
    if (!e || typeof e !== "object") return false;
    if (e.surfeAttempted === false) return false;
    const usageClass = classifyUsageClass(e, env);
    if (!includeFixtures && usageClass === "FIXTURE_TEST" && options.excludeFixtures !== false) {
      // Default dashboard: keep fixtures visible but tagged; use excludeFixtures=true to hide.
    }
    if (options.hideFixtures === true && usageClass === "FIXTURE_TEST") return false;
    if (hotelId && e.hotelId !== hotelId) return false;
    if (portfolioId && e.portfolioId !== portfolioId) return false;
    if (opportunityIdFilter && e.opportunityId !== opportunityIdFilter) return false;
    if (requestingOrgFilter) {
      const org = String(
        e.requestingOrganizationName || e.portfolioId || "",
      ).toLowerCase();
      if (!org.includes(requestingOrgFilter)) return false;
    }
    if (environmentFilter) {
      const de = String(e.dataEnvironment || "").toLowerCase();
      const uc = String(usageClass).toLowerCase();
      if (
        environmentFilter === "test" ||
        environmentFilter === "fixture_test"
      ) {
        if (uc !== "fixture_test" && de !== "test") return false;
      } else if (de !== environmentFilter && uc !== environmentFilter) {
        return false;
      }
    }
    if (usageClassFilter && usageClass !== usageClassFilter) return false;
    if (statusFilter) {
      const st = String(e.status || classifySurfeLookupStatus(e)).toLowerCase();
      if (st !== statusFilter) return false;
    }
    const t = Date.parse(e.at || "");
    if (fromMs && Number.isFinite(t) && t < fromMs) return false;
    if (toMs && Number.isFinite(t) && t > toMs) return false;
    return true;
  });

  const hotelNames = options.hotelNames || {};
  const totals = {
    totalLookups: entries.length,
    successfulLookups: 0,
    partialLookups: 0,
    noMatchLookups: 0,
    errorLookups: 0,
    creditsConsumed: 0,
    creditsConfirmed: 0,
    creditsEstimated: 0,
    creditsUnknownCount: 0,
    totalCostEstimateUsd: 0,
    costUnknownCount: 0,
    emailFoundCount: 0,
    phoneFoundCount: 0,
    fixtureTestCount: 0,
    developmentCount: 0,
    stagingCount: 0,
    productionCount: 0,
    accountingFailureCount: Array.isArray(doc.accountingFailures)
      ? doc.accountingFailures.length
      : 0,
    duplicateSuppressedNote:
      "Transport retries dedupe on clientRequestId / enrichmentId. Legitimate later lookups for the same person are counted. In-flight clicks are blocked server-side.",
    creditAccountingNote:
      "Confirmed = provider-reported. Estimated = success heuristic or shared balance delta. Unknown = no provider confirmation (including many misses).",
    adoptionNote:
      "FIXTURE_TEST / DEVELOPMENT / STAGING usage is never presented as customer adoption. Only PRODUCTION + resolved hotel counts toward adoption reporting.",
    accountingComplete: true,
  };

  const byDay = new Map();
  const byWeek = new Map();
  const byMonth = new Map();
  const byHotel = new Map();
  const byRequestingOrg = new Map();
  const byOpportunity = new Map();
  const errors = [];
  let latencySum = 0;
  let latencyN = 0;

  for (const e of entries) {
    const status = e.status || classifySurfeLookupStatus(e);
    const usageClass = classifyUsageClass(e, env);
    if (usageClass === "FIXTURE_TEST") totals.fixtureTestCount += 1;
    else if (usageClass === "DEVELOPMENT") totals.developmentCount += 1;
    else if (usageClass === "STAGING") totals.stagingCount += 1;
    else if (usageClass === "PRODUCTION") totals.productionCount += 1;

    if (status === "success") totals.successfulLookups += 1;
    else if (status === "partial") totals.partialLookups += 1;
    else if (status === "no_match") totals.noMatchLookups += 1;
    else totals.errorLookups += 1;

    if (e.emailFound === true || e.emailReturned === true) totals.emailFoundCount += 1;
    if (e.phoneFound === true || e.phoneReturned === true) totals.phoneFoundCount += 1;

    const c = creditForTotals(e);
    const cost = costForTotals(e);
    if (c.unknown) totals.creditsUnknownCount += 1;
    else {
      totals.creditsConsumed += c.add;
      totals.creditsConfirmed += c.confirmed;
      totals.creditsEstimated += c.estimated;
    }
    if (cost.unknown) totals.costUnknownCount += 1;
    else totals.totalCostEstimateUsd += cost.add;

    if (Number.isFinite(Number(e.providerLatencyMs))) {
      latencySum += Number(e.providerLatencyMs);
      latencyN += 1;
    }

    const dk = dayKey(e.at);
    const wk = weekKey(e.at);
    const mk = monthKey(e.at);
    for (const [map, key] of [
      [byDay, dk],
      [byWeek, wk],
      [byMonth, mk],
    ]) {
      if (!key) continue;
      if (!map.has(key)) {
        map.set(key, {
          key,
          lookups: 0,
          success: 0,
          unsuccessful: 0,
          credits: 0,
          costUsd: 0,
        });
      }
      const row = map.get(key);
      row.lookups += 1;
      if (status === "success" || status === "partial") row.success += 1;
      else row.unsuccessful += 1;
      if (!c.unknown) row.credits += c.add;
      if (!cost.unknown) row.costUsd += cost.add;
    }

    const hid = e.hotelId || "_unknown";
    const hotelDisplay =
      (e.hotelName && e.hotelName !== "UNKNOWN"
        ? e.hotelName
        : null) ||
      hotelNames[hid] ||
      (hid.startsWith("hotel_") ? hid : null) ||
      "UNKNOWN";
    if (!byHotel.has(hid)) {
      byHotel.set(hid, {
        hotelId: hid,
        hotelName: hotelDisplay,
        hotelCity: e.hotelCity || null,
        hotelMarket: e.hotelMarket || null,
        hotelCountry: e.hotelCountry || null,
        hotelLocationLine: e.hotelLocationLine || null,
        identityInvestigationFlag: e.hotelIdentityInvestigationFlag === true,
        portfolioId: e.portfolioId || null,
        usageClass,
        lookups: 0,
        emailSearches: 0,
        phoneSearches: 0,
        bothSearches: 0,
        successfulLookups: 0,
        creditsConsumed: 0,
        costEstimateUsd: 0,
      });
    }
    const h = byHotel.get(hid);
    h.lookups += 1;
    const lt = String(e.lookupType || e.channel || "").toLowerCase();
    if (lt === "email") h.emailSearches += 1;
    else if (lt === "phone" || lt === "mobile") h.phoneSearches += 1;
    else h.bothSearches += 1;
    if (status === "success" || status === "partial") h.successfulLookups += 1;
    if (!c.unknown) h.creditsConsumed += c.add;
    if (!cost.unknown) h.costEstimateUsd += cost.add;
    if (e.portfolioId) h.portfolioId = e.portfolioId;

    const orgKey =
      text(e.requestingOrganizationName) &&
      e.requestingOrganizationName !== "UNKNOWN"
        ? e.requestingOrganizationName
        : text(e.requestingPortfolioId) && e.requestingPortfolioId !== "UNKNOWN"
          ? e.requestingPortfolioId
          : "_unknown";
    if (!byRequestingOrg.has(orgKey)) {
      byRequestingOrg.set(orgKey, {
        requestingOrganizationName: orgKey === "_unknown" ? "UNKNOWN" : orgKey,
        requestingPortfolioId: e.requestingPortfolioId || null,
        requestingHotelOrOfficeId: e.requestingHotelOrOfficeId || null,
        requestingOfficeMarket: e.requestingOfficeMarket || null,
        requestingOfficeCity: e.requestingOfficeCity || null,
        hotelsResearched: new Set(),
        lookups: 0,
        creditsConsumed: 0,
        costEstimateUsd: 0,
      });
    }
    const orgRow = byRequestingOrg.get(orgKey);
    orgRow.lookups += 1;
    if (e.hotelId) orgRow.hotelsResearched.add(e.hotelId);
    if (!c.unknown) orgRow.creditsConsumed += c.add;
    if (!cost.unknown) orgRow.costEstimateUsd += cost.add;

    const oid = e.opportunityId || "_unknown";
    if (!byOpportunity.has(oid)) {
      byOpportunity.set(oid, {
        opportunityId: oid,
        opportunityTitle:
          e.opportunityTitle && e.opportunityTitle !== "UNKNOWN"
            ? e.opportunityTitle
            : "UNKNOWN",
        opportunityOrganizationName:
          e.opportunityOrganizationName &&
          e.opportunityOrganizationName !== "UNKNOWN"
            ? e.opportunityOrganizationName
            : null,
        opportunityCategory:
          e.opportunityCategory && e.opportunityCategory !== "UNKNOWN"
            ? e.opportunityCategory
            : null,
        hotelId: e.hotelId || null,
        hotelName: hotelDisplay,
        hotelMarket: e.hotelMarket || null,
        hotelCity: e.hotelCity || null,
        hotelCountry: e.hotelCountry || null,
        lookups: 0,
        creditsConsumed: 0,
        costEstimateUsd: 0,
        drawerHref: e.opportunityDrawerHref || null,
      });
    }
    const oppRow = byOpportunity.get(oid);
    oppRow.lookups += 1;
    if (!c.unknown) oppRow.creditsConsumed += c.add;
    if (!cost.unknown) oppRow.costEstimateUsd += cost.add;

    if (status === "error") {
      errors.push({
        at: e.at,
        hotelId: e.hotelId,
        hotelName: hotelDisplay,
        opportunityId: e.opportunityId,
        opportunityTitle: e.opportunityTitle || null,
        errorCategory: e.errorCategory || e.error || "ERROR",
        lookupEventId: e.lookupEventId,
      });
    }
  }

  if (totals.accountingFailureCount > 0) {
    totals.accountingComplete = false;
  }

  const successLike = totals.successfulLookups + totals.partialLookups;
  totals.successRate =
    totals.totalLookups > 0
      ? Math.round((successLike / totals.totalLookups) * 1000) / 10
      : 0;
  const knownCostLookups = successLike - Math.min(successLike, totals.costUnknownCount);
  totals.avgCostPerSuccessfulLookupUsd =
    knownCostLookups > 0
      ? Math.round((totals.totalCostEstimateUsd / knownCostLookups) * 10000) / 10000
      : null;
  totals.avgLatencyMs = latencyN > 0 ? Math.round(latencySum / latencyN) : null;
  totals.totalCostEstimateUsd =
    Math.round(totals.totalCostEstimateUsd * 10000) / 10000;
  totals.creditsConsumed = Math.round(totals.creditsConsumed * 1000) / 1000;
  totals.creditsConfirmed = Math.round(totals.creditsConfirmed * 1000) / 1000;
  totals.creditsEstimated = Math.round(totals.creditsEstimated * 1000) / 1000;

  const mapToArr = (m) =>
    [...m.values()]
      .map((r) => ({
        ...r,
        costUsd: Math.round(r.costUsd * 10000) / 10000,
      }))
      .sort((a, b) => String(a.key).localeCompare(String(b.key)));

  const latest = entries
    .slice()
    .sort((a, b) => String(b.at).localeCompare(String(a.at)))
    .slice(0, Number(options.latestLimit) || 50)
    .map((e) => {
      const usageClass = classifyUsageClass(e, env);
      const resolvedHotelName =
        (e.hotelName && e.hotelName !== "UNKNOWN" ? e.hotelName : null) ||
        hotelNames[e.hotelId] ||
        "UNKNOWN";
      return {
        lookupEventId: e.lookupEventId,
        clientRequestId: e.clientRequestId || null,
        at: e.at,
        hotelId: e.hotelId,
        hotelName: resolvedHotelName,
        hotelCity: e.hotelCity || null,
        hotelMarket: e.hotelMarket || null,
        hotelCountry: e.hotelCountry || null,
        hotelLocationLine: e.hotelLocationLine || null,
        hotelIdentityInvestigationFlag:
          e.hotelIdentityInvestigationFlag === true ||
          resolvedHotelName === "UNKNOWN",
        opportunityId: e.opportunityId,
        opportunityTitle:
          e.opportunityTitle && e.opportunityTitle !== "UNKNOWN"
            ? e.opportunityTitle
            : "UNKNOWN",
        opportunityOrganizationName:
          e.opportunityOrganizationName &&
          e.opportunityOrganizationName !== "UNKNOWN"
            ? e.opportunityOrganizationName
            : null,
        opportunityCategory:
          e.opportunityCategory && e.opportunityCategory !== "UNKNOWN"
            ? e.opportunityCategory
            : null,
        opportunityDrawerHref: e.opportunityDrawerHref || null,
        opportunityIdentityInvestigationFlag:
          e.opportunityIdentityInvestigationFlag === true ||
          !(e.opportunityTitle && e.opportunityTitle !== "UNKNOWN"),
        requestingOrganizationName:
          e.requestingOrganizationName &&
          e.requestingOrganizationName !== "UNKNOWN"
            ? e.requestingOrganizationName
            : "UNKNOWN",
        requestingHotelOrOfficeId:
          e.requestingHotelOrOfficeId &&
          e.requestingHotelOrOfficeId !== "UNKNOWN"
            ? e.requestingHotelOrOfficeId
            : "UNKNOWN",
        requestingOfficeMarket:
          e.requestingOfficeMarket && e.requestingOfficeMarket !== "UNKNOWN"
            ? e.requestingOfficeMarket
            : "UNKNOWN",
        lookupType: e.lookupType || e.channel,
        status: e.status || classifySurfeLookupStatus(e),
        emailFound: e.emailFound === true || e.emailReturned === true,
        phoneFound: e.phoneFound === true || e.phoneReturned === true,
        creditsCharged: e.creditsCharged ?? null,
        creditCertainty: e.creditCertainty || null,
        costEstimateUsd: e.costEstimateUsd ?? null,
        costCertainty: e.costCertainty || null,
        providerLatencyMs: e.providerLatencyMs ?? null,
        errorCategory: e.errorCategory || e.error || null,
        resolvedVia: e.resolvedVia || null,
        dataEnvironment: e.dataEnvironment || resolveDataEnvironment(env),
        usageClass,
        airtableSyncStatus: e.airtableSyncStatus || "pending",
      };
    });

  const recentFailures = (doc.accountingFailures || []).slice(-30).reverse();
  const syncHealth = airtableSyncHealthFromLedger(doc);

  return {
    version: GDI_SURFE_USAGE_TRACKING_V1_2,
    trackingCompatVersion: GDI_SURFE_USAGE_TRACKING_V1_1,
    accountingPath: file,
    failuresPath,
    durability: doc.durability || emptyDoc().durability,
    trackingStartedNote:
      doc.historicalNote ||
      "Historical usage before tracking instrumentation is not fabricated.",
    generatedAt: new Date().toISOString(),
    filters: {
      hotelId,
      portfolioId,
      opportunityId: opportunityIdFilter,
      requestingOrganization: requestingOrgFilter,
      environment: environmentFilter,
      usageClass: usageClassFilter,
      status: statusFilter,
      from: options.from || null,
      to: options.to || null,
      hideFixtures: options.hideFixtures === true,
    },
    totals,
    trends: {
      daily: mapToArr(byDay),
      weekly: mapToArr(byWeek),
      monthly: mapToArr(byMonth),
    },
    hotels: [...byHotel.values()]
      .map((h) => ({
        ...h,
        costEstimateUsd: Math.round(h.costEstimateUsd * 10000) / 10000,
      }))
      .sort(
        (a, b) =>
          b.lookups - a.lookups ||
          String(a.hotelName).localeCompare(String(b.hotelName)),
      ),
    requestingOrganizations: [...byRequestingOrg.values()]
      .map((r) => ({
        requestingOrganizationName: r.requestingOrganizationName,
        requestingPortfolioId: r.requestingPortfolioId,
        requestingHotelOrOfficeId: r.requestingHotelOrOfficeId,
        requestingOfficeMarket: r.requestingOfficeMarket,
        requestingOfficeCity: r.requestingOfficeCity,
        hotelsResearchedCount: r.hotelsResearched?.size || 0,
        lookups: r.lookups,
        costEstimateUsd: Math.round(r.costEstimateUsd * 10000) / 10000,
        creditsConsumed: Math.round(r.creditsConsumed * 1000) / 1000,
      }))
      .sort((a, b) => b.lookups - a.lookups),
    topOpportunities: [...byOpportunity.values()]
      .map((r) => ({
        ...r,
        costEstimateUsd: Math.round(r.costEstimateUsd * 10000) / 10000,
        creditsConsumed: Math.round(r.creditsConsumed * 1000) / 1000,
      }))
      .sort((a, b) => b.lookups - a.lookups)
      .slice(0, 25),
    airtableSyncHealth: syncHealth,
    latestActivity: latest,
    recentErrors: errors.slice(-30).reverse(),
    recentAccountingFailures: recentFailures,
    noMatchFrequency:
      totals.totalLookups > 0
        ? Math.round((totals.noMatchLookups / totals.totalLookups) * 1000) / 10
        : 0,
  };
}

export {
  hashUserId,
  accountingPath,
  eventsJsonlPath,
  failuresJsonlPath,
  FORBIDDEN_PII_KEYS,
};
