/**
 * Live Surfe gate for GDI contact reveal — credit check + in-flight dedupe +
 * non-PII usage accounting for Admin → Resources → Surfe Usage.
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

export const GDI_SURFE_LIVE_GATE_V1 = "gdi-surfe-live-gate-v1";
export const GDI_SURFE_USAGE_TRACKING_V1 = "gdi-surfe-usage-tracking-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const inFlight = new Set();

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

function accountingPath(env = process.env) {
  if (env.GDI_SURFE_ACCOUNTING_PATH) return path.resolve(env.GDI_SURFE_ACCOUNTING_PATH);
  return path.join(ROOT, "data/group-demand-intelligence/surfe-usage/gdi-live-accounting-v1.json");
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
 * Derive credits charged from before/after balances when available.
 * Falls back to worst-case estimate only when a channel was returned successfully
 * and balance delta is unavailable — never invent charges for misses.
 */
export function deriveSurfeCreditsAndCost(entry = {}) {
  const channel = String(entry.channel || "both").toLowerCase();
  const emailReturned = entry.emailReturned === true;
  const phoneReturned = entry.phoneReturned === true;
  const emailDelta = creditDelta(entry.creditsBefore, entry.creditsAfter, "totalEmail");
  const mobileDelta = creditDelta(entry.creditsBefore, entry.creditsAfter, "totalMobile");

  let emailCredits = emailDelta;
  let mobileCredits = mobileDelta;
  let creditSource = "balance_delta";

  if (emailCredits == null && mobileCredits == null) {
    creditSource = "estimate_on_success_only";
    emailCredits =
      emailReturned && (channel === "email" || channel === "both")
        ? SURFE_CREDIT_COSTS.email_enrichment_per_person_worst_case
        : 0;
    mobileCredits =
      phoneReturned && (channel === "phone" || channel === "mobile" || channel === "both")
        ? SURFE_CREDIT_COSTS.mobile_enrichment_per_person_worst_case
        : 0;
    if (!emailReturned && !phoneReturned) {
      emailCredits = 0;
      mobileCredits = 0;
      creditSource = "no_charge_assumed_on_miss";
    }
  } else {
    emailCredits = emailCredits == null ? 0 : emailCredits;
    mobileCredits = mobileCredits == null ? 0 : mobileCredits;
  }

  const creditsCharged = Number(emailCredits) + Number(mobileCredits);
  const costEstimateUsd =
    Number(emailCredits) * SURFE_EMAIL_CREDIT_USD_ESTIMATE +
    Number(mobileCredits) * SURFE_MOBILE_CREDIT_USD_ESTIMATE;

  return {
    emailCreditsCharged: Number(emailCredits) || 0,
    mobileCreditsCharged: Number(mobileCredits) || 0,
    creditsCharged: Number.isFinite(creditsCharged) ? creditsCharged : 0,
    costEstimateUsd: Math.round((costEstimateUsd || 0) * 10000) / 10000,
    costEstimateSource: creditSource,
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
  // Defense: drop any string that looks like an email/phone
  for (const [k, v] of Object.entries(out)) {
    if (typeof v !== "string") continue;
    if (/@/.test(v) && /\./.test(v)) delete out[k];
    if (/^\+?[\d\s().-]{8,}$/.test(v) && /\d{5,}/.test(v.replace(/\D/g, ""))) {
      delete out[k];
    }
  }
  return out;
}

function readAccountingDoc(env = process.env) {
  const file = accountingPath(env);
  let doc = {
    version: GDI_SURFE_LIVE_GATE_V1,
    trackingVersion: GDI_SURFE_USAGE_TRACKING_V1,
    entries: [],
    historicalNote:
      "Only events recorded after Surfe Usage Tracking V1 are complete operational telemetry. Do not invent pre-tracking history.",
  };
  if (fs.existsSync(file)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
      if (parsed && typeof parsed === "object") {
        doc = {
          ...doc,
          ...parsed,
          entries: Array.isArray(parsed.entries) ? parsed.entries : [],
        };
      }
    } catch {
      /* keep empty */
    }
  }
  return { file, doc };
}

/**
 * Append non-PII accounting row. Safe for retries: identical enrichmentId within
 * a short window is treated as duplicate and not double-counted.
 */
export function appendSurfeLiveAccounting(entry = {}, env = process.env) {
  const { file, doc } = readAccountingDoc(env);
  fs.mkdirSync(path.dirname(file), { recursive: true });

  const cleaned = stripForbidden(entry);
  const billing = deriveSurfeCreditsAndCost(cleaned);
  const status = classifySurfeLookupStatus(cleaned);
  const lookupEventId = text(cleaned.lookupEventId) || randomUUID();
  const at = text(cleaned.at) || new Date().toISOString();

  // Dedupe: same enrichmentId already recorded
  if (cleaned.enrichmentId) {
    const dup = (doc.entries || []).find(
      (e) => e && e.enrichmentId && e.enrichmentId === cleaned.enrichmentId,
    );
    if (dup) {
      return { path: file, entry: dup, deduplicated: true };
    }
  }
  // Dedupe: same identityKey + channel within 15s
  const recent = (doc.entries || [])
    .filter((e) => e && e.identityKey === cleaned.identityKey && e.channel === cleaned.channel)
    .slice(-3);
  for (const e of recent) {
    const t = Date.parse(e.at || "");
    if (Number.isFinite(t) && Date.now() - t < 15_000) {
      return { path: file, entry: e, deduplicated: true };
    }
  }

  const safe = {
    schema: GDI_SURFE_USAGE_TRACKING_V1,
    lookupEventId,
    at,
    hotelId: cleaned.hotelId || null,
    portfolioId: cleaned.portfolioId || cleaned.customerId || null,
    opportunityId: cleaned.opportunityId || null,
    requestingUserHash: cleaned.requestingUserHash || hashUserId(cleaned.userId) || null,
    identityKey: cleaned.identityKey || null,
    lookupType: cleaned.lookupType || cleaned.channel || null,
    channel: cleaned.channel || null,
    status,
    emailFound: cleaned.emailReturned === true,
    phoneFound: cleaned.phoneReturned === true,
    // Legacy booleans preserved for older readers
    emailReturned: cleaned.emailReturned === true,
    phoneReturned: cleaned.phoneReturned === true,
    creditsCharged: billing.creditsCharged,
    emailCreditsCharged: billing.emailCreditsCharged,
    mobileCreditsCharged: billing.mobileCreditsCharged,
    costEstimateUsd: billing.costEstimateUsd,
    costEstimateSource: billing.costEstimateSource,
    providerLatencyMs:
      Number.isFinite(Number(cleaned.providerLatencyMs))
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
  };

  doc.entries = Array.isArray(doc.entries) ? doc.entries : [];
  doc.entries.push(safe);
  doc.updatedAt = at;
  doc.trackingVersion = GDI_SURFE_USAGE_TRACKING_V1;
  fs.writeFileSync(file, `${JSON.stringify(doc, null, 2)}\n`);
  return { path: file, entry: safe, deduplicated: false };
}

/** Non-blocking wrapper — never throws into the customer reveal path. */
export function appendSurfeLiveAccountingSafe(entry = {}, env = process.env) {
  try {
    return appendSurfeLiveAccounting(entry, env);
  } catch (err) {
    return {
      ok: false,
      error: "ACCOUNTING_WRITE_FAILED",
      message: text(err?.message || err).slice(0, 160),
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

/**
 * Aggregate non-PII Surfe usage for admin dashboard.
 */
export function summarizeSurfeLiveAccounting(options = {}, env = process.env) {
  const { doc, file } = readAccountingDoc(env);
  let entries = Array.isArray(doc.entries) ? doc.entries.slice() : [];

  const hotelId = text(options.hotelId) || null;
  const portfolioId = text(options.portfolioId) || null;
  const fromMs = options.from ? Date.parse(options.from) : null;
  const toMs = options.to ? Date.parse(options.to) : null;

  entries = entries.filter((e) => {
    if (!e || typeof e !== "object") return false;
    // Only Surfe-attempted rows (legacy rows without flag count as attempted)
    if (e.surfeAttempted === false) return false;
    if (hotelId && e.hotelId !== hotelId) return false;
    if (portfolioId && e.portfolioId !== portfolioId) return false;
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
    totalCostEstimateUsd: 0,
    emailFoundCount: 0,
    phoneFoundCount: 0,
    duplicateSuppressedNote:
      "Duplicate clicks within 15s or identical enrichmentId are not double-counted at write time.",
  };

  const byDay = new Map();
  const byWeek = new Map();
  const byMonth = new Map();
  const byHotel = new Map();
  const errors = [];
  let latencySum = 0;
  let latencyN = 0;

  for (const e of entries) {
    const status = e.status || classifySurfeLookupStatus(e);
    if (status === "success") totals.successfulLookups += 1;
    else if (status === "partial") totals.partialLookups += 1;
    else if (status === "no_match") totals.noMatchLookups += 1;
    else totals.errorLookups += 1;

    if (e.emailFound === true || e.emailReturned === true) totals.emailFoundCount += 1;
    if (e.phoneFound === true || e.phoneReturned === true) totals.phoneFoundCount += 1;

    const credits =
      Number(e.creditsCharged) ||
      deriveSurfeCreditsAndCost(e).creditsCharged ||
      0;
    const cost =
      Number(e.costEstimateUsd) ||
      deriveSurfeCreditsAndCost(e).costEstimateUsd ||
      0;
    totals.creditsConsumed += credits;
    totals.totalCostEstimateUsd += cost;

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
      row.credits += credits;
      row.costUsd += cost;
    }

    const hid = e.hotelId || "_unknown";
    if (!byHotel.has(hid)) {
      byHotel.set(hid, {
        hotelId: hid,
        hotelName: hotelNames[hid] || hid,
        portfolioId: e.portfolioId || null,
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
    h.creditsConsumed += credits;
    h.costEstimateUsd += cost;
    if (e.portfolioId) h.portfolioId = e.portfolioId;

    if (status === "error") {
      errors.push({
        at: e.at,
        hotelId: e.hotelId,
        opportunityId: e.opportunityId,
        errorCategory: e.errorCategory || e.error || "ERROR",
        lookupEventId: e.lookupEventId,
      });
    }
  }

  const successLike = totals.successfulLookups + totals.partialLookups;
  totals.successRate =
    totals.totalLookups > 0
      ? Math.round((successLike / totals.totalLookups) * 1000) / 10
      : 0;
  totals.avgCostPerSuccessfulLookupUsd =
    successLike > 0
      ? Math.round((totals.totalCostEstimateUsd / successLike) * 10000) / 10000
      : 0;
  totals.avgLatencyMs =
    latencyN > 0 ? Math.round(latencySum / latencyN) : null;
  totals.totalCostEstimateUsd =
    Math.round(totals.totalCostEstimateUsd * 10000) / 10000;
  totals.creditsConsumed = Math.round(totals.creditsConsumed * 1000) / 1000;

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
    .map((e) => ({
      lookupEventId: e.lookupEventId,
      at: e.at,
      hotelId: e.hotelId,
      hotelName: hotelNames[e.hotelId] || e.hotelId,
      opportunityId: e.opportunityId,
      lookupType: e.lookupType || e.channel,
      status: e.status || classifySurfeLookupStatus(e),
      emailFound: e.emailFound === true || e.emailReturned === true,
      phoneFound: e.phoneFound === true || e.phoneReturned === true,
      creditsCharged: e.creditsCharged ?? null,
      costEstimateUsd: e.costEstimateUsd ?? null,
      providerLatencyMs: e.providerLatencyMs ?? null,
      errorCategory: e.errorCategory || e.error || null,
      resolvedVia: e.resolvedVia || null,
    }));

  return {
    version: GDI_SURFE_USAGE_TRACKING_V1,
    accountingPath: file,
    trackingStartedNote:
      doc.historicalNote ||
      "Historical usage before tracking instrumentation is not fabricated.",
    generatedAt: new Date().toISOString(),
    filters: { hotelId, portfolioId, from: options.from || null, to: options.to || null },
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
      .sort((a, b) => b.lookups - a.lookups || String(a.hotelName).localeCompare(String(b.hotelName))),
    latestActivity: latest,
    recentErrors: errors.slice(-30).reverse(),
    noMatchFrequency:
      totals.totalLookups > 0
        ? Math.round((totals.noMatchLookups / totals.totalLookups) * 1000) / 10
        : 0,
  };
}

export { hashUserId, accountingPath, FORBIDDEN_PII_KEYS };
