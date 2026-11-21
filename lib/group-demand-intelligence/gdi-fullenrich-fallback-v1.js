/**
 * GDI FullEnrich selective fallback V1 — eligibility + activation gates.
 *
 * Surfe remains primary. FullEnrich is never auto-executed.
 * V1 activation: work-email only. Phone fallback stays disabled.
 *
 * Env (server-side only):
 *   GDI_FULLENRICH_FALLBACK_ENABLED=1          — offer eligibility (still needs user confirm)
 *   GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED=1 — allow paid FE after user confirm
 *   GDI_FULLENRICH_FALLBACK_PHONE_ENABLED=1    — NOT for V1 (ignored / forced off)
 *   GDI_FULLENRICH_CREDIT_RESERVE=20           — refuse when balance <= reserve
 *   GDI_FULLENRICH_SPEND_CAP=10               — max FE credits this process/session budget
 *   GDI_FULLENRICH_CUSTOMER_DISPLAY=0         — must stay 0 until licensing confirmed
 */

import {
  FULLENRICH_CREDIT_COSTS,
  getFullEnrichApiKeyFromEnv,
  getFullEnrichCreditBalance,
} from "../fullenrich/client.js";

export const GDI_FULLENRICH_FALLBACK_V1 = "gdi-fullenrich-selective-fallback-v1";
export const CONTACT_PROVIDER_SURFE = "SURFE";
export const CONTACT_PROVIDER_FULLENRICH = "FULLENRICH";

const DEFAULT_RESERVE = 20;
const DEFAULT_SPEND_CAP = 10;

function flagOn(env, key) {
  const v = String(env?.[key] ?? "").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

export function readFullEnrichFallbackFlags(env = process.env) {
  const enabled = flagOn(env, "GDI_FULLENRICH_FALLBACK_ENABLED");
  const spendAuthorized = flagOn(env, "GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED");
  // V1 hard-off: phone fallback never activates even if env is set.
  const phoneEnabled = false;
  const customerDisplay = flagOn(env, "GDI_FULLENRICH_CUSTOMER_DISPLAY");
  const reserve = Math.max(
    0,
    Number(env.GDI_FULLENRICH_CREDIT_RESERVE ?? DEFAULT_RESERVE) || DEFAULT_RESERVE,
  );
  const spendCap = Math.max(
    0,
    Number(env.GDI_FULLENRICH_SPEND_CAP ?? DEFAULT_SPEND_CAP) || DEFAULT_SPEND_CAP,
  );
  const keyFromEnvBag =
    String(env?.FULL_ENRICH_API_KEY || "").trim() || null;
  return {
    version: GDI_FULLENRICH_FALLBACK_V1,
    enabled,
    spendAuthorized,
    phoneEnabled,
    customerDisplay,
    creditReserve: reserve,
    spendCap,
    // Prefer explicit env bag (tests); fall back to process for live servers.
    apiKeyPresent: Boolean(keyFromEnvBag || getFullEnrichApiKeyFromEnv()),
  };
}

/**
 * Inspect Surfe (or combined) contactDetails for usable channels.
 */
export function inspectReachability(contactDetails = null, channel = "both") {
  const email = contactDetails?.email ? String(contactDetails.email).trim() : "";
  const phone = contactDetails?.phone ? String(contactDetails.phone).trim() : "";
  const hasEmail = Boolean(email);
  const hasPhone = Boolean(phone);
  const ch = String(channel || "both").toLowerCase();
  return {
    channel: ch,
    hasEmail,
    hasPhone,
    emailMissing: !hasEmail,
    phoneMissing: !hasPhone,
    bothAvailable: hasEmail && hasPhone,
    eitherAvailable: hasEmail || hasPhone,
  };
}

/**
 * Pure eligibility after Surfe (or primary) response.
 * Does not call FullEnrich. Does not spend credits.
 *
 * Rules (founder V1):
 * - email missing → work-email fallback may be offered (if flags)
 * - phone missing → phone offer only when phoneEnabled (V1: never)
 * - both missing → prioritize work email
 * - both available → no FullEnrich
 */
export function evaluateFullEnrichFallbackEligibility({
  contactDetails = null,
  channel = "both",
  surfeAttempted = false,
  surfeMiss = false,
  surfeOk = true,
  flags = null,
  env = process.env,
} = {}) {
  const f = flags || readFullEnrichFallbackFlags(env);
  const reach = inspectReachability(contactDetails, channel);
  const base = {
    version: GDI_FULLENRICH_FALLBACK_V1,
    eligible: false,
    channel: null,
    enrichFields: [],
    estimatedCredits: 0,
    confirmationRequired: true,
    autoExecute: false,
    reason: null,
    flags: {
      enabled: f.enabled,
      spendAuthorized: f.spendAuthorized,
      phoneEnabled: f.phoneEnabled,
      customerDisplay: f.customerDisplay,
      apiKeyPresent: f.apiKeyPresent,
    },
    reachability: reach,
  };

  if (!f.enabled) {
    return { ...base, reason: "FEATURE_FLAG_OFF" };
  }
  if (!f.apiKeyPresent) {
    return { ...base, reason: "FULLENRICH_API_KEY_MISSING" };
  }
  if (f.customerDisplay) {
    // Safety: never treat customer-display as enabling spend; still block offer
    // until licensing review explicitly sets a separate allowlist path later.
    return { ...base, reason: "CUSTOMER_DISPLAY_REQUIRES_LICENSING_REVIEW" };
  }
  if (reach.bothAvailable) {
    return { ...base, reason: "CHANNELS_ALREADY_AVAILABLE" };
  }
  if (reach.hasEmail && String(channel).toLowerCase() === "email") {
    return { ...base, reason: "EMAIL_ALREADY_AVAILABLE" };
  }
  // Fallback only after a primary Surfe attempt (founder: after Surfe response).
  if (!surfeAttempted) {
    return { ...base, reason: "SURFE_NOT_ATTEMPTED" };
  }
  // Prefer email when both missing or email channel incomplete.
  if (reach.emailMissing) {
    return {
      ...base,
      eligible: true,
      channel: "email",
      enrichFields: ["contact.work_emails"],
      estimatedCredits:
        FULLENRICH_CREDIT_COSTS.work_email_found_deliverable_high_probability_or_catchall,
      reason:
        surfeMiss || !reach.hasEmail
          ? "SURFE_EMAIL_MISS"
          : "EMAIL_MISSING_AFTER_PRIMARY",
      surfeAttempted: true,
      surfeOk: surfeOk !== false,
    };
  }
  // Phone-only gap — V1 disabled
  if (reach.phoneMissing && f.phoneEnabled) {
    return {
      ...base,
      eligible: true,
      channel: "phone",
      enrichFields: ["contact.phones"],
      estimatedCredits: FULLENRICH_CREDIT_COSTS.mobile_phone_found,
      reason: "SURFE_PHONE_MISS",
    };
  }
  if (reach.phoneMissing && !f.phoneEnabled) {
    return { ...base, reason: "PHONE_FALLBACK_DISABLED_V1" };
  }
  return { ...base, reason: "NOT_ELIGIBLE" };
}

/**
 * Whether a confirmed FE execution is allowed (still requires user confirm flag on request).
 */
export function assertFullEnrichSpendAllowed({
  flags = null,
  balance = null,
  spentThisBudget = 0,
  env = process.env,
} = {}) {
  const f = flags || readFullEnrichFallbackFlags(env);
  if (!f.enabled) {
    return { ok: false, error: "FEATURE_FLAG_OFF", message: "FullEnrich fallback is disabled." };
  }
  if (!f.spendAuthorized) {
    return {
      ok: false,
      error: "SPEND_NOT_AUTHORIZED",
      message:
        "FullEnrich spend is not authorized on this server (GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED).",
    };
  }
  if (!f.apiKeyPresent) {
    return {
      ok: false,
      error: "FULLENRICH_API_KEY_MISSING",
      message: "FullEnrich API key is not configured.",
    };
  }
  if (f.customerDisplay) {
    return {
      ok: false,
      error: "CUSTOMER_DISPLAY_BLOCKED",
      message:
        "Customer-facing FullEnrich display is blocked until licensing is confirmed.",
    };
  }
  const bal = Number(balance);
  if (!Number.isFinite(bal)) {
    return {
      ok: false,
      error: "CREDITS_UNAVAILABLE",
      message: "Could not read FullEnrich credit balance.",
    };
  }
  if (bal <= f.creditReserve) {
    return {
      ok: false,
      error: "INSUFFICIENT_CREDITS_RESERVE",
      message: `FullEnrich balance ${bal} is at or below reserve floor ${f.creditReserve}.`,
      balance: bal,
      reserve: f.creditReserve,
    };
  }
  const remainingBudget = f.spendCap - Number(spentThisBudget || 0);
  if (remainingBudget < 1) {
    return {
      ok: false,
      error: "SPEND_CAP_REACHED",
      message: `FullEnrich spend cap (${f.spendCap}) reached for this budget window.`,
      spendCap: f.spendCap,
    };
  }
  return {
    ok: true,
    balance: bal,
    reserve: f.creditReserve,
    remainingBudget,
    estimatedCredits:
      FULLENRICH_CREDIT_COSTS.work_email_found_deliverable_high_probability_or_catchall,
  };
}

export async function preflightFullEnrichCredits(env = process.env) {
  if (!getFullEnrichApiKeyFromEnv()) {
    return { ok: false, error: "FULLENRICH_API_KEY_MISSING", balance: null };
  }
  try {
    const res = await getFullEnrichCreditBalance();
    const bal =
      typeof res.payload === "number"
        ? res.payload
        : Number(res.payload?.balance ?? res.payload?.credits ?? NaN);
    if (res.http_status !== 200 || !Number.isFinite(bal)) {
      return {
        ok: false,
        error: "CREDITS_UNAVAILABLE",
        http_status: res.http_status,
        balance: null,
      };
    }
    return { ok: true, balance: bal, http_status: res.http_status };
  } catch (err) {
    return {
      ok: false,
      error: "CREDITS_UNAVAILABLE",
      message: String(err?.message || err).slice(0, 160),
      balance: null,
    };
  }
}

/** Process-local spend tracker for spendCap (resets on process restart). */
const spendState = { spent: 0 };

export function getFullEnrichSpendBudgetState() {
  return { spent: spendState.spent };
}

export function recordFullEnrichSpend(credits) {
  const n = Number(credits);
  if (Number.isFinite(n) && n > 0) spendState.spent += n;
  return getFullEnrichSpendBudgetState();
}

export function resetFullEnrichSpendBudgetForTests() {
  spendState.spent = 0;
}
