/**
 * Findymail REST adapter — server-side only.
 * Permanent GDI Stage 4 professional-email enrichment provider.
 *
 * Never import from browser/public code. Never log or serialize the API key.
 * Surfe remains a separate explicit-click transient path — do not conflate.
 *
 * Env: FINDYMAIL_API_KEY
 * Docs: https://app.findymail.com/docs/
 * Auth: Authorization Bearer
 * Base: https://app.findymail.com/api
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";

export const GDI_FINDYMAIL_CLIENT_V1 = "gdi-findymail-client-v1";
export const FINDYMAIL_API_BASE = "https://app.findymail.com/api";

export const FINDYMAIL_CREDIT_COSTS = Object.freeze({
  find_from_name_when_found: 1,
  find_from_name_when_not_found: 0,
  find_phone_when_found: 10,
  find_phone_when_not_found: 0,
  verify_email: 1,
  docs: {
    index: "https://app.findymail.com/docs/",
    find_from_name: "POST /api/search/name",
    find_phone: "POST /api/search/phone",
  },
});

export function getFindymailApiKeyFromEnv() {
  const key = process.env.FINDYMAIL_API_KEY;
  if (!key || !String(key).trim()) return null;
  return String(key).trim();
}

export function describeFindymailKeyPresence() {
  const key = getFindymailApiKeyFromEnv();
  if (!key) return { present: false, env_var: "FINDYMAIL_API_KEY" };
  return { present: true, env_var: "FINDYMAIL_API_KEY", len: key.length };
}

async function findymailFetch(
  urlPath,
  { method = "POST", body, apiKey, timeoutMs = 60000, retries = 1 } = {},
) {
  const key = apiKey || getFindymailApiKeyFromEnv();
  if (!key) {
    const err = new Error("FINDYMAIL_API_KEY missing");
    err.code = "MISSING_FINDYMAIL_API_KEY";
    throw err;
  }
  const url = urlPath.startsWith("http")
    ? urlPath
    : `${FINDYMAIL_API_BASE}${urlPath.startsWith("/") ? "" : "/"}${urlPath}`;

  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method,
        headers: {
          Authorization: `Bearer ${key}`,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: body != null ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      const text = await res.text();
      let payload = null;
      try {
        payload = text ? JSON.parse(text) : null;
      } catch (parseErr) {
        payload = {
          parse_error: String(parseErr?.message || parseErr),
          raw_preview: text.slice(0, 400),
        };
      }
      if (res.status === 429 && attempt < retries) {
        const waitMs = Math.min(
          60_000,
          Number(payload?.retry_after || 5) * 1000,
        );
        await new Promise((r) => setTimeout(r, waitMs));
        continue;
      }
      return { ok: res.ok, status: res.status, payload, headers: res.headers };
    } catch (err) {
      lastErr = err;
      if (attempt >= retries) break;
    } finally {
      clearTimeout(timer);
    }
  }
  const err = lastErr || new Error("Findymail request failed");
  err.code = err.code || "FINDYMAIL_REQUEST_FAILED";
  throw err;
}

/**
 * Find professional email from verified full name + company domain/name.
 * Credit: 1 finder credit when found; 0 when not found (credit-based plans).
 */
export async function findymailFindEmailFromName(
  { name, domain, webhookUrl } = {},
  opts = {},
) {
  const fullName = String(name || "").replace(/\s+/g, " ").trim();
  const company = String(domain || "").replace(/\s+/g, " ").trim();
  if (!fullName || !company) {
    const err = new Error("Findymail requires name and domain");
    err.code = "FINDYMAIL_INVALID_IDENTITY";
    throw err;
  }
  if (opts.dryRun === true) {
    return {
      version: GDI_FINDYMAIL_CLIENT_V1,
      dryRun: true,
      operation: "FIND_FROM_NAME",
      request: { name: fullName, domain: company },
      contact: null,
      email: null,
      creditsDebitedEstimate: 0,
      disposition: "DRY_RUN",
    };
  }
  const body = { name: fullName, domain: company };
  if (webhookUrl) body.webhook_url = webhookUrl;
  const res = await findymailFetch("/search/name", { body, ...opts });
  if (res.status === 402) {
    const err = new Error("Findymail: not enough credits");
    err.code = "FINDYMAIL_INSUFFICIENT_CREDITS";
    err.status = 402;
    throw err;
  }
  if (!res.ok) {
    const err = new Error(
      `Findymail find failed (${res.status}): ${JSON.stringify(res.payload)?.slice(0, 200)}`,
    );
    err.code = "FINDYMAIL_FIND_FAILED";
    err.status = res.status;
    err.payload = res.payload;
    throw err;
  }
  const contact = res.payload?.contact || res.payload?.payload?.contact || null;
  const email =
    contact?.email && String(contact.email).includes("@")
      ? String(contact.email).trim().toLowerCase()
      : null;
  return {
    version: GDI_FINDYMAIL_CLIENT_V1,
    dryRun: false,
    operation: "FIND_FROM_NAME",
    request: { name: fullName, domain: company },
    contact,
    email,
    creditsDebitedEstimate: email
      ? FINDYMAIL_CREDIT_COSTS.find_from_name_when_found
      : FINDYMAIL_CREDIT_COSTS.find_from_name_when_not_found,
    disposition: email ? "FOUND" : "NOT_FOUND",
    rawStatus: res.status,
  };
}

/**
 * Find phone from LinkedIn URL.
 * Credit: 10 finder credits when found; 0 when not found.
 * EU citizens / EU numbers: provider returns no result (do not call paid path for known EU).
 * Docs: POST /api/search/phone
 */
export async function findymailFindPhoneFromLinkedIn({ linkedinUrl } = {}, opts = {}) {
  const url = String(linkedinUrl || "")
    .trim()
    .replace(/\/$/, "");
  if (!url || !/linkedin\.com\/in\//i.test(url)) {
    const err = new Error("Findymail phone requires a LinkedIn profile URL");
    err.code = "FINDYMAIL_INVALID_LINKEDIN";
    throw err;
  }
  if (opts.dryRun === true) {
    return {
      version: GDI_FINDYMAIL_CLIENT_V1,
      dryRun: true,
      operation: "FIND_PHONE",
      request: { linkedin_url: url },
      phone: null,
      lineType: null,
      creditsDebitedEstimate: 0,
      disposition: "DRY_RUN",
    };
  }
  const res = await findymailFetch("/search/phone", {
    body: { linkedin_url: url },
    ...opts,
  });
  if (res.status === 402) {
    const err = new Error("Findymail: not enough credits");
    err.code = "FINDYMAIL_INSUFFICIENT_CREDITS";
    err.status = 402;
    throw err;
  }
  if (!res.ok) {
    const err = new Error(
      `Findymail phone find failed (${res.status}): ${JSON.stringify(res.payload)?.slice(0, 200)}`,
    );
    err.code = "FINDYMAIL_PHONE_FIND_FAILED";
    err.status = res.status;
    err.payload = res.payload;
    throw err;
  }
  const phoneRaw = res.payload?.phone ?? res.payload?.payload?.phone ?? null;
  const phone =
    phoneRaw && String(phoneRaw).replace(/\D/g, "").length >= 7
      ? String(phoneRaw).trim()
      : null;
  const lineType = res.payload?.line_type || res.payload?.lineType || null;
  return {
    version: GDI_FINDYMAIL_CLIENT_V1,
    dryRun: false,
    operation: "FIND_PHONE",
    request: { linkedin_url: url },
    phone,
    lineType,
    creditsDebitedEstimate: phone
      ? FINDYMAIL_CREDIT_COSTS.find_phone_when_found
      : FINDYMAIL_CREDIT_COSTS.find_phone_when_not_found,
    disposition: phone ? "FOUND" : "NOT_FOUND",
    rawStatus: res.status,
  };
}

/**
 * Append-only usage ledger (repo-local). Never stores API keys.
 */
export function appendFindymailUsageLedger(entry, opts = {}) {
  const root = opts.repoRoot || process.cwd();
  const ledgerPath =
    opts.ledgerPath ||
    path.join(
      root,
      "data/group-demand-intelligence/findymail-usage-ledger-v1.jsonl",
    );
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const row = {
    ledgerId: "findymail_usage_v1",
    at: new Date().toISOString(),
    ...entry,
    clientVersion: GDI_FINDYMAIL_CLIENT_V1,
  };
  fs.appendFileSync(ledgerPath, `${JSON.stringify(row)}\n`, "utf8");
  return { ledgerPath, row };
}

export function readFindymailUsageLedger(opts = {}) {
  const root = opts.repoRoot || process.cwd();
  const ledgerPath =
    opts.ledgerPath ||
    path.join(
      root,
      "data/group-demand-intelligence/findymail-usage-ledger-v1.jsonl",
    );
  if (!fs.existsSync(ledgerPath)) return [];
  return fs
    .readFileSync(ledgerPath, "utf8")
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

/**
 * Cumulative Findymail credits for a hotel across ALL authorization IDs,
 * resume/recovery paths, and optional supplemental ledgers.
 * Hard caps must use this total — never a single-run budget alone.
 *
 * @param {string} hotelId
 * @param {object[]} [ledger]
 * @param {{ repoRoot?: string, supplementalLedgerPaths?: string[] }} [opts]
 */
export function sumFindymailCreditsDebitedForHotel(hotelId, ledger = null, opts = {}) {
  const id = String(hotelId || "").trim();
  if (!id) {
    return { hotelId: null, creditsDebited: 0, rows: 0, byAuthorizationId: {} };
  }
  const rows = [];
  const primary = Array.isArray(ledger) ? ledger : readFindymailUsageLedger(opts);
  rows.push(...primary);
  for (const p of opts.supplementalLedgerPaths || []) {
    const abs = path.isAbsolute(p) ? p : path.join(opts.repoRoot || process.cwd(), p);
    if (!fs.existsSync(abs)) continue;
    rows.push(...readFindymailUsageLedger({ ...opts, ledgerPath: abs }));
  }
  let creditsDebited = 0;
  let matched = 0;
  const byAuthorizationId = {};
  const seen = new Set();
  for (const row of rows) {
    if (String(row?.hotelId || "").trim() !== id) continue;
    const fingerprint = [
      row.at || "",
      row.personKey || "",
      row.operation || "",
      row.opportunityId || "",
      row.creditsDebited || 0,
    ].join("|");
    if (seen.has(fingerprint)) continue;
    seen.add(fingerprint);
    const c = Number(row.creditsDebited || row.creditsDebitedEstimate || 0) || 0;
    if (c <= 0) continue;
    creditsDebited += c;
    matched += 1;
    const auth = String(row.authorizationId || "(none)");
    byAuthorizationId[auth] = (byAuthorizationId[auth] || 0) + c;
  }
  return { hotelId: id, creditsDebited, rows: matched, byAuthorizationId };
}

/**
 * Seed / refresh a mutable run budget from cumulative hotel ledger usage.
 * Returns whether a new debit of `estimatedCredits` would exceed hardCap.
 */
export function assertFindymailHotelHardCapAllowsDebit(opts = {}) {
  const hotelId = String(opts.hotelId || "").trim();
  const hardCap = Number(opts.creditHardCap || opts.findymailCreditBudget?.hardCap || 0);
  const estimated = Number(opts.estimatedCredits || 1) || 1;
  if (!(hardCap > 0) || !hotelId) {
    return { ok: true, skipped: true, reason: "NO_HOTEL_HARD_CAP" };
  }
  const ledger =
    opts.ledger ||
    readFindymailUsageLedger({
      repoRoot: opts.repoRoot,
      ledgerPath: opts.ledgerPath,
    });
  const sum = sumFindymailCreditsDebitedForHotel(hotelId, ledger, opts);
  const runUsed = Number(opts.findymailCreditBudget?.used || 0) || 0;
  // Cumulative = max(ledger hotel total, in-run used) so resume cannot reset.
  const used = Math.max(sum.creditsDebited, runUsed);
  if (opts.findymailCreditBudget && typeof opts.findymailCreditBudget === "object") {
    opts.findymailCreditBudget.used = used;
    opts.findymailCreditBudget.hardCap = hardCap;
    opts.findymailCreditBudget.hotelId = hotelId;
    opts.findymailCreditBudget.cumulativeFromLedger = sum.creditsDebited;
  }
  if (used + estimated > hardCap) {
    return {
      ok: false,
      reason: "CREDIT_HARD_CAP_REACHED",
      hotelId,
      hardCap,
      used,
      estimatedCredits: estimated,
      ledgerCredits: sum.creditsDebited,
      byAuthorizationId: sum.byAuthorizationId,
    };
  }
  return {
    ok: true,
    hotelId,
    hardCap,
    used,
    estimatedCredits: estimated,
    ledgerCredits: sum.creditsDebited,
    byAuthorizationId: sum.byAuthorizationId,
  };
}

/**
 * @param {string} personKey
 * @param {object[]} ledger
 * @param {{ operation?: 'FIND_FROM_NAME'|'FIND_PHONE'|'any' }} [opts]
 */
export function personAlreadyEnrichedInLedger(personKey, ledger = [], opts = {}) {
  const key = String(personKey || "")
    .toLowerCase()
    .trim();
  if (!key) return false;
  const opWanted = String(opts.operation || "FIND_FROM_NAME").toUpperCase();
  return ledger.some((row) => {
    if (String(row.personKey || "").toLowerCase() !== key) return false;
    const op = String(row.operation || "").toUpperCase();
    if (opWanted === "FIND_PHONE") {
      if (op !== "FIND_PHONE") return false;
    } else if (opWanted === "ANY") {
      if (
        op !== "FIND" &&
        op !== "FIND_FROM_NAME" &&
        op !== "FIND_PHONE"
      ) {
        return false;
      }
    } else if (op !== "FIND" && op !== "FIND_FROM_NAME") {
      return false;
    }
    if (row.resultDisposition === "ERROR" || row.resultDisposition === "SKIPPED") {
      return false;
    }
    // Provider returned an email but Stage 4 never persisted PII (channel-loss
    // bug). Allow one recovery re-query so Live E2E can re-attach the channel.
    if (
      (op === "FIND" || op === "FIND_FROM_NAME") &&
      row.emailFound === true &&
      row.piiPersisted !== true
    ) {
      return false;
    }
    if (op === "FIND_PHONE" && row.phoneFound === true && row.piiPersisted !== true) {
      return false;
    }
    return true;
  });
}
