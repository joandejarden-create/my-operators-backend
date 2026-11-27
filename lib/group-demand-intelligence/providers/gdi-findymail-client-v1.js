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
  verify_email: 1,
  docs: {
    index: "https://app.findymail.com/docs/",
    find_from_name: "POST /api/search/name",
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

export function personAlreadyEnrichedInLedger(personKey, ledger = []) {
  const key = String(personKey || "")
    .toLowerCase()
    .trim();
  if (!key) return false;
  return ledger.some(
    (row) =>
      String(row.personKey || "").toLowerCase() === key &&
      (row.operation === "FIND" || row.operation === "FIND_FROM_NAME") &&
      row.resultDisposition !== "ERROR" &&
      row.resultDisposition !== "SKIPPED",
  );
}
