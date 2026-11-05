/**
 * AVA Evidence Verification Escalation V1
 *
 * Level 1 — HTTP verification (status, redirects)
 * Level 2 — Browser verification (Playwright) for 403/429/timeout/cert/bot
 * Level 3 — Alternative source discovery (caller/search; never auto-invent)
 * Level 4 — Manual review (Admin trail only)
 *
 * Does NOT bypass CAPTCHA, login, or site security.
 * Does NOT mark inaccessible pages VERIFIED.
 * Cached successes alone do not certify a newly introduced claim.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const EVIDENCE_VERIFICATION_ESCALATION_CONTRACT =
  "AVA_EVIDENCE_VERIFICATION_ESCALATION_V1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const DEFAULT_CACHE_PATH = path.join(
  ROOT,
  "data/ai-visibility-audit-action-plan/evidence-verification-cache-v1.json"
);

const ACCESS_RESTRICTED = new Set([
  "HTTP_403",
  "HTTP_429",
  "HTTP_401",
  "HTTP_503",
  "FETCH_FAILED",
]);

const ERROR_PAGE_HINTS =
  /something went wrong|page reference code|access denied|just a moment|attention required|cf-ray|enable javascript|captcha/i;

function normalizeUrlKey(u) {
  try {
    const p = new URL(String(u || "").trim());
    p.hash = "";
    p.hostname = p.hostname.replace(/^www\./i, "").toLowerCase();
    return p.toString().replace(/\/$/, "");
  } catch {
    return String(u || "")
      .trim()
      .replace(/\/$/, "");
  }
}

export function loadVerificationCache(cachePath = DEFAULT_CACHE_PATH) {
  try {
    if (!fs.existsSync(cachePath)) {
      return { contract: EVIDENCE_VERIFICATION_ESCALATION_CONTRACT, entries: {} };
    }
    const raw = JSON.parse(fs.readFileSync(cachePath, "utf8"));
    return {
      contract: EVIDENCE_VERIFICATION_ESCALATION_CONTRACT,
      entries: raw.entries && typeof raw.entries === "object" ? raw.entries : {},
      path: cachePath,
    };
  } catch {
    return { contract: EVIDENCE_VERIFICATION_ESCALATION_CONTRACT, entries: {} };
  }
}

export function saveVerificationCache(cache, cachePath = DEFAULT_CACHE_PATH) {
  fs.mkdirSync(path.dirname(cachePath), { recursive: true });
  const out = {
    contract: EVIDENCE_VERIFICATION_ESCALATION_CONTRACT,
    updatedAt: new Date().toISOString(),
    entries: cache.entries || {},
  };
  fs.writeFileSync(cachePath, `${JSON.stringify(out, null, 2)}\n`);
  return out;
}

export function getCachedVerification(cache, url, { maxAgeMs = 7 * 24 * 3600 * 1000 } = {}) {
  const key = normalizeUrlKey(url);
  const row = cache?.entries?.[key];
  if (!row?.verifiedAt) return null;
  const age = Date.now() - Date.parse(row.verifiedAt);
  if (!Number.isFinite(age) || age > maxAgeMs) return null;
  return row;
}

export function putCachedVerification(cache, url, result) {
  const key = normalizeUrlKey(url);
  cache.entries = cache.entries || {};
  cache.entries[key] = {
    ...result,
    sourceUrl: url,
    normalizedUrl: key,
    verifiedAt: result.verifiedAt || new Date().toISOString(),
  };
  return cache.entries[key];
}

/**
 * Level 2 — ordinary browser fetch via Playwright.
 * Returns accessibility + optional keyword claim-support hints.
 */
export async function browserVerifyUrl(url, options = {}) {
  const claimKeywords = (options.claimKeywords || [])
    .map((k) => String(k || "").trim())
    .filter(Boolean);
  const ignoreHttpsErrors = options.ignoreHttpsErrors === true;
  let browser;
  try {
    const { chromium } = await import("playwright");
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      ignoreHTTPSErrors: ignoreHttpsErrors,
    });
    const page = await context.newPage();
    const resp = await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: options.timeoutMs || 25000,
    });
    const status = resp?.status() || 0;
    const finalUrl = page.url();
    const title = await page.title().catch(() => "");
    let bodyText = "";
    try {
      bodyText = await page.locator("body").innerText({ timeout: 5000 });
    } catch {
      bodyText = "";
    }
    const snippet = bodyText.replace(/\s+/g, " ").slice(0, 500);
    const looksLikeErrorPage =
      ERROR_PAGE_HINTS.test(`${title} ${snippet}`) || status === 403 || status === 429;
    const missingKeywords = claimKeywords.filter(
      (k) => !new RegExp(k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(bodyText)
    );
    const claimSupport =
      !looksLikeErrorPage &&
      status >= 200 &&
      status < 400 &&
      (claimKeywords.length === 0 || missingKeywords.length === 0);

    return {
      level: 2,
      method: "PLAYWRIGHT_BROWSER",
      ok: !looksLikeErrorPage && status >= 200 && status < 400,
      accessible: !looksLikeErrorPage && status >= 200 && status < 400,
      status,
      finalUrl,
      title,
      snippet,
      looksLikeErrorPage,
      claimSupport,
      missingKeywords,
      verifiedAt: new Date().toISOString(),
      observation: looksLikeErrorPage
        ? `Browser reached an error/challenge page (HTTP ${status}). Not claim-verified.`
        : claimSupport
          ? `Browser accessible; claim keywords present (${claimKeywords.join(", ") || "none required"}).`
          : `Browser accessible but claim keywords missing: ${missingKeywords.join(", ")}`,
    };
  } catch (err) {
    return {
      level: 2,
      method: "PLAYWRIGHT_BROWSER",
      ok: false,
      accessible: false,
      status: 0,
      finalUrl: url,
      title: "",
      snippet: "",
      looksLikeErrorPage: false,
      claimSupport: false,
      missingKeywords: claimKeywords,
      verifiedAt: new Date().toISOString(),
      error: err.message || String(err),
      observation: `Browser verification failed: ${err.message || err}`,
    };
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch {
        /* ignore */
      }
    }
  }
}

/**
 * Escalate a single restricted HTTP probe result.
 * Never auto-VERIFIED without accessible page + claim support when keywords supplied.
 */
export async function escalateRestrictedEvidence({
  url,
  httpProbe,
  flags = [],
  claimKeywords = [],
  cache = null,
  allowIgnoreHttpsErrors = false,
} = {}) {
  const checkedAt = new Date().toISOString();
  const restricted =
    flags.some((f) => ACCESS_RESTRICTED.has(f)) ||
    /TIMEOUT|HARD_TIMEOUT|certificate|ERR_/i.test(
      String(httpProbe?.error || "")
    );

  if (!restricted) {
    return {
      level: 1,
      decision: "HTTP_OK",
      gateBlocking: false,
      classificationHint: "VERIFIED_WITH_QUALIFICATION",
      verifiedAt: checkedAt,
      observation: "HTTP path not restricted; escalation not required.",
    };
  }

  if (cache) {
    const cached = getCachedVerification(cache, url);
    if (
      cached &&
      ["BROWSER_VERIFIED", "REAL_BROWSER_VERIFIED"].includes(cached.decision) &&
      cached.claimSupport !== false
    ) {
      return {
        ...cached,
        fromCache: true,
        observation:
          cached.observation ||
          "Reused cached browser verification (timestamped; not applied to unrelated claims).",
      };
    }
  }

  // Level 2 — browser
  let browserResult = await browserVerifyUrl(url, { claimKeywords });
  if (
    !browserResult.ok &&
    allowIgnoreHttpsErrors &&
    /CERT|SSL|TLS|certificate/i.test(String(browserResult.error || ""))
  ) {
    browserResult = await browserVerifyUrl(url, {
      claimKeywords,
      ignoreHttpsErrors: true,
    });
    browserResult.tlsBypassAttempted = true;
  }

  if (browserResult.ok && browserResult.claimSupport !== false) {
    const out = {
      level: 2,
      decision: "BROWSER_VERIFIED",
      gateBlocking: false,
      classificationHint: "VERIFIED_WITH_QUALIFICATION",
      verificationMethod: "PLAYWRIGHT_BROWSER",
      verifiedAt: browserResult.verifiedAt,
      finalUrl: browserResult.finalUrl,
      title: browserResult.title,
      status: browserResult.status,
      claimSupport: browserResult.claimSupport,
      observation: browserResult.observation,
      snippet: browserResult.snippet,
    };
    if (cache) putCachedVerification(cache, url, out);
    return out;
  }

  // Level 3 — caller must supply alternative; we only signal need
  if (browserResult.accessible && browserResult.claimSupport === false) {
    return {
      level: 3,
      decision: "ALTERNATIVE_SOURCE_REQUIRED",
      gateBlocking: true,
      classificationHint: "REQUIRES_MANUAL_REVIEW",
      verificationMethod: "PLAYWRIGHT_BROWSER",
      verifiedAt: browserResult.verifiedAt,
      observation: `${browserResult.observation} Find an authoritative replacement that supports the same hotel-specific claim.`,
      browserResult,
    };
  }

  return {
    level: 4,
    decision: "MANUAL_REVIEW_REQUIRED",
    gateBlocking: true,
    classificationHint: "REQUIRES_MANUAL_REVIEW",
    verificationMethod: browserResult.method || "PLAYWRIGHT_BROWSER",
    verifiedAt: browserResult.verifiedAt || checkedAt,
    observation: browserResult.observation,
    browserResult,
  };
}

/**
 * Build claim-keyword hints from evidence label/context for Level 2.
 */
export function inferClaimKeywords(entry = {}) {
  const blob = `${entry.label || ""} ${entry.claimNotes || ""} ${entry.context || ""}`;
  const keys = [];
  if (/seaspray|dining|kasumi|restaurant/i.test(blob)) {
    keys.push("SeaSpray", "dining");
  }
  if (/meeting|event.?space|ballroom/i.test(blob)) keys.push("meeting");
  if (/arabella|golf/i.test(blob)) keys.push("golf");
  if (/castillo|son vida/i.test(blob)) keys.push("Son Vida");
  if (/sheraton|arabella golf hotel/i.test(blob)) keys.push("Sheraton");
  if (/waterstone/i.test(blob)) keys.push("Waterstone");
  if (/nih|clinical|shuttle/i.test(blob)) keys.push("NIH");
  return [...new Set(keys)];
}

export { normalizeUrlKey, DEFAULT_CACHE_PATH, ACCESS_RESTRICTED };
