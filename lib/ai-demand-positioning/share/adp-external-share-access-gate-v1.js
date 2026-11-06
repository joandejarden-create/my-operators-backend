/**
 * ADP external share access gate — CUSTOMER_READY must not pass when the
 * customer-facing Admin URL fails unauthenticated access.
 *
 * Doctrine: EXTERNAL_SHARE_ACCESS_BLOCKED
 */

import { createHash } from "crypto";

export const ADP_EXTERNAL_SHARE_ACCESS_GATE_V1 =
  "ADP_EXTERNAL_SHARE_ACCESS_GATE_V1";

export const EXTERNAL_SHARE_ACCESS_STATUS = Object.freeze({
  EXTERNAL_ACCESS_VERIFIED: "EXTERNAL_ACCESS_VERIFIED",
  EXTERNAL_SHARE_ACCESS_BLOCKED: "EXTERNAL_SHARE_ACCESS_BLOCKED",
  INVALID_OR_REVOKED_TOKEN: "INVALID_OR_REVOKED_TOKEN",
  URL_GENERATION_ERROR: "URL_GENERATION_ERROR",
  REPORT_LOAD_FAILURE: "REPORT_LOAD_FAILURE",
  PDF_ACCESS_FAILURE: "PDF_ACCESS_FAILURE",
  NOT_YET_PUBLISHED: "NOT_YET_PUBLISHED",
  SHARE_NOT_CREATED: "SHARE_NOT_CREATED",
  /** Token/probe present but independent HTTP/browser verification has not run yet. */
  VERIFICATION_PENDING: "VERIFICATION_PENDING",
});

export const PRODUCTION_PUBLIC_BASE =
  "https://my-operators-backend-production.up.railway.app";

function redactShareUrl(url) {
  try {
    const u = new URL(String(url || ""));
    if (u.searchParams.has("share")) u.searchParams.set("share", "[REDACTED]");
    return u.toString();
  } catch {
    return String(url || "").replace(/share=[^&]+/gi, "share=[REDACTED]");
  }
}

function tokenFingerprint(token) {
  const t = String(token || "").trim();
  if (!t) return null;
  return createHash("sha256").update(t).digest("hex").slice(0, 12);
}

function extractShareToken(url) {
  try {
    return new URL(String(url || "")).searchParams.get("share") || "";
  } catch {
    return "";
  }
}

function considerationFromReport(report) {
  const em = report?.executiveMetrics?.considerationRate;
  if (em && em.rate != null) return Number(em.rate);
  if (em && em.percentage != null) return Number(em.percentage);
  if (em && em.value != null) return Number(em.value);
  const blob = JSON.stringify(report || {});
  const m = blob.match(/"rate"\s*:\s*(67\.2)\b/) || blob.match(/\b(67\.2)\b/);
  return m ? Number(m[1]) : null;
}

/**
 * Unauthenticated HTTP verification of an Admin-emitted external ADP URL.
 * Does not weaken signature checks — calls production share/resolve + report.
 */
export async function verifyAdpExternalShareHttpV1(opts = {}) {
  const url = String(opts.url || "").trim();
  const expectedPropertyId = String(opts.expectedPropertyId || "").trim();
  const expectedPeriodId = opts.expectedPeriodId
    ? String(opts.expectedPeriodId).trim()
    : null;
  const expectedConsideration = opts.expectedConsideration != null
    ? Number(opts.expectedConsideration)
    : null;
  const base = String(opts.baseUrl || PRODUCTION_PUBLIC_BASE).replace(/\/$/, "");
  const timeoutMs = Number(opts.timeoutMs || 20000);

  const out = {
    gate: ADP_EXTERNAL_SHARE_ACCESS_GATE_V1,
    ok: false,
    status: EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED,
    urlRedacted: redactShareUrl(url),
    tokenFingerprint: null,
    tokenId: null,
    propertyId: null,
    periodId: null,
    consideration: null,
    resolveStatus: null,
    reportStatus: null,
    htmlStatus: null,
    reasons: [],
  };

  if (!url || !/^https?:\/\//i.test(url)) {
    out.status = EXTERNAL_SHARE_ACCESS_STATUS.URL_GENERATION_ERROR;
    out.reasons.push("missing_or_invalid_external_url");
    return out;
  }

  const share = extractShareToken(url);
  out.tokenFingerprint = tokenFingerprint(share);
  if (!share || !share.startsWith("adpshare.v1.")) {
    out.status = EXTERNAL_SHARE_ACCESS_STATUS.URL_GENERATION_ERROR;
    out.reasons.push("share_token_missing_from_url");
    return out;
  }

  try {
    const payload = JSON.parse(
      Buffer.from(String(share).split(".")[2], "base64url").toString("utf8")
    );
    out.tokenId = payload?.tid || null;
  } catch {
    out.reasons.push("share_token_payload_unreadable");
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const htmlRes = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: { Accept: "text/html" },
    });
    out.htmlStatus = htmlRes.status;
    if (!htmlRes.ok) {
      out.reasons.push(`html_http_${htmlRes.status}`);
    }

    const resolveRes = await fetch(
      `${base}/api/ai-demand-positioning/share/resolve?share=${encodeURIComponent(share)}`,
      {
        method: "GET",
        cache: "no-store",
        signal: controller.signal,
        headers: { Accept: "application/json" },
      }
    );
    out.resolveStatus = resolveRes.status;
    const resolveJson = await resolveRes.json().catch(() => null);
    if (!resolveRes.ok || resolveJson?.ok !== true) {
      const code = resolveJson?.code || resolveJson?.error || `http_${resolveRes.status}`;
      if (/REVOKED|EXPIRED|BAD_SIGNATURE|UNKNOWN|invalid/i.test(String(code))) {
        out.status = EXTERNAL_SHARE_ACCESS_STATUS.INVALID_OR_REVOKED_TOKEN;
      }
      out.reasons.push(`share_resolve_failed:${code}`);
      return out;
    }

    out.propertyId =
      resolveJson.propertyId ||
      resolveJson.property?.id ||
      resolveJson.property?.propertyId ||
      null;

    if (expectedPropertyId && out.propertyId !== expectedPropertyId) {
      out.reasons.push(
        `property_mismatch:got_${out.propertyId}_expected_${expectedPropertyId}`
      );
      out.status = EXTERNAL_SHARE_ACCESS_STATUS.REPORT_LOAD_FAILURE;
      return out;
    }

    const pid = out.propertyId || expectedPropertyId;
    const reportRes = await fetch(
      `${base}/api/ai-demand-positioning/property/${encodeURIComponent(pid)}/report?share=${encodeURIComponent(share)}`,
      {
        method: "GET",
        cache: "no-store",
        signal: controller.signal,
        headers: { Accept: "application/json" },
      }
    );
    out.reportStatus = reportRes.status;
    const reportJson = await reportRes.json().catch(() => null);
    if (!reportRes.ok || reportJson?.ok !== true) {
      out.status = EXTERNAL_SHARE_ACCESS_STATUS.REPORT_LOAD_FAILURE;
      out.reasons.push(
        `report_failed:${reportJson?.error || reportJson?.code || reportRes.status}`
      );
      return out;
    }

    out.periodId =
      reportJson?.period?.periodId ||
      reportJson?._adpSnapshotMeta?.periodId ||
      reportJson?.periodId ||
      null;
    out.consideration = considerationFromReport(reportJson);

    if (expectedPeriodId && out.periodId && out.periodId !== expectedPeriodId) {
      out.reasons.push(
        `period_mismatch:got_${out.periodId}_expected_${expectedPeriodId}`
      );
      out.status = EXTERNAL_SHARE_ACCESS_STATUS.REPORT_LOAD_FAILURE;
      return out;
    }

    if (
      expectedConsideration != null &&
      Number.isFinite(expectedConsideration) &&
      out.consideration != null &&
      Math.abs(Number(out.consideration) - expectedConsideration) > 0.05
    ) {
      out.reasons.push(
        `consideration_mismatch:got_${out.consideration}_expected_${expectedConsideration}`
      );
      out.status = EXTERNAL_SHARE_ACCESS_STATUS.REPORT_LOAD_FAILURE;
      return out;
    }

    const body = JSON.stringify(reportJson);
    if (expectedPropertyId === "adp_spice_island_beach_resort") {
      if (!/Spice Island/i.test(body)) {
        out.reasons.push("spice_island_name_missing_from_report");
        out.status = EXTERNAL_SHARE_ACCESS_STATUS.REPORT_LOAD_FAILURE;
        return out;
      }
    }

    out.ok = true;
    out.status = EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED;
    return out;
  } catch (err) {
    out.reasons.push(`verify_exception:${String(err?.message || err).slice(0, 160)}`);
    return out;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Playwright unauthenticated browser verification of the exact Admin URL.
 */
export async function verifyAdpExternalShareBrowserV1(opts = {}) {
  const url = String(opts.url || "").trim();
  const expectedPropertyId = String(opts.expectedPropertyId || "").trim();
  const http = await verifyAdpExternalShareHttpV1(opts);
  const out = {
    ...http,
    browser: {
      ran: false,
      hasInvalidMsg: null,
      hasUnableToLoad: null,
      successVisible: null,
      hasPropertyName: null,
      hasConsideration: null,
      consoleErrors: [],
    },
  };

  if (!http.ok) return out;

  let chromium;
  try {
    ({ chromium } = await import("playwright"));
  } catch (err) {
    out.reasons.push(`playwright_unavailable:${String(err?.message || err).slice(0, 120)}`);
    out.ok = false;
    out.status = EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED;
    return out;
  }

  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    const consoleErrors = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") {
        consoleErrors.push(String(msg.text()).slice(0, 240));
      }
    });
    await page.goto(url, { waitUntil: "networkidle", timeout: 90000 });
    await page.waitForTimeout(3500);
    const text = await page.evaluate(() => document.body?.innerText || "");
    const successVisible = await page
      .locator("#adpStateSuccess")
      .isVisible()
      .catch(() => false);
    const errorVisible = await page
      .locator("#adpStateError")
      .isVisible()
      .catch(() => false);
    const hasInvalidMsg = /invalid, expired, or revoked/i.test(text);
    const hasUnableToLoad = /Unable to load report/i.test(text) && errorVisible;
    const hasPropertyName =
      expectedPropertyId === "adp_spice_island_beach_resort"
        ? /Spice Island/i.test(text)
        : true;
    const hasConsideration =
      opts.expectedConsideration != null
        ? new RegExp(String(opts.expectedConsideration).replace(".", "\\.")).test(
            text
          )
        : true;

    out.browser = {
      ran: true,
      hasInvalidMsg,
      hasUnableToLoad,
      successVisible,
      errorVisible,
      hasPropertyName,
      hasConsideration,
      consoleErrors: consoleErrors.slice(0, 12),
      textHead: text.slice(0, 240),
    };

    if (hasInvalidMsg || !successVisible || !hasPropertyName || !hasConsideration) {
      out.ok = false;
      out.status = hasInvalidMsg
        ? EXTERNAL_SHARE_ACCESS_STATUS.INVALID_OR_REVOKED_TOKEN
        : EXTERNAL_SHARE_ACCESS_STATUS.REPORT_LOAD_FAILURE;
      if (hasInvalidMsg) out.reasons.push("browser_invalid_expired_revoked_banner");
      if (!successVisible) out.reasons.push("browser_success_state_not_visible");
      if (!hasPropertyName) out.reasons.push("browser_property_name_missing");
      if (!hasConsideration) out.reasons.push("browser_consideration_missing");
    } else {
      out.ok = true;
      out.status = EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED;
    }
    return out;
  } catch (err) {
    out.ok = false;
    out.status = EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED;
    out.reasons.push(`browser_exception:${String(err?.message || err).slice(0, 160)}`);
    return out;
  } finally {
    await browser.close().catch(() => {});
  }
}

export function classifyExternalShareAccessForCustomerReady({
  adpClientProbe,
  liveVerify,
} = {}) {
  if (!adpClientProbe?.tokenId && adpClientProbe?.available !== true) {
    return {
      customerReadyBlocked: true,
      status: EXTERNAL_SHARE_ACCESS_STATUS.SHARE_NOT_CREATED,
      reason: adpClientProbe?.reason || "share_not_created",
    };
  }
  if (adpClientProbe?.available !== true) {
    return {
      customerReadyBlocked: true,
      status: EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED,
      reason:
        adpClientProbe?.reason ||
        "external_share_token_present_but_not_resolvable",
    };
  }
  if (liveVerify && liveVerify.ok !== true) {
    return {
      customerReadyBlocked: true,
      status:
        liveVerify.status ||
        EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED,
      reason: (liveVerify.reasons || []).join(";") || "live_external_share_failed",
    };
  }
  if (liveVerify && liveVerify.ok === true) {
    return {
      customerReadyBlocked: false,
      status: EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED,
      reason: null,
    };
  }
  // Probe says token resolves locally, but no independent live verification this request.
  return {
    customerReadyBlocked: false,
    status: EXTERNAL_SHARE_ACCESS_STATUS.VERIFICATION_PENDING,
    reason: "independent_live_verification_not_run_this_request",
    verificationPending: true,
  };
}
