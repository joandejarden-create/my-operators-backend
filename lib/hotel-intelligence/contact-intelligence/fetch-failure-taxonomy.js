/**
 * Contact Intelligence V1.3 — fetch failure taxonomy.
 * Do not collapse every 403 into POLICY_RESTRICTED.
 */

export const FETCH_FAILURE_CLASS = Object.freeze({
  HTTP_403_ACCESS_DENIED: "HTTP_403_ACCESS_DENIED",
  POLICY_RESTRICTED_EXPLICIT: "POLICY_RESTRICTED_EXPLICIT",
  JS_RENDERING_REQUIRED: "JS_RENDERING_REQUIRED",
  AUTHENTICATION_REQUIRED: "AUTHENTICATION_REQUIRED",
  RATE_LIMITED: "RATE_LIMITED",
  TRANSIENT_FAILURE: "TRANSIENT_FAILURE",
  NETWORK_ERROR: "NETWORK_ERROR",
  UNKNOWN: "UNKNOWN",
});

/**
 * @param {{ ok?: boolean, status?: number, blocked?: boolean, text?: string, error?: string }} page
 */
export function classifyFetchFailure(page = {}) {
  const status = Number(page.status || 0);
  const text = String(page.text || "").slice(0, 8000);
  const err = String(page.error || "");

  if (status === 429 || /rate.?limit|too many requests/i.test(text + err)) {
    return {
      class: FETCH_FAILURE_CLASS.RATE_LIMITED,
      retryable: true,
      browser_fallback_appropriate: false,
      detail: `status=${status}`,
    };
  }
  if (status === 401 || status === 407 || /sign in|log in|authentication required|unauthorized/i.test(text)) {
    return {
      class: FETCH_FAILURE_CLASS.AUTHENTICATION_REQUIRED,
      retryable: false,
      browser_fallback_appropriate: false,
      detail: `status=${status}`,
    };
  }
  if (
    /terms of use|robots\.txt|scraping not allowed|do not scrape|access policy|forbidden by policy/i.test(
      text
    )
  ) {
    return {
      class: FETCH_FAILURE_CLASS.POLICY_RESTRICTED_EXPLICIT,
      retryable: false,
      browser_fallback_appropriate: false,
      detail: "explicit_policy_language_in_body",
    };
  }
  if (status === 403 || page.blocked) {
    // 403 alone is access denied — may be bot wall / WAF, not an explicit product policy.
    const jsHint =
      /cf-challenge|akamai|attention required|enable javascript|captcha|cloudflare/i.test(text) ||
      (text.length < 500 && /denied|forbidden/i.test(text));
    return {
      class: FETCH_FAILURE_CLASS.HTTP_403_ACCESS_DENIED,
      retryable: false,
      browser_fallback_appropriate: jsHint || status === 403,
      detail: jsHint ? "waf_or_bot_challenge_suspected" : `status=${status}`,
    };
  }
  if (
    status === 200 &&
    page.ok === false &&
    /__NEXT_DATA__|react-root|ng-app/i.test(text) &&
    text.replace(/<[^>]+>/g, "").trim().length < 200
  ) {
    return {
      class: FETCH_FAILURE_CLASS.JS_RENDERING_REQUIRED,
      retryable: false,
      browser_fallback_appropriate: true,
      detail: "shell_html_little_content",
    };
  }
  if (status >= 500 || /ECONNRESET|ETIMEDOUT|aborted|timeout/i.test(err)) {
    return {
      class: FETCH_FAILURE_CLASS.TRANSIENT_FAILURE,
      retryable: true,
      browser_fallback_appropriate: false,
      detail: err || `status=${status}`,
    };
  }
  if (err) {
    return {
      class: FETCH_FAILURE_CLASS.NETWORK_ERROR,
      retryable: true,
      browser_fallback_appropriate: false,
      detail: err.slice(0, 160),
    };
  }
  return {
    class: FETCH_FAILURE_CLASS.UNKNOWN,
    retryable: false,
    browser_fallback_appropriate: false,
    detail: `status=${status}`,
  };
}

export function isMarriottPropertyUrl(url) {
  try {
    const u = new URL(url);
    return /marriott\.com$/i.test(u.hostname.replace(/^www\./, "")) && /\/hotels\//i.test(u.pathname);
  } catch {
    return false;
  }
}
