/**
 * Contact Intelligence V1.3 — research page fetch with failure taxonomy
 * and optional Marriott Puppeteer fallback (existing helper only; no CAPTCHA bypass).
 */

import { fetchResearchPage } from "../room-count-research/fetch.js";
import {
  classifyFetchFailure,
  FETCH_FAILURE_CLASS,
  isMarriottPropertyUrl,
} from "./fetch-failure-taxonomy.js";

/**
 * @param {string} url
 * @param {{ timeoutMs?: number, allowBrowserFallback?: boolean }} [opts]
 */
export async function fetchContactResearchPage(url, opts = {}) {
  const allowBrowser = opts.allowBrowserFallback !== false;
  const page = await fetchResearchPage(url, { timeoutMs: opts.timeoutMs ?? 14000 });
  if (page.ok) {
    return {
      ...page,
      fetch_class: null,
      fetch_method: "static_fetch",
      browser_used: false,
    };
  }

  const failure = classifyFetchFailure(page);
  let browserAttempt = null;

  if (
    allowBrowser &&
    failure.browser_fallback_appropriate &&
    isMarriottPropertyUrl(url) &&
    failure.class !== FETCH_FAILURE_CLASS.POLICY_RESTRICTED_EXPLICIT &&
    failure.class !== FETCH_FAILURE_CLASS.AUTHENTICATION_REQUIRED
  ) {
    try {
      const { fetchMarriottOverviewHtmlPuppeteer } = await import(
        "../../marriott-hotel-content-fetch.js"
      );
      const pup = await fetchMarriottOverviewHtmlPuppeteer(url, {
        timeoutMs: opts.browserTimeoutMs || 60000,
      });
      browserAttempt = {
        status: pup.status,
        accessDenied: pup.accessDenied,
        source: pup.source,
      };
      if (!pup.accessDenied && pup.html && pup.html.length > 500) {
        return {
          ok: true,
          status: pup.status || 200,
          url: pup.url || url,
          text: pup.html.slice(0, 500_000),
          blocked: false,
          length: pup.html.length,
          fetch_class: null,
          fetch_method: "marriott_puppeteer_fallback",
          browser_used: true,
          prior_static_failure: failure,
        };
      }
      failure.browser_result = "access_denied_or_empty";
    } catch (err) {
      failure.browser_error = String(err?.message || err).slice(0, 200);
    }
  }

  return {
    ...page,
    ok: false,
    fetch_class: failure.class,
    fetch_failure: failure,
    fetch_method: "static_fetch",
    browser_used: Boolean(browserAttempt),
    browser_attempt: browserAttempt,
  };
}
