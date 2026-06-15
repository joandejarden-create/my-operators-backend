/**
 * Landing analytics report: private URL key OR Memberstack admin.
 * Set LANDING_ANALYTICS_REPORT_KEY on Railway for passwordless bookmark access.
 */

import { memberstackAuth } from "./memberstackAuth.js";
import { requireDealalityUser } from "./requireDealalityUser.js";
import { requireLandingAnalyticsAdmin } from "../api/marketing-landing-events-report.js";

export function landingAnalyticsKeyGate(req, res, next) {
  const expected = (process.env.LANDING_ANALYTICS_REPORT_KEY || "").trim();
  const provided = String(
    req.query?.key || req.headers["x-landing-analytics-key"] || ""
  ).trim();

  req.landingAnalyticsKeyConfigured = Boolean(expected);

  if (!expected) {
    req.landingAnalyticsKeyAuthorized = false;
    if (provided) {
      return res.status(503).json({
        ok: false,
        error: "report_key_not_configured",
        message:
          "LANDING_ANALYTICS_REPORT_KEY is not set on this Railway service yet. Add the variable, wait for redeploy, then use the same value as ?key= in your bookmark.",
      });
    }
    return next();
  }

  if (provided && provided === expected) {
    req.landingAnalyticsKeyAuthorized = true;
    return next();
  }

  if (provided) {
    return res.status(401).json({
      ok: false,
      error: "report_key_invalid",
      message:
        "That access key does not match LANDING_ANALYTICS_REPORT_KEY on Railway. Copy the variable value exactly (no extra spaces).",
    });
  }

  req.landingAnalyticsKeyAuthorized = false;
  return next();
}

function skipIfKeyAuthorized(middleware) {
  return (req, res, next) => {
    if (req.landingAnalyticsKeyAuthorized) return next();
    return middleware(req, res, next);
  };
}

export const landingAnalyticsReportAuth = [
  landingAnalyticsKeyGate,
  skipIfKeyAuthorized(memberstackAuth),
  skipIfKeyAuthorized(requireDealalityUser),
  skipIfKeyAuthorized(requireLandingAnalyticsAdmin),
];
