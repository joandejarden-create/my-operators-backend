/**
 * Landing analytics report: private URL key OR Memberstack admin.
 * Set LANDING_ANALYTICS_REPORT_KEY on Railway for passwordless bookmark access.
 */

import { memberstackAuth } from "./memberstackAuth.js";
import { requireDealalityUser } from "./requireDealalityUser.js";
import { requireLandingAnalyticsAdmin } from "../api/marketing-landing-events-report.js";

export function landingAnalyticsKeyGate(req, res, next) {
  const expected = (process.env.LANDING_ANALYTICS_REPORT_KEY || "").trim();
  if (!expected) {
    req.landingAnalyticsKeyAuthorized = false;
    return next();
  }
  const provided = String(
    req.query?.key || req.headers["x-landing-analytics-key"] || ""
  ).trim();
  req.landingAnalyticsKeyAuthorized = provided.length > 0 && provided === expected;
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
