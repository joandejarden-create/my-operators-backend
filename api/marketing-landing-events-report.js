import { buildLandingEventsReport } from "../lib/marketing-landing-events-read.js";
import {
  buildReportWindow,
  loadOptionsForWindow,
} from "../lib/marketing-landing-events-window.js";

function setReportNoStore(res) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  res.setHeader("Pragma", "no-cache");
}

function requireLandingAnalyticsAdmin(req, res, next) {
  const user = req.dealalityUser;
  if (!user) {
    return res.status(401).json({
      ok: false,
      error: "authentication_required",
      message: "Dealality user context required.",
    });
  }
  if (!user.isAdmin) {
    return res.status(403).json({
      ok: false,
      error: "admin_required",
      message: "Landing analytics report requires an admin account.",
    });
  }
  return next();
}

/**
 * GET /api/marketing/landing-events/report?days=7
 * Admin-only summary of append-only landing analytics JSONL.
 */
export async function getMarketingLandingEventsReport(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    setReportNoStore(res);
    const window = buildReportWindow(req.query?.days);
    const report = buildLandingEventsReport(loadOptionsForWindow(window), window);
    return res.status(200).json(report);
  } catch (err) {
    console.error("Error in marketing-landing-events-report:", err);
    return res.status(500).json({ error: "Could not build report" });
  }
}

export { requireLandingAnalyticsAdmin };
