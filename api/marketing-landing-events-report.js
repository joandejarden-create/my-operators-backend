import { buildLandingEventsReport } from "../lib/marketing-landing-events-read.js";

function parseDays(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 7;
  return Math.min(Math.floor(n), 90);
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

    const days = parseDays(req.query?.days);
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const report = buildLandingEventsReport({ since });
    return res.status(200).json(report);
  } catch (err) {
    console.error("Error in marketing-landing-events-report:", err);
    return res.status(500).json({ error: "Could not build report" });
  }
}

export { requireLandingAnalyticsAdmin };
