import { buildLandingEventsReport } from "../lib/marketing-landing-events-read.js";
import {
  buildReportWindow,
  loadOptionsForWindow,
} from "../lib/marketing-landing-events-window.js";
import { parseReportFilters } from "../lib/marketing-landing-events-version.js";

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
 * GET /api/marketing/landing-events/report?days=7&version=all|previous|old-home&lang=all|en|es&cutover=YYYY-MM-DD&era=all|before|after
 * Admin-only summary of append-only landing analytics JSONL.
 * Historical rows are preserved; version/locale/cutover only filter the view.
 */
export async function getMarketingLandingEventsReport(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    setReportNoStore(res);
    const window = buildReportWindow(req.query?.days);
    const filters = parseReportFilters(req.query);
    const excludeInternal =
      String(req.query?.excludeInternal || "").trim() === "1";
    const report = buildLandingEventsReport(
      {
        ...loadOptionsForWindow(window),
        excludeInternal,
      },
      window,
      filters
    );
    return res.status(200).json(report);
  } catch (err) {
    console.error("Error in marketing-landing-events-report:", err);
    return res.status(500).json({ error: "Could not build report" });
  }
}

export { requireLandingAnalyticsAdmin };
