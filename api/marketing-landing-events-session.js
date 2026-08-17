import { loadLandingEvents, getSessionTimeline } from "../lib/marketing-landing-events-read.js";
import {
  buildReportWindow,
  loadOptionsForWindow,
} from "../lib/marketing-landing-events-window.js";
import {
  applyLandingReportFilters,
  parseReportFilters,
} from "../lib/marketing-landing-events-version.js";

function setReportNoStore(res) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  res.setHeader("Pragma", "no-cache");
}

/**
 * GET /api/marketing/landing-events/session?sessionId=…&days=7&version=&cutover=&era=
 */
export async function getMarketingLandingEventsSession(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const sessionId = String(req.query?.sessionId || "").trim();
    if (!sessionId) {
      return res.status(400).json({
        ok: false,
        error: "session_id_required",
        message: "sessionId query parameter is required.",
      });
    }

    setReportNoStore(res);
    const window = buildReportWindow(req.query?.days);
    const filters = parseReportFilters(req.query);
    const excludeInternal =
      String(req.query?.excludeInternal || "").trim() === "1";
    const loaded = loadLandingEvents({
      ...loadOptionsForWindow(window),
      excludeInternal,
    });
    const { events } = applyLandingReportFilters(loaded, filters);
    const timeline = getSessionTimeline(events, sessionId);

    if (!timeline.length) {
      return res.status(404).json({
        ok: false,
        error: "session_not_found",
        message: "No events for this session in the selected time window.",
      });
    }

    return res.status(200).json({
      ok: true,
      sessionId,
      eventCount: timeline.length,
      timeline,
      filters,
    });
  } catch (err) {
    console.error("Error in marketing-landing-events-session:", err);
    return res.status(500).json({ error: "Could not load session timeline" });
  }
}
