import { loadLandingEvents, getSessionTimeline } from "../lib/marketing-landing-events-read.js";

function parseDays(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 7;
  return Math.min(Math.floor(n), 90);
}

/**
 * GET /api/marketing/landing-events/session?sessionId=…&days=7
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

    const days = parseDays(req.query?.days);
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const events = loadLandingEvents({ since });
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
    });
  } catch (err) {
    console.error("Error in marketing-landing-events-session:", err);
    return res.status(500).json({ error: "Could not load session timeline" });
  }
}
