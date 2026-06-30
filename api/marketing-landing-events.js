import fs from "fs";
import { resolveGeoFromRequest } from "../lib/client-ip-geo.js";
import {
  ensureLandingEventsLogDir,
  getLandingEventsLogFile,
} from "../lib/marketing-landing-events-path.js";

const ALLOWED_EVENTS = new Set([
  "page_land",
  "first_scroll",
  "scroll_depth",
  "section_view",
  "max_section_depth",
  "cta_click",
  "nav_click",
  "mobile_nav_open",
  "outbound_click",
  "hero_video_open",
  "hero_video_close",
  "video_progress",
  "video_complete",
  "email_capture_submit",
  "stage_tab",
  "stage_tour_pause",
  "stage_tour_complete",
  "audience_tab",
  "faq_open",
  "engagement_milestone",
  "insights_article_click",
]);

const SESSION_RATE_LIMIT = 120;
const sessionCounts = new Map();

function pruneSessionCounts() {
  if (sessionCounts.size < 5000) return;
  sessionCounts.clear();
}

function sanitizeString(value, maxLen) {
  if (value == null) return null;
  const s = String(value).trim().slice(0, maxLen);
  return s || null;
}

function appendEvent(record) {
  const logFile = getLandingEventsLogFile();
  ensureLandingEventsLogDir(logFile);
  fs.appendFileSync(logFile, JSON.stringify(record) + "\n", "utf8");
}

/**
 * POST /api/marketing/landing-events
 * Public landing analytics — append-only JSONL log (no PII in event payloads).
 * Body: { event, sessionId, embed?, device?, ...metadata }
 */
export default async function marketingLandingEvents(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    if (process.env.LANDING_ANALYTICS_ENABLED === "0") {
      return res.status(204).end();
    }

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const event = sanitizeString(body.event, 64);
    const sessionId = sanitizeString(body.sessionId, 64);

    if (!event || !ALLOWED_EVENTS.has(event)) {
      return res.status(400).json({ error: "Invalid or missing event" });
    }
    if (!sessionId || sessionId.length < 8) {
      return res.status(400).json({ error: "Invalid sessionId" });
    }

    const count = (sessionCounts.get(sessionId) || 0) + 1;
    sessionCounts.set(sessionId, count);
    pruneSessionCounts();
    if (count > SESSION_RATE_LIMIT) {
      return res.status(429).json({ error: "Rate limit exceeded" });
    }

    const record = {
      ts: new Date().toISOString(),
      event,
      sessionId,
      visitorId: sanitizeString(body.visitorId, 80),
      embed: body.embed === true || body.embed === "1",
      device: sanitizeString(body.device, 16),
      landingVersion: sanitizeString(body.landingVersion, 16),
      section: sanitizeString(body.section, 48),
      element: sanitizeString(body.element, 64),
      label: sanitizeString(body.label, 96),
      persona: sanitizeString(body.persona, 24),
      location: sanitizeString(body.location, 32),
      source: sanitizeString(body.source, 16),
      depth: typeof body.depth === "number" ? body.depth : null,
      seconds: typeof body.seconds === "number" ? body.seconds : null,
      questionId: sanitizeString(body.questionId, 64),
      destination: sanitizeString(body.destination, 256),
      outcome: sanitizeString(body.outcome, 16),
      path: sanitizeString(body.path, 128),
      referrer: sanitizeString(body.referrer, 256),
      utmSource: sanitizeString(body.utmSource, 64),
      utmMedium: sanitizeString(body.utmMedium, 64),
      utmCampaign: sanitizeString(body.utmCampaign, 64),
      surface: sanitizeString(body.surface, 24),
      language: sanitizeString(body.language, 16),
      geoCountry: null,
      geoCountryName: null,
      geoRegion: null,
      geoCity: null,
      geoLabel: null,
    };

    if (event === "page_land") {
      const geo = resolveGeoFromRequest(req);
      record.geoCountry = sanitizeString(geo.geoCountry, 8);
      record.geoCountryName = sanitizeString(geo.geoCountryName, 64);
      record.geoRegion = sanitizeString(geo.geoRegion, 32);
      record.geoCity = sanitizeString(geo.geoCity, 64);
      record.geoLabel = sanitizeString(geo.geoLabel, 96);
    }

    appendEvent(record);
    return res.status(204).end();
  } catch (err) {
    console.error("Error in marketing-landing-events:", err);
    return res.status(500).json({ error: "Could not record event" });
  }
}
