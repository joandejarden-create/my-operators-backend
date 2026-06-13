import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LOG_FILE = path.join(__dirname, "..", "data", "marketing-landing-events.jsonl");

function parseIso(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function readJsonlLines(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, "utf8");
  const lines = raw.split(/\r?\n/).filter(Boolean);
  const events = [];
  for (const line of lines) {
    try {
      const row = JSON.parse(line);
      if (row && typeof row === "object" && row.event) events.push(row);
    } catch (_err) {
      // skip malformed lines
    }
  }
  return events;
}

function increment(map, key, amount = 1) {
  if (!key) return;
  map[key] = (map[key] || 0) + amount;
}

/**
 * Load landing analytics events with optional time window.
 * @param {{ since?: string, until?: string, limit?: number }} opts
 */
export function loadLandingEvents(opts = {}) {
  const since = parseIso(opts.since);
  const until = parseIso(opts.until);
  const limit =
    typeof opts.limit === "number" && opts.limit > 0
      ? Math.min(Math.floor(opts.limit), 5000)
      : null;

  let events = readJsonlLines(LOG_FILE);
  if (since) {
    events = events.filter((e) => {
      const ts = parseIso(e.ts);
      return ts && ts >= since;
    });
  }
  if (until) {
    events = events.filter((e) => {
      const ts = parseIso(e.ts);
      return ts && ts <= until;
    });
  }
  if (limit) {
    events = events.slice(-limit);
  }
  return events;
}

export function aggregateLandingEvents(events) {
  const sessions = new Set();
  const byEvent = {};
  const scrollDepths = {};
  const sections = {};
  const ctaLocations = {};
  const navClicks = {};
  const devices = {};
  const personas = {};
  const utmSources = {};
  const sessionEmbed = {};
  const sessionDevice = {};
  const sessionLandingVersion = {};
  const firstScrollSeconds = [];
  const videoCompletes = 0;
  const emailCaptures = 0;
  const stageTourCompletes = 0;
  const engagementMilestones = { 30: 0, 60: 0, 120: 0 };

  for (const e of events) {
    if (e.sessionId) sessions.add(e.sessionId);
    increment(byEvent, e.event);

    if (e.device) {
      increment(devices, e.device);
      if (e.sessionId) sessionDevice[e.sessionId] = e.device;
    }
    if (e.sessionId && typeof e.embed === "boolean") {
      sessionEmbed[e.sessionId] = e.embed;
    }
    if (e.landingVersion && e.sessionId) {
      sessionLandingVersion[e.sessionId] = e.landingVersion;
    }

    if (e.event === "scroll_depth" && e.depth != null) {
      increment(scrollDepths, String(e.depth));
    }
    if (e.event === "section_view" && e.section) {
      increment(sections, e.section);
    }
    if (e.event === "cta_click" && e.location) {
      increment(ctaLocations, e.location);
    }
    if (e.event === "nav_click" && e.label) {
      increment(navClicks, e.label);
    }
    if (e.event === "audience_tab" && e.persona) {
      increment(personas, e.persona);
    }
    if (e.event === "page_land" && e.utmSource) {
      increment(utmSources, e.utmSource);
    }
    if (e.event === "first_scroll" && typeof e.seconds === "number") {
      firstScrollSeconds.push(e.seconds);
    }
    if (e.event === "video_complete") {
      // counted in byEvent
    }
    if (e.event === "email_capture_submit") {
      // counted in byEvent
    }
    if (e.event === "stage_tour_complete") {
      // counted in byEvent
    }
    if (e.event === "engagement_milestone" && e.seconds != null) {
      increment(engagementMilestones, String(e.seconds));
    }
  }

  const embedSessions = Object.values(sessionEmbed).filter(Boolean).length;
  const standaloneSessions = Object.values(sessionEmbed).filter((v) => v === false).length;

  const sortedFirstScroll =
    firstScrollSeconds.length > 0
      ? firstScrollSeconds.sort((a, b) => a - b)
      : [];
  const medianFirstScroll =
    sortedFirstScroll.length > 0
      ? sortedFirstScroll[Math.floor(sortedFirstScroll.length / 2)]
      : null;

  const recent = [...events]
    .sort((a, b) => String(b.ts).localeCompare(String(a.ts)))
    .slice(0, 50);

  return {
    totals: {
      events: events.length,
      sessions: sessions.size,
      embedSessions,
      standaloneSessions,
      videoCompletes: byEvent.video_complete || 0,
      emailCaptures: byEvent.email_capture_submit || 0,
      stageTourCompletes: byEvent.stage_tour_complete || 0,
      medianFirstScrollSeconds: medianFirstScroll,
    },
    byEvent: sortCountMap(byEvent),
    scrollDepths: sortCountMap(scrollDepths),
    sections: sortCountMap(sections),
    ctaLocations: sortCountMap(ctaLocations),
    navClicks: sortCountMap(navClicks),
    devices: sortCountMap(devices),
    personas: sortCountMap(personas),
    utmSources: sortCountMap(utmSources),
    engagementMilestones: sortCountMap(engagementMilestones),
    landingVersions: sortCountMap(
      Object.values(sessionLandingVersion).reduce((acc, v) => {
        increment(acc, v);
        return acc;
      }, {})
    ),
    recent,
  };
}

function sortCountMap(map) {
  return Object.entries(map)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([key, count]) => ({ key, count }));
}

export function buildLandingEventsReport(opts = {}) {
  const events = loadLandingEvents(opts);
  const aggregate = aggregateLandingEvents(events);
  const firstTs = events[0]?.ts || null;
  const lastTs = events.length ? events[events.length - 1]?.ts : null;
  return {
    ok: true,
    logFile: "data/marketing-landing-events.jsonl",
    window: {
      since: opts.since || null,
      until: opts.until || null,
      firstEventAt: firstTs,
      lastEventAt: lastTs,
    },
    ...aggregate,
  };
}
