import fs from "fs";
import {
  getLandingEventsLogFile,
  getLandingEventsStorageMeta,
} from "./marketing-landing-events-path.js";
import {
  buildFunnelFromEvents,
  buildCtaPathAnalysis,
  buildFaqHeatmap,
  buildReturnVisitors,
  buildTimingPatterns,
  buildBenchmarkResults,
  buildDailyUniqueUsers,
  buildSessionIndex,
  getSessionTimeline as buildSessionTimelineRows,
} from "./marketing-landing-events-sessions.js";

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

  let events = readJsonlLines(getLandingEventsLogFile());
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

const SECTION_ORDER = [
  "hero",
  "proofbar",
  "problem",
  "how",
  "audiences",
  "why",
  "faq",
  "cta",
];

const SECTION_LABELS = {
  hero: "Hero",
  proofbar: "Proof Bar",
  problem: "Problem Story",
  how: "Platform / How It Works",
  audiences: "Audiences (Owners, Brands, Partners)",
  why: "Why Dealality",
  faq: "FAQ",
  cta: "Bottom Signup CTA",
};

const EVENT_LABELS = {
  page_land: "Page Load",
  first_scroll: "Started Scrolling",
  scroll_depth: "Scroll Milestone",
  section_view: "Section Viewed",
  max_section_depth: "Deepest Section Reached",
  cta_click: "CTA Click",
  nav_click: "Nav Link Click",
  mobile_nav_open: "Mobile Menu Opened",
  outbound_click: "Outbound Link",
  hero_video_open: "Hero Video Opened",
  hero_video_close: "Hero Video Closed",
  video_progress: "Video Progress",
  video_complete: "Video Finished",
  email_capture_submit: "Email Capture",
  stage_tab: "Platform Stage Tab",
  stage_tour_pause: "Stage Tour Paused",
  stage_tour_complete: "Stage Tour Completed",
  audience_tab: "Audience Tab",
  faq_open: "FAQ Opened",
  engagement_milestone: "Time on Page Milestone",
};

const CTA_LABELS = {
  hero: "Hero",
  navbar: "Top Nav",
  cta_section: "Bottom CTA Block",
  audience_owners: "Owners Panel",
  audience_brands: "Brands Panel",
  audience_partners: "Partners Panel",
  footer: "Footer",
  mobile_menu: "Mobile Menu",
};

const DEVICE_LABELS = {
  desktop: "Desktop",
  mobile: "Mobile",
  tablet: "Tablet",
};

const PERSONA_LABELS = {
  owners: "Owners",
  brands: "Brands",
  partners: "Partners",
};

const ENGAGEMENT_MILESTONE_LABELS = {
  30: "30 Seconds on Page",
  60: "60 Seconds on Page",
  120: "120 Seconds on Page",
};

function pct(n, total) {
  if (!total) return 0;
  return Math.round((n / total) * 1000) / 10;
}

function buildInteractionsSummary(byEvent, navClicks, personas) {
  const items = [
    {
      key: "hero_video_open",
      label: "Hero Video Opens",
      count: byEvent.hero_video_open || 0,
    },
    {
      key: "video_complete",
      label: "Hero Video Completions",
      count: byEvent.video_complete || 0,
    },
    {
      key: "stage_tour_complete",
      label: "Platform Tour Completions",
      count: byEvent.stage_tour_complete || 0,
    },
    {
      key: "faq_open",
      label: "FAQ Expansions",
      count: byEvent.faq_open || 0,
    },
    {
      key: "audience_tab",
      label: "Audience Tab Clicks",
      count: byEvent.audience_tab || 0,
    },
    {
      key: "nav_click",
      label: "Nav Link Clicks",
      count: byEvent.nav_click || 0,
    },
    {
      key: "mobile_nav_open",
      label: "Mobile Menu Opens",
      count: byEvent.mobile_nav_open || 0,
    },
  ].filter((row) => row.count > 0);

  const audienceTabs = sortCountMap(personas).map((row) => ({
    ...row,
    label: PERSONA_LABELS[row.key] || row.key,
  }));

  const nav = sortCountMap(navClicks).map((row) => ({
    ...row,
    label: row.key,
  }));

  return { items, audienceTabs, navClicks: nav };
}

function buildInsights(aggregate, funnel) {
  const insights = [];
  const t = aggregate.totals;
  const sessions = funnel.sessionCount;

  if (sessions === 0) {
    insights.push({
      tone: "warn",
      title: "No Landing Sessions Yet",
      body: "Visit the homepage (Webflow + Railway embed) to generate data. Events appear within seconds of a real visit.",
    });
    return insights;
  }

  if (sessions < 10) {
    insights.push({
      tone: "info",
      title: "Early Sample Size",
      body: `Only ${sessions} session${sessions === 1 ? "" : "s"} in this window — treat trends as directional, not definitive.`,
    });
  }

  if (funnel.biggestDropOff && funnel.biggestDropOff.drop > 0 && sessions >= 3) {
    const d = funnel.biggestDropOff;
    insights.push({
      tone: "warn",
      title: "Biggest Funnel Drop-Off",
      body: `${d.drop} visitor${d.drop === 1 ? "" : "s"} (${d.dropRate}%) did not continue from “${d.fromLabel}” to “${d.toLabel}”.`,
    });
  }

  const embedPct = pct(t.embedSessions, sessions);
  if (embedPct >= 80) {
    insights.push({
      tone: "info",
      title: "Most Traffic Is the Webflow Homepage Embed",
      body: `${embedPct}% of sessions come from the dealality.com iframe embed — this matches production homepage behavior.`,
    });
  }

  const scrollStep = funnel.steps.find((s) => s.key === "scrolled");
  if (scrollStep && scrollStep.rate < 50 && sessions >= 3) {
    insights.push({
      tone: "warn",
      title: "Low Scroll Engagement",
      body: `Only ${scrollStep.rate}% of visitors started scrolling. Hero copy or load speed may need attention.`,
    });
  }

  const howStep = funnel.steps.find((s) => s.key === "reached_how");
  const heroDrop = scrollStep && howStep ? scrollStep.rate - howStep.rate : 0;
  if (heroDrop > 30 && sessions >= 3) {
    insights.push({
      tone: "warn",
      title: "Drop-Off Before Platform Section",
      body: `${heroDrop.toFixed(0)} pts fewer visitors reach “How It Works” than start scrolling — problem story or proof bar may be losing them.`,
    });
  }

  const ctaStep = funnel.steps.find((s) => s.key === "cta_click");
  if (ctaStep && ctaStep.count === 0 && sessions >= 2) {
    insights.push({
      tone: "warn",
      title: "No CTA Clicks Recorded",
      body: "Nobody clicked Request Early Access or signup buttons. Check CTA visibility on mobile and embed height.",
    });
  } else if (ctaStep && ctaStep.rate >= 5) {
    insights.push({
      tone: "good",
      title: "CTAs Are Getting Clicks",
      body: `${ctaStep.rate}% of sessions clicked a signup CTA — compare locations below to double down on what works.`,
    });
  }

  if (t.emailCaptures > 0) {
    insights.push({
      tone: "good",
      title: "Email Captures Working",
      body: `${t.emailCaptures} successful email capture${t.emailCaptures === 1 ? "" : "s"} in this window.`,
    });
  }

  if (t.medianFirstScrollSeconds != null && t.medianFirstScrollSeconds > 8) {
    insights.push({
      tone: "info",
      title: "Slow First Scroll",
      body: `Median time to first scroll is ${t.medianFirstScrollSeconds}s — visitors may be reading hero video or copy before engaging.`,
    });
  }

  if (t.videoCompletes > 0) {
    insights.push({
      tone: "good",
      title: "Hero Video Resonance",
      body: `${t.videoCompletes} visitor${t.videoCompletes === 1 ? "" : "s"} watched the full overview video.`,
    });
  }

  const milestone60 = aggregate.engagementMilestones?.find((m) => m.key === "60");
  if (milestone60 && milestone60.count > 0 && sessions >= 2) {
    insights.push({
      tone: "good",
      title: "Strong Time on Page",
      body: `${milestone60.count} session${milestone60.count === 1 ? "" : "s"} stayed at least 60 seconds — homepage content is holding attention.`,
    });
  }

  const topCountry = aggregate.geography?.countries?.[0];
  if (topCountry && funnel.sessionCount >= 2) {
    insights.push({
      tone: "info",
      title: "Top Visitor Country",
      body: `${topCountry.label} accounts for ${topCountry.count} of ${funnel.sessionCount} sessions in this window.`,
    });
  }

  return insights.slice(0, 7);
}

function humanizeRecentEvent(e) {
  const label = EVENT_LABELS[e.event] || e.event;
  const parts = [];
  if (e.event === "page_land" && e.geoLabel) parts.push(e.geoLabel);
  if (e.section) parts.push(SECTION_LABELS[e.section] || e.section);
  if (e.location) parts.push(CTA_LABELS[e.location] || e.location);
  if (e.label) parts.push(e.label);
  if (e.questionId && e.event === "faq_open") parts.push(e.questionId);
  if (e.persona) parts.push(PERSONA_LABELS[e.persona] || e.persona);
  if (e.depth != null && e.event === "scroll_depth") parts.push(e.depth + "% down page");
  if (e.outcome) parts.push(e.outcome);
  return { label, detail: parts.join(" · ") || "—" };
}

function buildGeoAggregate(events, sessionCount) {
  /** @type {Record<string, object>} */
  const sessionGeo = {};
  const countries = {};
  const cities = {};
  const locations = {};

  for (const e of events) {
    if (e.event !== "page_land" || !e.sessionId) continue;
    if (!e.geoLabel && !e.geoCountry && !e.geoCity) continue;
    const country = e.geoCountryName || e.geoCountry || "Unknown";
    const city = e.geoCity || null;
    const label =
      e.geoLabel ||
      [city, e.geoRegion, country].filter(Boolean).join(", ") ||
      country;
    sessionGeo[e.sessionId] = { label, country, city, region: e.geoRegion || null };
  }

  for (const g of Object.values(sessionGeo)) {
    increment(countries, g.country || "Unknown");
    if (g.city) {
      increment(cities, g.city + (g.country ? ", " + g.country : ""));
    }
    increment(locations, g.label);
  }

  const locatedSessions = Object.keys(sessionGeo).length;
  return {
    sessionGeo,
    locatedSessions,
    unknownSessions: Math.max(0, sessionCount - locatedSessions),
    countries: sortCountMap(countries).map((row) => ({
      ...row,
      label: row.key,
    })),
    cities: sortCountMap(cities).map((row) => ({
      ...row,
      label: row.key,
    })),
    locations: sortCountMap(locations).map((row) => ({
      ...row,
      label: row.key,
    })),
  };
}

function sortCountMap(map) {
  return Object.entries(map)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([key, count]) => ({ key, count }));
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
  const emailSuccessCount = { n: 0 };
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
    if (e.event === "email_capture_submit" && e.outcome === "success") {
      emailSuccessCount.n += 1;
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

  const funnel = buildFunnelFromEvents(events);
  const geography = buildGeoAggregate(events, sessions.size);
  const utmRows = sortCountMap(utmSources);
  const videoOpens = byEvent.hero_video_open || 0;
  const videoCompletes = byEvent.video_complete || 0;

  const recent = [...events]
    .sort((a, b) => String(b.ts).localeCompare(String(a.ts)))
    .slice(0, 50)
    .map((e) => ({
      ...e,
      ...humanizeRecentEvent(e),
      visitorLocation:
        e.geoLabel ||
        geography.sessionGeo[e.sessionId]?.label ||
        null,
    }));

  const base = {
    totals: {
      events: events.length,
      sessions: sessions.size,
      embedSessions,
      standaloneSessions,
      videoCompletes,
      videoOpens,
      videoCompletionRate:
        videoOpens > 0 ? pct(videoCompletes, videoOpens) : null,
      emailCaptures: emailSuccessCount.n,
      stageTourCompletes: byEvent.stage_tour_complete || 0,
      medianFirstScrollSeconds: medianFirstScroll,
      eventsPerSession:
        sessions.size > 0 ? Math.round((events.length / sessions.size) * 10) / 10 : 0,
      locatedSessions: geography.locatedSessions,
      unknownGeoSessions: geography.unknownSessions,
    },
    geography: {
      countries: geography.countries,
      cities: geography.cities,
      locations: geography.locations,
      locatedSessions: geography.locatedSessions,
      unknownSessions: geography.unknownSessions,
    },
    funnel,
    byEvent: sortCountMap(byEvent).map((row) => ({
      ...row,
      label: EVENT_LABELS[row.key] || row.key,
    })),
    scrollDepths: sortCountMap(scrollDepths),
    sections: sortCountMap(sections).map((row) => ({
      ...row,
      label: SECTION_LABELS[row.key] || row.key,
    })),
    ctaLocations: sortCountMap(ctaLocations).map((row) => ({
      ...row,
      label: CTA_LABELS[row.key] || row.key,
    })),
    navClicks: sortCountMap(navClicks),
    devices: sortCountMap(devices).map((row) => ({
      ...row,
      label: DEVICE_LABELS[row.key] || row.key,
    })),
    personas: sortCountMap(personas).map((row) => ({
      ...row,
      label: PERSONA_LABELS[row.key] || row.key,
    })),
    utmSources: utmRows,
    engagementMilestones: sortCountMap(engagementMilestones).map((row) => ({
      ...row,
      label: ENGAGEMENT_MILESTONE_LABELS[row.key] || row.key + " Seconds on Page",
    })),
    interactions: buildInteractionsSummary(byEvent, navClicks, personas),
    ctaPaths: buildCtaPathAnalysis(events),
    faqHeatmap: buildFaqHeatmap(events),
    returnVisitors: buildReturnVisitors(events),
    timing: buildTimingPatterns(events),
    benchmarks: buildBenchmarkResults(funnel),
    dailyUniqueUsers: buildDailyUniqueUsers(events),
    sessionIndex: buildSessionIndex(events, geography),
    landingVersions: sortCountMap(
      Object.values(sessionLandingVersion).reduce((acc, v) => {
        increment(acc, v);
        return acc;
      }, {})
    ),
    recent,
  };
  base.insights = buildInsights(base, funnel);
  return base;
}

export function getSessionTimeline(events, sessionId) {
  return buildSessionTimelineRows(events, sessionId, humanizeRecentEvent);
}

export function buildLandingEventsReport(opts = {}) {
  const events = loadLandingEvents(opts);
  const aggregate = aggregateLandingEvents(events);
  const storage = getLandingEventsStorageMeta();
  const firstTs = events[0]?.ts || null;
  const lastTs = events.length ? events[events.length - 1]?.ts : null;
  return {
    ok: true,
    storage,
    window: {
      since: opts.since || null,
      until: opts.until || null,
      firstEventAt: firstTs,
      lastEventAt: lastTs,
    },
    ...aggregate,
  };
}
