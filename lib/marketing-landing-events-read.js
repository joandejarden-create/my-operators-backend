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
  proofbar: "Proof bar",
  problem: "Problem story",
  how: "Platform / how it works",
  audiences: "Audiences (owners, brands, partners)",
  why: "Why Dealality",
  faq: "FAQ",
  cta: "Bottom signup CTA",
};

const EVENT_LABELS = {
  page_land: "Page load",
  first_scroll: "Started scrolling",
  scroll_depth: "Scroll milestone",
  section_view: "Section viewed",
  max_section_depth: "Deepest section reached",
  cta_click: "CTA click",
  nav_click: "Nav link click",
  mobile_nav_open: "Mobile menu opened",
  outbound_click: "Outbound link",
  hero_video_open: "Hero video opened",
  hero_video_close: "Hero video closed",
  video_progress: "Video progress",
  video_complete: "Video finished",
  email_capture_submit: "Email capture",
  stage_tab: "Platform stage tab",
  stage_tour_pause: "Stage tour paused",
  stage_tour_complete: "Stage tour completed",
  audience_tab: "Audience tab",
  faq_open: "FAQ opened",
  engagement_milestone: "Engaged 30s+",
};

const CTA_LABELS = {
  hero: "Hero",
  navbar: "Top nav",
  cta_section: "Bottom CTA block",
  audience_owners: "Owners panel",
  audience_brands: "Brands panel",
  audience_partners: "Partners panel",
  footer: "Footer",
  mobile_menu: "Mobile menu",
};

function pct(n, total) {
  if (!total) return 0;
  return Math.round((n / total) * 1000) / 10;
}

function buildSessionFunnel(events) {
  /** @type {Record<string, object>} */
  const bySession = {};

  for (const e of events) {
    if (!e.sessionId) continue;
    if (!bySession[e.sessionId]) {
      bySession[e.sessionId] = {
        landed: false,
        scrolled: false,
        scroll50: false,
        scroll75: false,
        sections: new Set(),
        maxSection: "hero",
        maxSectionRank: 0,
        cta: false,
        emailAttempt: false,
        emailSuccess: false,
        videoComplete: false,
        embed: false,
        device: null,
      };
    }
    const s = bySession[e.sessionId];

    if (e.event === "page_land") {
      s.landed = true;
      if (e.embed) s.embed = true;
      if (e.device) s.device = e.device;
    }
    if (e.event === "first_scroll") s.scrolled = true;
    if (e.event === "scroll_depth") {
      if (e.depth >= 50) s.scroll50 = true;
      if (e.depth >= 75) s.scroll75 = true;
    }
    if (e.event === "section_view" && e.section) {
      s.sections.add(e.section);
    }
    if (e.event === "max_section_depth" && e.section) {
      const rank =
        typeof e.depth === "number"
          ? e.depth
          : SECTION_ORDER.indexOf(e.section) + 1;
      if (rank > s.maxSectionRank) {
        s.maxSectionRank = rank;
        s.maxSection = e.section;
      }
    }
    if (e.event === "cta_click") s.cta = true;
    if (e.event === "email_capture_submit") {
      s.emailAttempt = true;
      if (e.outcome === "success") s.emailSuccess = true;
    }
    if (e.event === "video_complete") s.videoComplete = true;
  }

  const sessions = Object.values(bySession).filter((s) => s.landed);
  const scrolled = sessions.filter((s) => s.scrolled || s.scroll50).length;
  const pastHero = sessions.filter(
    (s) =>
      s.sections.has("proofbar") ||
      s.sections.has("problem") ||
      s.maxSectionRank >= 2
  ).length;
  const reachedHow = sessions.filter(
    (s) => s.sections.has("how") || s.maxSectionRank >= 4
  ).length;
  const reachedCta = sessions.filter(
    (s) => s.sections.has("cta") || s.maxSectionRank >= 8
  ).length;
  const deepScroll = sessions.filter(
    (s) => s.scroll75 || s.maxSectionRank >= 6
  ).length;
  const cta = sessions.filter((s) => s.cta).length;
  const email = sessions.filter((s) => s.emailSuccess).length;

  const steps = [
    {
      key: "landed",
      label: "Opened landing page",
      count: sessions.length,
      rate: 100,
    },
    {
      key: "scrolled",
      label: "Started scrolling",
      count: scrolled,
      rate: pct(scrolled, sessions.length),
    },
    {
      key: "past_hero",
      label: "Moved past hero",
      count: pastHero,
      rate: pct(pastHero, sessions.length),
    },
    {
      key: "reached_how",
      label: "Saw platform section",
      count: reachedHow,
      rate: pct(reachedHow, sessions.length),
    },
    {
      key: "deep_engagement",
      label: "Reached FAQ / why (deep read)",
      count: deepScroll,
      rate: pct(deepScroll, sessions.length),
    },
    {
      key: "reached_cta",
      label: "Reached bottom CTA",
      count: reachedCta,
      rate: pct(reachedCta, sessions.length),
    },
    {
      key: "cta_click",
      label: "Clicked a signup CTA",
      count: cta,
      rate: pct(cta, sessions.length),
    },
    {
      key: "email_success",
      label: "Submitted email successfully",
      count: email,
      rate: pct(email, sessions.length),
    },
  ];

  const sectionJourney = SECTION_ORDER.map((id) => {
    const count = sessions.filter(
      (s) =>
        s.sections.has(id) ||
        SECTION_ORDER.indexOf(s.maxSection) >= SECTION_ORDER.indexOf(id)
    ).length;
    return {
      key: id,
      label: SECTION_LABELS[id] || id,
      sessions: count,
      rate: pct(count, sessions.length),
    };
  });

  return { steps, sectionJourney, sessionCount: sessions.length };
}

function buildInsights(aggregate, funnel) {
  const insights = [];
  const t = aggregate.totals;
  const sessions = funnel.sessionCount;

  if (sessions === 0) {
    insights.push({
      tone: "warn",
      title: "No landing sessions yet",
      body: "Visit the homepage (Webflow + Railway embed) to generate data. Events appear within seconds of a real visit.",
    });
    return insights;
  }

  if (sessions < 10) {
    insights.push({
      tone: "info",
      title: "Early sample size",
      body: `Only ${sessions} session${sessions === 1 ? "" : "s"} in this window — treat trends as directional, not definitive.`,
    });
  }

  const embedPct = pct(t.embedSessions, sessions);
  if (embedPct >= 80) {
    insights.push({
      tone: "info",
      title: "Most traffic is the Webflow homepage embed",
      body: `${embedPct}% of sessions come from dealality.com iframe embed — this matches production homepage behavior.`,
    });
  }

  const scrollStep = funnel.steps.find((s) => s.key === "scrolled");
  if (scrollStep && scrollStep.rate < 50 && sessions >= 3) {
    insights.push({
      tone: "warn",
      title: "Low scroll engagement",
      body: `Only ${scrollStep.rate}% of visitors started scrolling. Hero copy or load speed may need attention.`,
    });
  }

  const howStep = funnel.steps.find((s) => s.key === "reached_how");
  const heroDrop = scrollStep && howStep ? scrollStep.rate - howStep.rate : 0;
  if (heroDrop > 30 && sessions >= 3) {
    insights.push({
      tone: "warn",
      title: "Drop-off before platform section",
      body: `${heroDrop.toFixed(0)} pts fewer visitors reach “How it works” than start scrolling — problem story or proof bar may be losing them.`,
    });
  }

  const ctaStep = funnel.steps.find((s) => s.key === "cta_click");
  if (ctaStep && ctaStep.count === 0 && sessions >= 2) {
    insights.push({
      tone: "warn",
      title: "No CTA clicks recorded",
      body: "Nobody clicked Request Early Access or signup buttons. Check CTA visibility on mobile and embed height.",
    });
  } else if (ctaStep && ctaStep.rate >= 5) {
    insights.push({
      tone: "good",
      title: "CTAs are getting clicks",
      body: `${ctaStep.rate}% of sessions clicked a signup CTA — compare locations below to double down on what works.`,
    });
  }

  if (t.emailCaptures > 0) {
    insights.push({
      tone: "good",
      title: "Email captures working",
      body: `${t.emailCaptures} successful email capture${t.emailCaptures === 1 ? "" : "s"} in this window.`,
    });
  }

  if (t.medianFirstScrollSeconds != null && t.medianFirstScrollSeconds > 8) {
    insights.push({
      tone: "info",
      title: "Slow first scroll",
      body: `Median time to first scroll is ${t.medianFirstScrollSeconds}s — visitors may be reading hero video/copy before engaging.`,
    });
  }

  if (t.videoCompletes > 0) {
    insights.push({
      tone: "good",
      title: "Hero video resonance",
      body: `${t.videoCompletes} visitor${t.videoCompletes === 1 ? "" : "s"} watched the full overview video.`,
    });
  }

  const topCountry = aggregate.geography?.countries?.[0];
  if (topCountry && funnel.sessionCount >= 2) {
    insights.push({
      tone: "info",
      title: "Top visitor country",
      body: `${topCountry.label} accounts for ${topCountry.count} of ${funnel.sessionCount} sessions in this window.`,
    });
  }

  return insights.slice(0, 6);
}

function humanizeRecentEvent(e) {
  const label = EVENT_LABELS[e.event] || e.event;
  const parts = [];
  if (e.event === "page_land" && e.geoLabel) parts.push(e.geoLabel);
  if (e.section) parts.push(SECTION_LABELS[e.section] || e.section);
  if (e.location) parts.push(CTA_LABELS[e.location] || e.location);
  if (e.label) parts.push(e.label);
  if (e.persona) parts.push(e.persona);
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

  const funnel = buildSessionFunnel(events);
  const geography = buildGeoAggregate(events, sessions.size);

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
      videoCompletes: byEvent.video_complete || 0,
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
  base.insights = buildInsights(base, funnel);
  return base;
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
