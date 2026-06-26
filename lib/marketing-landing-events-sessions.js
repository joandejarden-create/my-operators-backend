import {
  LANDING_ANALYTICS_BENCHMARKS,
  GA4_PROPERTY_ID,
  ga4RealtimeUrl,
} from "./marketing-landing-analytics-benchmarks.js";

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

const DEVICE_LABELS = {
  desktop: "Desktop",
  mobile: "Mobile",
  tablet: "Tablet",
};

const DAY_LABELS = [
  "Sunday",
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
];

function pct(n, total) {
  if (!total) return 0;
  return Math.round((n / total) * 1000) / 10;
}

function formatHourUtc(h) {
  const hour12 = h % 12 === 0 ? 12 : h % 12;
  const ampm = h < 12 ? "AM" : "PM";
  return `${hour12} ${ampm} UTC`;
}

/**
 * @param {object[]} events
 */
export function buildSessionsMap(events) {
  /** @type {Record<string, object>} */
  const bySession = {};

  for (const e of events) {
    if (!e.sessionId) continue;
    if (!bySession[e.sessionId]) {
      bySession[e.sessionId] = {
        sessionId: e.sessionId,
        landed: false,
        landedAt: null,
        scrolled: false,
        scroll50: false,
        scroll75: false,
        sections: new Set(),
        maxSection: "hero",
        maxSectionRank: 0,
        cta: false,
        ctaLocations: new Set(),
        reachedBottomCta: false,
        heroCtaWithoutScroll: false,
        emailAttempt: false,
        emailSuccess: false,
        videoComplete: false,
        videoOpened: false,
        embed: false,
        device: null,
        visitorId: null,
        utmSource: null,
        geoLabel: null,
        eventCount: 0,
      };
    }
    const s = bySession[e.sessionId];
    s.eventCount += 1;

    if (e.event === "page_land") {
      s.landed = true;
      s.landedAt = e.ts || s.landedAt;
      if (e.embed) s.embed = true;
      if (e.device) s.device = e.device;
      if (e.visitorId) s.visitorId = e.visitorId;
      if (e.utmSource) s.utmSource = e.utmSource;
      if (e.geoLabel) s.geoLabel = e.geoLabel;
    }
    if (e.event === "first_scroll") s.scrolled = true;
    if (e.event === "scroll_depth") {
      if (e.depth >= 50) s.scroll50 = true;
      if (e.depth >= 75) s.scroll75 = true;
    }
    if (e.event === "section_view" && e.section) {
      s.sections.add(e.section);
      if (e.section === "cta") s.reachedBottomCta = true;
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
      if (e.section === "cta" || rank >= 8) s.reachedBottomCta = true;
    }
    if (e.event === "cta_click") {
      s.cta = true;
      if (e.location) {
        s.ctaLocations.add(e.location);
        if (!s.scrolled && e.location === "hero") {
          s.heroCtaWithoutScroll = true;
        }
      }
    }
    if (e.event === "email_capture_submit") {
      s.emailAttempt = true;
      if (e.outcome === "success") s.emailSuccess = true;
    }
    if (e.event === "video_complete") s.videoComplete = true;
    if (e.event === "hero_video_open") s.videoOpened = true;
    if (e.visitorId && !s.visitorId) s.visitorId = e.visitorId;
  }

  return bySession;
}

function findBiggestDropOff(steps) {
  let best = null;
  for (let i = 1; i < steps.length; i++) {
    const prior = steps[i - 1];
    const current = steps[i];
    const drop = Math.max(0, prior.count - current.count);
    if (drop > 0 && (!best || drop > best.drop)) {
      best = {
        fromLabel: prior.label,
        toLabel: current.label,
        fromKey: prior.key,
        toKey: current.key,
        drop,
        dropRate: prior.count ? pct(drop, prior.count) : 0,
      };
    }
  }
  return best;
}

/**
 * @param {object[]} sessions
 */
export function funnelFromSessions(sessions) {
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
      label: "Opened Landing Page",
      count: sessions.length,
      rate: 100,
    },
    {
      key: "scrolled",
      label: "Started Scrolling",
      count: scrolled,
      rate: pct(scrolled, sessions.length),
    },
    {
      key: "past_hero",
      label: "Moved Past Hero",
      count: pastHero,
      rate: pct(pastHero, sessions.length),
    },
    {
      key: "reached_how",
      label: "Saw Platform Section",
      count: reachedHow,
      rate: pct(reachedHow, sessions.length),
    },
    {
      key: "deep_engagement",
      label: "Reached FAQ / Why (Deep Read)",
      count: deepScroll,
      rate: pct(deepScroll, sessions.length),
    },
    {
      key: "reached_cta",
      label: "Reached Bottom CTA",
      count: reachedCta,
      rate: pct(reachedCta, sessions.length),
    },
    {
      key: "cta_click",
      label: "Clicked a Signup CTA",
      count: cta,
      rate: pct(cta, sessions.length),
    },
    {
      key: "email_success",
      label: "Submitted Email Successfully",
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

  return {
    steps,
    sectionJourney,
    sessionCount: sessions.length,
    biggestDropOff: findBiggestDropOff(steps),
  };
}

export function buildFunnelFromEvents(events) {
  const bySession = buildSessionsMap(events);
  const sessions = Object.values(bySession).filter((s) => s.landed);
  return funnelFromSessions(sessions);
}

export function buildFunnelComparison(events) {
  const bySession = buildSessionsMap(events);
  const landed = Object.values(bySession).filter((s) => s.landed);
  const embedSessions = landed.filter((s) => s.embed);
  const standaloneSessions = landed.filter((s) => !s.embed);

  const keySteps = ["scrolled", "past_hero", "reached_how", "reached_cta", "cta_click"];

  function summarize(funnel, label) {
    return {
      label,
      sessionCount: funnel.sessionCount,
      steps: funnel.steps
        .filter((s) => keySteps.includes(s.key))
        .map((s) => ({ key: s.key, label: s.label, count: s.count, rate: s.rate })),
    };
  }

  return {
    embed: summarize(funnelFromSessions(embedSessions), "Webflow Embed"),
    standalone: summarize(
      funnelFromSessions(standaloneSessions),
      "Direct / Standalone"
    ),
  };
}

export function buildCtaPathAnalysis(events) {
  const bySession = buildSessionsMap(events);
  const sessions = Object.values(bySession).filter((s) => s.landed);
  const total = sessions.length;

  const reachedBottomNoClick = sessions.filter(
    (s) => s.reachedBottomCta && !s.cta
  ).length;
  const heroCtaNoScroll = sessions.filter((s) => s.heroCtaWithoutScroll).length;
  const clickedHero = sessions.filter((s) => s.ctaLocations.has("hero")).length;
  const clickedBottom = sessions.filter((s) =>
    s.ctaLocations.has("cta_section")
  ).length;

  return {
    paths: [
      {
        key: "reached_bottom_no_click",
        label: "Reached Bottom CTA but Didn't Click",
        count: reachedBottomNoClick,
        rate: pct(reachedBottomNoClick, total),
      },
      {
        key: "hero_cta_no_scroll",
        label: "Clicked Hero CTA Without Scrolling",
        count: heroCtaNoScroll,
        rate: pct(heroCtaNoScroll, total),
      },
      {
        key: "clicked_hero",
        label: "Clicked Hero CTA",
        count: clickedHero,
        rate: pct(clickedHero, total),
      },
      {
        key: "clicked_bottom",
        label: "Clicked Bottom CTA Block",
        count: clickedBottom,
        rate: pct(clickedBottom, total),
      },
    ],
    sessionCount: total,
  };
}

export function buildFaqHeatmap(events) {
  /** @type {Record<string, { key: string, label: string, count: number }>} */
  const map = {};
  for (const e of events) {
    if (e.event !== "faq_open") continue;
    const key = e.questionId || e.label || "unknown";
    const label = e.label || e.questionId || "Unknown Question";
    if (!map[key]) map[key] = { key, label, count: 0 };
    map[key].count += 1;
    if (e.label && map[key].label === e.questionId) {
      map[key].label = e.label;
    }
  }
  return Object.values(map).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
}

export function buildReturnVisitors(events) {
  const bySession = buildSessionsMap(events);
  /** @type {Record<string, object>} */
  const byVisitor = {};
  let sessionsWithVisitorId = 0;
  let sessionsWithoutVisitorId = 0;

  for (const s of Object.values(bySession)) {
    if (!s.landed) continue;
    if (!s.visitorId) {
      sessionsWithoutVisitorId += 1;
      continue;
    }
    sessionsWithVisitorId += 1;
    if (!byVisitor[s.visitorId]) {
      byVisitor[s.visitorId] = {
        visitorId: s.visitorId,
        sessionIds: [],
        visitDays: new Set(),
        embedSessions: 0,
        ctaSessions: 0,
      };
    }
    const v = byVisitor[s.visitorId];
    v.sessionIds.push(s.sessionId);
    if (s.landedAt) v.visitDays.add(String(s.landedAt).slice(0, 10));
    if (s.embed) v.embedSessions += 1;
    if (s.cta) v.ctaSessions += 1;
  }

  const visitors = Object.values(byVisitor).map((v) => ({
    visitorId: v.visitorId,
    sessionCount: v.sessionIds.length,
    visitDayCount: v.visitDays.size,
    embedSessions: v.embedSessions,
    ctaSessions: v.ctaSessions,
    isReturning: v.sessionIds.length > 1 || v.visitDays.size > 1,
    sessionIds: v.sessionIds.slice(0, 20),
  }));

  const returning = visitors.filter((v) => v.isReturning);
  const firstTimeVisitors = visitors.filter((v) => !v.isReturning);

  return {
    totalVisitors: visitors.length,
    returningVisitors: returning.length,
    firstTimeVisitors: firstTimeVisitors.length,
    returningRate: pct(returning.length, visitors.length),
    sessionsWithVisitorId,
    sessionsWithoutVisitorId,
    visitors: returning
      .sort(
        (a, b) =>
          b.sessionCount - a.sessionCount ||
          b.visitDayCount - a.visitDayCount
      )
      .slice(0, 25),
    firstTimeVisitorSample: firstTimeVisitors.slice(0, 10),
    note:
      sessionsWithoutVisitorId > 0
        ? `${sessionsWithoutVisitorId} session(s) lack visitor id (events before fix or storage blocked in browser). New homepage visits after deploy should include visitor id.`
        : "Return visitor = same browser visited on 2+ sessions or 2+ days. Close the tab and revisit the homepage to test.",
  };
}

export function buildTimingPatterns(events) {
  const hours = Array.from({ length: 24 }, (_, i) => ({
    key: String(i),
    label: formatHourUtc(i),
    count: 0,
  }));
  const days = DAY_LABELS.map((label, i) => ({
    key: String(i),
    label,
    count: 0,
  }));

  for (const e of events) {
    if (e.event !== "page_land" || !e.ts) continue;
    const d = new Date(e.ts);
    if (Number.isNaN(d.getTime())) continue;
    hours[d.getUTCHours()].count += 1;
    days[d.getUTCDay()].count += 1;
  }

  return {
    timezone: "UTC",
    hours: hours.filter((h) => h.count > 0).length ? hours : hours,
    days,
    peakHour: [...hours].sort((a, b) => b.count - a.count)[0],
    peakDay: [...days].sort((a, b) => b.count - a.count)[0],
  };
}

export function buildBenchmarkResults(funnel) {
  return LANDING_ANALYTICS_BENCHMARKS.map((b) => {
    const step = funnel?.steps?.find((s) => s.key === b.funnelKey);
    const actualRate = step?.rate ?? 0;
    let status = "bad";
    if (actualRate >= b.targetRate) status = "good";
    else if (actualRate >= b.goodMin) status = "warn";
    return {
      key: b.funnelKey,
      label: b.label,
      actualRate,
      targetRate: b.targetRate,
      goodMin: b.goodMin,
      status,
      sessionCount: step?.count ?? 0,
    };
  });
}

export function buildDailyUniqueUsers(events) {
  /** @type {Record<string, { visitors: Set<string>, sessions: Set<string> }>} */
  const byDay = {};
  const allVisitors = new Set();

  for (const e of events) {
    if (e.event !== "page_land" || !e.ts) continue;
    const d = new Date(e.ts);
    if (Number.isNaN(d.getTime())) continue;
    const dayKey = d.toISOString().slice(0, 10);
    if (!byDay[dayKey]) {
      byDay[dayKey] = { visitors: new Set(), sessions: new Set() };
    }
    const bucket = byDay[dayKey];
    if (e.sessionId) bucket.sessions.add(e.sessionId);
    const uid = (e.visitorId && String(e.visitorId).trim()) || e.sessionId;
    if (uid) {
      bucket.visitors.add(uid);
      allVisitors.add(uid);
    }
  }

  const monthNames = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
  ];

  const days = Object.entries(byDay)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([key, data]) => {
      const [, m, day] = key.split("-").map(Number);
      return {
        key,
        label: `${monthNames[m - 1]} ${day}`,
        uniqueUsers: data.visitors.size,
        sessions: data.sessions.size,
      };
    });

  const latest = days.length ? days[days.length - 1] : null;
  const prior = days.length > 1 ? days[days.length - 2] : null;
  const changeVsPriorDay =
    latest && prior ? latest.uniqueUsers - prior.uniqueUsers : null;

  let weekOverWeekChange = null;
  if (days.length >= 7) {
    const last7 = days.slice(-7);
    const prev7 = days.slice(-14, -7);
    if (prev7.length > 0) {
      const sum = (rows) => rows.reduce((n, r) => n + r.uniqueUsers, 0);
      weekOverWeekChange = sum(last7) - sum(prev7);
    }
  }

  const peakDay =
    days.length > 0
      ? [...days].sort((a, b) => b.uniqueUsers - a.uniqueUsers)[0]
      : null;

  return {
    timezone: "UTC",
    days,
    totalUniqueUsers: allVisitors.size,
    totalDays: days.length,
    latestDay: latest,
    changeVsPriorDay,
    weekOverWeekChange,
    peakDay,
    note: "Unique users = distinct visitor id per day (localStorage). Older events without visitor id count sessions instead.",
  };
}

export function buildGa4CrossCheck(utmSources, totals) {
  const campaigns = (utmSources || []).map((row) => ({
    source: row.key,
    sessions: row.count,
    label: row.key,
  }));

  return {
    propertyId: GA4_PROPERTY_ID,
    realtimeUrl: ga4RealtimeUrl(),
    homeUrl: "https://analytics.google.com/analytics/web/",
    campaigns,
    hasCampaigns: campaigns.length > 0,
    totalSessions: totals?.sessions ?? 0,
    note: "Compare session counts here with GA4 Realtime when UTM tags are present on campaign links.",
  };
}

export function buildSessionIndex(events, geography) {
  const bySession = buildSessionsMap(events);
  const returningIds = new Set(
    buildReturnVisitors(events).visitors.map((v) => v.visitorId)
  );

  return Object.values(bySession)
    .filter((s) => s.landed)
    .map((s) => ({
      sessionId: s.sessionId,
      startedAt: s.landedAt,
      embed: s.embed,
      embedLabel: s.embed ? "Webflow Embed" : "Direct",
      device: DEVICE_LABELS[s.device] || s.device || "—",
      location:
        s.geoLabel || geography?.sessionGeo?.[s.sessionId]?.label || null,
      eventCount: s.eventCount,
      ctaClicked: s.cta,
      maxSection: SECTION_LABELS[s.maxSection] || s.maxSection,
      visitorId: s.visitorId,
      isReturning: s.visitorId ? returningIds.has(s.visitorId) : false,
    }))
    .sort((a, b) => String(b.startedAt).localeCompare(String(a.startedAt)))
    .slice(0, 80);
}

export function getSessionTimeline(events, sessionId, humanize) {
  return events
    .filter((e) => e.sessionId === sessionId)
    .sort((a, b) => String(a.ts).localeCompare(String(b.ts)))
    .map((e) => ({
      ts: e.ts,
      event: e.event,
      label: humanize ? humanize(e).label : e.event,
      detail: humanize ? humanize(e).detail : "",
      section: e.section || null,
      location: e.location || null,
    }));
}
