/**
 * Homepage locale (EN / ES) helpers for landing analytics.
 * Old-home collector tags language=en|es from path; path is the fallback for untagged rows.
 */

export const LOCALE_ALL = "all";
export const LOCALE_EN = "en";
export const LOCALE_ES = "es";

export const LOCALE_LABELS = {
  [LOCALE_ALL]: "All Languages",
  [LOCALE_EN]: "English (/)",
  [LOCALE_ES]: "Spanish (/es)",
};

/**
 * @param {unknown} value
 * @returns {"all"|"en"|"es"}
 */
export function parseLocaleFilter(value) {
  const v = String(value || "all")
    .trim()
    .toLowerCase();
  if (v === "en" || v === "english") return LOCALE_EN;
  if (v === "es" || v === "spa" || v === "spanish" || v === "español" || v === "espanol") {
    return LOCALE_ES;
  }
  return LOCALE_ALL;
}

/**
 * Infer homepage locale from event language or path.
 * @param {object|null|undefined} event
 * @returns {"en"|"es"|null}
 */
export function eventLocale(event) {
  const lang = String(event?.language || "")
    .trim()
    .toLowerCase()
    .slice(0, 8);
  if (lang === "en" || lang.indexOf("en-") === 0) return LOCALE_EN;
  if (lang === "es" || lang.indexOf("es-") === 0) return LOCALE_ES;

  const path = String(event?.path || "")
    .trim()
    .toLowerCase();
  if (!path) return null;
  try {
    const pathname = path.startsWith("http")
      ? new URL(path).pathname
      : path.split("?")[0];
    const clean = pathname.replace(/\/+$/, "") || "/";
    if (clean === "/es" || clean.indexOf("/es/") === 0) return LOCALE_ES;
    if (clean === "/" || clean === "/old-home" || clean.indexOf("/old-home/") === 0) {
      return LOCALE_EN;
    }
  } catch (_e) {
    if (path === "/es" || path.indexOf("/es/") === 0 || path.indexOf("/es?") === 0) {
      return LOCALE_ES;
    }
  }
  return null;
}

/**
 * @param {Array<object>} events
 * @param {string} localeFilter
 */
export function filterEventsByLocale(events, localeFilter) {
  const filter = parseLocaleFilter(localeFilter);
  if (filter === LOCALE_ALL) return events || [];
  return (events || []).filter((e) => eventLocale(e) === filter);
}

function pct(n, total) {
  if (!total) return 0;
  return Math.round((n / total) * 1000) / 10;
}

function summarizeLocaleSlice(events, label, localeKey) {
  const sessions = new Set();
  const ctaSessions = new Set();
  const scrolledSessions = new Set();
  const videoOpenSessions = new Set();
  let faqOpens = 0;
  /** @type {Record<string, number>} */
  const ctaByLocation = {};
  /** @type {Record<string, number>} */
  const faqByKey = {};

  for (const e of events || []) {
    if (e.sessionId && e.event === "page_land") sessions.add(e.sessionId);
    if (e.event === "cta_click") {
      if (e.sessionId) ctaSessions.add(e.sessionId);
      const loc = e.location || "unknown";
      ctaByLocation[loc] = (ctaByLocation[loc] || 0) + 1;
    }
    if (
      (e.event === "first_scroll" || (e.event === "scroll_depth" && e.depth >= 25)) &&
      e.sessionId
    ) {
      scrolledSessions.add(e.sessionId);
    }
    if (
      e.sessionId &&
      (e.event === "hero_video_open" ||
        e.event === "platform_video_launcher_open" ||
        e.event === "platform_video_start")
    ) {
      videoOpenSessions.add(e.sessionId);
    }
    if (e.event === "faq_open") {
      faqOpens += 1;
      const key = e.questionId || e.label || "unknown";
      faqByKey[key] = (faqByKey[key] || 0) + 1;
    }
  }

  const sessionCount = sessions.size;
  const topCta = Object.entries(ctaByLocation).sort((a, b) => b[1] - a[1])[0];
  const topFaq = Object.entries(faqByKey).sort((a, b) => b[1] - a[1])[0];

  return {
    key: localeKey,
    label,
    sessions: sessionCount,
    events: (events || []).length,
    ctaClicks: Object.values(ctaByLocation).reduce((a, b) => a + b, 0),
    ctaSessions: ctaSessions.size,
    ctaRate: pct(ctaSessions.size, sessionCount),
    scrollRate: pct(scrolledSessions.size, sessionCount),
    videoOpenSessions: videoOpenSessions.size,
    videoOpenRate: pct(videoOpenSessions.size, sessionCount),
    faqOpens,
    topCta: topCta ? { key: topCta[0], count: topCta[1] } : null,
    topFaq: topFaq ? { key: topFaq[0], count: topFaq[1] } : null,
  };
}

/**
 * Side-by-side EN vs ES homepage snapshot (does not filter the main report).
 * @param {Array<object>} events
 */
export function buildLocaleCompare(events) {
  const enEvents = [];
  const esEvents = [];
  const unknown = [];
  for (const e of events || []) {
    const loc = eventLocale(e);
    if (loc === LOCALE_EN) enEvents.push(e);
    else if (loc === LOCALE_ES) esEvents.push(e);
    else unknown.push(e);
  }

  const en = summarizeLocaleSlice(enEvents, LOCALE_LABELS[LOCALE_EN], LOCALE_EN);
  const es = summarizeLocaleSlice(esEvents, LOCALE_LABELS[LOCALE_ES], LOCALE_ES);

  const deltas = [];
  if (en.sessions >= 3 && es.sessions >= 3) {
    const ctaGap = Math.round((en.ctaRate - es.ctaRate) * 10) / 10;
    const scrollGap = Math.round((en.scrollRate - es.scrollRate) * 10) / 10;
    const videoGap = Math.round((en.videoOpenRate - es.videoOpenRate) * 10) / 10;
    if (Math.abs(ctaGap) >= 2) {
      deltas.push({
        metric: "ctaRate",
        label: "CTA click rate",
        en: en.ctaRate,
        es: es.ctaRate,
        gapPts: ctaGap,
        leader: ctaGap > 0 ? "en" : "es",
      });
    }
    if (Math.abs(scrollGap) >= 5) {
      deltas.push({
        metric: "scrollRate",
        label: "Scroll start rate",
        en: en.scrollRate,
        es: es.scrollRate,
        gapPts: scrollGap,
        leader: scrollGap > 0 ? "en" : "es",
      });
    }
    if (Math.abs(videoGap) >= 3) {
      deltas.push({
        metric: "videoOpenRate",
        label: "Video open rate",
        en: en.videoOpenRate,
        es: es.videoOpenRate,
        gapPts: videoGap,
        leader: videoGap > 0 ? "en" : "es",
      });
    }
  }

  return {
    en,
    es,
    unknownSessionsHint: unknown.length
      ? `${unknown.length} event(s) lack language/path tags (usually pre-old-home traffic).`
      : null,
    deltas,
    hasBoth: en.sessions > 0 && es.sessions > 0,
  };
}
