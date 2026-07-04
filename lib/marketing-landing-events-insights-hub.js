/**
 * Aggregates for dealality.com/insights hub analytics.
 */

import { buildLandingDashboard } from "./marketing-landing-events-dashboard.js";
import { filterEventsBySurface, SURFACE_INSIGHTS } from "./marketing-landing-events-surface.js";

function increment(map, key, amount = 1) {
  if (!key) return;
  map[key] = (map[key] || 0) + amount;
}

function sortCountRows(map, labelFn) {
  return Object.entries(map)
    .map(([key, count]) => ({
      key,
      label: labelFn ? labelFn(key) : key,
      count,
    }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
}

const LANGUAGE_LABELS = {
  en: "English",
  es: "Spanish",
  pt: "Portuguese",
  fr: "French",
};

function languageLabel(code) {
  const key = String(code || "").trim().toLowerCase().slice(0, 2);
  return LANGUAGE_LABELS[key] || (key ? key.toUpperCase() : "Unknown");
}

export function buildInsightsHubReport(events) {
  const insightsEvents = filterEventsBySurface(events, SURFACE_INSIGHTS);
  const sessions = new Set();
  let pageviews = 0;
  const articleClicks = {};
  const articleMeta = {};
  const languages = {};
  const ctaLocations = {};
  const scrollDepths = {};

  for (const e of insightsEvents) {
    if (e.sessionId) sessions.add(e.sessionId);
    if (e.event === "page_land") pageviews += 1;
    if (e.event === "insights_article_click") {
      const label = (e.label || e.destination || "Article").trim();
      increment(articleClicks, label);
      articleMeta[label] = {
        destination: e.destination || null,
        language: e.language || articleMeta[label]?.language || null,
      };
      if (e.language) increment(languages, e.language);
    }
    if (e.event === "cta_click" && e.location) {
      increment(ctaLocations, e.location);
    }
    if (e.event === "scroll_depth" && e.depth != null) {
      increment(scrollDepths, String(e.depth));
    }
  }

  const articleRows = Object.entries(articleClicks)
    .map(([label, count]) => ({
      label,
      count,
      destination: articleMeta[label]?.destination || null,
      language: articleMeta[label]?.language || null,
      languageLabel: articleMeta[label]?.language
        ? languageLabel(articleMeta[label].language)
        : null,
    }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))
    .slice(0, 12);

  const languageRows = sortCountRows(languages, languageLabel);
  const ctaRows = sortCountRows(ctaLocations, (key) => {
    const labels = {
      hero: "Hero / Header",
      navbar: "Top Nav",
      footer: "Footer",
      mobile_menu: "Mobile Menu",
      insights_grid: "Article Grid",
    };
    return labels[key] || key;
  });
  const scrollRows = Object.entries(scrollDepths)
    .map(([key, count]) => ({ key, label: key + "% Down Page", count }))
    .sort((a, b) => Number(a.key) - Number(b.key));

  const ctaTotal = ctaRows.reduce((n, row) => n + row.count, 0);

  return {
    hasData: insightsEvents.length > 0,
    totals: {
      sessions: sessions.size,
      pageviews,
      articleClicks: Object.values(articleClicks).reduce((n, c) => n + c, 0),
      ctaClicks: ctaTotal,
    },
    dashboard: buildLandingDashboard(insightsEvents),
    articleClicks: articleRows,
    languages: languageRows,
    ctaLocations: ctaRows,
    scrollDepths: scrollRows,
  };
}
