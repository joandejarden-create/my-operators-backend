/**
 * Surface classification for marketing analytics events.
 * `landing` = Railway homepage landing / Webflow embed / old-home.
 * `insights` = dealality.com/insights hub.
 * `opportunity_review` = /opportunity-review form.
 * `site` = other public marketing pages (who-its-for, terms, privacy, …).
 */

export const SURFACE_LANDING = "landing";
export const SURFACE_INSIGHTS = "insights";
export const SURFACE_OPPORTUNITY_REVIEW = "opportunity_review";
export const SURFACE_SITE = "site";

const KNOWN_SURFACES = new Set([
  SURFACE_LANDING,
  SURFACE_INSIGHTS,
  SURFACE_OPPORTUNITY_REVIEW,
  SURFACE_SITE,
]);

export function normalizeSurface(value) {
  const s = String(value || "").trim().toLowerCase();
  if (KNOWN_SURFACES.has(s)) return s;
  return null;
}

export function inferSurfaceFromPath(path) {
  const raw = String(path || "").toLowerCase();
  const noQuery = raw.split("?")[0];
  const p =
    noQuery.length > 1 && noQuery.endsWith("/") ? noQuery.slice(0, -1) : noQuery || "/";
  const rest = p === "/es" ? "/" : p.startsWith("/es/") ? p.slice(3) || "/" : p;

  if (
    rest === "/insights" ||
    rest.startsWith("/insights/") ||
    rest === "/insights-posts" ||
    rest.startsWith("/insights-posts/")
  ) {
    return SURFACE_INSIGHTS;
  }
  if (rest === "/opportunity-review" || rest.startsWith("/opportunity-review/")) {
    return SURFACE_OPPORTUNITY_REVIEW;
  }
  if (
    rest === "/" ||
    rest === "/old-home" ||
    rest === "/home-legacy" ||
    /\/v[789](\/|$)/.test(rest) ||
    rest.startsWith("/marketing/landing") ||
    rest.startsWith("/marketing/dealality-landing")
  ) {
    return SURFACE_LANDING;
  }
  return SURFACE_SITE;
}

export function eventSurface(event) {
  return normalizeSurface(event?.surface) || inferSurfaceFromPath(event?.path);
}

export function isInsightsEvent(event) {
  return eventSurface(event) === SURFACE_INSIGHTS;
}

export function isLandingEvent(event) {
  return eventSurface(event) === SURFACE_LANDING;
}

export function isSiteEvent(event) {
  return eventSurface(event) === SURFACE_SITE;
}

export function isOpportunityReviewEvent(event) {
  return eventSurface(event) === SURFACE_OPPORTUNITY_REVIEW;
}

export function filterEventsBySurface(events, surface) {
  const target = normalizeSurface(surface);
  if (!target) return events;
  return (events || []).filter((e) => eventSurface(e) === target);
}
