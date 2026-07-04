/**
 * Surface classification for marketing analytics events.
 * `landing` = Railway homepage landing / Webflow embed.
 * `insights` = dealality.com/insights hub.
 */

export const SURFACE_LANDING = "landing";
export const SURFACE_INSIGHTS = "insights";

export function normalizeSurface(value) {
  const s = String(value || "").trim().toLowerCase();
  if (s === SURFACE_INSIGHTS) return SURFACE_INSIGHTS;
  if (s === SURFACE_LANDING) return SURFACE_LANDING;
  return null;
}

export function inferSurfaceFromPath(path) {
  const p = String(path || "").toLowerCase();
  if (p === "/insights" || p.startsWith("/insights/") || p.startsWith("/insights?")) {
    return SURFACE_INSIGHTS;
  }
  return SURFACE_LANDING;
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

export function filterEventsBySurface(events, surface) {
  const target = normalizeSurface(surface);
  if (!target) return events;
  return (events || []).filter((e) => eventSurface(e) === target);
}
