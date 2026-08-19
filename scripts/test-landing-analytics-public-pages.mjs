/**
 * Dealality public analytics — surface helpers smoke test.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import {
  eventSurface,
  inferSurfaceFromPath,
  isLandingEvent,
  isInsightsEvent,
  isOpportunityReviewEvent,
  isSiteEvent,
  SURFACE_LANDING,
  SURFACE_INSIGHTS,
  SURFACE_OPPORTUNITY_REVIEW,
  SURFACE_SITE,
} from "../lib/marketing-landing-events-surface.js";
import { buildPopularLandingPages } from "../lib/marketing-landing-events-dashboard.js";

assert.equal(inferSurfaceFromPath("/"), SURFACE_LANDING);
assert.equal(inferSurfaceFromPath("/es"), SURFACE_LANDING);
assert.equal(inferSurfaceFromPath("/insights"), SURFACE_INSIGHTS);
assert.equal(inferSurfaceFromPath("/es/insights/foo"), SURFACE_INSIGHTS);
assert.equal(inferSurfaceFromPath("/insights-posts/marriott-hilton"), SURFACE_INSIGHTS);
assert.equal(inferSurfaceFromPath("/opportunity-review"), SURFACE_OPPORTUNITY_REVIEW);
assert.equal(inferSurfaceFromPath("/es/who-its-for"), SURFACE_SITE);
assert.equal(inferSurfaceFromPath("/terms"), SURFACE_SITE);
assert.equal(inferSurfaceFromPath("/marketing/dealality-landing-v7.html"), SURFACE_LANDING);

assert.equal(eventSurface({ surface: "site", path: "/" }), SURFACE_SITE);
assert.ok(isLandingEvent({ path: "/" }));
assert.ok(isInsightsEvent({ path: "/insights" }));
assert.ok(isOpportunityReviewEvent({ path: "/opportunity-review" }));
assert.ok(isSiteEvent({ path: "/who-its-for" }));

const popular = buildPopularLandingPages([
  { event: "page_land", path: "/who-its-for?utm_source=x", embed: false },
  { event: "page_land", path: "/who-its-for", embed: false },
  { event: "page_land", path: "/insights", embed: false },
  { event: "page_land", embed: true, path: "/?embed=1" },
  { event: "scroll_depth", path: "/who-its-for" },
]);

assert.equal(popular[0].label, "/who-its-for");
assert.equal(popular[0].pageviews, 2);
assert.ok(popular.some((r) => r.label === "Homepage Embed (dealality.com)"));
assert.ok(popular.some((r) => r.label === "/insights"));

const here = path.dirname(fileURLToPath(import.meta.url));
const analyticsSrc = fs.readFileSync(
  path.join(here, "../public/js/dealality-public-analytics.js"),
  "utf8"
);
assert.match(
  analyticsSrc,
  /hotel-owner\|brand\|member\|asset-manager\|user-management\|my-brands-v2\|ai-visibility/
);
const noticeSrc = fs.readFileSync(
  path.join(here, "../public/js/dealality-webflow-account-notice.js"),
  "utf8"
);
assert.match(noticeSrc, /platform-skip/);
assert.match(noticeSrc, /d\.isBrand \|\| d\.canAccessBrandWorkspace/);
assert.match(noticeSrc, /wrappedToast\.options = original\.options/);

console.log("ok: public analytics surface + popular pages");
