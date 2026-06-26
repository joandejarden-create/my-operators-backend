/**
 * Landing funnel benchmarks — single source of truth for report targets.
 * goodMin = amber floor; targetRate = green threshold.
 */
export const LANDING_ANALYTICS_BENCHMARKS = [
  {
    funnelKey: "scrolled",
    label: "Started Scrolling",
    targetRate: 70,
    goodMin: 55,
  },
  {
    funnelKey: "past_hero",
    label: "Moved Past Hero",
    targetRate: 50,
    goodMin: 40,
  },
  {
    funnelKey: "reached_how",
    label: "Saw Platform Section",
    targetRate: 35,
    goodMin: 25,
  },
  {
    funnelKey: "deep_engagement",
    label: "Deep Read (FAQ / Why)",
    targetRate: 25,
    goodMin: 15,
  },
  {
    funnelKey: "reached_cta",
    label: "Reached Bottom CTA",
    targetRate: 20,
    goodMin: 12,
  },
  {
    funnelKey: "cta_click",
    label: "Clicked Signup CTA",
    targetRate: 8,
    goodMin: 4,
  },
];

export const GA4_PROPERTY_ID = "G-8ZW8FDHBV2";

export function ga4RealtimeUrl() {
  return "https://analytics.google.com/analytics/web/#/p0/reports/realtime";
}
