#!/usr/bin/env node
/**
 * Market Alerts V1.3 — actionability, sanitization, content quality, tags.
 */
import {
  isActionableForAudience,
  computeActionableFlags,
  actionabilityInvariantHolds,
} from "../lib/market-alerts-actionability.js";
import {
  isLowValuePersonnelNews,
  isConsumerPromotionalNoise,
  assessContentQuality,
  PATCH_CONSUMER_HEADLINE,
  NOVOTEL_FB_HEADLINE,
} from "../lib/market-alerts-content-quality.js";
import {
  sanitizeUserFacingTags,
  isInternalMarketAlertTag,
} from "../lib/market-alerts-user-tags.js";
import {
  sanitizeMarketAlertPlainText,
  stripGoogleNewsWrapperUrls,
} from "../lib/market-alerts-plain-text.js";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import { getRssSyncIntervalMinutes } from "../lib/market-alerts-rss-schedule.js";
import { getEarlySignalIntervalMinutes } from "../lib/market-alerts-early-signal-schedule.js";

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("OK:", msg);
  }
}

// 1. Unflagged proposed hotel — Brand + Operator actionable
{
  const intel = computeMarketAlertIntelligence({
    title: "Planning application filed for new 180-room hotel in Austin",
    summary:
      "A developer submitted plans for an unbranded full-service hotel with no operator named.",
  });
  assert(intel.meta.audience.brand.worthReviewing, "unflagged brand WR");
  assert(intel.meta.actionable.brand, "unflagged brand actionable");
  assert(intel.meta.actionable.operator, "unflagged operator actionable");
}

// 2. Hilton-branded planning — Brand not actionable
{
  const intel = computeMarketAlertIntelligence({
    title: "Hilton signs for new hotel in downtown Dallas planning phase",
    summary: "The project will operate as a Hilton Garden Inn upon completion.",
  });
  assert(!intel.meta.actionable?.brand, "branded planning brand not actionable");
}

// 3. Management agreement — Operator not actionable
{
  const intel = computeMarketAlertIntelligence({
    title: "Highgate signs management agreement for new Boston hotel",
    summary: "The owner selected Highgate as the operating partner for the development.",
  });
  assert(!intel.meta.actionable?.operator, "management agreement operator not actionable");
}

// 4. Hotel For Sale — Owner actionable
{
  const intel = computeMarketAlertIntelligence({
    title: "Rainier Square Hotel in Seattle for Sale",
    summary: "A 220-room hotel in downtown Seattle has been listed for sale.",
  });
  assert(intel.meta.actionable.owner, "hotel for sale owner actionable");
}

// 5. Completed sale — Owner not actionable
{
  const intel = computeMarketAlertIntelligence({
    title: "Investor acquires downtown boutique hotel",
    summary: "The buyer purchased the 120-room property from a private owner.",
  });
  assert(!intel.meta.actionable?.owner, "completed acquisition owner not actionable");
}

// 6. Rejected project — not actionable
{
  const flags = computeActionableFlags({
    treatment: "REVIEW",
    brand: {
      worthReviewing: true,
      signalType: "Potential Development Opportunity",
      decisionStage: "Early",
    },
    signalTiming: "Pre-Decision",
    projectDirection: "Rejected / Blocked",
    eventType: "Planning Application",
  });
  assert(!flags.brand, "rejected project brand not actionable");
}

// Invariants
assert(
  actionabilityInvariantHolds("brand", { signalType: "Potential Development Opportunity", decisionStage: "Likely Decided" }, "Decision Announced") === false,
  "development + decided invariant fails"
);
assert(
  actionabilityInvariantHolds("operator", { signalType: "Potential Management Opportunity", decisionStage: "Active" }, "Pre-Decision"),
  "management opportunity invariant holds"
);

// Sanitization
{
  const raw = '<a href="https://news.google.com/rss/articles/abc">Click here</a>';
  const cleaned = sanitizeMarketAlertPlainText(raw);
  assert(!/<a\b/i.test(cleaned), "no raw anchor markup");
  assert(cleaned.includes("Click here"), "anchor inner text preserved");
}
{
  const blob = "https://news.google.com/rss/articles/CBMiABCDEFG";
  assert(stripGoogleNewsWrapperUrls(blob) === "", "google wrapper blob stripped");
}

// Internal tags
assert(sanitizeUserFacingTags(["RSS", "EARLY_SIGNAL", "EARLY_SIGNAL_DEVELOPMENT", "Deals"]).length === 1, "internal tags hidden");
assert(isInternalMarketAlertTag("EARLY_SIGNAL_MIXED_USE"), "EARLY_SIGNAL_MIXED_USE internal");

// Personnel
assert(isLowValuePersonnelNews(NOVOTEL_FB_HEADLINE), "Novotel F&B low-value personnel");
assert(!isLowValuePersonnelNews("Marriott appoints Jane Doe as Chief Development Officer for Americas expansion"), "CDO strategic exception");

// Consumer noise
assert(isConsumerPromotionalNoise(PATCH_CONSUMER_HEADLINE), "Patch consumer roundup ignored");
assert(!isConsumerPromotionalNoise("Hotel XYZ listed for sale for $10 million"), "legitimate sale not rejected");

// Content quality → IGNORE
{
  const q = assessContentQuality({ title: NOVOTEL_FB_HEADLINE, summary: "" });
  assert(q.ignore, "personnel assessContentQuality ignore");
  const intel = computeMarketAlertIntelligence({ title: PATCH_CONSUMER_HEADLINE, summary: "" });
  assert(intel.meta.treatment === "IGNORE", "Patch headline IGNORE treatment");
}

// Weekly scheduling defaults
assert(getRssSyncIntervalMinutes() === 10080, "RSS weekly default 10080");
assert(getEarlySignalIntervalMinutes() === 10080, "Early signals weekly default 10080");

console.log(failed ? `\n${failed} test(s) failed` : "\nAll Market Alerts V1.3 tests passed");
process.exit(failed ? 1 : 0);
