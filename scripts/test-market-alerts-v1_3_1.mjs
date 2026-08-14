#!/usr/bin/env node
/**
 * Market Alerts V1.3.1 — stakeholder access, generic fallback, Top Read, source sanitization.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  MARKET_ALERTS_STAKEHOLDER_MATRIX,
  canonicalMarketAlertsAudience,
  marketAlertsFiltersEnabled,
  normalizeMarketAlertsAudienceParam,
} from "../lib/market-alerts-audience-resolve.js";
import {
  getUserFacingSourceName,
  userFacingTextHasInternalMetadata,
} from "../lib/market-alerts-user-facing.js";
import { intelligencePayloadForAudience, computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import { googleNewsSourceLabel } from "../lib/market-alerts-early-signal-queries.js";
import { isActionableForAudience } from "../lib/market-alerts-actionability.js";
import {
  assessContentQuality,
  isLowValuePersonnelNews,
  isConsumerPromotionalNoise,
  PATCH_CONSUMER_HEADLINE,
  NOVOTEL_FB_HEADLINE,
} from "../lib/market-alerts-content-quality.js";
import { sanitizeMarketAlertPlainText } from "../lib/market-alerts-plain-text.js";
import { dedupeFeedItemsByEntityKey } from "../lib/market-alerts-correlation.js";
import { getRssSyncIntervalMinutes } from "../lib/market-alerts-rss-schedule.js";
import { getEarlySignalIntervalMinutes } from "../lib/market-alerts-early-signal-schedule.js";
import { sanitizeUserFacingTags } from "../lib/market-alerts-user-tags.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("OK:", msg);
  }
}

const html = fs.readFileSync(path.join(ROOT, "public", "market-alerts.html"), "utf8");
const uiJs = fs.readFileSync(path.join(ROOT, "public", "market-alerts.js"), "utf8");

// --- STAKEHOLDER ACCESS ---
for (const row of MARKET_ALERTS_STAKEHOLDER_MATRIX) {
  assert(row.filtersEnabled === true, `${row.stakeholder} filters enabled`);
  assert(marketAlertsFiltersEnabled(row.stakeholder) === true, `${row.stakeholder} filters helper`);
  const flags = row.flags === undefined ? undefined : row.flags;
  if (row.flags && (row.flags.isOwner || row.flags.isBrand || row.flags.isOperator || row.flags.isAdmin || row.flags.isDemo)) {
    const audience = canonicalMarketAlertsAudience(row.flags);
    assert(audience === row.specializedAudience, `${row.stakeholder} audience=${audience}`);
  } else if (row.flags === null) {
    assert(canonicalMarketAlertsAudience(null) === "all", "Unsigned → all");
  }
}

assert(canonicalMarketAlertsAudience({ isOwner: true }) === "owner", "Owner specialized");
assert(canonicalMarketAlertsAudience({ isBrand: true }) === "brand", "Brand specialized");
assert(canonicalMarketAlertsAudience({ isOperator: true }) === "operator", "Operator specialized");
assert(canonicalMarketAlertsAudience({ isAdmin: true }) === "all", "Admin → all");
assert(canonicalMarketAlertsAudience({ isDemo: true }) === "all", "Demo → all");
assert(canonicalMarketAlertsAudience({ isBrand: true, isOperator: true }) === "brand", "Both → brand");
assert(canonicalMarketAlertsAudience({}) === "all", "empty flags → all");
assert(normalizeMarketAlertsAudienceParam("all") === "all", "query audience=all");
assert(normalizeMarketAlertsAudienceParam("owner") === "owner", "query audience=owner");

assert(/id="feedModeActionable"/.test(html) && !/id="feedModeActionable"[^>]*disabled/.test(html), "Actionable tab not disabled in HTML");
assert(/id="feedModeWorth"/.test(html) && !/id="feedModeWorth"[^>]*disabled/.test(html), "Worth Reviewing tab not disabled in HTML");
assert(/id="feedModeAll"/.test(html), "All Market Activity tab present");
assert(!/Actionable needs an Owner/.test(uiJs), "no Owner/Brand/Operator filter lock copy");
assert(!/audience_unavailable/.test(uiJs), "no audience_unavailable UI branch");

// --- GENERIC FALLBACK COPY ---
{
  const fields = {
    Title: "Planning application filed for new 180-room hotel in Austin",
    Summary: "A developer submitted plans for an unbranded full-service hotel with no operator named.",
    "Event Type": "Planning Application",
    "Worth Reviewing — Brand": true,
    "Worth Reviewing — Operator": true,
    "Actionable — Brand": true,
    "Actionable — Operator": true,
    "Signal Type — Brand": "Potential Development Opportunity",
    "Signal Type — Operator": "Potential Management Opportunity",
    "Intelligence Treatment": "REVIEW",
  };
  const generic = intelligencePayloadForAudience(fields, "all");
  assert(generic.audience === "all", "generic audience=all");
  assert(generic.actionable === true, "generic actionable = union");
  assert(generic.worthReviewing === true, "generic WR = union");
  assert(generic.signalType === "Potential Hotel Development Opportunity", "generic open signal is neutral");
  assert(!/for your workspace/i.test(generic.whyItMatters || ""), "generic why is not workspace-specific");
  assert(!/\bOwner\b/.test(generic.whyItMatters || ""), "generic why has no Owner persona");
  assert(!/\bBrand selection\b/.test(generic.whyItMatters || ""), "generic why has no Brand persona copy");
  assert(!/\boperator\b/i.test(generic.recommendedAction || ""), "generic action has no operator persona");

  const brand = intelligencePayloadForAudience(fields, "brand");
  assert(brand.audience === "brand", "brand payload remains specialized");
  assert(brand.signalType === "Potential Development Opportunity", "brand signal preserved");
}

{
  const ownerSale = {
    Title: "Rainier Square Hotel in Seattle for Sale",
    Summary: "A 220-room hotel in downtown Seattle has been listed for sale.",
    "Event Type": "Hotel For Sale",
    "Worth Reviewing — Owner": true,
    "Actionable — Owner": true,
    "Signal Type — Owner": "Capital / Transaction Signal",
    "Decision Stage — Owner": "Active",
    "Intelligence Treatment": "REVIEW",
  };
  const generic = intelligencePayloadForAudience(ownerSale, "all");
  assert(generic.actionable === true, "generic sale is actionable");
  const owner = intelligencePayloadForAudience(ownerSale, "owner");
  assert(owner.actionable === true, "owner sale remains actionable");
  assert(
    isActionableForAudience({
      audience: "owner",
      worthReviewing: true,
      signalType: "Capital / Transaction Signal",
      decisionStage: "Active",
      eventType: "Hotel For Sale",
    }),
    "owner sale actionability helper unchanged"
  );
}

{
  const wrOnly = {
    Title: "IHG Signs Noted Collection Hotel in Riyadh",
    Summary: "A franchise agreement was signed for a 107-key conversion.",
    "Event Type": "Brand Signing",
    "Worth Reviewing — Brand": true,
    "Actionable — Brand": false,
    "Signal Type — Brand": "Competitive Brand Move",
    "Decision Stage — Brand": "Likely Decided",
    "Intelligence Treatment": "REVIEW",
  };
  const generic = intelligencePayloadForAudience(wrOnly, "all");
  assert(generic.actionable === false, "signed brand move not generic actionable");
  assert(generic.worthReviewing === true, "signed brand move is generic WR");
  assert(generic.signalType === "Market Intelligence", "generic WR uses Market Intelligence");
  assert(/no currently open action window/i.test(generic.whyItMatters || ""), "generic WR copy");
}

// --- SOURCE SANITIZATION ---
assert(getUserFacingSourceName("Google News (EARLY_SIGNAL_DEVELOPMENT)") === "Google News", "dev family stripped");
assert(getUserFacingSourceName("Google News (EARLY_SIGNAL_PLANNING)") === "Google News", "planning family stripped");
assert(getUserFacingSourceName("Google News (EARLY_SIGNAL_ADAPTIVE_REUSE)") === "Google News", "adaptive family stripped");
assert(getUserFacingSourceName("Hospitality Net") === "Hospitality Net", "Hospitality Net preserved");
assert(getUserFacingSourceName("ET HospitalityWorld") === "ET HospitalityWorld", "ET HospitalityWorld preserved");
assert(getUserFacingSourceName("Early Bird Hospitality") === "Early Bird Hospitality", "legitimate Early publisher preserved");
assert(!userFacingTextHasInternalMetadata("Google News"), "clean Google News has no internal meta");
assert(userFacingTextHasInternalMetadata("Google News (EARLY_SIGNAL_DEVELOPMENT)"), "raw family is internal");
assert(googleNewsSourceLabel("planning") === "Google News", "future writes use clean Source Name");

const tags = sanitizeUserFacingTags(["RSS", "EARLY_SIGNAL", "EARLY_SIGNAL_PLANNING", "Deals"]);
assert(JSON.stringify(tags) === JSON.stringify(["Deals"]), "internal tags hidden");

// --- TOP READ / RAIL MARKERS ---
assert(/id="topReadList"/.test(html), "Top Read list restored");
assert(/Top Read/.test(html), "Top Read heading restored");
assert(/id="actionableNowRail"/.test(html), "Actionable Now rail present");
assert(/id="worthReviewingRail"/.test(html), "Worth Reviewing rail present");
assert(/Latest Market Activity/.test(html), "Latest Market Activity present");
assert(/topRead: data\.topRead/.test(uiJs) || /data\.topRead \|\| \[\]/.test(uiJs), "UI uses actual topRead array");
assert(!/topRead: latestMarketActivity/.test(fs.readFileSync(path.join(ROOT, "api", "market-alerts.js"), "utf8")), "API does not alias topRead to latest");

// --- NO INTERNAL METADATA IN UI SOURCE PATH ---
assert(/getUserFacingSourceName/.test(uiJs), "UI applies source sanitizer");

// --- V1.3 REGRESSIONS ---
{
  const htmlFix = sanitizeMarketAlertPlainText('<a href="https://x.com">Hotel sold</a>');
  assert(!/</.test(htmlFix), "raw HTML remains stripped");
  assert(/Hotel sold/.test(htmlFix), "anchor inner text preserved");
}
{
  const qN = assessContentQuality({ title: NOVOTEL_FB_HEADLINE, summary: "F&B appointment announced" });
  assert(qN.ignore, "Novotel F&B ignored");
  assert(isLowValuePersonnelNews(NOVOTEL_FB_HEADLINE), "Novotel personnel helper");
}
{
  const qP = assessContentQuality({ title: PATCH_CONSUMER_HEADLINE, summary: "$10 hotel rooms roundup" });
  assert(qP.ignore, "Patch consumer ignored");
  assert(isConsumerPromotionalNoise(PATCH_CONSUMER_HEADLINE), "Patch promotional helper");
  const intel = computeMarketAlertIntelligence({ title: PATCH_CONSUMER_HEADLINE, summary: "" });
  assert(intel.meta.treatment === "IGNORE", "Patch headline IGNORE treatment");
}
{
  const intel = computeMarketAlertIntelligence({
    title: "Mount Laurel wants to buy two distressed hotel properties to develop into affordable housing.",
    summary: "The township is considering converting two hotels into housing.",
  });
  assert(!intel.meta.audience.brand.worthReviewing, "Mount Laurel brand not WR");
  assert(!intel.meta.audience.operator.worthReviewing, "Mount Laurel operator not WR");
}
{
  const titles = [
    "Phoenix Office Tower to Become 340-Room JW Marriott - Hoodline",
    "Developer plans to convert Arizona Center office building to JW Marriott hotel - yourvalley.net",
  ];
  const now = new Date().toISOString();
  const items = titles.map((title, i) => {
    const intel = computeMarketAlertIntelligence({ title, summary: "" });
    return {
      id: `rec${i}`,
      fields: { Title: title, "Published At": now },
      intelligence: {
        eventType: intel.meta?.event?.eventType,
        entities: intel.meta?.entities,
      },
    };
  });
  const deduped = dedupeFeedItemsByEntityKey(items, { windowDays: 14 });
  assert(deduped.length === 1, "Phoenix WR entity dedupe");
}

assert(getRssSyncIntervalMinutes() === 10080, "RSS weekly default");
assert(getEarlySignalIntervalMinutes() === 10080, "Early signals weekly default");

if (failed) {
  console.error(`\n${failed} Market Alerts V1.3.1 test(s) failed`);
  process.exit(1);
}
console.log("\nAll Market Alerts V1.3.1 tests passed");
