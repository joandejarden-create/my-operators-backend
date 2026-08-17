#!/usr/bin/env node
/**
 * Guard: Executive Summary gap tiles must stay inside the selected geography.
 * Global view may use Regional Gap across commercial regions (never Global vs Region).
 */
import assert from "node:assert/strict";
import { buildExecutiveInsightBoxes } from "../lib/ai-visibility/brand-executive-insights.js";

const geographySummary = [
  {
    geography: "Global",
    brandsMonitored: 5,
    topBrandByAiPresence: {
      brandName: "Autograph Collection",
      presence: 0.857,
      display: "85.7%",
    },
  },
  {
    geography: "Europe",
    brandsMonitored: 5,
    topBrandByAiPresence: {
      brandName: "Autograph Collection",
      presence: 0.857,
      display: "85.7%",
    },
  },
  {
    geography: "CALA",
    brandsMonitored: 5,
    topBrandByAiPresence: {
      brandName: "Autograph Collection",
      presence: 0.81,
      display: "81%",
    },
  },
  {
    geography: "North America",
    brandsMonitored: 5,
    topBrandByAiPresence: {
      brandName: "Autograph Collection",
      presence: 0.7,
      display: "70%",
    },
  },
];

const cala = buildExecutiveInsightBoxes({
  geographyKey: "CALA",
  geographyScope: "Region",
  allowCrossRegionTakeaways: false,
  topByPresence: {
    brandName: "Autograph Collection",
    brandId: "a",
    presence: 0.81,
    display: "81%",
  },
  weakestPresence: {
    brandName: "Tribute Portfolio",
    brandId: "b",
    presence: 0.5,
    display: "50%",
  },
  geographySummary,
});

const calaGap = cala.boxes.find((b) => b.type === "WEAKEST_PRESENCE_AREA");
assert.ok(calaGap, "CALA should emit a portfolio gap when brands differ");
assert.equal(calaGap.title, "Portfolio Gap");
assert.match(String(calaGap.takeaway), /In CALA/);
assert.doesNotMatch(String(calaGap.takeaway), /\bEurope\b|\bGlobal\b|Across regions/i);

const calaFull = buildExecutiveInsightBoxes({
  geographyKey: "CALA",
  geographyScope: "Region",
  brandsMonitoredDisplay: "5 of 5",
  topByPresence: {
    brandName: "Autograph Collection",
    brandId: "a",
    presence: 0.917,
    display: "91.7%",
  },
  weakestPresence: {
    brandName: "AC Hotels by Marriott",
    brandId: "c",
    presence: 0.083,
    display: "8.3%",
  },
  questionsMissing: { value: 1, denominator: 12, display: "8.3%" },
  topMissingPromptFamily: {
    promptFamily: "Branded Residences",
    QUESTIONS_MISSING: 1,
    MONITORED_QUESTIONS: 2,
  },
  crossProvider: {
    NOT_COMPARABLE: false,
    PROVIDER_DISAGREEMENT: { status: "DISAGREE" },
    STRONGEST_PROVIDER_BY_PRESENCE: { provider: "openai", rate: 0.917 },
    WEAKEST_PROVIDER_BY_PRESENCE: { provider: "perplexity", rate: 0.619 },
    PRESENCE_RANGE: { spread: 0.298 },
    PROVIDERS_MONITORED: ["claude", "gemini", "openai", "perplexity"],
    BY_PROVIDER: {
      claude: { rate: 0.857 },
      gemini: { rate: 0.845 },
      openai: { rate: 0.917 },
      perplexity: { rate: 0.619 },
    },
  },
  geographySummary,
});

for (const box of calaFull.boxes) {
  assert.doesNotMatch(
    String(box.takeaway),
    /—/,
    `${box.type} takeaway must not use em dashes`
  );
  const sentences = String(box.takeaway)
    .split(/(?<=\.)\s+/)
    .filter(Boolean);
  assert.equal(
    sentences.length,
    2,
    `${box.type} takeaway should be two sentences (got ${sentences.length}): ${box.takeaway}`
  );
  assert.ok(
    String(box.takeaway).length >= 210,
    `${box.type} takeaway should be long enough for ~5 wrapped lines (got ${String(box.takeaway).length})`
  );
}

const calaForcedFlag = buildExecutiveInsightBoxes({
  geographyKey: "CALA",
  geographyScope: "Region",
  // Mis-set flag must not open cross-region tiles on a Region view.
  allowCrossRegionTakeaways: true,
  topByPresence: {
    brandName: "Autograph Collection",
    brandId: "a",
    presence: 0.81,
    display: "81%",
  },
  weakestPresence: {
    brandName: "Tribute Portfolio",
    brandId: "b",
    presence: 0.5,
    display: "50%",
  },
  geographySummary,
});
const forcedGap = calaForcedFlag.boxes.find((b) => b.type === "WEAKEST_PRESENCE_AREA");
assert.equal(forcedGap?.title, "Portfolio Gap");
assert.doesNotMatch(String(forcedGap?.takeaway || ""), /\bEurope\b|Across regions/i);

const global = buildExecutiveInsightBoxes({
  geographyKey: "Global",
  geographyScope: "Global",
  allowCrossRegionTakeaways: true,
  topByPresence: {
    brandName: "Autograph Collection",
    brandId: "a",
    presence: 0.857,
    display: "85.7%",
  },
  weakestPresence: {
    brandName: "Tribute Portfolio",
    brandId: "b",
    presence: 0.5,
    display: "50%",
  },
  geographySummary,
});
const regionalGap = global.boxes.find((b) => b.type === "WEAKEST_PRESENCE_AREA");
assert.ok(regionalGap, "Global view should emit Regional Gap when regions differ");
assert.equal(regionalGap.title, "Regional Gap");
assert.match(String(regionalGap.takeaway), /Across regions/);
assert.doesNotMatch(
  String(regionalGap.takeaway),
  /lead AI Presence is .* in Global versus|Global trails/i
);
assert.match(String(regionalGap.takeaway), /North America/);
assert.match(String(regionalGap.takeaway), /Europe|CALA/);

console.log("test-ai-visibility-executive-insight-geography-scope: PASS");
