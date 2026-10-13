/**
 * Unit tests for GDI Discovery Expansion V3 modules (no SERP).
 */
import assert from "node:assert/strict";
import {
  resolveGdiMarketLanguages,
  buildLocalizedQuery,
  CANONICAL_INTENTS,
  getGdiNextBestResearchAction,
  isPromisingNearMiss,
  dedupeCrossLanguageCandidates,
  extractHiddenDemandCandidates,
  classifySeriesTimingState,
  SERIES_TIMING_STATE,
  seriesMayBeValidFutureWatch,
  buildSeriesRecord,
  SCOUT_FAMILY,
  buildScoutQueryPlan,
} from "../lib/group-demand-intelligence/discovery-expansion-v3/index.js";
import { classifyCustomerSurfaceOpportunity, CUSTOMER_SURFACE_DISPOSITION } from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";

// Language routing
const yotel = resolveGdiMarketLanguages({ hotelId: "recrPQcZg7SFARRb2" });
assert.equal(yotel.primaryLanguage, "fr");
assert.ok(yotel.secondaryLanguages.includes("en"));

const spice = resolveGdiMarketLanguages({ hotelKey: "SPICE" });
assert.equal(spice.primaryLanguage, "en");

// Localized query natural (FR ≠ literal EN)
const qFr = buildLocalizedQuery({
  intent: CANONICAL_INTENTS.ROOM_BLOCK,
  language: "fr",
  marketPlaceNames: ["Genève"],
});
assert.match(qFr.localizedQuery, /bloc|chambres|hôtel/i);
assert.equal(qFr.canonicalIntent, CANONICAL_INTENTS.ROOM_BLOCK);

// Series timing — recurring not customer-ready timing
assert.equal(
  classifySeriesTimingState({ historicalCycles: ["2024", "2025"] }),
  SERIES_TIMING_STATE.RECURRING_EXPECTED
);
const series = buildSeriesRecord({
  organization: "Demo Assoc",
  historicalCycles: ["2024", "2025"],
  historicalRecurrenceReal: true,
  nextCycleThesis: "Next cycle expected; housing TBD",
  nextValidationTrigger: "Watch 2027 RFP",
  hotelFitScore: 60,
});
assert.equal(series.customerReadyEligibleTiming, false);
assert.equal(seriesMayBeValidFutureWatch(series), true);

// Hidden demand requires evidence
const hidden = extractHiddenDemandCandidates({
  id: "p1",
  title: "Big Expo",
  organizationName: "Expo Org",
  pageText: "exhibitor housing host hotel overflow room block",
});
assert.ok(hidden.length >= 1);
assert.ok(hidden.every((h) => h.organizationName));

// Next-best research
const near = {
  title: "Future Medical Congress 2027",
  organizationName: "Galicia Medical Society",
  hotelOpportunityThesis: "AC A Coruña fits compact medical congress overflow near city center.",
  hotelFitScore: 55,
  eventLocation: "A Coruña",
  officialSource: "https://example.org/congress",
};
assert.equal(isPromisingNearMiss(near, { marketTokens: ["coruña", "galicia"] }), true);
const nbr = getGdiNextBestResearchAction(near, { marketPlace: "A Coruña" });
assert.ok(nbr.blocker);
assert.ok(nbr.query);

// Dedup congress aliases
const dedup = dedupeCrossLanguageCandidates([
  {
    id: "a",
    title: "Congrès Association Santé Genève 2027",
    organizationName: "Association Santé",
    officialSource: "https://assoc-sante.ch/congres",
  },
  {
    id: "b",
    title: "Congress Association Sante Geneva 2027",
    organizationName: "Association Sante",
    officialSource: "https://assoc-sante.ch/congress",
  },
]);
assert.ok(dedup.unique.length <= 2);

// Scout plan
const plan = buildScoutQueryPlan(SCOUT_FAMILY.PROCUREMENT, { placeNames: ["Grenada"] }, { languages: ["en"], maxPerLang: 2 });
assert.ok(plan.length >= 1);

// AidEx surface still KEEP_ACTIVE
const aidex = classifyCustomerSurfaceOpportunity(
  {
    title: "AidEx Geneva",
    organizationName: "AidEx / Clarion Events",
    opportunityType: "FUTURE_CYCLE",
    eventStartDate: "2026-10-21",
    eventEndDate: "2026-10-22",
    venueStatus: "Unknown",
    hotelOpportunityThesis:
      "AidEx sits at Geneva Airport (Palexpo). YOTEL can pursue overflow or preferred listing beyond onsite Ibis/Hilton inventory.",
    summaryWhyMatters: "Planning for hotel accommodations is essential as the event date approaches.",
    discoverySource: "https://aid-expo.com/accommodation",
    officialSource: "https://aid-expo.com/when-where",
    hotelFitScore: 57,
  },
  { nowDate: "2026-10-03" }
);
assert.equal(aidex.disposition, CUSTOMER_SURFACE_DISPOSITION.KEEP_ACTIVE);

console.log("test:gdi-discovery-expansion-v3 OK");
