#!/usr/bin/env node
/**
 * Unit tests for Market Alerts actionable intelligence (deterministic V1).
 */
import { inferMarketAlertEvent } from "../lib/market-alerts-event-infer.js";
import { extractMarketAlertEntities } from "../lib/market-alerts-entity-extract.js";
import { resolveAudienceIntelligence } from "../lib/market-alerts-audience-rules.js";
import { buildAudienceTemplates } from "../lib/market-alerts-templates.js";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import { MAP_INTEL } from "../api/lib/market-alerts-intelligence-map.js";

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("OK:", msg);
  }
}

// Rainier acceptance: For Sale
{
  const title = "Rainier Square Hotel in Seattle for Sale";
  const summary = "A 220-room hotel in downtown Seattle has been listed for sale.";
  const event = inferMarketAlertEvent({ title, summary });
  assert(event.eventType === "Hotel For Sale", "Rainier event = Hotel For Sale");
  assert(
    event.whatChanged === "Property is being marketed for sale",
    "Rainier whatChanged"
  );
  const entities = extractMarketAlertEntities({
    title,
    summary,
    eventType: event.eventType,
  });
  assert(entities.rooms === 220, "Rainier rooms=220");
  assert(entities.assetProjectStage === "Operating", "Rainier stage Operating");
  const aud = resolveAudienceIntelligence({
    eventType: event.eventType,
    title,
    summary,
    entities,
  });
  assert(aud.owner.worthReviewing === true, "Rainier owner Worth Reviewing");
  assert(aud.owner.decisionStage === "Active", "Rainier owner stage Active");
  assert(aud.brand.worthReviewing === false, "Rainier brand not auto WR");
  assert(aud.operator.worthReviewing === false, "Rainier operator not auto WR");
  assert(aud.treatment === "REVIEW", "Rainier treatment REVIEW");
}

// Acquisition
{
  const title = "Investor acquires downtown boutique hotel";
  const summary = "The buyer purchased the 120-room property from a private owner.";
  const event = inferMarketAlertEvent({ title, summary });
  assert(event.eventType === "Acquisition", "Acquisition event");
  const aud = resolveAudienceIntelligence({
    eventType: event.eventType,
    title,
    summary,
    entities: extractMarketAlertEntities({ title, summary, eventType: event.eventType }),
  });
  assert(aud.owner.worthReviewing === true, "Acquisition owner WR");
  assert(aud.treatment === "REVIEW", "Acquisition REVIEW");
}

// RevPAR STANDARD
{
  const title = "Global hotel RevPAR rises in Q2";
  const summary = "Industry RevPAR and occupancy improved across regions.";
  const event = inferMarketAlertEvent({ title, summary });
  const aud = resolveAudienceIntelligence({
    eventType: event.eventType,
    title,
    summary,
    entities: {},
  });
  assert(aud.treatment === "STANDARD", "RevPAR STANDARD");
  assert(!aud.owner.worthReviewing && !aud.brand.worthReviewing, "RevPAR not WR");
}

// Loyalty STANDARD
{
  const title = "Hotel loyalty program adds new rewards points tiers";
  const summary = "Members can earn more points on stays.";
  const event = inferMarketAlertEvent({ title, summary });
  const aud = resolveAudienceIntelligence({
    eventType: event.eventType,
    title,
    summary,
    entities: {},
  });
  assert(aud.treatment === "STANDARD", "Loyalty STANDARD");
}

// Unflagged development → brand/operator REVIEW
{
  const title = "Local group announces new hotel development near airport";
  const summary = "A planned 180-room hotel development was announced with no brand named.";
  const event = inferMarketAlertEvent({ title, summary });
  assert(event.eventType === "New Development", "Unflagged development event");
  const entities = extractMarketAlertEntities({
    title,
    summary,
    eventType: event.eventType,
  });
  assert(!entities.brandInvolved, "Unflagged has no brand token");
  const aud = resolveAudienceIntelligence({
    eventType: event.eventType,
    title,
    summary,
    entities,
  });
  assert(aud.brand.worthReviewing === true, "Unflagged brand WR");
  assert(aud.operator.worthReviewing === true, "Unflagged operator WR");
  assert(aud.owner.worthReviewing === true, "Unflagged owner WR");
}

// Branded opening STANDARD for brand
{
  const title = "Marriott hotel opens in Miami Beach";
  const summary = "The new Marriott celebrated its opening this weekend.";
  const event = inferMarketAlertEvent({ title, summary });
  const entities = extractMarketAlertEntities({
    title,
    summary,
    eventType: event.eventType,
  });
  const aud = resolveAudienceIntelligence({
    eventType: event.eventType,
    title,
    summary,
    entities,
  });
  assert(aud.brand.worthReviewing === false, "Branded opening brand not WR");
  assert(aud.treatment === "STANDARD" || !aud.brand.worthReviewing, "Branded opening soft");
}

// Templates use may/could — no invented seeking language
{
  const tpl = buildAudienceTemplates("brand", {
    eventType: "New Development",
    signalType: "Potential Development Opportunity",
    decisionStage: "Early",
    entities: { hotelProject: "Airport Hotel", rooms: 180 },
  });
  assert(!!tpl.whyItMatters, "Brand template why present");
  assert(!/is seeking a brand/i.test(tpl.whyItMatters || ""), "No invented seeking");
  assert(/\b(could|may|possible|possibility)\b/i.test(tpl.whyItMatters || ""), "may/could language");
}

// Orchestrator patch shape
{
  const computed = computeMarketAlertIntelligence({
    title: "Rainier Square Hotel in Seattle for Sale",
    summary: "A 220-room hotel is for sale.",
  });
  assert(computed.fields[MAP_INTEL.eventType] === "Hotel For Sale", "patch eventType");
  assert(computed.fields[MAP_INTEL.worthReviewingOwner] === true, "patch owner WR");
  assert(computed.fields[MAP_INTEL.intelligenceTreatment] === "REVIEW", "patch treatment");
  assert(computed.fields[MAP_INTEL.intelligenceStatus] === "Ready", "patch status Ready");
}

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("\nAll market-alerts intelligence tests passed.");
