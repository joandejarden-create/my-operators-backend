#!/usr/bin/env node
/**
 * V1.1.1 regression tests — partnership context, geography entities, branded construction.
 */
import { inferMarketAlertEvent } from "../lib/market-alerts-event-infer.js";
import { extractMarketAlertEntities } from "../lib/market-alerts-entity-extract.js";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import {
  isUsableEntityName,
  isNonHotelAssetTransactionContext,
  validateHotelAssetTransaction,
} from "../lib/market-alerts-qualification-gate.js";
import { isGeographyOnlyLabel } from "../lib/market-alerts-geo-keywords.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else console.log("OK:", msg);
}

// A. Partnership / transaction context
{
  const uberTitle =
    "Uber Lands Hotel Deals with Accor and Expedia, Asia Pacific Pipeline Tops 980,000 Rooms";
  const uberSummary =
    "Uber separately confirmed it will sell Expedia-powered hotel bookings inside its app.";
  const uber = inferMarketAlertEvent({ title: uberTitle, summary: uberSummary });
  assert(uber.eventType !== "Sale" && uber.eventType !== "Acquisition", "Uber partnership → NOT Sale/Acquisition");
  assert(
    isNonHotelAssetTransactionContext(`${uberTitle} ${uberSummary}`, "Sale"),
    "Uber context flagged as non-asset transaction"
  );

  const partner = inferMarketAlertEvent({
    title: "Marriott partners with booking platform on distribution deal",
    summary: "",
  });
  assert(partner.eventType !== "Sale" && partner.eventType !== "Acquisition", "distribution partnership → NOT Sale");

  const vendor = inferMarketAlertEvent({
    title: "Hospitality software company acquired by technology vendor",
    summary: "Guest experience platform merger.",
  });
  assert(vendor.eventType !== "Acquisition", "vendor/software M&A → NOT hotel Acquisition");

  const loyalty = inferMarketAlertEvent({
    title: "Hotel group announces loyalty partnership with travel platform",
    summary: "",
  });
  assert(loyalty.eventType !== "Sale" && loyalty.eventType !== "Acquisition", "loyalty partnership → NOT transaction");

  const hotelSold = inferMarketAlertEvent({
    title: "ABC Capital acquires 220-room Hotel XYZ in Miami",
    summary: "The hotel property changed ownership.",
  });
  assert(hotelSold.eventType === "Acquisition", "named hotel acquisition remains Acquisition");

  const resortSold = inferMarketAlertEvent({
    title: "Resort portfolio acquired by investment company",
    summary: "Three resort properties sold in one transaction.",
  });
  assert(
    resortSold.eventType === "Acquisition" || resortSold.eventType === "Portfolio Acquisition",
    "resort portfolio acquisition remains transaction"
  );

  const tx = validateHotelAssetTransaction("Hotel XYZ sold to ABC Capital", "Sale");
  assert(tx.valid, "hotel asset sale validation passes");
}

// B. Entity geography validation
{
  assert(!isUsableEntityName("Riyadh"), "reject Riyadh as entity");
  assert(!isUsableEntityName("Orlando"), "reject Orlando as entity");
  assert(!isUsableEntityName("Cancún"), "reject Cancún as entity");
  assert(!isUsableEntityName("Spain"), "reject Spain as entity");
  assert(!isUsableEntityName("Florida"), "reject Florida as entity");
  assert(isGeographyOnlyLabel("Riyadh"), "Riyadh is geography-only");
  assert(isUsableEntityName("Four Seasons Hotel Riyadh"), "accept Four Seasons Hotel Riyadh");
  assert(isUsableEntityName("Moxy Tampa Downtown"), "accept Moxy Tampa Downtown");
  assert(isUsableEntityName("Grand Hyatt Cancún"), "accept Grand Hyatt Cancún");

  const ihg = extractMarketAlertEntities({
    title: "IHG Signs Noted Collection Hotel in Riyadh, Saudi Arabia",
    summary: "107-room hotel signing announced.",
    eventType: "Brand Signing",
  });
  assert(!ihg.hotelProject || !isGeographyOnlyLabel(ihg.hotelProject), "IHG Riyadh avoids city-only entity");
}

// C. Branded construction
{
  const mckibbon = computeMarketAlertIntelligence({
    title: "Mckibbon Breaks Ground on AC Hotel & Moxy Tampa Downtown",
    summary:
      "McKibbon Hospitality celebrated the groundbreaking of the dual-brand AC Hotel & Moxy Tampa Downtown, slated to open in early 2028.",
  });
  assert(mckibbon.meta.audience.owner.worthReviewing, "McKibbon owner WR");
  assert(
    mckibbon.meta.audience.owner.signalType === "New Competitive Supply",
    "McKibbon owner = New Competitive Supply"
  );
  assert(
    mckibbon.meta.audience.brand.signalType !== "Potential Development Opportunity",
    "McKibbon brand NOT open development opportunity"
  );
  assert(
    mckibbon.meta.audience.brand.signalType === "Competitive Brand Move",
    "McKibbon brand = Competitive Brand Move"
  );
  assert(
    mckibbon.meta.audience.brand.decisionStage === "Likely Decided",
    "McKibbon brand stage Likely Decided"
  );
  assert(
    mckibbon.meta.audience.operator.signalType !== "New Development Opportunity",
    "McKibbon operator NOT open development opportunity"
  );

  const skyUnbranded = computeMarketAlertIntelligence({
    title: "Skybridge Arizona Breaks Ground on New Hotel Development",
    summary: "Unbranded hotel development groundbreaking.",
  });
  assert(
    skyUnbranded.meta.audience.brand.signalType === "Potential Development Opportunity",
    "unbranded Skybridge may remain open brand opportunity"
  );

  const skyBranded = computeMarketAlertIntelligence({
    title: "Developer Breaks Ground on AC Hotel Phoenix Downtown",
    summary: "Dual-brand AC Hotel development underway.",
  });
  assert(
    skyBranded.meta.audience.brand.signalType === "Competitive Brand Move",
    "branded Skybridge-style dev = Competitive Brand Move"
  );
}

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("\nAll V1.1.1 intelligence tests passed.");
