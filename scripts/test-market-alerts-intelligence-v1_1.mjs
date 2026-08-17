#!/usr/bin/env node
/**
 * V1.1 regression tests — negation, asset validation, entity quality, decision state.
 */
import { inferMarketAlertEvent } from "../lib/market-alerts-event-infer.js";
import { extractMarketAlertEntities } from "../lib/market-alerts-entity-extract.js";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import {
  isUsableEntityName,
  validateHospitalityAsset,
  detectTransactionNegation,
} from "../lib/market-alerts-qualification-gate.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else console.log("OK:", msg);
}

// Negation
{
  const sold = inferMarketAlertEvent({ title: "Crown Hotel Camden sold", summary: "" });
  assert(sold.eventType === "Sale", "hotel sold → Sale");
  const notSold = inferMarketAlertEvent({
    title: "Eccles Hotel in Glengarriff not sold",
    summary: "",
  });
  assert(notSold.eventType === null, "hotel not sold → NOT Sale");
  const fell = inferMarketAlertEvent({ title: "Hotel sale fell through in Dublin", summary: "" });
  assert(fell.eventType === null, "sale fell through → NOT Sale");
  const withdrawn = inferMarketAlertEvent({
    title: "Asset withdrawn from sale after bidding",
    summary: "",
  });
  assert(withdrawn.eventType === null, "withdrawn from sale → NOT active For Sale");
}

// Asset validation
{
  const pub = validateHospitalityAsset(
    "Hotel heavyweights buy iconic coastal pub",
    "Acquisition"
  );
  assert(pub.assetScope === "non-hotel", "pub purchased → NOT hotel Acquisition scope");
  const apt = validateHospitalityAsset(
    "Apartment at erstwhile Juhu Centaur Hotel sold for ₹100 crore",
    "Sale"
  );
  assert(apt.assetScope === "partial", "apartment component → partial asset");
  const resort = validateHospitalityAsset(
    "Trinity Investments sells Grande Lakes Orlando Resort for $1.38B",
    "Sale"
  );
  assert(resort.isHotelAsset && resort.assetScope === "whole", "resort sold → hotel Sale");
}

// Entity validation
{
  assert(!isUsableEntityName("This property"), "reject This property");
  assert(!isUsableEntityName("Former Full-Service"), "reject Former Full-Service");
  assert(!isUsableEntityName("The hotel"), "reject The hotel");
  assert(isUsableEntityName("Grande Lakes Orlando Resort"), "accept Grande Lakes Orlando Resort");
  const springfield = extractMarketAlertEntities({
    title: "Former Full-Service Hotel in Downtown Springfield, IL for Sale",
    summary: "",
    eventType: "Hotel For Sale",
  });
  assert(
    !springfield.hotelProject || isUsableEntityName(springfield.hotelProject),
    "Springfield for-sale avoids Former Full-Service entity"
  );
}

// Brand decision state
{
  const ihg = computeMarketAlertIntelligence({
    title: "IHG Signs Noted Collection Hotel in Riyadh, Saudi Arabia",
    summary: "107-room hotel signing announced.",
  });
  assert(ihg.meta.audience.brand.worthReviewing === true, "IHG signing brand WR");
  assert(
    ihg.meta.audience.brand.signalType === "Competitive Brand Move",
    "IHG signing → Competitive Brand Move"
  );
  assert(
    ihg.meta.audience.brand.signalType !== "Potential Development Opportunity",
    "IHG signing NOT open brand opportunity"
  );
  const autograph = computeMarketAlertIntelligence({
    title: "Potomac Hotel joins Marriott's Autograph Collection",
    summary: "Confirmed conversion to Autograph.",
  });
  assert(
    autograph.meta.audience.brand.signalType === "Competitive Brand Move",
    "Autograph join → Competitive Brand Move"
  );
}

// Operator decision state
{
  const voco = computeMarketAlertIntelligence({
    title: "IHG Hotels & Resorts to Bring voco to the City of Lakes With New Signing in Udaipur",
    summary: "Management agreement signed.",
  });
  assert(voco.meta.audience.operator.worthReviewing === true, "voco operator WR as intelligence");
  assert(
    voco.meta.audience.operator.signalType !== "Potential Management Opportunity",
    "voco NOT open management opportunity"
  );
  assert(
    ["Management Agreement Announced", "Competitive Operator Move"].includes(
      voco.meta.audience.operator.signalType
    ),
    "voco → closed operator signal"
  );
}

// Manual regression IDs (compute-only)
const REGRESSION = [
  {
    id: "recjAvBUNJjtQb2rK",
    title: "Rainier Camp & Retreat Center Offered for $3.4 Million",
    summary: "Retreat center offered for sale.",
    expect: (m) =>
      m.meta.event.eventType === "Hotel For Sale" &&
      m.meta.audience.owner.worthReviewing &&
      !m.meta.audience.brand.worthReviewing,
  },
  {
    id: "recGNkn1IOCB0CPQm",
    title: "Former Full-Service Hotel in Downtown Springfield, IL for Sale",
    summary: "Bankruptcy sale of former Wyndham City Centre.",
    expect: (m) =>
      m.meta.event.eventType === "Hotel For Sale" &&
      (!m.meta.entities.hotelProject || !/former full-service/i.test(m.meta.entities.hotelProject)),
  },
  {
    id: "recigDVjrSTjQyVLD",
    title: "Eccles Hotel in Glengarriff not sold - The Southern Star",
    summary: "Hotel remains unsold.",
    expect: (m) => m.meta.treatment === "STANDARD" && !m.meta.event.eventType,
  },
  {
    id: "recSpjM7LprBf3DIw",
    title: "Apartment at erstwhile Juhu Centaur Hotel sold for over ₹100 crore",
    summary: "Residential unit sale.",
    expect: (m) => m.meta.treatment === "STANDARD" && !m.meta.audience.brand.worthReviewing,
  },
  {
    id: "recVk4LfdetgiojI9",
    title: "Hotel heavyweights buy iconic coastal pub for huge price",
    summary: "Pub acquisition by hotel investors.",
    expect: (m) => m.meta.treatment === "STANDARD",
  },
  {
    id: "recs0QzkTYryoMjyk",
    title: "Crown Hotel Camden sold - JLL",
    summary: "Hotel sale completed.",
    expect: (m) =>
      m.meta.event.eventType === "Sale" &&
      m.meta.audience.owner.worthReviewing &&
      !m.meta.audience.brand.worthReviewing &&
      !m.meta.audience.operator.worthReviewing,
  },
  {
    id: "recyJ4mPDFSHwDclj",
    title: "Trinity Investments sells Grande Lakes Orlando Resort for $1.38B",
    summary: "Major resort transaction.",
    expect: (m) =>
      m.meta.event.eventType === "Sale" &&
      m.meta.audience.owner.worthReviewing &&
      !m.meta.audience.brand.worthReviewing,
  },
];

for (const c of REGRESSION) {
  const m = computeMarketAlertIntelligence({
    title: c.title,
    summary: c.summary,
    alertId: c.id,
  });
  assert(c.expect(m), `regression ${c.id}`);
}

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("\nAll V1.1 intelligence tests passed.");
