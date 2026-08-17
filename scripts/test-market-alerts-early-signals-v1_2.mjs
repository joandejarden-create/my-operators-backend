#!/usr/bin/env node
/**
 * Early Signal V1.2 production gates — no network.
 */
import { classifyEarlySignalCandidate, assessEarlySignalProductionReady } from "../lib/market-alerts-early-signals.js";
import { listEarlySignalQueries } from "../lib/market-alerts-early-signal-queries.js";
import {
  EARLY_SIGNAL_DISABLED_FAMILIES,
  EARLY_SIGNAL_PRODUCTION_FAMILIES,
  isProductionEarlySignalFamily,
} from "../lib/market-alerts-early-signal-config.js";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import { isUsableEntityName, inferPublisherTokens } from "../lib/market-alerts-qualification-gate.js";
import { dedupeFeedItemsByEntityKey } from "../lib/market-alerts-correlation.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else console.log("OK:", msg);
}

{
  assert(
    EARLY_SIGNAL_PRODUCTION_FAMILIES.length === 4 &&
      EARLY_SIGNAL_PRODUCTION_FAMILIES.includes("planning") &&
      EARLY_SIGNAL_PRODUCTION_FAMILIES.includes("adaptiveReuse"),
    "production allowlist has four proven families"
  );
  for (const f of EARLY_SIGNAL_DISABLED_FAMILIES) {
    assert(!isProductionEarlySignalFamily(f), `disabled family ${f} not in production`);
  }
}

{
  const planning = listEarlySignalQueries("planning", { productionOnly: true });
  assert(planning.length >= 10, "planning production queries present");
  assert(
    !planning.some((q) => /city council approves/i.test(q.query)),
    "noisy city council approves query removed"
  );
  const prodAll = listEarlySignalQueries(null, { productionOnly: true });
  const disabledRaw = listEarlySignalQueries("landSite");
  assert(prodAll.length > 0 && disabledRaw.length > 0, "productionOnly filters families");
  assert(
    !prodAll.some((q) => q.family === "landSite"),
    "landSite excluded from productionOnly"
  );
}

{
  const civic = classifyEarlySignalCandidate({
    title: "City council approves new bus service expansion",
    summary: "The council voted to expand bus routes. No hotel or lodging mentioned.",
    source: "Google News (EARLY_SIGNAL_PLANNING)",
    family: "planning",
  });
  assert(civic.rejection === "off-topic" || civic.treatment === "REJECTED", "civic bus news rejected");
  assert(!assessEarlySignalProductionReady(civic).ok, "civic bus not production ready");

  const hotelPlan = classifyEarlySignalCandidate({
    title: "Planning board approves hotel zoning application for 180-room project",
    summary: "The hotel zoning application was approved. No brand or operator named.",
    source: "Google News (EARLY_SIGNAL_PLANNING)",
    family: "planning",
  });
  assert(hotelPlan.validHospitality, "hotel zoning planning valid hospitality");
  assert(assessEarlySignalProductionReady(hotelPlan).ok, "hotel zoning production ready");
}

{
  const pubs = inferPublisherTokens("MaltaToday Hotel project wins approval", "MaltaToday");
  assert(!isUsableEntityName("MaltaToday Hotel", { publishers: pubs }), "publisher-derived entity rejected");
  const c = classifyEarlySignalCandidate({
    title: "MaltaToday Hotel project wins planning approval",
    summary: "A 120-room hotel planning application was approved.",
    source: "MaltaToday",
    family: "planning",
  });
  assert(!c.hotelProject || !/malta/i.test(c.hotelProject), "publisher hotel not stored as proper entity");
}

{
  const branded = computeMarketAlertIntelligence({
    title: "Hilton signs brand agreement for new 220-room hotel in Austin",
    summary: "Hilton will flag the new hotel under a signed brand agreement.",
  });
  assert(
    branded.meta.audience.brand.signalType === "Competitive Brand Move",
    "branded development is competitive brand intelligence"
  );
  assert(
    branded.meta.audience.brand.signalType !== "Potential Development Opportunity",
    "branded development is not open brand opportunity"
  );

  const mgmt = computeMarketAlertIntelligence({
    title: "Accor signs management agreement for new resort in Cancun",
    summary: "Accor will operate the resort under a management contract.",
  });
  assert(
    mgmt.meta.audience.operator.signalType !== "Potential Management Opportunity",
    "confirmed operator is not open operator opportunity"
  );
}

{
  const challenged = computeMarketAlertIntelligence({
    title: "NGT files suo moto case over proposed hotel construction",
    summary: "The tribunal opened an environmental case over a proposed hotel. No brand named.",
  });
  assert(challenged.meta.projectDirection === "Challenged", "challenged direction preserved");
  assert(
    /regulatory|planning challenges|environmental|legal|facing/i.test(
      challenged.fields["Why It Matters — Brand"] || challenged.fields["Why It Matters — Owner"] || ""
    ),
    "challenged copy is cautious"
  );
}

{
  const now = new Date().toISOString();
  const items = [
    {
      publishedAt: now,
      intelligence: { entities: { entityKey: "phoenix|jw marriott|adaptive reuse" } },
      fields: { "Published At": now },
    },
    {
      publishedAt: now,
      intelligence: { entities: { entityKey: "phoenix|jw marriott|adaptive reuse" } },
      fields: { "Published At": now },
    },
    {
      publishedAt: now,
      intelligence: { entities: { entityKey: "miami|unrelated hotel" } },
      fields: { "Published At": now },
    },
  ];
  const deduped = dedupeFeedItemsByEntityKey(items, { windowDays: 14 });
  assert(deduped.length === 2, "entity-key feed dedupe suppresses duplicate project cards");
}

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("\nAll early-signal V1.2 tests passed.");
