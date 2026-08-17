#!/usr/bin/env node
/**
 * Early Signal V1.2.1 micro-hardening tests — no network.
 */
import fs from "fs";
import os from "os";
import path from "path";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import {
  classifyEarlySignalCandidate,
  assessEarlySignalProductionReady,
} from "../lib/market-alerts-early-signals.js";
import {
  isHotelToNonHotelChangeOfUse,
  isOfficeToHotelAdaptiveReuse,
} from "../lib/market-alerts-change-of-use.js";
import { inferCorrelationEntityKey } from "../lib/market-alerts-entity-extract.js";
import { dedupeFeedItemsByEntityKey } from "../lib/market-alerts-correlation.js";
import {
  evaluateEarlySignalSchedule,
  getEarlySignalIntervalMinutes,
  readEarlySignalScheduleState,
  writeEarlySignalScheduleState,
} from "../lib/market-alerts-early-signal-schedule.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else console.log("OK:", msg);
}

{
  const text = "Distressed hotel to be converted into affordable housing";
  assert(isHotelToNonHotelChangeOfUse(text), "hotel → housing detected");
  assert(!isOfficeToHotelAdaptiveReuse(text), "not office → hotel");

  const intel = computeMarketAlertIntelligence({
    title: text,
    summary: "Town officials are reviewing the conversion plan.",
  });
  assert(
    intel.meta.audience.owner.signalType !== "New Competitive Supply",
    "housing conversion not New Competitive Supply"
  );
  assert(!intel.meta.audience.brand.worthReviewing, "Brand WR false for hotel → housing");
  assert(!intel.meta.audience.operator.worthReviewing, "Operator WR false for hotel → housing");
  assert(
    intel.meta.audience.owner.signalType === "Strategic Market Change",
    "owner Strategic Market Change for supply removal"
  );

  const mountLaurel = computeMarketAlertIntelligence({
    title:
      "Mount Laurel wants to buy two distressed hotel properties to develop into affordable housing.",
    summary: "The municipality is pursuing a housing redevelopment.",
  });
  assert(!mountLaurel.meta.audience.brand.worthReviewing, "Mount Laurel Brand WR false");
  assert(!mountLaurel.meta.audience.operator.worthReviewing, "Mount Laurel Operator WR false");

  const c = classifyEarlySignalCandidate({
    title: text,
    summary: "",
    family: "planning",
  });
  assert(c.rejection === "non-hotel", "early signal rejects hotel → housing");
  assert(!assessEarlySignalProductionReady(c).ok, "not production ready");
}

{
  const intel = computeMarketAlertIntelligence({
    title: "Former hotel approved for residential conversion",
    summary: "Planning approval covers apartments, not a new hotel.",
  });
  assert(
    intel.meta.audience.brand.signalType !== "Potential Development Opportunity",
    "former hotel residential — no brand opportunity"
  );
  assert(
    intel.meta.audience.operator.signalType !== "Potential Management Opportunity",
    "former hotel residential — no operator opportunity"
  );
}

{
  const intel = computeMarketAlertIntelligence({
    title: "Vacant office tower to become 300-room hotel",
    summary: "Developer proposes adaptive reuse into a hotel. No brand announced.",
  });
  assert(intel.meta.event.eventType === "Adaptive Reuse Proposal", "office → hotel event");
  assert(intel.meta.audience.brand.worthReviewing, "unflagged office → hotel brand WR");
}

{
  const titles = [
    "Phoenix Office Tower to Become 340-Room JW Marriott - Hoodline",
    "Vacant Phoenix office tower headed for second act as JW Marriott - The Real Deal",
    "Vacant Arizona Center Office Tower Converting to JW Marriott Hotel - Connect CRE",
    "Las Vegas firm to transform Phoenix office tower into 340-room JW Marriott - The Business Journals",
    "Developer plans to convert Arizona Center office building to JW Marriott hotel - yourvalley.net",
  ];

  const keys = new Set(
    titles.map((title) =>
      inferCorrelationEntityKey({
        text: title,
        eventType: "Adaptive Reuse Proposal",
        brandInvolved: "JW Marriott",
        rooms: 340,
      })
    )
  );
  assert(keys.size === 1, "Phoenix JW Marriott cluster shares one correlation key");

  const now = new Date().toISOString();
  const items = titles.map((title, i) => {
    const intel = computeMarketAlertIntelligence({ title, summary: "" });
    return {
      id: `rec${i}`,
      fields: { Title: title, "Published At": now },
      intelligence: intel.meta
        ? {
            eventType: intel.meta.event?.eventType,
            entities: intel.meta.entities,
          }
        : { eventType: "Adaptive Reuse Proposal", entities: { brandInvolved: "JW Marriott", rooms: 340 } },
    };
  });

  const deduped = dedupeFeedItemsByEntityKey(items, { windowDays: 14 });
  assert(deduped.length === 1, "WR feed dedupe returns one Phoenix JW card");
}

{
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "es-schedule-"));
  const statePath = path.join(tmpDir, "state.json");
  const prevEnabled = process.env.MARKET_ALERTS_EARLY_SIGNALS_ENABLED;
  const prevStatePath = process.env.MARKET_ALERTS_EARLY_SIGNALS_STATE_PATH;
  const prevInterval = process.env.MARKET_ALERTS_EARLY_SIGNALS_INTERVAL_MINUTES;

  process.env.MARKET_ALERTS_EARLY_SIGNALS_STATE_PATH = statePath;
  process.env.MARKET_ALERTS_EARLY_SIGNALS_INTERVAL_MINUTES = "10080";
  process.env.MARKET_ALERTS_EARLY_SIGNALS_ENABLED = "true";

  try {
    assert(getEarlySignalIntervalMinutes() === 10080, "weekly interval default 10080 minutes");

    writeEarlySignalScheduleState({ lastSuccessfulRunAt: null });
    assert(evaluateEarlySignalSchedule().run === true, "never run → eligible");

    const day0 = Date.now();
    writeEarlySignalScheduleState({ lastSuccessfulRunAt: new Date(day0).toISOString() });
    assert(!evaluateEarlySignalSchedule(day0 + 4 * 60 * 60 * 1000).run, "Day 0 + 4h → skip");
    assert(!evaluateEarlySignalSchedule(day0 + 24 * 60 * 60 * 1000).run, "Day 1 → skip");
    assert(!evaluateEarlySignalSchedule(day0 + 6 * 24 * 60 * 60 * 1000).run, "Day 6 → skip");
    assert(
      evaluateEarlySignalSchedule(day0 + 7 * 24 * 60 * 60 * 1000 + 1000).run,
      "Day 7+ → eligible"
    );

    process.env.MARKET_ALERTS_EARLY_SIGNALS_ENABLED = "false";
    assert(!evaluateEarlySignalSchedule().run, "disabled → skip");

    const persisted = readEarlySignalScheduleState();
    assert(!!persisted.lastSuccessfulRunAt, "schedule state persists across reads");
  } finally {
    if (prevEnabled === undefined) delete process.env.MARKET_ALERTS_EARLY_SIGNALS_ENABLED;
    else process.env.MARKET_ALERTS_EARLY_SIGNALS_ENABLED = prevEnabled;
    if (prevStatePath === undefined) delete process.env.MARKET_ALERTS_EARLY_SIGNALS_STATE_PATH;
    else process.env.MARKET_ALERTS_EARLY_SIGNALS_STATE_PATH = prevStatePath;
    if (prevInterval === undefined) delete process.env.MARKET_ALERTS_EARLY_SIGNALS_INTERVAL_MINUTES;
    else process.env.MARKET_ALERTS_EARLY_SIGNALS_INTERVAL_MINUTES = prevInterval;
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("\nAll early-signal V1.2.1 tests passed.");
