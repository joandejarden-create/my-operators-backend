/**
 * Scout Phase 5A — demand overlay tests (read-only).
 *
 * Usage: node scripts/test-scout-demand-overlays.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { inspectOverlaySourceTables, buildDemandOverlaysReport } from "../lib/scout/demand-overlays.js";
import { buildMarketMapReport } from "../lib/scout/market-map.js";
import { isValidCoordinate } from "../lib/scout/market-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

function assertOverlayMarker(marker) {
  const required = [
    "overlayId",
    "overlayType",
    "category",
    "name",
    "source",
  ];
  for (const key of required) assert(key in marker, `overlay missing ${key}`);
  assert(marker.source.readOnly === true, "overlay.source.readOnly must be true");
  if (marker.latitude != null) {
    assert(isValidCoordinate(marker.latitude, marker.longitude), "invalid overlay coordinates");
  }
}

function checkOpportunityRadarUnchanged() {
  const appJs = fs.readFileSync(path.join(ROOT, "public", "app.js"), "utf8");
  assert(
    appJs.includes("'/opportunity-radar': { file: '/deal-capture-radar-with-ranked-list.html', title: 'The Radar' }"),
    "opportunity-radar route must remain unchanged"
  );
  assert(appJs.includes("'/scout-market-map'"), "scout-market-map route should exist");
}

function checkScoutPageExists() {
  assert(
    fs.existsSync(path.join(ROOT, "public", "app", "scout-market-map.html")),
    "scout-market-map.html must exist"
  );
  const js = fs.readFileSync(path.join(ROOT, "public", "js", "scout-market-map.js"), "utf8");
  assert(js.includes("includeDemandOverlays"), "scout page should request demand overlays");
  assert(js.includes("scoutMapShowTravelInfra"), "travel infra toggle expected");
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
    process.exit(1);
  }

  console.log("=== Scout demand overlay tests (Phase 5A) ===\n");
  let passed = 0;

  console.log("1) Regression checks...");
  checkOpportunityRadarUnchanged();
  checkScoutPageExists();
  console.log("   PASS\n");
  passed++;

  console.log("2) Inventory overlay sources...");
  const inventory = await inspectOverlaySourceTables();
  assert(inventory.ok, inventory.error || "inventory failed");
  console.log("   found:", inventory.tablesFound.join(", ") || "(none)");
  console.log("   missing:", inventory.tablesMissing.join(", ") || "(none)");
  assert(inventory.tablesFound.length >= 1, "expected at least Travel Infrastructure table");
  console.log("   PASS\n");
  passed++;

  console.log("3) GET demand-overlays (all)...");
  const all = await buildDemandOverlaysReport({ limit: 500 });
  assert(all.ok, all.error || "overlay report failed");
  assert(all.source.readOnly === true, "readOnly must be true");
  assert(all.source.writes === false, "writes must be false");
  for (const m of all.overlayMarkers) assertOverlayMarker(m);
  console.log(
    "   markers:",
    all.summary.overlayMarkers,
    "| without coords:",
    all.summary.withoutCoordinates
  );
  console.log("   PASS\n");
  passed++;

  console.log("4) GET demand-overlays?country=Mexico...");
  const mx = await buildDemandOverlaysReport({ country: "Mexico", limit: 500 });
  assert(mx.ok, mx.error || "Mexico overlay failed");
  if (mx.overlayMarkers.length) {
    assert(
      mx.overlayMarkers.every((m) => /mexico/i.test(m.country)),
      "Mexico filter should scope country"
    );
  }
  console.log("   markers:", mx.summary.overlayMarkers);
  console.log("   PASS\n");
  passed++;

  console.log("5) GET market-map?country=Mexico&includeDemandOverlays=1...");
  const map = await buildMarketMapReport({
    country: "Mexico",
    includeDemandOverlays: "1",
    includeSignals: "0",
    includeSavedSignals: "0",
    limit: 50,
  });
  assert(map.ok, map.error || "market map failed");
  assert(Array.isArray(map.demandOverlayMarkers), "demandOverlayMarkers expected");
  assert(map.demandOverlaySummary != null, "demandOverlaySummary expected");
  console.log(
    "   hotels:",
    map.summary.hotelMarkers,
    "| demand overlays:",
    map.demandOverlayMarkers.length
  );
  console.log("   PASS\n");
  passed++;

  console.log("6) No Hotel Census / overlay writes...");
  const overlaySrc = fs.readFileSync(path.join(ROOT, "lib/scout/demand-overlays.js"), "utf8");
  const mapSrc = fs.readFileSync(path.join(ROOT, "lib/scout/market-map.js"), "utf8");
  assert(!overlaySrc.includes(".create("), "demand-overlays must not create records");
  assert(!overlaySrc.includes(".update("), "demand-overlays must not update records");
  assert(!overlaySrc.includes(".destroy("), "demand-overlays must not delete records");
  assert(!mapSrc.includes(".create("), "market-map must not create census records");
  assert(!mapSrc.includes(".update("), "market-map must not update census records");
  assert(!mapSrc.includes(".destroy("), "market-map must not delete census records");
  console.log("   PASS\n");
  passed++;

  if (all.overlayMarkers[0]) {
    console.log("Example overlay marker:");
    console.log(JSON.stringify(all.overlayMarkers[0], null, 2));
  }

  console.log(`\n=== ${passed} checks passed ===`);
}

main().catch((err) => {
  console.error("\nFAIL:", err.message);
  process.exit(1);
});
