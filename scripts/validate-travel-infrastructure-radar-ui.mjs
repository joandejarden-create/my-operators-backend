#!/usr/bin/env node
/**
 * Smoke tests for Travel Infrastructure radar map API filters (UI layer).
 *   node scripts/validate-travel-infrastructure-radar-ui.mjs
 *   RADAR_API_BASE=http://localhost:8080 node scripts/validate-travel-infrastructure-radar-ui.mjs
 */
import "../load-env.js";

const BASE = (process.env.RADAR_API_BASE || "http://localhost:8080").replace(/\/$/, "");

/** Baseline counts before Puerto Rico additional-type import (2026-06). */
const MIN_TOTAL = 714;
const MIN_AIRPORT = 602;
const MIN_CRUISE = 90;
const MIN_CONVENTION = 22;
const MIN_PORT_MARITIME = 5;
const MIN_FERRY = 4;
const MIN_HIGHWAY = 4;
const MIN_BUS = 2;

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

async function fetchJson(path) {
  const url = BASE + path;
  const res = await fetch(url, {
    headers: { "ngrok-skip-browser-warning": "true" },
  });
  if (!res.ok) {
    throw new Error(path + " HTTP " + res.status);
  }
  return res.json();
}

function pointCount(data) {
  if (Array.isArray(data.points)) return data.points.length;
  if (data.totalCount != null) return data.totalCount;
  const stats = data.statistics || {};
  if (stats.totalInfrastructure != null) return stats.totalInfrastructure;
  const tc = stats.typeCounts || {};
  return Object.values(tc).reduce((s, n) => s + (Number(n) || 0), 0);
}

function typeCount(data, type) {
  const stats = data.statistics || {};
  if (stats.typeCounts && stats.typeCounts[type] != null) {
    return stats.typeCounts[type];
  }
  const layer = data.layers && data.layers["Travel Infrastructure"];
  if (layer && layer.byPointType && layer.byPointType[type] != null) {
    return layer.byPointType[type];
  }
  if (Array.isArray(data.points)) {
    return data.points.filter((p) => (p.pointType || p.type) === type).length;
  }
  return null;
}

async function main() {
  console.log("Travel Infrastructure radar UI API validation");
  console.log("Base URL:", BASE);

  let all;
  try {
    all = await fetchJson("/api/radar-map-points/travel-infrastructure");
  } catch (err) {
    console.error("Cannot reach API:", err.message);
    console.error("Start the server (e.g. npm start) and retry.");
    process.exit(1);
  }

  const total = pointCount(all);
  assert(total >= MIN_TOTAL, `all travel infrastructure count >= ${MIN_TOTAL} (got ${total})`);

  const airport = await fetchJson(
    "/api/radar-map-points/travel-infrastructure?pointTypeFilter=" + encodeURIComponent("Airport")
  );
  const airportCount = pointCount(airport);
  assert(airportCount >= MIN_AIRPORT, `Airport filter count >= ${MIN_AIRPORT} (got ${airportCount})`);

  const cruise = await fetchJson(
    "/api/radar-map-points/travel-infrastructure?pointTypeFilter=" + encodeURIComponent("Cruise Port")
  );
  const cruiseCount = pointCount(cruise);
  assert(cruiseCount >= MIN_CRUISE, `Cruise Port filter count >= ${MIN_CRUISE} (got ${cruiseCount})`);

  const convention = await fetchJson(
    "/api/radar-map-points/travel-infrastructure?pointTypeFilter=" +
      encodeURIComponent("Convention Center")
  );
  const conventionCount = pointCount(convention);
  assert(
    conventionCount >= MIN_CONVENTION,
    `Convention Center filter count >= ${MIN_CONVENTION} (got ${conventionCount})`
  );

  const portCount = typeCount(all, "Port / Maritime");
  if (portCount != null) {
    assert(portCount >= MIN_PORT_MARITIME, `Port / Maritime >= ${MIN_PORT_MARITIME} (got ${portCount})`);
  }

  const ferryCount = typeCount(all, "Ferry Terminal");
  if (ferryCount != null) {
    assert(ferryCount >= MIN_FERRY, `Ferry Terminal >= ${MIN_FERRY} (got ${ferryCount})`);
  }

  const highwayCount = typeCount(all, "Highway Access");
  if (highwayCount != null) {
    assert(highwayCount >= MIN_HIGHWAY, `Highway Access >= ${MIN_HIGHWAY} (got ${highwayCount})`);
  }

  const busCount = typeCount(all, "Bus Terminal");
  if (busCount != null) {
    assert(busCount >= MIN_BUS, `Bus Terminal >= ${MIN_BUS} (got ${busCount})`);
  }

  const summaryAirport = typeCount(all, "Airport");
  if (summaryAirport != null) {
    assert(summaryAirport >= MIN_AIRPORT, `layer summary Airport >= ${MIN_AIRPORT} (got ${summaryAirport})`);
  }

  const sample = (all.points || [])[0];
  if (sample) {
    assert(!!sample.pointType || !!sample.type, "sample point has pointType");
    assert(sample.demandRelevance != null || sample.name, "sample point has enriched fields");
    assert("submarket" in sample, "sample point includes submarket field");
  }

  if (failed) {
    console.error("\n" + failed + " assertion(s) failed");
    process.exit(1);
  }
  console.log("\nAll travel infrastructure radar UI API checks passed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
