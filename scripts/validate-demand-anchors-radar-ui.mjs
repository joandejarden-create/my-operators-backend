#!/usr/bin/env node
/**
 * Smoke tests for Demand Anchors radar map API.
 *   node scripts/validate-demand-anchors-radar-ui.mjs
 */
import "../load-env.js";

const BASE = (process.env.RADAR_API_BASE || "http://localhost:8080").replace(/\/$/, "");

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
  const res = await fetch(BASE + path, {
    headers: { "ngrok-skip-browser-warning": "true" },
  });
  return { status: res.status, data: await res.json().catch(() => ({})) };
}

function pointCount(data) {
  if (Array.isArray(data.points)) return data.points.length;
  if (data.totalCount != null) return data.totalCount;
  const stats = data.statistics || {};
  return stats.totalDemandAnchors != null ? stats.totalDemandAnchors : 0;
}

async function main() {
  console.log("Demand Anchors radar UI API validation");
  console.log("Base URL:", BASE);

  const all = await fetchJson("/api/radar-map-points/demand-anchors");
  if (all.status === 404) {
    console.error("API route not found — restart server with latest server.js");
    process.exit(1);
  }

  assert(all.data.success === true, "success true");
  if (all.data.setupNeeded) {
    console.log("setupNeeded: true — run ensure:demand-anchors-schema:apply and seed");
    assert(true, "missing table returns setupNeeded gracefully");
    process.exit(failed ? 1 : 0);
  }

  const total = pointCount(all.data);
  assert(total >= 12, "at least 12 demand anchors after seed (got " + total + ")");

  const medical = await fetchJson(
    "/api/radar-map-points/demand-anchors?pointTypeFilter=" + encodeURIComponent("Medical Campus")
  );
  assert(pointCount(medical.data) >= 1, "Medical Campus filter returns records");

  const hidden = await fetchJson("/api/radar-map-points/demand-anchors?includeHidden=1");
  assert(hidden.data.success === true, "includeHidden=1 succeeds");

  const sample = (all.data.points || [])[0];
  if (sample) {
    assert(!!sample.pointType, "sample has pointType");
    assert(sample.demandSegment != null || sample.demandRelevance, "sample has demand fields");
  }

  if (failed) {
    console.error("\n" + failed + " assertion(s) failed");
    process.exit(1);
  }
  console.log("\nAll demand anchors radar UI API checks passed.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
