#!/usr/bin/env node
/**
 * Live API validation for CALA Radar Buildout.
 *   node scripts/validate-radar-buildout-live.mjs
 *   RADAR_API_BASE=http://localhost:8080 node scripts/validate-radar-buildout-live.mjs
 */
import "../load-env.js";

const BASE = (process.env.RADAR_API_BASE || "http://localhost:8080").replace(/\/$/, "");

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else console.log("ok:", msg);
}

async function fetchJson(path) {
  const res = await fetch(BASE + path, { headers: { "ngrok-skip-browser-warning": "true" } });
  if (!res.ok) throw new Error(path + " HTTP " + res.status);
  return res.json();
}

async function main() {
  console.log("CALA Radar Buildout live validation");
  console.log("Base URL:", BASE);

  let list;
  try {
    list = await fetchJson("/api/radar-buildout/countries");
  } catch (err) {
    console.error("Cannot reach API:", err.message);
    console.error("Start server (npm start) and retry.");
    process.exit(1);
  }

  assert(list.ok === true, "countries list ok");
  assert(Array.isArray(list.countries), "countries array");
  assert(list.countries.length >= 10, "at least 10 country configs (got " + list.countries.length + ")");

  const pr = list.countries.find((c) => c.country === "Puerto Rico");
  assert(!!pr, "Puerto Rico in list");
  if (pr) {
    assert(pr.current.totalRadarPoints >= 60, "PR total points >= 60 (got " + pr.current.totalRadarPoints + ")");
    assert(
      ["Market Ready", "Deal Ready", "Intelligence Ready"].includes(pr.buildStatus),
      "PR build status ready tier (got " + pr.buildStatus + ")"
    );
  }

  const dr = list.countries.find((c) => c.country === "Dominican Republic");
  assert(!!dr, "Dominican Republic in list");
  if (dr) {
    assert(
      ["Market Ready", "Deal Ready", "Intelligence Ready", "Seeded"].includes(dr.buildStatus),
      "DR build status reflects progress (got " + dr.buildStatus + ")"
    );
    assert(dr.current.demandAnchors > 0, "DR has demand anchors (got " + dr.current.demandAnchors + ")");
    assert(dr.current.totalRadarPoints >= 50, "DR total radar points >= 50 (got " + dr.current.totalRadarPoints + ")");
  }

  const prDetail = await fetchJson("/api/radar-buildout/countries/" + encodeURIComponent("Puerto Rico"));
  assert(prDetail.ok === true, "PR detail ok");
  assert(prDetail.country === "Puerto Rico", "PR detail country");
  assert(prDetail.targets && prDetail.current && prDetail.coverage, "PR detail shape");

  const tier1 = await fetchJson("/api/radar-buildout/countries?priorityTier=Tier%201");
  assert(tier1.ok && tier1.countries.length >= 4, "tier 1 filter");

  if (failed) {
    console.error("\n" + failed + " assertion(s) failed");
    process.exit(1);
  }
  console.log("\nAll radar buildout live validation checks passed.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
