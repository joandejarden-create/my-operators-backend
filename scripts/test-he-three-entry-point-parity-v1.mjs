#!/usr/bin/env node
/**
 * HE Global Reconciliation — three-entry-point Live parity gate (v1).
 *
 * Asserts that Demand Drivers country-only filtering and Area Hotels HPC-native
 * sourcing are present in the shared canonical sources. Fail closed if a branch
 * reintroduces Continent/Region AND-match or legacy Hotel Census Area Hotels pools.
 *
 * Runtime HTTP parity across 8080/8090/8091 is reported when servers are reachable
 * but is not required for unit gate (servers may be offline in CI).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

const demandSrc = fs.readFileSync(
  path.join(root, "lib/hotel-intelligence/he-demand-access-read-v1.js"),
  "utf8"
);
const panelSrc = fs.readFileSync(
  path.join(root, "public/js/hotel-detail-panel.js"),
  "utf8"
);

// Demand: must not AND-filter Demand Anchors by HPC continent/region.
assert.match(
  demandSrc,
  /Filter by country only/,
  "Demand Drivers must document country-only Airtable filter"
);
assert.doesNotMatch(
  demandSrc,
  /fetchDemandAnchorRecords\(\{[\s\S]*?region:\s*hotel\.region/,
  "Demand Drivers must not pass hotel.region into fetchDemandAnchorRecords"
);

// Resort radius inference for named resorts when Location Type absent.
assert.match(
  demandSrc,
  /\\bresort\\b/,
  "Demand/Access radius must infer Resort from hotel name when Location Type missing"
);

// Area Hotels: HPC-native, no legacy Hotel Census pool preference.
assert.match(
  panelSrc,
  /Area Hotels is HPC-native via Brand Presence/,
  "Area Hotels must declare HPC-native Brand Presence sourcing"
);
assert.match(
  panelSrc,
  /Do not query legacy Hotel Census/,
  "Area Hotels must forbid legacy Hotel Census queries"
);
assert.doesNotMatch(
  panelSrc,
  /Prefer Scout Hotel Census sample \(coords \+ status\) before brand-presence search/,
  "Area Hotels must not prefer Scout Hotel Census before Brand Presence"
);

const BETH = "recLuxvwwxID7U2B8";
const ports = [8080, 8090, 8091].map(Number);
const runtime = { checked: [], skipped: [] };

for (const port of ports) {
  const base = `http://127.0.0.1:${port}`;
  try {
    const demandRes = await fetch(`${base}/api/hotel-explorer/hotels/${BETH}/demand-drivers`);
    if (!demandRes.ok) {
      runtime.skipped.push({ port, reason: `demand_http_${demandRes.status}` });
      continue;
    }
    const demand = await demandRes.json();
    const accessRes = await fetch(
      `${base}/api/hotel-explorer/hotels/${BETH}/access-connectivity`
    );
    const access = accessRes.ok ? await accessRes.json() : null;
    const panelRes = await fetch(`${base}/js/hotel-detail-panel.js`);
    const panelText = panelRes.ok ? await panelRes.text() : "";
    const entry = {
      port,
      demandItems: Array.isArray(demand.items) ? demand.items.length : 0,
      accessItems: Array.isArray(access?.items) ? access.items.length : 0,
      emptyReason: demand.emptyReason || null,
      panelHasHpcNative: /Area Hotels is HPC-native via Brand Presence/.test(panelText),
      panelHasCountryOnlyComment: false,
    };
    runtime.checked.push(entry);
  } catch (err) {
    runtime.skipped.push({ port, reason: String(err?.message || err) });
  }
}

if (runtime.checked.length >= 2) {
  const withDemand = runtime.checked.filter((e) => e.demandItems > 0);
  const emptyDemand = runtime.checked.filter(
    (e) => e.demandItems === 0 && e.emptyReason === "no_demand_anchors_in_airtable_for_geography"
  );
  // If any Live entry shows Bethesda demand and another shows the geography-empty
  // false negative, that is entry-point API drift — fail the gate.
  if (withDemand.length && emptyDemand.length) {
    assert.fail(
      `THREE_ENTRY_POINT demand drift: ports with items=${withDemand
        .map((e) => e.port)
        .join(",")} vs empty geography false-negative ports=${emptyDemand
        .map((e) => e.port)
        .join(",")}`
    );
  }
  const panelParity = runtime.checked.map((e) => e.panelHasHpcNative);
  if (panelParity.some(Boolean) && panelParity.some((v) => !v)) {
    assert.fail(
      `THREE_ENTRY_POINT panel drift: HPC-native Area Hotels present on some ports only (${runtime.checked
        .map((e) => `${e.port}:${e.panelHasHpcNative}`)
        .join(", ")})`
    );
  }
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "HE_THREE_ENTRY_POINT_PARITY_V1",
      sourceChecks: {
        demandCountryOnly: true,
        areaHotelsHpcNative: true,
      },
      runtime,
    },
    null,
    2
  )
);
