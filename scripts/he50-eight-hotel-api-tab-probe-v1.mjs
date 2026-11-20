#!/usr/bin/env node
/**
 * Probe Live HE API surfaces for all eight canonical HPC IDs.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { HE50_CANONICAL_HPC_BY_PILOT_KEY } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";
import { EIGHT_HOTEL_COMPLETENESS } from "../lib/hotel-intelligence/he-5-0-full-e2e/completeness-matrix-v1.js";

const BASE = process.env.HE50_PROBE_BASE || "http://127.0.0.1:8080";
const OUT = join(
  process.cwd(),
  "reports/hotel-explorer/he50-canonical-intelligence-recovery-v1",
);
mkdirSync(OUT, { recursive: true });

const ROUTES = [
  { tab: "hotel", path: (id) => `/api/brand-presence/hotel/${id}` },
  {
    tab: "submarket",
    path: (id) => `/api/hotel-explorer/hotels/${id}/submarket-snapshot`,
  },
  {
    tab: "demand",
    path: (id) => `/api/hotel-explorer/hotels/${id}/demand-drivers`,
  },
  {
    tab: "access",
    path: (id) => `/api/hotel-explorer/hotels/${id}/access-connectivity`,
  },
  {
    tab: "ownership",
    path: (id) => `/api/golden-demo/ownership/hotel/${id}`,
  },
  {
    tab: "sources",
    path: (id) => `/api/hotel-intelligence/hotels/${id}/research`,
  },
];

async function probe(id) {
  const tabs = {};
  for (const r of ROUTES) {
    const url = BASE + r.path(id);
    try {
      const res = await fetch(url);
      const text = await res.text();
      let json = null;
      try {
        json = JSON.parse(text);
      } catch {
        /* ignore */
      }
      tabs[r.tab] = {
        status: res.status,
        ok: res.ok && (json?.success !== false && json?.ok !== false),
        snippet: text.slice(0, 160),
        hasPayload: Boolean(
          json?.hotel ||
            json?.demandDrivers ||
            json?.access ||
            json?.items ||
            json?.snapshot ||
            json?.relationships ||
            json?.research,
        ),
      };
    } catch (e) {
      tabs[r.tab] = { status: 0, ok: false, error: String(e.message || e) };
    }
  }
  return tabs;
}

const hotels = [];
for (const row of EIGHT_HOTEL_COMPLETENESS) {
  const id = row.hpcId || HE50_CANONICAL_HPC_BY_PILOT_KEY[row.pilotKey];
  const tabs = await probe(id);
  hotels.push({
    hotel: row.hotel,
    pilotKey: row.pilotKey,
    hpcId: id,
    tabs,
    okCount: Object.values(tabs).filter((t) => t.ok).length,
    routeCount: ROUTES.length,
  });
}

const report = {
  ok: hotels.every((h) => h.hpcId && h.okCount >= 1),
  base: BASE,
  routeCount: ROUTES.length,
  hotels,
  certifications: {
    TWELVE_TAB_API_READBACK_VALIDATED: false, // only probed 6 API surfaces; full 12 needs browser
    EIGHT_HOTEL_API_IDENTITY_REACHABLE: hotels.every((h) => h.tabs.hotel?.ok),
  },
  note: "Full 12-tab Live render validation requires Opportunity Radar browser journey (separate evidence).",
};

writeFileSync(join(OUT, "EIGHT_HOTEL_API_TAB_PROBE.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
if (!report.ok) process.exit(1);
