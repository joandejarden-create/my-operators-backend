#!/usr/bin/env node
/**
 * Targeted Wayback CDX probe for steward-unmatched Hyatt CALA names
 * that are absent from the current directory extract.
 * Official hyatt.com URLs only.
 *
 *   node scripts/probe-hyatt-unmatched-cdx.mjs
 */
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import {
  mergeHyattDirectoryRows,
  parseHyattPropertyRowsFromLocs,
  writeHyattDirectoryExtract,
  HYATT_CDX_SOURCE,
} from "../lib/hyatt-brand-directory-extract.js";

const PATH = "reports/hyatt-cala-directory-extract.json";
const OUT = "reports/hyatt-unmatched-cdx-probe.json";

/** Slug token probes derived from unmatched census names missing from directory. */
const PROBES = [
  // Classic openings / missing archive
  { label: "hyatt-place-cancun-airport", pattern: "www.hyatt.com/en-US/hotel/*hyatt-place-cancun*" },
  { label: "hyatt-place-piedras", pattern: "www.hyatt.com/en-US/hotel/*piedras*" },
  { label: "hyatt-place-georgetown", pattern: "www.hyatt.com/en-US/hotel/*georgetown*" },
  { label: "hyatt-place-piantini", pattern: "www.hyatt.com/en-US/hotel/*piantini*" },
  { label: "hyatt-place-merida", pattern: "www.hyatt.com/en-US/hotel/*merida*" },
  { label: "hyatt-centric-queretaro", pattern: "www.hyatt.com/en-US/hotel/*queretaro*" },
  { label: "hyatt-centric-playa", pattern: "www.hyatt.com/en-US/hotel/*hyatt-centric-playa*" },
  { label: "hyatt-centric-santo-domingo", pattern: "www.hyatt.com/en-US/hotel/*hyatt-centric-santo*" },
  { label: "hyatt-centric-escazu", pattern: "www.hyatt.com/en-US/hotel/*escazu*" },
  { label: "park-hyatt-cancun", pattern: "www.hyatt.com/en-US/hotel/*park-hyatt-cancun*" },
  { label: "park-hyatt-mexico-city", pattern: "www.hyatt.com/en-US/hotel/*park-hyatt-mexico*" },
  { label: "grand-hyatt-baha-mar", pattern: "www.hyatt.com/en-US/hotel/*baha-mar*" },
  { label: "grand-hyatt-cayman", pattern: "www.hyatt.com/en-US/hotel/*grand-cayman*" },
  { label: "grand-hyatt-st-lucia", pattern: "www.hyatt.com/en-US/hotel/*grand-hyatt*lucia*" },
  { label: "grand-hyatt-los-cabos", pattern: "www.hyatt.com/en-US/hotel/*grand-hyatt*cabos*" },
  { label: "grand-hyatt-santa-fe", pattern: "www.hyatt.com/en-US/hotel/*grand-hyatt*santa-fe*" },
  { label: "hyatt-regency-trinidad", pattern: "www.hyatt.com/en-US/hotel/*trinidad*" },
  { label: "hyatt-regency-panama", pattern: "www.hyatt.com/en-US/hotel/*hyatt-regency-panama*" },
  { label: "thompson-monterrey", pattern: "www.hyatt.com/en-US/hotel/*thompson*monterrey*" },
  { label: "thompson-reforma", pattern: "www.hyatt.com/en-US/hotel/*thompson*reforma*" },
  { label: "thompson-puerto-vallarta", pattern: "www.hyatt.com/en-US/hotel/*thompson*vallarta*" },
  { label: "andaz-mayakoba", pattern: "www.hyatt.com/en-US/hotel/*andaz*mayakoba*" },
  { label: "andaz-turks", pattern: "www.hyatt.com/en-US/hotel/*andaz*turks*" },
  { label: "andaz-costa-rica", pattern: "www.hyatt.com/en-US/hotel/*andaz*costa-rica*" },
  { label: "hyatt-vivid-playa", pattern: "www.hyatt.com/en-US/hotel/*vivid*playa*" },
  // Inclusive gaps
  { label: "dreams-onyx", pattern: "www.hyatt.com/en-US/hotel/*onyx*" },
  { label: "secrets-the-vine", pattern: "www.hyatt.com/en-US/hotel/*the-vine*" },
  { label: "secrets-macao", pattern: "www.hyatt.com/en-US/hotel/*secrets*macao*" },
  { label: "breathless-cancun-soul", pattern: "www.hyatt.com/en-US/hotel/*cancun-soul*" },
  { label: "now-grand-island", pattern: "www.hyatt.com/en-US/hotel/*now*grand-island*" },
  { label: "dreams-grand-island", pattern: "www.hyatt.com/en-US/hotel/*dreams*grand-island*" },
  { label: "dreams-st-lucia", pattern: "www.hyatt.com/en-US/hotel/*dreams*lucia*" },
  { label: "dreams-rose-hall", pattern: "www.hyatt.com/en-US/hotel/*dreams*rose-hall*" },
  { label: "destination-placencia", pattern: "www.hyatt.com/en-US/hotel/*placencia*" },
  { label: "cariari", pattern: "www.hyatt.com/en-US/hotel/*cariari*" },
];

async function cdxFetch(urlPattern, limit = 50) {
  const cdx = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(
    urlPattern
  )}&output=json&fl=original&collapse=urlkey&limit=${limit}&filter=statuscode:200`;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 35000);
  try {
    const res = await fetch(cdx, {
      headers: { "User-Agent": "DealalityResearch/1.0 (hyatt census triage)" },
      signal: ctrl.signal,
    });
    const text = await res.text();
    let rows = [];
    try {
      rows = JSON.parse(text);
    } catch {
      rows = [];
    }
    const urls = Array.isArray(rows)
      ? [...new Set(rows.slice(1).map((r) => (Array.isArray(r) ? r[0] : "")).filter(Boolean))]
      : [];
    return { status: res.status, urls, error: null };
  } catch (err) {
    return { status: 0, urls: [], error: err?.message || String(err) };
  } finally {
    clearTimeout(timer);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

const existing = existsSync(PATH) ? JSON.parse(readFileSync(PATH, "utf8")) : { propertyRows: [] };
const beforeIds = new Set(
  (existing.propertyRows || []).map((r) => String(r.propertyId || "").toUpperCase()).filter(Boolean)
);

const log = [];
const locs = [];

console.log(`Probing ${PROBES.length} targeted CDX patterns (existing PIDs=${beforeIds.size})…`);

for (const p of PROBES) {
  const r = await cdxFetch(p.pattern);
  const hotelUrls = r.urls.filter((u) => /\/hotel\//i.test(u) && /hyatt\.com/i.test(u));
  log.push({
    label: p.label,
    pattern: p.pattern,
    status: r.status,
    error: r.error,
    urlCount: hotelUrls.length,
    sample: hotelUrls.slice(0, 5),
  });
  console.log(
    `  ${p.label}: ${r.error || r.status} → ${hotelUrls.length} urls${
      hotelUrls[0] ? ` e.g. ${hotelUrls[0]}` : ""
    }`
  );
  locs.push(...hotelUrls);
  await sleep(800);
}

const uniqueLocs = [...new Set(locs)];
const parsed = parseHyattPropertyRowsFromLocs(uniqueLocs, {
  source: `${HYATT_CDX_SOURCE}|unmatched_triage_probe`,
});
const newRows = (parsed || []).filter(
  (r) => r.propertyId && !beforeIds.has(String(r.propertyId).toUpperCase()) && r.isCala !== false
);

const report = {
  generatedAt: new Date().toISOString(),
  probes: log.length,
  uniqueLocs: uniqueLocs.length,
  parsedRows: (parsed || []).length,
  newCalaPropertyIds: newRows.map((r) => ({
    propertyId: r.propertyId,
    name: r.name,
    url: r.propertyUrl,
    country: r.censusCountry || r.country,
  })),
  newCount: newRows.length,
  log,
};

writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(`\nNew CALA PIDs: ${newRows.length}`);
for (const n of newRows) {
  console.log(`  + ${n.propertyId} ${n.name || ""} ${n.propertyUrl}`);
}

if (newRows.length) {
  const merged = mergeHyattDirectoryRows(existing.propertyRows || [], newRows);
  writeHyattDirectoryExtract(PATH, {
    ...existing,
    propertyRows: merged,
    unmatchedTriageCdxProbe: {
      generatedAt: report.generatedAt,
      newCount: newRows.length,
      newPropertyIds: newRows.map((r) => r.propertyId),
    },
  });
  console.log(`Merged into ${PATH}; unique now ${merged.length}`);
} else {
  console.log("No new safe directory rows — coverage exhausted for these probes.");
}

console.log(`Wrote ${OUT}`);
