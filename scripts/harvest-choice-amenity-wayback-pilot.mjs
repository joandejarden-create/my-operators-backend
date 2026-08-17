#!/usr/bin/env node
/**
 * Harvest Choice property HTML from Wayback (official choicehotels.com snapshots)
 * for amenity parse when live pages return 403/Akamai.
 *
 *   node scripts/harvest-choice-amenity-wayback-pilot.mjs
 *   node scripts/harvest-choice-amenity-wayback-pilot.mjs --limit=5
 */
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  parseChoiceAmenitiesFromHtml,
  hasChoiceAmenityMarkers,
} from "../lib/choice-hotel-content-fetch.js";

const OUT_DIR = "reports/choice-amenity-html";
const CSV = "reports/choice-amenities-pilot-worklist.csv";
const SUMMARY = "reports/choice-amenity-wayback-pilot.json";
const UA = "DealalityCensusBot/1.0 (hotel-census; wayback-read)";

function parseArgs() {
  const args = process.argv.slice(2);
  const limitEq = args.find((a) => a.startsWith("--limit="));
  let limit = limitEq ? Number(limitEq.split("=")[1]) : Infinity;
  const idx = args.indexOf("--limit");
  if (idx >= 0 && args[idx + 1] && !args[idx + 1].startsWith("--")) {
    limit = Number(args[idx + 1]);
  }
  return { limit: Number.isFinite(limit) ? limit : Infinity };
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else inQ = !inQ;
    } else if (c === "," && !inQ) {
      out.push(cur);
      cur = "";
    } else cur += c;
  }
  out.push(cur);
  return out;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlockedShell(html) {
  const head = String(html || "").slice(0, 800);
  return /Access Denied|Reference #|Akamai|Permission to access/i.test(head);
}

/**
 * @param {string} url
 */
async function cdxSearch(url) {
  const params = new URLSearchParams({
    url,
    output: "json",
    filter: "statuscode:200",
    fl: "timestamp,original,statuscode,length",
    limit: "30",
  });
  const res = await fetch(`https://web.archive.org/cdx/search/cdx?${params}`, {
    headers: { "user-agent": UA },
  });
  if (!res.ok) return { ok: false, status: res.status, rows: [] };
  const data = await res.json();
  return { ok: true, rows: Array.isArray(data) ? data.slice(1) : [] };
}

/**
 * @param {string} ts
 * @param {string} original
 */
async function fetchWaybackRaw(ts, original) {
  const wb = `https://web.archive.org/web/${ts}id_/${original}`;
  const res = await fetch(wb, {
    headers: { "user-agent": UA },
    redirect: "follow",
  });
  const html = await res.text();
  return { status: res.status, html, wb };
}

/**
 * @param {Array} rows
 * @param {string} id
 */
async function trySnapshots(rows, id) {
  const sorted = [...rows].sort((a, b) => Number(b[3] || 0) - Number(a[3] || 0));
  for (const row of sorted.slice(0, 6)) {
    const [ts, original, , len] = row;
    console.log(`  try ${ts} len=${len}`);
    const got = await fetchWaybackRaw(ts, original);
    if (isBlockedShell(got.html)) {
      console.log("    blocked shell");
      await sleep(400);
      continue;
    }
    const markers = hasChoiceAmenityMarkers(got.html);
    const parsed = parseChoiceAmenitiesFromHtml(got.html);
    const amenities = Array.isArray(parsed.amenities) ? parsed.amenities : [];
    console.log(
      `    status=${got.status} html=${got.html.length} markers=${markers} amenities=${amenities.length} errors=${(parsed.parseErrors || []).join("|")}`
    );
    if (markers && amenities.length >= 3) {
      const path = join(OUT_DIR, `${id}.html`);
      writeFileSync(path, got.html, "utf8");
      console.log(`    SAVED ${path}`);
      return {
        ts,
        original,
        amenityCount: amenities.length,
        amenities: amenities.slice(0, 15),
        parseErrors: parsed.parseErrors || [],
        wb: got.wb,
        path,
      };
    }
    await sleep(400);
  }
  return null;
}

async function main() {
  const { limit } = parseArgs();
  mkdirSync(OUT_DIR, { recursive: true });

  const raw = readFileSync(CSV, "utf8").trim().split(/\r?\n/);
  const header = parseCsvLine(raw[0]);
  const pidIdx = header.indexOf("propertyId");
  const urlIdx = header.indexOf("propertyUrl");
  const rows = raw
    .slice(1)
    .map(parseCsvLine)
    .map((p) => ({
      id: String(p[pidIdx] || "").toLowerCase(),
      url: p[urlIdx],
    }))
    .filter((r) => r.id && r.url)
    .slice(0, limit);

  console.log(`Wayback amenity harvest for ${rows.length} pilot rows…`);
  /** @type {object[]} */
  const summary = [];

  for (const r of rows) {
    console.log(`\n=== ${r.id} ${r.url}`);
    try {
      let found = null;
      const exact = await cdxSearch(r.url);
      console.log(`exact cdx: ${exact.rows.length}${exact.ok ? "" : ` (HTTP ${exact.status})`}`);
      if (exact.rows.length) found = await trySnapshots(exact.rows, r.id);

      if (!found) {
        const wildUrl = `www.choicehotels.com/*/${r.id}`;
        const wild = await cdxSearch(wildUrl);
        const filtered = wild.rows.filter((row) =>
          String(row[1] || "")
            .toLowerCase()
            .includes(`/${r.id}`)
        );
        console.log(`wildcard cdx: ${filtered.length}`);
        if (filtered.length) found = await trySnapshots(filtered, r.id);
      }

      summary.push({ id: r.id, url: r.url, found });
    } catch (err) {
      console.log("ERR", err?.message || err);
      summary.push({ id: r.id, url: r.url, error: String(err?.message || err) });
    }
    await sleep(800);
  }

  writeFileSync(SUMMARY, JSON.stringify({ generatedAt: new Date().toISOString(), summary }, null, 2));
  const saved = summary.filter((s) => s.found).length;
  console.log(`\nDONE saved ${saved}/${summary.length} → ${SUMMARY}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
