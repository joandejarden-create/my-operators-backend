#!/usr/bin/env node
/**
 * Retry missed Choice amenity Wayback harvests with alternate URL forms.
 *
 *   node scripts/retry-choice-amenity-wayback-misses.mjs
 */
import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import {
  parseChoiceAmenitiesFromHtml,
  hasChoiceAmenityMarkers,
} from "../lib/choice-hotel-content-fetch.js";

const OUT_DIR = "reports/choice-amenity-html";
const UA = "DealalityCensusBot/1.0 (hotel-census; wayback-read)";

const MISSES = [
  {
    id: "mx051",
    urls: [
      "https://www.choicehotels.com/nuevo-leon/monterrey/comfort-inn-hotels/mx051",
      "https://www.choicehotels.com/en-mx/nuevo-leon/monterrey/comfort-inn-hotels/mx051",
      "https://www.choicehotels.com/*/mx051",
      "www.choicehotels.com/*mx051*",
    ],
  },
  {
    id: "mx065",
    urls: [
      "https://www.choicehotels.com/coahuila/torreon/sleep-inn-hotels/mx065",
      "https://www.choicehotels.com/en-mx/coahuila/torreon/sleep-inn-hotels/mx065",
      "www.choicehotels.com/*mx065*",
    ],
  },
  {
    id: "mx070",
    urls: [
      "https://www.choicehotels.com/coahuila/saltillo/quality-inn-hotels/mx070",
      "https://www.choicehotels.com/en-mx/coahuila/saltillo/quality-inn-hotels/mx070",
      "www.choicehotels.com/*mx070*",
    ],
  },
  {
    id: "mx071",
    urls: [
      "https://www.choicehotels.com/sinaloa/mazatlan/quality-inn-hotels/mx071",
      "https://www.choicehotels.com/en-mx/sinaloa/mazatlan/quality-inn-hotels/mx071",
      "www.choicehotels.com/*mx071*",
    ],
  },
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlockedShell(html) {
  const head = String(html || "").slice(0, 800);
  return /Access Denied|Reference #|Akamai|Permission to access/i.test(head);
}

async function cdxSearch(url) {
  const params = new URLSearchParams({
    url,
    matchType: url.includes("*") ? "domain" : "exact",
    output: "json",
    filter: "statuscode:200",
    fl: "timestamp,original,statuscode,length",
    limit: "50",
  });
  // For wildcard path searches use url= without forcing domain matchType
  if (url.includes("*") && !url.startsWith("www.")) {
    params.set("matchType", "prefix");
  }
  if (url.includes("*")) {
    params.delete("matchType");
  }
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(`https://web.archive.org/cdx/search/cdx?${params}`, {
        headers: { "user-agent": UA },
      });
      if (res.status === 503 || res.status === 429) {
        await sleep(2000 * (attempt + 1));
        continue;
      }
      if (!res.ok) return { ok: false, status: res.status, rows: [] };
      const data = await res.json();
      return { ok: true, rows: Array.isArray(data) ? data.slice(1) : [] };
    } catch {
      await sleep(1500 * (attempt + 1));
    }
  }
  return { ok: false, status: 0, rows: [] };
}

async function fetchWaybackRaw(ts, original) {
  const wb = `https://web.archive.org/web/${ts}id_/${original}`;
  const res = await fetch(wb, { headers: { "user-agent": UA }, redirect: "follow" });
  const html = await res.text();
  return { status: res.status, html, wb };
}

async function trySnapshots(rows, id) {
  const filtered = rows.filter((r) =>
    String(r[1] || "")
      .toLowerCase()
      .includes(`/${id}`)
  );
  const sorted = [...(filtered.length ? filtered : rows)].sort(
    (a, b) => Number(b[3] || 0) - Number(a[3] || 0)
  );
  for (const row of sorted.slice(0, 8)) {
    const [ts, original, , len] = row;
    console.log(`  try ${ts} len=${len} ${String(original).slice(-70)}`);
    const got = await fetchWaybackRaw(ts, original);
    if (isBlockedShell(got.html)) {
      console.log("    blocked");
      await sleep(500);
      continue;
    }
    const markers = hasChoiceAmenityMarkers(got.html);
    const parsed = parseChoiceAmenitiesFromHtml(got.html);
    const amenities = parsed.amenities || [];
    console.log(
      `    status=${got.status} html=${got.html.length} markers=${markers} amenities=${amenities.length}`
    );
    if (markers && amenities.length >= 3) {
      const path = join(OUT_DIR, `${id}.html`);
      writeFileSync(path, got.html, "utf8");
      console.log(`    SAVED ${path}`);
      return { ts, original, amenityCount: amenities.length, path, wb: got.wb };
    }
    await sleep(600);
  }
  return null;
}

async function main() {
  mkdirSync(OUT_DIR, { recursive: true });
  const summary = [];

  for (const miss of MISSES) {
    if (existsSync(join(OUT_DIR, `${miss.id}.html`))) {
      console.log(`\n=== ${miss.id} already saved — skip`);
      summary.push({ id: miss.id, skipped: true });
      continue;
    }
    console.log(`\n=== retry ${miss.id}`);
    let found = null;
    for (const url of miss.urls) {
      console.log(` CDX ${url}`);
      const idx = await cdxSearch(url);
      console.log(`  rows=${idx.rows.length}${idx.ok ? "" : ` status=${idx.status}`}`);
      if (idx.rows.length) {
        found = await trySnapshots(idx.rows, miss.id);
        if (found) break;
      }
      await sleep(1200);
    }
    summary.push({ id: miss.id, found });
    await sleep(1500);
  }

  writeFileSync(
    "reports/choice-amenity-wayback-retry.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), summary }, null, 2)
  );
  console.log(
    `\nDONE retry saved ${summary.filter((s) => s.found).length}/${MISSES.length}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
