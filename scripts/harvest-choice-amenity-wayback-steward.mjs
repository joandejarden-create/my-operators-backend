#!/usr/bin/env node
/**
 * Full steward harvest: Choice property HTML from Wayback for blank Amenities rows.
 * Live choicehotels.com property pages remain 403/Akamai-blocked.
 *
 *   node scripts/harvest-choice-amenity-wayback-steward.mjs
 *   node scripts/harvest-choice-amenity-wayback-steward.mjs --limit=30 --offset=0
 *   node scripts/harvest-choice-amenity-wayback-steward.mjs --delay-ms=900
 *
 * Input: reports/choice-amenities-steward-worklist.csv
 * Output HTML: reports/choice-amenity-html/{propertyId}.html
 * Summary: reports/choice-amenity-wayback-steward.json
 */
import {
  mkdirSync,
  writeFileSync,
  readFileSync,
  existsSync,
} from "node:fs";
import { join } from "node:path";
import {
  parseChoiceAmenitiesFromHtml,
  hasChoiceAmenityMarkers,
} from "../lib/choice-hotel-content-fetch.js";

const OUT_DIR = "reports/choice-amenity-html";
const DEFAULT_CSV = "reports/choice-amenities-steward-worklist.csv";
const SUMMARY = "reports/choice-amenity-wayback-steward.json";
const UA = "DealalityCensusBot/1.0 (hotel-census; wayback-read)";

function parseArgs() {
  const args = process.argv.slice(2);
  const num = (name, def) => {
    const eq = args.find((a) => a.startsWith(`${name}=`));
    if (eq) return Number(eq.split("=")[1]);
    const idx = args.indexOf(name);
    if (idx >= 0 && args[idx + 1] && !args[idx + 1].startsWith("--")) {
      return Number(args[idx + 1]);
    }
    return def;
  };
  const str = (name, def) => {
    const eq = args.find((a) => a.startsWith(`${name}=`));
    if (eq) return eq.slice(name.length + 1);
    const idx = args.indexOf(name);
    if (idx >= 0 && args[idx + 1] && !args[idx + 1].startsWith("--")) {
      return args[idx + 1];
    }
    return def;
  };
  return {
    limit: num("--limit", Infinity),
    offset: num("--offset", 0),
    delayMs: num("--delay-ms", 900),
    input: str("--input", DEFAULT_CSV),
    nameFilter: str("--name-filter", ""),
  };
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
    limit: "40",
  });
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const res = await fetch(`https://web.archive.org/cdx/search/cdx?${params}`, {
        headers: { "user-agent": UA },
      });
      if (res.status === 503 || res.status === 429 || res.status === 504) {
        await sleep(1500 * (attempt + 1));
        continue;
      }
      if (!res.ok) return { ok: false, status: res.status, rows: [] };
      const data = await res.json();
      return { ok: true, rows: Array.isArray(data) ? data.slice(1) : [] };
    } catch {
      await sleep(1200 * (attempt + 1));
    }
  }
  return { ok: false, status: 0, rows: [] };
}

/**
 * @param {string} ts
 * @param {string} original
 */
async function fetchWaybackRaw(ts, original) {
  const wb = `https://web.archive.org/web/${ts}id_/${original}`;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(wb, {
        headers: { "user-agent": UA },
        redirect: "follow",
      });
      if (res.status === 503 || res.status === 429) {
        await sleep(1500 * (attempt + 1));
        continue;
      }
      const html = await res.text();
      return { status: res.status, html, wb };
    } catch {
      await sleep(1000 * (attempt + 1));
    }
  }
  return { status: 0, html: "", wb };
}

/**
 * @param {Array} rows
 * @param {string} id
 */
async function trySnapshots(rows, id) {
  const filtered = rows.filter((r) =>
    String(r[1] || "")
      .toLowerCase()
      .includes(`/${id}`)
  );
  const pool = filtered.length ? filtered : rows;
  const sorted = [...pool].sort((a, b) => Number(b[3] || 0) - Number(a[3] || 0));

  for (const row of sorted.slice(0, 6)) {
    const [ts, original, , len] = row;
    console.log(`  try ${ts} len=${len}`);
    const got = await fetchWaybackRaw(ts, original);
    if (!got.html || isBlockedShell(got.html)) {
      console.log(`    skip status=${got.status} blocked_or_empty`);
      await sleep(350);
      continue;
    }
    const markers = hasChoiceAmenityMarkers(got.html);
    const parsed = parseChoiceAmenitiesFromHtml(got.html);
    const amenities = Array.isArray(parsed.amenities) ? parsed.amenities : [];
    console.log(
      `    status=${got.status} html=${got.html.length} markers=${markers} amenities=${amenities.length}`
    );
    if (markers && amenities.length >= 3) {
      const path = join(OUT_DIR, `${id}.html`);
      writeFileSync(path, got.html, "utf8");
      console.log(`    SAVED ${path}`);
      return {
        ts,
        original,
        amenityCount: amenities.length,
        amenities: amenities.slice(0, 12),
        wb: got.wb,
        path,
      };
    }
    await sleep(350);
  }
  return null;
}

/**
 * @param {string} id
 */
function existingParseOk(id) {
  const path = join(OUT_DIR, `${id}.html`);
  if (!existsSync(path)) return null;
  try {
    const html = readFileSync(path, "utf8");
    if (isBlockedShell(html)) return null;
    const parsed = parseChoiceAmenitiesFromHtml(html);
    if (parsed.hasAmenityMarkers && parsed.amenities.length >= 3) {
      return {
        reused: true,
        path,
        amenityCount: parsed.amenities.length,
        amenities: parsed.amenities.slice(0, 12),
      };
    }
  } catch {
    return null;
  }
  return null;
}

function writeProgress(summary, meta) {
  writeFileSync(
    SUMMARY,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        ...meta,
        saved: summary.filter((s) => s.found).length,
        reused: summary.filter((s) => s.found?.reused).length,
        missed: summary.filter((s) => !s.found && !s.error).length,
        errors: summary.filter((s) => s.error).length,
        summary,
      },
      null,
      2
    )
  );
}

async function main() {
  const { limit, offset, delayMs, input: CSV, nameFilter } = parseArgs();
  mkdirSync(OUT_DIR, { recursive: true });

  if (!existsSync(CSV)) {
    console.error(`Missing ${CSV} — run: node scripts/export-choice-amenities-steward-worklist.mjs`);
    process.exit(1);
  }

  const raw = readFileSync(CSV, "utf8").trim().split(/\r?\n/);
  const header = parseCsvLine(raw[0]);
  const pidIdx = header.indexOf("propertyId");
  const urlIdx = header.indexOf("propertyUrl");
  const nameIdx = header.indexOf("censusName");
  const filterRe = nameFilter ? new RegExp(nameFilter, "i") : null;
  const all = raw
    .slice(1)
    .map(parseCsvLine)
    .map((p) => ({
      id: String(p[pidIdx] || "").toLowerCase(),
      url: p[urlIdx],
      name: p[nameIdx] || "",
    }))
    .filter((r) => r.id && r.url)
    .filter((r) => !filterRe || filterRe.test(r.name) || filterRe.test(r.url));

  const rows = all.slice(offset, Number.isFinite(limit) ? offset + limit : undefined);
  console.log(
    `Wayback steward harvest: ${rows.length} rows (offset=${offset}, pool=${all.length}, delayMs=${delayMs}${
      nameFilter ? `, filter=${nameFilter}` : ""
    })`
  );

  /** @type {object[]} */
  const summary = [];
  const meta = { offset, limit: Number.isFinite(limit) ? limit : null, poolSize: all.length };

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    console.log(`\n[${i + 1}/${rows.length}] === ${r.id} ${r.name || r.url}`);
    try {
      const reused = existingParseOk(r.id);
      if (reused) {
        console.log(`  reuse existing HTML (${reused.amenityCount} amenities)`);
        summary.push({ id: r.id, url: r.url, name: r.name, found: reused });
        writeProgress(summary, meta);
        continue;
      }

      let found = null;
      const exact = await cdxSearch(r.url);
      console.log(`  exact cdx: ${exact.rows.length}${exact.ok ? "" : ` (HTTP ${exact.status})`}`);
      if (exact.rows.length) found = await trySnapshots(exact.rows, r.id);

      if (!found) {
        const wild = await cdxSearch(`www.choicehotels.com/*/${r.id}`);
        const filtered = wild.rows.filter((row) =>
          String(row[1] || "")
            .toLowerCase()
            .includes(`/${r.id}`)
        );
        console.log(`  wildcard cdx: ${filtered.length}`);
        if (filtered.length) found = await trySnapshots(filtered, r.id);
      }

      summary.push({ id: r.id, url: r.url, name: r.name, found });
    } catch (err) {
      console.log("  ERR", err?.message || err);
      summary.push({
        id: r.id,
        url: r.url,
        name: r.name,
        error: String(err?.message || err),
      });
    }
    writeProgress(summary, meta);
    await sleep(delayMs);
  }

  writeProgress(summary, meta);
  const saved = summary.filter((s) => s.found).length;
  console.log(`\nDONE saved/reused ${saved}/${summary.length} → ${SUMMARY}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
