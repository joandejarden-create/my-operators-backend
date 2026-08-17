#!/usr/bin/env node
/**
 * Harvest Choice amenity HTML via Wayback for worklist rows that lack a usable saved file.
 *
 *   node scripts/harvest-choice-amenity-wayback-missing.mjs --limit=25
 */
import { spawnSync } from "node:child_process";
import {
  existsSync,
  readFileSync,
  writeFileSync,
  mkdirSync,
} from "node:fs";
import { parseChoiceAmenitiesFromHtml } from "../lib/choice-hotel-content-fetch.js";

const CSV = "reports/choice-amenities-steward-worklist.csv";
const OUT = "reports/choice-amenities-steward-worklist-missing.csv";
const HTML_DIR = "reports/choice-amenity-html";
const limit = Number(
  process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] || 25
);

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

function hasUsableHtml(pid) {
  const path = `${HTML_DIR}/${String(pid).toLowerCase()}.html`;
  if (!existsSync(path)) return false;
  try {
    const parsed = parseChoiceAmenitiesFromHtml(readFileSync(path, "utf8"));
    return Boolean(parsed.hasAmenityMarkers && parsed.amenities?.length >= 3);
  } catch {
    return false;
  }
}

const raw = readFileSync(CSV, "utf8").trim().split(/\r?\n/);
const header = parseCsvLine(raw[0]);
const pidIdx = header.indexOf("propertyId");
const missing = raw.slice(1).filter((line) => {
  const cols = parseCsvLine(line);
  const pid = cols[pidIdx];
  return pid && !hasUsableHtml(pid);
});

mkdirSync("reports", { recursive: true });
writeFileSync(OUT, [raw[0], ...missing.slice(0, limit)].join("\n") + "\n");
console.log(`Missing usable HTML in worklist: ${missing.length}; harvesting first ${Math.min(limit, missing.length)}`);

const r = spawnSync(
  process.execPath,
  [
    "scripts/harvest-choice-amenity-wayback-steward.mjs",
    `--input=${OUT}`,
    "--delay-ms=700",
  ],
  { stdio: "inherit" }
);
process.exit(r.status || 0);
