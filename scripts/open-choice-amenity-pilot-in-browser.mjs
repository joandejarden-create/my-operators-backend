#!/usr/bin/env node
/**
 * Open Choice amenity pilot URLs in the system default browser (Windows).
 *
 *   node scripts/open-choice-amenity-pilot-in-browser.mjs
 *   node scripts/open-choice-amenity-pilot-in-browser.mjs --limit 5
 */
import "../load-env.js";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const CSV = join("reports", "choice-amenities-pilot-worklist.csv");

function parseArgs() {
  const args = process.argv.slice(2);
  const limitEq = args.find((a) => a.startsWith("--limit="));
  let limit = limitEq ? Number(limitEq.split("=")[1]) : 5;
  const idx = args.indexOf("--limit");
  if (idx >= 0 && args[idx + 1] && !args[idx + 1].startsWith("--")) {
    limit = Number(args[idx + 1]);
  }
  return { limit };
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

async function openUrl(url) {
  if (process.platform === "win32") {
    await execFileAsync("cmd", ["/c", "start", "", url], { windowsHide: true });
    return;
  }
  if (process.platform === "darwin") {
    await execFileAsync("open", [url]);
    return;
  }
  await execFileAsync("xdg-open", [url]);
}

const { limit } = parseArgs();
const raw = readFileSync(CSV, "utf8").trim().split(/\r?\n/);
const header = parseCsvLine(raw[0]);
const urlIdx = header.indexOf("propertyUrl");
const pidIdx = header.indexOf("propertyId");

const rows = raw.slice(1, 1 + limit);
console.log(`Opening ${rows.length} pilot URLs in default browser…`);
console.log("Save each page to reports/choice-amenity-html/{propertyId}.html then run apply.\n");

for (const line of rows) {
  const p = parseCsvLine(line);
  const url = p[urlIdx];
  const pid = p[pidIdx];
  console.log(pid, url);
  await openUrl(url);
  await sleep(2500);
}

console.log("\nDone. After saves: node scripts/apply-choice-amenities-from-html.mjs --apply");
