#!/usr/bin/env node
/**
 * Fix Mexico Region on demand-anchor and travel-infrastructure fixtures.
 *   node scripts/patch-mexico-region-in-fixtures.mjs
 */
import { readFileSync, writeFileSync, readdirSync, statSync } from "fs";
import { join } from "path";
import { MEXICO_RADAR_REGION } from "../lib/radar-buildout/mexico-radar-region.js";

const WRONG = new Set(["Mexico", "Caribbean"]);
const ROOTS = ["fixtures", "public/fixtures"];

function walkJsonFiles(dir, out = []) {
  for (const name of readdirSync(dir)) {
    const abs = join(dir, name);
    if (statSync(abs).isDirectory()) continue;
    if (!name.endsWith(".json")) continue;
    if (!/mexico|los-cabos|guadalajara|monterrey|merida-yucatan|puerto-vallarta-riviera-nayarit/i.test(name)) {
      continue;
    }
    out.push(abs);
  }
  return out;
}

let filesChanged = 0;
let pointsPatched = 0;

for (const root of ROOTS) {
  let files = [];
  try {
    files = walkJsonFiles(root);
  } catch {
    continue;
  }

  for (const abs of files) {
    const payload = JSON.parse(readFileSync(abs, "utf8"));
    if (payload.country !== "Mexico") continue;

    let changed = false;
    if (WRONG.has(payload.region)) {
      payload.region = MEXICO_RADAR_REGION;
      changed = true;
    }

    for (const point of payload.points || []) {
      if (point.country === "Mexico" && WRONG.has(point.region)) {
        point.region = MEXICO_RADAR_REGION;
        pointsPatched += 1;
        changed = true;
      }
    }

    if (changed) {
      writeFileSync(abs, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      filesChanged += 1;
      console.log("patched", abs.replace(/\\/g, "/"));
    }
  }
}

console.log(`Done. ${filesChanged} file(s), ${pointsPatched} point region field(s) updated to ${MEXICO_RADAR_REGION}.`);
