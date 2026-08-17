#!/usr/bin/env node
/**
 * Sync lat/lng in DR source point modules from verified coordinate map.
 */
import { DR_VERIFIED_COORDINATES } from "../lib/radar-buildout/dominican-republic-verified-coordinates.js";
import { DOMINICAN_REPUBLIC_DEMAND_ANCHOR_POINTS } from "../lib/radar-buildout/dominican-republic-demand-anchors-points.js";
import { DR_DEMAND_ANCHORS_SECOND_PASS } from "../lib/radar-buildout/dominican-republic-demand-anchors-second-pass.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

function syncModule(relPath, exportName, points) {
  let src = fs.readFileSync(path.join(root, relPath), "utf8");
  let n = 0;
  for (const p of points) {
    const v = DR_VERIFIED_COORDINATES[p.name];
    if (!v) continue;
    const latChanged = Math.abs(p.latitude - v.latitude) > 0.0001;
    const lngChanged = Math.abs(p.longitude - v.longitude) > 0.0001;
    if (!latChanged && !lngChanged) continue;
    const latRe = new RegExp(
      `(name:\\s*"${p.name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"[\\s\\S]*?latitude:\\s*)[0-9.]+`,
      "m"
    );
    const lngRe = new RegExp(
      `(name:\\s*"${p.name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"[\\s\\S]*?longitude:\\s*)-?[0-9.]+`,
      "m"
    );
    if (!latRe.test(src) || !lngRe.test(src)) {
      console.warn("SKIP (pattern miss):", p.name);
      continue;
    }
    src = src.replace(latRe, `$1${v.latitude}`);
    src = src.replace(lngRe, `$1${v.longitude}`);
    n += 1;
    console.log("Synced:", p.name, "→", v.latitude, v.longitude);
  }
  fs.writeFileSync(path.join(root, relPath), src);
  return n;
}

const n1 = syncModule(
  "lib/radar-buildout/dominican-republic-demand-anchors-points.js",
  "DOMINICAN_REPUBLIC_DEMAND_ANCHOR_POINTS",
  DOMINICAN_REPUBLIC_DEMAND_ANCHOR_POINTS
);
const n2 = syncModule(
  "lib/radar-buildout/dominican-republic-demand-anchors-second-pass.js",
  "DR_DEMAND_ANCHORS_SECOND_PASS",
  DR_DEMAND_ANCHORS_SECOND_PASS
);
console.log("\nSynced", n1 + n2, "records in source modules");
