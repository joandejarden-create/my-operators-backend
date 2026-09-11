#!/usr/bin/env node
/**
 * Preflight: Golden Four Hotel Explorer share assets must be present before
 * any Railway production deploy. ADP-only CLI uploads have wiped these URLs
 * when the working tree lacked HI public/API surfaces.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const REQUIRED = [
  "public/hotel-intelligence-golden-demo.html",
  "public/hotel-explorer-share.html",
  "public/js/hotel-explorer.js",
  "public/js/hotel-intelligence-golden-demo-route.js",
  "public/js/hotel-intelligence-research-center.js",
  "public/css/hotel-explorer.css",
  "public/css/hotel-intelligence-research-center.css",
  "api/hotel-intelligence-research.js",
  "api/hotel-intelligence-dossier.js",
  "lib/hotel-intelligence/golden-demo/mexico-radar-fixture-fallback.js",
];

const GOLDEN_FOUR = [
  "recUNycnMwOVFX0hc", // KGPV
  "recIwaP1etgx2g9nA", // Cambridge
  "recsYJb2R1jarPpK3", // Sheraton GDL
  "recTYaiA4S6fR6ixx", // voco Cancún
];

const missing = REQUIRED.filter((rel) => !fs.existsSync(path.join(root, rel)));
if (missing.length) {
  console.error("FAIL assert-hotel-explorer-share-assets: missing files:");
  for (const rel of missing) console.error(`  - ${rel}`);
  console.error(
    "Do not railway-up production without Hotel Explorer share assets. Merge hotfix/restore-mexico-hi-share-onto-main (PR #40) or deploy from a tree that includes them."
  );
  process.exit(1);
}

const goldenHtml = fs.readFileSync(
  path.join(root, "public/hotel-intelligence-golden-demo.html"),
  "utf8"
);
const serverJs = fs.readFileSync(path.join(root, "server.js"), "utf8");
if (!/hotel-intelligence-golden-demo\.html/.test(serverJs)) {
  console.error(
    "FAIL: server.js must reference hotel-intelligence-golden-demo.html (explicit route)."
  );
  process.exit(1);
}

const routeJs = fs.readFileSync(
  path.join(root, "public/js/hotel-intelligence-golden-demo-route.js"),
  "utf8"
);
for (const id of GOLDEN_FOUR) {
  if (!routeJs.includes(id) && !goldenHtml.includes(id)) {
    // IDs may live only in route allowlists / fixtures — warn, do not hard-fail
    console.warn(`note: golden-four id ${id} not found in golden-demo HTML (ok if allowlisted in route JS only)`);
  }
}

console.log("PASS assert-hotel-explorer-share-assets");
for (const rel of REQUIRED) console.log(`  ok ${rel}`);
process.exit(0);
