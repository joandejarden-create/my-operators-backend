#!/usr/bin/env node
/**
 * Compare key product paths vs a backup tree. Report missing/differing files.
 */
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const backup =
  process.argv[2] ||
  "C:\\Dev\\dealality-backups\\LATEST\\deal-capture-proxy";

const AREAS = {
  "Brand Explorer": [
    "public/brand-explorer.html",
    "public/js/brand-explorer.js",
    "public/js/brand-explorer-atelier.js",
    "public/js/brand-explorer-route-state.js",
    "api/brand-library.js",
  ],
  "Brand AI Intelligence": [
    "public/ai-visibility.html",
    "public/ai-visibility-brand.html",
    "public/js/ai-visibility",
  ],
  "AI Demand Positioning": [
    "public/owner-ai-demand.html",
    "public/js/owner-ai-demand.js",
    "api/ai-demand-positioning.js",
  ],
  "AI Demand Admin": [
    "public/app/admin/ai-demand-admin.html",
    "public/js/admin-ai-demand-admin.js",
    "public/js/admin-ai-demand-reviews.js",
    "public/css/admin-ai-demand-admin.css",
    "public/css/admin-ai-demand-reviews.css",
  ],
  GDI: [
    "public/group-demand-intelligence.html",
    "public/js/group-demand-intelligence.js",
    "api/group-demand-intelligence.js",
  ],
  "Market Alerts": [
    "public/market-alerts.html",
    "public/market-alerts.js",
    "api/market-alerts.js",
    "lib/market-alerts-contact",
  ],
  "Hotel Intelligence": [
    "public/hotel-intelligence.html",
    "public/hotel-intelligence-golden-demo.html",
    "api/hotel-intelligence.js",
    "lib/hotel-intelligence",
  ],
  "Hotel Contact Intelligence": [
    "public/js/hotel-contact-intelligence.js",
    "public/css/hotel-contact-intelligence.css",
    "public/data/hotel-contact-intelligence",
  ],
  "Hotel Census": [
    "lib/hotel-census/census-map-snapshot.js",
    "lib/hotel-census/map-hotel-dto.js",
    "api/brand-presence.js",
  ],
};

function sha(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

function listFiles(p) {
  if (!fs.existsSync(p)) return [];
  const st = fs.statSync(p);
  if (st.isFile()) return [p];
  const out = [];
  const walk = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const fp = path.join(d, e.name);
      if (e.isDirectory()) walk(fp);
      else out.push(fp);
    }
  };
  walk(p);
  return out;
}

const rows = [];
for (const [feature, paths] of Object.entries(AREAS)) {
  for (const rel of paths) {
    const cur = path.join(root, rel);
    const bak = path.join(backup, rel);
    const curExists = fs.existsSync(cur);
    const bakExists = fs.existsSync(bak);
    if (!curExists && !bakExists) {
      rows.push({ feature, path: rel, status: "ABSENT_BOTH", recovery: "NO" });
      continue;
    }
    if (!curExists && bakExists) {
      rows.push({ feature, path: rel, status: "MISSING_CURRENT", recovery: "YES", backupOnly: true });
      continue;
    }
    if (curExists && !bakExists) {
      rows.push({ feature, path: rel, status: "CURRENT_ONLY_NEWER_OR_LOCAL", recovery: "NO" });
      continue;
    }
    // both exist — if dirs, compare file counts; if files compare hash
    const curFiles = listFiles(cur).map((f) => path.relative(cur, f));
    const bakFiles = listFiles(bak).map((f) => path.relative(bak, f));
    if (fs.statSync(cur).isDirectory()) {
      const missingInCur = bakFiles.filter((f) => !curFiles.includes(f));
      const onlyInCur = curFiles.filter((f) => !bakFiles.includes(f));
      rows.push({
        feature,
        path: rel,
        status: missingInCur.length ? "DIR_MISSING_FILES" : onlyInCur.length ? "DIR_EXTRA_OR_EQUAL" : "DIR_EQUAL_NAMES",
        recovery: missingInCur.length ? "YES" : "NO",
        missingInCur: missingInCur.slice(0, 20),
        onlyInCurCount: onlyInCur.length,
        bakCount: bakFiles.length,
        curCount: curFiles.length,
      });
    } else {
      const same = sha(cur) === sha(bak);
      rows.push({
        feature,
        path: rel,
        status: same ? "IDENTICAL" : "DIFFERENT",
        recovery: "NO",
        note: same ? null : "compare manually — may be EXPECTED NEWER or REGRESSION",
      });
    }
  }
}

const out = {
  backup,
  generatedAt: new Date().toISOString(),
  recoveryNeeded: rows.filter((r) => r.recovery === "YES"),
  rows,
};
const outPath = path.join(root, "reports", "pre-crash-backup-compare-20261001.json");
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
console.log(JSON.stringify({ backup, recoveryNeededCount: out.recoveryNeeded.length, recoveryNeeded: out.recoveryNeeded }, null, 2));
