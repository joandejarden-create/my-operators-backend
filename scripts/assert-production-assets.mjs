#!/usr/bin/env node
/**
 * Hard predeploy gate: required Dealality product surfaces must exist in the
 * deploy tree. Prevents ADP-only / HI-only Railway uploads from wiping peers.
 *
 * Usage:
 *   node scripts/assert-production-assets.mjs
 *   node scripts/assert-production-assets.mjs --surface=HOTEL_INTELLIGENCE
 *   node scripts/assert-production-assets.mjs --check-main
 *   node scripts/assert-production-assets.mjs --json
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifestPath = path.join(root, "config", "production-required-assets.json");
const prevManifestPath = path.join(
  root,
  "reports",
  "deployments",
  "latest-production-asset-snapshot.json"
);

const args = new Set(process.argv.slice(2));
const surfaceFilter = [...args]
  .find((a) => a.startsWith("--surface="))
  ?.slice("--surface=".length);
const checkMain = args.has("--check-main");
const asJson = args.has("--json");
const writeSnapshot = args.has("--write-snapshot");

function fail(code, message, details = []) {
  const payload = { ok: false, code, message, details };
  if (asJson) {
    console.log(JSON.stringify(payload, null, 2));
  } else {
    console.error(`FAIL ${code}: ${message}`);
    for (const d of details) console.error(`  - ${d}`);
  }
  process.exit(1);
}

function loadManifest() {
  if (!fs.existsSync(manifestPath)) {
    fail("MISSING_REQUIRED_FILE", "config/production-required-assets.json missing");
  }
  return JSON.parse(fs.readFileSync(manifestPath, "utf8"));
}

function fileExists(rel) {
  return fs.existsSync(path.join(root, rel));
}

function readRailwayIgnorePatterns() {
  const p = path.join(root, ".railwayignore");
  if (!fs.existsSync(p)) return [];
  return fs
    .readFileSync(p, "utf8")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith("#"));
}

/** Conservative: fail if a required path is explicitly ignored (no negation support beyond common !patterns). */
function isExplicitlyIgnored(rel, patterns) {
  const norm = rel.replace(/\\/g, "/");
  for (const pat of patterns) {
    if (pat.startsWith("!")) continue;
    if (pat === norm || pat === `${norm}/`) return true;
    if (pat.endsWith("/*") && norm.startsWith(pat.slice(0, -1))) return true;
    if (pat.endsWith("/") && norm.startsWith(pat)) return true;
    if (pat === "public" || pat === "public/") return true;
  }
  return false;
}

function parseLocalAssetRefs(htmlRel) {
  const abs = path.join(root, htmlRel);
  if (!fs.existsSync(abs)) return { missingHtml: htmlRel, refs: [] };
  const html = fs.readFileSync(abs, "utf8");
  const refs = [];
  const re = /(?:href|src)=["'](\/[^"'?#]+)/gi;
  let m;
  while ((m = re.exec(html))) {
    const urlPath = m[1];
    if (urlPath.startsWith("//")) continue;
    // Skip legacy / documentation-only links that are not deploy blockers
    if (/hotel-intelligence-reference\.html$/i.test(urlPath)) continue;
    refs.push(urlPath);
  }
  return { missingHtml: null, refs };
}

function localPathForUrl(urlPath) {
  const clean = urlPath.split("?")[0];
  if (clean.startsWith("/js/") || clean.startsWith("/css/") || clean.startsWith("/")) {
    return path.join("public", clean.replace(/^\//, ""));
  }
  return null;
}

function gitCatExists(ref, rel) {
  try {
    execSync(`git cat-file -e ${ref}:${rel}`, { cwd: root, stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

const manifest = loadManifest();
const surfaces = Object.entries(manifest.surfaces || {}).filter(([id]) =>
  surfaceFilter ? id === surfaceFilter : true
);

if (!surfaces.length) {
  fail("MISSING_REQUIRED_FILE", `Unknown surface filter: ${surfaceFilter}`);
}

const serverJs = fileExists("server.js")
  ? fs.readFileSync(path.join(root, "server.js"), "utf8")
  : "";
const ignorePatterns = readRailwayIgnorePatterns();

const errors = [];
const warnings = [];
const presentFiles = [];
const mainStatus = {};

for (const [surfaceId, surface] of surfaces) {
  for (const rel of surface.required_files || []) {
    if (!fileExists(rel)) {
      errors.push({
        code: "MISSING_REQUIRED_FILE",
        surface: surfaceId,
        detail: rel,
      });
    } else {
      presentFiles.push(rel);
      if (isExplicitlyIgnored(rel, ignorePatterns)) {
        errors.push({
          code: "MISSING_STATIC_ASSET",
          surface: surfaceId,
          detail: `${rel} is excluded by .railwayignore`,
        });
      }
    }
    if (checkMain) {
      const onMain = gitCatExists("origin/main", rel);
      mainStatus[rel] = onMain ? "ON_MAIN" : "NOT_ON_MAIN";
      if (!onMain) {
        warnings.push(`${surfaceId}: ${rel} NOT_ON_MAIN (PR #40 / merge required for HI)`);
      }
    }
  }

  for (const marker of surface.required_server_route_markers || []) {
    if (!serverJs.includes(marker)) {
      errors.push({
        code: "MISSING_SERVER_ROUTE",
        surface: surfaceId,
        detail: `server.js missing marker: ${marker}`,
      });
    }
  }

  for (const api of surface.required_api_routes || []) {
    // Marker check: strip param segments for substring presence
    const marker = api
      .replace(/:[^/]+/g, "")
      .replace(/\/+$/, "")
      .replace(/\/{2,}/g, "/");
    if (marker && !serverJs.includes(marker) && !serverJs.includes(api.split("/:")[0])) {
      // softer: check first two path segments under /api/
      const parts = api.split("/").filter(Boolean);
      const prefix = `/${parts.slice(0, 3).join("/")}`;
      if (!serverJs.includes(prefix)) {
        errors.push({
          code: "MISSING_API_ROUTE",
          surface: surfaceId,
          detail: api,
        });
      }
    }
  }

  for (const htmlRel of surface.html_entrypoints_for_static_refs || []) {
    const { missingHtml, refs } = parseLocalAssetRefs(htmlRel);
    if (missingHtml) {
      errors.push({
        code: "MISSING_REQUIRED_FILE",
        surface: surfaceId,
        detail: missingHtml,
      });
      continue;
    }
    for (const urlPath of refs) {
      const rel = localPathForUrl(urlPath);
      if (!rel) continue;
      if (!fileExists(rel)) {
        const code = urlPath.includes(".css")
          ? "MISSING_CSS"
          : urlPath.includes(".js")
            ? "MISSING_STATIC_ASSET"
            : "MISSING_STATIC_ASSET";
        errors.push({
          code,
          surface: surfaceId,
          detail: `${htmlRel} → ${urlPath} (expected ${rel})`,
        });
      }
    }
  }
}

// Coexistence: when asserting full tree, HI + ADP must both be present
if (!surfaceFilter) {
  const hiFile = "public/hotel-intelligence-golden-demo.html";
  const adpFile = "public/owner-ai-demand-share.html";
  if (!fileExists(hiFile)) {
    errors.push({
      code: "MISSING_SHARE_PAGE",
      surface: "HOTEL_INTELLIGENCE",
      detail: hiFile,
    });
  }
  if (!fileExists(adpFile)) {
    errors.push({
      code: "MISSING_SHARE_PAGE",
      surface: "ADP",
      detail: adpFile,
    });
  }
  if (!fileExists("public/css/hotel-intelligence-research-center.css")) {
    errors.push({
      code: "MISSING_CSS",
      surface: "HOTEL_INTELLIGENCE",
      detail: "public/css/hotel-intelligence-research-center.css",
    });
  }
}

// Surface removal vs previous successful deploy snapshot
if (fs.existsSync(prevManifestPath) && !surfaceFilter) {
  try {
    const prev = JSON.parse(fs.readFileSync(prevManifestPath, "utf8"));
    const prevFiles = new Set(prev.presentFiles || []);
    const now = new Set(presentFiles);
    const removed = [...prevFiles].filter((f) => !now.has(f));
    const criticalRemoved = removed.filter(
      (f) =>
        f.includes("hotel-intelligence") ||
        f.includes("owner-ai-demand") ||
        f.includes("brand-explorer-share") ||
        f.includes("operator-explorer-share")
    );
    if (criticalRemoved.length) {
      errors.push({
        code: "PRODUCTION_SURFACE_REMOVAL_DETECTED",
        surface: "COMPARE",
        detail: criticalRemoved.join(", "),
      });
    }
  } catch (err) {
    warnings.push(`Could not compare prior snapshot: ${err.message}`);
  }
}

if (writeSnapshot && !errors.length) {
  fs.mkdirSync(path.dirname(prevManifestPath), { recursive: true });
  fs.writeFileSync(
    prevManifestPath,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        manifestVersion: manifest.version,
        presentFiles: [...new Set(presentFiles)].sort(),
      },
      null,
      2
    )
  );
}

if (errors.length) {
  const byCode = {};
  for (const e of errors) {
    byCode[e.code] = byCode[e.code] || [];
    byCode[e.code].push(`[${e.surface}] ${e.detail}`);
  }
  if (asJson) {
    console.log(
      JSON.stringify({ ok: false, errors, warnings, mainStatus }, null, 2)
    );
  } else {
    for (const [code, details] of Object.entries(byCode)) {
      console.error(`FAIL ${code}:`);
      for (const d of details) console.error(`  - ${d}`);
    }
    for (const w of warnings) console.warn(`WARN ${w}`);
    console.error(
      "\nDo not railway-up. Fix the deploy tree or merge PR #40 / use a complete product tree."
    );
  }
  process.exit(1);
}

const result = {
  ok: true,
  gate: "PRODUCTION_REQUIRED_ASSET_MANIFEST",
  surfaces: surfaces.map(([id]) => id),
  fileCount: presentFiles.length,
  warnings,
  mainStatus: checkMain ? mainStatus : undefined,
};

if (asJson) {
  console.log(JSON.stringify(result, null, 2));
} else {
  console.log("PASS assert:production-assets");
  console.log(`  surfaces: ${result.surfaces.join(", ")}`);
  console.log(`  files ok: ${result.fileCount}`);
  for (const w of warnings) console.warn(`  WARN ${w}`);
  if (checkMain) {
    const missingMain = Object.entries(mainStatus).filter(([, s]) => s !== "ON_MAIN");
    if (missingMain.length) {
      console.log(
        `  MAIN_PRODUCTION_ASSET_COMPLETENESS: FAIL (${missingMain.length} not on origin/main) — merge PR #40`
      );
    } else {
      console.log("  MAIN_PRODUCTION_ASSET_COMPLETENESS: PASS");
    }
  }
}

process.exit(0);
