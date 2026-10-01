#!/usr/bin/env node
/**
 * Static HTML/JS asset audit — fail if referenced local JS/CSS is missing.
 * Catches the AI Demand Admin class of failure (HTML refs never-committed assets).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const publicDir = path.join(root, "public");

const PAGE_GLOBS = [
  "public/**/*.html",
  "public/app/**/*.html",
];

function walk(dir, pred, out = []) {
  if (!fs.existsSync(dir)) return out;
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) {
      if (ent.name === "node_modules" || ent.name === ".git") continue;
      walk(p, pred, out);
    } else if (pred(p)) out.push(p);
  }
  return out;
}

/** Paths served dynamically by Express (not static files on disk). */
const DYNAMIC_PUBLIC_ROUTES = new Set([
  "/js/generated/operator-match-scoring-config.js",
]);

function resolvePublicRef(ref, fromFile) {
  const clean = String(ref || "").trim().split(/[?#]/)[0];
  if (!clean) return null;
  if (/^(https?:|data:|mailto:|javascript:)/i.test(clean)) return null;
  if (clean.startsWith("//")) return null;
  // Absolute from public root
  if (clean.startsWith("/")) {
    return path.join(publicDir, clean.replace(/^\//, ""));
  }
  // Some HTML mistakenly uses repo-root style paths like public/marketing/...
  if (clean.replace(/\\/g, "/").startsWith("public/")) {
    return path.join(root, clean);
  }
  // Relative to HTML file
  return path.resolve(path.dirname(fromFile), clean);
}

const htmlFiles = walk(publicDir, (p) => p.endsWith(".html"));
const missing = [];
const checked = [];

const attrRe =
  /<(?:script|link)\b[^>]*?\b(?:src|href)\s*=\s*["']([^"']+)["'][^>]*>/gi;

for (const html of htmlFiles) {
  const text = fs.readFileSync(html, "utf8");
  let m;
  attrRe.lastIndex = 0;
  while ((m = attrRe.exec(text))) {
    const tag = m[0].toLowerCase();
    const ref = m[1];
    const isAsset =
      tag.includes("<script") ||
      (tag.includes("<link") &&
        (/\.css(\?|$)/i.test(ref) || /rel\s*=\s*["']stylesheet["']/i.test(tag)));
    if (!isAsset) continue;
    if (!/\.(js|mjs|css)(\?|$)/i.test(ref) && !tag.includes("stylesheet")) continue;
    const cleanRef = String(ref || "").trim().split(/[?#]/)[0];
    if (DYNAMIC_PUBLIC_ROUTES.has(cleanRef)) {
      checked.push({
        html: path.relative(root, html).replace(/\\/g, "/"),
        ref,
        resolved: "(dynamic-express-route)",
      });
      continue;
    }
    const resolved = resolvePublicRef(ref, html);
    if (!resolved) continue;
    // Only audit files under public/ or project root local paths
    const rel = path.relative(root, resolved);
    if (rel.startsWith("..")) continue;
    checked.push({ html: path.relative(root, html).replace(/\\/g, "/"), ref, resolved: rel.replace(/\\/g, "/") });
    if (!fs.existsSync(resolved)) {
      missing.push({
        html: path.relative(root, html).replace(/\\/g, "/"),
        ref,
        expected: rel.replace(/\\/g, "/"),
      });
    }
  }
}

const report = {
  htmlFilesScanned: htmlFiles.length,
  assetRefsChecked: checked.length,
  missingCount: missing.length,
  missing,
};

const outPath = path.join(root, "reports", "startup-static-page-assets-audit.json");
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
process.exit(missing.length ? 1 : 0);
