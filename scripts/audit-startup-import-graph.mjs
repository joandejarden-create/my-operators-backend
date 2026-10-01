#!/usr/bin/env node
/**
 * Static ESM import graph audit for server.js — finds missing local modules
 * without executing side-effectful server boot.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath, pathToFileURL } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const entry = process.argv[2] || "server.js";
const visited = new Set();
const missing = [];
const resolved = [];
const errors = [];

const IMPORT_RE =
  /(?:import|export)\s+(?:[\s\S]*?\s+from\s+)?['"](\.[^'"]+)['"]|import\s*\(\s*['"](\.[^'"]+)['"]\s*\)/g;

function resolveImport(fromFile, spec) {
  const base = path.resolve(path.dirname(fromFile), spec);
  const candidates = [
    base,
    base + ".js",
    base + ".mjs",
    base + ".cjs",
    base + ".json",
    path.join(base, "index.js"),
    path.join(base, "index.mjs"),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c) && fs.statSync(c).isFile()) return c;
  }
  return null;
}

function walk(relOrAbs) {
  const abs = path.isAbsolute(relOrAbs) ? relOrAbs : path.join(ROOT, relOrAbs);
  const key = abs.toLowerCase();
  if (visited.has(key)) return;
  visited.add(key);
  if (!fs.existsSync(abs)) {
    missing.push({ file: path.relative(ROOT, abs), reason: "entry_missing" });
    return;
  }
  let src;
  try {
    src = fs.readFileSync(abs, "utf8");
  } catch (e) {
    errors.push({ file: path.relative(ROOT, abs), error: String(e.message || e) });
    return;
  }
  // skip huge non-js
  if (!/\.(m?js|cjs)$/i.test(abs)) return;

  let m;
  IMPORT_RE.lastIndex = 0;
  while ((m = IMPORT_RE.exec(src))) {
    const spec = m[1] || m[2];
    if (!spec || !spec.startsWith(".")) continue;
    const target = resolveImport(abs, spec);
    if (!target) {
      missing.push({
        from: path.relative(ROOT, abs).replace(/\\/g, "/"),
        spec,
        tried: path.relative(ROOT, path.resolve(path.dirname(abs), spec)).replace(/\\/g, "/"),
      });
      continue;
    }
    const rel = path.relative(ROOT, target).replace(/\\/g, "/");
    if (!rel.startsWith("..") && !rel.includes("node_modules")) {
      resolved.push({ from: path.relative(ROOT, abs).replace(/\\/g, "/"), spec, to: rel });
      walk(target);
    }
  }
}

walk(entry);
const uniqMissing = [];
const seen = new Set();
for (const m of missing) {
  const k = `${m.from}|${m.spec}`;
  if (seen.has(k)) continue;
  seen.add(k);
  uniqMissing.push(m);
}

console.log(
  JSON.stringify(
    {
      entry,
      filesVisited: visited.size,
      missingCount: uniqMissing.length,
      missing: uniqMissing.slice(0, 200),
    },
    null,
    2
  )
);
