#!/usr/bin/env node
/**
 * Restore files whose Cursor History resource path contains the given
 * relative path fragment(s). Exact path-aware (not basename-only).
 */
import fs from "fs";
import path from "path";

const ROOT = process.cwd();
const historyRoot = path.join(process.env.APPDATA || "", "Cursor", "User", "History");
const fragments = process.argv.slice(2).map((s) => s.replace(/\\/g, "/"));
if (!fragments.length) {
  console.error("usage: fragments...");
  process.exit(1);
}

function walkEntries(d, depth = 0, acc = []) {
  if (depth > 3) return acc;
  let ents;
  try {
    ents = fs.readdirSync(d, { withFileTypes: true });
  } catch {
    return acc;
  }
  for (const e of ents) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) walkEntries(p, depth + 1, acc);
    else if (e.name === "entries.json") acc.push(p);
  }
  return acc;
}

function decodeResource(resource) {
  let s = String(resource || "");
  s = s.replace(/^file:\/\/\//, "");
  s = decodeURIComponent(s);
  return s.replace(/\\/g, "/");
}

const entriesFiles = walkEntries(historyRoot);
let restored = 0;
for (const ef of entriesFiles) {
  let j;
  try {
    j = JSON.parse(fs.readFileSync(ef, "utf8"));
  } catch {
    continue;
  }
  const decoded = decodeResource(j.resource);
  if (!/deal-capture-proxy/i.test(decoded)) continue;
  const m = decoded.match(/deal-capture-proxy\/(.+)$/i);
  if (!m) continue;
  const rel = m[1];
  if (!fragments.some((f) => rel.includes(f))) continue;
  const dest = path.join(ROOT, rel);
  if (fs.existsSync(dest) && fs.statSync(dest).size > 0) continue;
  const sorted = [...(j.entries || [])].sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
  const dir = path.dirname(ef);
  for (const ent of sorted) {
    const src = path.join(dir, ent.id);
    if (!fs.existsSync(src)) continue;
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    fs.copyFileSync(src, dest);
    console.log("restored", rel, fs.statSync(dest).size);
    restored++;
    break;
  }
}
console.log("restored_count", restored);
