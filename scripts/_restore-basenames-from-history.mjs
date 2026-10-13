#!/usr/bin/env node
/** Restore named basenames from Cursor History by resource match. */
import fs from "fs";
import path from "path";

const ROOT = process.cwd();
const historyRoot = path.join(process.env.APPDATA || "", "Cursor", "User", "History");
const names = process.argv.slice(2);
if (!names.length) process.exit(1);

function walk(d, depth = 0) {
  if (depth > 3) return [];
  let ents;
  try {
    ents = fs.readdirSync(d, { withFileTypes: true });
  } catch {
    return [];
  }
  const out = [];
  for (const e of ents) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) out.push(...walk(p, depth + 1));
    else if (e.name === "entries.json") out.push(p);
  }
  return out;
}

const entriesFiles = walk(historyRoot);
for (const name of names) {
  let restored = false;
  for (const ef of entriesFiles) {
    let j;
    try {
      j = JSON.parse(fs.readFileSync(ef, "utf8"));
    } catch {
      continue;
    }
    const resource = String(j.resource || "");
    if (!resource.includes(name)) continue;
    if (!/deal-capture-proxy/i.test(resource)) continue;
    const sorted = [...(j.entries || [])].sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
    const dir = path.dirname(ef);
    for (const ent of sorted) {
      const src = path.join(dir, ent.id);
      if (!fs.existsSync(src)) continue;
      // Derive dest from resource
      const decoded = decodeURIComponent(resource.replace(/^file:\/\/\//, "").replace(/^\/([A-Za-z]%3A)/, (_, x) => x.replace("%3A", ":")));
      let rel;
      const m = decoded.replace(/\\/g, "/").match(/deal-capture-proxy\/(.+)$/i);
      if (m) rel = m[1];
      else rel = path.join("lib/hotel-intelligence/contact-intelligence", name);
      const dest = path.join(ROOT, rel);
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      fs.copyFileSync(src, dest);
      console.log("restored", rel, "from", ent.id, "bytes", fs.statSync(dest).size);
      restored = true;
      break;
    }
    if (restored) break;
  }
  if (!restored) console.log("NOT_FOUND", name);
}
