#!/usr/bin/env node
import { readFileSync, writeFileSync } from "node:fs";

const html = readFileSync("reports/marriott-poplc-rooms-sample.html", "utf8");
const m = html.match(/window\.__INITIAL_JSON__\s*=\s*(\{[\s\S]*?\});?\s*<\/script>/);
if (!m) {
  console.error("no __INITIAL_JSON__");
  process.exit(1);
}
const data = JSON.parse(m[1]);
writeFileSync("reports/marriott-poplc-initial-json.json", JSON.stringify(data, null, 2));

function walk(obj, path = "", out = []) {
  if (!obj || typeof obj !== "object") return out;
  if (Array.isArray(obj)) {
    obj.forEach((v, i) => walk(v, `${path}[${i}]`, out));
    return out;
  }
  for (const [k, v] of Object.entries(obj)) {
    const p = path ? `${path}.${k}` : k;
    if (/amenit|overview|description|highSpeed|breakfast|spa/i.test(k)) {
      out.push({ path: p, preview: typeof v === "string" ? v.slice(0, 120) : Array.isArray(v) ? `array(${v.length})` : typeof v });
    }
    walk(v, p, out);
  }
  return out;
}

const hits = walk(data);
console.log("hits", hits.length);
for (const h of hits.slice(0, 40)) console.log(h.path, h.preview);

// top-level keys
console.log("\ntop keys", Object.keys(data));
