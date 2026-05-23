#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = fs.readFileSync(path.join(ROOT, "public", "new-deal-setup.html"), "utf8");
const re = /<select[^>]*name="([^"]+)"[^>]*>([\s\S]*?)<\/select>/gi;
const out = {};
let m;
while ((m = re.exec(html))) {
  const name = m[1];
  const opts = [...m[2].matchAll(/<option value="([^"]*)"/g)]
    .map((x) => x[1])
    .filter((v) => v);
  if (opts.length) out[name] = opts;
}
const outPath = path.join(ROOT, "lib", "deal-setup-form-options.json");
fs.writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n", "utf8");
console.log("Wrote", outPath, Object.keys(out).length, "fields");
