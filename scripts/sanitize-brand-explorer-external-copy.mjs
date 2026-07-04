/**
 * Strip internal ETL/editor phrasing from Brand Explorer presentation fixtures.
 *
 *   node scripts/sanitize-brand-explorer-external-copy.mjs
 *   node scripts/sanitize-brand-explorer-external-copy.mjs --dry-run
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { sanitizeExternalCopy } from "../lib/external-owner-copy.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES_DIR = path.join(__dirname, "../fixtures");
const dryRun = process.argv.includes("--dry-run");

const SKIP_KEYS = new Set(["instructions"]);

function sanitizeDeep(value, key = "") {
  if (typeof value === "string") {
    if (SKIP_KEYS.has(key)) return value;
    return sanitizeExternalCopy(value);
  }
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeDeep(item, key));
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = sanitizeDeep(v, k);
    }
    return out;
  }
  return value;
}

const files = fs
  .readdirSync(FIXTURES_DIR)
  .filter((f) => f.startsWith("brand-explorer-presentation") && f.endsWith(".json"))
  .sort();

let changed = 0;
for (const file of files) {
  const full = path.join(FIXTURES_DIR, file);
  const raw = fs.readFileSync(full, "utf8");
  const data = JSON.parse(raw);
  const next = sanitizeDeep(data);
  const out = JSON.stringify(next, null, 2) + "\n";
  if (out !== raw && !raw.endsWith("\n")) {
    // normalize trailing newline compare
  }
  if (out.replace(/\r\n/g, "\n") !== raw.replace(/\r\n/g, "\n")) {
    changed++;
    if (!dryRun) fs.writeFileSync(full, out, "utf8");
    console.log(dryRun ? "[dry-run] would update" : "updated", file);
  }
}

console.log(
  dryRun
    ? `Dry run: ${changed} of ${files.length} fixture(s) would change.`
    : `Done: ${changed} of ${files.length} fixture(s) updated.`
);
