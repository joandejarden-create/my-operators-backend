/**
 * Rewrite fixtures/brand-basics-from-choice-materials/*.json for external owner voice.
 *   node scripts/sanitize-choice-basics-fixtures.mjs
 *   node scripts/sanitize-choice-basics-fixtures.mjs --dry-run
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { sanitizeFieldMap } from "./lib/external-owner-voice.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DIR = path.resolve(__dirname, "../fixtures/brand-basics-from-choice-materials");
const dryRun = process.argv.includes("--dry-run");

const files = fs.readdirSync(DIR).filter((f) => f.endsWith(".json"));
let changed = 0;

for (const file of files) {
  const full = path.join(DIR, file);
  const prevText = fs.readFileSync(full, "utf8");
  const raw = JSON.parse(prevText);
  if (raw.fields && typeof raw.fields === "object") {
    raw.fields = sanitizeFieldMap(raw.fields);
  }
  delete raw.comment;
  const nextText = `${JSON.stringify(raw, null, 2)}\n`;
  if (nextText !== prevText) {
    changed++;
    if (!dryRun) fs.writeFileSync(full, nextText);
    console.log(dryRun ? "[dry-run] would update" : "updated", file);
  }
}

console.log(`${dryRun ? "Would update" : "Updated"} ${changed} file(s).`);
