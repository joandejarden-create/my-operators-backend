/**
 * Scan local Brand Explorer fixture JSON for internal/editorial phrasing.
 *
 *   node scripts/audit-brand-explorer-fixtures-owner-voice.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { scanTextForInternalVoice } from "./lib/owner-voice-phrases.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIXTURES = path.join(ROOT, "fixtures");

function collectStrings(val, out = []) {
  if (typeof val === "string" && val.trim()) out.push(val);
  else if (Array.isArray(val)) val.forEach((v) => collectStrings(v, out));
  else if (val && typeof val === "object") Object.values(val).forEach((v) => collectStrings(v, out));
  return out;
}

function main() {
  const files = fs
    .readdirSync(FIXTURES)
    .filter((f) => f.startsWith("brand-explorer") && f.endsWith(".json"))
    .sort();

  let totalHits = 0;
  for (const file of files) {
    const full = path.join(FIXTURES, file);
    const data = JSON.parse(fs.readFileSync(full, "utf8"));
    const rows = data.rows || [];
    const fileHits = [];
    for (const row of rows) {
      const texts = collectStrings(row);
      for (const text of texts) {
        const hits = scanTextForInternalVoice(text);
        if (!hits.length) continue;
        fileHits.push({
          slot: row.slotKey || row.SlotKey || "?",
          title: row.title || row.Title || "",
          ids: hits.map((h) => h.id),
          sample: text.slice(0, 100).replace(/\s+/g, " "),
        });
      }
    }
    if (!fileHits.length) continue;
    totalHits += fileHits.length;
    console.log(`\n${file} (${fileHits.length} hit(s))`);
    for (const h of fileHits.slice(0, 15)) {
      console.log(`  ${h.slot}${h.title ? ` · ${h.title}` : ""}: ${h.ids.join(", ")}`);
      console.log(`    ${h.sample}…`);
    }
    if (fileHits.length > 15) console.log(`  … +${fileHits.length - 15} more`);
  }

  if (!totalHits) console.log("\nNo internal-voice hits in brand-explorer fixture JSON files.");
  else {
    console.log(`\nTotal: ${totalHits} field(s) with internal phrasing across fixtures.`);
    process.exitCode = 1;
  }
}

main();
