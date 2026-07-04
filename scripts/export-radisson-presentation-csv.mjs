/**
 * One-off helper: emit fixtures/brand-explorer-presentation-radisson-airtable.csv
 * from fixtures/brand-explorer-presentation-radisson.example.json for copy-paste into Airtable.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const src = path.join(root, "fixtures", "brand-explorer-presentation-radisson.example.json");
const dest = path.join(root, "fixtures", "brand-explorer-presentation-radisson-airtable.csv");

const j = JSON.parse(fs.readFileSync(src, "utf8"));

function q(s) {
  if (s == null) s = "";
  return '"' + String(s).replace(/"/g, '""') + '"';
}

const lines = ["Slot Key,Title,Body,Sort Order"];
for (const r of j.rows || []) {
  lines.push([q(r.slotKey), q(r.title ?? ""), q(r.body ?? ""), r.sort ?? 0].join(","));
}
fs.writeFileSync(dest, lines.join("\r\n"), "utf8");
console.log("Wrote", lines.length - 1, "rows to", path.relative(root, dest));
