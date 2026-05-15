/**
 * Emit fixtures/brand-basics-radisson-brand-on-a-page.csv from
 * fixtures/brand-basics-radisson-brand-on-a-page.json (single source of truth).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const src = path.join(root, "fixtures", "brand-basics-radisson-brand-on-a-page.json");
const dest = path.join(root, "fixtures", "brand-basics-radisson-brand-on-a-page.csv");

function q(s) {
  return '"' + String(s).replace(/"/g, '""') + '"';
}

const spec = JSON.parse(fs.readFileSync(src, "utf8"));
const fields = spec.fields || {};
const pairs = [["Field Name", "Cell text (paste into Brand Setup - Brand Basics for Radisson)"]];
for (const [k, v] of Object.entries(fields)) {
  pairs.push([k, v]);
}
if (spec.suggestedTargetGuestSegments) {
  pairs.push([
    "Target Guest Segments (not sent via API if multi-select)",
    spec.suggestedTargetGuestSegments,
  ]);
}
const out = pairs.map((r) => [q(r[0]), q(r[1])].join(",")).join("\r\n");
fs.writeFileSync(dest, out, "utf8");
console.log("Wrote", pairs.length - 1, "data rows to", path.relative(root, dest));
