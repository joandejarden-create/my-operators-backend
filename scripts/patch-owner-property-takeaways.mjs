/**
 * Refresh owner-facing takeaways in Radisson presentation fixtures.
 * Usage: node scripts/patch-owner-property-takeaways.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { OWNER_PROPERTY_TAKEAWAYS } from "./lib/owner-property-takeaways.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const fixturePaths = [
  "fixtures/brand-explorer-presentation-radisson-blu-footprint-openings.json",
  "fixtures/brand-explorer-presentation-radisson-blu-case-studies.json",
  "fixtures/brand-explorer-presentation-radisson-footprint-openings.json",
  "fixtures/brand-explorer-presentation-radisson-case-studies.json",
  "fixtures/brand-explorer-presentation-radisson-paramaribo-opening.json",
  "fixtures/brand-explorer-presentation-radisson-blu-full.json",
];

function patchBodyTakeaway(body, newTakeaway) {
  if (!body || !newTakeaway) return body;
  const parts = String(body)
    .split(/\n\n+/)
    .map((p) => p.trim())
    .filter(Boolean);
  const urlParts = parts.filter((p) => /^https?:\/\//i.test(p));
  const nonUrl = parts.filter((p) => !/^https?:\/\//i.test(p));
  if (nonUrl.length >= 6) {
    nonUrl[5] = newTakeaway;
    return [...nonUrl, ...urlParts].join("\n\n");
  }
  if (nonUrl.length >= 3) {
    return [...nonUrl, newTakeaway, ...urlParts].join("\n\n");
  }
  return [...parts.filter((p) => !/^https?:\/\//i.test(p)), newTakeaway, ...urlParts].join("\n\n");
}

function patchFixture(relPath) {
  const full = path.join(root, relPath);
  if (!fs.existsSync(full)) {
    console.warn("skip missing", relPath);
    return 0;
  }
  const data = JSON.parse(fs.readFileSync(full, "utf8"));
  let n = 0;
  for (const row of data.rows || []) {
    const t = row.title?.trim();
    const patch = t && OWNER_PROPERTY_TAKEAWAYS[t];
    if (!patch) continue;
    if (patch.interpretation) {
      row.caseSummaryInterpretation = patch.interpretation;
      n++;
    }
    if (patch.bodyTakeaway && row.body) {
      row.body = patchBodyTakeaway(row.body, patch.bodyTakeaway);
    }
  }
  fs.writeFileSync(full, JSON.stringify(data, null, 2) + "\n");
  console.log(relPath, "patched", n, "rows");
  return n;
}

let total = 0;
for (const rel of fixturePaths) total += patchFixture(rel);
console.log("fixture rows updated:", total);
