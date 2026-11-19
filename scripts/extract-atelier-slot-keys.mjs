#!/usr/bin/env node
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const src = readFileSync(join(ROOT, "public/js/brand-explorer-atelier-from-api.js"), "utf8");
const slots = new Set();
const patterns = [
  /explorerBlocksForSlot\(\s*brand\s*,\s*'([^']+)'/g,
  /explorerBlocksForSlot\(\s*brand\s*,\s*"([^"]+)"/g,
  /explorerMergedBody\(\s*brand\s*,\s*'([^']+)'/g,
  /explorerMergedBody\(\s*brand\s*,\s*"([^"]+)"/g,
  /explorerFirstBlock\(\s*brand\s*,\s*'([^']+)'/g,
  /explorerFirstBlock\(\s*brand\s*,\s*"([^"]+)"/g,
  /explorerCardRowsForSlot\(\s*brand\s*,\s*'([^']+)'/g,
  /explorerCardRowsForSlot\(\s*brand\s*,\s*"([^"]+)"/g,
  /explorerPresentationLine\(\s*brand\s*,\s*'([^']+)'/g,
  /explorerPresentationLine\(\s*brand\s*,\s*"([^"]+)"/g,
];
for (const re of patterns) {
  for (const m of src.matchAll(re)) slots.add(m[1]);
}
// Known production prefixes from Curio baseline / slots doc
const doc = readFileSync(join(ROOT, "docs/brand-explorer-presentation-slots.md"), "utf8");
for (const m of doc.matchAll(/`([a-zA-Z][a-zA-Z0-9_.]+)`/g)) {
  const s = m[1];
  if (
    /^(overview|operations|valueOwners|standards|commercial|economics|loyalty|footprint|materials|insight|hero|snapshot|positioning|branded_residences)\./.test(
      s,
    )
  ) {
    slots.add(s.replace(/\.\.\.|…/g, ""));
  }
}

const out = {
  extractedAt: new Date().toISOString(),
  source: "public/js/brand-explorer-atelier-from-api.js + docs/brand-explorer-presentation-slots.md",
  slotCount: slots.size,
  slots: [...slots].sort(),
};
const dir = join(ROOT, "reports/brand-explorer/phase2d-curio-quality-recovery");
mkdirSync(dir, { recursive: true });
writeFileSync(join(dir, "atelier-referenced-slots.json"), JSON.stringify(out, null, 2));
console.log(JSON.stringify({ slotCount: out.slotCount, out: join(dir, "atelier-referenced-slots.json") }, null, 2));
