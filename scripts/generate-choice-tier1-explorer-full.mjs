/**
 * Generate full Brand Explorer presentation fixtures for all Tier 1 CHI brands.
 *
 *   node scripts/generate-choice-tier1-explorer-full.mjs
 *   node scripts/generate-choice-tier1-explorer-full.mjs --brand "Comfort Inn & Suites"
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { TIER1_BRANDS } from "./lib/choice-tier1-explorer-profiles.mjs";
import { buildFixture } from "./lib/choice-explorer-full-builder.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures");

function parseArgs() {
  const brand = process.argv.find((a, i) => process.argv[i - 1] === "--brand") || "";
  return { brand: String(brand).trim() };
}

const { brand: onlyBrand } = parseArgs();
const brands = onlyBrand ? TIER1_BRANDS.filter((b) => b.name === onlyBrand) : TIER1_BRANDS;

if (onlyBrand && brands.length === 0) {
  console.error(`Unknown brand: ${onlyBrand}`);
  process.exit(1);
}

for (const p of brands) {
  const fixture = buildFixture(p);
  const outPath = path.join(FIXTURES, `brand-explorer-presentation-${p.slug}-full.json`);
  fs.writeFileSync(outPath, `${JSON.stringify(fixture, null, 2)}\n`, "utf8");
  const keys = new Set(fixture.rows.map((r) => r.slotKey));
  console.log(`${p.name}: ${fixture.rows.length} rows, ${keys.size} unique slot keys → ${path.relative(ROOT, outPath)}`);
}

console.log(`\nDone. ${brands.length} fixture(s).`);
