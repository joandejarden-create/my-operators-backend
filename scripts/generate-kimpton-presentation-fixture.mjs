/**
 * Generate Kimpton Brand Explorer presentation fixture from Comfort Inn template
 * with full Kimpton/IHG slot overrides (no leftover Choice Hotels copy).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  KIMPTON_SLOT_OVERRIDES,
  KIMPTON_FORBIDDEN_PATTERN,
  overrideKey,
  applyKimptonGlobalReplacements,
} from "../lib/kimpton-brand-explorer-presentation-overrides.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const SCENARIO_TITLE_FIXES = {
  "overview.scenario.1": "Urban Lifestyle Conversion",
  "overview.scenario.2": "Gateway New-Build or Adaptive Reuse",
  "overview.scenario.3": "Portfolio Lifestyle Standardization",
  "overview.bestAt.1": "Gateway Urban Lifestyle",
  "overview.bestAt.2": "Conversion Repositioning",
  "overview.bestAt.3": "IHG Loyalty & Distribution",
  "overview.proof.1": "Pet-Friendly Lifestyle",
  "overview.proof.2": "60+ Americas Hotels",
  "overview.proof.3": "IHG Integration Since 2015",
  "overview.proof.4": "Wine Hour & Social F&B",
  "overview.proof.5": "IHG One Rewards®",
  "overview.proof.6": "Lifestyle Operators",
  "materials.caseStudy": "Kimpton Hotels — lifestyle & IHG scale",
};

function walkApplyGlobal(v) {
  if (typeof v === "string") return applyKimptonGlobalReplacements(v);
  if (Array.isArray(v)) return v.map(walkApplyGlobal);
  if (v && typeof v === "object") {
    const o = {};
    for (const [k, val] of Object.entries(v)) o[k] = walkApplyGlobal(val);
    return o;
  }
  return v;
}

function applyOverride(row) {
  const override =
    KIMPTON_SLOT_OVERRIDES[overrideKey(row.slotKey, row.title, row.sort)] ??
    KIMPTON_SLOT_OVERRIDES[row.slotKey];

  if (!override) return;

  if (typeof override === "string") {
    row.body = override;
    return;
  }

  if (override.title) row.title = override.title;
  if (override.body !== undefined) row.body = override.body;
  for (const field of [
    "caseSummaryOverview",
    "caseSummaryOwnerObjective",
    "caseSummaryBrandRelevance",
    "caseSummaryInterpretation",
    "caseSummaryTags",
  ]) {
    if (override[field] !== undefined) row[field] = override[field];
  }
}

const src = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "fixtures", "brand-explorer-presentation-comfort-inn-suites-full.json"),
    "utf8"
  )
);

const out = walkApplyGlobal(structuredClone(src));
out.targetBrandBasicsName = "Kimpton Hotels";
out.brandNameFallback = "Kimpton Hotels";
out.instructions =
  'Apply: node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name "Kimpton Hotels" --fixture fixtures/brand-explorer-presentation-kimpton-full.json --replace';

for (const row of out.rows) {
  if (SCENARIO_TITLE_FIXES[row.slotKey]) {
    row.title = SCENARIO_TITLE_FIXES[row.slotKey];
  }
  applyOverride(row);
  row.body = applyKimptonGlobalReplacements(row.body || "");
  if (row.title) row.title = applyKimptonGlobalReplacements(row.title);
  for (const field of [
    "caseSummaryOverview",
    "caseSummaryOwnerObjective",
    "caseSummaryBrandRelevance",
    "caseSummaryInterpretation",
    "caseSummaryTags",
  ]) {
    if (row[field]) row[field] = applyKimptonGlobalReplacements(row[field]);
  }
}

const violations = [];
for (const row of out.rows) {
  const blob = JSON.stringify(row);
  if (KIMPTON_FORBIDDEN_PATTERN.test(blob)) {
    violations.push(`${row.slotKey}${row.title ? ` :: ${row.title}` : ""}`);
  }
}

if (violations.length) {
  console.error("Forbidden Choice/Comfort terms remain in slots:");
  for (const v of violations) console.error("  -", v);
  process.exit(1);
}

const dest = path.join(ROOT, "fixtures", "brand-explorer-presentation-kimpton-full.json");
fs.writeFileSync(dest, JSON.stringify(out, null, 2));
console.log("Wrote", dest, "| rows:", out.rows.length, "| violations: 0");
