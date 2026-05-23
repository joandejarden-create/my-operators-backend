/**
 * Propose Brand Alias Mapping rows from QA coverage CSV (review-only, no Airtable writes).
 *
 * Usage: node scripts/propose-brand-aliases-from-coverage.mjs
 *
 * Input:  reports/brand-explorer-census-coverage.csv
 * Output: reports/proposed-brand-aliases.json
 */
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { exactMatchKey } from "../lib/hotel-census/brand-alias-resolve.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const COVERAGE_PATH = join(__dirname, "..", "reports", "brand-explorer-census-coverage.csv");
const OUTPUT_PATH = join(__dirname, "..", "reports", "proposed-brand-aliases.json");

/** Never propose parent-company strings as affiliation aliases. */
const BLOCKED_ALIAS_KEYS = new Set(
  [
    "choice hotels",
    "choice hotels international",
    "marriott international",
    "marriott international inc",
    "marriott",
    "choice",
    "radisson hotel group",
  ].map((s) => s.toLowerCase())
);

/**
 * Explicit safe short-name rules only (no contains/fuzzy discovery).
 * Each entry: match canonical display name (exact or regex), emit extra alias strings.
 */
const SAFE_SHORT_RULES = [
  {
    id: "park_inn",
    match: (name) => /^Park Inn by Radisson/i.test(name),
    aliases: ["Park Inn", "Park Inn by Radisson"],
    reason: "Known safe short variants for Park Inn by Radisson display names",
  },
  {
    id: "ascend",
    match: (name) => name === "Ascend Hotel Collection",
    aliases: ["Ascend"],
    reason: "Known safe short variant: Ascend Hotel Collection → Ascend",
  },
  {
    id: "city_express",
    match: (name) => name === "City Express by Marriott",
    aliases: ["City Express"],
    reason: "Known safe short variant: City Express by Marriott → City Express",
  },
  {
    id: "choice_suffix_strip",
    match: (name) => /\s*\(Choice\)\s*$/i.test(name),
    aliases: (name) => [name.replace(/\s*\(Choice\)\s*$/i, "").trim()],
    reason: "Strip (Choice) display suffix for census Affiliation exact match",
  },
  {
    id: "radisson_red_strip",
    match: (name) => /^Radisson RED\s+\(Choice\)/i.test(name),
    aliases: ["Radisson RED"],
    reason: "Normalize double-space display name to census Radisson RED",
  },
];

function proposalParentCompany(mvpParent) {
  const p = (mvpParent || "").trim();
  if (/choice/i.test(p)) return "Choice Hotels";
  if (/marriott/i.test(p)) return "Marriott International";
  return p;
}

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (c === '"') inQ = false;
      else cur += c;
    } else if (c === '"') inQ = true;
    else if (c === ",") {
      out.push(cur);
      cur = "";
    } else cur += c;
  }
  out.push(cur);
  return out;
}

function collectSafeAliases(brandName) {
  const canonical = exactMatchKey(brandName);
  const found = new Map();

  function add(alias, reason, confidence = "Medium", allowSameAsCanonical = false) {
    const a = exactMatchKey(alias);
    if (!a) return;
    if (!allowSameAsCanonical && a === canonical) return;
    if (BLOCKED_ALIAS_KEYS.has(a.toLowerCase())) return;
    if (!found.has(a)) found.set(a, { reason, confidence });
  }

  add(canonical, "Exact MVP Brand Explorer display name", "High", true);

  for (const rule of SAFE_SHORT_RULES) {
    if (!rule.match(canonical)) continue;
    const extras = typeof rule.aliases === "function" ? rule.aliases(canonical) : rule.aliases;
    for (const alias of extras) {
      add(alias, rule.reason, "High");
    }
  }

  return [...found.entries()].map(([alias, meta]) => ({ alias, ...meta }));
}

function buildProposalRow(brand, aliasMeta) {
  return {
    "Canonical Brand Name": brand.brandName,
    "Alias / Source Brand Name": aliasMeta.alias,
    "Parent Company": proposalParentCompany(brand.parentCompany),
    Active: true,
    "Match Confidence": aliasMeta.confidence,
    Notes: "Phase 1C proposal from coverage QA; not seeded automatically",
    "Proposal Reason": aliasMeta.reason,
    "Requires Human Review": true,
    _qa: {
      mvpExistingHotels: brand.mvpExistingHotels,
      censusOpenHotels: brand.censusOpenHotels,
      warnings: brand.warnings,
    },
  };
}

async function main() {
  const csv = readFileSync(COVERAGE_PATH, "utf8");
  const rows = parseCsv(csv);
  const fallbackBrands = rows.filter((r) => String(r.fallbackRecommended).toLowerCase() === "yes");

  const proposals = [];
  const seenKeys = new Set();

  for (const brand of fallbackBrands) {
    const aliases = collectSafeAliases(brand.brandName);
    for (const meta of aliases) {
      const key = [brand.brandName, meta.alias, proposalParentCompany(brand.parentCompany)]
        .map((s) => exactMatchKey(s))
        .join("\u0001");
      if (seenKeys.has(key)) continue;
      seenKeys.add(key);
      proposals.push(buildProposalRow(brand, meta));
    }
  }

  const output = {
    generatedAt: new Date().toISOString(),
    sourceCoverageCsv: "reports/brand-explorer-census-coverage.csv",
    instructions:
      "Review proposals. Copy approved rows to reports/proposed-brand-aliases-reviewed.json with Approved: true, then run node scripts/seed-reviewed-brand-aliases.mjs",
    fallbackBrandCount: fallbackBrands.length,
    proposedRowCount: proposals.length,
    rows: proposals,
  };

  mkdirSync(dirname(OUTPUT_PATH), { recursive: true });
  writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2), "utf8");

  console.log(JSON.stringify({
    fallbackBrands: fallbackBrands.length,
    proposedRows: proposals.length,
    output: OUTPUT_PATH,
  }, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
