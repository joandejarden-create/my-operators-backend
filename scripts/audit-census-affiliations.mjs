/**
 * Read-only inventory of Hotel Census Affiliation values (diagnostic).
 *
 * Usage:
 *   node scripts/audit-census-affiliations.mjs
 *   node scripts/audit-census-affiliations.mjs --parent="Choice Hotels"
 *   node scripts/audit-census-affiliations.mjs --contains=Radisson --contains=Ascend
 *   node scripts/audit-census-affiliations.mjs --choice-radisson-scan
 *
 * Output: reports/census-affiliation-inventory.csv (+ console)
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  CENSUS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
  HOTEL_CENSUS_TABLE,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "../lib/hotel-census/fields.js";
import { exactMatchKey } from "../lib/hotel-census/brand-alias-resolve.js";
import { getGovernanceFieldAvailability, shouldIncludeRowForBrandExplorer } from "../lib/hotel-census/census-governance.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_PATH = join(__dirname, "..", "reports", "census-affiliation-inventory.csv");
const COVERAGE_PATH = join(__dirname, "..", "reports", "brand-explorer-census-coverage.csv");

const CHOICE_RADISSON_CONTAINS = [
  "Choice",
  "Radisson",
  "Park",
  "Ascend",
  "Sleep",
  "Quality",
  "Clarion",
  "Comfort",
  "Cambria",
  "WoodSpring",
  "MainStay",
  "Suburban",
  "Econo",
  "Rodeway",
  "Everhome",
  "Country Inn",
];

const EXAMPLE_LIMIT = 3;

function parseArgs(argv) {
  const parents = [];
  const contains = [];
  let choiceRadissonScan = false;
  let compareActive = false;

  for (const arg of argv) {
    if (arg === "--choice-radisson-scan") choiceRadissonScan = true;
    else if (arg === "--compare-active") compareActive = true;
    else if (arg.startsWith("--parent=")) parents.push(arg.slice("--parent=".length));
    else if (arg.startsWith("--contains=")) contains.push(arg.slice("--contains=".length));
  }

  if (choiceRadissonScan) {
    compareActive = true;
    if (!contains.length) contains.push(...CHOICE_RADISSON_CONTAINS);
    if (!parents.length) {
      parents.push("Choice Hotels", "Choice Hotels International");
    }
  }

  return { parents, contains, compareActive };
}

function parseNum(v) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : 0;
}

function normalizeStatus(raw) {
  const s = exactMatchKey(raw);
  if (s === "open" || s === STATUS_OPEN) return STATUS_OPEN;
  if (s === "pipeline" || s === STATUS_PIPELINE) return STATUS_PIPELINE;
  return s || "Other";
}

function rowMatchesFilters(rec, filters) {
  const f = rec.fields || {};
  const affiliation = exactMatchKey(f[CENSUS_FIELDS.affiliation]);
  const parent = exactMatchKey(f[CENSUS_FIELDS.parentCompany]);
  const name = exactMatchKey(f[CENSUS_FIELDS.name]);

  if (filters.parents.length) {
    const parentNorm = parent.toLowerCase();
    const ok = filters.parents.some((p) => {
      const want = exactMatchKey(p).toLowerCase();
      return parentNorm === want || parentNorm.includes(want) || want.includes(parentNorm);
    });
    if (!ok) return false;
  }

  if (filters.contains.length) {
    const hay = `${affiliation} ${parent} ${name}`.toLowerCase();
    const ok = filters.contains.some((c) => hay.includes(String(c).toLowerCase()));
    if (!ok) return false;
  }

  return true;
}

function pushExample(arr, value) {
  const v = exactMatchKey(value);
  if (!v || arr.includes(v)) return;
  if (arr.length < EXAMPLE_LIMIT) arr.push(v);
}

function groupKey(affiliation, parentCompany) {
  return `${affiliation}\u0001${parentCompany}`;
}

function aggregateRecords(records, governance) {
  const groups = new Map();

  for (const rec of records) {
    const f = rec.fields || {};
    const affiliation = exactMatchKey(f[CENSUS_FIELDS.affiliation]) || "(blank)";
    if (affiliation === CENSUS_INDEPENDENT_AFFILIATION) continue;

    if (
      !shouldIncludeRowForBrandExplorer(
        f,
        CENSUS_FIELDS.includeInBrandExplorer,
        governance.includeInBrandExplorer
      )
    ) {
      continue;
    }

    const parentCompany = exactMatchKey(f[CENSUS_FIELDS.parentCompany]) || "(blank)";
    const key = groupKey(affiliation, parentCompany);

    if (!groups.has(key)) {
      groups.set(key, {
        affiliation,
        parentCompany,
        totalRecords: 0,
        openRecords: 0,
        pipelineRecords: 0,
        openKeys: 0,
        countries: new Set(),
        exampleNames: [],
        exampleCities: [],
        exampleChainScales: [],
        exampleLocations: [],
      });
    }

    const g = groups.get(key);
    g.totalRecords += 1;

    const status = normalizeStatus(f[CENSUS_FIELDS.status]);
    const rooms = parseNum(f[CENSUS_FIELDS.rooms]);
    const country = exactMatchKey(f[CENSUS_FIELDS.country]);

    if (status === STATUS_OPEN) {
      g.openRecords += 1;
      g.openKeys += rooms;
      if (country) g.countries.add(country);
    } else if (status === STATUS_PIPELINE) {
      g.pipelineRecords += 1;
    }

    pushExample(g.exampleNames, f[CENSUS_FIELDS.name]);
    pushExample(g.exampleCities, f[CENSUS_FIELDS.city]);
    pushExample(g.exampleChainScales, f[CENSUS_FIELDS.chainScale]);
    pushExample(g.exampleLocations, f[CENSUS_FIELDS.location]);
  }

  return [...groups.values()].sort(
    (a, b) => b.openRecords - a.openRecords || b.totalRecords - a.totalRecords
  );
}

function csvEscape(val) {
  const s = val == null ? "" : String(val);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsvRow(g) {
  const countries = [...g.countries].sort();
  return {
    Affiliation: g.affiliation,
    "Parent Company": g.parentCompany,
    "Total Records": g.totalRecords,
    "Open Records": g.openRecords,
    "Pipeline Records": g.pipelineRecords,
    "Open Keys": g.openKeys,
    "Country Count": countries.filter((c) => c && c !== "Unknown").length,
    Countries: countries.join("; "),
    "Example Hotel Names": g.exampleNames.join("; "),
    "Example Cities": g.exampleCities.join("; "),
    "Example Chain Scales": g.exampleChainScales.join("; "),
    "Example Locations": g.exampleLocations.join("; "),
  };
}

function loadActiveBrandsFromCoverage() {
  try {
    const text = readFileSync(COVERAGE_PATH, "utf8");
    const lines = text.trim().split(/\r?\n/);
    const headers = lines[0].split(",");
    return lines.slice(1).map((line) => {
      const cols = [];
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
          cols.push(cur);
          cur = "";
        } else cur += c;
      }
      cols.push(cur);
      const row = {};
      headers.forEach((h, i) => {
        row[h.trim()] = (cols[i] || "").trim();
      });
      return row;
    });
  } catch {
    return [];
  }
}

function compareActiveBrands(groups, activeBrands) {
  const affiliationSet = new Set(groups.map((g) => g.affiliation));
  const openAffiliationSet = new Set(
    groups.filter((g) => g.openRecords > 0).map((g) => g.affiliation)
  );

  const missing = [];
  const noOpenMatch = [];

  for (const b of activeBrands) {
    const name = b.brandName || b["brandName"];
    if (!name) continue;
    const exact = exactMatchKey(name);
    if (!affiliationSet.has(exact)) {
      missing.push({ brandName: name, parentCompany: b.parentCompany, censusOpenHotels: b.censusOpenHotels });
    } else if (!openAffiliationSet.has(exact)) {
      noOpenMatch.push({ brandName: name, parentCompany: b.parentCompany });
    }
  }

  return { missing, noOpenMatch, affiliationSet, openAffiliationSet };
}

function recommendAliases(groups, activeBrands) {
  const recommendations = [];
  const byAffLower = new Map();
  for (const g of groups) {
    if (g.openRecords === 0) continue;
    const k = g.affiliation.toLowerCase();
    if (!byAffLower.has(k)) byAffLower.set(k, []);
    byAffLower.get(k).push(g);
  }

  for (const b of activeBrands) {
    const canonical = exactMatchKey(b.brandName || b["brandName"]);
    if (!canonical) continue;
    if (byAffLower.has(canonical.toLowerCase())) {
      recommendations.push({
        canonical,
        alias: canonical,
        parentCompany: "Choice Hotels",
        reason: "Exact Affiliation match in census (display name)",
        openRecords: byAffLower.get(canonical.toLowerCase())[0].openRecords,
      });
      continue;
    }

    const stripped = canonical.replace(/\s*\(Choice\)\s*$/i, "").trim();
    if (stripped !== canonical && byAffLower.has(stripped.toLowerCase())) {
      const g = byAffLower.get(stripped.toLowerCase())[0];
      recommendations.push({
        canonical,
        alias: g.affiliation,
        parentCompany: g.parentCompany,
        reason: `Census Affiliation "${g.affiliation}" matches stripped display suffix`,
        openRecords: g.openRecords,
      });
      continue;
    }

    const candidates = groups.filter((g) => {
      if (g.openRecords === 0) return false;
      const a = g.affiliation.toLowerCase();
      const c = canonical.toLowerCase();
      const s = stripped.toLowerCase();
      return a.includes(s) || s.includes(a) || a.includes(c);
    });

    if (candidates.length === 1) {
      const g = candidates[0];
      recommendations.push({
        canonical,
        alias: g.affiliation,
        parentCompany: g.parentCompany,
        reason: `Single census Affiliation candidate (${g.openRecords} open) — verify exact match before seeding`,
        openRecords: g.openRecords,
        requiresReview: true,
      });
    }
  }

  return recommendations;
}

async function main() {
  const filters = parseArgs(process.argv.slice(2));
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  const base = new Airtable({ apiKey }).base(baseId);
  const governance = await getGovernanceFieldAvailability(base);

  const selectFields = [
    CENSUS_FIELDS.affiliation,
    CENSUS_FIELDS.parentCompany,
    CENSUS_FIELDS.status,
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.city,
    CENSUS_FIELDS.name,
    CENSUS_FIELDS.chainScale,
    CENSUS_FIELDS.location,
  ];
  if (governance.includeInBrandExplorer) {
    selectFields.push(CENSUS_FIELDS.includeInBrandExplorer);
  }

  console.log("Loading Hotel Census (read-only)...");
  let records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, pageSize: 100 })
    .all();

  const before = records.length;
  records = records.filter((r) => rowMatchesFilters(r, filters));
  console.log(`Filtered ${before} → ${records.length} rows`);
  if (filters.parents.length) console.log("  parent filter:", filters.parents.join(", "));
  if (filters.contains.length) console.log("  contains filter:", filters.contains.join(", "));

  const groups = aggregateRecords(records, governance);
  const csvRows = groups.map(toCsvRow);

  mkdirSync(dirname(REPORT_PATH), { recursive: true });
  const headers = Object.keys(csvRows[0] || toCsvRow({
    affiliation: "",
    parentCompany: "",
    totalRecords: 0,
    openRecords: 0,
    pipelineRecords: 0,
    openKeys: 0,
    countries: new Set(),
    exampleNames: [],
    exampleCities: [],
    exampleChainScales: [],
    exampleLocations: [],
  }));
  const csv = [
    headers.join(","),
    ...csvRows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")),
  ].join("\n");
  writeFileSync(REPORT_PATH, csv + "\n", "utf8");
  console.log(`\nWrote ${REPORT_PATH} (${groups.length} affiliation groups)\n`);

  console.log("Top affiliations by open records:");
  groups.slice(0, 40).forEach((g, i) => {
    console.log(
      `${String(i + 1).padStart(2)}. ${g.affiliation} | parent: ${g.parentCompany} | open: ${g.openRecords} | pipe: ${g.pipelineRecords} | keys: ${g.openKeys}`
    );
  });

  if (filters.compareActive) {
    const activeBrands = loadActiveBrandsFromCoverage();
    const { missing, noOpenMatch } = compareActiveBrands(groups, activeBrands);

    console.log("\n--- Active Brand Explorer brands: no exact census Affiliation ---");
    missing.forEach((b) => {
      console.log(`  ${b.brandName} (MVP parent: ${b.parentCompany || "—"})`);
    });

    console.log("\n--- Exact Affiliation exists but 0 open in filter set ---");
    noOpenMatch.forEach((b) => console.log(`  ${b.brandName}`));

    const recs = recommendAliases(groups, activeBrands);
    console.log("\n--- Recommended alias rows (from census data only; review before seeding) ---");
    recs.slice(0, 30).forEach((r) => {
      console.log(
        `  ${r.canonical} → "${r.alias}" | parent: ${r.parentCompany} | open: ${r.openRecords} | ${r.reason}${r.requiresReview ? " [REVIEW]" : ""}`
      );
    });

    console.log(
      JSON.stringify(
        {
          groupsInReport: groups.length,
          activeBrandsChecked: activeBrands.length,
          noExactAffiliation: missing.length,
          exactButNoOpen: noOpenMatch.length,
          recommendations: recs.length,
        },
        null,
        2
      )
    );
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
