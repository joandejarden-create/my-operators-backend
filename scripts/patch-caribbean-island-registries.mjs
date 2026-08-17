#!/usr/bin/env node
/**
 * Patch shared registry files for Caribbean remaining island builds.
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { CARIBBEAN_REMAINING_ISLAND_BUILDS } from "../lib/radar-buildout/caribbean-remaining-islands-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function islandCfgBlock(island) {
  const subs = JSON.stringify(island.submarkets);
  return `  ${JSON.stringify(island.country)}: cfg({
    region: "Caribbean",
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 2",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: [
      "Resort / Leisure",
      "Cruise / Port",
      "Airport / Transit",
      "Mixed-Use / Growth",
      "Urban / Corporate",
    ],
    submarkets: ${subs},
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: ${JSON.stringify(island.notes)},
  }),`;
}

function sequenceBlock(island) {
  return `  ${JSON.stringify(island.country)}: {
    recommendedBuildSequence: ${island.sequence},
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: ${JSON.stringify(island.notes + " Maintain corridor QA; mature pass can deepen secondary nodes.")},
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },`;
}

function auditBlock(island) {
  const kw = JSON.stringify(island.audit.keywords);
  const patterns = island.audit.expectedPatterns
    .map(
      (p) =>
        `      { label: ${JSON.stringify(p.label)}, patterns: [${p.patterns.map((r) => r.toString()).join(", ")}] },`
    )
    .join("\n");
  return `  ${JSON.stringify(island.country)}: {
    keywords: ${kw},
    expectedPatterns: [
${patterns}
    ],
  },`;
}

// country-configs.js — insert before Guatemala
const cfgPath = join(root, "lib/radar-buildout/country-configs.js");
let cfgText = readFileSync(cfgPath, "utf8");
const cfgInsert = CARIBBEAN_REMAINING_ISLAND_BUILDS.map(islandCfgBlock).join("\n");
if (!cfgText.includes('"Saint Lucia": cfg(')) {
  cfgText = cfgText.replace(/\n  Guatemala: cfg\(/, `\n${cfgInsert}\n  Guatemala: cfg(`);
  writeFileSync(cfgPath, cfgText);
  console.log("Updated country-configs.js");
}

// post-colombia-build-sequence.js — insert before Brazil closing
const seqPath = join(root, "lib/radar-buildout/post-colombia-build-sequence.js");
let seqText = readFileSync(seqPath, "utf8");
const seqInsert = CARIBBEAN_REMAINING_ISLAND_BUILDS.map(sequenceBlock).join("\n\n");
if (!seqText.includes('"Saint Lucia":')) {
  seqText = seqText.replace(/\n  Brazil: \{/, `\n${seqInsert}\n\n  Brazil: {`);
  writeFileSync(seqPath, seqText);
  console.log("Updated post-colombia-build-sequence.js");
}

// BUILT_RADAR_COUNTRIES
const regPath = join(root, "lib/radar-submarket-registry.js");
let regText = readFileSync(regPath, "utf8");
for (const island of CARIBBEAN_REMAINING_ISLAND_BUILDS) {
  if (!regText.includes(`"${island.country}"`)) {
    regText = regText.replace(
      /("Turks & Caicos",)\n\];/,
      `$1\n  "${island.country}",\n];`
    );
  }
}
writeFileSync(regPath, regText);
console.log("Updated radar-submarket-registry.js");

// audit configs — insert before Peru
const auditPath = join(root, "lib/radar-buildout/market-travel-infrastructure-audit-configs.js");
let auditText = readFileSync(auditPath, "utf8");
const auditInsert = CARIBBEAN_REMAINING_ISLAND_BUILDS.map(auditBlock).join("\n");
if (!auditText.includes("Saint Lucia:")) {
  auditText = auditText.replace(/\n  Peru: \{/, `\n${auditInsert}\n  Peru: {`);
  writeFileSync(auditPath, auditText);
  console.log("Updated market-travel-infrastructure-audit-configs.js");
}

// package.json scripts
const pkgPath = join(root, "package.json");
const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
for (const island of CARIBBEAN_REMAINING_ISLAND_BUILDS) {
  const slug = island.slug;
  const country = island.country;
  pkg.scripts[`build:${slug}-fixtures`] = `node scripts/build-${slug}-countrywide-fixtures.mjs`;
  pkg.scripts[`verify:${slug}-google`] =
    `node scripts/verify-demand-anchors-google.mjs --file fixtures/demand-anchors-${slug}-countrywide-candidates.json --country "${country}" --output fixtures/demand-anchors-${slug}-google-verification-report.json --verified-output fixtures/demand-anchors-${slug}-countrywide-real.json --cache --max-requests 220`;
  pkg.scripts[`build:${slug}-ti-fixtures`] = `node scripts/build-${slug}-ti-fixtures.mjs`;
  pkg.scripts[`audit:${slug}-ti`] =
    `node scripts/audit-market-travel-infrastructure.mjs --country "${country}" --market "${island.market}" --output data/${slug}-travel-infrastructure-audit.json`;
}
writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n");
console.log("Updated package.json scripts");

console.log("Registry patch complete.");
