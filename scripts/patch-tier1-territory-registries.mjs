#!/usr/bin/env node
/**
 * Patch package.json scripts + registries for Tier-1 market and territory builds.
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  ALL_MARKET_BUILD_SPECS,
  TERRITORY_MARKET_BUILDS,
} from "../lib/radar-buildout/tier1-territories-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

// package.json scripts
const pkgPath = join(root, "package.json");
const pkg = JSON.parse(readFileSync(pkgPath, "utf8"));
for (const spec of ALL_MARKET_BUILD_SPECS) {
  const { slug, country, market } = spec;
  pkg.scripts[`build:${slug}-fixtures`] = `node scripts/build-${slug}-fixtures.mjs`;
  pkg.scripts[`verify:${slug}-google`] =
    `node scripts/verify-demand-anchors-google.mjs --file fixtures/demand-anchors-${slug}-candidates.json --country "${country}" --output fixtures/demand-anchors-${slug}-google-verification-report.json --verified-output fixtures/demand-anchors-${slug}-real.json --cache --max-requests 280`;
  pkg.scripts[`build:${slug}-ti-fixtures`] = `node scripts/build-${slug}-ti-fixtures.mjs`;
  if (!spec.buildType) {
    pkg.scripts[`audit:${slug}-ti`] =
      `node scripts/audit-market-travel-infrastructure.mjs --country "${country}" --market "${market}" --output data/${slug}-travel-infrastructure-audit.json`;
  }
}
pkg.scripts["verify:colombia-google"] =
  "node scripts/verify-demand-anchors-google.mjs --file fixtures/demand-anchors-colombia-countrywide-candidates.json --country Colombia --output fixtures/demand-anchors-colombia-google-verification-report.json --verified-output fixtures/demand-anchors-colombia-countrywide-real.json --cache --max-requests 280";
writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n");
console.log("Updated package.json");

// Territory country configs
function territoryCfg(spec) {
  const subs = JSON.stringify(spec.submarkets);
  return `  ${JSON.stringify(spec.country)}: cfg({
    region: ${JSON.stringify(spec.region || "Caribbean")},
    buildStrategy: S.ISLAND_COUNTRYWIDE,
    priorityTier: "Tier 3",
    primaryHotelDemandProfile: "Resort / Leisure",
    primaryHotelDemandProfiles: ["Resort / Leisure", "Cruise / Port", "Urban / Corporate", "Mixed-Use / Growth"],
    submarkets: ${subs},
    nextBuildMarket: "Completed / Deal Ready",
    targets: {
      demandAnchors: { firstPass: { min: 35, max: 55 }, mature: { min: 50, max: 80 } },
      travelInfrastructure: { firstPass: { min: 12, max: 18 }, mature: { min: 18, max: 28 } },
      totalRadarPoints: { firstPass: { min: 50, max: 75 }, mature: { min: 75, max: 110 } },
    },
    notes: ${JSON.stringify(spec.notes)},
  }),`;
}

const cfgPath = join(root, "lib/radar-buildout/country-configs.js");
let cfgText = readFileSync(cfgPath, "utf8");
for (const spec of TERRITORY_MARKET_BUILDS) {
  if (!cfgText.includes(`"${spec.country}": cfg(`)) {
    const block = territoryCfg(spec);
    cfgText = cfgText.replace(/\n  Guatemala: cfg\(/, `\n${block}\n  Guatemala: cfg(`);
    console.log("Added country-config:", spec.country);
  }
}

// Update Tier 1 nextBuildMarket markers
cfgText = cfgText.replace(
  /(Colombia: cfg\(\{[\s\S]*?)nextBuildMarket: "[^"]*"/,
  '$1nextBuildMarket: "Completed / Deal Ready"'
);
cfgText = cfgText.replace(
  /(Panama: cfg\(\{[\s\S]*?)nextBuildMarket: "[^"]*"/,
  '$1nextBuildMarket: "Completed / Deal Ready"'
);
cfgText = cfgText.replace(
  /(Costa Rica: cfg\(\{[\s\S]*?)nextBuildMarket: "[^"]*"/,
  '$1nextBuildMarket: "Completed / Deal Ready"'
);
cfgText = cfgText.replace(
  /("Dominican Republic": cfg\(\{[\s\S]*?)nextBuildMarket: "[^"]*"/,
  '$1nextBuildMarket: "Completed / Deal Ready"'
);
cfgText = cfgText.replace(
  /(Mexico: cfg\(\{[\s\S]*?)nextBuildMarket: "[^"]*"/,
  '$1nextBuildMarket: "Completed / Tier 1 Markets"'
);
writeFileSync(cfgPath, cfgText);

// post-colombia sequence
const seqPath = join(root, "lib/radar-buildout/post-colombia-build-sequence.js");
let seqText = readFileSync(seqPath, "utf8");

const tier1Updates = {
  Colombia: { nextBuildMarket: "Completed / Deal Ready", notes: "All initial markets built and imported. Maintain market-by-market QA." },
  Panama: { nextBuildMarket: "Completed / Deal Ready", notes: "Countrywide corridor first pass complete." },
  "Costa Rica": { nextBuildMarket: "Completed / Deal Ready", notes: "Corridor-based countrywide first pass complete." },
  "Dominican Republic": { nextBuildMarket: "Completed / Deal Ready", notes: "Mature corridor pass complete with gap-fill import." },
  Mexico: { nextBuildMarket: "Completed / Tier 1 Markets", notes: "Cancún and Tier-1 initial markets (CDMX, Los Cabos, GDL, MTY, PV/Nayarit, Mérida) built. Secondary markets on hold." },
};

for (const [country, meta] of Object.entries(tier1Updates)) {
  const re = new RegExp(`("${country.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}": \\{[\\s\\S]*?)nextBuildMarket: "[^"]*"`, "m");
  if (re.test(seqText)) {
    seqText = seqText.replace(re, `$1nextBuildMarket: "${meta.nextBuildMarket}"`);
  }
}

let seq = 25;
for (const spec of TERRITORY_MARKET_BUILDS) {
  if (!seqText.includes(`"${spec.country}":`)) {
    const block = `  ${JSON.stringify(spec.country)}: {
    recommendedBuildSequence: ${seq},
    nextBuildMarket: "Completed / Deal Ready",
    buildApproachNotes: ${JSON.stringify(spec.notes + " Island/countrywide first pass complete.")},
    firstPassTargetDescription: "First pass 35–55 DA, 12–18 TI, 50–75 total.",
  },`;
    seqText = seqText.replace(/\n  Brazil: \{/, `\n${block}\n\n  Brazil: {`);
    seq++;
  }
}
writeFileSync(seqPath, seqText);

// BUILT_RADAR_COUNTRIES
const regPath = join(root, "lib/radar-submarket-registry.js");
let regText = readFileSync(regPath, "utf8");
for (const country of ["Panama", "Costa Rica", ...TERRITORY_MARKET_BUILDS.map((s) => s.country)]) {
  if (!regText.includes(`"${country}"`)) {
    regText = regText.replace(/\];\s*\n\s*\/\*\*/, `,\n  "${country}"\n];\n\n/**`);
  }
}
writeFileSync(regPath, regText);

// audit configs for territories
const auditPath = join(root, "lib/radar-buildout/market-travel-infrastructure-audit-configs.js");
let auditText = readFileSync(auditPath, "utf8");
for (const spec of TERRITORY_MARKET_BUILDS) {
  if (auditText.includes(`"${spec.country}":`)) continue;
  const patterns = spec.audit.expectedPatterns
    .map((p) => `      { label: ${JSON.stringify(p.label)}, patterns: [${p.patterns.map((r) => r.toString()).join(", ")}] },`)
    .join("\n");
  const block = `  ${JSON.stringify(spec.country)}: {
    keywords: ${JSON.stringify(spec.audit.keywords)},
    expectedPatterns: [
${patterns}
    ],
  },`;
  auditText = auditText.replace(/\n  Peru: \{/, `\n${block}\n  Peru: {`);
}
writeFileSync(auditPath, auditText);

console.log("Registry patch complete.");
