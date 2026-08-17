#!/usr/bin/env node
/**
 * Generate lib + script modules for Caribbean remaining island builds from manifest.
 */
import { writeFileSync, mkdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { CARIBBEAN_REMAINING_ISLAND_BUILDS } from "../lib/radar-buildout/caribbean-remaining-islands-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const libDir = join(root, "lib/radar-buildout");
const scriptsDir = join(root, "scripts");

function pascalFromSlug(slug) {
  return slug
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join("");
}

function camelFromSlug(slug) {
  const p = pascalFromSlug(slug);
  return p.charAt(0).toLowerCase() + p.slice(1);
}

function constPrefix(country) {
  return country
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
}

function writeGovernance(island) {
  const { slug, country, submarkets } = island;
  const pascal = pascalFromSlug(slug);
  const prefix = constPrefix(country);
  const content = `/**
 * Governance defaults for ${country} countrywide demand anchor candidates.
 */
import {
  ISLAND_POINT_TYPE_USE_CASE_TAGS,
  ISLAND_TIER_1_POINT_TYPES,
  applyIslandGovernanceDefaults,
} from "./island-country-shared.js";

export function apply${pascal}GovernanceDefaults(point, overrides = {}) {
  const pointType = String(point.pointType || "").trim();
  const submarket = String(point.submarket || "").trim();
  const useCaseTags =
    overrides.useCaseTags || ISLAND_POINT_TYPE_USE_CASE_TAGS[pointType] || ["Resort / Leisure"];

  return applyIslandGovernanceDefaults("${country}", point, {
    ...overrides,
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      \`${country} countrywide build — \${submarket} \${pointType} anchor for hotel demand.\`,
  });
}

export const ${prefix}_SUBMARKETS = ${JSON.stringify(submarkets)};
`;
  writeFileSync(join(libDir, `${slug}-demand-anchor-governance.js`), content);
}

function writeCandidates(island) {
  const { slug, country, candidates } = island;
  const pascal = pascalFromSlug(slug);
  const prefix = constPrefix(country);
  const rows = candidates
    .map((c) => {
      const parts = [`name: ${JSON.stringify(c.name)}`, `pointType: ${JSON.stringify(c.pointType)}`, `city: ${JSON.stringify(c.city)}`, `submarket: ${JSON.stringify(c.submarket)}`, `latitude: ${c.latitude}`, `longitude: ${c.longitude}`, `sourceReference: ${JSON.stringify(c.sourceReference)}`];
      if (c.manuallyVerified) parts.push("manuallyVerified: true");
      if (c.googleSearchQuery) parts.push(`googleSearchQuery: ${JSON.stringify(c.googleSearchQuery)}`);
      if (c.dataConfidence) parts.push(`dataConfidence: ${JSON.stringify(c.dataConfidence)}`);
      if (c.hotelDemandNote) parts.push(`hotelDemandNote: ${JSON.stringify(c.hotelDemandNote)}`);
      return `  pt({ ${parts.join(", ")} })`;
    })
    .join(",\n");

  const content = `/**
 * ${country} countrywide demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  apply${pascal}GovernanceDefaults,
  ${prefix}_SUBMARKETS,
} from "./${slug}-demand-anchor-governance.js";

const COUNTRY = ${JSON.stringify(country)};
const REGION = "Caribbean";

const pt = createIslandCandidateBuilder(COUNTRY, REGION, apply${pascal}GovernanceDefaults);

export const ${prefix}_COUNTRYWIDE_CANDIDATES = [
${rows},
];

export function get${pascal}Candidates() {
  return ${prefix}_COUNTRYWIDE_CANDIDATES;
}

export { ${prefix}_SUBMARKETS };
`;
  writeFileSync(join(libDir, `${slug}-demand-anchors-candidates.js`), content);
}

function writeCorrections(island) {
  const { slug, country, googleCorrections = {} } = island;
  const pascal = pascalFromSlug(slug);
  const prefix = constPrefix(country);
  const content = `/**
 * Google Places review corrections for ${country} countrywide candidates.
 */
import { REVIEW_TAG, createPlaceReviewApplier } from "./island-country-shared.js";

/** @type {Record<string, object>} */
export const ${prefix}_GOOGLE_PLACE_REVIEW_CORRECTIONS = ${JSON.stringify(googleCorrections, null, 2)};

export function apply${pascal}PlaceReviewCorrection(point) {
  const fix = ${prefix}_GOOGLE_PLACE_REVIEW_CORRECTIONS[point.name];
  if (!fix) return point;
  const merged = { ...point, ...fix };
  if (fix.manuallyVerified) {
    merged.manuallyVerified = true;
    merged.dataConfidence = fix.dataConfidence || "High";
  }
  merged.notes = \`\${point.notes || ""} \${REVIEW_TAG}\`.trim();
  return merged;
}

export function apply${pascal}PlaceReviewCorrections(points) {
  return points.map(apply${pascal}PlaceReviewCorrection);
}
`;
  writeFileSync(join(libDir, `${slug}-google-place-review-corrections.js`), content);
}

function writeTiDelta(island) {
  const { slug, country, market, tiRecords } = island;
  const pascal = pascalFromSlug(slug);
  const prefix = constPrefix(country);
  const rows = tiRecords
    .map((r) => {
      const parts = [`name: ${JSON.stringify(r.name)}`, `pointType: ${JSON.stringify(r.pointType)}`, `city: ${JSON.stringify(r.city)}`, `submarket: ${JSON.stringify(r.submarket)}`, `latitude: ${r.latitude}`, `longitude: ${r.longitude}`, `sourceReference: ${JSON.stringify(r.sourceReference)}`];
      if (r.pointSubtype) parts.push(`pointSubtype: ${JSON.stringify(r.pointSubtype)}`);
      if (r.notes) parts.push(`notes: ${JSON.stringify(r.notes)}`);
      if (r.useCaseTags) parts.push(`useCaseTags: ${JSON.stringify(r.useCaseTags)}`);
      return `  ti({ ${parts.join(", ")} })`;
    })
    .join(",\n");

  const content = `/**
 * ${country} countrywide Travel Infrastructure delta records (audit gap fill).
 */
import { createIslandTiBuilder, buildIslandTiDeltaFixture } from "./island-country-shared.js";

const COUNTRY = ${JSON.stringify(country)};
const MARKET = ${JSON.stringify(market)};
const ti = createIslandTiBuilder(COUNTRY, MARKET);

export const ${prefix}_TI_DELTA_RECORDS = [
${rows},
];

export function build${pascal}TiDeltaFixture() {
  return buildIslandTiDeltaFixture(COUNTRY, MARKET, ${prefix}_TI_DELTA_RECORDS);
}
`;
  writeFileSync(join(libDir, `${slug}-travel-infrastructure-delta.js`), content);
}

function writeCountrywideScript(island) {
  const { slug, country, market } = island;
  const pascal = pascalFromSlug(slug);
  const content = `#!/usr/bin/env node
/**
 * Build ${country} countrywide demand anchor candidate fixtures.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  get${pascal}Candidates,
  ${constPrefix(country)}_SUBMARKETS,
} from "../lib/radar-buildout/${slug}-demand-anchors-candidates.js";
import { apply${pascal}PlaceReviewCorrections } from "../lib/radar-buildout/${slug}-google-place-review-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const MARKET = ${JSON.stringify(market)};
const config = getCountryConfig(${JSON.stringify(country)});

const points = apply${pascal}PlaceReviewCorrections(get${pascal}Candidates());
const bySubmarket = {};
const byPointType = {};
const byCity = {};
for (const p of points) {
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
  byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  byCity[p.city] = (byCity[p.city] || 0) + 1;
}

const fixture = {
  country: ${JSON.stringify(country)},
  region: "Caribbean",
  market: MARKET,
  buildStrategy: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  submarkets: ${constPrefix(country)}_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
  points,
};

for (const rel of [
  "fixtures/demand-anchors-${slug}-countrywide-candidates.json",
  "public/fixtures/demand-anchors-${slug}-countrywide-candidates.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\\n");
}

console.log("${country} countrywide candidates written:", points.length);
console.log("By submarket:", bySubmarket);
`;
  writeFileSync(join(scriptsDir, `build-${slug}-countrywide-fixtures.mjs`), content);
}

function writeTiScript(island) {
  const { slug, country } = island;
  const pascal = pascalFromSlug(slug);
  const content = `#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { build${pascal}TiDeltaFixture } from "../lib/radar-buildout/${slug}-travel-infrastructure-delta.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const fixture = build${pascal}TiDeltaFixture();

for (const rel of [
  "fixtures/travel-infrastructure-${slug}-countrywide-real.json",
  "public/fixtures/travel-infrastructure-${slug}-countrywide-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\\n");
}

console.log("${country} TI delta:", fixture.points.length, "records");
`;
  writeFileSync(join(scriptsDir, `build-${slug}-ti-fixtures.mjs`), content);
}

function writeMicroPassScript(island) {
  const { slug, country, market, microPass } = island;
  if (!microPass) return;
  const content = `#!/usr/bin/env node
/**
 * Micro-pass fixture for ${country} DA batch-dedup skips (unique source refs).
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const MICRO_NAMES = new Set(${JSON.stringify(microPass.names || [], null, 2)});

function slug(name) {
  return String(name).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function uniqueSource(point) {
  const ref = String(point.sourceReference || "").trim();
  const key = slug(point.name);
  const rules = ${JSON.stringify(microPass.rules || [], null, 4)};
  for (const rule of rules) {
    const bases = Array.isArray(rule.base) ? rule.base : [rule.base];
    if (bases.some((b) => ref === b || ref === b.replace(/\\/$/, ""))) {
      return rule.template.replace("{key}", key);
    }
  }
  return \`\${ref.replace(/\\/$/, "")}#\${key}\`;
}

const real = JSON.parse(
  readFileSync(join(root, "fixtures/demand-anchors-${slug}-countrywide-real.json"), "utf8")
);

const points = (real.points || [])
  .filter((p) => MICRO_NAMES.has(p.name))
  .map((p) => ({
    ...p,
    sourceReference: uniqueSource(p),
    notes: \`\${p.notes || ""} ${country} micro-pass import with unique source reference.\`.trim(),
  }));

const fixture = {
  market: ${JSON.stringify(market)},
  country: ${JSON.stringify(country)},
  region: "Caribbean",
  buildBatch: "micro_pass",
  verification: real.verification,
  points,
};

for (const rel of [
  "fixtures/demand-anchors-${slug}-countrywide-micro-pass.json",
  "public/fixtures/demand-anchors-${slug}-countrywide-micro-pass.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\\n");
}
console.log("Micro-pass points:", points.length);
`;
  writeFileSync(join(scriptsDir, `build-${slug}-micro-pass-fixture.mjs`), content);
}

for (const island of CARIBBEAN_REMAINING_ISLAND_BUILDS) {
  writeGovernance(island);
  writeCandidates(island);
  writeCorrections(island);
  writeTiDelta(island);
  writeCountrywideScript(island);
  writeTiScript(island);
  writeMicroPassScript(island);
  console.log("Generated modules for", island.country);
}

console.log("Done:", CARIBBEAN_REMAINING_ISLAND_BUILDS.length, "islands");
