#!/usr/bin/env node
/**
 * Generate lib + script modules for Tier-1 market and territory builds.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { ALL_MARKET_BUILD_SPECS } from "../lib/radar-buildout/tier1-territories-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const libDir = join(root, "lib/radar-buildout");
const scriptsDir = join(root, "scripts");

function pascalFromSlug(slug) {
  return slug.split("-").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}

function slugConst(slug) {
  return slug.toUpperCase().replace(/-/g, "_");
}

function resolveBuildStrategy(spec) {
  if (spec.buildStrategy) return spec.buildStrategy;
  if (spec.country === "Mexico") return "MARKET_BY_MARKET";
  if (spec.buildType === "delta") return "CORRIDOR_BASED";
  return "ISLAND_COUNTRYWIDE";
}

function resolveScopeLevel(spec) {
  if (spec.scopeLevel) return spec.scopeLevel;
  if (spec.country === "Mexico") return "Market";
  return "Country";
}

function writeGovernance(spec) {
  const { slug, country, market, submarkets } = spec;
  const pascal = pascalFromSlug(slug);
  const prefix = slugConst(slug);
  const scopeLevel = resolveScopeLevel(spec);
  const content = `/**
 * Governance defaults for ${market} demand anchor candidates.
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
    scopeLevel: overrides.scopeLevel || "${scopeLevel}",
    useCaseTags,
    relevanceTier:
      overrides.relevanceTier || (ISLAND_TIER_1_POINT_TYPES.has(pointType) ? "Tier 1" : "Tier 2"),
    projectRelevanceLogic:
      overrides.projectRelevanceLogic ||
      \`${market} build — \${submarket} \${pointType} anchor for hotel demand.\`,
  });
}

export const ${prefix}_SUBMARKETS = ${JSON.stringify(submarkets)};
`;
  writeFileSync(join(libDir, `${slug}-demand-anchor-governance.js`), content);
}

function writeCandidates(spec) {
  const { slug, country, region, candidates } = spec;
  const pascal = pascalFromSlug(slug);
  const prefix = slugConst(slug);
  const rows = candidates
    .map((c) => {
      const parts = [
        `name: ${JSON.stringify(c.name)}`,
        `pointType: ${JSON.stringify(c.pointType)}`,
        `city: ${JSON.stringify(c.city)}`,
        `submarket: ${JSON.stringify(c.submarket)}`,
        `latitude: ${c.latitude}`,
        `longitude: ${c.longitude}`,
        `sourceReference: ${JSON.stringify(c.sourceReference)}`,
      ];
      if (c.manuallyVerified) parts.push("manuallyVerified: true");
      if (c.googleSearchQuery) parts.push(`googleSearchQuery: ${JSON.stringify(c.googleSearchQuery)}`);
      if (c.dataConfidence) parts.push(`dataConfidence: ${JSON.stringify(c.dataConfidence)}`);
      if (c.hotelDemandNote) parts.push(`hotelDemandNote: ${JSON.stringify(c.hotelDemandNote)}`);
      return `  pt({ ${parts.join(", ")} })`;
    })
    .join(",\n");

  const content = `/**
 * ${spec.market} demand anchor candidates (source-backed).
 */
import { createIslandCandidateBuilder } from "./island-country-shared.js";
import {
  apply${pascal}GovernanceDefaults,
  ${prefix}_SUBMARKETS,
} from "./${slug}-demand-anchor-governance.js";

const COUNTRY = ${JSON.stringify(country)};
const REGION = ${JSON.stringify(region)};

const pt = createIslandCandidateBuilder(COUNTRY, REGION, apply${pascal}GovernanceDefaults);

export const ${prefix}_CANDIDATES = [
${rows},
];

export function get${pascal}Candidates() {
  return ${prefix}_CANDIDATES;
}

export { ${prefix}_SUBMARKETS };
`;
  writeFileSync(join(libDir, `${slug}-demand-anchors-candidates.js`), content);
}

function writeCorrections(spec) {
  const { slug, market, googleCorrections = {} } = spec;
  const pascal = pascalFromSlug(slug);
  const prefix = slugConst(slug);
  const content = `/**
 * Google Places review corrections for ${market} candidates.
 */
import { REVIEW_TAG } from "./island-country-shared.js";

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

function writeTiDelta(spec) {
  const { slug, country, market, tiRecords } = spec;
  const pascal = pascalFromSlug(slug);
  const prefix = slugConst(slug);
  const rows = tiRecords
    .map((r) => {
      const parts = [
        `name: ${JSON.stringify(r.name)}`,
        `pointType: ${JSON.stringify(r.pointType)}`,
        `city: ${JSON.stringify(r.city)}`,
        `submarket: ${JSON.stringify(r.submarket)}`,
        `latitude: ${r.latitude}`,
        `longitude: ${r.longitude}`,
        `sourceReference: ${JSON.stringify(r.sourceReference)}`,
      ];
      if (r.pointSubtype) parts.push(`pointSubtype: ${JSON.stringify(r.pointSubtype)}`);
      if (r.notes) parts.push(`notes: ${JSON.stringify(r.notes)}`);
      if (r.useCaseTags) parts.push(`useCaseTags: ${JSON.stringify(r.useCaseTags)}`);
      return `  ti({ ${parts.join(", ")} })`;
    })
    .join(",\n");

  const content = `/**
 * ${market} Travel Infrastructure delta records.
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

function writeFixtureScript(spec) {
  const { slug, country, region, market } = spec;
  const pascal = pascalFromSlug(slug);
  const prefix = slugConst(slug);
  const strategy = resolveBuildStrategy(spec);
  const content = `#!/usr/bin/env node
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  get${pascal}Candidates,
  ${prefix}_SUBMARKETS,
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

const strategyMap = {
  MARKET_BY_MARKET: BUILD_STRATEGY_TYPES.MARKET_BY_MARKET,
  ISLAND_COUNTRYWIDE: BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE,
  CORRIDOR_BASED: BUILD_STRATEGY_TYPES.CORRIDOR_BASED,
};

const fixture = {
  country: ${JSON.stringify(country)},
  region: ${JSON.stringify(region)},
  market: MARKET,
  buildStrategy: strategyMap[${JSON.stringify(strategy)}] || BUILD_STRATEGY_TYPES.MARKET_BY_MARKET,
  submarkets: ${prefix}_SUBMARKETS,
  firstPassTargets: config?.targets || {},
  governanceRequired: true,
  googlePreImportVerificationRecommended: true,
  generatedAt: new Date().toISOString().slice(0, 10),
  status: "candidate_pre_verification",
  summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
  points,
};

for (const rel of [
  "fixtures/demand-anchors-${slug}-candidates.json",
  "public/fixtures/demand-anchors-${slug}-candidates.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\\n");
}

console.log("${market} candidates written:", points.length);
`;
  writeFileSync(join(scriptsDir, `build-${slug}-fixtures.mjs`), content);
}

function writeTiScript(spec) {
  const { slug, market } = spec;
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
  "fixtures/travel-infrastructure-${slug}-real.json",
  "public/fixtures/travel-infrastructure-${slug}-real.json",
]) {
  writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\\n");
}

console.log("${market} TI:", fixture.points.length, "records");
`;
  writeFileSync(join(scriptsDir, `build-${slug}-ti-fixtures.mjs`), content);
}

for (const spec of ALL_MARKET_BUILD_SPECS) {
  writeGovernance(spec);
  writeCandidates(spec);
  writeCorrections(spec);
  writeTiDelta(spec);
  writeFixtureScript(spec);
  writeTiScript(spec);
  console.log("Generated:", spec.market);
}

console.log("Done:", ALL_MARKET_BUILD_SPECS.length, "market builds");
