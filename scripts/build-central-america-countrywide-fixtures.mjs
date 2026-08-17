#!/usr/bin/env node
/**
 * Build Central America countrywide demand anchor candidate fixtures.
 *   node scripts/build-central-america-countrywide-fixtures.mjs
 *   node scripts/build-central-america-countrywide-fixtures.mjs --slug belize
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import { CENTRAL_AMERICA_COUNTRY_BUILDS } from "../lib/radar-buildout/central-america-build-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const slugArg = (() => {
  const idx = process.argv.indexOf("--slug");
  return idx >= 0 ? process.argv[idx + 1] : null;
})();

const jobs = slugArg
  ? CENTRAL_AMERICA_COUNTRY_BUILDS.filter((j) => j.slug === slugArg)
  : CENTRAL_AMERICA_COUNTRY_BUILDS;

if (!jobs.length) {
  console.error("No matching Central America build jobs.");
  process.exit(1);
}

function pascalFromSlug(slug) {
  return slug.split("-").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join("");
}

for (const job of jobs) {
  const pascal = pascalFromSlug(job.slug);
  const candidatesMod = await import(
    pathToFileURL(join(root, `lib/radar-buildout/${job.slug}-demand-anchors-candidates.js`)).href
  );
  const correctionsMod = await import(
    pathToFileURL(join(root, `lib/radar-buildout/${job.slug}-google-place-review-corrections.js`)).href
  );

  const getCandidates = candidatesMod[`get${pascal}Candidates`];
  const submarketKey = `${job.slug.toUpperCase().replace(/-/g, "_")}_SUBMARKETS`;
  const submarkets = candidatesMod[submarketKey] || job.submarkets;
  const applyCorrections = correctionsMod[`apply${pascal}PlaceReviewCorrections`];
  const config = getCountryConfig(job.country);

  const points = applyCorrections(getCandidates());
  const bySubmarket = {};
  const byPointType = {};
  const byCity = {};
  for (const p of points) {
    bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
    byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
    byCity[p.city] = (byCity[p.city] || 0) + 1;
  }

  const fixture = {
    country: job.country,
    region: "Central America",
    market: job.market,
    buildStrategy: BUILD_STRATEGY_TYPES.CORRIDOR_BASED,
    submarkets,
    firstPassTargets: config?.targets || {},
    governanceRequired: true,
    googlePreImportVerificationRecommended: true,
    generatedAt: new Date().toISOString().slice(0, 10),
    status: "candidate_pre_verification",
    summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
    points,
  };

  for (const rel of [
    `fixtures/demand-anchors-${job.slug}-countrywide-candidates.json`,
    `public/fixtures/demand-anchors-${job.slug}-countrywide-candidates.json`,
  ]) {
    writeFileSync(join(root, rel), `${JSON.stringify(fixture, null, 2)}\n`);
  }

  console.log(`${job.country} candidates written:`, points.length, bySubmarket);
}
