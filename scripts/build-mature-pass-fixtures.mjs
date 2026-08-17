#!/usr/bin/env node
/**
 * Build mature-pass candidate + TI delta fixtures from lib modules.
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  MATURE_PASS_BUILDS,
  MATURE_TI_ONLY_BUILDS,
} from "../lib/radar-buildout/mature-pass-build-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const slugArg = (() => {
  const idx = process.argv.indexOf("--slug");
  return idx >= 0 ? process.argv[idx + 1] : null;
})();

function pascalFromSlug(slug) {
  return slug
    .split("-")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join("");
}

function writeJson(rel, payload) {
  writeFileSync(join(root, rel), `${JSON.stringify(payload, null, 2)}\n`);
  const pub = rel.replace(/^fixtures\//, "public/fixtures/");
  writeFileSync(join(root, pub), `${JSON.stringify(payload, null, 2)}\n`);
}

async function buildDaCandidates(job) {
  const pascal = pascalFromSlug(job.slug);
  const mod = await import(
    pathToFileURL(
      join(root, `lib/radar-buildout/${job.slug}-demand-anchors-candidates.js`)
    ).href
  );
  let points = mod[`get${pascal}Candidates`]?.() || mod[`${job.slug.toUpperCase().replace(/-/g, "_")}_CANDIDATES`] || [];
  const correctionsMod = await import(
    pathToFileURL(
      join(root, `lib/radar-buildout/${job.slug}-google-place-review-corrections.js`)
    ).href
  ).catch(() => null);
  const applyFn = correctionsMod?.[`apply${pascal}PlaceReviewCorrections`];
  if (applyFn) points = applyFn(points);

  const submarketKey = `${job.slug.toUpperCase().replace(/-/g, "_")}_SUBMARKETS`;
  const submarkets = mod[submarketKey] || job.submarkets;
  const config = getCountryConfig(job.country);
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
    region: job.region,
    market: job.market,
    buildStrategy: BUILD_STRATEGY_TYPES.CORRIDOR_BASED,
    buildBatch: "mature-pass",
    submarkets,
    firstPassTargets: config?.targets || {},
    governanceRequired: true,
    googlePreImportVerificationRecommended: true,
    generatedAt: new Date().toISOString().slice(0, 10),
    status: "candidate_pre_verification",
    summary: { totalPoints: points.length, bySubmarket, byCity, byPointType },
    points,
  };

  writeJson(`fixtures/demand-anchors-${job.slug}-candidates.json`, fixture);
  console.log(`${job.slug} DA candidates:`, points.length, bySubmarket);
}

async function buildTiDelta(job) {
  const pascal = pascalFromSlug(job.slug);
  const mod = await import(
    pathToFileURL(
      join(root, `lib/radar-buildout/${job.slug}-travel-infrastructure-delta.js`)
    ).href
  );
  const buildFn =
    mod[`build${pascal}TiDeltaFixture`] ||
    mod[`build${pascal}TiMatureFixture`] ||
    mod.buildColombiaTiMatureFixture;
  const fixture = buildFn();
  writeJson(`fixtures/travel-infrastructure-${job.slug}-real.json`, fixture);
  console.log(`${job.slug} TI delta:`, fixture.points?.length || 0);
}

const daJobs = slugArg
  ? MATURE_PASS_BUILDS.filter((j) => j.slug === slugArg)
  : MATURE_PASS_BUILDS;
const tiOnlyJobs = slugArg
  ? MATURE_TI_ONLY_BUILDS.filter((j) => j.slug === slugArg)
  : MATURE_TI_ONLY_BUILDS;

for (const job of daJobs) {
  await buildDaCandidates(job);
  await buildTiDelta(job);
}
for (const job of tiOnlyJobs) {
  await buildTiDelta(job);
}

console.log("Mature pass fixture build complete.");
