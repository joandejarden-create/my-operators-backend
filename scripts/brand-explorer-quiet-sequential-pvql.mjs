#!/usr/bin/env node
/**
 * Quiet sequential PVQL for Active universe — one brand at a time with delay.
 * Avoids parallel audit chains that trip Airtable 429s.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { evaluateBrandPublicVisibility } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";
import { listActiveUniverseSlugs } from "../lib/partner-intelligence/brand-explorer-active-universe.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports", "brand-explorer-public-visibility-quality-lock-quiet.json");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const argv = process.argv.slice(2);
const brandsIdx = argv.indexOf("--brands");
const only =
  brandsIdx >= 0 && argv[brandsIdx + 1]
    ? argv[brandsIdx + 1]
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : null;

const allSlugs = await listActiveUniverseSlugs();
const slugs = (only || allSlugs).filter(Boolean);
console.log(`[quiet-pvql] active_slugs=${allSlugs.length} auditing=${slugs.length}`);
const results = [];
for (const slug of slugs) {
  process.stdout.write(`PVQL ${slug}... `);
  try {
    const row = await evaluateBrandPublicVisibility(slug);
    results.push(row);
    const fails = (row.failures || []).slice(0, 4).join("|");
    console.log(
      `${row.lockPass ? "PASS" : "FAIL"} ${row.publicDisplayState} full=${row.shouldRenderFullProfile} ${fails}`
    );
  } catch (e) {
    console.log(`ERR ${e.message}`);
    results.push({
      slug,
      lockPass: false,
      failures: [`exception:${e.message}`],
      error: e.message,
      shouldRenderFullProfile: false,
      publicFullProfile: false,
    });
  }
  await sleep(2000);
}

const publicFull = results.filter(
  (r) => r.shouldRenderFullProfile === true || r.publicFullProfile === true
);
const fail = publicFull.filter((r) => r.lockPass !== true);
const notFull = results.filter(
  (r) => !(r.shouldRenderFullProfile === true || r.publicFullProfile === true)
);
const scopedUniverse = !only;
const report = {
  version: "quiet-sequential-pvql-v1",
  generatedAt: new Date().toISOString(),
  dryRun: true,
  summary: {
    overallPass:
      fail.length === 0 &&
      notFull.length === 0 &&
      (scopedUniverse ? publicFull.length === allSlugs.length : publicFull.length === slugs.length),
    publicFullProfileCount: publicFull.length,
    scopedCount: slugs.length,
    universeActiveCount: allSlugs.length,
    brandsFilter: only,
    hardFails: fail.map((r) => ({ slug: r.slug, failures: r.failures })),
    notPublicFull: notFull.map((r) => ({
      slug: r.slug,
      display: r.publicDisplayState,
      failures: r.failures,
    })),
  },
  brands: results,
};
fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report.summary, null, 2));
console.log(`Wrote ${OUT}`);
process.exit(report.summary.overallPass ? 0 : 1);
