#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs";
import { listActiveUniverseSlugs } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { evaluateBrandPublicVisibility } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";

const slugs = await listActiveUniverseSlugs();
console.log("active_universe_count", slugs.length);
const rows = [];
for (const slug of slugs) {
  process.stdout.write(`eval ${slug}... `);
  const r = await evaluateBrandPublicVisibility(slug);
  const line = {
    slug,
    pf: r.publicFullProfile === true,
    sRFP: r.shouldRenderFullProfile === true,
    lockPass: r.lockPass === true,
    fails: r.failures || [],
    display: r.publicDisplayState,
    cohort: r.cohort,
  };
  rows.push(line);
  console.log(JSON.stringify(line));
}
const summary = {
  active: slugs.length,
  publicFull: rows.filter((r) => r.pf).length,
  shouldRenderFullProfile: rows.filter((r) => r.sRFP).length,
  publicFullLockPass: rows.filter((r) => r.pf && r.lockPass).length,
  failing: rows
    .filter((r) => !(r.pf && r.lockPass))
    .map((r) => ({ slug: r.slug, pf: r.pf, sRFP: r.sRFP, fails: r.fails, display: r.display })),
  rows,
};
fs.writeFileSync(
  "reports/_tmp-active-universe-pvql-inventory.json",
  JSON.stringify(summary, null, 2)
);
console.log(JSON.stringify({ ...summary, rows: undefined }, null, 2));
