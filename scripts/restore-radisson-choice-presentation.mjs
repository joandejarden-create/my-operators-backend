/**

 * Restore Radisson (Choice) — all Brand Explorer tabs from owner-voice fixtures.

 * Fixes accidental stub overwrite of materials/gallery and duplicate openings apply.

 *

 *   node scripts/restore-radisson-choice-presentation.mjs

 *   node scripts/restore-radisson-choice-presentation.mjs --dry-run

 *   node scripts/restore-radisson-choice-presentation.mjs --with-images

 */

import fs from "fs";

import { spawnSync } from "child_process";

import path from "path";

import { fileURLToPath } from "url";

import { resolveProfileForAirtableName } from "./lib/choice-chi-brand-resolve.mjs";

import { buildFixture } from "./lib/choice-explorer-full-builder.mjs";



const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const APPLY = path.join(ROOT, "scripts/apply-brand-explorer-presentation-fixture.mjs");

const BASICS_APPLY = path.join(ROOT, "scripts/apply-radisson-brand-basics-brand-on-a-page.mjs");

const BRAND_ID = process.env.RADISSON_CHOICE_BASICS_ID || "recywbx1YQSTCPqW1";

const BRAND_AIRTABLE = "Radisson by Choice";

const BRAND_PROFILE = "Radisson (Choice)";

const dryRun = process.argv.includes("--dry-run");

const withImages = process.argv.includes("--with-images");



function run(cmd, args, label) {

  if (label) console.log(`\n>> ${label}`);

  const r = spawnSync(cmd, args, { cwd: ROOT, stdio: "inherit", env: process.env });

  if (r.status !== 0) process.exit(r.status ?? 1);

}



function runApply(fixture, prefix) {

  const args = [

    APPLY,

    "--brand-record-id",

    BRAND_ID,

    "--fixture",

    path.join(ROOT, fixture),

    "--replace-slot-prefix",

    prefix,

  ];

  if (dryRun) args.push("--dry-run");

  console.log(`\n>> ${BRAND_PROFILE} · ${prefix} · ${fixture}`);

  const r = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit", env: process.env });

  if (r.status !== 0) process.exit(r.status ?? 1);

}



const geoGrowth = "fixtures/brand-explorer-presentation-radisson-footprint-geo-growth.json";

const core = "fixtures/brand-explorer-presentation-radisson.example.json";

const portfolio = "fixtures/brand-explorer-presentation-radisson-portfolio-compliance-similar.json";



const steps = [

  [geoGrowth, "footprint.geo"],

  [geoGrowth, "footprint.region."],

  [geoGrowth, "footprint.growth"],

  [geoGrowth, "footprint.editorial"],

  ["fixtures/brand-explorer-presentation-radisson-case-studies.json", "materials.caseStudy"],

  [core, "hero."],

  [core, "operations."],

  [core, "overview."],

  [core, "valueOwners."],

  [core, "loyalty."],

  [core, "insight.summary"],

  [portfolio, "insight.similar"],

  [portfolio, "footprint.portfolio_mix"],

  [portfolio, "operations.compliance."],

  ["fixtures/brand-explorer-presentation-radisson-footprint-momentum.json", "footprint.momentum"],

  ["fixtures/brand-explorer-presentation-radisson-footprint-openings.json", "footprint.openings"],

  ["fixtures/brand-explorer-presentation-radisson-choice-materials.json", "materials.file"],

  ["fixtures/brand-explorer-presentation-radisson-choice-gallery.json", "materials.gallery."],

  ["fixtures/brand-explorer-presentation-standards-radisson-choice.json", "standards."],

];



console.log(

  `Restoring ${BRAND_PROFILE} (${BRAND_ID}) — full tab restore${dryRun ? " [dry-run]" : ""}…`

);



for (const [fixture, prefix] of steps) {

  runApply(fixture, prefix);

}



// Economics only — do not stub materials/gallery.

const stubFixture = buildFixture(resolveProfileForAirtableName(BRAND_AIRTABLE));

const economicsRows = stubFixture.rows.filter((r) => String(r.slotKey || "").startsWith("economics."));

const restoreEconomics = path.join(ROOT, "fixtures/.restore-radisson-choice-economics-slice.json");

fs.writeFileSync(

  restoreEconomics,

  JSON.stringify(

    {

      targetBrandBasicsName: BRAND_PROFILE,

      brandNameFallback: BRAND_PROFILE,

      rows: economicsRows,

    },

    null,

    2

  ) + "\n"

);

runApply("fixtures/.restore-radisson-choice-economics-slice.json", "economics.");

try {

  fs.unlinkSync(restoreEconomics);

} catch {

  /* ignore */

}



// Portfolio Context (overview.portfolio_context) + Brand Basics ladder fields.

if (!dryRun) {

  run(process.execPath, [

    path.join(ROOT, "scripts/apply-choice-portfolio-context-batch.mjs"),

    "--brand",

    BRAND_AIRTABLE,

  ], `${BRAND_AIRTABLE} · portfolio context`);



  run(process.execPath, [

    BASICS_APPLY,

    "--brand-record-id",

    BRAND_ID,

    "--fixture",

    path.join(ROOT, "fixtures/brand-basics-radisson-choice-portfolio-context.json"),

  ], `${BRAND_PROFILE} · Brand Basics portfolio ladder`);

} else {

  console.log("\n[dry-run] Would run portfolio context batch + brand basics portfolio fixture.");

}



if (withImages && !dryRun) {

  run(

    process.execPath,

    [path.join(ROOT, "scripts/restore-radisson-choice-materials.mjs"), "--images-only"],

    "Scenario, gallery, and case study images"

  );

  run(

    process.execPath,

    [

      path.join(ROOT, "scripts/attach-choice-footprint-opening-images.mjs"),

      "--brand",

      BRAND_AIRTABLE,

      "--force",

    ],

    "Footprint opening images"

  );

} else if (!dryRun) {

  console.log(

    "\nTip: re-run with --with-images to attach gallery + footprint + case study hero images."

  );

}



console.log("\nRestore finished. Review Brand Explorer UI for Radisson by Choice.");


