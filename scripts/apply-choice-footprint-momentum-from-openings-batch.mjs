/**

 * Apply footprint.momentum only from curated Choice press announcements (media.choicehotels.com).

 * Clears momentum slots when no curated announcements exist — never uses property-page URLs.

 *

 * Radisson by Choice / Radisson Blu by Choice: fixtures/brand-explorer-presentation-radisson*-footprint-momentum.json

 *

 * Usage:

 *   node scripts/apply-choice-footprint-momentum-from-openings-batch.mjs --dry-run

 *   node scripts/apply-choice-footprint-momentum-from-openings-batch.mjs

 *   node scripts/apply-choice-footprint-momentum-from-openings-batch.mjs --brand "Ascend Hotel Collection"

 */

import fs from "fs";

import os from "os";

import path from "path";

import { spawnSync } from "child_process";

import { fileURLToPath } from "url";

import "../load-env.js";

import Airtable from "airtable";

import {

  buildCuratedMomentumPresentationRows,

  hasCuratedMomentum,

} from "./lib/choice-chi-footprint-momentum-curated.mjs";



const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ROOT = path.resolve(__dirname, "..");

const APPLY = path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs");

const BASICS = "Brand Setup - Brand Basics";

const TABLE = "Brand Setup - Brand Explorer Presentation";



function parseArgs(argv) {

  const i = argv.indexOf("--brand");

  return {

    dryRun: argv.includes("--dry-run"),

    brandFilter: i >= 0 ? String(argv[i + 1] || "").trim() : "",

  };

}



function slotKeyFromRecord(rec) {

  return String(rec.get("Slot Key") || "").trim();

}



async function listChiBrands(base) {

  const rows = await base(BASICS).select({ maxRecords: 500 }).all();

  return rows

    .filter((r) => String(r.get("Parent Company") || "").includes("Choice Hotels International"))

    .map((r) => ({ id: r.id, name: String(r.get("Brand Name") || "").trim() }))

    .filter((r) => Boolean(r.name))

    .sort((a, b) => a.name.localeCompare(b.name));

}



async function selectRowsForBrand(base, brandName) {

  const esc = brandName.replace(/"/g, '\\"');

  const merged = [];

  const seen = new Set();

  for (const formula of [`{Brand Name} = "${esc}"`, `{Brand} = "${esc}"`]) {

    try {

      const rows = await base(TABLE).select({ filterByFormula: formula, maxRecords: 500 }).all();

      for (const r of rows) {

        if (!seen.has(r.id)) {

          seen.add(r.id);

          merged.push(r);

        }

      }

    } catch {

      /* optional fields */

    }

  }

  return merged;

}



async function deleteRecords(base, ids, dryRun) {

  const chunk = 10;

  for (let i = 0; i < ids.length; i += chunk) {

    const slice = ids.slice(i, i + chunk);

    if (dryRun) continue;

    await base(TABLE).destroy(slice);

  }

}



/**

 * Remove footprint.momentum and footprint.momentum_label so UI shows empty Recent Momentum.

 */

async function clearMomentumSlots(base, brandName, dryRun) {

  const existing = await selectRowsForBrand(base, brandName);

  const toDrop = existing.filter((rec) => {

    const sk = slotKeyFromRecord(rec);

    return sk === "footprint.momentum" || sk === "footprint.momentum_label";

  });

  if (!toDrop.length) {

    console.log(`  (no momentum rows to clear)`);

    return 0;

  }

  console.log(`  Clearing ${toDrop.length} momentum row(s)…`);

  await deleteRecords(

    base,

    toDrop.map((r) => r.id),

    dryRun

  );

  return toDrop.length;

}



function runApply({ dryRun, brandName, brandRecordId, fixturePath }) {

  const args = [

    APPLY,

    "--brand-record-id",

    brandRecordId,

    "--fixture",

    fixturePath,

    "--replace-slot-prefix",

    "footprint.momentum",

  ];

  if (dryRun) args.push("--dry-run");

  const res = spawnSync(process.execPath, args, { cwd: ROOT, stdio: "inherit", env: process.env });

  if (res.status !== 0) throw new Error(`Failed for ${brandName}`);

}



async function main() {

  const { dryRun, brandFilter } = parseArgs(process.argv);

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

  let brands = await listChiBrands(base);

  if (brandFilter) {

    brands = brands.filter((b) => b.name === brandFilter);

    if (!brands.length) throw new Error(`No matching CHI brand: ${brandFilter}`);

  }



  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dc-choice-momentum-"));

  let applied = 0;

  let cleared = 0;

  try {

    for (const brandRec of brands) {

      const brandName = brandRec.name;

      console.log(`\n=== ${brandName} ===`);



      if (!hasCuratedMomentum(brandName)) {

        await clearMomentumSlots(base, brandName, dryRun);

        cleared += 1;

        continue;

      }



      const rows = buildCuratedMomentumPresentationRows(brandName);

      const itemCount = rows.filter((r) => r.slotKey === "footprint.momentum").length;

      const fixturePath = path.join(tmpDir, `${brandName.replace(/[^\w.-]+/g, "_")}.json`);

      fs.writeFileSync(

        fixturePath,

        JSON.stringify(

          {

            targetBrandBasicsName: brandName,

            instructions: "Curated Choice press announcements (media.choicehotels.com) only",

            rows,

          },

          null,

          2

        ),

        "utf8"

      );



      console.log(`  Applying ${itemCount} announcement(s) from fixture…`);

      runApply({ dryRun, brandName, brandRecordId: brandRec.id, fixturePath });

      applied += 1;

    }

  } finally {

    fs.rmSync(tmpDir, { recursive: true, force: true });

  }



  console.log(

    `\n${dryRun ? "Would apply" : "Applied"} curated momentum for ${applied} brand(s); ` +

      `${dryRun ? "would clear" : "cleared"} ${cleared} brand(s) with no press announcements.`

  );

}



main().catch((err) => {

  console.error(err);

  process.exit(1);

});


