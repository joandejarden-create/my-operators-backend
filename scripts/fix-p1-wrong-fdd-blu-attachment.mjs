/**
 * Remove incorrect Radisson Blu FDD attachment from brands with no brand-specific FDD on file.
 *
 *   node scripts/fix-p1-wrong-fdd-blu-attachment.mjs --dry-run
 *   node scripts/fix-p1-wrong-fdd-blu-attachment.mjs --apply
 */
import "../load-env.js";
import Airtable from "airtable";
import { clearBrandDetailCache } from "../api/brand-library.js";
import { FDD_SLOT_KEY, FDD_TITLE } from "./lib/choice-fdd-materials-config.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";

/** Brands with no dedicated FDD PDF in Dealality folder — must not show Radisson Blu FDD. */
const BRANDS_TO_FIX = ["Park Plaza by Choice", "Radisson Collection by Choice"];

const WRONG_FILENAME_PATTERN = /radisson blu fdd/i;

function parseArgs(argv) {
  return { apply: argv.includes("--apply"), dryRun: !argv.includes("--apply") || argv.includes("--dry-run") };
}

function isFddRow(rec) {
  return (
    String(rec.get("Slot Key") || "").trim() === FDD_SLOT_KEY &&
    /franchise disclosure document/i.test(String(rec.get("Title") || ""))
  );
}

function hasWrongBluAttachment(imgs) {
  if (!Array.isArray(imgs) || !imgs.length) return false;
  const fn = String(imgs[0]?.filename || "");
  return WRONG_FILENAME_PATTERN.test(fn);
}

async function fetchPresentation(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  for (const formula of [`{Brand Name} = "${esc}"`, `{Brand} = "${esc}"`]) {
    try {
      for (const r of await base(TABLE).select({ filterByFormula: formula, maxRecords: 500 }).all()) {
        if (!seen.has(r.id)) {
          seen.add(r.id);
          merged.push(r);
        }
      }
    } catch {
      /* schema */
    }
  }
  return merged;
}

async function main() {
  const { apply, dryRun } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

  console.log(`${dryRun ? "DRY RUN" : "APPLY"} — clear wrong Radisson Blu FDD on ${BRANDS_TO_FIX.length} brand(s)\n`);

  let fixed = 0;
  let skipped = 0;

  for (const brandName of BRANDS_TO_FIX) {
    console.log(brandName);
    const basics = await base(BASICS)
      .select({ filterByFormula: `{Brand Name} = "${brandName.replace(/"/g, '\\"')}"`, maxRecords: 1 })
      .firstPage();
    if (!basics.length) {
      console.log("  skip: no Brand Basics row");
      skipped++;
      continue;
    }

    const rows = await fetchPresentation(base, brandName);
    const fddRow = rows.find(isFddRow);
    if (!fddRow) {
      console.log("  skip: no FDD materials.file row");
      skipped++;
      continue;
    }

    const imgs = fddRow.get("Image");
    if (!hasWrongBluAttachment(imgs)) {
      console.log(`  skip: attachment is not Radisson Blu FDD (${imgs?.[0]?.filename || "none"})`);
      skipped++;
      continue;
    }

    const body =
      "PDF · Brand-specific FDD not on file in Dealality reference folder.\n" +
      "Badge: Unverified by Brand\n" +
      "Note: Removed incorrect Radisson Blu FDD attachment — source Park Plaza / Radisson Collection FDD when available.";

    if (dryRun) {
      console.log(`  would clear Image on ${fddRow.id} (${imgs[0].filename})`);
      console.log(`  would set Body: ${body.split("\n")[0]}…`);
      fixed++;
      continue;
    }

    await base(TABLE).update(fddRow.id, {
      Image: [],
      Body: body,
    });
    clearBrandDetailCache(basics[0].id);
    clearBrandDetailCache(brandName);
    console.log(`  cleared wrong attachment on ${fddRow.id}`);
    fixed++;
  }

  console.log(`\n${dryRun ? "Would fix" : "Fixed"} ${fixed}; skipped ${skipped}.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
