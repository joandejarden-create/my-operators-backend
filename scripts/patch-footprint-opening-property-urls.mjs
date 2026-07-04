/**
 * Fix footprint.openings Body URLs missing property id (Radisson Blu fixture legacy).
 *
 *   node scripts/patch-footprint-opening-property-urls.mjs --dry-run
 *   node scripts/patch-footprint-opening-property-urls.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import { extractPropertyUrlFromBody } from "./lib/choice-hotel-page-image.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const SLOT = "footprint.openings";

/** Truncated suffix → full property id suffix (census / sitemap). */
const URL_SUFFIX_FIXES = [
  {
    test: /\/argentina\/san-carlos-de-bariloche\/radisson-blu-hotels\/?$/i,
    full: "https://www.choicehotels.com/argentina/san-carlos-de-bariloche/radisson-blu-hotels/aa022",
  },
  {
    test: /\/sao-paulo\/sao-paulo\/radisson-blu-hotels\/?$/i,
    full: "https://www.choicehotels.com/sao-paulo/sao-paulo/radisson-blu-hotels/br167",
  },
  {
    test: /\/chile\/santiago\/radisson-blu-hotels\/?$/i,
    full: "https://www.choicehotels.com/chile/santiago/radisson-blu-hotels/cl012",
  },
  {
    test: /\/aruba\/palm-beach\/radisson-blu-hotels\/?$/i,
    full: "https://www.choicehotels.com/aruba/palm-beach/radisson-blu-hotels/aw007",
  },
  {
    test: /\/minas-gerais\/belo-horizonte\/radisson-blu-hotels\/?$/i,
    full: "https://www.choicehotels.com/minas-gerais/belo-horizonte/radisson-blu-hotels/br154",
  },
];

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

function fixBodyUrl(body) {
  const current = extractPropertyUrlFromBody(body);
  if (!current) return { body, changed: false };
  for (const { test, full } of URL_SUFFIX_FIXES) {
    if (test.test(current.trim())) {
      const nextBody = body.replace(current, full);
      return { body: nextBody, changed: true, from: current, to: full };
    }
  }
  return { body, changed: false };
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const rows = await base(TABLE)
    .select({ filterByFormula: `{Slot Key} = "${SLOT}"`, maxRecords: 1000 })
    .all();

  let updated = 0;
  for (const row of rows) {
    const body = String(row.get("Body") || "");
    const { body: nextBody, changed, from, to } = fixBodyUrl(body);
    if (!changed) continue;
    const brand = String(row.get("Brand Name") || "").trim();
    const title = String(row.get("Title") || "").trim();
    console.log(`- ${brand}: ${title}`);
    console.log(`  ${from}`);
    console.log(`  → ${to}`);
    if (!dryRun) {
      await base(TABLE).update(row.id, { Body: nextBody });
    }
    updated += 1;
  }
  console.log(`${dryRun ? "Would update" : "Updated"} ${updated} row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
