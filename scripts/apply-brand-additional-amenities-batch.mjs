/**
 * Populate Brand Setup - Brand Standards → Additional Amenities (brand-specific).
 *
 *   node scripts/apply-brand-additional-amenities-batch.mjs --dry-run --all
 *   node scripts/apply-brand-additional-amenities-batch.mjs --all --overwrite
 *   node scripts/apply-brand-additional-amenities-batch.mjs --brand "Kimpton Hotels" --overwrite
 *
 * Default: blank-only. --overwrite replaces the multi-select with the profile list.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import {
  ADDITIONAL_AMENITIES_ALLOWED,
  getBrandAdditionalAmenities,
} from "./lib/brand-additional-amenities-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_STD = "Brand Setup - Brand Standards";
const LINK = "Brand Setup - Brand Standards";
const FIELD = "Additional Amenities";

function parseArgs(argv) {
  const args = argv.slice(2);
  const bi = args.indexOf("--brand");
  const all = args.includes("--all");
  return {
    dryRun: args.includes("--dry-run"),
    overwrite: args.includes("--overwrite") || args.includes("--correct"),
    all,
    activeOnly: args.includes("--active-only") || (!all && bi < 0),
    brandFilter: bi >= 0 ? String(args[bi + 1] || "").trim() : "",
  };
}

function emptyAmen(v) {
  return v === undefined || v === null || (Array.isArray(v) && v.length === 0) || v === "";
}

function sameSet(a, b) {
  const aa = [...(a || [])].map(String).sort();
  const bb = [...(b || [])].map(String).sort();
  return aa.length === bb.length && aa.every((x, i) => x === bb[i]);
}

async function getMetaChoices(baseId, apiKey) {
  const r = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!r.ok) throw new Error(`Meta ${r.status}`);
  const j = await r.json();
  const t = j.tables.find((x) => x.name === TABLE_STD);
  const f = t?.fields?.find((x) => x.name === FIELD);
  return (f?.options?.choices || []).map((c) => c.name).filter((c) => String(c).trim());
}

function filterToMeta(list, metaChoices) {
  const allow = new Set(metaChoices.length ? metaChoices : ADDITIONAL_AMENITIES_ALLOWED);
  return list.filter((x) => allow.has(x));
}

async function loadBasics(base, opts) {
  const fields = ["Brand Name", "Brand Status", "Parent Company", LINK];
  if (opts.brandFilter) {
    const esc = opts.brandFilter.replace(/"/g, '\\"');
    return base(TABLE_BASICS)
      .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1, fields })
      .firstPage();
  }
  const rows = [];
  if (opts.activeOnly) {
    await base(TABLE_BASICS)
      .select({ filterByFormula: BRAND_STATUS_ACTIVE_FORMULA, fields })
      .eachPage((p, n) => {
        rows.push(...p);
        n();
      });
    return rows;
  }
  await base(TABLE_BASICS)
    .select({ fields })
    .eachPage((p, n) => {
      rows.push(...p);
      n();
    });
  return rows;
}

async function main() {
  const opts = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const base = new Airtable({ apiKey }).base(baseId);
  const metaChoices = await getMetaChoices(baseId, apiKey);
  const basics = await loadBasics(base, opts);

  const results = [];
  let updated = 0;

  for (const row of basics) {
    const name = String(row.fields["Brand Name"] || "").trim();
    const parent = String(row.fields["Parent Company"] || "").trim();
    const status = row.fields["Brand Status"] || null;
    const stdId = row.fields[LINK]?.[0];
    if (!stdId) {
      results.push({ brandName: name, action: "skip", reason: "no_standards_link" });
      continue;
    }

    const { amenities, resolveSource } = getBrandAdditionalAmenities(name, parent);
    const next = filterToMeta(amenities, metaChoices);
    if (!next.length) {
      results.push({ brandName: name, action: "skip", reason: "empty_profile_after_meta_filter" });
      continue;
    }

    const existing = await base(TABLE_STD).find(stdId);
    const cur = existing.fields[FIELD];
    const isEmpty = emptyAmen(cur);

    if (!opts.overwrite && !isEmpty) {
      results.push({
        brandName: name,
        action: "noop",
        resolveSource,
        currentCount: Array.isArray(cur) ? cur.length : 0,
      });
      continue;
    }
    if (opts.overwrite && sameSet(cur, next)) {
      results.push({ brandName: name, action: "noop-same", resolveSource });
      continue;
    }

    const entry = {
      brandName: name,
      status,
      parentCompany: parent || null,
      standardsRecordId: stdId,
      resolveSource,
      amenities: next,
      previous: isEmpty ? [] : cur,
    };

    if (opts.dryRun) {
      entry.action = "dry-run";
      results.push(entry);
      console.log(
        `[dry-run] ${name} (${status}) src=${resolveSource} n=${next.length} overwrite=${opts.overwrite && !isEmpty}`
      );
      continue;
    }

    await base(TABLE_STD).update(stdId, { [FIELD]: next }, { typecast: true });
    entry.action = "updated";
    results.push(entry);
    updated += 1;
    console.log(`Updated ${name} amenities=${next.length} src=${resolveSource}`);
  }

  const out = path.join(
    ROOT,
    "reports",
    opts.dryRun
      ? "brand-additional-amenities-dry-run.json"
      : "brand-additional-amenities-apply.json"
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(
    out,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: opts.dryRun ? "dry-run" : opts.overwrite ? "overwrite" : "blank-only",
        metaChoiceCount: metaChoices.length,
        brands: basics.length,
        updated,
        results,
      },
      null,
      2
    )
  );
  console.log(`Done brands=${basics.length} updated=${updated}`);
  console.log(`Wrote ${out}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
