/**
 * Populate Brand Setup - Project Fit → Acceptable Building Types (brand-specific).
 *
 *   node scripts/apply-brand-acceptable-building-types-batch.mjs --dry-run --all
 *   node scripts/apply-brand-acceptable-building-types-batch.mjs --all --correct
 *   node scripts/apply-brand-acceptable-building-types-batch.mjs --brand "Kimpton Hotels" --correct
 *
 * Default: blank-only. --correct / --overwrite replaces mismatches with the profile list.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import {
  ACCEPTABLE_BUILDING_TYPES_ALLOWED,
  getBrandAcceptableBuildingTypes,
} from "./lib/brand-acceptable-building-types-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_FIT = "Brand Setup - Project Fit";
const LINK = "Brand Setup - Project Fit";
const FIELD = "Acceptable Building Types";

function parseArgs(argv) {
  const args = argv.slice(2);
  const bi = args.indexOf("--brand");
  const all = args.includes("--all");
  return {
    dryRun: args.includes("--dry-run"),
    correct: args.includes("--correct") || args.includes("--overwrite"),
    all,
    activeOnly: args.includes("--active-only") || (!all && bi < 0),
    brandFilter: bi >= 0 ? String(args[bi + 1] || "").trim() : "",
  };
}

function emptyVal(v) {
  return v === undefined || v === null || (Array.isArray(v) && v.length === 0) || v === "";
}

function sameSet(a, b) {
  const aa = [...(a || [])].map(String).sort();
  const bb = [...(b || [])].map(String).sort();
  return aa.length === bb.length && aa.every((x, i) => x === bb[i]);
}

async function getMetaChoices(baseId, apiKey) {
  let lastErr;
  for (let attempt = 0; attempt < 6; attempt++) {
    const r = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (r.status === 429) {
      const wait = 2000 * (attempt + 1);
      console.warn(`Meta rate-limited; retry in ${wait}ms`);
      await new Promise((res) => setTimeout(res, wait));
      lastErr = new Error("Meta API 429");
      continue;
    }
    if (!r.ok) throw new Error(`Meta ${r.status}`);
    const j = await r.json();
    const t = j.tables.find((x) => x.name === TABLE_FIT);
    const f = t?.fields?.find((x) => x.name === FIELD);
    return (f?.options?.choices || []).map((c) => c.name).filter((c) => String(c).trim());
  }
  throw lastErr || new Error("Meta API failed");
}

function filterToMeta(list, metaChoices) {
  const allow = new Set(metaChoices.length ? metaChoices : ACCEPTABLE_BUILDING_TYPES_ALLOWED);
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

async function updateWithPruning(base, recordId, fields) {
  let payload = { ...fields };
  for (let attempt = 0; attempt < 20; attempt++) {
    if (!Object.keys(payload).length) return { updated: {} };
    try {
      await base(TABLE_FIT).update(recordId, payload, { typecast: true });
      return { updated: payload };
    } catch (err) {
      const msg = String(err.message || err);
      if (err.error === "UNKNOWN_FIELD_NAME" || err.error === "INVALID_MULTIPLE_CHOICE_OPTIONS") {
        const m = msg.match(/Field "([^"]+)"/) || msg.match(/Unknown field name: "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          console.warn(`  Skip invalid: ${m[1]}`);
          delete payload[m[1]];
          continue;
        }
      }
      throw err;
    }
  }
  return { updated: {} };
}

async function main() {
  const opts = parseArgs(process.argv);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const base = new Airtable({ apiKey }).base(baseId);
  const metaChoices = await getMetaChoices(baseId, apiKey);
  const basicsRows = await loadBasics(base, opts);

  const results = [];
  let updated = 0;
  let blankFills = 0;
  let corrections = 0;

  for (const row of basicsRows) {
    const name = String(row.fields["Brand Name"] || "").trim();
    const status = row.fields["Brand Status"] || null;
    const parent = String(row.fields["Parent Company"] || "").trim();
    const fitId = Array.isArray(row.fields[LINK]) ? row.fields[LINK][0] : null;

    if (!fitId) {
      results.push({ brandName: name, status, action: "skip", reason: "no_project_fit_link" });
      continue;
    }

    const { buildingTypes, resolveSource, segment } = getBrandAcceptableBuildingTypes(name, parent);
    const want = filterToMeta(buildingTypes, metaChoices);
    if (!want.length) {
      results.push({
        brandName: name,
        status,
        action: "skip",
        reason: "no_valid_meta_choices",
        resolveSource,
      });
      continue;
    }

    const existing = await base(TABLE_FIT).find(fitId);
    const cur = existing.fields[FIELD];
    const isEmpty = emptyVal(cur);
    const matches = sameSet(cur, want);

    const entry = {
      brandName: name,
      status,
      parentCompany: parent || null,
      projectFitRecordId: fitId,
      resolveSource,
      segment,
      buildingTypes: want,
      current: Array.isArray(cur) ? cur : cur ? [cur] : [],
      blank: isEmpty,
      mismatch: !isEmpty && !matches,
    };

    if (matches) {
      entry.action = "noop";
      results.push(entry);
      continue;
    }

    if (!isEmpty && !opts.correct) {
      entry.action = "skip_existing";
      results.push(entry);
      continue;
    }

    const toWrite = { [FIELD]: want };
    if (opts.dryRun) {
      entry.action = "dry-run";
      entry.overwrite = opts.correct && !isEmpty;
      results.push(entry);
      updated += 1;
      if (isEmpty) blankFills += 1;
      else corrections += 1;
      console.log(
        `[dry-run] ${name} src=${resolveSource} n=${want.length} ${isEmpty ? "blank" : "correct"} → ${want.join(", ")}`
      );
      continue;
    }

    await updateWithPruning(base, fitId, toWrite);
    entry.action = "updated";
    entry.overwrite = opts.correct && !isEmpty;
    results.push(entry);
    updated += 1;
    if (isEmpty) blankFills += 1;
    else corrections += 1;
    console.log(
      `Updated ${name} src=${resolveSource} n=${want.length} ${isEmpty ? "blank" : "correct"}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: opts.dryRun
      ? "dry-run"
      : opts.correct
        ? "apply-blank+correct"
        : "apply-blank-only",
    metaChoiceCount: metaChoices.length,
    metaChoices,
    summary: {
      brands: basicsRows.length,
      updated,
      blankFills,
      corrections,
      skippedNoLink: results.filter((r) => r.reason === "no_project_fit_link").length,
    },
    results,
  };

  const out = path.join(
    ROOT,
    "reports",
    opts.dryRun
      ? "brand-acceptable-building-types-dry-run.json"
      : "brand-acceptable-building-types-apply.json"
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(
    `Done brands=${basicsRows.length} updated=${updated} blank=${blankFills} correct=${corrections}`
  );
  console.log(`Wrote ${out}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
