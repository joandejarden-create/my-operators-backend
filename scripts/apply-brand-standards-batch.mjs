/**
 * Populate / correct Brand Setup - Brand Standards (excludes Additional Amenities).
 *
 *   node scripts/apply-brand-standards-batch.mjs --dry-run --all --correct
 *   node scripts/apply-brand-standards-batch.mjs --all --correct
 *   node scripts/apply-brand-standards-batch.mjs --brand "Kimpton Hotels" --correct
 *
 * Default: blank-only. --correct also overwrites clear mismatches.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import { getBrandStandardsProfile } from "./lib/brand-standards-profiles.mjs";
import {
  BRAND_STANDARDS_SELECT_COLS,
  WRITABLE_BRAND_STANDARDS_COLS,
  buildBrandStandardsFieldsFromProfile,
  diffBrandStandardsFields,
  isBrandStandardsFieldEmpty,
} from "./lib/build-brand-standards-fields.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_STD = "Brand Setup - Brand Standards";
const LINK_FIELD = "Brand Setup - Brand Standards";

const NEVER_WRITE = new Set(["Additional Amenities"]);

function parseArgs(argv) {
  const args = argv.slice(2);
  const bi = args.indexOf("--brand");
  const all = args.includes("--all");
  return {
    dryRun: args.includes("--dry-run"),
    correct: args.includes("--correct") || args.includes("--overwrite"),
    sourcedCorrect: args.includes("--sourced-correct"),
    all,
    activeOnly: args.includes("--active-only") || (!all && bi < 0),
    brandFilter: bi >= 0 ? String(args[bi + 1] || "").trim() : "",
  };
}

async function getMetaTable(baseId, apiKey, tableName) {
  const r = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!r.ok) throw new Error(`Meta API ${r.status}`);
  const j = await r.json();
  const t = j.tables.find((x) => x.name === tableName);
  if (!t) throw new Error(`Table not found: ${tableName}`);
  return t;
}

async function loadBasics(base, opts) {
  const fields = [
    "Brand Name",
    "Brand Status",
    "Parent Company",
    "Hotel Chain Scale",
    "Brand Model",
    "Hotel Service Model",
    "Brand Architecture",
    LINK_FIELD,
  ];
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
  for (let attempt = 0; attempt < 40; attempt++) {
    if (!Object.keys(payload).length) return { updated: {} };
    try {
      await base(TABLE_STD).update(recordId, payload, { typecast: true });
      return { updated: payload };
    } catch (err) {
      const msg = String(err.message || err);
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const m = msg.match(/Unknown field name: "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          console.warn(`  Skip unknown field: ${m[1]}`);
          delete payload[m[1]];
          continue;
        }
      }
      if (err.error === "INVALID_VALUE_FOR_COLUMN" || err.error === "INVALID_MULTIPLE_CHOICE_OPTIONS") {
        const m = msg.match(/Field "([^"]+)"/);
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
  const metaTable = await getMetaTable(baseId, apiKey, TABLE_STD);
  const metaFieldNames = new Set(metaTable.fields.map((f) => f.name));
  const metaChoices = {};
  for (const col of BRAND_STANDARDS_SELECT_COLS) {
    const f = metaTable.fields.find((x) => x.name === col);
    metaChoices[col] = (f?.options?.choices || [])
      .map((c) => c.name)
      .filter((c) => String(c).trim() !== "");
  }

  const basicsRows = await loadBasics(base, opts);
  const proposals = [];
  const results = [];
  let writeCount = 0;
  let blankCount = 0;
  let correctCount = 0;

  for (const row of basicsRows) {
    const name = String(row.fields["Brand Name"] || "").trim();
    const status = row.fields["Brand Status"] || null;
    const parent = String(row.fields["Parent Company"] || "").trim();
    const stdId = Array.isArray(row.fields[LINK_FIELD]) ? row.fields[LINK_FIELD][0] : null;

    if (!stdId) {
      results.push({ brandName: name, status, action: "skip", reason: "no_standards_link" });
      continue;
    }

    const { profile, resolveSource } = getBrandStandardsProfile(name, parent, {
      chainScale: row.fields["Hotel Chain Scale"],
      brandModel: row.fields["Brand Model"],
      serviceModel: row.fields["Hotel Service Model"],
      architecture: row.fields["Brand Architecture"],
    });

    const { fields: expected, resolved } = buildBrandStandardsFieldsFromProfile(
      profile,
      metaChoices,
      proposals,
      name
    );

    for (const k of Object.keys(expected)) {
      if (!metaFieldNames.has(k) || NEVER_WRITE.has(k)) delete expected[k];
      if (!WRITABLE_BRAND_STANDARDS_COLS.includes(k)) delete expected[k];
    }

    const existing = await base(TABLE_STD).find(stdId);
    const mismatches = diffBrandStandardsFields(expected, existing.fields);

    const toWrite = {};
    const filledBlank = [];
    const corrected = [];

    for (const [col, want] of Object.entries(expected)) {
      const cur = existing.fields[col];
      if (isBrandStandardsFieldEmpty(cur)) {
        toWrite[col] = want;
        filledBlank.push(col);
        continue;
      }
      if (!opts.correct) continue;
      if (opts.sourcedCorrect) {
        const sourced =
          profile.sourceTier === "brand-override" ||
          profile.sourceTier === "fdd" ||
          resolveSource === "brand-override";
        if (!sourced) continue;
      }
      if (!mismatches.find((m) => m.column === col)) continue;
      toWrite[col] = want;
      corrected.push({ column: col, from: cur, to: want });
    }

    const entry = {
      brandName: name,
      status,
      parentCompany: parent || null,
      standardsRecordId: stdId,
      resolveSource,
      resolved,
      mismatchCount: mismatches.length,
      fieldCountToWrite: Object.keys(toWrite).length,
      filledBlank,
      corrected,
      fields: toWrite,
    };

    if (!Object.keys(toWrite).length) {
      entry.action = "noop";
      results.push(entry);
      continue;
    }

    if (opts.dryRun) {
      entry.action = "dry-run";
      results.push(entry);
      writeCount += Object.keys(toWrite).length;
      blankCount += filledBlank.length;
      correctCount += corrected.length;
      console.log(
        `[dry-run] ${name} (${status}) src=${resolveSource} blank=${filledBlank.length} correct=${corrected.length}`
      );
      continue;
    }

    const { updated } = await updateWithPruning(base, stdId, toWrite);
    entry.action = "updated";
    entry.fieldCountWritten = Object.keys(updated).length;
    entry.fields = updated;
    results.push(entry);
    writeCount += Object.keys(updated).length;
    blankCount += filledBlank.length;
    correctCount += corrected.length;
    console.log(
      `Updated ${name} blank=${filledBlank.length} correct=${corrected.length} written=${Object.keys(updated).length}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: opts.dryRun
      ? "dry-run"
      : opts.correct
        ? opts.sourcedCorrect
          ? "apply-blank+sourced-correct"
          : "apply-blank+correct"
        : "apply-blank-only",
    summary: {
      brands: basicsRows.length,
      writeFields: writeCount,
      blankFills: blankCount,
      corrections: correctCount,
      skippedNoLink: results.filter((r) => r.reason === "no_standards_link").length,
    },
    optionProposals: proposals.slice(0, 80),
    results,
  };

  const out = path.join(
    ROOT,
    "reports",
    opts.dryRun ? "brand-standards-all-brands-dry-run.json" : "brand-standards-all-brands-apply.json"
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(
    `Done brands=${basicsRows.length} writeFields=${writeCount} blank=${blankCount} correct=${correctCount}`
  );
  console.log(`Wrote ${out}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
