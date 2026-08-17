/**
 * Populate / correct Brand Setup - Deal Terms for Brand Basics rows.
 *
 *   node scripts/apply-brand-deal-terms-batch.mjs --dry-run --all
 *   node scripts/apply-brand-deal-terms-batch.mjs --all --correct
 *   node scripts/apply-brand-deal-terms-batch.mjs --active-only
 *   node scripts/apply-brand-deal-terms-batch.mjs --brand "Curio Collection by Hilton" --correct
 *
 * Default: blank-only fills. `--correct` also overwrites cells that differ from the profile.
 * `--fdd-only-correct` with `--correct`: only overwrite when profile.sourceTier === 'fdd'
 *   (still blank-fills directional for empties).
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA, isBrandStatusActive } from "../lib/brand-status-active.js";
import {
  TABLE_BASICS,
  TABLE_DEAL,
  LINK_FIELD_DEAL,
  DEAL_TERMS_SELECT_COLUMNS,
  SKIP_DEAL_FIELDS,
  isEmptyDealValue,
} from "./lib/deal-terms-field-contract.mjs";
import { getDealTermsProfile } from "./lib/brand-deal-terms-profiles.mjs";
import {
  buildDealTermsFieldsFromProfile,
  diffDealTermsFields,
} from "./lib/build-deal-terms-fields.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

function parseArgs(argv) {
  const args = argv.slice(2);
  const bi = args.indexOf("--brand");
  const all = args.includes("--all");
  const activeOnly = args.includes("--active-only") || (!all && !args.includes("--under-review"));
  return {
    dryRun: args.includes("--dry-run"),
    correct: args.includes("--correct") || args.includes("--overwrite"),
    fddOnlyCorrect: args.includes("--fdd-only-correct") || args.includes("--sourced-correct"),
    all,
    activeOnly: all ? false : activeOnly,
    underReview: args.includes("--under-review"),
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
  const fields = ["Brand Name", "Brand Status", "Parent Company", LINK_FIELD_DEAL];
  const rows = [];
  if (opts.brandFilter) {
    const esc = opts.brandFilter.replace(/"/g, '\\"');
    const page = await base(TABLE_BASICS)
      .select({
        filterByFormula: `{Brand Name} = "${esc}"`,
        maxRecords: 1,
        fields,
      })
      .firstPage();
    return page;
  }
  if (opts.activeOnly) {
    await base(TABLE_BASICS)
      .select({ filterByFormula: BRAND_STATUS_ACTIVE_FORMULA, fields })
      .eachPage((page, next) => {
        rows.push(...page);
        next();
      });
    return rows;
  }
  await base(TABLE_BASICS)
    .select({ fields })
    .eachPage((page, next) => {
      rows.push(...page);
      next();
    });
  if (opts.underReview) {
    return rows.filter((r) => String(r.fields["Brand Status"] || "") === "Under Review");
  }
  return rows;
}

async function updateWithPruning(base, recordId, fields) {
  let payload = { ...fields };
  for (let attempt = 0; attempt < 30; attempt++) {
    if (!Object.keys(payload).length) return { updated: {} };
    try {
      await base(TABLE_DEAL).update(recordId, payload, { typecast: true });
      return { updated: payload };
    } catch (err) {
      const msg = String(err.message || err);
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const m = msg.match(/Unknown field name: "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          delete payload[m[1]];
          continue;
        }
      }
      if (err.error === "INVALID_VALUE_FOR_COLUMN") {
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
  const metaTable = await getMetaTable(baseId, apiKey, TABLE_DEAL);
  const metaFieldNames = new Set(metaTable.fields.map((f) => f.name));
  const metaChoices = {};
  for (const col of DEAL_TERMS_SELECT_COLUMNS) {
    const f = metaTable.fields.find((x) => x.name === col);
    metaChoices[col] = (f?.options?.choices || [])
      .map((c) => c.name)
      .filter((c) => String(c).trim() !== "");
  }

  const basicsRows = await loadBasics(base, opts);
  const proposals = [];
  const results = [];
  let writeCount = 0;
  let correctCount = 0;
  let blankCount = 0;

  for (const row of basicsRows) {
    const name = String(row.fields["Brand Name"] || "").trim();
    const status = row.fields["Brand Status"] || null;
    const parent = String(row.fields["Parent Company"] || "").trim();
    const dealId = Array.isArray(row.fields[LINK_FIELD_DEAL])
      ? row.fields[LINK_FIELD_DEAL][0]
      : null;

    if (!dealId) {
      results.push({ brandName: name, status, reason: "no_deal_terms_link", action: "skip" });
      continue;
    }

    const { profile, resolveSource } = getDealTermsProfile(name, parent);
    const { fields: expected, resolved } = buildDealTermsFieldsFromProfile(
      profile,
      metaChoices,
      proposals,
      name
    );

    // Drop columns missing in Meta
    for (const k of Object.keys(expected)) {
      if (!metaFieldNames.has(k) || SKIP_DEAL_FIELDS.has(k)) delete expected[k];
    }

    const existing = await base(TABLE_DEAL).find(dealId);
    const mismatches = diffDealTermsFields(expected, existing.fields);

    const toWrite = {};
    const corrected = [];
    const filledBlank = [];

    for (const [col, want] of Object.entries(expected)) {
      const cur = existing.fields[col];
      const empty = isEmptyDealValue(cur);
      if (want === null) {
        if (!opts.correct) continue;
        if (opts.fddOnlyCorrect) {
          const sourced =
            profile.sourceTier === "fdd" ||
            resolveSource === "brand-override" ||
            String(resolveSource).startsWith("choice-fdd");
          if (!sourced) continue;
        }
        if (!empty) {
          toWrite[col] = null;
          corrected.push({ column: col, from: cur, to: null });
        }
        continue;
      }
      if (empty) {
        toWrite[col] = want;
        filledBlank.push(col);
        continue;
      }
      if (!opts.correct) continue;
      if (opts.fddOnlyCorrect) {
        const sourced =
          profile.sourceTier === "fdd" ||
          resolveSource === "brand-override" ||
          String(resolveSource).startsWith("choice-fdd");
        if (!sourced) continue;
      }
      const miss = mismatches.find((m) => m.column === col);
      if (!miss) continue;
      toWrite[col] = want;
      corrected.push({ column: col, from: cur, to: want });
    }

    const entry = {
      brandName: name,
      basicsRecordId: row.id,
      dealTermsRecordId: dealId,
      status,
      parentCompany: parent || null,
      resolveSource,
      resolved,
      mismatchCount: mismatches.length,
      mismatches: mismatches.slice(0, 20),
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
      correctCount += corrected.length;
      blankCount += filledBlank.length;
      console.log(
        `[dry-run] ${name} (${status}) src=${resolveSource} blank=${filledBlank.length} correct=${corrected.length}`
      );
      continue;
    }

    const { updated } = await updateWithPruning(base, dealId, toWrite);
    entry.action = "updated";
    entry.fieldCountWritten = Object.keys(updated).length;
    entry.fields = updated;
    results.push(entry);
    writeCount += Object.keys(updated).length;
    correctCount += corrected.length;
    blankCount += filledBlank.length;
    console.log(
      `Updated ${name} blank=${filledBlank.length} correct=${corrected.length} written=${Object.keys(updated).length}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: opts.dryRun
      ? "dry-run"
      : opts.correct
        ? opts.fddOnlyCorrect
          ? "apply-blank+fdd-correct"
          : "apply-blank+correct"
        : "apply-blank-only",
    opts: {
      all: opts.all,
      activeOnly: opts.activeOnly,
      underReview: opts.underReview,
      correct: opts.correct,
      fddOnlyCorrect: opts.fddOnlyCorrect,
      brandFilter: opts.brandFilter || null,
    },
    summary: {
      brands: basicsRows.length,
      writeFields: writeCount,
      blankFills: blankCount,
      corrections: correctCount,
      withMismatches: results.filter((r) => (r.mismatchCount || 0) > 0).length,
    },
    optionProposals: proposals,
    results,
  };

  const outName = opts.dryRun
    ? "deal-terms-all-brands-dry-run.json"
    : "deal-terms-all-brands-apply.json";
  const outPath = path.join(ROOT, "reports", outName);
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    `Done brands=${basicsRows.length} writeFields=${writeCount} blank=${blankCount} correct=${correctCount}`
  );
  console.log(`Wrote ${outPath}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
