/**
 * Blank-fill (and optionally correct) Project Fit engagement multi-selects for all brands.
 * Writes each field in its own Airtable update to avoid coupled field wipe issues.
 *
 *   node scripts/apply-brand-project-fit-engagement-batch.mjs --dry-run --all
 *   node scripts/apply-brand-project-fit-engagement-batch.mjs --all
 *   node scripts/apply-brand-project-fit-engagement-batch.mjs --all --correct
 *
 * Default: blank-only. --correct also overwrites mismatches.
 * Does NOT touch Acceptable Building Types (separate batch).
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import {
  getBrandProjectFitEngagementFields,
  MAP_PROJECT_FIT_ENGAGEMENT,
} from "./lib/brand-project-fit-engagement-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_FIT = "Brand Setup - Project Fit";
const LINK = "Brand Setup - Project Fit";
const FIELD_COLS = Object.keys(MAP_PROJECT_FIT_ENGAGEMENT);

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
    const out = {};
    for (const col of FIELD_COLS) {
      const f = t?.fields?.find((x) => x.name === col);
      out[col] = (f?.options?.choices || []).map((c) => c.name).filter((c) => String(c).trim());
    }
    return out;
  }
  throw lastErr || new Error("Meta API failed");
}

function filterToMeta(list, metaChoices) {
  const allow = new Set(metaChoices || []);
  if (!allow.size) return [...list];
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

async function updateOneField(base, recordId, col, value) {
  try {
    await base(TABLE_FIT).update(recordId, { [col]: value }, { typecast: true });
    return { ok: true };
  } catch (err) {
    console.warn(`  Fail ${col}: ${err.error || ""} ${err.message || err}`);
    return { ok: false, error: String(err.message || err) };
  }
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
  let brandsTouched = 0;
  let blankFills = 0;
  let corrections = 0;
  let fieldsWritten = 0;
  let writeErrors = 0;

  for (const row of basicsRows) {
    const name = String(row.fields["Brand Name"] || "").trim();
    const status = row.fields["Brand Status"] || null;
    const parent = String(row.fields["Parent Company"] || "").trim();
    const fitId = Array.isArray(row.fields[LINK]) ? row.fields[LINK][0] : null;

    if (!fitId) {
      results.push({ brandName: name, status, action: "skip", reason: "no_project_fit_link" });
      continue;
    }

    const { fields: expectedRaw, resolveSource, segment, engagementSegment } =
      getBrandProjectFitEngagementFields(name, parent);

    const expected = {};
    for (const col of FIELD_COLS) {
      const want = filterToMeta(expectedRaw[col] || [], metaChoices[col] || []);
      if (want.length) expected[col] = want;
    }

    const existing = await base(TABLE_FIT).find(fitId);
    const toWrite = {};
    const filledBlank = [];
    const corrected = [];

    for (const [col, want] of Object.entries(expected)) {
      const cur = existing.fields[col];
      if (emptyVal(cur)) {
        toWrite[col] = want;
        filledBlank.push(col);
        continue;
      }
      if (!opts.correct) continue;
      if (sameSet(cur, want)) continue;
      toWrite[col] = want;
      corrected.push({ column: col, from: cur, to: want });
    }

    const entry = {
      brandName: name,
      status,
      parentCompany: parent || null,
      projectFitRecordId: fitId,
      resolveSource,
      segment,
      engagementSegment,
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
      brandsTouched += 1;
      blankFills += filledBlank.length;
      corrections += corrected.length;
      fieldsWritten += Object.keys(toWrite).length;
      console.log(
        `[dry-run] ${name} blank=${filledBlank.length} correct=${corrected.length} cols=${Object.keys(toWrite).join(",")}`
      );
      continue;
    }

    // Write one field at a time (Agreements last) to reduce wipe/coupling risk
    const writeOrder = [
      ...FIELD_COLS.filter((c) => c !== "Acceptable Agreements Type" && toWrite[c]),
      ...(toWrite["Acceptable Agreements Type"] ? ["Acceptable Agreements Type"] : []),
    ];
    const written = {};
    for (const col of writeOrder) {
      const res = await updateOneField(base, fitId, col, toWrite[col]);
      if (res.ok) {
        written[col] = toWrite[col];
        fieldsWritten += 1;
      } else {
        writeErrors += 1;
      }
    }

    // Verify agreements stuck when written
    if (written["Acceptable Agreements Type"]) {
      const check = await base(TABLE_FIT).find(fitId);
      if (emptyVal(check.fields["Acceptable Agreements Type"])) {
        console.warn(`  Agreements wiped after write for ${name}; retrying once`);
        const retry = await updateOneField(
          base,
          fitId,
          "Acceptable Agreements Type",
          toWrite["Acceptable Agreements Type"]
        );
        if (retry.ok) {
          const check2 = await base(TABLE_FIT).find(fitId);
          if (emptyVal(check2.fields["Acceptable Agreements Type"])) {
            writeErrors += 1;
            entry.agreementsWipe = true;
          }
        } else writeErrors += 1;
      }
    }

    entry.action = "updated";
    entry.fields = written;
    results.push(entry);
    brandsTouched += 1;
    blankFills += filledBlank.length;
    corrections += corrected.length;
    console.log(
      `Updated ${name} blank=${filledBlank.length} correct=${corrected.length} written=${Object.keys(written).length}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: opts.dryRun
      ? "dry-run"
      : opts.correct
        ? "apply-blank+correct"
        : "apply-blank-only",
    metaChoices,
    summary: {
      brands: basicsRows.length,
      brandsTouched,
      fieldsWritten,
      blankFills,
      corrections,
      writeErrors,
    },
    results,
  };

  const out = path.join(
    ROOT,
    "reports",
    opts.dryRun
      ? "brand-project-fit-engagement-dry-run.json"
      : "brand-project-fit-engagement-apply.json"
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(
    `Done brands=${basicsRows.length} touched=${brandsTouched} fields=${fieldsWritten} blank=${blankFills} correct=${corrections} errors=${writeErrors}`
  );
  console.log(`Wrote ${out}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
