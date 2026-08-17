/**
 * Populate Brand Setup - Project Fit → Acceptable Project Type + Acceptable Agreements Type.
 *
 *   node scripts/apply-brand-project-agreement-types-batch.mjs --dry-run --all --correct
 *   node scripts/apply-brand-project-agreement-types-batch.mjs --all --correct
 *   node scripts/apply-brand-project-agreement-types-batch.mjs --brand "Design Hotels" --correct
 *
 * Default: blank-only. --correct / --overwrite replaces mismatches with profile lists.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import {
  ACCEPTABLE_PROJECT_TYPES_ALLOWED,
  ACCEPTABLE_AGREEMENTS_ALLOWED,
  getBrandProjectAndAgreementTypes,
  MAP_PROJECT_AGREEMENT_TYPES,
} from "./lib/brand-project-agreement-types-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_FIT = "Brand Setup - Project Fit";
const LINK = "Brand Setup - Project Fit";
const FIELD_PROJ = MAP_PROJECT_AGREEMENT_TYPES.projectTypes;
const FIELD_AGR = MAP_PROJECT_AGREEMENT_TYPES.agreementTypes;

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

function filterToMeta(list, metaChoices, fallbackAllowed) {
  const allow = new Set(metaChoices.length ? metaChoices : fallbackAllowed);
  return list.filter((x) => allow.has(x));
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
    for (const col of [FIELD_PROJ, FIELD_AGR]) {
      const f = t?.fields?.find((x) => x.name === col);
      out[col] = (f?.options?.choices || []).map((c) => c.name).filter((c) => String(c).trim());
    }
    return out;
  }
  throw lastErr || new Error("Meta API failed");
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
  let brandsTouched = 0;
  let blankFills = 0;
  let corrections = 0;
  let fieldsWritten = 0;

  for (const row of basicsRows) {
    const name = String(row.fields["Brand Name"] || "").trim();
    const status = row.fields["Brand Status"] || null;
    const parent = String(row.fields["Parent Company"] || "").trim();
    const fitId = Array.isArray(row.fields[LINK]) ? row.fields[LINK][0] : null;

    if (!fitId) {
      results.push({ brandName: name, status, action: "skip", reason: "no_project_fit_link" });
      continue;
    }

    const { projectTypes, agreementTypes, resolveSource, segment } =
      getBrandProjectAndAgreementTypes(name, parent);

    const wantProj = filterToMeta(
      projectTypes,
      metaChoices[FIELD_PROJ] || [],
      ACCEPTABLE_PROJECT_TYPES_ALLOWED
    );
    const wantAgr = filterToMeta(
      agreementTypes,
      metaChoices[FIELD_AGR] || [],
      ACCEPTABLE_AGREEMENTS_ALLOWED
    );

    const existing = await base(TABLE_FIT).find(fitId);
    const curProj = existing.fields[FIELD_PROJ];
    const curAgr = existing.fields[FIELD_AGR];

    const toWrite = {};
    const filledBlank = [];
    const corrected = [];

    const planField = (col, want, cur) => {
      if (!want.length) return;
      const isEmpty = emptyVal(cur);
      const matches = sameSet(cur, want);
      if (matches) return;
      if (isEmpty) {
        toWrite[col] = want;
        filledBlank.push(col);
        return;
      }
      if (!opts.correct) return;
      toWrite[col] = want;
      corrected.push({ column: col, from: cur, to: want });
    };

    planField(FIELD_PROJ, wantProj, curProj);
    planField(FIELD_AGR, wantAgr, curAgr);

    const entry = {
      brandName: name,
      status,
      parentCompany: parent || null,
      projectFitRecordId: fitId,
      resolveSource,
      segment,
      projectTypes: wantProj,
      agreementTypes: wantAgr,
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
        `[dry-run] ${name} src=${resolveSource} blank=${filledBlank.length} correct=${corrected.length}`
      );
      continue;
    }

    const { updated } = await updateWithPruning(base, fitId, toWrite);
    entry.action = "updated";
    entry.fields = updated;
    results.push(entry);
    brandsTouched += 1;
    blankFills += filledBlank.length;
    corrections += corrected.length;
    fieldsWritten += Object.keys(updated).length;
    console.log(
      `Updated ${name} src=${resolveSource} blank=${filledBlank.length} correct=${corrected.length}`
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
      skippedNoLink: results.filter((r) => r.reason === "no_project_fit_link").length,
    },
    results,
  };

  const out = path.join(
    ROOT,
    "reports",
    opts.dryRun
      ? "brand-project-agreement-types-dry-run.json"
      : "brand-project-agreement-types-apply.json"
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(
    `Done brands=${basicsRows.length} touched=${brandsTouched} fields=${fieldsWritten} blank=${blankFills} correct=${corrections}`
  );
  console.log(`Wrote ${out}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
