/**
 * Populate / correct Project Fit stages, priority markets, markets to avoid (Other text),
 * and min/max room counts.
 *
 *   node scripts/apply-brand-project-fit-markets-rooms-batch.mjs --dry-run --all --correct
 *   node scripts/apply-brand-project-fit-markets-rooms-batch.mjs --all --correct
 *
 * Default: blank-only. --correct overwrites mismatches.
 * Markets to Avoid: only written when profile has non-empty list (hard-fail safe).
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import {
  PRIORITY_MARKETS_ALLOWED,
  getBrandMarketsRoomsStagesProfile,
  MAP_MARKETS_ROOMS_STAGES,
} from "./lib/brand-project-fit-markets-rooms-profiles.mjs";
import { PROJECT_STAGES_ALLOWED } from "./lib/brand-project-fit-engagement-profiles.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_FIT = "Brand Setup - Project Fit";
const LINK = "Brand Setup - Project Fit";

const FIELD_COLS = [
  MAP_MARKETS_ROOMS_STAGES.stages,
  MAP_MARKETS_ROOMS_STAGES.priorityMarkets,
  MAP_MARKETS_ROOMS_STAGES.marketsToAvoid,
  MAP_MARKETS_ROOMS_STAGES.otherPriority,
  MAP_MARKETS_ROOMS_STAGES.otherAvoid,
  MAP_MARKETS_ROOMS_STAGES.roomMin,
  MAP_MARKETS_ROOMS_STAGES.roomMax,
];

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
  return v === undefined || v === null || v === "" || (Array.isArray(v) && !v.length);
}

function sameValue(a, b) {
  if (Array.isArray(a) || Array.isArray(b)) {
    const aa = [...(Array.isArray(a) ? a : a != null && a !== "" ? [a] : [])].map(String).sort();
    const bb = [...(Array.isArray(b) ? b : b != null && b !== "" ? [b] : [])].map(String).sort();
    return aa.length === bb.length && aa.every((x, i) => x === bb[i]);
  }
  if (typeof a === "number" || typeof b === "number") {
    return Number(a) === Number(b);
  }
  return String(a ?? "").trim() === String(b ?? "").trim();
}

function filterMulti(list, allowed) {
  const allow = new Set(allowed);
  return (list || []).filter((x) => allow.has(x));
}

async function getMetaChoices(baseId, apiKey) {
  let lastErr;
  for (let attempt = 0; attempt < 6; attempt++) {
    const r = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (r.status === 429) {
      await new Promise((res) => setTimeout(res, 2000 * (attempt + 1)));
      lastErr = new Error("Meta API 429");
      continue;
    }
    if (!r.ok) throw new Error(`Meta ${r.status}`);
    const j = await r.json();
    const t = j.tables.find((x) => x.name === TABLE_FIT);
    const out = {};
    for (const col of [
      MAP_MARKETS_ROOMS_STAGES.stages,
      MAP_MARKETS_ROOMS_STAGES.priorityMarkets,
      MAP_MARKETS_ROOMS_STAGES.marketsToAvoid,
    ]) {
      const f = t?.fields?.find((x) => x.name === col);
      out[col] = (f?.options?.choices || []).map((c) => c.name).filter(Boolean);
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

async function updateOneField(base, recordId, col, value) {
  try {
    await base(TABLE_FIT).update(recordId, { [col]: value }, { typecast: true });
    return { ok: true };
  } catch (err) {
    console.warn(`  Fail ${col}: ${err.error || ""} ${err.message || err}`);
    return { ok: false };
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

    const { fields: raw, resolveSource, segment } = getBrandMarketsRoomsStagesProfile(name, parent);
    const expected = {};

    if (raw["Acceptable Project Stages"]) {
      expected["Acceptable Project Stages"] = filterMulti(
        raw["Acceptable Project Stages"],
        metaChoices["Acceptable Project Stages"]?.length
          ? metaChoices["Acceptable Project Stages"]
          : PROJECT_STAGES_ALLOWED
      );
    }
    if (raw["Priority Markets"]) {
      expected["Priority Markets"] = filterMulti(
        raw["Priority Markets"],
        metaChoices["Priority Markets"]?.length
          ? metaChoices["Priority Markets"]
          : PRIORITY_MARKETS_ALLOWED
      );
    }
    if (raw["Markets to Avoid"]) {
      expected["Markets to Avoid"] = filterMulti(
        raw["Markets to Avoid"],
        metaChoices["Markets to Avoid"]?.length
          ? metaChoices["Markets to Avoid"]
          : PRIORITY_MARKETS_ALLOWED
      );
    }
    if (raw["Other - Priority Markets Text"]) {
      expected["Other - Priority Markets Text"] = String(raw["Other - Priority Markets Text"]).trim();
    }
    if (raw["Other - Markets to Avoid Text"]) {
      expected["Other - Markets to Avoid Text"] = String(raw["Other - Markets to Avoid Text"]).trim();
    }
    if (raw["Min - Room Count"] != null) expected["Min - Room Count"] = Number(raw["Min - Room Count"]);
    if (raw["Max - Room Count"] != null) expected["Max - Room Count"] = Number(raw["Max - Room Count"]);

    const existing = await base(TABLE_FIT).find(fitId);
    const toWrite = {};
    const filledBlank = [];
    const corrected = [];

    for (const [col, want] of Object.entries(expected)) {
      if (want === undefined || want === null || want === "" || (Array.isArray(want) && !want.length)) {
        continue;
      }
      const cur = existing.fields[col];
      if (emptyVal(cur)) {
        toWrite[col] = want;
        filledBlank.push(col);
        continue;
      }
      if (!opts.correct) continue;
      if (sameValue(cur, want)) continue;
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
        `[dry-run] ${name} blank=${filledBlank.length} correct=${corrected.length}`
      );
      continue;
    }

    const written = {};
    for (const col of FIELD_COLS) {
      if (!(col in toWrite)) continue;
      const res = await updateOneField(base, fitId, col, toWrite[col]);
      if (res.ok) {
        written[col] = toWrite[col];
        fieldsWritten += 1;
      } else writeErrors += 1;
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
      ? "brand-project-fit-markets-rooms-dry-run.json"
      : "brand-project-fit-markets-rooms-apply.json"
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
