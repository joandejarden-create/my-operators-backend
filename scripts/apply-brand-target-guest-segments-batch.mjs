/**
 * Populate Brand Setup - Brand Basics → Target Guest Segments.
 *
 *   node scripts/apply-brand-target-guest-segments-batch.mjs --dry-run --all
 *   node scripts/apply-brand-target-guest-segments-batch.mjs --all --correct
 *   node scripts/apply-brand-target-guest-segments-batch.mjs --brand "Kimpton Hotels" --correct
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
  TARGET_GUEST_SEGMENTS_ALLOWED,
  getBrandTargetGuestSegments,
} from "./lib/brand-target-guest-segments-profiles.mjs";
import {
  BRAND_BASICS_TABLE,
  BRAND_GUEST_SEGMENTS_FIELD,
} from "./lib/target-guest-segment-vocabulary.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const TABLE = BRAND_BASICS_TABLE;
const FIELD = BRAND_GUEST_SEGMENTS_FIELD;

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
    const t = j.tables.find((x) => x.name === TABLE);
    const f = t?.fields?.find((x) => x.name === FIELD);
    return (f?.options?.choices || []).map((c) => c.name).filter((c) => String(c).trim());
  }
  throw lastErr || new Error("Meta API failed");
}

function filterToMeta(list, metaChoices) {
  const allow = new Set(
    metaChoices.length ? metaChoices.filter((c) => TARGET_GUEST_SEGMENTS_ALLOWED.includes(c)) : TARGET_GUEST_SEGMENTS_ALLOWED
  );
  return list.filter((x) => allow.has(x));
}

async function loadBasics(base, opts) {
  const fields = ["Brand Name", "Brand Status", "Parent Company", FIELD];
  if (opts.brandFilter) {
    const esc = opts.brandFilter.replace(/"/g, '\\"');
    return base(TABLE)
      .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1, fields })
      .firstPage();
  }
  const rows = [];
  if (opts.activeOnly) {
    await base(TABLE)
      .select({ filterByFormula: BRAND_STATUS_ACTIVE_FORMULA, fields })
      .eachPage((p, n) => {
        rows.push(...p);
        n();
      });
    return rows;
  }
  await base(TABLE)
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
  const basicsRows = await loadBasics(base, opts);

  const results = [];
  let updated = 0;
  let blankFills = 0;
  let corrections = 0;

  for (const row of basicsRows) {
    const name = String(row.fields["Brand Name"] || "").trim();
    const status = row.fields["Brand Status"] || null;
    const parent = String(row.fields["Parent Company"] || "").trim();
    const { segments, resolveSource, segment } = getBrandTargetGuestSegments(name, parent);
    const want = filterToMeta(segments, metaChoices);

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

    const cur = row.fields[FIELD];
    const isEmpty = emptyVal(cur);
    const matches = sameSet(cur, want);

    const entry = {
      brandName: name,
      status,
      parentCompany: parent || null,
      recordId: row.id,
      resolveSource,
      segment,
      segments: want,
      current: cur || [],
    };

    if (matches) {
      entry.action = "unchanged";
      results.push(entry);
      continue;
    }
    if (!isEmpty && !opts.correct) {
      entry.action = "skip";
      entry.reason = "already_filled_use_correct";
      results.push(entry);
      continue;
    }

    entry.action = opts.dryRun ? "would_update" : "update";
    entry.mode = isEmpty ? "blank_fill" : "correct";
    results.push(entry);

    if (opts.dryRun) continue;

    try {
      await base(TABLE).update(row.id, { [FIELD]: want }, { typecast: true });
      updated++;
      if (isEmpty) blankFills++;
      else corrections++;
      await new Promise((r) => setTimeout(r, 220));
    } catch (e) {
      entry.action = "error";
      entry.error = e.message;
      console.error("Fail", name, e.message);
    }
  }

  const stamp = opts.dryRun ? "dry-run" : "apply";
  const out = path.join(ROOT, "reports", `brand-target-guest-segments-${stamp}.json`);
  const report = {
    generatedAt: new Date().toISOString(),
    mode: stamp,
    correct: opts.correct,
    scope: opts.brandFilter || (opts.all ? "all" : "active"),
    metaChoiceCount: metaChoices.length,
    scanned: basicsRows.length,
    updated,
    blankFills,
    corrections,
    summary: {
      unchanged: results.filter((r) => r.action === "unchanged").length,
      would_update: results.filter((r) => r.action === "would_update").length,
      update: results.filter((r) => r.action === "update").length,
      skip: results.filter((r) => r.action === "skip").length,
      error: results.filter((r) => r.action === "error").length,
    },
    results,
  };
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log(
    `scanned=${basicsRows.length} updated=${updated} blank=${blankFills} correct=${corrections}`
  );
  console.log("Wrote", out);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
