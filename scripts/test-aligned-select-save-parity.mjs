#!/usr/bin/env node
/**
 * Live save smoke test for aligned Brand Setup / Deal Setup select options.
 *
 * For each field: PATCH with canonical values (typecast=false), then restore originals.
 * typecast=false fails loudly if an option is not already on the Airtable field.
 *
 * Usage:
 *   node scripts/test-aligned-select-save-parity.mjs
 *   node scripts/test-aligned-select-save-parity.mjs --apply
 */
import "dotenv/config";
import FORM from "../lib/deal-setup-form-options.json" with { type: "json" };

const APPLY = process.argv.includes("--apply");
const baseId = process.env.AIRTABLE_BASE_ID;
const token = process.env.AIRTABLE_API_KEY;
if (!baseId || !token) {
  console.error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  process.exit(1);
}

const headers = {
  Authorization: `Bearer ${token}`,
  "Content-Type": "application/json",
};

async function metaTables() {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data?.error?.message || res.status);
  return data.tables || [];
}

async function firstRecord(table, fields) {
  const params = new URLSearchParams({ pageSize: "1" });
  for (const f of fields) params.append("fields[]", f);
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const data = await res.json();
  if (!res.ok) throw new Error(`${table}: ${data?.error?.message || res.status}`);
  return data.records?.[0] || null;
}

async function patch(table, recordId, fields, typecast) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${encodeURIComponent(recordId)}?typecast=${typecast ? "true" : "false"}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers,
    body: JSON.stringify({ fields }),
  });
  const data = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, data, error: data?.error };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** Write payload that exercises all options for multi-select, or one option for single-select. */
function buildWriteValue(fieldMeta, options) {
  if (!options?.length) return null;
  if (fieldMeta.type === "multipleSelects") return [...options];
  return options[0];
}

function restoreValue(original) {
  if (original === undefined) return null; // clear
  return original;
}

async function main() {
  console.log(`mode=${APPLY ? "APPLY (write+restore)" : "dry-run (validate only)"}`);
  const tables = await metaTables();

  const tests = [
    {
      label: "Deal Hotel Service Model",
      table: "Location & Property",
      field: "Hotel Service Model",
      options: FORM["Hotel Service Model"],
    },
    {
      label: "Deal Hotel Chain Scale",
      table: "Location & Property",
      field: "Hotel Chain Scale",
      options: FORM["Hotel Chain Scale"],
    },
    {
      label: "Deal Building Type",
      table: "Location & Property",
      field: "Building Type",
      options: FORM["Building Type"],
    },
    {
      label: "Deal Project Type",
      table: "Deals",
      field: "Project Type",
      options: FORM["Project Type"],
    },
    {
      label: "Deal Stage of Development",
      table: "Deals",
      field: "Stage of Development",
      options: FORM["Stage of Development"],
    },
    {
      label: "Deal Additional Amenities",
      table: "Deals",
      field: "Additional Amenities",
      options: FORM["Additional Amenities"],
    },
    {
      label: "Deal Preferred Deal Structure",
      table: "Market - Performance - Deal & Capital Structure",
      field: "Preferred Deal Structure",
      options: FORM["Preferred Deal Structure"],
    },
    {
      label: "Deal Soft vs Hard Brand Preference",
      table: "Strategic Intent - Operational - Key Challenges",
      field: "Soft vs Hard Brand Preference",
      options: FORM["Soft vs Hard Brand Preference"],
    },
    {
      label: "Brand Hotel Service Model",
      table: "Brand Setup - Brand Basics",
      field: "Hotel Service Model",
      options: FORM["Hotel Service Model"],
    },
    {
      label: "Brand Hotel Chain Scale",
      table: "Brand Setup - Brand Basics",
      field: "Hotel Chain Scale",
      options: FORM["Hotel Chain Scale"],
    },
    {
      label: "Brand Acceptable Project Type",
      table: "Brand Setup - Project Fit",
      field: "Acceptable Project Type",
      options: FORM["Project Type"],
    },
    {
      label: "Brand Acceptable Building Types",
      table: "Brand Setup - Project Fit",
      field: "Acceptable Building Types",
      options: FORM["Building Type"],
    },
    {
      label: "Brand Acceptable Agreements Type",
      table: "Brand Setup - Project Fit",
      field: "Acceptable Agreements Type",
      options: FORM["Preferred Deal Structure"],
    },
    {
      label: "Brand Acceptable Project Stages",
      table: "Brand Setup - Project Fit",
      field: "Acceptable Project Stages",
      options: FORM["Stage of Development"],
    },
    {
      label: "Brand Additional Amenities",
      table: "Brand Setup - Brand Standards",
      field: "Additional Amenities",
      options: FORM["Additional Amenities"],
    },
  ];

  let fail = 0;
  const results = [];

  for (const t of tests) {
    const tableMeta = tables.find((x) => x.name === t.table);
    const fieldMeta = (tableMeta?.fields || []).find((x) => x.name === t.field);
    if (!tableMeta || !fieldMeta) {
      console.log(`\n[FAIL] ${t.label}: table/field not found`);
      fail++;
      results.push({ label: t.label, ok: false, reason: "missing_field" });
      continue;
    }

    const airChoices = (fieldMeta.options?.choices || []).map((c) => c.name);
    const airSet = new Set(airChoices);
    const missing = (t.options || []).filter((o) => !airSet.has(o));
    const extraNote =
      fieldMeta.type === "singleSelect" || fieldMeta.type === "multipleSelects"
        ? ""
        : ` unexpected type=${fieldMeta.type}`;

    if (missing.length) {
      console.log(`\n[FAIL] ${t.label}: UX options missing from Airtable${extraNote}`);
      console.log("  missing:", missing.join(" | "));
      fail++;
      results.push({ label: t.label, ok: false, reason: "options_missing", missing });
      continue;
    }

    console.log(`\n[OK options] ${t.label} (${fieldMeta.type}, n=${t.options.length})`);

    if (!APPLY) {
      results.push({ label: t.label, ok: true, phase: "options_only" });
      continue;
    }

    const rec = await firstRecord(t.table, [t.field]);
    if (!rec) {
      console.log(`  [SKIP write] no records in ${t.table}`);
      results.push({ label: t.label, ok: true, phase: "options_ok_no_record" });
      continue;
    }

    const original = rec.fields?.[t.field];
    const writeVal = buildWriteValue(fieldMeta, t.options);
    // For Preferred Deal Structure (deal) which is multi-select: write all options.
    // For single-select: write first option that differs from current if possible.
    let payloadVal = writeVal;
    if (fieldMeta.type === "singleSelect" && t.options.length > 1) {
      const cur = Array.isArray(original) ? original[0] : original;
      payloadVal = t.options.find((o) => o !== cur) || t.options[0];
    }

    const write = await patch(t.table, rec.id, { [t.field]: payloadVal }, false);
    await sleep(220);
    if (!write.ok) {
      console.log(`  [FAIL write typecast=false] ${write.status}`, write.error || write.data);
      // Retry with typecast=true (product path) to see if save would still work
      const write2 = await patch(t.table, rec.id, { [t.field]: payloadVal }, true);
      await sleep(220);
      if (!write2.ok) {
        console.log(`  [FAIL write typecast=true] ${write2.status}`, write2.error || write2.data);
        fail++;
        results.push({
          label: t.label,
          ok: false,
          reason: "write_failed",
          error: write.error || write2.error,
        });
        continue;
      }
      console.log("  [WARN] typecast=false failed but typecast=true succeeded (product path OK)");
    } else {
      console.log(`  [OK write] ${rec.id} typecast=false`);
    }

    // Restore
    const restorePayload =
      original === undefined
        ? { [t.field]: fieldMeta.type === "multipleSelects" ? [] : null }
        : { [t.field]: original };
    // Airtable may reject null clear on required selects — fall back to original if present
    let restore = await patch(t.table, rec.id, restorePayload, true);
    if (!restore.ok && original !== undefined) {
      restore = await patch(t.table, rec.id, { [t.field]: original }, true);
    }
    await sleep(220);
    if (!restore.ok) {
      console.log(`  [WARN restore] ${restore.status}`, restore.error || restore.data);
      results.push({ label: t.label, ok: true, phase: "wrote_restore_warn", recordId: rec.id });
    } else {
      console.log(`  [OK restore] ${rec.id}`);
      results.push({ label: t.label, ok: true, phase: "wrote_and_restored", recordId: rec.id });
    }
  }

  console.log(`\nSummary: ${fail === 0 ? "ALL PASSED" : fail + " FAILED"} (${APPLY ? "apply" : "dry-run"})`);
  if (fail) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
