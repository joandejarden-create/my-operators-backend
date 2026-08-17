#!/usr/bin/env node
/**
 * Ensure Brand + Deal Target Guest Segment Meta choices match KEEP vocabulary.
 * Renames known old choices in-place (same choice id) and adds missing KEEP names.
 * Does NOT delete obsolete choices (run remap apply first, then --prune).
 *
 * If Meta PATCH fails (known on this base), --apply falls back to typecast seeding
 * on one throwaway write per table (then restores original values).
 *
 * Usage:
 *   node scripts/ensure-target-guest-segment-vocab.mjs --dry-run
 *   node scripts/ensure-target-guest-segment-vocab.mjs --apply
 *   node scripts/ensure-target-guest-segment-vocab.mjs --apply --prune
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BRAND_BASICS_TABLE,
  BRAND_GUEST_SEGMENTS_FIELD,
  DEAL_SI_TABLE,
  DEAL_GUEST_SEGMENT_FIELD,
  TARGET_GUEST_SEGMENT_BRAND_KEEP,
  TARGET_GUEST_SEGMENT_DEAL_KEEP,
  TARGET_GUEST_SEGMENT_REMAP,
  remapBrandGuestSegments,
  remapDealGuestSegment,
} from "./lib/target-guest-segment-vocabulary.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const PRUNE = process.argv.includes("--prune");
const stamp = APPLY ? (PRUNE ? "apply-prune" : "apply") : "dry-run";
const OUT = path.join(ROOT, "reports", `target-guest-segment-meta-ensure-${stamp}.json`);

async function metaFetch(baseId, token, pathSuffix, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${pathSuffix}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

function planChoices(existingChoices, keepList, remap) {
  const byName = new Map((existingChoices || []).map((c) => [c.name, { ...c }]));
  const renameOps = [];
  const addOps = [];
  const deleteOps = [];

  // Rename in place when old name exists and new name not yet present
  for (const [from, to] of Object.entries(remap)) {
    const old = byName.get(from);
    if (!old) continue;
    if (byName.has(to) && byName.get(to).id !== old.id) {
      // Target already exists as separate choice — leave rename to record remap + prune
      continue;
    }
    renameOps.push({ id: old.id, from, to });
    byName.delete(from);
    byName.set(to, { ...old, name: to });
  }

  for (const name of keepList) {
    if (!byName.has(name)) {
      addOps.push(name);
      byName.set(name, { name });
    }
  }

  const keepSet = new Set(keepList);
  if (PRUNE) {
    for (const [name, c] of [...byName.entries()]) {
      if (!keepSet.has(name)) {
        deleteOps.push({ id: c.id, name });
        byName.delete(name);
      }
    }
  }

  const next = [...byName.values()].map((c) => {
    const out = { name: c.name };
    if (c.id) out.id = c.id;
    if (c.color) out.color = c.color;
    return out;
  });

  // Stable KEEP order first, then any leftovers
  next.sort((a, b) => {
    const ia = keepList.indexOf(a.name);
    const ib = keepList.indexOf(b.name);
    if (ia >= 0 && ib >= 0) return ia - ib;
    if (ia >= 0) return -1;
    if (ib >= 0) return 1;
    return a.name.localeCompare(b.name);
  });

  return { next, renameOps, addOps, deleteOps };
}

async function ensureField(baseId, token, tableName, fieldName, keepList, type) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`meta tables: ${res.status} ${JSON.stringify(json)}`);
  const table = (json.tables || []).find((t) => t.name === tableName);
  if (!table) throw new Error(`Table not found: ${tableName}`);
  const field = (table.fields || []).find((f) => f.name === fieldName);
  if (!field) throw new Error(`Field not found: ${tableName}.${fieldName}`);
  if (field.type !== type) throw new Error(`Expected ${type}, got ${field.type}`);

  const existing = field.options?.choices || [];
  const snapshot = existing.map((c) => ({ id: c.id, name: c.name, color: c.color }));
  const plan = planChoices(existing, keepList, TARGET_GUEST_SEGMENT_REMAP);

  const result = {
    table: tableName,
    tableId: table.id,
    field: fieldName,
    fieldId: field.id,
    type,
    snapshotBefore: snapshot,
    renameOps: plan.renameOps,
    addOps: plan.addOps,
    deleteOps: plan.deleteOps,
    nextCount: plan.next.length,
    keepCount: keepList.length,
  };

  const needsPatch =
    plan.renameOps.length > 0 || plan.addOps.length > 0 || plan.deleteOps.length > 0;

  if (!needsPatch) {
    result.status = "already_aligned";
    return result;
  }

  if (!APPLY) {
    result.status = "dry_run";
    result.nextPreview = plan.next.map((c) => c.name);
    return result;
  }

  const { res: patchRes, json: patchJson } = await metaFetch(
    baseId,
    token,
    `/tables/${table.id}/fields/${field.id}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        type,
        options: { choices: plan.next },
      }),
    }
  );

  if (!patchRes.ok) {
    result.status = "meta_patch_failed";
    result.error = patchJson?.error || patchJson;
    result.manualChecklist = {
      note: "Meta API PATCH failed — add via typecast seed; delete obsolete choices manually in Airtable UI",
      rename: plan.renameOps,
      add: plan.addOps,
      delete: plan.deleteOps,
    };
    return result;
  }

  result.status = "updated";
  result.after = (patchJson.options?.choices || []).map((c) => ({ id: c.id, name: c.name }));
  return result;
}

async function patchRecord(baseId, token, table, id, fields, typecast) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: !!typecast }),
    }
  );
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error?.message || JSON.stringify(data.error || data));
  return data;
}

async function listOne(baseId, token, table, fields) {
  const params = new URLSearchParams({ pageSize: "1", maxRecords: "1" });
  for (const f of fields) params.append("fields[]", f);
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const data = await res.json();
  if (!res.ok) throw new Error(`${table}: ${data?.error?.message || res.status}`);
  return (data.records || [])[0] || null;
}

/**
 * Create missing KEEP choices by writing them once with typecast:true, then restore.
 * Renames are achieved by writing remapped values on real records later (remap script).
 */
async function seedViaTypecast(baseId, token, side) {
  if (side === "brand") {
    const rec = await listOne(baseId, token, BRAND_BASICS_TABLE, [BRAND_GUEST_SEGMENTS_FIELD]);
    if (!rec) throw new Error("No Brand Basics record to seed against");
    const before = rec.fields?.[BRAND_GUEST_SEGMENTS_FIELD] || [];
    const restore = remapBrandGuestSegments(before).next;
    await patchRecord(
      baseId,
      token,
      BRAND_BASICS_TABLE,
      rec.id,
      { [BRAND_GUEST_SEGMENTS_FIELD]: [...TARGET_GUEST_SEGMENT_BRAND_KEEP] },
      true
    );
    await new Promise((r) => setTimeout(r, 300));
    await patchRecord(
      baseId,
      token,
      BRAND_BASICS_TABLE,
      rec.id,
      { [BRAND_GUEST_SEGMENTS_FIELD]: restore },
      true
    );
    return { seededRecordId: rec.id, restore };
  }

  const rec = await listOne(baseId, token, DEAL_SI_TABLE, [DEAL_GUEST_SEGMENT_FIELD]);
  if (!rec) throw new Error("No Strategic Intent record to seed against");
  const before = rec.fields?.[DEAL_GUEST_SEGMENT_FIELD] || [];
  const mapped = remapDealGuestSegment(before);
  const restore = mapped.next || [];
  await patchRecord(
    baseId,
    token,
    DEAL_SI_TABLE,
    rec.id,
    { [DEAL_GUEST_SEGMENT_FIELD]: [...TARGET_GUEST_SEGMENT_DEAL_KEEP] },
    true
  );
  await new Promise((r) => setTimeout(r, 300));
  await patchRecord(
    baseId,
    token,
    DEAL_SI_TABLE,
    rec.id,
    { [DEAL_GUEST_SEGMENT_FIELD]: restore },
    true
  );
  return { seededRecordId: rec.id, restore };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  if (!baseId || !token) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  console.log(`mode=${APPLY ? "APPLY" : "dry-run"} prune=${PRUNE}`);

  const brand = await ensureField(
    baseId,
    token,
    BRAND_BASICS_TABLE,
    BRAND_GUEST_SEGMENTS_FIELD,
    TARGET_GUEST_SEGMENT_BRAND_KEEP,
    "multipleSelects"
  );
  console.log(
    `Brand: ${brand.status} rename=${brand.renameOps.length} add=${brand.addOps.length} delete=${brand.deleteOps.length}`
  );

  const deal = await ensureField(
    baseId,
    token,
    DEAL_SI_TABLE,
    DEAL_GUEST_SEGMENT_FIELD,
    TARGET_GUEST_SEGMENT_DEAL_KEEP,
    "multipleSelects"
  );
  console.log(
    `Deal: ${deal.status} rename=${deal.renameOps.length} add=${deal.addOps.length} delete=${deal.deleteOps.length}`
  );

  const typecastSeed = { brand: null, deal: null };
  const needSeedBrand =
    APPLY &&
    brand.status === "meta_patch_failed" &&
    (brand.addOps.length > 0 || brand.renameOps.length > 0);
  const needSeedDeal =
    APPLY &&
    deal.status === "meta_patch_failed" &&
    (deal.addOps.length > 0 || deal.renameOps.length > 0);
  if (needSeedBrand || needSeedDeal) {
    console.log("Meta PATCH blocked — seeding missing KEEP choices via typecast…");
    if (needSeedBrand) {
      typecastSeed.brand = await seedViaTypecast(baseId, token, "brand");
      brand.status = "seeded_via_typecast";
      console.log("Brand typecast seed ok", typecastSeed.brand.seededRecordId);
    }
    if (needSeedDeal) {
      typecastSeed.deal = await seedViaTypecast(baseId, token, "deal");
      deal.status = "seeded_via_typecast";
      console.log("Deal typecast seed ok", typecastSeed.deal.seededRecordId);
    }
  } else if (APPLY && PRUNE && (brand.deleteOps.length || deal.deleteOps.length)) {
    console.log(
      "Meta prune PATCH blocked — see reports/target-guest-segment-manual-meta-prune.md"
    );
  }

  // Re-read Meta to confirm KEEP presence + leftover obsolete
  const { res: verifyRes, json: verifyJson } = await metaFetch(baseId, token, "/tables");
  let verify = null;
  if (verifyRes.ok) {
    const tables = verifyJson.tables || [];
    const bb = tables.find((t) => t.name === BRAND_BASICS_TABLE);
    const si = tables.find((t) => t.name === DEAL_SI_TABLE);
    const bf = bb?.fields?.find((f) => f.name === BRAND_GUEST_SEGMENTS_FIELD);
    const df = si?.fields?.find((f) => f.name === DEAL_GUEST_SEGMENT_FIELD);
    const brandNames = (bf?.options?.choices || []).map((c) => c.name);
    const dealNames = (df?.options?.choices || []).map((c) => c.name);
    verify = {
      brandMissing: TARGET_GUEST_SEGMENT_BRAND_KEEP.filter((n) => !brandNames.includes(n)),
      brandObsolete: brandNames.filter((n) => !TARGET_GUEST_SEGMENT_BRAND_KEEP.includes(n)),
      dealMissing: TARGET_GUEST_SEGMENT_DEAL_KEEP.filter((n) => !dealNames.includes(n)),
      dealObsolete: dealNames.filter((n) => !TARGET_GUEST_SEGMENT_DEAL_KEEP.includes(n)),
    };
    console.log(
      `Verify brand missing=${verify.brandMissing.length} obsolete=${verify.brandObsolete.length}`
    );
    console.log(
      `Verify deal missing=${verify.dealMissing.length} obsolete=${verify.dealObsolete.length}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? (PRUNE ? "apply-prune" : "apply") : "dry-run",
    brand,
    deal,
    typecastSeed,
    verify,
    pruneNote:
      PRUNE && (brand.status === "meta_patch_failed" || deal.status === "seeded_via_typecast")
        ? "Prune via Meta API still blocked — delete obsolete choices manually (see verify.*Obsolete)"
        : null,
  };
  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log("Wrote", OUT);

  if (verify?.brandMissing?.length || verify?.dealMissing?.length) {
    process.exitCode = 2;
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
