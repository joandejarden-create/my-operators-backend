#!/usr/bin/env node
/**
 * Remap Brand + Deal Target Guest Segment values to shared KEEP vocabulary.
 *
 * Usage:
 *   node scripts/remap-target-guest-segments.mjs --dry-run
 *   node scripts/remap-target-guest-segments.mjs --apply
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
  DEAL_GUEST_SEGMENT_OTHER_FIELD,
  remapBrandGuestSegments,
  remapDealGuestSegment,
} from "./lib/target-guest-segment-vocabulary.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const stamp = APPLY ? "apply" : "dry-run";
const OUT = path.join(ROOT, "reports", `target-guest-segment-remap-${stamp}.json`);

async function listAll(baseId, token, table, fields) {
  const out = [];
  let offset = null;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const data = await res.json();
    if (!res.ok) throw new Error(`${table}: ${data?.error?.message || res.status}`);
    out.push(...(data.records || []));
    offset = data.offset || null;
    await new Promise((r) => setTimeout(r, 220));
  } while (offset);
  return out;
}

async function updateRecord(baseId, token, table, id, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      // typecast true so remapped KEEP names exist even if Meta rename PATCH failed
      body: JSON.stringify({ fields, typecast: true }),
    }
  );
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data?.error?.message || JSON.stringify(data.error || data));
    err.body = data;
    throw err;
  }
  return data;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  if (!baseId || !token) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  console.log(`mode=${APPLY ? "APPLY" : "dry-run"}`);

  const brandRows = await listAll(baseId, token, BRAND_BASICS_TABLE, [
    "Brand Name",
    BRAND_GUEST_SEGMENTS_FIELD,
  ]);
  const dealRows = await listAll(baseId, token, DEAL_SI_TABLE, [
    DEAL_GUEST_SEGMENT_FIELD,
    DEAL_GUEST_SEGMENT_OTHER_FIELD,
  ]);

  const brandChanges = [];
  for (const r of brandRows) {
    const before = r.fields?.[BRAND_GUEST_SEGMENTS_FIELD] || [];
    if (!Array.isArray(before) || before.length === 0) continue;
    const { next, remapped, dropped } = remapBrandGuestSegments(before);
    const beforeKey = JSON.stringify([...before].map(String).sort());
    const nextKey = JSON.stringify([...next].sort());
    if (beforeKey === nextKey && remapped.length === 0 && dropped.length === 0) continue;
    brandChanges.push({
      id: r.id,
      brandName: r.fields?.["Brand Name"] || null,
      before,
      after: next,
      remapped,
      dropped,
    });
  }

  const dealChanges = [];
  for (const r of dealRows) {
    const beforeRaw = r.fields?.[DEAL_GUEST_SEGMENT_FIELD];
    const before = Array.isArray(beforeRaw)
      ? beforeRaw
      : beforeRaw
        ? [beforeRaw]
        : [];
    const beforeOther = r.fields?.[DEAL_GUEST_SEGMENT_OTHER_FIELD] || "";
    if (!before.length) continue;
    const mapped = remapDealGuestSegment(before, beforeOther);
    if (!mapped.remapped) continue;
    const beforeKey = JSON.stringify([...before].map(String).sort());
    const afterKey = JSON.stringify([...(mapped.next || [])].sort());
    if (beforeKey === afterKey && !mapped.toOther) continue;
    dealChanges.push({
      id: r.id,
      before,
      after: mapped.next || [],
      beforeOther: beforeOther || null,
      afterOther: mapped.otherText,
      toOther: mapped.toOther,
      from: mapped.from,
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    brand: {
      scanned: brandRows.length,
      changes: brandChanges.length,
      rows: brandChanges,
    },
    deal: {
      scanned: dealRows.length,
      changes: dealChanges.length,
      toOther: dealChanges.filter((c) => c.toOther).length,
      rows: dealChanges,
    },
  };

  if (APPLY) {
    let brandOk = 0;
    let brandFail = 0;
    for (const c of brandChanges) {
      try {
        await updateRecord(baseId, token, BRAND_BASICS_TABLE, c.id, {
          [BRAND_GUEST_SEGMENTS_FIELD]: c.after,
        });
        brandOk++;
        await new Promise((r) => setTimeout(r, 220));
      } catch (e) {
        brandFail++;
        c.error = e.message;
        console.error("Brand fail", c.id, e.message);
      }
    }
    let dealOk = 0;
    let dealFail = 0;
    for (const c of dealChanges) {
      try {
        const fields = { [DEAL_GUEST_SEGMENT_FIELD]: c.after };
        if (c.afterOther) fields[DEAL_GUEST_SEGMENT_OTHER_FIELD] = c.afterOther;
        await updateRecord(baseId, token, DEAL_SI_TABLE, c.id, fields);
        dealOk++;
        await new Promise((r) => setTimeout(r, 220));
      } catch (e) {
        dealFail++;
        c.error = e.message;
        console.error("Deal fail", c.id, e.message);
      }
    }
    report.apply = { brandOk, brandFail, dealOk, dealFail };
  }

  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
  console.log(
    `Brand changes=${brandChanges.length} Deal changes=${dealChanges.length} (toOther=${report.deal.toOther})`
  );
  console.log("Wrote", OUT);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
