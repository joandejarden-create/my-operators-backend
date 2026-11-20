#!/usr/bin/env node
/**
 * Post-migration quality quarantine: legal-suffix city bleed must not remain
 * IDENTITY_VERIFIED / VERIFIED_PROPERTY without address confirmation.
 *
 *   node scripts/sec-edgar-discovery-inventory-quality-quarantine-v23.mjs --dry-run
 *   node scripts/sec-edgar-discovery-inventory-quality-quarantine-v23.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const CITY_BLEED_RE = /\b(ltd\.?|l\.?p\.?|llc|inc\.?|corp\.?)\b/i;

async function main() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const ids = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"),
      "utf8",
    ),
  );
  const tableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
  const baseId = GRAPH_BASE_ID_CANONICAL;

  const records = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);

  const M = MAP_DISCOVERY_INVENTORY;
  const toFix = records.filter((r) => CITY_BLEED_RE.test(String(r.fields[M.city] || "")));
  const report = {
    mode: APPLY ? "APPLY" : "DRY_RUN",
    scanned: records.length,
    cityBleedCount: toFix.length,
    updates: [],
  };

  for (const r of toFix) {
    const prevNotes = String(r.fields[M.notesQuarantine] || "");
    const note =
      "QUARANTINED_CITY_BLEED: city field contains legal-entity suffix; demoted from VERIFIED until address/city confirmed";
    const fields = {
      [M.identityClass]: "PROBABLE_PROPERTY",
      [M.lifecycleStatus]: "DISCOVERY_PERSISTED",
      [M.persistenceStatus]: "QUARANTINED",
      [M.notesQuarantine]: prevNotes.includes("QUARANTINED_CITY_BLEED")
        ? prevNotes
        : [prevNotes, note].filter(Boolean).join("\n"),
    };
    report.updates.push({
      id: r.id,
      propertyId: r.fields[M.discoveryPropertyId],
      name: r.fields[M.hotelNameNormalized],
      city: r.fields[M.city],
      fromIdentity: r.fields[M.identityClass],
      toIdentity: "PROBABLE_PROPERTY",
    });
    if (!APPLY) continue;
    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}/${r.id}`, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: true }),
    });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
  }

  const outDir = path.join(ROOT, "reports", "sec-edgar-v23-discovery-inventory");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, APPLY ? "QUALITY_QUARANTINE_APPLY.json" : "QUALITY_QUARANTINE_DRY_RUN.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, outPath, cityBleedCount: report.cityBleedCount }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
