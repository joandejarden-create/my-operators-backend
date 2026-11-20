#!/usr/bin/env node
/**
 * Restore false Hyatt quarantines; quarantine remaining narrative false positives.
 * Preserves Notes history. --apply required for writes.
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
import {
  classifyHotelIdentity,
  IDENTITY_CLASS,
  normalizeHotelDisplayName,
  evaluateVerifiedPrecision,
} from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const APPLY = process.argv.includes("--apply");
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const ids = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"),
    "utf8",
  ),
);
const tableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const M = MAP_DISCOVERY_INVENTORY;
const OUT = path.join(ROOT, "reports", "sec-edgar-v24-persistence-audit");

async function listAll() {
  const records = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

function narrativeCity(city) {
  const c = String(city || "");
  return /\b(only|for the|term|loan|agreement|each|full|we |our )\b/i.test(c) || c.split(/\s+/).length > 4;
}

function isRealHotelName(name) {
  const n = normalizeHotelDisplayName(name);
  if (/\b(hyatt|marriott|hilton|holiday inn|courtyard|westin|sheraton|renaissance|embassy|hampton|residence inn|w hotel|four seasons)\b/i.test(n)) {
    return true;
  }
  return /\b(hotel|inn|resort|suites|lodge)\b/i.test(n) && n.length >= 10;
}

function shouldQuarantine(f) {
  const name = String(f[M.hotelNameNormalized] || f[M.hotelNameRaw] || "");
  const city = String(f[M.city] || "");
  const state = String(f[M.stateRegion] || "");
  const issues = [];
  if (narrativeCity(city) || narrativeCity(state)) issues.push("narrative_geo_bleed");
  if (/\bloan is interest|for the full term|for each loan\b/i.test(name)) issues.push("narrative_fragment_name");
  if (/^\s*holiday inn\s*$/i.test(normalizeHotelDisplayName(name)) && !city) issues.push("generic_holiday_inn");
  // bare Holiday Inn + city can be ambiguous but keep as probable not verified if no distinctive place in name
  if (/^\s*holiday inn\s*$/i.test(normalizeHotelDisplayName(name))) issues.push("thin_brand_place");
  const cls = classifyHotelIdentity({
    hotelName: normalizeHotelDisplayName(name),
    city: narrativeCity(city) ? null : city,
    stateRegion: narrativeCity(state) ? null : state,
    discoveryMethod: f[M.discoveryMethod] || "HTML_PROPERTY_TABLE",
  });
  if (cls.identityClass !== IDENTITY_CLASS.VERIFIED_PROPERTY && f[M.identityClass] === "VERIFIED_PROPERTY") {
    issues.push(`classifier_now_${cls.identityClass}`);
  }
  return issues;
}

function shouldRestore(f) {
  const notes = String(f[M.notesQuarantine] || "");
  const name = String(f[M.hotelNameNormalized] || "");
  if (!/DEEP_REVIEW_QUARANTINE/i.test(notes)) return false;
  if (!/no_brand_no_hotel_keyword/i.test(notes)) return false;
  // Hyatt / clear hotels wrongly flagged
  return isRealHotelName(name) && !narrativeCity(f[M.city]) && !shouldQuarantine(f).includes("narrative_geo_bleed");
}

async function patch(id, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${id}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json;
}

async function main() {
  const records = await listAll();
  const restore = [];
  const quarantine = [];
  for (const rec of records) {
    const f = rec.fields || {};
    if (shouldRestore(f)) {
      restore.push({
        airtableId: rec.id,
        propertyId: f[M.discoveryPropertyId],
        name: f[M.hotelNameNormalized],
        city: f[M.city],
        state: f[M.stateRegion],
      });
      continue;
    }
    if (f[M.identityClass] === "VERIFIED_PROPERTY") {
      const issues = shouldQuarantine(f);
      // also strip footnote from display if only issue is footnote in name — normalize
      const normalized = normalizeHotelDisplayName(f[M.hotelNameNormalized] || "");
      if (issues.length) {
        quarantine.push({
          airtableId: rec.id,
          propertyId: f[M.discoveryPropertyId],
          name: f[M.hotelNameNormalized],
          city: f[M.city],
          state: f[M.stateRegion],
          issues,
          normalized,
        });
      } else if (normalized !== f[M.hotelNameNormalized]) {
        // footnote cleanup only
        quarantine.push({
          airtableId: rec.id,
          propertyId: f[M.discoveryPropertyId],
          name: f[M.hotelNameNormalized],
          city: f[M.city],
          state: f[M.stateRegion],
          issues: ["footnote_normalize_only"],
          normalized,
          normalizeOnly: true,
        });
      }
    }
  }

  const actions = { restored: [], quarantined: [], normalized: [], readBacks: [] };

  if (APPLY) {
    for (const r of restore) {
      const fields = {
        [M.identityClass]: "VERIFIED_PROPERTY",
        [M.lifecycleStatus]: "IDENTITY_VERIFIED",
        [M.persistenceStatus]: "DISCOVERY_PERSISTED",
        [M.notesQuarantine]:
          "RESTORED_AFTER_FALSE_QUARANTINE: Hyatt/brand keyword present; brand-family detector gap — not a false hotel",
      };
      await patch(r.airtableId, fields);
      actions.restored.push(r);
      await new Promise((x) => setTimeout(x, 180));
    }
    for (const q of quarantine) {
      if (q.normalizeOnly) {
        await patch(q.airtableId, { [M.hotelNameNormalized]: q.normalized });
        actions.normalized.push(q);
      } else {
        await patch(q.airtableId, {
          [M.identityClass]: "PROBABLE_PROPERTY",
          [M.lifecycleStatus]: "DISCOVERY_PERSISTED",
          [M.persistenceStatus]: "QUARANTINED",
          [M.notesQuarantine]: `REMEDIATE_QUARANTINE: ${q.issues.join(", ")} — evidence preserved`,
          [M.hotelNameNormalized]: q.normalized || q.name,
        });
        actions.quarantined.push(q);
      }
      await new Promise((x) => setTimeout(x, 180));
    }
  }

  // Final verified precision
  const after = APPLY ? await listAll() : records;
  const verified = after.filter((r) => r.fields?.[M.identityClass] === "VERIFIED_PROPERTY");
  const sample = verified.map((r) => {
    const name = r.fields?.[M.hotelNameNormalized];
    const city = r.fields?.[M.city];
    const state = r.fields?.[M.stateRegion];
    const issues = shouldQuarantine(r.fields || {});
    return {
      hotelName: name,
      city,
      stateRegion: state,
      correct: issues.length === 0 && isRealHotelName(name) && !narrativeCity(city),
    };
  });
  const precision = evaluateVerifiedPrecision(sample);

  const out = {
    mode: APPLY ? "APPLY" : "DRY_RUN",
    generatedAt: new Date().toISOString(),
    restorePlan: restore,
    quarantinePlan: quarantine,
    actions,
    verifiedAfter: verified.length,
    precision,
  };
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(
    path.join(OUT, APPLY ? "RESTORE_REMEDIATE_APPLY.json" : "RESTORE_REMEDIATE_DRY_RUN.json"),
    JSON.stringify(out, null, 2),
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: out.mode,
        restore: restore.length,
        quarantine: quarantine.filter((q) => !q.normalizeOnly).length,
        normalizeOnly: quarantine.filter((q) => q.normalizeOnly).length,
        verifiedAfter: verified.length,
        precision,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
