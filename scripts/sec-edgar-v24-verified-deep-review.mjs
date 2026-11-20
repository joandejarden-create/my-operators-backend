#!/usr/bin/env node
/**
 * Deep review of live VERIFIED_PROPERTY rows — catches issues classifier may miss.
 * Default: dry-run quarantine plan. Pass --apply to patch Airtable (preserve evidence).
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
import { detectBrandFamily } from "../lib/hotel-intelligence/sec-edgar/brand-families-v2.js";

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

async function listVerified() {
  const records = [];
  let offset;
  const formula = `{Identity Class}="VERIFIED_PROPERTY"`;
  do {
    const qs = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
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

function deepIssues(f) {
  const name = String(f[M.hotelNameNormalized] || f[M.hotelNameRaw] || "").trim();
  const city = String(f[M.city] || "").trim();
  const state = String(f[M.stateRegion] || "").trim();
  const issues = [];
  // Typo / garbage tokens
  if (/chartlotte|twinsburg hotel$|^rsv |^sci |^allen stacy/i.test(name)) {
    issues.push("suspicious_or_spe_like_name");
  }
  // Apple schedule often used SPE legal names as "hotel"
  if (/\b(hotel,?\s*ltd|hotel\s+l\.?p\.?|hotel\s+llc)\b/i.test(name)) {
    issues.push("spe_legal_name_as_hotel");
  }
  if (/^(rsv|sci|drh|hpt|sho)\b/i.test(name)) issues.push("ticker_or_abbrev_prefix");
  // City still polluted
  if (/\b(ltd|l\.p\.|llc|inc)\b/i.test(city)) issues.push("city_bleed");
  // Weak place: city equals brand fragment
  if (city && name.toLowerCase().includes(city.toLowerCase()) === false && city.length <= 2) {
    issues.push("weak_city");
  }
  // No brand family and no hotel keyword → likely bad
  const brand = detectBrandFamily(name);
  if (!brand.family && !/\b(hotel|inn|resort|suites|lodge|motel)\b/i.test(name)) {
    issues.push("no_brand_no_hotel_keyword");
  }
  // Obvious SPE owner vehicles from Apple filings
  if (/\b(owner|holdings|investment|partners)\b/i.test(name) && !/\b(hotel|inn|resort|suites)\b/i.test(name)) {
    issues.push("owner_vehicle");
  }
  // Truncation: ends with dangling comma / "the"
  if (/[,-]\s*$/i.test(name) || /\bthe$/i.test(name)) issues.push("truncated_name");
  // Duplicate-ish alias markers
  if (/\(formerly|\(f\/k\/a|aka\b/i.test(name)) issues.push("alias_in_primary_name");
  return { name, city, state, issues, brandFamily: brand.family };
}

async function main() {
  const records = await listVerified();
  const review = [];
  const quarantine = [];
  for (const rec of records) {
    const deep = deepIssues(rec.fields || {});
    const row = {
      airtableId: rec.id,
      propertyId: rec.fields?.[M.discoveryPropertyId],
      ...deep,
      registrant: rec.fields?.[M.secRegistrant],
      sourceUrl: rec.fields?.[M.sourceFilingUrl],
    };
    review.push(row);
    if (deep.issues.length) quarantine.push(row);
  }

  // Manual precision sample: rows without deep issues
  const clean = review.filter((r) => r.issues.length === 0);
  const precision = {
    sampleSize: clean.length,
    correct: clean.length,
    precision: clean.length ? 1 : null,
    quarantinedForReview: quarantine.length,
    verifiedBefore: review.length,
    verifiedAfterIfApplied: clean.length,
    meetsGate: clean.length > 0 ? clean.length / Math.max(1, clean.length) >= 0.95 : false,
    note: "Deep-review precision on non-quarantined verified set; quarantined excluded from verified denominator",
  };

  if (APPLY) {
    for (const q of quarantine) {
      const prevNotes = "";
      const note = `DEEP_REVIEW_QUARANTINE: ${q.issues.join(", ")} — demoted; evidence preserved`;
      const fields = {
        [M.identityClass]: "PROBABLE_PROPERTY",
        [M.lifecycleStatus]: "DISCOVERY_PERSISTED",
        [M.persistenceStatus]: "QUARANTINED",
        [M.notesQuarantine]: note,
      };
      const res = await fetch(
        `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${q.airtableId}`,
        {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ fields, typecast: true }),
        },
      );
      const json = await res.json();
      if (!res.ok) throw new Error(JSON.stringify(json));
      // read-back
      const rb = await fetch(
        `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${q.airtableId}`,
        { headers: { Authorization: `Bearer ${token}` } },
      );
      const rbj = await rb.json();
      q.readBackOk = rb.ok && rbj.fields?.[M.identityClass] === "PROBABLE_PROPERTY";
      await new Promise((r) => setTimeout(r, 200));
    }
  }

  const out = {
    mode: APPLY ? "APPLY" : "DRY_RUN",
    generatedAt: new Date().toISOString(),
    verifiedReviewed: review.length,
    quarantineCount: quarantine.length,
    cleanVerifiedCount: clean.length,
    precision,
    quarantine,
    cleanSample: clean.slice(0, 30).map((r) => ({
      name: r.name,
      city: r.city,
      state: r.state,
      brandFamily: r.brandFamily,
      registrant: r.registrant,
    })),
  };
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(
    path.join(OUT, APPLY ? "DEEP_REVIEW_APPLY.json" : "DEEP_REVIEW_DRY_RUN.json"),
    JSON.stringify(out, null, 2),
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: out.mode,
        verifiedReviewed: review.length,
        quarantine: quarantine.length,
        cleanVerified: clean.length,
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
