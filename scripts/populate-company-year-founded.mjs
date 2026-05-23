/**
 * Populate empty "Year Founded" on Company Profile (high-confidence only).
 *
 *   node scripts/populate-company-year-founded.mjs           # dry-run
 *   node scripts/populate-company-year-founded.mjs --apply   # PATCH Airtable
 *
 * Skips companies without verified year (see reports/company-year-founded-audit.md).
 */
import "../load-env.js";
import { writeFileSync } from "fs";
import { join } from "path";

const TABLE = process.env.COMPANY_PROFILE_TABLE_ID || "tblItyfH6MlOnMKZ9";
const FIELD = "Year Founded";
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const apply = process.argv.includes("--apply");

/** recordId → year (only entries applied after public-source review) */
const POPULATE_EMPTY = {
  rectkHHTWMc6p4i63: "2006", // Arbor Lodging — HospitalityNet / company materials
  reccBsCVdn1v0MWJK: "2005", // Bridgepoint Hospitality (Caribbean) Ltd — Bridgepoint Hospitality formed 2005
  recFOG6MrXmlKxWNB: "2010", // Invest Costa Rica — investorlist / company site
  rec9CvB2NyQ6xZpYV: "2021", // Mullen Real Estate Capital — mullencap / Tracxn
  rec5SBhA13LifgMIZ: "2021", // Newbond Holdings — newbond.com / Tracxn
  recYiqZOGpRItn5DF: "1937", // Sonesta — Sonnabend hotel business origins
  rec9QLkCHI0IKBeWi: "2016", // Sygnus Group — incorporated June 2016
  // recb3dNcLhTyGOhlZ Cachagua Group — no public founding year; site is "launching soon"
};

async function fetchAll() {
  const records = [];
  let offset;
  for (;;) {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}?${qs}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }
  return records;
}

async function patchBatch(batch) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      records: batch.map(({ id, year }) => ({ id, fields: { [FIELD]: year } })),
      typecast: true,
    }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
}

const records = await fetchAll();
const byId = new Map(records.map((r) => [r.id, r]));

const planned = [];
for (const [id, year] of Object.entries(POPULATE_EMPTY)) {
  const rec = byId.get(id);
  if (!rec) {
    console.warn("Missing record", id);
    continue;
  }
  const name = String(rec.fields["Company Name"] || "").trim();
  const current = String(rec.fields[FIELD] || "").trim();
  if (current) {
    console.warn(`Skip ${name}: already has Year Founded "${current}"`);
    continue;
  }
  planned.push({ id, name, year });
}

console.log(apply ? "APPLY mode" : "DRY-RUN");
for (const p of planned) {
  console.log(`  ${p.name} → ${p.year} (${p.id})`);
}

if (planned.length && apply) {
  for (let i = 0; i < planned.length; i += 10) {
    await patchBatch(planned.slice(i, i + 10));
  }
  console.log(`Updated ${planned.length} record(s).`);
} else if (!apply) {
  console.log(`Would update ${planned.length} record(s). Pass --apply to write.`);
}
