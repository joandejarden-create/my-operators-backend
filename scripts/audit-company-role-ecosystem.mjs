/**
 * Audit Company Profile "Company's role in the hotel ecosystem" values vs normalization.
 * Usage: node scripts/audit-company-role-ecosystem.mjs
 */
import "../load-env.js";
import {
  COMPANY_ROLE_AIRTABLE_FIELD,
  companyRoleFromEcosystemField,
  normalizeCompanyRoleToForm,
} from "../lib/company-role-normalize.js";

const TABLE = process.env.COMPANY_PROFILE_TABLE_ID || "tblItyfH6MlOnMKZ9";
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;

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

const records = await fetchAll();
const byRaw = new Map();

for (const r of records) {
  const f = r.fields || {};
  let raw = f[COMPANY_ROLE_AIRTABLE_FIELD];
  let fieldKey = COMPANY_ROLE_AIRTABLE_FIELD;
  if (raw == null) {
    for (const k of Object.keys(f)) {
      if (k.toLowerCase().includes("role") && k.toLowerCase().includes("ecosystem")) {
        raw = f[k];
        fieldKey = k;
        break;
      }
    }
  }
  const rawStr = raw == null ? "(empty)" : String(raw).trim();
  const norm = companyRoleFromEcosystemField(f);
  const key = rawStr;
  if (!byRaw.has(key)) {
    byRaw.set(key, { count: 0, norm, fieldKey, samples: [] });
  }
  const row = byRaw.get(key);
  row.count++;
  if (row.samples.length < 3) {
    row.samples.push(String(f["Company Name"] || "").trim() || r.id);
  }
}

console.log("Field:", COMPANY_ROLE_AIRTABLE_FIELD);
console.log("Total companies:", records.length);
console.log("\nUnique values (raw → normalized):");
for (const [raw, info] of [...byRaw.entries()].sort((a, b) => b[1].count - a[1].count)) {
  console.log(
    JSON.stringify({
      raw,
      count: info.count,
      normalized: info.norm,
      fieldKey: info.fieldKey !== COMPANY_ROLE_AIRTABLE_FIELD ? info.fieldKey : undefined,
      samples: info.samples,
    })
  );
}

const both = records.filter((r) => companyRoleFromEcosystemField(r.fields) === "Both");
console.log("\nNormalized Both:", both.length);
for (const r of both.slice(0, 10)) {
  const raw = r.fields[COMPANY_ROLE_AIRTABLE_FIELD];
  console.log(" -", r.fields["Company Name"], "|", raw);
}

const unmapped = records.filter((r) => {
  const f = r.fields || {};
  const raw = f[COMPANY_ROLE_AIRTABLE_FIELD];
  if (raw == null || String(raw).trim() === "") return false;
  return !normalizeCompanyRoleToForm(raw);
});
if (unmapped.length) {
  console.log("\nNON-EMPTY BUT UNMAPPED:", unmapped.length);
  for (const r of unmapped.slice(0, 15)) {
    console.log(" -", r.fields["Company Name"], "|", r.fields[COMPANY_ROLE_AIRTABLE_FIELD]);
  }
}
