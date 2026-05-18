/**
 * Read-only audit: readiness field inventory + response quality sample.
 * Usage: node scripts/audit-deal-readiness-fields.mjs [--max=25] [--dealId=rec...]
 * Requires AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env (repo root).
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  REQUIRED_DEAL_SETUP_FIELDS,
  isFieldFilled,
  fetchDealWithMergedLinkedRecords,
} from "../api/my-deals.js";
import { readinessTabForField } from "../api/deal-readiness-field-tabs.js";
import {
  DEALS_FORM_TO_AIRTABLE,
  LOCATION_FORM_TO_AIRTABLE,
  DEALS_TABLE,
} from "../api/schemas/deal-setup-fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, "output");

const WEAK_PATTERNS = [
  /^tbd$/i,
  /^tbc$/i,
  /^n\/?a$/i,
  /^na$/i,
  /^unknown$/i,
  /^not sure$/i,
  /^to be determined$/i,
  /^flexible$/i,
  /^open$/i,
  /^any$/i,
  /^none$/i,
  /^pending$/i,
  /^todo$/i,
  /^test$/i,
  /lorem ipsum/i,
  /^—$/,
  /^-$/,
];

function airtableColumnForFormKey(formKey) {
  if (LOCATION_FORM_TO_AIRTABLE[formKey]) return LOCATION_FORM_TO_AIRTABLE[formKey];
  if (DEALS_FORM_TO_AIRTABLE[formKey]) return DEALS_FORM_TO_AIRTABLE[formKey];
  return formKey;
}

function isWeakValue(val) {
  if (val == null) return false;
  if (typeof val === "number" && Number.isFinite(val)) return false;
  if (Array.isArray(val)) {
    if (!val.length) return true;
    return val.every((x) => {
      const s = typeof x === "string" ? x : x?.name || "";
      return isWeakValue(s);
    });
  }
  if (typeof val !== "string") return false;
  const s = val.trim();
  if (!s) return true;
  return WEAK_PATTERNS.some((re) => re.test(s));
}

function strSample(val, max = 48) {
  if (val == null) return "";
  if (Array.isArray(val)) {
    return val
      .map((x) => (typeof x === "string" ? x : x?.name || ""))
      .filter(Boolean)
      .join(", ")
      .slice(0, max);
  }
  if (typeof val === "object" && val?.name) return String(val.name).slice(0, max);
  return String(val).trim().slice(0, max);
}

async function listDealRecords(baseId, apiKey, maxRecords) {
  const table = encodeURIComponent(DEALS_TABLE);
  const url = new URL(`https://api.airtable.com/v0/${baseId}/${table}`);
  url.searchParams.set("pageSize", String(Math.min(100, maxRecords)));
  url.searchParams.append(
    "fields[]",
    "Property Name"
  );
  const records = [];
  let offset;
  do {
    if (offset) url.searchParams.set("offset", offset);
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error?.message || res.statusText);
    records.push(...(data.records || []));
    offset = data.offset;
    if (records.length >= maxRecords) break;
  } while (offset);
  return records.slice(0, maxRecords);
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY in .env");
    process.exit(1);
  }

  const args = process.argv.slice(2);
  let maxDeals = 25;
  let singleDealId = "";
  for (const a of args) {
    if (a.startsWith("--max=")) maxDeals = Number(a.split("=")[1]) || 25;
    if (a.startsWith("--dealId=")) singleDealId = a.split("=")[1].trim();
  }

  const dealIds = [];
  if (singleDealId) {
    dealIds.push(singleDealId);
  } else {
    const listed = await listDealRecords(baseId, apiKey, maxDeals);
    for (const r of listed) dealIds.push(r.id);
  }

  const fieldStats = {};
  for (const f of REQUIRED_DEAL_SETUP_FIELDS) {
    fieldStats[f] = {
      blank: 0,
      populated: 0,
      weak: 0,
      valueCounts: {},
      samples: [],
    };
  }

  const dealSummaries = [];
  let sampled = 0;

  for (const dealId of dealIds) {
    try {
      const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, dealId);
      if (!full?.deal?.fields) continue;
      sampled += 1;
      const fields = full.deal.fields;
      const name = strSample(fields["Property Name"] || full.normalized?.propertyName, 80) || dealId;
      let blankN = 0;
      let weakN = 0;
      for (const fname of REQUIRED_DEAL_SETUP_FIELDS) {
        const filled = isFieldFilled(fields[fname]);
        const st = fieldStats[fname];
        if (!filled) {
          st.blank += 1;
          blankN += 1;
        } else {
          st.populated += 1;
          const sample = strSample(fields[fname]);
          if (sample) {
            st.valueCounts[sample] = (st.valueCounts[sample] || 0) + 1;
            if (st.samples.length < 5 && !st.samples.includes(sample)) st.samples.push(sample);
          }
          if (isWeakValue(fields[fname])) {
            st.weak += 1;
            weakN += 1;
          }
        }
      }
      dealSummaries.push({ dealId, name, blankN, weakN });
    } catch (e) {
      dealSummaries.push({ dealId, error: e.message });
    }
  }

  const quality = REQUIRED_DEAL_SETUP_FIELDS.map((fname) => {
    const st = fieldStats[fname];
    const topValues = Object.entries(st.valueCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([v, c]) => ({ value: v, count: c }));
    return {
      formKey: fname,
      tab: readinessTabForField(fname),
      airtableColumn: airtableColumnForFormKey(fname),
      sampled,
      blank: st.blank,
      populated: st.populated,
      weak: st.weak,
      blankPct: sampled ? Math.round((100 * st.blank) / sampled) : null,
      topValues,
      sampleValues: st.samples,
    };
  });

  mkdirSync(OUT_DIR, { recursive: true });
  const outPath = join(OUT_DIR, "deal-readiness-field-audit-data.json");
  writeFileSync(
    outPath,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        sampled,
        dealSummaries,
        fieldQuality: quality,
      },
      null,
      2
    )
  );
  console.log(`Wrote ${outPath} (${sampled} deals sampled)`);

  const xavier = dealSummaries.find((d) => /xavier/i.test(d.name || ""));
  if (xavier) console.log("Xavier candidate:", xavier.dealId, xavier.name);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
