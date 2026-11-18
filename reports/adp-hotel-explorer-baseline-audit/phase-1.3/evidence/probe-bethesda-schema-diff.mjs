import "dotenv/config";
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
const id = "recLuxvwwxID7U2B8";
const outDir = dirname(fileURLToPath(import.meta.url));

const metaRes = await fetch(
  `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
  { headers: { Authorization: `Bearer ${key}` } }
);
const meta = await metaRes.json();

function summarizeTable(t) {
  return {
    id: t.id,
    name: t.name,
    fields: (t.fields || []).map((f) => f.name).sort(),
  };
}

const census = (meta.tables || []).find((t) => t.name === "Hotel Census");
const propCensus = (meta.tables || []).find(
  (t) => t.name === "Hotel Property Census"
);

const byIdCensus = await fetch(
  `https://api.airtable.com/v0/${baseId}/${census.id}/${id}`,
  { headers: { Authorization: `Bearer ${key}` } }
).then((r) => r.json());

let byIdProp = null;
if (propCensus) {
  byIdProp = await fetch(
    `https://api.airtable.com/v0/${baseId}/${propCensus.id}/${id}`,
    { headers: { Authorization: `Bearer ${key}` } }
  ).then((r) => r.json());
}

const recordFields = Object.keys(byIdCensus.fields || {}).sort();
const schemaFields = new Set((census.fields || []).map((f) => f.name));
const onlyInRecord = recordFields.filter((f) => !schemaFields.has(f));
const onlyInSchemaSample = [...schemaFields]
  .filter((f) => /name|city|country|brand|affiliation|status|lat|room/i.test(f))
  .sort();

// Try listing with view that might include US / all
const views = (census.views || []).map((v) => ({ id: v.id, name: v.name, type: v.type }));
const viewProbes = [];
for (const v of views.slice(0, 12)) {
  const url = `https://api.airtable.com/v0/${baseId}/${census.id}?view=${encodeURIComponent(v.name)}&pageSize=100&filterByFormula=${encodeURIComponent(`RECORD_ID()='${id}'`)}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${key}` },
  });
  const json = await res.json();
  viewProbes.push({
    view: v.name,
    status: res.status,
    count: json.records?.length ?? null,
    error: json.error || json.message || null,
  });
}

const report = {
  census: summarizeTable(census),
  propCensus: propCensus ? summarizeTable(propCensus) : null,
  sameTableIds: census?.id && propCensus?.id ? census.id === propCensus.id : null,
  byIdCensusOk: !byIdCensus.error,
  byIdPropOk: byIdProp ? !byIdProp.error : null,
  byIdPropError: byIdProp?.error || null,
  recordFieldCount: recordFields.length,
  schemaFieldCount: schemaFields.size,
  onlyInRecord,
  onlyInSchemaSample,
  hasLegacyNameInSchema: schemaFields.has("name"),
  hasPropertyNameInSchema: schemaFields.has("Property Name"),
  hasCountryTitleInSchema: schemaFields.has("Country"),
  hasCountryLowerInSchema: schemaFields.has("country"),
  views,
  viewProbes,
};

writeFileSync(
  join(outDir, "bethesda-schema-diff.json"),
  JSON.stringify(report, null, 2)
);
console.log(
  JSON.stringify(
    {
      sameTableIds: report.sameTableIds,
      onlyInRecord: report.onlyInRecord,
      hasPropertyNameInSchema: report.hasPropertyNameInSchema,
      hasCountryTitleInSchema: report.hasCountryTitleInSchema,
      hasLegacyNameInSchema: report.hasLegacyNameInSchema,
      hasCountryLowerInSchema: report.hasCountryLowerInSchema,
      viewProbes: report.viewProbes,
      byIdPropOk: report.byIdPropOk,
      byIdPropError: report.byIdPropError,
    },
    null,
    2
  )
);
