import "dotenv/config";
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
const id = "recLuxvwwxID7U2B8";
const outDir = dirname(fileURLToPath(import.meta.url));

const meta = await fetch(
  `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
  { headers: { Authorization: `Bearer ${key}` } }
).then((r) => r.json());

const census = (meta.tables || []).find((t) => t.name === "Hotel Census");
const prop = (meta.tables || []).find((t) => t.name === "Hotel Property Census");

async function getTable(tableId) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}/${id}`, {
    headers: { Authorization: `Bearer ${key}` },
  });
  const json = await res.json();
  return {
    status: res.status,
    id: json.id || null,
    error: json.error || null,
    fieldCount: json.fields ? Object.keys(json.fields).length : 0,
    name: json.fields?.name || json.fields?.["Property Name"] || null,
    city: json.fields?.city || json.fields?.City || null,
    country: json.fields?.country || json.fields?.Country || null,
    sampleKeys: json.fields ? Object.keys(json.fields).sort().slice(0, 20) : [],
  };
}

async function listHas(tableId, label) {
  const formula = encodeURIComponent(`RECORD_ID()='${id}'`);
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${tableId}?filterByFormula=${formula}&maxRecords=1`,
    { headers: { Authorization: `Bearer ${key}` } }
  );
  const json = await res.json();
  return {
    label,
    status: res.status,
    count: json.records?.length ?? null,
    error: json.error || json.message || null,
  };
}

// Also try a random Hotel Census record list sample names
const sample = await fetch(
  `https://api.airtable.com/v0/${baseId}/${census.id}?pageSize=3&fields%5B%5D=name&fields%5B%5D=country`,
  { headers: { Authorization: `Bearer ${key}` } }
).then((r) => r.json());

const propSample = await fetch(
  `https://api.airtable.com/v0/${baseId}/${prop.id}?pageSize=3&fields%5B%5D=${encodeURIComponent("Property Name")}&fields%5B%5D=${encodeURIComponent("Country")}`,
  { headers: { Authorization: `Bearer ${key}` } }
).then((r) => r.json());

const report = {
  censusId: census.id,
  propId: prop.id,
  getCensus: await getTable(census.id),
  getProp: await getTable(prop.id),
  listCensus: await listHas(census.id, "Hotel Census"),
  listProp: await listHas(prop.id, "Hotel Property Census"),
  censusSample: (sample.records || []).map((r) => ({
    id: r.id,
    name: r.fields?.name,
    country: r.fields?.country,
  })),
  propSampleError: propSample.error || null,
  propSample: (propSample.records || []).map((r) => ({
    id: r.id,
    name: r.fields?.["Property Name"],
    country: r.fields?.Country,
  })),
  propSchemaHasPropertyName: (prop.fields || []).some(
    (f) => f.name === "Property Name"
  ),
  censusSchemaHasPropertyName: (census.fields || []).some(
    (f) => f.name === "Property Name"
  ),
};

writeFileSync(join(outDir, "bethesda-two-tables.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
