import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
const id = "recLuxvwwxID7U2B8";
const outDir = dirname(fileURLToPath(import.meta.url));

async function api(path) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${path}`, {
    headers: { Authorization: `Bearer ${key}` },
  });
  const json = await res.json().catch(() => ({}));
  return { status: res.status, json };
}

const metaRes = await fetch(
  `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
  { headers: { Authorization: `Bearer ${key}` } }
);
const meta = await metaRes.json();
const hotelTables = (meta.tables || [])
  .filter((t) => /census|hotel/i.test(t.name))
  .map((t) => ({
    id: t.id,
    name: t.name,
    fieldCount: (t.fields || []).length,
    views: (t.views || []).map((v) => ({ id: v.id, name: v.name, type: v.type })),
  }));

const census = (meta.tables || []).find((t) => t.name === "Hotel Census");
const nameFields = census
  ? (census.fields || [])
      .filter((f) =>
        /name|city|country|brand|affiliation|latitude|status|property/i.test(
          f.name
        )
      )
      .map((f) => ({ id: f.id, name: f.name, type: f.type }))
  : [];

const report = {
  hotelTables,
  censusTableId: census?.id || null,
  nameFields,
};

if (census) {
  const byId = await api(`${census.id}/${id}`);
  report.byId = {
    status: byId.status,
    name:
      byId.json?.fields?.["Property Name"] ||
      byId.json?.fields?.name ||
      byId.json?.error ||
      null,
    country:
      byId.json?.fields?.Country || byId.json?.fields?.country || null,
    fieldKeys: byId.json?.fields
      ? Object.keys(byId.json.fields).sort()
      : null,
  };

  const formula = encodeURIComponent(`RECORD_ID()='${id}'`);
  const listed = await api(`${census.id}?filterByFormula=${formula}&maxRecords=1`);
  report.listByRecordId = {
    status: listed.status,
    count: listed.json?.records?.length ?? null,
    error: listed.json?.error || listed.json?.message || null,
    firstId: listed.json?.records?.[0]?.id || null,
  };

  // Scan first 5 pages for the id
  let offset = null;
  let scanned = 0;
  let found = false;
  for (let page = 0; page < 8; page++) {
    const qs = new URLSearchParams({
      pageSize: "100",
      "fields[]": "name",
    });
    // URLSearchParams doesn't do multi fields well; build manually
    let path = `${census.id}?pageSize=100&fields%5B%5D=name&fields%5B%5D=Latitude`;
    if (offset) path += `&offset=${encodeURIComponent(offset)}`;
    const pageRes = await api(path);
    const records = pageRes.json?.records || [];
    scanned += records.length;
    if (records.some((r) => r.id === id)) found = true;
    offset = pageRes.json?.offset || null;
    if (!offset || found) break;
  }
  report.listScan = { scanned, found, stoppedEarly: found || !offset };

  // Count approximate via filter on Country title case using field id if available
  const countryField = (census.fields || []).find((f) => f.name === "Country");
  const nameField = (census.fields || []).find((f) => f.name === "Property Name");
  const legacyName = (census.fields || []).find((f) => f.name === "name");
  report.fieldIds = {
    Country: countryField?.id || null,
    "Property Name": nameField?.id || null,
    name: legacyName?.id || null,
  };

  if (countryField?.id) {
    const f = encodeURIComponent(`{${countryField.name}}='United States'`);
    const us = await api(
      `${census.id}?filterByFormula=${f}&maxRecords=5&fields%5B%5D=${encodeURIComponent(countryField.name)}&fields%5B%5D=${encodeURIComponent(nameField?.name || "name")}`
    );
    report.usByCountryField = {
      status: us.status,
      count: us.json?.records?.length ?? null,
      error: us.json?.error || us.json?.message || null,
      sample: (us.json?.records || []).map((r) => ({
        id: r.id,
        name: r.fields?.["Property Name"] || r.fields?.name || null,
      })),
    };
  }
}

mkdirSync(outDir, { recursive: true });
const outPath = join(outDir, "bethesda-airtable-meta.json");
writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ wrote: outPath, summary: {
  censusTableId: report.censusTableId,
  byId: report.byId,
  listByRecordId: report.listByRecordId,
  listScan: report.listScan,
  usByCountryField: report.usByCountryField,
  fieldIds: report.fieldIds,
}}, null, 2));
