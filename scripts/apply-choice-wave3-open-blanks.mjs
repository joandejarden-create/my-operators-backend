#!/usr/bin/env node
import { readFileSync, writeFileSync } from "fs";
import Airtable from "airtable";
import "../load-env.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const APPLY = process.argv.includes("--apply");
const p = JSON.parse(readFileSync("reports/choice-census-regional-enrichment-plan.json", "utf8"));

// Collect all regional hotels from fetchLog
const hotels = [];
for (const entry of p.fetchLog || []) {
  for (const h of entry.hotels || entry.properties || []) hotels.push(h);
}
// Also scan nested
function walk(obj, out = []) {
  if (!obj || typeof obj !== "object") return out;
  if (Array.isArray(obj)) {
    for (const x of obj) walk(x, out);
    return out;
  }
  if (obj.propertyUrl || obj.url) {
    if (obj.name || obj.propertyId) out.push(obj);
  }
  for (const v of Object.values(obj)) {
    if (v && typeof v === "object") walk(v, out);
  }
  return out;
}
const all = walk(p);
console.log("walked hotel-like", all.length);

const NEEDLES = [
  {
    recordId: "recMaPQqCZwid4h3j",
    re: /puembo|ec001/i,
    fallbackUrl: "https://www.choicehotels.com/ecuador/quito/ascend-hotels/ec001",
    fallbackPid: "EC001",
  },
  {
    recordId: "recXsdIoD3FNG093n",
    re: /emotions.*puerto plata|puerto plata.*emotions|do013/i,
    fallbackUrl: "https://www.choicehotels.com/dominican-republic/playa-dorada/ascend-hotels/do013",
    fallbackPid: "DO013",
  },
];

const planRows = [];
for (const n of NEEDLES) {
  const hit = all.find((h) => n.re.test(String(h.name || "")) || n.re.test(String(h.propertyUrl || h.url || "")));
  let url = hit?.propertyUrl || hit?.url || n.fallbackUrl;
  let pid = hit?.propertyId || hit?.code || n.fallbackPid;
  if (!pid && url) {
    const m = url.match(/\/([a-z]{2}\d{3})\/?$/i);
    if (m) pid = m[1].toUpperCase();
  }
  if (url && pid) {
    planRows.push({
      censusRecordId: n.recordId,
      propertyUrl: url.split("?")[0].replace(/\/$/, ""),
      propertyId: String(pid).toUpperCase(),
      sourceName: hit?.name || "fallback_search",
      applyFields: {
        Website: url.split("?")[0].replace(/\/$/, ""),
        "Property ID": String(pid).toUpperCase(),
      },
    });
  } else {
    console.log("NO MATCH", n.recordId, hit?.name, hit?.propertyUrl);
  }
}

console.log(JSON.stringify(planRows, null, 2));
writeFileSync(
  "reports/choice-wave3-open-blank-manual-plan.json",
  JSON.stringify({ generatedAt: new Date().toISOString(), planRows }, null, 2)
);

if (!APPLY) {
  console.log("DRY-RUN");
  process.exit(0);
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
for (const row of planRows) {
  await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
    typecast: true,
  });
  console.log("UPDATED", row.censusRecordId, row.propertyId);
}
