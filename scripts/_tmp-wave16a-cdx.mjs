import fs from "node:fs";

async function cdx(query) {
  const url = `http://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(query)}&matchType=prefix&output=json&filter=statuscode:200&limit=20&fl=timestamp,original,statuscode,mime`;
  const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 DealalityBot" } });
  const j = await r.json().catch(() => []);
  return Array.isArray(j) ? j.slice(1) : [];
}

const queries = [
  "www.marriott.com/en-us/hotels/*fairfield*",
  "www.marriott.com/en-us/hotels/*four-points*",
  "www.marriott.com/en-us/hotels/*delta-hotels*",
  "www.marriott.com/hotels/travel/*fairfield*",
  "www.marriott.com/hotels/travel/*four-points*",
  "www.marriott.com/hotels/travel/*delta*",
  "fairfield.marriott.com/*",
  "four-points.marriott.com/*",
  "delta-hotels.marriott.com/*",
];

const all = {};
for (const q of queries) {
  const rows = await cdx(q);
  console.log(q, rows.length);
  all[q] = rows.slice(0, 15).map((r) => ({ ts: r[0], original: r[1] }));
  for (const r of all[q].slice(0, 5)) console.log(" ", r.ts, r.original);
}

fs.writeFileSync("reports/_tmp-wave16a-cdx.json", JSON.stringify(all, null, 2));
