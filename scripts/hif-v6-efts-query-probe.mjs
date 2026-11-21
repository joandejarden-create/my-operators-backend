#!/usr/bin/env node
const UA = "Dealality Research joan@aohospitalityadvisors.com";

async function q(query, forms) {
  const p = new URLSearchParams({
    q: query,
    dateRange: "custom",
    startdt: "2010-01-01",
    enddt: "2026-11-21",
    from: "0",
    size: "5",
  });
  if (forms) p.set("forms", forms);
  const r = await fetch(`https://efts.sec.gov/LATEST/search-index?${p}`, {
    headers: { "User-Agent": UA, Accept: "application/json" },
  });
  const j = await r.json();
  return {
    query,
    forms: forms || null,
    total: j.hits?.total?.value ?? j.hits?.total,
    status: r.status,
  };
}

const queries = [
  ['"Franchise Expiration Date"', "424B2"],
  ['"Franchise Expiration Date"', null],
  ['"Franchise Expiration"', "424B2"],
  ['"Franchise Expiration Date"', "424B5"],
  ['"Management Agreement Expiration"', null],
  ['"Expiration of Franchise"', null],
  ['"Remaining Franchise Term"', null],
  ['"franchise expires" hotel', null],
  ['"Ground Lease Expiration" hotel', "424B2"],
  ['"Franchise / License Expiration"', null],
  ['"License Expiration Date" hotel', "424B2"],
];

const out = [];
for (const [qq, f] of queries) {
  out.push(await q(qq, f));
  await new Promise((r) => setTimeout(r, 250));
}
console.log(JSON.stringify(out, null, 2));
