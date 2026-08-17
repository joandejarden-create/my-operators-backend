#!/usr/bin/env node
import fs from "node:fs";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
  Accept: "text/html,image/*,*/*",
};

const CANDIDATES = [
  {
    marsha: "YYCDA",
    name: "Delta Hotels Calgary Downtown",
    url: "https://www.marriott.com/en-us/hotels/yycda-delta-hotels-calgary-downtown/overview/",
  },
  {
    marsha: "YYCDL",
    name: "Delta Hotels Calgary Downtown",
    url: "https://www.marriott.com/en-us/hotels/yycdl-delta-hotels-calgary-downtown/overview/",
  },
  {
    marsha: "YULDA",
    name: "Delta Hotels Montreal",
    url: "https://www.marriott.com/en-us/hotels/yulda-delta-hotels-montreal/overview/",
  },
  {
    marsha: "YULDL",
    name: "Delta Hotels Montreal",
    url: "https://www.marriott.com/en-us/hotels/yuldl-delta-hotels-montreal/overview/",
  },
  {
    marsha: "CUNDL",
    name: "Delta Hotels Cancun Inn",
    url: "https://www.marriott.com/en-us/hotels/cundl-delta-hotels-cancun-inn/overview/",
  },
  {
    marsha: "MEXDL",
    name: "Delta Hotels Mexico City Metropolitan",
    url: "https://www.marriott.com/en-us/hotels/mexdl-delta-hotels-mexico-city-metropolitan/overview/",
  },
  {
    marsha: "OTTDL",
    name: "Delta Hotels Ottawa City Centre",
    url: "https://www.marriott.com/en-us/hotels/ottdl-delta-hotels-ottawa-city-centre/overview/",
  },
  {
    marsha: "YOWDL",
    name: "Delta Hotels Ottawa City Centre",
    url: "https://www.marriott.com/en-us/hotels/yowdl-delta-hotels-ottawa-city-centre/overview/",
  },
];

async function headOk(url) {
  try {
    const r = await fetch(url, { method: "HEAD", headers: HEADERS, redirect: "follow" });
    const ct = r.headers.get("content-type") || "";
    return r.ok && /image/i.test(ct);
  } catch {
    return false;
  }
}

function extractDam(html, marsha) {
  const s = String(html || "").replace(/\\\//g, "/");
  const re = new RegExp(
    `https?:\\/\\/cache\\.marriott\\.com\\/content\\/dam\\/marriott-renditions\\/${marsha}\\/[a-z0-9._-]+\\.(?:jpg|jpeg|png|webp)`,
    "gi"
  );
  const found = [...s.matchAll(re)].map((m) => m[0].split("?")[0]);
  const re2 = new RegExp(
    `\\/content\\/dam\\/marriott-renditions\\/${marsha}\\/([a-z0-9._-]+\\.(?:jpg|jpeg|png|webp))`,
    "gi"
  );
  for (const m of s.matchAll(re2)) {
    found.push(`https://cache.marriott.com/content/dam/marriott-renditions/${marsha}/${m[1]}`);
  }
  return [...new Set(found)].filter((u) => /hor-wide|hor-clsc/i.test(u));
}

const out = {};
for (const c of CANDIDATES) {
  const cdx = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(c.url)}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=5`;
  let snaps = [];
  try {
    const text = await (await fetch(cdx, { headers: HEADERS })).text();
    if (text.trim().startsWith("[")) snaps = JSON.parse(text).slice(1);
  } catch {
    /* ignore */
  }
  const urls = new Set();
  for (const row of snaps.slice(0, 3)) {
    const wb = `https://web.archive.org/web/${row[0]}id_/${row[1]}`;
    try {
      const hr = await fetch(wb, { headers: HEADERS, redirect: "follow" });
      if (!hr.ok) continue;
      for (const u of extractDam(await hr.text(), c.marsha)) urls.add(u);
    } catch {
      /* ignore */
    }
  }
  const ok = [];
  for (const u of urls) {
    if (await headOk(u)) ok.push(u);
  }
  console.log(c.marsha, "snaps", snaps.length, "ok", ok.length);
  for (const u of ok.slice(0, 6)) console.log(" ", u.split("/").pop());
  out[c.marsha] = { ...c, ok };
}

fs.writeFileSync("reports/_tmp-wave16a-stage2b-delta-third.json", JSON.stringify(out, null, 2));
