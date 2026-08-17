#!/usr/bin/env node
async function cdx(url, matchType = "prefix") {
  const api = `https://web.archive.org/cdx/search/cdx?url=${encodeURIComponent(
    url
  )}&matchType=${matchType}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=20`;
  const r = await fetch(api, {
    headers: { "User-Agent": "Mozilla/5.0", Accept: "application/json" },
  });
  const t = await r.text();
  if (!t.startsWith("[")) {
    console.log("non-json", r.status, url, t.slice(0, 120));
    return [];
  }
  return JSON.parse(t).slice(1);
}

function hiltonIm(html) {
  return [
    ...new Set(
      [
        ...html.matchAll(
          /https:\/\/www\.hilton\.com\/im\/en\/[A-Z0-9]+\/\d+\/[^"'\\\s<>?]+\.(?:jpg|jpeg|png|webp)/gi
        ),
      ].map((m) => m[0])
    ),
  ];
}

function stories(html) {
  return [
    ...new Set(
      [
        ...html.matchAll(
          /https:\/\/stories-editor\.hilton\.com\/wp-content\/uploads\/[^"'\\\s<>]+\.(?:jpg|jpeg|png|webp)/gi
        ),
      ]
        .map((m) => m[0].replace(/\?.*$/, ""))
        .filter((u) => /canopy/i.test(u) && !/logo|hilton_black/i.test(u))
    ),
  ];
}

const targets = [
  "www.hilton.com/en/hotels/waswhcp",
  "www.hilton.com/en/hotels/rekcpcp",
  "www.hilton.com/en/hotels/pdxpdcp",
  "www.hilton.com/en/hotels/cnypycc",
  "stories.hilton.com/releases/canopy",
  "stories-editor.hilton.com/wp-content/uploads/*Canopy*",
];

for (const t of targets) {
  const rows = await cdx(t);
  console.log("\n", t, "rows", rows.length);
  for (const row of rows.slice(0, 3)) console.log(" ", row[0], row[1].slice(0, 100));
  if (!rows.length) continue;
  const mid = rows[Math.floor(rows.length / 2)];
  const wa = `https://web.archive.org/web/${mid[0]}id_/${mid[1]}`;
  try {
    const r = await fetch(wa, {
      headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" },
      redirect: "follow",
    });
    const html = await r.text();
    const im = hiltonIm(html);
    const st = stories(html);
    console.log("  WA", mid[0], r.status, "hiltonIm", im.length, "stories", st.length);
    console.log(...im.slice(0, 3).map((u) => "   " + u));
    console.log(...st.slice(0, 3).map((u) => "   " + u));
  } catch (e) {
    console.log("  WA err", e.message);
  }
}

// Direct known canopy property image probes via stories search pages
const more = [
  "https://stories.hilton.com/releases/hilton-opens-new-canopy-by-hilton-in-nashville",
  "https://stories.hilton.com/?s=%22Canopy+by+Hilton%22+opens",
  "https://www.hilton.com/en/brands/canopy-by-hilton/",
];
for (const u of more) {
  try {
    const r = await fetch(u, { headers: { "User-Agent": "Mozilla/5.0" }, redirect: "follow" });
    const html = await r.text();
    console.log("\nlive", r.status, u.slice(-50), "stories", stories(html).length, "im", hiltonIm(html).length);
    stories(html).slice(0, 8).forEach((x) => console.log(" ", x));
  } catch (e) {
    console.log("live err", e.message);
  }
}
