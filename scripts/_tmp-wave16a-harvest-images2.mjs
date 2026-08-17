import fs from "node:fs";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  Accept: "text/html,application/json,*/*",
};

function collapseScene7(urls) {
  const byBase = new Map();
  for (const raw of urls) {
    let u = String(raw || "").replace(/\\/g, "");
    if (!u.startsWith("http")) u = `https://${u}`;
    const bare = u.split("?")[0];
    const base = bare.replace(/:(Feature-Hor|Wide-Hor|Square|Classic-Hor|Pano-Hor).*$/i, "");
    if (!byBase.has(base)) byBase.set(base, `${base}:Wide-Hor?wid=1600&fit=constrain`);
  }
  return [...byBase.values()];
}

function extractAll(text) {
  const s = String(text || "");
  const scene7 = [
    ...s.matchAll(/https?:\\?\/\\?\/cache\.marriott\.com\/is\/image\/marriotts7prod\/[A-Za-z0-9_.:\-?=&,]+/g),
  ].map((m) => m[0]);
  const dam = [
    ...s.matchAll(/https?:\\?\/\\?\/cache\.marriott\.com\/content\/dam\/[^\"'\\s>]+/g),
  ].map((m) => m[0].replace(/\\/g, ""));
  const wp = [
    ...s.matchAll(/https?:\/\/[a-z0-9.-]*marriott\.com\/wp-content\/uploads\/[^\"'\\s>]+/gi),
  ].map((m) => m[0]);
  return {
    scene7: collapseScene7(scene7),
    dam: [...new Set(dam.map((u) => u.split("?")[0]))],
    wp: [...new Set(wp.map((u) => u.split("?")[0]))],
  };
}

async function fetchText(url) {
  const r = await fetch(url, { headers: HEADERS, redirect: "follow" });
  const text = await r.text();
  return { status: r.status, text, ct: r.headers.get("content-type") || "" };
}

const pages = [
  ["fairfield-brand", "https://fairfield.marriott.com/"],
  ["fp-brand", "https://four-points.marriott.com/"],
  ["delta-brand", "https://delta-hotels.marriott.com/"],
  ["delta-mi", "https://www.marriott.com/brands/hotels/delta-hotels"],
];

const out = {};
for (const [label, url] of pages) {
  const { status, text } = await fetchText(url);
  const ex = extractAll(text);
  console.log(label, status, "scene7", ex.scene7.length, "dam", ex.dam.length, "wp", ex.wp.length);
  out[label] = { url, status, ...ex };
  for (const u of [...ex.scene7, ...ex.dam, ...ex.wp].slice(0, 20)) {
    console.log(" ", u.slice(0, 150));
  }
}

// Probe known Scene7 naming patterns for property MARSHA codes
const probes = [
  // Fairfield
  "fi-cunfo",
  "fi-sdqfo",
  "fi-nycts",
  "fi-miafo",
  "fi-bogfo",
  // Four Points (NOT fpx/xf flex)
  "fp-cunfp",
  "fp-bogfp",
  "fp-miafp",
  "fp-nycfm",
  "fp-laxfp",
  // Delta
  "dl-cundl",
  "dl-mexdl",
  "dl-yyzdl",
  "dl-yvrdl",
  "dl-yycdl",
];

const found = [];
for (const stem of probes) {
  const candidates = [
    `https://cache.marriott.com/is/image/marriotts7prod/${stem}:Wide-Hor?wid=1600&fit=constrain`,
    `https://cache.marriott.com/is/image/marriotts7prod/${stem}-exterior:Wide-Hor?wid=1600&fit=constrain`,
    `https://cache.marriott.com/is/image/marriotts7prod/${stem}-lobby:Wide-Hor?wid=1600&fit=constrain`,
    `https://cache.marriott.com/is/image/marriotts7prod/${stem}-guest-room:Wide-Hor?wid=1600&fit=constrain`,
  ];
  for (const u of candidates) {
    try {
      const r = await fetch(u, { method: "HEAD", headers: HEADERS, redirect: "follow" });
      const ct = r.headers.get("content-type") || "";
      if (r.ok && /image/i.test(ct)) {
        console.log("HIT", r.status, ct, u.slice(0, 120));
        found.push(u);
      }
    } catch {
      // ignore
    }
  }
}

fs.writeFileSync(
  "reports/_tmp-wave16a-stage2b-harvest.json",
  JSON.stringify({ pages: out, probeHits: found }, null, 2)
);
console.log("probeHits", found.length);
