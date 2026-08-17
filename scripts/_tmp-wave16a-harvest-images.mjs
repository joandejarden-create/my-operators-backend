import "../load-env.js";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  Accept: "application/json,text/html,*/*",
};

function uniqScene7(text) {
  const urls = [
    ...String(text || "").matchAll(
      /https?:\\?\/\\?\/cache\.marriott\.com\/is\/image\/marriotts7prod\/[A-Za-z0-9_.:\-?=&,]+|cache\.marriott\.com\/is\/image\/marriotts7prod\/[A-Za-z0-9_.:\-?=&,]+/g
    ),
  ].map((m) => m[0].replace(/\\/g, "").replace(/^\/\//, "https://"));
  const cleaned = urls.map((u) => (u.startsWith("http") ? u : `https://${u}`).split("?")[0]);
  // Collapse crop variants of same photograph
  const byBase = new Map();
  for (const u of cleaned) {
    const base = u.replace(/:(Feature-Hor|Wide-Hor|Square|Classic-Hor|Pano-Hor).*$/i, "");
    if (!byBase.has(base)) byBase.set(base, `${base}:Wide-Hor?wid=1600&fit=constrain`);
  }
  return [...byBase.values()];
}

async function tryUrl(label, url) {
  try {
    const r = await fetch(url, { headers: HEADERS, redirect: "follow" });
    const ct = r.headers.get("content-type") || "";
    const body = await r.text();
    const imgs = uniqScene7(body);
    console.log(label, r.status, ct.slice(0, 40), "imgs", imgs.length);
    for (const u of imgs.slice(0, 10)) console.log(" ", u.slice(0, 140));
    return imgs;
  } catch (e) {
    console.log(label, "ERR", e.message);
    return [];
  }
}

const codes = [
  "cunfo",
  "sdqfo",
  "nycts",
  "miafo",
  "bogfo",
  "cunfp",
  "bogfp",
  "miafp",
  "nycfm",
  "laxfp",
  "cundl",
  "mexdl",
  "yyzdl",
  "yvrdl",
  "yycdl",
];

for (const c of codes) {
  await tryUrl(
    `${c}-json`,
    `https://cache.marriott.com/content/marriottdata/en-us/hotels/${c}.json`
  );
}

const sites = [
  ["delta-mi", "https://www.marriott.com/brands/delta-hotels.mi"],
  ["delta-dev", "https://hotel-development.marriott.com/brands/delta"],
  ["fairfield-dev", "https://hotel-development.marriott.com/brands/fairfield"],
  ["fp-dev", "https://hotel-development.marriott.com/brands/four-points"],
  ["delta-brand2", "https://delta-hotels.marriott.com/"],
  ["fairfield-brand", "https://fairfield.marriott.com/"],
  ["fp-brand", "https://four-points.marriott.com/"],
];

for (const [l, u] of sites) await tryUrl(l, u);
