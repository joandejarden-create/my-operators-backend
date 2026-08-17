#!/usr/bin/env node
import fs from "node:fs";

const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
  Accept: "text/html,*/*",
};

function collapse(urls) {
  const m = new Map();
  for (const raw of urls) {
    let u = String(raw).replace(/\\/g, "");
    if (!u.startsWith("http")) u = `https://${u}`;
    const bare = u.split("?")[0];
    const base = bare.replace(/:(Feature-Hor|Wide-Hor|Square|Classic-Hor|Pano-Hor).*$/i, "");
    if (!m.has(base)) m.set(base, `${base}:Wide-Hor?wid=1600&fit=constrain`);
  }
  return [...m.values()];
}

function extract(text) {
  const s = String(text || "");
  const scene7 = [
    ...s.matchAll(/cache\.marriott\.com\/is\/image\/marriotts7prod\/[A-Za-z0-9_.:\-?=&,]+/g),
  ].map((m) => `https://${m[0].replace(/\\/g, "")}`);
  const dam = [
    ...s.matchAll(/cache\.marriott\.com\/content\/dam\/[^\"'\\s>]+/g),
  ].map((m) => `https://${m[0].replace(/\\/g, "").split("?")[0]}`);
  const wp = [
    ...s.matchAll(/https?:\/\/[a-z0-9.-]*marriott\.com\/wp-content\/uploads\/[^\"'\\s>]+/gi),
  ].map((m) => m[0].split("?")[0]);
  const resource = [
    ...s.matchAll(/https?:\/\/hotel-development\.marriott\.com\/resourcefiles\/[^\"'\\s>]+/gi),
  ].map((m) => m[0].split("?")[0]);
  return {
    scene7: collapse(scene7),
    dam: [...new Set(dam)],
    wp: [...new Set(wp)],
    resource: [...new Set(resource)],
  };
}

const pages = [
  ["fairfield", "https://fairfield.marriott.com/"],
  ["fp", "https://four-points.marriott.com/"],
  ["fp-mi", "https://www.marriott.com/brands/four-points-by-sheraton.mi"],
  ["delta", "https://delta-hotels.marriott.com/"],
  ["delta-mi", "https://www.marriott.com/brands/delta-hotels.mi"],
  ["fi-dev", "https://hotel-development.marriott.com/brands/fairfield"],
  ["fp-dev", "https://hotel-development.marriott.com/brands/four-points"],
  ["dl-dev", "https://hotel-development.marriott.com/brands/delta-hotels"],
  ["fi-dev2", "https://hotel-development.marriott.com/brands/fairfield-inn-and-suites"],
];

const out = {};
for (const [label, url] of pages) {
  try {
    const r = await fetch(url, { headers: HEADERS, redirect: "follow" });
    const t = await r.text();
    const ex = extract(t);
    console.log(
      label,
      r.status,
      t.length,
      "s7",
      ex.scene7.length,
      "dam",
      ex.dam.length,
      "wp",
      ex.wp.length,
      "res",
      ex.resource.length
    );
    for (const u of [...ex.scene7, ...ex.dam, ...ex.wp, ...ex.resource].slice(0, 12)) {
      console.log(" ", u.slice(0, 160));
    }
    out[label] = { url, status: r.status, ...ex };
  } catch (e) {
    console.log(label, "ERR", e.message);
    out[label] = { url, error: e.message };
  }
}

fs.writeFileSync("reports/_tmp-wave16a-stage2b-brand-site-harvest.json", JSON.stringify(out, null, 2));
