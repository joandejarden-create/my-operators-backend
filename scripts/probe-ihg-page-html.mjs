#!/usr/bin/env node
import { writeFileSync } from "fs";
const url = "https://www.ihg.com/holidayinn/hotels/us/en/santo-domingo/sdqhi/hoteldetail";
const res = await fetch(url, {
  headers: {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36",
    Accept: "text/html",
  },
});
const html = await res.text();
writeFileSync("reports/ihg-page-probe.html", html);
console.log("status", res.status, "len", html.length);
const ld = [...html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi)];
console.log("ld count", ld.length);
for (const m of ld.slice(0, 3)) {
  try {
    const j = JSON.parse(m[1]);
    const t = j["@type"];
    console.log("type", t, "amenityFeature", (j.amenityFeature || []).length);
    if (j.amenityFeature?.length) console.log(JSON.stringify(j.amenityFeature.slice(0, 5)));
  } catch {}
}
const amenSection = html.match(/amenit[^<]{0,200}/gi);
console.log("samples", amenSection?.slice(0, 5));
