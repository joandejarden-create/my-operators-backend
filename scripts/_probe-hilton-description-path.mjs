import fs from "fs";
import { parseNextDataFromHtml, HILTON_FETCH_HEADERS } from "../lib/hilton-brand-directory-extract.js";

const url = "https://www.hilton.com/en/hotels/sjocuqq-gran-hotel-costa-rica/";
const headers = {
  ...HILTON_FETCH_HEADERS,
  "Accept-Language": "en-US,en;q=0.9",
  Referer: "https://www.hilton.com/en/locations/costa-rica/curio-collection/",
};
const res = await fetch(url, { headers, redirect: "follow" });
const html = await res.text();
console.log("status", res.status, "bytes", html.length);

const data = parseNextDataFromHtml(html);
const pp = data?.props?.pageProps || {};
console.log("pageProps keys:", Object.keys(pp).join(", "));

function findDescription(obj, path = "", depth = 0, hits = []) {
  if (depth > 12 || !obj || typeof obj !== "object") return hits;
  if (Array.isArray(obj)) {
    for (let i = 0; i < Math.min(obj.length, 5); i++) findDescription(obj[i], `${path}[${i}]`, depth + 1, hits);
    return hits;
  }
  for (const [k, v] of Object.entries(obj)) {
    const p = path ? `${path}.${k}` : k;
    if (/description|overview|shortDescription|longDescription|hotelDescription/i.test(k)) {
      if (typeof v === "string" && v.length > 40) hits.push({ path: p, preview: v.slice(0, 200) });
    }
    if (k === "overview" && v && typeof v === "object") {
      for (const [ok, ov] of Object.entries(v)) {
        if (typeof ov === "string" && ov.length > 40) hits.push({ path: `${p}.${ok}`, preview: ov.slice(0, 200) });
      }
    }
    if (depth < 10) findDescription(v, p, depth + 1, hits);
  }
  return hits;
}

const hits = findDescription(pp);
console.log("\nDescription-like fields:");
hits.forEach((h) => console.log(`  ${h.path}\n    ${h.preview}...\n`));

// dehydrated queries
const qs = pp.dehydratedQueryState?.queries || pp.dehydratedState?.queries || [];
console.log("dehydrated queries:", qs.length);
for (const q of qs.slice(0, 20)) {
  const key = q.queryKey?.[0];
  const op = key?.operationName || JSON.stringify(q.queryKey).slice(0, 80);
  const d = q.state?.data;
  if (d && typeof d === "object") {
    const dh = findDescription(d, op, 0, []);
    if (dh.length) {
      console.log("\nQuery", op);
      dh.forEach((h) => console.log(`  ${h.path}: ${h.preview.slice(0, 120)}...`));
    }
  }
}

fs.writeFileSync("reports/_hilton-detail-pageprops-sample.json", JSON.stringify(pp, null, 2).slice(0, 80000));
