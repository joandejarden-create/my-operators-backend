import fs from "fs";

const headers = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "text/html",
};
const url = "https://www.hilton.com/en/locations/curio-collection/";
const res = await fetch(url, { headers, redirect: "follow" });
const html = await res.text();
const m = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
if (!m) throw new Error("no __NEXT_DATA__");
const data = JSON.parse(m[1]);
const pageData = data.props?.pageProps?.pageData;
console.log("pageData type:", typeof pageData, Array.isArray(pageData) ? "array" : "");
console.log("pageData keys:", pageData && typeof pageData === "object" ? Object.keys(pageData).slice(0, 30) : "n/a");

function findHotels(obj, depth = 0, path = "") {
  if (!obj || depth > 8) return [];
  const out = [];
  if (Array.isArray(obj) && obj.length > 3 && obj[0]?.name && (obj[0]?.ctyhocn || obj[0]?.address)) {
    out.push({ path, count: obj.length, sample: obj[0] });
  }
  if (typeof obj === "object") {
    for (const [k, v] of Object.entries(obj)) {
      out.push(...findHotels(v, depth + 1, path ? `${path}.${k}` : k));
    }
  }
  return out;
}

const hits = findHotels(pageData);
console.log("\nHotel-like arrays found:", hits.length);
for (const h of hits.slice(0, 5)) {
  console.log("\nPath:", h.path, "count:", h.count);
  console.log("Sample keys:", Object.keys(h.sample).sort().join(", "));
  console.log("Sample name:", h.sample.name);
  console.log("Sample ctyhocn:", h.sample.ctyhocn);
}

// Find Gran Hotel Costa Rica specifically
function findByName(obj, needle, depth = 0) {
  if (!obj || depth > 12) return null;
  if (typeof obj === "object" && !Array.isArray(obj)) {
    if (String(obj.name || "").includes(needle)) return obj;
    for (const v of Object.values(obj)) {
      const f = findByName(v, needle, depth + 1);
      if (f) return f;
    }
  }
  if (Array.isArray(obj)) {
    for (const v of obj) {
      const f = findByName(v, needle, depth + 1);
      if (f) return f;
    }
  }
  return null;
}

const gran = findByName(pageData, "Gran Hotel Costa Rica");
if (gran) {
  console.log("\n=== Gran Hotel Costa Rica record ===");
  console.log(JSON.stringify(gran, null, 2).slice(0, 4000));
  fs.writeFileSync("reports/_hilton-gran-hotel-costa-rica-sample.json", JSON.stringify(gran, null, 2));
} else {
  console.log("\nGran Hotel Costa Rica not found in pageData tree");
}
