#!/usr/bin/env node
import { load } from "cheerio";
import { writeFileSync } from "node:fs";

const url = process.argv[2] || "https://www.wyndhamhotels.com/wyndham-alltra/playa-del-carmen-mexico/wyndham-alltra-playa-del-carmen-adults-only-all-inclusive/overview";
const html = await (await fetch(url, { headers: { "User-Agent": "Mozilla/5.0 Chrome/131" } })).text();
const $ = load(html);

console.log("ld+json hotels", $("script[type='application/ld+json']").length);
$("script[type='application/ld+json']").each((i, el) => {
  try {
    const j = JSON.parse($(el).html() || "");
    const types = Array.isArray(j["@type"]) ? j["@type"] : [j["@type"]];
    if (types.some((t) => /Hotel/i.test(String(t)))) {
      console.log("hotel json", j.name, "amenityFeature", j.amenityFeature?.length);
    }
  } catch {}
});

$("[class*='amenity' i]").each((i, el) => {
  const t = $(el).text().replace(/\s+/g, " ").trim();
  if (t && t.length > 2 && t.length < 80 && !/featured amenities/i.test(t)) console.log("amenity el", i, t);
});

const faq = html.match(/fitness|pool|wifi|parking|restaurant/gi) || [];
console.log("keyword hits", faq.length);

const m = html.match(/__NEXT_DATA__[^>]*>([\s\S]*?)<\/script/i);
if (m) {
  const j = JSON.parse(m[1]);
  const s = JSON.stringify(j);
  const idx = s.toLowerCase().indexOf("amenit");
  console.log("next amenit idx", idx, s.slice(idx, idx + 600));
}
const m2 = html.match(/"amenities"\s*:\s*\[[^\]]{0,800}\]/i);
console.log("amenities array", m2?.[0]?.slice(0, 400));
