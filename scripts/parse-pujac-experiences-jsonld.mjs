#!/usr/bin/env node
import { readFileSync, writeFileSync } from "node:fs";
import { load } from "cheerio";

const html = readFileSync("reports/pujac-experiences-full.html", "utf8");
const $ = load(html);
const data = JSON.parse($("#experiences-unap-schema-json").html() || "{}");
writeFileSync("reports/pujac-experiences-hotel-jsonld.json", JSON.stringify(data, null, 2));
console.log("containsPlace", data.containsPlace?.map((p) => `${p["@type"]}:${p.name}`));
for (const p of data.containsPlace || []) {
  if (p.amenityFeature?.length) {
    console.log(
      p.name,
      p.amenityFeature
        .filter((a) => a.value === true || a.value === "true")
        .map((a) => a.name)
        .slice(0, 12)
        .join(", ")
    );
  }
}
