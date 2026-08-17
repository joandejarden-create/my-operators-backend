#!/usr/bin/env node
import { load } from "cheerio";

const overview =
  "https://www.wyndhamhotels.com/wyndham-alltra/playa-del-carmen-mexico/wyndham-alltra-playa-del-carmen-adults-only-all-inclusive/overview";
const services = overview.replace(/\/overview\/?$/, "/services-amenities");
const h = await (await fetch(services, { headers: { "User-Agent": "Mozilla/5.0 Chrome/131" } })).text();
console.log("services url", services, "len", h.length);
const $ = load(h);
/** @type {string[]} */
const labels = [];
$("[itemprop='name']").each((_, el) => {
  const t = $(el).text().replace(/\s+/g, " ").trim();
  if (t && t.length < 80) labels.push(t);
});
$(".hotel-policies-amenities__list li, .amenities-list li, [class*='amenity'] h4, [class*='amenity'] span").each(
  (_, el) => {
    const t = $(el).text().replace(/\s+/g, " ").trim();
    if (t && t.length > 2 && t.length < 80 && !/amenities/i.test(t)) labels.push(t);
  }
);
console.log("unique labels", [...new Set(labels)].slice(0, 40));
