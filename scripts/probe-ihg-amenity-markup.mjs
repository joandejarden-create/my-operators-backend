#!/usr/bin/env node
import { readFileSync, writeFileSync } from "node:fs";
import { IHG_FETCH_HEADERS } from "../lib/ihg-brand-directory-extract.js";

const url = "https://www.ihg.com/holidayinn/hotels/us/en/santo-domingo/sdqex/hoteldetail";
const html = await (await fetch(url, { headers: IHG_FETCH_HEADERS })).text();
writeFileSync("reports/ihg-hoteldetail-amenity-sample.html", html);

const idx = html.toLowerCase().indexOf("hotel-amenities");
console.log("hotel-amenities idx", idx);
if (idx >= 0) console.log(html.slice(idx, idx + 2500));

const idx2 = html.indexOf("amenity-title");
console.log("\namenity-title idx", idx2);
if (idx2 >= 0) console.log(html.slice(Math.max(0, idx2 - 200), idx2 + 1500));

// Look for data attributes / JSON with amenities
const dataAmen = [...html.matchAll(/data-amenities?="([^"]*)"/gi)].slice(0, 5);
console.log("data-amenity attrs", dataAmen.map((m) => m[1].slice(0, 100)));

const jsonAmen = [...html.matchAll(/"amenities"\s*:\s*(\[[^\]]{0,500}\])/gi)].slice(0, 3);
console.log("json amenities arrays", jsonAmen.map((m) => m[1].slice(0, 200)));

const facility = [...html.matchAll(/"facilityName"\s*:\s*"([^"]+)"/gi)].map((m) => m[1]);
console.log("facilityName count", facility.length, facility.slice(0, 15));

const service = [...html.matchAll(/"serviceName"\s*:\s*"([^"]+)"/gi)].map((m) => m[1]);
console.log("serviceName", service.slice(0, 10));
