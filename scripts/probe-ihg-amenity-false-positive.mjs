#!/usr/bin/env node
import { writeFileSync } from "node:fs";
import { IHG_FETCH_HEADERS } from "../lib/ihg-brand-directory-extract.js";
import { extractIhgAmenitiesFromHtml } from "./probe-ihg-amenities-fetch.mjs";

const url =
  process.argv[2] ||
  "https://www.ihg.com/holidayinn/hotels/us/en/santo-domingo/sdqex/hoteldetail";
const res = await fetch(url, { headers: IHG_FETCH_HEADERS, redirect: "follow" });
const html = await res.text();
writeFileSync("reports/ihg-hoteldetail-amenity-sample.html", html);

const checks = {
  accessDenied: /access denied/i.test(html),
  captcha: /captcha/i.test(html),
  robot: /robot/i.test(html),
  akamai: /akamai/i.test(html),
  pleaseEnable: /please enable javascript/i.test(html),
  attention: /attention required/i.test(html),
  amenityList: (html.match(/amenity-list/gi) || []).length,
  amenityWord: (html.match(/amenit/gi) || []).length,
  ldJson: (html.match(/application\/ld\+json/gi) || []).length,
  hotelName: (html.match(/<title>([^<]+)/i) || [])[1],
  extracted: extractIhgAmenitiesFromHtml(html).slice(0, 30),
};
console.log(JSON.stringify(checks, null, 2));

// Find amenity-related class names
const classes = [...html.matchAll(/class="([^"]*amenit[^"]*)"/gi)].map((m) => m[1]);
console.log("amenity classes", [...new Set(classes)].slice(0, 20));
