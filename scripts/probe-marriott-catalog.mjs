#!/usr/bin/env node
const MARSHA = "POPLC";
const urls = [
  `https://catalog.marriott.com/v1/property/${MARSHA}`,
  `https://catalog.marriott.com/properties/${MARSHA}`,
  `https://catalog.marriott.com/api/v1/property/${MARSHA}`,
  `https://args.marriott.com/property/${MARSHA}`,
  `https://www.marriott.com/content/dam/marriott-digital/mi/global/en_us/hotels/${MARSHA.toLowerCase()}.json`,
];

const H = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  Accept: "application/json, text/html",
};

for (const url of urls) {
  try {
    const r = await fetch(url, { headers: H });
    const t = await r.text();
    console.log(r.status, url);
    console.log(" ", t.slice(0, 250).replace(/\s+/g, " "));
  } catch (e) {
    console.log("ERR", url, e.message);
  }
}
