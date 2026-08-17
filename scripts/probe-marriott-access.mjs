#!/usr/bin/env node
const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  "Cache-Control": "no-cache",
  Pragma: "no-cache",
  "Sec-Fetch-Dest": "document",
  "Sec-Fetch-Mode": "navigate",
  "Sec-Fetch-Site": "none",
  "Sec-Fetch-User": "?1",
  "Upgrade-Insecure-Requests": "1",
};

const urls = [
  "https://www.marriott.com/en-us/hotels/midcy-courtyard-merida-downtown/overview/",
  "https://www.marriott.com/mi/query/phoenixShopPropertiesByDestination",
  "https://www.marriott.com/hotel-search/search.mi",
];

for (const url of urls) {
  try {
    const res = await fetch(url, { headers: HEADERS, redirect: "follow" });
    const text = await res.text();
    console.log("\n===", url, "===");
    console.log("status", res.status, "len", text.length);
    console.log("snippet", text.slice(0, 200).replace(/\s+/g, " "));
  } catch (e) {
    console.log(url, "ERR", e.message);
  }
}
