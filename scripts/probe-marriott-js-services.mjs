#!/usr/bin/env node
/**
 * Discover marriott-hws / content API paths from clientlib bundles.
 */
const bundles = [
  "https://www.marriott.com/etc.clientlibs/mcom-hws/clientlibs/clientlib-sitev2.min.148fb7323216d31ff38185f74455e707.js",
  "https://www.marriott.com/etc.clientlibs/mcom-hws/clientlibs/clientlib-bptv2.min.491a080d5d6b1bebfa5fce5c351651ae.js",
  "https://www.marriott.com/etc.clientlibs/mcom-hws/clientlibs/clientlib-sign-in.min.37b316b2b6090df1898cfe4a6ad869ee.js",
];

const found = new Set();
for (const url of bundles) {
  const js = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } }).then((r) => r.text());
  for (const m of js.matchAll(/services\/marriott-hws\/[a-zA-Z0-9_-]+/g)) found.add(m[0]);
  for (const m of js.matchAll(/content\/marriott-hws[^"'\\]+/g)) found.add(m[0].slice(0, 120));
  for (const m of js.matchAll(/overview[A-Za-z0-9_/-]{0,40}/g)) {
    if (/overview/i.test(m[0]) && m[0].length < 40) found.add(`token:${m[0]}`);
  }
}
console.log([...found].sort().join("\n"));
