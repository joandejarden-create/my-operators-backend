#!/usr/bin/env node
import { writeFileSync } from "node:fs";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const r = await fetch("https://www.ihg.com/destinations/us/en/dominican-republic-hotels", {
  headers: { "User-Agent": UA },
});
const html = await r.text();
writeFileSync("reports/ihg-dr-destination.html", html);
console.log("status", r.status, "len", html.length);

const markers = ["SDQEX", "SDQHI", "SDQIC", "BAVSB"];
for (const code of markers) {
  const needle = `data-hotel-mnemonic="${code}"`;
  const idx = html.indexOf(needle);
  console.log("\n===", code, "idx", idx, "===");
  if (idx < 0) continue;
  console.log(html.slice(Math.max(0, idx - 100), idx + 1800));
}
