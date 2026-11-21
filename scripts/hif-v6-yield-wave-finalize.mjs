#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-yield-wave");
const raw = JSON.parse(fs.readFileSync(path.join(OUT, "YIELD_PROSPECTS.json"), "utf8"));

function bad(name) {
  const n = String(name || "").trim();
  if (n.length < 14) return true;
  if (/\.$/.test(n)) return true; // "Portland. Resort"
  if (/^(same-property|pebblebrook|embassy suites|hilton|marriott|hyatt)\b/i.test(n)) return true;
  if (/\b(same-property|system-wide|company|portfolio)\b/i.test(n)) return true;
  const tokens = n.split(/\s+/);
  if (tokens.length < 3) return true;
  return false;
}

const clean = [];
const quarantined = [];
for (const h of raw.hotels || []) {
  if (bad(h.hotelName)) quarantined.push({ ...h, reason: "NAME_QUALITY" });
  else clean.push(h);
}

const metrics = {
  raw: raw.count,
  clean: clean.length,
  quarantined: quarantined.length,
  brandConversion: clean.filter((c) => c.commercialList === "BRAND_CONVERSION_TARGETS").length,
  management: clean.filter((c) => c.commercialList === "MANAGEMENT_CONTRACT_TARGETS").length,
  byEvent: clean.reduce((a, c) => {
    a[c.eventType] = (a[c.eventType] || 0) + 1;
    return a;
  }, {}),
  researchCostUsd: 0,
  publicationState: "CUSTOMER_BLOCKED",
};

fs.writeFileSync(path.join(OUT, "YIELD_PROSPECTS_CLEAN.json"), JSON.stringify({ count: clean.length, hotels: clean }, null, 2));
fs.writeFileSync(path.join(OUT, "YIELD_PROSPECTS_QUARANTINED.json"), JSON.stringify({ count: quarantined.length, hotels: quarantined }, null, 2));
fs.writeFileSync(path.join(OUT, "YIELD_CLEAN_METRICS.json"), JSON.stringify(metrics, null, 2));
console.log(JSON.stringify(metrics, null, 2));
