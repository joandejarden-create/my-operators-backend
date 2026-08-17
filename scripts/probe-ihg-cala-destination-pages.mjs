#!/usr/bin/env node
/**
 * Probe which CALA country destination pages exist on ihg.com.
 */
import { writeFileSync } from "node:fs";
import { COUNTRY_CONFIG_LIST } from "../lib/radar-buildout/country-configs.js";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

function slugifyCountry(name) {
  return String(name || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

const ALIASES = {
  "turks-caicos": ["turks-and-caicos", "turks-caicos-islands"],
  "turks-and-caicos": ["turks-and-caicos", "turks-caicos"],
  "u-s-virgin-islands": ["us-virgin-islands", "united-states-virgin-islands", "virgin-islands"],
  "british-virgin-islands": ["british-virgin-islands", "virgin-islands-british"],
  curacao: ["curacao", "curaçao"],
  "saint-vincent-and-the-grenadines": ["st-vincent", "saint-vincent"],
  "saint-kitts-and-nevis": ["st-kitts", "saint-kitts-and-nevis"],
  "saint-lucia": ["st-lucia", "saint-lucia"],
};

async function probe(slug) {
  const url = `https://www.ihg.com/destinations/us/en/${slug}-hotels`;
  const r = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "text/html" },
    redirect: "follow",
  });
  const html = await r.text();
  const cards = [...html.matchAll(/data-hotel-mnemonic="([A-Z0-9]+)"/gi)].length;
  return {
    slug,
    url,
    status: r.status,
    finalUrl: r.url,
    len: html.length,
    cards,
    ok: r.status === 200 && cards > 0,
  };
}

async function main() {
  const results = [];
  for (const country of COUNTRY_CONFIG_LIST) {
    const base = slugifyCountry(country);
    const candidates = [base, ...(ALIASES[base] || [])];
    let best = null;
    for (const slug of [...new Set(candidates)]) {
      const hit = await probe(slug);
      console.log(
        country.padEnd(36),
        slug.padEnd(28),
        hit.status,
        "cards",
        hit.cards,
        "->",
        hit.finalUrl.slice(0, 60)
      );
      if (hit.ok) {
        best = { country, ...hit };
        break;
      }
      if (!best || hit.cards > (best.cards || 0)) best = { country, ...hit };
    }
    results.push(best);
  }

  const ok = results.filter((r) => r.ok);
  console.log("\nOK countries", ok.length, "/", results.length);
  writeFileSync(
    "reports/ihg-cala-destination-page-probe.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2)
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
