#!/usr/bin/env node
/**
 * Lakeville Inn — Exa Lite assist within cumulative $10 pilot budget.
 * Does not create HPC without brand-directory URL unless Exa surfaces one.
 * CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { createContractTimingV2Budget } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-opportunity-exa-v1");
const ledgerPath = path.join(ROOT, "data/sec-edgar/hif-track-c-wave3b-exa-budget-ledger.json");
const budget = createContractTimingV2Budget({
  ceilingUsd: 10,
  ledgerPath,
  cwd: ROOT,
});
const client = createExaContractTimingClient({
  budget,
  cacheDir: path.join(ROOT, "data/sec-edgar/hif-track-c-wave3b-exa-cache"),
});

const queries = [
  "Lakeville Inn 20851 Keokuk Avenue Minnesota hotel official",
  "Hampton Inn Lakeville Minnesota 20851 Keokuk closed reflag",
  "Lakeville Inn Elevate Hospitality JAMP brand agreement hotel",
];

const results = [];
for (const query of queries) {
  if (budget.remaining() < 0.01) break;
  const search = await client.search({ query, type: "lite", numResults: 5 });
  results.push({
    query,
    ok: search.ok,
    error: search.error || null,
    fromCache: Boolean(search.fromCache),
    costUsd: search.costDollars?.total ?? null,
    urls: (search.results || []).slice(0, 5).map((r) => ({ title: r.title, url: r.url })),
  });
}

const brandHit = results
  .flatMap((r) => r.urls || [])
  .find((u) => /hilton\.com|marriott\.com|ihg\.com|hyatt\.com|choicehotels|wyndham/i.test(u.url || ""));

const report = {
  asOf: new Date().toISOString(),
  hotelName: "Lakeville Inn",
  address: "20851 Keokuk Avenue, Lakeville, MN",
  cumulativeSpendUsdAfter: budget.spentUsd(),
  hardCapUsd: 10,
  remainingUsd: budget.remaining(),
  brandDirectoryUrlFound: brandHit?.url || null,
  createReadyUnlocked: Boolean(brandHit),
  conclusion: brandHit
    ? "Brand-directory URL found — eligible for CREATE_READY under existing gate"
    : "No brand-directory Official URL found. Remains UNRESOLVED_INDEPENDENT_URL_GATE. Free DMO/Chamber evidence sufficient for inventory identity; HPC create awaits founder gate decision for independents.",
  results,
};

fs.writeFileSync(path.join(OUT, "LAKEVILLE_EXA_ASSIST.json"), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify({ done: true, createReadyUnlocked: report.createReadyUnlocked, spend: report.cumulativeSpendUsdAfter, brandHit: report.brandDirectoryUrlFound }, null, 2));
