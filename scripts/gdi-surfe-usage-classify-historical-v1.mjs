#!/usr/bin/env node
/**
 * Classify existing Surfe usage ledger rows (V1.2).
 * Default dry-run. --apply adds usageClass / dataEnvironment / UNKNOWN names
 * without changing accounting totals or deleting rows.
 *
 *   node scripts/gdi-surfe-usage-classify-historical-v1.mjs
 *   node scripts/gdi-surfe-usage-classify-historical-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyUsageClass,
  resolveDataEnvironment,
  enrichSurfeUsageContext,
  buildHotelContextMap,
} from "../lib/group-demand-intelligence/gdi-surfe-usage-context-v1.js";
import {
  acquireSurfeAccountingLock,
  accountingPath,
} from "../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const apply = process.argv.includes("--apply");

const file = accountingPath(process.env);
const raw = fs.readFileSync(file, "utf8");
const doc = JSON.parse(raw);
const hotelMap = buildHotelContextMap();
const envDefault = resolveDataEnvironment(process.env);

const byClass = {};
const rows = [];

for (const e of doc.entries || []) {
  const usageClass = classifyUsageClass(
    { ...e, dataEnvironment: e.dataEnvironment || envDefault },
    process.env,
  );
  byClass[usageClass] = (byClass[usageClass] || 0) + 1;
  const enriched = enrichSurfeUsageContext({
    hotelId: e.hotelId,
    opportunityId: e.opportunityId,
    hotelMap,
    env: process.env,
  });
  rows.push({
    lookupEventId: e.lookupEventId,
    hotelId: e.hotelId,
    opportunityId: e.opportunityId,
    usageClass,
    dataEnvironment: e.dataEnvironment || envDefault,
    hotelNameBefore: e.hotelName || null,
    hotelNameAfter: e.hotelName || enriched.hotelName,
  });
}

const report = {
  ok: true,
  apply,
  accountingPath: file,
  total: (doc.entries || []).length,
  byClass,
  note:
    "hotel_parallel / hotel_demo / hotel_a / opp_p_* / cr_parallel_* are FIXTURE_TEST from unit tests — not customer adoption.",
  sample: rows.slice(0, 8),
};

const outDir = path.join(ROOT, "reports/gdi-surfe-usage-v1-2");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "historical-classification.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);

if (apply) {
  const lock = acquireSurfeAccountingLock(`${file}.lock`);
  try {
    const live = JSON.parse(fs.readFileSync(file, "utf8"));
    live.entries = (live.entries || []).map((e) => {
      const usageClass = classifyUsageClass(
        { ...e, dataEnvironment: e.dataEnvironment || envDefault },
        process.env,
      );
      const enriched = enrichSurfeUsageContext({
        hotelId: e.hotelId,
        opportunityId: e.opportunityId,
        hotelMap,
        env: process.env,
      });
      return {
        ...e,
        usageClass,
        dataEnvironment: e.dataEnvironment || envDefault,
        hotelName: e.hotelName || enriched.hotelName,
        hotelMarket: e.hotelMarket || enriched.hotelMarket,
        hotelCountry: e.hotelCountry || enriched.hotelCountry,
        hotelResolution: e.hotelResolution || enriched.hotelResolution,
        opportunityTitle: e.opportunityTitle || enriched.opportunityTitle,
        opportunityOrganizationName:
          e.opportunityOrganizationName || enriched.opportunityOrganizationName,
        opportunityDrawerHref:
          e.opportunityDrawerHref || enriched.opportunityDrawerHref,
        requestingPortfolioId:
          e.requestingPortfolioId || enriched.requestingPortfolioId,
        requestingHotelOrOfficeId:
          e.requestingHotelOrOfficeId || enriched.requestingHotelOrOfficeId,
        requestingOrganizationName:
          e.requestingOrganizationName || enriched.requestingOrganizationName,
        requestingOfficeMarket:
          e.requestingOfficeMarket || enriched.requestingOfficeMarket,
        historicalClassifiedAt: new Date().toISOString(),
        schemaVersion: "gdi-surfe-usage-tracking-v1.2",
      };
    });
    live.trackingVersion = "gdi-surfe-usage-tracking-v1.2";
    live.historicalClassificationNote =
      "V1.2 classification applied; original accounting fields preserved; FIXTURE_TEST rows retained for audit.";
    live.updatedAt = new Date().toISOString();
    const tmp = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, `${JSON.stringify(live, null, 2)}\n`);
    fs.renameSync(tmp, file);
    report.applied = true;
  } finally {
    lock.release();
  }
}

console.log(JSON.stringify(report, null, 2));
