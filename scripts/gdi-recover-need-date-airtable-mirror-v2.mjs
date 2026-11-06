#!/usr/bin/env node
/**
 * Read-only recovery: Airtable GDI Hotel Need Periods → local mirror JSON.
 * Never writes to Airtable. Never publishes. Never mutates share tokens.
 *
 * Usage:
 *   node scripts/gdi-recover-need-date-airtable-mirror-v2.mjs
 *   node scripts/gdi-recover-need-date-airtable-mirror-v2.mjs --hotel recLuxvwwxID7U2B8
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listNeedPeriodsFromAirtable,
  resolveNeedDateAirtableBaseId,
} from "../lib/group-demand-intelligence/need-date-demand/need-date-airtable-store-v3.js";
import { listCanonicalGdiEligibleHotels } from "../lib/group-demand-intelligence/need-date-demand/gdi-hotel-layer-model-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-universal-need-dates-and-full-e2e-v2");
fs.mkdirSync(OUT, { recursive: true });

process.env.GDI_NEED_DATES_AIRTABLE_LIVE =
  process.env.GDI_NEED_DATES_AIRTABLE_LIVE || "1";

const args = process.argv.slice(2);
const hotelArgIdx = args.indexOf("--hotel");
const onlyHotel =
  hotelArgIdx >= 0 ? String(args[hotelArgIdx + 1] || "").trim() : "";

const defaultHotels = [
  "recLuxvwwxID7U2B8", // Bethesda — known HOTEL_REPORTED
  "recgMYovrrZDJMqzX", // Waterstone — test scenarios only
  "recKRJjcPnb4tVDDS", // Spice — expect empty
];

const hotels = onlyHotel
  ? [onlyHotel]
  : [...new Set([...defaultHotels, ...listCanonicalGdiEligibleHotels().map((h) => h.hotelId)])];

function isAuthoritativeHotelConfirmed(p = {}) {
  const status = String(p.status || "");
  const source = String(p.source || "");
  if (p.isTestScenario === true) return false;
  if (/TEST_SCENARIO|Cancelled|Superseded|Filled/i.test(status)) return false;
  if (/founder_test|lifecycle_isolate|fixture|synthetic/i.test(source)) return false;
  return /HOTEL_REPORTED|HOTEL_SALES|HOTEL_PORTAL|HOTEL_CONFIRMED/i.test(source);
}

const recovery = {
  version: "GDI_NEED_DATE_AIRTABLE_RECOVERY_V2",
  recoveredAt: new Date().toISOString(),
  baseId: resolveNeedDateAirtableBaseId(),
  table: "GDI Hotel Need Periods",
  hotels: {},
  mirrorsWritten: [],
  blockers: [],
};

for (const hotelId of hotels) {
  try {
    const listed = await listNeedPeriodsFromAirtable(hotelId, {
      env: process.env,
    });
    const periods = Array.isArray(listed?.periods)
      ? listed.periods
      : Array.isArray(listed)
        ? listed
        : [];
    const authoritative = periods.filter(isAuthoritativeHotelConfirmed);
    const excluded = periods.filter((p) => !isAuthoritativeHotelConfirmed(p));

    recovery.hotels[hotelId] = {
      totalAirtableRows: periods.length,
      authoritativeHotelConfirmed: authoritative.length,
      excludedNonAuthoritative: excluded.length,
      authoritativeIds: authoritative.map((p) => p.needPeriodId),
      excluded: excluded.map((p) => ({
        needPeriodId: p.needPeriodId,
        status: p.status,
        source: p.source,
        reason: p.isTestScenario
          ? "TEST_SCENARIO"
          : /Cancelled/i.test(String(p.status))
            ? "CANCELLED"
            : "NON_HOTEL_REPORTED_SOURCE",
      })),
      periods: authoritative.map((p) => ({
        needPeriodId: p.needPeriodId,
        periodLabel: p.periodLabel,
        startDate: p.startDate,
        endDate: p.endDate,
        priority: p.priority,
        status: p.status,
        revenueNeed: p.revenueNeed,
        source: p.source,
        lastConfirmedByHotelAt: p.lastConfirmedByHotelAt,
        airtableRecordId: p.airtableRecordId,
        currentVersion: p.currentVersion,
        provenance: "HOTEL_CONFIRMED",
        hotelConfirmed: true,
      })),
    };

    const hotelDir = path.join(
      ROOT,
      "data/group-demand-intelligence/hotels",
      hotelId
    );
    fs.mkdirSync(hotelDir, { recursive: true });
    const mirrorPath = path.join(hotelDir, "need-periods.airtable-mirror.json");
    const mirrorDoc = {
      version: "GDI_NEED_DATE_AIRTABLE_MIRROR_V2",
      hotelId,
      authority: "AIRTABLE_GDI_HOTEL_NEED_PERIODS",
      baseId: recovery.baseId,
      table: recovery.table,
      recoveredAt: recovery.recoveredAt,
      note: "Read-only mirror of hotel-confirmed Airtable periods. Not a publish. Not a write-back.",
      periods: recovery.hotels[hotelId].periods,
      excludedNonAuthoritative: recovery.hotels[hotelId].excluded,
    };
    fs.writeFileSync(mirrorPath, JSON.stringify(mirrorDoc, null, 2) + "\n");
    recovery.mirrorsWritten.push({
      hotelId,
      path: mirrorPath,
      authoritativeCount: authoritative.length,
    });

    // Quarantine local demo stub if Airtable authority exists for Bethesda.
    const localPath = path.join(hotelDir, "need-periods.json");
    if (fs.existsSync(localPath) && authoritative.length > 0) {
      try {
        const local = JSON.parse(fs.readFileSync(localPath, "utf8"));
        if (
          /demo periods for universal|UI verification/i.test(String(local.note || ""))
        ) {
          const quarantine = path.join(
            hotelDir,
            "need-periods.local-demo-quarantined.json"
          );
          fs.renameSync(localPath, quarantine);
          recovery.hotels[hotelId].quarantinedLocalDemo = quarantine;
        }
      } catch {
        /* ignore */
      }
    }
  } catch (err) {
    recovery.blockers.push({
      hotelId,
      code: err.code || "AIRTABLE_READ_FAILED",
      message: err.message || String(err),
    });
  }
}

fs.writeFileSync(
  path.join(OUT, "airtable-need-periods-recovery.json"),
  JSON.stringify(recovery, null, 2) + "\n"
);

console.log(
  JSON.stringify(
    {
      ok: recovery.blockers.length === 0,
      baseId: recovery.baseId,
      mirrorsWritten: recovery.mirrorsWritten.length,
      blockers: recovery.blockers,
      summary: Object.fromEntries(
        Object.entries(recovery.hotels).map(([id, h]) => [
          id,
          {
            authoritative: h.authoritativeHotelConfirmed,
            excluded: h.excludedNonAuthoritative,
          },
        ])
      ),
      out: path.join(OUT, "airtable-need-periods-recovery.json"),
    },
    null,
    2
  )
);

if (recovery.blockers.length) process.exitCode = 2;
