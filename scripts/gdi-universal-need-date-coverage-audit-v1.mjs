#!/usr/bin/env node
/**
 * Hotel coverage audit — every selectable GDI hotel.
 * Read-only. No Airtable / paid / publish.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  auditUniversalNeedDateHotelCoverage,
  GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
} from "../lib/group-demand-intelligence/need-date-demand/gdi-universal-need-date-intelligence-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-universal-need-date-intelligence-v1"
);
fs.mkdirSync(OUT, { recursive: true });

const audit = auditUniversalNeedDateHotelCoverage();
fs.writeFileSync(
  path.join(OUT, "hotel-coverage-audit.json"),
  JSON.stringify(audit, null, 2) + "\n"
);

const lines = [];
lines.push(`# GDI Universal Need-Date Intelligence — Hotel Coverage Audit`);
lines.push(``);
lines.push(`Version: \`${GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1}\``);
lines.push(`Generated: ${audit.generatedAt}`);
lines.push(``);
lines.push(`## Counts`);
lines.push(``);
lines.push(`| Status | Count |`);
lines.push(`| --- | ---: |`);
lines.push(`| Total selectable GDI hotels | ${audit.counts.totalHotels} |`);
lines.push(`| HOTEL_CONFIRMED | ${audit.counts.HOTEL_CONFIRMED} |`);
lines.push(`| MARKET_ESTIMATED | ${audit.counts.MARKET_ESTIMATED} |`);
lines.push(`| NOT_ESTABLISHED | ${audit.counts.NOT_ESTABLISHED} |`);
lines.push(``);
lines.push(`## Hotels`);
lines.push(``);
lines.push(`| Hotel | Status | Confirmed periods | Estimated periods | Notes |`);
lines.push(`| --- | --- | ---: | ---: | --- |`);
for (const h of audit.hotels) {
  lines.push(
    `| ${h.hotelName} (\`${h.hotelId}\`) | ${h.status} | ${h.hotelConfirmedPeriods} | ${h.marketEstimatedPeriods} | ${h.estimateUnsupportedReason || "—"} |`
  );
}
lines.push(``);
lines.push(`## Missing / not established`);
lines.push(``);
if (!audit.missingHotels.length) {
  lines.push(`None.`);
} else {
  for (const h of audit.missingHotels) {
    lines.push(
      `- **${h.hotelName}** (\`${h.hotelId}\`): ${h.estimateUnsupportedReason}`
    );
  }
}
lines.push(``);
lines.push(`## Safeguards`);
lines.push(``);
for (const n of audit.contractNotes) lines.push(`- ${n}`);

fs.writeFileSync(path.join(OUT, "00-HOTEL_COVERAGE_AUDIT.md"), lines.join("\n"));
console.log(
  JSON.stringify(
    { ok: true, out: path.join(OUT, "00-HOTEL_COVERAGE_AUDIT.md"), counts: audit.counts },
    null,
    2
  )
);
