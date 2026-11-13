#!/usr/bin/env node
/**
 * Audit GDI filesystem opportunity titles against the permanent
 * customer-facing title contract. Read-only — never writes opportunities.json.
 *
 * Usage:
 *   node scripts/audit-gdi-customer-facing-opportunity-titles-v1.mjs
 *   node scripts/audit-gdi-customer-facing-opportunity-titles-v1.mjs --hotel recLuxvwwxID7U2B8
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_V1,
  normalizeCustomerFacingOpportunityTitle,
} from "../lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTELS_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels"
);
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-customer-facing-opportunity-title-contract-v1"
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadOpps(file) {
  try {
    const raw = JSON.parse(fs.readFileSync(file, "utf8"));
    if (Array.isArray(raw)) return raw;
    if (Array.isArray(raw?.opportunities)) return raw.opportunities;
    return [];
  } catch {
    return [];
  }
}

const hotelFilter = (() => {
  const i = process.argv.indexOf("--hotel");
  return i >= 0 ? String(process.argv[i + 1] || "").trim() : "";
})();

const hotelDirs = fs
  .readdirSync(HOTELS_DIR, { withFileTypes: true })
  .filter((d) => d.isDirectory())
  .map((d) => d.name)
  .filter((id) => !hotelFilter || id === hotelFilter);

const rows = [];
for (const hotelId of hotelDirs) {
  const file = path.join(HOTELS_DIR, hotelId, "opportunities.json");
  if (!fs.existsSync(file)) continue;
  for (const opp of loadOpps(file)) {
    const before =
      text(opp.displayTitle) ||
      text(opp.drawerHeaderTitle) ||
      text(opp.title) ||
      "";
    if (!before) continue;
    const {
      displayTitle: after,
      structurallyChanged,
      strippedSuffixes,
    } = normalizeCustomerFacingOpportunityTitle(before, {
      originalSourceTitle:
        text(opp.originalSourceTitle) ||
        text(opp.researchTitlePreserved) ||
        text(opp.title) ||
        before,
    });
    const needsCorrection =
      Boolean(structurallyChanged) || (strippedSuffixes || []).length > 0;
    rows.push({
      hotelId,
      opportunityId: text(opp.id || opp.opportunityId) || null,
      beforeTitle: before,
      afterTitle: after,
      changed: needsCorrection,
      structurallyChanged: Boolean(structurallyChanged),
      strippedSuffixes,
      segment: text(opp.researchSegment || opp.segment) || null,
    });
  }
}

const changed = rows.filter((r) => r.changed);
fs.mkdirSync(OUT_DIR, { recursive: true });
const report = {
  version: GDI_CUSTOMER_FACING_OPPORTUNITY_TITLE_CONTRACT_V1,
  generatedAt: new Date().toISOString(),
  hotelsAudited: hotelDirs.length,
  opportunitiesAudited: rows.length,
  titlesRequiringCorrection: changed.length,
  sampleBeforeAfter: changed.slice(0, 40).map((r) => ({
    opportunityId: r.opportunityId,
    hotelId: r.hotelId,
    beforeTitle: r.beforeTitle,
    afterTitle: r.afterTitle,
  })),
  founderExamples: [
    "AMWA 2027 Interim Meeting — Sep 11–12",
    "PDA TRI Bethesda Training — December 2026 Lodging",
    "ASAE Annual Meeting & Exposition 2029 — Washington, DC (Early Watch)",
  ].map((before) => ({
    beforeTitle: before,
    afterTitle: normalizeCustomerFacingOpportunityTitle(before).displayTitle,
  })),
};
fs.writeFileSync(
  path.join(OUT_DIR, "00-title-audit-summary.json"),
  JSON.stringify(report, null, 2)
);
fs.writeFileSync(
  path.join(OUT_DIR, "01-title-audit-rows.json"),
  JSON.stringify({ rows: changed }, null, 2)
);

console.log(
  JSON.stringify(
    {
      ok: true,
      version: report.version,
      hotelsAudited: report.hotelsAudited,
      opportunitiesAudited: report.opportunitiesAudited,
      titlesRequiringCorrection: report.titlesRequiringCorrection,
      reportDir: OUT_DIR,
    },
    null,
    2
  )
);
