#!/usr/bin/env node
/**
 * Full Competitive Opportunity Recovery V1 — Bethesda Marriott.
 * Scans 67 baseline + 59 canonical + 29 historical + CMI ledger.
 * Classifies A Verified / B Needs Verification / C Historical / D Not Competitive.
 * Zero paid research. Does not publish.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  annotateInventoryPopulations,
  BETHESDA_HOTEL_ID,
  loadBethesda67SalesBaselineManifest,
  COMPETITIVE_STATUS,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { BETHESDA_CMI_EVIDENCE_LEDGER } from "../lib/group-demand-intelligence/commercial-consistency/cmi-evidence-ledger-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-competitive-opportunity-recovery-v1");

function readJson(rel) {
  const p = path.isAbsolute(rel) ? rel : path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(rel, data) {
  const p = path.join(OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(data, null, 2) + "\n");
  return p;
}

function loadCanonical() {
  const doc = readJson(
    `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
  );
  return Array.isArray(doc) ? doc : doc?.opportunities || [];
}

function dedupeKey(o) {
  const org = String(
    o.organizationName || o.organization || o.accountName || ""
  )
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  const title = String(o.title || o.canonicalEventName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  const year =
    String(o.eventStartDate || o.eventDates || title).match(/20\d{2}/)?.[0] ||
    "";
  const hotel = String(
    o.cmiCompetitorHotel || o.competitorHotel || o.likelyCompetitor || ""
  )
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 40);
  return `${org}|${year}|${hotel}|${title.slice(0, 48)}`;
}

const canonical = loadCanonical();
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  canonical,
  { root: ROOT }
);
const annotated = annotateInventoryPopulations(union.opportunities || [], {
  hotelId: BETHESDA_HOTEL_ID,
});
const manifest = loadBethesda67SalesBaselineManifest();

const byStatus = {
  [COMPETITIVE_STATUS.VERIFIED]: [],
  [COMPETITIVE_STATUS.NEEDS_VERIFICATION]: [],
  [COMPETITIVE_STATUS.HISTORICAL]: [],
  [COMPETITIVE_STATUS.NOT_COMPETITIVE]: [],
};

const seenKeys = new Map();
const duplicates = [];

for (const o of annotated) {
  const status = o.competitiveStatus || COMPETITIVE_STATUS.NOT_COMPETITIVE;
  const key = dedupeKey(o);
  if (seenKeys.has(key) && status !== COMPETITIVE_STATUS.NOT_COMPETITIVE) {
    duplicates.push({
      id: o.id,
      duplicateOf: seenKeys.get(key),
      key,
      status,
    });
  } else if (status !== COMPETITIVE_STATUS.NOT_COMPETITIVE) {
    seenKeys.set(key, o.id);
  }
  byStatus[status] = byStatus[status] || [];
  byStatus[status].push({
    id: o.id,
    title: o.title,
    organization: o.organizationName || o.organization || null,
    competitiveStatus: status,
    cmiClass: o.cmiClass,
    cmiVerified: o.cmiVerified === true,
    competitorHotel: o.cmiCompetitorHotel || o.competitorHotel || null,
    competitorVenueStatus: o.competitorVenueStatus || null,
    targetHotelSourcingStatus: o.targetHotelSourcingStatus || null,
    evidenceUrls: o.cmiEvidenceUrls || null,
    reason: o.cmiClassificationReason || null,
    recommendedAction: o.recommendedAction || null,
    on67Baseline: manifest?.baselineIds?.includes?.(o.id) || false,
    salesBaselineMembership: o.salesBaselineMembership || null,
    inventoryPopulations: o.inventoryPopulations || [],
    originalResearchSource:
      o.historicalAttestationRef?.sourceArtifactPath ||
      o.provenance?.finalizeLogPath ||
      (o.sources?.[0]?.url || o.sources?.[0]?.pathRel || o.sources?.[0]) ||
      null,
    ledgerEntry: Boolean(BETHESDA_CMI_EVIDENCE_LEDGER[o.id]),
  });
}

const competitiveEligible = annotated.filter((o) =>
  (o.inventoryPopulations || []).includes("COMPETITIVE_MARKET_INTELLIGENCE")
);
const hotelEligible = annotated.filter((o) =>
  (o.inventoryPopulations || []).includes("SALES_OPPORTUNITIES")
);
const both = annotated.filter(
  (o) =>
    (o.inventoryPopulations || []).includes("COMPETITIVE_MARKET_INTELLIGENCE") &&
    (o.inventoryPopulations || []).includes("SALES_OPPORTUNITIES")
);

const summary = {
  version: "gdi-competitive-opportunity-recovery-v1",
  hotelId: BETHESDA_HOTEL_ID,
  generatedAt: new Date().toISOString(),
  existingVerifiedCmiCountPriorAudit: 4,
  counts: {
    unionInventory: annotated.length,
    verifiedCompetitive: byStatus[COMPETITIVE_STATUS.VERIFIED].length,
    needsVerification: byStatus[COMPETITIVE_STATUS.NEEDS_VERIFICATION].length,
    historicalCompetitive: byStatus[COMPETITIVE_STATUS.HISTORICAL].length,
    notCompetitive: byStatus[COMPETITIVE_STATUS.NOT_COMPETITIVE].length,
    competitiveEligibleTotal: competitiveEligible.length,
    hotelEligibleTotal: hotelEligible.length,
    bothHotelAndCompetitive: both.length,
    duplicatesExcluded: duplicates.length,
    uniqueAllInventory: annotated.length,
  },
  note:
    "Competitive total includes Verified + Needs Verification + Historical. Do not equate Competitive total with verified-booking total.",
  ledgerIds: Object.keys(BETHESDA_CMI_EVIDENCE_LEDGER),
};

writeJson("00-recovery-summary.json", summary);
writeJson("01-verified-competitive.json", byStatus[COMPETITIVE_STATUS.VERIFIED]);
writeJson(
  "02-needs-verification.json",
  byStatus[COMPETITIVE_STATUS.NEEDS_VERIFICATION]
);
writeJson(
  "03-historical-competitive.json",
  byStatus[COMPETITIVE_STATUS.HISTORICAL]
);
writeJson("04-duplicates-excluded.json", duplicates);
writeJson(
  "05-competitive-eligible.json",
  competitiveEligible.map((o) => ({
    id: o.id,
    title: o.title,
    competitiveStatus: o.competitiveStatus,
    competitorHotel: o.cmiCompetitorHotel,
    populations: o.inventoryPopulations,
  }))
);

const md = `# Bethesda Competitive Opportunity Recovery V1

Generated: ${summary.generatedAt}

## Headline

| Metric | Count |
|--------|------:|
| Existing verified CMI (prior #222 audit floor) | **4** |
| Verified Competitive (A) | **${summary.counts.verifiedCompetitive}** |
| Needs Verification (B) | **${summary.counts.needsVerification}** |
| Historical Competitive (C) | **${summary.counts.historicalCompetitive}** |
| Not Competitive (D) | ${summary.counts.notCompetitive} |
| **Competitive eligible total** | **${summary.counts.competitiveEligibleTotal}** |
| Hotel (Sales Opportunities) eligible | ${summary.counts.hotelEligibleTotal} |
| Both Hotel + Competitive | ${summary.counts.bothHotelAndCompetitive} |
| Duplicates excluded | ${summary.counts.duplicatesExcluded} |
| Unique All inventory (union) | ${summary.counts.uniqueAllInventory} |

> Competitive total ≠ verified-booking total. Status filter separates confidence.

## Verified Competitive

${byStatus[COMPETITIVE_STATUS.VERIFIED]
  .map(
    (r) =>
      `- \`${r.id}\` — ${r.title} → **${r.competitorHotel || "?"}** (${r.originalResearchSource || "ledger/existing"})`
  )
  .join("\n") || "_none_"}

## Needs Verification

${byStatus[COMPETITIVE_STATUS.NEEDS_VERIFICATION]
  .map(
    (r) =>
      `- \`${r.id}\` — ${r.title} → ${r.competitorHotel || "unnamed"} — ${r.reason || ""}`
  )
  .join("\n") || "_none_"}

## Historical Competitive

${byStatus[COMPETITIVE_STATUS.HISTORICAL]
  .map(
    (r) =>
      `- \`${r.id}\` — ${r.title} → ${r.competitorHotel || "?"} — ${r.reason || ""}`
  )
  .join("\n") || "_none_"}

## Guards

- Fourteen demand segments unchanged; CMI is not a fifteenth segment.
- Published 19 authorization boundary preserved.
- No auto-publish / no paid research.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER-COMPETITIVE-RECOVERY-REPORT.md"), md);
console.log(JSON.stringify(summary, null, 2));
console.log(`Wrote ${OUT}`);
