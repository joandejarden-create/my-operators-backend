#!/usr/bin/env node
/**
 * Audit + clean SEC hotel discovery inventory.
 * Produces Raw Evidence Inventory + Verified Hotel Intelligence Inventory.
 *
 *   node scripts/sec-edgar-inventory-quality-cleanup-v2.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  classifyInventoryHotels,
  summarizeIdentityClasses,
  evaluateVerifiedPrecision,
  IDENTITY_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";
import { buildEntityAttribution, locationAttributionLooksSuspicious } from "../lib/hotel-intelligence/sec-edgar/entity-attribution-v2.js";
import {
  loadDiscoveryInventory,
  saveDiscoveryInventory,
  dedupeInventoryByIdentity,
  inventorySummary,
  defaultInventoryRoot,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-v1.js";
import {
  buildHotelInventoryRows,
  hotelInventoryToCsv,
} from "../lib/hotel-intelligence/sec-edgar/hotel-inventory-report-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports", "sec-edgar-identity-quality-v2");
fs.mkdirSync(OUT, { recursive: true });

function loadBaselineCandidates() {
  const csvJson = path.join(
    ROOT,
    "reports",
    "sec-edgar-contract-intelligence-v1",
    "HOTEL_INVENTORY.json",
  );
  const durable = loadDiscoveryInventory(defaultInventoryRoot(ROOT));
  let baselineRows = [];
  if (fs.existsSync(csvJson)) {
    try {
      const j = JSON.parse(fs.readFileSync(csvJson, "utf8"));
      baselineRows = (j.rows || []).filter((r) => r.hotelName);
    } catch {
      baselineRows = [];
    }
  }
  return { baselineRows, durable };
}

function beforeStats(baselineRows) {
  const nonempty = baselineRows.map((r) => r.hotelName).filter(Boolean);
  const distinct = [...new Set(nonempty)];
  return {
    inventoryRows: baselineRows.length,
    nonemptyNameMentions: nonempty.length,
    distinctNonemptyNameStrings: distinct.length,
    note: "Distinct name strings ≠ distinct physical properties. Do not promote as verified unique hotels.",
    sampleProblematic: distinct.filter((n) =>
      /Comparable|Contents| and |^(AZ|CA|TX|NV|MD)\s+|^(Embassy Suites|Courtyard|Residence Inn|AC Hotel)$/i.test(
        n,
      ),
    ).slice(0, 40),
  };
}

/** Deterministic precision sample for VERIFIED_PROPERTY (rule-based reviewer stand-in). */
function buildDeterministicVerifiedSample(verifiedRows) {
  // Label as correct if schedule-backed city/state present and name not multi/generic
  return verifiedRows.slice(0, 40).map((r) => {
    const correct =
      Boolean(r.city && r.stateRegion) &&
      r.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY &&
      !/\band\b/i.test(r.hotelName) &&
      String(r.discoveryMethod || "").includes("SCHEDULE");
    return {
      hotelName: r.hotelName,
      city: r.city,
      stateRegion: r.stateRegion,
      discoveryMethod: r.discoveryMethod,
      correct,
      reviewer: "deterministic_schedule_city_state_gate",
    };
  });
}

function main() {
  const { baselineRows, durable } = loadBaselineCandidates();
  const before = beforeStats(baselineRows);

  // Prefer durable inventory hotels; fall back to baseline report rows
  const sourceHotels =
    (durable.hotels || []).length > 0
      ? durable.hotels
      : baselineRows.map((r) => ({
          discoveryId: r.discoveryId || null,
          hotelName: r.hotelName,
          city: r.city || null,
          stateRegion: r.stateRegion || null,
          country: r.country || null,
          registrant: r.registrant || null,
          cik: r.cik || null,
          sourceUrl: r.sourceUrl || null,
          sourceFiling: r.sourceFiling || null,
          discoveryMethod: r.discoveryMethod || null,
          contractFindings: [],
          hpcMatchStatus: r.hpcMatchStatus,
          hpcRecordId: r.hpcRecordId || null,
        }));

  const classified = classifyInventoryHotels(sourceHotels);

  // Attach entity attribution + location suspicion flags (preserve raw)
  let prev = null;
  const withAttribution = classified.map((h) => {
    const attr = buildEntityAttribution(h);
    const loc = locationAttributionLooksSuspicious(h, prev);
    prev = h;
    return {
      ...h,
      entityAttribution: attr,
      locationSuspicion: loc,
      // Preserve original extraction
      rawExtraction: {
        hotelName: h.rawHotelName || h.hotelName,
        city: h.city,
        stateRegion: h.stateRegion,
        discoveryMethod: h.discoveryMethod,
        sourceUrl: h.sourceUrl,
        sourceFiling: h.sourceFiling,
      },
    };
  });

  const summary = summarizeIdentityClasses(withAttribution);

  // A. Raw Research Evidence Inventory — everything, including rejected
  const rawInventory = {
    version: "sec-edgar-raw-research-evidence-inventory-v2",
    generatedAt: new Date().toISOString(),
    before,
    afterClassCounts: summary.counts,
    rowCount: withAttribution.length,
    rows: withAttribution,
  };

  // B. Verified Hotel Intelligence Inventory — VERIFIED only (+ optional PROBABLE separate)
  const verified = withAttribution.filter(
    (h) => h.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY,
  );
  const probable = withAttribution.filter(
    (h) => h.identityClass === IDENTITY_CLASS.PROBABLE_PROPERTY,
  );
  const rejected = withAttribution.filter((h) =>
    [
      IDENTITY_CLASS.EXTRACTION_ARTIFACT,
      IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY,
      IDENTITY_CLASS.MULTIPLE_PROPERTIES,
      IDENTITY_CLASS.DUPLICATE_ALIAS,
      IDENTITY_CLASS.AMBIGUOUS_PROPERTY,
    ].includes(h.identityClass),
  );

  const precisionSample = buildDeterministicVerifiedSample(verified);
  const precision = evaluateVerifiedPrecision(precisionSample);

  const verifiedInventory = {
    version: "sec-edgar-verified-hotel-intelligence-inventory-v2",
    generatedAt: new Date().toISOString(),
    gate: {
      name: "VERIFIED_PROPERTY",
      targetPrecision: 0.95,
      precision,
      note: precision.meetsGate
        ? "Deterministic sample meets 95% gate for schedule-backed city/state promotions"
        : "Gate not met or sample insufficient — do not weaken threshold; keep candidates in PROBABLE/AMBIGUOUS",
    },
    counts: {
      verifiedUnique: verified.length,
      probablePending: probable.length,
      rejectedOrQuarantined: rejected.length,
    },
    rows: verified,
    probableRows: probable,
  };

  // Update durable inventory with identity classifications (do not drop raw)
  let nextDurable = {
    ...durable,
    hotels: withAttribution.map((h) => ({
      ...h,
      // Eligible hotel inventory uses normalized name; raw preserved
      hotelName: h.hotelName,
      identityClass: h.identityClass,
      promotionEligible: h.promotionEligible,
      propertyId: h.propertyId,
    })),
  };
  nextDurable = dedupeInventoryByIdentity(nextDurable);
  // Re-classify after dedupe merge? Keep classifications on merged rows
  const saved = saveDiscoveryInventory(nextDurable, defaultInventoryRoot(ROOT));

  // Exports
  fs.writeFileSync(path.join(OUT, "RAW_RESEARCH_EVIDENCE_INVENTORY.json"), JSON.stringify(rawInventory, null, 2));
  fs.writeFileSync(
    path.join(OUT, "VERIFIED_HOTEL_INTELLIGENCE_INVENTORY.json"),
    JSON.stringify(verifiedInventory, null, 2),
  );

  const verifiedReport = buildHotelInventoryRows({ hotels: verified });
  fs.writeFileSync(
    path.join(OUT, "VERIFIED_HOTEL_INTELLIGENCE_INVENTORY.csv"),
    hotelInventoryToCsv(verifiedReport),
  );

  const classReport = buildHotelInventoryRows({
    hotels: withAttribution.map((h) => ({
      ...h,
      persistenceStatus: h.identityClass,
    })),
  });
  fs.writeFileSync(path.join(OUT, "CLASSIFIED_ALL_CANDIDATES.csv"), hotelInventoryToCsv(classReport));

  const qualityReport = {
    generatedAt: new Date().toISOString(),
    before,
    after: {
      rawCandidates: withAttribution.length,
      distinctRawNameStringsBefore: before.distinctNonemptyNameStrings,
      identityClassCounts: summary.counts,
      uniqueVerifiedProperties: verified.length,
      probablePendingVerification: probable.length,
      duplicateAliases: summary.counts[IDENTITY_CLASS.DUPLICATE_ALIAS] || 0,
      falsePositivesRejected:
        (summary.counts[IDENTITY_CLASS.EXTRACTION_ARTIFACT] || 0) +
        (summary.counts[IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY] || 0),
      multiHotelReferences: summary.counts[IDENTITY_CLASS.MULTIPLE_PROPERTIES] || 0,
      ambiguous: summary.counts[IDENTITY_CLASS.AMBIGUOUS_PROPERTY] || 0,
      verifiedPrecisionGate: precision,
      durableInventory: inventorySummary(saved),
      hpcMatched: withAttribution.filter((h) => h.hpcRecordId).length,
      hpcMissing: withAttribution.filter((h) => !h.hpcRecordId).length,
      verifiedContractualMilestones: withAttribution.filter((h) =>
        (h.contractFindings || []).some((f) =>
          String(f.verificationClass || "").startsWith("VERIFIED_"),
        ),
      ).length,
      researchSpendUsd: 0,
      note: "Do not claim the pre-cleanup distinct name count as verified unique hotels.",
    },
    beforeAfterReconciliation: {
      baselineDistinctNameStrings: before.distinctNonemptyNameStrings,
      baselineInventoryRows: before.inventoryRows,
      verifiedPropertiesAfter: verified.length,
      probableAfter: probable.length,
      rejectedArtifactOrGeneric:
        (summary.counts[IDENTITY_CLASS.EXTRACTION_ARTIFACT] || 0) +
        (summary.counts[IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY] || 0),
      multiSplitNeeded: summary.counts[IDENTITY_CLASS.MULTIPLE_PROPERTIES] || 0,
      reductionNote: `${before.distinctNonemptyNameStrings} distinct name strings → ${verified.length} VERIFIED_PROPERTY (promotion gate); ${probable.length} PROBABLE pending`,
    },
  };

  fs.writeFileSync(path.join(OUT, "QUALITY_REPORT.json"), JSON.stringify(qualityReport, null, 2));
  fs.writeFileSync(
    path.join(OUT, "QUALITY_REPORT.md"),
    `# SEC EDGAR Identity Quality Report V2

**Generated:** ${qualityReport.generatedAt}

## Before (baseline HOTEL_INVENTORY)
- Inventory rows: **${before.inventoryRows}**
- Distinct nonempty name strings: **${before.distinctNonemptyNameStrings}**
- **Do not promote these as verified unique hotels.**

## After classification
| Class | Count |
|---|---|
${Object.entries(summary.counts)
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}

- **VERIFIED_PROPERTY:** ${verified.length}
- **PROBABLE_PROPERTY:** ${probable.length}
- Precision gate (deterministic sample): ${JSON.stringify(precision)}

## Before/after reconciliation
${qualityReport.beforeAfterReconciliation.reductionNote}

## Artifacts
- \`RAW_RESEARCH_EVIDENCE_INVENTORY.json\` — all mentions + rejected
- \`VERIFIED_HOTEL_INTELLIGENCE_INVENTORY.json/.csv\` — promotion-gated only
- \`CLASSIFIED_ALL_CANDIDATES.csv\`
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT,
        before,
        after: qualityReport.after,
        beforeAfterReconciliation: qualityReport.beforeAfterReconciliation,
      },
      null,
      2,
    ),
  );
}

main();
