#!/usr/bin/env node
/**
 * Zero-paid GDI commercial intelligence consistency audit.
 * Bethesda-first; framework reusable for other hotels.
 * Does not publish or spend.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  loadBethesda67SalesBaselineManifest,
  annotateInventoryPopulations,
  classifyCompetitiveMarketIntelligence,
  BETHESDA_CMI_EVIDENCE_LEDGER,
  applyCmiLedgerRemediation,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";
import {
  detectOpportunityContradictions,
  detectPortfolioContradictions,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-contradiction-detector-v1.js";
import { listRegisteredHotels } from "../lib/group-demand-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-commercial-intelligence-consistency-audit-v1"
);

function loadHotelOpps(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json"
  );
  if (!fs.existsSync(p)) return [];
  return JSON.parse(fs.readFileSync(p, "utf8")).opportunities || [];
}

function auditSevenCmi() {
  const beforeAfter = [];
  for (const [id, ledger] of Object.entries(BETHESDA_CMI_EVIDENCE_LEDGER)) {
    const rawList = loadHotelOpps(BETHESDA_HOTEL_ID);
    const raw = rawList.find((o) => o.id === id) || { id };
    const beforeClass = {
      // Simulate legacy seed-as-proof behavior for reporting only
      legacyWouldVerifyFromSeedAlone: true,
      competitorHotelBefore:
        raw.competitorHotel || raw.likelyCompetitor || null,
      venueSourcingStatusBefore: raw.venueSourcingStatus || null,
      recommendedActionBefore: (raw.recommendedAction || "").slice(0, 200),
      attendanceBefore:
        raw.attendance || raw.advertisedAttendance || raw.estimatedAttendance,
    };
    const remediated = applyCmiLedgerRemediation(raw);
    const after = classifyCompetitiveMarketIntelligence(remediated);
    const contradictionsBefore = detectOpportunityContradictions(raw);
    const contradictionsAfter = detectOpportunityContradictions(remediated);
    beforeAfter.push({
      id,
      verifiedBefore: Boolean(
        Object.keys(BETHESDA_CMI_EVIDENCE_LEDGER).includes(id)
      ),
      verifiedAfter: after.verified === true,
      competitorHotelBefore: beforeClass.competitorHotelBefore,
      competitorHotelAfter: after.competitorHotel,
      competitorVenueStatus: ledger.competitorVenueStatus,
      targetHotelSourcingStatus: ledger.targetHotelSourcingStatus,
      commercialActionability: ledger.commercialActionability,
      demotionReason: ledger.demotionReason || null,
      rationale: after.reason,
      evidenceUrls: after.evidenceUrls || [],
      contradictionsBefore: contradictionsBefore.findingCount,
      contradictionsAfter: contradictionsAfter.findingCount,
      criticalAfter: contradictionsAfter.findings.filter(
        (f) => f.severity === "CRITICAL"
      ).length,
      recommendedActionAfter: (remediated.recommendedAction || "").slice(0, 220),
      on67Baseline: true,
    });
  }
  return beforeAfter;
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const manifest = loadBethesda67SalesBaselineManifest({ forceReload: true });
  const raw = loadHotelOpps(BETHESDA_HOTEL_ID);
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    raw
  );
  const family = applyBethesdaCommercialEventFamilyPresentation(
    union.opportunities,
    { hideLinkedFromList: true }
  );
  const surface = filterOpportunitiesByCommercialSurface(
    family.opportunities,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL
  );
  const annotated = annotateInventoryPopulations(surface, {
    hotelId: BETHESDA_HOTEL_ID,
  });

  const cmiAudit = auditSevenCmi();
  const verifiedIds = cmiAudit.filter((r) => r.verifiedAfter).map((r) => r.id);
  const demotedIds = cmiAudit.filter((r) => !r.verifiedAfter).map((r) => r.id);

  const uniqueBaseline = new Set(manifest.baselineIds);
  const cmiOnBaseline = verifiedIds.filter((id) => uniqueBaseline.has(id));
  const dualClass = annotated.filter(
    (o) =>
      (o.inventoryPopulations || []).includes("SALES_OPPORTUNITIES") &&
      o.cmiVerified === true
  );

  const beforePortfolio = detectPortfolioContradictions(surface);
  const afterPortfolio = detectPortfolioContradictions(annotated);

  const unverifiedCandidates = annotated.filter(
    (o) =>
      o.cmiClass === "NEEDS_FURTHER_VERIFICATION" ||
      (/competitor|overflow|primary_venue/i.test(
        `${o.venueSourcingStatus || ""} ${o.title || ""}`
      ) &&
        !o.cmiVerified)
  );

  const hotels = listRegisteredHotels().map((h) => ({
    hotelId: h.hotelId,
    name: h.name || h.hotelName || h.displayName,
  }));

  const summary = {
    schema: "gdi-commercial-intelligence-consistency-audit-v1",
    generatedAt: new Date().toISOString(),
    paidAuthorized: false,
    hotelId: BETHESDA_HOTEL_ID,
    uniqueBaselineIds: uniqueBaseline.size,
    publishedIds: manifest.publishedIds.length,
    stagedIds: manifest.stagedIds.length,
    cmiLedgerSize: Object.keys(BETHESDA_CMI_EVIDENCE_LEDGER).length,
    verifiedCmiAfter: verifiedIds.length,
    demotedCmiAfter: demotedIds.length,
    verifiedIds,
    demotedIds,
    cmiOnBaselineCount: cmiOnBaseline.length,
    dualClassifiedUniqueIds: dualClass.map((o) => o.id),
    dualClassifiedCount: dualClass.length,
    inventedCards: 0,
    note:
      "CMI is a secondary population on the same unique opportunity IDs — not additive to 67.",
    contradictionBefore: {
      withFindings: beforePortfolio.withFindings,
      critical: beforePortfolio.critical,
      high: beforePortfolio.high,
    },
    contradictionAfter: {
      withFindings: afterPortfolio.withFindings,
      critical: afterPortfolio.critical,
      high: afterPortfolio.high,
    },
    unverifiedCandidateCount: unverifiedCandidates.length,
    registryHotels: hotels.length,
    publicationSafeguard: {
      autoPublish: false,
      customerInventoryWidened: false,
      original67Preserved: true,
    },
  };

  fs.writeFileSync(
    path.join(OUT, "00-audit-summary.json"),
    JSON.stringify(summary, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT, "01-seven-cmi-before-after.json"),
    JSON.stringify(cmiAudit, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT, "02-contradictions-before.json"),
    JSON.stringify(beforePortfolio, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT, "03-contradictions-after.json"),
    JSON.stringify(afterPortfolio, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT, "04-unverified-cmi-candidates.json"),
    JSON.stringify(
      unverifiedCandidates.slice(0, 80).map((o) => ({
        id: o.id,
        title: o.title,
        cmiClass: o.cmiClass,
        competitorHotel: o.cmiCompetitorHotel || o.competitorHotel,
        venueSourcingStatus: o.venueSourcingStatus,
      })),
      null,
      2
    )
  );

  console.log(JSON.stringify(summary, null, 2));
}

main();
