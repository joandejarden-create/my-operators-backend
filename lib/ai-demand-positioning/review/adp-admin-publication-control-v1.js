/**
 * AI Admin — ADP publication control matrix (local staging vs production).
 * Durable workflow state from publication registry + Full E2E reviewer inventory.
 */

import { listAdpReviewerProperties } from "./list-adp-reviewer-properties-v1.js";
import {
  getHotelPublicationRecord,
  listAllPublicationRecords,
} from "./adp-publication-registry-v1.js";
import { loadStagedManifest } from "./adp-staged-report-store-v1.js";
import {
  summarizeProductionStatus,
  PRODUCTION_STATUS,
} from "./adp-production-verification-v1.js";
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { hotelLedgerAdminProjection } from "../../hotel-census/hpc-persistent-field-research-ledger-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../governance/adp-operational-registry-hygiene-v1.js";

export const ADP_ADMIN_PUBLICATION_CONTROL_V1 = "ADP_ADMIN_PUBLICATION_CONTROL_V1";

function loadFullE2eStatus(propertyId, cwd = process.cwd()) {
  const candidates = [
    join(cwd, "data/dealality-full-e2e-os", propertyId, "status.json"),
    join(cwd, "data/ai-demand-positioning/full-e2e", propertyId, "status.json"),
    join(cwd, `reports/gdi-adp-five-hotel-portfolio-e2e-v1/${propertyId}/FULL_E2E_STATUS.json`),
  ];
  for (const p of candidates) {
    if (!existsSync(p)) continue;
    try {
      return { path: p, ...(JSON.parse(readFileSync(p, "utf8")) || {}) };
    } catch {
      /* continue */
    }
  }
  return null;
}

function deriveFilterBucket(row) {
  if (row.publicationState === "CERTIFICATION_FAILED" || row.certificationStatus === "FAILED") {
    return "blocked";
  }
  if (row.productionVerificationStatus === PRODUCTION_STATUS.PRODUCTION_VERIFIED) {
    return "published_in_production";
  }
  if (
    row.productionVerificationStatus === PRODUCTION_STATUS.PRODUCTION_MISMATCH ||
    row.productionVerificationStatus === PRODUCTION_STATUS.PRODUCTION_UNREACHABLE
  ) {
    return "production_verification_failed";
  }
  if (row.publicationState === "APPROVED_FOR_PUBLICATION") {
    return "approved_awaiting_publication";
  }
  if (row.publicationState === "CHANGES_REQUESTED") {
    return "needs_correction";
  }
  if (
    row.publicationState === "READY_FOR_REVIEW" ||
    row.publicationState === "STAGED" ||
    row.hasStaged
  ) {
    if (row.reviewStatus === "NEEDS_REVIEW" || row.publicationState === "READY_FOR_REVIEW") {
      return "needs_review";
    }
    return "local_staging_only";
  }
  return "local_staging_only";
}

function nextActionFor(row) {
  if (row.eligibility === "EXCLUDED" || row.researchEligibility === "EXCLUDED") {
    if (row.hasPublished || row.hasStaged || row.hasHistoricalReport) {
      return "Historical review only — research exclusion active (see ADP_RESEARCH_EXCLUDED_PROPERTY_IDS)";
    }
    return "Excluded from ADP execution (no historical report)";
  }
  if (!row.hasStaged) return "Run Full E2E staging / rebuild atomic staged report";
  if (row.certificationStatus === "FAILED" || row.publicationState === "CERTIFICATION_FAILED") {
    return "Resolve certification blockers and rebuild staged snapshot";
  }
  if (row.publicationState === "CHANGES_REQUESTED") {
    return "Address review comments and restage";
  }
  if (row.publicationState === "READY_FOR_REVIEW" || row.publicationState === "STAGED") {
    return "Open staged review in Owner AI Demand";
  }
  if (row.publicationState === "APPROVED_FOR_PUBLICATION") {
    return "Authorized publication of approved artifact hash (ADP_PUBLICATION_AUTHORIZED)";
  }
  if (row.productionVerificationStatus === PRODUCTION_STATUS.PRODUCTION_NOT_VERIFIED) {
    return "Verify production delivery (set ADP_PRODUCTION_BASE_URL) or publish when authorized";
  }
  if (row.productionVerificationStatus === PRODUCTION_STATUS.PRODUCTION_VERIFIED) {
    return "Monitor; review newer staged revision if present";
  }
  return row.nextAction || "Review";
}

/**
 * Build control-center rows for all eligible (and optionally excluded) ADP hotels.
 * Does not probe production live unless liveProbes map is supplied.
 */
export function buildAdpAdminPublicationMatrix({
  includeExcluded = true,
  liveProbes = null,
  cwd = process.cwd(),
} = {}) {
  const properties = listAdpReviewerProperties({ includeExcluded }).filter(
    (p) => !isAdpSyntheticOrTestPropertyId(p.propertyId)
  );
  const registryById = Object.fromEntries(
    listAllPublicationRecords()
      .filter((r) => !isAdpSyntheticOrTestPropertyId(r.propertyId))
      .map((r) => [r.propertyId, r])
  );

  const rows = properties.map((p) => {
    const propertyId = p.propertyId;
    const pub = registryById[propertyId] || getHotelPublicationRecord(propertyId, cwd);
    const staged = loadStagedManifest(propertyId, cwd);
    const e2e = loadFullE2eStatus(propertyId, cwd);
    const prodSummary = summarizeProductionStatus({
      publicationRecord: pub,
      liveProbe: liveProbes?.[propertyId] || null,
    });

    // Portfolio audit overlay (optional, non-authoritative until refreshed)
    let portfolioOverlay = null;
    let airtableForensic = null;
    let sectionCompleteness = null;
    try {
      const manifestPath = join(
        cwd,
        "reports/dealality-adp-portfolio-audit-and-production-v1/PER_HOTEL_PUBLICATION_MANIFEST.json"
      );
      if (existsSync(manifestPath)) {
        const doc = JSON.parse(readFileSync(manifestPath, "utf8"));
        portfolioOverlay = (doc.hotels || []).find((h) => h.canonicalHotelId === propertyId) || null;
      }
      const forensicSummary = join(
        cwd,
        "reports/dealality-adp-portfolio-audit-and-production-v1/airtable-forensic-audit/summary.json"
      );
      const forensicMap = join(
        cwd,
        "reports/dealality-adp-portfolio-audit-and-production-v1/airtable-forensic-audit/ADP_HOTEL_RECORD_MAPPING.csv"
      );
      if (existsSync(forensicSummary)) {
        const sum = JSON.parse(readFileSync(forensicSummary, "utf8"));
        airtableForensic = {
          auditedAt: sum.generatedAt || null,
          liveCounts: sum.liveCounts || null,
          hotelCoverage: sum.hotelCoverage || null,
          mappingCsvPresent: existsSync(forensicMap),
        };
      }
      const sectionPath = join(
        cwd,
        "reports/dealality-adp-product-completeness-v1/PER_HOTEL_SECTION_STATUS.json"
      );
      if (existsSync(sectionPath)) {
        const secDoc = JSON.parse(readFileSync(sectionPath, "utf8"));
        sectionCompleteness =
          (secDoc.hotels || []).find((h) => h.propertyId === propertyId) || null;
      }
    } catch {
      portfolioOverlay = null;
      airtableForensic = null;
      sectionCompleteness = null;
    }

    let hpcResearch = null;
    try {
      hpcResearch = hotelLedgerAdminProjection(propertyId);
    } catch {
      hpcResearch = null;
    }

    const row = {
      propertyId,
      hotelName: p.hotelName || p.name || propertyId,
      market: p.market || p.city || "",
      eligibility: p.eligibility || "ELIGIBLE",
      hpcEnrichmentReadiness: hpcResearch?.enrichmentReadiness || null,
      hpcCriticalOpen: hpcResearch?.criticalOpen ?? null,
      hpcImportantOpen: hpcResearch?.importantOpen ?? null,
      hpcEnrichmentOpen: hpcResearch?.enrichmentOpen ?? null,
      hpcCriticalBlockers: (hpcResearch?.criticalBlockers || []).join("; ") || null,
      hpcLastResearchAttemptAt: hpcResearch?.lastResearchAttemptAt || null,
      hpcNextEligibleAction: hpcResearch?.nextEligibleAction || null,
      hpcRecordId: hpcResearch?.hpcRecordId || null,
      adpRunId: pub?.runId || p.latestRunId || e2e?.runId || null,
      measurementPeriod: pub?.measurementPeriod || p.measurementPeriod || e2e?.periodId || null,
      workflowVersion: pub?.workflowVersion || "ADP_FULL_E2E_V1",
      localStagedVersion: staged?.latestStagedVersionId || pub?.stagedReportVersion || null,
      localStagedArtifactHash: staged?.artifactHash || pub?.stagedArtifactHash || null,
      hasStaged: !!(staged?.latestStagedVersionId || p.hasStaged),
      certificationStatus:
        pub?.certificationResult?.status ||
        pub?.certificationResult ||
        p.certificationStatus ||
        (p.competitiveCertification === "NOT_CERTIFIED" ? "NOT_CERTIFIED" : "UNKNOWN"),
      reviewStatus: pub?.reviewStatus || p.reviewStatus || null,
      publicationState: pub?.publicationState || p.publicationState || "RESEARCH_IN_PROGRESS",
      approvedVersion: pub?.approvedReportVersion || null,
      approvedArtifactHash: pub?.approvedArtifactHash || pub?.stagedArtifactHash || null,
      ...prodSummary,
      lastPublishedDate: prodSummary.historicalPublishedAt,
      fullE2eLastStage: e2e?.lastSuccessfulStage || e2e?.currentStage || null,
      fullE2eBlocker: e2e?.currentBlocker || e2e?.blocker || null,
      fullE2eLastRunAt: e2e?.updatedAt || e2e?.lastRunAt || null,
      customerDropdownVisible: p.customerDropdownVisible === true,
      // Portfolio audit fields for ADP Status drawer
      airtableReconciled: portfolioOverlay?.grades?.AIRTABLE_RECONCILED === true,
      airtableForensicAuditedAt: airtableForensic?.auditedAt || null,
      airtableForensicMappingAvailable: airtableForensic?.mappingCsvPresent === true,
      airtableLivePublishedReports: airtableForensic?.liveCounts?.publishedReports ?? null,
      airtableLiveHotelAdpAttributes: airtableForensic?.liveCounts?.hotelAdpAttributes ?? null,
      pdfArtifact: portfolioOverlay?.pdfArtifact || null,
      pdfParityVerified: portfolioOverlay?.grades?.WEB_PDF_PARITY_VERIFIED === true,
      pdfReadiness: portfolioOverlay?.pdfArtifact
        ? portfolioOverlay?.grades?.WEB_PDF_PARITY_VERIFIED
          ? "PARITY_VERIFIED"
          : "PDF_PRESENT_PARITY_UNVERIFIED"
        : "PDF_MISSING",
      attributeGrade: portfolioOverlay?.grades
        ? portfolioOverlay.grades.READY_FOR_PUBLICATION_REVIEW
          ? "REVIEW_READY"
          : "NEEDS_WORK"
        : null,
      portfolioBucket: portfolioOverlay?.bucket || null,
      productionReportUrl: portfolioOverlay?.proposedCustomerUrl || null,
      productionPdfUrl: null,
      // Section completeness (product-completeness audit overlay)
      sectionCompletenessContract: sectionCompleteness?.contractVersion || null,
      sectionCertifiedCount: sectionCompleteness?.certifiedCount ?? null,
      sectionCount: sectionCompleteness?.sectionCount ?? null,
      sectionMaterialIncompleteCount: sectionCompleteness?.materialIncompleteCount ?? null,
      sectionPublicationReadyCandidate:
        sectionCompleteness?.publicationReadyCandidate === true,
      historicalTrendStatus: sectionCompleteness?.sections?.historical_trends?.status || null,
      brandRankingStatus:
        sectionCompleteness?.sections?.brand_portfolio_position?.status || null,
      attributeIntelligenceStatus:
        sectionCompleteness?.sections?.ai_reality_gaps?.status || null,
      sectionNextAction: sectionCompleteness?.primaryNextAction || null,
      nextAction: null,
      openReviewPath: `/app#/owner/ai-demand?propertyId=${encodeURIComponent(propertyId)}&version=staged`,
      comparePath: `/app#/owner/ai-demand?propertyId=${encodeURIComponent(propertyId)}&version=staged`,
    };
    row.filterBucket = deriveFilterBucket(row);
    row.nextAction =
      sectionCompleteness?.primaryNextAction ||
      (portfolioOverlay && portfolioOverlay.requiredAction) ||
      nextActionFor(row);
    return row;
  });

  const filters = {
    all: rows.length,
    local_staging_only: rows.filter((r) => r.filterBucket === "local_staging_only").length,
    needs_review: rows.filter((r) => r.filterBucket === "needs_review").length,
    needs_correction: rows.filter((r) => r.filterBucket === "needs_correction").length,
    approved_awaiting_publication: rows.filter(
      (r) => r.filterBucket === "approved_awaiting_publication"
    ).length,
    published_in_production: rows.filter((r) => r.filterBucket === "published_in_production")
      .length,
    production_verification_failed: rows.filter(
      (r) => r.filterBucket === "production_verification_failed"
    ).length,
    blocked: rows.filter((r) => r.filterBucket === "blocked").length,
  };

  return {
    ok: true,
    contractVersion: ADP_ADMIN_PUBLICATION_CONTROL_V1,
    generatedAt: new Date().toISOString(),
    hotelCount: rows.length,
    filters,
    productionBaseConfigured: !!(
      process.env.ADP_PRODUCTION_BASE_URL ||
      process.env.DEALALITY_PRODUCTION_BASE_URL ||
      process.env.PRODUCTION_APP_BASE_URL
    ),
    doctrine: {
      localPublishedFileIsNotProof: true,
      unknownProductionIsNotUnpublished: true,
      approvalIsNotPublicationAuthorization: true,
    },
    rows,
  };
}
