/**
 * ADP_CANONICAL_REPORT_VIEW_MODEL_V1
 *
 * Normalize any supported ADP owner payload (staged / published / share)
 * into one view model that feeds the master presentation components.
 *
 * Does not mutate raw monitoring observations.
 * Does not silently rewrite historical published artifacts on disk —
 * attach-at-read enrichment only.
 */

import {
  ADP_MASTER_REPORT_PRESENTATION_V1,
  ADP_MASTER_STRUCTURAL_REFERENCE,
  ADP_CANONICAL_SECTION_IDS_V1,
  certifyMasterReportPresentation,
} from "./adp-master-report-presentation-v1.js";
import { buildTrendsPresentationV1 } from "./adp-trends-presentation-v1.js";
import {
  applyBppCustomerPresentationV2,
  BPP_CUSTOMER_PRESENTATION_V2,
} from "../brand-portfolio/bpp-customer-presentation-v2.js";

export const ADP_CANONICAL_REPORT_VIEW_MODEL_V1 =
  "ADP_CANONICAL_REPORT_VIEW_MODEL_V1";

/**
 * Build / attach canonical view model onto an owner payload.
 *
 * @param {object} payload
 * @param {{ propertyId?: string }} [opts]
 * @returns {object}
 */
export function attachCanonicalReportViewModelV1(payload, opts = {}) {
  if (!payload || typeof payload !== "object") return payload;

  const propertyId =
    opts.propertyId ||
    payload.propertyId ||
    payload.property?.propertyId ||
    payload.profile?.propertyId ||
    null;

  let bpp = payload.brandPortfolioPosition || null;
  if (bpp) {
    bpp = applyBppCustomerPresentationV2(bpp);
  }

  const trendsPresentation = buildTrendsPresentationV1({
    ...payload,
    brandPortfolioPosition: bpp,
  });

  const viewModel = {
    contract: ADP_CANONICAL_REPORT_VIEW_MODEL_V1,
    masterPresentationContract: ADP_MASTER_REPORT_PRESENTATION_V1,
    structuralReference: ADP_MASTER_STRUCTURAL_REFERENCE,
    propertyId,
    reportVersion:
      payload.reportVersion ||
      payload.stagedVersionId ||
      payload.period?.periodId ||
      payload.periodId ||
      null,
    monitoringPeriodId:
      payload.period?.periodId || payload.periodId || payload.measurementPeriod || null,
    sectionOrder: [...ADP_CANONICAL_SECTION_IDS_V1],
    trends: trendsPresentation,
    bpp: bpp
      ? {
          presentationContractVersion:
            bpp.presentationContractVersion || BPP_CUSTOMER_PRESENTATION_V2,
          status: bpp.status || null,
          ready: bpp.status === "READY",
          primaryKpiCount: Array.isArray(bpp.kpis) ? bpp.kpis.length : 0,
          primaryKpiTarget: 4,
          hasRanking: !!(bpp.ranking?.rows?.length),
          customerState: bpp.customerState || null,
        }
      : {
          presentationContractVersion: BPP_CUSTOMER_PRESENTATION_V2,
          status: "PENDING",
          ready: false,
          primaryKpiCount: 0,
          primaryKpiTarget: 4,
          hasRanking: false,
          customerState: {
            statusLabel: "Portfolio measurement is being established",
          },
        },
  };

  const certification = certifyMasterReportPresentation({
    ...payload,
    brandPortfolioPosition: bpp,
    masterPresentationContract: ADP_MASTER_REPORT_PRESENTATION_V1,
    canonicalViewModel: viewModel,
    trendsPresentation,
  });

  return {
    ...payload,
    brandPortfolioPosition: bpp,
    masterPresentationContract: ADP_MASTER_REPORT_PRESENTATION_V1,
    trendsPresentation,
    canonicalViewModel: viewModel,
    masterPresentationCertification: certification,
  };
}

/**
 * Lightweight structural audit for one hotel payload (no browser).
 */
export function auditCanonicalViewModelStructure(payload) {
  const enriched = attachCanonicalReportViewModelV1(payload);
  const failures = [];
  const vm = enriched.canonicalViewModel;
  if (!vm) failures.push("MISSING_VIEW_MODEL");
  if (vm?.sectionOrder?.join(",") !== ADP_CANONICAL_SECTION_IDS_V1.join(",")) {
    failures.push("SECTION_ORDER_DRIFT");
  }
  if (vm?.trends?.kpiSlots?.length !== 4) failures.push("TRENDS_NOT_FOUR_SLOTS");
  if (vm?.bpp?.primaryKpiTarget !== 4) failures.push("BPP_TARGET_NOT_FOUR");
  if (
    enriched.brandPortfolioPosition &&
    enriched.brandPortfolioPosition.presentationContractVersion !==
      BPP_CUSTOMER_PRESENTATION_V2 &&
    enriched.brandPortfolioPosition.status === "READY"
  ) {
    failures.push("READY_BPP_NOT_V2");
  }
  return {
    propertyId: vm?.propertyId || null,
    trendsState: vm?.trends?.state || null,
    bppReady: vm?.bpp?.ready || false,
    pass: failures.length === 0 && enriched.masterPresentationCertification?.pass !== false,
    failures: [
      ...failures,
      ...(enriched.masterPresentationCertification?.failures || []),
    ],
  };
}
