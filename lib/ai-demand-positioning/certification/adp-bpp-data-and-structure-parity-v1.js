/**
 * ADP_BPP_DATA_AND_STRUCTURE_PARITY_V1
 *
 * Permanent Full E2E gate: measurement archive + BPP metrics + peer/rank
 * certification + pack attachment + Bethesda structure + runtime values.
 * Structural PASS must not erase data/ranking failures.
 */

import fs from "fs";
import path from "path";
import { loadLoyaltyBppStagedPack } from "../brand-portfolio/loyalty-bpp-staged-pack-store-v1.js";
import { loadAllPeriods, loadPropertyProfile } from "../data-model.js";
import { resolveBrandPortfolioPosition } from "../../../api/ai-demand-positioning.js";
import {
  BPP_CUSTOMER_COMPONENT_ORDER_V2,
  BPP_CUSTOMER_FORBIDDEN_COMPONENTS_V2,
  BPP_PRIMARY_KPI_IDS_V2,
} from "../brand-portfolio/bpp-customer-presentation-v2.js";
import { BPP_CUSTOMER_STATE_SUBJECT_MEASURED_RANK_BLOCKED } from "../brand-portfolio/bpp-subject-measured-partial-v1.js";

export const ADP_BPP_DATA_AND_STRUCTURE_PARITY_V1 =
  "ADP_BPP_DATA_AND_STRUCTURE_PARITY_V1";

const FALSE_UNMEASURED =
  /Portfolio measurement is being established|Portfolio measurement not yet established|monitoring cycle has not been run/i;

function latestLoyalty(propertyId) {
  const periods = loadAllPeriods(propertyId) || [];
  return (
    periods
      .filter((p) => String(p.measurementFamily || "").includes("LOYALTY"))
      .sort((a, b) => String(b.periodId).localeCompare(String(a.periodId)))[0] ||
    null
  );
}

/**
 * Assess one hotel against the BPP data+structure parity gate.
 */
export function assessBppDataAndStructureParity(propertyId, opts = {}) {
  const profile = loadPropertyProfile(propertyId);
  const loyalty = latestLoyalty(propertyId);
  const staged = loadLoyaltyBppStagedPack(propertyId);
  const resolved = resolveBrandPortfolioPosition(propertyId, profile, null, {
    preferLoyaltyStaged: opts.preferLoyaltyStaged !== false,
    reportVersion: opts.reportVersion || "staged",
  });

  const hasLoyaltyObs = (loyalty?.observations || []).length > 0;
  const hasPack =
    !!staged?.ok ||
    resolved?.status === "READY" ||
    resolved?.status === "EXCEPTION_SUPPRESSED";

  const kpis = Array.isArray(resolved?.kpis) ? resolved.kpis : [];
  const primaryIds = kpis.map((k) => k.id).filter(Boolean);
  const subjectMeasured =
    resolved?.bppCustomerState === BPP_CUSTOMER_STATE_SUBJECT_MEASURED_RANK_BLOCKED ||
    resolved?.partialCertification?.subjectMetricsCertified === true;
  const rankCertified =
    !subjectMeasured &&
    resolved?.status === "READY" &&
    Array.isArray(resolved?.ranking?.rows) &&
    resolved.ranking.rows.length > 1 &&
    !resolved.ranking?.rankBlocked;

  const customerText = JSON.stringify({
    customerState: resolved?.customerState || null,
    emptyMessage: resolved?.emptyMessage || null,
    kpis,
  });
  const falseUnmeasured =
    hasLoyaltyObs && FALSE_UNMEASURED.test(customerText);
  const statusPanelCopy =
    /PORTFOLIO MEASUREMENT COMPLETE\s*[—\-–]\s*PEER RANKING PENDING|peer ranking pending/i.test(
      customerText
    );

  const rankingRows = Array.isArray(resolved?.ranking?.rows)
    ? resolved.ranking.rows
    : [];
  const peerRows = rankingRows.filter((r) => r && !r.isSubject);
  const subjectOnlyTable =
    rankingRows.length === 1 &&
    rankingRows[0]?.isSubject === true &&
    peerRows.length === 0;
  const certifiedPeerCount =
    resolved?.peerSet?.peerCount ??
    resolved?.peerSet?.included?.length ??
    null;

  const layout = resolved?.presentationLayout || {};
  const forbiddenPresent = (layout.forbiddenComponents || []).length
    ? false
    : BPP_CUSTOMER_FORBIDDEN_COMPONENTS_V2.some((f) =>
        JSON.stringify(resolved || {}).includes(f)
      );

  const gates = {
    MEASUREMENT_ARCHIVE_PASS: hasLoyaltyObs || hasPack ? "PASS" : "FAIL",
    BPP_METRICS_PASS:
      kpis.length >= 3 ||
      resolved?.status === "EXCEPTION_SUPPRESSED" ||
      subjectMeasured
        ? "PASS"
        : hasLoyaltyObs
          ? "FAIL"
          : "N/A",
    PEER_COHORT_PASS: rankCertified
      ? certifiedPeerCount >= 3 || peerRows.length >= 3
        ? "PASS"
        : "FAIL"
      : subjectMeasured
        ? "BLOCKED"
        : resolved?.status === "EXCEPTION_SUPPRESSED"
          ? "N/A"
          : "BLOCKED",
    RANKING_PASS: rankCertified
      ? peerRows.length >= 3 && !subjectOnlyTable
        ? "PASS"
        : "FAIL"
      : subjectMeasured
        ? "BLOCKED_EXPLICIT"
        : resolved?.status === "EXCEPTION_SUPPRESSED"
          ? "N/A"
          : "FAIL",
    PEER_TABLE_POPULATION_PASS: rankCertified
      ? peerRows.length >= 3
        ? "PASS"
        : "FAIL"
      : subjectMeasured
        ? rankingRows.length === 0
          ? "PASS_EMPTY_UNAVAILABLE"
          : subjectOnlyTable
            ? "FAIL"
            : "PASS"
        : "N/A",
    BPP_PACK_ATTACHMENT_PASS: hasPack ? "PASS" : "FAIL",
    BPP_STRUCTURE_PASS:
      layout.referenceFormat === "BETHESDA_MARRIOTT" ||
      resolved?.presentationContractVersion === "BPP_CUSTOMER_PRESENTATION_V2"
        ? "PASS"
        : "FAIL",
    BPP_RUNTIME_VALUE_PASS: falseUnmeasured || statusPanelCopy ? "FAIL" : "PASS",
    BPP_VISUAL_PASS: "REQUIRES_BROWSER",
  };

  const exceptionSuppressed =
    resolved?.status === "EXCEPTION_SUPPRESSED" ||
    resolved?.renderState === "EXCEPTION_SUPPRESSED";

  const negatives = [];
  if (
    hasLoyaltyObs &&
    !staged?.ok &&
    resolved?._bppPackSource === "customer_published_core" &&
    !exceptionSuppressed
  ) {
    negatives.push("LOYALTY_COMPLETE_BUT_CORE_FALLBACK");
  }
  if (falseUnmeasured) negatives.push("FALSE_UNMEASURED_LANGUAGE");
  if (hasLoyaltyObs && kpis.length === 0 && !exceptionSuppressed) {
    negatives.push("BLANK_KPI_DESPITE_MEASUREMENT");
  }
  if (
    subjectMeasured &&
    rankCertified
  ) {
    negatives.push("UNSUPPORTED_RANKING_SHOWN_AS_CERTIFIED");
  }
  if (primaryIds.some((id) => !BPP_PRIMARY_KPI_IDS_V2.includes(id))) {
    negatives.push("NON_PRIMARY_KPI_IN_SHELL");
  }
  if (layout.customerRenderTerritoryFilter === true) {
    negatives.push("EXTRA_BPP_TERRITORY");
  }
  if (layout.customerRenderSupplemental === true) {
    negatives.push("EXTRA_BPP_SUPPLEMENTAL");
  }
  if (statusPanelCopy) {
    negatives.push("UNAPPROVED_PEER_RANKING_PENDING_STATUS_PANEL");
  }
  if (subjectOnlyTable) {
    negatives.push("SUBJECT_ONLY_PEER_TABLE_NOT_COMPETITIVE_OVERVIEW");
  }
  if (
    rankCertified &&
    (certifiedPeerCount == null || certifiedPeerCount < 3) &&
    peerRows.length < 3
  ) {
    negatives.push("READY_RANK_WITHOUT_CERTIFIED_PEER_COHORT");
  }
  if (
    subjectMeasured &&
    rankingRows.length > 0 &&
    peerRows.length === 0
  ) {
    negatives.push("RANK_BLOCKED_MUST_USE_EMPTY_TABLE_NOT_SUBJECT_ROW");
  }

  const hardFail = [
    gates.MEASUREMENT_ARCHIVE_PASS,
    gates.BPP_METRICS_PASS,
    gates.BPP_PACK_ATTACHMENT_PASS,
    gates.BPP_STRUCTURE_PASS,
    gates.BPP_RUNTIME_VALUE_PASS,
    gates.PEER_TABLE_POPULATION_PASS,
    gates.PEER_COHORT_PASS,
    gates.RANKING_PASS,
  ].some((g) => g === "FAIL");

  return {
    gate: ADP_BPP_DATA_AND_STRUCTURE_PARITY_V1,
    propertyId,
    hotelName: profile?.name || propertyId,
    loyaltyPeriodId: loyalty?.periodId || null,
    packSource: resolved?._bppPackSource || null,
    bppCustomerState: resolved?.bppCustomerState || resolved?.status || null,
    subjectMeasured,
    rankCertified,
    componentOrderExpected: BPP_CUSTOMER_COMPONENT_ORDER_V2,
    forbiddenComponents: BPP_CUSTOMER_FORBIDDEN_COMPONENTS_V2,
    gates,
    negatives,
    pass: !hardFail && negatives.length === 0,
    forbiddenPresent,
  };
}

export function loadCloseoutReadinessMatrix(cwd = process.cwd()) {
  const p = path.join(
    cwd,
    "reports/dealality-adp-bpp-31-hotel-data-structure-closeout-v1/FINAL_BPP_READINESS_MATRIX.csv"
  );
  if (!fs.existsSync(p)) return null;
  return fs.readFileSync(p, "utf8");
}
