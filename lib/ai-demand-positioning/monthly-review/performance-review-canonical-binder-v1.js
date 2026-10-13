/**
 * Canonical binder: Performance Review numbers must tie exactly to current published ADP.
 * Doctrine:
 *   PERFORMANCE_REVIEW_NUMBERS_MUST_TIE_EXACTLY_TO_CURRENT_PUBLISHED_ADP
 *   AI_DEMAND_REVIEW_NO_PARALLEL_ANALYTICS
 *   SAME_PROPERTY_SAME_PERIOD_SAME_EDITION_SAME_NUMBERS
 *   NO PDF-ONLY ANALYTICAL CALCULATION
 *
 * Only DISPLAY_TRANSFORM_ONLY is allowed (%, rounding, string formatting).
 */

export const PERFORMANCE_REVIEW_NUMBERS_MUST_TIE_EXACTLY_TO_CURRENT_PUBLISHED_ADP =
  "PERFORMANCE_REVIEW_NUMBERS_MUST_TIE_EXACTLY_TO_CURRENT_PUBLISHED_ADP";
export const AI_DEMAND_REVIEW_NO_PARALLEL_ANALYTICS =
  "AI_DEMAND_REVIEW_NO_PARALLEL_ANALYTICS";
export const AI_DEMAND_PERFORMANCE_REVIEW_SAME_EDITION_LOCK =
  "AI_DEMAND_PERFORMANCE_REVIEW_SAME_EDITION_LOCK";
export const PDF_NO_RAW_NULL_ANALYTICAL_FIELDS = "PDF_NO_RAW_NULL_ANALYTICAL_FIELDS";
export const PDF_COMPARABILITY_DISCLOSURE_PARITY =
  "PDF_COMPARABILITY_DISCLOSURE_PARITY";
export const PDF_DEMAND_TERRITORY_METRIC_SEMANTIC_PARITY =
  "PDF_DEMAND_TERRITORY_METRIC_SEMANTIC_PARITY";
export const PDF_BPP_PLATFORM_PAYLOAD_PARITY = "PDF_BPP_PLATFORM_PAYLOAD_PARITY";
export const PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY =
  "PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY";
export const PDF_CLAIM_FULL_LINEAGE = "PDF_CLAIM_FULL_LINEAGE";
export const ADP_PERFORMANCE_REVIEW_PLATFORM_PARITY_V1 =
  "ADP_PERFORMANCE_REVIEW_PLATFORM_PARITY_V1";

function round1(n) {
  if (n == null || Number.isNaN(Number(n))) return null;
  return Math.round(Number(n) * 10) / 10;
}

/**
 * Reality Coverage % from published Reality Gap object (display transform only).
 * Prefer recognizedCount/totalAttributes — never invent from observations.
 */
export function resolveCanonicalRealityCoveragePct(report) {
  const rg = report?.realityGap;
  if (
    rg &&
    typeof rg.recognizedCount === "number" &&
    typeof rg.totalAttributes === "number" &&
    rg.totalAttributes > 0
  ) {
    return round1((100 * rg.recognizedCount) / rg.totalAttributes);
  }
  if (typeof rg?.coverageRate === "number") return round1(rg.coverageRate);
  if (typeof rg?.overallRecognitionRate === "number") {
    return round1(rg.overallRecognitionRate);
  }
  if (typeof report?.executiveMetrics?.realityCoverage?.rate === "number") {
    return round1(report.executiveMetrics.realityCoverage.rate);
  }
  return null;
}

export function resolveOfficialComparablePrior(report) {
  const er = report?.executiveRead || {};
  return (
    er.COMPARABLE_PRIOR_AVAILABLE === true ||
    er.trend?.hasComparablePrior === true
  );
}

export function buildEditionLock(report, manifest, bppPackMeta = null) {
  const er = report?.executiveRead || {};
  return {
    gate: AI_DEMAND_PERFORMANCE_REVIEW_SAME_EDITION_LOCK,
    propertyId: report?.property?.propertyId || manifest?.propertyId || null,
    periodId: report?.period?.periodId || manifest?.latestPeriodId || null,
    editionId:
      er.editionId ||
      manifest?.executiveReadEditionId ||
      manifest?.reportEdition ||
      null,
    snapshotHash: er.sourceSnapshotHash || manifest?.snapshotHash || null,
    compositionHash: er.compositionHash || manifest?.executiveReadCompositionHash || null,
    compositionVersion:
      er.compositionVersion ||
      manifest?.executiveReadCompositionVersion ||
      report?.version ||
      null,
    reportEdition: manifest?.reportEdition || null,
    bppPublicationVersion: bppPackMeta?.publicationVersion || null,
    bppPeriodId: bppPackMeta?.periodId || null,
    displacementSupportSetVersion:
      manifest?.displacementSupportSetVersion || null,
    displacementEvidenceEditionId:
      manifest?.displacementEvidenceEditionId || null,
    generatedAt: new Date().toISOString(),
  };
}

/**
 * Extract platform-canonical numeric/narrative anchors for parity audits.
 */
export function extractPlatformCanonicalMetrics(report, bpp = null) {
  const em = report?.executiveMetrics || {};
  const er = report?.executiveRead || {};
  const sections = er.compositionV3?.sections || er.sections || {};
  const realityPct = resolveCanonicalRealityCoveragePct(report);
  const territories = Object.entries(report?.demandCapture?.byIntent || {}).map(
    ([intent, row]) => ({
      intent,
      metricName: "scenario_capture_rate",
      metricLabel: "Scenario Capture",
      rate: row?.rate ?? null,
      captured: row?.captured ?? null,
      total: row?.total ?? null,
      sourceField: `demandCapture.byIntent.${intent}.rate`,
    })
  );

  return {
    propertyId: report?.property?.propertyId || null,
    periodId: report?.period?.periodId || null,
    scenarioCount: report?.period?.scenarioCount ?? null,
    providerCount: report?.period?.providerCount ?? null,
    aiConsideration: em.considerationRate?.rate ?? null,
    scenarioPresence: em.scenarioPresence?.rate ?? null,
    realityCoverage: realityPct,
    realityGap: {
      recognizedCount: report?.realityGap?.recognizedCount ?? null,
      totalAttributes: report?.realityGap?.totalAttributes ?? null,
      gapScore: report?.realityGap?.gapScore ?? null,
      highSeverity: (report?.realityGap?.gaps || [])
        .filter((g) => String(g.severity || "").toUpperCase() === "HIGH")
        .map((g) => g.label || g.attribute),
    },
    rankMetrics: {
      topThreeAppearanceRate: em.rankMetrics?.topThreeAppearanceRate ?? null,
      numberOneAppearanceRate: em.rankMetrics?.numberOneAppearanceRate ?? null,
      rankEligibleN: em.rankMetrics?.rankEligibleN ?? null,
      topThreeCount: em.rankMetrics?.topThreeCount ?? null,
      numberOneCount: em.rankMetrics?.numberOneCount ?? null,
    },
    displacement: (report?.lostDemand?.displacement || []).map((d) => ({
      name: d.name,
      displacementCount: d.displacementCount,
      entityId: d.entityId || null,
    })),
    territories,
    bpp: bpp
      ? {
          status: bpp.status || null,
          kpis: (bpp.kpis || []).map((k) => ({
            id: k.id,
            value: k.value,
            valueRaw: k.valueRaw ?? null,
            meta: k.meta || null,
            available: k.available !== false,
          })),
        }
      : null,
    executiveRead: {
      primaryIssueId: er.primaryIssueId || null,
      sections,
      biggestStrength: er.summary?.biggestStrength || null,
      biggestConstraint: er.summary?.biggestConstraint || null,
      changeSinceLastComparableRun:
        er.summary?.changeSinceLastComparableRun || null,
      comparablePriorAvailable: resolveOfficialComparablePrior(report),
      numericAnchors: er.numericAnchors || [],
      editionId: er.editionId || null,
    },
    priorityActions: (report?.actions || []).map((a) => ({
      title: a.title,
      priority: a.priority,
      category: a.category,
    })),
  };
}

export function displayPct(rate, digits = 1) {
  if (rate == null || Number.isNaN(Number(rate))) return null;
  return `${Number(rate).toFixed(digits)}%`;
}

/** Never render raw null/undefined/NaN/— for customer-visible analytical fields. */
export function safeAnalyticalDisplay(value, unavailableLabel = "not published") {
  if (value == null) return unavailableLabel;
  const s = String(value).trim();
  if (!s || s === "null" || s === "undefined" || s === "NaN" || s === "—") {
    return unavailableLabel;
  }
  return s;
}
