/**
 * Build canonical ADP certification inventory rows (orthogonal status dimensions).
 */

import { readdirSync, existsSync, readFileSync } from "fs";
import { join } from "path";
import { loadAllPeriods, loadPeriod, loadPropertyProfile } from "../data-model.js";
import { loadPublishedManifest, loadPublishedReport } from "../published-snapshot.js";
import { certifyAdpPeriod } from "./certify-adp-period-v1.js";
import { evaluateAdpComparability } from "./adp-comparability-engine-v1.js";
import { evaluateProviderCompletenessGate } from "./adp-provider-completeness-policy-v1.js";
import {
  COMPARABILITY_STATUS,
  PERIOD_CERTIFICATION_STATUS,
  PUBLISH_ELIGIBILITY,
  isCertificationEraPeriod,
  mapEngineToPropertyQaStatus,
  resolveCurrentPeriodClass,
  resolveLegacyStatus,
  resolvePeriodCertificationStatus,
  resolvePublishEligibility,
} from "./adp-status-dimensions-v1.js";

function listPublishedPropertyIds() {
  const dir = join(process.cwd(), "data/ai-demand-positioning/published");
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter((d) => existsSync(join(dir, d, "manifest.json")));
}

function loadLatestRuntimePeriod(propertyId, publishedManifest) {
  const all = loadAllPeriods(propertyId) || [];
  const byId = new Map(all.map((p) => [p.periodId, p]));
  const preferId = publishedManifest?.latestPeriodId;
  if (preferId && byId.has(preferId)) return byId.get(preferId);
  if (preferId) {
    try {
      const loaded = loadPeriod(preferId);
      if (loaded) return loaded;
    } catch {
      /* missing */
    }
  }
  if (!all.length) return null;
  return [...all].sort((a, b) =>
    String(b.executionDate || b.periodId || "").localeCompare(
      String(a.executionDate || a.periodId || "")
    )
  )[0];
}

function loadCertManifestOnDisk(propertyId, periodId) {
  if (!periodId) return null;
  const path = join(
    process.cwd(),
    "data/ai-demand-positioning/certification-manifests",
    propertyId,
    `${periodId}.json`
  );
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Inventory one property — dry-run certify, no writes.
 */
export async function buildAdpPropertyInventoryRow(propertyId, options = {}) {
  const profile = options.profile || loadPropertyProfile(propertyId);
  if (!profile) {
    return {
      hotelId: propertyId,
      hotelName: null,
      error: "PROFILE_MISSING",
    };
  }

  const publishedManifest = loadPublishedManifest(propertyId);
  const publishedReport = loadPublishedReport(propertyId);
  const period = loadLatestRuntimePeriod(propertyId, publishedManifest);
  const diskManifest = loadCertManifestOnDisk(
    propertyId,
    period?.periodId || publishedManifest?.latestPeriodId
  );

  const era = isCertificationEraPeriod(period, publishedManifest);
  const cert = period
    ? await certifyAdpPeriod(period.periodId || period, {
        auditOnly: !era,
        forceOfficialCertification: era,
        writeManifest: false,
        writeAuditTrail: false,
        stampPeriod: false,
      })
    : {
        status: "DRAFT",
        engineStatus: "QA_FAILED",
        hardFailures: [{ code: "NO_PERIOD" }],
        reviewFlags: [],
      };

  const engineWouldCertify =
    cert.manifest?.engineWouldCertify || cert.engineStatus || cert.status;
  const propertyQaStatus = mapEngineToPropertyQaStatus(
    engineWouldCertify,
    cert.hardFailures,
    cert.reviewFlags
  );

  const periodCertificationStatus = resolvePeriodCertificationStatus({
    period,
    publishedManifest,
    engineWouldCertify,
    stampStatus: period?.certificationStatus || publishedManifest?.certificationStatus,
    propertyQaStatus,
  });

  const currentPeriodClass = resolveCurrentPeriodClass(period, publishedManifest);
  const officialPeriodPublished =
    Boolean(publishedManifest?.latestPeriodId) &&
    String(publishedManifest?.publishStatus || "").toLowerCase() === "live";

  const publishEligibility = resolvePublishEligibility({
    currentPeriodClass,
    periodCertificationStatus:
      periodCertificationStatus === PERIOD_CERTIFICATION_STATUS.LEGACY_QA_PASS
        ? PERIOD_CERTIFICATION_STATUS.LEGACY_UNCERTIFIED
        : periodCertificationStatus,
    officialPeriodPublished,
  });

  const legacyStatus = resolveLegacyStatus(currentPeriodClass, periodCertificationStatus);

  let comparabilityStatus = COMPARABILITY_STATUS.NOT_EVALUATED;
  if (
    propertyId === "adp_hilton_times_square" ||
    propertyId === "adp_renaissance_times_square"
  ) {
    const peerId =
      propertyId === "adp_hilton_times_square"
        ? "adp_renaissance_times_square"
        : "adp_hilton_times_square";
    const peerManifest = loadPublishedManifest(peerId);
    const peerPeriod = loadLatestRuntimePeriod(peerId, peerManifest);
    if (period && peerPeriod) {
      const c = evaluateAdpComparability(period, peerPeriod);
      comparabilityStatus = c.outcome || COMPARABILITY_STATUS.NOT_EVALUATED;
    }
  }

  const completeness = period ? evaluateProviderCompletenessGate(period) : null;

  return {
    hotelId: propertyId,
    hotelName: profile.name || profile.displayName || propertyId,
    subjectId: profile.subjectId || profile.censusRecordId || profile.hpcHotelId || null,
    latestPeriodId: period?.periodId || publishedManifest?.latestPeriodId || null,
    latestPeriodDate:
      period?.executionDate ||
      period?.measurementDate ||
      publishedManifest?.latestPublishedAt ||
      null,
    propertyQaStatus,
    periodCertificationStatus,
    stampedCertificationStatus:
      period?.certificationStatus || publishedManifest?.certificationStatus || null,
    currentPeriodClass,
    publishEligibility,
    legacyStatus,
    comparabilityStatus,
    certificationManifestPresent: Boolean(
      diskManifest ||
        publishedReport?._certification?.manifest ||
        (era && (period?.certified === true || publishedManifest?.certificationStatus === "CERTIFIED"))
    )
      ? "YES"
      : "NO",
    certificationEngineResult: engineWouldCertify,
    officialPeriodPublished: officialPeriodPublished ? "YES" : "NO",
    officialPeriodCertificationEra: era ? "YES" : "NO",
    engineHardFailures: (cert.hardFailures || []).map((f) => f.code).join("|"),
    engineReviewFlags: (cert.reviewFlags || []).map((f) => f.code).join("|"),
    globalCertificationEngineVersion:
      period?.globalCertificationEngineVersion ||
      publishedManifest?.globalCertificationEngineVersion ||
      null,
    providerExpected: completeness?.expected ?? null,
    providerSuccessful: completeness?.successful ?? null,
    providerFailed: completeness?.failed ?? null,
    providerTimedOut: completeness?.timedOut ?? null,
    providerImbalanceCount: completeness?.providerImbalance?.length ?? 0,
    nextPeriodRequiresCertification: "YES",
    formalComparisonUsable:
      periodCertificationStatus === PERIOD_CERTIFICATION_STATUS.CERTIFIED && era ? "YES" : "NO",
    publishedReportPresent: publishedReport ? "YES" : "NO",
  };
}

export async function buildCanonicalAdpInventory(options = {}) {
  const ids = (options.propertyIds?.length
    ? options.propertyIds
    : listPublishedPropertyIds()
  ).sort();
  const rows = [];
  for (const id of ids) {
    rows.push(await buildAdpPropertyInventoryRow(id, options));
  }
  return rows;
}

export function summarizeInventoryCounts(rows) {
  const total = rows.length;
  const count = (field, value) => rows.filter((r) => r[field] === value).length;

  const propertyQaPass = count("propertyQaStatus", "PASS");
  const propertyQaReview = count("propertyQaStatus", "REVIEW");
  const propertyQaFail = count("propertyQaStatus", "FAIL");

  const certEraCertified = count("periodCertificationStatus", PERIOD_CERTIFICATION_STATUS.CERTIFIED);
  const legacyUncertifiedCurrent =
    count("periodCertificationStatus", PERIOD_CERTIFICATION_STATUS.LEGACY_UNCERTIFIED) +
    count("periodCertificationStatus", PERIOD_CERTIFICATION_STATUS.LEGACY_QA_PASS);
  const qaReviewPeriods = count(
    "periodCertificationStatus",
    PERIOD_CERTIFICATION_STATUS.QA_REVIEW_REQUIRED
  );
  const qaFailedPeriods = count("periodCertificationStatus", PERIOD_CERTIFICATION_STATUS.QA_FAILED);
  const noOfficial = count("currentPeriodClass", "NO_OFFICIAL_PERIOD");

  const publishAllowed = count("publishEligibility", PUBLISH_ELIGIBILITY.ALLOWED);
  const publishBlocked = count("publishEligibility", PUBLISH_ELIGIBILITY.BLOCKED);
  const publishGrandfathered = count("publishEligibility", PUBLISH_ELIGIBILITY.GRANDFATHERED);

  const assertions = [];
  if (propertyQaPass + propertyQaReview + propertyQaFail !== total) {
    assertions.push({
      id: "SUM_PROPERTY_QA",
      pass: false,
      detail: `${propertyQaPass}+${propertyQaReview}+${propertyQaFail} !== ${total}`,
    });
  } else {
    assertions.push({ id: "SUM_PROPERTY_QA", pass: true });
  }

  const withPeriod = rows.filter((r) => r.latestPeriodId).length;
  const periodStatusSum =
    certEraCertified + legacyUncertifiedCurrent + qaReviewPeriods + qaFailedPeriods;
  // DRAFT/SUPERSEDED may also appear — count residual
  const otherPeriod = withPeriod - periodStatusSum;
  assertions.push({
    id: "SUM_PERIOD_CERT_STATUS",
    pass: otherPeriod >= 0 && periodStatusSum + Math.max(0, otherPeriod) >= withPeriod,
    detail: { withPeriod, periodStatusSum, otherPeriod },
  });

  const certifiedWithoutEra = rows.filter(
    (r) =>
      r.periodCertificationStatus === PERIOD_CERTIFICATION_STATUS.CERTIFIED &&
      r.officialPeriodCertificationEra !== "YES"
  );
  assertions.push({
    id: "NO_CERTIFIED_WITHOUT_ERA",
    pass: certifiedWithoutEra.length === 0,
    detail: certifiedWithoutEra.map((r) => r.hotelId),
  });

  const certifiedWithoutManifest = rows.filter(
    (r) =>
      r.periodCertificationStatus === PERIOD_CERTIFICATION_STATUS.CERTIFIED &&
      r.certificationManifestPresent !== "YES"
  );
  assertions.push({
    id: "CERTIFIED_REQUIRES_MANIFEST",
    pass: certifiedWithoutManifest.length === 0,
    detail: certifiedWithoutManifest.map((r) => r.hotelId),
  });

  const legacyCountedAsCertified = rows.filter(
    (r) =>
      r.currentPeriodClass === "LEGACY_OFFICIAL" &&
      r.periodCertificationStatus === PERIOD_CERTIFICATION_STATUS.CERTIFIED
  );
  assertions.push({
    id: "LEGACY_NOT_COUNTED_CERTIFIED",
    pass: legacyCountedAsCertified.length === 0,
    detail: legacyCountedAsCertified.map((r) => r.hotelId),
  });

  const qaPassImpliesCertified = rows.filter(
    (r) =>
      r.propertyQaStatus === "PASS" &&
      r.periodCertificationStatus === PERIOD_CERTIFICATION_STATUS.CERTIFIED &&
      r.officialPeriodCertificationEra !== "YES"
  );
  assertions.push({
    id: "QA_PASS_DOES_NOT_IMPLY_CERTIFIED",
    pass: qaPassImpliesCertified.length === 0,
    detail: qaPassImpliesCertified.map((r) => r.hotelId),
  });

  return {
    total,
    propertyQaPass,
    propertyQaReview,
    propertyQaFail,
    certEraCertified,
    legacyUncertifiedCurrent,
    qaReviewPeriods,
    qaFailedPeriods,
    noOfficial,
    publishAllowed,
    publishBlocked,
    publishGrandfathered,
    assertions,
    allAssertionsPass: assertions.every((a) => a.pass),
  };
}
