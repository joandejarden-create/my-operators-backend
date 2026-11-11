/**
 * ADP Single-Version Publication Contract V1
 *
 * WHAT WE APPROVE LOCALLY IS EXACTLY WHAT CUSTOMERS SEE.
 * One content version · one presentation version · one publication process.
 *
 * Contract: ADP_SINGLE_VERSION_PUBLICATION_CONTRACT_V1
 *
 * Does NOT publish, deploy, or mutate durable published volume.
 */

import { createHash } from "crypto";
import { existsSync, readFileSync } from "fs";
import {
  RENDERER_ASSET_VERSION,
  COMPOSITION_CONTRACT_VERSION,
  EVIDENCE_CONTRACT_VERSION,
} from "../governance/adp-production-output-baseline-v1.js";
import { AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1 } from "../monthly-review/ai-demand-performance-review-template-v1.js";
import { ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1 } from "./adp-independent-unprompted-methodology-contract-v1.js";
import { PUBLICATION_WORKFLOW_STEPS } from "./adp-permanent-publication-workflow-v1.js";

export const ADP_SINGLE_VERSION_PUBLICATION_CONTRACT_V1 =
  "ADP_SINGLE_VERSION_PUBLICATION_CONTRACT_V1";

export const ADP_CANONICAL_REPORT_PACKAGE_V1 = "ADP_CANONICAL_REPORT_PACKAGE_V1";

export const ADP_PUBLICATION_LIFECYCLE_V1 = Object.freeze({
  DRAFT: "DRAFT",
  CERTIFIED: "CERTIFIED",
  APPROVED: "APPROVED",
  PUBLISHED: "PUBLISHED",
  VERIFIED: "VERIFIED",
});

export const ADP_REPORT_CONTENT_VERSION_V1 = "ADP_REPORT_CONTENT_V1_1_1";
export const ADP_REPORT_TEMPLATE_VERSION_V1 =
  AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1;
export const ADP_PRESENTATION_BUNDLE_VERSION_V1 = RENDERER_ASSET_VERSION;

/** Surfaces that must resolve to the same approved package. */
export const ADP_CUSTOMER_SURFACES_V1 = Object.freeze([
  "LOCAL_ADMIN",
  "PRODUCTION_ADMIN",
  "PUBLISHED_CUSTOMER_REPORT",
  "PDF_PREVIEW",
  "PDF_DOWNLOAD",
  "EXTERNAL_CUSTOMER_SHARE",
]);

export const SINGLE_VERSION_HARD_RULES = Object.freeze([
  "Approved package is the exclusive publication source",
  "Do not regenerate or reinterpret content during publication",
  "Local and production use the same report components, layout, styling, and rendering logic",
  "Historical periods retain original content + presentation versions",
  "Existing share tokens and historical PDFs are preserved",
  "Current links resolve to latest authorized current; historical links stay pinned",
  "Durable registry is authoritative under ADP_DURABLE_PUBLISHED_MODE=prefer",
  "Static deploy-pack registry must not override newer durable publication",
  "Mismatch fails closed — section-count equality is not success",
  "GDI is out of scope and must not change",
]);

function sha256Text(text) {
  return createHash("sha256").update(String(text ?? "")).digest("hex");
}

function sha256Json(value) {
  return sha256Text(stableStringify(value));
}

function sha256File(absPath) {
  return createHash("sha256").update(readFileSync(absPath)).digest("hex");
}

export function stableStringify(value) {
  if (value == null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(",")}}`;
}

function unwrap(doc) {
  if (!doc) return null;
  if (doc.payload && typeof doc.payload === "object") return doc.payload;
  return doc;
}

function round1(n) {
  if (n == null || !Number.isFinite(Number(n))) return null;
  return Math.round(Number(n) * 10) / 10;
}

/**
 * Extract the certified measurement core that must be identical across surfaces.
 */
export function extractCertifiedMeasurementCore(payload) {
  const p = unwrap(payload) || {};
  const cons = p.executiveMetrics?.considerationRate || {};
  return {
    considerationRate: round1(cons.rate),
    presentObservations: cons.presentObservations ?? null,
    comparableObservations: cons.comparableObservations ?? null,
    methodologyId:
      cons.methodologyId ||
      p.measurementMethodology?.correctedMethodologyId ||
      p.measurementMethodology?.methodologyId ||
      null,
    methodologyFingerprintSha256:
      p.measurementMethodology?.methodologyFingerprintSha256 || null,
    eligibleScenarioIdsSha256:
      p.measurementMethodology?.appearanceKpis?.eligibleScenarioIdsSha256 || null,
    periodId: p.period?.periodId || null,
  };
}

/**
 * Extract presentation/content version stamps that must travel with the package.
 */
export function extractPresentationVersions(payload = {}, overrides = {}) {
  const p = unwrap(payload) || {};
  return {
    reportContentVersion:
      overrides.reportContentVersion ||
      p._publicationPackage?.reportContentVersion ||
      ADP_REPORT_CONTENT_VERSION_V1,
    reportTemplateVersion:
      overrides.reportTemplateVersion ||
      p._publicationPackage?.reportTemplateVersion ||
      ADP_REPORT_TEMPLATE_VERSION_V1,
    presentationBundleVersion:
      overrides.presentationBundleVersion ||
      p._publicationPackage?.presentationBundleVersion ||
      ADP_PRESENTATION_BUNDLE_VERSION_V1,
    compositionContractVersion:
      overrides.compositionContractVersion ||
      p.executiveRead?.compositionVersion ||
      COMPOSITION_CONTRACT_VERSION,
    evidenceContractVersion:
      overrides.evidenceContractVersion || EVIDENCE_CONTRACT_VERSION,
  };
}

/**
 * Build an immutable canonical report package from a certified candidate.
 * Freezes content + presentation versions. Does not publish.
 */
export function buildCanonicalReportPackage({
  propertyId,
  measurementPeriodId,
  methodologyId = ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  methodologyFingerprintSha256,
  reportPayload,
  certificationStamp = null,
  pdfPath = null,
  pdfSha256 = null,
  evidenceReferences = null,
  lifecycleStatus = ADP_PUBLICATION_LIFECYCLE_V1.CERTIFIED,
  reportContentVersion = ADP_REPORT_CONTENT_VERSION_V1,
  reportTemplateVersion = ADP_REPORT_TEMPLATE_VERSION_V1,
  presentationBundleVersion = ADP_PRESENTATION_BUNDLE_VERSION_V1,
  shareTokenPreservation = { preserveExistingTokens: true },
} = {}) {
  const payload = unwrap(reportPayload);
  if (!propertyId) {
    return { ok: false, code: "PROPERTY_ID_REQUIRED" };
  }
  if (!payload) {
    return { ok: false, code: "REPORT_PAYLOAD_REQUIRED" };
  }
  const periodId =
    measurementPeriodId ||
    payload?.period?.periodId ||
    certificationStamp?.measurementPeriodId ||
    null;
  if (!periodId) {
    return { ok: false, code: "MEASUREMENT_PERIOD_REQUIRED" };
  }

  const measurementCore = extractCertifiedMeasurementCore(payload);
  const presentation = extractPresentationVersions(payload, {
    reportContentVersion,
    reportTemplateVersion,
    presentationBundleVersion,
  });

  let resolvedPdfSha = pdfSha256 || null;
  if (!resolvedPdfSha && pdfPath && existsSync(pdfPath)) {
    resolvedPdfSha = sha256File(pdfPath);
  }

  const contentFingerprint = sha256Json({
    propertyId,
    measurementPeriodId: periodId,
    methodologyId,
    methodologyFingerprintSha256,
    measurementCore,
    competitor: {
      competitiveSetCount: Array.isArray(payload?.competitiveSet?.observed)
        ? payload.competitiveSet.observed.length
        : null,
      displacementCount: Array.isArray(payload?.lostDemand?.displacement)
        ? payload.lostDemand.displacement.length
        : null,
    },
    executiveFindingsHash: sha256Json(
      payload?.executiveRead?.sections ||
        payload?.executiveRead?.compositionV3?.sections ||
        payload?.executiveRead ||
        null
    ),
    recommendationsHash: sha256Json(payload?.actions || []),
    certificationStamp: certificationStamp
      ? {
          considerationRate: certificationStamp.considerationRate,
          numerator: certificationStamp.numerator,
          denominator: certificationStamp.denominator,
          eligibleScenarioIdsSha256: certificationStamp.eligibleScenarioIdsSha256,
          certificationVerdict: certificationStamp.certificationVerdict,
        }
      : null,
    ...presentation,
  });

  const packageBody = {
    contract: ADP_CANONICAL_REPORT_PACKAGE_V1,
    singleVersionContract: ADP_SINGLE_VERSION_PUBLICATION_CONTRACT_V1,
    propertyId,
    measurementPeriodId: periodId,
    methodologyId,
    methodologyFingerprintSha256: methodologyFingerprintSha256 || null,
    certifiedMeasurement: measurementCore,
    competitorData: {
      competitiveSetObserved: payload?.competitiveSet?.observed || null,
      displacement: payload?.lostDemand?.displacement || null,
      competitiveRankingByTerritory:
        payload?.competitiveRankingByTerritory || null,
    },
    executiveFindings: payload?.executiveRead || null,
    recommendationsAndActions: payload?.actions || [],
    evidenceReferences:
      evidenceReferences ||
      certificationStamp?.sourceEvidenceReferences ||
      payload?.measurementMethodology?.sourceEvidenceReferences ||
      null,
    reportContentVersion: presentation.reportContentVersion,
    reportTemplateVersion: presentation.reportTemplateVersion,
    presentationBundleVersion: presentation.presentationBundleVersion,
    compositionContractVersion: presentation.compositionContractVersion,
    evidenceContractVersion: presentation.evidenceContractVersion,
    pdfArtifactSha256: resolvedPdfSha,
    pdfPath: pdfPath || null,
    contentFingerprintSha256: contentFingerprint,
    certificationStamp: certificationStamp || null,
    shareTokenPreservation,
    lifecycleStatus,
    surfaces: [...ADP_CUSTOMER_SURFACES_V1],
    hardRules: [...SINGLE_VERSION_HARD_RULES],
    durableMode: "prefer",
    gdiChanges: false,
    frozenAt: null,
    publicationManifestSha256: null,
    approvedAt: null,
    publishedAt: null,
    verifiedAt: null,
  };

  const publicationManifestSha256 = sha256Json({
    ...packageBody,
    // exclude mutable lifecycle timestamps from identity when still CERTIFIED
    frozenAt: null,
    approvedAt: null,
    publishedAt: null,
    verifiedAt: null,
  });
  packageBody.publicationManifestSha256 = publicationManifestSha256;

  const blockers = validateCanonicalReportPackage(packageBody);
  return {
    ok: blockers.length === 0,
    blockers,
    package: packageBody,
  };
}

/**
 * Validate required fields on a canonical package (fail closed).
 */
export function validateCanonicalReportPackage(pkg) {
  const blockers = [];
  const required = [
    "propertyId",
    "measurementPeriodId",
    "methodologyId",
    "methodologyFingerprintSha256",
    "reportContentVersion",
    "reportTemplateVersion",
    "presentationBundleVersion",
    "contentFingerprintSha256",
    "publicationManifestSha256",
  ];
  for (const k of required) {
    if (!pkg?.[k]) blockers.push({ code: "PACKAGE_FIELD_MISSING", field: k });
  }
  if (pkg?.methodologyId !== ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1) {
    blockers.push({
      code: "METHODOLOGY_NOT_INDEPENDENT_UNPROMPTED_V1",
      methodologyId: pkg?.methodologyId,
    });
  }
  if (pkg?.certifiedMeasurement?.considerationRate == null) {
    blockers.push({ code: "CONSIDERATION_MISSING" });
  }
  if (
    pkg?.lifecycleStatus === ADP_PUBLICATION_LIFECYCLE_V1.APPROVED ||
    pkg?.lifecycleStatus === ADP_PUBLICATION_LIFECYCLE_V1.PUBLISHED ||
    pkg?.lifecycleStatus === ADP_PUBLICATION_LIFECYCLE_V1.VERIFIED
  ) {
    if (!pkg?.frozenAt) blockers.push({ code: "APPROVED_PACKAGE_NOT_FROZEN" });
  }
  return blockers;
}

/**
 * Freeze package at APPROVED — content + presentation locked.
 * Publication must promote this exact package without regeneration.
 */
export function approveCanonicalReportPackage(pkg, { approvedBy = "FOUNDER" } = {}) {
  const blockers = validateCanonicalReportPackage(pkg);
  if (blockers.length) {
    return { ok: false, blockers, package: pkg };
  }
  if (
    pkg.lifecycleStatus !== ADP_PUBLICATION_LIFECYCLE_V1.CERTIFIED &&
    pkg.lifecycleStatus !== ADP_PUBLICATION_LIFECYCLE_V1.APPROVED
  ) {
    return {
      ok: false,
      blockers: [
        {
          code: "INVALID_LIFECYCLE_FOR_APPROVAL",
          status: pkg.lifecycleStatus,
        },
      ],
      package: pkg,
    };
  }
  const frozen = {
    ...pkg,
    lifecycleStatus: ADP_PUBLICATION_LIFECYCLE_V1.APPROVED,
    frozenAt: pkg.frozenAt || new Date().toISOString(),
    approvedAt: new Date().toISOString(),
    approvedBy,
  };
  // Recompute manifest including freeze identity
  frozen.publicationManifestSha256 = sha256Json({
    propertyId: frozen.propertyId,
    measurementPeriodId: frozen.measurementPeriodId,
    methodologyId: frozen.methodologyId,
    methodologyFingerprintSha256: frozen.methodologyFingerprintSha256,
    contentFingerprintSha256: frozen.contentFingerprintSha256,
    reportContentVersion: frozen.reportContentVersion,
    reportTemplateVersion: frozen.reportTemplateVersion,
    presentationBundleVersion: frozen.presentationBundleVersion,
    pdfArtifactSha256: frozen.pdfArtifactSha256,
    certifiedMeasurement: frozen.certifiedMeasurement,
    frozenAt: frozen.frozenAt,
  });
  return { ok: true, blockers: [], package: frozen };
}

/**
 * Compare two surfaces against an approved package. Fail closed on mismatch.
 */
export function compareSurfaceToApprovedPackage({
  surfaceName,
  surfacePayload,
  approvedPackage,
  surfacePdfSha256 = null,
  surfaceTemplateVersion = null,
  surfacePresentationBundleVersion = null,
  surfacePeriodId = null,
  tolerancePp = 0.15,
} = {}) {
  const mismatches = [];
  const pkg = approvedPackage;
  if (!pkg || pkg.lifecycleStatus === ADP_PUBLICATION_LIFECYCLE_V1.DRAFT) {
    return {
      ok: false,
      surfaceName,
      mismatches: [{ code: "NO_APPROVED_PACKAGE" }],
    };
  }

  const core = extractCertifiedMeasurementCore(surfacePayload);
  if (surfacePeriodId && surfacePeriodId !== pkg.measurementPeriodId) {
    mismatches.push({
      code: "PERIOD_MISMATCH",
      expected: pkg.measurementPeriodId,
      actual: surfacePeriodId,
    });
  }
  if (
    core.considerationRate != null &&
    pkg.certifiedMeasurement?.considerationRate != null &&
    Math.abs(core.considerationRate - pkg.certifiedMeasurement.considerationRate) >
      tolerancePp
  ) {
    mismatches.push({
      code: "CONSIDERATION_MISMATCH",
      expected: pkg.certifiedMeasurement.considerationRate,
      actual: core.considerationRate,
    });
  }
  if (
    core.presentObservations != null &&
    pkg.certifiedMeasurement?.presentObservations != null &&
    Number(core.presentObservations) !==
      Number(pkg.certifiedMeasurement.presentObservations)
  ) {
    mismatches.push({
      code: "NUMERATOR_MISMATCH",
      expected: pkg.certifiedMeasurement.presentObservations,
      actual: core.presentObservations,
    });
  }
  if (
    core.comparableObservations != null &&
    pkg.certifiedMeasurement?.comparableObservations != null &&
    Number(core.comparableObservations) !==
      Number(pkg.certifiedMeasurement.comparableObservations)
  ) {
    mismatches.push({
      code: "DENOMINATOR_MISMATCH",
      expected: pkg.certifiedMeasurement.comparableObservations,
      actual: core.comparableObservations,
    });
  }

  const surfaceVersions = extractPresentationVersions(surfacePayload, {
    reportTemplateVersion: surfaceTemplateVersion,
    presentationBundleVersion: surfacePresentationBundleVersion,
  });
  if (surfaceVersions.reportTemplateVersion !== pkg.reportTemplateVersion) {
    mismatches.push({
      code: "TEMPLATE_VERSION_MISMATCH",
      expected: pkg.reportTemplateVersion,
      actual: surfaceVersions.reportTemplateVersion,
    });
  }
  if (
    surfaceVersions.presentationBundleVersion !== pkg.presentationBundleVersion
  ) {
    mismatches.push({
      code: "PRESENTATION_BUNDLE_MISMATCH",
      expected: pkg.presentationBundleVersion,
      actual: surfaceVersions.presentationBundleVersion,
    });
  }
  if (
    surfaceVersions.reportContentVersion !== pkg.reportContentVersion
  ) {
    mismatches.push({
      code: "CONTENT_VERSION_MISMATCH",
      expected: pkg.reportContentVersion,
      actual: surfaceVersions.reportContentVersion,
    });
  }

  if (pkg.pdfArtifactSha256 && surfacePdfSha256) {
    if (pkg.pdfArtifactSha256 !== surfacePdfSha256) {
      mismatches.push({
        code: "PDF_SHA_MISMATCH",
        expected: pkg.pdfArtifactSha256,
        actual: surfacePdfSha256,
      });
    }
  }

  // Semantic parity: executive + actions fingerprints
  const surfaceExecHash = sha256Json(
    unwrap(surfacePayload)?.executiveRead?.sections ||
      unwrap(surfacePayload)?.executiveRead?.compositionV3?.sections ||
      unwrap(surfacePayload)?.executiveRead ||
      null
  );
  const pkgExecHash = sha256Json(
    pkg.executiveFindings?.sections ||
      pkg.executiveFindings?.compositionV3?.sections ||
      pkg.executiveFindings ||
      null
  );
  if (surfaceExecHash !== pkgExecHash) {
    mismatches.push({
      code: "EXECUTIVE_FINDINGS_MISMATCH",
      expected: pkgExecHash.slice(0, 16),
      actual: surfaceExecHash.slice(0, 16),
    });
  }
  const surfaceActionsHash = sha256Json(unwrap(surfacePayload)?.actions || []);
  const pkgActionsHash = sha256Json(pkg.recommendationsAndActions || []);
  if (surfaceActionsHash !== pkgActionsHash) {
    mismatches.push({
      code: "RECOMMENDATIONS_MISMATCH",
      expected: pkgActionsHash.slice(0, 16),
      actual: surfaceActionsHash.slice(0, 16),
    });
  }

  return {
    ok: mismatches.length === 0,
    surfaceName,
    mismatches,
    approvedManifestSha256: pkg.publicationManifestSha256,
  };
}

/**
 * Pre-publication parity across surfaces — all must match approved package.
 */
export function runPrePublicationParityGate({
  approvedPackage,
  surfaces = [],
} = {}) {
  const results = [];
  for (const s of surfaces) {
    results.push(
      compareSurfaceToApprovedPackage({
        surfaceName: s.surfaceName,
        surfacePayload: s.payload,
        approvedPackage,
        surfacePdfSha256: s.pdfSha256 || null,
        surfaceTemplateVersion: s.templateVersion || null,
        surfacePresentationBundleVersion: s.presentationBundleVersion || null,
        surfacePeriodId: s.periodId || null,
      })
    );
  }
  const ok = results.every((r) => r.ok);
  return {
    contract: "ADP_PRE_PUBLICATION_PARITY_GATE_V1",
    ok,
    failClosed: !ok,
    results,
    blockers: results.flatMap((r) =>
      (r.mismatches || []).map((m) => ({ surface: r.surfaceName, ...m }))
    ),
  };
}

/**
 * Assert publication would promote the exact approved package (no regeneration).
 */
export function assertPublicationPromotesExactPackage({
  approvedPackage,
  candidateToPublish,
} = {}) {
  if (!approvedPackage?.publicationManifestSha256) {
    return { ok: false, code: "APPROVED_MANIFEST_MISSING" };
  }
  if (
    approvedPackage.lifecycleStatus !== ADP_PUBLICATION_LIFECYCLE_V1.APPROVED &&
    approvedPackage.lifecycleStatus !== ADP_PUBLICATION_LIFECYCLE_V1.PUBLISHED
  ) {
    return {
      ok: false,
      code: "PACKAGE_NOT_APPROVED",
      status: approvedPackage.lifecycleStatus,
    };
  }
  const candManifest =
    candidateToPublish?.publicationManifestSha256 ||
    candidateToPublish?.package?.publicationManifestSha256;
  if (!candManifest) {
    return { ok: false, code: "CANDIDATE_MANIFEST_MISSING" };
  }
  if (candManifest !== approvedPackage.publicationManifestSha256) {
    return {
      ok: false,
      code: "MANIFEST_DRIFT_REGENERATION_FORBIDDEN",
      approved: approvedPackage.publicationManifestSha256,
      candidate: candManifest,
    };
  }
  return {
    ok: true,
    code: "EXACT_PACKAGE_PROMOTION",
    publicationManifestSha256: approvedPackage.publicationManifestSha256,
  };
}

/**
 * Historical vs current resolution rules.
 */
export function resolvePeriodLinkPolicy({
  linkType, // "current" | "historical"
  requestedPeriodId = null,
  authorizedCurrentPeriodId = null,
  pinnedHistoricalPeriodId = null,
} = {}) {
  if (linkType === "historical") {
    const periodId = pinnedHistoricalPeriodId || requestedPeriodId;
    if (!periodId) {
      return { ok: false, code: "HISTORICAL_PERIOD_REQUIRED" };
    }
    return {
      ok: true,
      resolvesToPeriodId: periodId,
      pinned: true,
      mayNotSilentlyOverwrite: true,
    };
  }
  if (linkType === "current") {
    if (!authorizedCurrentPeriodId) {
      return { ok: false, code: "AUTHORIZED_CURRENT_MISSING" };
    }
    return {
      ok: true,
      resolvesToPeriodId: authorizedCurrentPeriodId,
      pinned: false,
      source: "durable_published_current_prefer",
    };
  }
  return { ok: false, code: "UNKNOWN_LINK_TYPE" };
}

export function getSingleVersionPublicationContractDescription() {
  return {
    contract: ADP_SINGLE_VERSION_PUBLICATION_CONTRACT_V1,
    packageContract: ADP_CANONICAL_REPORT_PACKAGE_V1,
    lifecycle: Object.values(ADP_PUBLICATION_LIFECYCLE_V1),
    workflowSteps: [...PUBLICATION_WORKFLOW_STEPS],
    surfaces: [...ADP_CUSTOMER_SURFACES_V1],
    hardRules: [...SINGLE_VERSION_HARD_RULES],
    reportContentVersion: ADP_REPORT_CONTENT_VERSION_V1,
    reportTemplateVersion: ADP_REPORT_TEMPLATE_VERSION_V1,
    presentationBundleVersion: ADP_PRESENTATION_BUNDLE_VERSION_V1,
    durableMode: "prefer",
    durableAuthority:
      "Production durable published-current registry is authoritative; static deploy-pack must not override newer durable publication",
    gdiChanges: false,
    founderObjective:
      "WHAT WE APPROVE LOCALLY IS EXACTLY WHAT CUSTOMERS SEE",
  };
}
