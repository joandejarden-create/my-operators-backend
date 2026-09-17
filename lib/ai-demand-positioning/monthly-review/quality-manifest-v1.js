/**
 * Per-review quality manifest for ADP Monthly Executive Review.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { auditMonthlyReviewLanguage } from "./monthly-review-language-audit-v1.js";
import { assertActionAccountabilityComplete } from "./action-register-schema-v1.js";
import { buildForbiddenEntitySetForProperty } from "./cross-property-isolation-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export function buildQualityManifest({ reviewId, propertyId, review, status }) {
  const language = auditMonthlyReviewLanguage(review);
  const accountability = assertActionAccountabilityComplete(review.actionAgenda || []);
  const isolation = buildForbiddenEntitySetForProperty(propertyId, review);
  const leakageHits = isolation.scanReviewText(review);

  const errors = [];
  const warnings = [...(review.eligibility?.warnings || [])];

  if (!review.gates?.MONTHLY_REVIEW_SINGLE_CANONICAL_PAYLOAD) {
    errors.push("MONTHLY_REVIEW_SINGLE_CANONICAL_PAYLOAD");
  }
  if (!language.ok) errors.push("MONTHLY_REVIEW_CLEAR_NOT_CLEVER_LANGUAGE");
  if (!accountability.ok && (review.actionAgenda || []).length) {
    errors.push("MONTHLY_REVIEW_ACTION_ACCOUNTABILITY_COMPLETE");
  }
  if (leakageHits.length) {
    errors.push("ADP_MONTHLY_REVIEW_NO_CROSS_PROPERTY_ENTITY_LEAKAGE");
  }
  if (review.actionIntelligence?.builderMode !== "generic_evidence_driven_v1") {
    errors.push("ADP_MONTHLY_REVIEW_NO_PROPERTY_SPECIFIC_BUILDER_FORKS");
  }

  const payloadIntegrity = Boolean(review.schema && review.property?.propertyId === propertyId);
  if (!payloadIntegrity) errors.push("payloadIntegrity");

  const finalStatus = errors.length ? "BLOCKED" : status || "READY_FOR_REVIEW";

  return {
    schema: "ADP_MONTHLY_REVIEW_QUALITY_MANIFEST_V1",
    reviewId,
    propertyId,
    payloadIntegrity,
    analyticalReconciliation: review.gates?.MONTHLY_REVIEW_COMPETITOR_EVIDENCE_TRACE_INTEGRITY ?? null,
    copyAudit: language.ok,
    actionQuality: accountability.ok || (review.actionAgenda || []).length === 0,
    visualIntegrity: null,
    pdfIntegrity: null,
    crossPropertyIsolation: leakageHits.length === 0,
    leakageHits,
    warnings,
    errors,
    finalStatus,
    liveProviderCalls: 0,
    gates: {
      ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED:
        review.gates?.ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED === true,
      ADP_MONTHLY_REVIEW_NO_PROPERTY_SPECIFIC_BUILDER_FORKS:
        review.gates?.ADP_MONTHLY_REVIEW_NO_PROPERTY_SPECIFIC_BUILDER_FORKS === true,
      ADP_MONTHLY_REVIEW_EXECUTIVE_ASSESSMENT_PROPERTY_SPECIFIC:
        review.gates?.ADP_MONTHLY_REVIEW_EXECUTIVE_ASSESSMENT_PROPERTY_SPECIFIC === true,
      ADP_MONTHLY_REVIEW_CROSS_PROPERTY_ISOLATION: leakageHits.length === 0,
      MONTHLY_REVIEW_CLEAR_NOT_CLEVER_LANGUAGE: language.ok,
    },
  };
}

export function writeQualityManifestForReview({ reviewId, propertyId, review, status }) {
  const manifest = buildQualityManifest({ reviewId, propertyId, review, status });
  const dir = path.join(
    ROOT,
    "reports/ai-demand-positioning/monthly-review/quality-manifests"
  );
  fs.mkdirSync(dir, { recursive: true });
  const filePath = path.join(dir, `${reviewId || propertyId}-quality-manifest.json`);
  fs.writeFileSync(filePath, `${JSON.stringify(manifest, null, 2)}\n`);
  return { ...manifest, path: filePath };
}
