/**
 * Immutable fingerprint for corrected Independent Unprompted V1 methodology.
 *
 * Contract: ADP_INDEPENDENT_UNPROMPTED_V1_METHODOLOGY_FINGERPRINT_V1_1_1
 *
 * Any change to eligibility/attribution/calculation source that alters this
 * fingerprint invalidates prior certifications until recertification.
 */

import { createHash } from "crypto";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  ADP_CONSIDERATION_REPRODUCTION_TOLERANCE_PP,
} from "./adp-independent-unprompted-methodology-contract-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "../../..");

export const ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION = "1.1.1";

export const ADP_INDEPENDENT_UNPROMPTED_V1_METHODOLOGY_FINGERPRINT_ID =
  "ADP_INDEPENDENT_UNPROMPTED_V1_METHODOLOGY_FINGERPRINT_V1_1_1";

/** Semantic rules freeze — descriptive SoT hashed into the fingerprint. */
export const CORRECTED_V1_METHODOLOGY_RULES = Object.freeze({
  methodologyId: ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  contractVersion: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION,
  scenarioEligibility: {
    include: ["INDEPENDENT_UNPROMPTED"],
    exclude: [
      "SUBJECT_PROMPTED",
      "COMPETITIVE_COMPARISON",
      "ATTRIBUTE_VERIFICATION",
      "AMBIGUOUS_REVIEW_REQUIRED",
      "OTHER_BRAND_SPECIFIC_PROMPT",
      "NOT_ACCOMMODATION_NEED",
    ],
  },
  geographicIdentityHandling: {
    rule: "City/market/submarket/region and trailing location tokens in brand+location hotel names are geography, not subject contamination",
    destinationPromptsAllowed: true,
    locationSuffixAbsorption: "TRAILING_NON_BRAND_NAME_TOKENS_AFTER_BRAND_TOKEN → geoBlock",
  },
  hotelAndBrandContamination: {
    subjectNameAndAliases: true,
    affiliationAndBrandStems: true,
    loyaltyStems: ["Bonvoy", "Honors"],
    leadingAssistedLanguageWithContamination: "EXCLUDE",
  },
  competitiveComparisonExclusions: {
    vsVersusComparedToWithSubject: true,
    bareCompareHotelsPrompts: true,
  },
  accommodationNeed: {
    required: true,
    venueOnlyWithoutStay: "EXCLUDE",
  },
  providerObservationAttribution: {
    grain: "OBSERVATION_RESPONSE",
    numerator:
      "detectPropertyMention(originalResponse|rawResponse, subjectProperty)=true on eligible observations",
    mentionDetector: "detectPropertyMention",
    runtimeMentionFlags: "NON_AUTHORITATIVE",
    certificationRequiresResponseTextAdjudication: true,
    failClosedMissingResponseText: true,
    exclude: [
      "other_hotels",
      "same_brand_different_property",
      "generic_brand_mentions",
      "similar_name_collisions",
      "geography_without_hotel_identity",
      "prompted_appearances",
    ],
  },
  numeratorDenominator: {
    numerator: "comparable eligible observations with verified subject-hotel appearance",
    denominator: "comparable parsed observations whose scenarioId ∈ frozen eligibleScenarioIds",
    rate: "round to 1 decimal (numer/denom*100)",
    tolerancePp: ADP_CONSIDERATION_REPRODUCTION_TOLERANCE_PP,
  },
  otherBrandSpecificPrompts: "EXCLUDE_FROM_SUBJECT_UNPROMPTED_DENOM",
});

/** Source files that define executable methodology behavior. */
export const METHODOLOGY_FINGERPRINT_SOURCE_FILES = Object.freeze([
  "lib/ai-demand-positioning/certification/adp-unprompted-independent-discovery-standard-v1.js",
  "lib/ai-demand-positioning/certification/adp-foundational-engine-and-metrics-certification-v1.js",
  "lib/ai-demand-positioning/certification/adp-independent-unprompted-methodology-contract-v1.js",
  "lib/ai-demand-positioning/certification/adp-independent-unprompted-publication-cert-v1.js",
  "lib/ai-demand-positioning/certification/adp-historical-eligibility-recovery-v1.js",
  "lib/ai-demand-positioning/execution/response-parser.js",
]);

function sha256Text(text) {
  return createHash("sha256").update(String(text)).digest("hex");
}

/**
 * Compute the immutable methodology fingerprint for the current working tree.
 */
export function computeIndependentUnpromptedV1MethodologyFingerprint({
  cwd = ROOT,
} = {}) {
  const sourceFileSha256 = {};
  for (const rel of METHODOLOGY_FINGERPRINT_SOURCE_FILES) {
    sourceFileSha256[rel] = createHash("sha256")
      .update(readFileSync(join(cwd, rel)))
      .digest("hex");
  }
  const rulesSha256 = sha256Text(JSON.stringify(CORRECTED_V1_METHODOLOGY_RULES));
  const sourcesSha256 = sha256Text(JSON.stringify(sourceFileSha256));
  const fingerprintSha256 = sha256Text(
    JSON.stringify({
      id: ADP_INDEPENDENT_UNPROMPTED_V1_METHODOLOGY_FINGERPRINT_ID,
      contractVersion: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION,
      methodologyId: ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
      rulesSha256,
      sourcesSha256,
    })
  );

  return Object.freeze({
    fingerprintId: ADP_INDEPENDENT_UNPROMPTED_V1_METHODOLOGY_FINGERPRINT_ID,
    methodologyId: ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
    contractVersion: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION,
    fingerprintSha256,
    rulesSha256,
    sourcesSha256,
    rules: CORRECTED_V1_METHODOLOGY_RULES,
    sourceFileSha256,
    computedAt: new Date().toISOString(),
    note: "Changing any fingerprinted source file or rules object changes fingerprintSha256 and requires recertification of affected baselines.",
    priorInvalidatedFingerprints: [
      {
        fingerprintId: "ADP_INDEPENDENT_UNPROMPTED_V1_METHODOLOGY_FINGERPRINT_V1_1_0",
        fingerprintSha256:
          "1a8cad25140fe48e4d780e241145a0cefa37df0d63495b829cc525e5582ffa80",
        reason:
          "Trusted runtime mentioned flags for Consideration numerator without required response-text adjudication",
      },
    ],
  });
}

/** Frozen expected fingerprint for CI — regenerated only with intentional methodology bump. */
let _cached = null;
export function getFrozenIndependentUnpromptedV1MethodologyFingerprint() {
  if (!_cached) _cached = computeIndependentUnpromptedV1MethodologyFingerprint();
  return _cached;
}

export function assertMethodologyFingerprintMatch(expectedSha256, { cwd = ROOT } = {}) {
  const live = computeIndependentUnpromptedV1MethodologyFingerprint({ cwd });
  if (String(expectedSha256 || "") !== live.fingerprintSha256) {
    const err = new Error(
      `METHODOLOGY_FINGERPRINT_MISMATCH: expected ${expectedSha256}, got ${live.fingerprintSha256}`
    );
    err.code = "METHODOLOGY_FINGERPRINT_MISMATCH";
    err.expected = expectedSha256;
    err.actual = live.fingerprintSha256;
    throw err;
  }
  return live;
}

export function stampRequiresRecertificationIfFingerprintDrifts({
  stampedFingerprintSha256,
  liveFingerprintSha256,
} = {}) {
  const stamped = String(stampedFingerprintSha256 || "").trim();
  const live = String(liveFingerprintSha256 || "").trim();
  if (!stamped) {
    return {
      ok: false,
      code: "FINGERPRINT_MISSING_ON_CERTIFICATION",
      requiresRecertification: true,
    };
  }
  if (stamped !== live) {
    return {
      ok: false,
      code: "FINGERPRINT_DRIFT_REQUIRES_RECERTIFICATION",
      requiresRecertification: true,
      stamped,
      live,
    };
  }
  return { ok: true, requiresRecertification: false, fingerprintSha256: live };
}
