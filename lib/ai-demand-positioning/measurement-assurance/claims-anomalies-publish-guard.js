/**
 * Customer claim assurance + anomaly detection + publish guard.
 */

import { CERTIFICATION_OUTCOMES, PIPELINE_STATES } from "./version.js";

const CAUSAL_UPLIFT =
  /would increase demand capture|will increase demand capture|would increase (your )?consideration|would lift (demand|consideration|presence)/i;

/**
 * Classify substantive customer strings in a payload.
 */
export function auditCustomerClaims(payload) {
  const findings = [];
  const walk = (node, path) => {
    if (node == null) return;
    if (typeof node === "string") {
      if (node.length < 24) return;
      let kind = "DIRECT_OBSERVATION";
      if (/review|consider improving|before estimating|analyze their/i.test(node)) {
        kind = "REVIEW_PROMPT";
      } else if (/rate|share of|appears in|captured/i.test(node)) {
        kind = "CALCULATED_FACT";
      } else if (/could|may help|opportunity/i.test(node)) {
        kind = "GOVERNED_INFERENCE";
      }
      if (CAUSAL_UPLIFT.test(node)) {
        kind = "UNSUPPORTED_CLAIM";
      }
      if (kind === "UNSUPPORTED_CLAIM" || kind === "REVIEW_PROMPT" || path.includes("actions")) {
        findings.push({ path: path.join("."), text: node.slice(0, 240), kind });
      }
      return;
    }
    if (Array.isArray(node)) {
      node.forEach((v, i) => walk(v, path.concat(String(i))));
      return;
    }
    if (typeof node === "object") {
      for (const [k, v] of Object.entries(node)) walk(v, path.concat(k));
    }
  };
  walk(payload, []);
  const unsupported = findings.filter((f) => f.kind === "UNSUPPORTED_CLAIM");
  return {
    findings: findings.slice(0, 100),
    unsupportedCount: unsupported.length,
    status: unsupported.length ? "FAIL" : "PASS",
  };
}

export function detectAnomalies({ ledger, reference, production, providers }) {
  const flags = [];
  if (ledger?.dualPathActive) {
    flags.push({
      code: "DUAL_SUBJECT_PRESENCE_PATH",
      severity: "MATERIAL",
      requiresReview: true,
      detail: `enrich flips=${ledger.enrichPathFlips}`,
    });
  }
  if (ledger?.falsePositives > 0) {
    flags.push({
      code: "SUBJECT_FALSE_POSITIVES",
      severity: ledger.falsePositives >= 10 ? "MATERIAL" : "REVIEW",
      requiresReview: true,
      detail: `count=${ledger.falsePositives}`,
    });
  }
  if (ledger?.falseNegatives > 0) {
    flags.push({
      code: "SUBJECT_FALSE_NEGATIVES",
      severity: ledger.falseNegatives >= 5 ? "MATERIAL" : "REVIEW",
      requiresReview: true,
      detail: `count=${ledger.falseNegatives}`,
    });
  }

  const prov = providers || reference?.providers || [];
  const rates = prov.map((p) => p.presence).filter((x) => x != null);
  if (rates.length >= 2) {
    const max = Math.max(...rates);
    const min = Math.min(...rates);
    if (max - min >= 30) {
      flags.push({
        code: "PROVIDER_SPREAD_GT_30PP",
        severity: "REVIEW",
        requiresReview: true,
        detail: `max=${max} min=${min}`,
      });
    }
  }
  for (const p of prov) {
    if (p.comparable > 0 && p.presence === 0 && (reference?.considerationRate || 0) > 10) {
      flags.push({
        code: "PROVIDER_NEAR_ZERO",
        severity: "REVIEW",
        requiresReview: true,
        detail: p.provider,
      });
    }
    if (p.mentioned > p.comparable) {
      flags.push({
        code: "NUMERATOR_GT_DENOMINATOR",
        severity: "MATERIAL",
        requiresReview: true,
        detail: p.provider,
      });
    }
  }

  if (
    production?.demandCapture != null &&
    reference?.demandCapture != null &&
    Math.abs(production.demandCapture - reference.demandCapture) >= 5
  ) {
    flags.push({
      code: "PRODUCTION_REFERENCE_DEMAND_CAPTURE_DELTA",
      severity: "MATERIAL",
      requiresReview: true,
      detail: `prod=${production.demandCapture} ref=${reference.demandCapture}`,
    });
  }
  if (
    production?.considerationRate != null &&
    reference?.considerationRate != null &&
    Math.abs(production.considerationRate - reference.considerationRate) >= 5
  ) {
    flags.push({
      code: "PRODUCTION_REFERENCE_CONSIDERATION_DELTA",
      severity: "MATERIAL",
      requiresReview: true,
      detail: `prod=${production.considerationRate} ref=${reference.considerationRate}`,
    });
  }

  return flags;
}

/**
 * Hard publish guard — refuses ordinary publish without certification.
 */
export function assertPublishAllowed({
  certificationStatus,
  allowCertifiedWithDisclosures = false,
  emergencyOverride = null,
}) {
  const ok =
    certificationStatus === CERTIFICATION_OUTCOMES.CERTIFIED ||
    (allowCertifiedWithDisclosures &&
      certificationStatus === CERTIFICATION_OUTCOMES.CERTIFIED_WITH_DISCLOSURES);

  if (ok) {
    return {
      allowed: true,
      pipelineState: PIPELINE_STATES.PUBLISHED,
      reason: null,
    };
  }

  if (emergencyOverride?.reason && emergencyOverride?.actor) {
    return {
      allowed: true,
      pipelineState: PIPELINE_STATES.PUBLISHED,
      reason: "EMERGENCY_OVERRIDE",
      override: {
        actor: emergencyOverride.actor,
        reason: emergencyOverride.reason,
        at: new Date().toISOString(),
      },
    };
  }

  return {
    allowed: false,
    pipelineState: PIPELINE_STATES.NOT_CERTIFIED,
    reason: `publishExistingHotelAdp refused: certificationStatus=${certificationStatus}`,
  };
}

export function publishExistingHotelAdp(bundle, certificationRecord, options = {}) {
  const gate = assertPublishAllowed({
    certificationStatus: certificationRecord?.certificationStatus,
    allowCertifiedWithDisclosures: options.allowCertifiedWithDisclosures === true,
    emergencyOverride: options.emergencyOverride || null,
  });
  if (!gate.allowed) {
    return { ok: false, error: "PUBLISH_GUARD_BLOCKED", ...gate };
  }
  return { ok: true, ...gate, bundle, certificationRecord };
}
