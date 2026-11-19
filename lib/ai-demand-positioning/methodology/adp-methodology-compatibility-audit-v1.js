/**
 * Hotel-by-hotel ADP methodology compatibility audit against canonical fingerprint.
 * Read-only. Does not mutate published customer reports.
 */

import { isAdpSyntheticOrTestPropertyId } from "../governance/adp-operational-registry-hygiene-v1.js";
import { buildPublishedAdpReviewCoverageV1 } from "../monthly-review/admin/published-review-coverage-v1.js";
import { loadPropertyProfile } from "../data-model.js";
import {
  ADP_METHODOLOGY_CANONICAL_V1,
  ADP_METHODOLOGY_COMPATIBILITY,
  ADP_METHODOLOGY_FINGERPRINT_CURRENT,
} from "./adp-methodology-canonical-v1.js";
import { resolveAdpMethodologyProvenance } from "./adp-methodology-provenance-v1.js";

function compareField(actual, expected) {
  if (!expected) return { match: true, actual, expected };
  if (!actual) return { match: false, actual: null, expected, gap: "missing" };
  return {
    match: String(actual) === String(expected),
    actual,
    expected,
    gap: String(actual) === String(expected) ? null : "mismatch",
  };
}

/**
 * Classify one hotel against current methodology.
 */
export function classifyAdpMethodologyCompatibility(propertyId, opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const coverageRow = opts.coverageRow || null;

  // Synthetics are excluded from live inventory; callers must not classify them
  // into the portfolio matrix.
  if (isAdpSyntheticOrTestPropertyId(propertyId)) {
    return null;
  }

  const profile = loadPropertyProfile(propertyId);
  const name =
    profile?.name ||
    coverageRow?.propertyName ||
    coverageRow?.name ||
    propertyId;

  const provenance = resolveAdpMethodologyProvenance(propertyId, {
    cwd,
    reviewId: coverageRow?.reviewId || null,
  });

  const reasons = [];
  const fieldChecks = {
    productVersion: compareField(
      provenance.productVersion,
      ADP_METHODOLOGY_FINGERPRINT_CURRENT.productVersion
    ),
    measurementContractVersion: compareField(
      provenance.measurementContractVersion,
      ADP_METHODOLOGY_FINGERPRINT_CURRENT.measurementContractVersion
    ),
    appearanceMethodologyId: compareField(
      provenance.appearanceMethodologyId,
      ADP_METHODOLOGY_FINGERPRINT_CURRENT.appearanceMethodologyId
    ),
    realityGapMethodologyId: compareField(
      provenance.realityGapMethodologyId,
      ADP_METHODOLOGY_FINGERPRINT_CURRENT.realityGapMethodologyId
    ),
    presenceIndexMethodologyId: compareField(
      provenance.presenceIndexMethodologyId,
      ADP_METHODOLOGY_FINGERPRINT_CURRENT.presenceIndexMethodologyId
    ),
    executiveCompositionVersion: compareField(
      provenance.executiveCompositionVersion,
      ADP_METHODOLOGY_FINGERPRINT_CURRENT.executiveCompositionVersion
    ),
  };

  const hasEvidence =
    provenance.hasDurableSnapshotReport ||
    provenance.evidenceSource === "published_snapshot" ||
    provenance.evidenceSource === "durable_snapshot";

  if (!hasEvidence) {
    reasons.push("no_durable_or_published_report_payload");
    return {
      hotel: name,
      propertyId,
      compatibility: ADP_METHODOLOGY_COMPATIBILITY.EVIDENCE_INSUFFICIENT,
      reasons,
      researchPeriodId: provenance.researchPeriodId,
      provenance,
      fieldChecks,
      recomputableFields: [],
      coverageStatus: coverageRow?.coverageStatus || null,
      reviewId: coverageRow?.reviewId || null,
      reportingMonth: coverageRow?.reportingMonth || null,
    };
  }

  // Historical binary RG without dual scores and without Independent Unprompted stamp
  // is valid historically but not fully comparable.
  const rgActual = provenance.realityGapMethodologyId;
  const appearanceActual = provenance.appearanceMethodologyId;
  const mismatched = Object.entries(fieldChecks).filter(([, c]) => !c.match);
  const missingCritical = mismatched.filter(([, c]) => c.gap === "missing");
  const wrongCritical = mismatched.filter(([, c]) => c.gap === "mismatch");

  const recomputableFields = [];
  if (
    !rgActual ||
    rgActual === ADP_METHODOLOGY_FINGERPRINT_CURRENT.realityGapHistoricalImmutableId
  ) {
    // Binary V1 preserved historically; V2 dual scores may be recomputed if attributes exist.
    if (provenance.realityGapTotalAttributes > 0 || provenance.hasDualRealityGapScores) {
      recomputableFields.push("realityGapMethodologyId");
    }
  } else if (
    rgActual !== ADP_METHODOLOGY_FINGERPRINT_CURRENT.realityGapMethodologyId
  ) {
    reasons.push(`reality_gap_method=${rgActual}`);
  }

  if (!appearanceActual) {
    recomputableFields.push("appearanceMethodologyId");
  } else if (
    appearanceActual !==
    ADP_METHODOLOGY_FINGERPRINT_CURRENT.appearanceMethodologyId
  ) {
    if (
      appearanceActual ===
      "ADP_AI_CONSIDERATION_MENTION_ALL_CORE_SCENARIOS_V1"
    ) {
      recomputableFields.push("appearanceMethodologyId");
      reasons.push("appearance_legacy_mention_all_core");
    } else {
      reasons.push(`appearance_method=${appearanceActual}`);
    }
  }

  if (!provenance.executiveCompositionVersion) {
    recomputableFields.push("executiveCompositionVersion");
  } else if (
    provenance.executiveCompositionVersion !==
    ADP_METHODOLOGY_FINGERPRINT_CURRENT.executiveCompositionVersion
  ) {
    recomputableFields.push("executiveCompositionVersion");
    reasons.push(
      `composition=${provenance.executiveCompositionVersion}`
    );
  }

  if (!provenance.presenceIndexMethodologyId) {
    recomputableFields.push("presenceIndexMethodologyId");
  } else if (
    provenance.presenceIndexMethodologyId !==
    ADP_METHODOLOGY_FINGERPRINT_CURRENT.presenceIndexMethodologyId
  ) {
    recomputableFields.push("presenceIndexMethodologyId");
    reasons.push(`presence_index=${provenance.presenceIndexMethodologyId}`);
  }

  for (const [k, c] of missingCritical) {
    if (!recomputableFields.includes(k)) reasons.push(`${k}_missing`);
  }
  for (const [k, c] of wrongCritical) {
    if (!reasons.some((r) => r.includes(k) || r.includes(String(c.actual)))) {
      reasons.push(`${k}=${c.actual}`);
    }
  }

  let compatibility;
  if (mismatched.length === 0) {
    compatibility = ADP_METHODOLOGY_COMPATIBILITY.CURRENT_COMPARABLE;
    reasons.push("all_canonical_stamps_match");
  } else if (
    recomputableFields.length > 0 &&
    recomputableFields.length >= mismatched.length - 1 &&
    hasEvidence
  ) {
    // Prefer RECOMPUTABLE when evidence exists and gaps are stamp/recompute class.
    const onlyHistoricalBinary =
      rgActual ===
        ADP_METHODOLOGY_FINGERPRINT_CURRENT.realityGapHistoricalImmutableId &&
      !provenance.hasDualRealityGapScores &&
      mismatched.length >= 3;
    if (onlyHistoricalBinary && !appearanceActual) {
      compatibility = ADP_METHODOLOGY_COMPATIBILITY.HISTORICAL_ONLY;
      reasons.push("pre_v2_payload_without_dual_rg_scores");
    } else if (mismatched.length <= 3 || recomputableFields.length > 0) {
      compatibility = ADP_METHODOLOGY_COMPATIBILITY.RECOMPUTABLE;
      reasons.push("deterministic_recompute_from_existing_evidence_possible");
    } else {
      compatibility = ADP_METHODOLOGY_COMPATIBILITY.PARTIALLY_COMPARABLE;
    }
  } else if (mismatched.length > 0 && mismatched.length < Object.keys(fieldChecks).length) {
    compatibility = ADP_METHODOLOGY_COMPATIBILITY.PARTIALLY_COMPARABLE;
    reasons.push("subset_of_metrics_match_current_definitions");
  } else {
    compatibility = ADP_METHODOLOGY_COMPATIBILITY.HISTORICAL_ONLY;
    reasons.push("cannot_normalize_accurately_without_new_evidence");
  }

  // Measurement contract V1.1 is candidate — treat as comparable-enough with note, not failure.
  if (
    provenance.measurementContractVersion === "ADP_MEASUREMENT_CONTRACT_V1_1"
  ) {
    reasons.push("measurement_contract_v1_1_candidate_fleet");
    if (compatibility === ADP_METHODOLOGY_COMPATIBILITY.CURRENT_COMPARABLE) {
      /* keep */
    } else if (
      fieldChecks.measurementContractVersion &&
      !fieldChecks.measurementContractVersion.match &&
      mismatched.length === 1
    ) {
      compatibility = ADP_METHODOLOGY_COMPATIBILITY.CURRENT_COMPARABLE;
      reasons.push("v1_1_candidate_accepted_as_comparable");
    }
  }

  return {
    hotel: name,
    propertyId,
    compatibility,
    reasons: [...new Set(reasons)],
    researchPeriodId: provenance.researchPeriodId,
    researchPublishedAt: provenance.researchPublishedAt,
    provenance,
    fieldChecks,
    recomputableFields: [...new Set(recomputableFields)],
    coverageStatus: coverageRow?.coverageStatus || null,
    reviewId: coverageRow?.reviewId || null,
    pdfStatus: coverageRow?.pdfStatus || null,
    reportingMonth: coverageRow?.reportingMonth || null,
    versionLabel: coverageRow?.versionLabel || null,
    reportTemplateVersion: provenance.reportTemplateVersion || null,
    presentationBundleVersion: provenance.presentationBundleVersion || null,
  };
}

/**
 * Full portfolio audit over live published inventory (38 legitimate hotels).
 * Synthetics are never included.
 */
export function buildAdpMethodologyCompatibilityAudit(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const coverage = buildPublishedAdpReviewCoverageV1({});
  const rows = [];
  for (const covRow of coverage.properties || []) {
    if (isAdpSyntheticOrTestPropertyId(covRow.propertyId)) continue;
    const row = classifyAdpMethodologyCompatibility(covRow.propertyId, {
      cwd,
      coverageRow: covRow,
    });
    if (row) rows.push(row);
  }

  const byCompatibility = {};
  for (const r of rows) {
    byCompatibility[r.compatibility] =
      (byCompatibility[r.compatibility] || 0) + 1;
  }

  return {
    gate: ADP_METHODOLOGY_CANONICAL_V1,
    generatedAt: new Date().toISOString(),
    canonicalFingerprint: ADP_METHODOLOGY_FINGERPRINT_CURRENT,
    inventoryCount: rows.length,
    legitimateCount: rows.length,
    byCompatibility,
    comparableCount:
      byCompatibility[ADP_METHODOLOGY_COMPATIBILITY.CURRENT_COMPARABLE] || 0,
    recomputableCount:
      byCompatibility[ADP_METHODOLOGY_COMPATIBILITY.RECOMPUTABLE] || 0,
    partiallyComparableCount:
      byCompatibility[ADP_METHODOLOGY_COMPATIBILITY.PARTIALLY_COMPARABLE] || 0,
    historicalOnlyCount:
      byCompatibility[ADP_METHODOLOGY_COMPATIBILITY.HISTORICAL_ONLY] || 0,
    evidenceInsufficientCount:
      byCompatibility[ADP_METHODOLOGY_COMPATIBILITY.EVIDENCE_INSUFFICIENT] || 0,
    rows: rows.sort((a, b) => a.hotel.localeCompare(b.hotel)),
    notes: [
      "Report version labels (V1/V8/V14) are not methodology versions.",
      "RG_BINARY_GAP_V1 remains historically immutable; RG_WEIGHTED_PARTIAL_V2 is current for new staged editions.",
      "No paid research; RECOMPUTABLE means deterministic recalculation from existing evidence only.",
      "Do not silently overwrite published customer reports — stage new versions.",
      "Synthetic/smoke-test properties are excluded from live inventory discovery permanently.",
    ],
  };
}

export function renderMethodologyMatrixMarkdown(audit) {
  const lines = [
    "# ADP Methodology Compatibility Matrix",
    "",
    `Generated: ${audit.generatedAt}`,
    "",
    `Canonical: \`${ADP_METHODOLOGY_CANONICAL_V1}\``,
    "",
    "## Summary",
    "",
    `| Class | Count |`,
    `|---|---:|`,
    `| CURRENT_COMPARABLE | ${audit.comparableCount} |`,
    `| RECOMPUTABLE | ${audit.recomputableCount} |`,
    `| PARTIALLY_COMPARABLE | ${audit.partiallyComparableCount} |`,
    `| HISTORICAL_ONLY | ${audit.historicalOnlyCount} |`,
    `| EVIDENCE_INSUFFICIENT | ${audit.evidenceInsufficientCount} |`,
    `| **Inventory (live)** | **${audit.inventoryCount}** |`,
    "",
    "## Hotel-by-hotel",
    "",
    "| Hotel | Property ID | Period | Compatibility | RG method | Appearance | Composition | Reasons |",
    "|---|---|---|---|---|---|---|---|",
  ];
  for (const r of audit.rows) {
    const p = r.provenance || {};
    lines.push(
      `| ${r.hotel} | \`${r.propertyId}\` | ${r.researchPeriodId || "—"} | ${r.compatibility} | ${p.realityGapMethodologyId || "—"} | ${p.appearanceMethodologyId || "—"} | ${p.executiveCompositionVersion || "—"} | ${(r.reasons || []).join("; ")} |`
    );
  }
  lines.push("", "## Notes", "");
  for (const n of audit.notes || []) lines.push(`- ${n}`);
  lines.push("");
  return lines.join("\n");
}
