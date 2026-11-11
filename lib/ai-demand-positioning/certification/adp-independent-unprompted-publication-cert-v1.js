/**
 * ADP_INDEPENDENT_UNPROMPTED_PUBLICATION_CERT_V1
 *
 * Hard gate for NEW Independent Unprompted V1 publications.
 * Does not grandfather historical reports into VERIFIED.
 * Does not block access to previously authorized published reports.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  ADP_CONSIDERATION_REPRODUCTION_TOLERANCE_PP,
  ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_V1,
  ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION,
  getIndependentUnpromptedMethodologyContract,
  isIndependentUnpromptedMethodologyId,
} from "./adp-independent-unprompted-methodology-contract-v1.js";
import {
  applyUnpromptedStandardToUniverse,
  loadArchivePromptMap,
} from "./adp-unprompted-independent-discovery-standard-v1.js";
import {
  buildSubjectMentionOverrideFromEvidence,
  extractExplicitEligibleScenarioIds,
  hashEligibleScenarioIds,
  recalculateConsiderationFromFrozenIds,
} from "./adp-historical-eligibility-recovery-v1.js";
import { roundAdpPercent } from "../format-percent.js";
import {
  computeIndependentUnpromptedV1MethodologyFingerprint,
} from "./adp-independent-unprompted-v1-methodology-fingerprint.js";

export const ADP_INDEPENDENT_UNPROMPTED_PUBLICATION_CERT_V1 =
  "ADP_INDEPENDENT_UNPROMPTED_PUBLICATION_CERT_V1";

function unwrap(doc) {
  return doc?.payload && typeof doc.payload === "object" ? doc.payload : doc;
}

function fail(code, detail, extra = {}) {
  return { ok: false, code, detail, ...extra };
}

/**
 * Build the required certification stamp object for a NEW V1 publication payload.
 */
export function buildIndependentUnpromptedPublicationStamp({
  propertyId,
  periodId,
  methodologyId = ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1,
  eligibleScenarioIds,
  numerator,
  denominator,
  considerationRate,
  providerCoverage,
  scenarioCoverage,
  sourceEvidenceReferences,
  sourceCorpusSha,
  calculationProvenance,
  certificationVerdict,
}) {
  return {
    contract: ADP_INDEPENDENT_UNPROMPTED_PUBLICATION_CERT_V1,
    methodologyId,
    methodologyContractId: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_V1,
    methodologyContractVersion: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION,
    measurementPeriodId: periodId,
    propertyId,
    providerCoverage: providerCoverage || null,
    scenarioCoverage: scenarioCoverage || null,
    eligibleScenarioIds: [...(eligibleScenarioIds || [])].map(String).sort(),
    eligibleScenarioIdsSha256: hashEligibleScenarioIds(eligibleScenarioIds),
    numerator,
    denominator,
    considerationRate,
    sourceEvidenceReferences: sourceEvidenceReferences || null,
    sourceCorpusSha: sourceCorpusSha || null,
    calculationProvenance: calculationProvenance || null,
    certificationVerdict: certificationVerdict || null,
    certifiedAt: new Date().toISOString(),
  };
}

/**
 * Freeze eligible IDs for a NEW baseline using the live methodology contract
 * against the period's scenario/prompt evidence (not inventing historical IDs).
 */
export function freezeEligibleScenarioIdsForNewBaseline({
  propertyId,
  periodId,
  scenarios,
  propertyProfile,
  promptMap = null,
}) {
  const map =
    promptMap ||
    (propertyId && periodId ? loadArchivePromptMap(propertyId, periodId) : new Map());
  const universe = applyUnpromptedStandardToUniverse({
    propertyId,
    periodId,
    scenarios,
    propertyProfile,
    promptMap: map,
  });
  const ids = [...(universe.eligibleScenarioIds || [])].map(String).sort();
  return {
    eligibleScenarioIds: ids,
    counts: universe.counts,
    coverage: universe.coverage,
    methodologyStamp: universe.methodologyStamp,
    source: "live_methodology_contract_partition",
  };
}

/**
 * Certify a candidate report (staged or draft) for NEW V1 publication.
 */
export function certifyIndependentUnpromptedPublicationV1({
  propertyId,
  reportDoc,
  period = null,
  periodId = null,
  cwd = process.cwd(),
  requireFrozenIdsInReport = true,
  allowFreezeFromLiveContract = true,
  /** V1.1.1+: always adjudicate mentions from original response text. */
  requireResponseTextMentionAdjudication = true,
  tolerancePp = ADP_CONSIDERATION_REPRODUCTION_TOLERANCE_PP,
} = {}) {
  const blockers = [];
  const warnings = [];
  const contract = getIndependentUnpromptedMethodologyContract();
  const payload = unwrap(reportDoc) || {};
  const resolvedPeriodId =
    periodId ||
    period?.periodId ||
    payload?.period?.periodId ||
    null;

  const cons = payload?.executiveMetrics?.considerationRate || {};
  const meth =
    cons.methodologyId ||
    cons.correctedMethodologyId ||
    payload?.measurementMethodology?.correctedMethodologyId ||
    payload?.correctionLineage?.correctedMethodologyId ||
    null;

  if (!isIndependentUnpromptedMethodologyId(meth)) {
    blockers.push(
      fail("METHODOLOGY_INCORRECT", `Expected ${ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1}`, {
        methodologyId: meth,
      })
    );
  }

  let embedded = extractExplicitEligibleScenarioIds(reportDoc);
  let freeze = null;
  if (!embedded?.eligibleScenarioIds?.length) {
    if (requireFrozenIdsInReport && !allowFreezeFromLiveContract) {
      blockers.push(
        fail("ELIGIBLE_ID_REGISTER_MISSING", "Report lacks frozen eligibleScenarioIds")
      );
    } else if (allowFreezeFromLiveContract && period && payload?.property) {
      const promptMap = loadArchivePromptMap(propertyId, resolvedPeriodId);
      if (!promptMap.size) {
        blockers.push(
          fail(
            "PROMPTS_MISSING_FROM_ARCHIVE",
            "Observation archive has no prompt text; cannot freeze eligibility without inventing historical decisions",
            { archivePromptMapSize: 0 }
          )
        );
      } else {
        const by = new Map();
        for (const o of period.observations || []) {
          if (!by.has(o.scenarioId)) {
            by.set(o.scenarioId, {
              scenarioId: o.scenarioId,
              query: promptMap.get(o.scenarioId) || "",
              prompt: promptMap.get(o.scenarioId) || "",
              intent: "leisure",
              source: String(o.scenarioId).startsWith("prop_")
                ? "property_specific"
                : "generated",
            });
          }
        }
        const emptyPrompts = [...by.values()].filter((s) => !String(s.query || "").trim())
          .length;
        if (emptyPrompts === by.size) {
          blockers.push(
            fail(
              "PROMPTS_MISSING_FROM_ARCHIVE",
              "All scenario prompts empty after archive load",
              { scenarioCount: by.size }
            )
          );
        } else {
          freeze = freezeEligibleScenarioIdsForNewBaseline({
            propertyId,
            periodId: resolvedPeriodId,
            scenarios: [...by.values()],
            propertyProfile: payload.property,
            promptMap,
          });
          if (!freeze.eligibleScenarioIds.length) {
            blockers.push(
              fail(
                "ELIGIBLE_ID_REGISTER_EMPTY",
                "Live partition produced zero eligible scenarios"
              )
            );
          } else {
            warnings.push({
              code: "ELIGIBLE_IDS_FROZEN_AT_CERT_TIME",
              detail:
                "Report lacked embedded register; IDs frozen from live methodology contract for NEW baseline certification",
            });
            embedded = {
              eligibleScenarioIds: freeze.eligibleScenarioIds,
              source: freeze.source,
              count: freeze.eligibleScenarioIds.length,
            };
          }
        }
      }
    } else {
      blockers.push(
        fail("ELIGIBLE_ID_REGISTER_MISSING", "No embedded register and cannot freeze")
      );
    }
  }

  const rtPath = resolvedPeriodId
    ? join(cwd, "data/ai-demand-positioning/runtime", `${resolvedPeriodId}.json`)
    : null;
  const archDir =
    propertyId && resolvedPeriodId
      ? join(
          cwd,
          "data/ai-demand-positioning/observation-archive",
          propertyId,
          resolvedPeriodId,
          "observations"
        )
      : null;
  const periodDoc =
    period ||
    (rtPath && existsSync(rtPath)
      ? JSON.parse(readFileSync(rtPath, "utf8"))
      : null);

  if (!periodDoc?.observations?.length) {
    blockers.push(
      fail("SOURCE_EVIDENCE_INSUFFICIENT", "Runtime period observations missing", {
        rtPath,
      })
    );
  }
  if (archDir && !existsSync(archDir)) {
    warnings.push({
      code: "ARCHIVE_DIR_MISSING",
      detail: archDir,
    });
  }

  let recalc = null;
  let mentionPath = "runtime_mentioned_flag";
  let mentionEvidence = null;
  if (periodDoc && embedded?.eligibleScenarioIds?.length) {
    if (requireResponseTextMentionAdjudication) {
      if (!payload?.property) {
        blockers.push(
          fail(
            "PROPERTY_PROFILE_MISSING",
            "Cannot adjudicate subject mentions without property profile"
          )
        );
      } else {
        mentionEvidence = buildSubjectMentionOverrideFromEvidence({
          period: { ...periodDoc, propertyId, periodId: resolvedPeriodId },
          propertyProfile: payload.property,
          archiveDir: archDir,
          eligibleScenarioIds: embedded.eligibleScenarioIds,
          requireResponseText: true,
          cwd,
        });
        if (!mentionEvidence.ok) {
          blockers.push(
            fail(
              "SOURCE_RESPONSE_EVIDENCE_MISSING",
              "Eligible observations lack original response text; refusing to invent mentions",
              {
                missing: mentionEvidence.missingResponseText.slice(0, 20),
                missingCount: mentionEvidence.stats.missingResponseText,
              }
            )
          );
        } else {
          mentionPath = mentionEvidence.mentionPath;
          if (mentionEvidence.stats.runtimeFlagConflicts > 0) {
            warnings.push({
              code: "RUNTIME_MENTION_FLAGS_UNRELIABLE",
              detail: `${mentionEvidence.stats.runtimeFlagConflicts} eligible observations disagree with response-text detectPropertyMention`,
              stats: mentionEvidence.stats,
            });
          }
          recalc = recalculateConsiderationFromFrozenIds({
            period: periodDoc,
            eligibleScenarioIds: embedded.eligibleScenarioIds,
            tolerancePp,
            publishedRate: cons.rate ?? null,
            publishedPresent: cons.presentObservations ?? null,
            publishedDenom: cons.comparableObservations ?? null,
            mentionOverride: mentionEvidence.mentionOverride,
            requireMentionOverride: true,
          });
        }
      }
    } else {
      // Legacy diagnostic path only — not valid for V1.1.1+ publication certification.
      recalc = recalculateConsiderationFromFrozenIds({
        period: periodDoc,
        eligibleScenarioIds: embedded.eligibleScenarioIds,
        tolerancePp,
        publishedRate: cons.rate ?? null,
        publishedPresent: cons.presentObservations ?? null,
        publishedDenom: cons.comparableObservations ?? null,
      });
      warnings.push({
        code: "RUNTIME_MENTION_FLAGS_USED",
        detail: "requireResponseTextMentionAdjudication=false; not publication-valid under V1.1.1+",
      });
    }

    if (recalc) {
      if (recalc.denominator !== Number(cons.comparableObservations)) {
        blockers.push(
          fail("DENOMINATOR_NOT_REPRODUCED", "Frozen-ID recalc denom ≠ report", {
            expected: cons.comparableObservations,
            got: recalc.denominator,
          })
        );
      }
      if (recalc.numerator !== Number(cons.presentObservations)) {
        blockers.push(
          fail("NUMERATOR_NOT_REPRODUCED", "Frozen-ID recalc numerator ≠ report", {
            expected: cons.presentObservations,
            got: recalc.numerator,
            mentionPath,
          })
        );
      }
      if (
        recalc.considerationRate != null &&
        cons.rate != null &&
        Math.abs(recalc.considerationRate - Number(cons.rate)) > tolerancePp
      ) {
        blockers.push(
          fail("CONSIDERATION_INCONSISTENT", "Rate outside tolerance", {
            expected: cons.rate,
            got: recalc.considerationRate,
            tolerancePp,
          })
        );
      }
    }
  }

  // PDF parity: if pdf metrics embedded or sidecar consideration present, must match
  const pdfCons =
    payload?._pdfParity?.considerationRate ??
    payload?.pdfMetrics?.considerationRate ??
    null;
  if (pdfCons != null && cons.rate != null) {
    if (Math.abs(Number(pdfCons) - Number(cons.rate)) > tolerancePp) {
      blockers.push(
        fail("PDF_ONLINE_METRIC_DISAGREE", "PDF consideration ≠ online report", {
          pdf: pdfCons,
          online: cons.rate,
        })
      );
    }
  }

  const manifestPath =
    propertyId && resolvedPeriodId
      ? join(
          cwd,
          "data/ai-demand-positioning/observation-archive",
          propertyId,
          resolvedPeriodId,
          "manifest.json"
        )
      : null;
  const corpusSha =
    periodDoc?.durableArchive?.corpusHash ||
    (manifestPath && existsSync(manifestPath)
      ? JSON.parse(readFileSync(manifestPath, "utf8")).corpusHash
      : null);

  const methodologyFingerprint = computeIndependentUnpromptedV1MethodologyFingerprint({ cwd });
  const ok = blockers.length === 0;
  const stamp = ok
    ? buildIndependentUnpromptedPublicationStamp({
        propertyId,
        periodId: resolvedPeriodId,
        eligibleScenarioIds: embedded.eligibleScenarioIds,
        numerator: cons.presentObservations ?? recalc?.numerator,
        denominator: cons.comparableObservations ?? recalc?.denominator,
        considerationRate: cons.rate ?? recalc?.considerationRate,
        providerCoverage: {
          providers: recalc?.providers || [],
          providerCount: (recalc?.providers || []).length || payload?.period?.providerCount,
        },
        scenarioCoverage: {
          researchScenarioInventory:
            payload?.measurementMethodology?.appearanceKpis?.researchScenarioInventory ??
            null,
          independentDiscoveryEligible: embedded.eligibleScenarioIds.length,
        },
        sourceEvidenceReferences: {
          runtimePeriod: resolvedPeriodId
            ? `data/ai-demand-positioning/runtime/${resolvedPeriodId}.json`
            : null,
          observationArchive: `data/ai-demand-positioning/observation-archive/${propertyId}/${resolvedPeriodId}/observations`,
        },
        sourceCorpusSha: corpusSha,
        calculationProvenance: {
          contract: ADP_INDEPENDENT_UNPROMPTED_PUBLICATION_CERT_V1,
          methodologyContract: contract.contractId,
          methodologyContractVersion: ADP_INDEPENDENT_UNPROMPTED_METHODOLOGY_CONTRACT_VERSION,
          methodologyFingerprintId: methodologyFingerprint.fingerprintId,
          methodologyFingerprintSha256: methodologyFingerprint.fingerprintSha256,
          eligibleIdsSource: embedded.source,
          recalcProducer: "recalculateConsiderationFromFrozenIds",
          mentionPath,
          mentionEvidenceStats: mentionEvidence?.stats || null,
          runtimeMentionFlagsAuthoritative: false,
        },
        certificationVerdict: "PASS",
      })
    : null;

  return {
    ok,
    contract: ADP_INDEPENDENT_UNPROMPTED_PUBLICATION_CERT_V1,
    propertyId,
    periodId: resolvedPeriodId,
    methodologyId: meth,
    blockers,
    warnings,
    recalc,
    mentionPath,
    mentionEvidenceStats: mentionEvidence?.stats || null,
    methodologyFingerprintSha256: methodologyFingerprint.fingerprintSha256,
    eligibleScenarioIds: embedded?.eligibleScenarioIds || null,
    stamp,
    verdict: ok ? "PASS" : "FAIL",
  };
}

/**
 * Produce a corrected Independent Unprompted V1 consideration triple from evidence.
 * Used to build certified candidates when prior report numerators trusted runtime flags.
 */
export function computeArchiveAdjudicatedConsiderationV1({
  propertyId,
  period,
  periodId = null,
  propertyProfile,
  eligibleScenarioIds,
  cwd = process.cwd(),
} = {}) {
  const resolvedPeriodId = periodId || period?.periodId || null;
  const evidence = buildSubjectMentionOverrideFromEvidence({
    period: { ...period, propertyId, periodId: resolvedPeriodId },
    propertyProfile,
    eligibleScenarioIds,
    requireResponseText: true,
    cwd,
  });
  if (!evidence.ok) {
    return {
      ok: false,
      code: "SOURCE_RESPONSE_EVIDENCE_MISSING",
      missingResponseText: evidence.missingResponseText,
      stats: evidence.stats,
    };
  }
  const recalc = recalculateConsiderationFromFrozenIds({
    period,
    eligibleScenarioIds,
    mentionOverride: evidence.mentionOverride,
    requireMentionOverride: true,
  });
  const runtimeBaseline = recalculateConsiderationFromFrozenIds({
    period,
    eligibleScenarioIds,
  });
  return {
    ok: true,
    ...recalc,
    mentionPath: evidence.mentionPath,
    mentionEvidenceStats: evidence.stats,
    runtimeFlagBaseline: {
      numerator: runtimeBaseline.numerator,
      denominator: runtimeBaseline.denominator,
      considerationRate: runtimeBaseline.considerationRate,
    },
    deltaVsRuntimeFlagsPp:
      recalc.considerationRate != null && runtimeBaseline.considerationRate != null
        ? Math.round((recalc.considerationRate - runtimeBaseline.considerationRate) * 10) / 10
        : null,
  };
}

/**
 * Validate arithmetic of a consideration triple without eligibility recovery.
 */
export function assertConsiderationArithmetic(present, denom, rate, tolerancePp = 0.15) {
  if (denom == null || present == null || rate == null) {
    return { ok: false, reason: "missing_fields" };
  }
  if (Number(denom) <= 0) return { ok: false, reason: "zero_denom" };
  if (Number(present) > Number(denom)) return { ok: false, reason: "numer_gt_denom" };
  const expected = roundAdpPercent((Number(present) / Number(denom)) * 100);
  const delta = Math.abs(expected - Number(rate));
  return { ok: delta <= tolerancePp, expected, delta };
}
