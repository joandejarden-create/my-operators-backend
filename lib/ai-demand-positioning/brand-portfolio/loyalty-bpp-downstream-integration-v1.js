/**
 * LOYALTY_BPP downstream integration — post-monitoring only (no provider calls).
 *
 * Pipeline:
 *   ARCHIVED → METRICS_CALCULATED → PEER_COHORT_CERTIFIED → RANKING_CERTIFIED
 *   → BPP_PACK_READY → STAGED_REPORT_READY → FOUNDER_REVIEW_READY
 *
 * Does not mutate customer-published Core BPP packs.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { createHash } from "crypto";
import { loadPropertyProfile } from "../data-model.js";
import { readbackDurableObservationArchive } from "../execution/durable-observation-archive-v1.js";
import { getBrandPortfolioPeerSet } from "./brand-portfolio-peer-set-v1.js";
import { getPortfolioMapping } from "./brand-portfolio-affiliation-mapping-v1.js";
import { buildBppCustomerReadyPayloadFromPeriod } from "./build-bpp-customer-ready-payload-v1.js";
import { applyBppCustomerPresentationV2 } from "./bpp-customer-presentation-v2.js";
import {
  classifyBppMeasurementProtocol,
  attachBppMethodologyDisclosure,
} from "./bpp-measurement-comparability-v1.js";
import {
  MEASUREMENT_FAMILY_LOYALTY_BPP,
  LOYALTY_BPP_SCENARIO_STANDARD_V1,
  LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1,
} from "./loyalty-bpp-scenario-standard-v1.js";
import {
  saveLoyaltyBppStagedPack,
  loadLoyaltyBppStagedPack,
} from "./loyalty-bpp-staged-pack-store-v1.js";
import {
  saveStagedAdpReport,
  loadStagedOwnerPayload,
} from "../review/adp-staged-report-store-v1.js";
import {
  mergeSubjectMeasuredPartialIntoStagedOwnerReport,
  BPP_SUBJECT_MEASURED_PARTIAL_V1,
} from "./bpp-subject-measured-partial-v1.js";

export const LOYALTY_BPP_DOWNSTREAM_INTEGRATION_V1 =
  "loyalty_bpp_downstream_integration_v1";

export const LOYALTY_BPP_PIPELINE_STAGES = Object.freeze([
  "ARCHIVED",
  "METRICS_CALCULATED",
  "PEER_COHORT_CERTIFIED",
  "RANKING_CERTIFIED",
  "BPP_PACK_READY",
  "STAGED_REPORT_READY",
  "FOUNDER_REVIEW_READY",
]);

function sha16(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}

export function loadLoyaltyPeriodRuntime(propertyId, periodId, cwd = process.cwd()) {
  const p = join(cwd, "data/ai-demand-positioning/runtime", `${periodId}.json`);
  if (!existsSync(p)) return null;
  try {
    const period = JSON.parse(readFileSync(p, "utf8"));
    if (period.propertyId && period.propertyId !== propertyId) return null;
    return period;
  } catch {
    return null;
  }
}

export function buildScenariosFromLoyaltyPeriod(period) {
  const ids =
    period?.scenarioIds ||
    [
      ...new Set(
        (period?.observations || []).map((o) => o.scenarioId).filter(Boolean)
      ),
    ];
  return ids.map((scenarioId) => {
    const parts = String(scenarioId).split("_");
    const category = parts[1] || "loyalty";
    return {
      scenarioId,
      territory: category,
      intent: category,
      measurementFamily: MEASUREMENT_FAMILY_LOYALTY_BPP,
    };
  });
}

/**
 * Resolve Loyalty period id from checkpoint job artifact (preferred) or runtime scan.
 */
export function resolveCompletedLoyaltyPeriodId(propertyId, cwd = process.cwd()) {
  const jobPath = join(
    cwd,
    "reports/dealality-adp-core-loyalty-execution-v1",
    `_job_${propertyId}_LOYALTY_BPP.json`
  );
  if (existsSync(jobPath)) {
    try {
      const job = JSON.parse(readFileSync(jobPath, "utf8"));
      if (job?.periodId && job.ok !== false) return job.periodId;
    } catch {
      /* fall through */
    }
  }
  const cpPath = join(
    cwd,
    "data/ai-demand-positioning/budget/core-loyalty-execution-checkpoint-v1.json"
  );
  if (existsSync(cpPath)) {
    try {
      const cp = JSON.parse(readFileSync(cpPath, "utf8"));
      const hit = (cp.completedJobs || []).find(
        (j) => j.jobKey === `${propertyId}::LOYALTY_BPP` && j.periodId
      );
      if (hit?.periodId) return hit.periodId;
    } catch {
      /* fall through */
    }
  }
  return null;
}

function stageResult(stage, ok, detail = {}) {
  return { stage, ok: !!ok, ...detail };
}

/**
 * Run post-monitoring Loyalty BPP pipeline for one hotel (no provider calls).
 */
export function runLoyaltyBppDownstreamIntegration({
  propertyId,
  periodId = null,
  cwd = process.cwd(),
  writeStagedAdpReport = true,
} = {}) {
  const stages = [];
  const blockers = [];
  const id = String(propertyId || "").trim();
  const resolvedPeriodId = periodId || resolveCompletedLoyaltyPeriodId(id, cwd);
  if (!resolvedPeriodId) {
    return {
      ok: false,
      propertyId: id,
      error: "loyalty_period_not_found",
      stages,
      blockers: ["NO_COMPLETED_LOYALTY_PERIOD"],
    };
  }

  const period = loadLoyaltyPeriodRuntime(id, resolvedPeriodId, cwd);
  if (!period) {
    return {
      ok: false,
      propertyId: id,
      periodId: resolvedPeriodId,
      error: "loyalty_runtime_missing",
      stages,
      blockers: ["RUNTIME_PERIOD_FILE_MISSING"],
    };
  }

  // --- ARCHIVED ---
  const readback = readbackDurableObservationArchive(id, resolvedPeriodId);
  const expectedObs =
    LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.expectedObservationsTarget;
  const archiveOk =
    readback?.ok === true &&
    (readback.withOriginalResponse || 0) >= expectedObs * 0.95 &&
    readback.hashMatch !== false &&
    readback.countMatch !== false;
  stages.push(
    stageResult("ARCHIVED", archiveOk, {
      withOriginalResponse: readback?.withOriginalResponse ?? 0,
      expectedCount: expectedObs,
      corpusHash: readback?.corpusHash || null,
      actualCount: readback?.actualCount ?? 0,
    })
  );
  if (!archiveOk) {
    blockers.push("ARCHIVE_READBACK_INCOMPLETE");
    return {
      ok: false,
      propertyId: id,
      periodId: resolvedPeriodId,
      stages,
      blockers,
      readback,
    };
  }

  const profile = loadPropertyProfile(id);
  if (!profile) {
    blockers.push("MISSING_PROPERTY_PROFILE");
    return { ok: false, propertyId: id, periodId: resolvedPeriodId, stages, blockers };
  }

  const peerSet = getBrandPortfolioPeerSet(id);
  const mapping = getPortfolioMapping(id);
  const peerOk =
    !!peerSet &&
    peerSet.adequacy?.canRank === true &&
    (peerSet.peerCountExcludingSubject || 0) >= 3;
  stages.push(
    stageResult("PEER_COHORT_CERTIFIED", peerOk, {
      peerSetId: peerSet?.peerSetId || null,
      peerCount: peerSet?.peerCountExcludingSubject ?? 0,
      adequacy: peerSet?.adequacy?.status || null,
      lensId: mapping?.defaultLensId || null,
    })
  );
  if (!peerOk) {
    blockers.push("PEER_COHORT_NOT_CERTIFIED_FOR_RANK");
    return {
      ok: false,
      propertyId: id,
      periodId: resolvedPeriodId,
      stages,
      blockers,
      certificationPending: true,
    };
  }

  const scenarios = buildScenariosFromLoyaltyPeriod(period);
  if (scenarios.length < LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.allowedMin) {
    blockers.push("SCENARIO_COUNT_BELOW_STANDARD");
    stages.push(stageResult("METRICS_CALCULATED", false, { scenarioCount: scenarios.length }));
    return { ok: false, propertyId: id, periodId: resolvedPeriodId, stages, blockers };
  }

  const built = buildBppCustomerReadyPayloadFromPeriod({
    profile,
    period,
    scenarios,
    measurementFamily: MEASUREMENT_FAMILY_LOYALTY_BPP,
    customerPublished: false,
    stagedReviewOnly: true,
    remediationTag: "loyalty_bpp_downstream_integration_v1",
    publicationVersion: `loyalty-bpp-staged-${resolvedPeriodId}`,
  });

  stages.push(
    stageResult("METRICS_CALCULATED", built.ok === true, {
      error: built.error || null,
      kpiCount: built.kpiCount,
      customerState: built.customerState,
    })
  );
  if (!built.ok) {
    blockers.push(`METRICS_${built.error || "FAILED"}`);
    return { ok: false, propertyId: id, periodId: resolvedPeriodId, stages, blockers };
  }

  const rows = built.payload?.ranking?.rows || [];
  const subjectRow = rows.find((r) => r.isSubject || r.name === profile.name);
  const rankingOk =
    rows.length >= 2 &&
    !!subjectRow &&
    Number.isFinite(Number(subjectRow.rank));
  stages.push(
    stageResult("RANKING_CERTIFIED", rankingOk, {
      rowCount: rows.length,
      subjectRank: subjectRow?.rank ?? null,
      subjectPresence: subjectRow?.presencePct ?? null,
      // Independently calculated — may match or differ from historical Core #1.
      historicalCoreRankNotCopied: true,
    })
  );
  if (!rankingOk) {
    blockers.push("RANKING_NOT_CERTIFIABLE");
    // Subject Loyalty is complete — never leave three subject KPI cards blank
    // solely because peer corpora are missing. Emit SUBJECT_MEASURED_RANK_BLOCKED.
    const partial = mergeSubjectMeasuredPartialIntoStagedOwnerReport(id, {
      periodId: resolvedPeriodId,
      cwd,
      writeStagedAdpReport,
    });
    stages.push(
      stageResult("SUBJECT_METRICS_PARTIAL_PACK", partial.ok === true, {
        contract: BPP_SUBJECT_MEASURED_PARTIAL_V1,
        metrics: partial.metrics || null,
        stagedVersionId: partial.stagedVersionId || null,
        error: partial.error || null,
      })
    );
    return {
      ok: partial.ok === true,
      propertyId: id,
      periodId: resolvedPeriodId,
      stages,
      blockers,
      certificationPending: true,
      subjectMetricsCertified: partial.ok === true,
      rankingCertified: false,
      bppMeasurementState: "BPP_PEER_RESEARCH_PENDING",
      metricsPreview: partial.payload?.kpis || built.payload?.kpis,
      partial,
    };
  }

  let presented = applyBppCustomerPresentationV2(built.payload);
  presented = attachBppMethodologyDisclosure(presented);
  const protocol = classifyBppMeasurementProtocol(presented);
  if (protocol.protocolId !== "BPP_MEASUREMENT_PROTOCOL_LOYALTY_CONSTRAINED_V1") {
    blockers.push("PROTOCOL_MISCLASSIFIED_AS_NON_LOYALTY");
  }

  const packSave = saveLoyaltyBppStagedPack({
    propertyId: id,
    periodId: resolvedPeriodId,
    payload: presented,
    certification: {
      status: blockers.length ? "CERTIFICATION_PENDING" : "STAGED_LOYALTY_BPP_READY",
      measurementFamily: MEASUREMENT_FAMILY_LOYALTY_BPP,
      scenarioStandard: LOYALTY_BPP_SCENARIO_STANDARD_V1,
      densityTier: LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.id,
      peerSetId: peerSet.peerSetId,
      protocolClassification: protocol.classification,
      readyRank: blockers.length === 0,
    },
    pipeline: {
      version: LOYALTY_BPP_DOWNSTREAM_INTEGRATION_V1,
      stagesCompleted: stages.filter((s) => s.ok).map((s) => s.stage),
    },
    cwd,
  });

  stages.push(
    stageResult("BPP_PACK_READY", packSave.ok === true && blockers.length === 0, {
      versionId: packSave.versionId,
      artifactHash: packSave.artifactHash,
      path: packSave.path,
    })
  );

  let stagedReport = null;
  if (writeStagedAdpReport && packSave.ok && blockers.length === 0) {
    // Prefer existing Core owner staged payload; overlay Loyalty BPP for founder review.
    const coreStaged = loadStagedOwnerPayload(id, cwd);
    const corePayload =
      coreStaged?.ok && coreStaged.payload?.competitiveSet
        ? coreStaged.payload
        : null;

    if (corePayload) {
      const {
        brandPortfolioPosition: _drop,
        _adpReportVersion,
        _adpStagedVersionId,
        _adpArtifactHash,
        ...rest
      } = corePayload;
      const merged = {
        ...rest,
        brandPortfolioPosition: {
          ...presented,
          customerPublished: false,
          stagedReviewOnly: true,
          _bppPackSource: "loyalty_bpp_staged",
          _bppLoyaltyStagedVersionId: packSave.versionId,
        },
        _adpBppSelection: {
          reportVersion: "STAGED",
          bppSource: "loyalty_bpp_staged",
          loyaltyPeriodId: resolvedPeriodId,
          loyaltyPackVersionId: packSave.versionId,
          historicalCorePublishedPreserved: true,
          selectionPolicy: "STAGED_PREFERS_CERTIFIED_LOYALTY_BPP_STANDARD_28",
        },
        measurementFamilyNote:
          "Core ADP owner sections from staged Core snapshot; BPP from LOYALTY_BPP STANDARD-28",
      };
      stagedReport = saveStagedAdpReport({
        propertyId: id,
        periodId: resolvedPeriodId,
        runId: `loyalty_bpp_downstream_${id}`,
        workflowVersion: LOYALTY_BPP_DOWNSTREAM_INTEGRATION_V1,
        payload: merged,
        certification: {
          status: "STAGED_FOUNDER_REVIEW_LOYALTY_BPP",
          measurementFamily: MEASUREMENT_FAMILY_LOYALTY_BPP,
          coreOwnerSectionsFrom: coreStaged.versionId,
          loyaltyBppPackVersionId: packSave.versionId,
          loyaltyPeriodId: resolvedPeriodId,
        },
        source: "loyalty_bpp_downstream_integration_v1",
        note: "Founder-review staged report: Core sections + certified Loyalty STANDARD-28 BPP. Does not publish.",
        customerFacing: true,
        cwd,
      });
    } else {
      blockers.push("NO_CORE_OWNER_STAGED_FOR_MERGE");
    }
  }

  stages.push(
    stageResult("STAGED_REPORT_READY", stagedReport?.ok === true, {
      versionId: stagedReport?.versionId || null,
      blocker: stagedReport?.ok ? null : blockers.includes("NO_CORE_OWNER_STAGED_FOR_MERGE")
        ? "NO_CORE_OWNER_STAGED_FOR_MERGE"
        : writeStagedAdpReport
          ? "STAGED_WRITE_SKIPPED_OR_FAILED"
          : "WRITE_DISABLED",
    })
  );

  const founderReady =
    archiveOk &&
    peerOk &&
    built.ok &&
    rankingOk &&
    packSave.ok &&
    stagedReport?.ok === true &&
    blockers.length === 0;
  stages.push(
    stageResult("FOUNDER_REVIEW_READY", founderReady, {
      selectionPolicy: "STAGED_PREFERS_CERTIFIED_LOYALTY_BPP_STANDARD_28",
      publishedCoreUnchanged: true,
    })
  );

  return {
    ok: founderReady,
    propertyId: id,
    periodId: resolvedPeriodId,
    stages,
    blockers,
    readback,
    metrics: {
      kpis: (presented.kpis || []).map((k) => ({
        id: k.id,
        value: k.value,
        meta: k.meta,
      })),
      ranking: (presented.ranking?.rows || []).map((r) => ({
        name: r.name,
        rank: r.rank,
        isSubject: !!r.isSubject,
        presencePct: r.presencePct,
      })),
    },
    protocol,
    pack: packSave.ok
      ? {
          versionId: packSave.versionId,
          artifactHash: packSave.artifactHash,
          path: packSave.path,
        }
      : null,
    stagedReport: stagedReport?.ok
      ? { versionId: stagedReport.versionId, path: stagedReport.path }
      : null,
    fingerprint: sha16({
      periodId: resolvedPeriodId,
      kpis: presented.kpis,
      rows: presented.ranking?.rows,
    }),
  };
}

/**
 * Audit hotels with completed Loyalty measurement vs downstream pack readiness.
 */
export function auditLoyaltyDownstreamStatus(propertyIds, cwd = process.cwd()) {
  const rows = [];
  for (const propertyId of propertyIds) {
    const periodId = resolveCompletedLoyaltyPeriodId(propertyId, cwd);
    const pack = loadLoyaltyBppStagedPack(propertyId, cwd);
    const period = periodId
      ? loadLoyaltyPeriodRuntime(propertyId, periodId, cwd)
      : null;
    const readback = periodId
      ? readbackDurableObservationArchive(propertyId, periodId)
      : null;
    const peerSet = getBrandPortfolioPeerSet(propertyId);
    rows.push({
      propertyId,
      loyaltyPeriodId: periodId || "",
      archiveOk: readback?.ok === true,
      withOriginal: readback?.withOriginalResponse ?? 0,
      expected: LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.expectedObservationsTarget,
      measurementFamily: period?.measurementFamily || "",
      peerSetId: peerSet?.peerSetId || "",
      peerCanRank: peerSet?.adequacy?.canRank === true,
      loyaltyPackPresent: !!pack?.ok,
      loyaltyPackVersionId: pack?.versionId || "",
      loyaltyPackPeriodId: pack?.periodId || "",
      downstreamGap:
        !!periodId &&
        readback?.ok === true &&
        !pack?.ok
          ? "MEASUREMENT_COMPLETE_PACK_MISSING"
          : !periodId
            ? "NO_LOYALTY_MEASUREMENT"
            : pack?.ok
              ? "PACK_READY"
              : "OTHER",
    });
  }
  return rows;
}
