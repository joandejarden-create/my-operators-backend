/**
 * ADP Master Full E2E Reconciliation V1 — independent per-hotel assessment.
 * Challenges prior green certifications with cross-artifact evidence checks.
 * Does not invent observations, ranks, or publication success.
 */

import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";
import {
  loadPropertyProfile,
  loadLatestPeriod,
  loadAllPeriods,
} from "../data-model.js";
import { listAdpReviewerProperties } from "../review/list-adp-reviewer-properties-v1.js";
import { assessHotelBpp, BPP_ENGINE_STATUS } from "../brand-portfolio/universal-bpp-engine-v1.js";
import { assessAdpSectionCompleteness } from "./adp-section-completeness-v1.js";
import { certifyTopObservedAlternativeRuntimeContract } from "./adp-top-observed-alternative-runtime-contract-v1.js";
import { assessPropertyAttributeIntelligence } from "../property-attributes/adp-property-attribute-intelligence-v1.js";
import { computeRealityGap } from "../intelligence/reality-gap.js";
import { getEntityRegistryForProperty } from "../metrics/adp-property-entity-registries.js";
import {
  resolveBrandPortfolioPosition,
  loadCustomerPublishedBrandPortfolio,
} from "../../../api/ai-demand-positioning.js";
import { ADP_OS_STAGES } from "../../dealality-full-e2e-os/adp-full-e2e-workflow-v1.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { assessRealityGapUniquenessFromPayload } from "./adp-reality-gap-canonical-attribute-uniqueness-v1.js";

export const MASTER_RECONCILIATION_VERSION = "ADP_MASTER_FULL_E2E_RECONCILIATION_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

function sha(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}

function exists(p) {
  return p && fs.existsSync(p);
}

function readJson(p) {
  if (!exists(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function listDir(p) {
  if (!exists(p)) return [];
  return fs.readdirSync(p);
}

function unwrapReportPayload(doc) {
  if (!doc || typeof doc !== "object") return null;
  // Published wrappers: { ok, payload: { executiveRead, ... } }
  if (doc.payload && typeof doc.payload === "object" && !doc.executiveRead) {
    return { ...doc.payload, _wrapper: { propertyId: doc.propertyId, periodId: doc.periodId } };
  }
  return doc;
}

function loadPublishedReport(propertyId) {
  const dir = path.join(REPO_ROOT, "data/ai-demand-positioning/published", propertyId);
  if (!exists(dir)) return { present: false, dir: null, manifest: null, report: null };
  const manifest = readJson(path.join(dir, "manifest.json"));
  const reports = listDir(dir).filter((f) => f.startsWith("report-") && f.endsWith(".json"));
  const reportFile = reports.sort().at(-1) || null;
  const raw = reportFile ? readJson(path.join(dir, reportFile)) : null;
  return {
    present: true,
    dir,
    manifest,
    reportFile,
    report: unwrapReportPayload(raw),
    rawWrapper: raw,
    files: listDir(dir),
  };
}

function loadStagedReport(propertyId) {
  // Prefer governed staged store resolver (skips LOYALTY_BPP measurement artifacts).
  const governed = loadStagedOwnerPayload(propertyId, REPO_ROOT);
  if (governed?.ok && governed.payload) {
    return {
      present: true,
      dir: path.join(REPO_ROOT, "data/ai-demand-positioning/staged", propertyId),
      report: unwrapReportPayload(governed.payload),
      source: "staged_store_customer_facing",
      versionId: governed.versionId || null,
      resolvedFromLoyaltyCorruption: !!governed.resolvedFromLoyaltyCorruption,
    };
  }

  const dir = path.join(REPO_ROOT, "data/ai-demand-positioning/staged", propertyId);
  const candidates = [
    "CORRECTED_FULL_PAYLOAD_STAGED.json",
    "latest.json",
    "report.json",
  ];
  if (!exists(dir)) {
    // also try review staged store
    const reviewDir = path.join(
      REPO_ROOT,
      "data/ai-demand-positioning/review/staged",
      propertyId
    );
    if (exists(reviewDir)) {
      const latest = listDir(reviewDir)
        .filter((f) => f.endsWith(".json"))
        .sort()
        .at(-1);
      return {
        present: true,
        dir: reviewDir,
        report: latest ? readJson(path.join(reviewDir, latest)) : null,
        source: "review_staged",
      };
    }
    return { present: false, dir: null, report: null };
  }
  for (const c of candidates) {
    const p = path.join(dir, c);
    if (exists(p)) {
      return { present: true, dir, report: unwrapReportPayload(readJson(p)), source: c };
    }
  }
  const any = listDir(dir)
    .filter((f) => f.startsWith("report-") && f.endsWith(".json"))
    .sort()
    .at(-1);
  return {
    present: !!any,
    dir,
    report: any ? unwrapReportPayload(readJson(path.join(dir, any))) : null,
    source: any || null,
  };
}

function observationArchivePaths(propertyId, periodId) {
  const candidates = [
    path.join(REPO_ROOT, "data/ai-demand-positioning/runtime", propertyId),
    path.join(REPO_ROOT, "data/ai-demand-positioning/periods", propertyId),
    path.join(REPO_ROOT, "data/ai-demand-positioning/observations", propertyId),
  ];
  if (periodId) {
    candidates.push(
      path.join(
        REPO_ROOT,
        "data/ai-demand-positioning/runtime",
        propertyId,
        periodId
      )
    );
  }
  return candidates.filter(exists);
}

/**
 * Independent metric recalculation from raw observations (presence grain).
 * Does not replace certified product metrics — used for cross-check.
 */
export function recalculateCorePresenceMetrics(observations = [], subjectTokens = []) {
  const obs = Array.isArray(observations) ? observations : [];
  const total = obs.length;
  if (!total) {
    return {
      totalObservations: 0,
      presentCount: 0,
      considerationRate: null,
      providers: {},
      recalculationNote: "NO_OBSERVATIONS",
    };
  }
  const tokens = (subjectTokens || [])
    .map((t) => String(t || "").toLowerCase().trim())
    .filter(Boolean);
  let present = 0;
  const byProvider = {};
  for (const o of obs) {
    const provider = String(o.provider || o.model || "unknown").toLowerCase();
    if (!byProvider[provider]) byProvider[provider] = { total: 0, present: 0 };
    byProvider[provider].total += 1;
    const text = String(
      o.responseText || o.rawResponse || o.answer || o.content || ""
    ).toLowerCase();
    const named =
      o.subjectPresent === true ||
      o.presence?.subjectPresent === true ||
      (tokens.length > 0 && tokens.some((t) => text.includes(t)));
    if (named) {
      present += 1;
      byProvider[provider].present += 1;
    }
  }
  return {
    totalObservations: total,
    presentCount: present,
    considerationRate: Number((present / total).toFixed(4)),
    providers: byProvider,
    recalculationNote:
      "INDEPENDENT_TEXT_TOKEN_OR_FLAG_CHECK — not a substitute for certified product grain",
  };
}

function cellStatus(ok, pending, blocked) {
  if (ok) return "CERTIFIED";
  if (blocked) return "BLOCKED";
  if (pending) return "PENDING";
  return "INCOMPLETE";
}

/**
 * Full independent assessment for one hotel.
 */
export function assessHotelMasterReconciliation(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestPeriod(propertyId);
  const periods = loadAllPeriods(propertyId) || [];
  const published = loadPublishedReport(propertyId);
  const staged = loadStagedReport(propertyId);
  const bppAssess = assessHotelBpp(propertyId, { profile, period, periods });
  const bppPack = loadCustomerPublishedBrandPortfolio(propertyId);
  const bppResolved = resolveBrandPortfolioPosition(propertyId, profile);
  const registry = getEntityRegistryForProperty(propertyId);
  const observations = period?.observations || [];
  const attrIntel = assessPropertyAttributeIntelligence(profile || { propertyId }, observations);
  const realityGap = computeRealityGap(observations, profile || { propertyId, attributes: [] });
  const archivePaths = observationArchivePaths(propertyId, period?.periodId);
  const subjectTokens = [
    profile?.name,
    profile?.shortName,
    ...(profile?.aliases || []),
    ...(profile?.subjectEntityIds || []),
  ].filter(Boolean);
  const independentMetrics = recalculateCorePresenceMetrics(observations, subjectTokens);

  const payload =
    staged.report ||
    published.report ||
    null;
  const payloadWithBpp = payload
    ? { ...payload, brandPortfolioPosition: bppResolved }
    : { brandPortfolioPosition: bppResolved };

  const sectionCompleteness = assessAdpSectionCompleteness({
    propertyId,
    payload: payloadWithBpp,
    context: {
      periodArtifactCount: periods.length,
      hasComparablePriorPeriod: periods.length >= 2,
      bppReady: bppAssess.engineStatus === BPP_ENGINE_STATUS.READY_RANK,
      marketRegistryPresent: !!registry,
      competitiveCertified: !!registry,
      airtableAttributesPresent: false,
    },
  });

  const issues = [];
  const issue = (sev, section, code, detail, repair) => {
    issues.push({
      issueId: `${propertyId}::${code}::${sha({ code, detail }).slice(0, 8)}`,
      propertyId,
      severity: sev,
      section,
      code,
      detail,
      proposedRepair: repair,
      executionAuthorization: "non_paid_staging_ok",
      resolutionState: "OPEN",
    });
  };

  // Systemic checks
  if (!profile) {
    issue("P0", "identity", "MISSING_PROPERTY_PROFILE", "No fixture profile", "Add governed profile fixture");
  }
  if (observations.length === 0 && published.present) {
    issue(
      "P0",
      "raw_evidence",
      "MISSING_LOCAL_OBSERVATION_ARCHIVE",
      "Published report exists but loadLatestPeriod returned 0 observations",
      "Restore period archive from durable runtime / historical snapshot; do not synthesize"
    );
  }
  if (observations.length > 0 && archivePaths.length === 0) {
    issue(
      "P1",
      "raw_evidence",
      "OBSERVATIONS_IN_MEMORY_ONLY",
      "Observations loadable via data-model but no runtime archive directory found",
      "Confirm data-model period path is durable SoT"
    );
  }
  if (!registry) {
    const extractedCompetitorMentions = observations.reduce(
      (n, o) => n + (o.competitorsMentioned?.length || 0),
      0
    );
    // P0 when Core extraction already found hotel names — otherwise customer
    // Competitive Set / displacement silently zeros under fail-closed resolution.
    issue(
      extractedCompetitorMentions > 0 ? "P0" : "P1",
      "competition",
      "MISSING_MARKET_REGISTRY",
      extractedCompetitorMentions > 0
        ? `No property entity registry while ${extractedCompetitorMentions} extracted competitor mention(s) exist — Competitive Set would silently empty`
        : "No property entity registry — competitive analysis must fail closed",
      "Govern-create market registry before customer-facing competitive certification; do not inherit foreign CORE"
    );
  }
  if (attrIntel.status === "EMPTY" && realityGap.measured !== true) {
    issue(
      "P1",
      "reality_gap",
      "EMPTY_GOVERNED_ATTRIBUTES",
      attrIntel.blocker?.message || "No governed attributes",
      attrIntel.blocker?.recovery || "Populate governed attributeSet from verified sources"
    );
  }
  if (
    bppAssess.engineStatus !== BPP_ENGINE_STATUS.READY_RANK &&
    bppAssess.engineStatus !== BPP_ENGINE_STATUS.STATUS_ONLY_HIERARCHY_EXHAUSTED
  ) {
    issue(
      "P2",
      "bpp",
      bppAssess.nextAction?.code || "BPP_INCOMPLETE",
      bppAssess.nextAction?.detail || bppAssess.engineStatus,
      bppAssess.nextAction?.code || "See universal BPP engine"
    );
  }
  if (periods.length < 2) {
    issue(
      "P2",
      "trends",
      "BASELINE_ONLY_NO_COMPARABLE_PRIOR",
      `Only ${periods.length} period(s) — trends must be baseline-only`,
      "Authorize next comparable monitoring cycle; do not fabricate history"
    );
  }
  if (!staged.present && published.present) {
    issue(
      "P2",
      "delivery",
      "NO_STAGED_REVIEW_SNAPSHOT",
      "Published present but no staged review payload",
      "Seed staged from corrected/published under local review auth"
    );
  }
  if ((sectionCompleteness.materialIncompleteCount || 0) > 0) {
    issue(
      "P1",
      "sections",
      "SECTION_COMPLETENESS_MATERIAL_GAPS",
      `${sectionCompleteness.materialIncompleteCount} material incomplete sections`,
      "Repair upstream evidence then rebuild atomic snapshot"
    );
  }

  // Property Snapshot / topObservedAlternative runtime contract (fail-closed on missing parent)
  if (payload) {
    const topAltCert = certifyTopObservedAlternativeRuntimeContract(payload, {
      propertyId,
      expectedPropertyId: propertyId,
      certification: staged.certification || null,
    });
    for (const i of topAltCert.issues || []) {
      issue(
        i.severity || "P0",
        "property_snapshot",
        i.code,
        i.message,
        "Restore Core owner staged/published payload with competitiveSet parent; do not fabricate competitors"
      );
    }
    if (staged.resolvedFromLoyaltyCorruption) {
      issue(
        "P1",
        "delivery",
        "STAGED_LOYALTY_POINTER_REPAIRED_AT_READ",
        "Manifest latest pointed at Loyalty artifact; customer-facing loader resolved prior Core snapshot",
        "Run repairStagedCustomerPointers; ensure Loyalty saves use customerFacing:false"
      );
    }
  }

  // Challenge circular certification: Reality Gap in payload vs independent recompute
  const payloadRg = payload?.realityGap || payload?.payload?.realityGap;
  if (
    payloadRg?.measured === true &&
    realityGap.measured !== true &&
    observations.length === 0
  ) {
    issue(
      "P0",
      "reality_gap",
      "PAYLOAD_RG_WITHOUT_LOCAL_OBS",
      "Report claims Reality Gap measured but local observations unavailable for independent recompute",
      "Restore observation archive before certifying RG evidence"
    );
  }
  if (
    realityGap.measurementStatus === "NOT_MEASURED_NO_OBSERVATIONS" ||
    (attrIntel.attributeCount > 0 && observations.length === 0)
  ) {
    issue(
      "P0",
      "reality_gap",
      "RG_ATTRIBUTES_WITHOUT_OBSERVATIONS",
      "Governed attributes exist but observation archive is empty — cannot certify Reality Gap",
      "Restore original monitoring archive or authorize remonitor; do not treat attribute-only 100% gap as measured"
    );
  }

  const uniqueness = assessRealityGapUniquenessFromPayload(
    payloadRg || realityGap,
    propertyId
  );
  if (payloadRg && !uniqueness.ok) {
    issue(
      "P0",
      "reality_gap",
      "DUPLICATE_REALITY_GAP_ATTRIBUTE",
      `Canonical Reality Gap uniqueness failed: ${uniqueness.issues.map((i) => i.code).join("|")}`,
      "Apply ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1 and restage before Full E2E PASS"
    );
  }

  const rawEvidenceOk = observations.length > 0;
  const coreMetricsOk =
    rawEvidenceOk &&
    independentMetrics.considerationRate != null &&
    (payload?.executiveMetrics || payload?.demandCapture);
  const trendsOk = periods.length >= 2;
  const trendsBaselineOnly = periods.length === 1 && rawEvidenceOk;
  const bppOk = bppAssess.engineStatus === BPP_ENGINE_STATUS.READY_RANK;
  const bppStatusOnly =
    bppAssess.engineStatus === BPP_ENGINE_STATUS.STATUS_ONLY_HIERARCHY_EXHAUSTED;
  const rgOk = realityGap.measured === true;
  const competitionOk = !!registry;
  const sectionsOk =
    (sectionCompleteness.materialIncompleteCount || 0) === 0 &&
    (sectionCompleteness.certifiedCount || 0) > 0;

  const fullE2eState = (() => {
    if (issues.some((i) => i.severity === "P0")) return "BLOCKED_EVIDENCE";
    if (!rawEvidenceOk && !published.present) return "NO_REPORT";
    if (bppOk && rgOk && competitionOk && coreMetricsOk && sectionsOk) {
      return "RESEARCH_COMPLETE_AWAITING_FOUNDER";
    }
    if (rawEvidenceOk) return "PARTIAL_CERTIFIED";
    return "INCOMPLETE";
  })();

  return {
    propertyId,
    hotelName: profile?.name || propertyId,
    hpcId: profile?.hpcId || profile?.hotelPropertyCensusId || null,
    researchEligibility: profile?.researchEligibility || null,
    identity: {
      profilePresent: !!profile,
      hpcId: profile?.hpcId || profile?.hotelPropertyCensusId || null,
      brand: profile?.brand || null,
      affiliation: profile?.affiliation || null,
      parentCompany: profile?.parentCompany || null,
      city: profile?.city || null,
      market: profile?.market || null,
    },
    rawEvidence: {
      status: cellStatus(rawEvidenceOk, false, !rawEvidenceOk && published.present),
      latestPeriodId: period?.periodId || null,
      observationCount: observations.length,
      periodCount: periods.length,
      periodIds: periods.map((p) => p.periodId).filter(Boolean),
      archivePaths,
      independentMetrics,
    },
    coreMetrics: {
      status: cellStatus(!!coreMetricsOk, rawEvidenceOk && !coreMetricsOk, !rawEvidenceOk),
      hasExecutiveMetrics: !!(payload?.executiveMetrics || payload?.demandCapture),
      independentConsiderationRate: independentMetrics.considerationRate,
    },
    trends: {
      status: trendsOk
        ? "CERTIFIED_COMPARABLE"
        : trendsBaselineOnly
          ? "BASELINE_ONLY"
          : "INCOMPLETE",
      periodCount: periods.length,
      comparable: trendsOk,
    },
    bpp: {
      status: bppOk
        ? "READY_RANK"
        : bppStatusOnly
          ? "STATUS_ONLY_LEGITIMATE"
          : bppAssess.engineStatus,
      lensLabel: bppAssess.lens?.lensLabel || null,
      peerCount: bppAssess.peerSet?.peerCount ?? 0,
      packReady: bppAssess.pack?.packReady === true,
      nextAction: bppAssess.nextAction,
    },
    realityGap: {
      status: rgOk
        ? "MEASURED"
        : realityGap.measurementStatus === "NOT_MEASURED_NO_OBSERVATIONS"
          ? "NOT_MEASURED_NO_OBSERVATIONS"
          : attrIntel.status === "EMPTY" ||
              realityGap.measurementStatus === "NOT_MEASURED_NO_GOVERNED_ATTRIBUTES"
            ? "NOT_MEASURED_NO_ATTRIBUTES"
            : realityGap.measurementStatus || "UNKNOWN",
      measured: realityGap.measured === true,
      gapScore: realityGap.gapScore,
      attributeCount: attrIntel.attributeCount,
      attributeStatus: attrIntel.status,
      display: realityGap.display,
      observationCount: realityGap.observationCount ?? observations.length,
    },
    competition: {
      status: competitionOk ? "REGISTRY_PRESENT" : "FAIL_CLOSED_NO_REGISTRY",
      registryVersion: registry?.version || registry?.registryVersion || null,
      marketRegistryPresent: !!registry,
    },
    citation: {
      status: payload?.citationIntelligence || payload?.citations ? "PAYLOAD_PRESENT" : "UNKNOWN",
      hasCitationPayload: !!(payload?.citationIntelligence || payload?.citations || payload?.sourceLandscape),
    },
    executive: {
      status: payload?.executiveRead || payload?.brief ? "PAYLOAD_PRESENT" : "MISSING",
      hasExecutiveRead: !!(payload?.executiveRead || payload?.brief),
    },
    airtable: {
      status: "FS_AUDIT_PRIMARY",
      note: "Live Airtable overlay audited separately; FS is primary SoT",
    },
    delivery: {
      publishedPresent: published.present,
      publishedManifest: published.manifest?.publishStatus || published.manifest?.status || null,
      stagedPresent: staged.present,
      stagedSource: staged.source || null,
      spaShare: published.present || staged.present ? "ARTIFACT_PRESENT" : "MISSING",
      pdf: "SEE_PDF_MANIFEST_AUDIT",
    },
    sections: sectionCompleteness,
    fullE2eState,
    nextAction: issues[0]
      ? {
          code: issues[0].code,
          detail: issues[0].detail,
          severity: issues[0].severity,
        }
      : {
          code: "FOUNDER_REVIEW",
          detail: "No P0/P1 open — submit for founder review (no auto-publish)",
          severity: "P3",
        },
    issues,
    artifactHashes: {
      profile: profile ? sha({ id: propertyId, name: profile.name }) : null,
      periodId: period?.periodId || null,
      bppPack: bppPack ? sha({ status: bppPack.status, rows: bppPack.ranking?.rows?.length }) : null,
      sectionCompleteness: sha({
        certified: sectionCompleteness.certifiedCount,
        incomplete: sectionCompleteness.materialIncompleteCount,
      }),
    },
    assessedAt: new Date().toISOString(),
    engineVersion: MASTER_RECONCILIATION_VERSION,
  };
}

export function runMasterPortfolioReconciliation(options = {}) {
  const properties = listAdpReviewerProperties({
    includeExcluded: true,
    includeHistoricalExcluded: true,
  });
  const hotels = [];
  const allIssues = [];

  for (const p of properties) {
    if (options.onlyHotel && p.propertyId !== options.onlyHotel) continue;
    const row = assessHotelMasterReconciliation(p.propertyId);
    row.researchEligibility =
      p.researchEligibility || row.researchEligibility || "ELIGIBLE";
    row.reviewVisibility = p.reviewVisibility || null;
    hotels.push(row);
    allIssues.push(...row.issues);
  }

  // Systemic root causes (portfolio-level)
  const runnerSrc = fs.readFileSync(
    path.join(REPO_ROOT, "lib/dealality-full-e2e-os/autonomous-stage-runner-v1.js"),
    "utf8"
  );
  const missingHandlers = ADP_OS_STAGES.map((s) => s.id).filter(
    (id) => !runnerSrc.includes(`async ${id}(`)
  );

  const rootCauses = [
    {
      id: "RC_MISSING_STAGE_HANDLERS",
      title: "Full E2E stage handlers missing or stubbed",
      detail:
        "Stages in ADP_OS_STAGES without real handlers (or with empty-input stubs) allow COMPLETE without verifying hotel evidence.",
      hotelsAffected: hotels.length,
      codePaths: [
        "lib/dealality-full-e2e-os/autonomous-stage-runner-v1.js",
        "lib/dealality-full-e2e-os/adp-full-e2e-workflow-v1.js",
      ],
      repair:
        "Wire property_attribute_intelligence, section_completeness_certification; fix property_reality_coverage to use real profile+observations",
      remainingRisk: missingHandlers.length
        ? `Still missing handlers: ${missingHandlers.join(", ")}`
        : "Handlers present — residual risk is shallow COMPLETE without qualityGate enforcement upstream",
      regressionTest: "test:dealality-full-e2e-os-v1 / master reconciliation",
    },
    {
      id: "RC_CIRCULAR_CERTIFICATION",
      title: "Certification that trusts producer output",
      detail:
        "Prior green paths often re-read the same payload the producer wrote without independent observation recompute.",
      hotelsAffected: hotels.filter((h) =>
        h.issues.some((i) => i.code === "PAYLOAD_RG_WITHOUT_LOCAL_OBS")
      ).length,
      repair: "Independent recalculation + cross-artifact compare in master reconciliation",
      remainingRisk: "Product metric grain still diverges from simple text-token check",
      regressionTest: "METRIC_RECALCULATION_MATRIX.csv",
    },
    {
      id: "RC_ATTACH_AT_READ_VS_SNAPSHOT",
      title: "Attach-at-read BPP vs immutable snapshot mismatch",
      detail:
        "BPP packs attach at read; historical PDFs/snapshots may lack pack unless versioned attachment is recorded.",
      hotelsAffected: hotels.filter((h) => h.bpp.status !== "READY_RANK").length,
      repair: "Universal BPP engine + publicationVersion on attach; regenerate PDF on pack merge",
      remainingRisk: "16+ hotels still need peer set or measurement authorization",
      regressionTest: "test:adp-universal-bpp-engine-v1",
    },
    {
      id: "RC_MISSING_OBS_ARCHIVES",
      title: "Published without local observation archive",
      detail:
        "Reports can show complete while loadLatestPeriod returns empty — blocks independent evidence certification.",
      hotelsAffected: hotels.filter((h) =>
        h.issues.some((i) => i.code === "MISSING_LOCAL_OBSERVATION_ARCHIVE")
      ).length,
      repair: "Restore archives; fail evidence certification when original obs unavailable",
      remainingRisk: "Remeasurement may be required if archives unrecoverable",
      regressionTest: "RAW_OBSERVATION_EVIDENCE_AUDIT.md",
    },
    {
      id: "RC_EMPTY_ATTRIBUTES_SILENT_RG",
      title: "Empty attributes treated as successful Reality Gap stage",
      detail:
        "property_reality_coverage previously called computeRealityGap([], {attributes:[]}) always.",
      hotelsAffected: hotels.filter((h) => h.realityGap.status === "NOT_MEASURED_NO_ATTRIBUTES")
        .length,
      repair: "Use real profile+observations; diagnose EMPTY vs MEASURED",
      remainingRisk: "Attribute provenance still incomplete for many hotels",
      regressionTest: "REALITY_GAP_EVIDENCE_RECONCILIATION.csv",
    },
    {
      id: "RC_RG_MEASURED_WITHOUT_OBSERVATIONS",
      title: "Reality Gap MEASURED with attributes but zero observations",
      detail:
        "computeRealityGap previously set measured=totalAttributes>0, yielding 100% gap scores with empty corpora.",
      hotelsAffected: hotels.filter((h) =>
        h.issues.some((i) => i.code === "RG_ATTRIBUTES_WITHOUT_OBSERVATIONS")
      ).length,
      repair:
        "Require observation corpus for MEASURED; status NOT_MEASURED_NO_OBSERVATIONS otherwise",
      remainingRisk: "Published snapshots may still embed stale measured=true until rebuilt",
      regressionTest: "REALITY_GAP_EVIDENCE_RECONCILIATION.csv",
    },
  ];

  const summary = {
    discovered: hotels.length,
    eligible: hotels.filter((h) => h.researchEligibility !== "EXCLUDED").length,
    historicalOnly: hotels.filter((h) => h.researchEligibility === "EXCLUDED").length,
    completeRawEvidence: hotels.filter((h) => h.rawEvidence.status === "CERTIFIED").length,
    certifiedCoreMetrics: hotels.filter((h) => h.coreMetrics.status === "CERTIFIED").length,
    certifiedCompetition: hotels.filter((h) => h.competition.status === "REGISTRY_PRESENT")
      .length,
    certifiedBppRankings: hotels.filter((h) => h.bpp.status === "READY_RANK").length,
    baselineOnlyTrends: hotels.filter((h) => h.trends.status === "BASELINE_ONLY").length,
    certifiedRealityGap: hotels.filter((h) => h.realityGap.status === "MEASURED").length,
    citationPayloadPresent: hotels.filter((h) => h.citation.hasCitationPayload).length,
    executivePayloadPresent: hotels.filter((h) => h.executive.hasExecutiveRead).length,
    sectionsMateriallyComplete: hotels.filter(
      (h) => (h.sections?.materialIncompleteCount || 0) === 0
    ).length,
    publishedPresent: hotels.filter((h) => h.delivery.publishedPresent).length,
    stagedPresent: hotels.filter((h) => h.delivery.stagedPresent).length,
    founderReviewReady: hotels.filter(
      (h) => h.fullE2eState === "RESEARCH_COMPLETE_AWAITING_FOUNDER"
    ).length,
    founderApproved: 0,
    productionPublishedClaimed: 0,
    productionDeliveryVerified: 0,
    openIssues: allIssues.filter((i) => i.resolutionState === "OPEN").length,
    p0Issues: allIssues.filter((i) => i.severity === "P0").length,
    p1Issues: allIssues.filter((i) => i.severity === "P1").length,
  };

  return {
    ok: true,
    engineVersion: MASTER_RECONCILIATION_VERSION,
    generatedAt: new Date().toISOString(),
    summary,
    hotels,
    issues: allIssues,
    rootCauses,
    missingStageHandlers: missingHandlers,
    osStages: ADP_OS_STAGES.map((s) => s.id),
  };
}
