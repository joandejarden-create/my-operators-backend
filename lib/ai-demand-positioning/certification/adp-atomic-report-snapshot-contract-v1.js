/**
 * Atomic ADP report snapshot contract — all customer-facing sections must share
 * one governed snapshot (or explicitly validated compatible versions).
 * Mixed staged/published or cross-market competitor narratives fail certification.
 */

import { createHash } from "crypto";

export const ADP_ATOMIC_REPORT_SNAPSHOT_CONTRACT_V1 =
  "adp_atomic_report_snapshot_contract_v1";

export const ADP_APPEARANCE_COUNT_GRAIN_V1 = "OBSERVATION_RESPONSE";
export const ADP_DISPLACEMENT_COUNT_GRAIN_V1 = "UNIQUE_SCENARIO";

const FOREIGN_MARKET_MARKERS = [
  /embassy suites by hilton boca raton/i,
  /embassy_suites_boca/i,
  /\bboca raton\b/i,
];

function sha(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

function collectCompetitorNames(payload) {
  const names = new Set();
  const ids = new Set();
  for (const row of payload?.lostDemand?.displacement || []) {
    if (row?.name) names.add(String(row.name));
    if (row?.entityId) ids.add(String(row.entityId));
  }
  for (const row of payload?.competitiveSet?.observed || []) {
    if (row?.name) names.add(String(row.name));
    if (row?.entityId) ids.add(String(row.entityId));
  }
  const byTerr = payload?.competitiveRankingByTerritory?.byTerritory || {};
  for (const block of Object.values(byTerr)) {
    for (const row of block?.displayRows || []) {
      if (row?.isSubject) continue;
      if (row?.name) names.add(String(row.name));
      if (row?.entityId) ids.add(String(row.entityId));
    }
  }
  for (const a of payload?.actions || []) {
    if (a?.title) names.add(String(a.title));
    if (a?.description) names.add(String(a.description));
  }
  const er = payload?.executiveRead || {};
  const sections = er.sections || er.compositionV3?.sections || {};
  for (const v of Object.values(sections)) {
    if (typeof v === "string") names.add(v);
  }
  return { names: [...names], ids: [...ids] };
}

function textBlob(payload) {
  const er = payload?.executiveRead || {};
  const sections = {
    ...(er.sections || {}),
    ...(er.compositionV3?.sections || {}),
  };
  return JSON.stringify({
    sections,
    actions: payload?.actions || [],
    lostDemand: payload?.lostDemand?.displacement || [],
    brief: payload?.brief || null,
  });
}

function findForeignMarketHits(payload, propertyId) {
  // Waterstone / Boca subject hotels may legitimately mention Boca entities.
  if (String(propertyId || "").includes("boca") || String(propertyId || "").includes("waterstone")) {
    return [];
  }
  const blob = textBlob(payload);
  const hits = [];
  for (const re of FOREIGN_MARKET_MARKERS) {
    if (re.test(blob)) hits.push(re.source);
  }
  // Ranking/displacement entity ids
  const { ids, names } = collectCompetitorNames(payload);
  for (const id of ids) {
    // Require Boca Raton identity — do not flag Cartagena Bocagrande (substring "boca").
    if (/boca[_ ]?raton/i.test(id) && !/charlotte/i.test(id)) hits.push(`entityId:${id}`);
  }
  for (const name of names) {
    if (/boca raton/i.test(name) && !/charlotte/i.test(name)) hits.push(`name:${name}`);
  }
  return [...new Set(hits)];
}

function appearanceExceedsScenarioCap(payload) {
  const scenarioCap = Number(payload?.period?.scenarioCount) || 0;
  if (!scenarioCap) return [];
  const issues = [];
  const byTerr = payload?.competitiveRankingByTerritory?.byTerritory || {};
  for (const [terr, block] of Object.entries(byTerr)) {
    for (const row of block?.displayRows || []) {
      const appearances = Number(row?.appearances);
      if (!Number.isFinite(appearances)) continue;
      // Observation-grain appearances may exceed scenario count; only illegal if
      // UI grain is claimed as UNIQUE_SCENARIO without versioning.
      if (
        row.appearanceGrain === "UNIQUE_SCENARIO" &&
        appearances > scenarioCap
      ) {
        issues.push({
          territory: terr,
          entityId: row.entityId,
          appearances,
          scenarioCap,
        });
      }
    }
  }
  return issues;
}

function narrativeCitesAbsentCompetitor(payload) {
  const er = payload?.executiveRead || {};
  const sections = er.sections || er.compositionV3?.sections || {};
  const narrativeText = Object.values(sections).filter((v) => typeof v === "string").join(" ");
  const actionText = (payload?.actions || [])
    .map((a) => `${a.title || ""} ${a.description || ""}`)
    .join(" ");
  const blob = `${narrativeText}\n${actionText}`;
  const evidenceIds = new Set(
    (payload?.lostDemand?.displacement || []).map((d) => d.entityId).filter(Boolean)
  );
  const evidenceNames = new Set(
    (payload?.lostDemand?.displacement || []).map((d) => String(d.name || "").toLowerCase())
  );
  for (const row of payload?.competitiveSet?.observed || []) {
    if (row.entityId) evidenceIds.add(row.entityId);
    if (row.name) evidenceNames.add(String(row.name).toLowerCase());
  }
  // Boca-specific: if narrative mentions Embassy Boca but evidence set does not include it
  if (/embassy suites by hilton boca raton/i.test(blob)) {
    const inEvidence =
      evidenceIds.has("embassy_suites_boca") ||
      [...evidenceNames].some((n) => n.includes("boca raton") && n.includes("embassy"));
    if (!inEvidence) {
      return {
        competitor: "Embassy Suites by Hilton Boca Raton",
        reason: "NARRATIVE_COMPETITOR_ABSENT_FROM_VALIDATED_EVIDENCE",
      };
    }
  }
  return null;
}

/** Reality Gap methodology IDs are orthogonal to section build stamps (buildOwnerPayload:atomic). */
const REALITY_GAP_METHODOLOGY_IDS = new Set([
  "RG_BINARY_GAP_V1",
  "RG_WEIGHTED_PARTIAL_V2",
]);

function isRealityGapMethodologyStamp(value) {
  const v = String(value || "");
  if (!v) return false;
  if (REALITY_GAP_METHODOLOGY_IDS.has(v)) return true;
  // Dedup restage stamps methodology + contract, e.g. RG_WEIGHTED_PARTIAL_V2+CANONICAL_DEDUP_V1
  if (v.startsWith("RG_")) return true;
  if (/CANONICAL_DEDUP/i.test(v)) return true;
  return false;
}

function versionMismatch(payload) {
  const meta = payload?._adpSnapshotMeta || payload?.snapshotMeta || null;
  if (!meta) return null;
  const sectionVersions = meta.sectionArtifactVersions || {};
  // Compare build/artifact stamps only — do not treat methodologyId as an incompatible
  // section artifact version (V2 activation / canonical dedup write methodology into realityGap stamp).
  const values = Object.entries(sectionVersions)
    .filter(([key, v]) => {
      if (!v) return false;
      if (key === "realityGap" && isRealityGapMethodologyStamp(v)) {
        return false;
      }
      return true;
    })
    .map(([, v]) => v);
  if (values.length < 2) return null;
  const uniq = new Set(values);
  if (uniq.size > 1) {
    return { sectionArtifactVersions: sectionVersions };
  }
  return null;
}

function missingDataAsZero(payload) {
  const rg = payload?.realityGap || {};
  const issues = [];
  if (
    (rg.measurementStatus === "NOT_MEASURED_NO_GOVERNED_ATTRIBUTES" ||
      rg.measured === false) &&
    (rg.gapScore === 0 || rg.display === 0 || rg.percent === 0) &&
    !(String(rg.displayLabel || rg.display || "").toLowerCase().includes("not measured"))
  ) {
    issues.push("REALITY_GAP_ZERO_FOR_NOT_MEASURED");
  }
  return issues;
}

/**
 * Assess atomic integrity of an owner report payload.
 */
export function assessAtomicReportSnapshot(payload, options = {}) {
  const propertyId =
    options.propertyId ||
    payload?.propertyId ||
    payload?.property?.propertyId ||
    null;
  const blockers = [];
  const warnings = [];

  if (!payload || typeof payload !== "object") {
    return {
      contract: ADP_ATOMIC_REPORT_SNAPSHOT_CONTRACT_V1,
      passed: false,
      fullyCertified: false,
      blockers: [{ code: "MISSING_PAYLOAD" }],
    };
  }

  const foreign = findForeignMarketHits(payload, propertyId);
  if (foreign.length) {
    blockers.push({
      code: "CROSS_MARKET_COMPETITOR_IN_REPORT",
      detail: foreign.slice(0, 10),
      message:
        "Report cites competitor identity from a different market without supported comparison.",
    });
  }

  const absent = narrativeCitesAbsentCompetitor(payload);
  if (absent) {
    blockers.push({
      code: "NARRATIVE_COMPETITOR_ABSENT_FROM_EVIDENCE",
      detail: absent,
    });
  }

  const mismatch = versionMismatch(payload);
  if (mismatch) {
    blockers.push({
      code: "INCOMPATIBLE_SECTION_ARTIFACT_VERSIONS",
      detail: mismatch,
    });
  }

  if (payload._adpMixedPublishedFallback === true || payload.publishedFallbackUsed === true) {
    blockers.push({
      code: "STAGED_MERGED_WITH_PUBLISHED_FALLBACK",
      message: "Staged report must not merge published section content.",
    });
  }

  const zeroIssues = missingDataAsZero(payload);
  for (const code of zeroIssues) {
    blockers.push({ code, message: "Missing data represented as zero." });
  }

  // Universal: subject hotel must never appear in competitive displacement rows
  const subjectSlug = String(propertyId || "")
    .replace(/^adp_/, "")
    .trim();
  for (const row of payload?.lostDemand?.displacement || []) {
    const eid = String(row?.entityId || "");
    const name = String(row?.name || "").toLowerCase();
    const subjectName = String(
      payload?.property?.name || payload?.propertyName || ""
    ).toLowerCase();
    if (
      (subjectSlug && eid === subjectSlug) ||
      (subjectName && name && name === subjectName) ||
      eid === "__subject__"
    ) {
      blockers.push({
        code: "SELF_DISPLACEMENT",
        detail: { entityId: eid, name: row?.name, count: row?.displacementCount },
        message: "Target hotel must not appear as its own competitive displacement competitor.",
      });
    }
  }

  const scenarioCap = Number(payload?.period?.scenarioCount) || 0;
  const obsCap =
    Number(payload?.period?.observationCount) ||
    Number(payload?.evidence?.totalObservations) ||
    scenarioCap * (Number(payload?.period?.providerCount) || 4);

  // If any ranking row claims UNIQUE_SCENARIO grain but exceeds scenario count
  for (const issue of appearanceExceedsScenarioCap(payload)) {
    blockers.push({
      code: "SCENARIO_COUNT_EXCEEDS_GOVERNED_SCENARIOS",
      detail: issue,
    });
  }

  // Warn when observation-grain appearances are unlabeled (UI risk)
  const overall = payload?.competitiveRankingByTerritory?.byTerritory?.overall;
  const top = overall?.displayRows?.[0];
  if (top && Number(top.appearances) > scenarioCap && !top.appearanceGrain) {
    warnings.push({
      code: "APPEARANCE_COUNT_UNLABELED_OBSERVATION_GRAIN",
      message:
        "Competitive appearance counts exceed scenario count; grain is observation/response — label must not say scenarios without versioning.",
      appearances: top.appearances,
      scenarioCap,
      observationCapHint: obsCap,
    });
  }

  // Thin displacement overclaim — only when the narrative asserts a displacement
  // pattern (not unrelated "consistently" / "concentrated" wording elsewhere).
  const topDisp = payload?.lostDemand?.displacement?.[0];
  const keyInsight =
    payload?.executiveRead?.sections?.keyInsight ||
    payload?.executiveRead?.compositionV3?.sections?.keyInsight ||
    "";
  const displacementOverclaim =
    /\b(repeatable|recurring|concentrated)\b[\s\w-]{0,40}\b(displacement|substitution)\b/i.test(
      keyInsight
    ) ||
    /\b(displacement|substitution)\b[\s\w-]{0,40}\b(repeatable|recurring|concentrated|consistently|pattern)\b/i.test(
      keyInsight
    ) ||
    /\bcertified recurring (substitution )?pattern\b/i.test(keyInsight) ||
    /\b(repeatable|recurring)\s+substitution\s+pattern\b/i.test(keyInsight);
  if (
    topDisp &&
    Number(topDisp.displacementCount) <= 2 &&
    displacementOverclaim
  ) {
    blockers.push({
      code: "UNSUPPORTED_RECURRING_DISPLACEMENT_CLAIM",
      detail: {
        displacementCount: topDisp.displacementCount,
        competitor: topDisp.name,
      },
      message:
        "Executive narrative claims recurring/concentrated displacement without sufficient scenario support.",
    });
  }

  const passed = blockers.length === 0;
  return {
    contract: ADP_ATOMIC_REPORT_SNAPSHOT_CONTRACT_V1,
    propertyId,
    passed,
    fullyCertified: passed && warnings.length === 0,
    blockers,
    warnings,
    appearanceCountGrain: ADP_APPEARANCE_COUNT_GRAIN_V1,
    displacementCountGrain: ADP_DISPLACEMENT_COUNT_GRAIN_V1,
    snapshotHash: sha({
      periodId: payload?.period?.periodId,
      lostDemand: payload?.lostDemand?.displacement?.slice?.(0, 5),
      actions: payload?.actions?.slice?.(0, 3),
      sections:
        payload?.executiveRead?.sections ||
        payload?.executiveRead?.compositionV3?.sections,
      realityGap: payload?.realityGap?.measurementStatus,
    }),
  };
}

/**
 * Stamp ranking rows with observation-grain metadata (non-breaking additive).
 */
export function stampAppearanceGrainOnRanking(payload) {
  if (!payload?.competitiveRankingByTerritory?.byTerritory) return payload;
  const byTerritory = { ...payload.competitiveRankingByTerritory.byTerritory };
  for (const [key, block] of Object.entries(byTerritory)) {
    if (!block?.displayRows) continue;
    byTerritory[key] = {
      ...block,
      appearanceGrain: ADP_APPEARANCE_COUNT_GRAIN_V1,
      appearanceGrainVersion: "adp_appearance_grain_label_v1",
      displayRows: block.displayRows.map((row) => ({
        ...row,
        appearanceGrain: ADP_APPEARANCE_COUNT_GRAIN_V1,
        appearancesLabel: "AI answers",
      })),
    };
  }
  return {
    ...payload,
    competitiveRankingByTerritory: {
      ...payload.competitiveRankingByTerritory,
      byTerritory,
      appearanceGrain: ADP_APPEARANCE_COUNT_GRAIN_V1,
    },
  };
}

export function attachAtomicSnapshotMeta(payload, meta = {}) {
  const stamped = stampAppearanceGrainOnRanking(payload);
  const assessment = assessAtomicReportSnapshot(stamped, meta);
  return {
    ...stamped,
    _adpSnapshotMeta: {
      contract: ADP_ATOMIC_REPORT_SNAPSHOT_CONTRACT_V1,
      ...meta,
      assessment,
      stampedAt: new Date().toISOString(),
    },
    _adpAtomicCertification: assessment,
  };
}
