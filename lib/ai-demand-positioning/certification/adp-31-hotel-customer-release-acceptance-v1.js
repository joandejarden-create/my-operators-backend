/**
 * ADP_31_HOTEL_CUSTOMER_RELEASE_ACCEPTANCE_V1
 *
 * Separate from MEASUREMENT_CERTIFIED and FOUNDER_REVIEW_READY.
 * Customer-facing content readiness — not a recycled technical PASS count.
 *
 * Do not launch paid monitoring or customer publication from this module.
 */

import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../governance/adp-operational-registry-hygiene-v1.js";
import { getCensusRecordIdForAdpProperty } from "../census-link-registry.js";
import {
  loadPropertyProfile,
  loadLatestCoreCustomerPeriod,
  isLoyaltyMeasurementPeriod,
} from "../data-model.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { loadPublishedManifest } from "../published-snapshot.js";
import { independentlyRecalculateRealityGapV1V2 } from "./adp-rg-v2-cross-surface-parity-certification-v1.js";
import { CONDITIONAL_APPROVED_METHODOLOGY_ID } from "../intelligence/reality-gap-methodology-v2.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import { assertAffiliationGold } from "../measurement-assurance/property-profile-affiliation-integrity-v1.js";
import { assessRealityGapUniquenessFromPayload } from "./adp-reality-gap-canonical-attribute-uniqueness-v1.js";
import { runDedupGate3PreCustomerRelease } from "./adp-full-e2e-canonical-attribute-dedup-gate-v1.js";

export const ADP_31_HOTEL_CUSTOMER_RELEASE_ACCEPTANCE_V1 =
  "ADP_31_HOTEL_CUSTOMER_RELEASE_ACCEPTANCE_V1";

export const CUSTOMER_RELEASE_VERDICT = Object.freeze({
  CUSTOMER_READY: "CUSTOMER_READY",
  CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS:
    "CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS",
  NOT_CUSTOMER_READY: "NOT_CUSTOMER_READY",
});

export const SEVERITY = Object.freeze({
  P0: "P0",
  P1: "P1",
  P2: "P2",
});

export const CUSTOMER_RELEASE_NEGATIVE_TESTS = Object.freeze([
  "WRONG_HOTEL_PDF",
  "STALE_REALITY_GAP_METHODOLOGY",
  "INCORRECT_BPP_RANK",
  "INCORRECT_HOTEL_AFFILIATION",
  "CONFLICTING_AIRTABLE_AMENITY",
  "BROKEN_EVIDENCE_LINK",
  "UNSUPPORTED_EXECUTIVE_SUMMARY_CLAIM",
  "MISALIGNED_PDF_PERIOD",
  "MISSING_REPORT_SECTION",
  "INVALID_SHARE_TOKEN",
  "INACCESSIBLE_STAGED_ARTIFACT",
  "SYNTHETIC_OPERATIONAL_HOTEL",
  "INCONSISTENT_HTML_PDF_METRIC",
  "GENERIC_COPY_SUBSTITUTED",
  "DUPLICATE_REALITY_GAP_ATTRIBUTE",
  "RECOGNIZE_AND_MISS_SAME_ATTRIBUTE",
  "CANONICAL_DEDUP_GATE3_FAILED",
]);

/** Founder-approved BPP ranks that must not regress. */
export const CERTIFIED_BPP_RANKS = Object.freeze({
  adp_wyndham_san_diego_bayside: "#1 of 5",
  adp_st_regis_cap_cana: "#7 of 11",
  adp_westin_monterrey_valle: "#2 of 9",
  adp_sheraton_mallorca_arabella_golf: "#3 of 4",
  adp_tampa_airport_marriott: "#6 of 6",
});

export const HONEST_BPP_RANK_BLOCKED = Object.freeze([
  "adp_nobu_london_portman",
  "adp_yotel_geneva_lake",
]);

const GENERIC_EXEC_PATTERNS = [
  /\bthis hotel\b/i,
  /\bthe property offers a unique\b/i,
  /\bleverage AI visibility\b/i,
  /\bin today's competitive landscape\b/i,
  /\boptimize your digital presence\b/i,
  /\bstay ahead of the competition\b/i,
];

function bppRankFromPayload(bpp) {
  const kpi = (bpp?.kpis || []).find((k) => k.id === "portfolioRank");
  if (!kpi) return null;
  if (kpi.available === false || kpi.value === "—" || kpi.valueRaw == null) {
    return "SUBJECT_MEASURED_RANK_BLOCKED";
  }
  return kpi.value || null;
}

function execText(v) {
  if (v == null) return "";
  if (typeof v === "string") return v;
  if (typeof v === "object") {
    return [v.title, v.headline, v.body, v.text, v.summary, v.label]
      .filter(Boolean)
      .map(String)
      .join(" ");
  }
  return String(v);
}

/**
 * Heuristic editorial quality score (0–100). Not a substitute for human founder review.
 * Reads Executive Summary V3 shapes: sections.*, ux.biggestStrength/Constraint, primaryStrength.
 */
export function scoreEditorialQuality(payload, profile) {
  const er = payload?.executiveRead || {};
  const sections = er.sections || {};
  const ux = er.ux || {};
  const blob = JSON.stringify(er);
  const name = profile?.name || "";
  const nameToken = name.split(",")[0].slice(0, 12);
  let score = 72;
  const flags = [];

  const keyInsight = execText(sections.keyInsight || er.keyInsight);
  const whyItMatters = execText(sections.whyItMatters || er.whyItMatters);
  const focusNow = execText(sections.focusNow || er.focusNow);
  const biggestStrength = execText(
    ux.biggestStrength ||
      er.summary?.biggestStrength ||
      er.primaryStrength ||
      er.current?.primaryStrength ||
      sections.biggestStrength
  );
  const biggestConstraint = execText(
    ux.biggestConstraint ||
      er.summary?.biggestConstraint ||
      er.primaryConstraint ||
      er.current?.primaryConstraint ||
      sections.biggestConstraint
  );

  const checks = [
    ["keyInsight", keyInsight, 60],
    ["whyItMatters", whyItMatters, 40],
    ["focusNow", focusNow, 40],
    ["biggestStrength", biggestStrength, 12],
    ["biggestConstraint", biggestConstraint, 12],
  ];
  for (const [k, text, minLen] of checks) {
    if (!text || text.trim().length < minLen) {
      score -= 8;
      flags.push(`thin_or_missing_${k}`);
    }
  }
  if (nameToken && !blob.toLowerCase().includes(nameToken.toLowerCase())) {
    score -= 15;
    flags.push("hotel_name_absent_from_executive");
  }
  for (const re of GENERIC_EXEC_PATTERNS) {
    if (re.test(blob)) {
      score -= 8;
      flags.push(`generic_pattern:${re.source}`);
    }
  }
  const actions = payload?.actions || [];
  if (!actions.length) {
    score -= 12;
    flags.push("no_priority_actions");
  } else {
    // Actions may reference attributes without repeating hotel name — only flag if all are generic
    const genericAction = actions.filter((a) => {
      const t = `${a.title || ""} ${a.body || a.text || ""}`.toLowerCase();
      return (
        /leverage ai|optimize your|digital presence|stay ahead/i.test(t) &&
        t.length < 80
      );
    }).length;
    if (genericAction === actions.length && actions.length > 0) {
      score -= 8;
      flags.push("actions_all_generic");
    }
  }
  score = Math.max(0, Math.min(100, score));
  return {
    score,
    flags,
    actionCount: actions.length,
    hasStrength: biggestStrength.length >= 12,
    hasConstraint: biggestConstraint.length >= 12,
  };
}

/**
 * Build defects for one hotel from staged payload + independent sources.
 * @param {object} opts
 */
export function auditHotelCustomerReleaseContent(opts = {}) {
  const {
    propertyId,
    hpcFields = null,
    pdfAudit = null,
    browserAudit = null,
    shareAudit = null,
    evidenceAudit = null,
  } = opts;
  const defects = [];
  const push = (severity, code, detail) =>
    defects.push({ propertyId, severity, code, detail });

  if (isAdpSyntheticOrTestPropertyId(propertyId)) {
    push(SEVERITY.P0, "SYNTHETIC_OPERATIONAL_HOTEL", propertyId);
  }

  const profile = loadPropertyProfile(propertyId);
  const staged = loadStagedOwnerPayload(propertyId);
  const published = loadPublishedManifest(propertyId);
  const hpcId = getCensusRecordIdForAdpProperty(propertyId);
  const core = loadLatestCoreCustomerPeriod(propertyId);
  const liveRg = independentlyRecalculateRealityGapV1V2(propertyId);

  if (!profile) push(SEVERITY.P0, "PROFILE_MISSING", propertyId);
  if (!hpcId) push(SEVERITY.P0, "HPC_LINK_MISSING", propertyId);
  if (!staged?.payload) push(SEVERITY.P0, "STAGED_REPORT_MISSING", propertyId);
  if (!core?.observations?.length) {
    push(SEVERITY.P0, "CORE_OBSERVATIONS_MISSING", propertyId);
  }
  if (core && isLoyaltyMeasurementPeriod(core)) {
    push(SEVERITY.P0, "CORE_PERIOD_IS_LOYALTY", core.periodId);
  }

  try {
    if (profile) assertAffiliationGold(profile);
  } catch (e) {
    push(SEVERITY.P0, "AFFILIATION_INTEGRITY", e.message);
  }

  const rg = staged?.payload?.realityGap || {};
  if (rg.methodologyId !== CONDITIONAL_APPROVED_METHODOLOGY_ID) {
    push(
      SEVERITY.P0,
      "STALE_REALITY_GAP_METHODOLOGY",
      rg.methodologyId || "missing"
    );
  }
  if (liveRg.ok && rg.gapScore !== liveRg.gapScoreV2) {
    push(
      SEVERITY.P0,
      "REALITY_GAP_MISMATCH",
      `staged=${rg.gapScore} live=${liveRg.gapScoreV2}`
    );
  }

  const uniqueness = assessRealityGapUniquenessFromPayload(rg, propertyId);
  if (!uniqueness.ok) {
    const codes = uniqueness.issues.map((i) => i.code);
    if (codes.includes("RECOGNIZE_AND_MISS_SAME_ATTRIBUTE")) {
      push(
        SEVERITY.P0,
        "RECOGNIZE_AND_MISS_SAME_ATTRIBUTE",
        codes.join("|")
      );
    }
    push(
      SEVERITY.P0,
      "DUPLICATE_REALITY_GAP_ATTRIBUTE",
      codes.join("|") || "uniqueness_fail"
    );
  }

  const dedupGate3 = runDedupGate3PreCustomerRelease(propertyId, {
    staged,
    realityGap: rg,
  });
  if (!dedupGate3.ok) {
    push(
      SEVERITY.P0,
      "CANONICAL_DEDUP_GATE3_FAILED",
      dedupGate3.issues.map((i) => i.code).join("|")
    );
  }

  // HPC vs profile identity
  if (hpcFields && profile) {
    const hpcName =
      hpcFields["Canonical Property Name"] ||
      hpcFields["Property Name"] ||
      hpcFields.name ||
      "";
    const hpcBrand = hpcFields["Current Brand"] || hpcFields.Affiliation || "";
    const hpcKey =
      hpcFields["Property Identity Key"] || hpcFields.identity_key || "";
    if (
      hpcName &&
      profile.name &&
      !String(hpcName)
        .toLowerCase()
        .includes(String(profile.name).split(",")[0].slice(0, 8).toLowerCase()) &&
      !String(profile.name)
        .toLowerCase()
        .includes(String(hpcName).split(",")[0].slice(0, 8).toLowerCase())
    ) {
      push(
        SEVERITY.P1,
        "HPC_NAME_PROFILE_MISMATCH",
        `hpc=${hpcName} profile=${profile.name}`
      );
    }
    if (
      propertyId === "adp_now_now_noho" &&
      /hyatt/i.test(String(hpcBrand))
    ) {
      push(SEVERITY.P0, "NOW_NOW_HYATT_AFFILIATION", hpcBrand);
    }
    if (
      propertyId === "adp_now_now_noho" &&
      hpcKey &&
      hpcKey !== "ind_nownow_us_noho" &&
      hpcKey !== "ind_hyatt_us_nycnh"
    ) {
      push(SEVERITY.P0, "NOW_NOW_UNEXPECTED_IDENTITY_KEY", hpcKey);
    }
  }

  const bpp = staged?.payload?.brandPortfolioPosition;
  const bppRank = bppRankFromPayload(bpp);
  const expectedBpp = CERTIFIED_BPP_RANKS[propertyId];
  if (expectedBpp && bppRank !== expectedBpp) {
    push(
      SEVERITY.P0,
      "INCORRECT_BPP_RANK",
      `expected=${expectedBpp} actual=${bppRank}`
    );
  }
  if (HONEST_BPP_RANK_BLOCKED.includes(propertyId)) {
    if (bppRank && /^#\d+\s+of\s+\d+$/.test(bppRank)) {
      push(SEVERITY.P0, "MANUFACTURED_BPP_RANK", bppRank);
    }
  }

  const peerSet = getBrandPortfolioPeerSet(propertyId);
  const er = staged?.payload?.executiveRead;
  if (!er || !Object.keys(er).length) {
    push(SEVERITY.P1, "MISSING_EXECUTIVE_SUMMARY", "empty");
  }
  const editorial = scoreEditorialQuality(staged?.payload, profile);
  if (editorial.score < 40) {
    push(
      SEVERITY.P1,
      "GENERIC_OR_THIN_EXECUTIVE",
      `score=${editorial.score}; ${editorial.flags.join("|")}`
    );
  } else if (editorial.score < 55 || editorial.flags.some((f) => f.startsWith("generic_pattern"))) {
    push(
      SEVERITY.P2,
      "EDITORIAL_POLISH",
      `score=${editorial.score}; ${editorial.flags.join("|")}`
    );
  }

  const actions = staged?.payload?.actions || [];
  if (!actions.length) {
    push(SEVERITY.P1, "MISSING_PRIORITY_ACTIONS", "empty");
  }

  const disp = staged?.payload?.lostDemand || staged?.payload?.competitiveDisplacement;
  if (!disp && !staged?.payload?.displacement) {
    push(SEVERITY.P2, "DISPLACEMENT_SECTION_THIN", "no_lostDemand_payload");
  }

  // Artifact checks (when provided)
  if (pdfAudit) {
    if (pdfAudit.verdict === "BLOCKED" || pdfAudit.exists === "N") {
      push(SEVERITY.P1, "STAGED_PDF_MISSING_OR_BLOCKED", pdfAudit.verdict);
    } else if (pdfAudit.hasV2Score === "N" || pdfAudit.hasHotelIdentity === "N") {
      push(
        SEVERITY.P0,
        "PDF_CONTENT_MISMATCH",
        `v2=${pdfAudit.hasV2Score} hotel=${pdfAudit.hasHotelIdentity}`
      );
    }
  }
  if (browserAudit) {
    if (browserAudit.verdict === "FAIL" || browserAudit.verdict === "BLOCKED") {
      push(SEVERITY.P0, "BROWSER_RENDER_FAIL", browserAudit.verdict);
    }
    if (browserAudit.kpiShowsV1BinaryInsteadOfV2 === "Y") {
      push(SEVERITY.P0, "BROWSER_SHOWS_V1_AS_CURRENT", "kpi");
    }
  }
  if (shareAudit) {
    if (shareAudit.stagedV2Ok === "N") {
      push(SEVERITY.P1, "STAGED_SHARE_CONTRACT_FAIL", shareAudit.verdict);
    }
  }
  if (evidenceAudit) {
    if (
      evidenceAudit.discovered > 0 &&
      evidenceAudit.clicked === 0 &&
      evidenceAudit.required === true
    ) {
      push(SEVERITY.P1, "EVIDENCE_NOT_EXERCISED", "zero_clicks");
    }
    if (evidenceAudit.broken > 0) {
      push(
        SEVERITY.P0,
        "BROKEN_EVIDENCE_LINK",
        `broken=${evidenceAudit.broken}`
      );
    }
  }

  const p0 = defects.filter((d) => d.severity === SEVERITY.P0);
  const p1 = defects.filter((d) => d.severity === SEVERITY.P1);
  const p2 = defects.filter((d) => d.severity === SEVERITY.P2);

  let verdict = CUSTOMER_RELEASE_VERDICT.CUSTOMER_READY;
  const disclosed = [];
  if (HONEST_BPP_RANK_BLOCKED.includes(propertyId)) {
    disclosed.push("BPP_SUBJECT_MEASURED_RANK_BLOCKED");
  }
  if (!peerSet && !expectedBpp && !HONEST_BPP_RANK_BLOCKED.includes(propertyId)) {
    // Faranda / Radisson SD often N/A BPP
    if (!bppRank) disclosed.push("BPP_NOT_APPLICABLE_OR_ABSENT");
  }
  // P2 polish alone does not block CUSTOMER_READY (release rules: no unresolved P0/P1).
  // Legitimate disclosed limitations (e.g. honest BPP rank blocker) → WITH_DISCLOSED_LIMITATIONS.
  if (p0.length || p1.length) {
    verdict = CUSTOMER_RELEASE_VERDICT.NOT_CUSTOMER_READY;
  } else if (disclosed.length) {
    verdict = CUSTOMER_RELEASE_VERDICT.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS;
  }

  return {
    gate: ADP_31_HOTEL_CUSTOMER_RELEASE_ACCEPTANCE_V1,
    propertyId,
    hotelName: profile?.name || propertyId,
    hpcId,
    stagedVersionId: staged?.versionId || null,
    corePeriodId: core?.periodId || null,
    publishedVersionId: published?.latestVersionId || null,
    publicationAuthorized: false,
    measurementPresent: Boolean(core?.observations?.length),
    realityGapV2: rg.gapScore ?? null,
    realityGapLive: liveRg.ok ? liveRg.gapScoreV2 : null,
    bppRank,
    editorial,
    defects,
    p0Count: p0.length,
    p1Count: p1.length,
    p2Count: p2.length,
    disclosedLimitations: disclosed,
    customerReleaseVerdict: verdict,
    founderReviewSeparate: true,
  };
}

export function certifyCanonical31CustomerReleaseAcceptance(perHotelResults) {
  const rows = perHotelResults || [];
  const counts = {
    CUSTOMER_READY: rows.filter(
      (r) => r.customerReleaseVerdict === CUSTOMER_RELEASE_VERDICT.CUSTOMER_READY
    ).length,
    CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS: rows.filter(
      (r) =>
        r.customerReleaseVerdict ===
        CUSTOMER_RELEASE_VERDICT.CUSTOMER_READY_WITH_DISCLOSED_LIMITATIONS
    ).length,
    NOT_CUSTOMER_READY: rows.filter(
      (r) =>
        r.customerReleaseVerdict === CUSTOMER_RELEASE_VERDICT.NOT_CUSTOMER_READY
    ).length,
  };
  const universe = listCanonicalAdp31PropertyIds();
  return {
    gate: ADP_31_HOTEL_CUSTOMER_RELEASE_ACCEPTANCE_V1,
    universeSize: universe.length,
    syntheticInUniverse: universe.some((id) =>
      isAdpSyntheticOrTestPropertyId(id)
    ),
    counts,
    ok:
      universe.length === 31 &&
      rows.length === 31 &&
      counts.NOT_CUSTOMER_READY === 0,
    rows,
  };
}
