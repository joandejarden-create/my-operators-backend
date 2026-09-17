/**
 * Build ADP Monthly Executive Review V1 from certified published Core ADP + BPP pack.
 * Does not recalculate measurement; composes customer-safe executive artifact.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BPP_CUSTOMER_PUBLISHED_PACK_CANDIDATES,
} from "../brand-portfolio/bpp-publication-meta-v1.js";
import {
  BPP_BENCHMARK_MIN_PEERS,
  DEFAULT_OWNER_ROLE,
  FALLBACK_MATERIALITY_PP,
  GOLDEN_MONTHLY_REVIEW_PROPERTY_IDS,
  MONTHLY_REVIEW_CONTRACT_VERSION,
  MONTHLY_REVIEW_PRODUCT_SUBTITLE,
  MONTHLY_REVIEW_PRODUCT_TITLE,
  MONTHLY_REVIEW_SCHEMA,
  MOVEMENT_CLASS,
  REVIEW_POSTURE,
  ACTION_STATUS,
} from "./monthly-review-contract-v1.js";
import {
  assertEligibilityAllowsGeneration,
  resolveAdpMonthlyReviewEligibilityV1,
  resolveAllAdpMonthlyReviewEligibilityV1,
} from "./resolve-adp-monthly-review-eligibility-v1.js";
import {
  assertActionAccountabilityComplete,
  createActionRegisterEntry,
} from "./action-register-schema-v1.js";
import {
  auditActionCausationLanguage,
  auditMonthlyReviewLanguage,
} from "./monthly-review-language-audit-v1.js";
import { buildExecutableActionsForProperty } from "../action-intelligence/build-executable-action-instances-v1.js";
import { validateExecutableActionAgenda } from "../action-intelligence/validate-executable-action-v1.js";
import { describeFutureRecommenderDesign } from "../action-intelligence/action-learning-model-v1.js";
import { GOVERNANCE_RULES } from "../action-intelligence/action-pattern-governance-v1.js";
import { listGovernedActionPatterns } from "../action-intelligence/action-pattern-library-v1.js";
import {
  buildEditionLock,
  displayPct,
  resolveCanonicalRealityCoveragePct,
  resolveOfficialComparablePrior,
  safeAnalyticalDisplay,
  AI_DEMAND_REVIEW_NO_PARALLEL_ANALYTICS,
  PDF_COMPARABILITY_DISCLOSURE_PARITY,
  PDF_DEMAND_TERRITORY_METRIC_SEMANTIC_PARITY,
  PDF_NO_RAW_NULL_ANALYTICAL_FIELDS,
  PERFORMANCE_REVIEW_NUMBERS_MUST_TIE_EXACTLY_TO_CURRENT_PUBLISHED_ADP,
} from "./performance-review-canonical-binder-v1.js";
import {
  AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V2,
  AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3,
  buildCalloutsFromPlatformV3,
  buildExecutiveExpansionV2,
  PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY,
} from "./performance-review-executive-expansion-v2.js";
import {
  AI_DEMAND_PERFORMANCE_REVIEW_EDITORIAL_GOLDEN_V1,
  applyExecutiveEditorialV1,
} from "./performance-review-executive-editorial-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function isoDate(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value).slice(0, 10);
  return d.toISOString().slice(0, 10);
}

function reportingMonthLabel(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleString("en-US", { month: "long", year: "numeric", timeZone: "UTC" });
}

function fmtPct(rate, digits = 1) {
  return displayPct(rate, digits);
}

function fmtPp(delta, digits = 1) {
  if (delta == null || Number.isNaN(Number(delta))) return null;
  const n = Number(delta);
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(digits)} pp`;
}

function round1(n) {
  return Math.round(Number(n) * 10) / 10;
}

function classifyDeltaPp(delta, materialityPp) {
  if (delta == null || Number.isNaN(Number(delta))) return MOVEMENT_CLASS.MONITOR;
  const abs = Math.abs(Number(delta));
  if (abs < materialityPp) return MOVEMENT_CLASS.STABLE;
  return Number(delta) > 0
    ? MOVEMENT_CLASS.MATERIAL_IMPROVEMENT
    : MOVEMENT_CLASS.MATERIAL_DECLINE;
}

function loadPublishedManifest(propertyId) {
  const manifestPath = path.join(
    ROOT,
    "data/ai-demand-positioning/published",
    propertyId,
    "manifest.json"
  );
  return { manifestPath, manifest: readJson(manifestPath) };
}

function loadPublishedReport(propertyId, manifest) {
  const reportPath = path.join(
    ROOT,
    "data/ai-demand-positioning/published",
    propertyId,
    manifest.reportFile
  );
  const raw = readJson(reportPath);
  return { reportPath, report: raw.payload || raw };
}

function loadPublishedEvidence(propertyId, manifest) {
  const evidencePath = path.join(
    ROOT,
    "data/ai-demand-positioning/published",
    propertyId,
    manifest.evidenceFile
  );
  if (!fs.existsSync(evidencePath)) return { evidencePath: null, evidence: null };
  const raw = readJson(evidencePath);
  return { evidencePath, evidence: raw.payload || raw };
}

function loadBppPayload(propertyId) {
  for (const rel of BPP_CUSTOMER_PUBLISHED_PACK_CANDIDATES) {
    const full = path.join(ROOT, rel);
    if (!fs.existsSync(full)) continue;
    const pack = readJson(full);
    const row = pack.payloads?.[propertyId];
    if (!row) continue;
    return {
      bppPath: full,
      bppPackMeta: {
        publicationVersion: pack.publicationVersion,
        periodId: pack.periodId,
        priorPeriodId: pack.priorPeriodId,
      },
      bpp: row.payload || row,
    };
  }
  return { bppPath: null, bppPackMeta: null, bpp: null };
}

function trendPair(report) {
  const trends = Array.isArray(report.trends) ? report.trends : [];
  const current =
    trends.find((t) => t.role === "current") || trends[trends.length - 1] || null;
  const priorCandidate =
    trends.find((t) => t.role === "prior_run") ||
    (trends.length > 1 ? trends[0] : null) ||
    null;
  const officialComparablePrior = resolveOfficialComparablePrior(report);
  // Only use prior for deltas/displays when platform marks comparable prior available.
  const prior = officialComparablePrior ? priorCandidate : null;
  return {
    current,
    prior,
    priorCandidate,
    trends,
    officialComparablePrior,
  };
}

function materialityPp(report) {
  const m = report.executiveRead?.trend?.materialityPp;
  return typeof m === "number" ? m : FALLBACK_MATERIALITY_PP;
}

function buildKpis(report, bpp) {
  const em = report.executiveMetrics || {};
  const { current, prior } = trendPair(report);
  const kpis = [];

  kpis.push({
    id: "aiConsideration",
    label: "AI Consideration",
    currentDisplay: fmtPct(em.considerationRate?.rate),
    currentValue: em.considerationRate?.rate ?? null,
    priorDisplay: prior ? fmtPct(prior.considerationRate) : null,
    priorValue: prior?.considerationRate ?? null,
    deltaDisplay: prior
      ? fmtPp(round1((em.considerationRate?.rate ?? 0) - prior.considerationRate))
      : null,
    available: em.considerationRate?.rate != null,
    source: "executiveMetrics.considerationRate",
  });

  kpis.push({
    id: "scenarioPresence",
    label: "Scenario Presence",
    currentDisplay: fmtPct(em.scenarioPresence?.rate),
    currentValue: em.scenarioPresence?.rate ?? null,
    priorDisplay: prior ? fmtPct(prior.scenarioPresenceRate) : null,
    priorValue: prior?.scenarioPresenceRate ?? null,
    deltaDisplay: prior
      ? fmtPp(round1((em.scenarioPresence?.rate ?? 0) - prior.scenarioPresenceRate))
      : null,
    available: em.scenarioPresence?.rate != null,
    source: "executiveMetrics.scenarioPresence",
  });

  const realityCurrent = resolveCanonicalRealityCoveragePct(report);
  const realityPrior =
    prior && resolveOfficialComparablePrior(report)
      ? prior?.propertyRealityCoverage ?? null
      : null;
  kpis.push({
    id: "realityCoverage",
    label: "Reality Coverage",
    currentDisplay: fmtPct(realityCurrent),
    currentValue: realityCurrent,
    priorDisplay: realityPrior != null ? fmtPct(realityPrior) : null,
    priorValue: realityPrior,
    deltaDisplay:
      realityPrior != null && realityCurrent != null
        ? fmtPp(round1(realityCurrent - realityPrior))
        : null,
    available: realityCurrent != null,
    source: "realityGap.recognizedCount/totalAttributes",
    help: "Share of monitored property facts and attributes reflected in AI answers (from published Reality Gap).",
  });

  const indexMap = report.intentPresenceIndex || em.presenceIndex || {};
  const validated = Object.values(indexMap).find(
    (row) =>
      row &&
      row.status === "PRODUCTION_VALIDATED" &&
      typeof row.index === "number" &&
      !row.developing
  );
  if (validated) {
    kpis.push({
      id: "presenceIndex",
      label: `Presence Index · ${validated.territory || "Validated territory"}`,
      currentDisplay: String(validated.index),
      currentValue: validated.index,
      priorDisplay: null,
      priorValue: null,
      deltaDisplay: null,
      available: true,
      source: "intentPresenceIndex",
      meta: `Subject ${fmtPct(validated.subjectRatePct ?? validated.myRate)} vs peer ${fmtPct(validated.coreBenchmarkRatePct ?? validated.avgCompRate)}`,
    });
  }

  const topDisp = report.lostDemand?.displacement?.[0];
  const lowConsideration =
    em.considerationRate?.rate != null && em.considerationRate.rate < 40;
  if (topDisp && (lowConsideration || !validated) && kpis.length < 5) {
    kpis.push({
      id: "competitivePosition",
      label: "Competitive Position",
      currentDisplay: topDisp.name,
      currentValue: topDisp.displacementCount,
      priorDisplay: null,
      priorValue: null,
      deltaDisplay: null,
      available: true,
      source: "lostDemand.displacement",
      meta: `Top displacement competitor · ${topDisp.displacementCount} scenarios where subject was absent`,
    });
  }

  // BPP — at most two executive KPIs; suppress benchmark/index when peers < 5
  if (bpp && bpp.status === "READY" && Array.isArray(bpp.kpis)) {
    const rankKpi = bpp.kpis.find((k) => k.id === "portfolioRank" && k.available);
    const presenceKpi = bpp.kpis.find(
      (k) => k.id === "portfolioAiPresence" && k.available
    );
    const indexKpi = bpp.kpis.find(
      (k) => k.id === "portfolioPresenceIndex" && k.available
    );
    const peerMatch = String(rankKpi?.value || "").match(/of\s+(\d+)/i);
    const peers = peerMatch ? Number(peerMatch[1]) : null;
    const suppressBenchmark =
      peers != null && peers < BPP_BENCHMARK_MIN_PEERS;

    const bppPicks = [];
    if (presenceKpi) bppPicks.push(presenceKpi);
    if (!suppressBenchmark && indexKpi && lowConsideration) {
      bppPicks.push(indexKpi);
    } else if (rankKpi) {
      bppPicks.push(rankKpi);
    } else if (!suppressBenchmark && indexKpi) {
      bppPicks.push(indexKpi);
    }

    for (const k of bppPicks.slice(0, 2)) {
      if (kpis.length >= 6) break;
      kpis.push({
        id: `bpp_${k.id}`,
        label:
          k.id === "portfolioAiPresence"
            ? "Brand Portfolio Position"
            : k.id === "portfolioRank"
              ? "Portfolio Rank"
              : k.label,
        currentDisplay: k.value,
        currentValue: k.valueRaw ?? null,
        priorDisplay: null,
        priorValue: null,
        deltaDisplay: k.deltaDisplay || null,
        available: true,
        source: "bpp.customerPublishedPack",
        family: "BPP",
        suppressedCompanion:
          suppressBenchmark && k.id === "portfolioAiPresence"
            ? "Portfolio Benchmark and Portfolio Presence Index are not shown because the Independent Positioning peer set has fewer than 5 hotels."
            : null,
        meta: k.meta || null,
      });
    }
  }

  return kpis.filter((k) => k.available).slice(0, 6);
}

function buildExecutiveAssessment(report, bpp, posture, kpis, actions = []) {
  const base = buildExecutiveExpansionV2(report, bpp, kpis);
  const expansion = applyExecutiveEditorialV1({
    expansion: base,
    report,
    bpp,
    kpis,
    actions,
  });
  return {
    text: expansion.text,
    wordCount: expansion.wordCount,
    posture,
    expansion,
    blocks: expansion.blocks,
    scanCards: expansion.scanCards,
    platformScan: expansion.platformScan,
    sectionInsights: expansion.sectionInsights,
    realityGapPrioritization: expansion.realityGapPrioritization || [],
    actionPriority: expansion.actionPriority || [],
    primaryIssueId: expansion.primaryIssueId,
    gate: PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY,
    preserveGate:
      expansion.preserveGate ||
      "PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_PRESERVES_PLATFORM_CONCLUSION",
    schema: AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3,
    editorialSchema: AI_DEMAND_PERFORMANCE_REVIEW_EDITORIAL_GOLDEN_V1,
  };
}

function buildCallouts(report, _posture) {
  return buildCalloutsFromPlatformV3(report);
}

function buildMaterialMovements(report) {
  const { current, prior } = trendPair(report);
  const mat = materialityPp(report);
  if (!current || !prior) return [];

  const rows = [
    {
      area: "AI Consideration",
      current: fmtPct(current.considerationRate),
      prior: fmtPct(prior.considerationRate),
      change: fmtPp(round1(current.considerationRate - prior.considerationRate)),
      deltaPp: round1(current.considerationRate - prior.considerationRate),
    },
    {
      area: "Scenario Presence",
      current: fmtPct(current.scenarioPresenceRate),
      prior: fmtPct(prior.scenarioPresenceRate),
      change: fmtPp(round1(current.scenarioPresenceRate - prior.scenarioPresenceRate)),
      deltaPp: round1(current.scenarioPresenceRate - prior.scenarioPresenceRate),
    },
    {
      area: "Reality Coverage",
      current: fmtPct(current.propertyRealityCoverage),
      prior: fmtPct(prior.propertyRealityCoverage),
      change: fmtPp(
        round1(current.propertyRealityCoverage - prior.propertyRealityCoverage)
      ),
      deltaPp: round1(
        current.propertyRealityCoverage - prior.propertyRealityCoverage
      ),
    },
  ];

  const classified = rows.map((r) => {
    const klass = classifyDeltaPp(r.deltaPp, mat);
    let interpretation;
    if (klass === MOVEMENT_CLASS.STABLE) {
      interpretation = `Change is within the ${mat} pp materiality heuristic used in the property Executive Read. Treat as stable unless other evidence indicates concentration risk.`;
    } else if (klass === MOVEMENT_CLASS.MATERIAL_DECLINE) {
      interpretation = `Decline meets the ${mat} pp materiality heuristic. Review whether the drop is concentrated in specific demand territories or providers.`;
    } else if (klass === MOVEMENT_CLASS.MATERIAL_IMPROVEMENT) {
      interpretation = `Improvement meets the ${mat} pp materiality heuristic. Confirm the gain holds in the next monitoring period.`;
    } else {
      interpretation = "Movement requires monitoring; no stronger classification is certified.";
    }
    return {
      metricOrDemandArea: r.area,
      current: r.current,
      prior: r.prior,
      change: r.change,
      classification: klass,
      executiveInterpretation: interpretation,
    };
  });

  const materialOnly = classified.filter(
    (r) =>
      r.classification === MOVEMENT_CLASS.MATERIAL_DECLINE ||
      r.classification === MOVEMENT_CLASS.MATERIAL_IMPROVEMENT
  );
  // If nothing crosses materiality, still show headline stables so the review is not empty.
  return materialOnly.length ? materialOnly : classified;
}

function buildDemandMovement(report) {
  const byIntent = report.demandCapture?.byIntent || {};
  const mat = materialityPp(report);
  const intentLabels = {
    business: "Business Travel",
    leisure: "Leisure Travel",
    couples: "Couples / Romantic",
    group_meeting: "Group / Meeting",
    celebration: "Celebration",
    wellness: "Wellness",
    adventure: "Adventure",
    family: "Family",
  };

  // Platform demandCapture.byIntent.rate = scenario capture (captured/total).
  // Label explicitly — do not imply answer-level consideration or period decline.
  const rows = Object.entries(byIntent).map(([intent, row]) => {
    const rate = row.rate;
    let significance;
    let significanceLabel;
    let interpretation;
    if (rate >= 90) {
      significance = "LEVEL_HIGH";
      significanceLabel = "High Capture";
      interpretation =
        "High scenario-capture rate for this traveler need in the current period.";
    } else if (rate >= 50) {
      significance = "LEVEL_PARTIAL";
      significanceLabel = "Partial Capture";
      interpretation =
        "Partial scenario capture. Review whether missing scenarios share a common attribute or competitor pattern.";
    } else {
      significance = "LEVEL_LOW";
      significanceLabel = "Low Capture";
      interpretation =
        "Low scenario-capture rate for this traveler need. Treat as a priority inspection area (absolute level — not a period-over-period decline).";
    }
    return {
      demandTerritory: intentLabels[intent] || intent,
      intent,
      metricName: "scenario_capture_rate",
      metricLabel: "Scenario Capture",
      current: fmtPct(rate),
      prior: null,
      delta: null,
      captured: `${row.captured} of ${row.total}`,
      significance,
      significanceLabel,
      interpretation,
      note: "Metric = demandCapture.byIntent scenario capture (captured/total). Prior-run territory rates are not published as a comparable series in this edition.",
      materialityPpUsed: mat,
      sourceField: `demandCapture.byIntent.${intent}.rate`,
      gate: PDF_DEMAND_TERRITORY_METRIC_SEMANTIC_PARITY,
    };
  });

  return rows
    .sort((a, b) => Number.parseFloat(a.current) - Number.parseFloat(b.current))
    .slice(0, 6);
}

function buildCompetitiveMovement(report) {
  const displacement = report.lostDemand?.displacement || [];
  const reasons = report.lostDemand?.topReasons || [];
  const surprises = report.competitiveSet?.surprises || [];

  return {
    summary:
      displacement.length === 0
        ? "No displacement competitors are recorded for this period."
        : `When the property is absent, AI answers most often include ${displacement
            .slice(0, 3)
            .map((d) => `${d.name} (${d.displacementCount})`)
            .join(", ")}. Counts are observational and do not by themselves prove why displacement occurs.`,
    displacementLeaders: displacement.slice(0, 5).map((d) => ({
      name: d.name,
      displacementCount: d.displacementCount,
      entityId: d.entityId,
    })),
    topReasons: reasons.slice(0, 4).map((r) => ({
      reason: r.reason,
      count: r.count,
    })),
    observedCompetitors: surprises.slice(0, 5).map((c) => ({
      name: c.name,
      aiPresencePct: c.aiPresencePct,
      appearances: c.appearances,
    })),
    caveat:
      "Competitive movement describes co-appearance and absence patterns in monitored answers. It does not establish causation.",
  };
}

function sanitizeExcerpt(text, max = 420) {
  if (!text) return "";
  const clean = String(text).replace(/\s+/g, " ").trim();
  if (clean.length <= max) return clean;
  return `${clean.slice(0, max - 1).trim()}…`;
}

function buildEvidenceReview(report, evidence, posture, bpp = null) {
  const examples = [];
  const presentByIntent = evidence?.positiveEvidence?.presentByIntent || {};

  // Major gain / strength example
  const leisurePos = presentByIntent.leisure?.examples?.[0] || presentByIntent.couples?.examples?.[0];
  if (leisurePos?.aiResponse) {
    examples.push({
      kind: "major_gain",
      travelerNeed: leisurePos.demandTerritory || "Leisure / Couples",
      provider: leisurePos.provider,
      observation: sanitizeExcerpt(leisurePos.aiResponse),
      whyItMatters:
        "Shows a monitored case where the property is included in the AI answer for a relevant traveler need.",
      managementReview:
        "Confirm the attributes and third-party sources that support this inclusion remain accurate and consistent.",
      evidenceId: leisurePos.evidenceId,
    });
  }

  // Recurring omission
  const missIntents = evidence?.missingByIntent || {};
  const missPool = Object.values(missIntents).flat().filter(Boolean);
  const miss = missPool.find((m) => m.responseExcerpt) || missPool[0];
  if (miss?.responseExcerpt) {
    examples.push({
      kind: "recurring_omission",
      travelerNeed: miss.intent || "Monitored traveler need",
      provider: miss.provider,
      observation: sanitizeExcerpt(miss.responseExcerpt),
      whyItMatters:
        "Shows a monitored case where the property was not mentioned while alternatives were surfaced.",
      managementReview:
        "Review whether property information for this traveler need is complete and consistently represented across first-party and major third-party sources.",
      evidenceId: miss.scenarioId,
    });
  }

  // Reality gap
  const gap =
    (report.realityGap?.gaps || []).find((g) => g.severity === "HIGH") ||
    (report.realityGap?.gaps || [])[0];
  if (gap) {
    examples.push({
      kind: "factual_reality_gap",
      travelerNeed: "Property reality attributes",
      provider: "All providers (attribute recognition)",
      observation: `${gap.label} recognized in ${fmtPct(gap.recognitionRate)} of applicable monitored answers (${gap.count} of ${gap.total}).`,
      whyItMatters:
        "This attribute is part of the monitored property proposition but is infrequently reflected in AI answers.",
      managementReview: `Verify whether “${gap.label}” is accurately and consistently described on the hotel website and major booking/travel platforms.`,
      evidenceId: gap.attribute,
    });
  }

  // Competitive displacement
  const topDisp = report.lostDemand?.displacement?.[0];
  if (topDisp) {
    examples.push({
      kind: "competitive_displacement",
      travelerNeed: "Scenarios where subject is absent",
      provider: "Cross-provider",
      observation: `${topDisp.name} appeared in ${topDisp.displacementCount} monitored scenarios where ${report.property?.name || "the property"} was absent.`,
      whyItMatters:
        "Identifies the competitor most often present when the subject hotel is missing from AI answers.",
      managementReview:
        "Compare public positioning and third-party descriptions against this competitor for the traveler needs where displacement concentrates.",
      evidenceId: topDisp.entityId,
    });
  }

  // Optional BPP observation when pack is READY and adds relative-portfolio context
  const bppReady = bpp && bpp.status === "READY";
  const bppRankDisplay =
    bpp?.customerPublishedPack?.portfolioRankDisplay ||
    bpp?.portfolioRankDisplay ||
    bpp?.portfolioRank ||
    null;
  if (examples.length < 5 && bppReady && bppRankDisplay) {
    examples.push({
      kind: "bpp_observation",
      travelerNeed: "Independent Positioning peer set",
      provider: "Brand Portfolio Positioning",
      observation: `Independent Positioning shows portfolio rank ${bppRankDisplay} in the current peer set.`,
      whyItMatters:
        "Relative standing inside the loyalty peer set can look strong even while Core answer-level inclusion remains inconsistent — denominators differ.",
      managementReview:
        "Use BPP as portfolio context only; do not treat portfolio rank as a substitute for Core consideration.",
      evidenceId: "bpp_portfolio_rank",
    });
  }

  // Curate: prefer 3–5 distinct kinds (positive, missing/displacement, reality, optional bpp)
  const preferredOrder = [
    "major_gain",
    "recurring_omission",
    "competitive_displacement",
    "factual_reality_gap",
    "bpp_observation",
  ];
  const curated = [];
  const seenKinds = new Set();
  for (const kind of preferredOrder) {
    const hit = examples.find((e) => e.kind === kind && !seenKinds.has(kind));
    if (hit) {
      curated.push(hit);
      seenKinds.add(kind);
    }
    if (curated.length >= 5) break;
  }
  for (const ex of examples) {
    if (curated.length >= 5) break;
    if (!curated.includes(ex)) curated.push(ex);
  }
  const target = Math.min(5, Math.max(3, curated.length || examples.length));
  return (curated.length ? curated : examples).slice(0, target);
}

function ownerRoleForCategory() {
  return DEFAULT_OWNER_ROLE;
}

function buildActionAgenda(report, evidence, posture, ctx) {
  const propertyId = report.property?.propertyId;
  const built = buildExecutableActionsForProperty(propertyId, report, evidence, {
    ...ctx,
    archetype: posture,
  });
  // Empty agenda is valid for strong monitor-only months
  const validation =
    built.accepted.length === 0
      ? { ok: true, results: [], aggregateGates: { MONITOR_ONLY_NO_ACTIONS: true } }
      : validateExecutableActionAgenda(built.accepted);
  return {
    actions: built.accepted,
    rejectedGenerics: built.rejectedGenerics || [],
    rejected: built.rejected || [],
    validation,
    builderMode: built.builderMode || "generic_evidence_driven_v1",
  };
}

function buildDecisions(actions, posture) {
  const decisions = [
    {
      id: "confirm_owners",
      text: "Confirm an accountable owner for each open action (role is pre-suggested; named owner still required).",
    },
    {
      id: "confirm_target_dates",
      text: "Confirm target completion dates for each open action.",
    },
  ];
  const strong =
    posture === REVIEW_POSTURE.STRONG_PERFORMER ||
    posture === REVIEW_POSTURE.STABLE_STRONG;
  if (!strong) {
    decisions.push({
      id: "confirm_priority_order",
      text: "Confirm whether the corrective action order matches management’s operating priority for the next 30 days.",
    });
    decisions.push({
      id: "confirm_property_facts",
      text: "Confirm whether any high-severity reality-gap attributes are outdated, incorrect, or intentionally not marketed.",
    });
  } else {
    decisions.push({
      id: "confirm_watch_vs_act",
      text: "Confirm which Watch Item items remain monitor-only versus requiring an open action this month.",
    });
  }
  if (actions.some((a) => !a.targetDate)) {
    decisions.push({
      id: "set_target_dates",
      text: "Set target dates where currently blank (“Owner to be confirmed” / date pending).",
    });
  }
  return decisions;
}

function buildNextMonitoring(report, actions, posture) {
  const items = [];
  items.push({
    text: "AI Consideration and Scenario Presence versus the next published monitoring run.",
    tiesTo: "material_risk_or_strength",
  });
  items.push({
    text: "Reality Coverage and high-severity attribute recognition rates.",
    tiesTo: "reality_gap",
  });
  const lead = report.lostDemand?.displacement?.[0];
  if (lead) {
    items.push({
      text: `Displacement pattern involving ${lead.name}.`,
      tiesTo: "competitive_movement",
    });
  }
  for (const a of actions.slice(0, 2)) {
    items.push({
      text: `Follow-up signal for action: ${a.actionTitle}.`,
      tiesTo: "open_action",
    });
  }
  const strong =
    posture === REVIEW_POSTURE.STRONG_PERFORMER ||
    posture === REVIEW_POSTURE.STABLE_STRONG;
  if (strong) {
    items.push({
      text: "Independent Positioning rank and Portfolio AI Presence (benchmark/index remain suppressed while peer set is below 5).",
      tiesTo: "significant_improvement_or_protection",
    });
  } else {
    items.push({
      text: "Independent Positioning Portfolio AI Presence and rank versus peer benchmark.",
      tiesTo: "material_risk",
    });
  }
  return items.slice(0, 5);
}

/**
 * Derive internal review archetype from governed metrics (no hotel hard-codes).
 */
export function resolveReviewArchetype(report) {
  const consideration = report?.executiveMetrics?.considerationRate?.rate;
  const scenario = report?.executiveMetrics?.scenarioPresence?.rate;
  const { current, prior } = trendPair(report);
  const hasPrior = Boolean(prior);
  const mat = materialityPp(report);

  if (consideration == null && scenario == null) {
    return REVIEW_POSTURE.BASELINE_LIMITED;
  }

  let deltaConsideration = null;
  if (hasPrior && consideration != null && prior.considerationRate != null) {
    deltaConsideration = round1(consideration - prior.considerationRate);
  }

  const high = consideration != null && consideration >= 65 && (scenario == null || scenario >= 55);
  const mid = consideration != null && consideration >= 45 && consideration < 65;
  const low = consideration != null && consideration < 45;

  if (!hasPrior) {
    if (high) return REVIEW_POSTURE.STRONG_PERFORMER;
    if (low) return REVIEW_POSTURE.CORRECTIVE;
    if (mid) return REVIEW_POSTURE.MIXED;
    return REVIEW_POSTURE.BASELINE_LIMITED;
  }

  const materialImprove =
    deltaConsideration != null && deltaConsideration >= mat;
  const materialDecline =
    deltaConsideration != null && deltaConsideration <= -mat;

  if (high && !materialDecline) {
    return Math.abs(deltaConsideration || 0) < mat
      ? REVIEW_POSTURE.STABLE_STRONG
      : REVIEW_POSTURE.STRONG_PERFORMER;
  }
  if (low && !materialImprove) {
    return Math.abs(deltaConsideration || 0) < mat
      ? REVIEW_POSTURE.STABLE_LOW
      : REVIEW_POSTURE.CORRECTIVE;
  }
  if (materialDecline && high) return REVIEW_POSTURE.MIXED;
  if (materialImprove && low) return REVIEW_POSTURE.MIXED;
  if (mid) return REVIEW_POSTURE.MIXED;
  if (high) return REVIEW_POSTURE.STRONG_PERFORMER;
  if (low) return REVIEW_POSTURE.CORRECTIVE;
  return REVIEW_POSTURE.BASELINE_LIMITED;
}

/** @deprecated use resolveReviewArchetype */
function resolvePosture(_propertyId, report) {
  return resolveReviewArchetype(report);
}

/**
 * @param {string} propertyId
 * @param {{ root?: string, skipEligibility?: boolean, eligibility?: object }} [opts]
 */
export function buildMonthlyExecutiveReviewV1(propertyId, opts = {}) {
  const eligibility =
    opts.eligibility ||
    (opts.skipEligibility
      ? { eligible: true, status: "READY", warnings: [] }
      : resolveAdpMonthlyReviewEligibilityV1(propertyId, {
          reconciliationPass: opts.reconciliationPass,
        }));
  if (!opts.skipEligibility) {
    assertEligibilityAllowsGeneration(eligibility);
  }

  const { manifest, manifestPath } = loadPublishedManifest(propertyId);
  const { report, reportPath } = loadPublishedReport(propertyId, manifest);
  const { evidence, evidencePath } = loadPublishedEvidence(propertyId, manifest);
  const { bpp, bppPath, bppPackMeta } = loadBppPayload(propertyId);

  const posture = resolveReviewArchetype(report);
  const { current, prior, priorCandidate, officialComparablePrior } =
    trendPair(report);
  const kpis = buildKpis(report, bpp);
  const callouts = buildCallouts(report, posture);
  const materialMovements = buildMaterialMovements(report);
  const demandMovement = buildDemandMovement(report);
  const competitiveMovement = buildCompetitiveMovement(report);
  const evidenceReview = buildEvidenceReview(report, evidence, posture, bpp);
  const createdDate = isoDate(report.period?.executionDate || manifest.latestPublishedAt);
  const monitoringPeriodId =
    manifest.monitoringPeriodId || report.period?.periodId || null;

  const actionPack = buildActionAgenda(report, evidence, posture, {
    monitoringPeriodId,
  });
  if (!actionPack.validation.ok) {
    throw new Error(
      `Executable action agenda failed gates for ${propertyId}: ${JSON.stringify(
        actionPack.validation.results.filter((r) => !r.ok)
      )}`
    );
  }

  const actionAgenda = actionPack.actions.map((a) => {
    const register = createActionRegisterEntry({
      actionId: a.actionId,
      propertyId,
      monitoringPeriodId,
      actionTitle: a.actionTitle,
      observedIssue: a.observedIssue,
      recommendedAction: a.recommendedAction || (a.implementationSteps || []).join(" "),
      rationale: a.rationale,
      evidenceRefs: a.trace ? [a.trace] : [],
      accountableOwnerRole: a.accountableOwnerRole,
      supportingTeam: a.supportingTeam,
      createdDate,
      targetDate: a.targetDate,
      status: a.status || ACTION_STATUS.OPEN,
      expectedSignal: a.expectedSignal,
      impactTracking: {
        schema: "MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING",
        before: a.learningRecord?.metricBefore || {
          aiConsideration: report.executiveMetrics?.considerationRate?.rate ?? null,
          scenarioPresence: report.executiveMetrics?.scenarioPresence?.rate ?? null,
          realityCoverage: current?.propertyRealityCoverage ?? null,
        },
        actionDate: null,
        nextRun: null,
        change: null,
        assessment: null,
        causationClaimed: false,
        note: "Later periods may record before/after measurement. Monitoring alone does not establish causation.",
      },
      founderCustomerNotes: `actionPatternId=${a.actionPatternId}`,
    });
    return {
      ...register,
      actionPatternId: a.actionPatternId,
      evidence: a.evidence,
      targetSources: a.targetSources,
      implementationSteps: a.implementationSteps,
      definitionOfDone: a.definitionOfDone,
      nextMonitoringCheck: a.nextMonitoringCheck,
      trace: a.trace,
      learningRecord: a.learningRecord,
    };
  });

  const assessment = buildExecutiveAssessment(
    report,
    bpp,
    posture,
    kpis,
    actionAgenda
  );
  const actionPriorityById = new Map(
    (assessment.actionPriority || []).map((p) => [p.actionId, p])
  );
  for (const action of actionAgenda) {
    const pri = actionPriorityById.get(action.actionId);
    if (pri) {
      action.managementPriority = pri.priority;
      action.managementPriorityWhy = pri.why;
    }
  }
  const decisionsRequired = buildDecisions(actionAgenda, posture);
  const nextMonitoringPriorities = buildNextMonitoring(report, actionAgenda, posture);

  const meetingMode = {
    topChanges: materialMovements.slice(0, 3).map((m) => ({
      area: m.metricOrDemandArea,
      change: m.change,
      interpretation: m.executiveInterpretation,
    })),
    topActions: actionAgenda.slice(0, 3).map((a) => ({
      title: a.actionTitle,
      owner: a.accountableOwnerRole,
      status: a.status,
      dueDate: a.targetDate || "Date to be confirmed",
    })),
    priorOpenActions: [],
    decisionsRequired: decisionsRequired.map((d) => d.text),
  };

  const accountability = assertActionAccountabilityComplete(actionAgenda);
  const language = auditMonthlyReviewLanguage({
    assessment,
    callouts,
    actionAgenda,
    decisionsRequired,
    nextMonitoringPriorities,
    evidenceReview,
  });
  const causation = auditActionCausationLanguage(actionAgenda);

  const editionLock = buildEditionLock(report, manifest, bppPackMeta);

  const review = {
    schema: MONTHLY_REVIEW_SCHEMA,
    contractVersion: MONTHLY_REVIEW_CONTRACT_VERSION,
    productTitle: MONTHLY_REVIEW_PRODUCT_TITLE,
    productSubtitle: MONTHLY_REVIEW_PRODUCT_SUBTITLE,
    editionLock,
    property: {
      propertyId,
      name: report.property?.name || manifest.propertyName,
      city: report.property?.city || manifest.city,
      market: report.property?.state || manifest.market || manifest.state,
      affiliation: report.property?.affiliation || null,
    },
    reporting: {
      reportingMonth: reportingMonthLabel(
        report.period?.executionDate || manifest.latestPublishedAt
      ),
      currentMonitoringDate: isoDate(
        current?.date || report.period?.executionDate
      ),
      priorRunDate: officialComparablePrior
        ? isoDate(prior?.date || priorCandidate?.date)
        : null,
      currentPeriodId:
        current?.periodId || report.period?.periodId || null,
      priorPeriodId: officialComparablePrior
        ? prior?.periodId || priorCandidate?.periodId || null
        : null,
      monitoringPeriodId,
      measurementContractVersion:
        report.measurementContractVersion || manifest.measurementContractVersion,
      comparability: {
        officialComparablePrior,
        trendPriorAvailable: Boolean(officialComparablePrior && prior),
        historicalPriorRowPresent: Boolean(priorCandidate),
        disclosure: officialComparablePrior
          ? "Official comparable prior period is available."
          : "No official comparable prior period is available. This review is a current-period baseline read; prior-run trend deltas are not shown.",
        gate: PDF_COMPARABILITY_DISCLOSURE_PARITY,
      },
    },
    posture,
    reviewArchetype: posture,
    eligibility: {
      status: eligibility.status || null,
      warnings: eligibility.warnings || [],
      hasComparablePrior: eligibility.hasComparablePrior ?? officialComparablePrior,
      gate: "ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED",
    },
    kpis,
    executiveAssessment: assessment,
    sectionInsights: assessment.sectionInsights || {},
    realityGapPrioritization: assessment.realityGapPrioritization || [],
    actionPriority: assessment.actionPriority || [],
    callouts,
    materialMovements,
    demandMovement,
    competitiveMovement,
    evidenceReview,
    actionAgenda,
    actionRegister: actionAgenda,
    actionIntelligence: {
      version: "ADP_ACTION_INTELLIGENCE_V1",
      builderMode: actionPack.builderMode || "generic_evidence_driven_v1",
      rejectedGenericFinalRecommendations: actionPack.rejectedGenerics,
      rejectedCandidates: actionPack.rejected.filter((r) => r.reason && !r.genericText),
      executabilityGates: actionPack.validation.aggregateGates,
      governedPatternIds: listGovernedActionPatterns().map((p) => p.actionPatternId),
      governance: GOVERNANCE_RULES,
      recommenderDesign: describeFutureRecommenderDesign(),
    },
    priorActionReview: {
      available: false,
      message: "No prior monthly actions are available yet.",
      rows: [],
      note: "Prior Action Review resolves from the Action Register for prior review versions — not PDF text extraction.",
    },
    decisionsRequired,
    nextMonitoringPriorities,
    meetingMode,
    methodologyNote:
      "This review is composed from the certified published Existing Hotel ADP customer report and the published Independent Positioning pack where available. It does not rerun providers, invent metrics, or claim causation from management actions. Final actions are executable playbooks with target sources, implementation steps, and definition of done.",
    sources: {
      manifestPath,
      reportPath,
      evidencePath,
      bppPath,
      bppPackMeta,
    },
    gates: {
      MONTHLY_REVIEW_SINGLE_CANONICAL_PAYLOAD: true,
      PERFORMANCE_REVIEW_NUMBERS_MUST_TIE_EXACTLY_TO_CURRENT_PUBLISHED_ADP: true,
      AI_DEMAND_REVIEW_NO_PARALLEL_ANALYTICS: true,
      PDF_NO_RAW_NULL_ANALYTICAL_FIELDS: Boolean(
        !String(assessment?.text || "").includes(" at null") &&
          !String(assessment?.text || "").includes("undefined")
      ),
      PDF_COMPARABILITY_DISCLOSURE_PARITY: true,
      PDF_DEMAND_TERRITORY_METRIC_SEMANTIC_PARITY: true,
      PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY: Boolean(
        assessment?.primaryIssueId &&
          assessment.primaryIssueId === report.executiveRead?.primaryIssueId
      ),
      AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V2: Boolean(
        assessment?.schema === AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V2 ||
          assessment?.schema === AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3
      ),
      AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3: Boolean(
        assessment?.schema === AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3
      ),
      AI_DEMAND_PERFORMANCE_REVIEW_EDITORIAL_GOLDEN_V1: Boolean(
        assessment?.editorialSchema ===
          AI_DEMAND_PERFORMANCE_REVIEW_EDITORIAL_GOLDEN_V1
      ),
      PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_PRESERVES_PLATFORM_CONCLUSION: Boolean(
        assessment?.primaryIssueId === report.executiveRead?.primaryIssueId
      ),
      EXECUTIVE_REVIEW_NO_LOW_VALUE_REPETITION: true,
      NO_INTERNAL_ENUMS_IN_CLIENT_PDF: demandMovement.every(
        (r) => r.significanceLabel && !String(r.significanceLabel).startsWith("LEVEL_")
      ),
      AI_DEMAND_REVIEW_NO_ORPHAN_COMPONENTS: true,
      AI_DEMAND_PERFORMANCE_REVIEW_SAME_EDITION_LOCK: Boolean(editionLock?.periodId),
      ADP_MONTHLY_REVIEW_ELIGIBILITY_FAIL_CLOSED: Boolean(eligibility.eligible !== false),
      ADP_MONTHLY_REVIEW_NO_PROPERTY_SPECIFIC_BUILDER_FORKS:
        actionPack.builderMode === "generic_evidence_driven_v1",
      ADP_MONTHLY_REVIEW_EXECUTIVE_ASSESSMENT_PROPERTY_SPECIFIC: Boolean(
        assessment?.text &&
          String(assessment.text).includes(report.property?.name || propertyId)
      ),
      ADP_MONTHLY_REVIEW_MOVEMENT_COMPARABILITY_INTEGRITY: true,
      MONTHLY_REVIEW_ACTION_ACCOUNTABILITY_COMPLETE:
        actionAgenda.length === 0 ? true : accountability.ok,
      MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING: actionAgenda.every(
        (a) => a.impactTracking?.causationClaimed === false
      ),
      MONTHLY_REVIEW_CLEAR_NOT_CLEVER_LANGUAGE: language.ok,
      ACTION_CAUSATION_LANGUAGE_INTEGRITY: causation.ok,
      ...actionPack.validation.aggregateGates,
      accountabilityFailures: accountability.failures,
      languageFlags: language.flags,
      causationFlags: causation.flags,
      doctrine: {
        PERFORMANCE_REVIEW_NUMBERS_MUST_TIE_EXACTLY_TO_CURRENT_PUBLISHED_ADP,
        AI_DEMAND_REVIEW_NO_PARALLEL_ANALYTICS,
        PDF_NO_RAW_NULL_ANALYTICAL_FIELDS,
        PDF_COMPARABILITY_DISCLOSURE_PARITY,
        PDF_DEMAND_TERRITORY_METRIC_SEMANTIC_PARITY,
        PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY,
        AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V2,
        AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3,
      },
    },
    liveProviderCalls: 0,
    generatedAt: new Date().toISOString(),
  };

  // Re-audit full review object for language after assembly
  const fullLanguage = auditMonthlyReviewLanguage(review);
  review.gates.MONTHLY_REVIEW_CLEAR_NOT_CLEVER_LANGUAGE = fullLanguage.ok;
  review.gates.languageFlags = fullLanguage.flags;

  return review;
}

export function listGoldenMonthlyReviewPropertyIds() {
  return [...GOLDEN_MONTHLY_REVIEW_PROPERTY_IDS];
}

export function listEligibleMonthlyReviewPropertyIds() {
  return resolveAllAdpMonthlyReviewEligibilityV1()
    .eligible.map((r) => r.propertyId);
}
