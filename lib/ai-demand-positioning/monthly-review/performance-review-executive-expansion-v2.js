/**
 * AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3
 *
 * Structured expansion of platform Executive Read V3 for the PDF product.
 * Doctrine:
 *   PLATFORM_EXECUTIVE_SUMMARY_IS_THE_ANALYTICAL_SPINE
 *   PERFORMANCE_REVIEW_EXPANDS_THE_SPINE
 *   EXPANSION_ADDS_CONTEXT_NOT_DIFFERENT_CONCLUSIONS
 *   PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_PRESERVES_PLATFORM_CONCLUSION
 *   EXECUTIVE_REVIEW_NO_LOW_VALUE_REPETITION
 */

import {
  displayPct,
  resolveCanonicalRealityCoveragePct,
  resolveOfficialComparablePrior,
} from "./performance-review-canonical-binder-v1.js";

export const AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V2 =
  "AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V2";
export const AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3 =
  "AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3";
export const PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY =
  "PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY";
export const PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_PRESERVES_PLATFORM_CONCLUSION =
  "PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_PRESERVES_PLATFORM_CONCLUSION";
export const EXECUTIVE_REVIEW_NO_LOW_VALUE_REPETITION =
  "EXECUTIVE_REVIEW_NO_LOW_VALUE_REPETITION";

const INTENT_LABELS = {
  business: "Business Travel",
  leisure: "Leisure Travel",
  couples: "Couples / Romantic",
  group_meeting: "Group / Meeting",
  family: "Family Travel",
  celebration: "Celebration",
  wellness: "Wellness",
  adventure: "Adventure",
};

function territorySoftness(report) {
  const byIntent = report?.demandCapture?.byIntent || {};
  const ranked = Object.entries(byIntent)
    .map(([intent, row]) => ({ intent, rate: row?.rate }))
    .filter((r) => typeof r.rate === "number")
    .sort((a, b) => a.rate - b.rate);
  if (!ranked.length) return null;
  const weakest = ranked[0];
  const strongest = ranked[ranked.length - 1];
  const strongBand = ranked.filter((r) => r.rate >= 85);
  return {
    weakestLabel: INTENT_LABELS[weakest.intent] || weakest.intent,
    weakestRate: weakest.rate,
    strongestLabel: INTENT_LABELS[strongest.intent] || strongest.intent,
    strongestRate: strongest.rate,
    strongLabels: strongBand.map((r) => INTENT_LABELS[r.intent] || r.intent),
    ranked,
  };
}

function highSeverityGaps(report) {
  return (report?.realityGap?.gaps || [])
    .filter((g) => String(g.severity || "").toUpperCase() === "HIGH")
    .map((g) => ({
      attribute: g.attribute,
      label: g.label || g.attribute,
      recognitionRate: g.recognitionRate,
    }));
}

/**
 * Concise demand insight for section 2 (generic from certified territory state).
 */
export function buildDemandMovementInsight(report) {
  const terr = territorySoftness(report);
  if (!terr) return null;
  const comparable = resolveOfficialComparablePrior(report);
  const strongList =
    terr.strongLabels.length >= 2
      ? terr.strongLabels.slice(0, 3)
      : [terr.strongestLabel];
  const strong =
    strongList.length === 1
      ? strongList[0]
      : strongList.length === 2
        ? `${strongList[0]} and ${strongList[1]}`
        : `${strongList.slice(0, -1).join(", ")}, and ${strongList[strongList.length - 1]}`;
  const verb = strongList.length === 1 ? "shows" : "show";
  let text = `${terr.weakestLabel} is the clearest current-period softness at ${displayPct(
    terr.weakestRate
  )} Scenario Capture. ${strong} ${verb} broader scenario coverage, suggesting the inclusion problem is concentrated rather than universal.`;
  if (!comparable) {
    text +=
      " These are absolute current-period levels — not period-over-period movement.";
  }
  return {
    text,
    metricName: "scenario_capture_rate",
    sourceField: "demandCapture.byIntent",
  };
}

/**
 * Competitive insight from canonical displacement leaders.
 */
export function buildCompetitiveMovementInsight(report) {
  const leaders = report?.lostDemand?.displacement || [];
  if (!leaders.length) return null;
  const top = leaders[0];
  const second = leaders[1];
  const third = leaders[2];
  const subject = report?.property?.name || "the property";
  const subjectBrand = String(report?.property?.affiliation || "")
    .toLowerCase()
    .split(/\s+/)[0];
  const sameBrand =
    subjectBrand &&
    leaders.filter((d) =>
      String(d.name || "")
        .toLowerCase()
        .includes(subjectBrand)
    ).length >= 1;
  let text = `${top.name} is the most frequently observed alternative when ${subject} is absent, appearing in ${top.displacementCount} displaced scenarios.`;
  if (second && third) {
    text += ` ${second.name} (${second.displacementCount}) and ${third.name} (${third.displacementCount}) also appear frequently`;
    if (sameBrand) {
      text +=
        ", indicating that displacement is spread across both same-brand and broader local alternatives.";
    } else {
      text +=
        ", indicating displacement is spread across multiple local alternatives rather than a single rival.";
    }
  } else if (second) {
    text += ` ${second.name} also appears frequently (${second.displacementCount}).`;
  }
  text +=
    " Counts are observational and do not establish why displacement occurs.";
  return { text, sourceField: "lostDemand.displacement" };
}

/**
 * BPP interpretation — reinforce / contrast / nuance vs Core.
 */
export function buildBppInterpretation(report, bpp, kpis = []) {
  if (!bpp || bpp.status !== "READY") return null;
  const bppRank = kpis.find((k) => k.id === "bpp_portfolioRank");
  const bppPresence = kpis.find((k) => k.id === "bpp_portfolioAiPresence");
  if (!bppRank && !bppPresence) return null;
  const consideration = report?.executiveMetrics?.considerationRate?.rate;
  const parts = [];
  if (bppRank && bppPresence) {
    parts.push(
      `Within the Independent Positioning peer set, the hotel ranks ${bppRank.currentDisplay} while Portfolio AI Presence is ${bppPresence.currentDisplay}.`
    );
  }
  if (
    bppPresence &&
    consideration != null &&
    Math.abs(Number(bppPresence.currentValue) * 100 - Number(consideration)) < 3
  ) {
    parts.push(
      "That portfolio presence level is directionally consistent with Core AI Consideration — relative standing inside the loyalty set can look strong even while absolute answer-level inclusion remains the binding constraint."
    );
  } else if (bppRank && consideration != null && consideration < 50) {
    parts.push(
      "Relative portfolio rank should not be read as a substitute for Core answer-level inclusion; denominators differ."
    );
  }
  if (!parts.length) return null;
  return {
    text: parts.join(" "),
    sourceField: "bpp.customerPublishedPack",
  };
}

/**
 * Reality Gap management translation.
 */
export function buildRealityGapInterpretation(report) {
  const reality = resolveCanonicalRealityCoveragePct(report);
  const rg = report?.realityGap;
  if (reality == null || !rg?.totalAttributes) return null;
  const high = highSeverityGaps(report);
  let text = `Only ${rg.recognizedCount} of ${rg.totalAttributes} monitored property attributes are consistently recognized (${displayPct(
    reality
  )} Reality Coverage).`;
  if (high.length) {
    text += ` Highest-severity gaps today include ${high
      .slice(0, 3)
      .map((g) => g.label)
      .join("; ")}.`;
    text +=
      " Prioritize attributes that overlap with weak traveler-need scenarios or displacement evidence — not every gap is equally material.";
  }
  return {
    text,
    sourceField: "realityGap",
    realityCoverage: reality,
  };
}

/**
 * @param {object} report
 * @param {object|null} bpp
 * @param {object[]} kpis
 */
export function buildExecutiveExpansionV2(report, bpp, kpis = []) {
  const er = report?.executiveRead || {};
  const sections = er.compositionV3?.sections || er.sections || {};
  const summary = er.summary || {};
  const em = report?.executiveMetrics || {};
  const consideration = em.considerationRate?.rate;
  const scenario = em.scenarioPresence?.rate;
  const reality = resolveCanonicalRealityCoveragePct(report);
  const comparable = resolveOfficialComparablePrior(report);
  const strength = summary.biggestStrength || null;
  const constraint = summary.biggestConstraint || null;
  const change = summary.changeSinceLastComparableRun || null;
  const topDisp = report?.lostDemand?.displacement?.[0] || null;
  const terr = territorySoftness(report);
  const bppRank = kpis.find((k) => k.id === "bpp_portfolioRank");
  const bppPresence = kpis.find((k) => k.id === "bpp_portfolioAiPresence");
  const claims = [];

  const conclusion =
    sections.headline ||
    `${report?.property?.name || "The property"} — see Key Insight for the binding constraint.`;
  claims.push({
    claimId: "executive_conclusion",
    sourceMetricId: "executiveRead.sections.headline",
    platformValue: sections.headline || null,
    pdfText: conclusion,
  });

  // WHAT THE DATA IS TELLING US — interpretive contrast, not KPI restatement dump
  let whatDataTellsUs = sections.keyInsight || null;
  if (
    whatDataTellsUs &&
    consideration != null &&
    scenario != null &&
    !/consideration|scenario presence/i.test(whatDataTellsUs)
  ) {
    // keep platform keyInsight as-is when already complete
  }
  claims.push({
    claimId: "what_data_tells_us",
    sourceMetricId: "executiveRead.sections.keyInsight",
    platformValue: sections.keyInsight || null,
    pdfText: whatDataTellsUs,
  });

  const whyItMatters = sections.whyItMatters || null;
  claims.push({
    claimId: "why_it_matters",
    sourceMetricId: "executiveRead.sections.whyItMatters",
    platformValue: whyItMatters,
    pdfText: whyItMatters,
  });

  const concentrationParts = [];
  if (terr) {
    concentrationParts.push(
      `${terr.weakestLabel} shows the softest Scenario Capture (${displayPct(
        terr.weakestRate
      )}), while ${terr.strongestLabel} remains stronger (${displayPct(
        terr.strongestRate
      )}). That contrast supports a concentrated softness reading — not a uniform demand failure.`
    );
  }
  if (topDisp) {
    concentrationParts.push(
      `${topDisp.name} most often appears when the hotel is absent (${topDisp.displacementCount} scenarios) — useful for source and fact reconciliation, not as proof of cause.`
    );
  }
  if (reality != null && report?.realityGap) {
    const high = highSeverityGaps(report)
      .slice(0, 3)
      .map((g) => g.label);
    concentrationParts.push(
      `Reality Coverage is ${displayPct(reality)} (${report.realityGap.recognizedCount}/${report.realityGap.totalAttributes} attributes).${
        high.length ? ` Material gaps include ${high.join("; ")}.` : ""
      }`
    );
  }
  const concentration = concentrationParts.length
    ? concentrationParts.join(" ")
    : null;
  if (concentration) {
    claims.push({
      claimId: "concentration",
      sourceMetricId: "territory+displacement+realityGap",
      platformValue: { terr, topDisp, reality },
      pdfText: concentration,
    });
  }

  const focusNow = sections.focusNow || null;
  claims.push({
    claimId: "focus_now",
    sourceMetricId: "executiveRead.sections.focusNow",
    platformValue: focusNow,
    pdfText: focusNow,
  });

  const reviewNext =
    sections.whatToReview ||
    sections.watch ||
    "Use the Evidence Review and Management Action Agenda to inspect the weak-need scenarios and attribute gaps named above before changing channel strategy.";
  claims.push({
    claimId: "review_next",
    sourceMetricId: "executiveRead.sections.whatToReview",
    platformValue: sections.whatToReview || sections.watch || null,
    pdfText: reviewNext,
  });

  const cautionParts = [];
  if (!comparable) {
    cautionParts.push(
      change?.body ||
        "No official comparable prior period is available. Evaluate absolute levels on their own; do not treat them as period-over-period movement."
    );
  }
  if (bppRank && bppPresence) {
    cautionParts.push(
      `Independent Positioning (${bppRank.currentDisplay}; Portfolio AI Presence ${bppPresence.currentDisplay}) uses a different denominator than Core consideration — treat it as relative portfolio context, not a second Core score.`
    );
  }
  const caution = cautionParts.length ? cautionParts.join(" ") : null;

  const blocks = {
    conclusion,
    whatDataTellsUs,
    whyItMatters,
    concentration,
    focusNow,
    reviewNext,
    caution,
  };

  // Flattened text for language audits / legacy consumers (structured, not a wall)
  const text = [
    blocks.conclusion && `Executive Conclusion. ${blocks.conclusion}`,
    blocks.whatDataTellsUs &&
      `What the Data Is Telling Us. ${blocks.whatDataTellsUs}`,
    blocks.whyItMatters && `Why It Matters. ${blocks.whyItMatters}`,
    blocks.concentration &&
      `Where the Pattern Is Concentrated. ${blocks.concentration}`,
    blocks.focusNow &&
      `What Management Should Focus On Now. ${blocks.focusNow}`,
    blocks.caution && `What Not to Overreact To. ${blocks.caution}`,
    blocks.reviewNext && `What to Review Next. ${blocks.reviewNext}`,
  ]
    .filter(Boolean)
    .join("\n\n");

  const demandInsight = buildDemandMovementInsight(report);
  const competitiveInsight = buildCompetitiveMovementInsight(report);
  const bppInterpretation = buildBppInterpretation(report, bpp, kpis);
  const realityInterpretation = buildRealityGapInterpretation(report);

  return {
    schema: AI_DEMAND_PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_V3,
    gate: PLATFORM_EXECUTIVE_READ_PDF_PRIMARY_ISSUE_PARITY,
    preserveGate: PERFORMANCE_REVIEW_EXECUTIVE_EXPANSION_PRESERVES_PLATFORM_CONCLUSION,
    primaryIssueId: er.primaryIssueId || null,
    platformSections: {
      headline: sections.headline || null,
      keyInsight: sections.keyInsight || null,
      whyItMatters: sections.whyItMatters || null,
      focusNow: sections.focusNow || null,
      whatToReview: sections.whatToReview || null,
      watch: sections.watch || null,
    },
    platformScan: {
      biggestStrength: strength,
      biggestConstraint: constraint,
      changeSinceLastComparableRun: change,
    },
    scanCards: {
      biggestStrength: strength
        ? {
            label: strength.sectionLabel || "BIGGEST STRENGTH",
            headline: strength.headline,
            body: strength.body,
          }
        : null,
      biggestConstraint: constraint
        ? {
            label: constraint.sectionLabel || "BIGGEST CONSTRAINT",
            headline: constraint.headline,
            body: constraint.body,
          }
        : null,
      changeSinceLastComparableRun: change
        ? {
            label: change.sectionLabel || "CHANGE SINCE LAST COMPARABLE RUN",
            headline: change.headline,
            body: change.body,
          }
        : null,
    },
    blocks,
    expansion: {
      conclusion: blocks.conclusion,
      whatDataTellsUs: blocks.whatDataTellsUs,
      whyItMatters: blocks.whyItMatters,
      concentration: blocks.concentration,
      focusNow: blocks.focusNow,
      reviewNext: blocks.reviewNext,
      caution: blocks.caution,
    },
    sectionInsights: {
      demandMovement: demandInsight,
      competitiveMovement: competitiveInsight,
      bpp: bppInterpretation,
      realityGap: realityInterpretation,
    },
    text,
    wordCount: text.split(/\s+/).filter(Boolean).length,
    claims,
    numericAnchors: er.numericAnchors || [],
    redundancyNote:
      "KPI band owns absolute rates; scan cards own platform strength/constraint; expansion owns interpretation — avoid restating the same sentence.",
  };
}

export function buildCalloutsFromPlatformV3(report) {
  // Retained for compatibility; PDF V3 prefers scan cards over long callout stack.
  const summary = report?.executiveRead?.summary || {};
  const sections =
    report?.executiveRead?.compositionV3?.sections ||
    report?.executiveRead?.sections ||
    {};
  return {
    positiveSignal: {
      title: "Positive Signal",
      body:
        summary.biggestStrength?.body ||
        summary.biggestStrength?.headline ||
        "No single dominant positive signal is certified for this period.",
      source: "executiveRead.summary.biggestStrength",
    },
    secondaryCallout: {
      title: "Watch Item",
      body:
        summary.biggestConstraint?.body ||
        sections.watch ||
        "Monitor consistency of consideration and any attribute gaps that could invite displacement.",
      source: "executiveRead.summary.biggestConstraint",
    },
    managementPriority: {
      title: "Management Priority",
      body:
        sections.focusNow ||
        "Confirm highest-severity reality gaps before expanding the action list.",
      source: "executiveRead.sections.focusNow",
    },
  };
}
