/**
 * AI Demand Performance Review — executive editorial polish V1
 *
 * Doctrine:
 *   PLATFORM_EXECUTIVE_SUMMARY_IS_THE_ANALYTICAL_SPINE
 *   PERFORMANCE_REVIEW_EXPANDS_THE_SPINE
 *   EXPANSION_ADDS_CONTEXT_NOT_DIFFERENT_CONCLUSIONS
 *   EXECUTIVE_REVIEW_NO_LOW_VALUE_REPETITION
 *   AI_DEMAND_PERFORMANCE_REVIEW_EDITORIAL_GOLDEN_V1
 *
 * Does not change metrics, methodology, or primaryIssueId.
 * Rewrites client-facing expansion copy for senior-hospitality clarity.
 */

import {
  displayPct,
  resolveCanonicalRealityCoveragePct,
  resolveOfficialComparablePrior,
} from "./performance-review-canonical-binder-v1.js";

export const AI_DEMAND_PERFORMANCE_REVIEW_EDITORIAL_GOLDEN_V1 =
  "AI_DEMAND_PERFORMANCE_REVIEW_EDITORIAL_GOLDEN_V1";

export const REALITY_GAP_PRIORITY = {
  ACTIONABLE_NOW: "ACTIONABLE_NOW",
  VALIDATE_FIRST: "VALIDATE_FIRST",
  MONITOR: "MONITOR",
};

export const ACTION_PRIORITY = {
  PRIORITY_1: "PRIORITY_1",
  PRIORITY_2: "PRIORITY_2",
  MONITOR_SUPPORTING: "MONITOR_SUPPORTING",
};

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

/** Client-language scrub for PDF narrative (keep analytics enums out of prose). */
const CLIENT_LANGUAGE_REPLACEMENTS = [
  [/binding constraint/gi, "primary constraint"],
  [/\bdenominators?\b/gi, "measurement basis"],
  [/\bobservation context\b/gi, "monitored answers"],
  [/\bobservation universe\b/gi, "monitored answers"],
  [/\banswer-level inclusion consistency\b/gi, "consistent appearance in AI answers"],
  [/\banswer-level misses\b/gi, "cases where the hotel is missing"],
  [/\banswer-level inclusion\b/gi, "appearance in AI answers"],
  [/\bLEVEL_(LOW|PARTIAL|HIGH)\b/g, "$1"],
  [/\bcanonical\b/gi, "certified"],
  [/\bsupport set\b/gi, "supporting evidence"],
  [/\bgoverned\b/gi, "approved"],
];

export function scrubClientLanguage(text) {
  if (!text) return text;
  let out = String(text);
  for (const [re, rep] of CLIENT_LANGUAGE_REPLACEMENTS) {
    out = out.replace(re, rep);
  }
  return out;
}

function territorySoftness(report) {
  const byIntent = report?.demandCapture?.byIntent || {};
  const ranked = Object.entries(byIntent)
    .map(([intent, row]) => ({ intent, rate: row?.rate }))
    .filter((r) => typeof r.rate === "number")
    .sort((a, b) => a.rate - b.rate);
  if (!ranked.length) return null;
  const weakest = ranked[0];
  const commercialStrong = ranked.filter(
    (r) =>
      ["business", "leisure", "group_meeting"].includes(r.intent) &&
      r.rate >= 85
  );
  const strongBand =
    commercialStrong.length >= 1
      ? commercialStrong
      : ranked.filter((r) => r.rate >= 85).slice(-3);
  return {
    weakestLabel: INTENT_LABELS[weakest.intent] || weakest.intent,
    weakestRate: weakest.rate,
    weakestIntent: weakest.intent,
    strongLabels: strongBand.map((r) => INTENT_LABELS[r.intent] || r.intent),
    ranked,
  };
}

function joinLabels(labels) {
  if (!labels?.length) return "";
  if (labels.length === 1) return labels[0];
  if (labels.length === 2) return `${labels[0]} and ${labels[1]}`;
  return `${labels.slice(0, -1).join(", ")}, and ${labels[labels.length - 1]}`;
}

/**
 * Classify Reality Gaps for management prioritization.
 */
export function prioritizeRealityGaps(report) {
  const terr = territorySoftness(report);
  const weakIntent = terr?.weakestIntent || null;
  const gaps = report?.realityGap?.gaps || [];
  const classified = gaps.map((g) => {
    const label = g.label || g.attribute || "";
    const attr = String(g.attribute || "").toLowerCase();
    const sev = String(g.severity || "").toUpperCase();
    const rate =
      typeof g.recognitionRate === "number" ? g.recognitionRate : null;
    let priority = REALITY_GAP_PRIORITY.MONITOR;
    let why = "Useful to watch; not the first operational lever this period.";

    const locationLike = /location|county|neighborhood|metro|downtown|city/.test(
      `${attr} ${label}`.toLowerCase()
    );
    const familyAttr = /family|kids|children|suite|connecting/.test(
      `${attr} ${label}`.toLowerCase()
    );
    const fitnessNiche = /peloton|spa|rooftop/.test(`${attr} ${label}`.toLowerCase());
    const fitnessCore = /fitness|gym|24.?hour/.test(`${attr} ${label}`.toLowerCase());

    if (sev === "HIGH" && locationLike) {
      priority = REALITY_GAP_PRIORITY.ACTIONABLE_NOW;
      why =
        "Location identity is foundational for traveler recognition and often overlaps weaker local-demand scenarios.";
    } else if (sev === "HIGH" && familyAttr) {
      priority = REALITY_GAP_PRIORITY.ACTIONABLE_NOW;
      why =
        "Directly overlaps family-oriented representation — closing this supports inclusion in the softest traveler-need context.";
    } else if (sev === "HIGH" && fitnessCore && !fitnessNiche) {
      priority = REALITY_GAP_PRIORITY.VALIDATE_FIRST;
      why =
        "High-severity amenity gap — confirm whether public sources already state the amenity clearly before investing in content changes.";
    } else if (sev === "HIGH" && fitnessNiche) {
      priority = REALITY_GAP_PRIORITY.MONITOR;
      why =
        "Narrow amenity signal; monitor unless evidence shows it coincides with repeated absences.";
    } else if (sev === "HIGH" && rate != null && rate < 15) {
      priority = REALITY_GAP_PRIORITY.VALIDATE_FIRST;
      why =
        "Rarely recognized today — validate source accuracy before treating as a primary fix.";
    } else if (sev === "HIGH" && weakIntent === "family") {
      priority = REALITY_GAP_PRIORITY.VALIDATE_FIRST;
      why =
        "High-severity gap while Family Travel is soft — validate source accuracy and overlap with absent scenarios before prioritizing.";
    } else if (sev !== "HIGH" && rate != null && rate < 40) {
      priority = REALITY_GAP_PRIORITY.VALIDATE_FIRST;
      why =
        "Attribute recognition is limited — validate public-source accuracy before elevating to a primary fix.";
    }

    return {
      attribute: g.attribute,
      label,
      severity: sev,
      recognitionRate: rate,
      priority,
      why,
    };
  });

  const order = {
    [REALITY_GAP_PRIORITY.ACTIONABLE_NOW]: 0,
    [REALITY_GAP_PRIORITY.VALIDATE_FIRST]: 1,
    [REALITY_GAP_PRIORITY.MONITOR]: 2,
  };
  classified.sort(
    (a, b) =>
      (order[a.priority] ?? 9) - (order[b.priority] ?? 9) ||
      (a.recognitionRate ?? 100) - (b.recognitionRate ?? 100)
  );
  return classified;
}

/**
 * Rank action agenda against primary issue + concentration (generic pattern rules).
 */
export function prioritizeActionAgenda(actions = [], report = {}) {
  const primary = report?.executiveRead?.primaryIssueId || "";
  const patternScore = (patternId) => {
    const id = String(patternId || "");
    if (id.includes("COMPETITIVE_ABSENCE")) return { rank: ACTION_PRIORITY.PRIORITY_1, weight: 10 };
    if (
      primary.includes("inclusion") ||
      primary.includes("consideration") ||
      primary.includes("answer_level")
    ) {
      if (id.includes("ROOM_EXPECTATION"))
        return { rank: ACTION_PRIORITY.PRIORITY_2, weight: 8 };
      if (id.includes("FAMILY_TRAVEL"))
        return { rank: ACTION_PRIORITY.PRIORITY_1, weight: 9 };
      if (id.includes("MEETINGS"))
        return { rank: ACTION_PRIORITY.PRIORITY_2, weight: 6 };
      if (id.includes("TRIPADVISOR") || id.includes("GOOGLE_BUSINESS"))
        return { rank: ACTION_PRIORITY.MONITOR_SUPPORTING, weight: 3 };
    }
    if (id.includes("MEETINGS"))
      return { rank: ACTION_PRIORITY.PRIORITY_2, weight: 5 };
    if (id.includes("TRIPADVISOR"))
      return { rank: ACTION_PRIORITY.MONITOR_SUPPORTING, weight: 2 };
    return { rank: ACTION_PRIORITY.PRIORITY_2, weight: 4 };
  };

  const whyFor = (rank, patternId) => {
    if (rank === ACTION_PRIORITY.PRIORITY_1) {
      if (String(patternId).includes("COMPETITIVE_ABSENCE")) {
        return "Most directly addresses inconsistent inclusion where competitors appear in the hotel’s absence.";
      }
      return "Closest operational lever for the primary inclusion-consistency issue.";
    }
    if (rank === ACTION_PRIORITY.MONITOR_SUPPORTING) {
      return "Important hygiene or reputation work — support the primary inclusion agenda rather than lead it.";
    }
    return "Material follow-on work after the primary inclusion and source-reconciliation focus.";
  };

  return actions.map((a, index) => {
    const scored = patternScore(a.actionPatternId);
    return {
      actionId: a.actionId,
      actionPatternId: a.actionPatternId,
      actionTitle: a.actionTitle,
      index,
      priority: scored.rank,
      weight: scored.weight,
      why: whyFor(scored.rank, a.actionPatternId),
    };
  }).sort((a, b) => b.weight - a.weight || a.index - b.index);
}

function resolveMetricPattern(scenario, consideration) {
  if (scenario == null || consideration == null) return "INSUFFICIENT_METRICS";
  const gap = scenario - consideration;
  if (scenario >= 65 && consideration <= 55 && gap >= 20) {
    return "HIGH_SCENARIO_LOW_CONSIDERATION";
  }
  if (scenario < 50 && consideration < 50) {
    return "LOW_SCENARIO_LOW_CONSIDERATION";
  }
  if (scenario >= 70 && consideration >= 60) {
    return "STRONG_CORE";
  }
  if (scenario < 60 && consideration < 45) {
    return "WEAK_CORE";
  }
  if (gap >= 15 && scenario >= 55) {
    return "RELEVANCE_AHEAD_OF_INCLUSION";
  }
  return "MIXED_CORE";
}

function buildEditorialBlocks(report, kpis = []) {
  const er = report?.executiveRead || {};
  const sections = er.compositionV3?.sections || er.sections || {};
  const summary = er.summary || {};
  const em = report?.executiveMetrics || {};
  const consideration = em.considerationRate?.rate;
  const scenario = em.scenarioPresence?.rate;
  const reality = resolveCanonicalRealityCoveragePct(report);
  const comparable = resolveOfficialComparablePrior(report);
  const terr = territorySoftness(report);
  const topDisp = report?.lostDemand?.displacement?.[0] || null;
  const name = report?.property?.name || "The hotel";
  const bppRank = kpis.find((k) => k.id === "bpp_portfolioRank");
  const bppPresence = kpis.find((k) => k.id === "bpp_portfolioAiPresence");
  const gapPri = prioritizeRealityGaps(report).filter(
    (g) => g.severity === "HIGH"
  );
  const actionable = gapPri.filter(
    (g) => g.priority === REALITY_GAP_PRIORITY.ACTIONABLE_NOW
  );
  const pattern = resolveMetricPattern(scenario, consideration);
  const recognitionGapPattern =
    pattern === "HIGH_SCENARIO_LOW_CONSIDERATION" ||
    pattern === "RELEVANCE_AHEAD_OF_INCLUSION";

  const conclusion = scrubClientLanguage(
    sections.headline ||
      `${name} — see Key Insight for the primary management issue this period.`
  );

  let whatDataTellsUs;
  if (recognitionGapPattern && scenario != null && consideration != null) {
    const strong = joinLabels(terr?.strongLabels?.slice(0, 3) || []);
    whatDataTellsUs = `Scenario Presence at ${displayPct(
      scenario
    )} shows broad recognition across monitored traveler needs. AI Consideration at ${displayPct(
      consideration
    )} shows that recognition does not consistently convert into selection in individual answers. The management problem is inclusion consistency — not awareness.`;
    if (terr) {
      whatDataTellsUs += ` Softness concentrates in ${terr.weakestLabel}`;
      if (strong) whatDataTellsUs += `; ${strong} remain stronger`;
      whatDataTellsUs += ".";
    }
    if (topDisp) {
      whatDataTellsUs += ` When the hotel is absent, ${topDisp.name} most often appears (${topDisp.displacementCount} scenarios).`;
    }
  } else if (pattern === "STRONG_CORE" && scenario != null && consideration != null) {
    whatDataTellsUs = scrubClientLanguage(
      sections.keyInsight ||
        `Scenario Presence (${displayPct(scenario)}) and AI Consideration (${displayPct(
          consideration
        )}) both indicate relatively strong current-period presence. Focus on protecting strengths and closing any concentrated weak spots.`
    );
    if (terr && terr.weakestRate < 85) {
      whatDataTellsUs += ` Softest coverage today is ${terr.weakestLabel} at ${displayPct(
        terr.weakestRate
      )} Scenario Capture.`;
    }
    if (topDisp) {
      whatDataTellsUs += ` When absent, ${topDisp.name} appears most often (${topDisp.displacementCount} scenarios).`;
    }
  } else if (
    (pattern === "LOW_SCENARIO_LOW_CONSIDERATION" || pattern === "WEAK_CORE") &&
    scenario != null &&
    consideration != null
  ) {
    whatDataTellsUs = scrubClientLanguage(
      sections.keyInsight ||
        `Scenario Presence (${displayPct(scenario)}) and AI Consideration (${displayPct(
          consideration
        )}) are both limited this period. The priority is building consistent representation where monitored demand is weakest.`
    );
    if (terr) {
      whatDataTellsUs += ` Softest Scenario Capture is ${terr.weakestLabel} at ${displayPct(
        terr.weakestRate
      )}.`;
    }
    if (topDisp) {
      whatDataTellsUs += ` ${topDisp.name} most often appears when the hotel is missing (${topDisp.displacementCount} scenarios).`;
    }
  } else {
    whatDataTellsUs = scrubClientLanguage(sections.keyInsight);
    if (
      whatDataTellsUs &&
      terr &&
      !/Scenario Capture|softest/i.test(whatDataTellsUs)
    ) {
      whatDataTellsUs += ` Softest Scenario Capture is ${terr.weakestLabel} at ${displayPct(
        terr.weakestRate
      )}.`;
    }
    if (
      whatDataTellsUs &&
      topDisp &&
      !whatDataTellsUs.includes(topDisp.name)
    ) {
      whatDataTellsUs += ` When the hotel is absent, ${topDisp.name} most often appears (${topDisp.displacementCount} scenarios).`;
    }
  }

  let whyItMatters;
  if (recognitionGapPattern) {
    whyItMatters = scrubClientLanguage(
      sections.whyItMatters ||
        `${name} is already understood as a relevant hotel across a broad share of monitored demand. The opportunity is to make that relevance translate more consistently into answer inclusion — especially in weaker traveler-need contexts — rather than rebuilding awareness or assuming a new competitive set is required.`
    );
  } else if (pattern === "STRONG_CORE") {
    whyItMatters = scrubClientLanguage(
      sections.whyItMatters ||
        `${name} shows relatively strong current presence. Management value is in protecting that standing, closing concentrated soft spots, and confirming sources remain accurate before expanding the action list.`
    );
  } else if (pattern === "LOW_SCENARIO_LOW_CONSIDERATION" || pattern === "WEAK_CORE") {
    whyItMatters = scrubClientLanguage(
      sections.whyItMatters ||
        `${name} is not yet consistently present across monitored traveler needs. Management attention should start with the weakest demand contexts and the facts competitors surface when the hotel is missing.`
    );
  } else {
    whyItMatters = scrubClientLanguage(
      sections.whyItMatters ||
        `${name}'s current AI demand picture has mixed strengths and constraints. Prioritize the primary issue named in the Executive Summary before expanding channel strategy.`
    );
  }

  const concentrationParts = [];
  if (terr) {
    const strong = joinLabels(terr.strongLabels.slice(0, 3));
    if (recognitionGapPattern) {
      concentrationParts.push(
        `${terr.weakestLabel} is the softest Scenario Capture at ${displayPct(
          terr.weakestRate
        )}${strong ? `, while ${strong} show broader coverage` : ""}. The inclusion issue is concentrated, not universal.`
      );
    } else {
      concentrationParts.push(
        `${terr.weakestLabel} is the softest Scenario Capture at ${displayPct(
          terr.weakestRate
        )}${strong ? `, while ${strong} show broader coverage` : ""}. Focus where the gap is concentrated rather than treating every territory equally.`
      );
    }
  }
  if (topDisp) {
    concentrationParts.push(
      `${topDisp.name} appears most often when the hotel is missing (${topDisp.displacementCount} scenarios) — use that pattern for source and fact checks, not as proof of cause.`
    );
  }
  if (reality != null && report?.realityGap) {
    const lead =
      actionable[0]?.label ||
      gapPri[0]?.label ||
      null;
    concentrationParts.push(
      `Reality Coverage is ${displayPct(reality)} (${report.realityGap.recognizedCount} of ${report.realityGap.totalAttributes} attributes).${
        lead
          ? ` Start with gaps that matter operationally — beginning with ${lead}.`
          : ""
      }`
    );
  }
  const concentration = concentrationParts.join(" ");

  const focusNow = scrubClientLanguage(
    sections.focusNow ||
      `Start with representation consistency in ${terr?.weakestLabel || "the weakest traveler-need context"}: confirm priority sources show the same use cases and attributes that appear when the hotel is missing.`
  );

  const reviewNext = scrubClientLanguage(
    sections.whatToReview ||
      "Compare scenarios where the hotel is recognized with those where it is missing. Inspect sources and attributes in the absence cases before changing channel strategy."
  );

  const cautionParts = [];
  if (!comparable) {
    cautionParts.push(
      "No comparable prior official period is available — read these as current levels, not period-over-period change."
    );
  }
  if (bppRank && bppPresence) {
    const cons = consideration;
    const bppVal =
      typeof bppPresence.currentValue === "number"
        ? bppPresence.currentValue * 100
        : null;
    if (
      recognitionGapPattern ||
      (cons != null && bppVal != null && Math.abs(bppVal - cons) < 5 && cons < 55)
    ) {
      cautionParts.push(
        `Independent Positioning (${bppRank.currentDisplay}; Portfolio AI Presence ${bppPresence.currentDisplay}) measures relative standing among loyalty peers. Strong peer rank does not mean consistent inclusion across the wider demand set.`
      );
    } else {
      cautionParts.push(
        `Independent Positioning (${bppRank.currentDisplay}; Portfolio AI Presence ${bppPresence.currentDisplay}) is peer-set context — keep it separate from Core demand metrics.`
      );
    }
  }
  const caution = cautionParts.join(" ");

  return {
    conclusion,
    whatDataTellsUs,
    whyItMatters,
    concentration,
    focusNow,
    reviewNext,
    caution: caution || null,
    metricPattern: pattern,
    platformPreserved: {
      headline: sections.headline || null,
      keyInsight: sections.keyInsight || null,
      whyItMatters: sections.whyItMatters || null,
      focusNow: sections.focusNow || null,
      whatToReview: sections.whatToReview || null,
      biggestStrength: summary.biggestStrength || null,
      biggestConstraint: summary.biggestConstraint || null,
      changeSinceLastComparableRun:
        summary.changeSinceLastComparableRun || null,
    },
  };
}

export function buildEditorialDemandInsight(report) {
  const terr = territorySoftness(report);
  if (!terr) return null;
  const comparable = resolveOfficialComparablePrior(report);
  const em = report?.executiveMetrics || {};
  const pattern = resolveMetricPattern(
    em.scenarioPresence?.rate,
    em.considerationRate?.rate
  );
  const strong = joinLabels(terr.strongLabels.slice(0, 3));
  let text = `${terr.weakestLabel} is the clearest softness at ${displayPct(
    terr.weakestRate
  )} Scenario Capture.`;
  if (strong) {
    if (
      pattern === "HIGH_SCENARIO_LOW_CONSIDERATION" ||
      pattern === "RELEVANCE_AHEAD_OF_INCLUSION"
    ) {
      text += ` ${strong} show broader coverage — the inclusion issue is concentrated rather than universal.`;
    } else {
      text += ` ${strong} show broader coverage — softness is concentrated rather than uniform.`;
    }
  }
  if (!comparable) {
    text += " These are current-period levels, not trend.";
  }
  return {
    text: scrubClientLanguage(text),
    metricName: "scenario_capture_rate",
    sourceField: "demandCapture.byIntent",
  };
}

export function buildEditorialCompetitiveInsight(report) {
  const leaders = report?.lostDemand?.displacement || [];
  if (!leaders.length) return null;
  const subject = report?.property?.name || "the hotel";
  const top = leaders[0];
  const second = leaders[1];
  const third = leaders[2];
  const subjectBrand = String(report?.property?.affiliation || "")
    .toLowerCase()
    .split(/\s+/)[0];
  const sameBrand =
    subjectBrand &&
    leaders.some((d) =>
      String(d.name || "")
        .toLowerCase()
        .includes(subjectBrand)
    );
  let text = `${top.name} is the most frequent alternative when ${subject} is absent (${top.displacementCount} scenarios).`;
  if (second && third) {
    text += ` ${second.name} (${second.displacementCount}) and ${third.name} (${third.displacementCount}) also appear often`;
    text += sameBrand
      ? " — displacement spans both same-brand and broader local options."
      : " — displacement is spread across multiple local options.";
  }
  text += " Counts show co-appearance patterns; they do not prove why displacement happens.";
  return {
    text: scrubClientLanguage(text),
    sourceField: "lostDemand.displacement",
  };
}

export function buildEditorialBppInterpretation(report, bpp, kpis = []) {
  if (!bpp || bpp.status !== "READY") return null;
  const bppRank = kpis.find((k) => k.id === "bpp_portfolioRank");
  const bppPresence = kpis.find((k) => k.id === "bpp_portfolioAiPresence");
  // RANK_ONLY / missing presence: limited commentary
  if (bppRank && !bppPresence) {
    return {
      text: scrubClientLanguage(
        `Independent Positioning shows portfolio rank ${bppRank.currentDisplay}. Presence within the peer set is not published for this edition — treat rank as relative peer context only.`
      ),
      sourceField: "bpp.customerPublishedPack",
      mode: "RANK_ONLY",
    };
  }
  if (!bppRank && !bppPresence) return null;
  const consideration = report?.executiveMetrics?.considerationRate?.rate;
  const bppVal =
    typeof bppPresence?.currentValue === "number"
      ? bppPresence.currentValue * 100
      : null;
  let text;
  if (bppRank && bppPresence && consideration != null && bppVal != null) {
    const aligned = Math.abs(bppVal - consideration) < 5;
    const weakAbs = consideration < 55;
    const strongAbs = consideration >= 60;
    if (aligned && weakAbs) {
      text = `Among loyalty peers in the Independent Positioning set, the hotel ranks ${bppRank.currentDisplay}. Portfolio AI Presence is ${bppPresence.currentDisplay} — aligned with Core AI Consideration. Relative strength inside the loyalty set can coexist with inconsistent inclusion in the broader answer set; peer rank is not a substitute for market-wide selection consistency.`;
    } else if (aligned && strongAbs) {
      text = `Independent Positioning rank ${bppRank.currentDisplay} and Portfolio AI Presence ${bppPresence.currentDisplay} reinforce a relatively strong Core consideration reading. Peer-set leadership is consistent with broader inclusion this period.`;
    } else if (bppVal >= consideration + 10) {
      text = `Independent Positioning looks stronger (${bppRank.currentDisplay}; Portfolio AI Presence ${bppPresence.currentDisplay}) than Core AI Consideration (${displayPct(
        consideration
      )}). Treat peer-set standing as relative context — it does not automatically imply stronger market-wide inclusion.`;
    } else if (consideration >= bppVal + 10) {
      text = `Core AI Consideration (${displayPct(
        consideration
      )}) is stronger than Portfolio AI Presence (${bppPresence.currentDisplay}) despite Independent Positioning rank ${bppRank.currentDisplay}. Keep peer-set and Core readings separate.`;
    } else {
      text = `Independent Positioning rank ${bppRank.currentDisplay} with Portfolio AI Presence ${bppPresence.currentDisplay} provides peer-set context alongside Core consideration (${displayPct(
        consideration
      )}). Use both, but do not collapse them into one score.`;
    }
  } else if (bppRank && bppPresence) {
    text = `Among loyalty peers, the hotel ranks ${bppRank.currentDisplay} with Portfolio AI Presence ${bppPresence.currentDisplay}. This is peer-set context, not a second Core score.`;
  } else {
    return null;
  }
  return {
    text: scrubClientLanguage(text),
    sourceField: "bpp.customerPublishedPack",
    mode: "READY",
  };
}

export function buildEditorialRealityGapInterpretation(report) {
  const reality = resolveCanonicalRealityCoveragePct(report);
  const rg = report?.realityGap;
  if (reality == null || !rg?.totalAttributes) return null;
  // Classify all gaps; prefer HIGH in ordering but do not drop non-HIGH when they are the only material list.
  const pri = prioritizeRealityGaps(report);
  const high = pri.filter((g) => g.severity === "HIGH");
  const working = high.length ? high : pri.slice(0, 5);
  const by = (p) => working.filter((g) => g.priority === p);
  const actionable = by(REALITY_GAP_PRIORITY.ACTIONABLE_NOW);
  const validate = by(REALITY_GAP_PRIORITY.VALIDATE_FIRST);
  const monitor = by(REALITY_GAP_PRIORITY.MONITOR);

  let text = `Only ${rg.recognizedCount} of ${rg.totalAttributes} monitored attributes are consistently recognized (${displayPct(
    reality
  )} Reality Coverage). Not every gap deserves equal urgency.`;
  if (!working.length) {
    text += " No attribute gaps are queued for action this period.";
  }
  if (actionable.length) {
    text += ` Actionable now: ${actionable.map((g) => g.label).join("; ")}.`;
    text += ` ${actionable[0].why}`;
  }
  if (validate.length) {
    text += ` Validate first: ${validate.map((g) => g.label).join("; ")}.`;
  }
  if (monitor.length) {
    text += ` Monitor: ${monitor.map((g) => g.label).join("; ")}.`;
  }
  return {
    text: scrubClientLanguage(text),
    sourceField: "realityGap",
    realityCoverage: reality,
    prioritizedGaps: working,
  };
}

/**
 * Apply editorial golden polish onto an expansion payload.
 * Preserves primaryIssueId and platform scan cards (platform spine).
 */
export function applyExecutiveEditorialV1({
  expansion,
  report,
  bpp,
  kpis = [],
  actions = [],
}) {
  const blocks = buildEditorialBlocks(report, kpis);
  const realityInterpretation = buildEditorialRealityGapInterpretation(report);
  const actionPriority = prioritizeActionAgenda(actions, report);

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

  return {
    ...expansion,
    editorialSchema: AI_DEMAND_PERFORMANCE_REVIEW_EDITORIAL_GOLDEN_V1,
    blocks: {
      conclusion: blocks.conclusion,
      whatDataTellsUs: blocks.whatDataTellsUs,
      whyItMatters: blocks.whyItMatters,
      concentration: blocks.concentration,
      focusNow: blocks.focusNow,
      reviewNext: blocks.reviewNext,
      caution: blocks.caution,
    },
    expansion: {
      conclusion: blocks.conclusion,
      whatDataTellsUs: blocks.whatDataTellsUs,
      whyItMatters: blocks.whyItMatters,
      concentration: blocks.concentration,
      focusNow: blocks.focusNow,
      reviewNext: blocks.reviewNext,
      caution: blocks.caution,
    },
    // Platform scan cards stay exact (analytical spine)
    scanCards: expansion.scanCards,
    platformScan: expansion.platformScan,
    sectionInsights: {
      demandMovement: buildEditorialDemandInsight(report),
      competitiveMovement: buildEditorialCompetitiveInsight(report),
      bpp: buildEditorialBppInterpretation(report, bpp, kpis),
      realityGap: realityInterpretation,
    },
    realityGapPrioritization: realityInterpretation?.prioritizedGaps || [],
    actionPriority,
    text,
    wordCount: text.split(/\s+/).filter(Boolean).length,
    editorialNote:
      "Editorial polish preserves platform spine and numeric anchors; story pattern derives from certified metric state (recognition→selection only when supported).",
    metricPattern: blocks.metricPattern || null,
  };
}
