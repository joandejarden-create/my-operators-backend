/**
 * Executive Read UX V2 — plain business language presentation layer.
 * Classification + metrics remain in executive-read-v1 (no formula changes).
 * Communication governance: explain business meaning first; formal metric names second.
 */

import { roundAdpPercent } from "../format-percent.js";
import {
  buildExecutiveRead,
  findCertifiedBenchmarkFinding,
  assertNoUnsupportedCausalLanguage,
  computePropertyRealityCoverage,
  TREND_STATES,
  MATERIALITY_PP_THRESHOLD,
  EXECUTIVE_READ_VERSION,
} from "./executive-read-v1.js";
import { MIN_RANK_SAMPLE } from "../metrics/position-metrics.js";

export const EXECUTIVE_READ_UX_VERSION = "adp_executive_read_ux_v2";
export const EXECUTIVE_SUMMARY_TITLE = "WHAT THE DATA SAYS";
export const PROPERTY_SPECIFIC_EXECUTIVE_COPY_CODE = 0;
export const BUSINESS_LANGUAGE_GOVERNANCE_VERSION = "adp_executive_read_business_language_v3";
export const EXECUTIVE_READ_LAYOUT_VERSION = "adp_executive_read_left_box_layout_v1";

const VAGUE_PHRASES = [
  /\bbroad visibility\b/i,
  /\bselective strength\b/i,
  /\bstrong positioning\b/i,
  /\bhealthy funnel\b/i,
  /\bconsideration consistency\b/i,
  /\brelative strength\b/i,
  /\bcompetitive momentum\b/i,
  /\bweak momentum\b/i,
  /\bnarrative strength\b/i,
  /\bvisibility constraint\b/i,
  /\bdemand-positioning friction\b/i,
  /\bprovider signal weakness\b/i,
  /\babove-benchmark territory presence\b/i,
];

/** Undefined jargon that must not appear without a plain-English explanation nearby. */
const UNDEFINED_JARGON = [
  { re: /\bCORE\b/, explainedBy: /comparable hotels used/i },
  { re: /\bAI Presence Index\b/i, explainedBy: /times as often|more often|less often|as often as the comparable/i },
  { re: /\bAI Scenario Presence\b/i, explainedBy: /appeared in at least one AI answer|traveler needs we tested/i },
  { re: /\bAI Consideration Rate\b/i, explainedBy: /individual AI answers|appeared .+ of the time/i },
  { re: /\bProperty Reality Coverage\b/i, explainedBy: /reflects .+ of the monitored facts|how completely AI/i },
  { re: /\bdemand scenarios?\b/i, explainedBy: /traveler needs/i },
  { re: /\brank[- ]eligible\b/i, explainedBy: /where hotel ranking could be measured|where ranking could be measured/i },
];

function num(v) {
  return v != null && Number.isFinite(Number(v)) ? Number(v) : null;
}

function fmtPct(v) {
  if (v == null) return null;
  return `${roundAdpPercent(v)}%`;
}

function fmtPpSigned(delta) {
  if (delta == null || !Number.isFinite(Number(delta))) return null;
  const n = Number(delta);
  return `${n > 0 ? "+" : ""}${n.toFixed(1)} pts`;
}

/**
 * Translate AI Presence Index into relative frequency language.
 * Index 100 = parity; 120 ≈ 20% more often; 764 ≈ 7.6× as often.
 */
export function translatePresenceIndex(index) {
  const idx = Number(index);
  if (!Number.isFinite(idx) || idx <= 0) return null;
  if (idx >= 200) {
    const times = (idx / 100).toFixed(1).replace(/\.0$/, "");
    return `about ${times} times as often as the comparable hotels used in this analysis`;
  }
  if (idx > 100) {
    const pct = Math.round(idx - 100);
    return `about ${pct}% more often than the comparable hotels used in this analysis`;
  }
  if (idx === 100) {
    return "about as often as the comparable hotels used in this analysis";
  }
  const pct = Math.round(100 - idx);
  return `about ${pct}% less often than the comparable hotels used in this analysis`;
}

function comparableHotelsPhrase() {
  return "the comparable hotels used in this analysis";
}

function findWeakestCertifiedBenchmark(payload) {
  const index = payload?.intentPresenceIndex || {};
  const certified = Object.entries(index)
    .filter(([, row]) => row && row.status === "PRODUCTION_VALIDATED" && row.index != null && !row.developing)
    .map(([intent, row]) => ({
      intent,
      territory: row.territory,
      index: row.index,
      subjectRatePct: row.subjectRatePct ?? row.myRate,
      coreBenchmarkRatePct: row.coreBenchmarkRatePct,
    }))
    .sort((a, b) => (a.index || 0) - (b.index || 0));
  const below = certified.filter((r) => Number(r.index) < 100);
  return below[0] || null;
}

function extractMetricInputs(payload, period, propertyProfile, executiveRead) {
  const em = payload?.executiveMetrics || {};
  const rg = payload?.realityGap;
  let reality = computePropertyRealityCoverage(period, propertyProfile);
  if (reality == null && rg?.totalAttributes) {
    reality = roundAdpPercent((rg.recognizedCount / rg.totalAttributes) * 100);
  }
  return {
    propertyName: payload?.property?.name || "This property",
    consideration: num(em.considerationRate?.rate),
    scenarioPresence: num(em.scenarioPresence?.rate),
    top3: num(em.rankMetrics?.topThreeAppearanceRate),
    numberOne: num(em.rankMetrics?.numberOneAppearanceRate),
    rankEligibleN: num(em.rankMetrics?.rankEligibleN),
    reality,
    competitorPresent: num(em.competitorPresentScenarios?.scenarioCount),
    totalScenarios: num(payload?.demandCapture?.totalScenarios) || num(payload?.period?.scenarioCount),
    deltas: executiveRead?.trend?.deltas || em.currentVsPrior?.deltas || null,
    hasComparablePrior: Boolean(executiveRead?.COMPARABLE_PRIOR_AVAILABLE),
    trendState: executiveRead?.trend?.state,
  };
}

function buildStrengthBox(inputs, strength, benchmarkFinding) {
  const name = inputs.propertyName;
  if (benchmarkFinding && strength?.key === "ABOVE_BENCHMARK_TERRITORY") {
    const sub = fmtPct(benchmarkFinding.subjectRatePct);
    const core = fmtPct(benchmarkFinding.coreBenchmarkRatePct);
    const relative = translatePresenceIndex(benchmarkFinding.index);
    let body =
      `For ${benchmarkFinding.territory} searches, ${name} appeared in ${sub} of AI answers, ` +
      `compared with ${core} for ${comparableHotelsPhrase()}.`;
    if (relative) {
      body += ` That means the hotel was surfaced ${relative}.`;
    }
    body +=
      " In practical terms, this is the area where the hotel is showing its clearest visibility advantage versus similar hotels.";
    return {
      sectionLabel: "BIGGEST STRENGTH",
      headline: `Strong visibility for ${benchmarkFinding.territory}`,
      body,
    };
  }

  if (strength?.key === "TOP3_PROMINENCE" && inputs.top3 != null) {
    return {
      sectionLabel: "BIGGEST STRENGTH",
      headline: "Often placed near the top when AI ranks hotels",
      body:
        `Among AI answers where hotel ranking could be measured, ${name} appeared in the Top 3 ${fmtPct(inputs.top3)} of the time. ` +
        "This shows how prominently the hotel is positioned when AI includes it.",
    };
  }

  if (strength?.key === "DEMAND_REACH" && inputs.scenarioPresence != null) {
    return {
      sectionLabel: "BIGGEST STRENGTH",
      headline: "Recognized across many traveler needs",
      body:
        `${name} appeared in at least one AI answer for ${fmtPct(inputs.scenarioPresence)} of the traveler needs we tested. ` +
        "This shows that AI recognizes the hotel as relevant across a broad range of stay occasions.",
    };
  }

  if (strength?.key === "CONSIDERATION" && inputs.consideration != null) {
    return {
      sectionLabel: "BIGGEST STRENGTH",
      headline: "Appears consistently in individual AI answers",
      body:
        `Across all individual AI answers in the monitored set, ${name} appeared ${fmtPct(inputs.consideration)} of the time. ` +
        "This shows how consistently a traveler would encounter the hotel across the AI answers we tested.",
    };
  }

  if (strength?.key === "PROPERTY_REPRESENTATION" && inputs.reality != null) {
    return {
      sectionLabel: "BIGGEST STRENGTH",
      headline: "AI reflects key property facts",
      body:
        `AI currently reflects ${fmtPct(inputs.reality)} of the monitored facts and attributes that define the hotel. ` +
        "This shows how completely AI represents what the property actually offers.",
    };
  }

  return {
    sectionLabel: "BIGGEST STRENGTH",
    headline: "Still developing",
    body: `${name} does not yet show a single dominant strength with enough comparable evidence in this period.`,
  };
}

function buildConstraintBox(inputs, constraint, weakBenchmark) {
  const name = inputs.propertyName;

  if (
    inputs.scenarioPresence != null &&
    inputs.consideration != null &&
    inputs.scenarioPresence - inputs.consideration >= 15
  ) {
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: "Not appearing consistently in AI answers",
      body:
        `The hotel appeared in at least one AI answer for ${fmtPct(inputs.scenarioPresence)} of the traveler needs we tested, ` +
        `but across all individual AI answers it appeared ${fmtPct(inputs.consideration)} of the time. ` +
        "In practical terms, AI recognizes the hotel as relevant for many types of trips, " +
        "but a traveler will not consistently see it among the hotel options surfaced.",
    };
  }

  if (inputs.reality != null && inputs.reality < 50) {
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: "AI does not fully reflect the property",
      body:
        `AI currently reflects ${fmtPct(inputs.reality)} of the monitored facts and attributes that define the hotel. ` +
        "Important parts of the property's proposition are not consistently reflected in AI results.",
    };
  }

  if (weakBenchmark && Number(weakBenchmark.index) < 100) {
    const sub = fmtPct(weakBenchmark.subjectRatePct);
    const core = fmtPct(weakBenchmark.coreBenchmarkRatePct);
    const relative = translatePresenceIndex(weakBenchmark.index);
    let body =
      `${name} appeared in ${sub} of AI answers for ${weakBenchmark.territory} searches, ` +
      `compared with ${core} for ${comparableHotelsPhrase()}. ` +
      "The hotel appeared less often than the comparable hotels used in this analysis.";
    if (relative) {
      body += ` In relative terms, that is ${relative}.`;
    }
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: `Weaker visibility for ${weakBenchmark.territory}`,
      body,
    };
  }

  if (inputs.scenarioPresence != null && inputs.scenarioPresence < 35) {
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: "Recognized for few traveler needs",
      body:
        `AI recognized the hotel as relevant for only ${fmtPct(inputs.scenarioPresence)} of the traveler needs tested. ` +
        "That means the hotel is absent from many types of trips and stay occasions AI was asked about.",
    };
  }

  if (inputs.consideration != null && inputs.consideration < 25) {
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: "Appears rarely in individual AI answers",
      body:
        `The hotel appeared in only ${fmtPct(inputs.consideration)} of individual AI answers. ` +
        "A traveler reviewing the AI answers we tested would encounter the hotel infrequently.",
    };
  }

  if (
    inputs.competitorPresent != null &&
    inputs.totalScenarios &&
    inputs.competitorPresent / inputs.totalScenarios >= 0.45
  ) {
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: "Competitors appear when this hotel does not",
      body:
        `In ${inputs.competitorPresent} traveler situations, other hotels appeared in AI answers while ${name} did not. ` +
        "These are areas where competing hotels are being surfaced and the subject hotel is missing.",
    };
  }

  if (constraint?.key === "BENCHMARK_DEVELOPING") {
    return {
      sectionLabel: "BIGGEST CONSTRAINT",
      headline: "Comparable-hotel benchmarks not yet certified",
      body:
        "Comparable-hotel benchmarks are not yet certified for every demand territory, " +
        "so territory-level comparisons are limited this period. Individual CORE hotel presence " +
        "can still be measured before the combined benchmark is published.",
    };
  }

  return {
    sectionLabel: "BIGGEST CONSTRAINT",
    headline: "No single dominant weakness",
    body: `${name} shows mixed results across the monitored measures. No one limitation clearly outweighs the others in the current data.`,
  };
}

function priorRate(current, delta) {
  if (current == null || delta == null) return null;
  return roundAdpPercent(current - delta);
}

function buildChangeBox(inputs) {
  const label = "CHANGE SINCE LAST COMPARABLE RUN";

  if (
    !inputs.hasComparablePrior ||
    inputs.trendState === TREND_STATES.NO_COMPARABLE_PRIOR ||
    inputs.trendState === TREND_STATES.BASELINE_PERIOD
  ) {
    return {
      sectionLabel: label,
      headline: "No comparable prior official period yet",
      body:
        "No comparable prior official monitoring period is available yet. " +
        "This period can be evaluated on its own; change over time is not shown.",
    };
  }

  const d = inputs.deltas || {};
  const crDelta = num(d.considerationRate);
  const spDelta = num(d.scenarioPresence);
  const rcDelta = num(d.propertyRealityCoverage);

  const material = [];
  if (crDelta != null && Math.abs(crDelta) >= MATERIALITY_PP_THRESHOLD) material.push("consideration");
  if (spDelta != null && Math.abs(spDelta) >= MATERIALITY_PP_THRESHOLD) material.push("scenario");
  if (rcDelta != null && Math.abs(rcDelta) >= MATERIALITY_PP_THRESHOLD) material.push("reality");

  if (!material.length) {
    return {
      sectionLabel: label,
      headline: "Little changed since the last comparable run",
      body:
        "AI surfaced the hotel in approximately the same share of traveler needs and individual AI answers " +
        "as in the prior comparable period.",
    };
  }

  const parts = [];
  if (inputs.consideration != null && crDelta != null && Math.abs(crDelta) >= MATERIALITY_PP_THRESHOLD) {
    const prior = priorRate(inputs.consideration, crDelta);
    if (prior != null) {
      parts.push(
        `The hotel appeared in individual AI answers ${crDelta >= 0 ? "more" : "less"} consistently: ` +
          `${fmtPct(prior)} to ${fmtPct(inputs.consideration)} (${fmtPpSigned(crDelta)})`
      );
    }
  }
  if (inputs.scenarioPresence != null && spDelta != null && Math.abs(spDelta) >= MATERIALITY_PP_THRESHOLD) {
    const prior = priorRate(inputs.scenarioPresence, spDelta);
    if (prior != null) {
      parts.push(
        `Recognition across traveler needs ${spDelta >= 0 ? "rose" : "fell"} from ${fmtPct(prior)} to ${fmtPct(inputs.scenarioPresence)} (${fmtPpSigned(spDelta)})`
      );
    }
  }
  if (inputs.reality != null && rcDelta != null && Math.abs(rcDelta) >= MATERIALITY_PP_THRESHOLD) {
    const prior = priorRate(inputs.reality, rcDelta);
    if (prior != null) {
      parts.push(
        `How completely AI reflects the hotel moved from ${fmtPct(prior)} to ${fmtPct(inputs.reality)} (${fmtPpSigned(rcDelta)})`
      );
    }
  }

  const headline =
    crDelta != null && Math.abs(crDelta) >= MATERIALITY_PP_THRESHOLD
      ? crDelta > 0
        ? "Visibility improved since the last comparable run"
        : "Visibility weakened since the last comparable run"
      : spDelta != null && Math.abs(spDelta) >= MATERIALITY_PP_THRESHOLD
        ? spDelta > 0
          ? "Visibility improved since the last comparable run"
          : "Visibility weakened since the last comparable run"
        : "Results shifted versus the prior comparable run";

  return {
    sectionLabel: label,
    headline,
    body: parts.length
      ? `${parts.join(". ")}.`
      : "Overall AI positioning changed modestly versus the prior comparable period.",
  };
}

function countDuplicateFindings(boxes, narrative) {
  const boxBodies = [boxes.biggestStrength?.body, boxes.biggestConstraint?.body, boxes.changeSinceLastRun?.body]
    .filter(Boolean)
    .map((b) => b.replace(/\s+/g, " ").trim().toLowerCase());
  const narr = (narrative || "").replace(/\s+/g, " ").trim().toLowerCase();
  let duplicates = 0;
  for (const body of boxBodies) {
    if (body.length < 40) continue;
    // Exact or near-verbatim reuse of a full box body in the write-up
    if (narr.includes(body.slice(0, Math.min(80, body.length)))) {
      // Allow shared numbers; fail only if majority of box body is copied
      const shared = body.split(/[.!?]/).filter((s) => s.trim().length > 30 && narr.includes(s.trim()));
      if (shared.length >= 2) duplicates += 1;
    }
  }
  return duplicates;
}

function countUndefinedJargon(text) {
  let count = 0;
  for (const rule of UNDEFINED_JARGON) {
    if (!rule.re.test(text)) continue;
    if (!rule.explainedBy.test(text)) count += 1;
  }
  return count;
}

function buildWriteUp(inputs, strengthBox, constraintBox, changeBox, benchmarkFinding, weakBenchmark) {
  const name = inputs.propertyName;
  const paragraphs = [];

  // Paragraph 1 — breadth + consistency
  if (inputs.scenarioPresence != null && inputs.consideration != null) {
    const gap = inputs.scenarioPresence - inputs.consideration;
    if (gap >= 15) {
      paragraphs.push(
        `${name} appeared in at least one AI answer for ${fmtPct(inputs.scenarioPresence)} of the traveler needs we tested, ` +
          `showing that AI recognizes the hotel as relevant across a broad range of stays. ` +
          `Across every individual AI answer, however, ${name} appeared ${fmtPct(inputs.consideration)} of the time. ` +
          `In practical terms, the hotel fits many of the searches being made, but a traveler will not consistently see ${name} in the hotel options surfaced.`
      );
    } else {
      paragraphs.push(
        `${name} appeared in at least one AI answer for ${fmtPct(inputs.scenarioPresence)} of the traveler needs we tested, ` +
          `and across every individual AI answer it appeared ${fmtPct(inputs.consideration)} of the time. ` +
          `Together, these results show how broadly AI recognizes the hotel and how consistently a traveler would encounter it.`
      );
    }
  } else if (inputs.scenarioPresence != null) {
    paragraphs.push(
      `${name} appeared in at least one AI answer for ${fmtPct(inputs.scenarioPresence)} of the traveler needs we tested. ` +
        "This shows how broadly AI recognizes the hotel as relevant across different types of trips."
    );
  } else if (inputs.consideration != null) {
    paragraphs.push(
      `Across all individual AI answers in the monitored set, ${name} appeared ${fmtPct(inputs.consideration)} of the time. ` +
        "This shows how consistently a traveler would encounter the hotel across the AI answers we tested."
    );
  }

  // Paragraph 2 — strongest comparative result + optional Top-3
  const p2 = [];
  if (benchmarkFinding && benchmarkFinding.subjectRatePct != null && benchmarkFinding.coreBenchmarkRatePct != null) {
    const sub = fmtPct(benchmarkFinding.subjectRatePct);
    const core = fmtPct(benchmarkFinding.coreBenchmarkRatePct);
    const relative = translatePresenceIndex(benchmarkFinding.index);
    let line =
      `${benchmarkFinding.territory} is ${name}'s clearest competitive strength. ` +
      `The hotel appeared in ${sub} of AI answers for these searches, compared with ${core} for ${comparableHotelsPhrase()}`;
    if (relative) {
      line += ` — ${relative}`;
    }
    line += ".";
    p2.push(line);
  } else if (strengthBox?.headline && strengthBox.headline !== "Still developing") {
    p2.push(
      `The clearest strength in this period is ${strengthBox.headline.toLowerCase()}. ` +
        "See the summary at left for the supporting numbers."
    );
  }

  if (
    inputs.top3 != null &&
    inputs.rankEligibleN != null &&
    inputs.rankEligibleN >= MIN_RANK_SAMPLE &&
    !(strengthBox?.headline || "").toLowerCase().includes("top 3")
  ) {
    p2.push(
      `Among AI answers where hotel ranking could be measured, ${name} appeared in the Top 3 ${fmtPct(inputs.top3)} of the time, ` +
        "showing how prominently it is positioned when AI includes it."
    );
  }
  if (p2.length) paragraphs.push(p2.join(" "));

  // Paragraph 3 — primary implication + trend (avoid repeating constraint box verbatim)
  const p3 = [];
  if (constraintBox?.headline === "Not appearing consistently in AI answers") {
    p3.push(
      "The main opportunity is consistency: the hotel is already relevant across many traveler needs, " +
        "so the next focus is understanding where it is absent from individual AI results."
    );
  } else if (inputs.reality != null && inputs.reality < 55) {
    p3.push(
      `AI currently reflects ${fmtPct(inputs.reality)} of the monitored facts and attributes that define the hotel, ` +
        "so important parts of the property proposition are not consistently represented in AI results."
    );
  } else if (weakBenchmark && Number(weakBenchmark.index) < 100 && constraintBox?.headline?.includes(weakBenchmark.territory)) {
    p3.push(
      `${weakBenchmark.territory} is a clear area for further investigation: ` +
        `${name} appeared less often than ${comparableHotelsPhrase()} for that type of traveler need.`
    );
  } else if (constraintBox?.headline && constraintBox.headline !== "No single dominant weakness") {
    p3.push(`The clearest current limitation is ${constraintBox.headline.toLowerCase()}.`);
  }

  if (inputs.hasComparablePrior && inputs.deltas) {
    const crDelta = num(inputs.deltas.considerationRate);
    const spDelta = num(inputs.deltas.scenarioPresence);
    const anyMaterial =
      (crDelta != null && Math.abs(crDelta) >= MATERIALITY_PP_THRESHOLD) ||
      (spDelta != null && Math.abs(spDelta) >= MATERIALITY_PP_THRESHOLD);
    if (!anyMaterial) {
      p3.push("Compared with the prior comparable run, the overall position is largely unchanged.");
    } else {
      const changeParts = [];
      if (crDelta != null && Math.abs(crDelta) >= MATERIALITY_PP_THRESHOLD && inputs.consideration != null) {
        const prior = priorRate(inputs.consideration, crDelta);
        if (prior != null) {
          changeParts.push(
            `appearance in individual AI answers moved from ${fmtPct(prior)} to ${fmtPct(inputs.consideration)} (${fmtPpSigned(crDelta)})`
          );
        }
      }
      if (spDelta != null && Math.abs(spDelta) >= MATERIALITY_PP_THRESHOLD && inputs.scenarioPresence != null) {
        const prior = priorRate(inputs.scenarioPresence, spDelta);
        if (prior != null) {
          changeParts.push(
            `recognition across traveler needs moved from ${fmtPct(prior)} to ${fmtPct(inputs.scenarioPresence)} (${fmtPpSigned(spDelta)})`
          );
        }
      }
      if (changeParts.length) {
        p3.push(`Since the prior comparable run, ${changeParts.join("; ")}.`);
      }
    }
  } else {
    p3.push("No comparable prior official monitoring period is available yet, so change over time is not shown.");
  }
  if (p3.length) paragraphs.push(p3.join(" "));

  let narrative = paragraphs.join("\n\n").replace(/[ \t]+/g, " ").replace(/\n /g, "\n").trim();
  const words = narrative.split(/\s+/).length;
  if (words > 180 && paragraphs.length > 2) {
    narrative = paragraphs.slice(0, 2).concat(paragraphs[paragraphs.length - 1]).join("\n\n").trim();
  }
  return narrative;
}

export function buildExecutiveReadUx(payload, period, scenarios, propertyProfile, executiveRead) {
  const benchmarkFinding = findCertifiedBenchmarkFinding(payload);
  const weakBenchmark = findWeakestCertifiedBenchmark(payload);
  const inputs = extractMetricInputs(payload, period, propertyProfile, executiveRead);
  const strength = executiveRead?.current?.primaryStrength;
  const constraint = executiveRead?.current?.primaryConstraint;

  const biggestStrength = buildStrengthBox(inputs, strength, benchmarkFinding);
  const biggestConstraint = buildConstraintBox(inputs, constraint, weakBenchmark);
  const changeSinceLastRun = buildChangeBox(inputs);
  const narrative = buildWriteUp(
    inputs,
    biggestStrength,
    biggestConstraint,
    changeSinceLastRun,
    benchmarkFinding,
    weakBenchmark
  );

  const combined = [biggestStrength.body, biggestConstraint.body, changeSinceLastRun.body, narrative].join(" ");
  const vagueHits = VAGUE_PHRASES.filter((re) => re.test(combined));
  const causal = assertNoUnsupportedCausalLanguage(combined);
  const undefinedJargon = countUndefinedJargon(combined);
  const duplicateFindings = countDuplicateFindings(
    { biggestStrength, biggestConstraint, changeSinceLastRun },
    narrative
  );

  return {
    version: EXECUTIVE_READ_UX_VERSION,
    businessLanguageVersion: BUSINESS_LANGUAGE_GOVERNANCE_VERSION,
    layoutVersion: EXECUTIVE_READ_LAYOUT_VERSION,
    layout: "two_column_v2",
    PROPERTY_SPECIFIC_EXECUTIVE_COPY_CODE,
    biggestStrength,
    biggestConstraint,
    changeSinceLastRun,
    executiveSummary: {
      title: EXECUTIVE_SUMMARY_TITLE,
      narrative,
    },
    safety: {
      VAGUE_UNEXPLAINED_PHRASES: vagueHits.length,
      MARKETING_STYLE_LANGUAGE: causal.ok ? 0 : causal.hits.length,
      UNSUPPORTED_CAUSAL_CLAIMS: causal.ok ? 0 : causal.hits.length,
      USES_ACTUAL_VALUES: /\d+%/.test(combined) ? 1 : 0,
      UNDEFINED_JARGON: undefinedJargon,
      DUPLICATE_EXECUTIVE_FINDING_WITHOUT_NEW_MEANING: duplicateFindings,
      PLAIN_BUSINESS_MEANING_FIRST: 1,
    },
  };
}

/**
 * Full executive read: v1 governance + v2 UX presentation.
 */
function mapUxToCustomerSummary(ux) {
  if (!ux?.biggestStrength) return null;
  return {
    biggestStrength: ux.biggestStrength,
    biggestConstraint: ux.biggestConstraint,
    changeSinceLastComparableRun: ux.changeSinceLastRun,
  };
}

function mapUxToCustomerWriteup(ux) {
  if (!ux?.executiveSummary?.narrative) return null;
  return {
    title: ux.executiveSummary.title || EXECUTIVE_SUMMARY_TITLE,
    body: ux.executiveSummary.narrative,
  };
}

function hasCompleteLeftSummary(er) {
  const summary = er?.summary;
  const ux = er?.ux;
  const strength = ux?.biggestStrength || summary?.biggestStrength;
  const constraint = ux?.biggestConstraint || summary?.biggestConstraint;
  const change =
    ux?.changeSinceLastRun ||
    summary?.changeSinceLastComparableRun ||
    summary?.changeSinceLastRun;
  return Boolean(strength?.headline && strength?.body && constraint?.body && change?.body);
}

/**
 * Stable customer presentation contract for renderer + read-service compatibility.
 */
export function resolveExecutiveReadPresentation(er) {
  if (!er || er.available === false) return null;

  const ux = er.ux;
  const summary = er.summary;
  const writeup = er.writeup;

  const biggestStrength = ux?.biggestStrength || summary?.biggestStrength || null;
  const biggestConstraint = ux?.biggestConstraint || summary?.biggestConstraint || null;
  const changeSinceLastRun =
    ux?.changeSinceLastRun ||
    summary?.changeSinceLastComparableRun ||
    summary?.changeSinceLastRun ||
    null;

  const narrative =
    writeup?.body ||
    ux?.executiveSummary?.narrative ||
    er.current?.narrative ||
    er.narrative ||
    null;
  const title =
    writeup?.title ||
    ux?.executiveSummary?.title ||
    EXECUTIVE_SUMMARY_TITLE;

  if (!narrative && !hasCompleteLeftSummary(er)) return null;

  const fallbackBox = (sectionLabel, headline, body) => ({
    sectionLabel,
    headline,
    body,
  });

  return {
    biggestStrength:
      biggestStrength ||
      fallbackBox(
        "BIGGEST STRENGTH",
        "Still developing",
        "A governed strength summary is not available for this view yet."
      ),
    biggestConstraint:
      biggestConstraint ||
      fallbackBox(
        "BIGGEST CONSTRAINT",
        "Still developing",
        "A governed constraint summary is not available for this view yet."
      ),
    changeSinceLastRun:
      changeSinceLastRun ||
      fallbackBox(
        "CHANGE SINCE LAST COMPARABLE RUN",
        "Change unavailable",
        "Comparable prior-period change is not available for this view yet."
      ),
    title,
    narrative,
  };
}

export function attachExecutiveReadUxLayer(read, payload, period, scenarios, propertyProfile) {
  if (!read?.available) {
    return {
      ...read,
      version: `${EXECUTIVE_READ_VERSION}+${EXECUTIVE_READ_UX_VERSION}`,
      ux: null,
      summary: null,
      writeup: null,
    };
  }
  const ux = buildExecutiveReadUx(payload, period, scenarios, propertyProfile, read);
  const enriched = {
    ...read,
    version: `${EXECUTIVE_READ_VERSION}+${EXECUTIVE_READ_UX_VERSION}`,
    ux,
    summary: mapUxToCustomerSummary(ux),
    writeup: mapUxToCustomerWriteup(ux),
    safety: {
      ...(read.safety || {}),
      ...(ux.safety || {}),
    },
  };
  return enriched;
}

export function buildExecutiveReadWithUx(payload, period, scenarios, propertyProfile, options = {}) {
  const read = buildExecutiveRead(payload, period, scenarios, propertyProfile, options);
  return attachExecutiveReadUxLayer(read, payload, period, scenarios, propertyProfile);
}

export function executiveReadNeedsUxEnrichment(er) {
  if (!er?.available) return false;
  if (!hasCompleteLeftSummary(er)) return true;
  const ux = er.ux;
  if (!ux) return true;
  if (ux.businessLanguageVersion !== BUSINESS_LANGUAGE_GOVERNANCE_VERSION) return true;
  if (ux.layoutVersion !== EXECUTIVE_READ_LAYOUT_VERSION) return true;
  return false;
}
