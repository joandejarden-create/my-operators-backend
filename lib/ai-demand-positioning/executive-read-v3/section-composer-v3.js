/**
 * V3 section composer — generic prose from analytical state.
 * CLEAR_NOT_CLEVER. No property hard-coded narrative maps.
 */

import { INSIGHT_ARCHETYPES } from "./insight-candidates-v3.js";
import {
  COMPOSITION_V3_RULES,
} from "../governance/adp-executive-read-composition-v3.js";

export const ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE = "ADP_EXECUTIVE_KEY_INSIGHT_SUBSTANTIVE";
export const ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE = "ADP_EXECUTIVE_WHY_IT_MATTERS_SUBSTANTIVE";
export const ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY = "ADP_EXECUTIVE_FOCUS_NOW_FINAL_QUALITY";
export const ADP_EXECUTIVE_WATCH_EXECUTIVE_MATERIALITY = "ADP_EXECUTIVE_WATCH_EXECUTIVE_MATERIALITY";
export const ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE =
  "ADP_EXECUTIVE_WHAT_TO_REVIEW_INCREMENTAL_VALUE";
export const ADP_EXECUTIVE_CLEAR_NOT_CLEVER = "ADP_EXECUTIVE_CLEAR_NOT_CLEVER";
export const ADP_EXECUTIVE_NO_IMPLIED_CAUSATION = "ADP_EXECUTIVE_NO_IMPLIED_CAUSATION";
export const ADP_EXECUTIVE_NO_CUSTOMER_FACING_METHODOLOGY_DEFENSE =
  "ADP_EXECUTIVE_NO_CUSTOMER_FACING_METHODOLOGY_DEFENSE";

const BANNED =
  /\bowns the answer set\b|\bcompetitor war\b|\bweak-brand\b|\bbrand lever\b|\belite\b|\bfranchise\b|\bwrong fight\b|\brival takeover\b|\bprotect-and-polish\b|\bprotect-and-deepen\b|\bunderused lever\b|\bshould (support|reinforce|drive|cause|improve)\b|\bis (driving|causing)\b|\bno causal relationship\b|\bcannot establish\b|\bdoes not,? by itself,? prove\b/i;

function scrub(text) {
  return String(text || "")
    .replace(BANNED, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function nameOf(input) {
  return input.property?.name || "The hotel";
}

function fmtPct(n) {
  if (typeof n !== "number") return null;
  return Number.isInteger(n) ? `${n}%` : `${Math.round(n * 10) / 10}%`;
}

function injectAnchors(text, anchors) {
  // Anchors are already woven into templates; keep helper for parity checks
  return text;
}

function watchEarnsFirst90Seconds(selection, input) {
  // Watch only if a secondary high-material signal differs from primary
  const secondary = selection.supporting?.[0] || selection.ranked?.[1];
  if (!secondary) return null;
  if (secondary.materiality < 0.75 || secondary.confidence < 0.7) return null;
  if (secondary.candidateId === selection.primary?.candidateId) return null;
  if (selection.primary?.archetype === INSIGHT_ARCHETYPES.STRONG_PROTECT && secondary.materiality >= 0.75) {
    // Residual gap for strong hotels is usually Focus Now, not Watch
    return null;
  }
  // Default: omit Watch (founder-hardened corpus largely OMITTED)
  return null;
}

/**
 * Compose V3 sections from input + primary selection + anchors.
 */
export function composeExecutiveReadSectionsV3(input, selection, anchorResult) {
  const hotel = nameOf(input);
  const primary = selection.primary;
  const anchors = anchorResult?.anchors || [];
  const c = input.aiConsideration;
  const sp = input.scenarioPresence;
  const top3 =
    input.rankMetrics?.customerNarrativeEligible === true ? input.rankMetrics?.top3 : null;
  const weak = input.presenceIndex?.weakest;
  const strong = input.presenceIndex?.strongest;
  const topDisp = input.competitiveDisplacement?.[0];
  const topGap =
    primary.supportingRealityGaps?.[0] ||
    selection.supporting?.find((s) => s.supportingRealityGaps?.[0])?.supportingRealityGaps?.[0] ||
    input.realityGaps?.[0]?.label;
  const gapRate = (input.realityGaps || []).find((g) => g.label === topGap)?.recognitionRate;
  const competitor = primary.supportingCompetitors?.[0] || topDisp?.name;
  const constraint = selection.constraintClass;

  let headline = "";
  let keyInsight = "";
  let whyItMatters = "";
  let focusNow = "";
  let whatToReview = "";

  if (primary.archetype === INSIGHT_ARCHETYPES.STRONG_PROTECT) {
    headline = scrub(
      `${hotel} shows broad strength across monitored demand, and the priority is to protect that position while closing any residual representation gaps.`
    );
    keyInsight = scrub(
      `${hotel} combines strong Scenario Presence${sp != null ? ` (${fmtPct(sp)})` : ""} with solid AI Consideration${c != null ? ` (${fmtPct(c)})` : ""}. That combination means the hotel is already relevant across most monitored traveler needs and is carried into answers often enough that inventing a broad competitive crisis would misdirect attention. ${
        topGap && gapRate != null
          ? `A residual Reality Gap remains for ${topGap} (${fmtPct(gapRate)} recognition), which is the kind of concentrated soft spot worth reviewing while the rest of the position stays strong.`
          : competitor
            ? `${competitor} still appears in some absent scenarios, but displacement is not the dominant story relative to overall strength.`
            : "No major deterioration or concentrated displacement dominates the picture."
      } The useful executive read is therefore protect-and-close, not restart.`
    );
    whyItMatters = scrub(
      `When overall position is already strong, the management risk is diffuse activity. Attention should stay on protecting what works and closing concentrated residual gaps. Treating average strength as unfinished work usually wastes focus that should stay on the few attributes or contexts that still under-represent the hotel.`
    );
    focusNow = scrub(
      topGap
        ? `Focus first on clarifying how ${topGap} is represented across priority sources. Confirm first-party and major travel-source coverage before treating this as a competitive problem. Keep leisure and other strong territories in view so residual work does not overwrite a working position.`
        : `Focus first on protecting current source representation for the hotel's strongest demand territories${strong ? ` (including ${strong.label})` : ""}. Review whether distinctive attributes remain complete and consistent across the sources AI systems actually cite.`
    );
    whatToReview = scrub(
      topGap
        ? `Open the Reality Gap evidence for ${topGap} and compare cited attributes with first-party and major travel-source pages. Then spot-check a small set of high-presence scenarios to confirm the residual gap is real and not an artifact of one provider.`
        : `Review the strongest and weakest demand territories side by side and confirm source representation for the hotel's core proposition before changing content priorities.`
    );
  } else if (constraint === "ENTRY" || primary.type === "ENTRY_PROBLEM_NOT_RANK") {
    headline = scrub(
      `${hotel}'s primary issue is entering AI consideration at all${weak ? `, especially for ${weak.label}` : ""}.`
    );
    keyInsight = scrub(
      `AI Consideration is ${fmtPct(c)}, so the hotel appears in only a limited share of individual comparable responses. ${
        competitor
          ? `When the hotel is absent, ${competitor} repeatedly appears instead — a concentrated substitution pattern rather than a diffuse competitor list.`
          : "The weakness is mainly entry, not rank after inclusion."
      } ${typeof top3 === "number" && top3 >= 60 ? `Top-3 appearance can look strong (${fmtPct(top3)}) when the hotel is ranked, which can hide how rarely it enters consideration in the first place.` : "Rank tactics have limited value until relevance improves."} The binding constraint is becoming part of the answer set for the traveler needs the hotel currently misses.`
    );
    whyItMatters = scrub(
      `Fine-tuning rank against individual competitors will have limited value until the hotel becomes relevant in more of the traveler needs it currently misses. Entry problems and rank problems require different management attention. If leadership optimizes competitor comparisons first, the hotel can look busy while still missing the larger answer-set gap.`
    );
    focusNow = scrub(
      `Focus first on how the hotel is represented for ${weak?.label || "the weakest demand territories"}. Review whether the attributes, positioning, and use cases most relevant to those travelers are complete and consistently represented across priority sources. Treat competitor-rank tactics as secondary until entry improves.`
    );
    whatToReview = scrub(
      competitor
        ? `Compare evidence from scenarios where ${competitor} appears with the property information currently available for ${hotel}. Check entry cues first — product type, occasion, and location framing — before broadening the review to secondary attributes.`
        : `Review scenarios where the hotel is absent and compare missing cues with first-party representation for the weakest demand territories.`
    );
  } else if (primary.archetype === INSIGHT_ARCHETYPES.COMPETITIVE_DISPLACEMENT) {
    headline = scrub(
      `${hotel}'s most important issue is concentrated competitive displacement${competitor ? ` by ${competitor}` : ""}, not a vague visibility problem.`
    );
    keyInsight = scrub(
      `${competitor || "A recurring alternative"} appears in ${topDisp?.displacementCount ?? "multiple"} monitored scenarios where ${hotel} is absent. That concentration matters more than a long competitor list because it identifies a repeatable substitution pattern. ${
        topGap && gapRate != null
          ? `Related Reality Gap pressure includes ${topGap} at ${fmtPct(gapRate)} recognition, which may help explain why the hotel is not carried into those answers.`
          : typeof sp === "number" && typeof c === "number" && sp - c >= 15
            ? `Scenario Presence ${fmtPct(sp)} versus Consideration ${fmtPct(c)} shows the hotel is known in the demand set but not consistently carried into answers.`
            : "The pattern is concentrated substitution, not broad random competition."
      }`
    );
    whyItMatters = scrub(
      `Leadership needs to understand why specific alternatives are surfaced instead of ${hotel} in recurring demand contexts. Otherwise attention may stay on average scores while missing the concentrated leak. A displacement pattern that repeats is more actionable than a soft overall visibility narrative.`
    );
    focusNow = scrub(
      `Focus first on the demand contexts where ${competitor || "the top alternative"} is consistently surfaced instead of ${hotel}. Determine whether the relevant attributes and use cases are clearly represented across priority sources, and whether the hotel's own proposition is complete for those same contexts.`
    );
    whatToReview = scrub(
      `Open the underlying displacement evidence for ${competitor || "the top alternative"} and compare attributes cited in those answers with ${hotel}'s first-party and major travel-source representation. Confirm whether the same cues appear when the hotel is present.`
    );
  } else if (primary.archetype === INSIGHT_ARCHETYPES.REALITY_DISCONNECT) {
    headline = scrub(
      `${hotel}'s priority is a Reality Gap in how a distinctive part of the proposition is represented in AI answers.`
    );
    keyInsight = scrub(
      `${topGap || "A material property attribute"} is recognized in only ${fmtPct(gapRate)} of monitored answers. That is low enough to matter commercially if the attribute is part of how the hotel should win relevant traveler needs. ${
        competitor ? `${competitor} also appears in related absent scenarios, which can compound the representation miss.` : "Overall scores can hide this concentrated representation miss."
      } ${typeof sp === "number" && typeof c === "number" ? `Broader Scenario Presence (${fmtPct(sp)}) and Consideration (${fmtPct(c)}) do not, by themselves, prove the distinctive attribute is traveling into answers.` : ""}`
    );
    whyItMatters = scrub(
      `If distinctive property attributes are weakly represented, the hotel can look broadly visible while still losing the moments where its real proposition should matter. This is a representation problem first, not automatically a peer-competition program.`
    );
    focusNow = scrub(
      `Focus first on ${topGap || "the weakest Reality Gap attribute"}. Verify first-party and major travel-source completeness for that attribute before treating the issue as a peer-competition problem. Confirm the attribute is stated clearly where AI systems are most likely to read it.`
    );
    whatToReview = scrub(
      `Open Reality Gap evidence for ${topGap || "the priority attribute"} and compare recognition cues with official property pages and high-authority travel sources. Then check a small set of related demand scenarios for whether the attribute appears when competitors are named.`
    );
  } else if (
    primary.archetype === INSIGHT_ARCHETYPES.BREADTH_PROBLEM ||
    primary.archetype === INSIGHT_ARCHETYPES.CONSISTENCY_PROBLEM
  ) {
    headline = scrub(
      `${hotel} reaches many monitored demand scenarios, but consideration consistency is the binding constraint.`
    );
    keyInsight = scrub(
      `Scenario Presence is ${fmtPct(sp)} while AI Consideration is ${fmtPct(c)}. The hotel is therefore relevant across a wide share of monitored traveler needs, yet it is not carried into individual answers nearly as consistently. ${
        weak ? `Softness concentrates in ${weak.label}${strong ? ` versus stronger ${strong.label}` : ""}.` : ""
      } ${competitor ? `${competitor} often fills absences in those weaker contexts.` : ""} That contrast is more important than either metric alone.`
    );
    whyItMatters = scrub(
      `Broad scenario reach without consistent answer-level inclusion means the hotel is known in the demand set but not reliably carried into traveler-facing answers. Management attention should start with consistency of representation, not with assuming the hotel needs an entirely new competitive set.`
    );
    focusNow = scrub(
      `Focus first on consistency of representation for ${weak?.label || "weaker demand territories"}. Confirm priority sources present the same use cases and attributes that appear in scenarios where the hotel is absent, and close gaps that repeatedly coincide with answer-level misses.`
    );
    whatToReview = scrub(
      `Compare high-presence versus low-consideration scenarios and inspect which sources and attributes appear when the hotel is missing. Use that evidence pack before changing channel strategy.`
    );
  } else if (primary.archetype === INSIGHT_ARCHETYPES.HIDDEN_STRENGTH) {
    const idx = primary.supportingMetrics?.find((m) => String(m.metricId).includes(".index"));
    const top3m =
      input.rankMetrics?.customerNarrativeEligible === true ? input.rankMetrics?.top3 : null;
    headline = scrub(
      `${hotel} shows unusual strength in parts of the monitored set, but that strength can hide concentrated soft spots.`
    );
    keyInsight = scrub(
      `${idx ? `Peer-relative Presence Index reaches ${Math.round(idx.value)}` : top3m != null ? `Top-3 appearance is ${fmtPct(top3m)} when ranked` : "Relative strength is material"}${
        c != null ? `, while overall Consideration is ${fmtPct(c)}` : ""
      }. That combination can look like a simple success story if leadership stops at the headline strength. ${
        competitor
          ? `${competitor} still displaces the hotel in specific contexts.`
          : topGap
            ? `${topGap} remains weakly recognized (${fmtPct(gapRate)}).`
            : "Overall averages can hide where the hotel remains soft."
      } The executive value is in the concentrated contrast, not in restating strength alone.`
    );
    whyItMatters = scrub(
      `The issue is concentrated, not broad. Leadership needs to understand where strength does not travel — otherwise attention may stay on strengths that already work while missing the soft spots that still affect commercially relevant demand contexts.`
    );
    focusNow = scrub(
      competitor
        ? `Focus first on the contexts where ${competitor} is surfaced instead of ${hotel}. Keep existing strength in view while closing this concentrated soft spot, and avoid turning a residual gap into a full repositioning exercise.`
        : `Focus first on the concentrated soft spot${weak ? ` in ${weak.label}` : ""}${topGap ? ` and ${topGap} representation` : ""}. Confirm whether the miss is source completeness, attribute clarity, or a true demand-fit issue.`
    );
    whatToReview = scrub(
      competitor
        ? `Open displacement evidence for ${competitor} and compare cited attributes with ${hotel}'s source representation. Then review the strongest territory evidence to keep the residual work proportional.`
        : `Review the strongest and weakest territory evidence packs side by side and confirm distinctive attribute coverage before changing priorities.`
    );
  } else {
    headline = scrub(`${hotel}'s monitored AI demand position requires a focused management review of the strongest contrast in the analytical state.`);
    keyInsight = scrub(primary.summary);
    whyItMatters = scrub(
      `Without a clear primary issue, leadership risk is diffuse attention. The analytical contrast above is the best-supported starting point.`
    );
    focusNow = scrub(
      `Focus first on validating the primary contrast in evidence — territories, competitors, and Reality Gaps that support the finding.`
    );
    whatToReview = scrub(
      `Open the supporting evidence refs for the primary candidate and confirm the finding before any content or channel change.`
    );
  }

  const watch = watchEarnsFirst90Seconds(selection, input);

  const sections = {
    headline: injectAnchors(headline, anchors),
    keyInsight: injectAnchors(keyInsight, anchors),
    whyItMatters,
    focusNow,
    watch,
    whatToReview,
  };

  const wordCount = Object.values(sections)
    .filter(Boolean)
    .join(" ")
    .split(/\s+/)
    .filter(Boolean).length;

  return {
    sections,
    wordCount,
    targetBand: COMPOSITION_V3_RULES.targetWordCount,
    watchOmitted: watch == null,
    watchMaterialityRule: ADP_EXECUTIVE_WATCH_EXECUTIVE_MATERIALITY,
  };
}
