/**
 * Controlled compression for Executive Read V3 layout overflow.
 * Presentation-only — does not rewrite measurement or change central meaning.
 */

import { evaluateLayoutFitV3, LAYOUT_FIT_STATUS } from "./layout-fit-v3.js";

export const ADP_EXECUTIVE_CONTROLLED_COMPRESSION_V3 =
  "ADP_EXECUTIVE_CONTROLLED_COMPRESSION_V3";
export const ADP_EXECUTIVE_COMPRESSION_SEMANTIC_PARITY =
  "ADP_EXECUTIVE_COMPRESSION_SEMANTIC_PARITY";

const REDUNDANT_SETUP = [
  /\bIn this monitoring period,?\s*/gi,
  /\bOverall,?\s+/gi,
  /\bIt is (also )?worth noting that\s+/gi,
  /\bAs a reminder,?\s*/gi,
  /\bNotably,?\s+/gi,
];

const TRANSITIONAL = [
  /\bThat said,?\s+/gi,
  /\bMeanwhile,?\s+/gi,
  /\bSeparately,?\s+/gi,
  /\bIn addition,?\s+/gi,
  /\bAt the same time,?\s+/gi,
];

function cloneSections(sections) {
  return {
    headline: sections?.headline ?? null,
    keyInsight: sections?.keyInsight ?? null,
    whyItMatters: sections?.whyItMatters ?? null,
    focusNow: sections?.focusNow ?? null,
    watch: sections?.watch ?? null,
    whatToReview: sections?.whatToReview ?? null,
  };
}

function shortenSecondaryCompetitorList(text) {
  if (!text) return text;
  // "A, B, and C as secondary substitutes" → "A and B as secondary substitutes"
  return String(text).replace(
    /,\s+([^,]+),\s+and\s+([^,.]+)\s+(as secondary substitutes|as substitutes)/gi,
    ", $1 and $2 $3"
  );
}

function tighten(text, aggressiveness) {
  let t = String(text || "");
  if (!t) return t;
  for (const re of REDUNDANT_SETUP) t = t.replace(re, "");
  if (aggressiveness >= 2) {
    for (const re of TRANSITIONAL) t = t.replace(re, "");
  }
  if (aggressiveness >= 3) t = shortenSecondaryCompetitorList(t);
  t = t.replace(/\s{2,}/g, " ").replace(/\s+([,.])/g, "$1").trim();
  return t;
}

function shortenSection(text, maxWords) {
  const words = String(text || "").trim().split(/\s+/).filter(Boolean);
  if (words.length <= maxWords) return words.join(" ");
  // Prefer sentence boundary before hard cut
  const joined = words.join(" ");
  const sentences = joined.match(/[^.!?]+[.!?]+|[^.!?]+$/g) || [joined];
  let out = "";
  for (const s of sentences) {
    const next = (out ? out + " " : "") + s.trim();
    if (next.split(/\s+/).length > maxWords && out) break;
    out = next;
  }
  return out.trim() || words.slice(0, maxWords).join(" ");
}

/**
 * One deterministic compression pass.
 * Never removes Focus Now, central conclusion, What to Review, or critical anchors.
 */
export function applyControlledCompressionV3(composition, opts = {}) {
  const surface = opts.surface || "desktop";
  const before = cloneSections(composition?.sections || {});
  const after = cloneSections(before);
  const steps = [];

  // 1–3: redundant setup + transitions on KEY INSIGHT / WHY
  after.keyInsight = tighten(after.keyInsight, 2);
  steps.push("remove_redundant_setup_and_transitions_keyInsight");
  after.whyItMatters = tighten(after.whyItMatters, 2);
  steps.push("tighten_whyItMatters_transitions");

  // 4: shorten secondary competitor lists
  after.keyInsight = shortenSecondaryCompetitorList(after.keyInsight);
  after.focusNow = shortenSecondaryCompetitorList(after.focusNow);
  steps.push("shorten_secondary_competitor_lists");

  // 5: remove marginal WATCH
  if (after.watch) {
    after.watch = null;
    steps.push("omit_marginal_watch");
  }

  // 6–7: tighten WHAT TO REVIEW / WHY (length only, keep sentence meaning)
  after.whatToReview = shortenSection(tighten(after.whatToReview, 1), 70);
  steps.push("tighten_whatToReview");
  after.whyItMatters = shortenSection(after.whyItMatters, 70);
  steps.push("tighten_whyItMatters");

  // Never empty critical sections
  if (!after.headline) after.headline = before.headline;
  if (!after.keyInsight) after.keyInsight = before.keyInsight;
  if (!after.focusNow) after.focusNow = before.focusNow;
  if (!after.whatToReview) after.whatToReview = before.whatToReview;

  const fitAfter = evaluateLayoutFitV3(after, surface);
  const parity = evaluateCompressionSemanticParityV3(
    { ...composition, sections: before },
    { ...composition, sections: after }
  );

  return {
    gate: ADP_EXECUTIVE_CONTROLLED_COMPRESSION_V3,
    applied: true,
    steps,
    before,
    after,
    fitAfter,
    semanticParity: parity,
    pass:
      parity.pass &&
      fitAfter.status !== LAYOUT_FIT_STATUS.LAYOUT_REVIEW_REQUIRED,
  };
}

export function evaluateCompressionSemanticParityV3(pre, post) {
  const fails = [];
  if (!post?.sections?.headline) fails.push("MISSING_HEADLINE");
  if (!post?.sections?.keyInsight) fails.push("MISSING_KEY_INSIGHT");
  if (!post?.sections?.focusNow) fails.push("MISSING_FOCUS_NOW");
  if (!post?.sections?.whatToReview) fails.push("MISSING_WHAT_TO_REVIEW");

  if (
    pre?.primaryIssueId &&
    post?.primaryIssueId &&
    pre.primaryIssueId !== post.primaryIssueId
  ) {
    fails.push("PRIMARY_ISSUE_ID_CHANGED");
  }
  if (
    pre?.insightArchetype &&
    post?.insightArchetype &&
    pre.insightArchetype !== post.insightArchetype
  ) {
    fails.push("INSIGHT_ARCHETYPE_CHANGED");
  }

  const preAnchors = JSON.stringify(pre?.numericAnchors || []);
  const postAnchors = JSON.stringify(post?.numericAnchors || []);
  // Anchors may be unchanged object; allow same set
  if (preAnchors !== postAnchors && (post?.numericAnchors || []).length === 0 && (pre?.numericAnchors || []).length > 0) {
    fails.push("NUMERIC_ANCHORS_REMOVED");
  }

  // Focus Now must remain substantive overlap with pre
  const preFocus = String(pre?.sections?.focusNow || "").toLowerCase();
  const postFocus = String(post?.sections?.focusNow || "").toLowerCase();
  if (preFocus && postFocus) {
    const preTokens = new Set(preFocus.split(/\s+/).filter((w) => w.length > 4));
    let hit = 0;
    for (const t of postFocus.split(/\s+/)) if (preTokens.has(t)) hit += 1;
    if (preTokens.size > 5 && hit / preTokens.size < 0.35) {
      fails.push("FOCUS_NOW_SEMANTIC_DRIFT");
    }
  }

  return {
    gate: ADP_EXECUTIVE_COMPRESSION_SEMANTIC_PARITY,
    pass: fails.length === 0,
    fails,
    status: fails.length ? "LAYOUT_REVIEW_REQUIRED" : "PASS",
  };
}

/**
 * Apply compression only when layout fit requires it.
 */
export function maybeCompressForLayoutV3(composition, opts = {}) {
  const surface = opts.surface || "desktop";
  const fit = evaluateLayoutFitV3(composition?.sections || {}, surface);
  if (
    fit.status !== LAYOUT_FIT_STATUS.COMPRESSION_REQUIRED &&
    fit.status !== LAYOUT_FIT_STATUS.LAYOUT_REVIEW_REQUIRED
  ) {
    return {
      composition,
      compressionApplied: false,
      fit,
      semanticParity: { pass: true, fails: [] },
    };
  }
  const result = applyControlledCompressionV3(composition, { surface });
  return {
    composition: {
      ...composition,
      sections: result.after,
      compressionMeta: {
        applied: true,
        steps: result.steps,
        gate: ADP_EXECUTIVE_CONTROLLED_COMPRESSION_V3,
      },
    },
    compressionApplied: true,
    fit: result.fitAfter,
    fitBefore: fit,
    semanticParity: result.semanticParity,
    layoutReviewRequired: result.fitAfter?.status === LAYOUT_FIT_STATUS.LAYOUT_REVIEW_REQUIRED,
  };
}
