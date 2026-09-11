/**
 * Packet 2.6C — deterministic follow-up recommendation engine.
 * Explicit open-question → template mappings. No LLM chooser.
 * Max 3 recommendations.
 */

import { FOLLOW_UP_TEMPLATE_IDS, getTemplate, customerTemplateView } from "./templates.js";

const MAX_RECOMMENDATIONS = 3;

/** Open-question id / keyword → template_id (priority order within a question). */
const QUESTION_TEMPLATE_RULES = [
  {
    template_id: "BRAND_OPERATOR_AGREEMENT",
    match: [/breathless/i, /conversion/i, /reflag/i, /franchise/i, /brand\s*agreement/i, /operator\s*agreement/i, /hacienda/i],
    reason_fallback:
      "The Full Hotel Intelligence Investigation left brand/operator conversion status unresolved.",
  },
  {
    template_id: "DECISION_AUTHORITY",
    match: [
      /signator/i,
      /signing\s*authority/i,
      /deed/i,
      /notarial/i,
      /\bgm\b/i,
      /director\s*general/i,
      /decision/i,
      /contact/i,
      /ubo/i,
      /natural-person/i,
    ],
    reason_fallback:
      "Decision makers, signing authority, or verified contacts remain unresolved.",
  },
  {
    template_id: "OWNERSHIP_CAPITAL_EVENTS",
    match: [/land\s*tenure/i, /ground\s*lease/i, /capital/i, /financing/i, /ownership/i, /mortgage/i, /lien/i],
    reason_fallback:
      "Ownership, land tenure, or capital-structure questions remain open.",
  },
  {
    template_id: "REPOSITIONING_DEVELOPMENT",
    match: [/renovat/i, /reposition/i, /expansion/i, /redevelop/i, /capex/i, /permit/i],
    reason_fallback: "Repositioning or development activity remains unresolved.",
  },
  {
    template_id: "OWNER_PORTFOLIO",
    match: [/portfolio/i, /multi-asset/i, /other\s*hotel/i, /group\s*hotel/i],
    reason_fallback: "Portfolio leverage around the owner/organization remains unresolved.",
  },
  {
    template_id: "CHANGE_OPPORTUNITY",
    match: [/why\s*now/i, /recent\s*change/i, /opportunity/i],
    reason_fallback:
      "Recent change signals plus physical, operating, or management-risk diligence remain unresolved.",
  },
];

function questionText(q) {
  if (!q) return "";
  if (typeof q === "string") return q;
  return String(q.title || q.question || q.text || q.open_question || "").trim();
}

function questionId(q) {
  if (!q || typeof q === "string") return "";
  return String(q.id || q.open_question_id || "").trim();
}

function matchRule(q) {
  const text = `${questionId(q)} ${questionText(q)}`;
  for (const rule of QUESTION_TEMPLATE_RULES) {
    if (rule.match.some((re) => re.test(text))) return rule;
  }
  return null;
}

function cardTitleFor(templateId, matchedQuestion) {
  const q = questionText(matchedQuestion);
  if (/breathless/i.test(q)) {
    return "Current Breathless Conversion Status";
  }
  if (/voco/i.test(q) && (/conversion|reflag|opening/i.test(q))) {
    return "Current voco Conversion Status";
  }
  if (/reflag|conversion|franchise|brand\s*agreement|operator\s*agreement/i.test(q)) {
    return "Brand / Operator Agreement Status";
  }
  if (/signator|deed|authority|notarial/i.test(q)) {
    return "Deed Signatories & Decision Authority";
  }
  if (/gm|director general/i.test(q)) {
    return "Property Leadership Identity";
  }
  if (/land tenure|ground lease/i.test(q)) {
    return "Land Tenure & Capital Structure";
  }
  if (/hacienda/i.test(q)) {
    return "Hacienda Treatment During Conversion";
  }
  if (/ubo|natural-person/i.test(q)) {
    return "Natural-Person Ownership Clarity";
  }
  const t = getTemplate(templateId);
  return t ? t.display_name : "Recommended Follow-up";
}

/**
 * @param {object} input
 * @param {Array} input.open_questions
 * @param {string[]} [input.completed_template_ids]
 * @param {string[]} [input.active_template_ids]
 * @param {boolean} [input.has_full_investigation]
 */
export function recommendFollowUps(input = {}) {
  const openQuestions = Array.isArray(input.open_questions) ? input.open_questions : [];
  const completed = new Set((input.completed_template_ids || []).map((x) => String(x).toUpperCase()));
  const active = new Set((input.active_template_ids || []).map((x) => String(x).toUpperCase()));
  const hasFull = Boolean(input.has_full_investigation);

  if (!hasFull) {
    return { recommendations: [], max: MAX_RECOMMENDATIONS, engine: "deterministic_v1" };
  }

  const byTemplate = new Map();

  for (const q of openQuestions) {
    const rule = matchRule(q);
    if (!rule) continue;
    const tid = rule.template_id;
    if (!FOLLOW_UP_TEMPLATE_IDS.includes(tid)) continue;
    if (completed.has(tid) || active.has(tid)) continue;
    if (!byTemplate.has(tid)) {
      byTemplate.set(tid, {
        template_id: tid,
        matched_open_questions: [],
        reason: rule.reason_fallback,
      });
    }
    const bucket = byTemplate.get(tid);
    bucket.matched_open_questions.push({
      open_question_id: questionId(q) || null,
      title: questionText(q),
      recommended_template_id: tid,
      origin_report_id: q.origin_report_id || input.origin_report_id || null,
      origin_finding_id: q.origin_finding_id || null,
    });
    if (bucket.matched_open_questions.length === 1) {
      const title = questionText(q);
      bucket.reason = title
        ? `The Full Hotel Intelligence Investigation left this unresolved: ${title}`
        : rule.reason_fallback;
      // Prefer plain-language reason for announced conversions still unproven complete
      if (/breathless/i.test(title)) {
        bucket.reason =
          "The Full Hotel Intelligence Investigation confirmed the announced conversion but found no evidence of completion.";
      } else if (/voco/i.test(title) && /conversion|reflag|opening/i.test(title)) {
        bucket.reason =
          "The Full Hotel Intelligence Investigation confirmed the announced voco conversion but found no evidence of completed opening under voco.";
      } else if (/conversion|reflag/i.test(title)) {
        bucket.reason =
          "The Full Hotel Intelligence Investigation confirmed an announced conversion/reflag but found completion status unresolved.";
      }
    }
  }

  // Priority order from QUESTION_TEMPLATE_RULES
  const ordered = [];
  for (const rule of QUESTION_TEMPLATE_RULES) {
    if (byTemplate.has(rule.template_id)) ordered.push(byTemplate.get(rule.template_id));
  }

  const recommendations = ordered.slice(0, MAX_RECOMMENDATIONS).map((item) => {
    const template = getTemplate(item.template_id);
    const primaryQ = item.matched_open_questions[0];
    return {
      card_title: cardTitleFor(item.template_id, primaryQ),
      template: customerTemplateView(template),
      reason: item.reason,
      matched_open_questions: item.matched_open_questions,
      actions: ["review_scope", "run_research"],
    };
  });

  return {
    recommendations,
    max: MAX_RECOMMENDATIONS,
    engine: "deterministic_v1",
  };
}
