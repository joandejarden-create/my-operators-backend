/**
 * Founder-led pilot outreach draft templates by segment.
 * Personal, concise, low-pressure — not salesy or defensive.
 */
export const PILOT_OUTREACH_SENDER_NAME = "Joan";

/**
 * @typedef {{
 *   subjects: string[],
 *   whyTheyMatter: string,
 *   personalization: string,
 *   emailPilotLine: string,
 *   emailIdeaLine: string,
 *   emailSmallGroupLine: string,
 *   linkedInDm: string,
 * }} SegmentTemplate
 */

/** @type {Record<string, SegmentTemplate>} */
export const SEGMENT_DRAFT_TEMPLATES = {
  "Owner / Investor": {
    subjects: [
      "Quick thought on hotel deal readiness",
      "Owner pilot for hotel deal alignment",
      "A small Dealality pilot",
    ],
    whyTheyMatter:
      "Potential owner/investor pilot contact who may have a real or realistic hotel opportunity and useful perspective on deal readiness and alignment.",
    personalization:
      "Given your work around hotel ownership and development, I thought your perspective would be especially useful.",
    emailPilotLine:
      "I'm testing a small Dealality pilot to help owners better structure hotel opportunities before brand/operator conversations.",
    emailIdeaLine:
      "The idea is simple: take one real or realistic opportunity, assess readiness, identify possible brand/operator alignment, and clarify what information may be missing before conversations move too far.",
    emailSmallGroupLine:
      "I'm keeping the first group small and would really value your perspective.",
    linkedInDm:
      "I'm testing a small Dealality pilot to help owners structure opportunities before brand/operator conversations. Given your ownership perspective, I'd really value yours—open to a quick chat?",
  },
  "Advisor / Consultant / Broker": {
    subjects: [
      "Advisor perspective on Dealality pilot",
      "Testing a hotel owner pilot",
      "Would value your view on Dealality",
    ],
    whyTheyMatter:
      "Potential advisor/consultant/broker contact who may help owners structure opportunities and prepare more focused brand/operator conversations.",
    personalization:
      "I'm reaching out because you sit close to the kind of owner/advisor conversations we are trying to make more structured.",
    emailPilotLine:
      "I'm testing a small Dealality pilot to help owners and advisors better structure hotel opportunities before brand/operator conversations.",
    emailIdeaLine:
      "The idea is simple: take one real or realistic opportunity, assess readiness, identify possible brand/operator alignment, and clarify what information may be missing before conversations move too far.",
    emailSmallGroupLine:
      "I'm keeping the first group small and would really value your perspective.",
    linkedInDm:
      "I'm testing a small Dealality pilot to help owners and advisors shape opportunities before brand/operator conversations. Given your advisory role, I'd really value your perspective—open to a quick chat?",
  },
  Operator: {
    subjects: [
      "Operator perspective on Dealality",
      "Testing owner/operator alignment",
      "Quick feedback request",
    ],
    whyTheyMatter:
      "Operator perspective useful for validating whether the owner/operator alignment workflow is useful before outreach begins.",
    personalization:
      "Based on your operator perspective in hospitality, I wanted to get your view on something we are testing.",
    emailPilotLine:
      "I'm testing a small Dealality pilot around how owners prepare for brand/operator conversations—and I'd really value an operator's perspective on whether the alignment workflow is useful.",
    emailIdeaLine:
      "I'm not asking operators to share confidential owner pipelines. I'd value your perspective on what makes an opportunity worth reviewing and what information owners often miss before outreach.",
    emailSmallGroupLine:
      "I'm keeping the pilot intentionally small so we can learn from a few real situations before opening it more broadly.",
    linkedInDm:
      "I'm testing a small Dealality pilot around owner/operator alignment and would really value an operator's perspective. Open to a quick conversation?",
  },
  "Brand / Referral Source": {
    subjects: [
      "Quick Dealality pilot question",
      "Would value your perspective",
      "Owner pilot referral question",
    ],
    whyTheyMatter:
      "Potential referral source or brand-side contact who may know owners or advisors with active hotel opportunities, or offer useful market feedback.",
    personalization:
      "I'm reaching out because your perspective—and any owners or advisors you know with active opportunities—could be helpful as we test a small pilot.",
    emailPilotLine:
      "I'm testing a small Dealality pilot to help owners and advisors better structure hotel opportunities before brand/operator conversations.",
    emailIdeaLine:
      "I'm not asking brands/operators to share confidential owner pipelines. I'd value your perspective on what makes an opportunity worth reviewing, plus any owner-opt-in introductions if relevant.",
    emailSmallGroupLine:
      "I'd really value your perspective if this is relevant to your network.",
    linkedInDm:
      "I'm testing a small Dealality pilot for owners/advisors and would value your perspective—or any relevant introductions. Open to a quick chat?",
  },
  "Capital Partner": {
    subjects: [
      "Quick thought on hotel deal readiness",
      "Would value your perspective on Dealality",
      "Small hospitality pilot question",
    ],
    whyTheyMatter:
      "Potential capital-side contact who may have perspective on how owners prepare opportunities before conversations with brands, operators, or capital partners.",
    personalization:
      "Based on your role in hospitality investment, I wanted to get your view on something we are testing.",
    emailPilotLine:
      "I'm testing a small Dealality pilot to help owners prepare hotel opportunities before conversations with brands, operators, or capital partners.",
    emailIdeaLine:
      "The idea is simple: take one real or realistic opportunity, assess readiness, and clarify what may still be missing before those conversations move too far.",
    emailSmallGroupLine:
      "I'm keeping the first group small and would really value your perspective.",
    linkedInDm:
      "I'm testing a small Dealality pilot around how owners prepare opportunities before brand/operator/capital conversations. I'd value your perspective—open to a quick chat if relevant?",
  },
  Other: {
    subjects: [
      "Quick Dealality pilot question",
      "Would value your perspective",
      "Small hospitality pilot question",
    ],
    whyTheyMatter:
      "Potential pilot contact with hospitality relevance; segment and fit should be confirmed before outreach.",
    personalization:
      "I wanted to reach out because your perspective in hospitality may be useful for a small pilot we are testing.",
    emailPilotLine:
      "I'm testing a small Dealality pilot to help owners and advisors better structure hotel opportunities before brand/operator conversations.",
    emailIdeaLine:
      "The idea is simple: take one real or realistic opportunity, assess readiness, identify possible brand/operator alignment, and clarify what information may be missing before conversations move too far.",
    emailSmallGroupLine:
      "I'm keeping the first group small and would really value your perspective.",
    linkedInDm:
      "I'm testing a small Dealality pilot for hotel owners/advisors and would really value your perspective if relevant. Open to a quick conversation?",
  },
};

export const EMAIL_DRAFT_CTA =
  "Would you be open to a short conversation to see if this might be relevant?";

export const FOLLOW_UP_DRAFT_TEMPLATE =
  "Hi {{firstName}} — just wanted to follow up on my note about the Dealality pilot. No rush at all—I’m keeping the first group small and would still really value your perspective if relevant.";

/** @deprecated Use buildEmailDraft segments; kept empty for grep/tests */
export const EMAIL_DRAFT_CLOSING = "";

export function pickSubject(segment, recordId = "") {
  const tpl = SEGMENT_DRAFT_TEMPLATES[segment] || SEGMENT_DRAFT_TEMPLATES.Other;
  const subjects = tpl.subjects;
  let hash = 0;
  for (let i = 0; i < recordId.length; i += 1) hash = (hash + recordId.charCodeAt(i)) % subjects.length;
  return subjects[hash] || subjects[0];
}

export function buildEmailDraft(firstName, segment, personalizationLine) {
  const tpl = SEGMENT_DRAFT_TEMPLATES[segment] || SEGMENT_DRAFT_TEMPLATES.Other;
  const greeting = firstName ? `Hi ${firstName},` : "Hi,";
  return [
    greeting,
    "",
    personalizationLine || tpl.personalization,
    "",
    tpl.emailPilotLine,
    "",
    tpl.emailIdeaLine,
    "",
    tpl.emailSmallGroupLine,
    "",
    EMAIL_DRAFT_CTA,
    "",
    "At your service,",
    PILOT_OUTREACH_SENDER_NAME,
  ].join("\n");
}

export function buildLinkedInDm(firstName, segment) {
  const tpl = SEGMENT_DRAFT_TEMPLATES[segment] || SEGMENT_DRAFT_TEMPLATES.Other;
  const greeting = firstName ? `Hi ${firstName} —` : "Hi —";
  return `${greeting} ${tpl.linkedInDm}`;
}

export function buildFollowUpDraft(firstName) {
  const name = firstName || "there";
  return FOLLOW_UP_DRAFT_TEMPLATE.replace("{{firstName}}", name);
}

/** Phrases that should never appear in generated outreach drafts. */
export const BANNED_DRAFT_PHRASES = [
  "not a public listing",
  "automated outreach",
  "not a mass outreach",
  "confidential workflow",
];

export function assertNaturalDraftCopy(text) {
  const lower = String(text || "").toLowerCase();
  for (const phrase of BANNED_DRAFT_PHRASES) {
    if (lower.includes(phrase)) {
      throw new Error(`Draft contains banned phrase: ${phrase}`);
    }
  }
}
