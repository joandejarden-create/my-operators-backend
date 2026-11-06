/**
 * ADP_SUBJECT_STANCE_ADJUDICATION_V1
 *
 * Separates raw subject *mention* (canonical Consideration numerator) from
 * recommendation *stance* (diagnostic / evidence labeling).
 *
 * Does NOT redefine AI Consideration Rate. Consideration remains mention-based
 * per adp-metric-semantics-registry-v1 / computeConsiderationMetrics.
 *
 * Stance classes (A–F):
 *   EXPLICITLY_RECOMMENDED | POSITIVELY_CONSIDERED | NEUTRALLY_MENTIONED |
 *   MENTIONED_WITH_UNCERTAINTY | EXPLICITLY_NOT_RECOMMENDED | ABSENT
 */

export const ADP_SUBJECT_STANCE_ADJUDICATION_V1 = "ADP_SUBJECT_STANCE_ADJUDICATION_V1";
export const SUBJECT_STANCE_VERSION = "adp_subject_stance_v1.2_explicit_rec_fix";

/** Strip markdown emphasis so "**Recommendation:**" still classifies. */
export function stripMarkdownForStance(text) {
  return String(text || "")
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/\*([^*]+)\*/g, "$1")
    .replace(/__([^_]+)__/g, "$1")
    .replace(/_([^_]+)_/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/^#{1,6}\s+/gm, "");
}

export const SUBJECT_STANCE = Object.freeze({
  EXPLICITLY_RECOMMENDED: "EXPLICITLY_RECOMMENDED",
  POSITIVELY_CONSIDERED: "POSITIVELY_CONSIDERED",
  NEUTRALLY_MENTIONED: "NEUTRALLY_MENTIONED",
  MENTIONED_WITH_UNCERTAINTY: "MENTIONED_WITH_UNCERTAINTY",
  EXPLICITLY_NOT_RECOMMENDED: "EXPLICITLY_NOT_RECOMMENDED",
  ABSENT: "ABSENT",
});

/** Customer-safe badge labels — never imply endorsement from mention alone. */
export const SUBJECT_STANCE_CUSTOMER_LABEL = Object.freeze({
  EXPLICITLY_RECOMMENDED: "Recommended",
  POSITIVELY_CONSIDERED: "Positively considered",
  NEUTRALLY_MENTIONED: "Mentioned",
  MENTIONED_WITH_UNCERTAINTY: "Mentioned with uncertainty",
  EXPLICITLY_NOT_RECOMMENDED: "Not recommended",
  ABSENT: "Missing",
});

const UNCERTAINTY_RE = [
  /\bcan(?:not|'t)\s+confirm\b/i,
  /\bunable\s+to\s+confirm\b/i,
  /\bcould\s+not\s+verify\b/i,
  /\bunable\s+to\s+verify\b/i,
  /\bcannot\s+verify\b/i,
  /\bno\s+(?:independent\s+)?(?:confirmation|verification)\b/i,
  /\bdoes\s+not\s+appear\s+to\s+(?:exist|be\s+(?:a\s+)?(?:real|listed))\b/i,
  /\bif\s+(?:this|the)\s+(?:hotel|property)\s+(?:exists|is\s+real)\b/i,
  /\bunverified\b/i,
  /\bexistence\s+(?:is\s+)?(?:unclear|unconfirmed|uncertain)\b/i,
  /\bi\s+(?:don'?t|do\s+not)\s+have\s+(?:verified|reliable|enough)\s+(?:information|evidence)\b/i,
  /\bdon'?t\s+have\s+verified\s+information\s+confirming\b/i,
  /\bno\s+verified\s+information\b/i,
  /\bas\s+of\s+my\s+(?:last\s+)?(?:update|knowledge(?:\s+cutoff)?)\b/i,
  /\bmight\s+not\s+be\s+readily\s+available\b/i,
  /\bfootprint\s+might\s+be\s+limited\b/i,
  /\bnot\s+(?:finding|found)\s+(?:a\s+)?(?:clear|reliable)\s+(?:listing|record)\b/i,
  /\brecommend\s+checking\s+(?:their|the)\s+(?:current\s+)?website\b/i,
];

const EXPLICIT_REFUSAL_RE = [
  /\bcan(?:not|'t)\s+(?:confidently\s+)?recommend\b/i,
  /\bunable\s+to\s+recommend\b/i,
  /\bi\s+(?:would\s+)?not\s+recommend\b/i,
  /\bdo\s+not\s+recommend\b/i,
  /\bcannot\s+endorse\b/i,
  /\bwithout\s+(?:being\s+able\s+to\s+)?(?:verify|confirm).{0,80}recommend/i,
  /\buntil\s+(?:you\s+)?(?:verify|confirm).{0,60}(?:would\s+)?not\s+recommend/i,
  /\bdecline\s+to\s+recommend\b/i,
  /\bwould\s+not\s+(?:feel\s+)?comfortable\s+recommending\b/i,
];

const EXPLICIT_RECOMMEND_RE = [
  // Exclude "recommend checking/contacting/verifying" — those are hedges, not hotel endorsements.
  /\bi\s+(?:would\s+)?(?:strongly\s+)?recommend\s+(?!checking\b|contacting\b|verifying\b|confirming\b|visiting\s+their\b|a\s+suite\b)/i,
  /\bhighly\s+recommend\b/i,
  /\bmy\s+(?:top\s+)?recommendation\s+is\b/i,
  /\bmy\s+top\s+(?:pick|choice)\s+is\b/i,
  /\bstandout\s+(?:choice|option|pick)\b/i,
  /\bworth\s+(?:booking|staying\s+at|choosing)\b/i,
  // Semantic direct recommendations (founder UMusic Gemini / Perplexity cases)
  /\brecommendation\s*:\s*\S/i,
  /\b(?:the\s+)?(?:clear\s+and\s+ideal|ideal|clear|best|top|short)\s+(?:answer|choice|option|pick)\s+is\b/i,
  /\b(?:clear\s+and\s+ideal|ideal)\s+choice\s+is\b/i,
];

const POSITIVE_CONSIDER_RE = [
  /\b(?:good|strong|solid|excellent|great|clear|ideal)\s+(?:and\s+ideal\s+)?(?:option|choice|fit|pick)\b/i,
  /\bclear\s+and\s+ideal\s+choice\b/i,
  /\bworth\s+considering\b/i,
  /\bwell[- ]suited\b/i,
  /\bideal\s+for\b/i,
  /\ba\s+(?:compelling|attractive)\s+(?:option|choice)\b/i,
  /\bstands\s+out\b/i,
  /^\s*recommendation\s*:\s*/im,
  /\brecommendation\s*:\s*/i,
  /\bthe\s+(?:clear|short)\s+answer\s+is\b/i,
];

const QUALIFIED_RE = [
  /\bmay\s+be\s+suitable\b/i,
  /\bcould\s+be\s+(?:a\s+)?(?:good|strong)\b/i,
  /\bif\s+(?:you\s+)?(?:verify|confirm|can\s+verify)\b/i,
  /\bsubject\s+to\s+(?:availability|verification)\b/i,
  /\bassuming\s+(?:it|the\s+hotel)\s+(?:exists|is\s+available)\b/i,
];

const INSTEAD_COMPETITOR_RE = [
  /\binstead\b/i,
  /\brather\s+than\b/i,
  /\bnot\s+.+\s*,\s*(?:consider|choose|book|try)\b/i,
  /\bconsider\s+.{0,40}\s+instead\b/i,
  /\bi\s+recommend\s+.+\s*,\s*not\b/i,
];

function escapeRegExp(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildSubjectNamePatterns(profile) {
  const names = new Set();
  const push = (n) => {
    const t = String(n || "").trim();
    if (t.length >= 3) names.add(t);
  };
  push(profile?.name);
  push(profile?.displayName);
  for (const a of profile?.identityAliases || []) push(a);
  // Short brand token for windowed checks
  const brand = String(profile?.brand || "").trim();
  if (brand.length >= 4) push(brand);
  return [...names];
}

function subjectWindows(text, names, radius = 220) {
  const t = String(text || "");
  const windows = [];
  for (const name of names) {
    const re = new RegExp(escapeRegExp(name), "ig");
    let m;
    while ((m = re.exec(t))) {
      const start = Math.max(0, m.index - radius);
      const end = Math.min(t.length, m.index + name.length + radius);
      windows.push(t.slice(start, end));
      if (windows.length >= 12) return windows;
    }
  }
  return windows;
}

function anyMatch(patterns, text) {
  for (const re of patterns) {
    if (re.test(text)) return re.source;
  }
  return null;
}

function promptNamesSubject(promptText, names) {
  const p = String(promptText || "");
  if (!p) return false;
  return names.some((n) => new RegExp(escapeRegExp(n), "i").test(p));
}

/**
 * @param {{
 *   rawResponse?: string,
 *   mentioned?: boolean,
 *   governedInterpretation?: { subjectMentioned?: boolean },
 *   scenarioQuery?: string,
 *   promptText?: string,
 * }} obs
 * @param {object} propertyProfile
 */
export function adjudicateSubjectStance(obs, propertyProfile = {}) {
  const mentioned =
    obs?.governedInterpretation?.subjectMentioned != null
      ? Boolean(obs.governedInterpretation.subjectMentioned)
      : Boolean(obs?.mentioned);
  const raw = stripMarkdownForStance(obs?.rawResponse || "");
  const names = buildSubjectNamePatterns(propertyProfile);
  const promptText = obs?.scenarioQuery || obs?.promptText || obs?.query || "";
  const promptLeakage = promptNamesSubject(promptText, names);

  if (!mentioned) {
    return {
      version: SUBJECT_STANCE_VERSION,
      contract: ADP_SUBJECT_STANCE_ADJUDICATION_V1,
      entityMentioned: false,
      stance: SUBJECT_STANCE.ABSENT,
      customerLabel: SUBJECT_STANCE_CUSTOMER_LABEL.ABSENT,
      positiveRecommendation: false,
      independentDiscoveryCredit: false,
      promptNamedSubject: promptLeakage,
      reasons: ["SUBJECT_ABSENT"],
      matchedPatterns: [],
    };
  }

  const windows = subjectWindows(raw, names);
  const focus = windows.length ? windows.join(" || ") : raw;
  const matchedPatterns = [];
  const reasons = [];

  const unc = anyMatch(UNCERTAINTY_RE, focus) || anyMatch(UNCERTAINTY_RE, raw);
  const refusal = anyMatch(EXPLICIT_REFUSAL_RE, focus) || anyMatch(EXPLICIT_REFUSAL_RE, raw);
  const instead = anyMatch(INSTEAD_COMPETITOR_RE, focus);
  // Prefer subject-anchored explicit recommendation; fall back to windowed patterns.
  let recommend = null;
  for (const name of names) {
    const n = escapeRegExp(name);
    const anchoredRec = [
      new RegExp(`recommendation\\s*:\\s*${n}`, "i"),
      new RegExp(
        `(?:clear(?:\\s+and\\s+ideal)?|ideal|clear|best|top|short)\\s+(?:answer|choice|option|pick)\\s+is\\s*:?\\s*${n}`,
        "i"
      ),
      new RegExp(`i\\s+(?:would\\s+)?(?:strongly\\s+)?recommend\\s+${n}`, "i"),
      new RegExp(`(?:book|choose|stay\\s+at)\\s+${n}`, "i"),
      new RegExp(`my\\s+(?:top\\s+)?recommendation\\s+is\\s+${n}`, "i"),
      new RegExp(`${n}\\s+is\\s+the\\s+(?:clear|best|strong|ideal|sole|only)\\s+choice`, "i"),
    ];
    recommend = anyMatch(anchoredRec, raw);
    if (recommend) break;
  }
  if (!recommend) recommend = anyMatch(EXPLICIT_RECOMMEND_RE, focus);
  const positive = anyMatch(POSITIVE_CONSIDER_RE, focus);
  const qualified = anyMatch(QUALIFIED_RE, focus);

  // Negation scope: "recommend X, not UMusic" / "not UMusic"
  const notSubject = names.some((n) =>
    new RegExp(
      `(?:\\bnot\\b\\s+${escapeRegExp(n)})|(?:recommend[\\s\\S]{0,80}\\bnot\\b\\s+${escapeRegExp(n)})`,
      "i"
    ).test(focus)
  );

  let stance = SUBJECT_STANCE.NEUTRALLY_MENTIONED;

  if (unc) {
    matchedPatterns.push(unc);
    reasons.push("UNCERTAINTY_ABOUT_EXISTENCE_OR_VERIFICATION");
    stance = SUBJECT_STANCE.MENTIONED_WITH_UNCERTAINTY;
  }
  if (refusal || notSubject) {
    if (refusal) matchedPatterns.push(refusal);
    reasons.push(notSubject ? "NEGATION_SCOPE_REJECTS_SUBJECT" : "EXPLICIT_REFUSAL_TO_RECOMMEND");
    stance = SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED;
  } else if (instead && unc) {
    reasons.push("DEFERS_TO_ALTERNATIVE_UNDER_UNCERTAINTY");
    stance = SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED;
  } else if (recommend && !unc && !refusal && !notSubject) {
    matchedPatterns.push(recommend);
    reasons.push("EXPLICIT_RECOMMENDATION_LANGUAGE");
    stance = SUBJECT_STANCE.EXPLICITLY_RECOMMENDED;
  } else if (positive && !unc && !refusal) {
    matchedPatterns.push(positive);
    reasons.push("POSITIVE_CONSIDERATION_LANGUAGE");
    stance =
      qualified || promptLeakage
        ? SUBJECT_STANCE.POSITIVELY_CONSIDERED
        : SUBJECT_STANCE.POSITIVELY_CONSIDERED;
  } else if (qualified) {
    matchedPatterns.push(qualified);
    reasons.push("QUALIFIED_CONDITIONAL_CONSIDERATION");
    stance = SUBJECT_STANCE.POSITIVELY_CONSIDERED;
  } else if (!reasons.length) {
    reasons.push(promptLeakage ? "NEUTRAL_MENTION_POSSIBLE_PROMPT_ECHO" : "NEUTRAL_ENTITY_MENTION");
  }

  const positiveRecommendation =
    stance === SUBJECT_STANCE.EXPLICITLY_RECOMMENDED ||
    (stance === SUBJECT_STANCE.POSITIVELY_CONSIDERED && !unc && !refusal);

  // Prompt leakage: repeating the subject name from the prompt is not independent discovery
  const independentDiscoveryCredit =
    mentioned && !promptLeakage && stance !== SUBJECT_STANCE.ABSENT;

  return {
    version: SUBJECT_STANCE_VERSION,
    contract: ADP_SUBJECT_STANCE_ADJUDICATION_V1,
    entityMentioned: true,
    stance,
    customerLabel: SUBJECT_STANCE_CUSTOMER_LABEL[stance],
    positiveRecommendation,
    independentDiscoveryCredit,
    promptNamedSubject: promptLeakage,
    reasons,
    matchedPatterns,
  };
}

export function isPositiveRecommendationStance(stance) {
  return (
    stance === SUBJECT_STANCE.EXPLICITLY_RECOMMENDED ||
    stance === SUBJECT_STANCE.POSITIVELY_CONSIDERED
  );
}

/** True when mention must not be shown as a positive evidence example. */
export function isNonEndorsementStance(stance) {
  return (
    stance === SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED ||
    stance === SUBJECT_STANCE.MENTIONED_WITH_UNCERTAINTY ||
    stance === SUBJECT_STANCE.ABSENT
  );
}

/**
 * Fixture-driven permanent regression cases (A–G).
 */
export function getSubjectStanceNegativeFixtures() {
  const profile = {
    name: "UMusic Hotel Madrid",
    brand: "UMusic Hotels",
    identityAliases: ["UMusic Hotel Madrid", "UMusic Madrid"],
  };
  return [
    {
      id: "A_UNVERIFIED_PROPERTY",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I cannot confirm that UMusic Hotel Madrid exists as a currently operating hotel. Please verify the property on an official booking channel before planning.",
      },
      expect: {
        entityMentioned: true,
        positiveRecommendation: false,
        stance: SUBJECT_STANCE.MENTIONED_WITH_UNCERTAINTY,
      },
    },
    {
      id: "B_EXPLICIT_REFUSAL",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I cannot confidently recommend UMusic Hotel Madrid because its existence is unverified in my sources.",
      },
      expect: {
        entityMentioned: true,
        positiveRecommendation: false,
        stance: SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED,
      },
    },
    {
      id: "C_COMPETITOR_INSTEAD",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I cannot verify UMusic Hotel Madrid. Consider Four Seasons Hotel Madrid instead for a confirmed luxury stay in Centro.",
      },
      expect: {
        entityMentioned: true,
        positiveRecommendation: false,
      },
    },
    {
      id: "D_GENUINE_ENDORSEMENT",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I recommend UMusic Hotel Madrid for travelers who want music-forward luxury near Puerta del Sol, anchored by Teatro Albéniz.",
      },
      expect: {
        entityMentioned: true,
        positiveRecommendation: true,
        stance: SUBJECT_STANCE.EXPLICITLY_RECOMMENDED,
      },
    },
    {
      id: "E_PROMPT_REPETITION",
      profile,
      obs: {
        mentioned: true,
        scenarioQuery: "Is UMusic Hotel Madrid a good boutique stay in Madrid Centro?",
        rawResponse:
          "Regarding UMusic Hotel Madrid: it is a hotel name that appears in your question. Here are general tips for booking boutique hotels in Madrid Centro.",
      },
      expect: {
        entityMentioned: true,
        positiveRecommendation: false,
        independentDiscoveryCredit: false,
        promptNamedSubject: true,
      },
    },
    {
      id: "F_QUALIFIED_CONSIDERATION",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "UMusic Hotel Madrid may be suitable for a culture stay, but verify availability and current operating status before booking.",
      },
      expect: {
        entityMentioned: true,
        positiveRecommendation: true,
        stance: SUBJECT_STANCE.POSITIVELY_CONSIDERED,
      },
    },
    {
      id: "G_NEGATION_SCOPE",
      profile,
      obs: {
        mentioned: true,
        rawResponse:
          "I recommend Four Seasons Hotel Madrid, not UMusic Hotel Madrid, for a verified luxury flagship in the city center.",
      },
      expect: {
        entityMentioned: true,
        positiveRecommendation: false,
        stance: SUBJECT_STANCE.EXPLICITLY_NOT_RECOMMENDED,
      },
    },
  ];
}

export function runSubjectStanceNegativeFixtures() {
  const fixtures = getSubjectStanceNegativeFixtures();
  const results = fixtures.map((fx) => {
    const got = adjudicateSubjectStance(fx.obs, fx.profile);
    const failures = [];
    for (const [k, v] of Object.entries(fx.expect || {})) {
      if (got[k] !== v) failures.push(`${k}: expected ${v}, got ${got[k]}`);
    }
    return {
      id: fx.id,
      pass: failures.length === 0,
      failures,
      got: {
        stance: got.stance,
        entityMentioned: got.entityMentioned,
        positiveRecommendation: got.positiveRecommendation,
        independentDiscoveryCredit: got.independentDiscoveryCredit,
        promptNamedSubject: got.promptNamedSubject,
      },
    };
  });
  return {
    contract: ADP_SUBJECT_STANCE_ADJUDICATION_V1,
    ok: results.every((r) => r.pass),
    results,
  };
}
