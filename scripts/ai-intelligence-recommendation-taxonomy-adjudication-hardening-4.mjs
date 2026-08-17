#!/usr/bin/env node
/**
 * Recommendation Taxonomy Adjudication (Hardening 4 Part 1–2).
 * Audits all DEV recommendation mismatches against taxonomy contract.
 * Does NOT change human labels. Does NOT run holdout. No provider calls.
 *
 * If GROUND_TRUTH_REVIEW_CANDIDATE cases exist → stop before classifier tuning.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import {
  RECOMMENDATION_ROLE_PRECEDENCE,
  RECOMMENDATION_CLASSIFIER_VERSION,
  detectRankMarker,
  detectBulletLine,
  detectOrderedListContext,
  detectNumberedBrandHeadingList,
  isDocumentTopicHeading,
  detectResponseSections,
  sectionAt,
  questionStatusFromRecommendationRole,
} from "../lib/ai-visibility/recommendation-classifier-v3.js";
import { TAXONOMY_HELP } from "../lib/ai-visibility/validation/golden-set-review-packet.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "data/ai-visibility/validation");
const AUDIT_PATH = path.join(OUT_DIR, "recommendation-taxonomy-adjudication-hardening-4.json");
const GT_REVIEW_PATH = path.join(OUT_DIR, "recommendation-taxonomy-ground-truth-review.json");

const POSITIVE =
  /\b(recommend(?:ed)?|should\s+consider|may\s+also\s+consider|also\s+consider|i\s+would\s+(?:consider|shortlist|solicit)|strong\s+(?:fit|candidate|alternative|option|options|choice|choices)|best\s+fit|best\s+suited|good\s+(?:option|fit)|preferred(?:\s+option)?|particularly\s+suitable|likely\s+fit|solicit\s+proposals?\s+from|primary\s+option|leading\s+candidate|issue\s+an\s+rfp\s+to|recomendad[oa]s?|recomiendo|opci[oó]n\s+(?:fuerte|s[oó]lida|preferida)|opciones\s+(?:fuertes|s[oó]lidas)|mejor\s+opci[oó]n|mejor\s+encaje|buen\s+encaje)\b/i;

const CONSIDERATION =
  /\b(options?\s+include|brands?\s+typically\s+considered|shortlist\s+includes|alternatives?\s+include|consideration\s+set|commonly\s+(?:associated|considered|cited)|typically\s+(?:associated|considered)|often\s+(?:associated|considered)|se\s+consideran|suelen\s+considerar|opciones\s+incluyen|marcas?\s+a\s+considerar|conviene\s+considerar|las\s+m[aá]s\s+citadas|commonly\s+cited\s+choices?)\b/i;

const COMPARATOR =
  /\b(versus|vs\.?|compared\s+(?:to|with)|alternative\s+to|unlike|similar\s+to|frente\s+a|comparad[oa]\s+con)\b/i;

const NEGATIVE =
  /\b(not\s+recommend(?:ed)?|avoid|poor\s+fit|weak\s+fit|no\s+recomend|evitar|menos\s+adecuado|not\s+ideal)\b/i;

const RANK_HEADER =
  /\b(recommended\s+shortlist|shortlist|top\s+\d+|top\s+options?|ranked|priority\s+list|lista\s+corta|marcas?\s+recomendadas?)\b/i;

const REC_HEADING =
  /\b(brands?\s+to\s+consider|operators?\s+to\s+consider|recommended\s+(?:brands?|operators?|shortlist)|marcas?\s+a\s+considerar|key\s+.*brands?\s+to\s+consider)\b/i;

const FIRST_LEAD =
  /\b(first\s+call|first\s+choice|first\s+option|top\s+choice|primary\s+recommendation|primera\s+opci[oó]n|#\s*1\b|priority\s+1|prioridad\s+1)\b/i;

/** Clarified mutually exclusive decision tree (proposed contract for adjudication). */
const PROPOSED_DEFINITIONS = {
  no_mention: "Entity absent from response.",
  negative_or_qualified:
    "Entity is materially discouraged, excluded, or negatively/qualified positioned.",
  first_recommendation:
    "Explicit lead semantics (#1 / first choice / primary recommendation) OR position 1 inside a confirmed ranked recommendation structure. First textual mention alone is never sufficient.",
  ranked_recommendation:
    "Entity is an ordered non-first recommendation inside a structure where ordering is semantically meaningful (table rank, ordered shortlist, priority N, #N). Document topic section numbers alone do not count.",
  explicit_recommendation:
    "Direct evaluative or directive positive language attributable to the entity (recommended, strong choice, best fit, should consider, recomendaría, opción sólida). Shortlist membership alone is NOT enough.",
  associated_option:
    "Entity is included in a viable choice / consideration / shortlist universe WITHOUT direct positive evaluation of that entity.",
  comparator: "Entity is used principally as a comparison reference, not as the recommended choice.",
  discussed:
    "Substantive neutral description/context without decision-set inclusion and without direct positive recommendation.",
  passing_mention: "Incidental/brief mention without recommendation weight.",
  source_only: "Appears only via source/citation framing.",
};

const CURRENT_DOC_DEFINITIONS = TAXONOMY_HELP.recommendationStatus;

const CURRENT_CODE_OPERATIONAL = {
  first_recommendation:
    "rank===1 with ranked/table/ordered/shortlist/strong-intent context OR first-call language OR shortlist structural position 1 OR post-pass promotion of leading explicit/ranked entity",
  ranked_recommendation:
    "rank>1 with ranked context OR shortlist bullet membership under RANKED_SHORTLIST_HEADERS (including 'commonly cited choices' / 'brands to consider')",
  explicit_recommendation:
    "POSITIVE_REC_CUES on entity line or after mention OR (shortlistNear && strongRecIntent && !bullet)",
  associated_option:
    "ASSOCIATION_CUES in close lookbehind OR consideration catalog heading OR association intent territory OR numbered brand catalog without shortlist/positive",
  comparator: "comparator object phrasing before entity OR comparison section role",
  discussed: "default when no higher-precedence path matches",
  passing_mention: "PASSING_CUES (for example / such as) without recommendation context",
  negative_or_qualified: "NEGATIVE_CUES in snip/window",
  source_only: "SOURCE_ONLY_CUES or source section",
  no_mention: "No entity span resolved (resolver layer)",
};

function lineAt(text, start) {
  const s = String(text || "");
  const a = s.lastIndexOf("\n", start - 1) + 1;
  const b = s.indexOf("\n", start);
  return s.slice(a, b === -1 ? s.length : b);
}

function localCtx(text, start, end) {
  return String(text || "").slice(Math.max(0, start - 280), Math.min(String(text || "").length, end + 220));
}

function entityStart(text, name) {
  if (!name) return 0;
  const idx = String(text || "").toLowerCase().indexOf(String(name).toLowerCase());
  return idx >= 0 ? idx : 0;
}

function evidenceFor(text, name) {
  const start = entityStart(text, name);
  const end = start + String(name || "").length;
  const line = lineAt(text, start);
  const behind = String(text || "").slice(Math.max(0, start - 350), start);
  const after = String(text || "").slice(start, Math.min(String(text || "").length, end + 160));
  const sections = detectResponseSections(text);
  const sec = sectionAt(text, start, sections);
  const rank = detectRankMarker(text, start);
  const bullet = detectBulletLine(text, start);
  const ordered = detectOrderedListContext(text, start);
  const numberedBrand = detectNumberedBrandHeadingList(text, start);
  const topicHeading = isDocumentTopicHeading(line) || isDocumentTopicHeading(sec?.title || "");

  const hasDirectPositive = POSITIVE.test(line) || POSITIVE.test(after);
  const hasConsideration = CONSIDERATION.test(behind) || CONSIDERATION.test(sec?.title || "");
  const hasComparator = COMPARATOR.test(behind.slice(-80)) || COMPARATOR.test(line);
  const hasNegative = NEGATIVE.test(line) || NEGATIVE.test(after);
  const hasRankHeader = RANK_HEADER.test(behind) || RANK_HEADER.test(sec?.title || "");
  const hasRecHeading = REC_HEADING.test(behind) || REC_HEADING.test(sec?.title || "");
  const hasFirstLead = FIRST_LEAD.test(line) || FIRST_LEAD.test(after) || rank === 1;

  // Ranking semantic adjudication
  let rankingKind = "NONE";
  if (rank != null && /\|/.test(line)) rankingKind = "TABLE_RANK";
  else if (rank != null && ordered && !topicHeading) rankingKind = "ORDERED_LIST_RANK";
  else if (rank != null && numberedBrand && !topicHeading && (hasRankHeader || hasRecHeading))
    rankingKind = "NUMBERED_BRAND_SHORTLIST";
  else if (rank != null && topicHeading) rankingKind = "SECTION_NUMBER_ONLY";
  else if (rank != null && isDocumentTopicHeading(line)) rankingKind = "SECTION_NUMBER_ONLY";
  else if (rank != null && !hasRankHeader && !hasRecHeading && !ordered && !/\|/.test(line))
    rankingKind = "AMBIGUOUS_NUMBER";
  else if (rank != null) rankingKind = "TRUE_RANKING_CANDIDATE";
  else if ((hasRankHeader || hasRecHeading) && (bullet || ordered)) rankingKind = "SHORTLIST_MEMBER_UNNUMBERED";

  const trueRanking =
    rankingKind === "TABLE_RANK" ||
    rankingKind === "ORDERED_LIST_RANK" ||
    rankingKind === "NUMBERED_BRAND_SHORTLIST" ||
    rankingKind === "TRUE_RANKING_CANDIDATE";

  return {
    start,
    end,
    LINE: line.slice(0, 240),
    LOCAL_ENTITY_CONTEXT: localCtx(text, start, end).slice(0, 700),
    SECTION_PARENT_HEADING: (sec?.title || "").slice(0, 160) || null,
    SECTION_ROLE: sec?.sectionRole || null,
    IS_ORDERED_LIST: ordered,
    IS_BULLET: bullet,
    ORDER_POSITION: rank,
    HAS_RANK_HEADER: hasRankHeader,
    HAS_RECOMMENDATION_HEADING: hasRecHeading,
    HAS_DIRECT_POSITIVE_CUE: hasDirectPositive,
    HAS_CONSIDERATION_SET_CUE: hasConsideration,
    HAS_COMPARATOR_CUE: hasComparator,
    HAS_NEGATIVE_CUE: hasNegative,
    HAS_FIRST_LEAD_CUE: hasFirstLead,
    HAS_ONLY_DESCRIPTION: !hasDirectPositive && !hasConsideration && !hasComparator && !hasNegative && !trueRanking,
    NUMBERED_BRAND_LIST: numberedBrand,
    TOPIC_SECTION_NUMBER: topicHeading && rank != null,
    RANKING_KIND: rankingKind,
    TRUE_RANKING: trueRanking,
    SECTION_NUMBER_ONLY: rankingKind === "SECTION_NUMBER_ONLY",
    AMBIGUOUS_RANKING: rankingKind === "AMBIGUOUS_NUMBER" || rankingKind === "SHORTLIST_MEMBER_UNNUMBERED",
  };
}

/**
 * Proposed tree label from evidence (adjudication only — not applied as GT).
 */
function proposedTreeLabel(ev) {
  if (ev.HAS_NEGATIVE_CUE) return "negative_or_qualified";
  if (ev.HAS_FIRST_LEAD_CUE && (ev.TRUE_RANKING || ev.HAS_DIRECT_POSITIVE_CUE || ev.ORDER_POSITION === 1)) {
    return "first_recommendation";
  }
  if (ev.TRUE_RANKING && ev.ORDER_POSITION === 1) return "first_recommendation";
  if (ev.TRUE_RANKING && ev.ORDER_POSITION != null && ev.ORDER_POSITION > 1) return "ranked_recommendation";
  if (ev.HAS_DIRECT_POSITIVE_CUE) return "explicit_recommendation";
  if (ev.HAS_CONSIDERATION_SET_CUE || ev.RANKING_KIND === "SHORTLIST_MEMBER_UNNUMBERED") {
    return "associated_option";
  }
  if (ev.HAS_COMPARATOR_CUE) return "comparator";
  if (ev.HAS_ONLY_DESCRIPTION) return "discussed";
  return "discussed";
}

/**
 * Classify adjudication outcome.
 */
function adjudicate(human, classifier, proposed, ev) {
  const flags = {
    CLASSIFIER_ERROR: false,
    TAXONOMY_AMBIGUITY: false,
    GROUND_TRUTH_REVIEW_CANDIDATE: false,
    STRUCTURAL_PARSE_ERROR: false,
  };
  let reason = "";

  // Structural: true ranking present but classifier missed → structural/classifier
  if (human === "ranked_recommendation" && classifier === "discussed" && ev.TRUE_RANKING) {
    flags.STRUCTURAL_PARSE_ERROR = true;
    flags.CLASSIFIER_ERROR = true;
    reason = "True ranking evidence present; classifier lost structural rank.";
  } else if (human === "ranked_recommendation" && classifier === "explicit_recommendation") {
    if (ev.TRUE_RANKING) {
      flags.CLASSIFIER_ERROR = true;
      reason = "True ranking present; classifier under-ranked to explicit.";
    } else if (ev.SECTION_NUMBER_ONLY) {
      flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
      flags.TAXONOMY_AMBIGUITY = true;
      reason =
        "Human labeled ranked but evidence looks like section numbering / non-rank structure. Review GT vs clarified ranked boundary.";
    } else if (ev.RANKING_KIND === "SHORTLIST_MEMBER_UNNUMBERED") {
      flags.TAXONOMY_AMBIGUITY = true;
      flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
      reason =
        "Unnumbered shortlist membership: clarified taxonomy prefers associated_option (or ranked only if order is explicitly meaningful). Human=ranked, classifier=explicit — both may conflict with clarified contract.";
    } else if (!ev.HAS_DIRECT_POSITIVE_CUE && ev.HAS_CONSIDERATION_SET_CUE) {
      flags.TAXONOMY_AMBIGUITY = true;
      flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
      reason =
        "Consideration-set membership without direct positive cue and without clear rank — ranked vs associated vs explicit boundary ambiguous.";
    } else {
      flags.TAXONOMY_AMBIGUITY = true;
      reason = "ranked→explicit without clear structural rank evidence.";
    }
  } else if (human === "discussed" && classifier === "associated_option") {
    if (ev.HAS_CONSIDERATION_SET_CUE) {
      flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
      flags.TAXONOMY_AMBIGUITY = true;
      reason =
        "Consideration-set cue present; clarified taxonomy → associated_option. Human=discussed may be inconsistent.";
    } else if (ev.HAS_ONLY_DESCRIPTION) {
      flags.CLASSIFIER_ERROR = true;
      reason = "Neutral description only; classifier over-associated.";
    } else {
      flags.TAXONOMY_AMBIGUITY = true;
      reason = "discussed vs associated boundary unclear from evidence.";
    }
  } else if (human === "associated_option" && classifier === "discussed") {
    if (ev.HAS_CONSIDERATION_SET_CUE || ev.RANKING_KIND === "SHORTLIST_MEMBER_UNNUMBERED") {
      flags.CLASSIFIER_ERROR = true;
      reason = "Choice-set / consideration cue present; classifier under-associated.";
    } else if (ev.HAS_ONLY_DESCRIPTION) {
      flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
      flags.TAXONOMY_AMBIGUITY = true;
      reason =
        "No clear choice-set cue; clarified taxonomy → discussed. Human=associated may be inconsistent.";
    } else {
      flags.TAXONOMY_AMBIGUITY = true;
      reason = "associated↔discussed ambiguous.";
    }
  } else if (human === "discussed" && classifier === "explicit_recommendation") {
    if (!ev.HAS_DIRECT_POSITIVE_CUE) {
      flags.CLASSIFIER_ERROR = true;
      reason = "No entity-linked positive cue; classifier over-promoted to explicit.";
    } else {
      flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
      reason = "Direct positive cue present; human=discussed may under-label recommendation.";
    }
  } else if (human === "explicit_recommendation" && classifier !== "explicit_recommendation") {
    if (ev.HAS_DIRECT_POSITIVE_CUE) {
      flags.CLASSIFIER_ERROR = true;
      reason = "Direct positive cue present; classifier missed explicit.";
    } else {
      flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
      reason = "Human=explicit without clear entity-linked positive cue under clarified boundary.";
    }
  } else if (proposed !== human && proposed === classifier) {
    flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
    flags.TAXONOMY_AMBIGUITY = true;
    reason = `Clarified tree proposes ${proposed} (matches classifier); human=${human}.`;
  } else if (proposed === human && proposed !== classifier) {
    flags.CLASSIFIER_ERROR = true;
    reason = `Clarified tree agrees with human (${human}); classifier=${classifier}.`;
  } else if (proposed !== human && proposed !== classifier) {
    flags.TAXONOMY_AMBIGUITY = true;
    if (human !== proposed) flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
    reason = `Three-way split: human=${human}, classifier=${classifier}, proposed=${proposed}.`;
  } else {
    flags.CLASSIFIER_ERROR = true;
    reason = `Mismatch human=${human} classifier=${classifier}.`;
  }

  // Never auto-change — mark review if tree disagrees with human
  if (proposed !== human && !flags.GROUND_TRUTH_REVIEW_CANDIDATE && flags.TAXONOMY_AMBIGUITY) {
    flags.GROUND_TRUTH_REVIEW_CANDIDATE = true;
  }

  return { ...flags, ADJUDICATION_REASON: reason, PROPOSED_TREE_LABEL: proposed };
}

const golden = loadGoldenSet();
const holdoutN = (golden.cases || []).filter((c) => c.holdoutSplit === "holdout").length;
if (holdoutN < 1) {
  console.error("BLOCKED: holdout partition missing");
  process.exit(2);
}

const score = await scoreGoldenSetHydrated(golden, { holdoutPolicy: "exclude" });
if (score.HOLDOUT_ACCESSED || score.HOLDOUT_METRICS_RUN) {
  console.error("BLOCKED: holdout accessed");
  process.exit(2);
}

const roles = new Set([
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "negative_or_qualified",
  "source_only",
]);

const recErrors = (score.errors || []).filter(
  (e) => roles.has(e.EXPECTED) || roles.has(e.ACTUAL)
);

const { cases: hydrated } = await hydrateGoldenSetCasesForScoring(
  (golden.cases || []).filter((c) => c.holdoutSplit !== "holdout"),
  {}
);
const byId = new Map(hydrated.map((c) => [c.caseId || c.id, c]));

const audited = [];
const pairBuckets = {
  "ranked_recommendation => explicit_recommendation": [],
  "discussed => associated_option": [],
  "associated_option => discussed": [],
  "ranked_recommendation => discussed": [],
  other: [],
};

let counts = {
  CLASSIFIER_ERRORS: 0,
  TAXONOMY_AMBIGUITIES: 0,
  GROUND_TRUTH_REVIEW_CANDIDATES: 0,
  STRUCTURAL_PARSE_ERRORS: 0,
};

const europe = [];

for (const e of recErrors) {
  const c = byId.get(e.CASE_ID) || {};
  const text = c.text || "";
  const entity = e.ENTITY || c.entityName || "";
  const ev = evidenceFor(text, entity);
  const proposed = proposedTreeLabel(ev);
  const adj = adjudicate(e.EXPECTED, e.ACTUAL, proposed, ev);
  const pair = `${e.EXPECTED} => ${e.ACTUAL}`;
  const row = {
    CASE_ID: e.CASE_ID,
    PROVIDER: e.PROVIDER || c.provider || null,
    LANGUAGE: e.LANGUAGE || c.language || null,
    GEOGRAPHY: e.GEOGRAPHY || c.geography || null,
    PROMPT_FAMILY: e.PROMPT_FAMILY || c.promptFamily || c.promptIntentTerritory || null,
    ENTITY: entity,
    HUMAN_LABEL: e.EXPECTED,
    CLASSIFIER_LABEL: e.ACTUAL,
    ...ev,
    ...adj,
  };
  audited.push(row);
  if (pairBuckets[pair]) pairBuckets[pair].push(row);
  else pairBuckets.other.push(row);

  if (adj.CLASSIFIER_ERROR) counts.CLASSIFIER_ERRORS += 1;
  if (adj.TAXONOMY_AMBIGUITY) counts.TAXONOMY_AMBIGUITIES += 1;
  if (adj.GROUND_TRUTH_REVIEW_CANDIDATE) counts.GROUND_TRUTH_REVIEW_CANDIDATES += 1;
  if (adj.STRUCTURAL_PARSE_ERROR) counts.STRUCTURAL_PARSE_ERRORS += 1;

  if (/europe/i.test(String(row.GEOGRAPHY || ""))) europe.push(row);
}

function summarizePair(rows, kind) {
  if (kind === "ranked_explicit") {
    return {
      TOTAL: rows.length,
      TRUE_RANKING: rows.filter((r) => r.TRUE_RANKING).length,
      SECTION_NUMBER_ONLY: rows.filter((r) => r.SECTION_NUMBER_ONLY).length,
      AMBIGUOUS: rows.filter((r) => r.AMBIGUOUS_RANKING || (!r.TRUE_RANKING && !r.SECTION_NUMBER_ONLY)).length,
      GT_REVIEW: rows.filter((r) => r.GROUND_TRUTH_REVIEW_CANDIDATE).length,
      CLASSIFIER_ERROR: rows.filter((r) => r.CLASSIFIER_ERROR).length,
      CASES: rows.map((r) => ({
        CASE_ID: r.CASE_ID,
        RANKING_KIND: r.RANKING_KIND,
        HUMAN: r.HUMAN_LABEL,
        CLASSIFIER: r.CLASSIFIER_LABEL,
        PROPOSED: r.PROPOSED_TREE_LABEL,
        FLAG: r.GROUND_TRUTH_REVIEW_CANDIDATE
          ? "GROUND_TRUTH_REVIEW_CANDIDATE"
          : r.CLASSIFIER_ERROR
            ? "CLASSIFIER_ERROR"
            : "TAXONOMY_AMBIGUITY",
        REASON: r.ADJUDICATION_REASON,
        HEADING: r.SECTION_PARENT_HEADING,
        LINE: r.LINE,
      })),
    };
  }
  return {
    TOTAL: rows.length,
    GT_REVIEW: rows.filter((r) => r.GROUND_TRUTH_REVIEW_CANDIDATE).length,
    CLASSIFIER_ERROR: rows.filter((r) => r.CLASSIFIER_ERROR).length,
    TAXONOMY_AMBIGUITY: rows.filter((r) => r.TAXONOMY_AMBIGUITY).length,
    CASES: rows.map((r) => ({
      CASE_ID: r.CASE_ID,
      HUMAN: r.HUMAN_LABEL,
      CLASSIFIER: r.CLASSIFIER_LABEL,
      PROPOSED: r.PROPOSED_TREE_LABEL,
      HAS_CONSIDERATION_SET_CUE: r.HAS_CONSIDERATION_SET_CUE,
      HAS_DIRECT_POSITIVE_CUE: r.HAS_DIRECT_POSITIVE_CUE,
      HAS_ONLY_DESCRIPTION: r.HAS_ONLY_DESCRIPTION,
      FLAG: r.GROUND_TRUTH_REVIEW_CANDIDATE
        ? "GROUND_TRUTH_REVIEW_CANDIDATE"
        : r.CLASSIFIER_ERROR
          ? "CLASSIFIER_ERROR"
          : "TAXONOMY_AMBIGUITY",
      REASON: r.ADJUDICATION_REASON,
      LINE: r.LINE,
    })),
  };
}

const europeBy = {
  PROVIDER: {},
  LANGUAGE: {},
  PROMPT_FAMILY: {},
  STRUCTURE: {},
};
for (const r of europe) {
  const p = r.PROVIDER || "unspecified";
  const l = r.LANGUAGE || "unspecified";
  const f = r.PROMPT_FAMILY || "unspecified";
  const st = r.RANKING_KIND || "NONE";
  europeBy.PROVIDER[p] = (europeBy.PROVIDER[p] || 0) + 1;
  europeBy.LANGUAGE[l] = (europeBy.LANGUAGE[l] || 0) + 1;
  europeBy.PROMPT_FAMILY[f] = (europeBy.PROMPT_FAMILY[f] || 0) + 1;
  europeBy.STRUCTURE[st] = (europeBy.STRUCTURE[st] || 0) + 1;
}

const gtCandidates = audited.filter((r) => r.GROUND_TRUTH_REVIEW_CANDIDATE);
const gtReviewDoc = {
  version: "recommendation_taxonomy_ground_truth_review_v1",
  generatedAt: new Date().toISOString(),
  note:
    "Candidates only. Do NOT auto-amend. Human actions: KEEP_HUMAN_LABEL | AMEND_HUMAN_LABEL | DEFER. Holdout untouched.",
  HOLDOUT_ACCESSED: false,
  CASE_COUNT: gtCandidates.length,
  actionsAllowed: ["KEEP_HUMAN_LABEL", "AMEND_HUMAN_LABEL", "DEFER"],
  cases: gtCandidates.map((r) => ({
    CASE_ID: r.CASE_ID,
    PROVIDER: r.PROVIDER,
    LANGUAGE: r.LANGUAGE,
    GEOGRAPHY: r.GEOGRAPHY,
    PROMPT_FAMILY: r.PROMPT_FAMILY,
    ENTITY: r.ENTITY,
    CURRENT_HUMAN_LABEL: r.HUMAN_LABEL,
    CLASSIFIER_OUTPUT: r.CLASSIFIER_LABEL,
    TAXONOMY_DECISION_PROPOSED: r.PROPOSED_TREE_LABEL,
    REASON: r.ADJUDICATION_REASON,
    EVIDENCE: {
      SECTION_PARENT_HEADING: r.SECTION_PARENT_HEADING,
      LINE: r.LINE,
      LOCAL_ENTITY_CONTEXT: r.LOCAL_ENTITY_CONTEXT,
      RANKING_KIND: r.RANKING_KIND,
      HAS_DIRECT_POSITIVE_CUE: r.HAS_DIRECT_POSITIVE_CUE,
      HAS_CONSIDERATION_SET_CUE: r.HAS_CONSIDERATION_SET_CUE,
      HAS_ONLY_DESCRIPTION: r.HAS_ONLY_DESCRIPTION,
      TRUE_RANKING: r.TRUE_RANKING,
      SECTION_NUMBER_ONLY: r.SECTION_NUMBER_ONLY,
    },
    HUMAN_ACTION: null,
  })),
};

const overlaps = [
  "TAXONOMY_HELP associated_option vs explicit: both can apply to shortlist members; help text does not require direct positive language for ranked/explicit vs associated.",
  "CODE treats shortlist bullet membership as ranked_recommendation; clarified contract prefers associated_option unless order is explicitly meaningful.",
  "CODE may promote leading explicit/ranked entity to first_recommendation in post-pass; clarified contract forbids first-textual-mention alone but allows position-1 in confirmed ranked structure.",
  "RANKED_SHORTLIST_HEADERS includes 'commonly cited choices' / 'brands to consider' which clarified contract treats as consideration-set (associated) unless ordering semantics exist.",
  "explicit precision collapse: shortlistNear+strongRecIntent path and shortlist→ranked/explicit conflation inflate explicit predictions.",
];

const report = {
  version: "ai_intelligence_recommendation_taxonomy_adjudication_hardening_4_v1",
  generatedAt: new Date().toISOString(),
  classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
  HOLDOUT_ACCESSED: false,
  HOLDOUT_CASES_INSPECTED: 0,
  HOLDOUT_METRICS_RUN: false,
  LIVE_PROVIDER_CALLS: 0,
  AUTO_GROUND_TRUTH_CHANGES: 0,
  taxonomy: {
    CURRENT_DOC_DEFINITIONS,
    CURRENT_CODE_OPERATIONAL,
    PROPOSED_MUTUALLY_EXCLUSIVE_DEFINITIONS: PROPOSED_DEFINITIONS,
    CURRENT_PRECEDENCE_ORDER: [...RECOMMENDATION_ROLE_PRECEDENCE],
    QUESTION_STATUS_MAPPING: {
      first_recommendation: "FIRST_RECOMMENDED",
      ranked_recommendation: "RECOMMENDED",
      explicit_recommendation: "RECOMMENDED",
      associated_option: "PRESENT",
      passing_mention: "PRESENT",
      negative_or_qualified: "NEGATIVE_OR_NOT_RECOMMENDED",
      discussed: "DISCUSSION_ONLY",
      comparator: "DISCUSSION_ONLY",
      source_only: "DISCUSSION_ONLY",
      no_mention: "MISSING",
    },
    IDENTIFIED_OVERLAPS: overlaps,
    PROPOSED_DECISION_TREE: [
      "1. entity absent → no_mention",
      "2. materially negative → negative_or_qualified",
      "3. explicit lead / rank position 1 in confirmed ranked structure → first_recommendation",
      "4. ordered non-first with meaningful ranking → ranked_recommendation",
      "5. direct positive entity-linked language → explicit_recommendation",
      "6. viable option / consideration set without direct positive → associated_option",
      "7. principally comparison → comparator",
      "8. substantive neutral discussion → discussed",
      "9. incidental → passing_mention",
      "10. citation/source only → source_only",
    ],
  },
  baselineDevMetrics: {
    RECOMMENDATION_ACCURACY: score.RECOMMENDATION_CLASSIFICATION_ACCURACY,
    RECOMMENDATION_PRECISION: score.RECOMMENDATION_PRECISION,
    RECOMMENDATION_RECALL: score.RECOMMENDATION_RECALL,
    RECOMMENDATION_F1: score.RECOMMENDATION_F1,
    FIRST_REC: score.FIRST_RECOMMENDATION_ACCURACY,
    QUESTION_STATUS: score.QUESTION_STATUS_ACCURACY,
    ENTITY_F1: score.ENTITY_RESOLUTION_F1,
    CASE_COUNT: score.CASE_COUNT,
  },
  adjudication: {
    TOTAL_ERRORS_AUDITED: audited.length,
    ...counts,
  },
  confusionPairs: {
    "ranked->explicit": summarizePair(
      pairBuckets["ranked_recommendation => explicit_recommendation"],
      "ranked_explicit"
    ),
    "discussed->associated": summarizePair(pairBuckets["discussed => associated_option"]),
    "associated->discussed": summarizePair(pairBuckets["associated_option => discussed"]),
    "ranked->discussed": summarizePair(pairBuckets["ranked_recommendation => discussed"]),
    others: summarizePair(pairBuckets.other),
  },
  europe: {
    ERRORS: europe.length,
    BY_PROVIDER: europeBy.PROVIDER,
    BY_LANGUAGE: europeBy.LANGUAGE,
    BY_PROMPT_FAMILY: europeBy.PROMPT_FAMILY,
    BY_STRUCTURE: europeBy.STRUCTURE,
    ROOT_CAUSES: [
      europe.filter((r) => r.SECTION_NUMBER_ONLY || r.RANKING_KIND === "AMBIGUOUS_NUMBER").length
        ? "markdown/section numbering ambiguity"
        : null,
      europe.filter((r) => r.HAS_ONLY_DESCRIPTION).length ? "long narrative / descriptive format" : null,
      europe.filter((r) => r.TAXONOMY_AMBIGUITY).length ? "taxonomy ambiguity (associated/ranked/explicit)" : null,
      europe.filter((r) => r.CLASSIFIER_ERROR && r.STRUCTURAL_PARSE_ERROR).length
        ? "structural parse loss"
        : null,
    ].filter(Boolean),
    CASES: europe.map((r) => ({
      CASE_ID: r.CASE_ID,
      PROVIDER: r.PROVIDER,
      LANGUAGE: r.LANGUAGE,
      PROMPT_FAMILY: r.PROMPT_FAMILY,
      HUMAN: r.HUMAN_LABEL,
      CLASSIFIER: r.CLASSIFIER_LABEL,
      PROPOSED: r.PROPOSED_TREE_LABEL,
      STRUCTURE: r.RANKING_KIND,
      FLAG: r.GROUND_TRUTH_REVIEW_CANDIDATE
        ? "GROUND_TRUTH_REVIEW_CANDIDATE"
        : r.CLASSIFIER_ERROR
          ? "CLASSIFIER_ERROR"
          : "TAXONOMY_AMBIGUITY",
    })),
  },
  groundTruth: {
    REVIEW_REQUIRED: gtCandidates.length > 0,
    CASES: gtCandidates.length,
    PATH: "data/ai-visibility/validation/recommendation-taxonomy-ground-truth-review.json",
  },
  allAuditedErrors: audited,
  nextStep:
    gtCandidates.length > 0
      ? "HUMAN_TAXONOMY_REVIEW_REQUIRED"
      : "PROCEED_TO_CLASSIFIER_HARDENING_4",
  status:
    gtCandidates.length > 0
      ? "AI_INTELLIGENCE_RECOMMENDATION_TAXONOMY_AND_HARDENING_4_REVIEW_REQUIRED"
      : "AI_INTELLIGENCE_RECOMMENDATION_TAXONOMY_ADUDICATION_CLEAN",
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(AUDIT_PATH, JSON.stringify(report, null, 2));
fs.writeFileSync(GT_REVIEW_PATH, JSON.stringify(gtReviewDoc, null, 2));

console.log(
  JSON.stringify(
    {
      AUDIT_PATH,
      GT_REVIEW_PATH,
      TOTAL_ERRORS_AUDITED: audited.length,
      ...counts,
      REVIEW_REQUIRED: gtCandidates.length > 0,
      GT_CASES: gtCandidates.length,
      ranked_explicit: report.confusionPairs["ranked->explicit"],
      discussed_associated: {
        TOTAL: report.confusionPairs["discussed->associated"].TOTAL,
        GT_REVIEW: report.confusionPairs["discussed->associated"].GT_REVIEW,
        CLASSIFIER_ERROR: report.confusionPairs["discussed->associated"].CLASSIFIER_ERROR,
      },
      associated_discussed: {
        TOTAL: report.confusionPairs["associated->discussed"].TOTAL,
        GT_REVIEW: report.confusionPairs["associated->discussed"].GT_REVIEW,
        CLASSIFIER_ERROR: report.confusionPairs["associated->discussed"].CLASSIFIER_ERROR,
      },
      ranked_discussed: {
        TOTAL: report.confusionPairs["ranked->discussed"].TOTAL,
        GT_REVIEW: report.confusionPairs["ranked->discussed"].GT_REVIEW,
        CLASSIFIER_ERROR: report.confusionPairs["ranked->discussed"].CLASSIFIER_ERROR,
      },
      EUROPE_ERRORS: europe.length,
      EUROPE_ROOT_CAUSES: report.europe.ROOT_CAUSES,
      NEXT: report.nextStep,
      STATUS: report.status,
      HOLDOUT_ACCESSED: false,
    },
    null,
    2
  )
);
