/**
 * Governed recommendation taxonomy for hybrid adjudication.
 * No freeform labels.
 */

export const GOVERNED_RECOMMENDATION_ROLES = Object.freeze([
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "negative_or_qualified",
  "source_only",
  "no_mention",
]);

export const TAXONOMY_DEFINITIONS = Object.freeze({
  first_recommendation:
    "Entity is the lead / #1 / first choice / primary recommendation. Requires separate lead or rank-1 evidence. Strong positive alone is not enough.",
  ranked_recommendation:
    "Entity has a meaningful non-first rank (e.g. #2, second choice) inside an ordered preference structure. Bare numbered formatting is not ranking.",
  explicit_recommendation:
    "Entity is directly recommended or positively evaluated (strong option/candidate/fit) without being established as first or ranked.",
  associated_option:
    "Entity is a member of a consideration / options / alternatives set without lead, meaningful rank, or entity-local direct positive override.",
  comparator:
    "Entity appears as the object of comparison (alternative to X, versus X) rather than as a recommended subject.",
  discussed:
    "Entity is substantively discussed in a neutral/descriptive way without decision-set membership or recommendation cues.",
  passing_mention:
    "Incidental / example-list mention without substantive treatment.",
  negative_or_qualified:
    "Entity is discouraged, excluded, or materially negatively/qualified positioned.",
  source_only:
    "Entity appears only as a citation/source attribution.",
  no_mention: "Entity is not present in the response.",
});

export const TAXONOMY_DECISION_RULES = Object.freeze([
  "RULE1: Positive language alone (strong candidate/option/fit) is NOT first_recommendation.",
  "RULE2: Consideration-list numbering alone is NOT ranking or first.",
  "RULE3: Ranking requires meaningful ordered preference/priority semantics.",
  "RULE4: Explicit lead/rank-1 evidence overrides consideration-set membership → first_recommendation; explicit non-first rank → ranked_recommendation.",
  "RULE5: Direct entity-linked positive in a consideration set → explicit_recommendation (unless lead/rank evidence supports higher).",
  "Neutral discussion / brand profiles / pipeline / partnership announcements are not associated_option.",
  "Comparator language wins when the entity is the comparison object.",
]);
