import { loadGoldenSet, scoreGoldenSetHydrated } from "../lib/ai-visibility/validation/golden-set.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import {
  extractEntityLocalEvidence as ex,
  buildTypedSections as bt,
  classifySectionType,
} from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import { decideRecommendationRoleFromEvidence as dec } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";

function role(text, name) {
  const start = text.indexOf(name);
  const typed = bt(text);
  const ev = ex({
    text,
    start,
    end: start + name.length,
    rawMention: name,
    typedSections: typed,
    canonicalEntityName: name,
  });
  return {
    role: dec(ev, { entityPresent: true }).role,
    sec: ev.sectionType,
    cons: ev.recommendationEvidence.considerationSetCue,
    rank: ev.rankPosition,
    conf: ev.confirmedRankStructure,
    pos: ev.recommendationEvidence.directPositiveCue || ev.recommendationEvidence.sectionPositiveCue,
    lead: ev.recommendationEvidence.leadCue,
  };
}

const cases = [
  ["consider", "## Brands to consider\n\n- Autograph Collection\n- Curio\n", "Autograph Collection"],
  ["neutral", "Brand profiles:\n\n- Autograph Collection\n- Curio\n", "Autograph Collection"],
  ["rank1", "Soft-brand shortlist:\n1. Autograph Collection\n2. Tribute Portfolio\n3. Ascend Hotel Collection\n", "Autograph Collection"],
  ["rank3", "Soft-brand shortlist:\n1. Autograph Collection\n2. Tribute Portfolio\n3. Ascend Hotel Collection\n", "Ascend Hotel Collection"],
  ["neutralOrdered", "1. Autograph Collection\n2. Curio\n", "Autograph Collection"],
  ["recSet", "## Recommended brands:\n\n- Autograph Collection\n", "Autograph Collection"],
  ["esCons", "Marcas a considerar:\n- Autograph Collection\n", "Autograph Collection"],
  ["esRank", "Orden de prioridad:\n1. Autograph Collection\n2. Curio Collection\n", "Curio Collection"],
  ["highgate", "Other alternatives include local operators; Highgate remains focused on urban.", "Highgate"],
  ["kimpton", "Kimpton Hotels is often associated with mixed-use lifestyle developments.", "Kimpton Hotels"],
  ["headingReset", "## Brands to consider\n- Autograph Collection\n\n## Brand profiles\n- Curio Collection\n", "Curio Collection"],
];

for (const [label, text, name] of cases) {
  console.log(label, role(text, name), "typeTitle", classifySectionType(text.split("\n")[0], ""));
}

const s = await scoreGoldenSetHydrated(loadGoldenSet(), { holdoutPolicy: "exclude" });
console.log({
  acc: s.RECOMMENDATION_CLASSIFICATION_ACCURACY,
  first: s.FIRST_RECOMMENDATION_ACCURACY,
  qs: s.QUESTION_STATUS_ACCURACY,
});

const ROLES = [
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "negative_or_qualified",
  "source_only",
];
const roleRank = new Map(ROLES.map((r, i) => [r, i]));
const g = loadGoldenSet();
const index = buildGoldenSetScoringEntityIndex({});
const { cases: devCases } = await hydrateGoldenSetCasesForScoring(
  g.cases.filter((c) => c.holdoutSplit !== "holdout"),
  {}
);
const byExp = {};
const byPair = {};
for (const c of devCases) {
  if (!c.expectedRecommendationRole) continue;
  const mentions = extractMentions({
    responseId: "x",
    text: c.text || "",
    entityIndex: index.aliasIndex,
  });
  const hits = mentions.filter((m) => m.canonicalEntityName === c.entityName);
  const got = hits.length
    ? hits
        .slice()
        .sort(
          (a, b) =>
            (roleRank.get(a.role) ?? 99) - (roleRank.get(b.role) ?? 99) ||
            a.mentionPosition - b.mentionPosition
        )[0].role
    : null;
  const exp = c.expectedRecommendationRole;
  if (!byExp[exp]) byExp[exp] = { n: 0, ok: 0 };
  byExp[exp].n++;
  if (exp === got) byExp[exp].ok++;
  else byPair[`${exp} => ${got}`] = (byPair[`${exp} => ${got}`] || 0) + 1;
}
console.log(
  "recall",
  Object.fromEntries(Object.entries(byExp).map(([k, v]) => [k, `${(v.ok / v.n).toFixed(2)} ${v.ok}/${v.n}`]))
);
console.log("top pairs", Object.entries(byPair).sort((a, b) => b[1] - a[1]).slice(0, 10));
