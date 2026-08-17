/**
 * Hardening 6 — audit v3.3-correct / v4-wrong + FN clusters (DEV only).
 * Holdout untouched.
 */
import fs from "node:fs";
import path from "node:path";
import { loadGoldenSet } from "../lib/ai-visibility/validation/golden-set.js";
import { buildGoldenSetScoringEntityIndex } from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { hydrateGoldenSetCasesForScoring } from "../lib/ai-visibility/validation/hydrate-golden-set-texts.js";
import { extractMentions } from "../lib/ai-visibility/extract-mentions.js";
import {
  classifyMentionRoleV3,
  detectResponseSections,
} from "../lib/ai-visibility/recommendation-classifier-v3.js";
import {
  extractEntityLocalEvidence as extractV4,
  aggregateEntityEvidence as aggregateV4,
} from "../lib/ai-visibility/recommendation-evidence-v4.js";
import { decideRecommendationRoleFromEvidence as decideV4 } from "../lib/ai-visibility/recommendation-classifier-v4.js";
import {
  extractEntityLocalEvidence as extractV41,
  aggregateEntityEvidence as aggregateV41,
  buildTypedSections,
} from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import { decideRecommendationRoleFromEvidence as decideV41 } from "../lib/ai-visibility/recommendation-classifier-v4_1.js";

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

function bestOf(roles) {
  return roles
    .filter(Boolean)
    .slice()
    .sort((a, b) => (roleRank.get(a) ?? 99) - (roleRank.get(b) ?? 99))[0] || null;
}

function classifyMissingEvidence(human, v4Role, evidence, text, entity) {
  const ev = evidence?.recommendationEvidence || {};
  const st = evidence?.structure || {};
  const secType = evidence?.sectionType || "";
  const t = String(text || "").toLowerCase();
  if (/primera|segunda|tercera|marcas a considerar|entre las opciones|orden de prioridad|opciones recomendadas|principales opciones|alternativas/.test(t)) {
    if (human === "associated_option" || human === "first_recommendation" || human === "ranked_recommendation") {
      return "SPANISH_CONTEXT";
    }
  }
  if (human === "first_recommendation") {
    if (!ev.leadCue && !(st.confirmedRankStructure && st.orderedPosition === 1)) return "LEAD_CONTEXT";
  }
  if (human === "ranked_recommendation") return "CONFIRMED_RANK_CONTEXT";
  if (human === "associated_option") {
    if (secType === "CONSIDERATION_SET_SECTION" || /consider|options include|alternatives include/.test(t)) {
      return "SECTION_CONSIDERATION_CONTEXT";
    }
    return "LIST_MEMBERSHIP_CONTEXT";
  }
  if (human === "explicit_recommendation") {
    if (/recommend/.test(t)) return "SECTION_RECOMMENDATION_CONTEXT";
    return "LOCAL_POSITIVE_CONTEXT";
  }
  if (/\|/.test(text || "")) return "TABLE_CONTEXT";
  return "OTHER";
}

function entitySpans(mentions, entityName) {
  return mentions.filter((m) => m.canonicalEntityName === entityName);
}

async function main() {
  const g = loadGoldenSet();
  const index = buildGoldenSetScoringEntityIndex({});
  const { cases } = await hydrateGoldenSetCasesForScoring(
    g.cases.filter((c) => c.holdoutSplit !== "holdout"),
    {}
  );

  const v33okV4wrong = [];
  const clusters = {
    first: [],
    ranked: [],
    associated: [],
    explicit: [],
  };

  for (const c of cases) {
    if (!c.expectedRecommendationRole) continue;
    const text = c.text || "";
    const mentions = extractMentions({
      responseId: "audit",
      text,
      entityIndex: index.aliasIndex,
    });
    const spans = entitySpans(mentions, c.entityName);

    const v33Roles = spans.map((m) => {
      const r = classifyMentionRoleV3({ ...m, text });
      return typeof r === "string" ? r : r?.role;
    });
    const v33 = bestOf(v33Roles);

    const typed = buildTypedSections(text);
    const ev4list = spans.map((m) =>
      extractV4({
        text,
        start: m.start ?? m.mentionPosition,
        end: m.end ?? (m.mentionPosition || 0) + String(m.rawMention || "").length,
        rawMention: m.rawMention,
        canonicalEntityId: m.canonicalEntityId,
        canonicalEntityName: m.canonicalEntityName,
      })
    );
    const agg4 = aggregateV4(ev4list);
    const v4 = spans.length ? decideV4(agg4, { entityPresent: true }).role : null;

    const ev41list = spans.map((m) =>
      extractV41({
        text,
        start: m.start ?? m.mentionPosition,
        end: m.end ?? (m.mentionPosition || 0) + String(m.rawMention || "").length,
        rawMention: m.rawMention,
        canonicalEntityId: m.canonicalEntityId,
        canonicalEntityName: m.canonicalEntityName,
        typedSections: typed,
      })
    );
    const agg41 = aggregateV41(ev41list);
    const v41 = spans.length ? decideV41(agg41, { entityPresent: true }).role : null;

    const exp = c.expectedRecommendationRole;
    if (v33 === exp && v4 !== exp) {
      const miss = classifyMissingEvidence(exp, v4, agg41, text, c.entityName);
      const ctxStart = Math.max(0, (spans[0]?.mentionPosition || 0) - 120);
      const ctxEnd = Math.min(text.length, (spans[0]?.mentionPosition || 0) + 200);
      v33okV4wrong.push({
        CASE_ID: c.caseId,
        ENTITY: c.entityName,
        HUMAN_ROLE: exp,
        V3_3_ROLE: v33,
        V4_ROLE: v4,
        V4_1_ROLE: v41,
        MISSING_EVIDENCE: miss,
        FULL_RELEVANT_CONTEXT: text.slice(ctxStart, ctxEnd),
        V4_EVIDENCE_OBJECT: {
          sectionType: agg41?.sectionType,
          recommendationEvidence: agg41?.recommendationEvidence,
          structure: agg41?.structure,
          confirmedDecisionSet: agg41?.confirmedDecisionSet,
          propagation: spans[0]
            ? {
                sectionId: ev41list[0]?.sectionId,
                propagationSource: ev41list[0]?.propagationSource,
                sectionPropagationAllowed: ev41list[0]?.sectionPropagationAllowed,
              }
            : null,
        },
        sections: typed.map((s) => ({
          id: s.sectionId,
          type: s.sectionType,
          title: s.title,
        })),
      });
    }

    if (exp === "first_recommendation" && v41 !== exp) {
      clusters.first.push({
        CASE_ID: c.caseId,
        ENTITY: c.entityName,
        V4_1: v41,
        V3_3: v33,
        sectionType: agg41?.sectionType,
        leadCue: agg41?.recommendationEvidence?.leadCue,
        rank: agg41?.structure,
        snippet: text.slice(0, 450),
      });
    }
    if (exp === "ranked_recommendation" && v41 !== exp) {
      clusters.ranked.push({
        CASE_ID: c.caseId,
        ENTITY: c.entityName,
        V4_1: v41,
        sectionType: agg41?.sectionType,
        snippet: text.slice(0, 400),
      });
    }
    if (exp === "associated_option" && v41 !== exp) {
      clusters.associated.push({
        CASE_ID: c.caseId,
        ENTITY: c.entityName,
        V4_1: v41,
        sectionType: agg41?.sectionType,
        consideration: agg41?.recommendationEvidence?.considerationSetCue,
        snippet: text.slice(0, 400),
      });
    }
    if (exp === "explicit_recommendation" && v41 !== exp) {
      clusters.explicit.push({
        CASE_ID: c.caseId,
        ENTITY: c.entityName,
        V4_1: v41,
        snippet: text.slice(0, 400),
      });
    }
  }

  const byMiss = {};
  for (const row of v33okV4wrong) {
    byMiss[row.MISSING_EVIDENCE] = (byMiss[row.MISSING_EVIDENCE] || 0) + 1;
  }

  const out = {
    TOTAL_V4_NEW_ERRORS: v33okV4wrong.length,
    BY_MISSING_EVIDENCE_TYPE: byMiss,
    samples: v33okV4wrong.slice(0, 50),
    clusters: {
      first: { COUNT: clusters.first.length, CASE_IDS: clusters.first.map((x) => x.CASE_ID), samples: clusters.first.slice(0, 15) },
      ranked: { COUNT: clusters.ranked.length, CASE_IDS: clusters.ranked.map((x) => x.CASE_ID), samples: clusters.ranked },
      associated: {
        COUNT: clusters.associated.length,
        CASE_IDS: clusters.associated.map((x) => x.CASE_ID),
        samples: clusters.associated.slice(0, 15),
      },
      explicit: { COUNT: clusters.explicit.length, CASE_IDS: clusters.explicit.map((x) => x.CASE_ID), samples: clusters.explicit },
    },
  };

  const outPath = path.join(
    "data/ai-visibility/validation",
    "hardening-6-miss-audit.json"
  );
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log(
    JSON.stringify(
      {
        TOTAL_V4_NEW_ERRORS: out.TOTAL_V4_NEW_ERRORS,
        BY_MISSING_EVIDENCE_TYPE: out.BY_MISSING_EVIDENCE_TYPE,
        clusterCounts: {
          first: clusters.first.length,
          ranked: clusters.ranked.length,
          associated: clusters.associated.length,
          explicit: clusters.explicit.length,
        },
        outPath,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
