/**
 * Classifier lab candidate registry.
 * Patches normalize CRLF so Windows checkouts match.
 * No case-specific / provider / geography rules.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const EVIDENCE = path.join(ROOT, "lib/ai-visibility/recommendation-evidence-v4_1.js");
const CLASSIFIER = path.join(ROOT, "lib/ai-visibility/recommendation-classifier-v4_1.js");

function readRaw(p) {
  return fs.readFileSync(p, "utf8");
}
function writeRaw(p, s) {
  fs.writeFileSync(p, s);
}

/** Snapshot files for revert */
export function snapshotLabTargets() {
  return {
    evidence: readRaw(EVIDENCE),
    classifier: readRaw(CLASSIFIER),
  };
}

export function restoreLabTargets(snap) {
  writeRaw(EVIDENCE, snap.evidence);
  writeRaw(CLASSIFIER, snap.classifier);
}

function replaceOnce(filePath, oldStr, newStr, label) {
  const raw = readRaw(filePath);
  const nl = raw.includes("\r\n") ? "\r\n" : "\n";
  const normOld = oldStr.replace(/\r\n/g, "\n").replace(/\n/g, nl);
  const normNew = newStr.replace(/\r\n/g, "\n").replace(/\n/g, nl);
  if (!raw.includes(normOld)) {
    // try LF-only match on normalized view
    const lf = raw.replace(/\r\n/g, "\n");
    const oldLf = oldStr.replace(/\r\n/g, "\n");
    const newLf = newStr.replace(/\r\n/g, "\n");
    if (!lf.includes(oldLf)) throw new Error(`patch_miss:${label}`);
    const nextLf = lf.replace(oldLf, newLf);
    writeRaw(filePath, nl === "\r\n" ? nextLf.replace(/\n/g, "\r\n") : nextLf);
    return;
  }
  const next = raw.replace(normOld, normNew);
  if (next === raw) throw new Error(`patch_noop:${label}`);
  writeRaw(filePath, next);
}

export const CANDIDATES = [
  {
    id: "lab_c01_tighten_consideration_lookbehind",
    hypothesis:
      "Reduce discussed→associated by requiring consideration lookbehind ≤80 chars and narrowing after-line association phrasing",
    errorCluster: "FALSE_ASSOCIATED_OVER_PROMOTION",
    apply() {
      replaceOnce(
        EVIDENCE,
        "const lookBehind140 = text.slice(Math.max(0, start - 140), start);",
        "const lookBehind140 = text.slice(Math.max(0, start - 80), start);",
        "c01-lookbehind"
      );
      replaceOnce(
        EVIDENCE,
        "? findCueInRange(text, CONSIDERATION_SET_RE, Math.max(0, start - 140), start)",
        "? findCueInRange(text, CONSIDERATION_SET_RE, Math.max(0, start - 80), start)",
        "c01-lookbehind-find"
      );
      replaceOnce(
        EVIDENCE,
        `if (!considerationSetCue && CONSIDERATION_SET_RE.test(afterLine)) {
    considerationSetCue = true;
  }`,
        `if (!considerationSetCue && /\\b(?:often|commonly|typically|frequently)\\s+(?:associated|considered)\\b/i.test(afterLine)) {
    considerationSetCue = true;
  }`,
        "c01-afterline"
      );
    },
  },
  {
    id: "lab_c02_block_profile_bullet_association",
    hypothesis:
      "Profile bullets under consideration (- **Brand** – long description) stay discussed unless local association cue",
    errorCluster: "FALSE_ASSOCIATED_FROM_NEUTRAL_OR_PROFILE_CATALOG",
    apply() {
      replaceOnce(
        EVIDENCE,
        `const isListOrTableChild = Boolean(bullet || tableRank != null || (rawRank != null && !topicNumber) || /^[-*•]\\s/.test(line.trim()) || /^\\d+[.)]\\s/.test(line.trim()));`,
        `const isListOrTableChild = Boolean(bullet || tableRank != null || (rawRank != null && !topicNumber) || /^[-*•]\\s/.test(line.trim()) || /^\\d+[.)]\\s/.test(line.trim()));
  const afterMentionOnLine = line.slice(Math.min(line.length, Math.max(0, end - lineStart)));
  const isProfileBullet =
    isListOrTableChild &&
    /[*_\\s]*[–—:]\\s+\\S/.test(afterMentionOnLine) &&
    line.replace(/\\*\\*/g, "").trim().length > 72;`,
        "c02-profile-def"
      );
      replaceOnce(
        EVIDENCE,
        `if (gate.PROPAGATION_ALLOWED && isListOrTableChild) {
        considerationSetCue = true;
        sectionPropagationAllowed = true;
        propagationSource = "section_consideration";`,
        `if (gate.PROPAGATION_ALLOWED && isListOrTableChild && !isProfileBullet) {
        considerationSetCue = true;
        sectionPropagationAllowed = true;
        propagationSource = "section_consideration";`,
        "c02-gate"
      );
      replaceOnce(
        EVIDENCE,
        `if (
    section?.sectionType === "CONSIDERATION_SET_SECTION" &&
    isListOrTableChild &&
    !RANK_SEMANTICS_RE.test(\`\${section.title}\\n\${section.sectionIntro}\`)
  ) {
    considerationSetCue = true;`,
        `if (
    section?.sectionType === "CONSIDERATION_SET_SECTION" &&
    isListOrTableChild &&
    !isProfileBullet &&
    !RANK_SEMANTICS_RE.test(\`\${section.title}\\n\${section.sectionIntro}\`)
  ) {
    considerationSetCue = true;`,
        "c02-forced"
      );
    },
  },
  {
    id: "lab_c03_expand_lead_cues",
    hypothesis:
      "Recover first from entity-local lead language: strongest option / best starting point / top recommendation",
    errorCluster: "MISSING_FIRST_FROM_LEAD_CUE",
    apply() {
      replaceOnce(
        EVIDENCE,
        "top\\s+choice|primary\\s+recommendation",
        "top\\s+choice|top\\s+recommendation|primary\\s+recommendation",
        "c03-lead-a"
      );
      replaceOnce(
        EVIDENCE,
        "recomendaci[oó]n\\s+principal)\\b/i;",
        "recomendaci[oó]n\\s+principal|strongest\\s+(?:option|choice|candidate)|best\\s+starting\\s+point|mejor\\s+punto\\s+de\\s+partida|clear\\s+first\\s+choice)\\b/i;",
        "c03-lead-b"
      );
    },
  },
  {
    id: "lab_c04_rank_semantics_shortlist_variants",
    hypothesis:
      "Expand confirmed rank semantics: in priority order / lista priorizada / short list: / recommended order",
    errorCluster: "MISSING_FIRST_FROM_CONFIRMED_RANK_OR_SHORTLIST",
    apply() {
      replaceOnce(
        EVIDENCE,
        "recommended\\s+in\\s+order|orden\\s+de\\s+prioridad",
        "recommended\\s+(?:in\\s+)?order|in\\s+priority\\s+order|orden\\s+de\\s+prioridad|lista\\s+priorizada",
        "c04-rank-a"
      );
      replaceOnce(
        EVIDENCE,
        "priority\\s+\\d+|shortlist:)\\b/i;",
        "priority\\s+\\d+|short\\s*list:|shortlist:)\\b/i;",
        "c04-rank-b"
      );
    },
  },
  {
    id: "lab_c05_same_sentence_catalog_associated",
    hypothesis:
      "Improve associated recall: same-sentence consideration cue distance 280→360",
    errorCluster: "MISSING_ASSOCIATED_FROM_DECISION_SET_MEMBERSHIP",
    apply() {
      replaceOnce(
        EVIDENCE,
        "if (sentCons2 && start - sentCons2.cueEnd <= 280) {",
        "if (sentCons2 && start - sentCons2.cueEnd <= 360) {",
        "c05-distance"
      );
    },
  },
  {
    id: "lab_c06_neutral_soft_brand_blocks_assoc",
    hypothesis:
      "Expand NEUTRAL_CATALOG titles for soft-brand/collection affiliation overviews",
    errorCluster: "FALSE_ASSOCIATED_FROM_NEUTRAL_OR_PROFILE_CATALOG",
    apply() {
      replaceOnce(
        EVIDENCE,
        "soft\\s+brand(?:s|\\s+collections?)|major\\s+(?:soft\\s+)?brand",
        "soft\\s+brand(?:s|\\s+collections?)|soft\\s+brand\\s+(?:&|and)\\s+collection|collection\\s+hotel\\s+affiliations?|affiliations?\\s+for\\s+independ|major\\s+(?:soft\\s+)?brand",
        "c06-neutral"
      );
    },
  },
  {
    id: "lab_c07_explicit_after_mention_160",
    hypothesis:
      "Recover explicit FN via after-mention same-line window end+120",
    errorCluster: "MISSING_EXPLICIT_POSITIVE_OR_SECTION",
    apply() {
      replaceOnce(
        EVIDENCE,
        `for (const chunk of [afterLine, beforeShort.length <= 120 ? beforeShort : ""]) {
    if (!chunk) continue;
    if (DIRECT_POSITIVE_RE.test(sanitizeCueText(chunk, rawMention))) {
      if (comparatorCue) break;
      directPositiveCue = true;
      break;
    }
  }`,
        `const afterMentionSameLine = text.slice(end, Math.min(text.length, lineEnd, end + 120));
  for (const chunk of [afterLine, afterMentionSameLine, beforeShort.length <= 120 ? beforeShort : ""]) {
    if (!chunk) continue;
    if (DIRECT_POSITIVE_RE.test(sanitizeCueText(chunk, rawMention))) {
      if (comparatorCue) break;
      directPositiveCue = true;
      break;
    }
  }`,
        "c07-positive"
      );
    },
  },
  {
    id: "lab_c08_decision_tree_confirmed_decision_set",
    hypothesis:
      "confirmedDecisionSet on CONSIDERATION_SET_SECTION → associated when cue flag missed",
    errorCluster: "MISSING_ASSOCIATED_RECALL",
    apply() {
      replaceOnce(
        CLASSIFIER,
        `if (ev.considerationSetCue || evidence.confirmedDecisionSet) {
    // confirmedDecisionSet alone from recommendation_set already handled as explicit above
    if (ev.considerationSetCue) {
      return {
        role: "associated_option",
        explicitRecommendation: false,
        recommendationPosition: null,
        reason: "consideration_set_evidence",
        classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
        evidence,
      };
    }
  }`,
        `if (ev.considerationSetCue) {
    return {
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "consideration_set_evidence",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }
  if (
    evidence.confirmedDecisionSet &&
    (evidence.sectionType === "CONSIDERATION_SET_SECTION" ||
      evidence.structure?.headingSemanticType === "CONSIDERATION_SET_SECTION")
  ) {
    return {
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "confirmed_consideration_section_membership",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }`,
        "c08-tree"
      );
    },
  },
  {
    id: "lab_c09_strip_often_associated_from_consideration_re",
    hypothesis:
      "Remove broad commonly/often/typically/frequently associated|considered from CONSIDERATION_SET_RE",
    errorCluster: "FALSE_ASSOCIATED_OVER_PROMOTION",
    apply() {
      replaceOnce(
        EVIDENCE,
        "brands?\\s+commonly\\s+(?:considered|cited|associated)|commonly\\s+(?:associated|considered|cited)(?:\\s+with)?|frequently\\s+(?:associated|considered)(?:\\s+with)?|typically\\s+(?:associated|considered)(?:\\s+with)?|often\\s+(?:associated|considered)(?:\\s+with)?|",
        "brands?\\s+commonly\\s+(?:considered|cited)|",
        "c09-cons-re"
      );
    },
  },
  {
    id: "lab_c10_inherit_only_short_list_items",
    hypothesis:
      "Consideration section inheritance only for short list lines (≤64 chars)",
    errorCluster: "FALSE_ASSOCIATED_FROM_BROAD_CONSIDERATION_PROPAGATION",
    apply() {
      replaceOnce(
        EVIDENCE,
        `const isListOrTableChild = Boolean(bullet || tableRank != null || (rawRank != null && !topicNumber) || /^[-*•]\\s/.test(line.trim()) || /^\\d+[.)]\\s/.test(line.trim()));`,
        `const isListOrTableChild = Boolean(bullet || tableRank != null || (rawRank != null && !topicNumber) || /^[-*•]\\s/.test(line.trim()) || /^\\d+[.)]\\s/.test(line.trim()));
  const isShortListMember =
    isListOrTableChild &&
    line.replace(/\\*\\*/g, "").replace(/^\\s*(?:[-*•]|\\d+[.)])\\s*/, "").trim().length <= 64;`,
        "c10-short-def"
      );
      replaceOnce(
        EVIDENCE,
        `if (gate.PROPAGATION_ALLOWED && isListOrTableChild) {
        considerationSetCue = true;
        sectionPropagationAllowed = true;
        propagationSource = "section_consideration";`,
        `if (gate.PROPAGATION_ALLOWED && isShortListMember) {
        considerationSetCue = true;
        sectionPropagationAllowed = true;
        propagationSource = "section_consideration";`,
        "c10-gate"
      );
    },
  },
  {
    id: "lab_c11_disable_untitled_consideration_prefatory",
    hypothesis:
      "Disable prefatory untitled retarget to CONSIDERATION (reduces whole-doc association bleed)",
    errorCluster: "FALSE_ASSOCIATED_FROM_BROAD_CONSIDERATION_PROPAGATION",
    apply() {
      replaceOnce(
        EVIDENCE,
        "openType === \"CONSIDERATION_SET_SECTION\" ||",
        "false && openType === \"CONSIDERATION_SET_SECTION\" ||",
        "c11-prefatory"
      );
    },
  },
  {
    id: "lab_c12_require_catalog_shape_for_behind_cons",
    hypothesis:
      "Lookbehind consideration only if between cue and entity has catalog separators (comma/and)",
    errorCluster: "FALSE_ASSOCIATED_OVER_PROMOTION",
    apply() {
      replaceOnce(
        EVIDENCE,
        `if (localCons) {
    // Entity after semicolon in a different clause without its own cue → do not link
    const fromCue = text.slice(localCons.cueStart, start);
    if (/;/.test(fromCue) && !CONSIDERATION_SET_RE.test(fromCue.slice(fromCue.lastIndexOf(";")))) {
      // skip
    } else {
      considerationSetCue = true;
    }
  }`,
        `if (localCons) {
    // Entity after semicolon in a different clause without its own cue → do not link
    const fromCue = text.slice(localCons.cueStart, start);
    if (/;/.test(fromCue) && !CONSIDERATION_SET_RE.test(fromCue.slice(fromCue.lastIndexOf(";")))) {
      // skip
    } else if (
      fromCue.length <= 48 ||
      /,/.test(fromCue) ||
      /\\b(?:and|y|e|include|incluyen)\\b/i.test(fromCue) ||
      /\\n\\s*[-*•\\d]/.test(fromCue)
    ) {
      considerationSetCue = true;
    }
  }`,
        "c12-behind-shape"
      );
    },
  },
  {
    id: "lab_c13_stop_unknown_subsection_inheritance",
    hypothesis:
      "Do not inherit CONSIDERATION into UNKNOWN ### parent-company subsections (profile catalogs stay discussed)",
    errorCluster: "FALSE_ASSOCIATED_FROM_NEUTRAL_OR_PROFILE_CATALOG",
    apply() {
      replaceOnce(
        EVIDENCE,
        `if (activeDecisionType && sec.sectionType === "UNKNOWN_SECTION") {
      sec.sectionType = activeDecisionType;
      sec.catalogSemantics = classifyCatalogSemantics(activeDecisionType);
      sec.confirmedDecisionSet = true;
      sec.confirmedRankStructure = false;
      sec.sectionCueType =
        activeDecisionType === "CONSIDERATION_SET_SECTION" ? "consideration" : "recommendation_set";
    }`,
        `if (activeDecisionType && sec.sectionType === "UNKNOWN_SECTION") {
      // Numbered parent-company / profile headings are catalogs, not decision-set children
      const titled = String(sec.title || "");
      if (/^#{1,3}\\s*\\d+[.)]/.test(titled) || /\\b(international|marriott|hilton|ihg|hyatt|accor|grupo)\\b/i.test(titled)) {
        // keep UNKNOWN — no inheritance
      } else {
        sec.sectionType = activeDecisionType;
        sec.catalogSemantics = classifyCatalogSemantics(activeDecisionType);
        sec.confirmedDecisionSet = true;
        sec.confirmedRankStructure = false;
        sec.sectionCueType =
          activeDecisionType === "CONSIDERATION_SET_SECTION" ? "consideration" : "recommendation_set";
      }
    }`,
        "c13-inherit"
      );
    },
  },
  {
    id: "lab_c14_comma_catalog_without_section",
    hypothesis:
      "Names that appear most often / most commonly considered + comma list → associated even without section type",
    errorCluster: "MISSING_ASSOCIATED_RECALL",
    apply() {
      replaceOnce(
        EVIDENCE,
        `const catalogShape = /,/.test(between) || /\\b(?:and|y|e)\\b/i.test(between) || /\\n\\s*[-*•]/.test(between);`,
        `const catalogShape =
        /,/.test(between) ||
        /\\b(?:and|y|e|o)\\b/i.test(between) ||
        /\\n\\s*[-*•]/.test(between) ||
        /\\*\\*[^*]+\\*\\*/.test(between);`,
        "c14-catalog-shape"
      );
    },
  },
  {
    id: "lab_c15_ranked_nearby_shortlist_header",
    hypothesis:
      "If lookbehind ≤240 has 'shortlist'/'prioriz'/'top options' and entity has list rank marker, confirm rank structure",
    errorCluster: "MISSING_RANKED_FROM_CONFIRMED_STRUCTURE",
    apply() {
      replaceOnce(
        EVIDENCE,
        `// Local numbered list under a nearby ranked intro line (same untitled block / ≤200 behind)
  if (!confirmedRankStructure && rawRank != null && !topicNumber && isListOrTableChild) {
    const behind200 = text.slice(Math.max(0, start - 200), start);
    if (RANK_SEMANTICS_RE.test(behind200)) {
      confirmedRankStructure = true;
      rankCue = true;
      rankPosition = rawRank;
      propagationSource = propagationSource || "local_ranked_intro";
      sectionPropagationAllowed = true;
      propagationDistance = 200;
    }
  }`,
        `// Local numbered list under a nearby ranked intro line (≤240 behind)
  if (!confirmedRankStructure && rawRank != null && !topicNumber && isListOrTableChild) {
    const behind240 = text.slice(Math.max(0, start - 240), start);
    if (
      RANK_SEMANTICS_RE.test(behind240) ||
      /\\b(short\\s*list|shortlist|priority\\s+order|orden\\s+de\\s+prioridad|top\\s+options?)\\b/i.test(behind240)
    ) {
      confirmedRankStructure = true;
      rankCue = true;
      rankPosition = rawRank;
      propagationSource = propagationSource || "local_ranked_intro";
      sectionPropagationAllowed = true;
      propagationDistance = 240;
    }
  }`,
        "c15-rank-nearby"
      );
    },
  },
  {
    id: "lab_c16_lead_from_rfp_multi_entity_first_only",
    hypothesis:
      "Issue an RFP to A and B: first entity after cue stays lead; co-mentions already get positive — ensure leadCue clears for non-adjacent",
    errorCluster: "MISSING_FIRST_RECALL",
    apply() {
      replaceOnce(
        EVIDENCE,
        `if (!leadCue) {
    const beforeLocal = text.slice(Math.max(0, start - 48), start);
    if (
      /(?:issue\\s+an\\s+rfp\\s+to|solicit\\s+proposals?\\s+from)\\s+(?:\\*\\*)?$/i.test(beforeLocal)
    ) {
      leadCue = true;
    }
  }`,
        `if (!leadCue) {
    const beforeLocal = text.slice(Math.max(0, start - 48), start);
    if (
      /(?:issue\\s+an\\s+rfp\\s+to|solicit\\s+proposals?\\s+from)\\s+(?:\\*\\*)?$/i.test(beforeLocal)
    ) {
      leadCue = true;
    }
  }
  // Soft lead: "My top pick is ENTITY" / "I would start with ENTITY"
  if (!leadCue) {
    const before90 = text.slice(Math.max(0, start - 90), start);
    if (/\\b(?:top\\s+pick\\s+is|would\\s+start\\s+with|begin\\s+with|start\\s+with)\\s+(?:\\*\\*)?$/i.test(before90)) {
      leadCue = true;
    }
  }`,
        "c16-lead-soft"
      );
    },
  },
  {
    id: "lab_c17_spanish_habitualmente_consideradas",
    hypothesis:
      "Spanish consideration membership: habitualmente/frecuentemente/usualmente consideradas + suelen ser consideradas",
    errorCluster: "MISSING_ASSOCIATED_FROM_DECISION_SET_MEMBERSHIP",
    apply() {
      replaceOnce(
        EVIDENCE,
        "se\\s+consideran|suelen\\s+considerar|",
        "se\\s+consideran|suelen\\s+considerar|suelen\\s+ser\\s+considerad[oa]s?|habitualmente\\s+considerad[oa]s?|frecuentemente\\s+considerad[oa]s?|usualmente\\s+considerad[oa]s?|",
        "c17-es-cons"
      );
    },
  },
  {
    id: "lab_c18_catalog_distance_400",
    hypothesis:
      "Same-sentence consideration catalog distance 280→400 for long como A, B, C lists",
    errorCluster: "MISSING_ASSOCIATED_FROM_DECISION_SET_MEMBERSHIP",
    apply() {
      replaceOnce(
        EVIDENCE,
        "if (sentCons2 && start - sentCons2.cueEnd <= 280) {",
        "if (sentCons2 && start - sentCons2.cueEnd <= 400) {",
        "c18-dist"
      );
    },
  },
  {
    id: "lab_c19_neutral_soft_brand_affiliation_titles",
    hypothesis:
      "Treat soft brand/collection affiliation overview titles as NEUTRAL_CATALOG (block assoc bleed)",
    errorCluster: "FALSE_ASSOCIATED_FROM_NEUTRAL_OR_PROFILE_CATALOG",
    apply() {
      replaceOnce(
        EVIDENCE,
        "soft\\s+brand(?:s|\\s+collections?)|major\\s+(?:soft\\s+)?brand",
        "soft\\s+brand(?:s|\\s+collections?)|soft\\s+brand\\s+and\\s+collection|collection\\s+hotel\\s+affiliations?|affiliations?\\s+that\\s+a\\s+hotel\\s+owner|major\\s+(?:soft\\s+)?brand",
        "c19-neutral-title"
      );
    },
  },
  {
    id: "lab_c20_block_pipeline_partnership_assoc",
    hypothesis:
      "Pipeline / partnership announcement lines are descriptive, not consideration membership",
    errorCluster: "FALSE_ASSOCIATED_FROM_NEUTRAL_OR_PROFILE_CATALOG",
    apply() {
      replaceOnce(
        EVIDENCE,
        `if (section?.sectionType === "NEUTRAL_CATALOG_SECTION") {
    descriptiveCue = true;
    // strip decision cues unless local direct positive/negative/lead
    if (!directPositiveCue && !leadCue && !directNegativeCue) {
      considerationSetCue = false;
      sectionPositiveCue = false;
      if (!confirmedRankStructure) {
        rankCue = false;
      }
    }
  }`,
        `if (section?.sectionType === "NEUTRAL_CATALOG_SECTION") {
    descriptiveCue = true;
    // strip decision cues unless local direct positive/negative/lead
    if (!directPositiveCue && !leadCue && !directNegativeCue) {
      considerationSetCue = false;
      sectionPositiveCue = false;
      if (!confirmedRankStructure) {
        rankCue = false;
      }
    }
  }
  // Partnership / pipeline narrative is descriptive even under decision-set parents
  if (
    /\\b(?:pipeline|partnership\\s+with|announced\\s+a\\s+partnership|forman?\\s+parte\\s+del\\s+pipeline|alianza\\s+con)\\b/i.test(
      line
    ) &&
    !directPositiveCue &&
    !leadCue &&
    !directNegativeCue
  ) {
    descriptiveCue = true;
    considerationSetCue = false;
    sectionPositiveCue = false;
  }`,
        "c20-pipeline"
      );
    },
  },
  {
    id: "lab_c21_como_catalog_after_consideration",
    hypothesis:
      "Spanish/English 'como/such as' catalog after consideration cue within 360 chars → associated",
    errorCluster: "MISSING_ASSOCIATED_RECALL",
    apply() {
      replaceOnce(
        EVIDENCE,
        `const catalogShape = /,/.test(between) || /\\b(?:and|y|e)\\b/i.test(between) || /\\n\\s*[-*•]/.test(between);`,
        `const catalogShape =
        /,/.test(between) ||
        /\\b(?:and|y|e|o|como|such\\s+as|including)\\b/i.test(between) ||
        /\\n\\s*[-*•]/.test(between) ||
        /\\*\\*[^*]+\\*\\*/.test(between);`,
        "c21-como"
      );
    },
  },
  {
    id: "lab_c22_comparator_requires_entity_as_object",
    hypothesis:
      "Suppress comparator when mention is subject of 'X is an alternative to Y' (entity before cue)",
    errorCluster: "COMPARATOR_BOUNDARY",
    apply() {
      replaceOnce(
        EVIDENCE,
        `const before80 = text.slice(Math.max(0, start - 80), start);
  let comparatorCue = COMPARATOR_BEFORE_RE.test(before80);`,
        `const before80 = text.slice(Math.max(0, start - 80), start);
  let comparatorCue = COMPARATOR_BEFORE_RE.test(before80);
  // Entity as subject of comparison sentence is not comparator-of-self
  if (
    comparatorCue &&
    /\\b(?:is|are|as)\\s+(?:a\\s+)?(?:strong\\s+)?(?:alternative|competitor|option)\\s+to\\b/i.test(
      text.slice(start, Math.min(text.length, end + 80))
    )
  ) {
    comparatorCue = false;
  }`,
        "c22-comp-subject"
      );
    },
  },
  {
    id: "lab_c23_block_assoc_under_parent_company_unknown",
    hypothesis:
      "Even with list child, do not force consideration cue under UNKNOWN parent-company ### headings",
    errorCluster: "FALSE_ASSOCIATED_FROM_NEUTRAL_OR_PROFILE_CATALOG",
    apply() {
      replaceOnce(
        EVIDENCE,
        `if (
    section?.sectionType === "CONSIDERATION_SET_SECTION" &&
    isListOrTableChild &&
    !RANK_SEMANTICS_RE.test(\`\${section.title}\\n\${section.sectionIntro}\`)
  ) {
    considerationSetCue = true;`,
        `if (
    section?.sectionType === "CONSIDERATION_SET_SECTION" &&
    isListOrTableChild &&
    !RANK_SEMANTICS_RE.test(\`\${section.title}\\n\${section.sectionIntro}\`) &&
    !/\\b(marriott|hilton|ihg|hyatt|accor|grupo|international)\\b/i.test(String(section.title || \"\"))
  ) {
    considerationSetCue = true;`,
        "c23-parent-block"
      );
    },
  },
  {
    id: "lab_c24_principales_marcas_consideradas_looser",
    hypothesis:
      "Broader Spanish principales marcas … consideradas (allow emoji/punct between tokens)",
    errorCluster: "MISSING_ASSOCIATED_FROM_DECISION_SET_MEMBERSHIP",
    apply() {
      replaceOnce(
        EVIDENCE,
        "principales\\s+marcas(?:\\s+\\w+){0,8}\\s+considerad[oa]s?",
        "principales\\s+marcas(?:[\\s\\S]{0,40}?)considerad[oa]s?",
        "c24-principales"
      );
    },
  },
  {
    id: "lab_c25_descriptive_presence_blocks_assoc",
    hypothesis:
      "Lines with presence/portfolio/operates descriptive verbs without local decision cue stay discussed",
    errorCluster: "FALSE_ASSOCIATED_OVER_PROMOTION",
    apply() {
      replaceOnce(
        EVIDENCE,
        `let descriptiveCue = DESCRIPTIVE_RE.test(sentence) || DESCRIPTIVE_RE.test(line);
  let incidentalCue = PASSING_RE.test(sentence) && !directPositiveCue && !considerationSetCue && !leadCue;`,
        `let descriptiveCue = DESCRIPTIVE_RE.test(sentence) || DESCRIPTIVE_RE.test(line);
  let incidentalCue = PASSING_RE.test(sentence) && !directPositiveCue && !considerationSetCue && !leadCue;
  if (
    descriptiveCue &&
    considerationSetCue &&
    !directPositiveCue &&
    !leadCue &&
    !section?.confirmedDecisionSet &&
    !isListOrTableChild
  ) {
    // Free-text descriptive co-mention after a distant consideration cue → discussed
    const localCons = findCueInRange(text, CONSIDERATION_SET_RE, Math.max(0, start - 100), start);
    if (!localCons) considerationSetCue = false;
  }`,
        "c25-desc-block"
      );
    },
  },
];

export function getCandidate(id) {
  return CANDIDATES.find((c) => c.id === id) || null;
}

export function candidatesForCluster(rootCause, exhausted = new Set()) {
  return CANDIDATES.filter((c) => c.errorCluster === rootCause && !exhausted.has(c.id));
}
