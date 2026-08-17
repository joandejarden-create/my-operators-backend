#!/usr/bin/env node
/**
 * Hardening 6 — section propagation + rank + Spanish structural tests.
 * No holdout. No provider calls.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import {
  extractEntityLocalEvidence,
  buildTypedSections,
  classifySectionType,
  classifyCatalogSemantics,
  evaluateSectionPropagation,
  RECOMMENDATION_EVIDENCE_VERSION,
} from "../lib/ai-visibility/recommendation-evidence-v4_1.js";
import {
  decideRecommendationRoleFromEvidence,
  RECOMMENDATION_CLASSIFIER_VERSION,
} from "../lib/ai-visibility/recommendation-classifier-v4_1.js";

function roleOf(text, name) {
  const start = text.indexOf(name);
  assert.ok(start >= 0, `missing ${name}`);
  const typed = buildTypedSections(text);
  const ev = extractEntityLocalEvidence({
    text,
    start,
    end: start + name.length,
    rawMention: name,
    typedSections: typed,
    canonicalEntityName: name,
  });
  return { ...decideRecommendationRoleFromEvidence(ev, { entityPresent: true }), evidence: ev };
}

function fsReadEvidence() {
  return fs.readFileSync(
    fileURLToPath(new URL("../lib/ai-visibility/recommendation-evidence-v4_1.js", import.meta.url)),
    "utf8"
  );
}

let passed = 0;
function check(name, fn) {
  fn();
  passed++;
  console.log("PASS", name);
}

check("CONSIDERATION_SECTION_PROPAGATES_TO_CHILD", () => {
  const r = roleOf("## Brands to consider\n\n- Autograph Collection\n- Curio\n", "Autograph Collection");
  assert.equal(r.role, "associated_option");
  assert.equal(r.evidence.recommendationEvidence.considerationSetCue, true);
});

check("NEUTRAL_CATALOG_DOES_NOT_PROPAGATE_ASSOCIATION", () => {
  const r = roleOf("## Brand profiles\n\n- Autograph Collection\n- Curio\n", "Autograph Collection");
  assert.equal(r.role, "discussed");
  assert.equal(r.evidence.recommendationEvidence.considerationSetCue, false);
});

check("RECOMMENDATION_SECTION_BOUNDED_PROPAGATION", () => {
  const r = roleOf("## Recommended brands:\n\n- Autograph Collection\n", "Autograph Collection");
  assert.equal(r.role, "explicit_recommendation");
  assert.equal(r.evidence.recommendationEvidence.sectionPositiveCue, true);
});

check("HEADING_RESET_STOPS_PROPAGATION", () => {
  const r = roleOf(
    "## Brands to consider\n- Autograph Collection\n\n## Brand profiles\n- Curio Collection\n",
    "Curio Collection"
  );
  assert.equal(r.role, "discussed");
  assert.equal(r.evidence.sectionType, "NEUTRAL_CATALOG_SECTION");
});

check("RANKED_SECTION_POSITION1_FIRST", () => {
  const r = roleOf(
    "Soft-brand shortlist:\n1. Autograph Collection\n2. Tribute Portfolio\n",
    "Autograph Collection"
  );
  assert.equal(r.role, "first_recommendation");
  assert.equal(r.evidence.rankPosition, 1);
});

check("RANKED_SECTION_POSITION2_RANKED", () => {
  const r = roleOf(
    "Soft-brand shortlist:\n1. Autograph Collection\n2. Tribute Portfolio\n",
    "Tribute Portfolio"
  );
  assert.equal(r.role, "ranked_recommendation");
  assert.equal(r.evidence.rankPosition, 2);
});

check("NEUTRAL_ORDERED_LIST_NOT_RANKED", () => {
  const r = roleOf("1. Autograph Collection\n2. Curio Collection\n", "Autograph Collection");
  assert.notEqual(r.role, "first_recommendation");
  assert.notEqual(r.role, "ranked_recommendation");
  assert.equal(r.evidence.confirmedRankStructure, false);
});

check("SECTION_NUMBER_NOT_RANK", () => {
  const text =
    "## Soft Brand overview\n\n### 1. Marriott International\nAutograph Collection operates in the region.\n";
  const r = roleOf(text, "Autograph Collection");
  assert.notEqual(r.role, "first_recommendation");
  assert.equal(r.evidence.confirmedRankStructure, false);
});

check("SPANISH_CONSIDERATION_SECTION_ASSOCIATED", () => {
  const r = roleOf("Marcas a considerar:\n- Autograph Collection\n", "Autograph Collection");
  assert.equal(r.role, "associated_option");
});

check("SPANISH_RANKED_SECTION_WORKS", () => {
  const r = roleOf(
    "Orden de prioridad:\n1. Autograph Collection\n2. Curio Collection\n",
    "Curio Collection"
  );
  assert.equal(r.role, "ranked_recommendation");
});

check("OTHER_ENTITY_CUE_NOT_INHERITED", () => {
  const text =
    "Other alternatives include local operators; Highgate remains focused on urban assets.";
  const r = roleOf(text, "Highgate");
  assert.notEqual(r.role, "associated_option");
});

check("UNBOUNDED_TEXT_NOT_USED", () => {
  const text =
    "We recommend soft brands for owners.\n\n## Background\n\nAutograph Collection launched in 2010.\n";
  const r = roleOf(text, "Autograph Collection");
  assert.notEqual(r.role, "explicit_recommendation");
  assert.equal(r.evidence.recommendationEvidence.directPositiveCue, false);
});

check("SECTION_TYPES_DETERMINISTIC", () => {
  assert.equal(classifySectionType("Brands to consider", ""), "CONSIDERATION_SET_SECTION");
  assert.equal(classifySectionType("Soft-brand shortlist:", ""), "RANKED_RECOMMENDATION_SECTION");
  assert.equal(classifySectionType("Recommended brands:", ""), "RECOMMENDATION_SET_SECTION");
  assert.equal(classifySectionType("Brand profiles", ""), "NEUTRAL_CATALOG_SECTION");
  assert.equal(classifyCatalogSemantics("CONSIDERATION_SET_SECTION"), "DECISION_SET");
  assert.equal(classifyCatalogSemantics("NEUTRAL_CATALOG_SECTION"), "NEUTRAL_CATALOG");
});

check("PROPAGATION_GATE_DISTANCE", () => {
  const sections = buildTypedSections("## Brands to consider\n\n- A\n");
  const gate = evaluateSectionPropagation({
    section: sections.find((s) => s.sectionType === "CONSIDERATION_SET_SECTION") || sections[0],
    entityStart: (sections.find((s) => s.sectionType === "CONSIDERATION_SET_SECTION") || sections[0])
      .start + 10,
    isListOrTableChild: true,
    requestedCue: "consideration",
  });
  assert.equal(gate.PROPAGATION_ALLOWED, true);
});

check("NO_CASE_SPECIFIC_RULES", () => {
  const src = fsReadEvidence();
  assert.ok(!/v1_g0|v2_cand_|caseId\s*===/.test(src));
});

check("NO_PROVIDER_SPECIFIC_HACKS", () => {
  const src = fsReadEvidence();
  assert.ok(!/openai|anthropic|gemini|perplexity/i.test(src));
});

check("NO_GEOGRAPHY_SPECIFIC_HACKS", () => {
  const src = fsReadEvidence();
  assert.ok(!/if\s*\(.*mexico|geography\s*===\s*['"]MX/i.test(src));
});

check("VERSIONS", () => {
  assert.equal(RECOMMENDATION_EVIDENCE_VERSION, "ai_visibility_recommendation_evidence_v4_1");
  assert.equal(
    RECOMMENDATION_CLASSIFIER_VERSION,
    "ai_visibility_recommendation_classifier_v4_1"
  );
});

console.log(`\n${passed} tests passed`);
