#!/usr/bin/env node
/**
 * Editorial Title Case — unit + proposed-profile + founder screenshot regressions.
 * Includes universal heading conjunction (& vs And) coverage.
 * Unit tests alone are insufficient for product claims; browser audit is separate.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_EDITORIAL_TITLE_CASE_V1,
  toEditorialTitleCase,
  isEditorialTitleCase,
  normalizeHeadingConjunctionAmpersand,
  applyEditorialTitleCaseToBlocks,
  validateEditorialTitleCase,
  FOUNDER_TITLE_CASE_REGRESSION_EXAMPLES,
} from "../lib/brand-explorer/be-editorial-title-case-v1.js";
import { buildCumulativeProposedBrand } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const preview = path.join(root, "public", "brand-explorer-phase2e-preview");
const fixturePath = path.join(
  root,
  "fixtures/brand-explorer-title-case-regressions/failing-founder-screenshot-headings.json",
);
const conjunctionFixturePath = path.join(
  root,
  "fixtures/brand-explorer-title-case-regressions/heading-conjunction-ampersand.json",
);

assert.equal(BE_EDITORIAL_TITLE_CASE_V1, "BE_EDITORIAL_TITLE_CASE_V1");

for (const ex of FOUNDER_TITLE_CASE_REGRESSION_EXAMPLES) {
  assert.equal(
    toEditorialTitleCase(ex.input),
    ex.expected,
    `${ex.input} → ${ex.expected}`,
  );
  if (ex.input !== ex.expected) {
    assert.equal(isEditorialTitleCase(ex.input), false);
  }
  assert.equal(isEditorialTitleCase(ex.expected), true);
}

assert.equal(toEditorialTitleCase("Support Across the Lifecycle"), "Support Across the Lifecycle");
assert.equal(toEditorialTitleCase("Opening & Conversion Path"), "Opening & Conversion Path");
assert.equal(toEditorialTitleCase("distribution & retail reach"), "Distribution & Retail Reach");
assert.equal(toEditorialTitleCase("F&B as Differentiator"), "F&B as Differentiator");
assert.equal(toEditorialTitleCase("GDS and OTA Reach"), "GDS & OTA Reach");
assert.equal(toEditorialTitleCase("GDS And OTA Reach"), "GDS & OTA Reach");
assert.equal(
  toEditorialTitleCase("Commercial Platform And Lobby Experience"),
  "Commercial Platform & Lobby Experience",
);
assert.equal(
  toEditorialTitleCase("Independent & Soft-Brand Conversion"),
  "Independent & Soft-Brand Conversion",
);
assert.equal(
  normalizeHeadingConjunctionAmpersand("Opening And Conversion Path"),
  "Opening & Conversion Path",
);

// Body prose must not be rewritten (including mid-sentence "and")
const prose =
  "Published Hilton brand materials describe a global Curio Collection footprint across dozens of countries with a substantial development pipeline.";
assert.equal(toEditorialTitleCase(prose), prose);
const proseAnd =
  "Resorts, beach and golf destinations where authenticity and dining drive rate power.";
assert.equal(toEditorialTitleCase(proseAnd), proseAnd);
assert.ok(proseAnd.includes(" and "), "body fixture must retain word and");

const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const before = validateEditorialTitleCase(fixture.blocks);
assert.equal(before.pass, false, "fixture must FAIL before normalization");
assert.ok(before.failures.length >= 3, "founder screenshot failures must be detected");

const repaired = applyEditorialTitleCaseToBlocks(fixture.blocks);
const after = validateEditorialTitleCase(repaired.blocks);
assert.equal(after.pass, true, JSON.stringify(after.failures, null, 2));
fixture.expectedAfterNormalization.forEach((expected, i) => {
  assert.equal(repaired.blocks[i].title, expected);
});

// Dedicated And / & conjunction fixture
const conj = JSON.parse(fs.readFileSync(conjunctionFixturePath, "utf8"));
const conjBefore = validateEditorialTitleCase(conj.blocks);
assert.equal(conjBefore.pass, false, "conjunction fixture must FAIL before normalization");
const conjRepaired = applyEditorialTitleCaseToBlocks(conj.blocks);
const conjAfter = validateEditorialTitleCase(conjRepaired.blocks);
assert.equal(conjAfter.pass, true, JSON.stringify(conjAfter.failures, null, 2));
conj.expectedAfterNormalization.forEach((expected, i) => {
  assert.equal(conjRepaired.blocks[i].title, expected, `conj title[${i}]`);
  assert.equal(conjRepaired.blocks[i].body, conj.bodyMustRemainUnchanged[i], `conj body[${i}]`);
});
assert.ok(
  conjRepaired.changedSlots.some(
    (c) =>
      c.from === "Commercial Platform And Lobby Experience" &&
      c.to === "Commercial Platform & Lobby Experience",
  ),
  "must record And→& heading change for Highlight Changes provenance",
);

// Proposed RIGHT must Title-Case CURRENT Curio proof headings; LEFT source unchanged
const current = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-gate1-renderer-parity-payload.json"), "utf8"),
);
const candidate = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-collection-phase2h-candidate.json"), "utf8"),
);
const leftProof1 = (current.brandExplorer.blocks || []).find((b) => b.slotKey === "overview.proof.1");
assert.ok(leftProof1);
assert.equal(leftProof1.title, "Global soft-collection scale", "LEFT source must remain sentence case");

const leftScenario2 = (current.brandExplorer.blocks || []).find(
  (b) => b.slotKey === "overview.scenario.2",
);
assert.ok(leftScenario2);
assert.equal(
  leftScenario2.title,
  "Commercial Platform And Lobby Experience",
  "LEFT Curio scenario must retain stored And spelling until founder approval",
);

const { brand, assembly } = buildCumulativeProposedBrand(current, candidate);
const rightProof1 = (brand.brandExplorer.blocks || []).find((b) => b.slotKey === "overview.proof.1");
assert.equal(rightProof1.title, "Global Soft-Collection Scale");
assert.ok(assembly.titleCase && assembly.titleCase.changedCount > 0);

const rightProof2 = (brand.brandExplorer.blocks || []).find((b) => b.slotKey === "overview.proof.2");
const rightProof3 = (brand.brandExplorer.blocks || []).find((b) => b.slotKey === "overview.proof.3");
assert.equal(rightProof2.title, "Conversion-Weighted Growth");
assert.equal(rightProof3.title, "Curio Since 2014");

const rightScenario2 = (brand.brandExplorer.blocks || []).find(
  (b) => b.slotKey === "overview.scenario.2",
);
assert.equal(rightScenario2.title, "Commercial Platform & Lobby Experience");
assert.ok(
  (assembly.titleCase.changedSlots || []).some(
    (c) =>
      c.slotKey === "overview.scenario.2" &&
      c.from === "Commercial Platform And Lobby Experience" &&
      c.to === "Commercial Platform & Lobby Experience",
  ),
  "assembler must flag scenario.2 And→& for Highlight Changes",
);

const rightAmpersandOk = (brand.brandExplorer.blocks || []).find(
  (b) => b.title === "Independent & Soft-Brand Conversion",
);
assert.ok(rightAmpersandOk, "already-correct & titles must remain unchanged");

// LEFT object untouched
assert.equal(leftProof1.title, "Global soft-collection scale");
assert.equal(leftScenario2.title, "Commercial Platform And Lobby Experience");

const rightValidation = validateEditorialTitleCase(brand.brandExplorer.blocks || [], {
  onlySemanticSlots: true,
});
assert.equal(
  rightValidation.pass,
  true,
  `RIGHT still has non-Title-Case headings:\n${JSON.stringify(rightValidation.failures.slice(0, 20), null, 2)}`,
);
const andWordHeadings = (brand.brandExplorer.blocks || []).filter(
  (b) => b.title && /\s+[Aa][Nn][Dd]\s+/.test(b.title) && !/\s+&\s+/.test(b.title),
);
assert.equal(
  andWordHeadings.length,
  0,
  `RIGHT still has standalone And conjunctions:\n${JSON.stringify(
    andWordHeadings.map((b) => ({ slotKey: b.slotKey, title: b.title })).slice(0, 10),
    null,
    2,
  )}`,
);

// Contract pointer
const contract = JSON.parse(
  fs.readFileSync(
    path.join(root, "configs/brand-explorer-permanent-content-standards-contract-v1.json"),
    "utf8",
  ),
);
assert.equal(contract.typographyAndWriting.sectionHeadings.case, "Title Case");
assert.equal(contract.typographyAndWriting.sectionHeadings.conjunction, "&");
assert.equal(contract.typographyAndWriting.sectionHeadings.forbidStandaloneWordAnd, true);
assert.equal(contract.typographyAndWriting.cardHeadings.conjunction, "&");
assert.ok(contract.typographyAndWriting.cardHeadings);
assert.equal(contract.typographyAndWriting.cardHeadings.enforcer, BE_EDITORIAL_TITLE_CASE_V1);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_EDITORIAL_TITLE_CASE_V1,
      founderExamplesPass: true,
      conjunctionFixturePass: true,
      fixtureFailuresBefore: before.failureCount,
      titleCaseChangedOnProposed: assembly.titleCase.changedCount,
      rightProofTitles: [rightProof1.title, rightProof2.title, rightProof3.title],
      rightScenario2: rightScenario2.title,
      leftScenario2Unchanged: leftScenario2.title,
      leftUnchanged: leftProof1.title,
      airtableWrites: 0,
      founderApproved: false,
    },
    null,
    2,
  ),
);
