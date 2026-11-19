#!/usr/bin/env node
/**
 * Dealality Insight advisory quality — ban internal language; require assessment structure.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_DEALALITY_INSIGHT_ADVISORY_V1,
  DEALALITY_INSIGHT_FAIL,
  DEALALITY_ASSESSMENT_SECTIONS,
  containsInternalCustomerLanguage,
  detectInternalCustomerLanguage,
  isInternalLanguageRegression,
  applyDealalityInsightAdvisoryToBlocks,
  validateDealalityInsightAdvisory,
  sanitizeCustomerFacingText,
} from "../lib/brand-explorer/be-dealality-insight-advisory-v1.js";
import {
  buildCumulativeProposedBrand,
  decideSlotProvenance,
} from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const preview = path.join(root, "public", "brand-explorer-phase2e-preview");
const fixturePath = path.join(
  root,
  "fixtures/brand-explorer-dealality-insight-advisory/internal-language-leak-hpc-editorial.json",
);

assert.equal(BE_DEALALITY_INSIGHT_ADVISORY_V1, "BE_DEALALITY_INSIGHT_ADVISORY_V1");
assert.equal(DEALALITY_ASSESSMENT_SECTIONS.length, 6);

const leak =
  "HPC Open hotels matched=20; keys=653; pipeline hotels=0 (certState=LIVE_UNVERIFIED_INVENTORY). Keep narrative HPC-qualified (certState); named openings need steward-confirmed press URLs.";
assert.equal(containsInternalCustomerLanguage(leak), true);
assert.ok(detectInternalCustomerLanguage(leak).length >= 3);
assert.equal(sanitizeCustomerFacingText(leak), "");
assert.equal(
  sanitizeCustomerFacingText(
    "Curio is a conversion-weighted soft collection for distinctive assets.",
  ).length > 0,
  true,
);
assert.equal(isInternalLanguageRegression(leak, "Short clean note."), true);

const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const before = validateDealalityInsightAdvisory(fixture.blocks);
assert.equal(before.pass, false);
assert.ok(
  before.failures.some((f) => f.code === DEALALITY_INSIGHT_FAIL.INTERNAL_LANGUAGE),
);

const repaired = applyDealalityInsightAdvisoryToBlocks(fixture.blocks, {
  brand: { id: "receQkxgjlezsc1xg", name: "Curio Collection by Hilton" },
});
assert.ok(repaired.changedCount >= 1);
assert.ok(repaired.internalEvidence.length >= 1);

const after = validateDealalityInsightAdvisory(repaired.blocks, {
  requireAssessmentStructure: true,
});
assert.equal(
  after.pass,
  true,
  JSON.stringify(after.failures.slice(0, 12), null, 2),
);

// Candidate with HPC dump must not replace clean CURRENT
const decision = decideSlotProvenance(
  "footprint.editorial",
  [{ slotKey: "footprint.editorial", title: "Footprint note", body: "Clean directional note for owners." }],
  [{ slotKey: "footprint.editorial", title: "Footprint editorial", body: leak }],
);
assert.equal(decision.applyCandidate, false);
assert.equal(decision.reason, "candidate_internal_language_not_customer_facing");

// Curio proposed RIGHT
const current = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-gate1-renderer-parity-payload.json"), "utf8"),
);
const candidate = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-collection-phase2h-candidate.json"), "utf8"),
);
const leftEd = (current.brandExplorer.blocks || []).find((b) => b.slotKey === "footprint.editorial");
assert.ok(leftEd);
assert.equal(containsInternalCustomerLanguage(leftEd.body), false);

const { brand, assembly } = buildCumulativeProposedBrand(current, candidate);
assert.ok(assembly.dealalityInsightAdvisory);
assert.ok(assembly.dealalityInsightAdvisory.changedCount >= 1);

const rightEd = (brand.brandExplorer.blocks || []).find((b) => b.slotKey === "footprint.editorial");
assert.ok(rightEd);
assert.equal(containsInternalCustomerLanguage(rightEd.body), false);
assert.ok(!/HPC Open hotels matched/i.test(rightEd.body));
assert.ok(/soft collection|Hilton Honors|conversion/i.test(rightEd.body));

for (const sec of DEALALITY_ASSESSMENT_SECTIONS) {
  const b = (brand.brandExplorer.blocks || []).find((x) => x.slotKey === sec.slotKey);
  assert.ok(b && b.body && b.body.length >= 80, `missing ${sec.slotKey}`);
  assert.equal(containsInternalCustomerLanguage(b.body), false);
}

const rightValidation = validateDealalityInsightAdvisory(brand.brandExplorer.blocks || [], {
  requireAssessmentStructure: true,
});
assert.equal(rightValidation.pass, true, JSON.stringify(rightValidation.failures, null, 2));

// LEFT unchanged
assert.equal(leftEd.body, "Treat open/pipeline figures as Brand Footprint-scoped. Absence in one region is not a global zero for Curio.");

// Ascend pack path (candidate-only synthetic current)
const ascendCandidate = JSON.parse(
  fs.readFileSync(path.join(preview, "ascend-phase2h-candidate.json"), "utf8"),
);
const ascendCurrent = {
  id: "reclkgOzvAcBheUSo",
  name: "Ascend Hotel Collection",
  brandExplorer: {
    version: 1,
    blocks: [
      {
        slotKey: "footprint.editorial",
        title: "Footprint note",
        body: "Treat open/pipeline figures as Brand Footprint-scoped for Ascend.",
      },
      {
        slotKey: "insight.summary",
        title: "Summary",
        body: "Ascend soft-collection summary retained for owners evaluating Choice distribution with local character.",
      },
    ],
  },
};
const ascendBuilt = buildCumulativeProposedBrand(ascendCurrent, ascendCandidate);
const ascendEd = (ascendBuilt.brand.brandExplorer.blocks || []).find(
  (b) => b.slotKey === "footprint.editorial",
);
assert.ok(ascendEd);
assert.equal(containsInternalCustomerLanguage(ascendEd.body), false);
assert.ok(!/HPC Open hotels matched/i.test(ascendEd.body || ""));
const ascendOverall = (ascendBuilt.brand.brandExplorer.blocks || []).find(
  (b) => b.slotKey === "insight.assessment.overall",
);
assert.ok(ascendOverall && /Choice/i.test(ascendOverall.body));
assert.ok(!/Curio|Hilton Honors/i.test(ascendOverall.body), "Ascend pack must not reuse Curio copy");

const contract = JSON.parse(
  fs.readFileSync(
    path.join(root, "configs/brand-explorer-permanent-content-standards-contract-v1.json"),
    "utf8",
  ),
);
assert.equal(
  contract.fixedCollections.dealalityInsightAdvisory.permanentStandard,
  BE_DEALALITY_INSIGHT_ADVISORY_V1,
);

const lessons = JSON.parse(
  fs.readFileSync(
    path.join(root, "configs/brand-explorer-founder-quality-lessons-registry-v1.json"),
    "utf8",
  ),
);
assert.ok(lessons.lessons.some((l) => l.lessonId === "BE-QL-DEALALITY-INSIGHT-001"));

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_DEALALITY_INSIGHT_ADVISORY_V1,
      fixtureFailuresBefore: before.failureCount,
      advisoryChangedOnProposed: assembly.dealalityInsightAdvisory.changedCount,
      rightEditorialPreview: String(rightEd.body).slice(0, 120),
      assessmentSlots: DEALALITY_ASSESSMENT_SECTIONS.map((s) => s.slotKey),
      ascendPackOk: true,
      airtableWrites: 0,
      founderApproved: false,
      curioAscend: "NOT_APPROVED",
      gate2: "BLOCKED_UNTIL_GATE1",
    },
    null,
    2,
  ),
);
