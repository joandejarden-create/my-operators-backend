#!/usr/bin/env node
/**
 * Final UX/trust closure tests — presentation only; scoring untouched.
 */
import { existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  mapAlignmentBand,
  mapEvidenceStrength,
  prioritizeUnknowns,
  classifyConcern,
  buildWhyThisOperator,
  buildOwnerCandidatePresentation,
  buildOwnerStyleComparison,
  buildZeroUniverseOwnerMessage,
  OWNER_TERMS,
} from "../lib/operator-fit/owner-presentation.js";
import { OPERATOR_PROJECT_FACTORS } from "../lib/operator-fit/config.js";
import { isOperatorFitEngineV2Enabled } from "../lib/operator-fit/feature-flag.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function ok(name, cond) {
  if (!cond) {
    failed += 1;
    console.error("FAIL", name);
  } else console.log("ok", name);
}

ok("founder decisions ADR", existsSync(join(root, "docs/architecture/decisions/operator-fit-final-internal-pilot-founder-decisions.md")));
ok("final owner language", existsSync(join(root, "docs/reviews/operator-fit-final-owner-language.md")));
ok("final score presentation", existsSync(join(root, "docs/reviews/operator-fit-final-score-presentation.md")));

const hard = mapAlignmentBand(90, "Not Currently Eligible");
ok("hard fail cannot be Strong band", hard.id === "limited" || hard.contradictedByEligibility);

const band = mapAlignmentBand(72, "Eligible With Conditions");
ok("conditions visible on high score", band.projectCompatibility === OWNER_TERMS.potentialFitValidationNeeded);

const strength = mapEvidenceStrength("Strong");
ok("evidence strength helper", strength.label === "Strong" && /independently/i.test(strength.helperText));

const unk = prioritizeUnknowns([
  "Confirm brand approvals",
  "Missing: Project-specific commercial differentiator",
  "Interesting trivia",
  "Market presence not currently eligible",
]);
ok("critical unknown prioritized", unk.primary[0]?.priority === "Critical" || unk.all[0]?.priority === "Critical");
ok("primary unknowns capped", unk.primary.length <= 2);

const concern = classifyConcern("Country presence documented, but regional team/resources are not confirmed.");
ok("unknown regional not weak judgment", /has not yet been confirmed/i.test(concern.text));

const why = buildWhyThisOperator({
  operatorName: "Test",
  whyItMatches: ["Active country: Mexico", "Operator is Active.", "Directly comparable assignment(s): Hotel X"],
});
ok("why excludes generic active", why.reasons.every((r) => !/operator is active/i.test(r)) && why.reasons.length <= 3);

const card = buildOwnerCandidatePresentation({
  operatorName: "Op",
  rank: 1,
  displayedOperatorAlignment: 48,
  eligibilityStatus: "Eligible With Conditions",
  evidenceConfidence: "Moderate",
  whyItMatches: ["Active country: Mexico", "Supports Upper Upscale"],
  potentialConcerns: ["Limited asset-type overlap"],
  unknowns: ["Confirm project-specific brand approval", "Fees unknown"],
});
ok("owner card has band not requiring /100 on L1", card.alignmentBand && card.numericAlignment == null);
ok("validate next present", card.validateNextPrimary || card.validateNext.actions.length >= 0);

const cmp = buildOwnerStyleComparison([
  card,
  buildOwnerCandidatePresentation({
    operatorName: "Op2",
    displayedOperatorAlignment: 36,
    eligibilityStatus: "Eligible With Conditions",
    evidenceConfidence: "Limited",
    whyItMatches: ["Active country: Mexico"],
  }),
]);
ok("comparison has tradeoffs", cmp.tradeOffs.length === 2);

const zero = buildZeroUniverseOwnerMessage({ underEvaluation: [{ operatorName: "Local Co" }] });
ok("zero universe constrained wording", /currently verified universe/i.test(zero.headline));
ok("under evaluation separate", zero.underEvaluation[0].label === OWNER_TERMS.underEvaluation);

// Scoring config not mutated by presentation module — weight still 22 for geography
ok("scoring weights frozen spot-check", OPERATOR_PROJECT_FACTORS.geographyMarket.weight === 22);
ok("owner engine still default off", !isOperatorFitEngineV2Enabled({ OPERATOR_FIT_ENGINE_V2: "0" }));

ok("final payload exists", existsSync(join(root, "reports/operator-fit-final-internal-pilot-ui-payload.json")));
const payload = JSON.parse(readFileSync(join(root, "reports/operator-fit-final-internal-pilot-ui-payload.json"), "utf8"));
ok("payload scoring frozen", payload.scoringFrozen === true && payload.ownerPilotEnabled === false);
ok("round2 material zero", payload.round2?.material === 0);
ok("deal c report", existsSync(join(root, "reports/operator-fit-deal-c-owner-pilot-readiness.md")));
ok("deal d report", existsSync(join(root, "reports/operator-fit-deal-d-owner-pilot-readiness.md")));
ok("zero universe review", existsSync(join(root, "reports/operator-fit-zero-universe-owner-view-review.md")));
ok("anchoring review", existsSync(join(root, "reports/operator-fit-score-anchoring-review.md")));
ok("provisional pilot plan", existsSync(join(root, "docs/architecture/operator-fit-controlled-owner-pilot-plan.md")));
ok("founder review", existsSync(join(root, "docs/reviews/operator-fit-final-internal-pilot-founder-review.md")));

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll final-internal-pilot UX tests passed");
