#!/usr/bin/env node
/**
 * Evidence-closure contract tests.
 */
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { mkdtempSync, rmSync, existsSync, readFileSync } from "fs";
import { tmpdir } from "os";
import {
  isExplicitApprovalStatus,
  projectApprovalFromEvidence,
  classifyBrandRelationshipDepth,
  PROJECT_APPROVAL,
} from "../lib/operator-fit/brand-relationship-depth.js";
import { evaluateBrandOperatorCompatibility } from "../lib/operator-fit/brand-operator-compatibility.js";
import { fieldPresent } from "../lib/operator-fit/adapters/field-state.js";
import {
  createShortlistEntry,
  removeShortlistEntry,
  listShortlistForDeal,
} from "../lib/operator-fit/shortlist-store.js";
import {
  upsertAdvisorScorecard,
  loadAdvisorScorecards,
} from "../lib/operator-fit/advisor-scorecards.js";
import { diagnoseMarketPresenceCliff } from "../lib/operator-fit/market-presence-cliff.js";
import { classifyRankChangeSensitivity } from "../lib/operator-fit/rank-change-actionability.js";
import { MARKET_PRESENCE_TYPE } from "../lib/operator-intelligence/market-presence.js";
import { evaluateOperatorFitInternalPilotAccess } from "../lib/operator-fit/internal-pilot-access.js";
import { isOperatorFitEngineV2Enabled } from "../lib/operator-fit/feature-flag.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function ok(name, cond) {
  if (!cond) {
    failed += 1;
    console.error("FAIL", name);
  } else console.log("ok", name);
}

ok("shortlist architecture doc", existsSync(join(root, "docs/architecture/operator-fit-shortlist-architecture.md")));
ok("evidence closure ADR", existsSync(join(root, "docs/architecture/decisions/operator-fit-internal-pilot-evidence-closure.md")));
ok("brand depth model doc", existsSync(join(root, "docs/data/operator-fit-brand-relationship-depth-model.md")));

ok("not global approval is not explicit Approved", !isExplicitApprovalStatus("Property-scoped — not global approval"));
ok("Approved is explicit", isExplicitApprovalStatus("Approved"));
ok("project approval default both confirm", projectApprovalFromEvidence({}) === PROJECT_APPROVAL.BOTH_MUST_CONFIRM);
ok("unknown project approval never Confirmed", projectApprovalFromEvidence({ confirmed: false }) !== PROJECT_APPROVAL.CONFIRMED);

{
  const d = classifyBrandRelationshipDepth({
    brand: "Hilton",
    relationshipStatus: "Verified Current",
    currentOrHistorical: "Current",
    approvalStatus: "Property-scoped — not global approval",
  });
  ok("depth separates project approval", d.projectApproval === PROJECT_APPROVAL.BOTH_MUST_CONFIRM);
  ok("depth approval unknown for property-scoped note", d.approvalStatus === "Approval Unknown");
}

{
  const project = { selectedOrEvaluatedBrands: fieldPresent(["Hilton"]) };
  const operator = {
    brandsOperated: fieldPresent(["Hilton"]),
    brandApprovals: [{ brand: "Hilton", status: "Property-scoped — not global approval" }],
  };
  const compat = evaluateBrandOperatorCompatibility(project, operator);
  ok("substring approval does not claim Approved category alone via false approval", compat.projectApproval === "Both Parties Must Confirm");
  ok("validation item for project approval present", (compat.validationItems || []).some((x) => /project-specific brand approval/i.test(x)));
}

{
  const hist = evaluateBrandOperatorCompatibility(
    { selectedOrEvaluatedBrands: fieldPresent(["Hilton"]) },
    {
      brandsOperated: fieldPresent(["Hilton"]),
      brandApprovals: [{ brand: "Hilton", status: "Historically Approved", currentOrHistorical: "Historical" }],
    }
  );
  ok("historical lower or partial numeric", hist.numericForComposition != null && hist.numericForComposition <= 70);
}

{
  const dir = mkdtempSync(join(tmpdir(), "of-ec-"));
  const path = join(dir, "sl.json");
  const e = createShortlistEntry(
    {
      dealId: "pilot_deal_c",
      operatorId: "recTEST",
      operatorName: "Test",
      alignment: 40,
      confidence: "Strong",
      coverage: 60,
      eligibility: "Eligible With Conditions",
      readiness: "Ranking Ready",
      reasons: ["geo"],
      engineVersion: "operator-fit-v2.1.0",
    },
    path
  );
  const snap = e.snapshot.alignment;
  removeShortlistEntry(e.id, { reason: "test", path });
  ok("snapshot immutable after remove", listShortlistForDeal("pilot_deal_c", { path })[0].snapshot.alignment === snap);
  rmSync(dir, { recursive: true, force: true });
}

{
  const dir = mkdtempSync(join(tmpdir(), "of-asc-"));
  const path = join(dir, "cards.json");
  const card = upsertAdvisorScorecard(
    {
      dealId: "pilot_deal_c",
      overallDecision: "Useful internally but needs improvement",
      rationale: "test",
      rankingCredibility: { overRanked: "Negative", comments: "too precise" },
    },
    path
  );
  ok("advisor card mutatesAlgorithmScores false", card.mutatesAlgorithmScores === false);
  const store = loadAdvisorScorecards(path);
  ok("advisor store separate", (store.scorecards || []).length >= 1 && store.note);
  rmSync(dir, { recursive: true, force: true });
}

{
  const cliff = diagnoseMarketPresenceCliff({
    fromType: MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST,
    toType: MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY,
  });
  ok("cliff diagnostic deterministic correct", cliff.verdict === "Correct eligibility behavior" && cliff.scoringRetuneRecommended === false);
  const cliff2 = diagnoseMarketPresenceCliff({
    fromType: MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST,
    toType: MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY,
  });
  ok("cliff deterministic json", JSON.stringify(cliff) === JSON.stringify(cliff2));
}

{
  const s = classifyRankChangeSensitivity({
    impact: "eligibility",
    criticality: "required_before_outreach",
    question: "Confirm Market Presence",
  });
  ok("rank change eligibility sensitive", s.sensitivity === "Eligibility-sensitive" && s.material);
}

ok(
  "owner pilot engine still off by default",
  !isOperatorFitEngineV2Enabled({ OPERATOR_FIT_ENGINE_V2: "0" })
);
ok(
  "owners blocked without internal pilot",
  !evaluateOperatorFitInternalPilotAccess({
    env: { OPERATOR_FIT_INTERNAL_PILOT: "0", OPERATOR_FIT_ENGINE_V2: "0" },
    user: { isAdmin: false },
    dealId: "pilot_deal_c",
    requireDeal: true,
  }).allowed
);

ok("migration report exists", existsSync(join(root, "reports/operator-fit-shortlist-migration.md")));
ok("schema ensure created or reported", existsSync(join(root, "reports/operator-fit-shortlist-schema-ensure.json")));
const ensure = JSON.parse(readFileSync(join(root, "reports/operator-fit-shortlist-schema-ensure.json"), "utf8"));
ok("shortlist table created", ensure.action === "created" || ensure.exists === true);
ok("odr untouched in ensure", ensure.notOdr === true && ensure.odrTableUntouched === true);

ok("terminology rec exists", existsSync(join(root, "docs/reviews/operator-fit-owner-terminology-recommendation.md")));
ok("score presentation rec exists", existsSync(join(root, "docs/reviews/operator-fit-score-presentation-recommendation.md")));
ok("handoff architecture exists", existsSync(join(root, "docs/architecture/operator-fit-shortlist-to-outreach-handoff.md")));
ok("live scorecards report exists", existsSync(join(root, "reports/operator-fit-live-advisor-scorecards.md")));

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll evidence-closure tests passed");
