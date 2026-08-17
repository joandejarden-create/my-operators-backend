#!/usr/bin/env node
/**
 * Operator Fit Internal Pilot — unit/contract tests (no live owner exposure).
 */
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { mkdtempSync, rmSync, existsSync } from "fs";
import { tmpdir } from "os";

import { evaluateOperatorFitInternalPilotAccess } from "../lib/operator-fit/internal-pilot-access.js";
import { explainRankingDifference } from "../lib/operator-fit/ranking-difference.js";
import { listRankingChangeValidations } from "../lib/operator-fit/ranking-change-validations.js";
import {
  createShortlistEntry,
  removeShortlistEntry,
  listShortlistForDeal,
  withCurrentVsSnapshot,
} from "../lib/operator-fit/shortlist-store.js";
import { buildShortlistComparison } from "../lib/operator-fit/shortlist-compare.js";
import { evaluateEligibility } from "../lib/operator-fit/eligibility.js";
import { fieldPresent } from "../lib/operator-fit/adapters/field-state.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function ok(name, cond) {
  if (!cond) {
    failed += 1;
    console.error("FAIL", name);
  } else {
    console.log("ok", name);
  }
}

// Access control matrix
{
  const baseAdmin = { email: "a@dealality.com", isAdmin: true, flags: { isAdmin: true } };
  const owner = { email: "owner@example.com", isAdmin: false, flags: {} };

  ok(
    "pilot off blocks",
    !evaluateOperatorFitInternalPilotAccess({
      env: { OPERATOR_FIT_INTERNAL_PILOT: "0", OPERATOR_FIT_ENGINE_V2: "0" },
      user: baseAdmin,
      dealId: "pilot_deal_a",
      requireDeal: true,
    }).allowed
  );

  ok(
    "pilot on + admin + allowlist allows",
    evaluateOperatorFitInternalPilotAccess({
      env: {
        OPERATOR_FIT_INTERNAL_PILOT: "1",
        OPERATOR_FIT_ENGINE_V2: "0",
        OPERATOR_FIT_PILOT_DEAL_ALLOWLIST: "pilot_deal_a,pilot_deal_b",
        NODE_ENV: "development",
      },
      user: baseAdmin,
      dealId: "pilot_deal_a",
      requireDeal: true,
    }).allowed
  );

  ok(
    "non-admin blocked",
    !evaluateOperatorFitInternalPilotAccess({
      env: {
        OPERATOR_FIT_INTERNAL_PILOT: "1",
        OPERATOR_FIT_PILOT_DEAL_ALLOWLIST: "pilot_deal_a",
        NODE_ENV: "development",
      },
      user: owner,
      dealId: "pilot_deal_a",
      requireDeal: true,
    }).allowed
  );

  ok(
    "deal not on allowlist blocked",
    !evaluateOperatorFitInternalPilotAccess({
      env: {
        OPERATOR_FIT_INTERNAL_PILOT: "1",
        OPERATOR_FIT_PILOT_DEAL_ALLOWLIST: "pilot_deal_a",
        NODE_ENV: "development",
      },
      user: baseAdmin,
      dealId: "other_deal",
      requireDeal: true,
    }).allowed
  );

  ok(
    "production blocked without allow flag",
    !evaluateOperatorFitInternalPilotAccess({
      env: {
        OPERATOR_FIT_INTERNAL_PILOT: "1",
        OPERATOR_FIT_PILOT_DEAL_ALLOWLIST: "pilot_deal_a",
        NODE_ENV: "production",
      },
      user: baseAdmin,
      dealId: "pilot_deal_a",
      requireDeal: true,
    }).allowed
  );

  ok(
    "global owner flag does not alone grant pilot",
    !evaluateOperatorFitInternalPilotAccess({
      env: {
        OPERATOR_FIT_INTERNAL_PILOT: "0",
        OPERATOR_FIT_ENGINE_V2: "1",
        OPERATOR_FIT_PILOT_DEAL_ALLOWLIST: "pilot_deal_a",
        NODE_ENV: "development",
      },
      user: baseAdmin,
      dealId: "pilot_deal_a",
      requireDeal: true,
    }).allowed
  );
}

// Ranking difference determinism
{
  const a = {
    operatorName: "Highgate",
    displayedOperatorAlignment: 48,
    evidenceConfidence: "Strong",
    dataCoveragePct: 70,
    factorBreakdown: [
      { key: "geo", label: "Geography", score: 80 },
      { key: "conv", label: "Conversion", score: 70 },
    ],
  };
  const b = {
    operatorName: "Operator B",
    displayedOperatorAlignment: 36,
    evidenceConfidence: "Moderate",
    dataCoveragePct: 55,
    factorBreakdown: [
      { key: "geo", label: "Geography", score: 40 },
      { key: "conv", label: "Conversion", score: 30 },
    ],
  };
  const d1 = explainRankingDifference(a, b);
  const d2 = explainRankingDifference(a, b);
  ok("ranking difference deterministic", JSON.stringify(d1) === JSON.stringify(d2));
  ok("ranking difference has drivers", d1.drivers.length >= 1 && d1.drivers.length <= 5);
  ok("ranking difference summary present", Boolean(d1.summary));
}

// Shortlist snapshot immutability
{
  const dir = mkdtempSync(join(tmpdir(), "of-sl-"));
  const path = join(dir, "store.json");
  const entry = createShortlistEntry(
    {
      dealId: "pilot_deal_a",
      operatorId: "recTEST",
      operatorName: "Test Op",
      alignment: 42,
      confidence: "Strong",
      coverage: 60,
      eligibility: "Eligible With Conditions",
      readiness: "Ranking Ready",
      lifecycle: "Research Stage",
      reasons: ["Active country: Argentina"],
      concerns: [],
      unknowns: ["Fees"],
      engineVersion: "operator-fit-v2.1.0",
    },
    path
  );
  const snapAlign = entry.snapshot.alignment;
  removeShortlistEntry(entry.id, { removedBy: "test", reason: "pilot", path });
  const listed = listShortlistForDeal("pilot_deal_a", { path });
  ok("shortlist remove preserves snapshot", listed[0].snapshot.alignment === snapAlign);
  const compared = withCurrentVsSnapshot(listed[0], { alignment: 50, readiness: "Ranking Ready" });
  ok(
    "current vs snapshot does not overwrite",
    compared.snapshot.alignment === snapAlign && compared.currentAlignment === 50
  );
  ok("shortlist not odr marker", listed[0].airtableFields != null);
  rmSync(dir, { recursive: true, force: true });
}

// Comparison
{
  const cmp = buildShortlistComparison([
    { operatorName: "A", alignment: 40, confidence: "Strong", coverage: 60, whyItMatches: ["Geo"] },
    { operatorName: "B", alignment: 30, confidence: "Limited", coverage: 40, whyItMatches: ["Scale"] },
  ]);
  ok("compare max structure", cmp.operators.length === 2 && cmp.pairDrivers.length === 1);
  ok("compare highlights differences", cmp.rows.some((r) => r.highlight));
}

// Validations
{
  const items = listRankingChangeValidations(
    { geography: { country: { value: "Argentina" } } },
    { geography: { marketPresence: [] }, operatingStructures: [], brandsOperated: [], comparables: [] }
  );
  ok("validations non-empty", items.length >= 3);
  ok(
    "validations phased",
    items.every((i) =>
      ["before_shortlist", "before_outreach", "during_outreach", "before_proposal", "before_final"].includes(
        i.phase
      )
    )
  );
}

// Research Stage is not production-eligible unless internal lane allow is set
{
  const blocked = evaluateEligibility(
    { geography: { country: fieldPresent("Argentina") } },
    { activeStatus: fieldPresent("Research Stage"), researchStageAllowed: false }
  );
  ok(
    "research stage hard conflict without allow",
    (blocked.hardConflicts || []).some((x) => /not Active/i.test(x))
  );
  const allowed = evaluateEligibility(
    { allowResearchStageLifecycle: true, geography: { country: fieldPresent("Argentina") } },
    {
      activeStatus: fieldPresent("Research Stage"),
      researchStageAllowed: true,
      geography: { marketPresence: [{ country: "Argentina", presenceType: "Current Managed Property" }] },
      operatingStructures: fieldPresent(["Full third-party management"]),
    }
  );
  ok(
    "research stage allowed in internal lane",
    !(allowed.hardConflicts || []).some((x) => /not Active/i.test(x))
  );
}

ok(
  "founder approvals doc exists",
  existsSync(join(root, "docs/architecture/decisions/operator-fit-internal-pilot-founder-approvals.md"))
);
ok(
  "shortlist architecture distinct from ODR",
  existsSync(join(root, "docs/architecture/operator-fit-shortlist-architecture.md"))
);

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll internal pilot tests passed");
