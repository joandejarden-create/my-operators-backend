/**
 * Gates for HE 5.0 Full E2E pilot engine (no live paid calls, no Airtable writes).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HE50_GLOBAL_USD_CAP,
  HE50_BETHESDA_HPC_ID,
  HE50_TABS,
  APPLY_GATE,
  FIELD_CLASS,
  createHe50PilotBudgetLedger,
  BETHESDA_FIELD_GAP_MATRIX,
  summarizeFieldGapMatrix,
  runHe50PilotSupervisor,
  buildHe50DryRunMutationPackage,
  SEVEN_HOTEL_FREE_IDENTITY_PACKETS,
  FREE_RESEARCH_SPEND_USD,
  summarizeReconciliation,
  MUTATION_REC,
  EIGHT_HOTEL_COMPLETENESS,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

assert.equal(HE50_GLOBAL_USD_CAP, 50);
assert.equal(HE50_BETHESDA_HPC_ID, "recLuxvwwxID7U2B8");
assert.equal(HE50_TABS.length, 12);
assert.equal(APPLY_GATE.applyAuthorized, false);
assert.equal(APPLY_GATE.airtableWritesAllowed, false);
assert.equal(APPLY_GATE.surfeAutomatic, false);
assert.equal(FREE_RESEARCH_SPEND_USD, 0);

// Budget: unknown cost refused; over-cap refused; concurrent reserve cannot oversubscribe
const ledger = createHe50PilotBudgetLedger({ globalUsdCap: 50 });
const unk = ledger.reserve({
  operation_id: "op_unknown",
  cost_usd: "UNKNOWN",
  provider: "exa",
  hotel_id: "h1",
});
assert.equal(unk.ok, false);

const r1 = ledger.reserve({
  operation_id: "op_a",
  cost_usd: 30,
  provider: "exa",
  hotel_id: "h1",
  research_category: "ownership_stakeholders",
  allowCategoryReallocation: true,
});
assert.equal(r1.ok, true);
const r2 = ledger.reserve({
  operation_id: "op_b",
  cost_usd: 25,
  provider: "exa",
  hotel_id: "h2",
  research_category: "property_identity_physical",
  allowCategoryReallocation: true,
});
assert.equal(r2.ok, false, "must not exceed shared $50 with reserved headroom");

ledger.settle("op_a", { cost_usd: 12, provider_cost_usd: 12, verified_findings: ["legal_owner"] });
const snap = ledger.snapshot();
assert.equal(snap.confirmedActualUsd, 12);
assert.ok(snap.remainingAuthorizedUsd >= 38);
assert.ok(snap.confirmedActualUsd !== snap.reservedUnreconciledUsd || snap.reservedUnreconciledUsd === 0);

// Idempotent settle
const again = ledger.settle("op_a", { cost_usd: 12, provider_cost_usd: 12 });
assert.ok(again.ok || again.already_settled);

// Field matrix coverage
assert.ok(BETHESDA_FIELD_GAP_MATRIX.length >= 40);
const classes = new Set(BETHESDA_FIELD_GAP_MATRIX.map((r) => r.classification));
assert.ok(classes.has(FIELD_CLASS.EXISTING_VERIFIED));
assert.ok(classes.has(FIELD_CLASS.RESEARCH_REQUIRED) || classes.has(FIELD_CLASS.UNRESOLVED_AFTER_RESEARCH));
const summary = summarizeFieldGapMatrix();
assert.equal(summary.tabsCovered, 12);

const tabIds = new Set(BETHESDA_FIELD_GAP_MATRIX.map((r) => r.tabId));
for (const t of HE50_TABS) {
  assert.ok(tabIds.has(t.id), `missing tab coverage: ${t.id}`);
}

// Seven packets + no auto verified holdings
assert.equal(SEVEN_HOTEL_FREE_IDENTITY_PACKETS.length, 7);
for (const p of SEVEN_HOTEL_FREE_IDENTITY_PACKETS) {
  if (p.pilotKey !== "jw_marriott_costa_elena") {
    assert.match(
      String(p.mullenAssociation),
      /PROPOSED|VERIFY/,
      `${p.pilotKey} must not claim verified Mullen holding`
    );
  }
  assert.ok(p.proposedHpcAction);
}

// Dry-run never authorizes apply; no Surfe persistence
const dry = buildHe50DryRunMutationPackage();
assert.equal(dry.applyGate.applyAuthorized, false);
assert.ok(dry.mutationCount >= 5);
assert.ok(dry.blockedOperations.some((b) => /Surfe/i.test(b)));
assert.ok(!JSON.stringify(dry).includes("surfe_email"));

// Supervisor free-first
const result = runHe50PilotSupervisor({ enablePaid: false });
assert.equal(result.budget.confirmedActualUsd, 0);
assert.equal(result.budget.remainingAuthorizedUsd, 50);
assert.equal(result.applyBlocked.applyAuthorized, false);
assert.match(result.stopMessage, /AWAITING SEPARATE FOUNDER AUTHORIZATION/);
assert.equal(result.hotelScorecard.length, 8);
assert.equal(result.aggregate.hotels, 8);

// Founder reconciliation
const recon = summarizeReconciliation();
assert.equal(recon.totalOps, 11);
assert.ok(recon.counts.READY >= 8);
assert.ok(recon.rejectIds.includes("M-UPD-secrets_cap_cana"));
assert.ok(recon.holdIds.includes("M-BETH-REF-STAKEHOLDER"));
assert.equal(recon.conflictResolutions.ac_rooms.resolvedValue, 140);
assert.equal(
  recon.conflictResolutions.secrets_cap_cana.disposition,
  "VERIFIED_DISTINCT_FROM_DREAMS"
);
assert.equal(
  recon.conflictResolutions.bethesda_renovation.disposition,
  "EVENT_LEVEL_DUAL_DATES_NOT_SINGLE_YEAR_CONFLICT"
);
assert.equal(EIGHT_HOTEL_COMPLETENESS.length, 8);

const ac = SEVEN_HOTEL_FREE_IDENTITY_PACKETS.find((p) => p.pilotKey === "ac_hotel_newtown_square");
assert.equal(ac.rooms.value, 140);
assert.equal(ac.rooms.confidence, "HIGH");
assert.equal(ac.rooms.discardedAlt, 146);

const cap = SEVEN_HOTEL_FREE_IDENTITY_PACKETS.find((p) => p.pilotKey === "secrets_cap_cana");
assert.equal(cap.proposedHpcAction, "NO_OP_IDENTITY_VERIFIED_DISTINCT_FROM_DREAMS");

const dry2 = buildHe50DryRunMutationPackage();
assert.ok(dry2.reconciliationSummary);
assert.equal(dry2.reconciliationSummary.REJECT, 1);
assert.match(dry2.stopMessage, /SEPARATE FOUNDER AUTHORIZATION/);
assert.ok(MUTATION_REC.READY);

// Duplicate CREATE prevention: Papagayo is promote not create-duplicate
const pap = dry.mutations.find((m) => m.id.includes("papagayo"));
assert.ok(pap);
assert.equal(pap.action, "STEWARD_PROMOTE_FROM_CENSUS");

// Ownership vs operator: Bethesda findings keep operator Marriott separate from economic owner
const beth = result.bethesdaFreeFindings;
assert.equal(beth.hpcId, HE50_BETHESDA_HPC_ID);
assert.ok(beth.findings.some((f) => f.fieldKey === "yearBuilt" && f.value === 1979));
assert.ok(beth.findings.some((f) => f.fieldKey === "lastRenovation"));

// Docs / workflow reuse
const workflow = fs.readFileSync(
  path.join(root, "docs/ai-build-system/HE_HPC_POPULATION_WORKFLOW_V1.md"),
  "utf8"
);
assert.ok(workflow.includes("DISCOVER → MATCH"));

const runner = fs.readFileSync(
  path.join(root, "scripts/he-5-0-full-e2e-pilot-run-v1.mjs"),
  "utf8"
);
assert.ok(runner.includes("STOP"));
assert.ok(!/--apply/.test(runner) || runner.includes("applyAuthorized: false"));

console.log("test:he-5-0-full-e2e-pilot-v1 PASS");
