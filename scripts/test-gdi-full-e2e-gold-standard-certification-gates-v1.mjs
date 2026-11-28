#!/usr/bin/env node
/**
 * Gold-standard Full E2E certification gates + Findymail cumulative hard-cap.
 *   node scripts/test-gdi-full-e2e-gold-standard-certification-gates-v1.mjs
 */
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  readHotelCompetitiveSet,
  classifyCadCertification,
  classifyLdiCertification,
  assessMaterialInventoryImprovementRestage,
  evaluateGoldStandardCertification,
  GDI_FULL_E2E_GOLD_STANDARD_CERTIFICATION_GATES_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-gold-standard-certification-gates-v1.js";
import { runCanonicalCadOptInStage } from "../lib/group-demand-intelligence/workflow/gdi-canonical-cad-opt-in-stage-v1.js";
import {
  sumFindymailCreditsDebitedForHotel,
  assertFindymailHotelHardCapAllowsDebit,
} from "../lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import {
  countContactChannelImprovements,
  buildLiveExecutiveSummaryReport,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-v3-orchestrator-v1.js";
import fs from "node:fs";
import os from "node:os";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const MEMMO = "rec1t35e3sPerzxYu";
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

check("contract_id", () => {
  assert.equal(
    GDI_FULL_E2E_GOLD_STANDARD_CERTIFICATION_GATES_V1,
    "gdi-full-e2e-gold-standard-certification-gates-v1",
  );
});

check("memmo_competitive_set_absent", () => {
  const set = readHotelCompetitiveSet(MEMMO, { repoRoot: ROOT });
  assert.equal(set.ok, true);
  assert.equal(set.configured, false);
  assert.equal(set.peerCount, 0);
});

check("cad_classify_without_set_fails_certification", () => {
  const cad = classifyCadCertification(
    { status: "CLASSIFIED", classifiedCount: 11, ok: true },
    { configured: false },
  );
  assert.equal(cad.ok, false);
  assert.equal(cad.reason, "NO_COMPETITIVE_SET_CONFIGURED");
});

check("cad_stage_blocks_when_requireCompetitiveSet", () => {
  const stage = runCanonicalCadOptInStage({
    requireCompetitiveSet: true,
    opportunities: [{ id: "disc_x", title: "Test", organizationName: "Acme" }],
  });
  assert.equal(stage.status, "BLOCKED_NO_COMPETITIVE_SET");
  assert.equal(stage.ok, false);
  assert.equal(stage.paidLiveCompetitorResearch, false);
});

check("cad_stage_classify_marks_not_live", () => {
  const stage = runCanonicalCadOptInStage({
    opportunities: [{ id: "disc_x", title: "Test", organizationName: "Acme" }],
  });
  assert.equal(stage.status, "CLASSIFIED");
  assert.equal(stage.paidLiveCompetitorResearch, false);
  assert.equal(stage.providerMode, "CLASSIFY_EXISTING_ROWS");
});

check("ldi_mock_cannot_pass_live_gate", () => {
  const ldi = classifyLdiCertification({
    status: "STANDARD_STAGE_COMPLETE",
    researchMode: "mock_live",
    paidCalls: 0,
    specializedLeisureExecution: false,
  });
  assert.equal(ldi.ok, false);
  assert.equal(ldi.reason, "MOCK_OR_FIXTURE_LDI_NOT_LIVE");
});

check("material_improvement_restage_requires_auth", () => {
  const blocked = assessMaterialInventoryImprovementRestage({
    newFinalizedQualifiedCount: 0,
    retainedQualifiedCount: 11,
    newVerifiedEmails: 7,
    authorizeMaterialImprovementRestage: false,
  });
  assert.equal(blocked.mayRestage, false);
  assert.match(blocked.reason, /BLOCKED_UNTIL_RESTAGE_AUTH/);
  const ok = assessMaterialInventoryImprovementRestage({
    newFinalizedQualifiedCount: 0,
    retainedQualifiedCount: 11,
    newVerifiedEmails: 7,
    authorizeMaterialImprovementRestage: true,
  });
  assert.equal(ok.mayRestage, true);
});

check("findymail_cumulative_hard_cap_across_auth_ids", () => {
  const ledger = [
    {
      hotelId: MEMMO,
      authorizationId: "AUTH_A",
      at: "2026-11-28T01:00:00.000Z",
      personKey: "a|org",
      operation: "FIND_FROM_NAME",
      creditsDebited: 10,
    },
    {
      hotelId: MEMMO,
      authorizationId: "AUTH_B_RECOVERY",
      at: "2026-11-28T02:00:00.000Z",
      personKey: "b|org",
      operation: "FIND_FROM_NAME",
      creditsDebited: 6,
    },
  ];
  const sum = sumFindymailCreditsDebitedForHotel(MEMMO, ledger);
  assert.equal(sum.creditsDebited, 16);
  const budget = { used: 0, hardCap: 15 };
  const gate = assertFindymailHotelHardCapAllowsDebit({
    hotelId: MEMMO,
    creditHardCap: 15,
    estimatedCredits: 1,
    ledger,
    findymailCreditBudget: budget,
  });
  assert.equal(gate.ok, false);
  assert.equal(gate.reason, "CREDIT_HARD_CAP_REACHED");
  assert.equal(gate.used, 16);
  assert.ok(budget.used >= 16);
});

check("findymail_supplemental_certification_backup_ledger", () => {
  const sum = sumFindymailCreditsDebitedForHotel(MEMMO, [], {
    repoRoot: ROOT,
    supplementalLedgerPaths: [
      "reports/gdi-memmo-bethesda-final-authorization-v1/findymail-usage-ledger-v1.CERTIFICATION_BACKUP.jsonl",
    ],
  });
  assert.equal(sum.creditsDebited, 16);
  assert.ok(sum.byAuthorizationId["FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127"] >= 15);
});

check("evaluate_memmo_prior_run_is_partial_or_blocked", () => {
  const result = evaluateGoldStandardCertification({
    hotelId: MEMMO,
    repoRoot: ROOT,
    cadReport: { status: "CLASSIFIED", classifiedCount: 11, ok: true },
    ldiReport: { status: "STANDARD_STAGE_COMPLETE", researchMode: "mock_live", paidCalls: 0 },
    findymailCreditsDebited: 16,
    findymailHardCap: 15,
    restageInput: {
      newFinalizedQualifiedCount: 0,
      retainedQualifiedCount: 11,
      newVerifiedEmails: 7,
      authorizeMaterialImprovementRestage: false,
    },
  });
  assert.equal(result.certified, false);
  assert.ok(["PARTIAL", "BLOCKED"].includes(result.classification));
  assert.ok(result.failedGateIds.length + result.blockedGateIds.length > 0);
});

check("contact_channel_improvement_counts", () => {
  const c = countContactChannelImprovements({
    results: [
      { email: "a@b.com" },
      { channels: { phone: "+351", linkedin: "https://linkedin.com/in/x" } },
    ],
  });
  assert.equal(c.emails, 1);
  assert.equal(c.phones, 1);
  assert.equal(c.linkedin, 1);
});

check("executive_summary_auto_generated_from_run_inventory", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-es-auto-"));
  const report = buildLiveExecutiveSummaryReport({
    hotelId: MEMMO,
    outDir: dir,
    passOpps: [
      {
        id: "disc_test_1",
        opportunityId: "disc_test_1",
        title: "Test Opp",
        organizationName: "Acme",
        demandSegment: "Corporate",
      },
    ],
    stagedProgression: {
      publicationVersion: "test_es_auto_v1",
      currentUnchanged: true,
    },
    hotelName: "Memmo Príncipe Real",
  });
  assert.equal(report.autoGenerated, true);
  assert.equal(report.outcome, "PASS");
  assert.equal(report.opportunityCount, 1);
  assert.ok(fs.existsSync(path.join(dir, "08-executive-summary.json")));
});

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll gold-standard gate checks passed.");
