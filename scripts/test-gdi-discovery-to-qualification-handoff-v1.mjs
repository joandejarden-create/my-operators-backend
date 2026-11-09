#!/usr/bin/env node
/**
 * Regression: provider response → unique demand → disposition → durable state.
 * Also detects duplicated commercial opportunities across live + historical.
 * Zero cost. No Airtable. No publication.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  detectDuplicateCommercialOpportunities,
  classifyPhase2Disposition,
  PHASE2_DISPOSITION,
  reconcileLivePilotFindings,
  loadLivePilotFindings,
} from "../lib/group-demand-intelligence/research/live-pilot-commercial-qualification-phase2-v1.js";
import {
  qualifyDiscoveryCandidates,
  attachQualificationHandoffToTrace,
  mapDiscoveryCandidateToAssessment,
} from "../lib/group-demand-intelligence/research/discovery-to-qualification-handoff-v1.js";
import { DISCOVERY_EXECUTION_STATUS } from "../lib/group-demand-intelligence/research/segment-discovery-executor-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-qual-handoff-"));

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

check("duplicate_detector_flags_cross_source_clusters", () => {
  const scan = detectDuplicateCommercialOpportunities({
    liveFindings: [
      {
        organizationName: "FIL Guadalajara",
        activityTitle: "FIL 2026 lodging",
        demandIdentityKey: "fil guadalajara|fil 2026 lodging",
        candidateId: "live_1",
      },
      {
        organizationName: "FIL Guadalajara",
        activityTitle: "FIL 2026 lodging",
        demandIdentityKey: "fil guadalajara|fil 2026 lodging",
        candidateId: "live_2",
      },
    ],
    inventoryRows: [
      {
        organizationName: "FIL Guadalajara",
        title: "FIL 2026 lodging",
        candidateId: "inv_1",
      },
    ],
    historicalRows: [],
  });
  assert.ok(scan.duplicateClusterCount >= 1);
});

check("classify_never_sales_ready_without_verified_buyer", () => {
  const assessment = {
    organizationName: "AMFECCO",
    activityTitle: "Congreso 2026",
    hasHttpEvidence: true,
    city: "Cancún",
    dates: "2026-05",
    eventStatus: "upcoming",
    urls: ["https://example.org/amfecco"],
    overflowSpeculative: false,
  };
  const contacts = {
    verifiedDirectBuyers: 0,
    verifiedEntryPoints: 0,
    functionalRoutes: [],
  };
  const d = classifyPhase2Disposition(assessment, contacts, null);
  assert.notEqual(d.disposition, PHASE2_DISPOSITION.SALES_READY);
  assert.ok(
    [
      PHASE2_DISPOSITION.ACCOUNT_DEVELOPMENT,
      PHASE2_DISPOSITION.NEEDS_MORE_RESEARCH,
      PHASE2_DISPOSITION.WATCH,
    ].includes(d.disposition)
  );
});

check("handoff_collapses_duplicate_demand_identities", () => {
  const result = qualifyDiscoveryCandidates(
    [
      {
        candidateId: "live_test_handoff_smne_a",
        organizationName: "SMNE",
        title: "Congreso Nacional",
        evidenceUrls: ["https://smne.org/congreso"],
        hasSourceEvidence: true,
      },
      {
        candidateId: "live_test_handoff_smne_b",
        organizationName: "SMNE",
        title: "Congreso Nacional",
        evidenceUrls: ["https://smne.org/congreso-2026"],
        hasSourceEvidence: true,
      },
      {
        candidateId: "live_test_handoff_empty",
        organizationName: "",
        title: "",
        evidenceUrls: [],
        hasSourceEvidence: false,
      },
    ],
    {
      hotelId: "recCAGqkERSXgiku4",
      segmentId: "associations_conferences_events",
    },
    {
      persist: true,
      persistDiscoveryState: false,
      outDir: path.join(tmp, "out"),
    }
  );
  assert.equal(result.reconcileDoc.waterfall.providerFindings, 3);
  assert.equal(result.reconcileDoc.waterfall.duplicateLiveCollapsed, 1);
  assert.equal(result.reconcileDoc.unique.length, 1);
  assert.ok(result.persisted.persistedCandidates >= 1);
  const stateFile = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels/recCAGqkERSXgiku4/opportunity-research-state",
    result.reconcileDoc.unique[0].candidateId + ".json"
  );
  assert.ok(fs.existsSync(stateFile), "durable opportunity-research-state written");
  const state = JSON.parse(fs.readFileSync(stateFile, "utf8"));
  assert.equal(state.customerPublicationReady, false);
  assert.ok(state.disposition);
  // Cleanup test fixture state — do not leave pollution in hotel research dirs.
  try {
    fs.unlinkSync(stateFile);
  } catch {
    /* ignore */
  }
});

check("attach_handoff_marks_qualification_partial_not_complete", () => {
  const out = attachQualificationHandoffToTrace(
    {
      hotelId: "recTk5I8VOzsPhxDz",
      segmentId: "corporate_accounts_scaling",
      researchRunId: "test_run",
      executionStatus: DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED,
      candidates: [
        {
          candidateId: "live_x",
          organizationName: "Test Co",
          title: "Plant expansion Cancun",
          evidenceUrls: ["https://example.com/news"],
          hasSourceEvidence: true,
        },
      ],
    },
    { persist: false, persistDiscoveryState: false }
  );
  assert.equal(out.commercialResearchComplete, false);
  assert.equal(
    out.executionStatus,
    DISCOVERY_EXECUTION_STATUS.QUALIFICATION_PARTIAL
  );
  assert.ok(out.qualificationHandoff?.uniqueCount >= 1);
  assert.equal(out.qualificationHandoff.salesReady, 0);
});

check("map_assessment_preserves_evidence_urls", () => {
  const a = mapDiscoveryCandidateToAssessment(
    {
      candidateId: "z",
      organizationName: "IMEF",
      title: "Convencion",
      evidenceUrls: ["https://imef.org.mx/evento"],
      hasSourceEvidence: true,
    },
    { hotelId: "recCAGqkERSXgiku4", segmentId: "associations_conferences_events" }
  );
  assert.equal(a.hasHttpEvidence, true);
  assert.ok(a.urls.includes("https://imef.org.mx/evento"));
});

check("live_pilot_reconcile_loads_without_spend", () => {
  const rawDir = path.join(
    ROOT,
    "reports/gdi-live-discovery-pilot-v1-executed/raw-exa"
  );
  if (!fs.existsSync(rawDir)) {
    console.log("SKIP live_pilot_reconcile (raw-exa missing)");
    return;
  }
  const findings = loadLivePilotFindings();
  assert.equal(findings.length, 41);
  const doc = reconcileLivePilotFindings(findings);
  assert.equal(doc.waterfall.providerFindings, 41);
  assert.ok(doc.waterfall.uniqueDemandActivities <= 41);
  assert.equal(doc.dispositionCounts.SALES_READY || 0, 0);
  assert.equal(doc.paidSpendUsd, 0);
  assert.equal(doc.contactSummary.verifiedDirectBuyers, 0);
});

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nALL PASS test-gdi-discovery-to-qualification-handoff-v1");
