#!/usr/bin/env node
/**
 * Prove original Full E2E chain connectivity without paid Exa:
 * Discovery candidate → Deep research (ORS fixture) → Contacts →
 * Qualification → Ten-section presentation → Durable state.
 * No Airtable. No publication. No lead cap in production path.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1,
  CHAIN_STAGES,
  mapCandidateToOpportunity,
  processOpportunityThroughOriginalChain,
  buildThreeHotelFullE2eBudgetManifest,
  executeOriginalFullE2eResearchChain,
} from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  return Promise.resolve()
    .then(fn)
    .then(() => console.log(`PASS ${name}`))
    .catch((err) => {
      failed += 1;
      console.error(`FAIL ${name}: ${err.message}`);
    });
}

await check("chain_stages_cover_full_e2e", () => {
  assert.ok(CHAIN_STAGES.includes("DISCOVERY"));
  assert.ok(CHAIN_STAGES.includes("DEEP_RESEARCH"));
  assert.ok(CHAIN_STAGES.includes("CONTACT_RESEARCH"));
  assert.ok(CHAIN_STAGES.includes("COMMERCIAL_QUALIFICATION"));
  assert.ok(CHAIN_STAGES.includes("SALES_INTELLIGENCE_OUTPUT"));
  assert.ok(CHAIN_STAGES.includes("PERSISTENCE_AND_CONTINUATION"));
});

await check("fourteen_segments_still_canonical", () => {
  assert.equal(listFourteenWorkflowSegmentIds().length, 14);
});

await check("budget_manifest_bounded_and_not_auto_authorized", () => {
  const m = buildThreeHotelFullE2eBudgetManifest({});
  assert.equal(m.authorizationStatus, "NOT_AUTHORIZED");
  assert.equal(m.discovery.cells, 42);
  assert.ok(m.hardCeilingUsd > 0);
  assert.ok(m.discovery.maxUsd > 0);
  assert.ok(m.deepResearch.maxUsd > 0);
  assert.ok(m.stopConditions.includes("hard_ceiling_reached"));
  assert.ok(m.excludes.includes("production_airtable_writes"));
});

await check("single_candidate_full_chain_zero_cost", async () => {
  const mapped = mapCandidateToOpportunity(
    {
      candidateId: "fixture_chain_fil_gdl",
      organizationName: "FIL Guadalajara",
      title: "Feria Internacional del Libro de Guadalajara 2026",
      evidenceUrls: ["https://www.fil.com.mx/viajes/hoteles.asp?ids=1"],
      hasSourceEvidence: true,
    },
    {
      hotelId: "recCAGqkERSXgiku4",
      hotelName: "Sheraton Guadalajara Expo",
      segmentId: "associations_conferences_events",
    }
  );
  const result = await processOpportunityThroughOriginalChain(mapped, {
    authorizePaidResearch: false,
    maxIterations: 2,
  });
  assert.equal(result.paidSpendUsd, 0);
  assert.ok(result.stages.some((s) => s.stage === "DEEP_RESEARCH"));
  assert.ok(result.stages.some((s) => s.stage === "CONTACT_RESEARCH"));
  assert.ok(result.stages.some((s) => s.stage === "COMMERCIAL_QUALIFICATION"));
  assert.ok(result.stages.some((s) => s.stage === "SALES_INTELLIGENCE_OUTPUT"));
  assert.ok((result.presentation.sectionIds || []).length >= 10);
  assert.equal(result.customerPublicationReady, false);
  const stateFile = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels/recCAGqkERSXgiku4/opportunity-research-state",
    "fixture_chain_fil_gdl.json"
  );
  assert.ok(fs.existsSync(stateFile), "durable state written");
  const state = JSON.parse(fs.readFileSync(stateFile, "utf8"));
  assert.equal(state.version, ORIGINAL_FULL_E2E_RESEARCH_CHAIN_V1);
  assert.equal(state.customerPublicationReady, false);
});

await check("execute_chain_with_seeds_no_paid_no_cap_flag", async () => {
  const summary = await executeOriginalFullE2eResearchChain({
    hotelIds: ["recCAGqkERSXgiku4"],
    authorizePaidDiscovery: false,
    authorizePaidResearch: false,
    useFixtureProvider: true,
    allowHistoricalReplay: true,
    persistState: true,
    skipHpc: true,
    seedOpportunities: [
      {
        candidateId: "seed_imef_chain",
        hotelId: "recCAGqkERSXgiku4",
        hotelName: "Sheraton Guadalajara Expo",
        segmentId: "associations_conferences_events",
        organizationName: "IMEF",
        title: "LIV Convención Nacional IMEF 2026",
        evidenceUrls: ["https://convencion.imef.org.mx/hotel-sede/"],
      },
      {
        candidateId: "seed_smne_chain",
        hotelId: "recCAGqkERSXgiku4",
        hotelName: "Sheraton Guadalajara Expo",
        segmentId: "associations_conferences_events",
        organizationName: "SMNE",
        title: "LXVI Congreso Internacional SMNE 2026",
        evidenceUrls: [
          "https://endocrinologia.org.mx/lxvi-congreso-internacional-de-la-sociedad-mexicana-de-nutricion-y-endocrinologia/sede-y-hospedaje/",
        ],
      },
    ],
    // Intentionally NOT setting maxCandidates — production path has no 10–15 cap.
    // For CI speed, skip re-processing huge fixture matrices by using only seeds +
    // allowing empty fixture discovery cells.
    outDir: path.join(
      ROOT,
      "reports/gdi-original-full-e2e-research-chain-v1/test-run"
    ),
  });
  assert.ok(summary.retainedCandidates >= 2);
  assert.ok(summary.processedCount >= 2);
  assert.equal(summary.paidSpendUsd, 0);
  assert.equal(summary.fullE2eCompleteClaimAllowed, false);
  assert.equal(summary.budgetManifest.authorizationStatus, "NOT_AUTHORIZED");
  assert.ok(summary.withPresentation >= 1);
});

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nALL PASS test-gdi-original-full-e2e-research-chain-v1");
