#!/usr/bin/env node
/**
 * Permanent regression — portfolio Qualified Opportunity directive V1.
 *   npm run test:gdi-portfolio-qualified-opportunities-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";
import {
  evaluatePortfolioQualifiedOpportunityFullE2eGate,
  FQE2E_REQUIRED_STEPS,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-qualified-opportunity-full-e2e-gate-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
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

const appHtml = fs.readFileSync(path.join(ROOT, "public/app.html"), "utf8");
const appJs = fs.readFileSync(path.join(ROOT, "public/app.js"), "utf8");
const gdiApp = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);

check("research_tab_removed_from_local_shell", () => {
  assert.match(appHtml, /Research browsing removed from local presentation/);
  assert.doesNotMatch(
    appHtml,
    /data-gdi-shell-inventory="research"/,
  );
  assert.match(appJs, /Local presentation: STAGED \/ Published only/);
});

check("no_view_research_cta_in_gdi_app", () => {
  assert.doesNotMatch(gdiApp, /VIEW RESEARCH/);
  assert.doesNotMatch(gdiApp, /data-gdi-recovery="view-research"/);
});

check("fqo_six_gates_contract_present", () => {
  const contract = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js",
    ),
    "utf8",
  );
  assert.match(contract, /EVIDENCE_BACKED_DEMAND/);
  assert.match(contract, /HOTEL_GEO_COMMERCIAL_FIT/);
  assert.match(contract, /FUTURE_SOURCING_PATHWAY/);
  assert.match(contract, /NAMED_COMMERCIAL_PERSON/);
  assert.match(contract, /PERSON_SPECIFIC_CHANNEL/);
  assert.match(contract, /HOTEL_SPECIFIC_ACTION/);
  assert.match(contract, /salesReadyRequired: false/);
});

check("design_hotels_with_staged_are_selectable", () => {
  const hotels = listGdiSelectableHotels();
  const ids = new Set(hotels.map((h) => h.hotelId));
  for (const id of [
    "rece0NdV9FCnaFlVS",
    "recaokE8psIbuVeuH",
    "rec1t35e3sPerzxYu",
    "recLuxvwwxID7U2B8",
  ]) {
    assert.ok(ids.has(id), `missing selectable ${id}`);
  }
});

check("full_e2e_gate_requires_every_opportunity_disposition", () => {
  assert.equal(FQE2E_REQUIRED_STEPS.length, 10);
  const incomplete = evaluatePortfolioQualifiedOpportunityFullE2eGate({
    hotelId: "rec35fExUxCClpOP6",
    opportunityIds: ["a", "b"],
    dispositions: [{ opportunityId: "a", disposition: "PASS" }],
    stepsCompleted: FQE2E_REQUIRED_STEPS,
    stagedCandidatePrepared: true,
  });
  assert.equal(incomplete.complete, false);
  assert.equal(incomplete.incompleteReason, "OPPORTUNITIES_MISSING_DISPOSITION");

  const complete = evaluatePortfolioQualifiedOpportunityFullE2eGate({
    hotelId: "rec35fExUxCClpOP6",
    opportunityIds: ["a", "b"],
    dispositions: [
      { opportunityId: "a", disposition: "PASS" },
      { opportunityId: "b", disposition: "HOLD" },
    ],
    stepsCompleted: FQE2E_REQUIRED_STEPS,
    stagedCandidatePrepared: true,
    stagedPointerActivated: false,
  });
  assert.equal(complete.complete, true);
});

check("generic_inbox_does_not_qualify", () => {
  const evaled = evaluateFinalizedQualifiedOpportunity({
    id: "x",
    organizationName: "Example Association",
    title: "Annual Meeting 2027",
    sourceUrls: ["https://example.org/event"],
    hotelOpportunityThesis:
      "This hotel is a strong fit for the association housing block in 2027 based on location and meeting space.",
    recommendedNextStep: "Contact the housing chair to introduce the hotel.",
    eventStartDate: "2027-06-01",
    primaryContact: {
      name: "Jane Buyer",
      email: "info@example.org",
    },
  });
  assert.equal(evaled.finalizedQualified, false);
});

check("reconcile_script_registered", () => {
  const raw = fs.readFileSync(path.join(ROOT, "package.json"), "utf8");
  assert.match(raw, /gdi:portfolio-qualified-opportunities-reconcile-v1/);
  assert.match(raw, /test:gdi-portfolio-qualified-opportunities-v1/);
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "scripts/gdi-portfolio-qualified-opportunities-reconcile-v1.mjs"),
    ),
  );
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-portfolio-qualified-opportunities-v1");
