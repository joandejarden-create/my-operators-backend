#!/usr/bin/env node
/**
 * Prove dry-run cannot invoke paid Surfe or production writes.
 * Injected spies throw on forbidden calls — no real paid requests.
 */
import assert from "node:assert/strict";
import {
  enrichBuyerContactForOpportunity,
  runBuyerContactEnrichmentPass,
  resolveEffectiveSurfeAuthorization,
} from "../lib/group-demand-intelligence/workflow/buyer-contact-enrichment-v1.js";
import { invokeWeeklyDownstreamWorkflow } from "../lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js";
import { CHANNEL_OWNERSHIP } from "../lib/group-demand-intelligence/workflow/buyer-contact-enrichment-contract-v1.js";

function forbiddenSurfe() {
  const err = new Error("FORBIDDEN_SURFE_CALL");
  err.code = "FORBIDDEN_SURFE_CALL";
  throw err;
}

function forbiddenCanonicalWrite() {
  const err = new Error("FORBIDDEN_CANONICAL_WRITE");
  err.code = "FORBIDDEN_CANONICAL_WRITE";
  throw err;
}

const namedGapOpp = {
  id: "gdi_dry_isolation_1",
  hotelId: "recDryIsoHotel",
  organizationName: "Dry Isolation Org",
  title: "Annual Meeting 2027",
  researchSegment: "local_meetings_catering",
  primaryContact: {
    name: "Jane Meetings",
    role: "Meetings Manager",
    email: null,
  },
  organizationContactEmail: "info@dryiso.example",
  functionalContactEmail: "info@dryiso.example",
  officialDomain: "dryiso.example",
  officialSource: "https://dryiso.example/staff",
};

// A. Effective auth: dryRun wins over authorizeSurfe
{
  const a = resolveEffectiveSurfeAuthorization({
    dryRun: true,
    authorizeSurfe: true,
  });
  assert.equal(a.authorizeSurfeEffective, false);
  assert.equal(a.reason, "dry_run_blocks_paid_surfe");
  const b = resolveEffectiveSurfeAuthorization({
    dryRun: false,
    authorizeSurfe: false,
  });
  assert.equal(b.authorizeSurfeEffective, false);
  const c = resolveEffectiveSurfeAuthorization({
    dryRun: false,
    authorizeSurfe: true,
  });
  assert.equal(c.authorizeSurfeEffective, true);
  console.log("PASS effective_surfe_auth");
}

// B. Direct enrich: dryRun + authorizeSurfe must not call Surfe or write
{
  const result = await enrichBuyerContactForOpportunity(namedGapOpp, {
    hotelId: namedGapOpp.hotelId,
    authorizeSurfe: true,
    dryRun: true,
    persistCanonical: true,
    surfeCaller: forbiddenSurfe,
    canonicalWriter: forbiddenCanonicalWrite,
  });
  assert.ok(
    !result.surfe || result.surfe.status === "skipped",
    "surfe must be skipped"
  );
  assert.match(
    String(result.surfe?.reason || ""),
    /dry_run|not_authorized|surfe_not_authorized|org_inbox/i
  );
  console.log("PASS direct_enrich_dry_run");
}

// C. Live authorizeSurfe:false must not call Surfe
{
  const result = await enrichBuyerContactForOpportunity(namedGapOpp, {
    hotelId: namedGapOpp.hotelId,
    authorizeSurfe: false,
    dryRun: false,
    persistCanonical: false,
    surfeCaller: forbiddenSurfe,
    canonicalWriter: forbiddenCanonicalWrite,
  });
  assert.ok(!result.surfe || result.surfe.status === "skipped");
  console.log("PASS live_surfe_not_authorized");
}

// D. Pass-level dry-run + restored authorizeSurfe from saved state
{
  const pass = await runBuyerContactEnrichmentPass({
    hotelId: namedGapOpp.hotelId,
    opportunities: [namedGapOpp],
    authorizeSurfe: false,
    dryRun: true,
    restoredAuthorization: { authorizeSurfe: true, dryRun: false },
    persistCanonical: true,
    surfeCaller: forbiddenSurfe,
    canonicalWriter: forbiddenCanonicalWrite,
    maxItems: 4,
  });
  // dryRun on the pass wins over restored authorizeSurfe
  assert.equal(pass.dryRun, true);
  assert.equal(pass.authorizeSurfeEffective, false);
  for (const r of pass.results || []) {
    assert.ok(!r.surfe || r.surfe.status !== "accepted");
  }
  console.log("PASS pass_dry_run_beats_restored_auth");
}

// E. Weekly downstream: dryRun + authorizeSurfe must not pay
{
  const out = await invokeWeeklyDownstreamWorkflow({
    hotelId: "rec0eTEnCqlsGJJt9",
    opportunities: [namedGapOpp],
    changeResults: [],
    dispositions: [],
    authorizePublish: true,
    dryRun: true,
    authorizeSurfe: true,
    executeBuyerContactEnrichment: true,
    workflowRunId: "test_weekly_dry_isolation",
  });
  const buyerStep = (out.chain || []).find(
    (s) => s.step === "buyer_contact_enrichment"
  );
  assert.ok(buyerStep, "buyer step present");
  assert.equal(buyerStep.dryRun, true);
  assert.match(String(buyerStep.mode || ""), /dry_run/);
  console.log("PASS weekly_dry_run_isolation");
}

console.log("OK gdi-dry-run-paid-isolation-v1");
