#!/usr/bin/env node
/**
 * $0 tests for authorized LDI live gate + commercial viability precheck.
 */
import assert from "assert";
import {
  assertLeisureAuthorizedLive,
  preflightLeisureAuthorizedLive,
  GDI_LDI_LIVE_ENV,
  GDI_LEISURE_AUTHORIZED_LIVE_V1,
} from "../lib/group-demand-intelligence/leisure-demand-intelligence/gdi-leisure-authorized-live-v1.js";
import {
  evaluateCommercialViabilityPrecheck,
  batchCommercialViabilityPrecheck,
  COMMERCIAL_VIABILITY_DECISION,
  GDI_COMMERCIAL_VIABILITY_PRECHECK_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-commercial-viability-precheck-v1.js";

let passed = 0;
function pass(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

const prev = process.env[GDI_LDI_LIVE_ENV];
delete process.env[GDI_LDI_LIVE_ENV];

{
  assert.equal(GDI_LEISURE_AUTHORIZED_LIVE_V1, "gdi-leisure-authorized-live-v1");
  const dry = assertLeisureAuthorizedLive({ researchMode: "fixture" });
  assert.equal(dry.ok, true);
  assert.equal(dry.live, false);
  pass("fixture_default_ok");
}

{
  const blocked = assertLeisureAuthorizedLive({
    researchMode: "live",
    authorizePaidResearch: true,
    allowLivePilot: true,
    authorizationId: "FOUNDER_GO_TEST",
    hardCeilingUsd: 13,
    budgetUsd: 2,
  });
  assert.equal(blocked.ok, false);
  assert.ok(blocked.errors.includes("GDI_LDI_LIVE_ENV_NOT_SET"));
  pass("live_blocked_without_env");
}

{
  const blocked = assertLeisureAuthorizedLive({
    researchMode: "live",
    authorizePaidResearch: true,
    allowLivePilot: true,
    // missing authorizationId
    hardCeilingUsd: 13,
    budgetUsd: 2,
    skipEnvGate: true,
  });
  assert.equal(blocked.ok, false);
  assert.ok(blocked.errors.includes("MISSING_AUTHORIZATION_ID"));
  pass("live_blocked_without_auth_id");
}

{
  const pf = preflightLeisureAuthorizedLive({
    researchMode: "live",
    authorizePaidResearch: true,
    allowLivePilot: true,
    authorizationId: "FOUNDER_GO_TEST_PREFLIGHT",
    hardCeilingUsd: 5,
    budgetUsd: 1,
    skipEnvGate: true,
  });
  assert.equal(pf.paid, false);
  assert.equal(pf.spentUsd, 0);
  assert.equal(pf.parallelEngineProhibited, true);
  pass("preflight_zero_dollar");
}

{
  assert.equal(
    GDI_COMMERCIAL_VIABILITY_PRECHECK_V1,
    "gdi-commercial-viability-precheck-v1",
  );
  const hist = evaluateCommercialViabilityPrecheck({
    id: "past",
    title: "2024 Summit",
    pastClosed: true,
    futureDemandPathway: false,
    organizationName: "Example Assoc",
  });
  assert.equal(
    hist.decision,
    COMMERCIAL_VIABILITY_DECISION.SKIP_PAID_CONTACT_RESEARCH,
  );
  assert.equal(hist.preserveWhoHow, true);
  pass("historical_without_future_skips_paid_who_how");
}

{
  const emerging = evaluateCommercialViabilityPrecheck({
    id: "emerging",
    title: "Regional training program",
    organizationName: "Acme Corp",
    evidenceUrls: ["https://example.com/program"],
    whyThisHotel: "Downtown San Diego fit for crew lodging",
    hotelOpportunityThesis: "Bayside walkable for training cohorts",
    futureDemandPathway: true,
    accountDevelopmentReactivated: true,
  });
  // May HOLD or PROCEED depending on FQO evidence/fit helpers — must not invent classifier
  assert.ok(emerging.competingClassifierProhibited);
  assert.ok(emerging.decision);
  pass("viability_reuses_fqo_not_competing_classifier");
}

{
  const batch = batchCommercialViabilityPrecheck([
    { id: "a", pastClosed: true, futureDemandPathway: false, title: "Old" },
    {
      id: "b",
      title: "Future",
      organizationName: "Org",
      evidenceUrls: ["https://example.com/x"],
      whyThisHotel: "fit thesis for hotel",
      hotelOpportunityThesis: "commercial fit",
      futureDemandPathway: true,
    },
  ]);
  assert.equal(batch.count, 2);
  assert.ok(batch.counts);
  pass("batch_viability");
}

if (prev != null) process.env[GDI_LDI_LIVE_ENV] = prev;
else delete process.env[GDI_LDI_LIVE_ENV];

console.log(`\n${passed} PASS — gdi-leisure-authorized-live + viability ($0)`);
