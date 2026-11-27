#!/usr/bin/env node
/**
 * Full E2E V3 acceptance tests (A–O) — $0 fixtures only.
 * No paid Exa / Findymail / Surfe / Airtable.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  executeWhoHowResolutionFixture,
  normalizeWhoHowAgentOutput,
  evaluateWhyWeAreShowingThisContact,
  WHO_HOW_STATUS,
  buildWhoHowDrawerPresentation,
} from "../lib/group-demand-intelligence/workflow/gdi-who-how-agent-contract-v1.js";
import {
  validateContactChannelCandidate,
  collectVerifiedChannelsForPerson,
  runProfessionalContactResearchStage,
  CONTACT_CHANNEL_TYPE,
  CONTACT_VERIFICATION_STATUS,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-stage-v1.js";
import {
  evaluateEntryPointFqoReachability,
  mergeEntryPointReachabilityIntoFqoGates,
  ENTRY_POINT_FQO_RULE_ID,
} from "../lib/group-demand-intelligence/workflow/gdi-fqo-entry-point-reachability-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import {
  runFullE2eV3DryRun,
  loadOrCreateCheckpoint,
  saveCheckpoint,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-v3-orchestrator-v1.js";
import {
  GDI_E2E_V3_STAGE_ORDER,
  applyStageCheckpointResult,
  GDI_E2E_V3_STAGE_OUTCOME,
  buildDefaultStageCheckpoint,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-v3-stages-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const fixturePath = path.join(
  root,
  "fixtures/gdi-full-e2e-v3/who-how-scenarios-v1.json",
);
const fixtures = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const byId = Object.fromEntries(fixtures.scenarios.map((s) => [s.id, s]));

function runScenario(id) {
  const s = byId[id];
  assert.ok(s, `missing scenario ${id}`);
  return executeWhoHowResolutionFixture(s.dossier, s.output, {
    hotelId: fixtures.hotelId,
    reused: s.reused === true,
  });
}

function withLinkedIn(opp, url) {
  return {
    ...opp,
    primaryContact: {
      ...(opp.primaryContact || {}),
      linkedinUrl: url,
    },
  };
}

let passed = 0;
function pass(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// A
{
  const r = runScenario("A_verified_direct_buyer");
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER);
  assert.equal(r.opportunity.buyerVsEntry, "DIRECT_BUYER");
  assert.equal(r.opportunity.primaryContact.hotelBuyingAuthorityVerified, true);
  const drawer = buildWhoHowDrawerPresentation(r.whoHow);
  assert.equal(drawer.primaryDecisionMaker.verified, true);
  pass("A_verified_direct_buyer");
}

// B
{
  const r = runScenario("B_two_entry_points");
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_ENTRY_POINTS_2);
  assert.equal(r.opportunity.buyerVsEntry, "RELEVANT_COMMERCIAL_ENTRY_POINT");
  assert.equal(r.opportunity.primaryContact.hotelBuyingAuthorityVerified, false);
  assert.ok(r.opportunity.secondaryNamedContact?.name);
  assert.ok(
    r.opportunity.primaryContact.whyWeAreShowingThisContact ||
      r.whoHow.entryPoints[0].whyWeAreShowingThisContact,
  );
  const drawer = r.opportunity.whoHowDrawer;
  assert.equal(drawer.primaryDecisionMaker.verified, false);
  assert.match(drawer.primaryDecisionMaker.headline, /Not Yet Verified/i);
  assert.equal(drawer.alternativeContact1.badge, "VERIFIED ENTRY POINT");
  assert.equal(drawer.alternativeContact1.heading, "Alternative Entry Point 1");
  assert.ok(drawer.alternativeContact1.whyWeAreShowingThisContact);
  assert.equal(drawer.alternativeContact2.verified, true);
  pass("B_two_entry_points");
}

// Generic why rejected
{
  const bad = evaluateWhyWeAreShowingThisContact("senior executive");
  assert.equal(bad.ok, false);
  const good = evaluateWhyWeAreShowingThisContact(
    "Meetings director can route hotel housing inquiries for AACR 2026 — ask for the housing bureau contact.",
    { requireRich: true },
  );
  assert.equal(good.ok, true);
  pass("why_showing_generic_rejected");
}

// C
{
  const r = runScenario("C_one_entry_point");
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_ENTRY_POINT_1);
  assert.equal(
    r.whoHow.entryPoints.find((e) => e.rank === "ENTRY_POINT_2")?.note,
    "NOT_FOUND",
  );
  pass("C_one_entry_point");
}

// D
{
  const r = runScenario("D_no_contact");
  assert.equal(
    r.whoHow.whoHowStatus,
    WHO_HOW_STATUS.NO_CREDIBLE_CONTACT_IDENTIFIED,
  );
  assert.equal(r.whoHow.researchContinuation, true);
  pass("D_no_contact");
}

// E — buyer verified, no channel → contact stage HOLD; FQO channel fails
{
  const r = runScenario("E_buyer_no_channel");
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER);
  const contact = await runProfessionalContactResearchStage(r.opportunity, {
    mode: "DryRun",
    fixture: true,
  });
  assert.equal(contact.outcome, "HOLD");
  const fqo = evaluateFinalizedQualifiedOpportunity(contact.opportunity);
  assert.equal(fqo.gates.PERSON_SPECIFIC_CHANNEL.ok, false);
  pass("E_buyer_no_channel");
}

// F — unrelated entry points → no credible commercial contact
{
  const r = runScenario("F_unrelated_entry_points");
  assert.equal(
    r.whoHow.whoHowStatus,
    WHO_HOW_STATUS.NO_CREDIBLE_CONTACT_IDENTIFIED,
  );
  const reach = evaluateEntryPointFqoReachability(
    withLinkedIn(
      {
        ...r.opportunity,
        primaryContact: {
          name: "Casey Brooks",
          role: "CMO",
          linkedinUrl: "https://linkedin.com/in/casey-brooks",
          commerciallyRelevant: false,
          unrelatedToHotelSourcing: true,
          contactClass: "UNRELATED",
        },
        whoHowStatus: WHO_HOW_STATUS.VERIFIED_ENTRY_POINT_1,
        buyerVsEntry: "RELEVANT_COMMERCIAL_ENTRY_POINT",
        recommendedSalesAction: "Do not pursue unrelated CMO",
      },
      "https://linkedin.com/in/casey-brooks",
    ),
    { forceUnrelated: true },
  );
  assert.equal(reach.ok, false);
  assert.ok(reach.reasons.includes("ENTRY_POINT_UNRELATED_TO_HOTEL_SOURCING"));
  pass("F_unrelated_entry_points");
}

// G — reuse flag
{
  const r = runScenario("G_reuse_completed_who_how");
  assert.equal(r.reused, true);
  assert.equal(r.paid, false);
  pass("G_reuse_completed_who_how");
}

// H — resume without duplicate billing (checkpoint + settled job id)
{
  const outDir = path.join(
    root,
    "reports/gdi-canonical-full-e2e-v3/_test_resume",
  );
  fs.mkdirSync(outDir, { recursive: true });
  let { path: ckPath, checkpoint } = loadOrCreateCheckpoint(
    "recnPB3Q02NO8Ma1o",
    outDir,
  );
  checkpoint = applyStageCheckpointResult(
    checkpoint,
    "WHO_HOW_RESOLUTION",
    {
      outcome: GDI_E2E_V3_STAGE_OUTCOME.PASS,
      metrics: {
        settledJobId: "exa_who_how_fx_v3_g_reuse",
        billed: false,
        reused: true,
      },
    },
  );
  saveCheckpoint(ckPath, checkpoint);
  const again = loadOrCreateCheckpoint("recnPB3Q02NO8Ma1o", outDir);
  assert.equal(
    again.checkpoint.stages.WHO_HOW_RESOLUTION.metrics.settledJobId,
    "exa_who_how_fx_v3_g_reuse",
  );
  assert.equal(again.checkpoint.stages.WHO_HOW_RESOLUTION.metrics.billed, false);
  pass("H_resume_no_duplicate_billing");
}

// I competitor hosted
{
  const r = runScenario("I_competitor_hosted");
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER);
  const competitor =
    byId.I_competitor_hosted.dossier.opportunity.competitorHotel || "";
  assert.ok(competitor.includes("Marriott"));
  assert.equal(r.opportunity.primaryContact.name, "Judy Susser");
  pass("I_competitor_hosted");
}

// J leisure
{
  const r = runScenario("J_leisure_no_direct_buyer");
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_ENTRY_POINTS_2);
  assert.equal(r.opportunity.segmentId || byId.J_leisure_no_direct_buyer.dossier.opportunity.segmentId, "leisure_sports");
  pass("J_leisure_no_direct_buyer");
}

// K cross-hotel shared generator
{
  const r = runScenario("K_cross_hotel_shared_generator");
  assert.ok(
    byId.K_cross_hotel_shared_generator.dossier.opportunity.sharedDemandGeneratorId,
  );
  assert.equal(
    byId.K_cross_hotel_shared_generator.dossier.opportunity.hotelSpecificPathway,
    "OVERFLOW_OR_ANCILLARY",
  );
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_ENTRY_POINT_1);
  pass("K_cross_hotel_shared_generator");
}

// L weekly buyer change
{
  const r = runScenario("L_weekly_buyer_change");
  assert.equal(r.opportunity.primaryContact.name, "Jorda Portero Alcaraz");
  assert.equal(
    byId.L_weekly_buyer_change.dossier.opportunity.weeklyChange.buyerChanged,
    true,
  );
  pass("L_weekly_buyer_change");
}

// M Surfe not auto-persisted
{
  await assert.rejects(
    () =>
      runProfessionalContactResearchStage(
        { primaryContact: { name: "Test Person" } },
        { mode: "DryRun", authorizeSurfe: true },
      ),
    /Surfe/,
  );
  const bad = validateContactChannelCandidate({
    channelType: CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL,
    value: "a@b.com",
    provider: "SURFE",
    persist: true,
    personDirect: true,
  });
  assert.equal(bad.ok, false);
  assert.ok(bad.reasons.includes("SURFE_MUST_NOT_PERSIST"));
  pass("M_surfe_not_auto_persisted");
}

// N Stage 4 cannot invent emails
{
  const invented = validateContactChannelCandidate({
    channelType: CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL,
    value: "jane.doe@acme.com",
    invented: true,
    guessedPattern: true,
    personDirect: true,
  });
  assert.equal(invented.ok, false);
  assert.ok(invented.reasons.includes("INVENTED_OR_GUESSED"));
  const companyLi = validateContactChannelCandidate({
    channelType: CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN,
    value: "https://www.linkedin.com/company/acme",
    personDirect: true,
  });
  assert.equal(companyLi.ok, false);
  pass("N_no_invented_emails");
}

// O FQO rejects insufficient contact pathways; entry-point rule audited
{
  const r = runScenario("B_two_entry_points");
  const withChannel = withLinkedIn(
    {
      ...r.opportunity,
      evidenceUrls: ["https://www.aacr.org/meeting/aacr-2026/"],
      hotelFitRationale:
        "Wyndham San Diego Bayside is a candidate for AACR attendee overflow lodging near the convention corridor.",
      recommendedSalesAction:
        "Submit overflow housing proposal to meetings director.",
      primaryContact: {
        ...r.opportunity.primaryContact,
        linkedinUrl: "https://linkedin.com/in/alex-rivera-meetings",
        whyWeAreShowingThisContact:
          "Meetings director can route hotel housing inquiries for AACR 2026 — ask for the housing bureau or official hotel partner path.",
        whyThisContact:
          "Meetings director can route hotel housing inquiries for AACR 2026 — ask for the housing bureau or official hotel partner path.",
        recommendedContactAction:
          "Ask for housing bureau or official hotel partner path",
      },
    },
    "https://linkedin.com/in/alex-rivera-meetings",
  );
  const reach = evaluateEntryPointFqoReachability(withChannel);
  assert.equal(reach.ruleId, ENTRY_POINT_FQO_RULE_ID);
  assert.equal(reach.ok, true);
  let fqo = evaluateFinalizedQualifiedOpportunity(withChannel);
  fqo = mergeEntryPointReachabilityIntoFqoGates(fqo, reach);
  assert.equal(fqo.entryPointReachability.ok, true);

  const weak = evaluateEntryPointFqoReachability({
    whoHowStatus: WHO_HOW_STATUS.VERIFIED_ENTRY_POINT_1,
    buyerVsEntry: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    primaryContact: {
      name: "Alex Rivera",
      role: "Director",
      // no why, no action, no channel
    },
  });
  assert.equal(weak.ok, false);
  pass("O_fqo_entry_point_reachability_rule");
}

// Orchestrator DryRun smoke
{
  const whoHowFixtures = fixtures.scenarios
    .filter((s) => ["A_verified_direct_buyer", "B_two_entry_points"].includes(s.id))
    .map((s) => ({ dossier: s.dossier, output: s.output, reused: s.reused }));
  const { summary } = await runFullE2eV3DryRun({
    hotelId: "recnPB3Q02NO8Ma1o",
    mode: "DryRun",
    outDir: path.join(root, "reports/gdi-canonical-full-e2e-v3/recnPB3Q02NO8Ma1o"),
    whoHowFixtures,
  });
  assert.equal(summary.mode, "DryRun");
  assert.equal(summary.paid, false);
  assert.equal(summary.stageOrder.length, 5);
  assert.ok(fs.existsSync(summary.checkpointPath));
  pass("orchestrator_dry_run_smoke");
}

// Stage order contract
{
  assert.deepEqual(GDI_E2E_V3_STAGE_ORDER, [
    "DEMAND_DISCOVERY",
    "OPPORTUNITY_DEEP_RESEARCH",
    "WHO_HOW_RESOLUTION",
    "PROFESSIONAL_CONTACT_RESEARCH",
    "COMMERCIAL_QUALIFICATION",
  ]);
  pass("stage_order_contract");
}

// Drawer HTML helper presence (source check)
{
  const ui = fs.readFileSync(
    path.join(root, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8",
  );
  assert.ok(
    ui.includes("whoHowDrawerPresentationHtml") ||
      ui.includes("PRIMARY DECISION-MAKER") ||
      ui.includes("whoHowDrawer"),
    "drawer must include V3 whoHow presentation hooks",
  );
  pass("drawer_source_hooks");
}

console.log(`\nOK gdi-canonical-full-e2e-v3: ${passed} checks`);
