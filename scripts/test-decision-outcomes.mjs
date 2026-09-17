#!/usr/bin/env node
/**
 * Decision & Outcome regression suite â€” canonical layer + GDI/ADP bridges.
 * Forces filesystem persistence so unit tests stay offline.
 */

process.env.DECISION_OUTCOMES_PERSISTENCE = "filesystem";
process.env.DECISION_OUTCOMES_FS_MIRROR = "0";

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  PRODUCT_MODULE,
  DECISION_TYPE,
  SUBJECT_TYPE,
  GDI_VALIDATION_TYPE,
  GDI_FAMILIARITY,
  GDI_COMMERCIAL_VALUE,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
  ADP_VALIDATION_VALUE,
  ADP_ACTION_TYPE,
  ADP_OUTCOME_TYPE,
  CAUSAL_CONFIDENCE,
  createDecision,
  getDecision,
  recordValidation,
  recordAction,
  recordOutcome,
  getDecisionTimeline,
  ensureGdiOpportunityDecision,
  ensureAdpFindingDecision,
  ingestGdiAuthFeedback,
  suggestAdpMeasurementOutcome,
  getHotelDecisionMetrics,
  getDecisionOutcomesRoot,
  listDecisionSummaries,
  loadEvents,
} from "../lib/decision-outcomes/index.js";
import { assertHotelBoundary } from "../lib/decision-outcomes/store.js";
import {
  assertNotLegacyMvpCanonicalBase,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE_HOTEL = "dec_outcome_fixture_hotel";
let failed = 0;

async function check(name, fn) {
  try {
    await fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

function wipeFixtureHotel() {
  const dir = path.join(getDecisionOutcomesRoot(), "hotels", FIXTURE_HOTEL);
  if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
}

wipeFixtureHotel();

await check("0_reject_legacy_mvp_canonical_base", async () => {
  assert.throws(
    () =>
      assertNotLegacyMvpCanonicalBase(LEGACY_DEAL_CAPTURE_MVP_BASE_ID, {
        surface: "test",
      }),
    /WRONG_CANONICAL_AIRTABLE_BASE/
  );
});

await check("1_decision_idempotency", async () => {
  const a = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    subjectId: "opp_lifecycle_a",
    recommendation: "Contact housing provider",
    recommendationVersion: 1,
  });
  const b = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    subjectId: "opp_lifecycle_a",
    recommendation: "Contact housing provider (same)",
    recommendationVersion: 1,
  });
  assert.equal(a.created, true);
  assert.equal(b.created, false);
  assert.equal(a.decision.decisionId, b.decision.decisionId);
});

await check("1b_subject_version_reuses_active_decision", async () => {
  const a = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    subjectId: "opp_subject_reuse_a",
    recommendation: "First create",
    recommendationVersion: 1,
  });
  // Simulate missing/mismatched key path by creating with same subject/version again
  const b = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    subjectId: "opp_subject_reuse_a",
    recommendation: "Retry ensure",
    recommendationVersion: 1,
  });
  assert.equal(b.created, false);
  assert.equal(a.decision.decisionId, b.decision.decisionId);
});

await check("2_3_4_append_only_validation_action_outcome", async () => {
  const { decision } = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectId: "opp_lifecycle_full",
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    recommendation: "Pursue now",
  });
  await recordValidation(FIXTURE_HOTEL, decision.decisionId, {
    validationType: GDI_VALIDATION_TYPE.COMMERCIAL_VALUE,
    validationValue: GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW,
  });
  await recordValidation(FIXTURE_HOTEL, decision.decisionId, {
    validationType: GDI_VALIDATION_TYPE.FAMILIARITY,
    validationValue: GDI_FAMILIARITY.NEVER_SEEN,
  });
  await recordAction(FIXTURE_HOTEL, decision.decisionId, {
    actionType: GDI_ACTION_TYPE.CONTACTED,
  });
  await recordAction(FIXTURE_HOTEL, decision.decisionId, {
    actionType: GDI_ACTION_TYPE.RFP_RECEIVED,
  });
  await recordOutcome(FIXTURE_HOTEL, decision.decisionId, {
    outcomeType: GDI_OUTCOME_TYPE.WON,
    roomNights: 120,
  });
  const events = loadEvents(FIXTURE_HOTEL, decision.decisionId);
  assert.equal(events.length, 5);
  assert.equal(events[0].validationValue, GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW);
});

await check("5_current_state_projection", async () => {
  const summaries = listDecisionSummaries(FIXTURE_HOTEL);
  const full = summaries.find((s) => s.subjectId === "opp_lifecycle_full");
  const bundle = await getDecision(FIXTURE_HOTEL, full.decisionId);
  assert.equal(bundle.current.latestOutcome.outcomeType, GDI_OUTCOME_TYPE.WON);
  assert.equal(bundle.current.latestAction.actionType, GDI_ACTION_TYPE.RFP_RECEIVED);
  assert.equal(bundle.current.decisionLifecycleStage, "OUTCOME_RECORDED");
});

await check("6_gdi_ensure_skips_disqualified", async () => {
  const r = await ensureGdiOpportunityDecision({
    hotelId: FIXTURE_HOTEL,
    opportunity: { id: "opp_dq", priority: "DISQUALIFIED", title: "Bad" },
  });
  assert.equal(r.skipped, "disqualified");
});

await check("7_hotel_isolation_boundary", async () => {
  const { decision } = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectId: "opp_iso",
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    recommendation: "x",
  });
  assert.throws(
    () => assertHotelBoundary("other_hotel", decision.hotelId),
    /hotel_boundary/
  );
});

await check("8_adp_finding_links_decision", async () => {
  const r = await ensureAdpFindingDecision({
    hotelId: FIXTURE_HOTEL,
    findingId: "adp_finding_family",
    recommendation: "Improve family/leisure positioning on website",
  });
  assert.equal(r.created, true);
  assert.equal(r.decision.productModule, PRODUCT_MODULE.ADP);
  assert.equal(r.decision.subjectId, "adp_finding_family");
});

await check("9_recommendation_version_preserved", async () => {
  const v1 = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.ADP,
    decisionType: DECISION_TYPE.POSITIONING_ACTION,
    subjectId: "adp_v",
    subjectType: SUBJECT_TYPE.ADP_FINDING,
    recommendation: "v1 text",
    recommendationVersion: 1,
  });
  const v2 = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.ADP,
    decisionType: DECISION_TYPE.POSITIONING_ACTION,
    subjectId: "adp_v",
    subjectType: SUBJECT_TYPE.ADP_FINDING,
    recommendation: "v2 text materially new",
    recommendationVersion: 2,
  });
  assert.equal(v1.created, true);
  assert.equal(v2.created, true);
  assert.notEqual(v1.decision.decisionId, v2.decision.decisionId);
  assert.equal(v1.decision.recommendation, "v1 text");
});

await check("10_evidence_snapshot_preserved", async () => {
  const { decision } = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectId: "opp_ev",
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    recommendation: "x",
    evidenceSnapshotSummary: { title: "Frozen Event", frozenAt: "2026-01-01" },
    confidenceMethodologyVersion: "gdi_evidence_confidence_v1",
  });
  const loaded = await getDecision(FIXTURE_HOTEL, decision.decisionId);
  assert.equal(loaded.decision.evidenceSnapshotSummary.title, "Frozen Event");
  assert.equal(
    loaded.decision.confidenceMethodologyVersion,
    "gdi_evidence_confidence_v1"
  );
});

await check("11_12_outcome_does_not_overwrite_validation", async () => {
  const { decision } = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectId: "opp_sep",
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    recommendation: "x",
  });
  await recordValidation(FIXTURE_HOTEL, decision.decisionId, {
    validationType: GDI_VALIDATION_TYPE.COMMERCIAL_VALUE,
    validationValue: GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW,
  });
  await recordOutcome(FIXTURE_HOTEL, decision.decisionId, {
    outcomeType: GDI_OUTCOME_TYPE.LOST,
  });
  const bundle = await getDecision(FIXTURE_HOTEL, decision.decisionId);
  assert.equal(
    bundle.current.latestValidationsByType.COMMERCIAL_VALUE.validationValue,
    GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW
  );
  assert.equal(bundle.current.latestOutcome.outcomeType, GDI_OUTCOME_TYPE.LOST);
});

await check("13_action_does_not_imply_outcome", async () => {
  const { decision } = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectId: "opp_act_only",
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    recommendation: "x",
  });
  await recordAction(FIXTURE_HOTEL, decision.decisionId, {
    actionType: GDI_ACTION_TYPE.CONTACTED,
  });
  const bundle = await getDecision(FIXTURE_HOTEL, decision.decisionId);
  assert.ok(bundle.current.latestAction);
  assert.equal(bundle.current.latestOutcome, null);
  assert.equal(bundle.current.decisionLifecycleStage, "ACTION_TAKEN");
});

await check("14_adp_metric_movement_no_causation", async () => {
  await ensureAdpFindingDecision({
    hotelId: FIXTURE_HOTEL,
    findingId: "adp_metric",
    recommendation: "Improve presence",
  });
  const { suggestion, written } = await suggestAdpMeasurementOutcome({
    hotelId: FIXTURE_HOTEL,
    findingId: "adp_metric",
    beforeMetric: 10,
    afterMetric: 18,
    metricName: "presence_index",
    write: true,
  });
  assert.equal(suggestion.causalConfidence, CAUSAL_CONFIDENCE.UNKNOWN);
  assert.equal(suggestion.temporalAssociation, "movement_observed_after_action");
  assert.equal(written.event.causalConfidence, CAUSAL_CONFIDENCE.UNKNOWN);
  assert.match(written.event.outcomeNote, /does not prove/i);
});

await check("15_cross_hotel_private_outcomes_never_leak_via_boundary", async () => {
  const m = await getHotelDecisionMetrics(FIXTURE_HOTEL);
  assert.equal(m.hotelId, FIXTURE_HOTEL);
  assert.ok(m.decisionCount >= 1);
});

await check("16_gdi_ingest_auth_feedback", async () => {
  const r = await ingestGdiAuthFeedback({
    hotelId: FIXTURE_HOTEL,
    opportunityId: "opp_known",
    opportunity: {
      id: "opp_known",
      title: "Known lead",
      priority: "MEDIUM_PRIORITY",
      recommendedAction: "Re-engage",
    },
    feedback: {
      familiarity: "Familiar",
      commercialStatus: "Worth Pursuing",
      salesOutcome: "Contacted",
      comment: "Good lead",
    },
    actor: { userId: "tester@dealality.com", role: "DOS" },
  });
  assert.ok(r.decision);
  assert.ok(r.events.length >= 2);
});

await check("17_18_repeated_ensure_no_duplicate", async () => {
  const a = await ensureAdpFindingDecision({
    hotelId: FIXTURE_HOTEL,
    findingId: "adp_dup",
    recommendation: "Same",
    recommendationVersion: 1,
  });
  const b = await ensureAdpFindingDecision({
    hotelId: FIXTURE_HOTEL,
    findingId: "adp_dup",
    recommendation: "Same",
    recommendationVersion: 1,
  });
  assert.equal(a.created, true);
  assert.equal(b.created, false);
  assert.equal(a.decision.decisionId, b.decision.decisionId);
});

await check("fixture_b_known_lead_lost", async () => {
  const { decision } = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectId: "fixture_b",
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    recommendation: "Pursue known lead",
  });
  await recordValidation(FIXTURE_HOTEL, decision.decisionId, {
    validationType: GDI_VALIDATION_TYPE.FAMILIARITY,
    validationValue: GDI_FAMILIARITY.ALREADY_KNOWN,
  });
  await recordValidation(FIXTURE_HOTEL, decision.decisionId, {
    validationType: GDI_VALIDATION_TYPE.COMMERCIAL_VALUE,
    validationValue: GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW,
  });
  await recordAction(FIXTURE_HOTEL, decision.decisionId, {
    actionType: GDI_ACTION_TYPE.CONTACTED,
  });
  await recordOutcome(FIXTURE_HOTEL, decision.decisionId, {
    outcomeType: GDI_OUTCOME_TYPE.LOST,
    outcomeReason: "RATE",
  });
  const t = await getDecisionTimeline(FIXTURE_HOTEL, decision.decisionId);
  assert.equal(t.timeline.length, 4);
});

await check("fixture_c_not_relevant", async () => {
  const { decision } = await createDecision({
    hotelId: FIXTURE_HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectId: "fixture_c",
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    recommendation: "Skip",
  });
  await recordValidation(FIXTURE_HOTEL, decision.decisionId, {
    validationType: GDI_VALIDATION_TYPE.FAMILIARITY,
    validationValue: GDI_FAMILIARITY.NOT_RELEVANT,
  });
  await recordAction(FIXTURE_HOTEL, decision.decisionId, {
    actionType: GDI_ACTION_TYPE.NO_ACTION,
  });
  const bundle = await getDecision(FIXTURE_HOTEL, decision.decisionId);
  assert.ok(["VALIDATED", "CLOSED"].includes(bundle.current.decisionLifecycleStage));
});

await check("fixture_d_adp_improved", async () => {
  await ensureAdpFindingDecision({
    hotelId: FIXTURE_HOTEL,
    findingId: "fixture_d",
    recommendation: "Update website family content",
  });
  const bundle = await getDecision(
    FIXTURE_HOTEL,
    listDecisionSummaries(FIXTURE_HOTEL).find((s) => s.subjectId === "fixture_d")
      .decisionId
  );
  await recordValidation(FIXTURE_HOTEL, bundle.decision.decisionId, {
    validationType: "HOTEL_RESPONSE",
    validationValue: ADP_VALIDATION_VALUE.AGREE,
  });
  await recordAction(FIXTURE_HOTEL, bundle.decision.decisionId, {
    actionType: ADP_ACTION_TYPE.WEBSITE_CONTENT_UPDATED,
  });
  await suggestAdpMeasurementOutcome({
    hotelId: FIXTURE_HOTEL,
    findingId: "fixture_d",
    beforeMetric: 5,
    afterMetric: 12,
    write: true,
  });
  const after = await getDecision(FIXTURE_HOTEL, bundle.decision.decisionId);
  assert.equal(
    after.current.latestOutcome.outcomeType,
    ADP_OUTCOME_TYPE.PRESENCE_IMPROVED
  );
});

await check("fixture_e_adp_disagree", async () => {
  await ensureAdpFindingDecision({
    hotelId: FIXTURE_HOTEL,
    findingId: "fixture_e",
    recommendation: "Change OTA copy",
  });
  const id = listDecisionSummaries(FIXTURE_HOTEL).find(
    (s) => s.subjectId === "fixture_e"
  ).decisionId;
  await recordValidation(FIXTURE_HOTEL, id, {
    validationType: "HOTEL_RESPONSE",
    validationValue: ADP_VALIDATION_VALUE.DISAGREE,
  });
  await recordAction(FIXTURE_HOTEL, id, {
    actionType: ADP_ACTION_TYPE.NO_ACTION,
  });
});

await check("fixture_f_adp_too_early", async () => {
  await ensureAdpFindingDecision({
    hotelId: FIXTURE_HOTEL,
    findingId: "fixture_f",
    recommendation: "Add structured data",
  });
  await suggestAdpMeasurementOutcome({
    hotelId: FIXTURE_HOTEL,
    findingId: "fixture_f",
    tooEarly: true,
    write: true,
  });
  const id = listDecisionSummaries(FIXTURE_HOTEL).find(
    (s) => s.subjectId === "fixture_f"
  ).decisionId;
  const bundle = await getDecision(FIXTURE_HOTEL, id);
  assert.equal(
    bundle.current.latestOutcome.outcomeType,
    ADP_OUTCOME_TYPE.TOO_EARLY_TO_MEASURE
  );
});

await check("gdi_added_to_sourced_properties_progression", async () => {
  wipeFixtureHotel();
  const usageDir = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    FIXTURE_HOTEL
  );
  if (fs.existsSync(usageDir)) fs.rmSync(usageDir, { recursive: true, force: true });
  const {
    createDeterministicEventId,
    deriveCommercialFunnelStage,
    projectCommercialProgression,
    labelGdiOutcomeType,
    GDI_COMMERCIAL_FUNNEL_STAGE,
  } = await import("../lib/decision-outcomes/index.js");
  const { upsertCustomerUsageObservation } =
    await import("../lib/group-demand-intelligence/customer-usage-observations.js");

  assert.equal(
    labelGdiOutcomeType(GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES),
    "Added to sourced properties"
  );
  assert.ok(GDI_OUTCOME_TYPE.WON);
  assert.ok(GDI_OUTCOME_TYPE.LOST);

  const ensured = await ensureGdiOpportunityDecision({
    hotelId: FIXTURE_HOTEL,
    opportunity: {
      id: "gdi_opp_nice_fixture",
      title: "18th Annual NICE Conference fixture",
      priority: "MEDIUM_PRIORITY",
      opportunityType: "PRIMARY_PURSUIT",
      recommendedAction: "Contact NICE program office",
      evidenceConfidence: 70,
    },
  });
  const decisionId = ensured.decision.decisionId;
  const evidenceKey = "test_francesca_email_2026-09-17";
  const actionEventId = createDeterministicEventId({
    prefix: "act",
    hotelId: FIXTURE_HOTEL,
    decisionId,
    eventKind: "ACTION",
    subtype: GDI_ACTION_TYPE.CONTACTED,
    eventDate: "2026-09-17",
    evidenceKey,
  });
  const outcomeEventId = createDeterministicEventId({
    prefix: "out",
    hotelId: FIXTURE_HOTEL,
    decisionId,
    eventKind: "OUTCOME",
    subtype: GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES,
    eventDate: "2026-09-17",
    evidenceKey,
  });

  assert.equal(
    loadEvents(FIXTURE_HOTEL, decisionId).length,
    0,
    "fresh decision must start with zero events"
  );

  const a1 = await recordAction(FIXTURE_HOTEL, decisionId, {
    actionEventId,
    actionType: GDI_ACTION_TYPE.CONTACTED,
    actionDate: "2026-09-17T12:00:00.000Z",
    sourceSurface: "CUSTOMER_REPORTED",
  });
  const a2 = await recordAction(FIXTURE_HOTEL, decisionId, {
    actionEventId,
    actionType: GDI_ACTION_TYPE.CONTACTED,
    actionDate: "2026-09-17T12:00:00.000Z",
    sourceSurface: "CUSTOMER_REPORTED",
  });
  assert.equal(a1.created, true);
  assert.equal(a2.created, false);

  const o1 = await recordOutcome(FIXTURE_HOTEL, decisionId, {
    outcomeEventId,
    outcomeType: GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES,
    outcomeDate: "2026-09-17T12:00:00.000Z",
    causalConfidence: CAUSAL_CONFIDENCE.UNKNOWN,
    sourceSurface: "CUSTOMER_REPORTED",
  });
  const o2 = await recordOutcome(FIXTURE_HOTEL, decisionId, {
    outcomeEventId,
    outcomeType: GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES,
    outcomeDate: "2026-09-17T12:00:00.000Z",
    causalConfidence: CAUSAL_CONFIDENCE.UNKNOWN,
    sourceSurface: "CUSTOMER_REPORTED",
  });
  assert.equal(o1.created, true);
  assert.equal(o2.created, false);

  const bundle = await getDecision(FIXTURE_HOTEL, decisionId);
  assert.equal(bundle.decision.hotelId, FIXTURE_HOTEL);
  assert.equal(
    bundle.current.latestAction.actionType,
    GDI_ACTION_TYPE.CONTACTED
  );
  assert.equal(
    bundle.current.latestOutcome.outcomeType,
    GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES
  );
  assert.equal(
    deriveCommercialFunnelStage(bundle.events),
    GDI_COMMERCIAL_FUNNEL_STAGE.ADDED_TO_SOURCING
  );
  assert.equal(
    bundle.current.commercialProgression.currentStatusLabel,
    "Added to sourced properties"
  );
  assert.equal(
    bundle.current.commercialProgression.compactStatusLabel,
    "Added to sourcing"
  );
  assert.equal(
    projectCommercialProgression(bundle.decision, bundle.events)
      .latestActionLabel,
    "Hotel contacted opportunity"
  );

  const events = loadEvents(FIXTURE_HOTEL, decisionId);
  assert.equal(
    events.filter((e) => e.actionType === GDI_ACTION_TYPE.CONTACTED).length,
    1
  );
  assert.equal(
    events.filter(
      (e) => e.outcomeType === GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES
    ).length,
    1
  );

  assert.throws(
    () => assertHotelBoundary("other_hotel", FIXTURE_HOTEL),
    /hotel_boundary_violation/
  );

  const usage1 = upsertCustomerUsageObservation(FIXTURE_HOTEL, {
    kind: "OUTREACH_SUMMARY",
    observationDate: "2026-09-17",
    evidenceKey: "test_21_of_29",
    opportunitiesSurfaced: 29,
    opportunitiesContacted: 21,
    sourceActor: "Francesca Moore",
  });
  const usage2 = upsertCustomerUsageObservation(FIXTURE_HOTEL, {
    kind: "OUTREACH_SUMMARY",
    observationDate: "2026-09-17",
    evidenceKey: "test_21_of_29",
    opportunitiesSurfaced: 29,
    opportunitiesContacted: 21,
    sourceActor: "Francesca Moore",
  });
  assert.equal(usage1.created, true);
  assert.equal(usage2.created, false);
  assert.equal(usage1.observation.opportunitiesContacted, 21);

  const metrics = await getHotelDecisionMetrics(FIXTURE_HOTEL, {
    productModule: PRODUCT_MODULE.GDI,
  });
  assert.ok(metrics.commercialFunnel);
  assert.ok(metrics.commercialFunnel.addedToSourcing >= 1);
  assert.ok(metrics.commercialFunnel.contacted >= 1);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nAll Decision & Outcome regressions passed.");
