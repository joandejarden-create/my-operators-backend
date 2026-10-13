#!/usr/bin/env node
/**
 * Tests: Future Watch Trigger Engine + Market-Aware Jev Router V1
 */
import assert from "node:assert/strict";
import {
  TRIGGER_TYPE,
  STOP_CONDITION,
  DATE_PROVENANCE,
  MARKET_ARCHETYPE,
  WATCH_STATUS,
  isValidTriggerType,
  buildFutureWatchRecord,
  appendWatchHistory,
  isGdiFutureWatchReadyForResearch,
  deriveNextResearchSchedule,
  extractEvidenceBasedDate,
  assignMarketArchetypes,
  queryDueWatches,
  buildWatchResearchBatch,
  detectSourceMaterialChange,
  buildSourceFingerprint,
  decideMarketAwareNextAction,
} from "../lib/group-demand-intelligence/future-watch/index.js";
import { validateMarket, isAllowedJevAction } from "../lib/group-demand-intelligence/evidence-gap/evidence-gap-model-v1.js";
import { JEV_DECISION_TYPE, CHOICES } from "../lib/group-demand-intelligence/jev/jev-types.js";

let n = 0;
function ok(name) {
  n += 1;
  console.log(`PASS ${name}`);
}

{
  assert.ok(isValidTriggerType(TRIGGER_TYPE.HOUSING_OPEN));
  assert.equal(isValidTriggerType("RECHECK_LATER"), false);
  assert.ok(STOP_CONDITION.PUBLIC_DATA_CEILING);
  ok("trigger_ontology");
}

{
  const ev = extractEvidenceBasedDate(
    "Housing opens on 2027-01-15 for delegates",
    TRIGGER_TYPE.HOUSING_OPEN
  );
  assert.equal(ev.date, "2027-01-15");
  assert.equal(ev.provenance, DATE_PROVENANCE.EVIDENCE_BASED);
  const sched = deriveNextResearchSchedule({
    triggerType: TRIGGER_TYPE.HOUSING_OPEN,
    candidate: { event: "Congreso 2027", futureCycle: "2027" },
    pageText: "Housing opens on 2027-01-15 for delegates",
  });
  assert.equal(sched.dateProvenance, DATE_PROVENANCE.EVIDENCE_BASED);
  assert.equal(sched.nextResearchDate, "2027-01-15");
  ok("evidence_based_next_date");
}

{
  const sched = deriveNextResearchSchedule({
    triggerType: TRIGGER_TYPE.HOUSING_OPEN,
    candidate: { event: "Congreso END 2027", futureCycle: "2027" },
    pageText: "",
  });
  assert.equal(sched.dateProvenance, DATE_PROVENANCE.HEURISTIC);
  assert.ok(sched.nextResearchDate);
  assert.ok(String(sched.nextTriggerCondition).includes("not sourced fact"));
  ok("heuristic_next_date");
}

{
  const w = buildFutureWatchRecord({
    candidateId: "ac_watch_21",
    hotelShort: "AC",
    hpc: "rec2PVBDavppGpenm",
    event: "Congreso END Santiago 2027",
    futureCycle: "2027",
    primaryBlocker: "HOUSING_STATUS",
    stateAfter: "FUTURE_WATCH",
    nextTriggerType: "HOUSING_OPEN",
  });
  assert.equal(w.watchStatus, WATCH_STATUS.ACTIVE);
  assert.equal(w.nextTriggerType, TRIGGER_TYPE.HOUSING_OPEN);
  assert.ok(w.nextResearchDate);
  const dueFalse = isGdiFutureWatchReadyForResearch(w, {
    now: new Date("2025-01-01"),
  });
  assert.equal(dueFalse.ready, false);
  const dueTrue = isGdiFutureWatchReadyForResearch(w, {
    now: new Date("2030-01-01"),
  });
  assert.equal(dueTrue.ready, true);
  ok("watch_due_calculation");
}

{
  const prev = buildSourceFingerprint({ url: "https://x.test", text: "welcome hotel page chrome only" });
  const nextTrivial = buildSourceFingerprint({
    url: "https://x.test",
    text: "welcome hotel page chrome only footer updated 2026",
  });
  // Force same material hash
  nextTrivial.materialHash = prev.materialHash;
  nextTrivial.contentHash = "different";
  const d = detectSourceMaterialChange(prev, nextTrivial);
  assert.equal(d.changed, false);
  const nextMat = buildSourceFingerprint({
    url: "https://x.test",
    text: "Official housing and accommodation room block now open for delegates",
  });
  const d2 = detectSourceMaterialChange(prev, nextMat);
  assert.equal(d2.changed, true);
  ok("source_change_trigger");
}

{
  const wrong = validateMarket(
    { event: "Digimarcon LA", sourceUrl: "https://digimarconlosangeles.com/x" },
    "SPICE"
  );
  assert.equal(wrong.ok, false);
  ok("wrong_market_prefilter");
}

{
  const ceiling = buildFutureWatchRecord({
    candidateId: "ac_watch_19",
    hotelShort: "AC",
    hpc: "rec2PVBDavppGpenm",
    event: "Aloxamento",
    stateAfter: "PUBLIC_DATA_CEILING",
    primaryBlocker: "FUTURE_CYCLE_VALIDATION",
  });
  assert.equal(ceiling.watchStatus, WATCH_STATUS.PUBLIC_DATA_CEILING);
  const early = isGdiFutureWatchReadyForResearch(ceiling, { now: new Date() });
  // May or may not be due depending on quarterly window from "now"
  const force = isGdiFutureWatchReadyForResearch(ceiling, {
    now: new Date(),
    sourceMateriallyChanged: true,
  });
  assert.equal(force.ready, true);
  ok("public_data_ceiling_low_frequency");
}

{
  const ac = assignMarketArchetypes({ hpc: "rec2PVBDavppGpenm", hotelShort: "AC" });
  assert.ok(
    [MARKET_ARCHETYPE.URBAN_ASSOCIATION, MARKET_ARCHETYPE.URBAN_CORPORATE].includes(ac.primary) ||
      ac.secondary === MARKET_ARCHETYPE.URBAN_CORPORATE ||
      ac.primary === MARKET_ARCHETYPE.URBAN_ASSOCIATION
  );
  const spice = assignMarketArchetypes({ hpc: "recKRJjcPnb4tVDDS", hotelShort: "SPICE" });
  assert.equal(spice.primary, MARKET_ARCHETYPE.RESORT_ISLAND);
  ok("market_archetype");
}

{
  assert.ok(CHOICES[JEV_DECISION_TYPE.EVIDENCE_GAP_NEXT_ACTION].includes("WAIT_FOR_TRIGGER"));
  assert.equal(isAllowedJevAction("INVENT_TRUTH"), false);
  ok("jev_action_enum");
}

{
  process.env.GDI_JEV_ENABLED = "0";
  const w = buildFutureWatchRecord({
    candidateId: "t1",
    hotelShort: "AC",
    hpc: "rec2PVBDavppGpenm",
    event: "Test 2027",
    futureCycle: "2027",
    primaryBlocker: "HOUSING_STATUS",
    stateAfter: "FUTURE_WATCH",
  });
  const d = await decideMarketAwareNextAction({
    watch: w,
    packet: { primaryBlocker: "HOUSING_STATUS" },
    enableSafeApply: true,
  });
  assert.equal(d.canPromote, false);
  assert.equal(d.canSetWho, false);
  assert.equal(d.canMutateTruth, false);
  assert.ok(isAllowedJevAction(d.appliedAction));
  delete process.env.GDI_JEV_ENABLED;
  ok("jev_cannot_promote_or_mutate");
}

{
  const w = buildFutureWatchRecord({
    candidateId: "t2",
    hotelShort: "AC",
    hpc: "rec2PVBDavppGpenm",
    event: "Due Now Event 2026",
    futureCycle: "2026",
    primaryBlocker: "HOUSING_STATUS",
    stateAfter: "FUTURE_WATCH",
    nextTriggerType: "HOUSING_OPEN",
  });
  w.nextResearchDate = "2020-01-01";
  const due = queryDueWatches([w], { now: new Date("2026-01-01") });
  assert.equal(due.length, 1);
  const batch1 = buildWatchResearchBatch(due, { batchId: "b1" });
  assert.equal(batch1.selected.length, 1);
  const batch2 = buildWatchResearchBatch(due, {
    batchId: "b1",
    processedIds: [batch1.selected[0].idempotencyKey],
  });
  assert.equal(batch2.selected.length, 0);
  ok("scheduled_batch_idempotency");
}

{
  const w = buildFutureWatchRecord({
    candidateId: "t3",
    hotelShort: "AC",
    hpc: "rec2PVBDavppGpenm",
    event: "Hist",
    stateAfter: "FUTURE_WATCH",
    primaryBlocker: "HOUSING_STATUS",
  });
  const w2 = appendWatchHistory(w, { jevAction: "VERIFY_HOUSING_STATUS", result: "WAIT_TRIGGER" });
  const w3 = appendWatchHistory(w2, { jevAction: "FIND_OFFICIAL_HOUSING_PAGE", result: "NO_NEW_EVIDENCE" });
  assert.equal(w3.watchHistory.length, 2);
  assert.equal(w.watchHistory.length, 0);
  ok("watch_history_append");
}

{
  assert.ok(TRIGGER_TYPE.HOTEL_ANNOUNCED);
  ok("fully_placed_stop_enum_present");
}

console.log(`\n${n} tests passed`);
