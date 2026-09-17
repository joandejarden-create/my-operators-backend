#!/usr/bin/env node
/**
 * Live Airtable read-back verification for Decision & Outcome.
 * Creates clearly marked test records, reads them back, then leaves them
 * (hotelId prefix dec_at_verify_*) for optional cleanup.
 *
 * Usage: node scripts/verify-decision-outcomes-airtable.mjs
 */
import "../load-env.js";

process.env.DECISION_OUTCOMES_PERSISTENCE = "airtable";
process.env.DECISION_OUTCOMES_FS_MIRROR = "1";
process.env.DECISION_OUTCOMES_FS_FALLBACK = "0";
process.env.DECISION_OUTCOMES_READ_FS_FALLBACK = "0";

import {
  PRODUCT_MODULE,
  DECISION_TYPE,
  SUBJECT_TYPE,
  GDI_VALIDATION_TYPE,
  GDI_COMMERCIAL_VALUE,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
  ADP_VALIDATION_VALUE,
  ADP_ACTION_TYPE,
  createDecision,
  getDecision,
  recordValidation,
  recordAction,
  recordOutcome,
  ensureAdpFindingDecision,
  recordAdpHotelResponse,
  recordAdpAction,
  getPersistenceMode,
  getDecisionOutcomesAirtableBaseId,
} from "../lib/decision-outcomes/index.js";
import {
  findDecisionRecordByDecisionId,
  findEventRecordByEventId,
  extractEventId,
} from "../lib/decision-outcomes/airtable-store.js";

const HOTEL = "dec_at_verify_hotel";
const results = { ok: true, checks: [], baseId: getDecisionOutcomesAirtableBaseId() };

function check(name, pass, detail) {
  results.checks.push({ name, pass: !!pass, detail: detail || null });
  if (!pass) results.ok = false;
  console.log(pass ? "PASS" : "FAIL", name, detail || "");
}

async function main() {
  console.log("persistence", getPersistenceMode(), "base", results.baseId);

  const gdi = await createDecision({
    hotelId: HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    subjectId: "opp_at_verify_1",
    recommendation: "VERIFY Airtable GDI decision — safe test",
    recommendationVersion: 1,
    createdBy: { userId: "verify_script", role: "SYSTEM" },
  });
  check("gdi_decision_created", gdi.created && gdi.decision?.decisionId, gdi.decision?.decisionId);

  const gdiAt = await findDecisionRecordByDecisionId(gdi.decision.decisionId);
  check("gdi_decision_airtable_readback", !!gdiAt, gdiAt?.id);
  check(
    "gdi_decision_fields",
    gdiAt?.fields?.hotelId === HOTEL &&
      gdiAt?.fields?.productModule === "GDI" &&
      gdiAt?.fields?.subjectId === "opp_at_verify_1",
    JSON.stringify({
      hotelId: gdiAt?.fields?.hotelId,
      productModule: gdiAt?.fields?.productModule,
      subjectId: gdiAt?.fields?.subjectId,
    })
  );

  const val = await recordValidation(HOTEL, gdi.decision.decisionId, {
    validationType: GDI_VALIDATION_TYPE.COMMERCIAL_VALUE,
    validationValue: GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW,
    userId: "verify_script",
    sourceSurface: "verify_script",
  });
  const valId = extractEventId(val.event);
  const valAt = await findEventRecordByEventId(valId);
  check("gdi_validation_airtable", valAt?.fields?.eventType === "VALIDATION", valId);

  const act = await recordAction(HOTEL, gdi.decision.decisionId, {
    actionType: GDI_ACTION_TYPE.CONTACTED,
    userId: "verify_script",
    sourceSurface: "verify_script",
  });
  const actId = extractEventId(act.event);
  const actAt = await findEventRecordByEventId(actId);
  check("gdi_action_airtable", actAt?.fields?.eventType === "ACTION", actId);

  const out = await recordOutcome(HOTEL, gdi.decision.decisionId, {
    outcomeType: GDI_OUTCOME_TYPE.LOST,
    outcomeReason: "RATE",
    userId: "verify_script",
    sourceSurface: "verify_script",
  });
  const outId = extractEventId(out.event);
  const outAt = await findEventRecordByEventId(outId);
  check("gdi_outcome_airtable", outAt?.fields?.eventType === "OUTCOME", outId);

  const bundle = await getDecision(HOTEL, gdi.decision.decisionId);
  check(
    "gdi_projection",
    bundle?.current?.latestAction?.actionType === GDI_ACTION_TYPE.CONTACTED &&
      bundle?.current?.latestOutcome?.outcomeType === GDI_OUTCOME_TYPE.LOST &&
      bundle?.current?.decisionLifecycleStage === "OUTCOME_RECORDED",
    bundle?.current?.decisionLifecycleStage
  );

  // Idempotent event retry
  const again = await recordAction(HOTEL, gdi.decision.decisionId, {
    actionType: GDI_ACTION_TYPE.CONTACTED,
    userId: "verify_script",
  });
  // New action creates new eventId — append-only. Test eventId dedupe via appendEvent path:
  const { appendEvent } = await import("../lib/decision-outcomes/airtable-store.js");
  const dup = await appendEvent(HOTEL, gdi.decision.decisionId, act.event);
  check("event_id_idempotent", dup.created === false, "duplicate eventId rejected");

  const adp = await ensureAdpFindingDecision({
    hotelId: HOTEL,
    findingId: "adp_at_verify_1",
    recommendation: "VERIFY Airtable ADP decision — safe test",
  });
  check("adp_decision_created", !!adp.decision?.decisionId, adp.decision?.decisionId);
  const adpAt = await findDecisionRecordByDecisionId(adp.decision.decisionId);
  check("adp_decision_airtable", adpAt?.fields?.productModule === "ADP", adpAt?.id);

  await recordAdpHotelResponse({
    hotelId: HOTEL,
    findingId: "adp_at_verify_1",
    validationValue: ADP_VALIDATION_VALUE.AGREE,
    actor: { userId: "verify_script" },
  });
  await recordAdpAction({
    hotelId: HOTEL,
    findingId: "adp_at_verify_1",
    actionType: ADP_ACTION_TYPE.WEBSITE_CONTENT_UPDATED,
    actor: { userId: "verify_script" },
  });
  const adpBundle = await getDecision(HOTEL, adp.decision.decisionId);
  check(
    "adp_validation_action",
    adpBundle?.current?.validationCount >= 1 && adpBundle?.current?.actionCount >= 1,
    JSON.stringify({
      v: adpBundle?.current?.validationCount,
      a: adpBundle?.current?.actionCount,
    })
  );

  // Same shared tables
  check(
    "shared_tables",
    gdiAt && adpAt && gdi.decision.decisionId !== adp.decision.decisionId,
    "GDI+ADP in Decisions table"
  );

  console.log("\n" + (results.ok ? "ALL VERIFIED" : "VERIFICATION FAILED"));
  console.log(JSON.stringify(results, null, 2));
  process.exit(results.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
