#!/usr/bin/env node
/**
 * Post-cutover verification: writes hit intelligence base only; MVP gets zero new records.
 */
import "../load-env.js";

process.env.DECISION_OUTCOMES_PERSISTENCE = "airtable";
process.env.DECISION_OUTCOMES_READ_FS_FALLBACK = "0";
process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
process.env.GDI_OPPORTUNITIES_READ_FS_FALLBACK = "0";
process.env.DECISION_OUTCOMES_ALLOW_MVP_BASE = "0";

import Airtable from "airtable";
import {
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  CANONICAL_INTELLIGENCE_BASE_ID,
  getDecisionOutcomesAirtableBaseId,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";
import {
  createDecision,
  recordValidation,
  recordAction,
  recordOutcome,
  PRODUCT_MODULE,
  DECISION_TYPE,
  SUBJECT_TYPE,
  GDI_VALIDATION_TYPE,
  GDI_COMMERCIAL_VALUE,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
} from "../lib/decision-outcomes/index.js";
import {
  findDecisionRecordByDecisionId,
  findEventRecordByEventId,
  extractEventId,
} from "../lib/decision-outcomes/airtable-store.js";
import {
  upsertOpportunity,
  findOpportunityRecordById,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { escapeAirtableFormulaValue } from "../lib/airtable-utils.js";

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const HOTEL = "dec_cutover_verify_hotel";

function check(name, pass, detail) {
  console.log(pass ? "PASS" : "FAIL", name, detail || "");
  return !!pass;
}

async function existsOnBase(baseId, tableName, field, value) {
  const table = new Airtable({ apiKey: token }).base(baseId)(tableName);
  const formula = `{${field}}='${escapeAirtableFormulaValue(value)}'`;
  const rows = await table
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

async function main() {
  let ok = true;
  const resolved = getDecisionOutcomesAirtableBaseId();
  const gdiBase = getGdiOpportunitiesAirtableBaseId();
  ok =
    check(
      "resolved_base",
      resolved === CANONICAL_INTELLIGENCE_BASE_ID,
      resolved
    ) && ok;
  ok =
    check(
      "gdi_base",
      gdiBase === CANONICAL_INTELLIGENCE_BASE_ID,
      gdiBase
    ) && ok;

  // Wrong-base guard
  const prev = process.env.AIRTABLE_INTELLIGENCE_BASE_ID;
  const prevDec = process.env.AIRTABLE_DECISION_OUTCOME_BASE_ID;
  const prevDo = process.env.DECISION_OUTCOMES_AIRTABLE_BASE_ID;
  const prevAdp = process.env.ADP_AIRTABLE_BASE_ID;
  process.env.AIRTABLE_INTELLIGENCE_BASE_ID = "";
  process.env.AIRTABLE_DECISION_OUTCOME_BASE_ID = "";
  process.env.DECISION_OUTCOMES_AIRTABLE_BASE_ID = "";
  process.env.ADP_AIRTABLE_BASE_ID = "";
  process.env.AIRTABLE_BASE_ID = LEGACY_DEAL_CAPTURE_MVP_BASE_ID;
  let guardOk = false;
  try {
    const { assertNotLegacyMvpCanonicalBase, getDecisionOutcomesAirtableBaseId: g } =
      await import("../lib/decision-outcomes/airtable-base.js");
    assertNotLegacyMvpCanonicalBase(g(), { surface: "guard_test" });
  } catch (err) {
    guardOk = err.code === "WRONG_CANONICAL_AIRTABLE_BASE";
  }
  process.env.AIRTABLE_INTELLIGENCE_BASE_ID = prev || CANONICAL_INTELLIGENCE_BASE_ID;
  process.env.AIRTABLE_DECISION_OUTCOME_BASE_ID = prevDec || CANONICAL_INTELLIGENCE_BASE_ID;
  process.env.DECISION_OUTCOMES_AIRTABLE_BASE_ID = prevDo || CANONICAL_INTELLIGENCE_BASE_ID;
  process.env.ADP_AIRTABLE_BASE_ID = prevAdp || "";
  ok = check("mvp_guard", guardOk) && ok;

  const created = await createDecision({
    hotelId: HOTEL,
    productModule: PRODUCT_MODULE.GDI,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
    subjectType: SUBJECT_TYPE.GROUP_OPPORTUNITY,
    subjectId: "opp_cutover_verify_1",
    recommendation: "CUTOVER VERIFY — safe test decision",
    recommendationVersion: 1,
    createdBy: { userId: "cutover_verify", role: "SYSTEM" },
  });
  const decisionId = created.decision.decisionId;
  ok = check("decision_created", !!decisionId, decisionId) && ok;

  const onTarget = await findDecisionRecordByDecisionId(decisionId);
  ok = check("decision_on_target", !!onTarget, onTarget?.id) && ok;

  const onMvp = await existsOnBase(
    LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
    "Decisions",
    "decisionId",
    decisionId
  );
  ok = check("decision_not_on_mvp", !onMvp, onMvp?.id || "absent") && ok;

  const val = await recordValidation(HOTEL, decisionId, {
    validationType: GDI_VALIDATION_TYPE.COMMERCIAL_VALUE,
    validationValue: GDI_COMMERCIAL_VALUE.WORTH_PURSUING_NOW,
    userId: "cutover_verify",
  });
  const act = await recordAction(HOTEL, decisionId, {
    actionType: GDI_ACTION_TYPE.CONTACTED,
    userId: "cutover_verify",
  });
  const out = await recordOutcome(HOTEL, decisionId, {
    outcomeType: GDI_OUTCOME_TYPE.DEFERRED,
    userId: "cutover_verify",
  });

  for (const [label, ev] of [
    ["validation", val.event],
    ["action", act.event],
    ["outcome", out.event],
  ]) {
    const eid = extractEventId(ev);
    const at = await findEventRecordByEventId(eid);
    const mvp = await existsOnBase(
      LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
      "Decision Events",
      "eventId",
      eid
    );
    ok = check(`${label}_on_target`, !!at, eid) && ok;
    ok = check(`${label}_not_on_mvp`, !mvp, "absent") && ok;
  }

  const opp = await upsertOpportunity({
    id: "gdi_opp_cutover_verify_1",
    hotelId: HOTEL,
    title: "CUTOVER VERIFY opportunity",
    priority: "WATCHLIST",
    opportunityType: "PRIMARY_PURSUIT",
    hotelFitScore: 50,
    opportunityQualification: "WEAK",
    decisionId,
  });
  const oppAt = await findOpportunityRecordById("gdi_opp_cutover_verify_1");
  const oppMvp = await existsOnBase(
    LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
    "Group Demand Opportunities",
    "opportunityId",
    "gdi_opp_cutover_verify_1"
  );
  ok = check("opp_on_target", !!oppAt, opp.airtableRecordId) && ok;
  ok = check("opp_not_on_mvp", !oppMvp, "absent") && ok;

  const summary = {
    ok,
    targetBase: CANONICAL_INTELLIGENCE_BASE_ID,
    resolvedBase: getDecisionOutcomesAirtableBaseId(),
    decisionId,
    opportunityId: "gdi_opp_cutover_verify_1",
  };
  console.log("\n" + (ok ? "CUTOVER VERIFIED" : "CUTOVER FAILED"));
  console.log(JSON.stringify(summary, null, 2));
  process.exit(ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
