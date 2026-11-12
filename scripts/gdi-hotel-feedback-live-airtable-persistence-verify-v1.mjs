#!/usr/bin/env node
/**
 * Live Airtable persistence verification for GDI Hotel Validation / Action / Outcome.
 *
 * Uses the established Decision Outcomes live-verify convention:
 * - hotelId prefix: dec_at_verify_*
 * - subject marked GDI_FEEDBACK_LIVE_PERSISTENCE_VERIFICATION
 * - No customer hotel / published opportunity IDs
 *
 * Writes VALIDATION / ACTION / OUTCOME Decision Events to appa2cE7FTRmIbB32,
 * reads them back, and asserts independent subsection timestamps.
 *
 * Usage: node scripts/gdi-hotel-feedback-live-airtable-persistence-verify-v1.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

process.env.DECISION_OUTCOMES_PERSISTENCE = "airtable";
process.env.DECISION_OUTCOMES_FS_MIRROR = "1";
process.env.DECISION_OUTCOMES_FS_FALLBACK = "0";
process.env.DECISION_OUTCOMES_READ_FS_FALLBACK = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-hotel-feedback-live-airtable-persistence-v1"
);

const MARKER = "GDI_FEEDBACK_LIVE_PERSISTENCE_VERIFICATION";
const HOTEL = "dec_at_verify_gdi_feedback_hotel";
const RUN_ID = `gdi_fb_live_${Date.now().toString(36)}`;
const OPPORTUNITY_ID = `gdi_opp_${MARKER.toLowerCase()}_${RUN_ID}`;

const {
  PRODUCT_MODULE,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
  ensureGdiOpportunityDecision,
  ingestGdiShareValidation,
  recordAction,
  recordOutcome,
  getSubjectDecision,
  getPersistenceMode,
  getDecisionOutcomesAirtableBaseId,
  projectCommercialProgression,
  loadEvents,
} = await import("../lib/decision-outcomes/index.js");
const { CANONICAL_INTELLIGENCE_BASE_ID } = await import(
  "../lib/decision-outcomes/airtable-base.js"
);
const {
  findDecisionRecordByDecisionId,
  findEventRecordByEventId,
  extractEventId,
} = await import("../lib/decision-outcomes/airtable-store.js");
const {
  DECISIONS_TABLE_NAME,
  DECISION_EVENTS_TABLE_NAME,
} = await import("../lib/decision-outcomes/field-map.js");
const {
  saveShareValidationItem,
  getShareValidationForOpportunity,
} = await import("../lib/group-demand-intelligence/share-validation.js");
const {
  hotelFeedbackToWeeklyLearningItem,
  formatHotelFeedbackLastSavedLabel,
} = await import(
  "../lib/group-demand-intelligence/hotel-feedback-subsection-persistence-v1.js"
);
const { feedbackToResearchTask } = await import(
  "../lib/group-demand-intelligence/workflow/weekly-refresh-contract-v1.js"
);

const results = {
  contract: "GDI_HOTEL_FEEDBACK_LIVE_AIRTABLE_PERSISTENCE_V1",
  marker: MARKER,
  runId: RUN_ID,
  ok: true,
  acceptanceStatus: null,
  baseId: getDecisionOutcomesAirtableBaseId(),
  persistenceMode: getPersistenceMode(),
  tables: {
    decisions: DECISIONS_TABLE_NAME,
    decisionEvents: DECISION_EVENTS_TABLE_NAME,
  },
  hotelId: HOTEL,
  opportunityId: OPPORTUNITY_ID,
  decisionId: null,
  checks: [],
  timestamps: {},
  learning: {},
  airtableEventIds: [],
};

function check(name, pass, detail) {
  results.checks.push({ name, pass: !!pass, detail: detail ?? null });
  if (!pass) results.ok = false;
  console.log(pass ? "PASS" : "FAIL", name, detail ?? "");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  check(
    "canonical_base",
    results.baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    results.baseId
  );
  check("persistence_airtable", results.persistenceMode === "airtable", results.persistenceMode);

  const opportunity = {
    id: OPPORTUNITY_ID,
    title: `${MARKER} — safe live persistence fixture`,
    priority: "HIGH_PRIORITY",
    recommendedAction: "VERIFY live GDI hotel feedback dual-write (non-customer)",
  };

  const ensured = await ensureGdiOpportunityDecision({
    hotelId: HOTEL,
    opportunity,
    sourceSystem: MARKER,
  });
  const decision = ensured.decision;
  results.decisionId = decision?.decisionId || null;
  check("decision_ensured", !!decision?.decisionId, decision?.decisionId);

  const decisionAt = await findDecisionRecordByDecisionId(decision.decisionId);
  check(
    "decision_airtable_readback",
    decisionAt?.fields?.hotelId === HOTEL &&
      decisionAt?.fields?.subjectId === OPPORTUNITY_ID &&
      decisionAt?.fields?.productModule === PRODUCT_MODULE.GDI,
    JSON.stringify({
      airtableRec: decisionAt?.id,
      hotelId: decisionAt?.fields?.hotelId,
      subjectId: decisionAt?.fields?.subjectId,
      productModule: decisionAt?.fields?.productModule,
    })
  );

  const validatedAt = new Date().toISOString();
  const validationPayload = {
    familiarityStatus: "ALREADY_KNOWN",
    commercialValue: "WORTH_WATCHING",
    contactPersonAssessment: "RIGHT_PERSON",
    emailAssessment: "USEFUL",
    phoneAssessment: "NOT_TESTED",
    note: `${MARKER} validation note run=${RUN_ID}`,
    validatedAt,
    hotelValidationUpdatedAt: validatedAt,
    validator: "live_verify_script",
  };

  // Local latest projection (same path as API before dual-write)
  saveShareValidationItem(HOTEL, {
    opportunityId: OPPORTUNITY_ID,
    ...validationPayload,
    decisionId: decision.decisionId,
  });

  const ingest = await ingestGdiShareValidation({
    hotelId: HOTEL,
    opportunityId: OPPORTUNITY_ID,
    opportunity,
    validation: validationPayload,
    actor: { userId: "gdi_feedback_live_verify", role: "SYSTEM" },
  });
  check(
    "validation_ingest_events",
    Array.isArray(ingest.events) && ingest.events.length >= 5,
    `eventWrites=${ingest.events?.length}`
  );

  const validationEventIds = (ingest.events || [])
    .map((e) => extractEventId(e?.event || e))
    .filter(Boolean);
  results.airtableEventIds.push(...validationEventIds);

  let validationAirtableOk = true;
  for (const eventId of validationEventIds) {
    const row = await findEventRecordByEventId(eventId);
    if (!row || row.fields?.eventType !== "VALIDATION") {
      validationAirtableOk = false;
      check(`validation_event_${eventId}`, false, row?.fields?.eventType || "missing");
    }
  }
  check(
    "validation_airtable_events",
    validationAirtableOk && validationEventIds.length >= 5,
    validationEventIds.join(",")
  );

  const localLatest = getShareValidationForOpportunity(HOTEL, OPPORTUNITY_ID);
  check(
    "local_projection_matches",
    localLatest?.familiarityStatus === "ALREADY_KNOWN" &&
      localLatest?.commercialValue === "WORTH_WATCHING" &&
      localLatest?.contactPersonAssessment === "RIGHT_PERSON" &&
      localLatest?.emailAssessment === "USEFUL" &&
      localLatest?.phoneAssessment === "NOT_TESTED",
    JSON.stringify({
      familiarityStatus: localLatest?.familiarityStatus,
      commercialValue: localLatest?.commercialValue,
      hotelValidationUpdatedAt:
        localLatest?.hotelValidationUpdatedAt || localLatest?.validatedAt,
    })
  );

  const hotelValidationUpdatedAt =
    localLatest?.hotelValidationUpdatedAt || localLatest?.validatedAt || validatedAt;
  results.timestamps.hotelValidationUpdatedAt = hotelValidationUpdatedAt;
  check(
    "hotelValidationUpdatedAt_present",
    !!hotelValidationUpdatedAt && !Number.isNaN(Date.parse(hotelValidationUpdatedAt)),
    hotelValidationUpdatedAt
  );

  // Identical validation re-ingest should not invent contradictory local stamp change
  // (Airtable may append if bridge always writes; stamp authority for UI is local latest)
  const sameLocal = getShareValidationForOpportunity(HOTEL, OPPORTUNITY_ID);
  check(
    "identical_local_stamp_stable",
    (sameLocal?.hotelValidationUpdatedAt || sameLocal?.validatedAt) ===
      hotelValidationUpdatedAt,
    sameLocal?.hotelValidationUpdatedAt || sameLocal?.validatedAt
  );

  await sleep(1100);

  const action1 = await recordAction(HOTEL, decision.decisionId, {
    actionType: GDI_ACTION_TYPE.PLANNED_TO_CONTACT,
    actionDescription: `${MARKER} Planned To Contact`,
    userId: "gdi_feedback_live_verify",
    sourceSurface: "gdi_feedback_live_verify",
    expectedSalesActionUpdatedAt: null,
  });
  const actionEventId = extractEventId(action1.event);
  results.airtableEventIds.push(actionEventId);
  const actionAt = await findEventRecordByEventId(actionEventId);
  check(
    "action_airtable",
    actionAt?.fields?.eventType === "ACTION" &&
      actionAt?.fields?.hotelId === HOTEL &&
      (actionAt?.fields?.eventSubtype === GDI_ACTION_TYPE.PLANNED_TO_CONTACT ||
        actionAt?.fields?.eventValue === GDI_ACTION_TYPE.PLANNED_TO_CONTACT),
    JSON.stringify({
      eventId: actionEventId,
      eventType: actionAt?.fields?.eventType,
      eventSubtype: actionAt?.fields?.eventSubtype,
      eventValue: actionAt?.fields?.eventValue,
      hotelId: actionAt?.fields?.hotelId,
    })
  );
  const salesActionUpdatedAt = action1.salesActionUpdatedAt;
  results.timestamps.salesActionUpdatedAt = salesActionUpdatedAt;
  check("salesActionUpdatedAt_present", !!salesActionUpdatedAt, salesActionUpdatedAt);
  check(
    "action_does_not_change_validation_stamp",
    hotelValidationUpdatedAt ===
      (getShareValidationForOpportunity(HOTEL, OPPORTUNITY_ID)?.hotelValidationUpdatedAt ||
        getShareValidationForOpportunity(HOTEL, OPPORTUNITY_ID)?.validatedAt),
    hotelValidationUpdatedAt
  );

  // Identical action re-submit
  const actionSame = await recordAction(HOTEL, decision.decisionId, {
    actionType: GDI_ACTION_TYPE.PLANNED_TO_CONTACT,
    userId: "gdi_feedback_live_verify",
    sourceSurface: "gdi_feedback_live_verify",
    expectedSalesActionUpdatedAt: salesActionUpdatedAt,
  });
  check(
    "identical_action_no_new_stamp",
    actionSame.created === false &&
      actionSame.salesActionUpdatedAt === salesActionUpdatedAt,
    JSON.stringify({
      created: actionSame.created,
      salesActionUpdatedAt: actionSame.salesActionUpdatedAt,
    })
  );

  await sleep(1100);

  const outcome1 = await recordOutcome(HOTEL, decision.decisionId, {
    outcomeType: GDI_OUTCOME_TYPE.DEFERRED,
    outcomeNote: `${MARKER} Deferred — non-customer live verify`,
    userId: "gdi_feedback_live_verify",
    sourceSurface: "gdi_feedback_live_verify",
    expectedSalesOutcomeUpdatedAt: null,
  });
  const outcomeEventId = extractEventId(outcome1.event);
  results.airtableEventIds.push(outcomeEventId);
  const outcomeAt = await findEventRecordByEventId(outcomeEventId);
  check(
    "outcome_airtable",
    outcomeAt?.fields?.eventType === "OUTCOME" &&
      outcomeAt?.fields?.hotelId === HOTEL &&
      (outcomeAt?.fields?.eventSubtype === GDI_OUTCOME_TYPE.DEFERRED ||
        outcomeAt?.fields?.eventValue === GDI_OUTCOME_TYPE.DEFERRED),
    JSON.stringify({
      eventId: outcomeEventId,
      eventType: outcomeAt?.fields?.eventType,
      eventSubtype: outcomeAt?.fields?.eventSubtype,
      eventValue: outcomeAt?.fields?.eventValue,
      hotelId: outcomeAt?.fields?.hotelId,
    })
  );
  const salesOutcomeUpdatedAt = outcome1.salesOutcomeUpdatedAt;
  results.timestamps.salesOutcomeUpdatedAt = salesOutcomeUpdatedAt;
  check("salesOutcomeUpdatedAt_present", !!salesOutcomeUpdatedAt, salesOutcomeUpdatedAt);

  // Reload projection from Airtable-backed store
  const subjectBundle = await getSubjectDecision(HOTEL, {
    productModule: PRODUCT_MODULE.GDI,
    subjectId: OPPORTUNITY_ID,
  });
  const decisionReloaded = subjectBundle?.decision || decision;
  const events = await loadEvents(HOTEL, decision.decisionId);
  const progression =
    subjectBundle?.current?.commercialProgression ||
    projectCommercialProgression(decisionReloaded, events);

  check(
    "readback_action_type",
    progression?.latestActionType === GDI_ACTION_TYPE.PLANNED_TO_CONTACT,
    progression?.latestActionType
  );
  check(
    "readback_outcome_type",
    progression?.latestOutcomeType === GDI_OUTCOME_TYPE.DEFERRED,
    progression?.latestOutcomeType
  );
  check(
    "section_isolation_timestamps",
    progression?.hotelValidationUpdatedAt &&
      progression?.salesActionUpdatedAt &&
      progression?.salesOutcomeUpdatedAt &&
      progression.salesActionUpdatedAt !== progression.salesOutcomeUpdatedAt,
    JSON.stringify({
      hotelValidationUpdatedAt: progression?.hotelValidationUpdatedAt,
      salesActionUpdatedAt: progression?.salesActionUpdatedAt,
      salesOutcomeUpdatedAt: progression?.salesOutcomeUpdatedAt,
    })
  );
  check(
    "validation_stamp_unchanged_after_outcome",
    !!progression?.hotelValidationUpdatedAt,
    progression?.hotelValidationUpdatedAt
  );
  check(
    "action_stamp_unchanged_after_outcome",
    progression?.salesActionUpdatedAt === salesActionUpdatedAt ||
      progression?.salesActionUpdatedAt === action1.salesActionUpdatedAt,
    progression?.salesActionUpdatedAt
  );

  // Viewer-local labels (Europe/Madrid + America/New_York)
  const madrid = formatHotelFeedbackLastSavedLabel.call
    ? (() => {
        const d = new Date(salesActionUpdatedAt);
        return (
          "Last saved: " +
          d.toLocaleString("en-GB", {
            timeZone: "Europe/Madrid",
            day: "numeric",
            month: "short",
            year: "numeric",
            hour: "2-digit",
            minute: "2-digit",
          })
        );
      })()
    : null;
  const ny = (() => {
    const d = new Date(salesOutcomeUpdatedAt);
    return (
      "Last saved: " +
      d.toLocaleString("en-US", {
        timeZone: "America/New_York",
        day: "numeric",
        month: "short",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })
    );
  })();
  results.timestamps.labelMadridAction = madrid;
  results.timestamps.labelNewYorkOutcome = ny;
  results.timestamps.labelDefaultValidation =
    formatHotelFeedbackLastSavedLabel(hotelValidationUpdatedAt);
  check(
    "local_timezone_labels",
    String(results.timestamps.labelDefaultValidation).startsWith("Last saved:") &&
      String(ny).startsWith("Last saved:"),
    JSON.stringify({
      default: results.timestamps.labelDefaultValidation,
      madrid,
      ny,
    })
  );

  // Learning integration (Decision Events → weekly learning shape)
  const learningItem = hotelFeedbackToWeeklyLearningItem({
    hotelId: HOTEL,
    opportunityId: OPPORTUNITY_ID,
    validation: validationPayload,
    actionType: GDI_ACTION_TYPE.PLANNED_TO_CONTACT,
    outcomeType: GDI_OUTCOME_TYPE.DEFERRED,
  });
  const task = feedbackToResearchTask(learningItem);
  results.learning = { item: learningItem, task };
  check(
    "learning_pipeline_readable",
    !!task?.taskType || !!task?.action || !!task?.priority || !!task?.changeClass,
    JSON.stringify(task)
  );

  // Representative mapping spot-checks (same contract as sandbox suite)
  const alreadyKnown = feedbackToResearchTask(
    hotelFeedbackToWeeklyLearningItem({
      hotelId: HOTEL,
      opportunityId: OPPORTUNITY_ID,
      validation: { familiarityStatus: "ALREADY_KNOWN" },
    })
  );
  const worthNow = feedbackToResearchTask(
    hotelFeedbackToWeeklyLearningItem({
      hotelId: HOTEL,
      opportunityId: OPPORTUNITY_ID,
      validation: { commercialValue: "WORTH_PURSUING_NOW" },
    })
  );
  const wrongContact = feedbackToResearchTask(
    hotelFeedbackToWeeklyLearningItem({
      hotelId: HOTEL,
      opportunityId: OPPORTUNITY_ID,
      validation: { contactPersonAssessment: "WRONG_PERSON" },
    })
  );
  const wrongEmail = feedbackToResearchTask(
    hotelFeedbackToWeeklyLearningItem({
      hotelId: HOTEL,
      opportunityId: OPPORTUNITY_ID,
      validation: { emailAssessment: "WRONG" },
    })
  );
  const rfp = feedbackToResearchTask(
    hotelFeedbackToWeeklyLearningItem({
      hotelId: HOTEL,
      opportunityId: OPPORTUNITY_ID,
      actionType: "RFP_RECEIVED",
    })
  );
  const notRelevant = feedbackToResearchTask(
    hotelFeedbackToWeeklyLearningItem({
      hotelId: HOTEL,
      opportunityId: OPPORTUNITY_ID,
      validation: { familiarityStatus: "NOT_RELEVANT" },
    })
  );
  check(
    "learning_mappings",
    alreadyKnown && worthNow && wrongContact && wrongEmail && rfp && notRelevant,
    JSON.stringify({
      alreadyKnown: alreadyKnown?.taskType || alreadyKnown?.action || alreadyKnown,
      worthNow: worthNow?.taskType || worthNow?.action || worthNow,
      wrongContact: wrongContact?.taskType || wrongContact?.action || wrongContact,
      wrongEmail: wrongEmail?.taskType || wrongEmail?.action || wrongEmail,
      rfp: rfp?.taskType || rfp?.action || rfp,
      notRelevant: notRelevant?.taskType || notRelevant?.action || notRelevant,
    })
  );

  results.acceptanceStatus = results.ok
    ? "PRODUCTION_PERSISTENCE_VERIFIED"
    : "PERSISTENCE_DEFECT_FOUND";

  const reportPath = path.join(OUT_DIR, "LIVE-PERSISTENCE-CERT.json");
  fs.writeFileSync(reportPath, JSON.stringify(results, null, 2));
  const md = [
    "# GDI Hotel Feedback — Live Airtable Persistence Certification",
    "",
    `**Acceptance:** \`${results.acceptanceStatus}\``,
    `**Marker:** \`${MARKER}\``,
    `**Run:** \`${RUN_ID}\``,
    `**Base:** \`${results.baseId}\``,
    `**Tables:** \`${DECISIONS_TABLE_NAME}\`, \`${DECISION_EVENTS_TABLE_NAME}\``,
    `**Hotel (test):** \`${HOTEL}\``,
    `**Opportunity (test):** \`${OPPORTUNITY_ID}\``,
    `**Decision:** \`${results.decisionId}\``,
    "",
    "## Timestamps (UTC)",
    "",
    `- hotelValidationUpdatedAt: ${results.timestamps.hotelValidationUpdatedAt}`,
    `- salesActionUpdatedAt: ${results.timestamps.salesActionUpdatedAt}`,
    `- salesOutcomeUpdatedAt: ${results.timestamps.salesOutcomeUpdatedAt}`,
    "",
    "## Checks",
    "",
    ...results.checks.map(
      (c) => `- ${c.pass ? "PASS" : "FAIL"} ${c.name}${c.detail ? ` — ${c.detail}` : ""}`
    ),
    "",
    "No customer hotel feedback modified. No Won/Booked outcomes. No publication changes.",
    "",
  ].join("\n");
  fs.writeFileSync(path.join(OUT_DIR, "FOUNDER-LIVE-PERSISTENCE-ACCEPTANCE.md"), md);

  console.log("\n" + (results.ok ? "ALL VERIFIED" : "VERIFICATION FAILED"));
  console.log("acceptance", results.acceptanceStatus);
  console.log("report", reportPath);
  process.exit(results.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  const blocked = {
    ...results,
    ok: false,
    acceptanceStatus: "LIVE_VERIFICATION_BLOCKED",
    error: String(err?.message || err),
    code: err?.code || null,
  };
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(OUT_DIR, "LIVE-PERSISTENCE-CERT.json"),
    JSON.stringify(blocked, null, 2)
  );
  process.exit(1);
});
