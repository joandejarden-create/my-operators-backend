#!/usr/bin/env node
/**
 * Capture Bethesda Marriott NICE 2027 commercial progression
 * (CONTACTED + ADDED_TO_SOURCED_PROPERTIES) from Francesca Moore email 2026-09-17.
 *
 * Idempotent. Does NOT invent WORTH_PURSUING_NOW validation.
 * Does NOT claim Dealality caused the sourcing outcome.
 *
 * Usage:
 *   node scripts/apply-bethesda-nice-commercial-progression.mjs --dry-run
 *   node scripts/apply-bethesda-nice-commercial-progression.mjs --apply
 */

import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  PRODUCT_MODULE,
  DECISION_TYPE,
  SUBJECT_TYPE,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
  ACTOR_ROLE,
  CAUSAL_CONFIDENCE,
  ensureGdiOpportunityDecision,
  getSubjectDecision,
  recordAction,
  recordOutcome,
  createDeterministicEventId,
  getPersistenceMode,
  getDecisionOutcomesAirtableBaseId,
} from "../lib/decision-outcomes/index.js";
import { assertNotLegacyMvpCanonicalBase } from "../lib/decision-outcomes/airtable-base.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { upsertCustomerUsageObservation } from "../lib/group-demand-intelligence/customer-usage-observations.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const HOTEL_ID = "recLuxvwwxID7U2B8";
const OPPORTUNITY_ID = "gdi_opp_nice_2027";
const EVENT_DATE = "2026-09-17";
const EVIDENCE_KEY = "francesca_moore_email_2026-09-17_nice_sourced";
const OUTREACH_EVIDENCE_KEY = "francesca_moore_email_2026-09-17_21_of_29";

const EXPECTED_BASE = "appa2cE7FTRmIbB32";

function parseArgs(argv) {
  const apply = argv.includes("--apply");
  const dryRun = !apply || argv.includes("--dry-run");
  return { apply, dryRun: dryRun && !apply };
}

async function main() {
  const { apply, dryRun } = parseArgs(process.argv.slice(2));
  const mode = getPersistenceMode();
  const baseId = getDecisionOutcomesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId || EXPECTED_BASE, {
    surface: "bethesda-nice-commercial-progression",
  });
  if (baseId && baseId !== EXPECTED_BASE) {
    throw new Error(
      `Unexpected decision-outcomes base ${baseId}; expected ${EXPECTED_BASE}`
    );
  }

  const doc = await loadOpportunitiesCanonical(HOTEL_ID);
  const opportunity = (doc.opportunities || []).find(
    (o) => o.id === OPPORTUNITY_ID
  );
  if (!opportunity) {
    throw new Error(`NICE opportunity not found: ${OPPORTUNITY_ID}`);
  }

  console.log(
    JSON.stringify(
      {
        hotelId: HOTEL_ID,
        opportunityId: opportunity.id,
        title: opportunity.title,
        priority: opportunity.priority,
        opportunityType: opportunity.opportunityType,
        venueSourcingStatus: opportunity.venueSourcingStatus,
        persistenceMode: mode,
        baseId: baseId || null,
        dryRun,
        apply,
      },
      null,
      2
    )
  );

  if (dryRun && !apply) {
    console.log(
      "\nDRY RUN — re-run with --apply to create decision + events + 21/29 observation."
    );
    return;
  }

  const ensured = await ensureGdiOpportunityDecision({
    hotelId: HOTEL_ID,
    opportunity,
    sourceSystem: "GDI_CUSTOMER_PROGRESSION",
  });
  const decision = ensured.decision;
  if (!decision) {
    throw new Error("Failed to ensure GDI decision for NICE");
  }

  const actionEventId = createDeterministicEventId({
    prefix: "act",
    hotelId: HOTEL_ID,
    decisionId: decision.decisionId,
    eventKind: "ACTION",
    subtype: GDI_ACTION_TYPE.CONTACTED,
    eventDate: EVENT_DATE,
    evidenceKey: EVIDENCE_KEY,
  });
  const outcomeEventId = createDeterministicEventId({
    prefix: "out",
    hotelId: HOTEL_ID,
    decisionId: decision.decisionId,
    eventKind: "OUTCOME",
    subtype: GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES,
    eventDate: EVENT_DATE,
    evidenceKey: EVIDENCE_KEY,
  });

  const actionResult = await recordAction(HOTEL_ID, decision.decisionId, {
    actionEventId,
    actionType: GDI_ACTION_TYPE.CONTACTED,
    actionStatus: "RECORDED",
    actionDescription:
      "Bethesda Marriott sales team contacted the NICE 2027 opportunity (customer-reported).",
    actionDate: `${EVENT_DATE}T12:00:00.000Z`,
    displayName: "Francesca Moore",
    role: ACTOR_ROLE.SALES_MANAGER,
    userId: "customer:francesca_moore",
    sourceSurface: "CUSTOMER_REPORTED",
    provenance: {
      evidenceType: "CUSTOMER_FIRST_PARTY_EMAIL",
      sourceActor: "Francesca Moore",
      sourceOrganization: "Bethesda Marriott",
      evidenceDate: EVENT_DATE,
      evidenceKey: EVIDENCE_KEY,
      note: "Customer-reported contact; not a Dealality-attributed causal claim.",
    },
  });

  const outcomeResult = await recordOutcome(HOTEL_ID, decision.decisionId, {
    outcomeEventId,
    outcomeType: GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES,
    outcomeValue: GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES,
    outcomeNote:
      "Progression observed after hotel outreach: hotel reported being added to the sourced properties for NICE 2027 (location TBD, Jun 7–9). Dealality surfaced the opportunity/contact information; causal confidence not claimed.",
    outcomeDate: `${EVENT_DATE}T12:00:00.000Z`,
    causalConfidence: CAUSAL_CONFIDENCE.UNKNOWN,
    displayName: "Francesca Moore",
    role: ACTOR_ROLE.SALES_MANAGER,
    userId: "customer:francesca_moore",
    sourceSurface: "CUSTOMER_REPORTED",
    provenance: {
      evidenceType: "CUSTOMER_FIRST_PARTY_EMAIL",
      sourceActor: "Francesca Moore",
      sourceOrganization: "Bethesda Marriott",
      evidenceDate: EVENT_DATE,
      evidenceKey: EVIDENCE_KEY,
      language:
        "Progression observed after hotel outreach — not claimed as Dealality-caused.",
    },
  });

  const usage = upsertCustomerUsageObservation(HOTEL_ID, {
    kind: "OUTREACH_SUMMARY",
    observationDate: EVENT_DATE,
    evidenceKey: OUTREACH_EVIDENCE_KEY,
    opportunitiesSurfaced: 29,
    opportunitiesContacted: 21,
    sourceActor: "Francesca Moore",
    sourceOrganization: "Bethesda Marriott",
    sourceType: "CUSTOMER_FIRST_PARTY_EMAIL",
    evidenceSummary:
      "Francesca reported reaching out to 21/29 opportunities using phone/email from the platform. Per-opportunity CONTACTED events not written (which 21 unknown).",
    note: "Bethesda GDI outreach summary — aggregate only.",
  });

  const verified = await getSubjectDecision(HOTEL_ID, {
    productModule: PRODUCT_MODULE.GDI,
    subjectId: OPPORTUNITY_ID,
    decisionType: DECISION_TYPE.OPPORTUNITY_PURSUIT,
  });

  const report = {
    hotelId: HOTEL_ID,
    hotelName: "Bethesda Marriott",
    opportunityId: OPPORTUNITY_ID,
    opportunityTitle: opportunity.title,
    decisionId: decision.decisionId,
    decisionCreated: ensured.created === true,
    actionEventId:
      actionResult.event?.actionEventId ||
      actionResult.event?.eventId ||
      actionEventId,
    actionCreated: actionResult.created !== false,
    outcomeEventId:
      outcomeResult.event?.outcomeEventId ||
      outcomeResult.event?.eventId ||
      outcomeEventId,
    outcomeCreated: outcomeResult.created !== false,
    outreachSummary: {
      captured: true,
      observationId: usage.observation.observationId,
      created: usage.created,
      opportunitiesSurfaced: 29,
      opportunitiesContacted: 21,
      path: `data/group-demand-intelligence/hotels/${HOTEL_ID}/customer-usage-observations.json`,
    },
    current: verified?.current || null,
    commercialProgression:
      verified?.current?.commercialProgression || null,
    persistence: mode,
    baseId: baseId || null,
    appliedAt: new Date().toISOString(),
  };

  const outDir = path.join(ROOT, "reports/group-demand-intelligence");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(
    outDir,
    "bethesda-nice-commercial-progression-apply.json"
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  console.log(JSON.stringify(report, null, 2));
  console.log("\nWrote", outPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
