#!/usr/bin/env node
/**
 * Read-only simulation of Priority + Action Window contract v1 against
 * frozen five-hotel facing snapshots. Does NOT write Airtable.
 *
 *   node scripts/gdi-priority-action-window-simulate-v1.mjs
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  assignPriorityAndActionWindowV1,
  applyPriorityActionAssignment,
  PRIORITY_ACTION_ASSIGNMENT_VERSION,
  PROSPECT_COMMERCIAL_CLASS_LABEL,
} from "../lib/group-demand-intelligence/priority-action-window/assignment-contract-v1.js";
import { hasUsableContactRoute } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-priority-action-window-parity");

const HOTELS = {
  bethesda: { id: "recLuxvwwxID7U2B8", label: "Bethesda Marriott" },
  waterstone: { id: "recgMYovrrZDJMqzX", label: "Waterstone" },
  renaissance_ts: {
    id: "recG66DQJKP2c0UNh",
    label: "Renaissance Times Square",
  },
  mayflower: { id: "rec0eTEnCqlsGJJt9", label: "Mayflower" },
  san_diego: { id: "recnPB3Q02NO8Ma1o", label: "Wyndham San Diego Bayside" },
};

function countPri(rows) {
  const c = { HIGH_PRIORITY: 0, MEDIUM_PRIORITY: 0, WATCHLIST: 0, DISQUALIFIED: 0, MISSING: 0 };
  for (const r of rows) {
    const p = r.priority || "MISSING";
    c[p] = (c[p] || 0) + 1;
  }
  return c;
}

function countBw(rows) {
  const c = {
    CONTACT_NOW: 0,
    QUALIFY_NOW: 0,
    RESEARCH_FURTHER: 0,
    WATCH: 0,
    TOO_EARLY: 0,
    MISSING: 0,
  };
  for (const r of rows) {
    const b = r.bookingWindowStatus || "MISSING";
    c[b] = (c[b] || 0) + 1;
  }
  return c;
}

function origin(o) {
  if (o.prospectReviewSection) return "reviewed_package";
  if (o.peVenueId || /VENUE_PARTNERSHIP|PRIVATE_EVENTS/i.test(`${o.opportunityType}|${o.demandFamily}`)) {
    return "private_events";
  }
  if (o.researchVersion || o.schemaVersion) return "legacy_or_research";
  return "legacy_or_unknown";
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const reconciliation = {
    generatedAt: new Date().toISOString(),
    ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
    mode: "simulation_only_no_airtable_writes",
    hotels: {},
    changeLog: [],
    productionChangeSet: [],
  };

  for (const [key, meta] of Object.entries(HOTELS)) {
    const all = await listOpportunitiesForHotel(meta.id);
    const facing = filterCustomerFacingOpportunities(all);
    const beforeRows = [];
    const afterRows = [];
    const changes = [];

    for (const o of facing) {
      const assignment = assignPriorityAndActionWindowV1(o);
      // Simulate production patch: only upgrade default Watch stamps; never demote
      // evidence-backed HIGH / CONTACT_NOW / QUALIFY_NOW / TOO_EARLY.
      const applied = applyPriorityActionAssignment(o, {});
      const before = {
        hotelKey: key,
        hotelId: meta.id,
        opportunityId: o.id || o.opportunityId,
        title: o.title || o.opportunityName,
        organizationName: o.organizationName,
        researchOrigin: origin(o),
        displayedPriority: o.priority || null,
        displayedActionWindow: o.bookingWindowStatus || null,
        commercialClass: o.prospectReviewSection || null,
        commercialClassLabel:
          PROSPECT_COMMERCIAL_CLASS_LABEL[String(o.prospectReviewSection || "").toUpperCase()] ||
          null,
        sourcingStatus: o.venueSourcingStatus || o.venueStatus || null,
        occasionDates: o.eventStartDate || o.eventDateDisplay || null,
        contactUsable: hasUsableContactRoute(o).ok,
        contactRouteReason: hasUsableContactRoute(o).reason,
        fitEvidence: Boolean(
          o.hotelFitScore != null ||
            o.hotelOpportunityThesis ||
            o.summaryWhyHotel ||
            o.hotelRelevanceDisposition === "READY_FOR_CARD"
        ),
        hotelFitScore: o.hotelFitScore ?? null,
        hotelRelevanceDisposition: o.hotelRelevanceDisposition || null,
        assignmentRuleVersion: o.priorityActionAssignmentVersion || null,
        priorityReason: o.priorityReason || null,
        bookingWindowReason: o.bookingWindowReason || null,
        actionStatus: o.actionStatus || o.salesActionStatus || null,
        decisionId: o.decisionId || null,
      };
      beforeRows.push(before);

      const afterPri = applied.opportunity.priority;
      const afterBw = applied.opportunity.bookingWindowStatus;
      afterRows.push({
        ...before,
        displayedPriority: afterPri,
        displayedActionWindow: afterBw,
        assignmentRuleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
        priorityReason: assignment.priorityReason,
        bookingWindowReason: assignment.bookingWindowReason,
        enrichmentNeeded: assignment.enrichmentNeeded,
      });

      if (applied.applied.priority || applied.applied.bookingWindowStatus) {
        const change = {
          hotelKey: key,
          opportunityId: before.opportunityId,
          title: before.title,
          researchOrigin: before.researchOrigin,
          beforePriority: before.displayedPriority,
          afterPriority: afterPri,
          beforeAction: before.displayedActionWindow,
          afterAction: afterBw,
          priorityReason: assignment.priorityReason,
          bookingWindowReason: assignment.bookingWindowReason,
          legitimateHotelDifference: false,
          cause: !before.fitEvidence
            ? "missing_fit_would_block"
            : before.displayedPriority === "WATCHLIST" &&
                before.displayedActionWindow === "WATCH" &&
                before.fitEvidence &&
                before.contactUsable
              ? "import_default_watch_stamp"
              : "evidence_reassessment",
        };
        changes.push(change);
        reconciliation.changeLog.push(change);
        reconciliation.productionChangeSet.push({
          hotelId: meta.id,
          opportunityId: before.opportunityId,
          patch: {
            priority: afterPri,
            bookingWindowStatus: afterBw,
            priorityReason: assignment.priorityReason,
            bookingWindowReason: assignment.bookingWindowReason,
            priorityActionAssignmentVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
            priorityActionAssessedAt: assignment.assessedAt,
          },
          preserve: [
            "decisionId",
            "customerValidation",
            "validationStatus",
            "actionStatus",
            "salesActionStatus",
            "outcomeStatus",
            "feedback",
            "prospectReviewSection",
          ],
        });
      }
    }

    reconciliation.hotels[key] = {
      label: meta.label,
      hotelId: meta.id,
      facingCount: facing.length,
      before: {
        priority: countPri(beforeRows.map((r) => ({ priority: r.displayedPriority }))),
        actionWindow: countBw(
          beforeRows.map((r) => ({ bookingWindowStatus: r.displayedActionWindow }))
        ),
        uiPriority: {
          High: beforeRows.filter((r) => r.displayedPriority === "HIGH_PRIORITY").length,
          Medium: beforeRows.filter((r) => r.displayedPriority === "MEDIUM_PRIORITY").length,
          Watch: beforeRows.filter(
            (r) => r.displayedPriority === "WATCHLIST" || !r.displayedPriority
          ).length,
        },
        uiAction: {
          PursueNow: beforeRows.filter((r) => r.displayedActionWindow === "CONTACT_NOW").length,
          Contact: beforeRows.filter((r) =>
            ["QUALIFY_NOW", "RESEARCH_FURTHER"].includes(r.displayedActionWindow)
          ).length,
          Watch: beforeRows.filter((r) => r.displayedActionWindow === "WATCH").length,
          TooEarly: beforeRows.filter((r) => r.displayedActionWindow === "TOO_EARLY").length,
        },
        section: beforeRows.reduce((acc, r) => {
          const s = r.commercialClass || "MISSING";
          acc[s] = (acc[s] || 0) + 1;
          return acc;
        }, {}),
      },
      after: {
        priority: countPri(afterRows.map((r) => ({ priority: r.displayedPriority }))),
        actionWindow: countBw(
          afterRows.map((r) => ({ bookingWindowStatus: r.displayedActionWindow }))
        ),
        uiPriority: {
          High: afterRows.filter((r) => r.displayedPriority === "HIGH_PRIORITY").length,
          Medium: afterRows.filter((r) => r.displayedPriority === "MEDIUM_PRIORITY").length,
          Watch: afterRows.filter(
            (r) => r.displayedPriority === "WATCHLIST" || !r.displayedPriority
          ).length,
        },
        uiAction: {
          PursueNow: afterRows.filter((r) => r.displayedActionWindow === "CONTACT_NOW").length,
          Contact: afterRows.filter((r) =>
            ["QUALIFY_NOW", "RESEARCH_FURTHER"].includes(r.displayedActionWindow)
          ).length,
          Watch: afterRows.filter((r) => r.displayedActionWindow === "WATCH").length,
          TooEarly: afterRows.filter((r) => r.displayedActionWindow === "TOO_EARLY").length,
        },
      },
      changeCount: changes.length,
      changes,
      rowsBefore: beforeRows,
      rowsAfter: afterRows,
    };

    console.log(
      key,
      "facing",
      facing.length,
      "changes",
      changes.length,
      "before",
      reconciliation.hotels[key].before.uiPriority,
      reconciliation.hotels[key].before.uiAction,
      "after",
      reconciliation.hotels[key].after.uiPriority,
      reconciliation.hotels[key].after.uiAction
    );
  }

  fs.writeFileSync(
    path.join(OUT, "simulation-report.json"),
    JSON.stringify(reconciliation, null, 2) + "\n"
  );
  fs.writeFileSync(
    path.join(OUT, "production-change-set.json"),
    JSON.stringify(
      {
        generatedAt: reconciliation.generatedAt,
        ruleVersion: PRIORITY_ACTION_ASSIGNMENT_VERSION,
        applyAuthorized: false,
        note: "Prepared only — do not apply until founder confirms",
        changeCount: reconciliation.productionChangeSet.length,
        changes: reconciliation.productionChangeSet,
      },
      null,
      2
    ) + "\n"
  );

  console.log(
    "wrote",
    path.join(OUT, "simulation-report.json"),
    "changes",
    reconciliation.changeLog.length
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
