/**
 * Focused gates: hotel activity history + opportunity type filter.
 * Does not mutate opportunity inventory.
 */
import assert from "node:assert/strict";
import {
  buildHotelActivityHistory,
  formatRunStatusForCustomer,
  ACTIVITY_KIND,
} from "../lib/group-demand-intelligence/hotel-activity-history-v1.js";
import {
  classifyOpportunityTypeFilter,
  opportunityMatchesTypeFilter,
  countByOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { reconcileHotelFitScoreForDisplay } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";

const MAY = "rec0eTEnCqlsGJJt9";
const SD = "recnPB3Q02NO8Ma1o";

function testHistoryDefinitions() {
  const now = "2026-10-15T18:00:00.000Z";
  // runId + lastVerifiedAt alone must NOT invent Completed / Last Research
  const may = buildHotelActivityHistory("hotel_runid_only_zzz", {
    opportunities: [
      {
        id: "a",
        lastVerifiedAt: "2026-10-14T12:00:00.000Z",
        prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
        createdAt: "2026-10-13T08:00:00.000Z",
        runId: "gdi_weekly_rec0eTEn_2026-10-15T09-24-33-542Z_e21d45",
      },
    ],
    opportunityDoc: {
      reviewedProspectImport: { importedAt: "2026-10-14T18:00:00.000Z" },
    },
    now,
  });
  assert.equal(may.lastResearchAt, null, "verification/runId ≠ Last Research");
  assert.ok(may.lastSourceVerificationAt);
  assert.equal(may.runStatus, "STATUS_UNAVAILABLE");
  assert.equal(may.runStatusLabel, "Status unavailable");
  assert.ok(may.lastPublishedUpdateAt, "Last Published Update from import/basis");
  assert.equal(may.lastPublishedUpdateLabel, "Last Published Update");

  const missing = buildHotelActivityHistory("hotel_with_no_history_zzz", {
    opportunities: [],
    opportunityDoc: {},
    now,
  });
  assert.equal(missing.runStatus, "NOT_YET_RUN");
  assert.equal(missing.runStatusLabel, "Not yet run");
  assert.equal(missing.lastResearchAt, null);
  assert.equal(missing.lastPublishedUpdateAt, null);

  const evidenceOnly = buildHotelActivityHistory("hotel_evidence_only_zzz", {
    opportunities: [
      {
        id: "b",
        lastVerifiedAt: "2026-09-01T00:00:00.000Z",
        researchVersion: "gdi-scaling-companies-research-v1", // static version ≠ instance run
      },
    ],
    opportunityDoc: {},
    now,
  });
  assert.equal(evidenceOnly.lastResearchAt, null);
  assert.ok(evidenceOnly.lastSourceVerificationAt);
  assert.equal(evidenceOnly.runStatus, "HISTORY_UNAVAILABLE");
  assert.equal(
    formatRunStatusForCustomer(evidenceOnly.runStatus, evidenceOnly.runStatusLabel),
    "History unavailable"
  );

  // Importing must not fabricate research completion from updatedAt alone
  const importOnly = buildHotelActivityHistory("hotel_import_only_zzz", {
    opportunities: [
      {
        id: "c",
        prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
        createdAt: "2026-08-01T00:00:00.000Z",
        updatedAt: "2026-10-15T12:00:00.000Z",
      },
    ],
    opportunityDoc: {
      updatedAt: "2026-10-15T12:00:00.000Z",
      reviewedProspectImport: { importedAt: "2026-08-01T00:00:00.000Z" },
    },
    now,
  });
  assert.equal(importOnly.lastResearchAt, null);
  assert.ok(importOnly.lastPublishedUpdateAt);
  assert.equal(importOnly.lastPublishedUpdateAt.slice(0, 10), "2026-08-01");

  console.log("ok history definitions");
}

function testTypeFilter() {
  assert.equal(
    classifyOpportunityTypeFilter({
      researchSegment: "scaling_companies",
      opportunityType: "CORPORATE_ACCOUNT_RELATIONSHIP",
    }),
    OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY
  );
  assert.equal(
    classifyOpportunityTypeFilter({
      opportunityType: "VENUE_PARTNERSHIP",
    }),
    OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP
  );
  assert.equal(
    classifyOpportunityTypeFilter({
      opportunityType: "SPECIFIC_PRIVATE_EVENT",
    }),
    OPPORTUNITY_TYPE_FILTER.SPECIFIC_PRIVATE_EVENT
  );
  assert.equal(
    classifyOpportunityTypeFilter({
      researchSegment: "local_meetings_catering",
    }),
    OPPORTUNITY_TYPE_FILTER.LOCAL_MEETINGS_CATERING
  );
  assert.equal(
    classifyOpportunityTypeFilter({
      opportunityType: "OVERFLOW_HOUSING",
      segment: "Association",
    }),
    OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
  );
  assert.ok(
    opportunityMatchesTypeFilter(
      { researchSegment: "scaling_companies" },
      OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY
    )
  );
  assert.equal(
    opportunityMatchesTypeFilter(
      { researchSegment: "scaling_companies" },
      OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_TRAINING
    ),
    false
  );

  const counts = countByOpportunityTypeFilter([
    { researchSegment: "scaling_companies" },
    { opportunityType: "VENUE_PARTNERSHIP" },
    { opportunityType: "UNKNOWN_X" },
  ]);
  assert.equal(counts.all, 3);
  assert.equal(counts.SCALING_COMPANY, 1);
  assert.equal(counts.PREFERRED_LODGING_PARTNERSHIP, 1);
  assert.equal(counts.OTHER_UNCLASSIFIED, 1);
  console.log("ok type filter");
}

function testAssessmentThesisNotOnCard() {
  const dto = toOpportunityListDto({
    id: "ahima_test",
    title: "AHIMA Advocacy Summit 2027",
    opportunityType: "OVERFLOW_HOUSING",
    summaryWhat:
      "AHIMA Advocacy Summit 2027 is dated in Washington, DC — downtown advocacy lodging.",
    hotelOpportunityThesis:
      "Dimension-level hotel-fit assessment for The Mayflower Hotel — overall numeric score withheld until evidence improves.",
    summaryWhyHotel:
      "Dimension-level hotel-fit assessment for The Mayflower Hotel — overall numeric score withheld.",
    priority: "HIGH_PRIORITY",
  });
  assert.ok(dto.summaryWhat);
  assert.ok(!/dimension-level hotel-fit assessment/i.test(dto.hotelOpportunityThesis || ""));
  assert.ok(!/dimension-level hotel-fit assessment/i.test(dto.summaryWhyHotel || ""));
  assert.ok(/AHIMA/.test(dto.hotelOpportunityThesis || dto.summaryWhat));
  assert.equal(dto.opportunityTypeLabel, "Overflow / Housing");
  console.log("ok assessment thesis scrub");
}

function testLiveHotelsInventoryFreeze() {
  // Import lazily so unit tests above still run if FS data missing
  return import("../lib/group-demand-intelligence/repository.js").then(
    async ({ loadOpportunities }) => {
      const { filterCustomerFacingOpportunities } = await import(
        "../lib/group-demand-intelligence/customer-visibility.js"
      );
      const { filterSalespersonView } = await import(
        "../lib/group-demand-intelligence/opportunity-factory.js"
      );
      const freeze = {};
      for (const id of [MAY, SD]) {
        const doc = loadOpportunities(id);
        const facing = filterCustomerFacingOpportunities(
          filterSalespersonView(doc.opportunities || []),
          { hotelId: id }
        );
        freeze[id] = {
          facingCount: facing.length,
          ids: facing.map((o) => o.id).sort(),
          typeCounts: countByOpportunityTypeFilter(facing),
        };
        const hist = buildHotelActivityHistory(id, {
          opportunities: doc.opportunities || [],
          opportunityDoc: doc,
        });
        freeze[id].history = {
          lastResearchAt: hist.lastResearchAt,
          runStatus: hist.runStatus,
          runStatusLabel: hist.runStatusLabel,
          latestRunId: hist.latestRunId,
          lastPublishedUpdateAt: hist.lastPublishedUpdateAt,
          lastReviewedPackageImportAt: hist.lastReviewedPackageImportAt,
        };
        assert.notEqual(hist.runStatus, "NEVER_RUN");
        assert.ok(
          hist.lastResearchAt ||
            hist.runStatus === "NOT_YET_RUN" ||
            hist.runStatus === "HISTORY_UNAVAILABLE" ||
            hist.runStatus === "STATUS_UNAVAILABLE"
        );
      }
      // Local FS snapshot is optional (production reads Airtable). Skip hard
      // inventory assert when this worktree has no hotel opportunity files.
      if ((freeze[MAY].facingCount || 0) + (freeze[SD].facingCount || 0) === 0) {
        console.log("ok live freeze skipped (no local facing inventory)");
        return freeze;
      }
      assert.ok(freeze[MAY].facingCount >= 1);
      console.log(
        "ok live freeze",
        JSON.stringify({
          mayFacing: freeze[MAY].facingCount,
          sdFacing: freeze[SD].facingCount,
          mayTypes: freeze[MAY].typeCounts,
          mayHistory: freeze[MAY].history,
          sdHistory: freeze[SD].history,
        })
      );
      return freeze;
    }
  );
}

testHistoryDefinitions();
testTypeFilter();
testAssessmentThesisNotOnCard();
assert.equal(reconcileHotelFitScoreForDisplay({ hotelFitScore: 72 }), 72);
const freeze = await testLiveHotelsInventoryFreeze();
assert.ok(freeze[MAY].ids.length === freeze[MAY].facingCount);
console.log("PASS test-gdi-hotel-activity-history-and-type-filter-v1");
