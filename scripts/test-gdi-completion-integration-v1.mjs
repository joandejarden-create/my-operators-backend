#!/usr/bin/env node
/**
 * Focused regressions for GDI completion integration.
 *   node scripts/test-gdi-completion-integration-v1.mjs
 */
import assert from "node:assert/strict";
import {
  CHANGE_TYPE,
  buildLedgerFromPaidArtifacts,
  dedupeChangeLedger,
  createChangeLedgerRow,
} from "../lib/group-demand-intelligence/workflow/opportunity-change-ledger-v1.js";
import {
  buildHotelSegmentComparisonTable,
  TASK_EXECUTION_STATUS,
  SEGMENT_COVERAGE_STATUS,
  createEmptyScorecardRow,
} from "../lib/group-demand-intelligence/workflow/research-scorecard-v1.js";
import {
  assessContactResearchAxes,
  NAMED_RESEARCH_STATUS,
  RESPONSIBILITY_CLASS,
  CHANNEL_OWNERSHIP,
} from "../lib/group-demand-intelligence/workflow/buyer-contact-enrichment-contract-v1.js";
import { sanitizeContactForCustomerDisplay } from "../lib/group-demand-intelligence/workflow/contact-field-provenance-v1.js";
import { packagesFromDispositions } from "../lib/group-demand-intelligence/workflow/corporate-package-persist-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// 1. The Lane restoration is not net-new
{
  const ledger = buildLedgerFromPaidArtifacts({
    authoritativeOutcomes: [
      {
        organizationName: "deck655",
        hotelId: "recnPB3Q02NO8Ma1o",
        segment: "weddings_social_private_events",
        changeType: CHANGE_TYPE.NEW_PROSPECT_FIRST_PUBLISHED,
      },
      {
        organizationName: "The Lane",
        hotelId: "recnPB3Q02NO8Ma1o",
        segment: "weddings_social_private_events",
        changeType: CHANGE_TYPE.RESTORED_REAPPLIED,
      },
    ],
  });
  assert.equal(ledger.netNewFirstPublished, 1);
  assert.equal(ledger.restoredReapplied, 1);
  ok("lane_restore_not_net_new");
}

// 2. Reprocessed scaling counts once under hotel/segment
{
  const rows = dedupeChangeLedger([
    createChangeLedgerRow({
      hotelId: "rec0eTEnCqlsGJJt9",
      organizationOrVenueName: "Quorum",
      primaryResearchSegment: "scaling_companies",
      originalDiscoveryTaskId: "scaling_discovery_may",
      recoveryOrReprocessTaskId: "quarantine_reprocess",
      changeType: CHANGE_TYPE.NEW_PROSPECT_FIRST_PUBLISHED,
      taskId: "scaling_discovery_may",
    }),
    createChangeLedgerRow({
      hotelId: "rec0eTEnCqlsGJJt9",
      organizationOrVenueName: "Quorum",
      primaryResearchSegment: "scaling_companies",
      originalDiscoveryTaskId: "scaling_discovery_may",
      taskId: "quarantine_reprocess",
      changeType: CHANGE_TYPE.NEW_PROSPECT_FIRST_PUBLISHED,
    }),
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].originalDiscoveryTaskId, "scaling_discovery_may");
  ok("reprocess_scaling_counts_once");
}

// 3. Contact enrichment does not inflate net-new
{
  const ledger = buildLedgerFromPaidArtifacts({
    authoritativeOutcomes: [
      {
        organizationName: "Wisedocs",
        hotelId: "rec0eTEnCqlsGJJt9",
        segment: "scaling_companies",
        changeType: CHANGE_TYPE.EXISTING_PROSPECT_MATERIALLY_ENRICHED,
        contactEnrichmentActivity: true,
      },
      {
        organizationName: "Corgan",
        hotelId: "rec0eTEnCqlsGJJt9",
        segment: "scaling_companies",
        changeType: CHANGE_TYPE.NEW_PROSPECT_FIRST_PUBLISHED,
      },
    ],
  });
  assert.equal(ledger.netNewFirstPublished, 1);
  assert.equal(ledger.contactEnrichmentActivities, 1);
  const table = buildHotelSegmentComparisonTable([], { ledgerSummary: ledger });
  const may = table.find(
    (r) =>
      r.hotelId === "rec0eTEnCqlsGJJt9" && r.segmentId === "scaling_companies"
  );
  assert.equal(may.netNewQualified, 1);
  assert.equal(may.enriched, 1);
  ok("enrichment_does_not_inflate_net_new");
}

// 4. Completed task ≠ completed coverage
{
  const row = createEmptyScorecardRow({
    hotelId: "x",
    segmentId: "scaling_companies",
  });
  row.scope.taskExecutionStatus = TASK_EXECUTION_STATUS.COMPLETED;
  row.scope.segmentCoverageStatus = SEGMENT_COVERAGE_STATUS.PARTIAL;
  assert.notEqual(
    row.scope.taskExecutionStatus,
    row.scope.segmentCoverageStatus
  );
  assert.equal(
    row.scope.segmentCoverageStatus,
    SEGMENT_COVERAGE_STATUS.PARTIAL
  );
  ok("task_exec_separate_from_coverage");
}

// 5. Named person + generic inbox ≠ direct email ownership / ≠ enrichment complete
{
  const axes = assessContactResearchAxes({
    primaryContact: {
      name: "Manal Abu-Shawar",
      email: "info@wisedocs.com",
      emailOwnership: CHANNEL_OWNERSHIP.ORGANIZATION_DEPARTMENT,
    },
    functionalContactEmail: "info@wisedocs.com",
    contactResponsibilityClass: RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT,
  });
  assert.equal(
    axes.namedResearchStatus,
    NAMED_RESEARCH_STATUS.PERSON_ESTABLISHED_CHANNEL_PENDING
  );
  assert.ok(
    axes.queueReasons.includes(
      "named_person_established_person_specific_channel_still_pending"
    )
  );
  ok("generic_inbox_does_not_complete_named_enrichment");
}

// 6. Sanitize does not leak Surfe; channel labels allowed
{
  const s = sanitizeContactForCustomerDisplay({
    name: "Manal",
    email: null,
    organizationContactRoute: "info@wisedocs.com",
    organizationContactRouteLabel: "Organization contact route: info@wisedocs.com",
    askToBeDirectedTo: "Ask to be directed to People Ops (Manal)",
    surfeEnrichmentId: "should-not-appear",
  });
  assert.equal(s.email, null);
  assert.ok(s.organizationContactRouteLabel);
  assert.equal(s.surfeEnrichmentId, undefined);
  ok("sanitize_channel_labels_no_surfe");
}

// 7. Auto packages from dispositions (no handwritten)
{
  const pkgs = packagesFromDispositions([
    {
      disposition: "qualified_publishable",
      segment: "scaling_companies",
      taskId: "scaling_discovery_may_v2",
      package: {
        organizationName: "Corgan",
        hotelId: "rec0eTEnCqlsGJJt9",
      },
    },
  ]);
  assert.equal(pkgs.scalingCompanies.length, 1);
  assert.equal(pkgs.scalingCompanies[0]._fromTaskId, "scaling_discovery_may_v2");
  ok("auto_packages_from_dispositions");
}

// 8. Customer list DTO excludes org inbox as person email when ownership is org
{
  const dto = toOpportunityListDto({
    id: "gdi_sc_test",
    hotelId: "rec0eTEnCqlsGJJt9",
    organizationName: "Wisedocs",
    title: "Wisedocs",
    primaryContact: {
      name: "Manal Abu-Shawar",
      email: "info@wisedocs.com",
      emailOwnership: "organization_or_department",
    },
    functionalContactEmail: "info@wisedocs.com",
  });
  assert.equal(dto.primaryContact.email, null);
  assert.equal(dto.functionalContactEmail, "info@wisedocs.com");
  ok("list_dto_separates_org_inbox");
}

console.log(`\n${passed} passed`);
