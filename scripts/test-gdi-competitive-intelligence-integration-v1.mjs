#!/usr/bin/env node
/**
 * GDI Competitive Intelligence Integration V1 —
 * research disposition → canonical competitor fields → Market Scope.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  annotateInventoryPopulations,
  BETHESDA_HOTEL_ID,
  isBethesda67BaselineId,
  loadBethesda67SalesBaselineManifest,
  INVENTORY_POPULATION,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import {
  applyResearchCompetitorCommitmentMapping,
  extractCompetitorHotelFromResearchEvidence,
  mapResearchCompetitorCommitmentToCanonical,
  planResearchCompetitorCommitmentPersistence,
  RESEARCH_COMPETITOR_COMMITMENT_DISPOSITION,
} from "../lib/group-demand-intelligence/commercial-consistency/gdi-research-competitor-commitment-mapping-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  COMPETITOR_VENUE_STATUS,
  TARGET_HOTEL_SOURCING_STATUS,
} from "../lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIXTURE = path.join(
  ROOT,
  "fixtures/gdi-competitive-intelligence-integration-v1/aaei-aha-verified-research-v1.json"
);

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const fixture = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
const aaei = fixture.candidates.find((c) => c.opportunityId === "disc_0a11a0bb1158");
const aha = fixture.candidates.find((c) => c.opportunityId === "disc_9334f4f1e3dd");
const manifest = loadBethesda67SalesBaselineManifest({ forceReload: true });

check("fixture_has_aaei_and_aha", () => {
  assert.ok(aaei);
  assert.ok(aha);
  assert.equal(
    aaei.dispositionRecommendation,
    RESEARCH_COMPETITOR_COMMITMENT_DISPOSITION.STOP_PRIMARY_COMPETITOR_COMMITTED
  );
  assert.equal(
    aha.dispositionRecommendation,
    RESEARCH_COMPETITOR_COMMITMENT_DISPOSITION.STOP_PRIMARY_COMPETITOR_COMMITTED
  );
});

check("aaei_maps_hotel_washington_not_reagan_venue", () => {
  const hotel = extractCompetitorHotelFromResearchEvidence(aaei);
  assert.match(hotel, /Hotel Washington/i);
  assert.doesNotMatch(hotel, /Ronald Reagan/i);
  const mapped = mapResearchCompetitorCommitmentToCanonical(aaei);
  assert.equal(mapped.ok, true);
  assert.equal(mapped.canonicalFields.competitorHotel, hotel);
  assert.equal(
    mapped.canonicalFields.competitorVenueStatus,
    COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK
  );
  assert.equal(
    mapped.canonicalFields.targetHotelSourcingStatus,
    TARGET_HOTEL_SOURCING_STATUS.SOURCING_CLOSED
  );
  assert.equal(mapped.canonicalFields.customerVisible, false);
  assert.equal(mapped.canonicalFields.doNotAddToBethesda67SalesBaseline, true);
});

check("aha_maps_marriott_marquis_housing_commitment", () => {
  const hotel = extractCompetitorHotelFromResearchEvidence(aha);
  assert.match(hotel, /Marriott Marquis/i);
  const mapped = mapResearchCompetitorCommitmentToCanonical(aha);
  assert.equal(mapped.ok, true);
  assert.match(mapped.canonicalFields.competitorHotel, /Marriott Marquis/i);
  assert.equal(mapped.canonicalFields.competitorVenueConfirmed, true);
  assert.equal(mapped.canonicalFields.customerVisible, false);
});

check("venue_alone_does_not_map_without_booking_language", () => {
  const mapped = mapResearchCompetitorCommitmentToCanonical({
    opportunityId: "probe_venue_only",
    dispositionRecommendation: "STOP_PRIMARY_COMPETITOR_COMMITTED",
    venue: "Some Convention Center",
    hotelSourcingDecisionEvidence: "Event takes place at Some Convention Center.",
    sourcesInspected: ["https://example.org/event"],
  });
  assert.equal(mapped.ok, false);
  assert.equal(mapped.skipReason, "NO_BOOKING_OR_HOUSING_LANGUAGE_IN_EVIDENCE");
});

check("aaei_aha_before_after_market_scope", () => {
  for (const research of [aaei, aha]) {
    const beforeShell = {
      id: research.opportunityId,
      hotelId: BETHESDA_HOTEL_ID,
      title: research.eventTitle,
      eventStartDate: research.eventStartDate,
    };
    const [before] = annotateInventoryPopulations([beforeShell], {
      hotelId: BETHESDA_HOTEL_ID,
    });
    assert.equal(before.hotelMarketScopeEligible, false);
    assert.equal(before.competitiveMarketScopeEligible, false);
    assert.deepEqual(before.inventoryPopulations || [], []);

    const applied = applyResearchCompetitorCommitmentMapping(beforeShell, research);
    assert.equal(applied.ok, true);
    const [after] = annotateInventoryPopulations([applied.opportunity], {
      hotelId: BETHESDA_HOTEL_ID,
    });
    assert.equal(after.competitiveMarketScopeEligible, true);
    assert.equal(after.hotelMarketScopeEligible, false);
    assert.ok(
      (after.inventoryPopulations || []).includes(
        INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE
      )
    );
    assert.ok(
      !(after.inventoryPopulations || []).includes(
        INVENTORY_POPULATION.SALES_OPPORTUNITIES
      )
    );
    assert.equal(after.competitiveStatus, "VERIFIED");
    assert.equal(isBethesda67BaselineId(after.id, manifest), false);
  }
});

check("not_customer_facing_without_publication_auth", () => {
  for (const research of [aaei, aha]) {
    const applied = applyResearchCompetitorCommitmentMapping(
      { id: research.opportunityId, hotelId: BETHESDA_HOTEL_ID },
      research
    );
    const facing = filterCustomerFacingOpportunities([applied.opportunity], {
      hotelId: BETHESDA_HOTEL_ID,
      forceApproved: false,
    });
    assert.equal(facing.length, 0);
  }
});

check("partial_overflow_retains_hotel_and_competitive", () => {
  const research = {
    ...aaei,
    opportunityId: "probe_aaei_overflow",
    overflowVerified: true,
    lodgingDemandForPilotHotel: true,
  };
  const applied = applyResearchCompetitorCommitmentMapping(
    { id: research.opportunityId, hotelId: "recXh4NgF8sDbi9cQ" },
    research
  );
  assert.equal(
    applied.mapping.canonicalFields.targetHotelSourcingStatus,
    TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED
  );
  const [ann] = annotateInventoryPopulations([applied.opportunity], {
    hotelId: "recXh4NgF8sDbi9cQ",
  });
  assert.equal(ann.hotelMarketScopeEligible, true);
  assert.equal(ann.competitiveMarketScopeEligible, true);
});

check("unknown_sourcing_stays_hotel_when_eligible", () => {
  const [ann] = annotateInventoryPopulations(
    [
      {
        id: "probe_unknown_open",
        hotelId: "recXh4NgF8sDbi9cQ",
        title: "Open Demand Conference 2027",
        eventStartDate: "2027-09-01",
        targetHotelSourcingStatus: "OPEN_CONFIRMED",
      },
    ],
    { hotelId: "recXh4NgF8sDbi9cQ" }
  );
  assert.equal(ann.hotelMarketScopeEligible, true);
  assert.equal(ann.competitiveMarketScopeEligible, false);
});

check("competitor_mentioned_not_committed_no_false_competitive", () => {
  const mapped = mapResearchCompetitorCommitmentToCanonical({
    opportunityId: "probe_mention",
    dispositionRecommendation: "WATCHLIST_LOW_PRIORITY_OR_STOP",
    hotelSourcingDecisionEvidence:
      "Possibly near Hotel Example; housing information will follow soon.",
    sourcesInspected: ["https://example.org/event"],
  });
  assert.equal(mapped.ok, false);
  const [ann] = annotateInventoryPopulations(
    [
      {
        id: "probe_mention_opp",
        hotelId: "recXh4NgF8sDbi9cQ",
        title: "Mention Only 2027",
        eventStartDate: "2027-09-01",
        competitorHotel: "Hotel Example",
      },
    ],
    { hotelId: "recXh4NgF8sDbi9cQ" }
  );
  // Soft name alone → needs verification; non-Bethesda may list Competitive on
  // NEEDS_VERIFICATION. Ensure we did not VERIFIED-commit without booking evidence.
  assert.notEqual(ann.competitiveStatus, "VERIFIED");
});

check("persistence_plan_is_dry_run_non_destructive", () => {
  const plan = planResearchCompetitorCommitmentPersistence(fixture.candidates);
  assert.equal(plan.mode, "DRY_RUN");
  assert.equal(plan.executed, false);
  assert.equal(plan.airtableWrites, 0);
  assert.equal(plan.customerPublications, 0);
  assert.equal(plan.bethesda67Additions, 0);
  assert.equal(plan.mappedCount, 2);
  assert.equal(plan.stopForFounderReview, true);
  for (const row of plan.rows) {
    assert.equal(row.bethesda67Membership, false);
    assert.equal(row.customerPublication, false);
    assert.equal(row.airtableWrite, false);
  }
});

check("club_med_fixture_still_competitive_only", () => {
  const wyPath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels/recXh4NgF8sDbi9cQ/opportunities.json"
  );
  if (!fs.existsSync(wyPath)) {
    console.log("SKIP club_med_fixture (opportunities missing)");
    return;
  }
  const raw = JSON.parse(fs.readFileSync(wyPath, "utf8"));
  const ops = Array.isArray(raw) ? raw : raw.opportunities || [];
  const club = ops.filter(
    (o) =>
      /club med/i.test(o.competitorHotel || "") ||
      /club med/i.test(o.title || "")
  );
  assert.ok(club.length >= 1, "expected Club Med fixture rows");
  const annotated = annotateInventoryPopulations(club, {
    hotelId: "recXh4NgF8sDbi9cQ",
  });
  for (const row of annotated) {
    assert.equal(row.hotelMarketScopeEligible, false);
    assert.equal(row.competitiveMarketScopeEligible, true);
  }
});

const outDir = path.join(
  ROOT,
  "reports/gdi-competitive-intelligence-integration-v1"
);
fs.mkdirSync(outDir, { recursive: true });
const plan = planResearchCompetitorCommitmentPersistence(fixture.candidates);
const beforeAfter = [aaei, aha].map((research) => {
  const beforeShell = {
    id: research.opportunityId,
    hotelId: BETHESDA_HOTEL_ID,
    title: research.eventTitle,
    eventStartDate: research.eventStartDate,
  };
  const [before] = annotateInventoryPopulations([beforeShell], {
    hotelId: BETHESDA_HOTEL_ID,
  });
  const applied = applyResearchCompetitorCommitmentMapping(beforeShell, research);
  const [after] = annotateInventoryPopulations([applied.opportunity], {
    hotelId: BETHESDA_HOTEL_ID,
  });
  return {
    opportunityId: research.opportunityId,
    organizationName: research.organizationName,
    before: {
      hotelMarketScopeEligible: before.hotelMarketScopeEligible,
      competitiveMarketScopeEligible: before.competitiveMarketScopeEligible,
      inventoryPopulations: before.inventoryPopulations,
      competitiveStatus: before.competitiveStatus,
    },
    after: {
      hotelMarketScopeEligible: after.hotelMarketScopeEligible,
      competitiveMarketScopeEligible: after.competitiveMarketScopeEligible,
      inventoryPopulations: after.inventoryPopulations,
      competitiveStatus: after.competitiveStatus,
      competitorHotel: after.competitorHotel || after.cmiCompetitorHotel,
      competitorVenueStatus: after.competitorVenueStatus,
      targetHotelSourcingStatus: after.targetHotelSourcingStatus,
      customerVisible: after.customerVisible,
      onBethesda67: isBethesda67BaselineId(after.id, manifest),
    },
    mapping: applied.mapping,
  };
});
fs.writeFileSync(
  path.join(outDir, "aaei-aha-before-after.json"),
  JSON.stringify({ generatedAt: new Date().toISOString(), beforeAfter }, null, 2)
);
fs.writeFileSync(
  path.join(outDir, "persistence-plan-dry-run.json"),
  JSON.stringify(plan, null, 2)
);

console.log(
  JSON.stringify(
    { suite: "gdi-competitive-intelligence-integration-v1", passed, failed },
    null,
    2
  )
);
process.exit(failed ? 1 : 0);
