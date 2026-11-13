#!/usr/bin/env node
/**
 * Opportunity Type classification regression lock (Bethesda + portfolio).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  annotateInventoryPopulations,
  selectBethesdaSalesOpportunityInventory,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
  opportunityTypeFilterOptions,
  findUnclassifiedCustomerVisibleOpportunities,
  isCustomerFacingOpportunityTypeFilter,
  countByOpportunityTypeFilter,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { applyCmiLedgerRemediation } from "../lib/group-demand-intelligence/commercial-consistency/cmi-evidence-ledger-v1.js";
import {
  resolveLegacyResearchSegmentAlias,
  preserveDemandSegmentOnResearchUpdate,
  inferDemandSegmentIdFromResearchEvidence,
} from "../lib/group-demand-intelligence/legacy-demand-segment-alias-map-v1.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("cmi_remediation_preserves_demand_type", () => {
  const next = applyCmiLedgerRemediation({
    id: "gdi_opp_acvnu_renal_week_20270412",
    title: "ACVNU Renal Week",
    segment: "Overflow Housing",
    opportunityType: "OVERFLOW_HOUSING",
    opportunityTypeLabel: "OVERFLOW HOUSING",
  });
  assert.equal(next.opportunityType, "OVERFLOW_HOUSING");
  assert.equal(next.demandSegmentId, "associations_conferences_events");
  assert.equal(
    classifyOpportunityTypeFilter(next),
    OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
  );
});

check("lifecycle_types_do_not_drive_opportunity_type", () => {
  assert.equal(
    classifyOpportunityTypeFilter({
      segment: "wedding",
      opportunityType: "FUTURE_CYCLE",
    }),
    OPPORTUNITY_TYPE_FILTER.SOCIAL_SPECIAL_EVENTS
  );
  assert.equal(
    classifyOpportunityTypeFilter({
      segment: "Housing / Overflow",
      opportunityType: "FUTURE_WATCH",
    }),
    OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
  );
  assert.equal(
    classifyOpportunityTypeFilter({
      title: "Interiors from Spain — EXHIBITOR BLOCK",
      segment: "Discovered",
      opportunityType: "FUTURE_WATCH",
    }),
    OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
  );
});

check("legacy_alias_map_documented", () => {
  assert.equal(
    resolveLegacyResearchSegmentAlias("Marine / Boat Show"),
    "associations_conferences_events"
  );
  assert.equal(
    resolveLegacyResearchSegmentAlias("travel advisor"),
    "travel_trade_group_intermediaries"
  );
  assert.equal(resolveLegacyResearchSegmentAlias("Discovered"), null);
});

check("research_update_preserves_demand_segment", () => {
  const next = preserveDemandSegmentOnResearchUpdate(
    {
      id: "x",
      demandSegmentId: "sports_entertainment",
      opportunityType: "OVERFLOW_HOUSING",
    },
    {
      id: "x",
      opportunityType: "FUTURE_CYCLE",
      opportunityTypeLabel: "Future Cycle",
    }
  );
  assert.equal(next.demandSegmentId, "sports_entertainment");
  assert.equal(next.opportunityType, "FUTURE_CYCLE");
});

check("live_commercial_quality_preserves_demand_segment", () => {
  const out = applyLiveCommercialQuality({
    id: "y",
    title: "Test",
    demandSegmentId: "education_scientific",
    opportunityType: "FUTURE_CYCLE",
  });
  assert.equal(out.demandSegmentId, "education_scientific");
});

const doc = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
    ),
    "utf8"
  )
);
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  doc.opportunities || []
);
const selected = selectBethesdaSalesOpportunityInventory(
  union.opportunities || [],
  { founderReview: true }
).selected;
const annotated = annotateInventoryPopulations(selected, {
  hotelId: BETHESDA_HOTEL_ID,
});

check("bethesda_67_inventory_loaded", () => {
  assert.equal(annotated.length, 67);
});

check("acvnu_bethesda_classified", () => {
  const acvnu = annotated.find(
    (o) => String(o.id || "") === "gdi_opp_acvnu_renal_week_20270412"
  );
  assert.ok(acvnu);
  const dto = toOpportunityListDto(acvnu);
  assert.equal(
    dto.opportunityTypeFilterCategory,
    OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
  );
});

check("bethesda_no_unclassified", () => {
  assert.equal(findUnclassifiedCustomerVisibleOpportunities(annotated).length, 0);
  const counts = countByOpportunityTypeFilter(annotated);
  assert.equal(counts.OTHER_UNCLASSIFIED || 0, 0);
  assert.equal(counts.all, 67);
});

check("bethesda_market_scope_counts", () => {
  assert.equal(annotated.length, 67);
  assert.equal(
    annotated.filter((o) =>
      (o.inventoryPopulations || []).includes("SALES_OPPORTUNITIES")
    ).length,
    61
  );
  assert.equal(
    annotated.filter((o) =>
      (o.inventoryPopulations || []).includes("COMPETITIVE_MARKET_INTELLIGENCE")
    ).length,
    10
  );
});

check("other_not_customer_facing_option", () => {
  assert.equal(isCustomerFacingOpportunityTypeFilter("OTHER_UNCLASSIFIED"), false);
  const opts = opportunityTypeFilterOptions({
    all: 10,
    OTHER_UNCLASSIFIED: 3,
    ASSOCIATION_CONFERENCE_GROUP: 5,
  });
  assert.equal(opts.some((o) => o.value === "OTHER_UNCLASSIFIED"), false);
});

check("frontend_hides_other_option", () => {
  const ui = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  assert.match(ui, /if \(value === "OTHER_UNCLASSIFIED"\) return;/);
});

{
  const name = "portfolio_recovery_from_baseline_audit";
  try {
    const before = JSON.parse(
      fs.readFileSync(
        path.join(
          ROOT,
          "reports/gdi-opportunity-type-classification-v1/00-multi-hotel-audit.json"
        ),
        "utf8"
      )
    );
    const affected = before.filter((r) => (r.otherCount || 0) > 0);
    let remaining = 0;
    let recovered = 0;
    for (const h of affected) {
      const live = await loadOpportunitiesCanonical(h.hotelId);
      const opps = annotateInventoryPopulations(live.opportunities || [], {
        hotelId: h.hotelId,
      });
      const byId = new Map(opps.map((o) => [String(o.id), o]));
      for (const id of h.unclassifiedIds || []) {
        const o = byId.get(String(id));
        if (!o) continue;
        const cat = classifyOpportunityTypeFilter(o);
        if (cat === OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED) remaining += 1;
        else recovered += 1;
      }
    }
    assert.ok(recovered >= 100, `expected >=100 recovered, got ${recovered}`);
    assert.equal(
      inferDemandSegmentIdFromResearchEvidence({
        segment: "Discovered",
        opportunityType: "FUTURE_WATCH",
      }).demandSegmentId,
      null
    );
    console.log(
      "PASS",
      name,
      `(recovered=${recovered}, remaining_research=${remaining})`
    );
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll opportunity-type classification checks passed");
