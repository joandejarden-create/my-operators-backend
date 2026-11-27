#!/usr/bin/env node
/**
 * Regression: HE shared hotel view model + cross-tab parity + submarket cohorts.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildSharedHotelViewModel,
  hydratePortfolioHotelsFromSharedView,
} from "../lib/hotel-intelligence/he-shared-hotel-view-model-v1.js";
import {
  validateBethesdaGoldenParityFixture,
  validateCrossTabParity,
  HE_CROSS_TAB_PARITY_GATES,
} from "../lib/hotel-intelligence/he-cross-tab-parity-v1.js";
import {
  classifySubmarketGrouping,
  computeHpcSubmarketMetrics,
  dedupeHpcHotelsByIdentity,
} from "../lib/hotel-intelligence/he-hpc-submarket-snapshot-v1.js";
import { buildOwnerPortfolioOperatorView } from "../lib/hotel-intelligence/ownership/owner-portfolio-operator-view-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const beth = {
  id: "recLuxvwwxID7U2B8",
  name: "Bethesda Marriott",
  rooms: 407,
  brand: "Marriott Hotels",
  market: "Bethesda / Montgomery County",
  submarket: "Pooks Hill / Rockville Pike",
  managementCompany: "Marriott International",
  chainScale: "Upper Upscale",
  status: "Open",
  city: "Bethesda",
  country: "United States",
};

const shared = buildSharedHotelViewModel({
  hpcHotel: beth,
  ownership: {
    report: {
      ownership_and_control: {
        property_company: { name: "Marbeth Acquisition" },
        economic_owner: {
          name: "Mullen Real Estate Capital",
          founder_classification: "Proposed",
        },
        operator: { name: "Marriott International" },
      },
    },
  },
  hotelId: "recLuxvwwxID7U2B8",
});

assert.equal(shared.rooms, 407);
assert.equal(shared.brand, "Marriott Hotels");
assert.equal(shared.market, "Bethesda / Montgomery County");
assert.equal(shared.operator, "Marriott International");
assert.equal(shared.propertyCompany, "Marbeth Acquisition");
assert.equal(shared.economicOwnerClassification, "Proposed");

const unresolved = [
  {
    hotel_name: "Bethesda Marriott",
    airtable_record_id: "recLuxvwwxID7U2B8",
    focus_hotel: true,
    economic_owner_verified: true,
    include_as_owned: true,
  },
  {
    hotel_name: "Other Proposed Asset",
    airtable_record_id: null,
    rooms: null,
    economic_owner_verified: true,
    include_as_owned: true,
  },
];

const hydrated = hydratePortfolioHotelsFromSharedView(unresolved, shared, {
  focusHotelId: "recLuxvwwxID7U2B8",
});
assert.equal(hydrated[0].rooms, 407);
assert.equal(hydrated[0].rooms_known, true);
assert.equal(hydrated[0].brand_display, "Marriott Hotels");
assert.equal(hydrated[0].pending_hpc_match, false);
assert.equal(hydrated[1].rooms_known == null || hydrated[1].rooms == null, true);

const view = buildOwnerPortfolioOperatorView(unresolved, {
  ownerDisplayName: "Mullen Real Estate Capital",
  sharedHotelView: shared,
  focusHotelId: "recLuxvwwxID7U2B8",
});
const focus = view.ownedHotels.find(
  (h) => h.airtable_record_id === "recLuxvwwxID7U2B8"
);
assert.ok(focus);
assert.equal(focus.rooms, 407);
assert.equal(focus.rooms_known, true);

const grouping = classifySubmarketGrouping(beth);
assert.equal(grouping.mode, "submarket");
const matched = dedupeHpcHotelsByIdentity([beth]);
assert.equal(matched.length, 1);
assert.equal(computeHpcSubmarketMetrics(matched).openHotels, 1);

const fixture = validateBethesdaGoldenParityFixture();
assert.equal(fixture.ok, true, JSON.stringify(fixture.conflicts, null, 2));
assert.equal(
  fixture.gates[HE_CROSS_TAB_PARITY_GATES.HE_CROSS_TAB_PARITY_VALIDATED],
  "YES"
);
assert.equal(
  fixture.gates[HE_CROSS_TAB_PARITY_GATES.HE_ORG_PORTFOLIO_HYDRATION_VALIDATED],
  "YES"
);
assert.equal(
  fixture.gates[HE_CROSS_TAB_PARITY_GATES.HE_SUBMARKET_COHORT_VALIDATED],
  "YES"
);
assert.equal(
  fixture.gates[HE_CROSS_TAB_PARITY_GATES.HE_SUBMARKET_COVERAGE_DISCLOSED],
  "YES"
);

// Conflict must fail closed
const conflicted = validateCrossTabParity({
  hpcHotel: beth,
  ownership: {
    report: {
      ownership_and_control: {
        property_company: { name: "Marbeth Acquisition" },
        economic_owner: {
          name: "Mullen Real Estate Capital",
          founder_classification: "Proposed",
        },
        operator: { name: "Marriott International" },
      },
    },
  },
  overview: { rooms: 407, brand: "Marriott Hotels", market: beth.market, operator: beth.managementCompany, propertyCompany: "Marbeth Acquisition", economicOwnerClassification: "Proposed", chainScale: beth.chainScale, status: "Open", location: "Bethesda, United States" },
  organizationPortfolio: {
    ownedHotels: [
      {
        airtable_record_id: "recLuxvwwxID7U2B8",
        focus_hotel: true,
        rooms: 200,
        rooms_known: true,
        brand_display: "Marriott Hotels",
        market_display: beth.market,
        operator: beth.managementCompany,
      },
    ],
    propertyCompany: "Marbeth Acquisition",
    economicOwnerClassification: "Proposed",
  },
  submarketSnapshot: {
    ok: true,
    selectedHotel: beth,
    coverage: { disclosed: true, marketShareFabricated: false, partial: true },
    cohorts: {
      matchedHpcInventory: { hotels: [beth] },
      geographicSubmarketInventory: { hotels: [beth] },
      competitiveSet: { hotels: [beth] },
    },
  },
  hotelId: "recLuxvwwxID7U2B8",
});
assert.equal(conflicted.ok, false);
assert.ok(conflicted.conflicts.some((c) => c.field === "rooms"));

// Source files present
assert.ok(
  fs.existsSync(
    path.join(ROOT, "lib/hotel-intelligence/he-shared-hotel-view-model-v1.js")
  )
);
assert.ok(
  fs
    .readFileSync(
      path.join(ROOT, "lib/hotel-intelligence/he-hpc-submarket-snapshot-v1.js"),
      "utf8"
    )
    .includes("geographicSubmarketInventory")
);
assert.ok(
  fs
    .readFileSync(path.join(ROOT, "api/golden-demo-ownership.js"), "utf8")
    .includes("sharedHotelView")
);

// Eight-hotel IDs must not hardcode Bethesda rooms
const eightPath = path.join(
  ROOT,
  "configs/hotel-explorer/he50-eight-hotel-batch-v1.json"
);
if (fs.existsSync(eightPath)) {
  const eight = JSON.parse(fs.readFileSync(eightPath, "utf8"));
  const hotels = eight.hotels || eight.batch || [];
  for (const h of hotels) {
    if (h.hpcId && h.hpcId !== "recLuxvwwxID7U2B8") {
      assert.notEqual(h.rooms, 407, "non-Bethesda hotel must not inherit Bethesda rooms");
    }
  }
}

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:he-cross-tab-parity-v1",
    heSharedHotelAttributesConsistent: "YES",
    heOrgPortfolioHydrationValidated: "YES",
    heSubmarketCohortValidated: "YES",
    heSubmarketCoverageDisclosed: "YES",
    heCrossTabParityValidated: "YES",
  })
);
