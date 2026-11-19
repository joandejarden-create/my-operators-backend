#!/usr/bin/env node
/**
 * Owner portfolio ↔ operator group reconciliation gates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildOwnerPortfolioOperatorView,
  dedupeOwnedHotels,
  OPERATOR_NOT_VERIFIED_LABEL,
  OWNER_OPERATED_LABEL,
} from "../lib/hotel-intelligence/ownership/owner-portfolio-operator-view-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

const failures = [];
function run(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failures.push({ name, message: err.message });
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

run("dedupe_by_canonical_property_id", () => {
  const rows = dedupeOwnedHotels([
    {
      hotel_name: "A",
      airtable_record_id: "recA",
      rooms: 100,
      include_as_owned: true,
      operator: null,
    },
    {
      hotel_name: "A duplicate",
      airtable_record_id: "recA",
      rooms: 100,
      include_as_owned: true,
      operator: "Marriott International",
    },
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].operator, "Marriott International");
});

run("unknown_rooms_excluded_from_known_sum", () => {
  const view = buildOwnerPortfolioOperatorView(
    [
      {
        hotel_name: "Known",
        airtable_record_id: "rec1",
        rooms: 407,
        include_as_owned: true,
        operator: "Marriott International",
      },
      {
        hotel_name: "Unknown rooms",
        airtable_record_id: null,
        rooms: null,
        include_as_owned: true,
        operator: null,
      },
    ],
    { ownerDisplayName: "Mullen Real Estate Capital" }
  );
  assert.equal(view.totals.hotels, 2);
  assert.equal(view.totals.roomsKnown, 407);
  assert.equal(view.totals.roomsUnknownHotelCount, 1);
  assert.equal(view.reconciliation.ok, true);
});

run("portfolio_and_operator_totals_match", () => {
  const owned = [
    {
      hotel_name: "H1",
      airtable_record_id: "r1",
      rooms: 100,
      include_as_owned: true,
      operator: "Marriott International",
    },
    {
      hotel_name: "H2",
      airtable_record_id: "r2",
      rooms: 200,
      include_as_owned: true,
      operator: "Marriott International",
    },
    {
      hotel_name: "H3",
      airtable_record_id: "r3",
      rooms: 50,
      include_as_owned: true,
      operator: "Owner Co",
      owner_operated: true,
    },
    {
      hotel_name: "H4",
      airtable_record_id: "r4",
      rooms: null,
      include_as_owned: true,
      operator: null,
    },
  ];
  const view = buildOwnerPortfolioOperatorView(owned, { ownerDisplayName: "Owner Co" });
  assert.equal(view.reconciliation.hotelsMatch, true);
  assert.equal(view.reconciliation.roomsMatch, true);
  assert.equal(view.totals.hotels, 4);
  assert.equal(view.totals.roomsKnown, 350);
  const marriott = view.operatorGroups.find((g) => g.operatorName === "Marriott International");
  const ownerOp = view.operatorGroups.find((g) => g.operatorName === OWNER_OPERATED_LABEL);
  const unverified = view.operatorGroups.find(
    (g) => g.operatorName === OPERATOR_NOT_VERIFIED_LABEL
  );
  assert.equal(marriott.hotelCount, 2);
  assert.equal(marriott.roomsKnown, 300);
  assert.equal(ownerOp.hotelCount, 1);
  assert.equal(unverified.hotelCount, 1);
  assert.equal(
    marriott.hotelCount + ownerOp.hotelCount + unverified.hotelCount,
    view.totals.hotels
  );
});

run("excludes_operated_only_from_owned_portfolio", () => {
  const view = buildOwnerPortfolioOperatorView(
    [
      {
        hotel_name: "Owned",
        airtable_record_id: "ro",
        rooms: 10,
        include_as_owned: true,
        relationship_type: "OWNED_BY",
        operator: "Op",
      },
      {
        hotel_name: "Managed only",
        airtable_record_id: "rm",
        rooms: 999,
        include_as_owned: false,
        relationship_type: "OPERATED_BY",
        operator: "Op",
      },
    ],
    { ownerDisplayName: "Owner" }
  );
  assert.equal(view.totals.hotels, 1);
  assert.equal(view.totals.roomsKnown, 10);
  assert.ok(!view.ownedHotels.some((h) => h.hotel_name === "Managed only"));
});

run("excludes_historical_from_current_totals", () => {
  const view = buildOwnerPortfolioOperatorView(
    [
      {
        hotel_name: "Current",
        airtable_record_id: "rc",
        rooms: 10,
        include_as_owned: true,
        current_historical: "CURRENT",
      },
      {
        hotel_name: "Sold",
        airtable_record_id: "rh",
        rooms: 500,
        include_as_owned: true,
        current_historical: "HISTORICAL",
      },
    ],
    { ownerDisplayName: "Owner" }
  );
  assert.equal(view.totals.hotels, 1);
  assert.equal(view.totals.roomsKnown, 10);
});

run("mullen_staged_portfolio_reconciles", () => {
  const staged = JSON.parse(
    read("reports/hotel-explorer/phase4-quality/HE-BETH-MULLEN-OWNER-PORTFOLIO-STAGED-V1.json")
  );
  assert.equal(staged.applyAuthorized, false);
  const view = buildOwnerPortfolioOperatorView(staged.owned_controlled_current, {
    ownerDisplayName: staged.owner.display_name,
  });
  assert.equal(view.totals.hotels, staged.owned_controlled_current.length);
  assert.equal(view.reconciliation.ok, true);
  assert.equal(view.totals.roomsKnown, 407);
  assert.ok(view.stats.hotelsOperatorUnresolved >= 1);
  const beth = view.ownedHotels.find((h) => h.airtable_record_id === "recLuxvwwxID7U2B8");
  assert.ok(beth);
  assert.equal(beth.operator, "Marriott International");
});

run("gsf_fixture_owned_subset_reconciles", () => {
  const fixture = JSON.parse(
    read("fixtures/hotel-intelligence/owner-portfolio/dle_06G6AB1VK0BCCD94DNN7W8DRWZ.json")
  );
  const owned = (fixture.profile.hotel_relationships || []).filter((h) => h.include_as_owned);
  const view = buildOwnerPortfolioOperatorView(owned, {
    ownerDisplayName: fixture.profile.owner_display_name,
  });
  assert.ok(view.totals.hotels >= 1);
  assert.equal(view.reconciliation.hotelsMatch, true);
  assert.equal(view.reconciliation.roomsMatch, true);
  assert.ok(!/OPERATOR\b/.test(JSON.stringify(view.ownedHotels)));
});

run("ui_wiring_present", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  assert.ok(ws.includes("Who Manages These Hotels?"));
  assert.ok(ws.includes("Owned / Controlled Hotels"));
  assert.ok(ws.includes("HeOwnerPortfolioOperatorView"));
  assert.ok(ws.includes("data-hex-op-toggle"));
  assert.ok(!ws.includes(">Operated / Managed Hotels<"));
  const preview = read("public/hotel-explorer-preview.html");
  assert.ok(preview.includes("he-owner-portfolio-operator-view-v1.js"));
  const api = read("api/hotel-explorer-founder-preview.js");
  assert.ok(api.includes("HE-BETH-MULLEN-OWNER-PORTFOLIO-STAGED-V1.json"));
  assert.ok(api.includes("owned_controlled_hotels"));
});

if (failures.length) {
  console.error(`\n${failures.length} gate(s) failed`);
  process.exit(1);
}
console.log("\nAll owner portfolio operator view gates passed.");
