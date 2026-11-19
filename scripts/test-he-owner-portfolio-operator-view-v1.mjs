#!/usr/bin/env node
/**
 * Owner portfolio ↔ operator group reconciliation + verified/proposed scope gates.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildOwnerPortfolioOperatorView,
  classifyPortfolioHotel,
  dedupeOwnedHotels,
  partitionPortfolioHotels,
  selectHotelsForScope,
  OPERATOR_NOT_VERIFIED_LABEL,
  OWNER_OPERATED_LABEL,
  PORTFOLIO_EVIDENCE,
  PORTFOLIO_SCOPE,
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
      verification_bucket: "HIGH",
      economic_owner_verified: true,
    },
    {
      hotel_name: "A duplicate",
      airtable_record_id: "recA",
      rooms: 100,
      include_as_owned: true,
      operator: "Marriott International",
      verification_bucket: "HIGH",
      economic_owner_verified: true,
    },
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].operator, "Marriott International");
});

run("proposed_not_classified_as_verified", () => {
  const proposed = classifyPortfolioHotel({
    hotel_name: "AC Hotel",
    include_as_owned: true,
    verification_bucket: "PROPOSED",
    census_match_status: "UNRESOLVED_HOTEL_CANDIDATE",
    economic_owner_verified: false,
  });
  assert.equal(proposed.class, PORTFOLIO_EVIDENCE.UNRESOLVED_MATCH);
  assert.ok(/Pending HPC Match/i.test(proposed.label));

  const beth = classifyPortfolioHotel({
    hotel_name: "Bethesda Marriott",
    airtable_record_id: "recLuxvwwxID7U2B8",
    include_as_owned: true,
    verification_bucket: "PROPOSED",
    ownership_evidence_class: "PROPOSED",
  });
  assert.equal(beth.class, PORTFOLIO_EVIDENCE.PROPOSED);
  assert.notEqual(beth.class, PORTFOLIO_EVIDENCE.VERIFIED);

  const verified = classifyPortfolioHotel({
    hotel_name: "KGPV",
    airtable_record_id: "recUNycnMwOVFX0hc",
    include_as_owned: true,
    confidence: "HIGH",
    economic_owner_verified: true,
  });
  assert.equal(verified.class, PORTFOLIO_EVIDENCE.VERIFIED);
});

run("scope_filter_keeps_sections_reconciled", () => {
  const rows = [
    {
      hotel_name: "Verified Hotel",
      airtable_record_id: "recV",
      rooms: 100,
      include_as_owned: true,
      confidence: "HIGH",
      economic_owner_verified: true,
      operator: "Op A",
    },
    {
      hotel_name: "Proposed Hotel",
      airtable_record_id: "recP",
      rooms: 50,
      include_as_owned: true,
      verification_bucket: "PROPOSED",
      operator: "Op B",
    },
    {
      hotel_name: "Historical Hotel",
      airtable_record_id: "recH",
      rooms: 999,
      include_as_owned: true,
      current_historical: "HISTORICAL",
      confidence: "HIGH",
      economic_owner_verified: true,
      operator: "Op C",
    },
    {
      hotel_name: "Managed only",
      airtable_record_id: "recM",
      rooms: 10,
      include_as_owned: false,
      relationship_type: "OPERATED_BY",
      operator: "Op A",
    },
  ];
  const verifiedOnly = buildOwnerPortfolioOperatorView(rows, {
    ownerDisplayName: "Owner",
    scope: PORTFOLIO_SCOPE.VERIFIED_ONLY,
  });
  assert.equal(verifiedOnly.totals.hotels, 1);
  assert.equal(verifiedOnly.totals.roomsKnown, 100);
  assert.equal(verifiedOnly.reconciliation.ok, true);
  assert.ok(!verifiedOnly.ownedHotels.some((h) => /Proposed|Historical|Managed/i.test(h.hotel_name)));

  const withProposed = buildOwnerPortfolioOperatorView(rows, {
    ownerDisplayName: "Owner",
    scope: PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED,
  });
  assert.equal(withProposed.totals.hotels, 2);
  assert.equal(withProposed.totals.roomsKnown, 150);
  assert.equal(withProposed.reconciliation.hotelsMatch, true);
  assert.equal(withProposed.reconciliation.roomsMatch, true);
  assert.equal(
    withProposed.operatorGroups.reduce((s, g) => s + g.hotelCount, 0),
    withProposed.totals.hotels
  );
});

run("unknown_rooms_excluded_from_known_sum", () => {
  const view = buildOwnerPortfolioOperatorView(
    [
      {
        hotel_name: "Known",
        airtable_record_id: "rec1",
        rooms: 407,
        include_as_owned: true,
        verification_bucket: "PROPOSED",
        operator: "Marriott International",
      },
      {
        hotel_name: "Unknown rooms",
        airtable_record_id: null,
        rooms: null,
        include_as_owned: true,
        verification_bucket: "PROPOSED",
        census_match_status: "UNRESOLVED_HOTEL_CANDIDATE",
        operator: null,
      },
    ],
    {
      ownerDisplayName: "Mullen Real Estate Capital",
      scope: PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED,
    }
  );
  assert.equal(view.totals.hotels, 2);
  assert.equal(view.totals.roomsKnown, 407);
  assert.equal(view.totals.roomsKnownHotelCount, 1);
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
      confidence: "HIGH",
      economic_owner_verified: true,
      operator: "Marriott International",
    },
    {
      hotel_name: "H2",
      airtable_record_id: "r2",
      rooms: 200,
      include_as_owned: true,
      confidence: "HIGH",
      economic_owner_verified: true,
      operator: "Marriott International",
    },
    {
      hotel_name: "H3",
      airtable_record_id: "r3",
      rooms: 50,
      include_as_owned: true,
      confidence: "HIGH",
      economic_owner_verified: true,
      operator: "Owner Co",
      owner_operated: true,
    },
    {
      hotel_name: "H4",
      airtable_record_id: "r4",
      rooms: null,
      include_as_owned: true,
      confidence: "HIGH",
      economic_owner_verified: true,
      operator: null,
    },
  ];
  const view = buildOwnerPortfolioOperatorView(owned, {
    ownerDisplayName: "Owner Co",
    scope: PORTFOLIO_SCOPE.VERIFIED_ONLY,
  });
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
  assert.equal(ownerOp.hotelCount, 1);
  assert.equal(unverified.hotelCount, 1);
});

run("mullen_staged_portfolio_scopes", () => {
  const staged = JSON.parse(
    read("reports/hotel-explorer/phase4-quality/HE-BETH-MULLEN-OWNER-PORTFOLIO-STAGED-V1.json")
  );
  assert.equal(staged.applyAuthorized, false);
  const parts = partitionPortfolioHotels(staged.owned_controlled_current);
  assert.equal(parts.verified.length, 0);
  assert.ok(parts.proposed.length + parts.unresolved_match.length === 8);
  const verifiedOnly = buildOwnerPortfolioOperatorView(staged.owned_controlled_current, {
    ownerDisplayName: staged.owner.display_name,
    scope: PORTFOLIO_SCOPE.VERIFIED_ONLY,
  });
  assert.equal(verifiedOnly.totals.hotels, 0);
  assert.equal(verifiedOnly.reconciliation.ok, true);
  const withProposed = buildOwnerPortfolioOperatorView(staged.owned_controlled_current, {
    ownerDisplayName: staged.owner.display_name,
    scope: PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED,
  });
  assert.equal(withProposed.totals.hotels, 8);
  assert.equal(withProposed.totals.roomsKnown, 407);
  assert.equal(withProposed.totals.roomsKnownHotelCount, 1);
  assert.equal(withProposed.reconciliation.ok, true);
  assert.ok(
    withProposed.ownedHotels.every((h) => h.ownership_evidence_class !== PORTFOLIO_EVIDENCE.VERIFIED)
  );
  const beth = withProposed.ownedHotels.find((h) => h.airtable_record_id === "recLuxvwwxID7U2B8");
  assert.ok(beth);
  assert.equal(beth.pending_hpc_match, false);
  assert.ok(withProposed.ownedHotels.filter((h) => h.pending_hpc_match).length === 7);
});

run("gsf_fixture_verified_owned_subset", () => {
  const fixture = JSON.parse(
    read("fixtures/hotel-intelligence/owner-portfolio/dle_06G6AB1VK0BCCD94DNN7W8DRWZ.json")
  );
  const owned = (fixture.profile.hotel_relationships || []).filter((h) => h.include_as_owned);
  const verifiedOnly = buildOwnerPortfolioOperatorView(owned, {
    ownerDisplayName: fixture.profile.owner_display_name,
    scope: PORTFOLIO_SCOPE.VERIFIED_ONLY,
  });
  assert.ok(verifiedOnly.totals.hotels >= 1);
  assert.equal(verifiedOnly.reconciliation.hotelsMatch, true);
  assert.ok(
    verifiedOnly.ownedHotels.every((h) => h.ownership_evidence_class === PORTFOLIO_EVIDENCE.VERIFIED)
  );
  assert.ok(!verifiedOnly.ownedHotels.some((h) => h.include_as_owned === false));
});

run("historical_excluded_from_scopes", () => {
  const selected = selectHotelsForScope(
    [
      {
        hotel_name: "Sold",
        include_as_owned: true,
        current_historical: "HISTORICAL",
        confidence: "HIGH",
        economic_owner_verified: true,
      },
    ],
    PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED
  );
  assert.equal(selected.length, 0);
});

run("ui_wiring_present", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  assert.ok(ws.includes("Who Manages These Hotels?"));
  assert.ok(ws.includes("Owned / Controlled Hotels"));
  assert.ok(ws.includes("data-hex-portfolio-scope"));
  assert.ok(ws.includes("Pending HPC Match"));
  assert.ok(ws.includes("Verified only"));
  assert.ok(ws.includes("Verified + proposed"));
  assert.ok(!ws.includes(">Operated / Managed Hotels<"));
  const preview = read("public/hotel-explorer-preview.html");
  assert.ok(preview.includes("he-owner-portfolio-operator-view-v1.js"));
  const he = read("public/js/hotel-explorer.js");
  assert.ok(he.includes("he-owner-portfolio-scope-v1"));
  const api = read("api/hotel-explorer-founder-preview.js");
  assert.ok(api.includes("HE-BETH-MULLEN-OWNER-PORTFOLIO-STAGED-V1.json"));
});

if (failures.length) {
  console.error(`\n${failures.length} gate(s) failed`);
  process.exit(1);
}
console.log("\nAll owner portfolio operator view gates passed.");
