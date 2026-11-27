#!/usr/bin/env node
/**
 * Regression: consolidated HE cross-tab consistency report (factory gate).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCrossTabConsistencyReport,
  renderCrossTabConsistencyMarkdown,
  writeCrossTabConsistencyArtifacts,
  HE_CONSISTENCY_RESULT,
  HE_ACTIVE_CONSISTENCY_TABS,
} from "../lib/hotel-intelligence/he-factory/he-cross-tab-consistency-report-v1.js";
import { buildSharedHotelViewModel } from "../lib/hotel-intelligence/he-shared-hotel-view-model-v1.js";
import { buildOwnerPortfolioOperatorView } from "../lib/hotel-intelligence/ownership/owner-portfolio-operator-view-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const TMP = path.join(ROOT, "reports/hotel-explorer/_consistency-test");

assert.equal(HE_ACTIVE_CONSISTENCY_TABS.length, 11);
assert.ok(!HE_ACTIVE_CONSISTENCY_TABS.includes("sentiment"));

const hpcHotel = {
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

const ownership = {
  hotel: { ...hpcHotel },
  report: {
    ownership_and_control: {
      property_company: { name: "Marbeth Acquisition", verification_state: "Validated" },
      economic_owner: {
        name: "Mullen Real Estate Capital",
        founder_classification: "Proposed",
      },
      operator: { name: "Marriott International" },
    },
    corporate_contacts: { company_email: "info@mullencap.com" },
  },
  organization: {
    owned_controlled_hotels: [
      {
        airtable_record_id: "recLuxvwwxID7U2B8",
        focus_hotel: true,
        rooms: 407,
        rooms_known: true,
        brand_display: "Marriott Hotels",
        market_display: "Bethesda / Montgomery County",
        operator: "Marriott International",
        status: "Open",
        economic_owner_verified: true,
        include_as_owned: true,
      },
    ],
  },
};

const shared = buildSharedHotelViewModel({
  hpcHotel,
  ownership,
  hotelId: "recLuxvwwxID7U2B8",
});
const portfolioView = buildOwnerPortfolioOperatorView(
  ownership.organization.owned_controlled_hotels,
  { sharedHotelView: shared, focusHotelId: "recLuxvwwxID7U2B8" }
);
const submarketSnapshot = {
  ok: true,
  selectedHotel: hpcHotel,
  hotels: [hpcHotel],
  coverage: { partial: true, disclosed: true, marketShareFabricated: false },
  cohorts: {
    matchedHpcInventory: { id: "matched_hpc_inventory", hotels: [hpcHotel] },
    geographicSubmarketInventory: {
      id: "geographic_submarket_inventory",
      hotels: [hpcHotel],
    },
    competitiveSet: { id: "property_relevant_competitive_set", hotels: [hpcHotel] },
  },
};

// 1) SkipBrowser → browserResult NOT_TESTED, never PASS
const skipped = buildCrossTabConsistencyReport({
  hotelId: "recLuxvwwxID7U2B8",
  mode: "Validate",
  hpcHotel,
  ownership,
  portfolioView,
  submarketSnapshot,
  browserEnabled: false,
  shared,
});
assert.equal(skipped.browserEnabled, false);
assert.equal(skipped.ok, true);
assert.equal(skipped.gates.HE_E2E_CROSS_TAB_CERTIFICATION, "YES");
assert.equal(skipped.gates.HE_CROSS_TAB_CONSISTENCY_REPORT_GENERATED, "YES");
for (const attr of skipped.attributes) {
  for (const row of attr.tabResults) {
    assert.notEqual(row.browserResult, HE_CONSISTENCY_RESULT.PASS);
    if (row.result !== HE_CONSISTENCY_RESULT.NOT_APPLICABLE) {
      assert.equal(row.browserResult, HE_CONSISTENCY_RESULT.NOT_TESTED);
    }
  }
}

// 2) Verified rooms missing on Org → FAIL (mapping defect)
const missingOrg = buildCrossTabConsistencyReport({
  hotelId: "recLuxvwwxID7U2B8",
  mode: "Audit",
  hpcHotel,
  ownership,
  portfolioView: {
    ownedHotels: [
      {
        airtable_record_id: "recLuxvwwxID7U2B8",
        focus_hotel: true,
        rooms: null,
        rooms_known: false,
        brand_display: "Marriott Hotels",
        market_display: "Bethesda / Montgomery County",
        operator: "Marriott International",
        economic_owner_verified: true,
        include_as_owned: true,
      },
    ],
  },
  submarketSnapshot,
  browserEnabled: false,
  shared,
});
assert.equal(missingOrg.ok, false);
assert.equal(missingOrg.gates.HE_E2E_CROSS_TAB_CERTIFICATION, "NO");
assert.ok(
  missingOrg.summary.unresolvedVerifiedOrProposedFails.includes("portfolioFocusRooms") ||
    missingOrg.summary.unresolvedVerifiedOrProposedFails.includes("rooms")
);

// 3) Markdown + JSON artifacts
fs.mkdirSync(TMP, { recursive: true });
const md = renderCrossTabConsistencyMarkdown(skipped);
assert.match(md, /Cross-Tab Consistency Report/);
assert.match(md, /E2E certification/);
const arts = writeCrossTabConsistencyArtifacts(skipped, {
  reportsDir: TMP,
  hotelId: "recLuxvwwxID7U2B8",
});
assert.ok(fs.existsSync(arts.jsonPath));
assert.ok(fs.existsSync(arts.mdPath));

// 4) PS1 / runner wiring present
const ps1 = fs.readFileSync(
  path.join(ROOT, "scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1"),
  "utf8"
);
assert.match(ps1, /HE-CROSS-TAB-CONSISTENCY/);
assert.match(ps1, /SkipBrowser/);
assert.match(ps1, /Audit/);
assert.match(ps1, /HE_E2E_CROSS_TAB_CERTIFICATION/);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:he-cross-tab-consistency-report-v1",
    heCrossTabConsistencyReportGenerated: "YES",
    heE2eCrossTabCertification: "YES",
    skipBrowserNeverPass: true,
    missingVerifiedFailsE2e: true,
  })
);
