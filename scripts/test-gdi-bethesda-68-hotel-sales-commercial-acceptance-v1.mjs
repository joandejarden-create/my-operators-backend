#!/usr/bin/env node
/**
 * Permanent gate: Bethesda primary commercial list is hotel-sales defensible.
 * - Union 88 preserved (59+29)
 * - No forbidden IDs on commercial list
 * - ACCP Salt Lake out-of-market excluded
 * - Every commercial row has revenue type + contact coverage A/B/C/D
 * - Scorecard usability verdicts cover 100% of commercial rows
 * - No unpaid maturity promotion / publication
 */
import assert from "node:assert/strict";
import {
  buildBethesda68HotelSalesCommercialAcceptance,
  SALES_USABILITY,
} from "../lib/group-demand-intelligence/research/bethesda-68-hotel-sales-commercial-acceptance-v1.js";
import {
  BETHESDA_EXPECTED_UNION_COUNT,
  BETHESDA_PROTECTED_SCALING_IDS,
  COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
  sha256File,
  hotelOpportunitiesPath,
  BETHESDA_HOTEL_ID,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { primaryCommercialForbiddenIdList } from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";

const ACCP = "gdi_opp_accp_annual_meeting_20261001";

function main() {
  const acceptance = buildBethesda68HotelSalesCommercialAcceptance();
  const { lock, counts, rows } = acceptance;

  assert.equal(lock.unionCount, BETHESDA_EXPECTED_UNION_COUNT, "88 union preserved");
  assert.equal(lock.storeCount, 59, "59 canonical preserved");
  assert.equal(lock.rawIdsPreserved, true);
  assert.equal(lock.protectedScalingPresent, true);
  assert.deepEqual(lock.forbiddenLeaks, []);
  assert.ok(
    counts.commercial >= 60 && counts.commercial <= 70,
    `commercial count in band, got ${counts.commercial}`,
  );

  const storeSha = sha256File(hotelOpportunitiesPath(BETHESDA_HOTEL_ID));
  assert.equal(
    storeSha,
    COMMERCIAL_ACCEPTANCE_REMEDIATED_59_ROW_SHA256,
    "operational store SHA matches acceptance remediation",
  );
  assert.equal(lock.storeSha, storeSha);

  const ids = new Set(rows.map((r) => r.id));
  assert.equal(ids.has(ACCP), false, "ACCP out-of-market must not be on commercial list");
  for (const id of primaryCommercialForbiddenIdList()) {
    assert.equal(ids.has(id), false, `forbidden ${id} leaked`);
  }
  for (const id of BETHESDA_PROTECTED_SCALING_IDS) {
    assert.equal(ids.has(id), true, `scaling ${id} missing from commercial`);
  }

  assert.equal(counts.revenue.UNCONFIRMED || 0, 0, "no unconfirmed revenue types");
  assert.equal(
    (counts.revenue.GUEST_ROOMS || 0) +
      (counts.revenue.LOCAL_CATERING || 0) +
      (counts.revenue.BOTH || 0),
    counts.commercial,
  );

  for (const row of rows) {
    assert.ok(row.id, "row id");
    assert.ok(
      Object.values(SALES_USABILITY).includes(row.salesUsability),
      `usability ${row.salesUsability}`,
    );
    assert.ok(row.nextAction && row.nextAction.length >= 40, `next action ${row.id}`);
    assert.ok(
      ["GUEST_ROOMS", "LOCAL_CATERING", "BOTH"].includes(row.revenueType),
      `revenue ${row.id}`,
    );
    assert.ok(
      /^(A_|B_|C_|D_)/.test(row.contactCoverage || ""),
      `contact coverage ${row.id} ${row.contactCoverage}`,
    );
    assert.equal(row.displayEligible, true);
  }

  assert.equal(counts.usability[SALES_USABILITY.REMOVE_FROM_COMMERCIAL] || 0, 0);
  assert.ok(
    (counts.usability[SALES_USABILITY.ACTIONABLE_NOW] || 0) >= 1,
    "at least one ACTIONABLE_NOW",
  );
  assert.ok(
    (counts.usability[SALES_USABILITY.ACCOUNT_DEVELOPMENT] || 0) >= 5,
    "account-development band present",
  );

  // Engine SR stamps are not auto-persisted as product maturity
  assert.ok(
    acceptance.maturityPromotionCandidates.length >= 1,
    "maturity promotions staged for founder approval",
  );
  assert.equal(acceptance.paidAuthorized, false);
  assert.equal(acceptance.publicationAuthorized, false);

  console.log(
    JSON.stringify(
      {
        ok: true,
        commercialCount: counts.commercial,
        usability: counts.usability,
        revenue: counts.revenue,
        storeSha,
        unionCount: lock.unionCount,
      },
      null,
      2,
    ),
  );
}

main();
