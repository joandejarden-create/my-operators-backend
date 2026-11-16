#!/usr/bin/env node
import assert from "assert";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";

const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const profile = loadPropertyProfile(PROPERTY_ID);
const bpp = resolveBrandPortfolioPosition(PROPERTY_ID, profile, { query: {} });

assert.equal(profile.founderPreviewEligible, true);
assert.equal(bpp.status, "READY");
assert.notEqual(bpp.status, "AWAITING_FIRST_MONITORING");
assert.ok(!(bpp.customerState && /measurement is being established/i.test(bpp.customerState.statusLabel || "")));

const customerPublished =
  profile.customerDropdownVisible === true ||
  profile.officialBaselinePublished === true ||
  bpp.customerPublished === true ||
  bpp.assuranceStatus === "CUSTOMER_READY";

if (customerPublished) {
  assert.equal(profile.customerDropdownVisible, true);
  assert.equal(bpp.status, "READY");
  assert.ok(
    (bpp.kpis || []).some((k) => k.id === "portfolioRank" && String(k.value).includes("#1 of 6"))
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        gate: "test:adp-brand-portfolio-founder-staged-v1",
        founderReviewed: true,
        founderApproved: true,
        customerReady: true,
        customerPublished: true,
        portfolioMeasurementAvailable: true,
      },
      null,
      2
    )
  );
} else {
  assert.equal(profile.customerDropdownVisible, false);
  assert.equal(bpp.customerPublished, false);
  assert.equal(bpp.stagedReviewOnly !== false, true);
  assert.ok(
    bpp.assuranceStatus === "STAGED_COMPARISON_CERTIFIED" ||
      bpp.bppCustomerState === "BPP_COMPARISON_CERTIFIED"
  );
  console.log(
    JSON.stringify(
      {
        ok: true,
        gate: "test:adp-brand-portfolio-founder-staged-v1",
        founderReviewed: true,
        founderApproved: false,
        customerReady: false,
        customerPublished: false,
        portfolioMeasurementAvailable: true,
      },
      null,
      2
    )
  );
}
