/**
 * Zero-paid targeted research gates — no paid calls, no Sales Ready, store untouched.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  stageBethesda29TargetedCommercialResearch,
  PRIORITY_TARGET_IDS,
  PROPOSED_PAID_BATCH_V1,
} from "../lib/group-demand-intelligence/research/bethesda-29-targeted-commercial-research-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const EXPECTED =
  "fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad";

const checks = [];
function check(name, fn) {
  try {
    fn();
    checks.push({ name, ok: true });
  } catch (err) {
    checks.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const oppPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "opportunities.json"
);
const beforeHash = crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex");

const staged = stageBethesda29TargetedCommercialResearch({
  root: ROOT,
  writeFixture: true,
  writeReports: true,
});
const doc = staged.doc;

check("seven_priority_enriched", () => {
  assert.equal(doc.counts.enriched, 7);
  assert.equal(PRIORITY_TARGET_IDS.length, 7);
});

check("no_sales_ready_and_paid_not_authorized", () => {
  assert.equal(doc.counts.salesReady, 0);
  assert.equal(doc.safety.paidResearchAuthorized, false);
  assert.equal(PROPOSED_PAID_BATCH_V1.authorized, false);
  assert.equal(PROPOSED_PAID_BATCH_V1.opportunityIds.length, 3);
  for (const row of doc.enrichedOnly) {
    assert.equal(row.salesReady, false);
    assert.notEqual(row.commercialMaturity, "SALES_READY");
  }
});

check("categories_still_classified", () => {
  for (const id of PRIORITY_TARGET_IDS) {
    const row = doc.enrichedOnly.find((o) => o.id === id);
    assert.ok(row, id);
    assert.notEqual(
      classifyOpportunityTypeFilter(row),
      OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED,
      id
    );
  }
});

check("named_contacts_where_publicly_verified", () => {
  const chamber = doc.enrichedOnly.find(
    (o) => o.id === "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce"
  );
  assert.equal(chamber.primaryContact?.name, "Clarissa Harris");
  const cohera = doc.enrichedOnly.find(
    (o) => o.id === "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_"
  );
  assert.equal(cohera.primaryContact?.name, "Annette Hawks");
  const iwlca = doc.enrichedOnly.find(
    (o) =>
      o.id === "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup"
  );
  assert.equal(iwlca.commercialMaturity, "OVERFLOW_WATCH");
});

const canonicalDoc = await loadOpportunitiesCanonical(HOTEL);
const union = mergeBethesdaInternalResearchOpportunities(
  HOTEL,
  canonicalDoc.opportunities || [],
  { root: ROOT }
);

check("union_88_and_customer_isolation", () => {
  assert.equal(union.opportunities.length, 88);
  const facing = filterCustomerFacingOpportunities(union.opportunities, {
    hotelId: HOTEL,
  });
  assert.equal(
    facing.filter((o) => BETHESDA_29_ATTESTED_IDS.includes(o.id || o.opportunityId))
      .length,
    0
  );
});

check("store_hash_unchanged", () => {
  const after = crypto.createHash("sha256").update(fs.readFileSync(oppPath)).digest("hex");
  assert.equal(after, beforeHash);
  assert.equal(after, EXPECTED);
});

const failed = checks.filter((c) => !c.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      beforeHash,
      counts: doc.counts,
      proposedPaidBatch: PROPOSED_PAID_BATCH_V1,
      checks,
      stop: "AWAITING_FOUNDER_PAID_APPROVAL",
    },
    null,
    2
  )
);
if (failed.length) process.exit(1);
