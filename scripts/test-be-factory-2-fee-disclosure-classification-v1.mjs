#!/usr/bin/env node
import assert from "assert";
import {
  BE_FACTORY_2_0_FEE_DISCLOSURE_CLASSIFICATION_V1,
  FEE_DISCLOSURE_CLASS,
  classifyFeeDisclosure,
} from "../lib/brand-explorer/factory-2/be-factory-2-fee-disclosure-classification-v1.js";

assert.equal(
  BE_FACTORY_2_0_FEE_DISCLOSURE_CLASSIFICATION_V1,
  "BE_FACTORY_2_0_FEE_DISCLOSURE_CLASSIFICATION_V1"
);

{
  const r = classifyFeeDisclosure({
    brandName: "SureStay Plus by Best Western",
    parentCompany: "Best Western",
    blocks: [
      {
        slotKey: "economics.fee.join",
        title: "Join",
        body: "Confirm current membership / affiliation exhibits. Amounts are not staged without a verified disclosure.",
      },
    ],
  });
  assert.equal(r.classification, FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL);
  assert.equal(r.certifiableWithoutAmounts, true);
  assert.equal(r.isResearchGap, false);
}

{
  const r = classifyFeeDisclosure({
    brandName: "Preferred Hotels & Resorts",
    blocks: [{ slotKey: "economics.fee.join", body: "Not publicly disclosed as a franchise schedule." }],
  });
  assert.equal(r.classification, FEE_DISCLOSURE_CLASS.D_NON_CONVENTIONAL_MODEL);
}

{
  const r = classifyFeeDisclosure({
    brandName: "Courtyard by Marriott",
    parentCompany: "Marriott International",
    blocks: [
      {
        slotKey: "economics.fee.operate",
        body: "Confirm current Marriott franchise / development fee exhibits. Amounts are not staged without a verified disclosure.",
      },
    ],
  });
  assert.equal(r.classification, FEE_DISCLOSURE_CLASS.C_NOT_PUBLICLY_DISCLOSED);
  assert.equal(r.isResearchGap, true, "FDD-likely parents remain research gaps until exhibit attached");
}

{
  const r = classifyFeeDisclosure({
    brandName: "AC Hotels by Marriott",
    parentCompany: "Marriott",
    blocks: [
      {
        slotKey: "economics.fee.operate",
        body: "Royalty 5% of rooms revenue per current FDD exhibit (illustrative verified).",
      },
    ],
  });
  assert.equal(r.classification, FEE_DISCLOSURE_CLASS.A_CURRENT_AUTHORITATIVE);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_FACTORY_2_0_FEE_DISCLOSURE_CLASSIFICATION_V1,
      membershipNotResearchGap: true,
      fddLikelyRemainsGap: true,
    },
    null,
    2
  )
);
