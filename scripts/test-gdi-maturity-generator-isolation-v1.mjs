/**
 * test-gdi-maturity-generator-isolation-v1.mjs
 * SIGNAL/generator rows never appear as account opportunities via maturity.
 */
import assert from "node:assert/strict";
import {
  assignGdiMaturityState,
  GDI_MATURITY_STATE,
} from "../lib/group-demand-intelligence/gdi-maturity-v1.js";
import {
  isCustomerFacingOpportunity,
  isGeneratorOnlyCustomerRecord,
} from "../lib/group-demand-intelligence/customer-visibility.js";

const generators = [
  {
    id: "gdi_gen_1",
    title: "SailGP Rome 2027",
    isDemandGenerator: true,
    demandFamily: "DEMAND_GENERATOR",
    organizationName: "SailGP",
    customerVisible: true,
  },
  {
    id: "gdi_camp_1",
    title: "Maker Faire Rome",
    opportunityType: "DEMAND_CAMPAIGN",
    demandFamily: "DEMAND_CAMPAIGN",
    gdiCampaignShell: true,
    organizationName: "Maker Faire",
  },
];

for (const g of generators) {
  assert.equal(isGeneratorOnlyCustomerRecord(g), true);
  const m = assignGdiMaturityState(g);
  assert.equal(m.gdiMaturityState, GDI_MATURITY_STATE.SIGNAL, g.id);
  assert.equal(
    isCustomerFacingOpportunity(
      { ...g, gdiMaturityState: m.gdiMaturityState },
      {
        env: {
          GDI_MATURITY_FUNNEL_V1: "1",
          GDI_QUALIFIED_CUSTOMER_VISIBILITY_V1: "1",
        },
      }
    ),
    false,
    `generator must not be customer-facing: ${g.id}`
  );
}

console.log("test-gdi-maturity-generator-isolation-v1: PASS");
