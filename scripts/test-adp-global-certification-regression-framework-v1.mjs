#!/usr/bin/env node
/**
 * Fast gate for global ADP certification framework contracts.
 */

import assert from "assert/strict";
import { assertMissingAliasWouldFailCanary } from "../lib/ai-demand-positioning/certification/adp-hotel-identity-contract-v1.js";
import {
  evaluateAdpComparability,
  ADP_COMPARABILITY_OUTCOMES,
  buildCustomerComparisonGate,
} from "../lib/ai-demand-positioning/certification/adp-comparability-engine-v1.js";
import { auditSourceAttributionLabels } from "../lib/ai-demand-positioning/certification/adp-source-attribution-contract-v1.js";
import { resolveDomainOwnershipForProperty } from "../lib/ai-demand-positioning/certification/adp-domain-ownership-registry-v1.js";
import { forensicProviderZeroPresence } from "../lib/ai-demand-positioning/certification/adp-anomaly-rules-v1.js";
import { assertPeriodMutableForMetricWrite } from "../lib/ai-demand-positioning/certification/adp-period-immutability-v1.js";
import { ADP_REGRESSION_CONTROL_HOTELS_V1 } from "../lib/ai-demand-positioning/certification/adp-regression-control-set-v1.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { isCustomerOfficialAllowed } from "../lib/ai-demand-positioning/certification/adp-period-pipeline-states-v1.js";

const hilton = loadPropertyProfile("adp_hilton_times_square");
assert.ok(hilton, "hilton profile");

const alias = assertMissingAliasWouldFailCanary("adp_hilton_times_square", "Hilton Times Square");
assert.equal(alias.missingAliasWouldFailCanary, true, "missing Hilton Times Square alias must fail canary");

const a = {
  periodId: "a50",
  scenarioIds: Array.from({ length: 50 }, (_, i) => `s${i}`),
  providerSet: ["openai", "gemini", "perplexity", "claude"],
};
const b = {
  periodId: "b65",
  scenarioIds: [...a.scenarioIds, ...Array.from({ length: 15 }, (_, i) => `p${i}`)],
  providerSet: ["openai", "gemini", "perplexity", "claude"],
};
const comp = evaluateAdpComparability(a, b);
assert.notEqual(comp.outcome, ADP_COMPARABILITY_OUTCOMES.EXACT_COMPARABLE);
const gate = buildCustomerComparisonGate(ADP_COMPARABILITY_OUTCOMES.NOT_COMPARABLE);
assert.equal(gate.allowComparativeLanguage, false);

assert.equal(
  resolveDomainOwnershipForProperty("marriott.com", hilton).ownershipType,
  "COMPETITOR_OWNED"
);
assert.equal(resolveDomainOwnershipForProperty("hilton.com", hilton).ownershipType, "BRAND_OWNED");

const mislabel = auditSourceAttributionLabels(
  { observations: [{ sourcesCited: [{ url: "https://www.marriott.com/x" }] }] },
  hilton,
  { claimedTopSourceDomain: "marriott.com" }
);
assert.ok(
  mislabel.hardFailures.some((f) => f.code === "COMPETITOR_SOURCE_MISLABELED_AS_PROPERTY_TOP_SOURCE")
);

const zero = forensicProviderZeroPresence(
  {
    observations: Array.from({ length: 10 }, (_, i) => ({
      provider: "openai",
      scenarioId: `s${i}`,
      mentioned: false,
      rawResponse: "A list of Midtown hotels without the subject.",
      parsed: true,
    })),
  },
  "openai"
);
assert.equal(zero.zeroPresence, true);
assert.ok(zero.explanationClass);

const imm = assertPeriodMutableForMetricWrite({
  certified: true,
  certificationStatus: "CERTIFIED",
});
assert.equal(imm.allowed, false);

assert.equal(isCustomerOfficialAllowed("CERTIFIED"), true);
assert.equal(isCustomerOfficialAllowed("QA_FAILED"), false);
assert.ok(ADP_REGRESSION_CONTROL_HOTELS_V1.length >= 8);

console.log("test:adp-global-certification-regression-framework-v1 PASS");
