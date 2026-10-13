#!/usr/bin/env node
/**
 * Guard: W Rome ADP baseline completion — entity registry + attribute definitions + rebuild integrity.
 * No LLM calls. Uses existing period observations.
 */
import assert from "node:assert/strict";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { getEntityRegistryForProperty } from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";
import { getPortfolioMapping } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-affiliation-mapping-v1.js";
import { resolveCustomerFacingEntity } from "../lib/ai-demand-positioning/customer/customer-entity-resolution-v1.js";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const PROPERTY_ID = "adp_w_rome";
const PERIOD_ID = "adp_period_adp_w_rome_20260925105322_1192cf";

const profile = loadPropertyProfile(PROPERTY_ID);
assert.ok(profile, "profile");
assert.ok(getEntityRegistryForProperty(PROPERTY_ID), "rome entity registry wired");
assert.ok(getPortfolioMapping(PROPERTY_ID)?.defaultLensId === "marriott_bonvoy", "portfolio affiliation mapped");

// Profile attributes must resolve in Reality Gap (no silent drop of undefined keys)
const attrs = profile.attributes || [];
assert.ok(attrs.length >= 5, `expected >=5 tracked attributes, got ${attrs.length}`);
assert.ok(attrs.includes("marriott_bonvoy"));
assert.ok(attrs.includes("meeting_space"));
assert.ok(!attrs.includes("rome_destination"), "rome-specific undefined attr keys removed");

const period = loadPeriod(PERIOD_ID);
assert.equal(period?.observations?.length, 80, "80 observations preserved");

// Certified peers bind
for (const name of [
  "Hotel de Russie",
  "The St. Regis Rome",
  "The Rome EDITION",
  "Hotel Eden",
  "Westin Excelsior Rome",
]) {
  const r = resolveCustomerFacingEntity(name, profile);
  assert.equal(r.ok, true, `peer/observed bind: ${name} → ${r.reason}`);
}

// Observed-not-peer binds
const obs = resolveCustomerFacingEntity("Six Senses Rome", profile);
assert.equal(obs.ok, true, "observed-not-peer Six Senses binds");

const scenarios = buildScenarioUniverse(profile);
const payload = buildOwnerPayload(period, scenarios, profile);
assert.ok(payload.ok !== false);
assert.ok((payload.competitiveSet?.observedCount || 0) >= 5, "competitive set nonempty");
assert.ok((payload.lostDemand?.displacement || []).length >= 1, "displacement nonempty");
assert.ok((payload.realityGap?.totalAttributes || 0) >= 5, "reality attrs expanded");
assert.ok(payload.competitiveSet?.topObservedAlternative?.name, "top observed alternative set");

const published = loadPublishedReport(PROPERTY_ID);
const pub = published?.payload || published;
assert.ok((pub?.competitiveSet?.observedCount || 0) >= 5, "published competitive set");
assert.ok((pub?.lostDemand?.displacement || []).length >= 1, "published displacement");

// Peer pack must not be the only competitive set source — surprises / observed-not-declared OK
const observedNames = (payload.competitiveSet?.observed || []).map((o) => o.name);
assert.ok(
  observedNames.some((n) => /six senses|baglioni|bulgari|jk\.? place/i.test(n)),
  "observed-not-peer hotels appear in competitive set"
);

console.log("PASS test-adp-w-rome-baseline-completion-rebuild-v1");
console.log(
  JSON.stringify({
    observed: payload.competitiveSet.observedCount,
    displacement: payload.lostDemand.displacement.length,
    realityAttrs: payload.realityGap.totalAttributes,
    topAlt: payload.competitiveSet.topObservedAlternative?.name,
  })
);
