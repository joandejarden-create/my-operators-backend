import test from "node:test";
import assert from "node:assert/strict";
import {
  isIndependentOrUnbranded,
  scorePropertyBrandingSignals,
  scoreOwnerBrandingIntent,
  parseDevelopmentCountFromProfileNotes,
  buildBrandingOutreachTarget,
  inferOwnerDealTrigger,
} from "../lib/gtm-owner-target/branding-decision-signals.js";

test("isIndependentOrUnbranded detects empty and independent labels", () => {
  assert.equal(isIndependentOrUnbranded(""), true);
  assert.equal(isIndependentOrUnbranded("Independent"), true);
  assert.equal(isIndependentOrUnbranded("Marriott"), false);
});

test("recent branded new build is post-decision not primary new_build", () => {
  const result = scorePropertyBrandingSignals({
    buildingName: "City Express Plus Mazatlan",
    brandAffiliation: "City Express by Marriott",
    trueOwner: "Norte 19",
    yearBuilt: 2024,
    country: "Mexico",
  });
  assert.equal(result.brandDecisionTiming, "post_decision");
  assert.ok(result.signals.some((s) => s.id === "recent_open_branded"));
  assert.notEqual(result.primaryDealTrigger, "new_build");
});

test("unbranded development is pre-decision new_build", () => {
  const result = scorePropertyBrandingSignals({
    buildingName: "Proyecto Hotel Cancun",
    brandAffiliation: "Independent",
    propertyType: "Under Construction",
    country: "Mexico",
  });
  assert.equal(result.brandDecisionTiming, "pre_decision");
  assert.equal(result.primaryDealTrigger, "new_build");
});

test("scorePropertyBrandingSignals flags owner-operator mismatch", () => {
  const result = scorePropertyBrandingSignals({
    buildingName: "Courtyard Cancun",
    brandAffiliation: "Courtyard by Marriott",
    hotelOperator: "ABC Hotel Management LLC",
    trueOwner: "Fibra Inn",
    country: "Mexico",
    yearBuilt: 2015,
  });
  assert.ok(result.signals.some((s) => s.id === "reflag_operator_mismatch"));
});

test("scoreOwnerBrandingIntent rolls up development pipeline from profile notes", () => {
  const count = parseDevelopmentCountFromProfileNotes(
    "CoStar stats: 7 owned; 2 under development (89,125 SF)."
  );
  assert.equal(count, 2);
  const intent = scoreOwnerBrandingIntent(
    [{ buildingName: "Hotel A", brandAffiliation: "Hilton", country: "Mexico", yearBuilt: 2010 }],
    { developmentPipelineCount: count, ownerName: "Test Owner" }
  );
  assert.ok(intent.signals.some((s) => s.id === "development_pipeline"));
});

test("buildBrandingOutreachTarget marks outreach ready with verified contact", () => {
  const intent = scoreOwnerBrandingIntent(
    [{ buildingName: "Hotel B", brandAffiliation: "Independent", country: "Mexico", yearBuilt: 1995 }],
    { ownerName: "Test Owner" }
  );
  const target = buildBrandingOutreachTarget(
    { id: "rec1", ownerName: "Test Owner", priorityTier: "A", calaPropertyCount: 5 },
    intent,
    { primaryContactEmail: "ceo@test.com", hasVerifiedContact: true, primaryContactName: "CEO" }
  );
  assert.equal(target.outreachReady, true);
  assert.ok(target.outreachScore >= 45);
});

test("inferOwnerDealTrigger respects manual override", () => {
  assert.equal(
    inferOwnerDealTrigger([], { existingDealTrigger: "sale_process" }),
    "sale_process"
  );
});
