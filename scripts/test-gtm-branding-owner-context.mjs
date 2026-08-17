import test from "node:test";
import assert from "node:assert/strict";
import {
  isHouseBrandForOwner,
  isThirdPartyFranchiseBrand,
  isGenuineThirdPartyOperatorMismatch,
  isBrandDecisionEligibleProperty,
  classifyOwnerOutreachTrack,
} from "../lib/gtm-owner-target/branding-owner-context.js";
import {
  scorePropertyBrandingSignals,
  scoreOwnerBrandingIntent,
  buildBrandingOutreachTarget,
} from "../lib/gtm-owner-target/branding-decision-signals.js";

test("isHouseBrandForOwner detects Iberostar sub-brands", () => {
  assert.equal(isHouseBrandForOwner("Iberostar Hotels & Resorts", "Iberostar Waves"), true);
  assert.equal(isHouseBrandForOwner("Iberostar Hotels & Resorts", "JOIA by Iberostar"), true);
  assert.equal(isHouseBrandForOwner("Iberostar Hotels & Resorts", "City Express by Marriott"), false);
});

test("isThirdPartyFranchiseBrand flags Marriott franchise on independent owner", () => {
  assert.equal(isThirdPartyFranchiseBrand("City Express by Marriott", "Norte 19"), true);
  assert.equal(isThirdPartyFranchiseBrand("Fiesta Americana", "Grupo Posadas, S.A.B. DE C.V."), false);
});

test("Visit US Inc operator on Iberostar house brand is not genuine reflag", () => {
  assert.equal(
    isGenuineThirdPartyOperatorMismatch(
      "Iberostar Hotels & Resorts",
      "Visit US, Inc",
      "Iberostar Selection Cancun",
      "owner_operator"
    ),
    false
  );
});

test("Aimbridge on Fibra asset is genuine third-party operator mismatch", () => {
  assert.equal(
    isGenuineThirdPartyOperatorMismatch(
      "Fibra Inn",
      "Aimbridge LATAM",
      "The Westin Monterrey Valle",
      "reit"
    ),
    true
  );
});

test("scorePropertyBrandingSignals suppresses false reflag for Iberostar", () => {
  const result = scorePropertyBrandingSignals(
    {
      buildingName: "Iberostar Selection Cancun",
      brandAffiliation: "Iberostar Selection",
      hotelOperator: "Visit US, Inc",
      trueOwner: "Iberostar Hotels & Resorts",
      country: "Mexico",
      yearBuilt: 1999,
    },
    new Date().getFullYear(),
    { ownerName: "Iberostar Hotels & Resorts", icpSegment: "owner_operator" }
  );
  assert.equal(result.brandDecisionEligible, false);
  assert.equal(result.signals.some((s) => s.id === "reflag_operator_mismatch"), false);
});

test("scoreOwnerBrandingIntent marks Iberostar as house-brand-only track", () => {
  const intent = scoreOwnerBrandingIntent(
    [
      {
        buildingName: "Iberostar Waves Cozumel",
        brandAffiliation: "Iberostar Waves",
        hotelOperator: "Visit US, Inc",
        trueOwner: "Iberostar Hotels & Resorts",
        country: "Mexico",
        yearBuilt: 1999,
      },
    ],
    { ownerName: "Iberostar Hotels & Resorts", icpSegment: "owner_operator" }
  );
  assert.equal(intent.outreachTrack, "integrated_operator_house_brand_only");
  assert.equal(intent.brandDecisionEligiblePropertyCount, 0);
});

test("Norte 19 keeps third-party franchise assets as brand-decision eligible", () => {
  const intent = scoreOwnerBrandingIntent(
    [
      {
        buildingName: "City Express Saltillo",
        brandAffiliation: "City Express by Marriott",
        hotelOperator: "Norte 19",
        trueOwner: "Norte 19",
        country: "Mexico",
        yearBuilt: 2010,
      },
      {
        buildingName: "one Ciudad Juárez",
        brandAffiliation: "Independent",
        hotelOperator: "Norte 19",
        trueOwner: "Norte 19",
        country: "Mexico",
        yearBuilt: 2020,
      },
    ],
    { ownerName: "Norte 19", icpSegment: "owner_operator" }
  );
  assert.ok(intent.brandDecisionEligiblePropertyCount >= 1);
  assert.notEqual(intent.outreachTrack, "integrated_operator_house_brand_only");
});

test("buildBrandingOutreachTarget excludes Iberostar house-brand-only from outreach ready", () => {
  const intent = scoreOwnerBrandingIntent(
    [
      {
        buildingName: "Iberostar Waves Cozumel",
        brandAffiliation: "Iberostar Waves",
        hotelOperator: "Visit US, Inc",
        trueOwner: "Iberostar Hotels & Resorts",
        country: "Mexico",
      },
    ],
    { ownerName: "Iberostar Hotels & Resorts", icpSegment: "owner_operator" }
  );
  const target = buildBrandingOutreachTarget(
    { id: "rec1", ownerName: "Iberostar Hotels & Resorts", priorityTier: "A", calaPropertyCount: 10 },
    intent,
    { email: "alberto.gil@iberostar.com", name: "Alberto Gil" }
  );
  assert.equal(target.outreachReady, false);
  assert.equal(target.outreachTrack, "integrated_operator_house_brand_only");
});
