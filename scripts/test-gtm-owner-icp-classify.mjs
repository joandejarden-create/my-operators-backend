import test from "node:test";
import assert from "node:assert/strict";
import { classifyOwnerIcp, isVerifiedOwnerContact } from "../lib/gtm-owner-target/icp-classify.js";

test("classifyOwnerIcp marks Marriott as franchisor_brand", () => {
  const result = classifyOwnerIcp({
    ownerName: "Marriott International",
    ownerType: "integrated_operator",
    priorityTier: "A",
    propertyCount: 12,
    properties: [{ country: "Mexico" }, { country: "Mexico" }, { country: "Brazil" }],
    contact: { hasVerifiedContact: true, primaryContactEmail: "test@marriott.com" },
  });
  assert.equal(result.icpSegment, "franchisor_brand");
  assert.equal(result.strikeList, false);
});

test("classifyOwnerIcp marks integrated allowlist as owner_operator", () => {
  const result = classifyOwnerIcp({
    ownerName: "Grupo Posadas, S.A.B. DE C.V.",
    ownerType: "integrated_operator",
    priorityTier: "A",
    propertyCount: 44,
    properties: Array.from({ length: 5 }, () => ({ country: "Mexico" })),
    contact: { hasVerifiedContact: true, primaryContactEmail: "ceo@posadas.com" },
  });
  assert.equal(result.icpSegment, "owner_operator");
  assert.equal(result.strikeList, true);
});

test("classifyOwnerIcp blocks strike without contact", () => {
  const result = classifyOwnerIcp({
    ownerName: "Norte 19",
    ownerType: "integrated_operator",
    priorityTier: "A",
    propertyCount: 142,
    properties: Array.from({ length: 5 }, () => ({ country: "Mexico" })),
    contact: {},
  });
  assert.equal(result.icpSegment, "owner_operator");
  assert.equal(result.strikeList, false);
  assert.match(result.icpClassificationNotes, /strike_blocked_no_contact/);
});

test("isVerifiedOwnerContact requires owner_exact and cala yes", () => {
  assert.equal(
    isVerifiedOwnerContact(
      { email: "a@b.com", contactRelevance: "hospitality", calaHotelContact: "yes" },
      { matchType: "owner_exact", calaHotelContact: "yes" }
    ),
    true
  );
  assert.equal(
    isVerifiedOwnerContact(
      { email: "a@b.com", contactRelevance: "broker", calaHotelContact: "yes" },
      { matchType: "owner_exact", calaHotelContact: "yes" }
    ),
    false
  );
});
