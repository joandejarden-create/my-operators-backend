#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  detectGenericCommercialProse,
  sanitizeGenericHotelFitNarrative,
  evaluateCertifiedDrawerCommercialContent,
  resolveCoreCommercialBodies,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";

const boilerplate =
  "Bethesda Marriott banquet and day-meeting inventory is a plausible local venue for chapter, civic, and community programmes in the Bethesda–Rockville corridor when independent demand and a commercial contact are verified. Guestrooms are not required for a catering-led opportunity.";

assert.equal(detectGenericCommercialProse(boilerplate).generic, true);
assert.equal(detectGenericCommercialProse("NIH shuttle listing includes this hotel.").generic, false);

const rewritten = sanitizeGenericHotelFitNarrative(
  {
    organizationName: "Montgomery County SHRM",
    hotelName: "Bethesda Marriott",
  },
  boilerplate,
);
assert.equal(rewritten.rewritten, true);
assert.match(rewritten.text, /Montgomery County SHRM/);
assert.doesNotMatch(rewritten.text, /plausible local venue for chapter/i);

const bodies = resolveCoreCommercialBodies({
  organizationName: "Montgomery County SHRM",
  hotelName: "Bethesda Marriott",
  whatIsHappening:
    "MCSHRM runs recurring chapter meetings plus dated 2026 programmes at competitor venues.",
  potentialHotelBusiness: "Day meetings / banquet when buyer verified.",
  whyThisHotel: boilerplate,
  whyActNow: "Competitor venues already hosting 2026 programmes — qualify overflow/switch.",
  recommendedNextStep: "Identify programs chair and ask about 2027 venue shortlist.",
});
assert.equal(bodies.whyThisHotelRewrittenFromGeneric, true);
assert.match(bodies.whyThisHotel, /Montgomery County SHRM/);

const evalRow = evaluateCertifiedDrawerCommercialContent({
  id: "demo_local_catering",
  organizationName: "Montgomery County SHRM",
  hotelName: "Bethesda Marriott",
  title: "Chapter meetings",
  whatIsHappening:
    "MCSHRM runs recurring chapter meetings plus dated 2026 programmes at competitor venues.",
  potentialHotelBusiness: "Day meetings / banquet when buyer verified.",
  whyThisHotel: boilerplate,
  whyActNow: "Competitor venues already hosting 2026 programmes — qualify overflow/switch.",
  recommendedNextStep: "Identify programs chair and ask about 2027 venue shortlist.",
  primaryContact: { name: "Programs Chair", title: "VP Programs" },
});
assert.equal(evalRow.hotelFitRewritten, true);
assert.ok(evalRow.warnings.some((w) => w.code === "generic_hotel_fit_boilerplate_rewritten"));

console.log(
  JSON.stringify(
    {
      ok: true,
      genericDetected: true,
      hotelFitRewritten: true,
      rewrittenPreview: rewritten.text.slice(0, 120),
    },
    null,
    2,
  ),
);
