#!/usr/bin/env node
/**
 * Drawer hotel-fit must not print an unresolved city as "the market".
 * Site-specific copy is rebuilt from evidence already on the opportunity.
 */
import assert from "node:assert/strict";
import { buildHotelSpecificFitAssessment } from "../lib/group-demand-intelligence/scaling-commercial-intelligence-brief-v1.js";
import {
  resolveCoreCommercialBodies,
  rewritePlaceholderSameMarketFit,
} from "../lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js";

const guidePointFit = buildHotelSpecificFitAssessment(
  {
    organizationName: "GuidePoint Security",
    hotelDisplayName: "Bethesda Marriott",
    activityLocation:
      "Reston/Northern Virginia headquarters with offices listed in Tampa, Hinsdale, New York, Fishers, Norwood, Frisco and Irvine; federal work spans the DC/MD/VA region.",
  },
  {},
);

assert.match(guidePointFit.whyThisHotel, /Bethesda Marriott/);
assert.match(guidePointFit.whyThisHotel, /Reston\/Northern Virginia headquarters/);
assert.doesNotMatch(guidePointFit.whyThisHotel, /\bthe market\b/i);
assert.doesNotMatch(guidePointFit.whyThisHotel, /same commercial market/i);
assert.doesNotMatch(guidePointFit.whyThisHotel, /region\.\./);
assert.match(guidePointFit.whyThisHotel, /No confirmed room block/i);
assert.equal(guidePointFit.fitConfidence, "GROUNDED");

const cityOnly = buildHotelSpecificFitAssessment(
  {
    organizationName: "Example Labs",
    hotelDisplayName: "Bethesda Marriott",
  },
  { city: "Bethesda", name: "Bethesda Marriott" },
);
assert.match(cityOnly.whyThisHotel, /Bethesda Marriott is in Bethesda/);
assert.doesNotMatch(cityOnly.whyThisHotel, /\bthe market\b/i);
assert.doesNotMatch(cityOnly.whyThisHotel, /same commercial market/i);

const unresolved = buildHotelSpecificFitAssessment(
  { organizationName: "Example Labs" },
  { name: "Bethesda Marriott" },
);
assert.match(unresolved.whyThisHotel, /verified Example Labs activity site/i);
assert.doesNotMatch(unresolved.whyThisHotel, /\bthe market\b/i);

const stored =
  "Bethesda Marriott sits in the same commercial market (the market) as the documented GuidePoint Security activity. Company activity reference point: Reston/Northern Virginia headquarters with offices listed in Tampa, Hinsdale, New York, Fishers, Norwood, Frisco and Irvine; federal work spans the DC/MD/VA region.. No confirmed room block, RFP, or preferred-hotel agreement is established from public research alone.";

const rewritten = rewritePlaceholderSameMarketFit(
  { organizationName: "GuidePoint Security", hotelName: "Bethesda Marriott" },
  stored,
);
assert.equal(rewritten.rewritten, true);
assert.match(rewritten.text, /GuidePoint Security/);
assert.match(rewritten.text, /Reston\/Northern Virginia headquarters/);
assert.doesNotMatch(rewritten.text, /\bthe market\b/i);
assert.doesNotMatch(rewritten.text, /same commercial market/i);
assert.doesNotMatch(rewritten.text, /region\.\./);

const bodies = resolveCoreCommercialBodies({
  organizationName: "GuidePoint Security",
  hotelName: "Bethesda Marriott",
  whatIsHappening: "GuidePoint opened a Reston headquarters supporting customer engagement.",
  potentialHotelBusiness: "Visitor and customer overnight stays if travel to Reston is confirmed.",
  whyThisHotel: stored,
  whyActNow: "Qualify whether visiting customers need rooms.",
  recommendedNextStep: "Contact Jason Danner and ask about visitor lodging cadence.",
});
assert.equal(bodies.whyThisHotelRewrittenFromGeneric, true);
assert.match(bodies.whyThisHotel, /Reston\/Northern Virginia/);
assert.doesNotMatch(bodies.whyThisHotel, /\bthe market\b/i);
assert.equal(
  bodies.recommendedNextStep,
  "Contact Jason Danner and ask about visitor lodging cadence.",
);

const specific = rewritePlaceholderSameMarketFit(
  { organizationName: "GuidePoint Security", hotelName: "Bethesda Marriott" },
  "Bethesda Marriott can host short training blocks for teams working at the Reston headquarters.",
);
assert.equal(specific.rewritten, false);
assert.match(specific.text, /Reston headquarters/);

console.log(
  JSON.stringify(
    {
      ok: true,
      generatedPreview: guidePointFit.whyThisHotel.slice(0, 180),
      drawerPreview: bodies.whyThisHotel.slice(0, 180),
    },
    null,
    2,
  ),
);
