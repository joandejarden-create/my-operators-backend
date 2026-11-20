#!/usr/bin/env node
import assert from "assert";
import { toEditorialTitleCase, isEditorialTitleCase } from "../lib/brand-explorer/be-editorial-title-case-v1.js";
import { buildDepthGapFillBlocks } from "../lib/brand-explorer/factory-2/be-factory-2-depth-candidate-builder-v1.js";

const cases = [
  ["Design & guest promise review", "Design & Guest Promise Review"],
  ["Public-space and amenity capital", "Public-Space & Amenity Capital"],
  ["Training and service culture", "Training & Service Culture"],
  ["Guest-room product standards", "Guest-Room Product Standards"],
  ["Ongoing quality review", "Ongoing Quality Review"],
  ["Marriott Bonvoy systems participation", "Marriott Bonvoy Systems Participation"],
  [
    "Hyatt's loyalty program (parent platform) systems participation",
    "Hyatt's Loyalty Program (Parent Platform) Systems Participation",
  ],
];

for (const [input, expected] of cases) {
  const got = toEditorialTitleCase(input);
  assert.equal(got, expected, `${input} => ${got} (expected ${expected})`);
  assert.ok(isEditorialTitleCase(got));
  assert.ok(!/\sand\s/i.test(got), `${got} still has "and"`);
  console.log(`OK ${input} => ${got}`);
}

const pack = {
  slug: "ac-hotels-by-marriott",
  brandName: "AC Hotels by Marriott",
  parentCompany: "Marriott",
  loyaltyProgram: "Marriott Bonvoy",
  positioning: "Design-led select",
  scenarios: [{ title: "A", body: "b" }],
  standards: [
    { title: "Design & guest promise review", body: "Confirm design." },
    { title: "Public-space and amenity capital", body: "Confirm capital." },
    { title: "Training and service culture", body: "Confirm training." },
  ],
  sources: { brandPage: "https://example.com" },
  operatingModel: "Franchise",
  footprint: "Confirm footprint.",
  insight: "Insight.",
  feeNote: "Confirm fees.",
};
const blocks = buildDepthGapFillBlocks(pack);
const req = blocks.filter((b) => b.slotKey === "standards.requirement");
assert.ok(req.length >= 3, "expected standards.requirement rows");
for (const b of req) {
  assert.ok(isEditorialTitleCase(b.title), `not title case: ${b.title}`);
  assert.ok(!/\sand\s/i.test(b.title), `has and: ${b.title}`);
}
console.log("OK depth builder emits Proper Case Requirement Area titles");
console.log("test-be-factory-2-requirement-area-title-case-v1: PASS");
