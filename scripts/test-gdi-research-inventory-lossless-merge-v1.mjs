#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  mergeProposedResearchInventoryFields,
  MATERIAL_RESEARCH_FIELDS,
  diffMaterialResearchFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import {
  buildSlimResearchPayload,
  collectNamedContacts,
} from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

assert.ok(
  !MATERIAL_RESEARCH_FIELDS.includes(F.lastMaterialChangeAt),
  "lastMaterialChangeAt must not drive material updates alone",
);

const livePayload = {
  opportunityId: "opp_merge_test",
  hotelId: "recHotelTest",
  organizationName: "Live Org",
  primaryContact: {
    name: "Andrea Snader",
    email: "shared@example.org",
  },
  contacts: [
    { name: "Andrea Snader", email: "shared@example.org" },
    { name: "Justin Fessler", email: "shared@example.org" },
    { name: "Andrea Snader / Justin Fessler", email: "shared@example.org" },
  ],
  backupContacts: [{ name: "Justin Fessler", email: "shared@example.org" }],
  sources: [
    "https://example.org/a",
    "https://example.org/b",
  ],
  evidence: [
    { url: "https://example.org/a" },
    { url: "https://example.org/b" },
  ],
};

const proposedSlim = buildSlimResearchPayload({
  opportunityId: "opp_merge_test",
  hotelId: "recHotelTest",
  title: "Short",
  organizationName: null,
  primaryContact: { name: "Andrea Snader", email: "shared@example.org" },
  sources: [{ url: "https://example.org/a", label: "A" }],
  finalizedQualified: true,
  fqoEvaluation: {
    finalizedQualified: true,
    status: "PASS",
    reasons: ["named_contact"],
  },
  disposition: "INCLUDED_IN_CERTIFIED_CANDIDATE",
  recommendedNextStep: "Outreach",
});
assert.equal(proposedSlim.ok, true);
assert.ok(proposedSlim.payload.hotelSpecificQualification);

const existing = {
  [F.opportunityId]: "opp_merge_test",
  [F.hotelId]: "recHotelTest",
  [F.organizationName]: "Live Org",
  [F.title]: "Longer live title with context — keep me",
  [F.sourceSummary]:
    "https://example.org/a | https://example.org/b | https://example.org/a",
  [F.researchPayloadJson]: JSON.stringify(livePayload),
};

const proposed = {
  [F.opportunityId]: "opp_merge_test",
  [F.hotelId]: "recHotelTest",
  [F.organizationName]: null,
  [F.title]: "Short. title",
  [F.sourceSummary]: "https://example.org/a",
  [F.dgGeneratorId]: "dg_new",
  [F.researchPayloadJson]: proposedSlim.json,
  [F.commercialEligibilityStatus]: "GLOBAL_ORG_ONLY",
};

const merged = mergeProposedResearchInventoryFields(proposed, existing);
assert.equal(merged.ok, true, merged.code);
assert.equal(merged.fields[F.organizationName], "Live Org");
assert.equal(
  merged.fields[F.title],
  "Longer live title with context — keep me",
);
const afterUrls = [
  ...new Set(
    String(merged.fields[F.sourceSummary]).match(/https?:\/\/\S+/g) || [],
  ),
];
assert.ok(afterUrls.includes("https://example.org/a"));
assert.ok(afterUrls.includes("https://example.org/b"));
assert.equal(merged.fields[F.dgGeneratorId], "dg_new");

const afterPayload = JSON.parse(merged.fields[F.researchPayloadJson]);
const names = collectNamedContacts(afterPayload);
assert.ok(names.includes("Andrea Snader"));
assert.ok(names.includes("Justin Fessler"));
assert.ok(afterPayload.hotelSpecificQualification?.finalizedQualified === true);

const stampOnly = {
  ...merged.fields,
  [F.lastMaterialChangeAt]: "2099-01-01T00:00:00.000Z",
};
const diff = diffMaterialResearchFields(stampOnly, merged.fields);
assert.equal(
  diff.materialChanged,
  false,
  "timestamp-only must not count as material",
);

console.log(
  JSON.stringify(
    {
      ok: true,
      preservedContacts: names,
      sourceSummary: merged.fields[F.sourceSummary],
      fqo: afterPayload.hotelSpecificQualification?.fqoStatus || null,
    },
    null,
    2,
  ),
);
