#!/usr/bin/env node
/**
 * Tests for GDI Repeatable Processing V2 — dedupe, config proposal shape,
 * universal HotelsFile parse, contact retention policy.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  dedupeCandidatesAcrossSegments,
  canonicalProgramClusterKey,
  orgsLikelySameEntity,
} from "../lib/group-demand-intelligence/research/gdi-cross-segment-opportunity-dedupe-v2.js";
import { selectCandidatesForDeepResearch } from "../lib/group-demand-intelligence/research/gdi-deep-research-selection-register-v1.js";
import {
  parseHotelsInput,
  UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE,
} from "../lib/group-demand-intelligence/workflow/gdi-universal-cohort-processing-v2.js";
import {
  evaluateNamedContactRetention,
} from "../lib/group-demand-intelligence/research/gdi-public-contact-research-policy-v2.js";
import {
  validateProposedAgainstPilotConfig,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-config-proposal-v2.js";
import { loadHotelGdiConfig } from "../lib/group-demand-intelligence/research/commercial-demand-discovery-v4.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// CARPHA cross-segment case
const carphaA = {
  id: "disc_a6def3588729",
  organizationName: "Caribbean Public Health Agency (CARPHA)",
  title: "CARPHA 71st Annual Health Research Conference 2027",
  eventStartDate: "2027-04-01",
  researchSegment: "associations_conferences_events",
  seedUrls: ["https://carpha.org/conference"],
};
const carphaB = {
  id: "disc_023923963f08",
  organizationName: "Caribbean Public Health Agency (CARPHA)",
  title: "CARPHA 71st Annual Health Research Conference",
  eventStartDate: "2027-04-01",
  researchSegment: "education_scientific",
  seedUrls: ["https://carpha.org/"],
};
assert.equal(orgsLikelySameEntity(carphaA, carphaB), true);
const carphaDedupe = dedupeCandidatesAcrossSegments([carphaA, carphaB]);
assert.equal(carphaDedupe.canonicalCount, 1);
assert.equal(carphaDedupe.suppressedCount, 1);
assert.ok(
  carphaDedupe.suppressedCandidates[0].crossSegmentDedupeV2.canonicalId ===
    carphaDedupe.canonicalCandidates[0].id ||
    carphaDedupe.decisions[0].suppressedIds.length === 1,
);

// Advantage identical org/title across segments
const advA = {
  id: "disc_9cd2c082c05d",
  organizationName: "Advantage Travel Partnership",
  title: "Advantage Travel Partnership Annual Conference 2027",
  researchSegment: "travel_trade_group_intermediaries",
};
const advB = {
  id: "disc_51073448c775",
  organizationName: "Advantage Travel Partnership",
  title: "Advantage Travel Partnership Annual Conference 2027",
  researchSegment: "associations_conferences_events",
};
assert.equal(
  canonicalProgramClusterKey(advA).split("|")[0],
  canonicalProgramClusterKey(advB).split("|")[0],
);
const advDedupe = dedupeCandidatesAcrossSegments([advA, advB]);
assert.equal(advDedupe.canonicalCount, 1);
assert.equal(advDedupe.suppressedCount, 1);

// Selection register must not re-select both after fairness floor
const selection = selectCandidatesForDeepResearch(
  [
    { ...carphaA, preDeepScreen: { eligibleForDeep: true, outcome: "ELIGIBLE_FOR_DEEP_RESEARCH" } },
    { ...carphaB, preDeepScreen: { eligibleForDeep: true, outcome: "ELIGIBLE_FOR_DEEP_RESEARCH" } },
    {
      id: "disc_other",
      organizationName: "Pure Grenada Sailing Week Board",
      title: "Pure Grenada Sailing Week 2027",
      researchSegment: "sports_entertainment",
      preDeepScreen: { eligibleForDeep: true, outcome: "ELIGIBLE_FOR_DEEP_RESEARCH" },
    },
  ],
  {
    maxCandidates: 4,
    fairMinPerSegment: 1,
    hotelProfile: {
      capabilityProfile: { rooms: 16, classification: "leisure_destination" },
    },
  },
);
const selectedIds = selection.selected.map((o) => o.id);
assert.ok(
  !(
    selectedIds.includes("disc_a6def3588729") &&
    selectedIds.includes("disc_023923963f08")
  ),
  "must not select both CARPHA segment paths",
);
assert.ok(
  selection.crossSegmentDuplicatesSkipped >= 1 ||
    selectedIds.filter((id) => id.startsWith("disc_a6") || id.startsWith("disc_023"))
      .length <= 1,
);

// Contact retention without email
const keep = evaluateNamedContactRetention({
  name: "Maria Example",
  role: "Event Manager",
  employer: "CARPHA",
  sourceUrl: "https://carpha.org/team",
});
assert.equal(keep.retain, true);
assert.equal(keep.salesReady, false);
assert.equal(keep.reasonCode, "VERIFIED_NAME_WITHOUT_EMAIL_OK");

const rejectGap = evaluateNamedContactRetention({
  name: "Contact Research Required",
  sourceUrl: "https://example.com",
});
assert.equal(rejectGap.retain, false);

// HotelsFile parse
const parsed = parseHotelsInput({
  hotels: "rece0NdV9FCnaFlVS,recaokE8psIbuVeuH,rec1t35e3sPerzxYu",
  root: ROOT,
});
assert.equal(parsed.ok, true);
assert.equal(parsed.hotelIds.length, 3);
assert.equal(UNIVERSAL_COHORT_FOUNDER_SPEND_PHRASE, "FOUNDER_GDI_UNIVERSAL_COHORT_PAID");

// Pilot config self-validation
for (const id of [
  "rece0NdV9FCnaFlVS",
  "recaokE8psIbuVeuH",
  "rec1t35e3sPerzxYu",
]) {
  const cfg = loadHotelGdiConfig(id);
  assert.ok(cfg, `pilot config missing for ${id}`);
  const v = validateProposedAgainstPilotConfig(cfg, cfg);
  assert.equal(v.ok, true, `self-validate ${id}: ${v.failed}`);
}
void fs;

console.log(
  JSON.stringify(
    {
      ok: true,
      tests: [
        "carpha_dedupe",
        "advantage_dedupe",
        "selection_no_double_carpha",
        "contact_retain_without_email",
        "hotels_file_parse",
        "pilot_config_self_validate",
      ],
    },
    null,
    2,
  ),
);
