#!/usr/bin/env node
/**
 * Deterministic tests — fourteen-segment Exa mission upgrades V1.
 * No paid provider calls.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertAllFourteenSegmentsHaveMissions,
  buildUpgradedExaDiscoveryContextBlock,
  resolveHotelDiscoveryCatchment,
  SEGMENT_EXA_MISSION_UPGRADES,
  GDI_FOURTEEN_SEGMENT_EXA_MISSION_UPGRADES_V1,
} from "../lib/group-demand-intelligence/research/exa-agent/gdi-fourteen-segment-exa-mission-upgrades-v1.js";
import { inspectExaLiveDiscoveryReadiness } from "../lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
  }
}

const BETHESDA = {
  hotelId: "recLuxvwwxID7U2B8",
  displayName: "Bethesda Marriott",
  city: "Bethesda",
  country: "United States",
  demandTerritory: { label: "Bethesda / Montgomery County" },
};

const GENEVA = {
  hotelId: "recYotelGenevaFixture",
  displayName: "YOTEL Geneva Lake",
  city: "Geneva",
  country: "Switzerland",
  demandTerritory: { label: "Geneva / Lake Geneva" },
};

const LONDON = {
  hotelId: "recNobuLondonFixture",
  displayName: "Nobu Hotel London Portman Square",
  city: "London",
  country: "United Kingdom",
  demandTerritory: { label: "Central London" },
};

check("all_fourteen_segments_have_missions", () => {
  const cov = assertAllFourteenSegmentsHaveMissions();
  assert.equal(cov.ok, true, `missing: ${cov.missing.join(",")}`);
  assert.equal(Object.keys(SEGMENT_EXA_MISSION_UPGRADES).length, 14);
  assert.equal(listFourteenWorkflowSegmentIds().length, 14);
});

check("missions_are_segment_differentiated", () => {
  const blocks = listFourteenWorkflowSegmentIds().map((s) =>
    buildUpgradedExaDiscoveryContextBlock(BETHESDA, s),
  );
  const unique = new Set(blocks);
  assert.equal(unique.size, 14, "each segment must produce a distinct mission block");
  for (const s of listFourteenWorkflowSegmentIds()) {
    const b = buildUpgradedExaDiscoveryContextBlock(BETHESDA, s);
    assert.ok(b.includes(`SEGMENT MISSION — ${s}`));
    assert.ok(b.includes("Targets:"));
    assert.ok(b.includes("Exclusions:"));
  }
});

check("bethesda_catchment_is_dmv_specific", () => {
  const c = resolveHotelDiscoveryCatchment(BETHESDA);
  assert.ok(/Bethesda/i.test(c.primaryMarket));
  assert.ok(
    (c.negativeFilters || []).some((f) => /DC metro national|National Mall|distant US/i.test(f)),
  );
  const block = buildUpgradedExaDiscoveryContextBlock(
    BETHESDA,
    "associations_conferences_events",
  );
  assert.ok(/Bethesda/i.test(block));
  assert.ok(/National Mall|DC metro|Montgomery/i.test(block));
});

check("geneva_does_not_inherit_bethesda_filters", () => {
  const c = resolveHotelDiscoveryCatchment(GENEVA);
  assert.ok(/Geneva/i.test(c.primaryMarket));
  // Must not copy Bethesda primary geography / National Mall overflow rules.
  assert.ok(!/Bethesda|Montgomery County|National Mall/i.test(c.primaryMarket));
  assert.ok(
    !(c.negativeFilters || []).some((f) =>
      /National Mall|distant US cities \(Chicago/i.test(f),
    ),
  );
  assert.ok((c.negativeFilters || []).some((f) => /Do not apply US\/DMV/i.test(f)));
  const block = buildUpgradedExaDiscoveryContextBlock(GENEVA, "education_scientific");
  assert.ok(/Geneva/i.test(block));
  assert.ok(!/Bethesda Marriott|National Mall|Montgomery County/i.test(block));
  assert.ok(/Do not apply US\/DMV/i.test(block));
});

check("london_does_not_inherit_bethesda_filters", () => {
  const c = resolveHotelDiscoveryCatchment(LONDON);
  assert.ok(/London/i.test(c.primaryMarket));
  const block = buildUpgradedExaDiscoveryContextBlock(
    LONDON,
    "corporate_accounts_scaling",
  );
  assert.ok(/London/i.test(block));
  assert.ok(!/Montgomery County|National Mall/i.test(block));
});

check("corporate_excludes_hiring_only_noise", () => {
  const m = SEGMENT_EXA_MISSION_UPGRADES.corporate_accounts_scaling;
  assert.ok(m.exclusions.some((x) => /hiring-only/i.test(x)));
});

check("social_excludes_invented_private_events", () => {
  const m = SEGMENT_EXA_MISSION_UPGRADES.social_special_events;
  assert.ok(m.exclusions.some((x) => /invented private/i.test(x)));
});

check("preferred_lodging_unknown_rooms_honesty", () => {
  const m = SEGMENT_EXA_MISSION_UPGRADES.preferred_lodging_partnerships;
  assert.ok(m.exclusions.some((x) => /UNKNOWN/i.test(x)));
});

check("bridge_readiness_includes_catchment_and_mission", () => {
  const r = inspectExaLiveDiscoveryReadiness({
    hotel: BETHESDA,
    segmentId: "associations_conferences_events",
    authorizeExaAgentLive: false,
  });
  assert.equal(r.payload.hotelCatchmentPresent, true);
  assert.equal(r.payload.segmentMissionPresent, true);
  assert.equal(r.payload.hotelContextPresent, true);
});

check("track_context_can_attach_to_upgraded_block", () => {
  const block = buildUpgradedExaDiscoveryContextBlock(
    BETHESDA,
    "professional_event_planners",
    { researchTrack: "planner_partnerships" },
  );
  assert.ok(/LOCAL DEMAND RESEARCH TRACK/i.test(block));
  assert.ok(block.includes(GDI_FOURTEEN_SEGMENT_EXA_MISSION_UPGRADES_V1));
});

check("module_and_bridge_wired", () => {
  const bridge = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js",
    ),
    "utf8",
  );
  assert.ok(bridge.includes("buildUpgradedExaDiscoveryContextBlock"));
  assert.ok(bridge.includes("gdi-fourteen-segment-exa-mission-upgrades-v1"));
  const disc = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-execution-v1.js",
    ),
    "utf8",
  );
  assert.ok(disc.includes("researchTrackContextBlock"));
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: GDI_FOURTEEN_SEGMENT_EXA_MISSION_UPGRADES_V1,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);
if (failed.length) {
  console.error("FAILED:", failed.map((f) => f.name).join(", "));
  process.exit(1);
}
console.log("OK: fourteen-segment Exa mission upgrades V1");
