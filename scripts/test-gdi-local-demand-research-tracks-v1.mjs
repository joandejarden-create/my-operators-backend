#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveLocalDemandTrack,
  listLocalDemandTracks,
  LOCAL_DEMAND_TRACK,
  buildLocalDemandTrackContextBlock,
} from "../lib/group-demand-intelligence/research/gdi-local-demand-research-tracks-v1.js";
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

check("three_tracks_exist", () => {
  assert.equal(listLocalDemandTracks().length, 3);
});

check("tracks_map_to_existing_fourteen_segments_only", () => {
  const fourteen = new Set(listFourteenWorkflowSegmentIds());
  for (const t of listLocalDemandTracks()) {
    assert.ok(t.segments.length >= 2);
    for (const s of t.segments) assert.ok(fourteen.has(s), s);
  }
});

check("social_track_includes_social_and_catering", () => {
  const t = resolveLocalDemandTrack("weddings");
  assert.equal(t.trackId, LOCAL_DEMAND_TRACK.SOCIAL_CELEBRATIONS);
  assert.ok(t.segments.includes("social_special_events"));
  assert.ok(t.segments.includes("local_catering_day_meetings"));
});

check("planner_track_includes_professional_event_planners", () => {
  const t = resolveLocalDemandTrack("planner_partnerships");
  assert.ok(t.segments.includes("professional_event_planners"));
});

check("venue_track_requires_unknown_room_honesty", () => {
  const t = resolveLocalDemandTrack("venues");
  assert.ok(
    t.mission.exclusions.some((x) => /assume no rooms/i.test(x)),
  );
  const block = buildLocalDemandTrackContextBlock(t.trackId, {
    displayName: "Bethesda Marriott",
    city: "Bethesda",
  });
  assert.ok(/LOCAL DEMAND RESEARCH TRACK/i.test(block));
  assert.ok(/Bethesda Marriott/.test(block));
});

check("cli_and_wrappers_exist", () => {
  const cli = fs.readFileSync(
    path.join(ROOT, "scripts/gdi-canonical-full-e2e-entry-v1.mjs"),
    "utf8",
  );
  assert.ok(cli.includes("--research-track"));
  for (const name of [
    "Invoke-GdiLocalDemandTrack.ps1",
    "Invoke-GdiSocialCelebrationsTrack.ps1",
    "Invoke-GdiPlannerPartnershipsTrack.ps1",
    "Invoke-GdiVenueLodgingPartnershipsTrack.ps1",
  ]) {
    assert.ok(fs.existsSync(path.join(ROOT, "scripts/gdi-local-tracks", name)));
  }
});

check("canonical_entry_resolves_track", () => {
  const src = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js",
    ),
    "utf8",
  );
  assert.ok(src.includes("resolveLocalDemandTrack"));
  assert.ok(src.includes("researchTrack"));
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-local-demand-research-tracks-v1",
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
