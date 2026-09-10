#!/usr/bin/env node
/**
 * Hotel Explorer share-mode Research Center parity.
 * Golden Four: KGPV, Cambridge, Sheraton GDL, voco/Real Inn Cancún.
 * No Webhound. Direct lib imports (temp research store).
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const GOLDEN_FOUR = Object.freeze({
  kgpv: "recUNycnMwOVFX0hc",
  cambridge: "recIwaP1etgx2g9nA",
  sheraton: "recsYJb2R1jarPpK3",
  voco: "recTYaiA4S6fR6ixx",
});

const {
  buildResearchCenterPayload,
  createResearchRepository,
  ensureCompletedDossierArchiveBackfill,
  dossierBackfillRequestId,
} = await import("../lib/hotel-intelligence/research/index.js");
const { listDossiersForHotel } = await import("../lib/hotel-intelligence/dossier/index.js");
const { assertDossierShareScope } = await import("../api/hotel-intelligence-dossier.js");
const { getDossierById } = await import("../lib/hotel-intelligence/dossier/index.js");

function tmpRepo() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "hi-share-rc-"));
  return { root, repository: createResearchRepository({ root }) };
}

function assertNoRequestIds(cards) {
  for (const c of cards || []) {
    assert.equal(c.request_id, undefined, "share card must omit request_id");
    assert.equal(c.admin, undefined, "share card must omit admin");
  }
}

function assertShareSafe(payload, hotelId) {
  assert.equal(payload.ok, true);
  assert.equal(payload.mode_flags?.share_readonly, true);
  assert.ok(!payload.execution_mode, "share payload must omit execution_mode");
  assert.ok(!payload.research_more || payload.research_more.length === 0);
  assert.ok(!payload.recommended_follow_up || payload.recommended_follow_up.length === 0);
  assert.equal(payload.first_use, null);
  assert.deepEqual(payload.active_requests || [], []);
  assert.deepEqual(payload.templates || [], []);
  assert.ok(!payload.internal_research_controls || payload.internal_research_controls.visible !== true);
  assert.equal(payload.hotel?.hotel_id, hotelId);
  assertNoRequestIds(payload.archive);
  assertNoRequestIds(payload.completed_reports);
  if (payload.latest_investigation) {
    assert.equal(payload.latest_investigation.request_id, undefined);
  }
  const blob = JSON.stringify(payload);
  assert.ok(!/"SIMULATION"/.test(blob) || !/execution_mode/.test(blob), "no SIMULATION execution badge");
  assert.ok(!blob.includes("req_"), "share payload must not expose request_id strings");
}

console.log("→ idempotent ensureCompletedDossierArchiveBackfill (Cambridge)");
{
  const { repository } = tmpRepo();
  const hotelId = GOLDEN_FOUR.cambridge;
  const a = ensureCompletedDossierArchiveBackfill(repository, hotelId);
  const b = ensureCompletedDossierArchiveBackfill(repository, hotelId);
  assert.ok(a.requests.length >= 1, "Cambridge must backfill ≥1 archive request");
  assert.equal(b.created, 0, "second call must not create duplicates");
  const research = repository.listHotelResearch(hotelId);
  const reportIds = research.requests.map((r) => r.report_id).filter(Boolean);
  assert.equal(new Set(reportIds).size, reportIds.length, "no duplicate report_ids");
  const camDossier = "dossier_cambridge_beaches_full_hi_v3";
  assert.ok(
    research.requests.some((r) => r.report_id === camDossier || r.request_id === dossierBackfillRequestId(camDossier)),
    "Cambridge Full HI present in archive"
  );
  console.log("  Cambridge archive requests:", research.requests.length, "created first pass:", a.created);
}

console.log("→ Golden Four share vs internal archive parity");
const summary = {};
for (const [key, hotelId] of Object.entries(GOLDEN_FOUR)) {
  const { repository } = tmpRepo();
  const internal = buildResearchCenterPayload({
    hotel_id: hotelId,
    repository,
    env: { ...process.env, HI_EXTERNAL_RESEARCH_ENABLED: "0" },
  });
  const share = buildResearchCenterPayload({
    hotel_id: hotelId,
    repository,
    clientSafeShare: true,
    env: { ...process.env, HI_EXTERNAL_RESEARCH_ENABLED: "0" },
  });

  assertShareSafe(share, hotelId);

  const internalIds = (internal.archive || [])
    .map((a) => a.report_id)
    .filter(Boolean)
    .sort();
  const shareIds = (share.archive || [])
    .map((a) => a.report_id)
    .filter(Boolean)
    .sort();
  assert.deepEqual(
    shareIds,
    internalIds,
    `${key}: share archive report_ids must match internal client-safe archive`
  );

  const dossierCards = listDossiersForHotel({ airtableRecordId: hotelId });
  summary[key] = {
    hotelId,
    internal_archive: internalIds.length,
    share_archive: shareIds.length,
    dossier_registry: dossierCards.length,
    report_ids: shareIds,
    mode: share.mode,
  };
  console.log(
    `  ${key}: share=${shareIds.length} internal=${internalIds.length} dossiers=${dossierCards.length}`
  );
}

assert.ok(summary.cambridge.share_archive >= 1, "Cambridge archive length >= 1 after backfill");
assert.ok(summary.kgpv.share_archive >= 1, "KGPV must have Full HI");
// Honest KGPV count: Full HI + Change Opportunity addendum when present in registry/addenda.
const kgpvHasAddendum = summary.kgpv.report_ids.some((id) =>
  String(id).includes("change_opportunity")
);
console.log(
  "  KGPV reports:",
  summary.kgpv.share_archive,
  kgpvHasAddendum ? "(Full HI + Change Opportunity addendum PRESENT)" : "(addendum NOT PRESENT in archive set)"
);

console.log("→ assertDossierShareScope wrong hotel → denied");
{
  const cam = getDossierById("dossier_cambridge_beaches_full_hi_v3");
  assert.ok(cam);
  const ok = assertDossierShareScope(cam, {
    share: "1",
    share_hotel: GOLDEN_FOUR.cambridge,
  });
  assert.equal(ok.denied, false);
  const bad = assertDossierShareScope(cam, {
    share: "1",
    share_hotel: GOLDEN_FOUR.kgpv,
  });
  assert.equal(bad.denied, true);
  const bare = assertDossierShareScope(cam, { share: "1" });
  assert.equal(bare.denied, true, "share=1 without hotel scope must deny");
}

console.log("\nPASS test-hotel-explorer-share-research-parity");
console.log(JSON.stringify({ summary }, null, 2));
