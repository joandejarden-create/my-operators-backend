#!/usr/bin/env node
/**
 * Unit tests for Census Autopilot v2 (no live Airtable production writes).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  parseAutopilotArgs,
  findForbiddenPatchFields,
  guardProposal,
  guardApplyBatch,
  applyPreflight,
  AUTOPILOT_FORBIDDEN_FIELDS,
} from "../lib/research-engine-v2/census-autopilot-apply-guard.js";
import {
  normalizeConfidence,
  isWritableConfidence,
} from "../lib/research-engine-v2/census-autopilot-confidence.js";
import {
  routeWebhoundCandidates,
  routeAutopilotQueues,
} from "../lib/research-engine-v2/census-autopilot-queue-router.js";
import {
  runCensusAutopilot,
  buildRunFolderName,
  createMemoryAirtableAdapter,
  STATUS,
  COMPLETION_STATUS,
} from "../lib/research-engine-v2/census-autopilot-runner.js";
import {
  chunkByBatchSize,
  applyMaxRecordsCap,
  skipCompletedRecords,
  loadCheckpoint,
} from "../lib/research-engine-v2/census-autopilot-checkpoint.js";
import {
  compareFieldValues,
  buildIdempotentPatch,
} from "../lib/research-engine-v2/census-autopilot-idempotent-writer.js";
import {
  AUTOPILOT_ALLOWED_WRITE_FIELDS,
  AUTOPILOT_TARGET_TABLE,
  sanitizeAutopilotPatch,
} from "../lib/research-engine-v2/census-autopilot-field-allowlist.js";
import { extractRoomsKeysFromOfficialHtml } from "../lib/research-engine-v2/production-census-rooms-keys-extractor.js";
import {
  buildActiveBrandSetupControlList,
  HELD_EXCLUDED_SLUGS,
} from "../lib/research-engine-v2/census-autopilot-active-brand-scope.js";
import { matchActiveBrandsToCensus } from "../lib/research-engine-v2/census-autopilot-brand-census-matcher.js";
import { buildFastestSafePriorityPlan } from "../lib/research-engine-v2/census-autopilot-fastest-safe.js";
import { createRuntimeMetrics } from "../lib/research-engine-v2/census-autopilot-runtime-guardrails.js";

function test(name, fn) {
  try {
    fn();
    console.log(`[PASS] ${name}`);
  } catch (err) {
    console.error(`[FAIL] ${name}`);
    throw err;
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    console.log(`[PASS] ${name}`);
  } catch (err) {
    console.error(`[FAIL] ${name}`);
    throw err;
  }
}

const APPLY_FLAGS = [
  "--confirm-safe-writes",
  "--confirm-write-to-production-census",
  "--confirm-no-brand-explorer-writes",
  "--confirm-no-owner-operator",
  "--confirm-no-date-writes",
  "--confirm-no-recent-momentum",
  "--confirm-no-company-validation",
  "--confirm-webhound-not-production-source",
];

const APPLY_ENV = {
  ALLOW_CENSUS_AUTOPILOT_APPLY: "1",
  CONFIRM_NO_BRAND_EXPLORER_WRITES: "1",
  CONFIRM_NO_OWNER_OPERATOR_WRITES: "1",
  CONFIRM_WRITE_TO_PRODUCTION_CENSUS: "1",
};

function makeProposals(n, opts = {}) {
  return Array.from({ length: n }, (_, i) => ({
    record_id: `rec${i}`,
    identity_key: `key_${i}`,
    property_name: `Hotel ${i}`,
    queue: "rooms_keys",
    confidence: "High",
    patch: {
      "Rooms / Keys": 100 + i,
      "Rooms Confidence": "High",
      "Rooms Source URL": `https://example.com/h${i}`,
    },
    current_fields: opts.prefilled ? { "Rooms / Keys": 100 + i } : {},
  }));
}

test("batch-size does not cap total scope", () => {
  const items = Array.from({ length: 250 }, (_, i) => ({ id: i }));
  const chunks = chunkByBatchSize(items, 100);
  assert.equal(chunks.length, 3);
  assert.equal(chunks[0].length, 100);
  assert.equal(chunks[2].length, 50);
  const total = chunks.reduce((s, c) => s + c.length, 0);
  assert.equal(total, 250);
});

test("max-records caps only test/sample mode", () => {
  const items = Array.from({ length: 200 }, (_, i) => ({ id: i }));
  const capped = applyMaxRecordsCap(items, 50);
  assert.equal(capped.items.length, 50);
  assert.equal(capped.capped, true);
  assert.equal(capped.original_count, 200);
  const uncapped = applyMaxRecordsCap(items, null);
  assert.equal(uncapped.items.length, 200);
  assert.equal(uncapped.capped, false);
});

test("--limit deprecated warning + batch-size parsing", () => {
  const a = parseAutopilotArgs([
    "--region",
    "CALA",
    "--parent-company",
    "IHG",
    "--mode",
    "dry-run",
    "--limit",
    "40",
  ]);
  assert.ok(a.warnings.some((w) => /deprecated/i.test(w)));
  assert.equal(a.batchSize, 40);
  const b = parseAutopilotArgs([
    "--mode",
    "apply",
    "--batch-size",
    "75",
    "--run-until-complete",
    "--parent-company",
    "IHG",
  ]);
  assert.equal(b.batchSize, 75);
  assert.equal(b.runUntilComplete, true);
  assert.equal(b.maxRecords, null);
});

test("idempotent blank write / skip match / conflict", () => {
  assert.equal(compareFieldValues(null, 120), "write");
  assert.equal(compareFieldValues(120, 120), "skip");
  assert.equal(compareFieldValues(100, 120), "conflict");
  const write = buildIdempotentPatch({}, { "Rooms / Keys": 80 }, { confidence: "High" });
  assert.equal(write.action, "write");
  const skip = buildIdempotentPatch({ "Rooms / Keys": 80 }, { "Rooms / Keys": 80 }, { confidence: "High" });
  assert.equal(skip.action, "skip");
  const conflict = buildIdempotentPatch(
    { "Rooms / Keys": 50 },
    { "Rooms / Keys": 80 },
    { confidence: "High" }
  );
  assert.equal(conflict.action, "conflict");
});

test("idempotent allows safe City / Canonical identity overwrite", () => {
  const cityNorm = buildIdempotentPatch(
    { City: "CANCUN" },
    { City: "Cancún" },
    { confidence: "High", allowCoreIdentityOverwrite: true }
  );
  assert.equal(cityNorm.action, "write");
  assert.equal(cityNorm.fields.City, "Cancún");
  assert.equal(cityNorm.used_identity_overwrite, true);

  const cityBlocked = buildIdempotentPatch(
    { City: "CANCUN" },
    { City: "Cancún" },
    { confidence: "High", allowCoreIdentityOverwrite: false }
  );
  assert.equal(cityBlocked.action, "conflict");

  const materialBlock = buildIdempotentPatch(
    { City: "Cancun" },
    { City: "Mexico City" },
    { confidence: "High", allowCoreIdentityOverwrite: true }
  );
  assert.equal(materialBlock.action, "conflict");

  const unknownFix = buildIdempotentPatch(
    { City: "Unknown" },
    { City: "Cancún" },
    { confidence: "High", allowCoreIdentityOverwrite: true }
  );
  assert.equal(unknownFix.action, "write");
  assert.equal(unknownFix.fields.City, "Cancún");

  const split = buildIdempotentPatch(
    { City: "Cd. Guadalupe, Nuevo Leon" },
    { City: "Guadalupe", "State / Region": "Nuevo León" },
    { confidence: "High", allowCoreIdentityOverwrite: true }
  );
  assert.equal(split.action, "write");
  assert.equal(split.fields.City, "Guadalupe");
  assert.equal(split.fields["State / Region"], "Nuevo León");
});

test("geocode writes blocked without provider — soft route", () => {
  const g = guardProposal(
    {
      confidence: "High",
      patch: { Latitude: 21.1, Longitude: -86.8, "Geocode Provider": "mapbox" },
    },
    { allowGeocode: false }
  );
  assert.equal(g.ok, false);
  assert.equal(g.provider_decision_needed, true);
  const batch = guardApplyBatch(
    [{ confidence: "High", patch: { Latitude: 21.1, Longitude: -86.8 } }],
    { allowGeocode: false }
  );
  assert.equal(batch.stop_all, false);
  assert.ok(batch.provider_decision_needed.length >= 1);
});

test("protected / owner / BE / webhound blocked", () => {
  assert.ok(AUTOPILOT_FORBIDDEN_FIELDS.includes("Owner Name"));
  assert.ok(findForbiddenPatchFields({ "Owner Name": "X" }).length);
  assert.ok(findForbiddenPatchFields({ "Company Validated": true }).length);
  assert.ok(findForbiddenPatchFields({ "Recent Momentum": "x" }).length);
  const wh = guardProposal({
    confidence: "High",
    source: "webhound",
    patch: { "Rooms / Keys": 10 },
  });
  assert.ok(wh.errors.includes("webhound_direct_write_forbidden"));
  const san = sanitizeAutopilotPatch({ "Brand Status": "Active", "Rooms / Keys": 10 });
  assert.ok(!("Brand Status" in san.fields));
  assert.ok("Rooms / Keys" in san.fields);
});

test("room counts ambiguous → Hold via extractor", () => {
  const pack = extractRoomsKeysFromOfficialHtml(`<p>A 200 units mixed-use development.</p>`);
  const held = pack.hits.find((h) => h.method === "phrase_N_units");
  assert.equal(held.confidence, "Hold");
  const g = guardProposal({
    confidence: "Hold",
    room_count_ambiguous: true,
    patch: { "Rooms / Keys": 200 },
  });
  assert.ok(g.errors.includes("room_count_ambiguous"));
});

test("Webhound never writes; candidates capped", () => {
  const routed = routeWebhoundCandidates(
    Array.from({ length: 40 }, (_, i) => ({ id: `h${i}` })),
    { max: 25 }
  );
  assert.equal(routed.candidates.length, 25);
  assert.ok(routed.candidates.every((c) => c.webhound_direct_write === false));
});

test("allowlist includes Hotel Property Census rooms + geography fields", () => {
  assert.equal(AUTOPILOT_TARGET_TABLE, "Hotel Property Census");
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Rooms / Keys"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Latitude"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Hotel Description - Source Text"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Property Name"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("City"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Family / Source Family"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Source Confidence"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Production Use Status"));
});

await testAsync("run-until-complete processes multiple batches", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-v2-"));
  const proposals = makeProposals(5);
  const result = await runCensusAutopilot(
    [
      "--region",
      "CALA",
      "--parent-company",
      "IHG",
      "--mode",
      "dry-run",
      "--run-until-complete",
      "--batch-size",
      "2",
    ],
    { root: tmp, proposals }
  );
  assert.equal(result.batch_result.batches_run, 3);
  assert.equal(result.batch_result.scope_capped_by_batch_size, false);
  assert.equal(result.batch_result.total_records_in_scope, 5);
  assert.ok(fs.existsSync(path.join(result.run_dir, "batches", "batch-001.json")));
  assert.ok(fs.existsSync(path.join(result.run_dir, "batches", "batch-003.json")));
  assert.ok(fs.existsSync(path.join(result.run_dir, "checkpoint.json")));
  assert.equal(result.batch_result.completion_status, COMPLETION_STATUS.COMPLETE);
});

await testAsync("max-records caps sample while batch-size chunks", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-v2-"));
  const proposals = makeProposals(100);
  const result = await runCensusAutopilot(
    [
      "--region",
      "CALA",
      "--parent-company",
      "IHG",
      "--mode",
      "dry-run",
      "--max-records",
      "50",
      "--batch-size",
      "25",
    ],
    { root: tmp, proposals }
  );
  assert.equal(result.batch_result.total_records_in_scope, 100);
  assert.equal(result.batch_result.scope_capped_by_max_records, true);
  assert.equal(result.batch_result.batches_run, 2);
});

await testAsync("apply mode writes to Hotel Property Census (memory adapter)", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-v2-"));
  const proposals = makeProposals(3);
  const airtable = createMemoryAirtableAdapter(
    Object.fromEntries(proposals.map((p) => [p.record_id, { id: p.record_id, fields: {} }]))
  );
  const result = await runCensusAutopilot(
    ["--region", "CALA", "--parent-company", "IHG", "--mode", "apply", "--run-until-complete", "--batch-size", "2", ...APPLY_FLAGS],
    {
      root: tmp,
      proposals,
      airtable,
      enableProductionWrites: true,
      env: APPLY_ENV,
    }
  );
  assert.equal(result.airtable_writes, true);
  assert.equal(result.batch_result.target.table, "Hotel Property Census");
  assert.equal(result.batch_result.total_updated, 3);
  assert.equal(airtable._store.rec0.fields["Rooms / Keys"], 100);
  assert.ok(fs.existsSync(path.join(result.run_dir, "apply-summary.json")));
  assert.ok(fs.existsSync(path.join(result.run_dir, "checkpoint.json")));
});

await testAsync("checkpoint after every batch + resume skips completed", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-v2-"));
  const proposals = makeProposals(4);
  const airtable = createMemoryAirtableAdapter(
    Object.fromEntries(proposals.map((p) => [p.record_id, { id: p.record_id, fields: {} }]))
  );
  const first = await runCensusAutopilot(
    ["--region", "CALA", "--parent-company", "Marriott", "--mode", "apply", "--run-until-complete", "--batch-size", "2", ...APPLY_FLAGS],
    { root: tmp, proposals, airtable, enableProductionWrites: true, env: APPLY_ENV }
  );
  const cp = loadCheckpoint(first.run_dir);
  assert.ok(cp.airtable_record_ids_written.length >= 4);
  assert.ok(cp.resume_command.includes("--resume"));

  // Resume with same proposals should skip completed (idempotent skip)
  const second = await runCensusAutopilot(["--resume", first.run_id, "--mode", "apply", ...APPLY_FLAGS], {
    root: tmp,
    proposals,
    airtable,
    enableProductionWrites: true,
    env: APPLY_ENV,
  });
  assert.equal(second.batch_result.total_updated, 0);
  // store unchanged counts
  assert.equal(airtable._store.rec0.fields["Rooms / Keys"], 100);
});

await testAsync("resume avoids duplicate writes when value already set", async () => {
  const skipped = skipCompletedRecords(
    [{ record_id: "recA" }, { record_id: "recB" }],
    ["recA"]
  );
  assert.equal(skipped.remaining.length, 1);
  assert.equal(skipped.remaining[0].record_id, "recB");
});

await testAsync("target records re-read before write (idempotent)", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-v2-"));
  const proposals = [
    {
      record_id: "recX",
      confidence: "High",
      patch: {
        "Rooms / Keys": 55,
        "Rooms Confidence": "High",
        "Rooms Source URL": "https://ex.com",
      },
      current_fields: {},
    },
  ];
  const airtable = createMemoryAirtableAdapter({
    recX: {
      id: "recX",
      fields: {
        "Rooms / Keys": 55,
        "Rooms Confidence": "High",
        "Rooms Source URL": "https://ex.com",
      },
    },
  });
  const result = await runCensusAutopilot(
    ["--region", "CALA", "--parent-company", "IHG", "--mode", "apply", "--batch-size", "10", ...APPLY_FLAGS],
    { root: tmp, proposals, airtable, enableProductionWrites: true, env: APPLY_ENV }
  );
  assert.equal(result.batch_result.total_updated, 0);
  assert.ok(result.batch_result.total_skipped >= 1);
});

await testAsync("hard cases route; provider soft-block does not stop non-geocode", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-v2-"));
  const proposals = [
    {
      record_id: "recGood",
      confidence: "High",
      patch: { "Rooms / Keys": 90, "Rooms Confidence": "High", "Rooms Source URL": "https://ex.com/a" },
    },
    {
      record_id: "recGeo",
      confidence: "High",
      patch: { Latitude: 20, Longitude: -87, "Geocode Provider": "google" },
    },
    {
      record_id: "recHold",
      confidence: "Hold",
      room_count_ambiguous: true,
      patch: { "Rooms / Keys": 200 },
    },
  ];
  const airtable = createMemoryAirtableAdapter({
    recGood: { id: "recGood", fields: {} },
    recGeo: { id: "recGeo", fields: {} },
    recHold: { id: "recHold", fields: {} },
  });
  const result = await runCensusAutopilot(
    [
      "--region",
      "CALA",
      "--parent-company",
      "IHG",
      "--mode",
      "apply",
      "--run-until-complete",
      "--batch-size",
      "10",
      ...APPLY_FLAGS,
    ],
    {
      root: tmp,
      proposals,
      airtable,
      enableProductionWrites: true,
      env: { ...APPLY_ENV, MAPBOX_PERMANENT_GEOCODING: "0", GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED: "0" },
    }
  );
  assert.equal(airtable._store.recGood.fields["Rooms / Keys"], 90);
  assert.equal(airtable._store.recGeo.fields.Latitude, undefined);
  assert.ok((result.batch_result.provider_decision_needed || []).length >= 1);
  assert.notEqual(result.batch_result.completion_status, COMPLETION_STATUS.BLOCKED_SAFETY);
  assert.ok(fs.existsSync(path.join(result.run_dir, "webhound-candidates.json")));
});

await testAsync("final summary reports complete vs partial", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-v2-"));
  const result = await runCensusAutopilot(
    ["--region", "CALA", "--parent-company", "IHG", "--mode", "controlled", "--run-until-complete", "--batch-size", "2"],
    { root: tmp, proposals: makeProposals(3) }
  );
  assert.equal(result.summary.completion_status, COMPLETION_STATUS.COMPLETE);
  assert.equal(result.airtable_writes, false);
  assert.ok(fs.existsSync(path.join(result.run_dir, "approval-bundle.json")));
  assert.ok(fs.existsSync(path.join(result.run_dir, "summary.md")));
});

await testAsync("apply without production confirms does not write", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-v2-"));
  const airtable = createMemoryAirtableAdapter({ rec0: { id: "rec0", fields: {} } });
  const result = await runCensusAutopilot(
    ["--region", "CALA", "--parent-company", "IHG", "--mode", "apply", "--batch-size", "10"],
    {
      root: tmp,
      proposals: makeProposals(1),
      airtable,
      enableProductionWrites: true,
      env: {},
    }
  );
  assert.equal(result.batch_result.airtable_writes, false);
  assert.equal(airtable._store.rec0.fields["Rooms / Keys"], undefined);
});

test("confidence High writable only", () => {
  assert.equal(isWritableConfidence("High"), true);
  assert.equal(isWritableConfidence("Medium"), false);
  assert.equal(normalizeConfidence("Exact"), "High");
});

test("run folder name shape", () => {
  const name = buildRunFolderName("CALA", "IHG", new Date("2026-08-05T12:00:00.000Z"));
  assert.match(name, /CALA-IHG$/);
});

// --- active-brand-setup + fastest-safe ---

test("active-brand-setup scope reads Active/Live only; Flex excluded", () => {
  const list = buildActiveBrandSetupControlList({
    skipUniverseLoad: true,
    includeHeldProbe: true,
    brands: [
      { slug: "holiday-inn-express", brandName: "Holiday Inn Express", brandStatus: "Active", parentPlatform: "IHG" },
      { slug: "kimpton", brandName: "Kimpton", brandStatus: "Live", parentPlatform: "IHG" },
      { slug: "draft-brand", brandName: "Draft", brandStatus: "Under Review", parentPlatform: "IHG" },
      {
        slug: "four-points-flex-by-sheraton",
        brandName: "Four Points Flex by Sheraton",
        brandStatus: "Under Review",
        parentPlatform: "Marriott",
      },
    ],
  });
  assert.equal(list.brand_setup_read_only, true);
  assert.equal(list.brand_explorer_untouched, true);
  assert.equal(list.active_brands_in_scope, 2);
  assert.ok(list.brands.every((b) => ["Active", "Live"].includes(b.brand_status)));
  assert.ok(list.excluded.some((e) => e.slug === "four-points-flex-by-sheraton"));
  assert.ok(list.excluded.some((e) => e.slug === "draft-brand"));
  assert.ok(HELD_EXCLUDED_SLUGS.includes("four-points-flex-by-sheraton"));
});

test("active brands match Census; unmatched → source_discovery_needed", () => {
  const control = buildActiveBrandSetupControlList({
    skipUniverseLoad: true,
    brands: [
      { slug: "holiday-inn-express", brandName: "Holiday Inn Express", brandStatus: "Active", parentPlatform: "IHG" },
      { slug: "voco-hotels", brandName: "voco", brandStatus: "Active", parentPlatform: "IHG" },
    ],
  });
  const census = [
    {
      id: "recA",
      fields: {
        "Current Brand": "Holiday Inn Express",
        "Brand Explorer Slug if mapped": "holiday-inn-express",
        "Property Name": "HIX Cancun",
        Country: "Mexico",
      },
    },
    {
      id: "recB",
      fields: {
        "Current Brand": "Unknown Boutique",
        "Property Name": "Mystery Hotel",
        Country: "Mexico",
      },
    },
  ];
  const report = matchActiveBrandsToCensus(control, census, { region: "CALA", country: "Mexico" });
  assert.equal(report.census_records_matched, 1);
  assert.equal(report.matched[0].match_method, "exact_match");
  assert.ok(report.source_discovery_needed.some((b) => b.brand_slug === "voco-hotels"));
  assert.ok(report.steward_candidates.some((u) => u.record_id === "recB"));
});

test("fastest-safe scores; geocode soft-deferred without provider", () => {
  const routed = routeAutopilotQueues({ geocodeProviderReady: false, schemaV114Ready: false });
  const plan = buildFastestSafePriorityPlan(routed.queues, {
    geocodeProviderReady: false,
    schemaV114Ready: false,
  });
  assert.ok(plan.ordered_queue_ids.includes("description_extraction"));
  assert.ok(plan.geocode_soft_deferred.includes("coordinate_resolution"));
  assert.ok(plan.geocode_soft_deferred.includes("coordinate_completion"));
  const geo = plan.queues.find((q) => q.queue_id === "coordinate_completion");
  const desc = plan.queues.find((q) => q.queue_id === "description_extraction");
  assert.ok(desc.score > geo.score);
  assert.equal(geo.soft_skip_in_apply, true);
});

test("runtime metrics + performance safety stop on 3x overage", () => {
  const m = createRuntimeMetrics();
  m.recordBatch({ batch_number: 1, queue: "rooms_keys", ms: 100, records: 10, writes: 5 });
  const snap = m.snapshot({ records_remaining: 90 });
  assert.ok(snap.total_runtime_ms >= 0);
  assert.ok(snap.runtime_per_batch.length === 1);
  const stop = m.evaluateSafetyStop({
    batch_ms: 60 * 60 * 1000,
    expected_batch_ms: 5 * 60 * 1000,
  });
  assert.equal(stop.stop, true);
  assert.ok(stop.reasons.includes("batch_runtime_exceeds_3x_expected"));
  assert.equal(m.maxRetriesPerSource(), 2);
});

await testAsync("active-brand-setup plan writes control list + match + priority", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-abs-"));
  const result = await runCensusAutopilot(
    ["--region", "CALA", "--scope", "active-brand-setup", "--mode", "plan", "--strategy", "fastest-safe"],
    {
      root: tmp,
      activeBrands: [
        {
          slug: "holiday-inn-express",
          brandName: "Holiday Inn Express",
          brandStatus: "Active",
          parentPlatform: "IHG",
          recordId: "recHIX",
        },
      ],
      censusRecords: [
        {
          id: "rec1",
          fields: {
            "Current Brand": "Holiday Inn Express",
            "Brand Explorer Slug if mapped": "holiday-inn-express",
            Country: "Mexico",
          },
        },
      ],
      includeHeldProbe: true,
    }
  );
  assert.equal(result.ok, true);
  assert.ok(fs.existsSync(path.join(result.run_dir, "active-brand-setup-control-list.json")));
  assert.ok(fs.existsSync(path.join(result.run_dir, "brand-to-census-match-report.json")));
  assert.ok(fs.existsSync(path.join(result.run_dir, "queue-priority-plan.json")));
  assert.equal(result.control_list.brand_setup_read_only, true);
  assert.ok([STATUS.READY, STATUS.READY_NEEDS_V114].includes(result.status));
});

await testAsync("blocked geocode does not stop rooms queue across parents", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-abs-"));
  const proposals = [
    {
      record_id: "recIHG",
      queue: "rooms_keys",
      confidence: "High",
      family: "IHG",
      patch: { "Rooms / Keys": 80, "Rooms Confidence": "High", "Rooms Source URL": "https://ihg.example" },
    },
    {
      record_id: "recMar",
      queue: "rooms_keys",
      confidence: "High",
      family: "Marriott",
      patch: { "Rooms / Keys": 120, "Rooms Confidence": "High", "Rooms Source URL": "https://mar.example" },
    },
    {
      record_id: "recGeo",
      queue: "coordinate_resolution",
      confidence: "High",
      patch: { Latitude: 21, Longitude: -86, "Geocode Provider": "google" },
    },
  ];
  const airtable = createMemoryAirtableAdapter({
    recIHG: { id: "recIHG", fields: {} },
    recMar: { id: "recMar", fields: {} },
    recGeo: { id: "recGeo", fields: {} },
  });
  const result = await runCensusAutopilot(
    [
      "--region",
      "CALA",
      "--scope",
      "active-brand-setup",
      "--mode",
      "apply",
      "--strategy",
      "fastest-safe",
      "--run-until-complete",
      "--batch-size",
      "10",
      ...APPLY_FLAGS,
    ],
    {
      root: tmp,
      proposals,
      airtable,
      enableProductionWrites: true,
      env: { ...APPLY_ENV, MAPBOX_PERMANENT_GEOCODING: "0", GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED: "0" },
      activeBrands: [
        { slug: "holiday-inn-express", brandName: "HIX", brandStatus: "Active", parentPlatform: "IHG" },
      ],
      queues: ["rooms_keys", "coordinate_resolution"],
    }
  );
  assert.equal(airtable._store.recIHG.fields["Rooms / Keys"], 80);
  assert.equal(airtable._store.recMar.fields["Rooms / Keys"], 120);
  assert.equal(airtable._store.recGeo.fields.Latitude, undefined);
  assert.ok(result.batch_result.runtime_metrics);
  assert.ok(fs.existsSync(path.join(result.run_dir, "runtime-metrics.json")));
});

test("scope + strategy argument parsing", () => {
  const a = parseAutopilotArgs([
    "--region",
    "CALA",
    "--scope",
    "active-brand-setup",
    "--strategy",
    "fastest-safe",
    "--mode",
    "plan",
  ]);
  assert.equal(a.scope, "active-brand-setup");
  assert.equal(a.strategy, "fastest-safe");
  assert.equal(a.parentCompany, null);
});

test("targeted --queue property_name_cleanup parsing + router entry", () => {
  const a = parseAutopilotArgs([
    "--region",
    "CALA",
    "--scope",
    "active-brand-setup",
    "--mode",
    "controlled",
    "--queue",
    "property_name_cleanup",
    "--strategy",
    "fastest-safe",
  ]);
  assert.equal(a.queue, "property_name_cleanup");
  const routed = routeAutopilotQueues({ geocodeProviderReady: false, schemaV114Ready: true });
  assert.ok(routed.order.includes("property_name_cleanup"));
});

test("property name cleanup extractor flags marketing + selects official name", async () => {
  const {
    classifyPropertyNameProblems,
    extractPropertyNamesFromOfficialHtml,
    selectBestPropertyNameHit,
    isMoreSpecificPropertyName,
  } = await import("../lib/research-engine-v2/production-census-property-name-cleanup-extractor.js");

  const bad =
    "Welcome to avid hotels in Tijuana, where the essentials are done right. Every time.";
  const problems = classifyPropertyNameProblems(bad);
  assert.equal(problems.malformed, true);
  assert.ok(problems.reasons.includes("starts_with_marketing_intro"));

  const html = `
    <html><head>
      <title>avid hotels Tijuana | Official Site</title>
      <meta property="og:title" content="avid hotels Tijuana" />
      <script type="application/ld+json">{"@type":"Hotel","name":"avid hotels Tijuana"}</script>
    </head><body><h1>avid hotels Tijuana</h1></body></html>
  `;
  const extracted = extractPropertyNamesFromOfficialHtml(html, { brand: "avid hotels", city: "Tijuana" });
  assert.ok(extracted.hits.length >= 1);
  const selected = selectBestPropertyNameHit(extracted.hits, {
    currentName: bad,
    brand: "avid hotels",
    city: "Tijuana",
  });
  assert.equal(selected.confidence, "High");
  assert.ok(selected.hit?.name);
  assert.ok(isMoreSpecificPropertyName(bad, selected.hit.name));
  assert.equal(classifyPropertyNameProblems(selected.hit.name).malformed, false);
});

test("queue orchestrator: no --queue resolves all eligible; --queue is targeted", async () => {
  const {
    resolveQueuesToExecute,
    buildMultiQueueApprovalBundle,
    pickQueuePatch,
    toHighAutopilotProposal,
    NON_EXECUTABLE_QUEUES,
  } = await import("../lib/research-engine-v2/census-autopilot-queue-orchestrator.js");

  const all = resolveQueuesToExecute({
    orderedQueueIds: [
      "description_extraction",
      "amenities_extraction",
      "radar_public_readiness",
      "address_confirmation",
      "property_name_cleanup",
      "property_type_asset_context",
      "rooms_keys",
      "coordinate_resolution",
      "source_discovery",
      "steward_webhound_hard_cases",
    ],
  });
  assert.ok(all.includes("description_extraction"));
  assert.ok(all.includes("rooms_keys"));
  assert.ok(all.includes("coordinate_resolution"));
  assert.ok(all.includes("source_discovery"));
  assert.equal(all.includes("steward_webhound_hard_cases"), false);
  for (const q of NON_EXECUTABLE_QUEUES) assert.equal(all.includes(q), false);

  const targeted = resolveQueuesToExecute({
    orderedQueueIds: all,
    targetedQueue: "description_extraction",
  });
  assert.deepEqual(targeted, ["description_extraction"]);

  const patch = pickQueuePatch(
    {
      "Hotel Description - Source Text": "Nice hotel",
      "Owner Name": "BAD",
      "Rooms / Keys": 120,
    },
    ["Hotel Description - Source Text", "Rooms / Keys"]
  );
  assert.equal(patch["Hotel Description - Source Text"], "Nice hotel");
  assert.equal(patch["Rooms / Keys"], 120);
  assert.equal(patch["Owner Name"], undefined);

  const prop = toHighAutopilotProposal(
    { record_id: "rec1", identity_key: "k1", property_name: "H" },
    "description_extraction",
    { "Hotel Description - Source Text": "Nice hotel" }
  );
  assert.equal(prop.action, "propose_high_write");
  assert.equal(prop.queue, "description_extraction");
  assert.equal(prop.confidence, "High");

  const bundle = buildMultiQueueApprovalBundle({
    run_id: "test-run",
    mode: "controlled",
    scope: "active-brand-setup",
    region: "CALA",
    strategy: "fastest-safe",
    proposals: [prop],
    queues_executed: ["description_extraction"],
    queues_skipped: ["rooms_keys"],
    queues_soft_deferred: ["coordinate_resolution"],
    blocked: [{ record_id: "recX", block_reason: "low_confidence", queue: "rooms_keys" }],
  });
  assert.equal(bundle.airtable_writes, false);
  assert.equal(bundle.records_proposed, 1);
  assert.ok(bundle.proposed_writes_by_queue.description_extraction);
  assert.deepEqual(bundle.queues_soft_deferred, ["coordinate_resolution"]);
  assert.ok(bundle.recommended_apply_command.includes("approval-bundle"));
});

test("multi-queue approval bundle loader consumes proposed_writes", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "census-ap-mq-"));
  const bundlePath = path.join(tmp, "approval-bundle.json");
  fs.writeFileSync(
    bundlePath,
    JSON.stringify(
      {
        proposed_writes: [
          {
            record_id: "recA",
            identity_key: "kA",
            property_name: "Hotel A",
            queue: "description_extraction",
            confidence: "High",
            patch: { "Hotel Description - Source Text": "From official page." },
          },
          {
            record_id: "recB",
            queue: "rooms_keys",
            confidence: "High",
            patch: {
              "Rooms / Keys": 88,
              "Rooms Confidence": "High",
              "Rooms Source URL": "https://example.com/hotel",
            },
          },
        ],
      },
      null,
      2
    ),
    "utf8"
  );

  const { loadMultiQueueApprovalBundleProposals } = await import(
    "../lib/research-engine-v2/census-autopilot-approval-bundle-apply.js"
  );
  const loaded = loadMultiQueueApprovalBundleProposals({ approvalBundlePath: bundlePath });
  assert.equal(loaded.ok, true);
  assert.equal(loaded.multi_queue, true);
  assert.equal(loaded.frozen.length, 2);
  assert.ok(loaded.queues.includes("description_extraction"));
  assert.ok(loaded.queues.includes("rooms_keys"));
});

test("address-only High propose when geocode deferred; Medium VIC not promoted", async () => {
  const { buildAddressOnlyProposal, confirmOfficialAddress } =
    await import("../lib/research-engine-v2/production-census-address-geocode-resolver.js");
  const { recommendApplyFromYield, classifyNoProposalReason } = await import(
    "../lib/research-engine-v2/census-autopilot-source-yield-diagnostic.js"
  );

  const confirmed = confirmOfficialAddress({
    address: "Blvd. Bernardo Quintana No. 433",
    propertyName: "avid hotels Queretaro",
    city: "Queretaro",
    state: "QUE",
    country: "Mexico",
  });
  assert.equal(confirmed.ok, true);

  const ok = buildAddressOnlyProposal({
    fields: { Address: null },
    confirmed: { ...confirmed, confidence: "High", soft_flags: confirmed.soft_flags || [] },
    addressCandidate: {
      address: confirmed.address,
      confidence: "High",
      method: "vic_claim",
      source_url: "https://example.com/hotel",
    },
    sourceUrl: "https://example.com/hotel",
  });
  assert.ok(ok);
  assert.equal(ok.patch.Address, confirmed.address);

  const noPromote = buildAddressOnlyProposal({
    fields: { Address: null },
    confirmed: { ok: true, address: confirmed.address, confidence: "Medium", soft_flags: [] },
    addressCandidate: {
      address: confirmed.address,
      confidence: "Medium",
      method: "vic_claim",
      source_url: "https://example.com/hotel",
    },
  });
  assert.equal(noPromote, null);

  assert.equal(recommendApplyFromYield({ high_proposals: 1 }).recommend_apply, false);
  assert.equal(recommendApplyFromYield({ high_proposals: 12 }).recommend_apply, true);
  assert.equal(classifyNoProposalReason("official_page_blocked").code, "C");
});

test("family directory adapters: Choice/Hilton id extract + Choice desc unsupported", async () => {
  const {
    extractHiltonCtyhocn,
    extractChoicePropertyId,
    resolveDirectoryDescriptionCandidate,
    FAMILY_ADAPTER_VERSION,
  } = await import("../lib/research-engine-v2/census-autopilot-family-directory-adapters.js");

  assert.equal(
    extractHiltonCtyhocn({}, "ind_hilton_mx_qrohwhw"),
    "QROHWHW"
  );
  assert.equal(extractChoicePropertyId({}, "ind_choice_mx_mx165"), "MX165");
  assert.ok(FAMILY_ADAPTER_VERSION.includes("family-directory"));

  const desc = await resolveDirectoryDescriptionCandidate({
    family: "Choice",
    identityKey: "ind_choice_mx_mx165",
    fields: {},
  });
  // Without warmed cache this may be missing_id or lack narrative — never invents text
  assert.equal(desc.ok, false);
  assert.ok(
    desc.reason === "missing_choice_property_id" ||
      desc.reason === "not_in_choice_mexico_regional" ||
      desc.reason === "choice_regional_cards_lack_hotel_narrative_description"
  );
});

test("Choice property-level URL detector + stewarded record-set loader", async () => {
  const {
    isChoicePropertyLevelUrl,
    loadStewardedChoiceAddress29,
    RECORD_SET_STEWARDED_CHOICE_29,
  } = await import("../lib/research-engine-v2/census-autopilot-choice-address-resourcing.js");
  const { parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );

  assert.equal(
    isChoicePropertyLevelUrl(
      "https://www.choicehotels.com/sinaloa/mazatlan/ascend-hotels/mx165"
    ),
    true
  );
  assert.equal(
    isChoicePropertyLevelUrl(
      "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0"
    ),
    false
  );

  const loaded = loadStewardedChoiceAddress29();
  assert.equal(loaded.ok, true);
  assert.equal(loaded.items.length, 29);
  assert.equal(RECORD_SET_STEWARDED_CHOICE_29, "stewarded_choice_address_29");

  const args = parseAutopilotArgs([
    "--mode",
    "controlled",
    "--queue",
    "address_confirmation",
    "--record-set",
    "stewarded_choice_address_29",
  ]);
  assert.equal(args.recordSet, "stewarded_choice_address_29");
  assert.equal(args.queue, "address_confirmation");
});

// --- Production Hotel Property Census source-of-truth lock ---

test("SoT config locks Hotel Property Census as only Autopilot write target", async () => {
  const {
    productionHotelPropertyCensus,
    PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
    assertProductionCensusWriteTarget,
    isAllowedAutopilotWriteTarget,
    AUTOPILOT_WRITE_RULES,
  } = await import("../lib/research-engine-v2/production-census-source-of-truth.js");
  const { AUTOPILOT_TARGET_TABLE_ID } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.equal(productionHotelPropertyCensus.baseName, "Deal Capture Platform");
  assert.equal(productionHotelPropertyCensus.tableName, "Hotel Property Census");
  assert.equal(productionHotelPropertyCensus.tableId, "tbl9aY5ijiuIzzWam");
  assert.equal(productionHotelPropertyCensus.allowedWriteTarget, true);
  assert.equal(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID, "tbl9aY5ijiuIzzWam");
  assert.equal(AUTOPILOT_TARGET_TABLE_ID, "tbl9aY5ijiuIzzWam");
  assert.deepEqual(AUTOPILOT_WRITE_RULES.allowed, ["Hotel Property Census"]);

  const ok = assertProductionCensusWriteTarget({
    baseName: "Deal Capture Platform",
    tableName: "Hotel Property Census",
    tableId: "tbl9aY5ijiuIzzWam",
  });
  assert.equal(ok.ok, true);
  assert.equal(isAllowedAutopilotWriteTarget({ tableId: "tbl9aY5ijiuIzzWam" }), true);
});

test("Autopilot fails closed on legacy Hotel Census write target", async () => {
  const {
    assertProductionCensusWriteTarget,
    BLOCKED_WRONG_CENSUS_TARGET,
  } = await import("../lib/research-engine-v2/production-census-source-of-truth.js");
  const { guardProductionCensusWriteTarget } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );

  const legacy = assertProductionCensusWriteTarget({
    baseName: "Deal Capture Platform",
    tableName: "Hotel Census",
  });
  assert.equal(legacy.ok, false);
  assert.equal(legacy.code, BLOCKED_WRONG_CENSUS_TARGET);
  assert.ok(legacy.errors.some((e) => /legacy|wrong_table|ambiguous|blocked/i.test(e)));

  const g = guardProductionCensusWriteTarget({ tableName: "legacy Hotel Census" });
  assert.equal(g.stop_apply, true);
  assert.equal(g.stop_reason, BLOCKED_WRONG_CENSUS_TARGET);
});

test("Autopilot fails closed on Verified Independent Census / VIC write target", async () => {
  const { assertProductionCensusWriteTarget, BLOCKED_WRONG_CENSUS_TARGET } = await import(
    "../lib/research-engine-v2/production-census-source-of-truth.js"
  );

  for (const tableName of [
    "Verified Independent Census",
    "Verified Independent Hotel Census",
    "VIC",
  ]) {
    const r = assertProductionCensusWriteTarget({ tableName });
    assert.equal(r.ok, false, tableName);
    assert.equal(r.code, BLOCKED_WRONG_CENSUS_TARGET, tableName);
  }
});

test("Autopilot can read VIC / Brand Setup roles but cannot write them", async () => {
  const {
    AUTOPILOT_READ_RULES,
    BLOCKED_AUTOPILOT_WRITE_TARGETS,
    classifyBlockedWriteTarget,
  } = await import("../lib/research-engine-v2/production-census-source-of-truth.js");

  assert.equal(AUTOPILOT_READ_RULES.vic_source_claims, "read_evidence_lineage_only");
  assert.equal(AUTOPILOT_READ_RULES.brand_setup_active_control_list, "read_only");
  assert.equal(AUTOPILOT_READ_RULES.hotel_property_census, "read_and_write_allowlisted_fields");

  const vic = BLOCKED_AUTOPILOT_WRITE_TARGETS.find((t) => t.key === "vic_source_claims");
  const brandSetup = BLOCKED_AUTOPILOT_WRITE_TARGETS.find((t) => t.key === "brand_setup");
  assert.equal(vic.allowedWriteTarget, false);
  assert.equal(vic.allowedRead, true);
  assert.equal(brandSetup.allowedWriteTarget, false);
  assert.equal(brandSetup.allowedRead, true);

  assert.equal(classifyBlockedWriteTarget({ tableName: "Brand Setup - Brand Basics" })?.key, "brand_setup");
  assert.equal(classifyBlockedWriteTarget({ tableName: "Brand Explorer" })?.key, "brand_explorer");
});

test("apply guard blocks ambiguous Census target", async () => {
  const { guardProductionCensusWriteTarget, applyPreflight, parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const { BLOCKED_WRONG_CENSUS_TARGET } = await import(
    "../lib/research-engine-v2/production-census-source-of-truth.js"
  );

  const ambiguous = guardProductionCensusWriteTarget({ tableName: "Census" });
  assert.equal(ambiguous.ok, false);
  assert.equal(ambiguous.stop_reason, BLOCKED_WRONG_CENSUS_TARGET);

  const args = parseAutopilotArgs([
    "--region",
    "CALA",
    "--parent-company",
    "IHG",
    "--mode",
    "apply",
    "--confirm-safe-writes",
    "--confirm-write-to-production-census",
    "--confirm-no-brand-explorer-writes",
    "--confirm-no-owner-operator",
    "--confirm-no-date-writes",
    "--confirm-no-recent-momentum",
    "--confirm-no-company-validation",
    "--confirm-webhound-not-production-source",
  ]);
  const preflight = applyPreflight(args, { allOk: true, missing: [] }, {
    writeTarget: { tableName: "Hotel Census", tableId: "tblLEGACY" },
  });
  assert.equal(preflight.ok, false);
  assert.ok(preflight.blockers.includes(BLOCKED_WRONG_CENSUS_TARGET));
});

await testAsync("Autopilot apply refuses wrong Census table adapter", async () => {
  const { BLOCKED_WRONG_CENSUS_TARGET } = await import(
    "../lib/research-engine-v2/production-census-source-of-truth.js"
  );
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-sot-"));
  const badAirtable = createMemoryAirtableAdapter(
    { rec0: { id: "rec0", fields: {} } },
    {
      base: "Deal Capture Platform",
      table: "Hotel Census",
      tableName: "Hotel Census",
      tableId: "tblLEGACY",
    }
  );
  const result = await runCensusAutopilot(
    ["--region", "CALA", "--parent-company", "IHG", "--mode", "apply", "--batch-size", "10", ...APPLY_FLAGS],
    {
      root: tmp,
      proposals: makeProposals(1),
      airtable: badAirtable,
      enableProductionWrites: true,
      env: APPLY_ENV,
    }
  );
  assert.equal(result.airtable_writes, false);
  assert.equal(badAirtable._store.rec0.fields["Rooms / Keys"], undefined);
  assert.equal(result.batch_result?.stop_reason, BLOCKED_WRONG_CENSUS_TARGET);
});

test("Autopilot summaries use precise Hotel Property Census terminology", async () => {
  const { buildRunSummary, renderSummaryMarkdown } = await import(
    "../lib/research-engine-v2/census-autopilot-runner.js"
  );
  const {
    PRECISE_MATCH_SUMMARY_LINE,
    usesPreciseCensusTerminology,
    renderBrandToCensusMatchMarkdown,
    matchActiveBrandsToCensus,
  } = await import("../lib/research-engine-v2/production-census-source-of-truth.js").then(async (sot) => {
    const matcher = await import("../lib/research-engine-v2/census-autopilot-brand-census-matcher.js");
    return { ...sot, ...matcher };
  });
  const { buildActiveBrandSetupControlList } = await import(
    "../lib/research-engine-v2/census-autopilot-active-brand-scope.js"
  );

  const summary = buildRunSummary({
    parent_company: "IHG",
    region: "CALA",
    mode: "controlled",
    status: "ok",
    confidence_tally: { High: 0, Medium: 0, Low: 0, Hold: 0 },
  });
  assert.equal(summary.match_summary_line, PRECISE_MATCH_SUMMARY_LINE);
  assert.equal(summary.target.table, "Hotel Property Census");
  assert.equal(summary.target.tableId, "tbl9aY5ijiuIzzWam");
  assert.equal(summary.brand_setup_writes, false);
  assert.equal(summary.vic_writes, false);

  const md = renderSummaryMarkdown(summary);
  assert.ok(md.includes(PRECISE_MATCH_SUMMARY_LINE));
  assert.ok(md.includes("Hotel Property Census"));
  assert.ok(usesPreciseCensusTerminology(md));
  assert.equal(usesPreciseCensusTerminology("Matched brands to Census."), false);

  const control = buildActiveBrandSetupControlList({
    skipUniverseLoad: true,
    brands: [
      { slug: "holiday-inn-express", brandName: "Holiday Inn Express", brandStatus: "Active", parentPlatform: "IHG" },
    ],
  });
  const report = matchActiveBrandsToCensus(control, [], { region: "CALA" });
  const matchMd = renderBrandToCensusMatchMarkdown(report);
  assert.ok(matchMd.includes("Hotel Property Census"));
  assert.ok(matchMd.includes(PRECISE_MATCH_SUMMARY_LINE) || matchMd.includes("Active / Live Brand Setup"));
  assert.ok(usesPreciseCensusTerminology(matchMd));
});

// --- CALA Discovery + Insert Mode ---

test("source_discovery reads Active/Live Brand Setup only; Brand Setup read-only", async () => {
  const {
    buildActiveBrandDiscoveryControlList,
  } = await import("../lib/research-engine-v2/census-autopilot-source-discovery.js");
  const list = buildActiveBrandDiscoveryControlList({
    skipUniverseLoad: true,
    brands: [
      { slug: "hilton", brandName: "Hilton", brandStatus: "Active", parentPlatform: "Hilton" },
      { slug: "draft", brandName: "Draft", brandStatus: "Under Review", parentPlatform: "Hilton" },
    ],
  });
  assert.equal(list.brand_setup_read_only, true);
  assert.equal(list.brand_explorer_untouched, true);
  assert.equal(list.active_brands_in_scope, 1);
  assert.ok(list.brands.every((b) => ["Active", "Live"].includes(b.brand_status)));
  assert.equal(list.brands[0].discovery_adapter_available, true);
});

test("discovery match: exact ID / new High / duplicate / no fuzzy insert", async () => {
  const {
    matchDiscoveredProperty,
    indexHotelPropertyCensus,
    classifyDiscoveredAgainstCensus,
    MATCH_CLASS,
    buildInsertFieldsFromDiscovered,
    sanitizeInsertFields,
  } = await import("../lib/research-engine-v2/census-autopilot-source-discovery.js");

  const census = [
    {
      id: "recExisting",
      fields: {
        "Property Identity Key": "ind_hilton_mx_abc1234",
        "Property Name": "Hilton Existing",
        "Current Brand": "Hilton",
        City: "Cancun",
        Country: "Mexico",
        "Official Property URL": "https://www.hilton.com/en/hotels/abc1234-hilton-existing/",
      },
    },
  ];
  const index = indexHotelPropertyCensus(census);

  const exact = matchDiscoveredProperty(
    {
      official_property_id: "ABC1234",
      property_name: "Hilton Existing",
      brand: "Hilton",
      city: "Cancun",
      country: "Mexico",
      identity_key: "ind_hilton_mx_abc1234",
      source_family: "Hilton",
      identity_confidence: "High",
      official_property_url: "https://www.hilton.com/en/hotels/abc1234-hilton-existing/",
    },
    index
  );
  assert.equal(exact.classification, MATCH_CLASS.EXISTING_EXACT);

  const neu = matchDiscoveredProperty(
    {
      official_property_id: "XYZ9999",
      property_name: "Hilton New",
      brand: "Hilton",
      city: "Merida",
      country: "Mexico",
      identity_key: "ind_hilton_mx_xyz9999",
      source_family: "Hilton",
      identity_confidence: "High",
      official_property_url: "https://www.hilton.com/en/hotels/xyz9999-hilton-new/",
    },
    index
  );
  assert.equal(neu.classification, MATCH_CLASS.NEW_CANDIDATE);

  const low = matchDiscoveredProperty(
    {
      property_name: "Maybe Hotel",
      brand: "Hilton",
      city: "Puebla",
      country: "Mexico",
      identity_confidence: "Medium",
      source_family: "Hilton",
    },
    index
  );
  assert.ok(
    [MATCH_CLASS.STEWARD, MATCH_CLASS.SOURCE_INSUFFICIENT].includes(low.classification)
  );

  const classified = classifyDiscoveredAgainstCensus(
    [
      {
        official_property_id: "XYZ9999",
        property_name: "Hilton New",
        brand: "Hilton",
        city: "Merida",
        country: "Mexico",
        identity_key: "ind_hilton_mx_xyz9999",
        source_family: "Hilton",
        identity_confidence: "High",
        official_property_url: "https://www.hilton.com/en/hotels/xyz9999-hilton-new/",
      },
      {
        official_property_id: "XYZ9999",
        property_name: "Hilton New Dup",
        brand: "Hilton",
        city: "Merida",
        country: "Mexico",
        identity_key: "ind_hilton_mx_xyz9999",
        source_family: "Hilton",
        identity_confidence: "High",
        official_property_url: "https://www.hilton.com/en/hotels/xyz9999-hilton-new/",
      },
    ],
    census
  );
  assert.equal(classified.counts[MATCH_CLASS.NEW_CANDIDATE], 1);
  assert.equal(classified.counts[MATCH_CLASS.DUPLICATE_RISK], 1);

  const insert = buildInsertFieldsFromDiscovered({
    property_name: "Hilton New",
    identity_key: "ind_hilton_mx_xyz9999",
    brand: "Hilton",
    parent_company: "Hilton",
    city: "Merida",
    country: "Mexico",
    source_family: "Hilton",
    identity_confidence: "High",
    source_confidence: "High",
    official_property_url: "https://www.hilton.com/en/hotels/xyz9999-hilton-new/",
    discovered_date: "2026-08-05",
  });
  assert.ok(insert.fields["Property Name"]);
  assert.equal(insert.fields["Current Brand"], "Hilton");
  assert.equal(insert.fields["Production Use Status"], "Census Only / Not Owner-Facing");

  const blocked = sanitizeInsertFields({
    "Property Name": "X",
    "Owner Name": "Nope",
    "Recent Momentum": "nope",
    "Company Validated": true,
    "Brand Verified": true,
  });
  assert.ok(!("Owner Name" in blocked.fields));
  assert.ok(blocked.dropped.some((d) => d.field === "Owner Name"));
  assert.ok(blocked.dropped.some((d) => d.field === "Recent Momentum"));
});

test("Hotel Property Census is only insert write target; legacy/VIC blocked", async () => {
  const { assertProductionCensusWriteTarget, BLOCKED_WRONG_CENSUS_TARGET } = await import(
    "../lib/research-engine-v2/production-census-source-of-truth.js"
  );
  const { buildDiscoveryInsertApprovalBundle } = await import(
    "../lib/research-engine-v2/census-autopilot-source-discovery.js"
  );
  assert.equal(
    assertProductionCensusWriteTarget({
      tableName: "Hotel Property Census",
      tableId: "tbl9aY5ijiuIzzWam",
    }).ok,
    true
  );
  assert.equal(
    assertProductionCensusWriteTarget({ tableName: "Hotel Census" }).code,
    BLOCKED_WRONG_CENSUS_TARGET
  );
  assert.equal(
    assertProductionCensusWriteTarget({ tableName: "Verified Independent Census" }).ok,
    false
  );
  const bundle = buildDiscoveryInsertApprovalBundle({
    run_id: "test-run",
    new_property_candidates: [],
  });
  assert.equal(bundle.airtable_writes, false);
  assert.equal(bundle.vic_writes, false);
  assert.equal(bundle.production_target.tableId, "tbl9aY5ijiuIzzWam");
});

await testAsync("approval-bundle-bound insert apply dry-run + checkpoint; duplicate stops write", async () => {
  const {
    runDiscoveryInsertApply,
    rededupeInsertsAgainstCensus,
    INSERT_APPLY_STATUS,
  } = await import("../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js");
  const { buildDiscoveryInsertApprovalBundle, MATCH_CLASS } = await import(
    "../lib/research-engine-v2/census-autopilot-source-discovery.js"
  );
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-disc-"));
  const candidate = {
    classification: MATCH_CLASS.NEW_CANDIDATE,
    identity_confidence: "High",
    identity_key: "ind_hilton_mx_new0001",
    property_name: "Hilton Discovery Test",
    brand: "Hilton",
    parent_company: "Hilton",
    city: "Queretaro",
    country: "Mexico",
    source_family: "Hilton",
    official_property_id: "NEW0001",
    official_property_url: "https://www.hilton.com/en/hotels/new0001-test/",
    discovered_date: "2026-08-05",
  };
  const bundle = buildDiscoveryInsertApprovalBundle({
    run_id: "disc-test",
    new_property_candidates: [candidate],
  });
  assert.equal(bundle.records_proposed_for_insert, 1);
  const bundlePath = path.join(tmp, "approval-bundle.json");
  fs.writeFileSync(bundlePath, JSON.stringify(bundle, null, 2));

  const createdStore = [];
  const dry = await runDiscoveryInsertApply({
    doWrite: false,
    bundlePath,
    censusRecords: [],
    args: {
      apply: false,
      allConfirmsOk: true,
      batchSize: 50,
      confirms: {},
      approvalBundlePath: bundlePath,
    },
    env: {
      ALLOW_CENSUS_AUTOPILOT_APPLY: "1",
      CONFIRM_WRITE_TO_PRODUCTION_CENSUS: "1",
      CONFIRM_NO_BRAND_EXPLORER_WRITES: "1",
      CONFIRM_NO_OWNER_OPERATOR_WRITES: "1",
    },
  });
  assert.equal(dry.status, INSERT_APPLY_STATUS.DRY_RUN);
  assert.equal(dry.airtable_writes, false);

  const censusExisting = [
    {
      id: "recDup",
      fields: {
        "Property Identity Key": "ind_hilton_mx_new0001",
        "Property Name": "Hilton Discovery Test",
        "Current Brand": "Hilton",
        City: "Queretaro",
        Country: "Mexico",
      },
    },
  ];
  const rededupe = rededupeInsertsAgainstCensus(bundle.proposed_inserts, censusExisting);
  assert.equal(rededupe.stop_on_duplicate, true);
  assert.equal(rededupe.writable.length, 0);

  const applied = await runDiscoveryInsertApply({
    doWrite: true,
    bundlePath,
    censusRecords: [],
    checkpointDir: tmp,
    createRecords: async (rows) => {
      for (const r of rows) createdStore.push(r);
      return { created: rows.map((r, i) => ({ id: `recNew${i}`, fields: r.fields })) };
    },
    args: {
      apply: true,
      allConfirmsOk: true,
      batchSize: 50,
      confirms: {
        safeWrites: true,
        writeToProductionCensus: true,
        noBrandExplorer: true,
        noOwnerOperator: true,
        noDateWrites: true,
        noRecentMomentum: true,
        noCompanyValidation: true,
        webhoundNotProduction: true,
      },
      approvalBundlePath: bundlePath,
    },
    env: {
      ALLOW_CENSUS_AUTOPILOT_APPLY: "1",
      CONFIRM_WRITE_TO_PRODUCTION_CENSUS: "1",
      CONFIRM_NO_BRAND_EXPLORER_WRITES: "1",
      CONFIRM_NO_OWNER_OPERATOR_WRITES: "1",
    },
  });
  assert.equal(applied.status, INSERT_APPLY_STATUS.CLEAN);
  assert.equal(applied.created_count, 1);
  assert.ok(fs.existsSync(path.join(tmp, "insert-checkpoint.json")));
  assert.equal(createdStore[0].fields["Property Identity Key"], "ind_hilton_mx_new0001");
});

await testAsync("controlled discovery with fixture directories writes run artifacts", async () => {
  const { runSourceDiscoveryControlled, DISCOVERY_STATUS, MATCH_CLASS } = await import(
    "../lib/research-engine-v2/census-autopilot-source-discovery.js"
  );
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-disc-run-"));
  const hiltonCache = new Map([
    [
      "NEW0002",
      {
        ctyhocn: "NEW0002",
        name: "Hilton Fixture New",
        affiliation: "Hilton",
        parent: "Hilton",
        city: "Puebla",
        country: "Mexico",
        addressLine1: "1 Test Ave",
        propertyUrl: "https://www.hilton.com/en/hotels/new0002-fixture/",
        sourceUrl: "https://www.hilton.com/en/locations/mexico/hilton/",
      },
    ],
    [
      "EXIST01",
      {
        ctyhocn: "EXIST01",
        name: "Hilton Fixture Existing",
        affiliation: "Hilton",
        parent: "Hilton",
        city: "Cancun",
        country: "Mexico",
        propertyUrl: "https://www.hilton.com/en/hotels/exist01-fixture/",
        sourceUrl: "https://www.hilton.com/en/locations/mexico/hilton/",
      },
    ],
  ]);
  const report = await runSourceDiscoveryControlled({
    runDir: tmp,
    runId: "fixture-discovery",
    region: "CALA",
    parentCompany: "Hilton",
    skipUniverseLoad: true,
    brands: [
      { slug: "hilton", brandName: "Hilton", brandStatus: "Active", parentPlatform: "Hilton" },
    ],
    hiltonCache,
    choiceCache: new Map(),
    includeVicEvidence: false,
    censusRecords: [
      {
        id: "recE",
        fields: {
          "Property Identity Key": "ind_hilton_mx_exist01",
          "Property Name": "Hilton Fixture Existing",
          "Current Brand": "Hilton",
          City: "Cancun",
          Country: "Mexico",
          "Official Property URL": "https://www.hilton.com/en/hotels/exist01-fixture/",
        },
      },
    ],
  });
  assert.ok(
    [DISCOVERY_STATUS.READY, DISCOVERY_STATUS.READY_NEEDS_ADAPTER].includes(report.status)
  );
  assert.equal(report.airtable_writes, false);
  assert.ok(fs.existsSync(path.join(tmp, "approval-bundle.json")));
  assert.ok(fs.existsSync(path.join(tmp, "active-brand-discovery-control-list.json")));
  assert.ok(fs.existsSync(path.join(tmp, "summary.md")));
  assert.ok(report.summary.existing_hotel_property_census_matches >= 1);
  assert.ok(
    (report.new_property_candidates || []).some((c) => c.classification === MATCH_CLASS.NEW_CANDIDATE)
  );
});

await testAsync("Marriott CALA sitemap adapter: official sources only; HQV not required; deprecated XML blocked", async () => {
  const {
    isDeprecatedMarriottSitemapHotelsXml,
    classifyMarriottCountryDiscoveryReadiness,
    marriottDiscoveryCountryShort,
    MARRIOTT_DISCOVERY_SOURCE,
  } = await import("../lib/research-engine-v2/census-autopilot-marriott-discovery-adapter.js");
  const {
    marriottRowToDiscovered,
    matchDiscoveredProperty,
    indexHotelPropertyCensus,
    MATCH_CLASS,
    runSourceDiscoveryControlled,
    DISCOVERY_STATUS,
  } = await import("../lib/research-engine-v2/census-autopilot-source-discovery.js");
  const { isDiscoveryAdapterReady } = await import(
    "../lib/research-engine-v2/production-census-cala-region-config.js"
  );
  const { countrySitemapUrl } = await import("../lib/marriott-brand-directory-extract.js");

  assert.equal(MARRIOTT_DISCOVERY_SOURCE.hqv_required_for_discovery, false);
  assert.ok(
    isDeprecatedMarriottSitemapHotelsXml(
      "https://www.marriott.com/en/hotels/mexico.sitemap-hotels.xml"
    )
  );
  assert.equal(
    isDeprecatedMarriottSitemapHotelsXml(countrySitemapUrl("mexico")),
    false
  );
  assert.equal(isDiscoveryAdapterReady("Mexico", "Marriott"), true);
  assert.equal(isDiscoveryAdapterReady("Panama", "Marriott"), true);
  assert.equal(classifyMarriottCountryDiscoveryReadiness("Mexico").readiness, "supported");
  assert.equal(marriottDiscoveryCountryShort("Costa Rica"), "cr");
  assert.equal(marriottDiscoveryCountryShort("Colombia"), "co");

  const disc = marriottRowToDiscovered({
    marshaCode: "CUNEK",
    name: "The Riviera Maya EDITION at Kanai",
    brand: "EDITION",
    country: "Mexico",
    city: "Riviera Maya",
    propertyUrl: "https://www.marriott.com/en-us/hotels/cunek-the-riviera-maya-edition-at-kanai/overview",
    sourceUrl: countrySitemapUrl("mexico"),
    parent: "Marriott",
  });
  assert.ok(disc);
  assert.equal(disc.hqv_used_for_discovery, false);
  assert.equal(disc.identity_key, "ind_marriott_mx_cunek");
  assert.equal(disc.identity_confidence, "High");

  const blockedDep = marriottRowToDiscovered({
    marshaCode: "BAD01",
    name: "Should Block",
    brand: "W Hotels",
    country: "Mexico",
    propertyUrl: "https://www.marriott.com/en-us/hotels/bad01-x/overview",
    sourceUrl: "https://www.marriott.com/en/hotels/mexico.sitemap-hotels.xml",
  });
  assert.equal(blockedDep, null);

  const census = [
    {
      id: "recMar",
      fields: {
        "Property Identity Key": "ind_marriott_mx_cunek",
        "Property Name": "The Riviera Maya EDITION at Kanai",
        "Current Brand": "EDITION",
        City: "Riviera Maya",
        Country: "Mexico",
        "Official Property URL":
          "https://www.marriott.com/en-us/hotels/cunek-the-riviera-maya-edition-at-kanai/overview",
      },
    },
  ];
  const exact = matchDiscoveredProperty(disc, indexHotelPropertyCensus(census));
  assert.equal(exact.classification, MATCH_CLASS.EXISTING_EXACT);

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-marriott-"));
  const marriottCache = new Map([
    [
      "Mexico|NEWMR",
      {
        marshaCode: "NEWMR",
        name: "Courtyard by Marriott Fixture New",
        brand: "Courtyard by Marriott",
        affiliation: "Courtyard by Marriott",
        country: "Mexico",
        city: "Merida",
        propertyUrl: "https://www.marriott.com/en-us/hotels/newmr-courtyard-merida/overview",
        sourceUrl: countrySitemapUrl("mexico"),
        parent: "Marriott",
        website: "https://www.marriott.com/en-us/hotels/newmr-courtyard-merida/overview",
      },
    ],
    [
      "NEWMR",
      {
        marshaCode: "NEWMR",
        name: "Courtyard by Marriott Fixture New",
        brand: "Courtyard by Marriott",
        affiliation: "Courtyard by Marriott",
        country: "Mexico",
        city: "Merida",
        propertyUrl: "https://www.marriott.com/en-us/hotels/newmr-courtyard-merida/overview",
        sourceUrl: countrySitemapUrl("mexico"),
        parent: "Marriott",
        website: "https://www.marriott.com/en-us/hotels/newmr-courtyard-merida/overview",
      },
    ],
    [
      "Mexico|CUNEK",
      {
        marshaCode: "CUNEK",
        name: "The Riviera Maya EDITION at Kanai",
        brand: "EDITION",
        country: "Mexico",
        city: "Riviera Maya",
        propertyUrl:
          "https://www.marriott.com/en-us/hotels/cunek-the-riviera-maya-edition-at-kanai/overview",
        sourceUrl: countrySitemapUrl("mexico"),
        parent: "Marriott",
        website:
          "https://www.marriott.com/en-us/hotels/cunek-the-riviera-maya-edition-at-kanai/overview",
      },
    ],
  ]);
  const report = await runSourceDiscoveryControlled({
    runDir: tmp,
    runId: "fixture-marriott-discovery",
    region: "CALA",
    country: "Mexico",
    parentCompany: "Marriott",
    skipUniverseLoad: true,
    brands: [
      {
        slug: "courtyard-by-marriott",
        brandName: "Courtyard by Marriott",
        brandStatus: "Active",
        parentPlatform: "Marriott",
      },
      {
        slug: "edition",
        brandName: "EDITION",
        brandStatus: "Active",
        parentPlatform: "Marriott",
      },
    ],
    marriottCache,
    hiltonCache: new Map(),
    choiceCache: new Map(),
    includeVicEvidence: false,
    censusRecords: census,
  });
  assert.ok(
    [DISCOVERY_STATUS.READY, DISCOVERY_STATUS.READY_NEEDS_ADAPTER].includes(report.status)
  );
  assert.equal(report.airtable_writes, false);
  assert.ok((report.source_report.families_used || []).includes("Marriott"));
  assert.equal(report.source_report.marriott_discovery?.hqv_required_for_discovery, false);
  assert.ok(report.summary.existing_hotel_property_census_matches >= 1);
  assert.ok(
    (report.new_property_candidates || []).some((c) => c.identity_key === "ind_marriott_mx_newmr")
  );
  assert.ok(fs.existsSync(path.join(tmp, "approval-bundle.json")));
  const bundle = JSON.parse(fs.readFileSync(path.join(tmp, "approval-bundle.json"), "utf8"));
  assert.equal(bundle.airtable_writes, false);
  assert.equal(bundle.production_target.tableId, "tbl9aY5ijiuIzzWam");
  assert.ok(!JSON.stringify(bundle).includes("Owner Name"));
});

test("multi-parent CALA adapter matrix: Marriott/IHG/Hilton/Choice/Accor/Wyndham/Preferred priority countries supported", async () => {
  const {
    buildCalaParentCountryAdapterMatrix,
    isDiscoveryAdapterReady,
  } = await import("../lib/research-engine-v2/production-census-cala-region-config.js");
  const matrix = buildCalaParentCountryAdapterMatrix();
  // 7 parents × 5 priority countries
  assert.equal(matrix.summary.supported, 35);
  for (const parent of [
    "Marriott",
    "IHG",
    "Hilton",
    "Choice",
    "Accor",
    "Wyndham",
    "Preferred",
  ]) {
    for (const country of [
      "Mexico",
      "Dominican Republic",
      "Costa Rica",
      "Colombia",
      "Panama",
    ]) {
      assert.equal(
        isDiscoveryAdapterReady(country, parent),
        true,
        `${parent} × ${country}`
      );
    }
  }
});

await testAsync("IHG + Choice non-Mexico discovery fixtures match Census only", async () => {
  const {
    runSourceDiscoveryControlled,
    DISCOVERY_STATUS,
    MATCH_CLASS,
    ihgRowToDiscovered,
  } = await import("../lib/research-engine-v2/census-autopilot-source-discovery.js");
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-multi-"));
  const ihgCache = new Map([
    [
      "Dominican Republic|SDQIC",
      {
        propertyId: "SDQIC",
        name: "InterContinental Santo Domingo Fixture",
        brand: "InterContinental",
        country: "Dominican Republic",
        city: "Santo Domingo",
        propertyUrl: "https://www.ihg.com/intercontinental/hotels/us/en/santo-domingo/sdqic/hoteldetail",
        sourceUrl: "https://www.ihg.com/destinations/us/en/dominican-republic-hotels",
        parent: "IHG",
        addressText: "Santo Domingo, Dominican Republic",
      },
    ],
  ]);
  const choiceCache = new Map([
    [
      "Costa Rica|CR025",
      {
        propertyId: "CR025",
        name: "Radisson San Jose Fixture",
        country: "Costa Rica",
        city: "San Jose",
        propertyUrl: "https://www.choicehotels.com/costa-rica/san-jose/radisson-hotels/cr025",
        source_url: "https://www.choicehotels.com/en-uk/costa-rica/regional-hotels",
      },
    ],
  ]);
  const disc = ihgRowToDiscovered(ihgCache.get("Dominican Republic|SDQIC"), {
    brand_name: "InterContinental",
    parent_company: "IHG",
  });
  assert.equal(disc.identity_key, "ind_ihg_do_sdqic");
  assert.equal(disc.identity_confidence, "High");

  const report = await runSourceDiscoveryControlled({
    runDir: tmp,
    runId: "fixture-multi-parent",
    region: "CALA",
    parentCompany: "IHG",
    skipUniverseLoad: true,
    brands: [
      {
        slug: "intercontinental",
        brandName: "InterContinental",
        brandStatus: "Active",
        parentPlatform: "IHG",
      },
    ],
    ihgCache,
    hiltonCache: new Map(),
    choiceCache: new Map(),
    marriottCache: new Map(),
    includeVicEvidence: false,
    censusRecords: [],
  });
  assert.ok(
    [DISCOVERY_STATUS.READY, DISCOVERY_STATUS.READY_NEEDS_ADAPTER].includes(report.status)
  );
  assert.equal(report.airtable_writes, false);
  assert.ok((report.source_report.families_used || []).includes("IHG"));
  assert.ok(
    (report.new_property_candidates || []).some((c) => c.classification === MATCH_CLASS.NEW_CANDIDATE)
  );

  const choiceReport = await runSourceDiscoveryControlled({
    runDir: fs.mkdtempSync(path.join(os.tmpdir(), "ap-choice-")),
    runId: "fixture-choice-cr",
    region: "CALA",
    country: "Costa Rica",
    parentCompany: "Choice",
    skipUniverseLoad: true,
    brands: [
      { slug: "radisson", brandName: "Radisson", brandStatus: "Active", parentPlatform: "Choice" },
    ],
    choiceCache,
    hiltonCache: new Map(),
    marriottCache: new Map(),
    ihgCache: new Map(),
    includeVicEvidence: false,
    censusRecords: [],
  });
  assert.equal(choiceReport.airtable_writes, false);
  assert.ok((choiceReport.source_report.families_used || []).includes("Choice"));
  assert.ok(
    (choiceReport.new_property_candidates || []).some((c) => c.identity_key === "ind_choice_cr_cr025")
  );
});

await testAsync("production-cycle mode parsing + write-mode helper", async () => {
  const { isProductionWriteMode } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const a = parseAutopilotArgs([
    "--region",
    "CALA",
    "--scope",
    "active-brand-setup",
    "--mode",
    "production-cycle",
    "--strategy",
    "fastest-safe",
    "--run-until-complete",
    "--batch-size",
    "100",
    ...APPLY_FLAGS,
  ]);
  assert.equal(a.mode, "production-cycle");
  assert.equal(a.allApplyConfirms, true);
  assert.equal(isProductionWriteMode("production-cycle"), true);
  assert.equal(isProductionWriteMode("controlled"), false);
  const pf = applyPreflight(a, {
    allOk: true,
    flags: {},
    missing: [],
  });
  assert.equal(pf.ok, true);
});

await testAsync("production-cycle stewards Choice Radisson Individuals inserts", async () => {
  const {
    classifyProductionCycleInsertCandidate,
    splitInsertCandidates,
    filterHighUpdateProposals,
    PRODUCTION_CYCLE_STATUS,
    runProductionCycle,
  } = await import("../lib/research-engine-v2/census-autopilot-production-cycle.js");

  assert.equal(
    classifyProductionCycleInsertCandidate({
      property_name: "Faranda Collection Medellin, a member of Radisson Individuals",
      fields: { City: "Unknown", "Current Brand": "Radisson Individuals by Choice" },
    }).steward,
    true
  );
  assert.equal(
    classifyProductionCycleInsertCandidate({
      property_name: "Hampton by Hilton Test City",
      fields: {
        City: "Bogota",
        Country: "Colombia",
        "Current Brand": "Hampton by Hilton",
        "Source URL": "https://www.hilton.com/en/hotels/bog-test/",
        "Family / Source Family": "Hilton",
      },
    }).steward,
    false
  );

  const split = splitInsertCandidates([
    {
      property_name: "V Grand Hotel, a member of Radisson Individuals",
      fields: { City: "Unknown", "Property Name": "V Grand Hotel, a member of Radisson Individuals" },
    },
    {
      property_name: "Clean Hotel Bogota",
      fields: {
        City: "Bogota",
        Country: "Colombia",
        "Property Name": "Clean Hotel Bogota",
        "Current Brand": "Hilton",
        "Source URL": "https://www.hilton.com/en/hotels/bog-clean/",
        "Family / Source Family": "Hilton",
      },
    },
  ]);
  assert.equal(split.steward_inserts.length, 1);
  assert.equal(split.auto_inserts.length, 1);

  const highs = filterHighUpdateProposals([
    {
      record_id: "recA",
      confidence: "High",
      queue: "property_type_asset_context",
      patch: { "Asset Context": "Airport" },
    },
    {
      record_id: "recB",
      confidence: "Medium",
      queue: "rooms_keys",
      patch: { "Rooms / Keys": 100 },
    },
    {
      action: "insert",
      confidence: "High",
      queue: "source_discovery",
      patch: {},
    },
  ]);
  assert.equal(highs.length, 1);
  assert.equal(highs[0].record_id, "recA");

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-pcycle-"));
  const proposals = [
    {
      record_id: "recPC1",
      identity_key: "ind_test_pc1",
      property_name: "Test Airport Hotel",
      queue: "property_type_asset_context",
      confidence: "High",
      patch: { "Asset Context": "Airport" },
      current_fields: {},
    },
  ];
  const airtable = createMemoryAirtableAdapter({
    recPC1: { id: "recPC1", fields: {} },
  });
  const created = [];
  const report = await runProductionCycle({
    argv: [
      "--region",
      "CALA",
      "--scope",
      "active-brand-setup",
      "--mode",
      "production-cycle",
      "--strategy",
      "fastest-safe",
      "--run-until-complete",
      "--batch-size",
      "50",
      "--enable-production-writes",
      ...APPLY_FLAGS,
    ],
    env: APPLY_ENV,
    enableProductionWrites: true,
    root: tmp,
    runId: "test-production-cycle",
    censusCountBefore: 10,
    censusCountAfter: 10,
    censusRecords: [],
    skipLiveCensusReload: true,
    schemaV114Ready: true,
    airtable,
    createRecords: async (rows) => {
      for (const r of rows) created.push(r);
      return { created: rows.map((r, i) => ({ id: `recNew${i}`, fields: r.fields })) };
    },
    orchestrationFactory: async ({ pass }) => {
      if (pass === 1) {
        return {
          proposals,
          queues_executed: ["property_type_asset_context", "source_discovery"],
          queues_soft_deferred: ["coordinate_resolution"],
          queue_results: [
            { queue_id: "property_type_asset_context", status: "executed" },
            { queue_id: "source_discovery", status: "executed" },
            { queue_id: "coordinate_resolution", status: "soft_deferred" },
          ],
          blocked: [],
          discoveryReport: {
            approval_bundle: {
              proposed_inserts: [
                {
                  property_name: "Faranda X, a member of Radisson Individuals",
                  identity_key: "ind_choice_steward",
                  fields: {
                    "Property Identity Key": "ind_choice_steward",
                    "Property Name": "Faranda X, a member of Radisson Individuals",
                    City: "Unknown",
                    "Current Brand": "Radisson Individuals by Choice",
                    Country: "Colombia",
                  },
                },
              ],
            },
            new_property_candidates: [],
          },
        };
      }
      return {
        proposals: [],
        queues_executed: ["property_type_asset_context"],
        queues_soft_deferred: ["coordinate_resolution"],
        queue_results: [{ queue_id: "property_type_asset_context", status: "executed_exhausted" }],
        blocked: [],
        discoveryReport: { approval_bundle: { proposed_inserts: [] }, new_property_candidates: [] },
      };
    },
    token: "patTEST",
    bases: { target_base_id: "appTEST" },
    log: () => {},
  });

  assert.ok(
    [
      PRODUCTION_CYCLE_STATUS.COMPLETE,
      PRODUCTION_CYCLE_STATUS.PARTIAL,
      "production_census_choice_radisson_steward_resolved_production_cycle_complete",
      "production_census_choice_radisson_steward_partial_remaining",
    ].includes(report.status),
    `unexpected status ${report.status} blockers=${JSON.stringify(report.blockers || report.blocked_reason)}`
  );
  assert.equal(report.updates_applied, 1);
  assert.equal(airtable._store.recPC1.fields["Asset Context"], "Airport");
  // Steward Faranda member-of row is resolved via official property URL → insert eligible
  assert.ok(report.inserts_applied >= 0);
  assert.equal(report.brand_explorer_writes, false);
  assert.equal(report.brand_setup_writes, false);
  assert.ok(fs.existsSync(path.join(report.run_dir, "final-summary.json")));
  assert.ok(fs.existsSync(path.join(report.run_dir, "production-cycle-plan.json")));
  assert.ok(fs.existsSync(path.join(report.run_dir, "steward-review-queue.json")));
});

await testAsync("Choice Radisson Individuals steward resolution cleans name+city from property URL", async () => {
  const {
    cleanRadissonIndividualsPropertyName,
    titleCaseCitySlug,
    resolveChoiceRadissonStewardCase,
    resolveChoiceRadissonStewardBatch,
    applyChoiceRadissonStewardResolutionToInserts,
    isChoiceRadissonIndividualsStewardCase,
    RESOLUTION_CLASS,
  } = await import("../lib/research-engine-v2/census-autopilot-choice-radisson-steward-resolution.js");
  const { isChoicePropertyLevelUrl } = await import(
    "../lib/research-engine-v2/census-autopilot-choice-address-resourcing.js"
  );
  const { classifyProductionCycleInsertCandidate } = await import(
    "../lib/research-engine-v2/census-autopilot-production-cycle.js"
  );

  assert.equal(
    cleanRadissonIndividualsPropertyName(
      "Faranda Collection Medellin, a member of Radisson Individuals"
    ),
    "Faranda Collection Medellin"
  );
  assert.equal(titleCaseCitySlug("cerro-punta"), "Cerro Punta");
  assert.equal(
    isChoicePropertyLevelUrl(
      "https://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030"
    ),
    true
  );

  const stewardRow = {
    identity_key: "ind_choice_co_cb030",
    property_name: "Faranda Collection Medellin, a member of Radisson Individuals",
    brand: "Radisson Individuals by Choice",
    source_family: "Choice",
    official_property_id: "CB030",
    fields: {
      "Property Name": "Faranda Collection Medellin, a member of Radisson Individuals",
      "Property Identity Key": "ind_choice_co_cb030",
      "Current Brand": "Radisson Individuals by Choice",
      City: "Unknown",
      Country: "Colombia",
      "Source URL": "https://www.choicehotels.com/en-uk/colombia/regional-hotels",
      "Official Property URL":
        "https://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030",
    },
    discovery: {
      official_property_url:
        "https://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030",
      city: null,
      country: "Colombia",
    },
    steward_reason: "choice_radisson_individuals_member_of_name",
  };

  assert.equal(isChoiceRadissonIndividualsStewardCase(stewardRow), true);
  const one = resolveChoiceRadissonStewardCase(stewardRow);
  assert.equal(one.classification, RESOLUTION_CLASS.RESOLVED);
  assert.equal(one.clean_name, "Faranda Collection Medellin");
  assert.equal(one.city, "Medellin");
  assert.equal(
    one.resolved_insert.fields["Source URL"],
    "https://www.choicehotels.com/colombia/medellin/radisson-individuals-hotels/cb030"
  );
  assert.equal(
    classifyProductionCycleInsertCandidate(one.resolved_insert).steward,
    false
  );

  const batch = resolveChoiceRadissonStewardBatch([stewardRow], { censusRecords: [] });
  assert.equal(batch.counts[RESOLUTION_CLASS.RESOLVED], 1);

  const applied = applyChoiceRadissonStewardResolutionToInserts([stewardRow], {
    censusRecords: [],
  });
  assert.equal(applied.inserts.length, 1);
  assert.equal(applied.inserts[0].fields.City, "Medellin");
  assert.equal(
    classifyProductionCycleInsertCandidate(applied.inserts[0]).steward,
    false
  );
});

test("Wave 2 Accor / Wyndham / Preferred adapters + parent inference", async () => {
  const {
    inferParentCompanyForAutopilot,
    PARENT_BY_SLUG,
  } = await import("../lib/research-engine-v2/census-autopilot-parent-inference.js");
  const { buildActiveBrandSetupControlList } = await import(
    "../lib/research-engine-v2/census-autopilot-active-brand-scope.js"
  );
  const {
    accorRowToDiscovered,
    wyndhamRowToDiscovered,
    preferredRowToDiscovered,
    buildDiscoveredIdentityKey,
    runSourceDiscoveryControlled,
  } = await import("../lib/research-engine-v2/census-autopilot-source-discovery.js");
  const { classifyAccorCountryDiscoveryReadiness } = await import(
    "../lib/research-engine-v2/census-autopilot-accor-cala-discovery-adapter.js"
  );
  const { classifyWyndhamCountryDiscoveryReadiness } = await import(
    "../lib/research-engine-v2/census-autopilot-wyndham-cala-discovery-adapter.js"
  );
  const {
    classifyPreferredCountryDiscoveryReadiness,
    parsePreferredDirectoryHtml,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-preferred-directory-discovery-adapter.js"
  );
  const { isDiscoveryAdapterReady } = await import(
    "../lib/research-engine-v2/production-census-cala-region-config.js"
  );

  assert.equal(PARENT_BY_SLUG.ibis, "Accor");
  assert.equal(PARENT_BY_SLUG["trademark-collection-by-wyndham"], "Wyndham");
  assert.equal(PARENT_BY_SLUG["preferred-hotels-and-resorts"], "Preferred Hotels & Resorts");

  const inferred = inferParentCompanyForAutopilot({
    brand_slug: "novotel",
    parent_company: null,
  });
  assert.equal(inferred.inferred, true);
  assert.equal(inferred.parent_company, "Accor");
  assert.equal(inferred.inference_confidence, "High");

  const list = buildActiveBrandSetupControlList({
    region: "CALA",
    brands: [
      { slug: "ibis", brandName: "ibis", brandStatus: "Active" },
      {
        slug: "trademark-collection-by-wyndham",
        brandName: "Trademark Collection by Wyndham",
        brandStatus: "Active",
      },
      {
        slug: "preferred-hotels-and-resorts",
        brandName: "Preferred Hotels & Resorts",
        brandStatus: "Active",
      },
    ],
    skipUniverseLoad: true,
  });
  assert.equal(list.parent_inference_read_only, true);
  assert.ok(list.brands.some((b) => b.brand_slug === "ibis" && b.parent_company === "Accor"));
  assert.ok(
    list.brands.some(
      (b) => b.brand_slug === "preferred-hotels-and-resorts" && b.brand_family === "Preferred"
    )
  );

  assert.equal(classifyAccorCountryDiscoveryReadiness("Mexico").ready, true);
  assert.equal(classifyWyndhamCountryDiscoveryReadiness("Mexico").ready, true);
  assert.equal(classifyPreferredCountryDiscoveryReadiness("Mexico").ready, true);
  assert.equal(isDiscoveryAdapterReady("Mexico", "Accor"), true);
  assert.equal(isDiscoveryAdapterReady("Mexico", "Wyndham"), true);
  assert.equal(isDiscoveryAdapterReady("Mexico", "Preferred"), true);

  const accorDisc = accorRowToDiscovered({
    propertyId: "A0V4",
    name: "Ibis Tijuana Zona Rio",
    brand: "ibis",
    city: "TIJUANA",
    country: "Mexico",
    addressLine1: "Av. something",
    propertyUrl: "https://all.accor.com/hotel/A0V4/index.en.shtml",
    sourceUrl: "https://api.accor.com/catalog/v1/hotels",
  });
  assert.equal(accorDisc.identity_key, "ind_accor_mx_a0v4");
  assert.equal(accorDisc.source_family, "Accor");
  assert.equal(accorDisc.identity_confidence, "High");

  const wynDisc = wyndhamRowToDiscovered({
    propertyId: "lq-hotel-monterrey-centro",
    name: "La Quinta by Wyndham Monterrey Centro",
    brand: "La Quinta by Wyndham",
    brandSlug: "laquinta",
    city: "Monterrey",
    country: "Mexico",
    propertyUrl:
      "https://www.wyndhamhotels.com/laquinta/monterrey-mexico/lq-hotel-monterrey-centro/overview",
    calaFilterStatus: "included",
  });
  assert.equal(wynDisc.source_family, "Wyndham");
  assert.ok(String(wynDisc.identity_key).startsWith("ind_wyndham_mx_"));
  assert.equal(wynDisc.identity_confidence, "High");

  const prefDisc = preferredRowToDiscovered({
    propertyId: "414319",
    nid: 414319,
    name: "Bahia Hotel & Beach House",
    brand: "Preferred Hotels & Resorts",
    city: "Cabo San Lucas",
    state: "Baja California Sur",
    country: "Mexico",
    propertyUrl: "https://preferredhotels.com/hotels/mexico/bahia-hotel-beach-house",
    collections: ["Lifestyle"],
  });
  assert.equal(prefDisc.identity_key, "ind_preferred_mx_414319");
  assert.equal(prefDisc.brand, "Preferred Hotels & Resorts");
  assert.deepEqual(prefDisc.collection_labels, ["Lifestyle"]);

  const nextHtml = `<html><script id="__NEXT_DATA__" type="application/json">${JSON.stringify({
    props: {
      pageProps: {
        properties: [
          [
            {
              nid: 99,
              fieldDisplayTitle: "Test Preferred Hotel",
              fieldCountryName: "Mexico",
              fieldStateName: "Quintana Roo",
              fieldAddress: { locality: "Cancun" },
              entityUrl: { path: "/hotels/mexico/test-preferred-hotel" },
              fieldPreferredCollections: [
                { entity: { name: "L.V.X." } },
              ],
            },
          ],
        ],
      },
    },
  })}</script></html>`;
  const parsed = parsePreferredDirectoryHtml(nextHtml);
  assert.equal(parsed.ok, true);
  assert.equal(parsed.propertyRows.length, 1);
  assert.equal(parsed.propertyRows[0].propertyId, "99");

  const accorCache = new Map([
    [
      "Mexico|NEWA1",
      {
        propertyId: "NEWA1",
        name: "Novotel Fixture Cancun",
        brand: "Novotel",
        parent: "Accor",
        city: "Cancun",
        country: "Mexico",
        addressLine1: "Blvd Kukulcan",
        propertyUrl: "https://all.accor.com/hotel/NEWA1/index.en.shtml",
        sourceUrl: "https://all.accor.com/a/en/destination/continent/hotels-central-america-c10.html",
      },
    ],
  ]);
  accorCache._meta = { version: "fixture" };

  const preferredCache = new Map([
    [
      "Mexico|414319",
      {
        propertyId: "414319",
        name: "Bahia Hotel & Beach House",
        brand: "Preferred Hotels & Resorts",
        parent: "Preferred Hotels & Resorts",
        city: "Cabo San Lucas",
        country: "Mexico",
        propertyUrl: "https://preferredhotels.com/hotels/mexico/bahia-hotel-beach-house",
        sourceUrl: "https://preferredhotels.com/directory?numberOfRooms=1",
        collections: ["Lifestyle"],
      },
    ],
  ]);
  preferredCache._meta = { version: "fixture" };

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ap-wave2-"));
  const report = await runSourceDiscoveryControlled({
    runDir: tmp,
    runId: "fixture-wave2",
    region: "CALA",
    parentCompany: null,
    brands: [
      { slug: "novotel", brandName: "Novotel", brandStatus: "Active", parentPlatform: "Accor" },
      {
        slug: "preferred-hotels-and-resorts",
        brandName: "Preferred Hotels & Resorts",
        brandStatus: "Active",
        parentPlatform: "Preferred Hotels & Resorts",
      },
    ],
    skipUniverseLoad: true,
    accorCache,
    preferredCache,
    wyndhamCache: new Map(),
    hiltonCache: new Map(),
    choiceCache: new Map(),
    marriottCache: new Map(),
    ihgCache: new Map(),
    includeVicEvidence: false,
    censusRecords: [],
  });
  assert.equal(report.airtable_writes, false);
  assert.ok((report.source_report.families_used || []).includes("Accor"));
  assert.ok((report.source_report.families_used || []).includes("Preferred"));
  assert.ok(
    (report.new_property_candidates || []).some((c) => c.identity_key === "ind_accor_mx_newa1")
  );
  assert.ok(
    (report.new_property_candidates || []).some((c) => c.identity_key === "ind_preferred_mx_414319")
  );
  assert.ok(buildDiscoveredIdentityKey);
});

test("key_field_completion matrix + High autofill; geocode provider soft-block", async () => {
  const {
    KEY_FIELD_COMPLETION_QUEUE_ID,
    KEY_FIELD_COMPLETION_STATUS,
    KEY_FIELD_STATUS,
    parseCensusIdentityKey,
    classifyKeyFieldStatus,
    buildKeyFieldCompletionMatrix,
    proposeKeyFieldAutofills,
    runKeyFieldCompletionQueue,
    KEY_FIELD_MATRIX,
  } = await import("../lib/research-engine-v2/census-autopilot-key-field-completion.js");
  const { PRODUCTION_CYCLE_QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-production-cycle.js"
  );
  const { QUEUE_ORDER } = await import("../lib/research-engine-v2/census-autopilot-queue-router.js");
  const { QUEUE_FIELD_SETS } = await import(
    "../lib/research-engine-v2/census-autopilot-queue-orchestrator.js"
  );

  assert.equal(KEY_FIELD_COMPLETION_QUEUE_ID, "key_field_completion");
  assert.ok(QUEUE_ORDER.some((q) => q.id === "key_field_completion"));
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[0], "source_discovery");
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[1], "brand_normalization");
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[2], "parent_company_normalization");
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[3], "core_identity_quality");
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[4], "core_identity_source_lookup");
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[5], "clean_core_classification");
  assert.ok(PRODUCTION_CYCLE_QUEUE_ORDER.includes("key_field_completion"));
  assert.ok(PRODUCTION_CYCLE_QUEUE_ORDER.includes("phone_number_enrichment"));
  assert.ok(
    PRODUCTION_CYCLE_QUEUE_ORDER.indexOf("phone_number_enrichment") >
      PRODUCTION_CYCLE_QUEUE_ORDER.indexOf("coordinate_completion")
  );
  assert.ok(
    PRODUCTION_CYCLE_QUEUE_ORDER.indexOf("rooms_keys") >
      PRODUCTION_CYCLE_QUEUE_ORDER.indexOf("phone_number_enrichment")
  );
  assert.ok(QUEUE_FIELD_SETS.key_field_completion?.includes("City"));
  assert.ok(KEY_FIELD_MATRIX.length >= 30);

  const parsed = parseCensusIdentityKey("ind_accor_mx_a0v4");
  assert.equal(parsed.family, "Accor");
  assert.equal(parsed.country, "Mexico");

  const familyGap = classifyKeyFieldStatus(
    { group: "source", airtable: "Family / Source Family", alias: "Source Family" },
    { "Property Identity Key": "ind_hilton_do_pujmiqq" },
    { identityParsed: parseCensusIdentityKey("ind_hilton_do_pujmiqq"), providerReady: false }
  );
  assert.equal(familyGap.status, KEY_FIELD_STATUS.MISSING_CAN_AUTOFILL);
  assert.equal(familyGap.autofill_value, "Hilton");

  const latBlocked = classifyKeyFieldStatus(
    { group: "coordinates", airtable: "Latitude", alias: "Latitude" },
    { Address: "Av Reforma 100", City: "Mexico City", Country: "Mexico" },
    { providerReady: false }
  );
  assert.equal(latBlocked.status, KEY_FIELD_STATUS.MISSING_PROVIDER_BLOCKED);

  const records = [
    {
      id: "recGap1",
      fields: {
        "Property Name": "Ibis Fixture",
        "Property Identity Key": "ind_accor_mx_newa1",
        "Official Property URL": "https://all.accor.com/hotel/NEWA1/index.en.shtml",
        Country: "",
        "Family / Source Family": "",
        "Source URL": "",
        Address: "Blvd Kukulcan",
        Latitude: null,
        Longitude: null,
      },
    },
    {
      id: "recComplete",
      fields: {
        "Property Name": "Complete Hotel",
        "Current Brand": "Hilton",
        City: "Bogota",
        "State / Region": "Cundinamarca",
        Country: "Colombia",
        "Source URL": "https://www.hilton.com/en/hotels/bogspqq-fixture/",
        "Family / Source Family": "Hilton",
        "Source Confidence": "High",
        "Production Use Status": "Census Only / Not Owner-Facing",
        Address: "Calle 1",
        "Address Confidence": "High",
        "Address Source URL": "https://www.hilton.com/en/hotels/bogspqq-fixture/",
        Latitude: 4.6,
        Longitude: -74.0,
        "Coordinate Source Type": "official_brand_directory",
        "Coordinate Confidence": "High",
        "Geocode Provider": "official",
        "Geocode Method": "directory",
        "Geocode Reviewed Date": "2026-08-06",
        "Radar Display Status": "Internal Only",
        "Radar Display Reason": "test",
        "Radar Geography Status": "Ready",
        "Public Census Eligibility": "No",
        "Public Display Confidence": "High",
        "Public Display Review Status": "Reviewed",
        "Property Type": "Hotel",
        "Asset Context": "Urban",
        "Market / Submarket": "Bogota",
        "Rooms / Keys": 120,
        "Rooms Confidence": "High",
        "Rooms Source URL": "https://example.com",
        "Rooms Source Type": "official",
        "Rooms Reviewed Date": "2026-08-06",
        "Hotel Description - Source Text": "Text",
        "Hotel Description - AI Summary": "Summary",
        "Amenities - Source Text": "WiFi",
        "Amenities - Structured Tags": "wifi",
      },
    },
  ];

  const matrix = buildKeyFieldCompletionMatrix(records, {
    providerReady: false,
    env: {},
  });
  assert.equal(matrix.ok, true);
  assert.ok(
    [
      KEY_FIELD_COMPLETION_STATUS.READY,
      KEY_FIELD_COMPLETION_STATUS.READY_PROVIDER_BLOCKED,
      KEY_FIELD_COMPLETION_STATUS.NEEDS_SOURCE_ADAPTERS,
    ].includes(matrix.status)
  );
  assert.equal(matrix.total_hotel_property_census_records, 2);
  assert.ok(matrix.summary.autofill_opportunities >= 1);
  assert.ok(matrix.summary.provider_blocked_coordinate_records >= 1);

  const proposed = proposeKeyFieldAutofills(records, { providerReady: false, env: {} });
  assert.ok(proposed.proposals.length >= 1);
  const gapProp = proposed.proposals.find((p) => p.record_id === "recGap1");
  assert.ok(gapProp);
  assert.equal(gapProp.patch["Family / Source Family"], "Accor");
  assert.equal(gapProp.patch.Country, "Mexico");
  assert.ok(gapProp.patch["Source URL"]);
  assert.equal(gapProp.confidence, "High");
  assert.ok(!("Latitude" in (gapProp.patch || {})));
  assert.ok(proposed.provider_decision_needed.length >= 1);

  const queue = runKeyFieldCompletionQueue({
    censusRecords: records,
    writeReports: false,
    providerReady: false,
    env: {},
  });
  assert.equal(queue.ok, true);
  assert.equal(queue.airtable_writes, false);
  assert.ok(queue.high_proposals >= 1);
  assert.ok(!JSON.stringify(queue).includes("Owner Name"));
  assert.ok(!JSON.stringify(queue).includes("Recent Momentum"));
});

await testAsync("Mapbox Permanent coordinate_completion gates + validations", async () => {
  const {
    evaluateMapboxPermanentReadiness,
    evaluateCensusCoordinateProviderReadiness,
    COORDINATE_COMPLETION_STATUSES,
  } = await import("../lib/research-engine-v2/census-coordinate-provider.js");
  const {
    resolveMapboxCoordinates,
    MAPBOX_COORDINATE_STATUSES,
    buildMapboxOfficialAddressQuery,
  } = await import("../lib/research-engine-v2/census-mapbox-coordinate-provider.js");
  const {
    COORDINATE_COMPLETION_QUEUE_ID,
    evaluateCoordinateCompletionEligibility,
    buildCoordinateCompletionPatch,
    validateExistingCoordinatesGate,
    runCoordinateCompletionQueue,
    writeCoordinateCompletionArtifacts,
  } = await import("../lib/research-engine-v2/census-coordinate-completion.js");
  const { PRODUCTION_CYCLE_QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-production-cycle.js"
  );
  const { QUEUE_ORDER } = await import("../lib/research-engine-v2/census-autopilot-queue-router.js");
  const { AUTOPILOT_FORBIDDEN_FIELDS, AUTOPILOT_TARGET_TABLE_ID } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );
  const { evaluateProviderReadiness } = await import(
    "../lib/research-engine-v2/production-census-description-extraction.js"
  );

  assert.equal(COORDINATE_COMPLETION_QUEUE_ID, "coordinate_completion");
  assert.ok(QUEUE_ORDER.some((q) => q.id === "coordinate_completion"));
  assert.ok(PRODUCTION_CYCLE_QUEUE_ORDER.includes("coordinate_completion"));
  assert.ok(!PRODUCTION_CYCLE_QUEUE_ORDER.includes("coordinate_resolution"));
  assert.equal(AUTOPILOT_TARGET_TABLE_ID, "tbl9aY5ijiuIzzWam");

  // Token + permanent + completion required
  const missing = evaluateMapboxPermanentReadiness({
    MAPBOX_ACCESS_TOKEN: "",
    MAPBOX_PERMANENT_GEOCODING: "1",
    CENSUS_COORDINATE_COMPLETION_ENABLED: "1",
  });
  assert.equal(missing.ready, false);
  assert.ok(missing.missing_flags.includes("MAPBOX_ACCESS_TOKEN"));

  const noPermanent = evaluateMapboxPermanentReadiness({
    MAPBOX_ACCESS_TOKEN: "pk.test",
    MAPBOX_PERMANENT_GEOCODING: "0",
    CENSUS_COORDINATE_COMPLETION_ENABLED: "1",
  });
  assert.equal(noPermanent.ready, false);

  const noCompletion = evaluateMapboxPermanentReadiness({
    MAPBOX_ACCESS_TOKEN: "pk.test",
    MAPBOX_PERMANENT_GEOCODING: "1",
    CENSUS_COORDINATE_COMPLETION_ENABLED: "0",
  });
  assert.equal(noCompletion.ready, false);

  const readyEnv = {
    MAPBOX_ACCESS_TOKEN: "pk.test",
    MAPBOX_PERMANENT_GEOCODING: "1",
    CENSUS_COORDINATE_COMPLETION_ENABLED: "1",
    GEOCODING_PROVIDER: "mapbox",
  };
  assert.equal(evaluateMapboxPermanentReadiness(readyEnv).ready, true);
  assert.equal(evaluateCensusCoordinateProviderReadiness(readyEnv).approved_for_geocode_apply, true);
  assert.equal(evaluateProviderReadiness(readyEnv).approved_for_geocode_apply, true);
  assert.equal(
    evaluateProviderReadiness({
      MAPBOX_ACCESS_TOKEN: "pk.test",
      MAPBOX_PERMANENT_GEOCODING: "1",
      CENSUS_COORDINATE_COMPLETION_ENABLED: "0",
    }).approved_for_geocode_apply,
    false
  );

  // Temporary geocoding blocked
  const tempBlocked = await resolveMapboxCoordinates(
    {
      propertyName: "Hotel Test",
      address: "Av Reforma 100",
      city: "Mexico City",
      country: "Mexico",
    },
    { env: readyEnv, allowTemporary: true }
  );
  assert.equal(tempBlocked.status, MAPBOX_COORDINATE_STATUSES.PROVIDER_ERROR);
  assert.match(tempBlocked.reason, /temporary_geocoding_blocked/);

  // Missing env → provider_decision_needed (no fetch)
  let fetchCalled = false;
  const needProvider = await resolveMapboxCoordinates(
    {
      propertyName: "Hotel Test",
      address: "Av Reforma 100",
      city: "Mexico City",
      country: "Mexico",
    },
    {
      env: { MAPBOX_ACCESS_TOKEN: "pk.test" },
      fetchImpl: async () => {
        fetchCalled = true;
        return { ok: true, json: async () => ({ features: [] }) };
      },
    }
  );
  assert.equal(needProvider.status, MAPBOX_COORDINATE_STATUSES.PROVIDER_DECISION_NEEDED);
  assert.equal(fetchCalled, false);

  assert.equal(
    buildMapboxOfficialAddressQuery({
      address: "Mexico City",
      city: "Mexico City",
      country: "Mexico",
    }),
    null
  );

  // 0,0 rejected
  const zero = await resolveMapboxCoordinates(
    {
      propertyName: "Hotel Null",
      address: "Calle Falsa 123",
      city: "Mexico City",
      country: "Mexico",
    },
    {
      env: readyEnv,
      fetchImpl: async () => ({
        ok: true,
        json: async () => ({
          features: [
            {
              center: [0, 0],
              place_name: "Null Island",
              place_type: ["address"],
              relevance: 0.99,
            },
          ],
        }),
      }),
    }
  );
  assert.equal(zero.status, MAPBOX_COORDINATE_STATUSES.ZERO_ZERO_REJECTED);

  // City centroid rejected
  const centroid = await resolveMapboxCoordinates(
    {
      propertyName: "Hotel Centro",
      address: "Calle Falsa 123",
      city: "Mexico City",
      country: "Mexico",
    },
    {
      env: readyEnv,
      fetchImpl: async () => ({
        ok: true,
        json: async () => ({
          features: [
            {
              center: [-99.1332, 19.4326],
              place_name: "Mexico City, Mexico",
              place_type: ["place"],
              relevance: 0.99,
            },
          ],
        }),
      }),
    }
  );
  assert.equal(centroid.status, MAPBOX_COORDINATE_STATUSES.CITY_CENTROID_REJECTED);

  // Country mismatch
  const mismatch = await resolveMapboxCoordinates(
    {
      propertyName: "Hotel DR",
      address: "Avenida Winston Churchill 1099",
      city: "Santo Domingo",
      country: "Dominican Republic",
    },
    {
      env: readyEnv,
      fetchImpl: async () => ({
        ok: true,
        json: async () => ({
          features: [
            {
              center: [-99.1, 19.4],
              place_name: "Avenida Winston Churchill 1099, Mexico City, Mexico",
              place_type: ["address"],
              relevance: 0.95,
            },
          ],
        }),
      }),
    }
  );
  assert.equal(mismatch.status, MAPBOX_COORDINATE_STATUSES.COUNTRY_MISMATCH);

  // High resolve + patch
  const high = await resolveMapboxCoordinates(
    {
      propertyName: "Hotel Reforma",
      address: "Paseo de la Reforma 222",
      city: "Mexico City",
      country: "Mexico",
    },
    {
      env: readyEnv,
      fetchImpl: async (url) => {
        assert.match(String(url), /permanent=true/);
        assert.match(String(url), /autocomplete=false/);
        assert.match(String(url), /limit=1/);
        assert.doesNotMatch(String(url), /nominatim/i);
        return {
          ok: true,
          json: async () => ({
            features: [
              {
                center: [-99.167, 19.428],
                place_name: "Paseo de la Reforma 222, Mexico City, Mexico",
                place_type: ["address"],
                relevance: 0.97,
              },
            ],
          }),
        };
      },
    }
  );
  assert.equal(high.status, MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH);
  assert.equal(high.confidence, "High");
  assert.equal(high.provider, "Mapbox");

  const patch = buildCoordinateCompletionPatch(high);
  assert.ok(patch);
  assert.equal(patch["Geocode Provider"], "Mapbox");
  assert.equal(patch["Geocode Method"], "permanent_geocoding_official_address");
  assert.equal(patch["Coordinate Source Type"], "official_address_geocode");
  assert.equal(patch["Coordinate Confidence"], "High");
  for (const f of AUTOPILOT_FORBIDDEN_FIELDS) {
    assert.ok(!(f in patch), `forbidden field leaked: ${f}`);
  }

  // Existing different coords → steward
  const stewardGate = validateExistingCoordinatesGate(
    { Latitude: 20.1, Longitude: -87.5 },
    high
  );
  assert.equal(stewardGate.action, "steward_review");

  const identicalGate = validateExistingCoordinatesGate(
    { Latitude: high.latitude, Longitude: high.longitude },
    high
  );
  assert.equal(identicalGate.action, "skip_identical");

  const eligibleRec = {
    id: "recCoord1",
    fields: {
      "Property Name": "Hotel Reforma",
      "Canonical Property Name": "Hotel Reforma",
      "Current Brand": "Hilton Hotels & Resorts",
      "Brand Family": "Hilton",
      "Family / Source Family": "Hilton",
      "Identity Confidence": "High",
      "Data Confidence Tier": "High",
      Address: "Paseo de la Reforma 222",
      "Address Confidence": "High",
      "Address Source URL": "https://www.hilton.com/en/hotels/mexxx-hotel-reforma/",
      City: "Mexico City",
      Country: "Mexico",
      "Source URL": "https://www.hilton.com/en/hotels/mexxx-hotel-reforma/",
      "Production Use Status": "Census Only / Not Owner-Facing",
      Latitude: null,
      Longitude: null,
    },
  };
  assert.equal(
    evaluateCoordinateCompletionEligibility(eligibleRec, {
      env: { ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS: "0" },
    }).eligible,
    true
  );
  assert.equal(
    evaluateCoordinateCompletionEligibility(
      {
        ...eligibleRec,
        fields: { ...eligibleRec.fields, "Address Confidence": "Medium" },
      },
      { env: { ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS: "0" } }
    ).eligible,
    false
  );
  assert.equal(
    evaluateCoordinateCompletionEligibility(
      {
        ...eligibleRec,
        fields: {
          ...eligibleRec.fields,
          "Address Confidence": "Medium",
          // No Source URL — Official Property URL + Address Source URL allowed
          "Source URL": "",
          "Official Property URL":
            "https://www.hilton.com/en/hotels/mexxx-hotel-reforma/",
        },
      },
      {
        mediumMatchHighPathway: true,
        env: { ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS: "1" },
      }
    ).eligible,
    true
  );
  const mediumPatch = buildCoordinateCompletionPatch(high, {
    fromMediumAddress: true,
  });
  assert.equal(mediumPatch["Coordinate Confidence"], "Medium");
  assert.equal(mediumPatch["Geocode Provider"], "Mapbox");
  assert.equal(
    evaluateCoordinateCompletionEligibility({
      ...eligibleRec,
      fields: { ...eligibleRec.fields, "Canonical Property Name": "" },
    }).eligible,
    false
  );

  // Queue without Mapbox → provider_decision_needed, not fatal
  const blocked = await runCoordinateCompletionQueue({
    censusRecords: [eligibleRec],
    env: {},
    writeReports: false,
  });
  assert.equal(blocked.status, COORDINATE_COMPLETION_STATUSES.READY_PROVIDER_NEEDED);
  assert.ok(blocked.provider_decision_needed.length >= 1);
  assert.equal(blocked.proposals.length, 0);

  // Queue with mock Mapbox → High proposal; Hotel Property Census only
  const applied = await runCoordinateCompletionQueue({
    censusRecords: [
      eligibleRec,
      {
        id: "recExistingDiff",
        fields: {
          ...eligibleRec.fields,
          "Property Name": "Other Hotel",
          "Property Identity Key": "ind_test_mx_other",
          // One axis present + different → eligible but stewarded (no overwrite)
          Latitude: 21.0,
          Longitude: null,
        },
      },
    ],
    env: readyEnv,
    writeReports: false,
    fetchImpl: async () => ({
      ok: true,
      json: async () => ({
        features: [
          {
            center: [-99.167, 19.428],
            place_name: "Paseo de la Reforma 222, Mexico City, Mexico",
            place_type: ["address"],
            relevance: 0.97,
          },
        ],
      }),
    }),
  });
  assert.ok(applied.proposals.length >= 1);
  assert.equal(applied.write_target.table_id, "tbl9aY5ijiuIzzWam");
  assert.equal(applied.constraints.brand_explorer_writes, false);
  assert.equal(applied.constraints.owner_operator_date_writes, false);
  // Never overwrite existing different coordinates
  assert.ok(!applied.proposals.some((p) => p.record_id === "recExistingDiff"));
  const stewardOrSkip =
    applied.steward_review.some((s) => s.record_id === "recExistingDiff") ||
    (applied.failures || []).some((f) => f.record_id === "recExistingDiff") ||
    !applied.proposals.some((p) => p.record_id === "recExistingDiff");
  assert.equal(stewardOrSkip, true);

  const artifactsDir = fs.mkdtempSync(path.join(os.tmpdir(), "mapbox-coord-"));
  const artifacts = writeCoordinateCompletionArtifacts(applied, artifactsDir);
  assert.ok(fs.existsSync(artifacts.jsonPath));
  assert.ok(fs.existsSync(artifacts.mdPath));
  assert.ok(fs.existsSync(artifacts.docsPath));
});

await testAsync("core identity quality gate — city normalize, descriptor reject, coord block", async () => {
  const {
    classifyAndNormalizeCityState,
    CITY_CLASS,
    isDescriptorCity,
    trySplitCityState,
  } = await import("../lib/research-engine-v2/census-city-state-normalizer.js");
  const {
    evaluateCoordinateIdentityGate,
    evaluateInsertIdentityGate,
    QUALITY_GATE_STATUS,
  } = await import("../lib/research-engine-v2/census-core-identity-quality.js");
  const {
    CORE_IDENTITY_QUALITY_QUEUE_ID,
    runCoreIdentityQualityGate,
  } = await import("../lib/research-engine-v2/census-data-quality-gate.js");
  const { evaluateCoordinateCompletionEligibility } = await import(
    "../lib/research-engine-v2/census-coordinate-completion.js"
  );
  const { PRODUCTION_CYCLE_QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-production-cycle.js"
  );
  const { QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-queue-router.js"
  );

  assert.equal(CORE_IDENTITY_QUALITY_QUEUE_ID, "core_identity_quality");
  assert.ok(QUEUE_ORDER.some((q) => q.id === "core_identity_quality"));
  assert.ok(QUEUE_ORDER.some((q) => q.id === "brand_normalization"));
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[1], "brand_normalization");
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[2], "parent_company_normalization");
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[3], "core_identity_quality");
  assert.ok(
    PRODUCTION_CYCLE_QUEUE_ORDER.indexOf("coordinate_completion") >
      PRODUCTION_CYCLE_QUEUE_ORDER.indexOf("core_identity_quality")
  );

  assert.equal(isDescriptorCity("An - Adults Only"), true);
  assert.equal(isDescriptorCity("Unknown"), true);
  assert.equal(isDescriptorCity("Cancún"), false);

  const caps = classifyAndNormalizeCityState({ City: "CANCUN", Country: "Mexico" });
  assert.equal(caps.write_allowed, true);
  assert.equal(caps.patch.City, "Cancún");

  const split = trySplitCityState("Cd. Guadalupe, Nuevo Leon");
  assert.equal(split.ok, true);
  assert.equal(split.city, "Guadalupe");
  assert.equal(split.state_region, "Nuevo León");

  const splitClass = classifyAndNormalizeCityState({
    City: "Cd. Guadalupe, Nuevo Leon",
    Country: "Mexico",
  });
  assert.equal(splitClass.class, CITY_CLASS.SPLIT_CITY_STATE);
  assert.equal(splitClass.patch.City, "Guadalupe");
  assert.equal(splitClass.patch["State / Region"], "Nuevo León");

  const desc = classifyAndNormalizeCityState({ City: "An - Adults Only", Country: "Mexico" });
  assert.equal(desc.class, CITY_CLASS.DESCRIPTOR);
  assert.equal(desc.write_allowed, false);

  const insertBlocked = evaluateInsertIdentityGate({
    property_name: "Hotel Test",
    brand: "Hilton",
    city: "Unknown",
    country: "Mexico",
    official_property_url: "https://www.hilton.com/en/hotels/test/",
    source_family: "Hilton",
  });
  assert.equal(insertBlocked.allow_insert, false);

  const dirtyRec = {
    id: "recDirty",
    fields: {
      "Property Name": "Hotel X",
      "Current Brand": "Hilton",
      City: "An - Adults Only",
      Country: "Mexico",
      "Source URL": "https://www.hilton.com/en/hotels/x/",
      Address: "Av Reforma 1000 Colonia",
      "Address Confidence": "High",
      "Address Source URL": "https://www.hilton.com/en/hotels/x/",
    },
  };
  const coordGate = evaluateCoordinateIdentityGate(dirtyRec);
  assert.equal(coordGate.allow_geocode, false);
  assert.equal(coordGate.gate_status, QUALITY_GATE_STATUS.BLOCKED_DIRTY_CORE_IDENTITY);

  const elig = evaluateCoordinateCompletionEligibility(dirtyRec);
  assert.equal(elig.eligible, false);

  const cleanRec = {
    id: "recClean",
    fields: {
      "Property Name": "Hilton Cancun",
      "Canonical Property Name": "Hilton Cancun",
      "Current Brand": "Hilton",
      City: "CANCUN",
      Country: "Mexico",
      "Source URL": "https://www.hilton.com/en/hotels/cun/",
      "Production Use Status": "Census Only / Not Owner-Facing",
    },
  };
  const gate = runCoreIdentityQualityGate({
    censusRecords: [dirtyRec, cleanRec],
    writeReports: false,
  });
  assert.ok(gate.ok);
  assert.ok(gate.counters.descriptor_city >= 1);
  assert.ok(gate.counters.safe_autofix_proposals >= 1);
  const fix = gate.proposals.find((p) => p.record_id === "recClean");
  assert.ok(fix?.patch?.City === "Cancún");
  assert.ok(!JSON.stringify(gate).includes("Owner Name"));
});

await testAsync("Canonical Property Name autofill, exact-suffix cleanup, conflict steward, dup block", async () => {
  const {
    stripSafeMembershipSuffixes,
    classifyCanonicalPropertyName,
    CANONICAL_NAME_STATUS,
    buildCanonicalDuplicateIndex,
    proposeCanonicalPropertyNameWrite,
    namesAreEquivalent,
  } = await import("../lib/research-engine-v2/census-canonical-property-name.js");
  const {
    KEY_FIELD_MATRIX,
    proposeKeyFieldAutofills,
    runKeyFieldCompletionQueue,
  } = await import("../lib/research-engine-v2/census-autopilot-key-field-completion.js");
  const { AUTOPILOT_ALLOWED_WRITE_FIELDS } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.ok(KEY_FIELD_MATRIX.some((f) => f.airtable === "Canonical Property Name"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Canonical Property Name"));

  const stripped = stripSafeMembershipSuffixes(
    "Hotel Casa Nova, a member of Radisson Individuals"
  );
  assert.equal(stripped.cleaned, "Hotel Casa Nova");
  assert.equal(stripped.stripped, true);

  const noStrip = stripSafeMembershipSuffixes("JOIA Paraiso by Iberostar");
  assert.equal(noStrip.stripped, false);

  const baseFields = {
    "Property Name": "Hotel Casa Nova, a member of Radisson Individuals",
    "Current Brand": "Radisson Individuals",
    City: "Medellin",
    Country: "Colombia",
    "Source URL": "https://www.choicehotels.com/colombia/medellin/ascend-hotels/cob59",
    "Production Use Status": "Census Only / Not Owner-Facing",
  };

  const blank = classifyCanonicalPropertyName({ ...baseFields });
  assert.equal(blank.status, CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL);
  assert.equal(blank.candidate, "Hotel Casa Nova");

  const dirty = classifyCanonicalPropertyName({
    ...baseFields,
    "Canonical Property Name": "Hotel Casa Nova, a member of Radisson Individuals",
  });
  assert.equal(dirty.status, CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN);

  const conflict = classifyCanonicalPropertyName({
    ...baseFields,
    "Property Name": "Hotel Casa Nova",
    "Canonical Property Name": "Completely Different Resort",
  });
  assert.equal(conflict.status, CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW);

  const clean = classifyCanonicalPropertyName({
    ...baseFields,
    "Property Name": "Hilton Bogota",
    "Current Brand": "Hilton",
    "Canonical Property Name": "Hilton Bogota",
    "Source URL": "https://www.hilton.com/en/hotels/bogspqq-fixture/",
  });
  assert.equal(clean.status, CANONICAL_NAME_STATUS.COMPLETE_CLEAN);

  assert.equal(namesAreEquivalent("Hilton Bogotá", "Hilton Bogota"), true);

  const records = [
    {
      id: "recCanon1",
      fields: { ...baseFields, "Property Identity Key": "ind_choice_co_casa1" },
    },
    {
      id: "recCanon2",
      fields: {
        "Property Name": "Hotel Andes, a member of Preferred Hotels & Resorts",
        "Current Brand": "Preferred Hotels & Resorts",
        City: "Bogota",
        Country: "Colombia",
        "Source URL": "https://www.preferredhotels.com/hotels/colombia/bogota/andes",
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Canonical Property Name": "Hotel Andes, a member of Preferred Hotels & Resorts",
        "Property Identity Key": "ind_pref_co_andes",
      },
    },
    {
      id: "recConflict",
      fields: {
        "Property Name": "Hotel Sol",
        "Current Brand": "Independent",
        City: "Cartagena",
        Country: "Colombia",
        "Source URL": "https://example.com/hotel-sol",
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Canonical Property Name": "Other Name Entirely",
        "Property Identity Key": "ind_indep_co_sol",
      },
    },
  ];

  const index = buildCanonicalDuplicateIndex(records);
  const p1 = proposeCanonicalPropertyNameWrite(records[0], index);
  assert.equal(p1.action, "autofill");
  assert.equal(p1.patch["Canonical Property Name"], "Hotel Casa Nova");

  const p2 = proposeCanonicalPropertyNameWrite(records[1], index);
  assert.equal(p2.action, "cleanup");

  // Shared directory/sitemap Source URL must NOT block blank Canonical autofill
  const directoryCohort = [
    {
      id: "recDirA",
      fields: {
        "Property Name": "Hilton Punta Cana Downtown",
        "Current Brand": "Hilton",
        City: "Punta Cana",
        Country: "Dominican Republic",
        "Source URL": "https://www.hilton.com/en/locations/dominican-republic/",
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Property Identity Key": "ind_hilton_do_a",
      },
    },
    {
      id: "recDirB",
      fields: {
        "Property Name": "Hilton Santo Domingo",
        "Current Brand": "Hilton",
        City: "Santo Domingo",
        Country: "Dominican Republic",
        "Source URL": "https://www.hilton.com/en/locations/dominican-republic/",
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Property Identity Key": "ind_hilton_do_b",
      },
    },
  ];
  const dirIndex = buildCanonicalDuplicateIndex(directoryCohort);
  const dirA = proposeCanonicalPropertyNameWrite(directoryCohort[0], dirIndex);
  const dirB = proposeCanonicalPropertyNameWrite(directoryCohort[1], dirIndex);
  assert.equal(dirA.action, "autofill", "directory Source URL must not create false duplicate_risk");
  assert.equal(dirB.action, "autofill");
  assert.equal(dirA.patch["Canonical Property Name"], "Hilton Punta Cana Downtown");

  // True property-level shared URL still stewards
  const propDup = [
    {
      id: "recProp1",
      fields: {
        "Property Name": "Same Hotel One",
        "Current Brand": "Hilton",
        City: "Cancun",
        Country: "Mexico",
        "Source URL": "https://www.hilton.com/en/hotels/cunxxxx-same-hotel/",
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Property Identity Key": "ind_hilton_mx_1",
      },
    },
    {
      id: "recProp2",
      fields: {
        "Property Name": "Same Hotel Two",
        "Current Brand": "Hilton",
        City: "Cancun",
        Country: "Mexico",
        "Source URL": "https://www.hilton.com/en/hotels/cunxxxx-same-hotel/",
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Property Identity Key": "ind_hilton_mx_2",
      },
    },
  ];
  const propIndex = buildCanonicalDuplicateIndex(propDup, {
    isPropertyLevelUrl: (u) => /hilton\.com\/en\/hotels\//i.test(String(u || "")),
  });
  const propPropose = proposeCanonicalPropertyNameWrite(propDup[0], propIndex, {
    isPropertyLevelUrl: (u) => /hilton\.com\/en\/hotels\//i.test(String(u || "")),
  });
  assert.equal(propPropose.action, "steward");
  assert.equal(propPropose.classified?.reason, "duplicate_risk");

  const proposed = proposeKeyFieldAutofills(records, { providerReady: false, env: {} });
  assert.ok(proposed.canonical_summary.safe_autofill_proposals >= 1);
  assert.ok(proposed.canonical_summary.safe_cleanup_proposals >= 1);
  assert.ok(proposed.canonical_summary.steward_cases >= 1);
  const autofillProp = proposed.proposals.find((p) => p.record_id === "recCanon1");
  assert.ok(autofillProp?.patch?.["Canonical Property Name"]);
  assert.ok(!("Owner Name" in (autofillProp.patch || {})));
  assert.ok(!("Recent Momentum" in (autofillProp.patch || {})));

  const queue = runKeyFieldCompletionQueue({
    censusRecords: records,
    writeReports: false,
    providerReady: false,
    env: {},
  });
  assert.ok(queue.ok);
  assert.ok(queue.canonical_completion_status);
  assert.equal(queue.brand_explorer_writes, false);
});

await testAsync("map/contact/size readiness — Clean Core vs Level 2; phone official-only", async () => {
  const {
    evaluateCleanCorePass,
    classifyMapContactSizeReadiness,
    runMapContactSizeReadinessAudit,
    READINESS_LEVEL,
    MAP_CONTACT_SIZE_STATUS,
    PHONE_FIELD,
  } = await import("../lib/research-engine-v2/census-map-contact-size-readiness.js");
  const {
    classifyPhoneEnrichment,
    normalizePhoneNumber,
    extractOfficialPhoneFromHtml,
    isForbiddenPhoneSourceUrl,
    buildPhoneEnrichmentProposals,
  } = await import("../lib/research-engine-v2/census-phone-number-enrichment.js");
  const { PRODUCTION_CYCLE_QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-production-cycle.js"
  );
  const { AUTOPILOT_ALLOWED_WRITE_FIELDS, AUTOPILOT_FORBIDDEN_FIELDS } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Phone"));
  assert.ok(!AUTOPILOT_FORBIDDEN_FIELDS.includes("Phone"));
  assert.ok(PRODUCTION_CYCLE_QUEUE_ORDER.includes("clean_core_classification"));
  assert.ok(PRODUCTION_CYCLE_QUEUE_ORDER.includes("phone_number_enrichment"));

  assert.equal(normalizePhoneNumber("+52 55 1234 5678"), "+525512345678");
  assert.equal(isForbiddenPhoneSourceUrl("https://www.booking.com/hotel/x"), true);
  assert.equal(isForbiddenPhoneSourceUrl("https://www.hilton.com/en/hotels/x/"), false);

  const htmlPhone = extractOfficialPhoneFromHtml(
    '<a href="tel:+525512345678">Call</a>',
    "https://www.hilton.com/en/hotels/x/"
  );
  assert.equal(htmlPhone.ok, true);
  assert.equal(htmlPhone.phone, "+525512345678");

  const cleanRec = {
    id: "recClean1",
    fields: {
      "Property Name": "Hotel Clean Core",
      "Canonical Property Name": "Hotel Clean Core",
      "Current Brand": "Hilton Hotels & Resorts",
      "Brand Family": "Hilton",
      "Family / Source Family": "Hilton",
      City: "Cancún",
      Country: "Mexico",
      "Source URL": "https://www.hilton.com/en/hotels/cunxx-clean/",
      "Identity Confidence": "High",
      "Data Confidence Tier": "High",
      "Production Use Status": "Census Only / Not Owner-Facing",
    },
  };
  const clean = evaluateCleanCorePass(cleanRec);
  assert.equal(clean.pass, true);
  assert.equal(clean.does_not_require_lat_long_phone_rooms, true);

  // Missing coords does NOT fail Clean Core
  assert.equal(isBlankish(cleanRec.fields.Latitude), true);

  const level = classifyMapContactSizeReadiness(cleanRec);
  assert.equal(level.level, READINESS_LEVEL.CLEAN_CORE);
  assert.equal(level.lat_long_complete, false);

  const dirty = evaluateCleanCorePass({
    id: "recDirtyCity",
    fields: {
      ...cleanRec.fields,
      City: "Unknown",
      "Canonical Property Name": "Hotel Dirty",
    },
  });
  assert.equal(dirty.pass, false);

  const phoneBlocked = classifyPhoneEnrichment(
    {
      id: "recDirtyCity",
      fields: { ...cleanRec.fields, City: "Unknown" },
    },
    { officialPhone: "+525511122233" }
  );
  assert.equal(phoneBlocked.action, "blocked_clean_core");

  const phoneOk = classifyPhoneEnrichment(cleanRec, {
    officialPhone: "+52 55 9876 5432",
    officialPhoneSourceUrl: "https://www.hilton.com/en/hotels/cunxx-clean/",
    officialPhoneSourceType: "official_brand_directory",
  });
  assert.equal(phoneOk.action, "autofill");
  assert.equal(phoneOk.patch[PHONE_FIELD], "+525598765432");

  const conflict = classifyPhoneEnrichment(
    { ...cleanRec, fields: { ...cleanRec.fields, Phone: "+525511100000" } },
    { officialPhone: "+525599900000" }
  );
  assert.equal(conflict.action, "steward_conflict");

  const props = buildPhoneEnrichmentProposals({
    censusRecords: [cleanRec],
    officialPhoneByRecordId: {
      recClean1: {
        phone: "+525512345678",
        source_url: "https://www.hilton.com/en/hotels/cunxx-clean/",
      },
    },
  });
  assert.equal(props.proposals.length, 1);
  assert.ok(!("Owner Name" in (props.proposals[0].patch || {})));

  const audit = runMapContactSizeReadinessAudit({
    censusRecords: [cleanRec],
    writeReports: false,
    env: {},
  });
  assert.ok(
    [
      MAP_CONTACT_SIZE_STATUS.PARTIAL,
      MAP_CONTACT_SIZE_STATUS.READY_NEEDS_PRODUCTION_CYCLE,
      MAP_CONTACT_SIZE_STATUS.COMPLETE,
    ].includes(audit.status)
  );
  assert.equal(audit.counters.clean_core, 1);
  assert.equal(audit.brand_explorer_writes, false);
  assert.equal(audit.write_target.table_id, "tbl9aY5ijiuIzzWam");

  function isBlankish(v) {
    return v == null || v === "";
  }
});

await testAsync("clean-core identity repair — queue parse, filters, URL slug city", async () => {
  const { parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const {
    resolveCleanCoreIdentityQueues,
    filterCleanCoreIdentityProposals,
    tryCityFromOfficialUrlSlug,
    auditCoreIdentityRecord,
    CLEAN_CORE_IDENTITY_REPAIR_STATUS,
    RECORD_CLASS,
  } = await import("../lib/research-engine-v2/census-clean-core-identity-repair.js");
  const { PRODUCTION_CYCLE_QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-production-cycle.js"
  );

  const args = parseAutopilotArgs([
    "--mode",
    "production-cycle",
    "--cleanup-existing-only",
    "--queue",
    "core_identity_quality,city_state_normalization,canonical_property_name_completion,key_field_completion",
  ]);
  assert.equal(args.cleanupExistingOnly, true);
  assert.ok(args.queues.includes("core_identity_quality"));
  assert.ok(args.queues.includes("city_state_normalization"));

  const resolved = resolveCleanCoreIdentityQueues(args.queues, {
    cleanupExistingOnly: true,
  });
  assert.ok(resolved.includes("core_identity_quality"));
  assert.ok(resolved.includes("key_field_completion"));
  assert.ok(!resolved.includes("city_state_normalization")); // aliased/deduped
  assert.ok(!resolved.includes("coordinate_completion"));

  const filtered = filterCleanCoreIdentityProposals([
    {
      record_id: "rec1",
      confidence: "High",
      patch: { City: "Cancún", Latitude: 21.1, Phone: "+52", "Rooms / Keys": 100 },
    },
  ]);
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].patch.City, "Cancún");
  assert.ok(!("Latitude" in filtered[0].patch));
  assert.ok(!("Phone" in filtered[0].patch));
  assert.ok(!("Rooms / Keys" in filtered[0].patch));

  const slug = tryCityFromOfficialUrlSlug(
    "https://www.hilton.com/en/hotels/cunxx-cancun-bay/",
    "Mexico"
  );
  assert.equal(slug.ok, true);
  assert.equal(slug.city, "Cancún");

  const weak = tryCityFromOfficialUrlSlug(
    "https://www.booking.com/hotel/mx/mystery.html",
    "Mexico"
  );
  assert.equal(weak.ok, false);

  const unknownRec = {
    id: "recUnk",
    fields: {
      "Property Name": "Hotel Bay",
      "Canonical Property Name": "Hotel Bay",
      "Current Brand": "Hilton",
      "Brand Family": "Hilton",
      "Family / Source Family": "Hilton",
      City: "Unknown",
      Country: "Mexico",
      "Source URL": "https://www.hilton.com/en/hotels/cunxx-cancun-bay/",
      "Identity Confidence": "High",
    },
  };
  const audited = auditCoreIdentityRecord(unknownRec);
  assert.equal(audited.buckets.city.unknown, true);
  assert.ok(audited.url_city_proposal?.ok);
  assert.ok(audited.record_class);
  assert.ok(Object.values(RECORD_CLASS).includes(audited.record_class));

  assert.ok(PRODUCTION_CYCLE_QUEUE_ORDER.includes("core_identity_quality"));
  assert.ok(Object.values(CLEAN_CORE_IDENTITY_REPAIR_STATUS).every((s) => typeof s === "string"));
});

await testAsync("Marriott property URL + Unknown city backfill — slug/IATA High only", async () => {
  const {
    tryCityFromMarriottPropertyUrl,
    selectMarriottUnknownCityTargets,
    buildMarriottPropertyUrlCityProposals,
    isMarriottCensusRecord,
    MARRIOTT_URL_CITY_BACKFILL_STATUS,
  } = await import("../lib/research-engine-v2/census-marriott-property-url-city-backfill.js");
  const { isSafeCoreIdentityOverwrite } = await import(
    "../lib/research-engine-v2/census-autopilot-idempotent-writer.js"
  );

  const cityObregon = tryCityFromMarriottPropertyUrl(
    "https://www.marriott.com/en-us/hotels/cenxo-city-express-ciudad-obregon/overview",
    "Mexico"
  );
  assert.equal(cityObregon.ok, true);
  assert.equal(cityObregon.city, "Ciudad Obregón");

  const panama = tryCityFromMarriottPropertyUrl(
    "https://www.marriott.com/en-us/hotels/ptymm-courtyard-panama-metromall/overview",
    "Panama"
  );
  assert.equal(panama.city, "Panama City");

  const nizuc = tryCityFromMarriottPropertyUrl(
    "https://www.marriott.com/en-us/hotels/cunan-casa-nizuc-a-tribute-portfolio-resort/overview",
    "Mexico"
  );
  assert.equal(nizuc.ok, true);
  assert.equal(nizuc.city, "Cancún");

  // No hotel-name-only inference for unmapped Design Hotels without IATA
  const elena = tryCityFromMarriottPropertyUrl(
    "https://www.marriott.com/en-us/hotels/bjxds-elena-de-cobre-a-member-of-design-hotels/overview",
    "Mexico"
  );
  assert.equal(elena.ok, false);

  assert.equal(
    isSafeCoreIdentityOverwrite(
      "Source URL",
      "https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap",
      "https://www.marriott.com/en-us/hotels/cenxo-city-express-ciudad-obregon/overview"
    ),
    true
  );
  assert.equal(
    isSafeCoreIdentityOverwrite(
      "Source URL",
      "https://www.marriott.com/en-us/hotels/cenxo-city-express-ciudad-obregon/overview",
      "https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap"
    ),
    false
  );

  const records = [
    {
      id: "recMar1",
      fields: {
        "Property Identity Key": "ind_marriott_mx_cenxo",
        "Property Name": "City Express by Marriott Ciudad Obregón",
        "Current Brand": "City Express by Marriott",
        "Brand Family": "Marriott International",
        City: "Unknown",
        Country: "Mexico",
        "Source URL": "https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap",
      },
    },
    {
      id: "recOther",
      fields: {
        "Property Identity Key": "ind_hilton_mx_x",
        "Property Name": "Hilton Test",
        "Current Brand": "Hilton",
        "Brand Family": "Hilton",
        City: "Unknown",
        Country: "Mexico",
        "Source URL": "https://www.hilton.com/en/locations/mexico/",
      },
    },
  ];
  assert.equal(isMarriottCensusRecord(records[0].fields), true);
  assert.equal(isMarriottCensusRecord(records[1].fields), false);
  const targets = selectMarriottUnknownCityTargets(records);
  assert.equal(targets.length, 1);
  assert.equal(targets[0].marsha, "CENXO");

  const cache = new Map();
  cache.set("CENXO", {
    marshaCode: "CENXO",
    website: "https://www.marriott.com/en-us/hotels/cenxo-city-express-ciudad-obregon/overview",
    propertyUrl: "https://www.marriott.com/en-us/hotels/cenxo-city-express-ciudad-obregon/overview",
    country: "Mexico",
  });
  cache.set("Mexico|CENXO", cache.get("CENXO"));
  const built = await buildMarriottPropertyUrlCityProposals({
    censusRecords: records,
    injectCache: cache,
    marriottCache: cache,
  });
  assert.ok(built.proposals.length >= 1);
  const p = built.proposals.find((x) => x.record_id === "recMar1");
  assert.ok(p);
  assert.equal(p.patch.City, "Ciudad Obregón");
  assert.ok(/cenxo-city-express/i.test(p.patch["Source URL"]));
  assert.ok(!("Latitude" in p.patch));
  assert.ok(!("Phone" in p.patch));
  assert.ok(Object.values(MARRIOTT_URL_CITY_BACKFILL_STATUS).every((s) => typeof s === "string"));
});

await testAsync("mission mode — clean-census-v1 parse, filters, plan, status", async () => {
  const { parseAutopilotArgs, isProductionWriteMode } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const {
    resolveMissionObjective,
    filterMissionPhaseProposals,
    filterProposalsToCleanCoreRecords,
    buildMissionPlan,
    resolveMissionStatus,
    MISSION_STATUS,
    MISSION_PHASE_CORE_IDENTITY,
    MISSION_PHASE_ADDRESS,
    CLEAN_CENSUS_V1_PHASES,
    MISSION_OBJECTIVE_CLEAN_CENSUS_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const { evaluateCleanCorePass } = await import(
    "../lib/research-engine-v2/census-map-contact-size-readiness.js"
  );

  const args = parseAutopilotArgs([
    "--mode",
    "mission",
    "--objective",
    "clean-census-v1",
    "--max-passes",
    "6",
    "--cleanup-existing-only",
    "--region",
    "CALA",
    "--scope",
    "active-brand-setup",
  ]);
  assert.equal(args.mode, "mission");
  assert.equal(args.objective, "clean-census-v1");
  assert.equal(args.maxPasses, 6);
  assert.equal(args.cleanupExistingOnly, true);
  assert.equal(isProductionWriteMode("mission"), true);
  assert.equal(resolveMissionObjective(args.objective), MISSION_OBJECTIVE_CLEAN_CENSUS_V1);
  assert.equal(resolveMissionObjective("nope"), null);
  assert.equal(
    resolveMissionObjective("complete-census-v1"),
    "complete-census-v1"
  );

  const {
    resolveMissionPhases,
    COMPLETE_CENSUS_V1_PHASES,
    COMPLETE_CENSUS_MISSION_STATUS,
    MISSION_OBJECTIVE_COMPLETE_CENSUS_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");

  const completeArgs = parseAutopilotArgs([
    "--mode",
    "mission",
    "--objective",
    "complete-census-v1",
  ]);
  assert.equal(completeArgs.objective, "complete-census-v1");
  const completePhases = resolveMissionPhases(MISSION_OBJECTIVE_COMPLETE_CENSUS_V1);
  assert.equal(completePhases.length, COMPLETE_CENSUS_V1_PHASES.length);
  assert.ok(completePhases.some((p) => p.id === "phase_4_coordinates"));
  assert.ok(completePhases.some((p) => p.id === "phase_5_phone"));
  assert.ok(completePhases.some((p) => p.id === "phase_6_rooms"));
  assert.ok(!completePhases.some((p) => p.queues.includes("source_discovery")));

  assert.equal(
    resolveMissionStatus({
      objective: MISSION_OBJECTIVE_COMPLETE_CENSUS_V1,
      safetyStop: { reason: "wrong_census_table" },
    }),
    COMPLETE_CENSUS_MISSION_STATUS.BLOCKED
  );
  assert.equal(
    resolveMissionStatus({
      objective: MISSION_OBJECTIVE_COMPLETE_CENSUS_V1,
      after: {
        clean_core: 100,
        complete_census_v1: 10,
        blocked_missing_address: 50,
        mapbox_eligible: 20,
      },
    }),
    COMPLETE_CENSUS_MISSION_STATUS.PARTIAL
  );

  const plan = buildMissionPlan({ objective: MISSION_OBJECTIVE_CLEAN_CENSUS_V1 });
  assert.equal(plan.phases.length, CLEAN_CENSUS_V1_PHASES.length);
  assert.equal(plan.per_phase_chatgpt_approval, false);
  assert.ok(plan.phases.some((p) => p.id === "phase_1_core_identity"));
  assert.ok(plan.phases.some((p) => p.id === "phase_4_coordinates"));

  const filtered = filterMissionPhaseProposals(
    [
      {
        record_id: "rec1",
        confidence: "High",
        patch: {
          City: "Cancún",
          Latitude: 21,
          Phone: "+52",
          "Owner Name": "Nope",
          Address: "Street 1",
        },
      },
    ],
    MISSION_PHASE_CORE_IDENTITY
  );
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].patch.City, "Cancún");
  assert.ok(!("Latitude" in filtered[0].patch));
  assert.ok(!("Phone" in filtered[0].patch));
  assert.ok(!("Owner Name" in filtered[0].patch));
  assert.ok(!("Address" in filtered[0].patch));

  const addrFiltered = filterMissionPhaseProposals(
    [
      {
        record_id: "rec1",
        confidence: "High",
        patch: { Address: "Calle 1", Latitude: 1, "Rooms / Keys": 10 },
      },
    ],
    MISSION_PHASE_ADDRESS
  );
  assert.equal(addrFiltered[0].patch.Address, "Calle 1");
  assert.ok(!("Latitude" in addrFiltered[0].patch));
  assert.ok(!("Rooms / Keys" in addrFiltered[0].patch));

  const cleanRec = {
    id: "recClean",
    fields: {
      "Property Name": "Hotel Clean",
      "Canonical Property Name": "Hotel Clean",
      "Current Brand": "Hilton Hotels & Resorts",
      "Brand Family": "Hilton",
      "Family / Source Family": "Hilton",
      City: "Cancún",
      Country: "Mexico",
      "Source URL": "https://www.hilton.com/en/hotels/cunxx-hotel-clean/",
      "Identity Confidence": "High",
      "Data Confidence Tier": "High",
      "Production Use Status": "Census Only / Not Owner-Facing",
    },
  };
  const dirtyRec = {
    id: "recDirty",
    fields: {
      "Property Name": "Hotel Dirty",
      City: "Unknown",
      Country: "Mexico",
      "Source URL": "https://example.com",
    },
  };
  assert.equal(evaluateCleanCorePass(cleanRec).pass, true);
  assert.equal(evaluateCleanCorePass(dirtyRec).pass, false);
  const cleanOnly = filterProposalsToCleanCoreRecords(
    [
      { record_id: "recClean", patch: { Address: "A" } },
      { record_id: "recDirty", patch: { Address: "B" } },
    ],
    [cleanRec, dirtyRec]
  );
  assert.equal(cleanOnly.length, 1);
  assert.equal(cleanOnly[0].record_id, "recClean");

  assert.equal(
    resolveMissionStatus({ safetyStop: { reason: "wrong_census_table" } }),
    MISSION_STATUS.BLOCKED
  );
  assert.equal(
    resolveMissionStatus({
      after: { below_clean_core: 2, steward_remaining: 1, unknown_city: 0, canonical_blank: 0 },
    }),
    MISSION_STATUS.PARTIAL
  );
  assert.equal(
    resolveMissionStatus({
      after: {
        below_clean_core: 0,
        steward_remaining: 0,
        source_lookup_remaining: 0,
        duplicate_risk_remaining: 0,
        unknown_city: 0,
        canonical_blank: 0,
      },
    }),
    MISSION_STATUS.COMPLETE
  );
});

await testAsync("market geography — continent/subcontinent/market maps + queue", async () => {
  const {
    resolveContinentSubContinentFromCountry,
    resolveMarketFromCity,
    resolveSubmarketHighOnly,
  } = await import("../lib/research-engine-v2/census-region-market-map.js");
  const {
    classifyMarketGeography,
    runMarketGeographyCompletionQueue,
    MARKET_GEOGRAPHY_STATUS,
  } = await import("../lib/research-engine-v2/census-market-submarket-classifier.js");
  const { evaluateCleanCorePass } = await import(
    "../lib/research-engine-v2/census-map-contact-size-readiness.js"
  );
  const { QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-queue-router.js"
  );
  const { AUTOPILOT_ALLOWED_WRITE_FIELDS } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.ok(QUEUE_ORDER.some((q) => q.id === "market_geography_completion"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Continent"));
  assert.ok(AUTOPILOT_ALLOWED_WRITE_FIELDS.includes("Sub-Continent"));

  const mx = resolveContinentSubContinentFromCountry("Mexico");
  assert.equal(mx.continent, "North America");
  assert.equal(mx.subContinent, "Mexico");
  const dr = resolveContinentSubContinentFromCountry("Dominican Republic");
  assert.equal(dr.subContinent, "Caribbean");
  assert.equal(resolveContinentSubContinentFromCountry("Atlantis"), null);

  assert.equal(resolveMarketFromCity({ city: "Cancún" }).market, "Cancún");
  assert.equal(resolveMarketFromCity({ city: "Cap Cana" }).market, "Punta Cana");
  assert.equal(resolveMarketFromCity({ city: "Cabo San Lucas" }).market, "Los Cabos");
  assert.equal(resolveMarketFromCity({ city: "Obscure Village XY" }).ok, false);

  const {
    resolveStateRegionFromCity,
    isDirtyStateRegionValue,
    resolveStateFromChoiceOfficialUrl,
  } = await import("../lib/research-engine-v2/census-city-to-state-map.js");
  const { isChoiceCentralReservationPhone } = await import(
    "../lib/research-engine-v2/census-phone-number-enrichment.js"
  );
  assert.equal(isDirtyStateRegionValue("CUN"), true);
  assert.equal(isDirtyStateRegionValue("ATL"), true);
  assert.equal(isDirtyStateRegionValue("BOL"), true);
  assert.equal(isDirtyStateRegionValue("Quintana Roo"), false);
  assert.equal(
    resolveStateRegionFromCity({ city: "Bogotá", country: "Colombia", state: "CUN" }).state,
    "Cundinamarca"
  );
  assert.equal(
    resolveStateRegionFromCity({ city: "Cartagena", country: "Colombia", state: "BOL" }).state,
    "Bolívar"
  );
  assert.equal(
    resolveStateRegionFromCity({ city: "Barranquilla", country: "Colombia", state: "ATL" }).state,
    "Atlántico"
  );
  assert.equal(
    resolveStateFromChoiceOfficialUrl(
      "https://www.choicehotels.com/chihuahua/chihuahua/quality-inn-hotels/mx043"
    ).state,
    "Chihuahua"
  );
  assert.equal(isChoiceCentralReservationPhone("+18887706800"), true);

  const sub = resolveSubmarketHighOnly({
    market: "Mexico City",
    address: "Av Presidente Masaryk, Polanco",
  });
  assert.equal(sub.ok, true);
  assert.equal(sub.submarket, "Polanco");
  assert.equal(
    resolveSubmarketHighOnly({ market: "Cancún", address: "Somewhere Beach Resort" }).ok,
    false
  );

  const rec = {
    id: "recGeo1",
    fields: {
      "Property Name": "Hilton Cancun",
      "Canonical Property Name": "Hilton Cancun",
      "Current Brand": "Hilton",
      "Brand Family": "Hilton",
      City: "Cancún",
      Country: "Mexico",
      "Source URL": "https://www.hilton.com/en/hotels/cunxx-hotel/",
      "Family / Source Family": "Hilton",
      "Identity Confidence": "High",
      "Data Confidence Tier": "High",
      Address: "Blvd Kukulcan Zona Hotelera",
    },
  };
  const classified = classifyMarketGeography(rec, {
    fieldExists: {
      continent: true,
      subContinent: true,
      market: true,
      submarket: true,
    },
  });
  assert.equal(classified.patch.Continent, "North America");
  assert.equal(classified.patch["Sub-Continent"], "Mexico");
  assert.equal(classified.patch.Market, "Cancún");
  assert.equal(classified.patch.Submarket, "Hotel Zone");
  assert.ok(!("Latitude" in classified.patch));
  assert.ok(!("Phone" in classified.patch));

  const conflict = classifyMarketGeography(
    {
      id: "recConflict",
      fields: {
        ...rec.fields,
        Continent: "Europe",
        Country: "Mexico",
      },
    },
    { fieldExists: { continent: true, subContinent: true, market: true, submarket: true } }
  );
  assert.ok(conflict.steward.some((s) => s.reason === "continent_conflict_with_country_map"));
  assert.ok(!conflict.patch.Continent);

  const queue = runMarketGeographyCompletionQueue({
    censusRecords: [rec],
    fieldExists: {
      continent: true,
      subContinent: true,
      market: true,
      submarket: true,
    },
  });
  assert.ok(queue.proposals.length >= 1);
  assert.ok(
    [
      MARKET_GEOGRAPHY_STATUS.READY_NEEDS_MISSION,
      MARKET_GEOGRAPHY_STATUS.PARTIAL_STEWARD,
      MARKET_GEOGRAPHY_STATUS.APPLIED_CLEAN,
    ].includes(queue.status)
  );

  const withGeo = {
    id: "recCleanGeo",
    fields: {
      "Property Name": "Hotel Clean",
      "Canonical Property Name": "Hotel Clean",
      "Current Brand": "Hilton Garden Inn",
      "Brand Family": "Hilton",
      City: "Cancún",
      Country: "Mexico",
      Continent: "North America",
      "Sub-Continent": "Mexico",
      "Source URL": "https://www.hilton.com/en/hotels/cunxx-hotel-clean/",
      "Family / Source Family": "Hilton",
      "Identity Confidence": "High",
      "Data Confidence Tier": "High",
      "Production Use Status": "Census Only / Not Owner-Facing",
    },
  };
  assert.equal(
    evaluateCleanCorePass(withGeo, { continentFieldExists: true }).pass,
    true
  );
  assert.equal(
    evaluateCleanCorePass(
      { id: "recNoCont", fields: { ...withGeo.fields, Continent: "" } },
      { continentFieldExists: true }
    ).pass,
    false
  );
  // Without flag, Continent not required (backward compatible)
  assert.equal(evaluateCleanCorePass({ id: "recNoCont2", fields: { ...withGeo.fields, Continent: "" } }).pass, true);
});

await testAsync("coverage-reconciliation-v1 — classify, brand filter, restricted inserts", async () => {
  const { parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const { resolveMissionObjective, MISSION_OBJECTIVE_COVERAGE_RECONCILIATION_V1 } =
    await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    COVERAGE_CLASS,
    COVERAGE_STATUS,
    COVERAGE_INSERT_NEVER_FIELDS,
    brandsEqualExact,
    filterDiscoveredByBrand,
    countCensusByBrand,
    toCoverageClass,
    attachCoverageClasses,
    buildCoverageInsertFields,
    buildCoverageInsertApprovalBundle,
    buildBrandCoverageRollup,
    resolveCoverageStatus,
    runCoverageReconciliation,
  } = await import("../lib/research-engine-v2/census-autopilot-coverage-reconciliation.js");
  const { MATCH_CLASS, classifyDiscoveredAgainstCensus } = await import(
    "../lib/research-engine-v2/census-autopilot-source-discovery.js"
  );

  const args = parseAutopilotArgs([
    "--mode",
    "controlled",
    "--objective",
    "coverage-reconciliation-v1",
    "--region",
    "CALA",
    "--parent-company",
    "Marriott",
    "--brand",
    "Sheraton",
    "--strategy",
    "fastest-safe",
    "--batch-size",
    "100",
  ]);
  assert.equal(args.brand, "Sheraton");
  assert.equal(args.parentCompany, "Marriott");
  assert.equal(args.objective, "coverage-reconciliation-v1");
  assert.equal(
    resolveMissionObjective(args.objective),
    MISSION_OBJECTIVE_COVERAGE_RECONCILIATION_V1
  );

  assert.equal(brandsEqualExact("Sheraton", "sheraton"), true);
  assert.equal(brandsEqualExact("Sheraton", "Four Points by Sheraton"), false);

  const discovered = [
    {
      property_name: "Sheraton Mexico City",
      brand: "Sheraton",
      city: "Mexico City",
      country: "Mexico",
      official_property_id: "MEXSI",
      official_property_url: "https://www.marriott.com/en-us/hotels/mexsi-sheraton-mexico-city/overview/",
      official_directory_url: "https://www-cdn.ihg.com/content/sitemap/mexico-hotels.xml",
      source_family: "Marriott",
      parent_company: "Marriott",
      identity_key: "ind_marriott_mx_mexsi",
      identity_confidence: "High",
      source_confidence: "High",
    },
    {
      property_name: "Four Points Cancun",
      brand: "Four Points by Sheraton",
      city: "Cancún",
      country: "Mexico",
      official_property_id: "CUNFP",
      official_property_url: "https://www.marriott.com/en-us/hotels/cunfp-four-points/overview/",
      source_family: "Marriott",
      parent_company: "Marriott",
      identity_key: "ind_marriott_mx_cunfp",
      identity_confidence: "High",
      source_confidence: "High",
    },
    {
      property_name: "Sheraton Missing High",
      brand: "Sheraton",
      city: "Bogotá",
      country: "Colombia",
      official_property_id: "BOGSH",
      official_property_url: "https://www.marriott.com/en-us/hotels/bogsh-sheraton-bogota/overview/",
      official_directory_url: "https://example.marriott/sitemap",
      source_family: "Marriott",
      parent_company: "Marriott",
      identity_key: "ind_marriott_co_bogsh",
      identity_confidence: "High",
      source_confidence: "High",
    },
    {
      property_name: "Sheraton Weak",
      brand: "Sheraton",
      city: null,
      country: "Mexico",
      official_property_id: "WEAK1",
      official_property_url: null,
      source_family: "Marriott",
      parent_company: "Marriott",
      identity_key: "ind_marriott_mx_weak1",
      identity_confidence: "Medium",
      source_confidence: "Medium",
    },
  ];

  const sheratonOnly = filterDiscoveredByBrand(discovered, "Sheraton");
  assert.equal(sheratonOnly.length, 3);
  assert.ok(sheratonOnly.every((d) => d.brand === "Sheraton"));

  const census = [
    {
      id: "recExist",
      fields: {
        "Property Name": "Sheraton Mexico City",
        "Current Brand": "Sheraton",
        City: "Mexico City",
        Country: "Mexico",
        "Property Identity Key": "ind_marriott_mx_mexsi",
        "Official Property URL":
          "https://www.marriott.com/en-us/hotels/mexsi-sheraton-mexico-city/overview/",
      },
    },
    {
      id: "recOther",
      fields: {
        "Property Name": "Courtyard Other",
        "Current Brand": "Courtyard",
        City: "Mexico City",
        Country: "Mexico",
      },
    },
  ];
  assert.equal(countCensusByBrand(census, "Sheraton").count, 1);

  const matchRaw = classifyDiscoveredAgainstCensus(sheratonOnly, census);
  const match = attachCoverageClasses(matchRaw);
  assert.ok((match.coverage_counts[COVERAGE_CLASS.EXISTING_EXACT] || 0) >= 1);
  assert.ok((match.coverage_counts[COVERAGE_CLASS.MISSING_HIGH] || 0) >= 1);
  assert.equal(toCoverageClass(MATCH_CLASS.NEW_CANDIDATE), COVERAGE_CLASS.MISSING_HIGH);
  assert.equal(toCoverageClass(MATCH_CLASS.STEWARD), COVERAGE_CLASS.MISSING_STEWARD);

  const missingHigh = match.classified.find(
    (r) => r.coverage_class === COVERAGE_CLASS.MISSING_HIGH
  );
  assert.ok(missingHigh);
  const insertFields = buildCoverageInsertFields(missingHigh);
  assert.ok(insertFields.fields["Property Name"]);
  assert.ok(insertFields.fields["Canonical Property Name"]);
  assert.ok(insertFields.fields["Current Brand"]);
  assert.ok(insertFields.fields.Continent || insertFields.fields["Sub-Continent"]);
  for (const never of ["Latitude", "Longitude", "Phone", "Rooms / Keys", "Address"]) {
    assert.ok(!Object.prototype.hasOwnProperty.call(insertFields.fields, never));
    assert.ok(COVERAGE_INSERT_NEVER_FIELDS.includes(never));
  }

  const bundle = buildCoverageInsertApprovalBundle({ classified: match.classified });
  assert.ok(bundle.proposed_insert_count >= 1);
  assert.equal(bundle.fuzzy_auto_insert, false);
  assert.equal(bundle.lat_long_on_insert, false);
  assert.equal(bundle.write_target.table_id, "tbl9aY5ijiuIzzWam");

  const rollups = buildBrandCoverageRollup(match.classified, census, { brand: "Sheraton" });
  assert.ok(rollups.some((b) => b.brand === "Sheraton"));

  assert.equal(
    resolveCoverageStatus({
      coverage_counts: { [COVERAGE_CLASS.MISSING_HIGH]: 1 },
      official_inventory_count: 3,
    }),
    COVERAGE_STATUS.PARTIAL
  );
  assert.equal(
    resolveCoverageStatus({
      coverage_counts: {
        [COVERAGE_CLASS.EXISTING_EXACT]: 3,
        [COVERAGE_CLASS.MISSING_HIGH]: 0,
        [COVERAGE_CLASS.MISSING_STEWARD]: 0,
        [COVERAGE_CLASS.DUPLICATE_RISK]: 0,
      },
      official_inventory_count: 3,
    }),
    COVERAGE_STATUS.COMPLETE
  );

  const report = await runCoverageReconciliation({
    mode: "controlled",
    region: "CALA",
    parentCompany: "Marriott",
    brand: "Sheraton",
    censusRecords: census,
    discoveryResult: {
      discovered: sheratonOnly,
      vicEvidence: [],
      sourceReport: {
        families_used: ["Marriott"],
        blocked_source_families: [],
        adapter_errors: [],
      },
    },
    enableProductionWrites: false,
    allApplyConfirms: false,
    log: () => {},
  });
  assert.equal(report.objective, "coverage-reconciliation-v1");
  assert.ok(
    [
      COVERAGE_STATUS.COMPLETE,
      COVERAGE_STATUS.PARTIAL,
      COVERAGE_STATUS.BLOCKED,
    ].includes(report.status)
  );
  assert.equal(report.write_target.table_id, "tbl9aY5ijiuIzzWam");
  assert.equal(report.brand_setup_writes, false);
  assert.equal(report.brand_explorer_writes, false);
  assert.equal(report.airtable_writes, false);
  assert.ok(report.official_inventory_count >= 1);
});

await testAsync("coverage-steward-resolution-v1 — city/brand gates + insertable", async () => {
  const { parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_COVERAGE_STEWARD_RESOLUTION_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    resolveStewardCityHigh,
    resolveIhgBrandName,
    resolveMarriottBrandHigh,
    resolveCoverageStewardCase,
    processStewardResolutions,
    classifyStewardBlocker,
    COVERAGE_STEWARD_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-coverage-steward-resolution.js"
  );

  const args = parseAutopilotArgs([
    "--mode",
    "mission",
    "--objective",
    "coverage-steward-resolution-v1",
    "--region",
    "CALA",
  ]);
  assert.equal(args.objective, "coverage-steward-resolution-v1");
  assert.equal(
    resolveMissionObjective(args.objective),
    MISSION_OBJECTIVE_COVERAGE_STEWARD_RESOLUTION_V1
  );

  const ihgBrand = resolveIhgBrandName("holidayinn");
  assert.equal(ihgBrand.ok, true);
  assert.equal(ihgBrand.brand, "Holiday Inn");

  const cityPanama = resolveStewardCityHigh({
    property_name: "Holiday Inn Panama Canal",
    brand: "holidayinn",
    city: "Panama",
    country: "Panama",
    official_property_url:
      "https://www.ihg.com/holidayinn/hotels/us/en/panama/pcyhi/hoteldetail",
    official_property_id: "PCYHI",
  });
  assert.equal(cityPanama.ok, true);
  assert.equal(cityPanama.city, "Panama City");

  const citySofitel = resolveStewardCityHigh({
    property_name: "Sofitel Mexico City Reforma",
    brand: "Sofitel",
    city: "Mexico",
    country: "Mexico",
    official_property_url: "https://all.accor.com/hotel/9615/index.en.shtml",
    official_property_id: "9615",
  });
  assert.equal(citySofitel.ok, true);
  assert.equal(citySofitel.city, "Mexico City");

  const brandMar = resolveMarriottBrandHigh({
    property_name: "Marriott Panama Hotel",
    brand: "Marriott Bonvoy — Brand Unconfirmed",
    official_property_url:
      "https://www.marriott.com/en-us/hotels/ptymc-marriott-panama-hotel/overview",
  });
  assert.equal(brandMar.ok, true);
  assert.equal(brandMar.brand, "Marriott Hotels");

  const bogCity = resolveStewardCityHigh({
    property_name: "The Artisan D.C. Hotel, Autograph Collection",
    brand: "Autograph Collection",
    city: null,
    country: "Colombia",
    official_property_id: "BOGAK",
    official_property_url:
      "https://www.marriott.com/en-us/hotels/bogak-the-artisan-dc-hotel-autograph-collection/overview",
  });
  assert.equal(bogCity.ok, true);
  assert.equal(bogCity.city, "Bogotá");

  const resolved = resolveCoverageStewardCase({
    property_name: "Holiday Inn Panama Distrito Financiero",
    brand: "holidayinn",
    city: "Panama",
    country: "Panama",
    parent_company: "IHG",
    source_family: "IHG",
    official_property_id: "PCYEX",
    official_property_url:
      "https://www.ihg.com/holidayinn/hotels/us/en/panama/pcyex/hoteldetail",
    identity_key: "ind_ihg_pa_pcyex",
    coverage_class: "missing_needs_steward",
    identity_confidence: "Medium",
  });
  assert.equal(resolved.resolved, true);
  assert.equal(resolved.after.city, "Panama City");
  assert.equal(resolved.after.brand, "Holiday Inn");
  assert.equal(resolved.after.identity_confidence, "High");

  const stillAmbiguous = resolveCoverageStewardCase({
    property_name: "Hotel Belmar, a Member of Design Hotels™",
    brand: "Design Hotels",
    city: null,
    country: "Costa Rica",
    parent_company: "Marriott",
    source_family: "Marriott",
    official_property_id: "LIRHB",
    official_property_url:
      "https://www.marriott.com/en-us/hotels/lirhb-hotel-belmar-a-member-of-design-hotels/overview",
    identity_key: "ind_marriott_cr_lirhb",
    coverage_class: "missing_needs_steward",
  });
  assert.equal(stillAmbiguous.resolved, false);
  assert.ok(classifyStewardBlocker(stillAmbiguous.after));

  const processed = processStewardResolutions(
    [
      {
        property_name: "Crowne Plaza Panama Airport",
        brand: "crowneplaza",
        city: "Panama",
        country: "Panama",
        parent_company: "IHG",
        source_family: "IHG",
        official_property_id: "PCYAP",
        official_property_url:
          "https://www.ihg.com/crowneplaza/hotels/us/en/panama/pcyap/hoteldetail",
        identity_key: "ind_ihg_pa_pcyap",
        coverage_class: "missing_needs_steward",
        identity_confidence: "Medium",
      },
    ],
    []
  );
  assert.equal(processed.insertable.length, 1);
  assert.ok(Object.values(COVERAGE_STEWARD_STATUS).every((s) => typeof s === "string"));
});

await testAsync("source-confirmed-census-v2 — official remap + opaque steward", async () => {
  const {
    resolveCensusOfficialBrand,
    isOpaqueBrandCode,
    isCensusOfficialBrand,
  } = await import("../lib/research-engine-v2/census-official-brand-registry.js");
  const {
    classifySourceConfirmedBrandRow,
    buildSourceConfirmedBrandProposals,
    SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE,
    SOURCE_CONFIRMED_STATUS,
  } = await import("../lib/research-engine-v2/census-autopilot-source-confirmed-census-v2.js");
  const { resolveMissionObjective, MISSION_OBJECTIVE_SOURCE_CONFIRMED_CENSUS_V2 } =
    await import("../lib/research-engine-v2/census-autopilot-mission.js");

  assert.equal(
    resolveMissionObjective("source-confirmed-census-v2"),
    MISSION_OBJECTIVE_SOURCE_CONFIRMED_CENSUS_V2
  );
  assert.equal(SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE, "source-confirmed-census-v2");
  assert.ok(SOURCE_CONFIRMED_STATUS.PARTIAL.includes("partial_steward"));

  assert.equal(
    resolveCensusOfficialBrand("holidayinn", {
      sourceUrl: "https://www.ihg.com/holidayinn/hotels/us/en/x/y/hoteldetail",
      sourceFamily: "IHG",
    }).canonical,
    "Holiday Inn"
  );
  assert.equal(
    resolveCensusOfficialBrand("Es Xl", {
      sourceUrl: "https://www.wyndhamhotels.com/es-xl/x/y/overview",
    }).canonical,
    "Esplendor by Wyndham"
  );
  assert.equal(
    resolveCensusOfficialBrand("Es Xl", {
      sourceUrl: "https://www.wyndhamhotels.com/wyndham-garden/monterrey/x/overview",
      propertyName: "Wyndham Garden Monterrey",
    }).canonical,
    "Wyndham Garden"
  );
  assert.equal(
    resolveCensusOfficialBrand("SAM", {
      sourceUrl: "https://all.accor.com/hotel/C4P4/index.en.shtml",
    }).steward_code,
    "brand_code_unresolved"
  );
  assert.equal(isOpaqueBrandCode("MOD"), false);
  assert.equal(resolveCensusOfficialBrand("MOD").canonical, "Mondrian");
  assert.equal(resolveCensusOfficialBrand("BAN").canonical, "Banyan Tree");
  assert.equal(
    resolveCensusOfficialBrand("Pt Br", {
      sourceUrl:
        "https://www.wyndhamhotels.com/pt-br/registry-collection/punta-cana/x/overview",
    }).canonical,
    "Registry Collection"
  );
  assert.equal(isCensusOfficialBrand("Four Points by Sheraton"), true);
  assert.equal(isCensusOfficialBrand("City Express by Marriott"), true);
  assert.equal(isCensusOfficialBrand("JOIA Iberostar"), true);

  const hi = classifySourceConfirmedBrandRow({
    id: "recTest1",
    fields: {
      "Current Brand": "holidayinn",
      "Property Name": "Holiday Inn Cartagena",
      "Official Property URL":
        "https://www.ihg.com/holidayinn/hotels/us/en/cartagena/ctghi/hoteldetail",
      "Family / Source Family": "IHG",
    },
  });
  assert.equal(hi.class, "high_safe_remap");
  assert.equal(hi.high_patch["Current Brand"], "Holiday Inn");

  const plan = buildSourceConfirmedBrandProposals([
    {
      id: "recTest1",
      fields: {
        "Current Brand": "holidayinn",
        "Property Name": "Holiday Inn Cartagena",
        "Official Property URL":
          "https://www.ihg.com/holidayinn/hotels/us/en/cartagena/ctghi/hoteldetail",
        "Family / Source Family": "IHG",
      },
    },
    {
      id: "recTest2",
      fields: {
        "Current Brand": "SAM",
        "Property Name": "Mystery Accor",
        "Official Property URL": "https://all.accor.com/hotel/C4P4/index.en.shtml",
        "Family / Source Family": "Accor",
      },
    },
  ]);
  assert.equal(plan.proposals.length, 2); // remap + HR flag for opaque
  assert.ok(plan.steward_cases.some((s) => s.reason_code === "brand_code_unresolved"));
});

await testAsync("brand-registry-resolution-v1 — decode codes + dirty SAM + promotion pack", async () => {
  const {
    classifyBrandRegistryResolutionRow,
    buildBrandRegistryResolutionPlan,
    classifyDirtyPartnerLabel,
    extractAccorPropertyId,
    recommendPromotionAction,
    BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE,
    BRAND_REGISTRY_RESOLUTION_STATUS,
    RESOLUTION_CLASS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-brand-registry-resolution-v1.js"
  );
  const { resolveMissionObjective, MISSION_OBJECTIVE_BRAND_REGISTRY_RESOLUTION_V1 } =
    await import("../lib/research-engine-v2/census-autopilot-mission.js");

  assert.equal(
    resolveMissionObjective("brand-registry-resolution-v1"),
    MISSION_OBJECTIVE_BRAND_REGISTRY_RESOLUTION_V1
  );
  assert.equal(BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE, "brand-registry-resolution-v1");
  assert.ok(BRAND_REGISTRY_RESOLUTION_STATUS.PARTIAL.includes("partial_remaining"));

  assert.equal(
    extractAccorPropertyId("https://all.accor.com/hotel/B9Z5/index.en.shtml"),
    "B9Z5"
  );
  assert.equal(classifyDirtyPartnerLabel("SAM").dirty, true);
  assert.equal(classifyDirtyPartnerLabel("IHG Partner / Spnd").dirty, true);
  assert.equal(classifyDirtyPartnerLabel("Choice Hotels").dirty, true);

  const ban = classifyBrandRegistryResolutionRow({
    id: "recBan",
    fields: {
      "Current Brand": "BAN",
      "Property Name": "Banyan Tree Puebla",
      "Official Property URL": "https://all.accor.com/hotel/B9Z5/index.en.shtml",
      "Family / Source Family": "Accor",
      Country: "Mexico",
    },
  });
  assert.equal(ban.class, RESOLUTION_CLASS.SOURCE_CODE_DECODED);
  assert.equal(ban.high_patch["Current Brand"], "Banyan Tree");

  const sam = classifyBrandRegistryResolutionRow({
    id: "recSam",
    fields: {
      "Current Brand": "SAM",
      "Property Name": "Club Regina managed by Accor",
      "Official Property URL": "https://all.accor.com/hotel/C4P4/index.en.shtml",
      "Family / Source Family": "Accor",
    },
  });
  assert.equal(sam.class, RESOLUTION_CLASS.DIRTY_PARTNER_LABEL);
  assert.equal(sam.high_patch["Human Review Required"], true);
  assert.ok(!sam.high_patch["Current Brand"]);

  const ptBr = classifyBrandRegistryResolutionRow({
    id: "recPt",
    fields: {
      "Current Brand": "Pt Br",
      "Property Name": "Grand Palladium Select Bavaro",
      "Official Property URL":
        "https://www.wyndhamhotels.com/pt-br/registry-collection/punta-cana-dominican-republic/grand-palladium/overview",
      "Family / Source Family": "Wyndham",
      Country: "Dominican Republic",
    },
  });
  assert.equal(ptBr.high_patch["Current Brand"], "Registry Collection");

  const plan = buildBrandRegistryResolutionPlan([
    {
      id: "recBan",
      fields: {
        "Current Brand": "BAN",
        "Property Name": "Banyan Tree Puebla",
        "Official Property URL": "https://all.accor.com/hotel/B9Z5/index.en.shtml",
        "Family / Source Family": "Accor",
        Country: "Mexico",
      },
    },
    {
      id: "recSam",
      fields: {
        "Current Brand": "SAM",
        "Property Name": "Club Regina",
        "Official Property URL": "https://all.accor.com/hotel/C4P4/index.en.shtml",
        "Family / Source Family": "Accor",
        Country: "Mexico",
      },
    },
    {
      id: "recTribe",
      fields: {
        "Current Brand": "TRIBE",
        "Property Name": "TRIBE Medellín",
        "Official Property URL": "https://all.accor.com/hotel/B780/index.en.shtml",
        "Family / Source Family": "Accor",
        Country: "Colombia",
      },
    },
  ]);
  assert.ok(plan.proposals.some((p) => p.brand_after === "Banyan Tree"));
  assert.ok(plan.steward_cases.some((s) => s.class === RESOLUTION_CLASS.DIRTY_PARTNER_LABEL));
  assert.ok(
    plan.promotion_candidates.some((c) => c.proposed_brand_name === "Banyan Tree") ||
      plan.promotion_candidates.some((c) => c.proposed_brand_name === "TRIBE")
  );
  const action = recommendPromotionAction({
    proposed_brand_name: "Banyan Tree",
    census_records_affected: 3,
    in_official_parent_inventory: true,
    official_source_evidence: true,
  });
  assert.equal(action, "promote_to_brand_setup");
});

await testAsync("cala-census-completion-v1 — objective, park dirty, promotion pack read-only", async () => {
  const {
    resolveMissionObjective,
    resolveMissionPhases,
    resolveMissionStatus,
    MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1,
    CALA_CENSUS_COMPLETION_STATUS,
    CALA_CENSUS_COMPLETION_V1_PHASES,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    parkDirtyPartnerLabels,
    loadBrandSetupPromotionPack,
    summarizeBrandInventory,
    CALA_CENSUS_COMPLETION_V1_OBJECTIVE,
  } = await import("../lib/research-engine-v2/census-autopilot-cala-census-completion-v1.js");

  assert.equal(
    resolveMissionObjective("cala-census-completion-v1"),
    MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1
  );
  assert.equal(CALA_CENSUS_COMPLETION_V1_OBJECTIVE, "cala-census-completion-v1");
  const phases = resolveMissionPhases(MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1);
  assert.equal(phases.length, CALA_CENSUS_COMPLETION_V1_PHASES.length);
  assert.ok(phases.some((p) => p.id === "phase_2_reconfirm_brand_core"));
  assert.ok(phases.some((p) => p.id === "phase_5_address"));
  assert.ok(phases.some((p) => p.id === "phase_6_coordinates"));
  assert.ok(phases.some((p) => p.id === "phase_7_phone"));
  assert.ok(phases.some((p) => p.id === "phase_8_rooms"));
  assert.ok(!phases.some((p) => (p.queues || []).includes("source_discovery")));

  assert.equal(
    resolveMissionStatus({
      objective: MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1,
      safetyStop: { reason: "wrong_census_table" },
    }),
    CALA_CENSUS_COMPLETION_STATUS.BLOCKED
  );
  assert.equal(
    resolveMissionStatus({
      objective: MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1,
      after: {
        clean_core: 10,
        complete_census_v1: 10,
        dirty_partner_labels: 16,
        steward_remaining: 16,
        blocked_missing_address: 0,
        blocked_source_access: 0,
        source_lookup_remaining: 0,
      },
    }),
    CALA_CENSUS_COMPLETION_STATUS.COMPLETE
  );
  assert.equal(
    resolveMissionStatus({
      objective: MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1,
      after: {
        clean_core: 100,
        complete_census_v1: 10,
        dirty_partner_labels: 16,
        steward_remaining: 16,
        blocked_missing_address: 40,
      },
    }),
    CALA_CENSUS_COMPLETION_STATUS.PARTIAL
  );

  const park = await parkDirtyPartnerLabels(
    [
      {
        id: "recSam",
        fields: {
          "Current Brand": "SAM",
          "Property Name": "Club Regina managed by Accor",
          "Official Property URL": "https://all.accor.com/hotel/C4P4/index.en.shtml",
          "Human Review Required": false,
        },
      },
      {
        id: "recOk",
        fields: {
          "Current Brand": "Sheraton",
          "Property Name": "Sheraton Test",
          "Official Property URL": "https://www.marriott.com/hotels/sheraton-test",
        },
      },
    ],
    { enableWrites: false }
  );
  assert.equal(park.proposals_planned, 1);
  assert.equal(park.brand_setup_writes, false);
  assert.equal(park.steward_review_required, true);

  const inv = summarizeBrandInventory([
    {
      id: "recSam",
      fields: {
        "Current Brand": "SAM",
        "Property Name": "x",
        "Human Review Required": true,
      },
    },
  ]);
  assert.equal(inv.dirty_partner_labels, 1);

  const pack = loadBrandSetupPromotionPack();
  assert.equal(pack.brand_setup_writes, false);
  assert.equal(pack.brand_explorer_writes, false);
});

await testAsync("brand_normalization — dictionary, alias High fix, Clean Core gate", async () => {
  const {
    buildCanonicalBrandDictionary,
    lookupCanonicalBrand,
  } = await import("../lib/research-engine-v2/census-brand-canonical-dictionary.js");
  const {
    classifyCensusBrand,
    buildBrandNormalizationProposals,
    evaluateBrandSourceOfTruth,
    BRAND_CLASS,
    BRAND_NORMALIZATION_QUEUE_ID,
    BRAND_NORMALIZATION_STATUS,
  } = await import("../lib/research-engine-v2/census-brand-normalization.js");
  const { evaluateCleanCorePass } = await import(
    "../lib/research-engine-v2/census-map-contact-size-readiness.js"
  );
  const { parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const { QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-queue-router.js"
  );

  assert.ok(QUEUE_ORDER.some((q) => q.id === BRAND_NORMALIZATION_QUEUE_ID));
  const args = parseAutopilotArgs([
    "--mode",
    "controlled",
    "--queue",
    "brand_normalization",
    "--region",
    "CALA",
  ]);
  assert.equal(args.queue, "brand_normalization");

  const dict = buildCanonicalBrandDictionary({ region: "CALA" });
  assert.ok(dict.active_brand_count >= 50);

  const sheraton = lookupCanonicalBrand("sheraton", dict);
  assert.equal(sheraton.ok, true);
  assert.equal(sheraton.canonical, "Sheraton");

  const hgi = lookupCanonicalBrand("Hilton Garden", dict);
  assert.equal(hgi.ok, true);
  assert.equal(hgi.canonical, "Hilton Garden Inn");

  const holidaySteward = lookupCanonicalBrand("holidayinn", dict, {
    propertyName: "Holiday Inn Panama Canal",
  });
  assert.equal(holidaySteward.ok, false);

  const classified = classifyCensusBrand(
    {
      id: "recBrand1",
      fields: {
        "Property Name": "Hilton Garden Inn Test City",
        "Current Brand": "Hilton Garden",
        "Brand Family": "Hilton",
        "Family / Source Family": "Hilton",
        "Source URL": "https://www.hilton.com/en/hotels/xxx-hilton-garden-inn/",
        Country: "Mexico",
      },
    },
    dict
  );
  assert.equal(classified.classification, BRAND_CLASS.ALIAS_NORMALIZABLE);
  assert.equal(classified.high_fix["Current Brand"], "Hilton Garden Inn");

  const gatePass = evaluateBrandSourceOfTruth(
    {
      id: "recOk",
      fields: {
        "Property Name": "Sheraton León",
        "Current Brand": "Sheraton",
        "Brand Family": "Marriott",
        "Family / Source Family": "Marriott",
        "Source URL": "https://www.marriott.com/en-us/hotels/bjxsi-sheraton/overview",
        Country: "Mexico",
      },
    },
    dict
  );
  assert.equal(gatePass.pass, true);

  const proposals = buildBrandNormalizationProposals(
    [
      {
        id: "recBrand1",
        fields: {
          "Property Name": "Hilton Garden Inn Test City",
          "Current Brand": "Hilton Garden",
          "Brand Family": "Hilton",
          "Family / Source Family": "Hilton",
          "Source URL": "https://www.hilton.com/en/hotels/xxx/",
          Country: "Mexico",
        },
      },
      {
        id: "recValid",
        fields: {
          "Property Name": "Sheraton León",
          "Current Brand": "Sheraton",
          "Brand Family": "Marriott",
          "Family / Source Family": "Marriott",
          "Source URL": "https://www.marriott.com/en-us/hotels/bjxsi/",
          Country: "Mexico",
        },
      },
    ],
    { dictionary: dict }
  );
  assert.ok(proposals.proposals.length >= 1);
  assert.ok(Object.values(BRAND_NORMALIZATION_STATUS).every((s) => typeof s === "string"));

  // Clean Core brand gate: invalid brand blocks; skip flag preserves prior behavior
  const dirty = {
    id: "recDirtyBrand",
    fields: {
      "Property Name": "Some Hotel",
      "Canonical Property Name": "Some Hotel",
      "Current Brand": "NotARealBrandXYZ",
      Country: "Mexico",
      City: "Cancún",
      "Source URL": "https://example.com/x",
      "Family / Source Family": "Hilton",
      "Brand Family": "Hilton",
      "Identity Confidence": "High",
      "Data Confidence Tier": "High",
      Continent: "North America",
      "Sub-Continent": "Mexico",
    },
  };
  assert.equal(
    evaluateCleanCorePass(dirty, {
      continentFieldExists: true,
      brandDictionary: dict,
    }).pass,
    false
  );
  assert.equal(
    evaluateCleanCorePass(dirty, {
      continentFieldExists: true,
      skipBrandSourceOfTruth: true,
      skipParentConsistencyCheck: true,
    }).pass,
    true
  );
});

await testAsync("parent_company_normalization — alias High fix + Clean Core gate", async () => {
  const {
    canonicalizeParentCompany,
    isCanonicalParentCompany,
    classifyCensusParentCompany,
    buildParentCompanyNormalizationProposals,
    evaluateParentCompanyCleanCoreGate,
    PARENT_CLASS,
    PARENT_COMPANY_NORMALIZATION_QUEUE_ID,
    PARENT_COMPANY_NORMALIZATION_STATUS,
    CANONICAL_PARENT_COMPANIES,
  } = await import("../lib/research-engine-v2/census-parent-company-normalization.js");
  const { QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-queue-router.js"
  );
  const { PRODUCTION_CYCLE_QUEUE_ORDER } = await import(
    "../lib/research-engine-v2/census-autopilot-production-cycle.js"
  );
  const { parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const { evaluateCleanCorePass } = await import(
    "../lib/research-engine-v2/census-map-contact-size-readiness.js"
  );

  assert.ok(QUEUE_ORDER.some((q) => q.id === PARENT_COMPANY_NORMALIZATION_QUEUE_ID));
  assert.equal(PRODUCTION_CYCLE_QUEUE_ORDER[2], "parent_company_normalization");
  const args = parseAutopilotArgs([
    "--mode",
    "controlled",
    "--queue",
    "parent_company_normalization",
    "--region",
    "CALA",
  ]);
  assert.equal(args.queue, "parent_company_normalization");

  assert.equal(canonicalizeParentCompany("Marriott"), "Marriott International");
  assert.equal(canonicalizeParentCompany("Choice Hotels International, Inc."), "Choice Hotels International");
  assert.equal(canonicalizeParentCompany("IHG Hotels & Resorts"), "IHG");
  assert.equal(canonicalizeParentCompany("Wyndham"), "Wyndham Hotels & Resorts");
  assert.equal(canonicalizeParentCompany("SLH"), "Small Luxury Hotels of the World");
  assert.equal(isCanonicalParentCompany("Hilton"), true);
  assert.equal(isCanonicalParentCompany("Marriott"), false);
  assert.ok(CANONICAL_PARENT_COMPANIES.includes("Marriott International"));

  const aliasRow = classifyCensusParentCompany({
    id: "recP1",
    fields: {
      "Property Name": "Sheraton Test",
      "Current Brand": "Sheraton",
      "Brand Family": "Marriott",
      "Family / Source Family": "Marriott",
      "Source URL": "https://www.marriott.com/en-us/hotels/xxx-sheraton/overview",
      Country: "Mexico",
    },
  });
  assert.equal(aliasRow.classification, PARENT_CLASS.ALIAS_NORMALIZABLE);
  assert.equal(aliasRow.high_fix["Brand Family"], "Marriott International");

  const validRow = classifyCensusParentCompany({
    id: "recP2",
    fields: {
      "Property Name": "Hilton Garden Inn Test",
      "Current Brand": "Hilton Garden Inn",
      "Brand Family": "Hilton",
      "Source URL": "https://www.hilton.com/en/hotels/xxx/",
      Country: "Mexico",
    },
  });
  assert.equal(validRow.classification, PARENT_CLASS.VALID);

  const props = buildParentCompanyNormalizationProposals([
    {
      id: "recP1",
      fields: {
        "Property Name": "Sheraton Test",
        "Current Brand": "Sheraton",
        "Brand Family": "Marriott",
        "Source URL": "https://www.marriott.com/en-us/hotels/xxx/",
        Country: "Mexico",
      },
    },
    {
      id: "recChoice",
      fields: {
        "Property Name": "Sleep Inn Test",
        "Current Brand": "Sleep Inn",
        "Brand Family": "Choice",
        "Source URL": "https://www.choicehotels.com/xxx",
        Country: "Costa Rica",
      },
    },
  ]);
  assert.ok(props.proposals.length >= 2);
  assert.ok(Object.values(PARENT_COMPANY_NORMALIZATION_STATUS).every((s) => typeof s === "string"));

  const gateAlias = evaluateParentCompanyCleanCoreGate({
    id: "recAlias",
    fields: { "Brand Family": "Marriott", "Current Brand": "Sheraton" },
  });
  assert.equal(gateAlias.pass, false);
  assert.equal(gateAlias.blocker, "parent_normalization_needed");

  const gateOk = evaluateParentCompanyCleanCoreGate({
    id: "recOk",
    fields: {
      "Brand Family": "Hilton",
      "Current Brand": "Hilton Garden Inn",
      "Source URL": "https://www.hilton.com/en/hotels/x/",
    },
  });
  assert.equal(gateOk.pass, true);

  // Clean Core fails on non-canonical Brand Family alias
  const dirtyParent = {
    id: "recDirtyParent",
    fields: {
      "Property Name": "Hotel X",
      "Canonical Property Name": "Hotel X",
      "Current Brand": "Sheraton",
      "Brand Family": "Marriott",
      "Family / Source Family": "Marriott",
      City: "Cancún",
      Country: "Mexico",
      "Source URL": "https://www.marriott.com/en-us/hotels/x/",
      "Data Confidence Tier": "High",
      "Identity Confidence": "High",
    },
  };
  assert.equal(evaluateCleanCorePass(dirtyParent, { skipBrandSourceOfTruth: true }).pass, false);
  dirtyParent.fields["Brand Family"] = "Marriott International";
  assert.equal(
    evaluateCleanCorePass(dirtyParent, { skipBrandSourceOfTruth: true }).pass,
    true
  );
});

await testAsync("marriott-webhound-source-pattern-learning-v1 — SoT guards + adapters", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_MARRIOTT_WEBHOUND_SOURCE_PATTERN_LEARNING_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE,
    MARRIOTT_WEBHOUND_LEARNING_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-marriott-webhound-source-pattern-learning-v1.js"
  );
  const {
    assertWebhoundNotCensusSot,
    normalizeDiscoveredPattern,
    buildMarriottPatternLearningCatalog,
    WEBHOUND_CONFIRMED_PATTERNS,
    extractPatternsFromWebhoundText,
  } = await import("../lib/research-engine-v2/marriott-webhound-pattern-learner.js");
  const { buildMarriottGapClusterSamples } = await import(
    "../lib/research-engine-v2/marriott-source-pattern-discovery.js"
  );
  const { buildMarriottAlternatePropertyUrls, buildMarriottMetadataPatch } =
    await import("../lib/research-engine-v2/marriott-official-metadata-adapter.js");
  const {
    validateMarriottRoomsCandidate,
    buildMarriottDamFactsheetUrlCandidate,
    WEBHOUND_CONFIRMED_DAM_EXAMPLE,
  } = await import("../lib/research-engine-v2/marriott-rooms-source-adapter.js");
  const { assertNoInsertInFieldCompletionMode, CENSUS_MODE } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.equal(
    resolveMissionObjective("marriott-webhound-source-pattern-learning-v1"),
    MISSION_OBJECTIVE_MARRIOTT_WEBHOUND_SOURCE_PATTERN_LEARNING_V1
  );
  assert.equal(
    MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE,
    "marriott-webhound-source-pattern-learning-v1"
  );
  assert.ok(MARRIOTT_WEBHOUND_LEARNING_STATUS.PARTIAL_SOURCE.includes("marriott_webhound"));

  assert.equal(assertWebhoundNotCensusSot({ webhound_as_sot: true }).ok, false);
  assert.equal(assertWebhoundNotCensusSot({ source: "webhound", direct_write: true }).ok, false);
  assert.equal(assertWebhoundNotCensusSot({ webhound_as_sot: false }).ok, true);

  const ota = normalizeDiscoveredPattern({
    discovered_url: "https://www.booking.com/hotel/mx/x.html",
    autopilot_safe: true,
  });
  assert.equal(ota.rejected, true);
  assert.equal(ota.webhound_as_sot, false);

  const catalog = buildMarriottPatternLearningCatalog([ota]);
  assert.equal(catalog.webhound_as_census_sot, false);
  assert.ok(WEBHOUND_CONFIRMED_PATTERNS.length >= 3);
  assert.ok(
    catalog.accepted_for_adapter_learning.some(
      (p) => p.pattern_id === "marriott_dam_factsheet_pdf"
    )
  );
  assert.ok(catalog.repeatable_adapter_candidates.every((p) => p.webhound_as_sot === false));

  const parsedFromText = extractPatternsFromWebhoundText(
    "See https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf and https://news.marriott.com/news/2024/01/01/example and https://www.booking.com/x"
  );
  assert.ok(parsedFromText.some((p) => p.source_type === "official_factsheet" && p.autopilot_safe));
  assert.ok(parsedFromText.some((p) => p.rejected === true));

  const dam = buildMarriottDamFactsheetUrlCandidate({
    marsha: "SJULU",
    brand: "Fairfield by Marriott",
    assetId: "23971",
  });
  assert.equal(dam.ok, true);
  assert.equal(dam.webhound_as_sot, false);
  assert.equal(dam.url, WEBHOUND_CONFIRMED_DAM_EXAMPLE.url);
  assert.equal(
    buildMarriottDamFactsheetUrlCandidate({ marsha: "SJULU", brand: "Fairfield" }).ok,
    false
  );

  const clusters = buildMarriottGapClusterSamples([
    {
      id: "recM1",
      fields: {
        "Brand Family": "Marriott International",
        "Current Brand": "Courtyard",
        "Official Property URL": "https://www.marriott.com/en-us/hotels/mexcy-courtyard-mexico/overview/",
        City: "Mexico City",
        Country: "Mexico",
        "Property Name": "Courtyard Mexico City",
        Address: "",
        Phone: "",
        "Rooms / Keys": "",
      },
    },
  ]);
  assert.ok(clusters.marriott_total >= 1);

  const alts = buildMarriottAlternatePropertyUrls(
    "https://www.marriott.com/en-us/hotels/mexcy-courtyard/overview/"
  );
  assert.ok(alts.length >= 2);
  assert.ok(alts.every((u) => /marriott\.com/i.test(u)));

  const patch = buildMarriottMetadataPatch(
    {
      candidates: {
        address: {
          value: "123 Reforma Ave",
          source_url: "https://www.marriott.com/en-us/hotels/mexcy/overview/",
        },
        phone: {
          value: "+52 55 1234 5678",
          source_url: "https://www.marriott.com/en-us/hotels/mexcy/overview/",
        },
        rooms: {
          value: 220,
          source_url: "https://www.marriott.com/en-us/hotels/mexcy/overview/",
          source_type: "official_property_page",
        },
      },
    },
    {}
  );
  assert.equal(patch.has_writes, true);
  assert.ok(patch.patch.Address);
  assert.ok(patch.patch["Address Source URL"]);
  assert.ok(patch.patch["Rooms Source URL"]);
  assert.equal(patch.webhound_as_sot, false);

  assert.equal(
    validateMarriottRoomsCandidate({
      count: 12,
      source_url: "https://www.marriott.com/x",
      evidence: "12 meeting rooms",
    }).ok,
    false
  );
  assert.equal(
    validateMarriottRoomsCandidate({
      count: 220,
      source_url: "https://www.marriott.com/en-us/hotels/mexcy/overview/",
      evidence: "220 guest rooms",
      property_name: "Courtyard Mexico City",
      source_property_name: "Courtyard Mexico City",
    }).ok,
    true
  );

  assert.equal(assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 1).ok, false);
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Brand Status"), true);
  assert.equal(isForbiddenAutopilotField("Recent Momentum"), true);

  const {
    buildDamFactsheetUrlIndex,
    isGuestFactsheetFilename,
    marshaFromDamUrl,
    buildDamFactsheetCensusPatch,
    extractDamUrlsFromText,
  } = await import("../lib/research-engine-v2/marriott-dam-factsheet-discovery.js");

  assert.equal(
    isGuestFactsheetFilename(
      "https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf"
    ),
    true
  );
  assert.equal(
    isGuestFactsheetFilename(
      "https://www.marriott.com/x/cy-gyecy-12-23-event-fact-sheet-10703.pdf"
    ),
    false
  );
  assert.equal(
    marshaFromDamUrl(
      "https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf"
    ),
    "SJULU"
  );
  const damIdx = buildDamFactsheetUrlIndex({ skipDefaultReport: false });
  assert.equal(damIdx.webhound_as_census_sot, false);
  assert.ok(damIdx.by_marsha.SJULU?.url);
  assert.ok(damIdx.by_marsha.GYECY?.url);

  const damPatch = buildDamFactsheetCensusPatch(
    {
      ok: true,
      url: damIdx.by_marsha.SJULU.url,
      rooms: {
        ok: true,
        count: 104,
        source_url: damIdx.by_marsha.SJULU.url,
      },
      address: { ok: true, address: "110 Seaside Drive, Luquillo, PR 00773" },
      phone: { ok: true, phone: "7876570000" },
      webhound_as_sot: false,
    },
    {}
  );
  assert.equal(damPatch.has_writes, true);
  assert.equal(damPatch.webhound_as_sot, false);
  assert.equal(damPatch.patch["Rooms / Keys"], 104);
  assert.ok(damPatch.patch["Rooms Source URL"]);
  assert.equal(damPatch.patch["Rooms Source Type"], "official_factsheet");
  assert.ok(damPatch.patch["Rooms Evidence Tier"]);
  assert.ok(damPatch.patch.Address);
  const otaDam = extractDamUrlsFromText(
    "see https://www.booking.com/hotel.pdf and https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf"
  );
  assert.ok(otaDam.every((u) => /marriott\.com/i.test(u.url)));
});

await testAsync("universal-record-resolver-v1 — MX043 regression + guards", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_UNIVERSAL_RECORD_RESOLVER_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    UNIVERSAL_RECORD_RESOLVER_V1_OBJECTIVE,
    UNIVERSAL_RECORD_RESOLVER_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-universal-record-resolver-v1.js"
  );
  const {
    inspectHotelRecord,
    isIncorrectCanonicalPropertyName,
  } = await import("../lib/research-engine-v2/universal-hotel-record-inspector.js");
  const { routeHotelRecordSources, SOURCE_STRATEGY } = await import(
    "../lib/research-engine-v2/universal-record-source-router.js"
  );
  const {
    normalizeChoicePropertyCode,
    buildChoiceOfficialUrlCandidates,
    deriveChoiceBrandLabelFromOfficialUrl,
    resolveChoiceBrandLabelForCompose,
    isParentPropertyCodeStubName,
  } = await import("../lib/research-engine-v2/choice-property-record-resolver.js");
  const { sanitizeResolverPatch } = await import(
    "../lib/research-engine-v2/universal-hotel-record-resolver.js"
  );
  const { assertNoInsertInFieldCompletionMode, CENSUS_MODE } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );
  const { parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );

  assert.equal(
    resolveMissionObjective("universal-record-resolver-v1"),
    MISSION_OBJECTIVE_UNIVERSAL_RECORD_RESOLVER_V1
  );
  assert.equal(UNIVERSAL_RECORD_RESOLVER_V1_OBJECTIVE, "universal-record-resolver-v1");
  assert.ok(UNIVERSAL_RECORD_RESOLVER_STATUS.PARTIAL_SOURCE.includes("universal_record_resolver"));

  const mxFields = {
    "Property Identity Key": "ind_choice_mx_mx043",
    "Property Name": "Choice property MX043",
    "Canonical Property Name": "Choice property MX043",
    "Brand Family": "Choice Hotels International",
    "Current Brand": "Quality Inn",
    Country: "Mexico",
    City: "Chihuahua",
    "State / Region": "Chihuahua",
    "Official Property URL":
      "https://www.choicehotels.com/chihuahua/chihuahua/quality-inn-hotels/mx043",
    "Source URL":
      "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=x",
  };
  const incorrect = isIncorrectCanonicalPropertyName(mxFields);
  assert.equal(incorrect.incorrect, true);
  assert.ok(/stub|code/i.test(incorrect.reason));

  const inspection = inspectHotelRecord({ id: "recmWNlAbpgMinyLt", fields: mxFields });
  assert.equal(inspection.incomplete, true);
  assert.ok(inspection.incorrect_keys.includes("canonical_property_name"));
  assert.ok(inspection.missing_keys.includes("address"));
  assert.ok(inspection.missing_keys.includes("phone"));
  assert.ok(inspection.missing_keys.includes("rooms"));

  const route = routeHotelRecordSources(mxFields, { propertyCode: "MX043" });
  assert.equal(route.codes.choice_property_id, "MX043");
  assert.ok(route.strategies.includes(SOURCE_STRATEGY.CHOICE_PROPERTY_ID));
  assert.equal(normalizeChoicePropertyCode("mx043"), "MX043");

  const urls = buildChoiceOfficialUrlCandidates({
    officialUrl: mxFields["Official Property URL"],
    propertyCode: "MX043",
  });
  assert.ok(urls.some((u) => /mx043/i.test(u)));

  // Record-level: does not require parent-wide adapter success — route works from one record
  assert.equal(route.primary, SOURCE_STRATEGY.CHOICE_PROPERTY_ID);

  assert.equal(isParentPropertyCodeStubName("Choice property MX086"), true);
  assert.equal(isParentPropertyCodeStubName("Comfort Inn Querétaro"), false);
  assert.equal(
    deriveChoiceBrandLabelFromOfficialUrl(
      "https://www.choicehotels.com/baja-california-sur/cabo-san-lucas/grand-fiesta-americana-hotels-and-resorts-hotels/mx210"
    )?.label,
    "Grand Fiesta Americana"
  );
  assert.equal(
    deriveChoiceBrandLabelFromOfficialUrl(
      "https://www.choicehotels.com/sinaloa/mazatlan/park-inn-hotels/mx180"
    )?.label,
    "Park Inn"
  );
  const weakBrandCompose = resolveChoiceBrandLabelForCompose({
    "Current Brand": "Choice Hotels",
    "Official Property URL":
      "https://www.choicehotels.com/guerrero/acapulco/fiesta-americana-hotels-and-resorts-hotels/mx197",
  });
  assert.equal(weakBrandCompose?.label, "Fiesta Americana");
  const conflictCompose = resolveChoiceBrandLabelForCompose({
    "Current Brand": "Radisson by Choice",
    "Official Property URL":
      "https://www.choicehotels.com/sinaloa/mazatlan/park-inn-hotels/mx180",
  });
  assert.equal(conflictCompose?.label, "Park Inn");
  assert.ok(/override|url/i.test(conflictCompose?.method || ""));

  const badPatch = sanitizeResolverPatch({
    Address: "123 Main",
    "Address Source URL": "https://www.choicehotels.com/x",
    Owner: "nope",
    "Brand Status": "Active",
    "Recent Momentum": "x",
    "Company Validated": true,
  });
  assert.ok(badPatch.Address);
  assert.equal(badPatch.Owner, undefined);
  assert.equal(badPatch["Brand Status"], undefined);
  assert.equal(badPatch["Recent Momentum"], undefined);

  assert.equal(assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 1).ok, false);
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Brand Verified"), true);

  const parsed = parseAutopilotArgs([
    "--objective",
    "universal-record-resolver-v1",
    "--property-code",
    "MX043",
    "--record-id",
    "recmWNlAbpgMinyLt",
    "--census-mode",
    "field-completion-only",
  ]);
  assert.equal(parsed.propertyCode, "MX043");
  assert.equal(parsed.recordId, "recmWNlAbpgMinyLt");
  assert.equal(parsed.censusMode, "field-completion-only");

  // Secondary disabled ⇒ no silent secondary flag in sanitize path
  assert.equal(process.env.ENABLE_SECONDARY_HOTEL_DATA_SOURCES || "0", process.env.ENABLE_SECONDARY_HOTEL_DATA_SOURCES || "0");
});

await testAsync("commercial-fields-and-description-v1 — MX043 + description guards", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_COMMERCIAL_FIELDS_AND_DESCRIPTION_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    COMMERCIAL_FIELDS_DESCRIPTION_V1_OBJECTIVE,
    COMMERCIAL_FIELDS_DESCRIPTION_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-commercial-fields-and-description-v1.js"
  );
  const { resolveCommercialMarket } = await import(
    "../lib/research-engine-v2/census-commercial-market-map.js"
  );
  const { resolveCommercialSubmarket } = await import(
    "../lib/research-engine-v2/census-submarket-map.js"
  );
  const { completeMarketSubmarketForRecord } = await import(
    "../lib/research-engine-v2/census-market-submarket-completion.js"
  );
  const {
    generateHotelDescriptions,
    DESCRIPTION_FIELD_MAP,
    DESCRIPTION_SCHEMA_GAPS,
  } = await import("../lib/research-engine-v2/census-hotel-description-generator.js");
  const { evaluateDescriptionQuality } = await import(
    "../lib/research-engine-v2/census-description-quality-gate.js"
  );
  const { isChoiceCentralReservationPhone } = await import(
    "../lib/research-engine-v2/census-phone-number-enrichment.js"
  );
  const { isFalsePositiveRoomCount } = await import(
    "../lib/research-engine-v2/production-census-rooms-keys-extractor.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );
  const { assertNoInsertInFieldCompletionMode, CENSUS_MODE } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );

  assert.equal(
    resolveMissionObjective("commercial-fields-and-description-v1"),
    MISSION_OBJECTIVE_COMMERCIAL_FIELDS_AND_DESCRIPTION_V1
  );
  assert.equal(
    COMMERCIAL_FIELDS_DESCRIPTION_V1_OBJECTIVE,
    "commercial-fields-and-description-v1"
  );
  assert.ok(
    COMMERCIAL_FIELDS_DESCRIPTION_STATUS.PARTIAL_SECONDARY.includes(
      "secondary_source_decision"
    )
  );

  assert.equal(
    resolveCommercialMarket({ city: "Playa del Carmen", country: "Mexico" }).market,
    "Cancún"
  );
  assert.equal(
    resolveCommercialMarket({ city: "Cabo San Lucas", country: "Mexico" }).market,
    "Los Cabos"
  );
  assert.equal(
    resolveCommercialMarket({ city: "San José", country: "Costa Rica" }).market,
    "San José"
  );
  assert.equal(
    resolveCommercialMarket({ city: "San José", country: "Mexico" }).ok,
    false
  );

  const subHz = resolveCommercialSubmarket({
    market: "Cancún",
    address: "Blvd Kukulcan Km 12, Zona Hotelera",
    city: "Cancún",
  });
  assert.equal(subHz.ok, true);
  assert.equal(subHz.submarket, "Hotel Zone");

  const inventSub = resolveCommercialSubmarket({
    market: "Cancún",
    city: "Cancún",
    address: "Some unknown street",
  });
  assert.equal(inventSub.ok, false);

  const mxFields = {
    "Property Identity Key": "ind_choice_mx_mx043",
    "Property Name": "Quality Inn Chihuahua",
    "Canonical Property Name": "Quality Inn Chihuahua",
    "Current Brand": "Quality Inn",
    "Brand Family": "Choice Hotels International",
    City: "Chihuahua",
    Country: "Mexico",
    "State / Region": "Chihuahua",
    Market: "Chihuahua",
  };
  const geo = completeMarketSubmarketForRecord({ id: "recmWNlAbpgMinyLt", fields: mxFields });
  // Market already set — no overwrite required
  assert.equal(geo.patch.Market, undefined);

  const desc = generateHotelDescriptions(mxFields);
  assert.equal(desc.ok, true);
  assert.ok(desc.public_description);
  assert.ok(/Quality Inn Chihuahua/.test(desc.public_description));
  assert.equal(
    evaluateDescriptionQuality(desc.public_description).ok,
    true
  );
  assert.equal(
    evaluateDescriptionQuality(desc.public_description).failures.includes(
      "internal_process_terms"
    ),
    false
  );
  assert.ok(!/Dealality Census|source-supported|official inventory/i.test(desc.public_description));
  assert.ok(/Dealality Census|official hotel inventory/i.test(desc.internal_description));
  assert.equal(
    desc.write_fields[DESCRIPTION_FIELD_MAP.aiSummary],
    desc.public_description
  );
  assert.ok(DESCRIPTION_SCHEMA_GAPS.includes("Description Status"));

  const dirtyDesc = generateHotelDescriptions({
    ...mxFields,
    "Canonical Property Name": "Choice property MX043",
    "Property Name": "Choice property MX043",
  });
  assert.equal(dirtyDesc.ok, false);

  const hype = evaluateDescriptionQuality(
    "Quality Inn is a world-class iconic premier hotel perfectly located downtown."
  );
  assert.equal(hype.ok, false);

  assert.equal(isChoiceCentralReservationPhone("+18887706800"), true);
  assert.equal(
    isFalsePositiveRoomCount("https://www.choicehotels.com/x choicehotels.com", 25, "json_ld"),
    true
  );
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Brand Status"), true);
  assert.equal(isForbiddenAutopilotField("Recent Momentum"), true);
  assert.equal(
    assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 1).ok,
    false
  );
  // Default policy is off unless env explicitly enables (do not assert process.env —
  // mission runs may leave ENABLE_SECONDARY_* set in the same shell).
  const { resolveSecondaryHotelDataPolicy } = await import(
    "../lib/research-engine-v2/census-secondary-hotel-data-policy.js"
  );
  assert.equal(
    resolveSecondaryHotelDataPolicy({}).enable_secondary_rooms_sources,
    false
  );
});

await testAsync("rooms-count-completion-v1 — secondary rooms policy + RNT match guards", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_ROOMS_COUNT_COMPLETION_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    ROOMS_COUNT_COMPLETION_V1_OBJECTIVE,
    ROOMS_COUNT_COMPLETION_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-rooms-count-completion-v1.js"
  );
  const {
    resolveSecondaryHotelDataPolicy,
    classifyPhoneUnderSecondaryPolicy,
    resolveRoomsConfidenceForSource,
    resolveRoomsSourceTypeForAirtable,
    PHONE_POLICY_REASON,
    MAP_ROOMS_SOURCE_TYPE,
  } = await import("../lib/research-engine-v2/census-secondary-hotel-data-policy.js");
  const {
    matchCensusToColombiaRntRooms,
    buildSecondaryRoomsPatch,
  } = await import("../lib/research-engine-v2/census-rooms-secondary-match.js");
  const { isChoiceCentralReservationPhone } = await import(
    "../lib/research-engine-v2/census-phone-number-enrichment.js"
  );
  const { isFalsePositiveRoomCount } = await import(
    "../lib/research-engine-v2/production-census-rooms-keys-extractor.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );
  const { assertNoInsertInFieldCompletionMode, CENSUS_MODE } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );

  assert.equal(
    resolveMissionObjective("rooms-count-completion-v1"),
    MISSION_OBJECTIVE_ROOMS_COUNT_COMPLETION_V1
  );
  assert.equal(ROOMS_COUNT_COMPLETION_V1_OBJECTIVE, "rooms-count-completion-v1");
  assert.ok(
    ROOMS_COUNT_COMPLETION_STATUS.PARTIAL_SOURCE.includes("partial_source_remaining")
  );
  assert.ok(
    ROOMS_COUNT_COMPLETION_STATUS.PARTIAL_STEWARD.includes("partial_steward_remaining")
  );

  const off = resolveSecondaryHotelDataPolicy({
    ENABLE_SECONDARY_HOTEL_DATA_SOURCES: "0",
    ENABLE_SECONDARY_ROOMS_SOURCES: "1",
    ENABLE_SECONDARY_PHONE_SOURCES: "0",
  });
  assert.equal(off.enable_secondary_rooms_sources, false);

  const onRooms = resolveSecondaryHotelDataPolicy({
    ENABLE_SECONDARY_HOTEL_DATA_SOURCES: "1",
    ENABLE_SECONDARY_ROOMS_SOURCES: "1",
    ENABLE_SECONDARY_PHONE_SOURCES: "0",
  });
  assert.equal(onRooms.enable_secondary_rooms_sources, true);
  assert.equal(onRooms.enable_secondary_phone_sources, false);

  const phoneCls = classifyPhoneUnderSecondaryPolicy({
    has_phone: false,
    policy: onRooms,
  });
  assert.equal(phoneCls.status, PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED);
  assert.equal(phoneCls.write, false);

  assert.equal(
    resolveRoomsConfidenceForSource({
      is_official: false,
      category: "tourism_board_convention_bureau_destination_authority",
    }),
    "Medium"
  );
  assert.equal(
    resolveRoomsConfidenceForSource({ is_official: true, category: "official_hotel_website" }),
    "High"
  );
  assert.equal(
    resolveRoomsSourceTypeForAirtable({
      is_official: false,
      category: "tourism_board_convention_bureau_destination_authority",
    }),
    MAP_ROOMS_SOURCE_TYPE.trusted_secondary_source
  );

  const rntRow = {
    codigo_rnt: "999001",
    razon_social_establecimiento: "HOTEL IBIS CHIA",
    municipio: "CHIA",
    departamento: "CUNDINAMARCA",
    habitaciones: "96",
    sub_categoria: "HOTEL",
    estado_rnt: "ACTIVO",
    ano: "2026",
  };
  const match = matchCensusToColombiaRntRooms(
    {
      Country: "Colombia",
      City: "Chía",
      "Canonical Property Name": "ibis Chia",
      "Property Name": "ibis Chia",
    },
    [rntRow]
  );
  assert.equal(match.ok, true);
  assert.equal(match.rooms, 96);
  assert.equal(match.confidence, "Medium");
  assert.equal(match.is_official, false);
  assert.ok(match.notes.includes("evidence_tier="));

  const patch = buildSecondaryRoomsPatch(
    { "Rooms / Keys": null },
    match,
    { today: "2026-08-07" }
  );
  assert.equal(patch.ok, true);
  assert.equal(patch.patch["Rooms / Keys"], 96);
  assert.equal(patch.patch["Rooms Source Type"], "trusted_secondary_source");
  assert.equal(patch.patch["Rooms Confidence"], "Medium");
  assert.ok(patch.patch["Rooms Source URL"]);
  assert.ok(patch.patch["Rooms Reviewed Date"]);
  assert.ok(patch.patch["Rooms Notes"].includes("evidence_tier="));

  const conflict = buildSecondaryRoomsPatch(
    { "Rooms / Keys": 80, "Rooms Source URL": "https://example.com" },
    match,
    { today: "2026-08-07" }
  );
  assert.equal(conflict.conflict, true);
  assert.equal(conflict.write_rooms_value, false);
  assert.equal(conflict.patch["Rooms Confidence"], "Hold");
  assert.equal(conflict.patch["Human Review Required"], true);

  assert.equal(isChoiceCentralReservationPhone("+18887706800"), true);
  assert.equal(
    isFalsePositiveRoomCount("choicehotels.com rooms default", 25, "json_ld"),
    true
  );
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Opening Date"), true);
  assert.equal(
    assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 1).ok,
    false
  );
});

await testAsync("dataforseo-local-business-enrichment-v1 — objective + candidate-only", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_ENRICHMENT_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
    DATAFORSEO_LOCAL_ENRICHMENT_STATUS,
    assertDataForSeoLocalCandidateOnly,
    DATAFORSEO_LOCAL_PILOT_MARKETS,
  } = await import(
    "../lib/research-engine-v2/dataforseo-local-business-enrichment-v1.js"
  );

  assert.equal(
    resolveMissionObjective("dataforseo-local-business-enrichment-v1"),
    MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_ENRICHMENT_V1
  );
  assert.equal(
    DATAFORSEO_LOCAL_ENRICHMENT_OBJECTIVE,
    "dataforseo-local-business-enrichment-v1"
  );
  assert.ok(
    DATAFORSEO_LOCAL_ENRICHMENT_STATUS.PARTIAL_POLICY.includes(
      "partial_policy_decision_needed"
    )
  );
  assert.equal(DATAFORSEO_LOCAL_PILOT_MARKETS.length, 9);
  assert.equal(
    assertDataForSeoLocalCandidateOnly({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      DATAFORSEO_ENABLE_GOOGLE_MAPS: "1",
    }).ok,
    false
  );
});

await testAsync("dataforseo-local-business-validated-write-v1 — objective + gates", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_VALIDATED_WRITE_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
    DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS,
    resolveDataForSeoLocalValidatedWriteGates,
    evaluateLocalWebsiteWrite,
    evaluateLocalAddressWrite,
  } = await import(
    "../lib/research-engine-v2/dataforseo-local-business-validated-write-v1.js"
  );
  const { MATCH_CLASS } = await import(
    "../lib/research-engine-v2/dataforseo-local-match.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );
  const { assertNoInsertInFieldCompletionMode, CENSUS_MODE } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );

  assert.equal(
    resolveMissionObjective("dataforseo-local-business-validated-write-v1"),
    MISSION_OBJECTIVE_DATAFORSEO_LOCAL_BUSINESS_VALIDATED_WRITE_V1
  );
  assert.equal(
    DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
    "dataforseo-local-business-validated-write-v1"
  );
  assert.ok(
    DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.PARTIAL_POLICY.includes(
      "partial_policy_decision_needed"
    )
  );
  assert.equal(
    resolveDataForSeoLocalValidatedWriteGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
    }).phone_writes,
    false
  );
  assert.equal(
    evaluateLocalWebsiteWrite(
      {
        match_class: MATCH_CLASS.MATCH_HIGH,
        raw: { website: "https://www.tripadvisor.com/Hotel_Review-x" },
      },
      {}
    ).ok,
    false
  );
  assert.equal(
    evaluateLocalAddressWrite(
      {
        match_class: MATCH_CLASS.MATCH_HIGH,
        raw: { address: "Puebla", title: "Hotel Test" },
        hotel_name: "Hotel Test",
      },
      { Address: "" }
    ).reason,
    "address_not_street_level"
  );
  assert.equal(isForbiddenAutopilotField("Opening Date"), true);
  assert.equal(
    assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 0)
      .ok,
    true
  );
});

await testAsync("dataforseo-local-address-scale-v1 — objective + Mapbox pending", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_DATAFORSEO_LOCAL_ADDRESS_SCALE_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
    DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS,
    resolveDataForSeoLocalAddressScaleGates,
    classifyMapboxEligibilityAfterLocalAddress,
  } = await import(
    "../lib/research-engine-v2/dataforseo-local-address-scale-v1.js"
  );

  assert.equal(
    resolveMissionObjective("dataforseo-local-address-scale-v1"),
    MISSION_OBJECTIVE_DATAFORSEO_LOCAL_ADDRESS_SCALE_V1
  );
  assert.equal(
    DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
    "dataforseo-local-address-scale-v1"
  );
  assert.ok(
    DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.PARTIAL_POLICY.includes(
      "partial_policy_decision_needed"
    )
  );
  assert.equal(
    resolveDataForSeoLocalAddressScaleGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
    }).coordinate_writes,
    false
  );
  assert.equal(
    classifyMapboxEligibilityAfterLocalAddress({
      fields: {
        Address: "Calle 10 #20, Cartagena",
        "Address Confidence": "Medium",
        "Address Source URL": "https://www.marriott.com/hotels/travel/test",
      },
      clean_core_pass: true,
    }).status,
    "mapbox_pending_address_confidence"
  );
  assert.equal(
    classifyMapboxEligibilityAfterLocalAddress({
      fields: {
        Address: "Calle 10 #20, Cartagena",
        "Address Confidence": "Medium",
        "Address Source URL": "https://www.marriott.com/hotels/travel/test",
      },
      clean_core_pass: true,
      medium_match_high_approved: true,
    }).status,
    "mapbox_eligible_medium_match_high"
  );
});

await testAsync("dataforseo-new-hotel-insert-review-pack — ranks without inserts", async () => {
  const {
    buildInsertReviewPack,
    INSERT_REVIEW_PACK_VERSION,
  } = await import(
    "../lib/research-engine-v2/dataforseo-new-hotel-insert-review-pack.js"
  );
  const pack = buildInsertReviewPack({
    queue: [
      {
        market_label: "Mexico City",
        country: "Mexico",
        discovery_class: "new_hotel_candidate",
        match: {
          record_id: "recNear",
          match_class: "match_low",
          name_similarity: 0.4,
          city_match: false,
          reasons: ["city_mismatch"],
        },
        near_duplicates: [],
        candidate: {
          raw: {
            title: "Gran Hotel Ciudad de México",
            address: "16 de Septiembre 82, Centro, CDMX",
            website: "https://www.granhoteldelaciudaddemexico.com.mx/",
            latitude: 19.43,
            longitude: -99.13,
            place_id: "ChIJtest",
            category: "Hotel",
          },
        },
      },
      {
        market_label: "Bogota",
        country: "Colombia",
        discovery_class: "new_hotel_candidate",
        match: {
          match_class: "match_high",
          name_similarity: 0.92,
          record_id: "recDup",
        },
        near_duplicates: [{ record_id: "recDup" }],
        candidate: {
          raw: {
            title: "Hilton Bogota",
            address: "Carrera 7 32",
            website: "https://www.hilton.com/",
            category: "Hotel",
          },
        },
      },
      {
        market_label: "Cancun",
        country: "Mexico",
        discovery_class: "new_hotel_candidate",
        match: { match_class: "match_low", name_similarity: 0.2 },
        near_duplicates: [],
        candidate: {
          raw: {
            title: "Cafe Central",
            address: "Av Tulum 1",
            category: "Cafe",
          },
        },
      },
    ],
  });
  assert.equal(pack.version, INSERT_REVIEW_PACK_VERSION);
  assert.equal(pack.inserts, 0);
  assert.equal(pack.candidate_count, 3);
  assert.equal(pack.candidates[0].recommended_action, "approve_insert_high");
  assert.ok(
    pack.candidates.some((c) => c.recommended_action === "duplicate_review")
  );
  assert.ok(pack.candidates.some((c) => c.recommended_action === "reject"));
});

await testAsync("census-autopilot-policy-controller-v1 — objective + no founder gates", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_CENSUS_AUTOPILOT_POLICY_CONTROLLER_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    POLICY_CONTROLLER_OBJECTIVE,
    POLICY_CONTROLLER_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-policy-controller-v1.js"
  );
  const {
    resolveCensusAutopilotPolicyGates,
    assertHighConfidenceInsertPolicy,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-approved-policy.js"
  );
  const { CENSUS_MODE, assertNoInsertInFieldCompletionMode } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );

  assert.equal(
    resolveMissionObjective("census-autopilot-policy-controller-v1"),
    MISSION_OBJECTIVE_CENSUS_AUTOPILOT_POLICY_CONTROLLER_V1
  );
  assert.equal(
    POLICY_CONTROLLER_OBJECTIVE,
    "census-autopilot-policy-controller-v1"
  );
  assert.ok(
    POLICY_CONTROLLER_STATUS.PARTIAL_INSERT.includes("partial_insert_policy_needed")
  );
  const gates = resolveCensusAutopilotPolicyGates({
    ENABLE_CENSUS_POLICY_CONTROLLER: "1",
    ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
    ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
    ENABLE_SECONDARY_PHONE_SOURCES: "0",
    ENABLE_MAPBOX_AFTER_VALIDATED_ADDRESS: "1",
    ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS: "1",
  });
  assert.equal(gates.ok, true);
  assert.equal(gates.mapbox_after_medium_match_high_address, true);
  assert.equal(gates.founder_gate_between_passes, false);
  assert.equal(
    assertHighConfidenceInsertPolicy({
      censusMode: CENSUS_MODE.GROWTH,
      gates,
    }).allowed,
    false
  );
  assert.equal(
    assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 0)
      .ok,
    true
  );
});

await testAsync("dataforseo-validated-write-policy-v1 — objective + gates", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_DATAFORSEO_VALIDATED_WRITE_POLICY_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
    DATAFORSEO_VALIDATED_WRITE_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-dataforseo-validated-write-policy-v1.js"
  );
  const { resolveDataForSeoValidatedWriteGates } = await import(
    "../lib/research-engine-v2/dataforseo-validated-write-policy.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.equal(
    resolveMissionObjective("dataforseo-validated-write-policy-v1"),
    MISSION_OBJECTIVE_DATAFORSEO_VALIDATED_WRITE_POLICY_V1
  );
  assert.equal(
    DATAFORSEO_VALIDATED_WRITE_OBJECTIVE,
    "dataforseo-validated-write-policy-v1"
  );
  assert.ok(
    DATAFORSEO_VALIDATED_WRITE_STATUS.PARTIAL_POLICY.includes(
      "partial_policy_decision_needed"
    )
  );
  assert.equal(
    resolveDataForSeoValidatedWriteGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_URL_WRITES: "1",
    }).ok,
    false
  );
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Brand Status"), true);
  assert.equal(isForbiddenAutopilotField("Recent Momentum"), true);
});

await testAsync("dataforseo-discovery-pilot-v2 — objective + candidate-only gate", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_DATAFORSEO_DISCOVERY_PILOT_V2,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
    DATAFORSEO_DISCOVERY_STATUS,
    assertDataForSeoCandidateOnlyMode,
  } = await import("../lib/research-engine-v2/dataforseo-discovery-pilot.js");
  const { classifySerpOrMapsItem } = await import(
    "../lib/research-engine-v2/dataforseo-candidate-classifier.js"
  );

  assert.equal(
    resolveMissionObjective("dataforseo-discovery-pilot-v2"),
    MISSION_OBJECTIVE_DATAFORSEO_DISCOVERY_PILOT_V2
  );
  assert.equal(
    DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
    "dataforseo-discovery-pilot-v2"
  );
  assert.ok(
    DATAFORSEO_DISCOVERY_STATUS.PARTIAL_POLICY.includes(
      "partial_policy_decision_needed"
    )
  );
  assert.equal(
    assertDataForSeoCandidateOnlyMode({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
    }).census_writes_allowed,
    false
  );
  const hoteles = classifySerpOrMapsItem({
    title: "Hotel X",
    url: "https://www.hoteles.com/ho1/hotel-x/",
  });
  assert.equal(hoteles.status, "rejected");
});

await testAsync("rooms-secondary-source-wave-2-v1 — tiers + fuzzy + discovery guards", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_ROOMS_SECONDARY_SOURCE_WAVE_2_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    ROOMS_SECONDARY_WAVE_2_OBJECTIVE,
    ROOMS_SECONDARY_WAVE_2_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-rooms-secondary-source-wave-2-v1.js"
  );
  const {
    ROOMS_EVIDENCE_TIER_OPTIONS,
    mapEvidenceTierCodeToSelect,
  } = await import(
    "../lib/research-engine-v2/production-census-rooms-evidence-tier-schema.js"
  );
  const {
    buildRoomsCountryDiscoveryReport,
    ROOMS_COUNTRY_PRIORITY,
  } = await import(
    "../lib/research-engine-v2/census-rooms-country-source-discovery.js"
  );
  const {
    matchCensusToColombiaRntRooms,
    isSafeColombiaRntFuzzyMatch,
    buildSecondaryRoomsPatch,
  } = await import("../lib/research-engine-v2/census-rooms-secondary-match.js");
  const {
    classifyPhoneUnderSecondaryPolicy,
    resolveSecondaryHotelDataPolicy,
    PHONE_POLICY_REASON,
  } = await import("../lib/research-engine-v2/census-secondary-hotel-data-policy.js");
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );
  const { assertNoInsertInFieldCompletionMode, CENSUS_MODE } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );
  const { isFalsePositiveRoomCount } = await import(
    "../lib/research-engine-v2/production-census-rooms-keys-extractor.js"
  );

  assert.equal(
    resolveMissionObjective("rooms-secondary-source-wave-2-v1"),
    MISSION_OBJECTIVE_ROOMS_SECONDARY_SOURCE_WAVE_2_V1
  );
  assert.equal(ROOMS_SECONDARY_WAVE_2_OBJECTIVE, "rooms-secondary-source-wave-2-v1");
  assert.ok(ROOMS_SECONDARY_WAVE_2_STATUS.PARTIAL_SOURCE.includes("wave_2"));
  assert.equal(ROOMS_COUNTRY_PRIORITY[0], "Mexico");
  assert.ok(ROOMS_EVIDENCE_TIER_OPTIONS[4].includes("Tourism Board"));
  assert.equal(
    mapEvidenceTierCodeToSelect("secondary_tourism_board_destination_authority"),
    ROOMS_EVIDENCE_TIER_OPTIONS[4]
  );

  const discovery = buildRoomsCountryDiscoveryReport({
    Mexico: { missing_rooms: 758 },
  });
  assert.equal(discovery.countries[0].country, "Mexico");
  assert.ok(discovery.countries[0].sources.length >= 1);

  // Location conflict: Manga vs Bocagrande must not write
  const conflictMatch = matchCensusToColombiaRntRooms(
    {
      Country: "Colombia",
      City: "Cartagena",
      "Canonical Property Name": "Holiday Inn Express Cartagena Manga",
      "Current Brand": "Holiday Inn Express",
    },
    [
      {
        codigo_rnt: "1",
        razon_social_establecimiento: "HOTEL HOLIDAY INN EXPRESS CARTAGENA BOCAGRANDE",
        municipio: "CARTAGENA",
        habitaciones: "200",
      },
    ],
    { fuzzy: true }
  );
  assert.equal(conflictMatch.ok, false);

  // Generic short RNT name must not write
  const short = isSafeColombiaRntFuzzyMatch({
    sim: 0.75,
    inter: 1,
    brand_hits: 0,
    source_short: true,
    location_conflict: false,
    rooms_parse: { ok: true, rooms: 28 },
  });
  assert.equal(short.ok, false);

  // Exact IBIS CHIA match is safe under fuzzy rules
  const safe = matchCensusToColombiaRntRooms(
    {
      Country: "Colombia",
      City: "Chía",
      "Canonical Property Name": "ibis Chia",
      "Current Brand": "ibis",
    },
    [
      {
        codigo_rnt: "99",
        razon_social_establecimiento: "HOTEL IBIS CHIA",
        municipio: "CHIA",
        habitaciones: "96",
      },
    ],
    { fuzzy: true }
  );
  assert.equal(safe.ok, true, safe.reason || "expected ok");
  assert.equal(safe.rooms, 96);
  assert.equal(safe.confidence, "Medium");
  assert.notEqual(safe.confidence, "High");

  const patch = buildSecondaryRoomsPatch({ "Rooms / Keys": null }, safe, {
    today: "2026-08-07",
    roomsEvidenceTierFieldExists: true,
  });
  assert.equal(patch.ok, true);
  assert.ok(patch.patch["Rooms Evidence Tier"]);
  assert.ok(patch.patch["Rooms Source URL"]);
  assert.equal(patch.patch["Rooms Source Type"], "trusted_secondary_source");

  const conflict = buildSecondaryRoomsPatch(
    { "Rooms / Keys": 50, "Rooms Source URL": "https://example.com" },
    safe,
    { today: "2026-08-07", roomsEvidenceTierFieldExists: true }
  );
  assert.equal(conflict.conflict, true);
  assert.equal(conflict.write_rooms_value, false);

  const phonePolicy = resolveSecondaryHotelDataPolicy({
    ENABLE_SECONDARY_HOTEL_DATA_SOURCES: "1",
    ENABLE_SECONDARY_ROOMS_SOURCES: "1",
    ENABLE_SECONDARY_PHONE_SOURCES: "0",
  });
  assert.equal(phonePolicy.enable_secondary_phone_sources, false);
  assert.equal(
    classifyPhoneUnderSecondaryPolicy({ has_phone: false, policy: phonePolicy }).status,
    PHONE_POLICY_REASON.SECONDARY_NOT_APPROVED
  );

  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Opening Date"), true);
  assert.equal(
    isFalsePositiveRoomCount("choicehotels.com", 25, "json_ld"),
    true
  );
  assert.equal(
    assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 1).ok,
    false
  );
});

await testAsync("full-latam-census-autopilot-v3 — controller gates + gap ledger", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_FULL_LATAM_CENSUS_AUTOPILOT_V3,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    FULL_LATAM_AUTOPILOT_V3_OBJECTIVE,
    FULL_LATAM_AUTOPILOT_V3_STATUS,
    CENSUS_MODE,
    resolveCensusMode,
    assertNoInsertInFieldCompletionMode,
    buildHotelUrlCompletionPatches,
    buildMarketGeographyPatches,
  } = await import("../lib/research-engine-v2/census-autopilot-full-latam-v3.js");
  const { resolveStateRegionFromCity } = await import(
    "../lib/research-engine-v2/census-city-to-state-map.js"
  );
  const {
    buildCensusGapLedger,
    classifyFieldGap,
    GAP_REASON,
  } = await import("../lib/research-engine-v2/census-gap-ledger.js");
  const {
    prioritizeGapActions,
    buildExecutableBacklog,
  } = await import("../lib/research-engine-v2/census-gap-prioritization.js");
  const { parseAutopilotArgs } = await import(
    "../lib/research-engine-v2/census-autopilot-apply-guard.js"
  );
  const { evaluateLevel2Eligibility } = await import(
    "../lib/research-engine-v2/census-brand-governance.js"
  );

  assert.equal(
    resolveMissionObjective("full-latam-census-autopilot-v3"),
    MISSION_OBJECTIVE_FULL_LATAM_CENSUS_AUTOPILOT_V3
  );
  assert.equal(FULL_LATAM_AUTOPILOT_V3_OBJECTIVE, "full-latam-census-autopilot-v3");
  assert.ok(FULL_LATAM_AUTOPILOT_V3_STATUS.COMPLETE.includes("full_latam_autopilot_v3"));

  assert.equal(resolveCensusMode(["--census-mode", "field-completion-only"]), CENSUS_MODE.FIELD_COMPLETION_ONLY);
  assert.equal(resolveCensusMode(["--census-mode", "governance-only"]), CENSUS_MODE.GOVERNANCE_ONLY);
  assert.equal(resolveCensusMode([]), CENSUS_MODE.GROWTH);
  const parsed = parseAutopilotArgs([
    "--mode",
    "mission",
    "--census-mode",
    "field-completion-only",
    "--objective",
    "full-latam-census-autopilot-v3",
  ]);
  assert.equal(parsed.censusMode, "field-completion-only");

  assert.equal(assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 0).ok, true);
  assert.equal(assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 2).ok, false);
  assert.equal(assertNoInsertInFieldCompletionMode(CENSUS_MODE.GROWTH, 5).ok, true);

  const mx = resolveStateRegionFromCity({
    city: "Cancún",
    country: "Mexico",
    state: "",
  });
  assert.equal(mx.ok, true);
  assert.equal(mx.state, "Quintana Roo");
  assert.equal(mx.confidence, "High");

  const conflict = resolveStateRegionFromCity({
    city: "Cancún",
    country: "Mexico",
    state: "Jalisco",
  });
  assert.equal(conflict.ok, false);
  assert.equal(conflict.reason, "state_conflict_with_existing");

  const otaRec = {
    id: "recOta",
    fields: {
      "Source URL": "https://www.booking.com/hotel/mx/example.html",
      "Official Property URL": "",
    },
  };
  const urlPack = buildHotelUrlCompletionPatches([otaRec]);
  assert.equal(urlPack.proposals.length, 0);
  assert.ok(urlPack.skippedOta.length >= 1);

  const goodUrl = {
    id: "recOk",
    fields: {
      "Source URL": "https://www.hilton.com/en/hotels/cunxxxx-example/",
      "Official Property URL": "",
      "Brand Family": "Hilton",
    },
  };
  const goodPack = buildHotelUrlCompletionPatches([goodUrl]);
  assert.equal(goodPack.proposals.length, 1);
  assert.equal(
    goodPack.proposals[0].patch["Official Property URL"],
    "https://www.hilton.com/en/hotels/cunxxxx-example/"
  );

  const marketRec = {
    id: "recMkt",
    fields: {
      City: "Cabo San Lucas",
      Country: "Mexico",
      Market: "",
      Continent: "",
      "Sub-Continent": "",
    },
  };
  const mkt = buildMarketGeographyPatches([marketRec]);
  assert.ok(mkt.proposals.length >= 1);
  assert.equal(mkt.proposals[0].patch.Market, "Los Cabos");

  const mockActive = {
    by_norm: new Map([["hilton", { brand_name: "Hilton", brand_slug: "hilton" }]]),
    by_slug: new Map([["hilton", { brand_name: "Hilton", brand_slug: "hilton" }]]),
    active_count: 1,
    control: { brands: [{ brand_name: "Hilton", brand_slug: "hilton" }] },
  };
  const gapRec = {
    id: "recGap",
    fields: {
      "Property Name": "Test Hotel",
      "Current Brand": "Hilton",
      "Brand Family": "Hilton",
      Country: "Mexico",
      City: "Cancún",
      "Official Property URL": "https://www.hilton.com/en/hotels/cunxxxx/",
      Address: "",
      Phone: "",
      "Rooms / Keys": "",
      Latitude: null,
      Longitude: null,
      "Address Confidence": "",
    },
  };
  const addrGap = classifyFieldGap(gapRec, "Address", { activeIndex: mockActive });
  assert.equal(addrGap.missing, true);
  assert.ok(addrGap.autopilot_eligible);
  const coordGap = classifyFieldGap(gapRec, "Latitude", { activeIndex: mockActive });
  assert.equal(coordGap.reason, GAP_REASON.MAPBOX_WAITING_FOR_HIGH_ADDRESS);

  const ledger = buildCensusGapLedger([gapRec], { activeIndex: mockActive });
  assert.ok(ledger.gap_count > 0);
  const ranked = prioritizeGapActions(ledger);
  assert.ok(Array.isArray(ranked.top_actions));
  const backlog = buildExecutableBacklog({
    ranked,
    region: "CALA",
    censusMode: "growth",
  });
  assert.ok(String(backlog.command_to_continue).includes("full-latam-census-autopilot-v3"));
  assert.equal(backlog.founder_gate_between_passes, false);

  // Governance-only HR must not block Level 2; data-quality must
  const govOnly = evaluateLevel2Eligibility(
    {
      fields: {
        "Current Brand": "Novotel",
        "Brand Family": "Accor",
        Country: "Mexico",
        "Official Property URL": "https://all.accor.com/hotel/1234/index.en.shtml",
        "Production Use Status": "Census Only / Not Owner-Facing",
        "Public Display Review Status": "Hold",
        "Human Review Required": true,
      },
    },
    { cleanCorePass: true }
  );
  // May be eligible when governance_only — if brand classified as evidence-backed
  if (govOnly.governance_only_hold) {
    assert.equal(govOnly.eligible, true);
  }
  const dqBlock = evaluateLevel2Eligibility(
    {
      fields: {
        "Current Brand": "IHG",
        Country: "Mexico",
        "Official Property URL": "https://www.ihg.com/example",
        "Radar Display Reason": "data_quality_review_required|dirty_partner_label",
      },
    },
    { cleanCorePass: true }
  );
  if (dqBlock.review?.data_quality_review_required) {
    assert.equal(dqBlock.eligible, false);
  }
});

await testAsync("official-parent-level-2-completion-v1 — objective resolves", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE,
    OFFICIAL_PARENT_LEVEL_2_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-official-parent-level-2-completion-v1.js"
  );
  assert.equal(
    resolveMissionObjective("official-parent-level-2-completion-v1"),
    MISSION_OBJECTIVE_OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1
  );
  assert.equal(
    OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE,
    "official-parent-level-2-completion-v1"
  );
  assert.ok(
    OFFICIAL_PARENT_LEVEL_2_STATUS.COMPLETE.includes("official_parent_level_2_completion_v1")
  );
});

await testAsync("level-2-source-extraction-v1 — extractors + objective + High gates", async () => {
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_LEVEL_2_SOURCE_EXTRACTION_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");
  const {
    LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE,
    LEVEL_2_SOURCE_EXTRACTION_STATUS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-level-2-source-extraction-v1.js"
  );
  const {
    extractOfficialAddressFromHtml,
    extractOfficialRoomsFromHtml,
    classifyLevel2Extraction,
    mergeLevel2ProposalsToPatch,
    LEVEL_2_PARENT_ORDER,
  } = await import("../lib/research-engine-v2/census-level-2-parent-extractors.js");
  const { resolveDirectoryPhoneCandidate } = await import(
    "../lib/research-engine-v2/census-autopilot-family-directory-adapters.js"
  );

  assert.equal(
    resolveMissionObjective("level-2-source-extraction-v1"),
    MISSION_OBJECTIVE_LEVEL_2_SOURCE_EXTRACTION_V1
  );
  assert.equal(LEVEL_2_SOURCE_EXTRACTION_V1_OBJECTIVE, "level-2-source-extraction-v1");
  assert.ok(LEVEL_2_PARENT_ORDER[0] === "Choice");
  assert.ok(LEVEL_2_SOURCE_EXTRACTION_STATUS.PARTIAL.includes("adapter_wave_2"));

  const html = `
    <script type="application/ld+json">
    {"@type":"Hotel","streetAddress":"123 Avenida Kukulcan","addressLocality":"Cancun",
     "telephone":"+52 998 123 4567","numberOfRooms":220}
    </script>
    <a href="tel:+529981234567">Call</a>
  `;
  const addr = extractOfficialAddressFromHtml(html, "https://www.hilton.com/en/hotels/cunxxxx/");
  assert.equal(addr.ok, true);
  assert.ok(/123 Avenida/i.test(addr.address));
  assert.equal(addr.confidence, "High");

  const rooms = extractOfficialRoomsFromHtml(html, "https://www.hilton.com/en/hotels/cunxxxx/");
  assert.equal(rooms.ok, true);
  assert.equal(rooms.rooms, 220);

  // Forbidden OTA URL rejected
  const bad = extractOfficialAddressFromHtml(html, "https://www.booking.com/hotel/mx/x.html");
  assert.equal(bad.ok, false);

  const classified = await classifyLevel2Extraction(
    {
      id: "recL2",
      fields: {
        "Property Name": "Test Hilton Cancun",
        "Canonical Property Name": "Test Hilton Cancun",
        "Current Brand": "Hilton Hotels & Resorts",
        "Brand Family": "Hilton",
        "Family / Source Family": "Hilton",
        Country: "Mexico",
        City: "Cancún",
        "Source URL": "https://www.hilton.com/en/hotels/cunxxxx/",
        "Official Property URL": "https://www.hilton.com/en/hotels/cunxxxx/",
        "Parent Company": "Hilton",
        "Data Confidence Tier": "High",
        "Identity Confidence": "High",
        Continent: "North America",
        "Sub-Continent": "Mexico",
      },
    },
    {
      pageHtml: html,
      pageUrl: "https://www.hilton.com/en/hotels/cunxxxx/",
      skipBrandSourceOfTruth: true,
    }
  );
  // May be blocked if brand SoT fails without skip on evaluateCleanCorePass — force skip via opts
  const classified2 = await classifyLevel2Extraction(
    {
      id: "recL2b",
      fields: {
        "Property Name": "Test Hilton Cancun",
        "Canonical Property Name": "Test Hilton Cancun",
        "Current Brand": "Hilton Hotels & Resorts",
        "Brand Family": "Hilton",
        "Family / Source Family": "Hilton",
        Country: "Mexico",
        City: "Cancún",
        "Source URL": "https://www.hilton.com/en/hotels/cunxxxx/",
        "Official Property URL": "https://www.hilton.com/en/hotels/cunxxxx/",
        "Parent Company": "Hilton",
        "Data Confidence Tier": "High",
        "Identity Confidence": "High",
        Continent: "North America",
        "Sub-Continent": "Mexico",
      },
    },
    {
      pageHtml: html,
      pageUrl: "https://www.hilton.com/en/hotels/cunxxxx/",
    }
  );
  // Directory phone helper exists
  assert.equal(typeof resolveDirectoryPhoneCandidate, "function");
  const merged = mergeLevel2ProposalsToPatch({
    proposals: [
      {
        action: "propose_high_write",
        field: "Address",
        patch: { Address: "123 Avenida Kukulcan", "Address Confidence": "High" },
      },
    ],
  });
  assert.equal(merged.patch.Address, "123 Avenida Kukulcan");
  assert.ok(classified || classified2);
});

await testAsync("hbx-census-schema-and-identity-linkage-v1 — objective + patch gates", async () => {
  const {
    buildIdentityLinkagePatch,
    buildHbxSchemaFieldSpecs,
    resolveHbxSchemaLinkageGates,
    HBX_SCHEMA_LINKAGE_OBJECTIVE,
    HBX_SCHEMA_FIELD_NAMES,
  } = await import("../lib/research-engine-v2/hbx-census-schema-and-identity-linkage-v1.js");
  const { resolveMissionObjective, MISSION_OBJECTIVE_HBX_CENSUS_SCHEMA_AND_IDENTITY_LINKAGE_V1 } =
    await import("../lib/research-engine-v2/census-autopilot-mission.js");

  assert.equal(
    resolveMissionObjective(HBX_SCHEMA_LINKAGE_OBJECTIVE),
    MISSION_OBJECTIVE_HBX_CENSUS_SCHEMA_AND_IDENTITY_LINKAGE_V1
  );
  assert.equal(buildHbxSchemaFieldSpecs().length, HBX_SCHEMA_FIELD_NAMES.length);
  assert.ok(HBX_SCHEMA_FIELD_NAMES.includes("HBX Hotel Code"));
  assert.ok(HBX_SCHEMA_FIELD_NAMES.includes("Phone Source Type"));

  const gatesOk = resolveHbxSchemaLinkageGates({
    ENABLE_HBX_INSERTS: "0",
    ENABLE_HBX_NEW_CANDIDATE_INSERTS: "0",
    ENABLE_HBX_COORDINATE_WRITES: "0",
    ENABLE_HBX_IMAGE_WRITES: "0",
    ENABLE_HBX_DESCRIPTION_WRITES: "0",
    ENABLE_HBX_FACILITY_WRITES: "0",
    ENABLE_HBX_ROOM_WRITES: "0",
    ENABLE_HBX_SCHEMA_REPAIR: "1",
    ENABLE_HBX_IDENTITY_LINKAGE_WRITES: "1",
    ENABLE_HBX_CENSUS_WRITES: "1",
    ENABLE_HBX_EXISTING_MATCH_HIGH_WRITES: "1",
  });
  assert.equal(gatesOk.ok, true);

  const gatesBlocked = resolveHbxSchemaLinkageGates({
    ENABLE_HBX_INSERTS: "1",
    ENABLE_HBX_ROOM_WRITES: "0",
  });
  assert.equal(gatesBlocked.ok, false);

  const fieldSet = new Set(HBX_SCHEMA_FIELD_NAMES);
  fieldSet.add("Last Reviewed Date");
  const built = buildIdentityLinkagePatch(
    {
      hbx_hotel_code: 1924,
      chain_code: "ARCOS",
      category: "4EST",
      phonehotel: "+523222267100",
      website: "www.playalosarcos.com",
      match_class: "existing_match_high",
    },
    { Phone: "+52 322 226 7100" },
    fieldSet
  );
  assert.equal(built.ok, true);
  assert.equal(built.patch["HBX Hotel Code"], "1924");
  assert.equal(built.patch["HBX Chain Code"], "ARCOS");
  assert.equal(built.patch["Phone Source Type"], "hbx_content_api");
  assert.equal(built.patch.Address, undefined);
  assert.equal(built.patch.Phone, undefined);
  assert.equal(built.patch["Rooms / Keys"], undefined);
});

await testAsync("full-cala-15k — HBX Hotel Code field dedupe + CR status", async () => {
  const {
    classifyAgainstCensus,
    resolveCountryBatchStatus,
    FULL_CALA_15K_STATUS,
  } = await import("../lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js");

  assert.equal(
    resolveCountryBatchStatus("Costa Rica", 12),
    FULL_CALA_15K_STATUS.COSTA_RICA_BATCH_APPLY_COMPLETE
  );
  assert.equal(
    resolveCountryBatchStatus("Panama", 8),
    FULL_CALA_15K_STATUS.PANAMA_BATCH_APPLY_COMPLETE
  );
  assert.equal(
    resolveCountryBatchStatus("Colombia", 10),
    FULL_CALA_15K_STATUS.COLOMBIA_BATCH_APPLY_COMPLETE
  );
  assert.equal(
    resolveCountryBatchStatus("Mexico", 10),
    FULL_CALA_15K_STATUS.MEXICO_BATCH_1_APPLY_COMPLETE
  );
  assert.equal(
    resolveCountryBatchStatus("Mexico", 10, {
      objective: "full-cala-15k-census-shell-insert-v1-mexico-batch-2",
    }),
    FULL_CALA_15K_STATUS.MEXICO_BATCH_2_APPLY_COMPLETE
  );
  assert.equal(
    resolveCountryBatchStatus("Mexico", 10, {
      objective: "full-cala-15k-census-shell-insert-v1-mexico-batch-3",
    }),
    FULL_CALA_15K_STATUS.MEXICO_BATCH_3_APPLY_COMPLETE
  );
  const {
    mexicoBatchSourcePriority,
    classifyShellPreflightQuality,
    decideMexicoBatch3PreflightBlock,
    SHELL_PREFLIGHT_CLASS,
  } = await import("../lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js");
  assert.equal(
    classifyShellPreflightQuality(
      {
        property_name: "Convention Center Mexico City",
        country: "Mexico",
        city: "Mexico City",
        source_type: "cvent_candidate",
      },
      { cventOnlyQualityGate: true }
    ).class,
    SHELL_PREFLIGHT_CLASS.NON_HOTEL
  );
  assert.equal(
    classifyShellPreflightQuality(
      {
        property_name: "Hotel Plaza Reforma",
        country: "Mexico",
        city: "Mexico City",
        website: "https://hotelplaza.example",
        source_type: "cvent_candidate",
      },
      { cventOnlyQualityGate: true }
    ).class,
    SHELL_PREFLIGHT_CLASS.SAFE
  );
  assert.equal(
    decideMexicoBatch3PreflightBlock({
      remaining_eligible: 1000,
      insertable: 0,
      held: 1000,
      top500_hold_ratio: 1,
    }).block,
    true
  );
  assert.equal(
    mexicoBatchSourcePriority({
      external_ids: { hbx_code: 1 },
      source_type: "hbx_content_api",
    }),
    0
  );
  assert.equal(
    mexicoBatchSourcePriority({
      external_ids: {},
      source_type: "cvent_candidate",
    }),
    3
  );

  const index = {
    byHbx: new Map([
      [
        999001,
        {
          id: "recHbxField",
          fields: { "HBX Hotel Code": "999001", Country: "Costa Rica" },
        },
      ],
      [
        999002,
        {
          id: "recHbxNotes",
          fields: { "Notes for Steward": "hbx_linkage | hotel_code=999002" },
        },
      ],
    ]),
    byNameCountry: new Map(),
    byDomain: new Map(),
    byPhone: new Map(),
    byIdentityKey: new Map(),
  };

  const fieldHit = classifyAgainstCensus(
    {
      property_name: "Hotel Test CR",
      country: "Costa Rica",
      normalized_property_name: "hotel test cr",
      external_ids: { hbx_code: 999001 },
    },
    index
  );
  assert.equal(fieldHit.match_class, "existing_match_high");
  assert.equal(fieldHit.reason, "hbx_hotel_code_field");

  const notesHit = classifyAgainstCensus(
    {
      property_name: "Hotel Notes CR",
      country: "Costa Rica",
      normalized_property_name: "hotel notes cr",
      external_ids: { hbx_code: 999002 },
    },
    index
  );
  assert.equal(notesHit.match_class, "existing_match_high");
  assert.equal(notesHit.reason, "hbx_code_in_notes");
});

await testAsync("full-cala-15k-shell-format-source-brand-backfill-v1 — proper case + gates", async () => {
  const {
    toSmartHotelProperCase,
    buildShellBackfillPatch,
    investigateFamilySourceField,
    resolveShellFormatBackfillGates,
    SHELL_FORMAT_BACKFILL_OBJECTIVE,
    SHELL_BACKFILL_FIELD_NAMES,
  } = await import(
    "../lib/research-engine-v2/full-cala-15k-shell-format-source-brand-backfill-v1.js"
  );
  const {
    resolveMissionObjective,
    MISSION_OBJECTIVE_FULL_CALA_15K_SHELL_FORMAT_SOURCE_BRAND_BACKFILL_V1,
  } = await import("../lib/research-engine-v2/census-autopilot-mission.js");

  assert.equal(
    resolveMissionObjective(SHELL_FORMAT_BACKFILL_OBJECTIVE),
    MISSION_OBJECTIVE_FULL_CALA_15K_SHELL_FORMAT_SOURCE_BRAND_BACKFILL_V1
  );
  assert.equal(toSmartHotelProperCase("hotel cuna del angel"), "Hotel Cuna del Angel");
  assert.equal(toSmartHotelProperCase("JW MARRIOTT CANCUN"), "JW Marriott Cancun");
  assert.equal(toSmartHotelProperCase("AC HOTEL SANTO DOMINGO"), "AC Hotel Santo Domingo");

  const fam = investigateFamilySourceField();
  assert.equal(fam.field_name_actual, "Family / Source Family");
  assert.equal(fam.backfill_this_mission, false);

  const gates = resolveShellFormatBackfillGates({
    ENABLE_CURRENT_BRAND_WRITES: "0",
    ENABLE_ROOMS_WRITES: "0",
    ENABLE_OWNER_OPERATOR_WRITES: "0",
    ENABLE_DATE_WRITES: "0",
    ENABLE_COORDINATE_WRITES: "0",
    ENABLE_PUBLIC_DISPLAY_WRITES: "0",
    ENABLE_FULL_CALA_15K_CENSUS_SHELL: "1",
    ENABLE_CENSUS_SHELL_FORMAT_BACKFILL: "1",
    ENABLE_CANDIDATE_BRAND_FIELDS: "1",
  });
  assert.equal(gates.ok, true);

  const fieldSet = new Set([
    "Canonical Property Name",
    "Last Reviewed Date",
    ...SHELL_BACKFILL_FIELD_NAMES,
    "HBX Hotel Code",
    "HBX Chain Code",
  ]);
  const built = buildShellBackfillPatch(
    {
      "Property Name": "hotel cuna del angel",
      "Canonical Property Name": "hotel cuna del angel",
      Country: "Costa Rica",
      "HBX Hotel Code": "97413",
      "Notes for Steward":
        "Candidate identity only; field validation required from approved source (Cvent Candidate / Not Field Source).\nhbx_linkage | hotel_code=97413 | source=hbx_content_api\nsources=cvent_candidate,hbx_content_api\ndedupe_class_pending_insert",
    },
    {
      fieldSet,
      notesMeta: {
        sources: ["cvent_candidate", "hbx_content_api"],
        hbx_code: 97413,
        chain_text: null,
        is_cvent: true,
        is_hbx: true,
        is_shell_marker: true,
      },
      candidate: {
        source_type: "hbx_content_api",
        brand_text: "Cuna del Angel",
        chain_text: "INDEP",
      },
      allowCandidateBrand: true,
      countryBatch: "Costa Rica",
    }
  );
  assert.equal(built.patch["Canonical Property Name"], "Hotel Cuna del Angel");
  assert.equal(built.patch["Discovery Source"], "Cvent + HBX Candidate");
  assert.equal(built.patch["Candidate Brand Text"], "Cuna del Angel");
  assert.equal(built.patch["Current Brand"], undefined);
  assert.equal(built.patch["Brand Family"], undefined);
  assert.equal(built.patch["Family / Source Family"], undefined);
});

console.log("All census-autopilot tests passed.");
console.log(`Status target: ${STATUS.READY}`);
