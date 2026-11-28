#!/usr/bin/env node
/**
 * Regression: Unified V3 Live orchestrator (hotel-agnostic).
 *
 * Proves fail-closed behavior and wiring WITHOUT spending Exa / Findymail credits:
 *  - Live without authorizePaidResearch is refused
 *  - Live FULL without WHO/HOW authorization params fails closed (no Wyndham defaults)
 *  - Live Findymail without key / without paid auth fails closed
 *  - Surfe is refused
 *  - Live happy path with injected (mock) WHO/HOW adapter + persistence bridge
 *  - Missing discovery/deep artifacts HOLD (never silent PASS)
 *  - DryRun still works; CLI never exits 3
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  runFullE2eV3DryRun,
  runFullE2eV3Live,
  runContactResearchStageLive,
  runDiscoveryStageLive,
  runDeepResearchStageLive,
  runWhoHowStageLive,
  validateFullE2eV3LiveArtifacts,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-v3-orchestrator-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CLI = path.join(ROOT, "scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs");
const HOTEL = "recTestLiveGapNotAHotel";

// Never allow a real provider key into this test process.
delete process.env.FINDYMAIL_API_KEY;
delete process.env.GDI_EXA_AGENT_LIVE;
delete process.env.EXA_API_KEY;
delete process.env.EXA_KEY;
delete process.env.SERPAPI_API_KEY;
delete process.env.SERPAPI_KEY;
delete process.env.GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES;

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-v3-live-"));
const tmp = (name) => path.join(tmpRoot, name);

async function rejectsWithCode(promiseFactory, code, label) {
  let err = null;
  try {
    await promiseFactory();
  } catch (e) {
    err = e;
  }
  assert.ok(err, `${label}: expected rejection`);
  assert.equal(err.code, code, `${label}: expected ${code} got ${err.code} (${err.message})`);
}

function runCli(args, env = {}) {
  return spawnSync("node", [CLI, ...args], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: 120000,
    env: {
      ...process.env,
      FINDYMAIL_API_KEY: "",
      GDI_EXA_AGENT_LIVE: "",
      EXA_API_KEY: "",
      SERPAPI_API_KEY: "",
      GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES: "",
      ...env,
    },
  });
}

try {
  // ---- Live gating --------------------------------------------------------
  await rejectsWithCode(
    () => runFullE2eV3Live({ hotelId: HOTEL, mode: "Live", outDir: tmp("a") }),
    "LIVE_REQUIRES_PAID_AUTHORIZATION",
    "Live without authorizePaidResearch",
  );
  assert.equal(fs.existsSync(tmp("a")), false, "no side effects before auth gate");

  await rejectsWithCode(
    () =>
      runFullE2eV3Live({
        hotelId: HOTEL,
        mode: "DryRun",
        authorizePaidResearch: true,
        outDir: tmp("b"),
      }),
    "LIVE_REQUIRES_MODE_LIVE",
    "Live runner refuses DryRun mode",
  );

  // DryRun orchestrator still refuses Live without authorization / mismatched flags
  await assert.rejects(
    () => runFullE2eV3DryRun({ hotelId: HOTEL, mode: "Live", outDir: tmp("c") }),
    /requires authorizePaidResearch/,
  );

  // ---- WHO/HOW params fail closed (no Wyndham / hotel defaults) -----------
  const base = { hotelId: HOTEL, mode: "Live", authorizePaidResearch: true };
  await rejectsWithCode(
    () => runFullE2eV3Live({ ...base, outDir: tmp("d1") }),
    "LIVE_WHO_HOW_REQUIRES_AUTHORIZATION_ID",
    "missing authorizationId",
  );
  await rejectsWithCode(
    () => runFullE2eV3Live({ ...base, outDir: tmp("d2"), authorizationId: "AUTH_X" }),
    "LIVE_WHO_HOW_REQUIRES_HARD_CEILING",
    "missing hardCeilingUsd",
  );
  await rejectsWithCode(
    () =>
      runFullE2eV3Live({
        ...base,
        outDir: tmp("d3"),
        authorizationId: "AUTH_X",
        hardCeilingUsd: 5,
      }),
    "LIVE_WHO_HOW_REQUIRES_MANIFEST",
    "missing manifest",
  );
  await rejectsWithCode(
    () =>
      runWhoHowStageLive(HOTEL, {
        authorizePaidResearch: true,
        authorizationId: "AUTH_X",
        hardCeilingUsd: 5,
        whoHowManifestPath: tmp("does-not-exist.json"),
      }),
    "LIVE_WHO_HOW_MANIFEST_NOT_FOUND",
    "manifest not found",
  );
  assert.equal(fs.existsSync(tmp("d1")), false, "params validated before mkdir");

  // Surfe never; Findymail needs key + paid auth + Stage 4; persistence apply needs auth
  await rejectsWithCode(
    () => runFullE2eV3Live({ ...base, skipWhoHow: true, authorizeSurfe: true, outDir: tmp("e1") }),
    "SURFE_FORBIDDEN_IN_FULL_E2E",
    "Surfe refused",
  );
  await rejectsWithCode(
    () =>
      runFullE2eV3Live({
        ...base,
        skipWhoHow: true,
        authorizeFindymail: true,
        outDir: tmp("e2"),
      }),
    "MISSING_FINDYMAIL_API_KEY",
    "Live Findymail without key",
  );
  await rejectsWithCode(
    () =>
      runFullE2eV3Live({
        ...base,
        skipWhoHow: true,
        authorizeFindymail: true,
        skipContact: true,
        outDir: tmp("e3"),
      }),
    "FINDYMAIL_REQUIRES_CONTACT_STAGE",
    "Findymail with skipped contact",
  );
  await rejectsWithCode(
    () =>
      runFullE2eV3Live({
        ...base,
        skipWhoHow: true,
        applyResearchPersistence: true,
        outDir: tmp("e4"),
      }),
    "RESEARCH_PERSISTENCE_APPLY_REQUIRES_AUTH",
    "paid auth alone must not unlock persistence apply",
  );
  await rejectsWithCode(
    () =>
      runContactResearchStageLive([], {
        hotelId: HOTEL,
        authorizeFindymail: true,
      }),
    "FINDYMAIL_REQUIRES_PAID_AUTHORIZATION",
    "contact Live Findymail without paid auth",
  );
  await rejectsWithCode(
    () =>
      runContactResearchStageLive([], {
        hotelId: HOTEL,
        authorizePaidResearch: true,
        authorizeFindymail: true,
      }),
    "MISSING_FINDYMAIL_API_KEY",
    "contact Live Findymail without key",
  );

  // Findymail planning path (dry) needs no key and spends nothing
  const planning = await runContactResearchStageLive([], {
    hotelId: HOTEL,
    authorizePaidResearch: true,
    authorizeFindymail: true,
    findymailDryRun: true,
  });
  assert.equal(planning.mode, "Live");
  assert.equal(planning.paid, false);
  assert.equal(planning.findymailAuthorized, false);
  assert.equal(planning.surfe, false);

  // ---- Reuse semantics: missing artifacts HOLD ----------------------------
  const disc = runDiscoveryStageLive(HOTEL, { outDir: tmp("f") });
  assert.equal(disc.outcome, "HOLD");
  assert.equal(disc.mode, "Live");
  assert.equal(disc.paid, false);
  assert.equal(disc.reuseExistingIntelligence, true);
  assert.equal(disc.reason, "DISCOVERY_ARTIFACTS_MISSING");
  const deep = runDeepResearchStageLive(HOTEL, { outDir: tmp("f") });
  assert.equal(deep.outcome, "HOLD");
  assert.equal(deep.paid, false);
  assert.equal(deep.reason, "DEEP_RESEARCH_ARTIFACTS_MISSING");

  // ---- Live happy path (mock adapter + mock persistence; STAGED skipped) ---
  const manifestPath = tmp("manifest.json");
  fs.writeFileSync(
    manifestPath,
    JSON.stringify({
      opportunities: [
        {
          opportunityId: "opp_live_test_1",
          organizationName: "Example Org",
          title: "Example group demand",
          segmentId: "corporate",
        },
      ],
    }),
  );
  let adapterCalls = 0;
  const whoHowAdapter = {
    assertWhoHowLiveAuthorization: (o) => {
      assert.equal(o.mode, "Live");
      assert.equal(o.authorizationId, "AUTH_TEST");
      return { ok: true, errors: [] };
    },
    reconcileWhoHowJobSpend: () => ({ totalUsd: 0 }),
    executeWhoHowTranche: async (items, o) => {
      adapterCalls += 1;
      assert.equal(o.mode, "Live");
      assert.equal(o.hardCeilingUsd, 5);
      assert.equal(items[0].dossier.opportunity.hotelId, HOTEL);
      return {
        spentUsd: 0,
        remainingUsd: 5,
        processed: items.length,
        blocked: null,
        results: items.map((i) => ({
          opportunityId: i.opportunityId,
          paid: false,
          costUsd: 0,
          whoHow: { whoHowStatus: "ORGANIZATIONAL_ROUTE_ONLY" },
          opportunity: { ...i.dossier.opportunity, whoHowStatus: "ORGANIZATIONAL_ROUTE_ONLY" },
        })),
      };
    },
  };
  let persistenceInput = null;
  const persistenceBridge = {
    persistFullE2eResearchInventory: async (input) => {
      persistenceInput = input;
      return { dryRun: true, authorization: { productionUnlocked: false }, counts: {} };
    },
  };
  const live = await runFullE2eV3Live({
    ...base,
    outDir: tmp("g"),
    authorizationId: "AUTH_TEST",
    hardCeilingUsd: 5,
    whoHowManifestPath: manifestPath,
    whoHowAdapter,
    contactStageOpts: { fixture: true },
    skipStagedProgression: true,
    planResearchPersistence: true,
    persistenceBridge,
  });
  assert.equal(adapterCalls, 1);
  assert.equal(live.summary.mode, "Live");
  assert.equal(live.summary.paid, false, "paid only when spend actually occurred");
  assert.equal(live.summary.stagedProgression.activated, false);
  assert.equal(live.summary.stagedProgression.currentUnchanged, true);
  assert.equal(live.summary.researchPersistence.requested, true);
  assert.equal(live.summary.researchPersistence.applied, false);
  assert.equal(persistenceInput.authorizePaidResearch, false);
  assert.equal(persistenceInput.apply, false);
  assert.equal(live.stageReports.discovery.outcome, "HOLD");
  assert.equal(live.stageReports.whoHow.mode, "Live");
  assert.equal(live.stageReports.contact.mode, "Live");
  assert.equal(live.stageReports.contact.surfe, false);
  assert.ok(fs.existsSync(path.join(tmp("g"), "00-ORCHESTRATOR_SUMMARY.json")));

  // Continuation-only: skip WHO/HOW, reuse prior LIVE report (never a DryRun one)
  const cont = await runFullE2eV3Live({
    ...base,
    outDir: tmp("g"),
    skipWhoHow: true,
    skipDiscovery: true,
    skipDeep: true,
    continuationOnly: true,
    contactStageOpts: { fixture: true },
    skipStagedProgression: true,
  });
  assert.equal(cont.stageReports.contact.results.length, 1, "reused prior Live WHO/HOW opps");
  assert.equal(adapterCalls, 1, "WHO/HOW not re-run");

  // Validation-only never activates / spends
  const v = validateFullE2eV3LiveArtifacts({ hotelId: HOTEL, outDir: tmp("g") });
  assert.equal(v.validationOnly, true);
  assert.equal(v.paid, false);
  assert.equal(v.activated, false);

  // ---- Unified Live spine: Leisure / CAD / PLP / Receipt / Findymail persist ----
  // $0: provider keys unset; leisure is mock_live/assess-only, CAD is classification.
  const matrixFile = tmp("matrix.json");
  fs.writeFileSync(
    matrixFile,
    JSON.stringify({
      cells: [
        { segmentId: "corporate_group_travel" },
        { segmentId: "preferred_lodging_partnerships" },
      ],
    }),
  );
  const spineOpps = [
    {
      id: "opp_spine_1",
      opportunityId: "opp_spine_1",
      organizationName: "Example Org",
      title: "Example group demand",
      segmentId: "corporate_group_travel",
    },
  ];
  const spineBase = {
    ...base,
    skipWhoHow: true,
    skipStagedProgression: true,
    opportunities: spineOpps,
    discoveryMatrixPath: matrixFile,
    contactStageOpts: { fixture: true },
  };

  // Default (no replay/paid-discovery flag): spine NOT enabled; nothing invented.
  const noSpine = await runFullE2eV3Live({ ...spineBase, outDir: tmp("s0") });
  assert.equal(noSpine.summary.researchSpine.leisureEnabled, false);
  assert.equal(noSpine.summary.researchSpine.cadEnabled, false);
  assert.equal(noSpine.summary.stageInventory.leisure.status, "NOT_ENABLED");
  assert.equal(noSpine.summary.stageInventory.cad.status, "NOT_ENABLED");
  assert.equal(fs.existsSync(path.join(tmp("s0"), "01b-leisure-opt-in.json")), false);
  assert.equal(fs.existsSync(path.join(tmp("s0"), "01c-cad-opt-in.json")), false);
  // PLP is read from the reused discovery matrix regardless of the spine.
  assert.equal(noSpine.summary.plpAssessed, true);
  assert.equal(noSpine.stageReports.discovery.metrics.plpCellCount, 1);

  // Historical replay: Leisure + CAD assess/classify, receipt written, no spend.
  const spine = await runFullE2eV3Live({
    ...spineBase,
    allowHistoricalReplay: true,
    outDir: tmp("s1"),
  });
  const s1 = spine.summary;
  assert.equal(s1.researchSpine.defaultOn, true);
  assert.equal(s1.researchSpine.failed, false);
  assert.equal(s1.paid, false, "spine must not spend");
  assert.equal(s1.paidBreakdown.leisure, false);
  assert.equal(s1.paidBreakdown.cad, false);
  assert.equal(s1.paidBreakdown.discovery, false);
  assert.equal(s1.paidDiscoveryInvoked, false);
  assert.equal(s1.plpAssessed, true);
  for (const k of [
    "discovery", "leisure", "cad", "plpAssessed", "deep", "whoHow", "contact",
    "findymail", "qualification", "staged", "persistence", "researchReceipt",
  ]) {
    assert.ok(k in s1.stageInventory, `stageInventory.${k}`);
  }
  for (const k of [
    "discovery", "leisure", "cad", "plp", "deep", "whoHow", "contact",
    "findymail", "qualification", "staged", "persistence", "researchReceipt",
  ]) {
    assert.ok(k in s1.paidBreakdown, `paidBreakdown.${k}`);
  }
  assert.equal(s1.stageInventory.leisure.executed, true);
  assert.equal(s1.stageInventory.cad.executed, true);
  assert.equal(s1.stageInventory.cad.paid, false);
  const leisureReport = JSON.parse(
    fs.readFileSync(path.join(tmp("s1"), "01b-leisure-opt-in.json"), "utf8"),
  );
  assert.ok(leisureReport.status, "leisure report has status");
  assert.equal(Number(leisureReport.spentUsd || 0), 0);
  assert.equal(Number(leisureReport.paidCalls || 0), 0);
  const cadReport = JSON.parse(
    fs.readFileSync(path.join(tmp("s1"), "01c-cad-opt-in.json"), "utf8"),
  );
  assert.equal(cadReport.status, "CLASSIFIED");
  assert.equal(cadReport.classifiedCount, 1);
  assert.equal(cadReport.fqoAutoPromoted, false);
  assert.equal(cadReport.persistenceAuthorized, false);
  const receiptFile = path.join(tmp("s1"), "07-research-receipt.json");
  assert.ok(fs.existsSync(receiptFile), "07-research-receipt.json written");
  const receiptReport = JSON.parse(fs.readFileSync(receiptFile, "utf8"));
  assert.ok(receiptReport.receipt?.contractId, "canonical receipt contract id");
  assert.equal(receiptReport.currentUnchanged, true);
  assert.equal(receiptReport.runStageMetrics.plpAssessed, true);
  assert.ok(s1.researchReceipt.path.endsWith("07-research-receipt.json"));

  // Explicit disable wins over the default-on spine.
  const noLeisure = await runFullE2eV3Live({
    ...spineBase,
    allowHistoricalReplay: true,
    enableLeisure: false,
    outDir: tmp("s2"),
  });
  assert.equal(noLeisure.summary.stageInventory.leisure.status, "NOT_ENABLED");
  assert.equal(noLeisure.summary.stageInventory.cad.executed, true);
  assert.equal(fs.existsSync(path.join(tmp("s2"), "01b-leisure-opt-in.json")), false);

  // Explicit enable works without replay flags; stage failure is surfaced (not silent).
  const forced = await runFullE2eV3Live({
    ...spineBase,
    enableCad: true,
    enableLeisure: true,
    leisureStageRunner: async () => {
      throw new Error("injected leisure failure");
    },
    outDir: tmp("s3"),
  });
  assert.equal(forced.summary.researchSpine.failed, true);
  assert.equal(forced.stageReports.leisure.status, "APPLICABLE_EXECUTION_FAILED");
  assert.match(forced.stageReports.leisure.error, /injected leisure failure/);
  assert.equal(forced.stageReports.cad.status, "CLASSIFIED");

  // ---- Findymail persist: default false, separate authorization only ------
  let contactSeenPersist = null;
  const findymailBase = {
    ...spineBase,
    authorizeFindymail: true,
    findymailDryRun: true,
  };
  const noPersist = await runFullE2eV3Live({ ...findymailBase, outDir: tmp("p0") });
  contactSeenPersist = noPersist.stageReports.contact.persistFindymailResults;
  assert.equal(contactSeenPersist, false, "persistFindymailResults defaults false");
  assert.deepEqual(noPersist.summary.findymailPersist, { requested: false, applied: false });
  assert.equal(noPersist.summary.stageInventory.findymail.persist.status, "NOT_REQUESTED");

  await rejectsWithCode(
    () => runFullE2eV3Live({ ...findymailBase, persistFindymailResults: true, outDir: tmp("p1") }),
    "FINDYMAIL_PERSIST_REQUIRES_AUTH",
    "paid research + findymail auth alone must not unlock persistence",
  );
  assert.equal(fs.existsSync(tmp("p1")), false, "persist auth validated before side effects");
  await rejectsWithCode(
    () =>
      runFullE2eV3Live({
        ...base,
        skipWhoHow: true,
        persistFindymailResults: true,
        authorizeFindymailPersist: true,
        authorizeAirtableProductionWrites: true,
        outDir: tmp("p2"),
      }),
    "FINDYMAIL_PERSIST_REQUIRES_FINDYMAIL",
    "persist requires authorizeFindymail",
  );
  await rejectsWithCode(
    () =>
      runFullE2eV3Live({
        ...findymailBase,
        persistFindymailResults: true,
        authorizeFindymailPersist: true,
        outDir: tmp("p3"),
      }),
    "FINDYMAIL_PERSIST_REQUIRES_AUTH",
    "dedicated persist auth needs Airtable production writes flag too",
  );
  await rejectsWithCode(
    () => runFullE2eV3Live({ ...findymailBase, applyFindymailPersist: true, outDir: tmp("p4") }),
    "FINDYMAIL_PERSIST_APPLY_REQUIRES_PERSIST",
    "apply without persist",
  );
  delete process.env.GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES;
  await rejectsWithCode(
    () =>
      runFullE2eV3Live({
        ...findymailBase,
        persistFindymailResults: true,
        applyFindymailPersist: true,
        authorizeFindymailPersist: true,
        authorizeAirtableProductionWrites: true,
        outDir: tmp("p5"),
      }),
    "FINDYMAIL_PERSIST_PRODUCTION_ENV_GATE_REQUIRED",
    "apply needs production env gate",
  );

  let persistCall = null;
  const persisted = await runFullE2eV3Live({
    ...findymailBase,
    persistFindymailResults: true,
    authorizeFindymailPersist: true,
    authorizeAirtableProductionWrites: true,
    findymailPersistFn: async (patches, o) => {
      persistCall = { patches, o };
      return { dryRun: true, wouldWrite: 0, rows: [] };
    },
    outDir: tmp("p6"),
  });
  assert.ok(persistCall, "Findymail persist step invoked with separate auth");
  assert.equal(persistCall.o.apply, false, "persist is dry-run unless apply requested");
  assert.equal(persistCall.o.dryRun, true);
  assert.equal(persistCall.o.hotelId, HOTEL);
  assert.equal(persisted.stageReports.contact.persistFindymailResults, true);
  assert.equal(persisted.summary.findymailPersist.dryRun, true);
  assert.equal(persisted.summary.findymailPersist.applied, false);
  assert.equal(persisted.summary.paid, false, "no Findymail spend in dry planning");
  assert.ok(fs.existsSync(path.join(tmp("p6"), "04b-findymail-persist.json")));

  // STAGED / CURRENT pointers untouched by this test hotel.
  assert.equal(spine.summary.stagedProgression.activated, false);
  assert.equal(spine.summary.stagedProgression.currentUnchanged, true);

  // ---- DryRun still works --------------------------------------------------
  const dry = await runFullE2eV3DryRun({
    hotelId: HOTEL,
    mode: "DryRun",
    outDir: tmp("h"),
    skipStagedProgression: true,
  });
  assert.equal(dry.summary.mode, "DryRun");
  assert.equal(dry.summary.paid, false);

  // ---- CLI -----------------------------------------------------------------
  const cliNoAuth = runCli(["--stage", "FULL", "--hotel-id", HOTEL, "--mode", "Live", "--out-dir", tmp("i")]);
  assert.equal(cliNoAuth.status, 2, "CLI Live without --authorize-paid-research");

  const cliNoWho = runCli([
    "--stage", "FULL", "--hotel-id", HOTEL, "--mode", "Live",
    "--authorize-paid-research", "--out-dir", tmp("j"),
  ]);
  assert.equal(cliNoWho.status, 2, "CLI Live FULL without WHO params fails closed");
  assert.match(cliNoWho.stderr, /authorizationId/i);

  const cliWhoNoParams = runCli([
    "--stage", "WHO_HOW", "--hotel-id", HOTEL, "--mode", "Live",
    "--authorize-paid-research", "--out-dir", tmp("k"),
  ]);
  assert.equal(cliWhoNoParams.status, 2);
  assert.match(cliWhoNoParams.stderr, /Checkpoint A|--authorization-id/);

  const cliFindymailNoKey = runCli([
    "--stage", "CONTACT", "--hotel-id", HOTEL, "--mode", "Live",
    "--authorize-paid-research", "--authorize-findymail", "--out-dir", tmp("l"),
  ]);
  assert.equal(cliFindymailNoKey.status, 4, "Live Findymail without key refuses cleanly");

  const cliPersistSingle = runCli([
    "--stage", "DISCOVERY", "--hotel-id", HOTEL, "--mode", "Live",
    "--authorize-paid-research", "--apply-research-persistence", "--out-dir", tmp("m"),
  ]);
  assert.equal(cliPersistSingle.status, 2);

  const cliDiscovery = runCli([
    "--stage", "DISCOVERY", "--hotel-id", HOTEL, "--mode", "Live",
    "--authorize-paid-research", "--out-dir", tmp("n"),
  ]);
  assert.equal(cliDiscovery.status, 0);
  assert.match(cliDiscovery.stdout, /"outcome": "HOLD"/);

  const cliValidate = runCli(["--stage", "VALIDATE", "--hotel-id", HOTEL, "--mode", "Live", "--out-dir", tmp("o")]);
  assert.equal(cliValidate.status, 1, "validation-only on empty dir: not ok, not exit 3");
  assert.equal(fs.existsSync(path.join(tmp("o"), "00-ORCHESTRATOR_SUMMARY.json")), false);

  const cliDry = runCli(["--stage", "FULL", "--hotel-id", HOTEL, "--mode", "DryRun", "--out-dir", tmp("p")]);
  assert.equal(cliDry.status, 0, `CLI DryRun FULL: ${cliDry.stderr}`);

  // New unified flags
  const cliPersistDry = runCli([
    "--stage", "FULL", "--hotel-id", HOTEL, "--mode", "DryRun",
    "--persist-findymail-results", "--out-dir", tmp("q"),
  ]);
  assert.equal(cliPersistDry.status, 2, "persist flag requires Live");

  const cliPersistNoAuth = runCli([
    "--stage", "FULL", "--hotel-id", HOTEL, "--mode", "Live",
    "--authorize-paid-research", "--skip-who-how", "--authorize-findymail",
    "--findymail-dry-run", "--persist-findymail-results", "--out-dir", tmp("r"),
  ]);
  assert.equal(cliPersistNoAuth.status, 2, "persist without separate auth fails closed");
  assert.match(cliPersistNoAuth.stderr, /FINDYMAIL_PERSIST_REQUIRES_AUTH/);

  const cliSpine = runCli([
    "--stage", "FULL", "--hotel-id", HOTEL, "--mode", "Live",
    "--authorize-paid-research", "--skip-who-how", "--skip-staged-progression",
    "--allow-historical-replay", "--enable-leisure", "--enable-cad",
    "--discovery-matrix", matrixFile, "--out-dir", tmp("t"),
  ]);
  assert.equal(cliSpine.status, 0, `CLI Live spine: ${cliSpine.stderr}`);
  assert.match(cliSpine.stdout, /"leisureEnabled": true/);
  assert.match(cliSpine.stdout, /"cadEnabled": true/);
  assert.ok(fs.existsSync(path.join(tmp("t"), "01b-leisure-opt-in.json")));
  assert.ok(fs.existsSync(path.join(tmp("t"), "01c-cad-opt-in.json")));
  assert.ok(fs.existsSync(path.join(tmp("t"), "07-research-receipt.json")));

  // Hotel-agnostic Findymail persist helper (dry-run default; no provider calls)
  const PERSIST_CLI = path.join(ROOT, "scripts/gdi/run-gdi-findymail-airtable-persist-v1.mjs");
  const runPersistCli = (args) =>
    spawnSync("node", [PERSIST_CLI, ...args], {
      cwd: ROOT,
      encoding: "utf8",
      timeout: 120000,
      env: {
        ...process.env,
        FINDYMAIL_API_KEY: "",
        GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES: "",
      },
    });
  assert.equal(runPersistCli([]).status, 2, "persist helper requires --hotel-id");
  const oppFile = tmp("findymail-opps.json");
  fs.writeFileSync(
    oppFile,
    JSON.stringify({
      opportunities: [
        {
          id: "opp_fm_1",
          primaryContact: {
            name: "Pat Example",
            email: "pat@example.org",
            channels: [{ source: "FINDYMAIL", value: "pat@example.org" }],
          },
        },
        { id: "opp_fm_2", primaryContact: { name: "No Channel" } },
      ],
    }),
  );
  const persistDry = runPersistCli([
    "--hotel-id", HOTEL, "--opportunities-file", oppFile, "--out-dir", tmp("u"),
  ]);
  assert.equal(persistDry.status, 0, `persist helper dry-run: ${persistDry.stderr}`);
  assert.match(persistDry.stdout, /"dryRun": true/);
  assert.match(persistDry.stdout, /"candidateCount": 1/);
  assert.match(persistDry.stdout, /NO_FINDYMAIL_CHANNEL/);
  const persistApplyNoAuth = runPersistCli([
    "--hotel-id", HOTEL, "--opportunities-file", oppFile, "--apply",
    "--out-dir", tmp("v"),
  ]);
  assert.equal(persistApplyNoAuth.status, 2, "apply without auth fails closed");
  const persistApplyNoEnv = runPersistCli([
    "--hotel-id", HOTEL, "--opportunities-file", oppFile, "--apply",
    "--authorize-findymail-persist", "--authorize-airtable-production-writes",
    "--out-dir", tmp("w"),
  ]);
  assert.equal(persistApplyNoEnv.status, 2, "apply without env gate fails closed");
  assert.match(persistApplyNoEnv.stderr, /ENV_GATE/);
  const persistSrc = fs.readFileSync(PERSIST_CLI, "utf8");
  assert.doesNotMatch(persistSrc, /wyndham|recnPB3Q02NO8Ma1o/i, "persist helper has no hotel defaults");

  const cliSrc = fs.readFileSync(CLI, "utf8");
  assert.doesNotMatch(cliSrc, /process\.exit\(3\)/, "CLI must not blanket-exit 3 on Live");
  assert.doesNotMatch(cliSrc, /FOUNDER_GO_WYNDHAM|wyndham/i, "CLI has no Wyndham defaults");

  console.log("OK test:gdi-full-e2e-v3-live-orchestrator-v1");
} finally {
  fs.rmSync(tmpRoot, { recursive: true, force: true });
}
