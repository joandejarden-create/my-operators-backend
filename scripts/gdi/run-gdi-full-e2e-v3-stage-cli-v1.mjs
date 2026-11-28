#!/usr/bin/env node
/**
 * Shared CLI for GDI Full E2E V3 stage runners + orchestrator.
 * Default Mode=DryRun. Never embeds API keys. Hotel-agnostic (no hotel-specific
 * authorization ids, manifests, or paths).
 *
 * Live (--mode Live --authorize-paid-research):
 *   FULL            -> runFullE2eV3Live
 *   WHO_HOW         -> existing Exa adapter (needs --authorization-id,
 *                      --hard-ceiling-usd, --manifest)
 *   CONTACT         -> Live contact stage (Findymail only with --authorize-findymail)
 *   DISCOVERY/DEEP  -> reuse existing artifacts; when MISSING and BOTH
 *                      --authorize-paid-discovery + --authorize-paid-research
 *                      (+ --authorization-id, --hard-ceiling-usd) are given, invoke
 *                      the canonical chain (NEW HOTEL FULL E2E). Otherwise HOLD.
 *   QUALIFICATION   -> strict FQO on prior Live opportunities
 *   VALIDATE        -> read-only validation (no activate, no paid)
 *
 * Exit codes: 0 ok · 1 stage failure / invalid STAGED inventory · 2 usage or
 * missing authorization params (fail closed) · 4 provider authorization refused
 * (env gate / missing provider key).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  runFullE2eV3DryRun,
  runFullE2eV3Live,
  validateFullE2eV3LiveArtifacts,
  runDiscoveryStageDry,
  runDeepResearchStageDry,
  runDiscoveryStageLive,
  runDeepResearchStageLive,
  runWhoHowStageDry,
  runWhoHowStageLive,
  runContactResearchStageDry,
  runContactResearchStageLive,
  runQualificationStageDry,
} from "../../lib/group-demand-intelligence/workflow/gdi-full-e2e-v3-orchestrator-v1.js";
import { GDI_E2E_V3_STAGE } from "../../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-v3-stages-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "../..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return fallback;
}
function flag(name) {
  return process.argv.includes(name);
}

function usage() {
  console.log(`Usage:
  node scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs --stage <STAGE|FULL> --hotel-id <rec...> [--mode DryRun|Live] [options]

Stages: DEMAND_DISCOVERY | OPPORTUNITY_DEEP_RESEARCH | WHO_HOW_RESOLUTION |
        PROFESSIONAL_CONTACT_RESEARCH | COMMERCIAL_QUALIFICATION | FULL | VALIDATE

Options:
  --out-dir <path>
  --opportunity-id <id>
  --continuation-only
  --fixtures <path-to-who-how-scenarios.json>   (DryRun only)
  --opportunities-file <path>                   (Live contact/qualification input)
  --discovery-matrix <path>                     (Live discovery artifact reuse)
  --queue-dossier-dir <path>                    (Live deep-research artifact reuse)
  --authorize-paid-research   (required with --mode Live; refused otherwise)
  --authorize-findymail       (Live Stage 4 Findymail; needs FINDYMAIL_API_KEY)
  --findymail-dry-run         (plan Findymail eligibility only; no spend)
  --authorization-id <id>     (Live WHO/HOW; founder-authorized per hotel/run)
  --hard-ceiling-usd <n>      (Live WHO/HOW hard budget ceiling, > 0)
  --manifest <path>           (Live WHO/HOW tranche manifest)
  --skip-discovery --skip-deep --skip-who-how --skip-contact --skip-qualification
  --skip-staged-progression
  --activate-staged-pointer | --no-activate-staged-pointer   (Live FULL; default activate STAGED)
  --authorize-paid-discovery  (Live: REQUIRED for a NEW HOTEL FULL E2E. With --authorize-paid-research, when
                               discovery/deep artifacts are missing the canonical chain runs (Exa spend,
                               bounded by --hard-ceiling-usd; needs --authorization-id, EXA_API_KEY and
                               GDI_EXA_AGENT_LIVE=1). Artifacts present -> reused, no spend. Not authorized ->
                               HOLD DISCOVERY_ARTIFACTS_MISSING_PAID_NOT_AUTHORIZED. Also default-enables
                               the Leisure/CAD spine.)
  --operational-mode <NEW_HOTEL_FULL_E2E|EXISTING_HOTEL_CONTINUATION|RESUME>
                              (optional override/label. EXISTING_HOTEL_CONTINUATION forbids paid discovery
                               even when authorized; default is derived from routing + --resume)
  --allow-historical-replay   (Live FULL: replay historical matrix; default-enables Leisure/CAD assess at $0)
  --enable-leisure | --no-leisure   (Live FULL: force Leisure standard stage on/off; $0 mock_live only)
  --enable-cad | --no-cad           (Live FULL: force CAD standard stage on/off; classification only)
  --findymail-credit-hard-cap <n>  (Live Stage 4 shared Findymail credit ceiling; default 40)
  --authorize-material-improvement-restage  (Live FULL: restage retained inventory when contacts/timing improve without net-new FQO)
  --persist-findymail-results (Live FULL: Findymail -> Airtable persist plan; needs --authorize-findymail AND
                               separate auth: --authorize-research-persistence + --exact-write-set-authorization,
                               OR --authorize-findymail-persist + --authorize-airtable-production-writes)
  --authorize-findymail-persist --authorize-airtable-production-writes
  --apply-findymail-persist   (also needs env GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES=1)
  --plan-research-persistence                   (Airtable inventory dry-run plan)
  --authorize-research-persistence
  --exact-write-set-authorization <text>
  --apply-research-persistence                  (needs both authorizations + env gate)
  --resume
`);
}

function normalizeStage(raw) {
  const s = String(raw || "FULL").toUpperCase();
  if (s === "FULL" || s === "VALIDATE") return s;
  if (s === GDI_E2E_V3_STAGE.DEMAND_DISCOVERY || s === "DISCOVERY") return "DISCOVERY";
  if (s === GDI_E2E_V3_STAGE.OPPORTUNITY_DEEP_RESEARCH || s === "DEEP") return "DEEP";
  if (
    s === GDI_E2E_V3_STAGE.WHO_HOW_RESOLUTION ||
    s === "WHOHOW" ||
    s === "WHO_HOW"
  ) {
    return "WHO_HOW";
  }
  if (s === GDI_E2E_V3_STAGE.PROFESSIONAL_CONTACT_RESEARCH || s === "CONTACT") {
    return "CONTACT";
  }
  if (s === GDI_E2E_V3_STAGE.COMMERCIAL_QUALIFICATION || s === "QUALIFICATION") {
    return "QUALIFICATION";
  }
  return null;
}

function readJson(p) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/** Live opportunity inputs: explicit file → prior Live contact → prior Live WHO/HOW. */
function loadLiveOpportunities(outDir, hotelId) {
  const oppId = arg("--opportunity-id");
  let opps = [];
  const file = arg("--opportunities-file");
  if (file) {
    const raw = readJson(path.resolve(file));
    opps = Array.isArray(raw) ? raw : raw?.opportunities || [];
  } else {
    for (const name of ["04-contact-research.json", "03-who-how.json"]) {
      const rep = readJson(path.join(outDir, name));
      if (rep?.mode === "Live" && Array.isArray(rep.results)) {
        opps = rep.results.map((r) => r.opportunity).filter(Boolean);
        if (opps.length) break;
      }
    }
  }
  if (oppId) {
    opps = opps.filter(
      (o) => (o.opportunityId || o.id || o.candidateId) === oppId,
    );
  }
  return opps.map((o) => ({ ...o, hotelId: o.hotelId || hotelId }));
}

function liveFullOptions(hotelId, outDir) {
  const ceiling = arg("--hard-ceiling-usd");
  return {
    hotelId,
    mode: "Live",
    outDir,
    authorizePaidResearch: true,
    authorizeFindymail: flag("--authorize-findymail"),
    allowFindymail: flag("--authorize-findymail"),
    findymailDryRun: flag("--findymail-dry-run"),
    authorizationId: arg("--authorization-id"),
    hardCeilingUsd: ceiling == null ? undefined : Number(ceiling),
    whoHowManifestPath: arg("--manifest"),
    opportunityId: arg("--opportunity-id"),
    continuationOnly: flag("--continuation-only"),
    resume: flag("--resume"),
    skipDiscovery: flag("--skip-discovery"),
    skipDeep: flag("--skip-deep"),
    skipWhoHow: flag("--skip-who-how"),
    skipContact: flag("--skip-contact"),
    skipQualification: flag("--skip-qualification"),
    skipStagedProgression: flag("--skip-staged-progression"),
    activateStagedPointer: flag("--no-activate-staged-pointer")
      ? false
      : flag("--activate-staged-pointer")
        ? true
        : undefined,
    authorizePaidDiscovery: flag("--authorize-paid-discovery") ? true : undefined,
    operationalMode: arg("--operational-mode") || undefined,
    allowHistoricalReplay: flag("--allow-historical-replay") ? true : undefined,
    enableLeisure: flag("--no-leisure") ? false : flag("--enable-leisure") ? true : undefined,
    enableCad: flag("--no-cad") ? false : flag("--enable-cad") ? true : undefined,
    persistFindymailResults: flag("--persist-findymail-results"),
    authorizeFindymailPersist: flag("--authorize-findymail-persist"),
    authorizeAirtableProductionWrites: flag("--authorize-airtable-production-writes"),
    applyFindymailPersist: flag("--apply-findymail-persist"),
    findymailCreditHardCap: (() => {
      const raw = arg("--findymail-credit-hard-cap");
      if (raw == null) return undefined;
      const n = Number(raw);
      return Number.isFinite(n) && n > 0 ? n : undefined;
    })(),
    authorizeMaterialImprovementRestage: flag(
      "--authorize-material-improvement-restage",
    )
      ? true
      : undefined,
    planResearchPersistence: flag("--plan-research-persistence"),
    authorizeResearchPersistence: flag("--authorize-research-persistence"),
    exactWriteSetAuthorization: arg("--exact-write-set-authorization"),
    applyResearchPersistence: flag("--apply-research-persistence"),
    discoveryMatrixPath: arg("--discovery-matrix") || undefined,
    queueDossierDir: arg("--queue-dossier-dir") || undefined,
    opportunities: arg("--opportunities-file")
      ? loadLiveOpportunities(outDir, hotelId)
      : undefined,
  };
}

/** Single-stage Live DISCOVERY / DEEP options (paid path needs explicit flags). */
function liveDiscoveryStageOptions(hotelId, outDir) {
  const ceiling = arg("--hard-ceiling-usd");
  return {
    hotelId,
    outDir,
    mode: "Live",
    authorizePaidResearch: true,
    authorizePaidDiscovery: flag("--authorize-paid-discovery") ? true : undefined,
    authorizationId: arg("--authorization-id"),
    hardCeilingUsd: ceiling == null ? undefined : Number(ceiling),
    operationalMode: arg("--operational-mode") || undefined,
    discoveryMatrixPath: arg("--discovery-matrix") || undefined,
    queueDossierDir: arg("--queue-dossier-dir") || undefined,
  };
}

const PARAM_ERROR_CODES =
  /^(HOTEL_ID_REQUIRED|INVALID_OPERATIONAL_MODE|LIVE_REQUIRES_|LIVE_WHO_HOW_REQUIRES_|LIVE_WHO_HOW_MANIFEST|SURFE_FORBIDDEN|FINDYMAIL_REQUIRES_|FINDYMAIL_PERSIST_|RESEARCH_PERSISTENCE_APPLY_)/;
const PROVIDER_REFUSAL_CODES =
  /^(LIVE_WHO_HOW_AUTHORIZATION_REFUSED|MISSING_FINDYMAIL_API_KEY)$/;

function exitForLiveError(err) {
  const code = String(err?.code || "");
  if (PARAM_ERROR_CODES.test(code)) {
    console.error(`Live fail-closed (${code}): ${err.message}`);
    process.exit(2);
  }
  if (PROVIDER_REFUSAL_CODES.test(code)) {
    console.error(`Live provider authorization refused (${code}): ${err.message}`);
    process.exit(4);
  }
  throw err;
}

async function runLive({ stage, hotelId, outDir }) {
  try {
    if (stage === "FULL") {
      const result = await runFullE2eV3Live(liveFullOptions(hotelId, outDir));
      const s = result.summary;
      console.log(
        JSON.stringify(
          {
            ok: true,
            stage,
            mode: "Live",
            hotelId,
            operationalMode: s.operationalMode,
            paidDiscoveryInvoked: s.paidDiscoveryInvoked,
            discoveryOutcome: s.discoveryOutcome,
            deepOutcome: s.deepOutcome,
            paid: s.paid,
            paidBreakdown: s.paidBreakdown,
            plpAssessed: s.plpAssessed,
            researchSpine: s.researchSpine,
            stageInventory: s.stageInventory,
            findymailPersist: s.findymailPersist,
            researchReceipt: s.researchReceipt,
            completeness: s.completeness,
            stagedProgression: s.stagedProgression,
            stagedInventoryValidation: s.stagedInventoryValidation,
            researchPersistence: s.researchPersistence,
            outDir: s.outDir,
          },
          null,
          2,
        ),
      );
      // Authorized paid discovery that fail-closed (env gate / prerequisites) is a
      // provider-authorization refusal, not a silent HOLD.
      if (
        [s.discoveryOutcome?.reason, s.deepOutcome?.reason].some((r) =>
          /^PAID_DISCOVERY_(PREREQUISITES_BLOCKED|CHAIN_BLOCKED)$/.test(String(r || "")),
        )
      ) {
        console.error(
          `Live paid discovery fail-closed (${s.discoveryOutcome?.reason || s.deepOutcome?.reason}); see ${path.join(outDir, "01-discovery.json")}`,
        );
        process.exit(4);
      }
      process.exit(
        s.completeness.failed ||
          s.stagedInventoryValidation.ok === false ||
          s.researchSpine?.failed === true
          ? 1
          : 0,
      );
    }

    if (
      flag("--plan-research-persistence") ||
      flag("--authorize-research-persistence") ||
      flag("--apply-research-persistence") ||
      flag("--persist-findymail-results") ||
      flag("--apply-findymail-persist")
    ) {
      console.error(
        "Research persistence flags are supported only with --stage FULL (Airtable writes are never unlocked by a single stage or by --authorize-paid-research).",
      );
      process.exit(2);
    }

    let report;
    if (stage === "DISCOVERY") {
      report = await runDiscoveryStageLive(hotelId, liveDiscoveryStageOptions(hotelId, outDir));
    } else if (stage === "DEEP") {
      report = await runDeepResearchStageLive(hotelId, liveDiscoveryStageOptions(hotelId, outDir));
    } else if (stage === "WHO_HOW") {
      if (
        !arg("--authorization-id") ||
        !(Number(arg("--hard-ceiling-usd")) > 0) ||
        !arg("--manifest")
      ) {
        console.error(
          "Live WHO/HOW requires --authorization-id, --hard-ceiling-usd (> 0) and --manifest (hotel-specific; no defaults). Alternatively use Invoke-GdiWhoHow.ps1 (Checkpoint A).",
        );
        process.exit(2);
      }
      report = await runWhoHowStageLive(hotelId, {
        outDir,
        authorizePaidResearch: true,
        authorizationId: arg("--authorization-id"),
        hardCeilingUsd: Number(arg("--hard-ceiling-usd")),
        whoHowManifestPath: arg("--manifest"),
        opportunityId: arg("--opportunity-id"),
        continuationOnly: flag("--continuation-only"),
      });
    } else if (stage === "CONTACT") {
      report = await runContactResearchStageLive(
        loadLiveOpportunities(outDir, hotelId),
        {
          hotelId,
          authorizePaidResearch: true,
          authorizeFindymail: flag("--authorize-findymail"),
          allowFindymail: flag("--authorize-findymail"),
          findymailDryRun: flag("--findymail-dry-run"),
        },
      );
    } else if (stage === "QUALIFICATION") {
      const qual = runQualificationStageDry(loadLiveOpportunities(outDir, hotelId), {});
      report = { ...qual, mode: "Live", paid: false };
    } else {
      usage();
      process.exit(2);
    }

    const outFile = path.join(outDir, `stage-${stage.toLowerCase()}-live.json`);
    fs.writeFileSync(outFile, JSON.stringify(report, null, 2));
    console.log(
      JSON.stringify(
        {
          ok: true,
          stage,
          mode: "Live",
          outFile,
          outcome: report.outcome,
          paid: report.paid === true,
          paidDiscoveryInvoked: report.paidDiscoveryInvoked === true,
          reason: report.reason || null,
          routing: report.routing || null,
        },
        null,
        2,
      ),
    );
    if (/^PAID_DISCOVERY_(PREREQUISITES_BLOCKED|CHAIN_BLOCKED)$/.test(String(report.reason || ""))) {
      console.error(`Live paid discovery fail-closed (${report.reason})`);
      process.exit(4);
    }
    process.exit(0);
  } catch (err) {
    exitForLiveError(err);
  }
}

async function main() {
  const stage = normalizeStage(arg("--stage", "FULL"));
  const hotelId = arg("--hotel-id");
  const mode = arg("--mode", "DryRun");
  if (!hotelId || !stage) {
    usage();
    process.exit(2);
  }
  if (mode !== "DryRun" && mode !== "Live") {
    console.error("--mode must be DryRun or Live");
    process.exit(2);
  }
  // VALIDATE is read-only and never needs paid authorization, even with --mode Live.
  const validateOnly = stage === "VALIDATE";
  if (mode === "Live" && !validateOnly && !flag("--authorize-paid-research")) {
    console.error("Live mode requires --authorize-paid-research");
    process.exit(2);
  }
  if (
    mode !== "Live" &&
    (flag("--authorize-paid-discovery") ||
      flag("--persist-findymail-results") ||
      flag("--apply-findymail-persist"))
  ) {
    console.error(
      "--authorize-paid-discovery / --persist-findymail-results / --apply-findymail-persist require --mode Live",
    );
    process.exit(2);
  }
  if (flag("--authorize-paid-research") && mode !== "Live") {
    console.error("--authorize-paid-research requires --mode Live");
    process.exit(2);
  }

  const outDir =
    arg("--out-dir") ||
    path.join(root, "reports/gdi-canonical-full-e2e-v3", hotelId);

  if (mode === "Live") {
    if (validateOnly) {
      const v = validateFullE2eV3LiveArtifacts({ hotelId, outDir });
      console.log(JSON.stringify(v, null, 2));
      process.exit(v.ok ? 0 : 1);
    }
    fs.mkdirSync(outDir, { recursive: true });
    await runLive({ stage, hotelId, outDir });
    return;
  }

  fs.mkdirSync(outDir, { recursive: true });

  const fixturesPath =
    arg("--fixtures") ||
    path.join(root, "fixtures/gdi-full-e2e-v3/who-how-scenarios-v1.json");
  let whoHowFixtures = [];
  if (fs.existsSync(fixturesPath)) {
    const fx = JSON.parse(fs.readFileSync(fixturesPath, "utf8"));
    let scenarios = fx.scenarios || [];
    const oppId = arg("--opportunity-id");
    if (oppId) {
      scenarios = scenarios.filter(
        (s) =>
          s.output?.opportunityId === oppId ||
          s.dossier?.opportunity?.candidateId === oppId ||
          s.id === oppId,
      );
    }
    if (flag("--continuation-only")) {
      scenarios = scenarios.filter((s) =>
        /entry|no_contact|leisure|unrelated|one_entry|two_entry/i.test(s.id),
      );
    }
    whoHowFixtures = scenarios.map((s) => ({
      dossier: s.dossier,
      output: s.output,
      reused: s.reused === true,
    }));
  }

  if (stage === "VALIDATE" || stage === "FULL") {
    const result = await runFullE2eV3DryRun({
      hotelId,
      mode: "DryRun",
      outDir,
      whoHowFixtures,
      continuationOnly: flag("--continuation-only"),
      resume: flag("--resume"),
    });
    console.log(
      JSON.stringify(
        {
          ok: true,
          stage,
          hotelId,
          completeness: result.summary.completeness,
          outDir: result.summary.outDir,
        },
        null,
        2,
      ),
    );
    process.exit(result.summary.completeness.failed ? 1 : 0);
  }

  let report;
  if (stage === "DISCOVERY") {
    report = runDiscoveryStageDry(hotelId, {});
  } else if (stage === "DEEP") {
    report = runDeepResearchStageDry(hotelId, {});
  } else if (stage === "WHO_HOW") {
    report = runWhoHowStageDry(hotelId, {
      whoHowFixtures,
      continuationOnly: flag("--continuation-only"),
    });
  } else if (stage === "CONTACT") {
    const who = runWhoHowStageDry(hotelId, { whoHowFixtures });
    report = await runContactResearchStageDry(
      (who.results || []).map((r) => r.opportunity),
      {
        allowFindymail: flag("--authorize-findymail"),
        findymailDryRun: flag("--authorize-findymail"),
        findymailPermanentComponent: true,
      },
    );
    if (flag("--authorize-findymail")) {
      report.findymailPermanentComponent = true;
      report.findymailNote =
        "DryRun plans Findymail eligibility only; Live enrichment: --mode Live --authorize-paid-research --authorize-findymail (FINDYMAIL_API_KEY required)";
    }
  } else if (stage === "QUALIFICATION") {
    const who = runWhoHowStageDry(hotelId, { whoHowFixtures });
    const contact = await runContactResearchStageDry(
      (who.results || []).map((r) => r.opportunity),
      {},
    );
    report = runQualificationStageDry(
      (contact.results || []).map((r) => r.opportunity),
      {},
    );
  } else {
    usage();
    process.exit(2);
  }

  const outFile = path.join(outDir, `stage-${String(arg("--stage", stage)).toLowerCase()}.json`);
  fs.writeFileSync(outFile, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, stage, outFile, outcome: report.outcome }, null, 2));
  process.exit(0);
}

main().catch((err) => {
  console.error(err?.stack || err);
  process.exit(1);
});
