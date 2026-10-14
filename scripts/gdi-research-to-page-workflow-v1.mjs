#!/usr/bin/env node
/**
 * GDI research-to-page workflow coordinator (v1 + v2 reuse/discovery stages).
 *
 * Single entry point over existing modules. Defaults to dry-run / preview.
 * Publication apply is explicit (--authorize-publish).
 * Webhound is RETIRED — never invoked.
 *
 * Usage:
 *   node scripts/gdi-research-to-page-workflow-v1.mjs --hotel rec0eTEnCqlsGJJt9 --dry-run
 *   node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recnPB3Q02NO8Ma1o --run-discovery --stage discover_gaps
 *   node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage preview
 *
 * Stages (resume via --resume-run <id> + stage ledger):
 *   validate_config → reuse_evidence → discover_gaps → assess_fit →
 *   identify_people → enrich_contacts → evaluate_eligibility →
 *   generate_cards → prepare_manifest → preview_verify → apply_publish
 *
 * v2 wiring inside those stages:
 *   reuse_evidence → reuse_prior_intelligence ledger
 *   discover_gaps → Parallel adapter (replay default; live plan only)
 *   assess_fit → independent hotel fit
 *   enrich_contacts → targeted contact plan (Surfe off by default)
 *   generate_cards → drawer completeness + optional Jev advisory
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createHash, randomBytes } from "node:crypto";
import {
  buildReusePriorIntelligenceLedger,
  reuseLedgerToCsv,
  researchPathSelectionToCsv,
} from "../lib/group-demand-intelligence/workflow/reuse-prior-intelligence-v1.js";
import { peIntelligenceToCsv } from "../lib/group-demand-intelligence/workflow/private-event-intelligence-v1.js";
import { buildPrivateEventIntelligence } from "../lib/group-demand-intelligence/workflow/private-event-intelligence-v1.js";
import { assessIndependentHotelFitBatch } from "../lib/group-demand-intelligence/workflow/independent-hotel-fit-v1.js";
import {
  runParallelDiscoveryStage,
  writeDiscoveryStageArtifacts,
  DISCOVERY_MODE,
} from "../lib/group-demand-intelligence/workflow/parallel-discovery-adapter-v1.js";
import { buildTargetedContactPlan } from "../lib/group-demand-intelligence/workflow/targeted-contact-plan-v1.js";
import {
  reportDrawerCompleteness,
  mapLabCardToDrawerShape,
} from "../lib/group-demand-intelligence/workflow/drawer-content-contract-v1.js";
import {
  runJevAdvisoryShadow,
  writeJevAdvisoryArtifacts,
} from "../lib/group-demand-intelligence/workflow/jev-advisory-shadow-v1.js";
import { WEBHOUND_PROVIDER_STATUS } from "../lib/group-demand-intelligence/workflow/webhound-retired-v1.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}

const STAGES = [
  "validate_config",
  "reuse_evidence",
  "discover_gaps",
  "assess_fit",
  "identify_people",
  "enrich_contacts",
  "evaluate_eligibility",
  "generate_cards",
  "prepare_manifest",
  "preview_verify",
  "apply_publish",
];

const hotelId = String(arg("--hotel", "") || "").trim();
const horizon = String(arg("--horizon", "18m") || "18m").trim();
const budgetUsd = Number(arg("--budget-usd", "5") || 5);
const budgetSearches = Number(arg("--budget-searches", "60") || 60);
const budgetPages = Number(arg("--budget-pages", "100") || 100);
const targetEnv = String(arg("--target-env", "local") || "local").trim();
const adapter = String(arg("--adapter", "filesystem") || "filesystem").trim();
const packageDirArg = arg("--package-dir");
const outDir =
  arg("--out-dir") ||
  path.join(ROOT, "reports/gdi-repeatable-workflow-v1");
const resumeRun = arg("--resume-run");
const stageOnly = arg("--stage");
const authorizePublish = has("--authorize-publish");
const dryRun = !authorizePublish || has("--dry-run");
const runDiscovery = has("--run-discovery");
const authorizePaidDiscovery = has("--authorize-paid-discovery");
const authorizeSurfe = has("--authorize-surfe");
const skipAirtableReuse = has("--skip-airtable-reuse");
const runJevShadow = has("--jev-shadow");

if (!hotelId) {
  console.error("Required: --hotel <canonicalHotelId>");
  process.exit(2);
}

const runId =
  resumeRun ||
  `gdi_workflow_${hotelId.slice(0, 8)}_${new Date()
    .toISOString()
    .replace(/[:.]/g, "-")}_${randomBytes(3).toString("hex")}`;

const runDir = path.join(outDir, "runs", runId);
fs.mkdirSync(runDir, { recursive: true });
const ledgerPath = path.join(runDir, "stage-completion-ledger.json");

function loadLedger() {
  if (fs.existsSync(ledgerPath)) {
    return JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  }
  return {
    runId,
    hotelId,
    horizon,
    targetEnv,
    adapter,
    budget: {
      maxUsd: budgetUsd,
      maxSearches: budgetSearches,
      maxPages: budgetPages,
      usedUsd: 0,
      usedSearches: 0,
      usedPages: 0,
      exhausted: false,
      incompleteCoverageReason: null,
    },
    authorizePublish,
    dryRun,
    startedAt: new Date().toISOString(),
    stages: {},
    remainingManualSteps: [],
    failures: [],
  };
}

function saveLedger(ledger) {
  ledger.updatedAt = new Date().toISOString();
  fs.writeFileSync(ledgerPath, JSON.stringify(ledger, null, 2) + "\n");
}

function mark(ledger, stage, status, detail = {}) {
  ledger.stages[stage] = {
    status,
    at: new Date().toISOString(),
    ...detail,
  };
  saveLedger(ledger);
}

function shaFile(p) {
  if (!fs.existsSync(p)) return null;
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function resolveConfigPath(id) {
  return path.join(ROOT, "config/group-demand-intelligence/hotels", `${id}.json`);
}

function resolveStoreDir(id) {
  const root = process.env.GDI_DATA_ROOT
    ? path.resolve(process.env.GDI_DATA_ROOT)
    : path.join(ROOT, "data/group-demand-intelligence");
  return path.join(root, "hotels", id);
}

function defaultPackageDir(id) {
  if (packageDirArg) return path.resolve(packageDirArg);
  if (id === "recLuxvwwxID7U2B8" || id === "rec0eTEnCqlsGJJt9") {
    return path.join(
      ROOT,
      "reports/gdi-bethesda-process-replication/hotel-review"
    );
  }
  if (id === "recG66DQJKP2c0UNh") {
    return path.join(outDir, "renaissance-package");
  }
  if (id === "recnPB3Q02NO8Ma1o") {
    return path.join(outDir, "packages", "san-diego-bayside-lab-replay");
  }
  return path.join(outDir, "packages", id);
}

const ledger = loadLedger();
ledger.horizonResolved = {
  requested: horizon,
  canonicalPrimary: {
    start: "2026-10-14",
    end: "2028-04-14",
    months: 18,
  },
  note:
    horizon === "24m" || horizon === "24"
      ? "Requested 24m differs from canonical 18m primary used by Bethesda/Mayflower packages — do not silently alter those packages; treat 24m as explicit override for this run only."
      : "Using canonical 18-month primary horizon (2026-10-14 → 2028-04-14).",
};

const pkgDir = defaultPackageDir(hotelId);
const stagesToRun = stageOnly
  ? [stageOnly]
  : STAGES.filter((s) => {
      const prev = ledger.stages[s];
      if (resumeRun && prev?.status === "completed") return false;
      if (s === "apply_publish" && dryRun) return false;
      return true;
    });

console.log(
  JSON.stringify(
    {
      runId,
      hotelId,
      dryRun,
      authorizePublish,
      targetEnv,
      adapter,
      packageDir: pkgDir,
      stagesToRun,
      ledgerPath,
      runDiscovery,
      authorizePaidDiscovery,
      webhound: WEBHOUND_PROVIDER_STATUS.status,
    },
    null,
    2
  )
);

async function runStages() {
for (const stage of stagesToRun) {
  try {
    if (stage === "validate_config") {
      const cfgPath = resolveConfigPath(hotelId);
      if (!fs.existsSync(cfgPath)) {
        mark(ledger, stage, "failed", { error: "hotel_config_missing", cfgPath });
        throw new Error(`Hotel config missing: ${cfgPath}`);
      }
      const cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
      mark(ledger, stage, "completed", {
        configPath: cfgPath,
        displayName: cfg.displayName || null,
        onboarding: cfg.gdiOnboarding?.status || null,
        rooms: cfg.capabilityProfile?.totalGuestrooms || null,
      });
      continue;
    }

    if (stage === "reuse_evidence") {
      const store = resolveStoreDir(hotelId);
      const reuseDir = path.join(runDir, "reuse_prior_intelligence");
      fs.mkdirSync(reuseDir, { recursive: true });
      const ledgerDoc = await buildReusePriorIntelligenceLedger(hotelId, {
        skipAirtable: skipAirtableReuse,
        runDiscovery,
      });
      fs.writeFileSync(
        path.join(reuseDir, "reuse-ledger.json"),
        JSON.stringify(ledgerDoc, null, 2) + "\n"
      );
      fs.writeFileSync(
        path.join(reuseDir, "reuse-ledger.csv"),
        reuseLedgerToCsv(ledgerDoc)
      );
      if (ledgerDoc.researchPathSelection) {
        fs.writeFileSync(
          path.join(reuseDir, "research-path-selection.json"),
          JSON.stringify(ledgerDoc.researchPathSelection, null, 2) + "\n"
        );
        fs.writeFileSync(
          path.join(reuseDir, "research-path-selection.csv"),
          researchPathSelectionToCsv(ledgerDoc.researchPathSelection)
        );
      }
      const peDoc = await buildPrivateEventIntelligence(hotelId, {
        skipAirtable: skipAirtableReuse,
      });
      fs.writeFileSync(
        path.join(reuseDir, "private-event-intelligence.json"),
        JSON.stringify(peDoc, null, 2) + "\n"
      );
      fs.writeFileSync(
        path.join(reuseDir, "private-event-intelligence.csv"),
        peIntelligenceToCsv(peDoc)
      );
      ledger.reusePriorIntelligence = {
        path: path.join(reuseDir, "reuse-ledger.json"),
        counts: ledgerDoc.counts,
        gapsBeforePaidDiscovery: ledgerDoc.gapsBeforePaidDiscovery,
        demandGeneratorFit: ledgerDoc.demandGeneratorFit,
        privateEventIntelligence: ledgerDoc.privateEventIntelligence,
        researchPathSelectionSummary: ledgerDoc.researchPathSelection?.summary || null,
      };
      mark(ledger, stage, "completed", {
        module: "reuse_prior_intelligence_v1",
        storeDir: store,
        opportunityCount: ledgerDoc.opportunityCount,
        counts: ledgerDoc.counts,
        gapsBeforePaidDiscovery: ledgerDoc.gapsBeforePaidDiscovery,
        privateEvent: {
          source: peDoc.source,
          venues: peDoc.counts?.venues,
          fits: peDoc.counts?.fits,
          signals: peDoc.counts?.signals,
          dispositions: peDoc.counts?.byDisposition,
        },
        researchPathSelection: ledgerDoc.researchPathSelection?.summary || null,
        webhound: WEBHOUND_PROVIDER_STATUS,
        note: "Prior intelligence + PE graph + path selection before paid discovery; DG/PE ≠ auto-qualify",
      });
      continue;
    }

    if (stage === "discover_gaps") {
      const discDir = path.join(runDir, "parallel_discovery");
      fs.mkdirSync(discDir, { recursive: true });
      if (!runDiscovery) {
        ledger.budget.incompleteCoverageReason =
          "new_external_discovery_not_run_reuse_only";
        const planOnly = await runParallelDiscoveryStage(hotelId, {
          runDiscovery: false,
          maxUsd: budgetUsd,
          maxTasks: Math.min(8, budgetSearches),
          maxPages: budgetPages,
        });
        writeDiscoveryStageArtifacts(planOnly, discDir);
        mark(ledger, stage, "incomplete_coverage", {
          reason: "reuse_existing_evidence_only",
          module: "parallel_discovery_adapter_v1",
          planPath: path.join(discDir, "bounded-live-discovery-plan.json"),
          note: "Did not claim no demand. Pass --run-discovery for frozen Parallel replay (default) or bounded live plan.",
          budgetRemainingUsd: Math.max(0, budgetUsd - ledger.budget.usedUsd),
          webhound: WEBHOUND_PROVIDER_STATUS,
        });
        ledger.remainingManualSteps.push({
          step: "discover_gaps",
          action:
            "Optionally: --run-discovery for frozen lab replay, or separately authorize a bounded live Parallel run",
        });
        continue;
      }
      const discovery = await runParallelDiscoveryStage(hotelId, {
        runDiscovery: true,
        mode: authorizePaidDiscovery ? DISCOVERY_MODE.LIVE : DISCOVERY_MODE.REPLAY,
        authorizePaidDiscovery,
        maxUsd: Math.min(budgetUsd, 2),
        maxTasks: Math.min(8, budgetSearches),
        maxPages: budgetPages,
      });
      writeDiscoveryStageArtifacts(discovery, discDir);
      ledger.parallelDiscovery = {
        status: discovery.status,
        paidCalls: discovery.paidCalls || 0,
        freshDiscoveryClaimed: discovery.freshDiscoveryClaimed === true,
        candidateCount: (discovery.candidates || []).length,
        path: path.join(discDir, "parallel-discovery-stage.json"),
      };
      if (discovery.status === "completed_replay") {
        mark(ledger, stage, "completed", {
          module: "parallel_discovery_adapter_v1",
          mode: "replay",
          paidCalls: 0,
          freshDiscoveryClaimed: false,
          candidateCount: (discovery.candidates || []).length,
          reviewPackage: discovery.reviewPackage || null,
          note: "Frozen Parallel lab replay — not a fresh discovery success claim",
        });
      } else {
        ledger.budget.incompleteCoverageReason =
          discovery.reason || "discovery_incomplete";
        mark(ledger, stage, "incomplete_coverage", {
          module: "parallel_discovery_adapter_v1",
          reason: discovery.reason,
          paidCalls: discovery.paidCalls || 0,
          note: discovery.note,
        });
        ledger.remainingManualSteps.push({
          step: "discover_gaps",
          action: discovery.note || "Review bounded live plan; do not treat stop as no demand",
        });
      }
      continue;
    }

    if (stage === "assess_fit") {
      const fitDir = path.join(runDir, "independent_hotel_fit");
      fs.mkdirSync(fitDir, { recursive: true });
      const doc = await loadOpportunitiesCanonical(hotelId);
      const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
      // Cross-hotel reuse sample: Bethesda sports candidates when assessing Mayflower
      let crossHotel = [];
      if (hotelId === "rec0eTEnCqlsGJJt9") {
        try {
          const beth = await loadOpportunitiesCanonical("recLuxvwwxID7U2B8");
          crossHotel = (beth?.opportunities || [])
            .filter((o) =>
              /soccer|lacrosse|sports?\s*tournament|soccerplex|stay[- ]to[- ]play/i.test(
                `${o.title || ""} ${o.organizationName || ""} ${o.summaryWhat || ""}`
              )
            )
            .slice(0, 12)
            .map((o) => ({ ...o, hotelId: "recLuxvwwxID7U2B8" }));
        } catch {
          crossHotel = [];
        }
      }
      const batch = assessIndependentHotelFitBatch(
        [...ops.slice(0, 40), ...crossHotel],
        hotelId
      );
      // Also score any replayed discovery candidates
      const discPath = path.join(
        runDir,
        "parallel_discovery",
        "parallel-discovery-stage.json"
      );
      let discoveryFit = null;
      if (fs.existsSync(discPath)) {
        const disc = JSON.parse(fs.readFileSync(discPath, "utf8"));
        discoveryFit = assessIndependentHotelFitBatch(
          disc.candidates || [],
          hotelId
        );
      }
      fs.writeFileSync(
        path.join(fitDir, "independent-hotel-fit.json"),
        JSON.stringify({ store: batch, discoveryReplay: discoveryFit }, null, 2) +
          "\n"
      );
      ledger.independentHotelFit = {
        path: path.join(fitDir, "independent-hotel-fit.json"),
        counts: batch.counts,
        discoveryReplayCounts: discoveryFit?.counts || null,
      };
      mark(ledger, stage, "completed", {
        module: "independent_hotel_fit_v1",
        counts: batch.counts,
        discoveryReplayCounts: discoveryFit?.counts || null,
        note: "Independent per-hotel fit; sports not copied to Mayflower; unsupported orgs not promoted",
      });
      ledger.remainingManualSteps.push({
        step: "assess_fit",
        action:
          "Human shortlist of NEEDS_HUMAN_REVIEW / POSSIBLE_FIT; do not auto-publish fit outcomes",
      });
      continue;
    }

    if (stage === "identify_people") {
      mark(ledger, stage, "completed", {
        mode: "deferred_to_enrich_contacts_targets",
        note: "People identification runs only for targeted contact plan on qualified/enrichment candidates",
      });
      continue;
    }

    if (stage === "enrich_contacts") {
      const contactDir = path.join(runDir, "targeted_contact");
      fs.mkdirSync(contactDir, { recursive: true });
      const doc = await loadOpportunitiesCanonical(hotelId);
      const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
      const reusePath = path.join(
        runDir,
        "reuse_prior_intelligence",
        "reuse-ledger.json"
      );
      const reuseRows = fs.existsSync(reusePath)
        ? JSON.parse(fs.readFileSync(reusePath, "utf8")).rows || []
        : [];
      const plan = buildTargetedContactPlan({
        hotelId,
        opportunities: ops,
        reuseRows,
        authorizeSurfe,
        maxTargets: 25,
      });
      fs.writeFileSync(
        path.join(contactDir, "targeted-contact-plan.json"),
        JSON.stringify(plan, null, 2) + "\n"
      );
      ledger.targetedContact = {
        path: path.join(contactDir, "targeted-contact-plan.json"),
        targetCount: plan.targetCount,
        surfeInvoked: false,
        authorizeSurfe,
      };
      mark(ledger, stage, "completed", {
        module: "targeted_contact_plan_v1",
        targetCount: plan.targetCount,
        surfe: authorizeSurfe
          ? "authorized_but_not_invoked_in_dry_coordinator"
          : "not_authorized",
        order: plan.targets[0]?.resolutionOrder || [
          "reuse_verified_existing",
          "official_sources",
          "native_who",
          "surfe_if_authorized",
        ],
        note: "Plan only — execute contact-resolution modules separately on listed IDs; Surfe off unless --authorize-surfe",
      });
      if (plan.targetCount > 0) {
        ledger.remainingManualSteps.push({
          step: "enrich_contacts",
          action: `Run contact-resolution on ${plan.targetCount} targeted IDs (existing records + official sources first)`,
        });
      }
      continue;
    }

    if (stage === "evaluate_eligibility") {
      const test = spawnSync(
        process.execPath,
        [path.join(ROOT, "scripts/test-gdi-prospect-publication-eligibility-v1.mjs")],
        { cwd: ROOT, encoding: "utf8" }
      );
      mark(ledger, stage, test.status === 0 ? "completed" : "failed", {
        exitCode: test.status,
        note: "Eligibility SoT gate (prospect-publication-eligibility-v1)",
      });
      if (test.status !== 0) throw new Error("eligibility_gate_failed");
      continue;
    }

    if (stage === "generate_cards") {
      const drawerDir = path.join(runDir, "drawer_completeness");
      fs.mkdirSync(drawerDir, { recursive: true });

      if (hotelId === "recG66DQJKP2c0UNh") {
        const builder = path.join(
          outDir,
          "_build-renaissance-package.mjs"
        );
        const r = spawnSync(process.execPath, [builder], {
          cwd: ROOT,
          encoding: "utf8",
        });
        if (r.status !== 0) {
          mark(ledger, stage, "failed", { stderr: r.stderr?.slice(0, 500) });
          throw new Error("renaissance_package_build_failed");
        }
      }

      // San Diego: evaluate frozen lab cards for drawer contract (no package CSV required)
      if (hotelId === "recnPB3Q02NO8Ma1o") {
        const discPath = path.join(
          runDir,
          "parallel_discovery",
          "parallel-discovery-stage.json"
        );
        let cards = [];
        if (fs.existsSync(discPath)) {
          const disc = JSON.parse(fs.readFileSync(discPath, "utf8"));
          cards = (disc.candidates || []).map((c) =>
            mapLabCardToDrawerShape(
              {
                cardId: c.candidateId,
                account: c.organizationName,
                timing: {
                  start: c.eventStartDate,
                  end: c.eventEndDate,
                  raw: c.title,
                },
                demandSignal: c.demandSignal,
                whyWyndhamFits: c.hotelOpportunityThesis,
                sourcingStatus: c.sourcingStatus,
                recommendedFirstAction: c.recommendedAction,
                evidenceLinks: c.evidenceLinks,
                contact: c.primaryContact,
                materialLimitations: c.materialLimitations,
                noveltyVsBaseline: c.provenance?.noveltyVsBaseline,
              },
              hotelId
            )
          );
        }
        const drawerReport = reportDrawerCompleteness(cards, { hotelId });
        fs.writeFileSync(
          path.join(drawerDir, "drawer-completeness.json"),
          JSON.stringify(drawerReport, null, 2) + "\n"
        );
        const jev = runJevAdvisoryShadow({
          hotelId,
          cards: cards.map((c, i) => ({
            cardId: c.id,
            evidenceLinks: c.evidenceLinks,
            whyWyndhamFits: c.hotelOpportunityThesis,
            contact: c.primaryContact,
            noveltyVsBaseline: c.noveltyVsBaseline,
          })),
          invokeJev: runJevShadow,
        });
        writeJevAdvisoryArtifacts(jev, path.join(runDir, "jev_advisory"));
        ledger.drawerCompleteness = {
          path: path.join(drawerDir, "drawer-completeness.json"),
          pass: drawerReport.pass,
          fail: drawerReport.fail,
          total: drawerReport.total,
        };
        mark(ledger, stage, "completed", {
          packageDir: null,
          mode: "san_diego_lab_replay_cards",
          drawerCompleteness: ledger.drawerCompleteness,
          jevAdvisory: "deterministic_only",
          note: "Drawer scored on replayed lab cards; lab ≠ production classifier PASS",
        });
        continue;
      }

      if (!fs.existsSync(path.join(pkgDir, "final-prospect-register.csv"))) {
        // Still score store opportunities for drawer when package missing
        const doc = await loadOpportunitiesCanonical(hotelId);
        const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
        const drawerReport = reportDrawerCompleteness(ops.slice(0, 40), {
          hotelId,
        });
        fs.writeFileSync(
          path.join(drawerDir, "drawer-completeness.json"),
          JSON.stringify(drawerReport, null, 2) + "\n"
        );
        ledger.drawerCompleteness = {
          path: path.join(drawerDir, "drawer-completeness.json"),
          pass: drawerReport.pass,
          fail: drawerReport.fail,
          total: drawerReport.total,
        };
        mark(ledger, stage, "incomplete_coverage", {
          error: "package_missing",
          pkgDir,
          drawerCompleteness: ledger.drawerCompleteness,
          note: "Package CSV missing — drawer scored on store opportunities only",
        });
        ledger.remainingManualSteps.push({
          step: "generate_cards",
          action: `Provide reviewed package at ${pkgDir} or build hotel-specific shortlist`,
        });
        continue;
      }

      const doc = await loadOpportunitiesCanonical(hotelId);
      const ops = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
      const drawerReport = reportDrawerCompleteness(ops, { hotelId });
      fs.writeFileSync(
        path.join(drawerDir, "drawer-completeness.json"),
        JSON.stringify(drawerReport, null, 2) + "\n"
      );
      const jev = runJevAdvisoryShadow({
        hotelId,
        cards: ops.slice(0, 30),
        invokeJev: runJevShadow,
      });
      writeJevAdvisoryArtifacts(jev, path.join(runDir, "jev_advisory"));
      ledger.drawerCompleteness = {
        path: path.join(drawerDir, "drawer-completeness.json"),
        pass: drawerReport.pass,
        fail: drawerReport.fail,
        total: drawerReport.total,
      };
      mark(ledger, stage, "completed", {
        packageDir: pkgDir,
        registerHash: shaFile(path.join(pkgDir, "final-prospect-register.csv")),
        packagesHash: shaFile(path.join(pkgDir, "contact-packages.json")),
        drawerCompleteness: ledger.drawerCompleteness,
        jevAdvisory: "deterministic_shadow_only",
      });
      continue;
    }

    if (stage === "prepare_manifest") {
      if (
        hotelId === "recnPB3Q02NO8Ma1o" &&
        !fs.existsSync(path.join(pkgDir, "final-prospect-register.csv"))
      ) {
        mark(ledger, stage, "incomplete_coverage", {
          reason: "san_diego_lab_replay_not_publication_package",
          note: "Frozen Parallel replay is not a reviewed publication package — no import manifest",
        });
        ledger.remainingManualSteps.push({
          step: "prepare_manifest",
          action:
            "Human-review San Diego lab cards → build reviewed prospect package before publication prep",
        });
        continue;
      }
      const dataRoot = path.join(outDir, "_local-preview");
      fs.mkdirSync(dataRoot, { recursive: true });
      // Seed local preview store from canonical FS hotel pack when present
      const src = resolveStoreDir(hotelId);
      const dst = path.join(dataRoot, "hotels", hotelId);
      fs.mkdirSync(dst, { recursive: true });
      for (const f of ["opportunities.json", "profile.json"]) {
        const from = path.join(src, f);
        if (fs.existsSync(from)) {
          fs.copyFileSync(from, path.join(dst, f));
        }
      }
      const dryArgs = [
        path.join(ROOT, "scripts/gdi-import-reviewed-prospects.mjs"),
        "--hotel",
        hotelId,
        "--dry-run",
        "--adapter",
        adapter === "airtable" && authorizePublish ? "airtable" : "filesystem",
        "--data-root",
        dataRoot,
        "--package-dir",
        pkgDir,
        "--out-dir",
        path.join(runDir, "manifest-dryrun"),
      ];
      const r = spawnSync(process.execPath, dryArgs, {
        cwd: ROOT,
        encoding: "utf8",
        env: { ...process.env, GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1" },
      });
      fs.writeFileSync(
        path.join(runDir, "manifest-dryrun-stdout.txt"),
        r.stdout || ""
      );
      mark(ledger, stage, r.status === 0 ? "completed" : "failed", {
        exitCode: r.status,
        dataRoot,
        note: "Dry-run import manifest prepared; see runDir/manifest-dryrun",
      });
      if (r.status !== 0) throw new Error("manifest_dryrun_failed");
      continue;
    }

    if (stage === "preview_verify") {
      if (
        hotelId === "recnPB3Q02NO8Ma1o" &&
        !fs.existsSync(path.join(pkgDir, "final-prospect-register.csv"))
      ) {
        mark(ledger, stage, "incomplete_coverage", {
          reason: "san_diego_preview_requires_reviewed_package",
          note: "Skipped local preview apply — lab replay cards are not an import package",
        });
        continue;
      }
      const dataRoot = path.join(outDir, "_local-preview");
      const applyArgs = [
        path.join(ROOT, "scripts/gdi-import-reviewed-prospects.mjs"),
        "--hotel",
        hotelId,
        "--apply",
        "--adapter",
        "filesystem",
        "--data-root",
        dataRoot,
        "--package-dir",
        pkgDir,
        "--out-dir",
        path.join(runDir, "preview-apply"),
        "--backup-dir",
        path.join(runDir, "preview-apply/backups"),
      ];
      const r1 = spawnSync(process.execPath, applyArgs, {
        cwd: ROOT,
        encoding: "utf8",
        env: { ...process.env, GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1" },
      });
      const r2 = spawnSync(process.execPath, applyArgs, {
        cwd: ROOT,
        encoding: "utf8",
        env: { ...process.env, GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1" },
      });
      // Local feedback preservation probe (filesystem share-validation)
      const sharePath = path.join(
        dataRoot,
        "hotels",
        hotelId,
        "share-validation.json"
      );
      const probeOpp = "gdi_opp_2026_education_engagement_forum_1";
      const shareDoc = fs.existsSync(sharePath)
        ? JSON.parse(fs.readFileSync(sharePath, "utf8"))
        : { hotelId, items: {} };
      shareDoc.items = shareDoc.items || {};
      shareDoc.items[probeOpp] = {
        opportunityId: probeOpp,
        familiarityStatus: "ALREADY_KNOWN",
        commercialValue: null,
        note: "workflow_v1_local_preview_probe — not a customer sales judgment",
        validatedAt: new Date().toISOString(),
        validator: "workflow_v1_local_preview",
      };
      fs.writeFileSync(sharePath, JSON.stringify(shareDoc, null, 2) + "\n");
      const r3 = spawnSync(process.execPath, applyArgs, {
        cwd: ROOT,
        encoding: "utf8",
        env: { ...process.env, GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "1" },
      });
      const shareAfter = JSON.parse(fs.readFileSync(sharePath, "utf8"));
      const feedbackPreserved = !!shareAfter.items?.[probeOpp];
      mark(ledger, stage, "completed", {
        apply1: r1.status,
        apply2: r2.status,
        apply3_after_feedback: r3.status,
        feedbackPreserved,
        previewDataRoot: dataRoot,
        previewUrlHint: `/group-demand-intelligence.html?hotelId=${hotelId}&embed=1`,
        note: "Local FS preview only — not production publication",
      });
      if (r1.status !== 0 || r2.status !== 0) {
        throw new Error("preview_import_failed");
      }
      continue;
    }

    if (stage === "apply_publish") {
      if (!authorizePublish) {
        mark(ledger, stage, "skipped", { reason: "authorize_publish_required" });
        continue;
      }
      if (targetEnv !== "production") {
        mark(ledger, stage, "failed", {
          error: "target_env_must_be_production_for_publish",
        });
        throw new Error("Set --target-env production with --authorize-publish");
      }
      if (hotelId === "recG66DQJKP2c0UNh") {
        mark(ledger, stage, "blocked", {
          reason: "renaissance_production_publication_not_authorized_in_this_step",
        });
        throw new Error("Renaissance production publish not authorized");
      }
      const r = spawnSync(
        process.execPath,
        [
          path.join(ROOT, "scripts/gdi-import-reviewed-prospects.mjs"),
          "--hotel",
          hotelId,
          "--apply",
          "--adapter",
          "airtable",
          "--package-dir",
          pkgDir,
          "--out-dir",
          path.join(runDir, "production-apply"),
          "--backup-dir",
          path.join(runDir, "production-apply/backups"),
        ],
        {
          cwd: ROOT,
          encoding: "utf8",
          env: {
            ...process.env,
            GDI_OPPORTUNITIES_PERSISTENCE: "airtable",
            GDI_REVIEWED_PROSPECT_PUBLICATION_V1: "0",
          },
        }
      );
      mark(ledger, stage, r.status === 0 ? "completed" : "failed", {
        exitCode: r.status,
        note: "Airtable apply with flag-off import; enable GDI_REVIEWED_PROSPECT_PUBLICATION_V1 separately",
      });
      if (r.status !== 0) throw new Error("production_apply_failed");
      continue;
    }

    mark(ledger, stage, "skipped", { reason: "unknown_stage" });
  } catch (err) {
    ledger.failures.push({
      stage,
      error: err?.message || String(err),
      at: new Date().toISOString(),
    });
    saveLedger(ledger);
    console.error(JSON.stringify({ ok: false, stage, error: err?.message }, null, 2));
    process.exit(1);
  }
}

ledger.finishedAt = new Date().toISOString();
ledger.status = "ok";
saveLedger(ledger);
// Convenience copy for deliverable path
fs.copyFileSync(
  ledgerPath,
  path.join(outDir, "stage-completion-ledger.json")
);
console.log(
  JSON.stringify({ ok: true, runId, ledgerPath, copiedLedger: path.join(outDir, "stage-completion-ledger.json") }, null, 2)
);
}

runStages().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: err?.message || String(err) }, null, 2));
  process.exit(1);
});
