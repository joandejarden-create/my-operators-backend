#!/usr/bin/env node
/**
 * Shared CLI for GDI Full E2E V3 stage runners + orchestrator.
 * Default Mode=DryRun. Never embeds API keys.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  runFullE2eV3DryRun,
  runDiscoveryStageDry,
  runDeepResearchStageDry,
  runWhoHowStageDry,
  runContactResearchStageDry,
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
  --fixtures <path-to-who-how-scenarios.json>
  --authorize-paid-research   (Live only; still refused unless Mode=Live)
  --resume
`);
}

async function main() {
  const stage = String(arg("--stage", "FULL")).toUpperCase();
  const hotelId = arg("--hotel-id");
  const mode = arg("--mode", "DryRun");
  if (!hotelId) {
    usage();
    process.exit(2);
  }
  if (mode === "Live" && !flag("--authorize-paid-research")) {
    console.error("Live mode requires --authorize-paid-research");
    process.exit(2);
  }
  if (flag("--authorize-paid-research") && mode !== "Live") {
    console.error("--authorize-paid-research requires --mode Live");
    process.exit(2);
  }
  // This CLI does not spend even when Live flags present — paid adapters not wired here.
  if (mode === "Live") {
    console.error(
      "Live paid execution is not enabled in this V3 CLI build — use DryRun / fixtures. Founder paid tranche runs through existing Exa adapters after IC merge.",
    );
    process.exit(3);
  }

  const outDir =
    arg("--out-dir") ||
    path.join(root, "reports/gdi-canonical-full-e2e-v3", hotelId);
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
  if (stage === GDI_E2E_V3_STAGE.DEMAND_DISCOVERY || stage === "DISCOVERY") {
    report = runDiscoveryStageDry(hotelId, {});
  } else if (
    stage === GDI_E2E_V3_STAGE.OPPORTUNITY_DEEP_RESEARCH ||
    stage === "DEEP"
  ) {
    report = runDeepResearchStageDry(hotelId, {});
  } else if (
    stage === GDI_E2E_V3_STAGE.WHO_HOW_RESOLUTION ||
    stage === "WHOHOW" ||
    stage === "WHO_HOW"
  ) {
    report = runWhoHowStageDry(hotelId, {
      whoHowFixtures,
      continuationOnly: flag("--continuation-only"),
    });
  } else if (
    stage === GDI_E2E_V3_STAGE.PROFESSIONAL_CONTACT_RESEARCH ||
    stage === "CONTACT"
  ) {
    const who = runWhoHowStageDry(hotelId, { whoHowFixtures });
    report = await runContactResearchStageDry(
      (who.results || []).map((r) => r.opportunity),
      {},
    );
  } else if (
    stage === GDI_E2E_V3_STAGE.COMMERCIAL_QUALIFICATION ||
    stage === "QUALIFICATION"
  ) {
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

  const outFile = path.join(outDir, `stage-${stage.toLowerCase()}.json`);
  fs.writeFileSync(outFile, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, stage, outFile, outcome: report.outcome }, null, 2));
  process.exit(0);
}

main().catch((err) => {
  console.error(err?.stack || err);
  process.exit(1);
});
