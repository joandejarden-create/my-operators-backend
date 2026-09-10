/**
 * Copy selected ADP / sample intelligence into Leak Audit records (read-only source).
 * Never writes production ADP, Census, Brand Explorer, or Operator Explorer.
 *
 * npm run adp-leak-audit-copy-from-adp-v1 -- --dry-run
 * npm run adp-leak-audit-copy-from-adp-v1 -- --hotelName "Cambridge Beaches Resort & Spa" --mode leak_audit_lite --scope 15x4 --output filesystem
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  createLeakAuditStore,
  loadLeakAuditSampleReport,
  generateSinglePropertyLeakAuditReport,
} from "./index.js";
import { createLeakAuditRepository } from "./leak-audit-repository.js";
import { LEAK_AUDIT_LITE_COST_CONTROLS } from "./research-mode-lite-v1.js";
import { buildCoverMetaLine, buildScenariosMonitoredLabel } from "./research-mode-lite-v1.js";

function parseArgs(argv) {
  const get = (flag) => {
    const i = argv.indexOf(flag);
    if (i === -1) return null;
    return argv[i + 1] || null;
  };
  const apply = argv.includes("--apply");
  const dryRun = argv.includes("--dry-run") || !apply;
  const scope = String(get("--scope") || "15x4").toLowerCase();
  const mode = String(get("--mode") || "leak_audit_lite");
  const output = String(get("--output") || "filesystem").toLowerCase();
  return {
    apply: apply && !argv.includes("--dry-run"),
    dryRun,
    hotelName: get("--hotelName") || get("--hotel-name"),
    sourceHotelId: get("--sourceHotelId") || get("--source-hotel-id"),
    mode,
    scope,
    output,
  };
}

function parseScope(scope) {
  const m = String(scope || "15x4").match(/(\d+)\s*[x×]\s*(\d+)/i);
  if (!m) {
    return {
      maxScenarios: LEAK_AUDIT_LITE_COST_CONTROLS.maxScenarios,
      maxProviders: LEAK_AUDIT_LITE_COST_CONTROLS.maxProviders,
      maxObservations: LEAK_AUDIT_LITE_COST_CONTROLS.maxObservations,
      label: "15 × 4",
    };
  }
  const maxScenarios = Number(m[1]);
  const maxProviders = Number(m[2]);
  return {
    maxScenarios,
    maxProviders,
    maxObservations: maxScenarios * maxProviders,
    label: `${maxScenarios} × ${maxProviders}`,
  };
}

export async function copyFromAdpToLeakAudit(options = {}) {
  const dryRun = options.dryRun !== false;
  const sample = loadLeakAuditSampleReport();
  const hotelName = options.hotelName || sample.hotelName;
  const mode = options.mode || "leak_audit_lite";
  const scope = parseScope(options.scope || "15x4");
  const output = options.output || "filesystem";

  const plan = {
    source:
      options.sourceHotelId ||
      "fixtures/ai-demand-positioning/leak-audit-sample-report-v1.json",
    sourceMode: options.sourceHotelId
      ? "published_adp_shaped_copy_read_only"
      : "sample_seed_from_adp_shaped_fixture",
    sourceHotelId: options.sourceHotelId || null,
    hotelName,
    mode,
    scope: scope.label,
    maxScenarios: scope.maxScenarios,
    maxProviders: scope.maxProviders,
    maxObservations: scope.maxObservations,
    output,
    metrics: (sample.executiveSignals || []).length,
    competitors: (sample.competitorDisplacementRank?.rows || []).length,
    evidence: (sample.evidenceLibrary || []).length,
    actions: (sample.fixes || []).length,
    productionAdpWrites: 0,
    note:
      "Phase 2A copies report-ready fields into Leak Audit IDs only. Full prompts and production IDs are not copied into client payloads.",
  };

  if (dryRun) {
    return { ok: true, dryRun: true, plan, applied: false, storageBackend: output };
  }

  let store;
  let root;
  let storageBackend = "filesystem";

  if (output === "airtable") {
    const repo = createLeakAuditRepository({
      root: options.root,
      dualWrite: true,
    });
    store = repo;
    root = repo.root;
    storageBackend = repo.storageBackend;
  } else {
    root =
      options.root ||
      join(
        process.cwd(),
        "data/ai-demand-positioning/leak-audit/live",
        `copy_${Date.now().toString(36)}`
      );
    mkdirSync(root, { recursive: true });
    store = createLeakAuditStore({ root });
    storageBackend = "filesystem";
  }

  const request = store.createRequest({
    hotelName,
    hotelWebsite: sample.hotelWebsite || "https://www.cambridgebeaches.com",
    city: sample.hotelLocation?.split(",")[0]?.trim() || "Sandys Parish",
    country: "Bermuda",
    contactName: "Demo Contact",
    contactEmail: "demo@dealality.example",
    source: "sample",
    audienceType: "owner",
    prioritySource: "inferred_from_results",
    researchMode: mode,
    notes: "Copied from ADP-shaped source into Leak Audit namespace (read-only source)",
  });
  store.updateRequest(request.id, { status: "approved" });

  const generated = await generateSinglePropertyLeakAuditReport(store, {
    requestId: request.id,
    mode: "sample_seed",
  });

  // Stamp lite scope labels onto run/report
  const scenariosMonitored = buildScenariosMonitoredLabel({
    scenariosRun: scope.maxScenarios,
    providersRun: scope.maxProviders,
  });
  const coverMetaLine = buildCoverMetaLine({
    providers: scope.maxProviders,
    scenarios: scope.maxScenarios,
    observations: scope.maxObservations,
    actionItems: 3,
  });

  const run = store.updateRun(generated.run.id, {
    researchMode: mode,
    maxScenarios: scope.maxScenarios,
    maxProviders: scope.maxProviders,
    maxObservations: scope.maxObservations,
    scenarioCount: scope.maxScenarios,
    providerCount: scope.maxProviders,
    actualScenariosRun: scope.maxScenarios,
    actualProvidersRun: scope.maxProviders,
    actualObservations: scope.maxObservations,
    totalObservations: scope.maxObservations,
    scenariosMonitoredLabel: scenariosMonitored.value,
    observationsLabel: scenariosMonitored.valueSubLabel,
    promptSetVersion: "leak_audit_lite_v1",
    completenessFlag: "adequate",
  });

  const report = store.updateReport(generated.report.id, {
    researchMode: mode,
    scenariosMonitored,
    scenarioSummaryLabel: scenariosMonitored.value,
    diagnosticScopeLabel: `Limited diagnostic · ${scope.maxScenarios} traveler scenarios across ${scope.maxProviders} AI providers`,
    coverMeta: {
      ...(generated.report.coverMeta || {}),
      providers: scope.maxProviders,
      scenarios: scope.maxScenarios,
      observations: scope.maxObservations,
      actionItems: 3,
      coverMetaLine,
    },
    coverMetaLine,
  });

  const summary = {
    ok: true,
    dryRun: false,
    plan,
    applied: true,
    root,
    storageBackend,
    requestId: request.id,
    hotelAuditId: generated.hotel.id,
    runId: run.id,
    reportId: report.id,
    shareToken: report.shareToken,
    shareUrl: report.shareUrl || generated.shareUrl,
    productionAdpTouched: false,
  };

  writeFileSync(
    join(root, "copy-from-adp-summary.json"),
    JSON.stringify(summary, null, 2),
    "utf8"
  );
  return summary;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const result = await copyFromAdpToLeakAudit({
    dryRun: args.dryRun,
    hotelName: args.hotelName,
    sourceHotelId: args.sourceHotelId,
    mode: args.mode,
    scope: args.scope,
    output: args.output,
  });
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exitCode = 1;
}

const isMain =
  process.argv[1] &&
  String(process.argv[1]).replace(/\\/g, "/").endsWith("copy-from-adp-v1.js");
if (isMain) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
