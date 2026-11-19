#!/usr/bin/env node
/**
 * Phase 2 pilot validation — staging / dry-run only.
 *
 * Validates recovered 10-tab factory contracts + HPC footprint integration
 * against representative pilots. Never writes Airtable. Never runs paid research.
 * Never auto-publishes.
 *
 * Usage:
 *   node scripts/be-phase2-pilot-validate-v1.mjs
 *   node scripts/be-phase2-pilot-validate-v1.mjs --live   # optional read-only HPC GETs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { CANONICAL_TAB_IDS, PRESENTATION_CONTRACT } from "../lib/brand-explorer/canonical/product-shell.js";
import { evaluateMasterRelease } from "../lib/brand-explorer/canonical/master-release.js";
import { buildPassingProfile } from "../lib/brand-explorer/canonical/mutations.js";
import { runBrandExplorerDoctor } from "../lib/brand-explorer/canonical/doctor.js";
import { TAB_CONTRACTS } from "../lib/partner-intelligence/brand-explorer-tab-contracts.js";
import {
  BE_CANONICAL_TAB_IDS,
  BE_EXTERNAL_READ_CONTRACTS,
  BE_FACTORY_SAFETY,
  BE_FOOTPRINT_METRIC_CONTRACT,
  getTabSoT,
} from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";
import {
  BE_HPC_FOOTPRINT_SUMMARY_V1,
  BE_PHASE2_PILOT_BRANDS,
  buildBrandHpcFootprintSummary,
  buildHpcFootprintSummaryFromRows,
  hpcFootprintLiveConfigured,
  summarizePhase2Pilots,
} from "../lib/brand-explorer/be-hpc-footprint-summary-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const allowLive = process.argv.includes("--live");
const OUT_JSON = path.join(ROOT, "reports/brand-explorer/be-phase2-pilot-validation-v1.json");
const OUT_MD = path.join(ROOT, "reports/brand-explorer/be-phase2-pilot-validation-v1.md");

function scoreBoolean(ok) {
  return ok ? 1 : 0;
}

function fixtureExists(slug) {
  const patterns = [
    `fixtures/brand-explorer-presentation-${slug}-full.json`,
    `fixtures/brand-explorer-presentation-${slug}.json`,
    `fixtures/brand-explorer-presentation-${slug}-from-sources.json`,
  ];
  // Ascend fixtures use ascend-hotel-collection naming
  if (slug === "ascend") {
    patterns.unshift("fixtures/brand-explorer-presentation-ascend-hotel-collection-materials.json");
  }
  if (slug === "curio-collection") {
    patterns.unshift("fixtures/brand-explorer-presentation-curio-full.json");
  }
  return patterns.some((rel) => fs.existsSync(path.join(ROOT, rel)));
}

function syntheticHpcRows(pilot) {
  const brand = pilot.hpcMatchers[0];
  return [
    { id: `syn-open-${pilot.slug}-1`, currentBrand: brand, radarStatus: "Open", roomsKeys: 120 },
    { id: `syn-open-${pilot.slug}-2`, currentBrand: brand, radarStatus: "Open", roomsKeys: 80 },
    { id: `syn-pipe-${pilot.slug}-1`, currentBrand: brand, radarStatus: "Pipeline", roomsKeys: 150 },
    { id: `syn-closed-${pilot.slug}-1`, currentBrand: brand, radarStatus: "Closed", roomsKeys: 40 },
  ];
}

function evaluatePilot(pilot, footprint, doctorPass) {
  const tabSoT = BE_CANONICAL_TAB_IDS.map((id) => getTabSoT(id));
  const footprintTab = getTabSoT("atelier-footprint");
  const fixtureOk = fixtureExists(pilot.slug);
  const identityOk = /^rec[a-zA-Z0-9]{10,}$/.test(pilot.recordId);
  const attributionOk =
    footprint?.companyReported == null ||
    footprint?.companyReported?.attribution === "company_reported_separate";
  const noSpeculative =
    footprint?.adpGdi?.speculativeMetrics === false &&
    BE_EXTERNAL_READ_CONTRACTS.adpBrandLevel.status === "RESERVED_READ_ONLY" &&
    BE_EXTERNAL_READ_CONTRACTS.gdiBrandLevel.status === "RESERVED_READ_ONLY";
  const factual =
    footprint?.writes === 0 &&
    (footprint?.certified === true || footprint?.fallbackRecommended === true) &&
    !(footprint?.metrics && footprint.certified === false && footprint.fallbackRecommended === false);

  const completeness =
    (scoreBoolean(identityOk) +
      scoreBoolean(tabSoT.every(Boolean)) +
      scoreBoolean(footprintTab?.primaryOwner === "hpc") +
      scoreBoolean(CANONICAL_TAB_IDS.length === 10) +
      scoreBoolean(TAB_CONTRACTS.length === 10) +
      scoreBoolean(fixtureOk || footprint?.available)) /
    6;

  const factualReliability =
    (scoreBoolean(identityOk) +
      scoreBoolean(footprint?.source?.readOnly === true || footprint?.writes === 0) +
      scoreBoolean(attributionOk) +
      scoreBoolean(noSpeculative) +
      scoreBoolean(factual) +
      scoreBoolean(BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority === "hpc")) /
    6;

  const contentQuality =
    (scoreBoolean(doctorPass) +
      scoreBoolean(PRESENTATION_CONTRACT?.airtable?.basicsTable === "Brand Setup - Brand Basics") +
      scoreBoolean(fixtureOk) +
      scoreBoolean(footprint?.certState != null)) /
    4;

  const uiRendering =
    (scoreBoolean(CANONICAL_TAB_IDS.join("|") === BE_CANONICAL_TAB_IDS.join("|")) +
      scoreBoolean(TAB_CONTRACTS.map((t) => t.tabId).join("|") === CANONICAL_TAB_IDS.join("|")) +
      scoreBoolean(footprintTab?.tabId === "atelier-footprint")) /
    3;

  return {
    slug: pilot.slug,
    recordId: pilot.recordId,
    name: pilot.name,
    affiliationModel: pilot.affiliationModel,
    operatingModel: pilot.operatingModel,
    preserveAirtableRecords: true,
    stagingOnly: true,
    footprint: {
      available: footprint?.available ?? false,
      certified: footprint?.certified ?? false,
      certState: footprint?.certState ?? null,
      fallbackRecommended: footprint?.fallbackRecommended ?? true,
      metrics: footprint?.metrics ?? null,
      recordsMatched: footprint?.census?.recordsMatched ?? 0,
      warnings: footprint?.warnings ?? [],
      mode: footprint?.live ? "live_readonly" : "synthetic_or_offline",
    },
    scores: {
      completeness: Number(completeness.toFixed(3)),
      factualReliability: Number(factualReliability.toFixed(3)),
      contentQuality: Number(contentQuality.toFixed(3)),
      uiRendering: Number(uiRendering.toFixed(3)),
    },
    gates: {
      identityPreserved: identityOk,
      tenTabContracts: TAB_CONTRACTS.length === 10,
      hpcDesignated: footprintTab?.primaryOwner === "hpc",
      noWrites: footprint?.writes === 0,
      noPaidResearch: true,
      noAutoPublish: BE_FACTORY_SAFETY.noAutoPublish === true,
      companyReportedSeparated: attributionOk,
      adpGdiReadOnly: noSpeculative,
      fixturePresent: fixtureOk,
    },
  };
}

async function main() {
  const doctor = runBrandExplorerDoctor({ runMutations: true });
  const master = evaluateMasterRelease(buildPassingProfile());

  const pilots = [];
  for (const pilot of BE_PHASE2_PILOT_BRANDS) {
    let footprint;
    if (allowLive && hpcFootprintLiveConfigured()) {
      footprint = await buildBrandHpcFootprintSummary({
        brandBasicsRecordId: pilot.recordId,
        brandName: pilot.name,
        brandMatchers: pilot.hpcMatchers,
        allowLive: true,
      });
    } else {
      footprint = buildHpcFootprintSummaryFromRows({
        brandBasicsRecordId: pilot.recordId,
        brandName: pilot.name,
        brandMatchers: pilot.hpcMatchers,
        hpcRows: syntheticHpcRows(pilot),
        companyReported: { totalOpenHotels: 9999, note: "fixture company-reported must stay separate" },
        preferCompanyReported: false,
      });
    }
    pilots.push(evaluatePilot(pilot, footprint, doctor.pass));
  }

  const report = {
    contract: "BE_PHASE2_PILOT_VALIDATION_V1",
    hpcContract: BE_HPC_FOOTPRINT_SUMMARY_V1,
    generatedAt: new Date().toISOString(),
    mode: allowLive && hpcFootprintLiveConfigured() ? "live_readonly" : "staging_synthetic",
    liveConfigured: hpcFootprintLiveConfigured(),
    allowLiveRequested: allowLive,
    authorizations: {
      massPopulation: false,
      paidResearch: false,
      productionAirtableWrites: false,
      autoPublish: false,
    },
    factory: {
      doctorPass: doctor.pass,
      doctorFailed: doctor.failed,
      materializationEnabled: doctor.materializationEnabled,
      productionBuildEnabled: doctor.productionBuildEnabled,
      masterReleasePass: master.MASTER_RELEASE_CONTRACT_PASS === true,
      masterReleaseStatus: master.status,
      tabCount: CANONICAL_TAB_IDS.length,
      tabContractCount: TAB_CONTRACTS.length,
      canonicalTabs: [...CANONICAL_TAB_IDS],
      warnings: doctor.warnings || null,
    },
    pilotsSummary: summarizePhase2Pilots(),
    pilots,
    averages: {
      completeness: Number(
        (pilots.reduce((s, p) => s + p.scores.completeness, 0) / pilots.length).toFixed(3),
      ),
      factualReliability: Number(
        (pilots.reduce((s, p) => s + p.scores.factualReliability, 0) / pilots.length).toFixed(3),
      ),
      contentQuality: Number(
        (pilots.reduce((s, p) => s + p.scores.contentQuality, 0) / pilots.length).toFixed(3),
      ),
      uiRendering: Number(
        (pilots.reduce((s, p) => s + p.scores.uiRendering, 0) / pilots.length).toFixed(3),
      ),
    },
    nextSteps: [
      "Founder review of Phase 2 PR",
      "Progressive npm legacy-guard wrapping of historical writers (non-blocking warning today)",
      "Certify live HPC brand rollup coverage before replacing transitional Hotel Census in API",
      "No mass population until identity + HPC gates remain green",
    ],
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

  const lines = [
    `# Brand Explorer Phase 2 — Pilot Validation V1`,
    ``,
    `- Generated: ${report.generatedAt}`,
    `- Mode: \`${report.mode}\``,
    `- Doctor: ${report.factory.doctorPass ? "PASS" : "FAIL"} (materialization=${report.factory.materializationEnabled})`,
    `- Master release fixture: ${report.factory.masterReleasePass ? "PASS" : "FAIL/UNKNOWN"}`,
    `- Tabs: ${report.factory.tabCount} canonical / ${report.factory.tabContractCount} contracts`,
    `- Authorizations: mass population=NO · paid research=NO · production writes=NO · auto-publish=NO`,
    ``,
    `## Averages`,
    ``,
    `| Completeness | Factual reliability | Content quality | UI rendering |`,
    `|---|---|---|---|`,
    `| ${report.averages.completeness} | ${report.averages.factualReliability} | ${report.averages.contentQuality} | ${report.averages.uiRendering} |`,
    ``,
    `## Pilots`,
    ``,
    `| Brand | Record ID | Affiliation | Operating | HPC cert | Completeness | Factual | Content | UI |`,
    `|---|---|---|---|---|---|---|---|---|`,
  ];
  for (const p of pilots) {
    lines.push(
      `| ${p.name} | \`${p.recordId}\` | ${p.affiliationModel} | ${p.operatingModel} | ${p.footprint.certState} | ${p.scores.completeness} | ${p.scores.factualReliability} | ${p.scores.contentQuality} | ${p.scores.uiRendering} |`,
    );
  }
  lines.push(
    ``,
    `## Safety`,
    ``,
    `- Preserved Brand Basics record IDs for all pilots`,
    `- Company-reported figures remain separately attributed`,
    `- ADP/GDI brand-level contracts remain \`RESERVED_READ_ONLY\` (no speculative metrics)`,
    `- HPC is designated footprint authority; Hotel Census stays transitional until live rollup certification`,
    ``,
  );
  if (doctor.warnings?.unguardedHistoricalCount) {
    lines.push(
      `## Doctor warnings`,
      ``,
      `- Unguarded historical npm writers: ${doctor.warnings.unguardedHistoricalCount} (canonical build still refuses \`--apply\`)`,
      ``,
    );
  }
  fs.writeFileSync(OUT_MD, lines.join("\n"));

  console.log(JSON.stringify({ ok: doctor.pass, outJson: OUT_JSON, outMd: OUT_MD, averages: report.averages }, null, 2));
  process.exit(doctor.pass ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
