#!/usr/bin/env node
/**
 * Phase 2 pilot validation — staging / dry-run only.
 *
 * Distinguishes structural/contract compliance from dimensions that require
 * live evidence or rendered-app checks (those stay NOT_VALIDATED until proven).
 *
 * Usage:
 *   node scripts/be-phase2-pilot-validate-v1.mjs
 *   node scripts/be-phase2-pilot-validate-v1.mjs --live
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
  BE_HPC_DATA_ORIGINS,
  BE_HPC_FOOTPRINT_CERT_STATES,
  BE_HPC_FOOTPRINT_SUMMARY_V1,
  BE_PHASE2_PILOT_BRANDS,
  buildBrandHpcFootprintSummary,
  buildHpcFootprintSummaryFromRows,
  hpcFootprintLiveConfigured,
  summarizePhase2Pilots,
} from "../lib/brand-explorer/be-hpc-footprint-summary-v1.js";
import { auditLegacyNpmWriteGuards } from "../lib/brand-explorer/canonical/legacy-npm-write-guard-audit.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const allowLive = process.argv.includes("--live");
const OUT_JSON = path.join(ROOT, "reports/brand-explorer/be-phase2-pilot-validation-v1.json");
const OUT_MD = path.join(ROOT, "reports/brand-explorer/be-phase2-pilot-validation-v1.md");
const NOT_VALIDATED = "NOT_VALIDATED";

function fixtureExists(slug) {
  const patterns = [
    `fixtures/brand-explorer-presentation-${slug}-full.json`,
    `fixtures/brand-explorer-presentation-${slug}.json`,
    `fixtures/brand-explorer-presentation-${slug}-from-sources.json`,
  ];
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

function dim(status, detail = "", score = null) {
  return { status, detail, score };
}

function evaluatePilot(pilot, footprint, doctorPass, mode) {
  const footprintTab = getTabSoT("atelier-footprint");
  const fixtureOk = fixtureExists(pilot.slug);
  const identityOk = /^rec[a-zA-Z0-9]{10,}$/.test(pilot.recordId);
  const tabParity =
    CANONICAL_TAB_IDS.join("|") === BE_CANONICAL_TAB_IDS.join("|") &&
    TAB_CONTRACTS.map((t) => t.tabId).join("|") === CANONICAL_TAB_IDS.join("|");
  const isSynthetic = footprint?.dataOrigin === BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE;
  const isLive = footprint?.dataOrigin === BE_HPC_DATA_ORIGINS.LIVE_AIRTABLE;
  const neverCertifiedIfSynthetic =
    !(isSynthetic && footprint?.certState === BE_HPC_FOOTPRINT_CERT_STATES.CERTIFIED);

  // 1) Contract / schema compliance — structural only
  const contractSchemaPass =
    identityOk &&
    tabParity &&
    TAB_CONTRACTS.length === 10 &&
    footprintTab?.primaryOwner === "hpc" &&
    BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority === "hpc" &&
    doctorPass &&
    footprint?.writes === 0 &&
    neverCertifiedIfSynthetic;

  const dimensions = {
    contractSchemaCompliance: dim(
      contractSchemaPass ? "PASS" : "FAIL",
      "Ten-tab shell, SoT ownership, identity id shape, doctor, write=0, synthetic≠CERTIFIED",
      contractSchemaPass ? 1 : 0,
    ),
    profileFieldCompleteness: dim(
      NOT_VALIDATED,
      fixtureOk
        ? "Local presentation fixture present — not a full Airtable field completeness audit"
        : "No local full fixture; live Brand Setup field completeness not run",
      null,
    ),
    evidenceBasedFactualAccuracy: dim(
      NOT_VALIDATED,
      isSynthetic
        ? "Synthetic HPC rows only — factual inventory accuracy not validated"
        : isLive
          ? "Live HPC rows fetched but independent evidence QA / source attribution not completed"
          : "No live inventory evidence in this run",
      null,
    ),
    brandSpecificContentQuality: dim(
      NOT_VALIDATED,
      "Brand-specific copy quality requires human/source QA; not scored from fixtures alone",
      null,
    ),
    renderedApplicationValidation: dim(
      NOT_VALIDATED,
      "Authenticated Brand Explorer UI render not exercised in this Phase 2 staging harness",
      null,
    ),
    liveHpcFootprintVerification: isLive
      ? dim(
          footprint?.certified
            ? "PASS"
            : footprint?.certState === BE_HPC_FOOTPRINT_CERT_STATES.LIVE_PARTIAL_COVERAGE
              ? "PARTIAL"
              : footprint?.certState === BE_HPC_FOOTPRINT_CERT_STATES.LIVE_UNVERIFIED_INVENTORY
                ? "LIVE_UNVERIFIED"
                : "FAIL",
          `dataOrigin=${footprint?.dataOrigin}; certState=${footprint?.certState}; certified=${Boolean(footprint?.certified)}`,
          footprint?.certified ? 1 : null,
        )
      : dim(
          NOT_VALIDATED,
          isSynthetic
            ? "Synthetic fixture validation only — live HPC verification not run"
            : "Live HPC lookup unavailable / not requested",
          null,
        ),
  };

  return {
    slug: pilot.slug,
    recordId: pilot.recordId,
    name: pilot.name,
    affiliationModel: pilot.affiliationModel,
    operatingModel: pilot.operatingModel,
    preserveAirtableRecords: true,
    stagingOnly: true,
    validationMode: mode,
    footprint: {
      available: footprint?.available ?? false,
      certified: footprint?.certified ?? false,
      dataOrigin: footprint?.dataOrigin ?? null,
      certState: footprint?.certState ?? null,
      fallbackRecommended: footprint?.fallbackRecommended ?? true,
      metrics: footprint?.metrics ?? null,
      recordsMatched: footprint?.census?.recordsMatched ?? 0,
      paginationComplete: footprint?.census?.paginationComplete ?? false,
      certificationGates: footprint?.certificationGates ?? null,
      warnings: footprint?.warnings ?? [],
    },
    dimensions,
    structuralScore: {
      contractSchemaCompliance: dimensions.contractSchemaCompliance.score,
      note: "Only structural/contract dimensions are scored. Other dimensions are NOT_VALIDATED until live evidence or rendered-app checks exist.",
    },
    gates: {
      identityPreserved: identityOk,
      tenTabContracts: TAB_CONTRACTS.length === 10,
      hpcDesignated: footprintTab?.primaryOwner === "hpc",
      noWrites: footprint?.writes === 0,
      noPaidResearch: true,
      noAutoPublish: BE_FACTORY_SAFETY.noAutoPublish === true,
      companyReportedSeparated:
        footprint?.companyReported == null ||
        footprint?.companyReported?.attribution === "company_reported_separate",
      adpGdiReadOnly:
        BE_EXTERNAL_READ_CONTRACTS.adpBrandLevel.status === "RESERVED_READ_ONLY" &&
        BE_EXTERNAL_READ_CONTRACTS.gdiBrandLevel.status === "RESERVED_READ_ONLY",
      syntheticNeverCertified: neverCertifiedIfSynthetic,
      fixturePresent: fixtureOk,
      presentationBasicsTable:
        PRESENTATION_CONTRACT?.airtable?.basicsTable === "Brand Setup - Brand Basics",
    },
  };
}

async function main() {
  const doctor = runBrandExplorerDoctor({ runMutations: true });
  const master = evaluateMasterRelease(buildPassingProfile());
  const legacyAudit = auditLegacyNpmWriteGuards(ROOT);

  const mode = allowLive && hpcFootprintLiveConfigured() ? "live_readonly" : "staging_synthetic";
  const pilots = [];
  for (const pilot of BE_PHASE2_PILOT_BRANDS) {
    let footprint;
    if (mode === "live_readonly") {
      footprint = await buildBrandHpcFootprintSummary({
        brandBasicsRecordId: pilot.recordId,
        brandName: pilot.name,
        brandMatchers: pilot.hpcMatchers,
        allowLive: true,
        independentCertification: false,
      });
    } else {
      footprint = buildHpcFootprintSummaryFromRows({
        brandBasicsRecordId: pilot.recordId,
        brandName: pilot.name,
        brandMatchers: pilot.hpcMatchers,
        hpcRows: syntheticHpcRows(pilot),
        companyReported: { totalOpenHotels: 9999, note: "fixture company-reported must stay separate" },
        preferCompanyReported: false,
        dataOrigin: BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE,
        paginationComplete: true,
        independentCertification: false,
      });
    }
    pilots.push(evaluatePilot(pilot, footprint, doctor.pass, mode));
  }

  const structuralPasses = pilots.filter((p) => p.dimensions.contractSchemaCompliance.status === "PASS").length;
  const syntheticCertifiedCount = pilots.filter(
    (p) =>
      p.footprint.dataOrigin === BE_HPC_DATA_ORIGINS.SYNTHETIC_FIXTURE && p.footprint.certified === true,
  ).length;

  const report = {
    contract: "BE_PHASE2_PILOT_VALIDATION_V1",
    hpcContract: BE_HPC_FOOTPRINT_SUMMARY_V1,
    generatedAt: new Date().toISOString(),
    mode,
    liveConfigured: hpcFootprintLiveConfigured(),
    allowLiveRequested: allowLive,
    scoringPolicy: {
      scored: ["contractSchemaCompliance"],
      notValidatedUnlessProven: [
        "profileFieldCompleteness",
        "evidenceBasedFactualAccuracy",
        "brandSpecificContentQuality",
        "renderedApplicationValidation",
        "liveHpcFootprintVerification",
      ],
      note: "Do not treat synthetic fixture or tab-ID alignment as factual/UI validation.",
    },
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
    legacyWriteSafety: legacyAudit,
    pilotsSummary: summarizePhase2Pilots(),
    pilots,
    summary: {
      structuralContractPassCount: structuralPasses,
      pilotCount: pilots.length,
      syntheticCertifiedViolations: syntheticCertifiedCount,
      averagesScoredOnly: {
        contractSchemaCompliance: Number((structuralPasses / pilots.length).toFixed(3)),
      },
      notValidatedDimensions: [
        "profileFieldCompleteness",
        "evidenceBasedFactualAccuracy",
        "brandSpecificContentQuality",
        "renderedApplicationValidation",
        mode === "live_readonly" ? null : "liveHpcFootprintVerification",
      ].filter(Boolean),
    },
    nextSteps: [
      "Demonstrate ≥1 genuine read-only live HPC rollup after merge",
      "Demonstrate ≥1 complete ten-tab profile with real source evidence",
      "Do not request population authorization until live evidence is reported",
    ],
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

  const lines = [
    `# Brand Explorer Phase 2 — Pilot Validation V1 (corrected)`,
    ``,
    `- Generated: ${report.generatedAt}`,
    `- Mode: \`${report.mode}\``,
    `- Doctor: ${report.factory.doctorPass ? "PASS" : "FAIL"} (materialization=${report.factory.materializationEnabled})`,
    `- Master release fixture: ${report.factory.masterReleasePass ? "PASS" : "FAIL"}`,
    `- Synthetic CERTIFIED violations: ${syntheticCertifiedCount}`,
    `- Legacy npm writers guarded: ${legacyAudit.guardedCount}/${legacyAudit.applyCapableCount} (factory-critical unguarded: ${legacyAudit.factoryCriticalUnguarded.length})`,
    `- Authorizations: mass population=NO · paid research=NO · production writes=NO · auto-publish=NO`,
    ``,
    `## Scoring policy`,
    ``,
    `- Scored: contract/schema compliance only`,
    `- \`NOT_VALIDATED\` until proven: profile field completeness, evidence-based factual accuracy, brand-specific content quality, rendered application validation, live HPC footprint verification`,
    ``,
    `## Structural averages (scored only)`,
    ``,
    `| Contract/schema compliance |`,
    `|---|`,
    `| ${report.summary.averagesScoredOnly.contractSchemaCompliance} |`,
    ``,
    `## Pilots`,
    ``,
    `| Brand | Record ID | dataOrigin | certState | certified | Contract | Profile | Factual | Content | UI render | Live HPC |`,
    `|---|---|---|---|---|---|---|---|---|---|---|`,
  ];
  for (const p of pilots) {
    const d = p.dimensions;
    lines.push(
      `| ${p.name} | \`${p.recordId}\` | ${p.footprint.dataOrigin} | ${p.footprint.certState} | ${p.footprint.certified} | ${d.contractSchemaCompliance.status} | ${d.profileFieldCompleteness.status} | ${d.evidenceBasedFactualAccuracy.status} | ${d.brandSpecificContentQuality.status} | ${d.renderedApplicationValidation.status} | ${d.liveHpcFootprintVerification.status} |`,
    );
  }
  lines.push(
    ``,
    `## Safety`,
    ``,
    `- Preserved Brand Basics record IDs`,
    `- Synthetic fixtures never return \`HPC_BRAND_ROLLUP_CERTIFIED\``,
    `- Company-reported figures remain separately attributed`,
    `- ADP/GDI remain \`RESERVED_READ_ONLY\``,
    ``,
  );
  fs.writeFileSync(OUT_MD, lines.join("\n"));

  const ok =
    doctor.pass &&
    syntheticCertifiedCount === 0 &&
    legacyAudit.factoryCriticalUnguarded.length === 0 &&
    structuralPasses === pilots.length;

  console.log(
    JSON.stringify(
      {
        ok,
        outJson: OUT_JSON,
        outMd: OUT_MD,
        summary: report.summary,
        legacy: {
          applyCapable: legacyAudit.applyCapableCount,
          guarded: legacyAudit.guardedCount,
          factoryCriticalUnguarded: legacyAudit.factoryCriticalUnguarded,
        },
      },
      null,
      2,
    ),
  );
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
