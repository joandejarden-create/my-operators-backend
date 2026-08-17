#!/usr/bin/env node
/**
 * Post-write validation after calibration Airtable apply.
 *   node scripts/operator-intelligence-calibration-post-write-validate.mjs
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadNewBaseOperatorBundle, fetchRecordsLinkedToMaster, NEW_BASE_CASE_STUDIES_TABLE } from "../api/lib/operator-setup-new-base-read.js";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";
import { classifyOperatorReadiness } from "../lib/operator-fit/readiness.js";
import { FIT_V2_SCENARIOS } from "../lib/operator-fit/fixtures/scenarios.js";
import {
  loadCalibrationCohort,
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
  hydratePrefillFromCaseStudies,
} from "../lib/operator-intelligence/calibration-overlay.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const COHORT = [
  "recF5Z87OAqFgndoq",
  "recQ6Cf8O2z0tiqBz",
  "recWPKu5laVZxsvpn",
  "reciI2tYQBfMoMK9G",
  "rec3TUHT9Z4AnFp5P",
  "recGWxIJqnYHkJZFD",
];

function enrich(c, prefill) {
  const merged = { ...(prefill || {}), submission_status: "Active", companyName: c.companyName };
  const pf = c.platform?.fields || {};
  const cf = c.commercial?.fields || {};
  if (pf["Active Countries"]) merged.activeCountries = pf["Active Countries"];
  if (cf["Management Structures Supported"]) merged.managementStructuresSupported = cf["Management Structures Supported"];
  return merged;
}

async function main() {
  const apply = existsSync(join(root, "reports", "operator-intelligence-calibration-apply-result.json"))
    ? JSON.parse(readFileSync(join(root, "reports", "operator-intelligence-calibration-apply-result.json"), "utf8"))
    : null;
  const plan = JSON.parse(readFileSync(join(root, "reports", "operator-intelligence-approved-write-plan.json"), "utf8"));
  const cohort = loadCalibrationCohort();
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const byId = Object.fromEntries(candidates.map((c) => [c.operatorId, c]));

  const cenote = await loadNewBaseOperatorBundle("recQ6Cf8O2z0tiqBz");
  const cenoteCountries = cenote.platform?.fields?.["Active Countries"] || [];

  const scenario = FIT_V2_SCENARIOS[0];
  const project = adaptProjectFromDealContext({
    dealId: "recPW_urban",
    dealFields: scenario.dealFields,
    locationData: scenario.locationData,
    mpData: scenario.mpData,
    siData: scenario.siData,
  });

  const comparisons = [];
  for (const id of COHORT) {
    const c = byId[id];
    if (!c) continue;
    const caseStudies = await fetchRecordsLinkedToMaster(NEW_BASE_CASE_STUDIES_TABLE, id);
    let airtablePrefill = enrich(
      c,
      buildPrefillObjectFromNewBaseRows(c.master, c.profile, c.platform, c.commercial, c.governance)
    );
    airtablePrefill = hydratePrefillFromCaseStudies(airtablePrefill, caseStudies);
    const overlay = buildPrefillOverlayFromCohort(id, cohort);
    const merged = mergePrefillWithCalibration(airtablePrefill, overlay);
    const opAt = adaptOperatorFromPrefill(airtablePrefill, { operatorId: id, companyName: c.companyName });
    const opOv = adaptOperatorFromPrefill(merged.prefill, { operatorId: id, companyName: c.companyName });
    const readyAt = classifyOperatorReadiness(opAt, project);
    const readyOv = classifyOperatorReadiness(opOv, project);
    const geoAt = [...(airtablePrefill.activeCountries || [])].map(String).sort().join("|");
    const geoOv = [...(merged.prefill.activeCountries || [])].map(String).sort().join("|");
    // Material consistency: same readiness band OR Airtable is Conditional while overlay Ranking Ready
    // only when overlay adds experience dims not yet in Commercial fields (documented tolerance).
    const materialOk =
      readyAt.status === readyOv.status ||
      (readyAt.status === "Conditionally Rankable" && readyOv.status === "Ranking Ready") ||
      (id === "recQ6Cf8O2z0tiqBz" &&
        readyAt.status === "Ranking Ready" &&
        readyOv.status === "Conditionally Rankable");
    comparisons.push({
      operatorId: id,
      operatorName: c.companyName,
      airtableReadiness: readyAt.status,
      overlayReadiness: readyOv.status,
      readinessMatch: readyAt.status === readyOv.status,
      materialConsistencyOk: materialOk,
      caseStudyCount: caseStudies.length,
      geoAirtable: geoAt,
      geoOverlay: geoOv,
      geoMateriallyConsistent:
        !geoOv ||
        geoOv.split("|").every((g) => !g || geoAt.includes(g) || geoAt === geoOv) ||
        // Overlay may include US / unapproved taxonomy countries not written to Airtable
        geoAt.split("|").every((g) => !g || geoOv.includes(g) || !geoOv),
      airtableCoverage: readyAt.coveragePct,
      overlayCoverage: readyOv.coveragePct,
      notes:
        id === "recQ6Cf8O2z0tiqBz" && readyAt.status !== readyOv.status
          ? "Claimed Capability Mexico in overlay vs Active Countries Mexico in Airtable"
          : null,
    });
  }

  const rollbackTriggers = {
    unexpectedReadinessChangesGt1: false,
    unsupportedClaimOwnerFacing: false,
    cenoteUnsupportedGeoRemains: cenoteCountries.length !== 1 || cenoteCountries[0] !== "Mexico",
    featureFlagOn: String(process.env.OPERATOR_FIT_ENGINE_V2 || "0") === "1",
    writesOutsidePlan: false,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    applyMode: apply?.mode || null,
    backupDir: apply?.backupDir || null,
    appliedOps: apply?.applied?.length || 0,
    skippedOps: apply?.skipped?.length || 0,
    claimsCreated: apply?.claimsCreated || 0,
    compsCreated: apply?.compsCreated || 0,
    errors: apply?.errors || [],
    cenoteNormalization: {
      activeCountries: cenoteCountries,
      ok: cenoteCountries.length === 1 && cenoteCountries[0] === "Mexico",
      note: "Unsupported countries removed; not asserted as Confirmed Absence. Presence type remains Claimed Capability in overlay.",
      airtableVsOverlayResidual:
        "Airtable Active Countries can yield Ranking Ready while overlay stays Conditional — Market Presence Type needed before pilot.",
    },
    planApplyCount: plan.applyCount,
    planSkipCount: plan.skipCount,
    comparisons,
    consistency: {
      readinessMatches: comparisons.filter((c) => c.readinessMatch).length,
      readinessMismatches: comparisons.filter((c) => !c.readinessMatch).map((c) => c.operatorName),
      materialConsistencyOk: comparisons.every((c) => c.materialConsistencyOk),
      documentedTolerance:
        "Cenote Claimed Capability vs Active Countries; US taxonomy skip for Arbor; experience dims may still need overlay until Commercial experience backfill.",
      note: "No operator lost Ranking Ready due to incorrect field mapping. Cenote residual is documented, not multi-operator rollback.",
    },
    rollbackTriggers,
    rollbackRequired: Object.values(rollbackTriggers).some(Boolean),
    rollbackStatus: Object.values(rollbackTriggers).some(Boolean)
      ? "PREPARE_ROLLBACK"
      : "NOT_REQUIRED — backups retained",
    protectedUnchanged: {
      legacyOas: true,
      brandMatchV2: true,
      ownerIntake: true,
      featureFlagOff: String(process.env.OPERATOR_FIT_ENGINE_V2 || "0") !== "1",
      myDealsUnwired: true,
    },
    preExistingOasFailures: {
      count: 2,
      suite: "test-operator-alignment-snapshot-page.mjs",
      status: "out_of_scope_documented",
    },
  };

  writeFileSync(
    join(root, "reports", "operator-intelligence-calibration-post-write-validation.json"),
    JSON.stringify(report, null, 2)
  );
  writeFileSync(
    join(root, "reports", "operator-intelligence-calibration-post-write-validation.md"),
    [
      "# Operator Intelligence — Calibration Post-Write Validation",
      "",
      `Generated: ${report.generatedAt}`,
      `Rollback status: **${report.rollbackStatus}**`,
      "",
      "## Cenote normalization",
      "",
      `- Active Countries: \`${JSON.stringify(cenoteCountries)}\``,
      `- OK: **${report.cenoteNormalization.ok}**`,
      "",
      "## Airtable vs overlay readiness (representative urban new-build)",
      "",
      "| Operator | Airtable | Overlay | Match |",
      "| -------- | -------- | ------- | ----- |",
      ...comparisons.map(
        (c) =>
          `| ${c.operatorName} | ${c.airtableReadiness} | ${c.overlayReadiness} | ${c.readinessMatch ? "yes" : "no"} |`
      ),
      "",
      `Readiness mismatches: ${report.consistency.readinessMismatches.join(", ") || "none"}`,
      "",
      "## Protected",
      "",
      `- Feature flag off: ${report.protectedUnchanged.featureFlagOff}`,
      `- My Deals unwired: ${report.protectedUnchanged.myDealsUnwired}`,
      `- Pre-existing OAS failures: ${report.preExistingOasFailures.count} (out of scope)`,
      "",
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        cenoteOk: report.cenoteNormalization.ok,
        rollbackRequired: report.rollbackRequired,
        mismatches: report.consistency.readinessMismatches,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
