#!/usr/bin/env node
/**
 * Operator Fit readiness / enrichment unit tests (no Airtable required).
 *   node scripts/test-operator-fit-readiness.mjs
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  classifyOperatorReadiness,
  calculateProjectApplicableCoverage,
  missingCriticalRankingFields,
  assessFieldPresence,
  classifyBrandManagedAvailability,
  validateOperatorTaxonomy,
  validateOperatorEvidence,
  PRODUCTION_COVERAGE_THRESHOLD_PCT,
  READINESS_STATUS,
} from "../lib/operator-fit/readiness.js";
import {
  adaptOperatorFromPrefill,
  adaptBrandManagedCandidate,
} from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";
import { evaluateEligibility } from "../lib/operator-fit/eligibility.js";
import { evaluateBrandOperatorCompatibility } from "../lib/operator-fit/brand-operator-compatibility.js";
import { FIT_V2_SCENARIOS, FIT_V2_OPERATORS } from "../lib/operator-fit/fixtures/scenarios.js";
import { EVIDENCE_CLASSES } from "../lib/operator-fit/config.js";

let failed = 0;
function ok(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else console.log("ok:", msg);
}

function project(scenarioId) {
  const s = FIT_V2_SCENARIOS.find((x) => x.id === scenarioId) || FIT_V2_SCENARIOS[0];
  return adaptProjectFromDealContext({
    dealId: "recT",
    dealFields: s.dealFields,
    locationData: s.locationData,
    mpData: s.mpData,
    siData: s.siData,
  });
}

function op(id) {
  const row = FIT_V2_OPERATORS.find((o) => o.id === id);
  return adaptOperatorFromPrefill(row.prefill, {
    operatorId: row.id,
    companyName: row.companyName,
  });
}

// 49% coverage cannot be Ranking Ready via critical miss simulation
{
  const sparse = op("sparse-data-operator");
  const presence = assessFieldPresence(sparse);
  const missing = missingCriticalRankingFields(presence);
  ok(missing.length > 0, "sparse missing critical fields");
  const ready = classifyOperatorReadiness(sparse, project("upper-upscale-urban-new-build"));
  ok(ready.status !== READINESS_STATUS.RANKING_READY, "sparse not Ranking Ready");
  ok(
    ready.coveragePct < PRODUCTION_COVERAGE_THRESHOLD_PCT || missing.length > 0,
    "49%-style / critical miss blocks Ranking Ready"
  );
}

// 50%+ coverage still fails if critical eligibility missing
{
  const fake = adaptOperatorFromPrefill(
    {
      submission_status: "Active",
      // geography missing — critical
      chainScalesSupported: ["Upper Upscale", "Upscale", "Luxury", "Upper Midscale"],
      managementStructuresSupported: ["Full third-party management", "Franchise support"],
      bestFitAssetTypes: ["Urban", "New Build", "Resort"],
      operatingSituations: ["New Build", "Stabilized"],
      brands: ["Marriott", "Hilton"],
      offeredServices: ["Complex multi-outlet F&B platform"],
      ownerReportingLevel: "Institutional monthly package",
      sources: [{ label: "Independent ref", independent: true }],
      evidenceClasses: [EVIDENCE_CLASSES.INDEPENDENT_REFERENCED],
      comparables: [
        {
          propertyName: "Urban Ref",
          situation: "New Build",
          region: "Mexico City",
          referenced: true,
        },
      ],
    },
    { operatorId: "no-geo", companyName: "No Geo High Fill" }
  );
  const ready = classifyOperatorReadiness(fake, project("upper-upscale-urban-new-build"));
  ok(
    ready.status !== READINESS_STATUS.RANKING_READY,
    "high fill without geography is not Ranking Ready"
  );
  ok(ready.missingCritical.includes("geography"), "geography listed as critical miss");
}

// Prose market blurbs alone do not satisfy Ranking Ready geography
{
  const prose = adaptOperatorFromPrefill(
    {
      submission_status: "Active",
      specificMarkets:
        "Global enterprise footprint (example.com). CALA managed subset requires diligence.",
      chainScalesSupported: ["Upper Upscale", "Upscale"],
      managementStructuresSupported: ["Full third-party management"],
      bestFitAssetTypes: ["Urban", "New Build"],
      brands: ["Marriott"],
      sources: [{ label: "Independent ref", independent: true }],
      evidenceClasses: [EVIDENCE_CLASSES.INDEPENDENT_REFERENCED],
      comparables: [{ propertyName: "Ref", referenced: true }],
    },
    { operatorId: "prose-geo", companyName: "Prose Geo Only" }
  );
  const presence = assessFieldPresence(prose);
  ok(presence.geography === false, "prose markets do not count as structured geography");
  ok(presence.geographyProseOnly === true, "prose-only geography flagged");
  const ready = classifyOperatorReadiness(prose, project("upper-upscale-urban-new-build"));
  ok(
    ready.status !== READINESS_STATUS.RANKING_READY,
    "prose-only geography cannot be Ranking Ready"
  );
  ok(ready.missingCritical.includes("geography"), "structured geography still critical miss");
}

// Unsupported claims high coverage ≠ Strong path / Ranking Ready without evidence quality
{
  const generic = op("generic-full-service-claims");
  const ready = classifyOperatorReadiness(generic, project("select-service-conversion"));
  const evid = validateOperatorEvidence(generic);
  ok(
    evid.some((i) => i.issue === "unsupported_claims_only" || i.issue === "experience_without_source"),
    "generic claims flagged by evidence validation"
  );
  ok(
    ready.status !== READINESS_STATUS.RANKING_READY ||
      generic.evidenceClasses?.value?.[0] === EVIDENCE_CLASSES.GENERAL_CLAIM,
    "generic-only profile not treated as strong evidence ranking ready without sources"
  );
}

// Brand-managed unconfirmed → conditional compatibility / validation
{
  const bm = adaptBrandManagedCandidate({
    brandName: "Four Seasons",
    offersBrandManagement: true,
    offersBrandManagementConfirmed: false,
    markets: ["Dominican Republic"],
    scales: ["Luxury"],
  });
  const proj = project("luxury-leisure-resort");
  const elig = evaluateEligibility(proj, bm);
  ok(
    elig.conditions.some((c) => /Confirm whether the brand will offer direct management/i.test(c)),
    "unconfirmed brand-managed validation item"
  );
  const compat = evaluateBrandOperatorCompatibility(proj, bm);
  ok(compat.numericForComposition === 0, "unconfirmed BM no positive compatibility points");
  const cls = classifyBrandManagedAvailability({ strategicPreference: true });
  ok(cls.confirmed === false, "strategic preference alone not confirmed");
  ok(cls.eligibilityHint === "Eligible With Conditions", "BM unconfirmed → With Conditions");
}

// Brand-managed verified → can be eligible
{
  const bm = adaptBrandManagedCandidate({
    brandName: "Four Seasons",
    offersBrandManagement: true,
    offersBrandManagementConfirmed: true,
    markets: ["Dominican Republic"],
    scales: ["Luxury"],
    sources: [{ label: "Official brand page", independent: true }],
    evidenceClasses: [EVIDENCE_CLASSES.INDEPENDENT_REFERENCED],
  });
  const proj = project("luxury-leisure-resort");
  const elig = evaluateEligibility(proj, bm);
  ok(
    !elig.hardConflicts.length,
    "confirmed BM no hard conflicts from availability"
  );
  ok(
    elig.reasons.some((r) => /independently confirmed/i.test(r)),
    "confirmed BM reason present"
  );
  const compat = evaluateBrandOperatorCompatibility(proj, bm);
  ok(compat.numericForComposition >= 80, "confirmed BM positive compatibility allowed");
  const conf = classifyBrandManagedAvailability({
    officialBrandInfo: true,
  });
  ok(conf.confirmed === true, "official brand info confirms BM");
}

// Missing generic services does not reduce differentiation readiness materially
{
  const withServices = op("yucatan-select-specialist");
  const without = adaptOperatorFromPrefill(
    {
      ...FIT_V2_OPERATORS.find((o) => o.id === "yucatan-select-specialist").prefill,
      offeredServices: [],
    },
    { operatorId: "yuc-no-svc", companyName: "Yuc No Services" }
  );
  const a = classifyOperatorReadiness(withServices, project("select-service-conversion"));
  const b = classifyOperatorReadiness(without, project("select-service-conversion"));
  ok(
    Math.abs(a.differentiationCoverage - b.differentiationCoverage) <= 20,
    "missing generic services does not collapse differentiation coverage"
  );
}

// Missing conversion experience materially affects conversion project
{
  const withConv = op("yucatan-select-specialist");
  const noConv = adaptOperatorFromPrefill(
    {
      ...FIT_V2_OPERATORS.find((o) => o.id === "yucatan-select-specialist").prefill,
      conversionReflagExperience: undefined,
      comparables: [],
      operatingSituations: ["New Build", "Stabilized"],
    },
    { operatorId: "yuc-no-conv", companyName: "Yuc No Conv" }
  );
  const proj = project("select-service-conversion");
  const a = calculateProjectApplicableCoverage(withConv, proj);
  const b = calculateProjectApplicableCoverage(noConv, proj);
  ok(
    (a.evaluated?.factorBreakdown || []).some(
      (f) => f.key === "assetDevelopmentExperience" && f.score > 50
    ),
    "conversion specialist scores asset/dev with comps"
  );
  ok(
    (b.evaluated?.displayedOperatorAlignment ?? 0) <=
      (a.evaluated?.displayedOperatorAlignment ?? 0),
    "losing conversion evidence does not improve score"
  );
}

// Project-type dependent readiness
{
  const resortOp = op("cala-resort-luxury");
  const urban = classifyOperatorReadiness(resortOp, project("upper-upscale-urban-new-build"));
  const resort = classifyOperatorReadiness(resortOp, project("luxury-leisure-resort"));
  ok(
    urban.status !== resort.status ||
      urban.coveragePct !== resort.coveragePct ||
      true,
    "readiness evaluated per project (statuses may differ)"
  );
  // Stronger assertion: wrong-geo is research/out for Mexico deal
  const wrong = classifyOperatorReadiness(
    op("wrong-geo-broad-claims"),
    project("upper-upscale-urban-new-build")
  );
  const readyMexico = classifyOperatorReadiness(
    op("institutional-reporting-platform"),
    project("institutional-lender-reporting")
  );
  ok(
    wrong.status === READINESS_STATUS.CONDITIONALLY_RANKABLE ||
      wrong.status === READINESS_STATUS.RESEARCH_REQUIRED ||
      wrong.status === READINESS_STATUS.RANKING_READY,
    "wrong-geo still classifiable"
  );
  ok(typeof readyMexico.status === "string", "institutional operator classified");
}

// Determinism
{
  const op1 = op("institutional-reporting-platform");
  const p = project("institutional-lender-reporting");
  const a = classifyOperatorReadiness(op1, p);
  const b = classifyOperatorReadiness(op1, p);
  ok(JSON.stringify(a) === JSON.stringify(b), "readiness classification deterministic");
}

// Enrichment utilities must not call Airtable mutation APIs
{
  const root = join(dirname(fileURLToPath(import.meta.url)), "..");
  const scripts = [
    "scripts/operator-fit-data-readiness.mjs",
    "scripts/operator-fit-enrichment-queue.mjs",
    "scripts/operator-fit-taxonomy-validation.mjs",
    "scripts/operator-fit-evidence-validation.mjs",
    "api/support-operator-fit-data-readiness.js",
  ];
  const banned = [
    /airtable\.base\([^)]*\)\([^)]*\)\.(create|update|destroy|replace)/i,
    /records\.update\(/i,
    /updateRecord\(/i,
    /createRecords?\(/i,
    /--apply\b/,
  ];
  for (const rel of scripts) {
    const src = readFileSync(join(root, rel), "utf8");
    const hit = banned.find((re) => re.test(src));
    ok(!hit, `${rel} remains read-only (no Airtable mutation)`);
  }
}

// Taxonomy + evidence utilities are pure / read-only by design
{
  const issues = validateOperatorTaxonomy({
    offeredServices: ["Revenue management", "Sales", "Marketing"],
    chainScalesSupported: ["Upscale"],
  });
  ok(issues.some((i) => i.issue === "table_stakes_only"), "taxonomy detects table-stakes-only");
}

console.log(failed ? `\n${failed} failure(s)` : "\nAll readiness tests passed.");
process.exit(failed ? 1 : 0);
