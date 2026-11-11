/**
 * ADP_FOUNDATIONAL_ENGINE_AND_METRICS_CERTIFICATION_V1
 *
 * Permanent Full ADP E2E gate: provenance, scenario-type integrity, stance V2,
 * numerator/denominator integrity. Shadow methodology only — does not activate
 * customer-facing V2 metrics.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runThreeMetricNegativeFixturesV2,
  computeThreeMetricRatesV2,
  ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2,
  CUSTOMER_ACTIVATION,
} from "./adp-ai-visibility-consideration-recommendation-v2.js";
import {
  runSubjectStanceNegativeFixtures,
  ADP_SUBJECT_STANCE_ADJUDICATION_V1,
} from "./adp-subject-stance-adjudication-v1.js";
import { POSITIVE_EVIDENCE_EXCLUDES_NON_ENDORSEMENT_STANCES } from "../customer/positive-evidence-v1.js";
import { loadLatestCoreCustomerPeriod, loadPropertyProfile } from "../data-model.js";

export const ADP_FOUNDATIONAL_ENGINE_AND_METRICS_CERTIFICATION_V1 =
  "ADP_FOUNDATIONAL_ENGINE_AND_METRICS_CERTIFICATION_V1";

export const SCENARIO_DISCOVERY_CLASS = Object.freeze({
  D1_INDEPENDENT_DISCOVERY: "D1_INDEPENDENT_DISCOVERY",
  D2_ASSISTED_CONSIDERATION: "D2_ASSISTED_CONSIDERATION",
  D3_COMPETITIVE_COMPARISON: "D3_COMPETITIVE_COMPARISON",
  D4_ATTRIBUTE_VERIFICATION: "D4_ATTRIBUTE_VERIFICATION",
  D5_OTHER: "D5_OTHER",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export function classifyScenarioDiscoveryType(promptText, profile = {}) {
  const p = String(promptText || "");
  const names = [
    profile?.name,
    profile?.displayName,
    ...(profile?.identityAliases || []),
    profile?.brand,
    profile?.affiliation,
  ]
    .filter(Boolean)
    .map((n) => String(n).trim())
    .filter((n) => n.length >= 3);

  const subjectNamed = names.some((n) => new RegExp(escapeRe(n), "i").test(p));
  const affOrBrand = `${profile?.brand || ""} ${profile?.affiliation || ""} ${profile?.name || ""}`;
  const brandNamed = profile?.brand
    ? new RegExp(escapeRe(profile.brand), "i").test(p)
    : /umusic/i.test(p) ||
      (/\bmarriott\b/i.test(affOrBrand) && /\b(?:marriott|bonvoy|jw)\b/i.test(p)) ||
      (/\bwestin\b/i.test(affOrBrand) && /\bwestin\b/i.test(p)) ||
      (/\bsheraton\b/i.test(affOrBrand) && /\bsheraton\b/i.test(p)) ||
      (/\bhilton\b/i.test(affOrBrand) && /\b(?:hilton|honors)\b/i.test(p)) ||
      (/\bhyatt\b/i.test(affOrBrand) && /\bhyatt\b/i.test(p)) ||
      (/\byotel\b/i.test(affOrBrand) && /\byotel\b/i.test(p));
  const competitorCompare =
    (/^\s*compare\b/i.test(p) ||
      /\bcompare(?:\s+\w+){0,6}\s+hotels?\b/i.test(p) ||
      (/\b(?:or|vs\.?|versus|compared to|instead of)\b/i.test(p) &&
        (subjectNamed || brandNamed) &&
        /\b(?:four seasons|ritz|marriott|hilton|edition|westin|mandarin|sofitel|nh |meli[aá]|urso|urban)\b/i.test(
          p
        )));
  const attributeAsk =
    /^(?:does|do|is there|are there|has|have)\b/i.test(p.trim()) ||
    /\b(?:offer|offers|have|has|include|includes|feature|features|rooftop|wifi|wi-fi|pool|spa|meeting)\b/i.test(
      p
    ) &&
      (subjectNamed || brandNamed) &&
      p.length < 160;

  let discoveryClass = SCENARIO_DISCOVERY_CLASS.D1_INDEPENDENT_DISCOVERY;
  if (competitorCompare) discoveryClass = SCENARIO_DISCOVERY_CLASS.D3_COMPETITIVE_COMPARISON;
  else if (attributeAsk && (subjectNamed || brandNamed))
    discoveryClass = SCENARIO_DISCOVERY_CLASS.D4_ATTRIBUTE_VERIFICATION;
  else if (subjectNamed || brandNamed)
    discoveryClass = SCENARIO_DISCOVERY_CLASS.D2_ASSISTED_CONSIDERATION;

  return {
    discoveryClass,
    subjectNamed,
    brandNamed,
    competitorNamedInPrompt: competitorCompare,
    independentlyDiscoverable:
      discoveryClass === SCENARIO_DISCOVERY_CLASS.D1_INDEPENDENT_DISCOVERY,
    leading: /\b(?:best|recommend|ideal|must[- ]stay)\b/i.test(p) && (subjectNamed || brandNamed),
  };
}

function escapeRe(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function loadDurableArchiveObservation(propertyId, periodId, observationId) {
  const fp = path.join(
    ROOT,
    "data/ai-demand-positioning/observation-archive",
    propertyId,
    periodId,
    "observations",
    `${observationId}.json`
  );
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

export function assessObservationProvenance(runtimeObs, archiveObs) {
  const failures = [];
  const warnings = [];
  if (!runtimeObs?.rawResponse && !archiveObs?.originalResponse) {
    failures.push("MISSING_RAW_RESPONSE");
  }
  if (runtimeObs?.dryRun || runtimeObs?.simulated || runtimeObs?.mock) {
    failures.push("SIMULATED_OR_DRY_RUN_RESPONSE");
  }
  if (!runtimeObs?.provider) failures.push("MISSING_PROVIDER");
  if (!runtimeObs?.model && !archiveObs?.model) warnings.push("MISSING_MODEL");
  if (!runtimeObs?.timestamp && !archiveObs?.timestamp) warnings.push("MISSING_TIMESTAMP");
  if (!archiveObs?.providerJobProvenance) warnings.push("MISSING_PROVIDER_JOB_PROVENANCE_OBJECT");
  if (archiveObs?.providerJobProvenance && archiveObs.providerJobProvenance.jobId == null) {
    warnings.push("PROVIDER_RESPONSE_ID_NULL");
  }
  if (!archiveObs?.observationHash) warnings.push("MISSING_OBSERVATION_HASH");
  if (
    runtimeObs?.rawResponse &&
    archiveObs?.originalResponse &&
    String(runtimeObs.rawResponse) !== String(archiveObs.originalResponse)
  ) {
    failures.push("RUNTIME_ARCHIVE_RESPONSE_MISMATCH");
  }
  const prompt = archiveObs?.prompt || runtimeObs?.prompt || runtimeObs?.scenarioQuery || "";
  return {
    ok: failures.length === 0,
    failures,
    warnings,
    prompt,
    hasProviderResponseId: Boolean(archiveObs?.providerJobProvenance?.jobId),
    observationHash: archiveObs?.observationHash || null,
    archivePresent: Boolean(archiveObs),
  };
}

/**
 * @param {{ propertyId?: string, requireArchive?: boolean }} opts
 */
export function assessFoundationalEngineAndMetricsCertification(opts = {}) {
  const failures = [];
  const warnings = [];
  const propertyId = String(opts.propertyId || "").trim();

  const stanceFixtures = runSubjectStanceNegativeFixtures();
  if (!stanceFixtures.ok) failures.push("SUBJECT_STANCE_NEGATIVE_FIXTURES_FAILED");

  const threeMetricFixtures = runThreeMetricNegativeFixturesV2();
  if (!threeMetricFixtures.ok) failures.push("THREE_METRIC_V2_NEGATIVE_FIXTURES_FAILED");

  if (!POSITIVE_EVIDENCE_EXCLUDES_NON_ENDORSEMENT_STANCES) {
    failures.push("POSITIVE_EVIDENCE_NON_ENDORSEMENT_EXCLUSION_DISABLED");
  }

  let propertyScan = null;
  if (propertyId) {
    const profile = loadPropertyProfile(propertyId);
    const core = loadLatestCoreCustomerPeriod(propertyId);
    if (!profile || !core?.observations?.length) {
      warnings.push("PROPERTY_CORE_UNAVAILABLE");
    } else {
      const rates = computeThreeMetricRatesV2(core.observations, profile);
      if (!rates.invariant.ok) failures.push("THREE_METRIC_SUBSET_INVARIANT_FAILED");

      let archiveChecked = 0;
      let archiveOk = 0;
      let missingResponseId = 0;
      let responseMismatch = 0;
      for (const obs of (core.observations || []).slice(0, opts.maxProvenanceSample || 252)) {
        const arch = loadDurableArchiveObservation(
          propertyId,
          core.periodId,
          obs.observationId
        );
        const prov = assessObservationProvenance(obs, arch);
        archiveChecked += 1;
        if (prov.ok) archiveOk += 1;
        if (!prov.hasProviderResponseId) missingResponseId += 1;
        if (prov.failures.includes("RUNTIME_ARCHIVE_RESPONSE_MISMATCH")) responseMismatch += 1;
        if (prov.failures.includes("SIMULATED_OR_DRY_RUN_RESPONSE")) {
          failures.push(`SIMULATED_RESPONSE_${obs.observationId}`);
        }
      }
      propertyScan = {
        periodId: core.periodId,
        observationCount: core.observations.length,
        threeMetricRates: rates.rates,
        threeMetricCounts: rates.counts,
        invariantOk: rates.invariant.ok,
        provenanceSample: {
          checked: archiveChecked,
          ok: archiveOk,
          missingProviderResponseId: missingResponseId,
          responseMismatch,
        },
      };
      if (responseMismatch > 0) failures.push("RUNTIME_ARCHIVE_RESPONSE_MISMATCHES");
      if (missingResponseId === archiveChecked) {
        warnings.push(
          "ALL_OBSERVATIONS_LACK_PROVIDER_RESPONSE_ID — budget ledger + archive hash required as secondary provenance"
        );
      }
    }
  }

  const ok = failures.length === 0;
  return {
    gate: ADP_FOUNDATIONAL_ENGINE_AND_METRICS_CERTIFICATION_V1,
    stanceContract: ADP_SUBJECT_STANCE_ADJUDICATION_V1,
    threeMetricContract: ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2,
    customerActivation: CUSTOMER_ACTIVATION,
    ok,
    blocksCustomerReady: !ok,
    failures,
    warnings,
    stanceFixtures,
    threeMetricFixtures,
    propertyScan,
    note: "Fail-closed on material integrity defects. V2 metrics remain shadow-only until founder activation.",
  };
}
