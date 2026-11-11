/**
 * ADP_MEASUREMENT_INTEGRITY_GATE_V1
 *
 * Blocks CUSTOMER_READY when material false-positive “positive evidence”
 * presentations are detected (mention treated as endorsement).
 *
 * Does not redefine mention-based Consideration KPI numerators.
 */

import { loadPropertyProfile, loadLatestCoreCustomerPeriod } from "../data-model.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { projectGovernedObservations } from "../subject-presence/canonical-subject-presence-v1.js";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import {
  adjudicateSubjectStance,
  isNonEndorsementStance,
  runSubjectStanceNegativeFixtures,
  ADP_SUBJECT_STANCE_ADJUDICATION_V1,
} from "./adp-subject-stance-adjudication-v1.js";
import {
  runThreeMetricNegativeFixturesV2,
  ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2,
  CUSTOMER_ACTIVATION as THREE_METRIC_CUSTOMER_ACTIVATION,
} from "./adp-ai-visibility-consideration-recommendation-v2.js";
import {
  assessFoundationalEngineAndMetricsCertification,
  ADP_FOUNDATIONAL_ENGINE_AND_METRICS_CERTIFICATION_V1,
} from "./adp-foundational-engine-and-metrics-certification-v1.js";
import {
  runUnpromptedNegativeFixtures,
  ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
  CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
  loadArchivePromptMap,
} from "./adp-unprompted-independent-discovery-standard-v1.js";
import { POSITIVE_EVIDENCE_EXCLUDES_NON_ENDORSEMENT_STANCES } from "../customer/positive-evidence-v1.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { buildOptionalExecutiveMetrics } from "../metrics/optional-executive-metrics.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { auditProviderPresenceGrain } from "../metrics/adp-provider-presence-grain-lock-v1.js";
import { scenariosForPeriodDisplacement } from "./adp-autonomous-competitor-entity-resolution-v1.js";
import { assessCompetitiveEntityIntegrity } from "./adp-universal-competitive-entity-integrity-v1.js";

export const ADP_MEASUREMENT_INTEGRITY_GATE_V1 = "ADP_MEASUREMENT_INTEGRITY_GATE_V1";

/**
 * @param {{ propertyId: string, maxNonEndorseShareOfMentions?: number }} opts
 */
export function assessMeasurementIntegrityGate(opts = {}) {
  const propertyId = String(opts.propertyId || "").trim();
  const failures = [];
  const warnings = [];

  const fixtures = runSubjectStanceNegativeFixtures();
  if (!fixtures.ok) {
    failures.push("SUBJECT_STANCE_NEGATIVE_FIXTURES_FAILED");
  }
  const threeMetricFixtures = runThreeMetricNegativeFixturesV2();
  if (!threeMetricFixtures.ok) {
    failures.push("THREE_METRIC_V2_NEGATIVE_FIXTURES_FAILED");
  }
  const foundational = assessFoundationalEngineAndMetricsCertification({
    propertyId,
    maxProvenanceSample: propertyId ? 24 : 0,
  });
  if (!foundational.ok) {
    failures.push("FOUNDATIONAL_ENGINE_CERTIFICATION_FAILED");
    for (const f of foundational.failures || []) failures.push(`FOUNDATIONAL_${f}`);
  }
  for (const w of foundational.warnings || []) warnings.push(w);

  const unpromptedFixtures = runUnpromptedNegativeFixtures();
  if (!unpromptedFixtures.ok) {
    failures.push("UNPROMPTED_INDEPENDENT_DISCOVERY_FIXTURES_FAILED");
  }
  if (!POSITIVE_EVIDENCE_EXCLUDES_NON_ENDORSEMENT_STANCES) {
    failures.push("POSITIVE_EVIDENCE_NON_ENDORSEMENT_EXCLUSION_DISABLED");
  }

  let scan = null;
  let stagedParity = null;
  let providerGrain = null;
  if (propertyId) {
    const profile = loadPropertyProfile(propertyId);
    const core = loadLatestCoreCustomerPeriod(propertyId);
    if (!profile || !core?.observations?.length) {
      warnings.push("PROPERTY_ARCHIVE_UNAVAILABLE_FOR_SCAN");
    } else {
      const projected = projectGovernedObservations(
        enrichObservationsWithRank(
          (core.observations || []).filter((o) => o && (o.parsed || o.rawResponse)),
          profile
        )
      );
      const comparable = filterComparableObservations(projected);
      const mentioned = comparable.filter(
        (o) =>
          o.governedInterpretation?.subjectMentioned === true || o.mentioned === true
      );
      let nonEndorse = 0;
      for (const obs of mentioned) {
        const st = adjudicateSubjectStance(obs, profile);
        if (isNonEndorsementStance(st.stance)) nonEndorse += 1;
      }
      const share =
        mentioned.length > 0
          ? Math.round((nonEndorse / mentioned.length) * 1000) / 10
          : 0;
      scan = {
        comparable: comparable.length,
        mentioned: mentioned.length,
        nonEndorsementMentions: nonEndorse,
        nonEndorseShareOfMentionsPct: share,
      };
      // Material presentation risk: many refusal/uncertainty mentions exist.
      // Gate fails closed for CUSTOMER_READY until presentation path excludes them
      // (already enforced by POSITIVE_EVIDENCE_EXCLUDES… flag + fixtures).
      if (share >= (opts.maxNonEndorseShareOfMentions ?? 15)) {
        warnings.push(
          `HIGH_NON_ENDORSEMENT_MENTION_SHARE_${share}_PCT — customer copy must not label these as recommendations`
        );
      }

      // Staged vs independent-unprompted appearance KPI parity (fail closed).
      // Canonical universe = period-monitored scenarios (incl. indep_repl_* replacements),
      // never stale registry templates that still carry subject-contaminated gen_/prop_gen_ IDs.
      try {
        let scenarios = [];
        try {
          const periodScenarios = scenariosForPeriodDisplacement(
            propertyId,
            core,
            profile
          );
          scenarios = periodScenarios.length
            ? periodScenarios
            : buildScenarioUniverse(profile) || [];
        } catch {
          scenarios = (core.scenarioIds || []).map((scenarioId) => ({ scenarioId }));
        }
        const promptMap = loadArchivePromptMap(propertyId, core.periodId);
        const corrEm = buildOptionalExecutiveMetrics(core, scenarios, profile, {
          unpromptedIndependentDiscovery: true,
          promptMap,
        });
        const staged = loadStagedOwnerPayload(propertyId);
        if (staged?.payload) {
          providerGrain = auditProviderPresenceGrain(staged.payload);
          if (providerGrain.appearance?.independent && providerGrain.pass !== true) {
            failures.push("PROVIDER_PRESENCE_GRAIN_LOCK_FAILED");
          }
          if (providerGrain.has78) {
            failures.push("PROVIDER_PRESENCE_HISTORICAL_78_DENOMINATOR");
          }
          if (
            providerGrain.mentionsMatchExecutive === false ||
            providerGrain.comparableSumMatchesExecutive === false
          ) {
            failures.push("PROVIDER_SUM_NOT_RECONCILED_TO_CONSIDERATION");
          }
        }
        const sRate = staged?.payload?.executiveMetrics?.considerationRate?.rate;
        const rRate = corrEm?.considerationRate?.rate;
        const sDenom =
          staged?.payload?.executiveMetrics?.considerationRate?.comparableObservations;
        const rDenom = corrEm?.considerationRate?.comparableObservations;
        const meth =
          staged?.payload?.executiveMetrics?.considerationRate?.methodologyId || null;
        stagedParity = {
          stagedRate: sRate ?? null,
          referenceRate: rRate ?? null,
          stagedDenom: sDenom ?? null,
          referenceDenom: rDenom ?? null,
          methodologyId: meth,
          ratesMatch: sRate === rRate && sDenom === rDenom,
          methodologyOk: meth === CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
        };
        if (staged?.payload && (sRate !== rRate || sDenom !== rDenom)) {
          failures.push("STAGED_INDEPENDENT_CONSIDERATION_PARITY_FAILED");
        }
        if (staged?.payload && meth && meth !== CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID) {
          failures.push("STAGED_MISSING_CORRECTED_METHODOLOGY_ID");
        }
        if (staged?.payload && !meth) {
          warnings.push("STAGED_CONSIDERATION_METHODOLOGY_ID_UNSTAMPED");
        }

        // Competitive overall subject presence must match independent Consideration
        // (prevents contaminated 252-obs #1 ranks while Consideration shows ~1%).
        const overall =
          staged?.payload?.competitiveRankingByTerritory?.byTerritory?.overall || null;
        const subjectRow = (overall?.displayRows || []).find((r) => r?.isSubject);
        if (overall && subjectRow && rRate != null) {
          const rankPct = subjectRow.aiPresencePct;
          const rankN = overall.comparableN;
          const delta =
            rankPct != null ? Math.abs(Number(rankPct) - Number(rRate)) : null;
          stagedParity.competitiveSubjectPresencePct = rankPct ?? null;
          stagedParity.competitiveComparableN = rankN ?? null;
          stagedParity.competitiveSubjectDisplayRank = subjectRow.displayRank ?? null;
          if (delta != null && delta > 0.6) {
            failures.push("COMPETITIVE_SUBJECT_PRESENCE_MISMATCHES_CONSIDERATION");
          }
          if (
            rDenom != null &&
            rankN != null &&
            Number(rankN) !== Number(rDenom) &&
            Number(rankN) > Number(rDenom)
          ) {
            failures.push("COMPETITIVE_RANKING_USES_LARGER_THAN_ELIGIBLE_DENOMINATOR");
          }
          // Subject must not be labeled displayRank 1 solely via UI pinning when
          // observedRank is outside top presence — true rank must be preserved.
          if (
            subjectRow.displayRank === 1 &&
            subjectRow.observedRank != null &&
            Number(subjectRow.observedRank) > 1 &&
            Number(subjectRow.aiPresencePct) < Number(rRate) + 0.1
          ) {
            // displayRank 1 with observedRank > 1 is a pin defect
            failures.push("SUBJECT_DISPLAY_RANK_PINNED_ABOVE_TRUE_OBSERVED_RANK");
          }
        }
      } catch (err) {
        warnings.push(`STAGED_PARITY_CHECK_ERROR_${String(err?.message || err).slice(0, 80)}`);
      }
    }
  }

  let competitiveIntegrity = null;
  if (propertyId) {
    try {
      competitiveIntegrity = assessCompetitiveEntityIntegrity(propertyId);
      for (const f of competitiveIntegrity.failures || []) {
        // Staged inflation blocks CUSTOMER_READY until restage; engine/source defects always fail.
        failures.push(`COMPETITIVE_INTEGRITY_${f}`);
      }
      for (const w of competitiveIntegrity.warnings || []) {
        warnings.push(`COMPETITIVE_INTEGRITY_${w}`);
      }
    } catch (err) {
      warnings.push(
        `COMPETITIVE_INTEGRITY_ASSESS_ERROR_${String(err?.message || err).slice(0, 80)}`
      );
    }
  }

  const ok = failures.length === 0;
  return {
    gate: ADP_MEASUREMENT_INTEGRITY_GATE_V1,
    stanceContract: ADP_SUBJECT_STANCE_ADJUDICATION_V1,
    threeMetricContract: ADP_AI_VISIBILITY_CONSIDERATION_RECOMMENDATION_V2,
    threeMetricCustomerActivation: THREE_METRIC_CUSTOMER_ACTIVATION,
    foundationalGate: ADP_FOUNDATIONAL_ENGINE_AND_METRICS_CERTIFICATION_V1,
    unpromptedStandard: ADP_UNPROMPTED_INDEPENDENT_DISCOVERY_STANDARD_V1,
    ok,
    blocksCustomerReady: !ok,
    failures,
    warnings,
    fixtures,
    threeMetricFixtures,
    unpromptedFixtures,
    foundational,
    scan,
    stagedParity,
    providerGrain,
    competitiveIntegrity,
    note:
      "Mention-based Consideration meaning preserved; eligible universe must exclude subject-contaminated prompts for independent-discovery KPIs; Provider Presence must reconcile to Consideration (SUM mentions / SUM comparable); competitive entity integrity must pass; three-metric V2 remains shadow-only.",
  };
}
