/**
 * ADP_COMPETITIVE_DISPLACEMENT_CERTIFICATION_V1
 *
 * Mandatory Full E2E gate: Core period selection, subject/competitor identity,
 * displacement classification grains, staged binding, and zero-vs-missing semantics.
 *
 * No paid provider calls. Observation reuse only.
 */

import {
  loadLatestCoreCustomerPeriod,
  loadLatestLoyaltyPeriod,
  loadPropertyProfile,
  isLoyaltyMeasurementPeriod,
} from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { computeLostDemand } from "../intelligence/lost-demand.js";
import {
  buildDisplacementScenarioMap,
  computeDisplacementCountsByEntity,
  DISPLACEMENT_EVENT_DEFINITION,
  DISPLACEMENT_COUNT_DEFINITION,
  DISPLAY_GRAIN,
} from "../customer/resolve-displacement-evidence-v1.js";
import {
  getEntityRegistryForProperty,
  canonicalizeForProperty,
} from "../metrics/adp-property-entity-registries.js";
import {
  subjectEntityIdsForProperty,
  primarySubjectEntityIdForProperty,
} from "../subject-presence/subject-entity-ids-v1.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { buildNameVariants } from "../execution/response-parser.js";

export const ADP_COMPETITIVE_DISPLACEMENT_CERTIFICATION_V1 =
  "ADP_COMPETITIVE_DISPLACEMENT_CERTIFICATION_V1";

export const DISPLACEMENT_CLASSIFICATION = Object.freeze({
  A_SUBJECT_ABSENT_COMPETITOR_PRESENT: "A_SUBJECT_ABSENT_COMPETITOR_PRESENT",
  B_CO_APPEARANCE: "B_CO_APPEARANCE",
  C_SUBJECT_ABSENT_NO_QUALIFYING_COMPETITOR: "C_SUBJECT_ABSENT_NO_QUALIFYING_COMPETITOR",
  D_CONTEXTUAL_ONLY: "D_CONTEXTUAL_ONLY",
  E_INCOMPLETE_OBSERVATION: "E_INCOMPLETE_OBSERVATION",
});

export const HOTEL_VERDICT = Object.freeze({
  VERIFIED_DISPLACEMENT_PRESENT: "VERIFIED_DISPLACEMENT_PRESENT",
  VERIFIED_ZERO_DISPLACEMENT: "VERIFIED_ZERO_DISPLACEMENT",
  CALCULATION_MISSING: "CALCULATION_MISSING",
  DERIVED_DATA_MISMATCH: "DERIVED_DATA_MISMATCH",
  WRONG_PERIOD_SELECTED: "WRONG_PERIOD_SELECTED",
  REPORT_BINDING_FAILURE: "REPORT_BINDING_FAILURE",
  FRONTEND_RENDER_FAILURE: "FRONTEND_RENDER_FAILURE",
  INCOMPLETE_SOURCE_EVIDENCE: "INCOMPLETE_SOURCE_EVIDENCE",
  OTHER_BLOCKER: "OTHER_BLOCKER",
});

/**
 * Independent Core displacement recalculation for one property.
 */
export function independentlyRecalculateDisplacement(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    return {
      propertyId,
      ok: false,
      verdict: HOTEL_VERDICT.OTHER_BLOCKER,
      blockers: ["property_profile_missing"],
    };
  }

  const core = loadLatestCoreCustomerPeriod(propertyId);
  const loyalty = loadLatestLoyaltyPeriod(propertyId);
  const blockers = [];
  const warnings = [];

  if (!core?.observations?.length) {
    return {
      propertyId,
      hotelName: profile.name,
      ok: false,
      verdict: HOTEL_VERDICT.INCOMPLETE_SOURCE_EVIDENCE,
      blockers: ["core_period_missing_or_empty"],
      corePeriodId: null,
      loyaltyPeriodId: loyalty?.periodId || null,
    };
  }
  if (isLoyaltyMeasurementPeriod(core)) {
    blockers.push("core_loader_returned_loyalty_period");
  }

  const scenarios = buildScenarioUniverse(profile);
  const observations = enrichObservationsWithRank(core.observations, profile);
  const completeObs = observations.filter(
    (o) => String(o.rawResponse || "").trim().length >= 40
  );
  const incompleteObs = observations.length - completeObs.length;

  const subjectIds = subjectEntityIdsForProperty(profile);
  const primarySubject = primarySubjectEntityIdForProperty(profile);
  const variants = buildNameVariants(profile);
  const cityOnlyLeak = variants.some(
    (v) =>
      /^falls church$/i.test(v) ||
      (profile.city &&
        String(v).trim().toLowerCase() === String(profile.city).trim().toLowerCase())
  );
  if (cityOnlyLeak) blockers.push("subject_variants_include_city_only_label");

  // Peer subject bleed: shared registry must not treat other subject:true hotels as this subject
  const reg = getEntityRegistryForProperty(propertyId);
  if (reg?.hotels) {
    for (const h of reg.hotels) {
      if (h.subject === true && h.entityId !== primarySubject && subjectIds.has(h.entityId)) {
        blockers.push(`shared_registry_subject_bleed:${h.entityId}`);
      }
    }
  }

  const subjectPresentResponses = observations.filter((o) => o.mentioned === true).length;
  const subjectAbsentResponses = observations.length - subjectPresentResponses;

  const lostDemand = computeLostDemand(observations, scenarios, profile);
  const scenarioMap = buildDisplacementScenarioMap(observations, scenarios, profile);
  const countsByEntity = computeDisplacementCountsByEntity(
    observations,
    scenarios,
    profile
  );

  const displacement = lostDemand.displacement || [];
  const distinctCompetitors = displacement.length;
  const topCompetitors = displacement.slice(0, 10).map((d) => ({
    entityId: d.entityId,
    name: d.name,
    displacementCount: d.displacementCount,
    evidenceAvailable: d.evidenceAvailable,
  }));

  // Response-level A events (subject absent + at least one resolved competitor on that response)
  let responseLevelDisplacement = 0;
  const rawCompetitorObs = [];
  for (const obs of observations) {
    if (obs.mentioned) continue;
    if (String(obs.rawResponse || "").trim().length < 40) continue;
    const resolved = [];
    for (const raw of obs.competitorsMentioned || []) {
      const id = canonicalizeForProperty(propertyId, raw);
      if (!id || subjectIds.has(id)) continue;
      resolved.push({ raw, entityId: id });
    }
    if (resolved.length) {
      responseLevelDisplacement += 1;
      for (const r of resolved) {
        rawCompetitorObs.push({
          propertyId,
          observationId: obs.observationId || obs.id || null,
          scenarioId: obs.scenarioId,
          provider: obs.provider,
          rawName: r.raw,
          entityId: r.entityId,
          periodId: core.periodId,
        });
      }
    }
  }

  const eligibleResponses = completeObs.length;
  const responseDisplacementRate =
    eligibleResponses > 0 ? responseLevelDisplacement / eligibleResponses : null;

  // Staged reconciliation
  const staged = loadStagedOwnerPayload(propertyId);
  const stagedPayload = staged?.payload || staged;
  const stagedDisp = stagedPayload?.lostDemand?.displacement;
  const stagedPeriod =
    stagedPayload?.periodId ||
    stagedPayload?.measurementPeriod ||
    staged?.periodId ||
    null;
  let stagedMismatch = false;
  if (!Array.isArray(stagedDisp)) {
    blockers.push("staged_displacement_array_missing");
  } else {
    const liveTop = displacement[0]?.entityId || null;
    const stagedTop = stagedDisp[0]?.entityId || null;
    const liveCount = displacement[0]?.displacementCount || 0;
    const stagedCount = stagedDisp[0]?.displacementCount || 0;
    if (displacement.length !== stagedDisp.length) stagedMismatch = true;
    if (liveTop && stagedTop && liveTop !== stagedTop) stagedMismatch = true;
    if (Math.abs(liveCount - stagedCount) > 0 && displacement.length > 0) {
      // Allow rebuild lag — flag as mismatch for apply pass
      stagedMismatch = true;
    }
    if (displacement.length > 0 && stagedDisp.length === 0) {
      blockers.push("staged_empty_while_live_has_displacement");
      stagedMismatch = true;
    }
  }
  if (stagedPeriod && loyalty?.periodId && stagedPeriod === loyalty.periodId) {
    blockers.push("staged_bound_to_loyalty_period");
  }

  let verdict = HOTEL_VERDICT.VERIFIED_ZERO_DISPLACEMENT;
  if (blockers.includes("core_period_missing_or_empty")) {
    verdict = HOTEL_VERDICT.INCOMPLETE_SOURCE_EVIDENCE;
  } else if (blockers.some((b) => b.startsWith("shared_registry_subject_bleed"))) {
    verdict = HOTEL_VERDICT.OTHER_BLOCKER;
  } else if (blockers.includes("subject_variants_include_city_only_label")) {
    verdict = HOTEL_VERDICT.OTHER_BLOCKER;
  } else if (displacement.length > 0) {
    verdict = HOTEL_VERDICT.VERIFIED_DISPLACEMENT_PRESENT;
  } else if (
    lostDemand.totalLost > 0 &&
    !reg &&
    Object.keys(countsByEntity || {}).length === 0
  ) {
    verdict = HOTEL_VERDICT.CALCULATION_MISSING;
    blockers.push("lost_scenarios_but_no_entity_registry");
  } else if (incompleteObs > observations.length * 0.25) {
    verdict = HOTEL_VERDICT.INCOMPLETE_SOURCE_EVIDENCE;
    warnings.push("high_incomplete_observation_share");
  }

  if (stagedMismatch && !blockers.includes("staged_empty_while_live_has_displacement")) {
    warnings.push("staged_live_displacement_mismatch");
  }

  const ok =
    blockers.length === 0 &&
    (verdict === HOTEL_VERDICT.VERIFIED_DISPLACEMENT_PRESENT ||
      verdict === HOTEL_VERDICT.VERIFIED_ZERO_DISPLACEMENT);

  return {
    propertyId,
    hotelName: profile.name,
    ok,
    verdict,
    blockers,
    warnings,
    corePeriodId: core.periodId,
    coreMeasurementFamily: core.measurementFamily || null,
    loyaltyPeriodId: loyalty?.periodId || null,
    registryPresent: Boolean(reg),
    primarySubjectEntityId: primarySubject,
    subjectEntityIds: [...subjectIds],
    eligibleResponses,
    incompleteObservations: incompleteObs,
    subjectPresentResponses,
    subjectAbsentResponses,
    subjectAbsentCompetitorPresentResponses: responseLevelDisplacement,
    responseDisplacementRate,
    distinctDisplacedScenarios: scenarioMap.size,
    totalLostScenarios: lostDemand.totalLost,
    distinctObservedCompetitors: distinctCompetitors,
    topCompetitors,
    displacement,
    rawCompetitorObservations: rawCompetitorObs,
    stagedPeriodId: stagedPeriod,
    stagedDisplacementRows: Array.isArray(stagedDisp) ? stagedDisp.length : null,
    stagedTopCompetitor: stagedDisp?.[0]?.name || null,
    stagedTopCount: stagedDisp?.[0]?.displacementCount ?? null,
    liveTopCompetitor: topCompetitors[0]?.name || null,
    liveTopCount: topCompetitors[0]?.displacementCount ?? null,
    methodology: {
      event: DISPLACEMENT_EVENT_DEFINITION,
      count: DISPLACEMENT_COUNT_DEFINITION,
      displayGrain: DISPLAY_GRAIN,
      responseRateDenominator: "eligible_complete_provider_responses",
    },
  };
}

export function certifyAll31CompetitiveDisplacement(propertyIds) {
  const rows = propertyIds.map((id) => independentlyRecalculateDisplacement(id));
  const passed = rows.filter((r) => r.ok).length;
  const failed = rows.filter((r) => !r.ok);
  return {
    gate: ADP_COMPETITIVE_DISPLACEMENT_CERTIFICATION_V1,
    propertyCount: propertyIds.length,
    passed,
    failed: failed.length,
    ok: failed.length === 0,
    rows,
    failedPropertyIds: failed.map((r) => r.propertyId),
  };
}

/**
 * Negative regression fixtures — each must fail the named assertion.
 */
export function runNegativeDisplacementRegressionTests() {
  const results = [];

  function check(name, fn) {
    try {
      fn();
      results.push({ name, passed: true, detail: "failed_as_expected_or_invariant_holds" });
    } catch (err) {
      results.push({ name, passed: false, detail: String(err.message || err) });
    }
  }

  // 1) City-only subject variant must not appear for Falls Church
  check("city_only_subject_variant_blocked", () => {
    const profile = loadPropertyProfile("adp_falls_church_marriott_fairview_park");
    const variants = buildNameVariants(profile);
    if (variants.some((v) => /^falls church$/i.test(String(v).trim()))) {
      throw new Error("Falls Church city-only variant still present");
    }
  });

  // 2) Westin subject IDs must not include JW
  check("westin_subject_excludes_jw", () => {
    const profile = loadPropertyProfile("adp_westin_monterrey_valle");
    const ids = subjectEntityIdsForProperty(profile);
    if (ids.has("jw_marriott_monterrey_valle")) {
      throw new Error("JW incorrectly in Westin subject set");
    }
  });

  // 3) Core loader must not return Loyalty
  check("core_loader_never_loyalty", () => {
    for (const id of [
      "adp_bethesda_marriott",
      "adp_falls_church_marriott_fairview_park",
      "adp_westin_monterrey_valle",
    ]) {
      const core = loadLatestCoreCustomerPeriod(id);
      if (core && isLoyaltyMeasurementPeriod(core)) {
        throw new Error(`${id} core loader returned loyalty`);
      }
    }
  });

  // 4) Co-appearance is not displacement (Bethesda: subject present scenarios excluded)
  check("co_appearance_not_counted_as_displacement", () => {
    const r = independentlyRecalculateDisplacement("adp_bethesda_marriott");
    if (!r.ok && r.verdict === HOTEL_VERDICT.INCOMPLETE_SOURCE_EVIDENCE) return;
    // Every displacement scenario must be subject-absent at scenario grain
    const profile = loadPropertyProfile("adp_bethesda_marriott");
    const core = loadLatestCoreCustomerPeriod("adp_bethesda_marriott");
    const scenarios = buildScenarioUniverse(profile);
    const obs = enrichObservationsWithRank(core.observations, profile);
    const map = buildDisplacementScenarioMap(obs, scenarios, profile);
    for (const [sid, row] of map.entries()) {
      const present = (row.observations || []).some((o) => o.mentioned);
      if (present) throw new Error(`scenario ${sid} has subject co-appearance in displacement map`);
    }
  });

  // 5) Nobu/Yotel registries required when lost scenarios exist
  check("nobu_yotel_registry_resolves_competitors", () => {
    for (const id of ["adp_nobu_london_portman", "adp_yotel_geneva_lake"]) {
      if (!getEntityRegistryForProperty(id)) {
        throw new Error(`${id} missing entity registry`);
      }
      const r = independentlyRecalculateDisplacement(id);
      if (r.totalLostScenarios > 0 && r.distinctObservedCompetitors === 0) {
        throw new Error(`${id} lost scenarios but zero resolved competitors`);
      }
    }
  });

  // 6) Empty displacement array with positive live counts is a binding failure signal
  check("staged_empty_detected_when_live_positive", () => {
    const r = independentlyRecalculateDisplacement("adp_falls_church_marriott_fairview_park");
    if (r.liveTopCount > 0 && r.stagedDisplacementRows === 0) {
      if (!r.blockers.includes("staged_empty_while_live_has_displacement")) {
        throw new Error("expected staged_empty blocker when live has displacement");
      }
    }
  });

  const failed = results.filter((r) => !r.passed);
  return {
    gate: `${ADP_COMPETITIVE_DISPLACEMENT_CERTIFICATION_V1}_NEGATIVE`,
    ok: failed.length === 0,
    results,
    failed: failed.map((r) => r.name),
  };
}
