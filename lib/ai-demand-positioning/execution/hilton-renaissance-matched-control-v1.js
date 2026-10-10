/**
 * Hilton × Renaissance NY Times Square — matched-control ADP measurement.
 * Freezes Hilton certified 65-scenario universe; both hotels measured on identical
 * controlScenarioIds + traveler needs (subject name/brand/loyalty substitution only
 * on property_capability rows). No threshold/methodology changes.
 *
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import { createHash } from "crypto";
import { MEASUREMENT_CONTRACT_VERSION } from "../contracts/adp-measurement-contract-v1.js";
import { loadPropertyProfile, savePeriod, PROVIDERS } from "../data-model.js";
import { executeMonitoringPeriod, estimateCost, PROVIDER_CONFIGS } from "./multi-provider-runner.js";
import { parseObservation, detectPropertyMention } from "./response-parser.js";
import { attachFirstOfficialPropertyPeriodMetadata } from "../period-eligibility-v1.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../published-snapshot.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import { HILTON_TS_ENTITY_VERSION } from "./hilton-times-square-baseline-period-001-v1.js";
import { loadFrozenContractHash } from "./hilton-times-square-baseline-period-001-v1.js";

export const MATCHED_CONTROL_SET_ID = "ADP_HILTON_RTS_MATCHED_CONTROL_QUERY_SET_V1";
export const MATCHED_CONTROL_MARKER = "adp_hilton_renaissance_matched_control_v1";
export const RENAISSANCE_TS_ENTITY_VERSION = "adp_renaissance_times_square_entity_v2";
export const MATCHED_COST_CAP_USD = 30;

const CONTROL_PATH = join(
  process.cwd(),
  "data/ai-demand-positioning/contracts/hilton-rts-matched-control-query-set-v1.json"
);

const HILTON_ID = "adp_hilton_times_square";
const REN_ID = "adp_renaissance_times_square";

function loyaltyFor(profile) {
  const blob = `${profile.affiliation || ""} ${profile.brand || ""} ${profile.parentCompany || ""}`;
  if (/marriott|bonvoy|renaissance/i.test(blob)) return "Marriott Bonvoy";
  if (/hilton|honors/i.test(blob)) return "Hilton Honors";
  return profile.brand || "loyalty program";
}

function placeFor(profile) {
  const city = profile.city || "New York";
  const sub = profile.submarket;
  return sub ? `${sub}, ${city}` : city;
}

export function loadMatchedControlContract() {
  if (!existsSync(CONTROL_PATH)) {
    throw new Error(`Missing control contract: ${CONTROL_PATH}`);
  }
  return JSON.parse(readFileSync(CONTROL_PATH, "utf8"));
}

/**
 * Materialize frozen control scenarios for a subject hotel.
 * Standard rows: exact frozen queries. Capability rows: subject substitution only.
 */
export function materializeControlScenarios(contract, profile) {
  const subject = profile.name;
  const brand = profile.brand || subject;
  const loyalty = loyaltyFor(profile);
  const city = profile.city || "New York";
  const place = placeFor(profile);

  return (contract.scenarios || []).map((row) => {
    let query = row.queryTemplate;
    if (row.subjectSubstitution) {
      query = query
        .split("{{SUBJECT_NAME}}")
        .join(subject)
        .split("{{BRAND}}")
        .join(brand)
        .split("{{LOYALTY}}")
        .join(loyalty)
        .split("{{CITY}}")
        .join(city)
        .split("{{PLACE}}")
        .join(place);
    }
    return {
      scenarioId: row.controlScenarioId,
      intent: row.intent,
      frame: row.frame,
      query,
      layer: row.layer,
      source: row.source,
      propertyId: profile.propertyId,
      controlSetId: contract.controlSetId,
      subjectSubstitution: !!row.subjectSubstitution,
      rankEligible: row.rankEligible !== false,
      scenarioUniverseVersion: MATCHED_CONTROL_SET_ID,
    };
  });
}

/**
 * Renaissance eligibility vs Hilton control set — traveler-need validity, not builder history.
 */
export function auditRenaissanceEligibility(contract, renProfile) {
  const hasMeetings =
    Number(renProfile.meetingSpace?.meetingRooms || 0) > 0 ||
    Number(renProfile.meetingSpace?.totalSqFt || 0) > 0;
  return (contract.scenarios || []).map((row) => {
    let classification = "ELIGIBLE";
    let reason = "Same NYC Times Square upper-upscale traveler need; valid Ren subject";
    if (row.subjectSubstitution) {
      reason =
        "Property-capability traveler need; subject/brand/loyalty substituted — not hotel-specific exclusion";
    }
    // Capability meeting studio wording still valid when Ren has meeting inventory
    if (/meeting studios|compact meeting/i.test(row.queryFrozenHilton || row.queryTemplate) && !hasMeetings) {
      classification = "NOT_ELIGIBLE_TRUE_CAPABILITY";
      reason = "Control prompt assumes meeting studios; Ren meeting inventory missing";
    }
    return {
      controlScenarioId: row.controlScenarioId,
      territory: row.territory,
      intent: row.intent,
      classification,
      reason,
      inCommonComparable: classification === "ELIGIBLE",
    };
  });
}

function availableProviders() {
  return PROVIDERS.filter((p) => {
    if (p === "openai") return !!(process.env.OPENAI_API_KEY || process.env.FDD_OPENAI_API_KEY);
    if (p === "gemini") {
      return !!(
        process.env.GEMINI_API_KEY ||
        process.env.GOOGLE_GENAI_API_KEY ||
        process.env.GOOGLE_GENERATIVE_AI_API_KEY ||
        process.env.FDD_GEMINI_API_KEY
      );
    }
    if (p === "perplexity") return !!(process.env.PERPLEXITY_API_KEY || process.env.PPLX_API_KEY);
    if (p === "claude") {
      return !!(
        process.env.ANTHROPIC_API_KEY ||
        process.env.CLAUDE_API_KEY ||
        process.env.FDD_ANTHROPIC_API_KEY
      );
    }
    return false;
  });
}

function forceParsePeriod(period, profile) {
  return {
    ...period,
    observations: (period.observations || []).map((obs) => parseObservation(obs, profile)),
    status: "PARSED",
  };
}

function completeness(period, scenarioCount, providers) {
  const obs = period.observations || [];
  const byProvider = {};
  let success = 0;
  let failed = 0;
  for (const p of providers) {
    byProvider[p] = { expected: scenarioCount, attempted: 0, successful: 0, failed: 0, mentioned: 0 };
  }
  for (const o of obs) {
    const p = o.provider;
    if (!byProvider[p]) byProvider[p] = { expected: scenarioCount, attempted: 0, successful: 0, failed: 0, mentioned: 0 };
    byProvider[p].attempted += 1;
    if (o.rawResponse && o.rawResponseLength > 20) {
      byProvider[p].successful += 1;
      success += 1;
    } else {
      byProvider[p].failed += 1;
      failed += 1;
    }
    if (o.mentioned) byProvider[p].mentioned += 1;
  }
  return { attempted: obs.length, success, failed, byProvider };
}

export function buildMatchedControlPreflight() {
  const contract = loadMatchedControlContract();
  const hilton = loadPropertyProfile(HILTON_ID);
  const ren = loadPropertyProfile(REN_ID);
  const eligibility = auditRenaissanceEligibility(contract, ren);
  const common = eligibility.filter((e) => e.inCommonComparable);
  const commonIds = new Set(common.map((e) => e.controlScenarioId));
  const scenariosH = materializeControlScenarios(contract, hilton).filter((s) =>
    commonIds.has(s.scenarioId)
  );
  const scenariosR = materializeControlScenarios(contract, ren).filter((s) =>
    commonIds.has(s.scenarioId)
  );
  const providers = availableProviders();
  const perHotelCost = estimateCost(scenariosH.length, providers.length ? providers : PROVIDERS);
  const queryParity = scenariosH.every((h, i) => {
    const r = scenariosR[i];
    if (!r || h.scenarioId !== r.scenarioId) return false;
    if (!h.subjectSubstitution) return h.query === r.query;
    // capability: only subject/brand/loyalty may differ
    return true;
  });

  return {
    MATCHED_CONTROL_SET_ID,
    controlHash: contract.controlHash,
    controlScenarioCount: contract.scenarioCount,
    commonComparableCount: common.length,
    excludedCount: eligibility.length - common.length,
    excluded: eligibility.filter((e) => !e.inCommonComparable),
    eligibility,
    hiltonCensus: hilton.censusRecordId,
    renCensus: ren.censusRecordId || "recG66DQJKP2c0UNh",
    hiltonEntity: HILTON_TS_ENTITY_VERSION,
    renEntity: RENAISSANCE_TS_ENTITY_VERSION,
    hiltonAliasCount: (hilton.identityAliases || []).length,
    renAliasCount: (ren.identityAliases || []).length,
    AVAILABLE_PROVIDERS: providers,
    TOTAL_PLANNED_CALLS: scenariosH.length * providers.length * 2,
    TOTAL_ESTIMATED_COST: +(perHotelCost.total * 2).toFixed(2),
    COST_CAP_USD: MATCHED_COST_CAP_USD,
    queryParityOk: queryParity,
    PROVIDER_CONFIGS: Object.fromEntries(
      Object.entries(PROVIDER_CONFIGS).map(([k, v]) => [k, { model: v.model, costPerCall: v.costPerCall }])
    ),
    PREFLIGHT:
      providers.length >= 4 &&
      common.length >= 50 &&
      hilton.censusRecordId === "rec35fExUxCClpOP6" &&
      queryParity
        ? "PASS"
        : "FAIL",
  };
}

async function runOneHotel({
  propertyId,
  profile,
  scenarios,
  providers,
  dryRun,
  certify,
  pairId,
  onProgress,
  entityVersion,
}) {
  const period = await executeMonitoringPeriod({
    propertyId,
    scenarios,
    dryRun,
    providers,
    delayMsOverride: dryRun ? 0 : 200,
    checkpointEvery: 20,
    onProgress: onProgress
      ? (completed, total) => onProgress({ propertyId, completed, total })
      : null,
  });

  let finalPeriod = dryRun ? period : forceParsePeriod(period, profile);
  finalPeriod = attachFirstOfficialPropertyPeriodMetadata(finalPeriod, {
    measurementContractHash: loadFrozenContractHash(),
    baselineMarker: MATCHED_CONTROL_MARKER,
    baselineSequence: propertyId === HILTON_ID ? 2 : 1,
    scenarioUniverseVersion: MATCHED_CONTROL_SET_ID,
    entityResolutionVersion: entityVersion,
    sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
    providerSet: providers,
    certified: certify === true && !dryRun,
    priorComparablePeriod: null,
  });
  finalPeriod.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
  finalPeriod.matchedControlPairId = pairId;
  finalPeriod.matchedControlSetId = MATCHED_CONTROL_SET_ID;
  finalPeriod.customerTrendEligible = false; // matched-control pair — not automatic trend join

  const comp = completeness(finalPeriod, scenarios.length, providers);
  let published = null;
  if (!dryRun) {
    savePeriod(finalPeriod);
    const bundle = buildPublishedSnapshotBundle({
      period: finalPeriod,
      profile,
      scenarios,
    });
    if (!bundle.ok) {
      return { ok: false, status: "PUBLISH_FAILED", PERIOD_ID: finalPeriod.periodId, bundle, PROVIDER_COMPLETENESS: comp.byProvider };
    }
    published = savePublishedSnapshotBundle(bundle, { seed: false });
  } else {
    savePeriod(finalPeriod);
  }

  return {
    ok: comp.failed === 0,
    PERIOD_ID: finalPeriod.periodId,
    propertyId,
    scenarioCount: scenarios.length,
    PROVIDER_COMPLETENESS: comp,
    published,
    period: finalPeriod,
  };
}

export async function executeMatchedControlPair({
  dryRun = true,
  certify = false,
  onProgress = null,
} = {}) {
  const preflight = buildMatchedControlPreflight();
  if (!dryRun && preflight.PREFLIGHT !== "PASS") {
    return { ok: false, status: "ABORTED_PREFLIGHT", preflight };
  }
  if (!dryRun && preflight.TOTAL_ESTIMATED_COST > MATCHED_COST_CAP_USD) {
    return { ok: false, status: "ABORTED_COST", preflight };
  }

  const contract = loadMatchedControlContract();
  const hilton = loadPropertyProfile(HILTON_ID);
  const ren = loadPropertyProfile(REN_ID);
  const eligibility = auditRenaissanceEligibility(contract, ren);
  const commonIds = new Set(eligibility.filter((e) => e.inCommonComparable).map((e) => e.controlScenarioId));
  const scenariosH = materializeControlScenarios(contract, hilton).filter((s) => commonIds.has(s.scenarioId));
  const scenariosR = materializeControlScenarios(contract, ren).filter((s) => commonIds.has(s.scenarioId));
  const providers = dryRun ? [...PROVIDERS] : availableProviders();
  const pairId = `matched_pair_${createHash("sha256")
    .update(`${MATCHED_CONTROL_SET_ID}|${Date.now()}`)
    .digest("hex")
    .slice(0, 12)}`;

  const hiltonResult = await runOneHotel({
    propertyId: HILTON_ID,
    profile: hilton,
    scenarios: scenariosH,
    providers,
    dryRun,
    certify,
    pairId,
    onProgress,
    entityVersion: HILTON_TS_ENTITY_VERSION,
  });

  const renResult = await runOneHotel({
    propertyId: REN_ID,
    profile: ren,
    scenarios: scenariosR,
    providers,
    dryRun,
    certify,
    pairId,
    onProgress,
    entityVersion: RENAISSANCE_TS_ENTITY_VERSION,
  });

  const completenessComparable =
    hiltonResult.PROVIDER_COMPLETENESS?.failed === 0 &&
    renResult.PROVIDER_COMPLETENESS?.failed === 0 &&
    hiltonResult.PROVIDER_COMPLETENESS?.attempted === renResult.PROVIDER_COMPLETENESS?.attempted;

  return {
    ok: hiltonResult.ok && renResult.ok && completenessComparable,
    status: dryRun
      ? "DRY_RUN_COMPLETE"
      : certify && completenessComparable
        ? "CERTIFIED_MATCHED_PAIR"
        : "EXECUTION_COMPLETE",
    pairId,
    preflight,
    hilton: {
      PERIOD_ID: hiltonResult.PERIOD_ID,
      completeness: hiltonResult.PROVIDER_COMPLETENESS,
      published: hiltonResult.published,
    },
    renaissance: {
      PERIOD_ID: renResult.PERIOD_ID,
      completeness: renResult.PROVIDER_COMPLETENESS,
      published: renResult.published,
    },
    FORMALLY_COMPARABLE: !dryRun && completenessComparable,
    scenariosH,
    scenariosR,
    hiltonPeriod: hiltonResult.period,
    renPeriod: renResult.period,
  };
}

/** Identity false-negative audit on a parsed period. */
export function auditIdentityFalseNegatives(period, profile) {
  const rows = [];
  for (const o of period.observations || []) {
    const raw = o.rawResponse || "";
    const det = detectPropertyMention(raw, profile);
    if (o.mentioned) {
      if (!det.mentioned) {
        rows.push({
          scenarioId: o.scenarioId,
          provider: o.provider,
          classification: "STORED_TRUE_DETECT_FALSE",
          storedMentioned: true,
          reparseMentioned: false,
          matchedVariant: "",
        });
      }
      continue;
    }
    let classification = "TRUE_ABSENCE";
    if (!raw || raw.length < 40) classification = "PROVIDER_OR_EMPTY";
    else if (det.mentioned) classification = "PARSER_MISS";
    rows.push({
      scenarioId: o.scenarioId,
      provider: o.provider,
      classification,
      storedMentioned: false,
      reparseMentioned: det.mentioned,
      matchedVariant: det.matchedVariant || "",
    });
  }
  return rows;
}
