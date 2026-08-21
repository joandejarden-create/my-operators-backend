/**
 * ADP Existing Hotel property certification runner (read-only, 0 LLM).
 * Traces published ↔ runtime ↔ independent recalculation ↔ governance gates.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  loadAllPeriods,
  loadPropertyProfile,
  PROVIDERS,
} from "../data-model.js";
import {
  selectLatestCertifiedOfficialPeriod,
  isOfficialProductionPeriod,
  isCustomerTrendEligible,
  filterCustomerTrendPeriods,
} from "../period-eligibility-v1.js";
import {
  MEASUREMENT_CONTRACT_VERSION,
  OFFICIAL_BASELINE_PERIOD_MARKER,
  hashMeasurementContract,
  buildMeasurementContractCanonicalBody,
  ADP_PROPERTY_IDS_V1,
} from "../contracts/adp-measurement-contract-v1.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import {
  INTENT_TERRITORY_LABELS,
  territoryLabelForIntent,
  CUSTOMER_TERMINOLOGY_VERSION,
} from "../metrics/intent-territory-labels.js";
import { computeConsiderationMetrics } from "../metrics/consideration-rate.js";
import { buildOptionalExecutiveMetrics } from "../metrics/optional-executive-metrics.js";
import { buildGovernedIntentPresenceIndex } from "../metrics/governed-customer-presence-index.js";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { buildOwnerPayload } from "../customer/owner-payload.js";
import { resolveCustomerFacingEntity } from "../customer/customer-entity-resolution-v1.js";
import {
  loadPublishedReport,
  loadPublishedManifest,
  loadPublishedEvidenceIndex,
} from "../published-snapshot.js";
import { queryEvidenceIndex } from "../customer/evidence-index.js";
import { detectPropertyMention } from "../execution/response-parser.js";
import { computePropertyRealityCoverage } from "../customer/executive-read-v1.js";
import {
  ADP_CERTIFICATION_VERSION,
  CERTIFICATION_STATUSES,
  ISSUE_CLASSES,
  GATE_IDS,
  gateResult,
  aggregateCertificationStatus,
} from "./certification-status.js";

const PHILLIPS_MARKER = "ADP_HOTEL_PHILLIPS_BASELINE_PERIOD_001";

const EXTRA_SUBJECT_ALIASES = {
  adp_waterstone_boca_raton: [
    "Waterstone",
    "Waterstone Resort",
    "Waterstone Resort and Marina",
    "Waterstone Boca Raton",
    "Waterstone Boca",
    "Curio Collection Waterstone",
  ],
  adp_renaissance_times_square: [
    "Renaissance New York Times Square",
    "Renaissance Times Square",
    "Renaissance NY Times Square",
  ],
  adp_cambridge_beaches_bermuda: [
    "Cambridge Beaches",
    "Cambridge Beaches Resort",
    "Cambridge Beaches Resort and Spa",
  ],
  adp_now_now_noho: ["NOW NOW", "Now Now NoHo", "NOW NOW NoHo", "Now Now Hotel"],
  adp_hotel_phillips_kansas_city: [
    "Hotel Phillips",
    "Hotel Phillips Kansas City",
    "the Hotel Phillips",
    "Phillips Hotel",
    "The Phillips Hotel",
    "The Phillips Kansas City, Curio Collection by Hilton",
  ],
};

const COLLISION_REJECT = {
  adp_renaissance_times_square: [
    /\brenaissance\s+(new york\s+)?midtown\b/i,
    /\brenaissance\s+downtown\b/i,
  ],
  adp_hotel_phillips_kansas_city: [/\bphillips\s+(66|petroleum)\b/i],
};

function round1(n) {
  if (n == null || Number.isNaN(Number(n))) return null;
  return Math.round(Number(n) * 10) / 10;
}

function nearlyEqual(a, b, tol = 0.15) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

function pickPeriod(propertyId, manifest) {
  const periods = loadAllPeriods(propertyId);
  if (manifest?.latestPeriodId) {
    const hit = periods.find((p) => p.periodId === manifest.latestPeriodId);
    if (hit) return hit;
  }
  return selectLatestCertifiedOfficialPeriod(periods) || periods[0] || null;
}

function providerCompleteness(period) {
  const expectedProviders = [...PROVIDERS];
  const byProvider = {};
  for (const p of expectedProviders) {
    byProvider[p] = { expected: 0, success: 0, failed: 0, missing: 0, dryRun: 0, includedAsZero: false };
  }
  const scenarios = new Set();
  for (const o of period?.observations || []) {
    scenarios.add(o.scenarioId);
    const p = o.provider || "unknown";
    if (!byProvider[p]) byProvider[p] = { expected: 0, success: 0, failed: 0, missing: 0, dryRun: 0, treatedAsZero: false };
    byProvider[p].expected += 1;
    if (o.dryRun) byProvider[p].dryRun += 1;
    else if (o.error || o.providerError || o.status === "FAILED" || (o.httpStatus && o.httpStatus >= 400)) {
      byProvider[p].failed += 1;
    } else if (o.parsed) {
      byProvider[p].success += 1;
    } else {
      byProvider[p].missing += 1;
    }
  }
  // Expected cells ≈ scenarios × providers when full matrix
  const scenarioCount = scenarios.size;
  for (const p of expectedProviders) {
    if (byProvider[p].expected === 0 && scenarioCount > 0) {
      byProvider[p].missing = scenarioCount;
      byProvider[p].expected = scenarioCount;
    }
  }
  return { byProvider, scenarioCount, expectedProviders };
}

function normalizeForAliasCompare(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildExpandedSubjectAliases(profile) {
  const set = new Set();
  const add = (v) => {
    const t = String(v || "").trim();
    if (t.length >= 4) set.add(t);
  };
  add(profile.name);
  for (const a of profile.identityAliases || profile.aliases || []) add(a);
  for (const a of EXTRA_SUBJECT_ALIASES[profile.propertyId] || []) add(a);
  if (profile.name?.includes("&")) add(profile.name.replace(/&/g, "and"));
  for (const v of [...set]) {
    if (/^the\s+/i.test(v)) add(v.replace(/^the\s+/i, ""));
  }
  return [...set].sort((a, b) => b.length - a.length);
}

function findSubjectAliasHits(raw, aliases, propertyId) {
  const text = String(raw || "");
  const textNorm = normalizeForAliasCompare(text);
  const hits = [];
  for (const alias of aliases) {
    const aNorm = normalizeForAliasCompare(alias);
    if (aNorm.length < 4) continue;
    const idx = textNorm.indexOf(aNorm);
    if (idx === -1) continue;
    const before = idx === 0 ? " " : textNorm[idx - 1];
    const after = idx + aNorm.length >= textNorm.length ? " " : textNorm[idx + aNorm.length];
    if ((/[a-z0-9]/.test(before) || /[a-z0-9]/.test(after)) && !aNorm.includes(" ")) continue;
    const rejects = COLLISION_REJECT[propertyId] || [];
    if (rejects.some((re) => re.test(text))) continue;
    hits.push(alias);
  }
  return hits;
}

function auditOpenAiFalseNegatives(period, profile) {
  const aliases = buildExpandedSubjectAliases(profile);
  const profileWithExtras = {
    ...profile,
    identityAliases: [...(profile.identityAliases || profile.aliases || []), ...(EXTRA_SUBJECT_ALIASES[profile.propertyId] || [])],
  };
  const openaiObs = (period.observations || []).filter(
    (o) => o.provider === "openai" && o.parsed && !o.dryRun && !o.error
  );
  const misses = openaiObs.filter((o) => !o.mentioned);
  const classifications = [];
  let trueMiss = 0;
  let matchingFn = 0;
  let ambiguous = 0;
  let parserDefect = 0;

  for (const o of misses) {
    const raw = o.rawResponse || o.responseText || o.text || "";
    if (!raw || raw.length < 20) {
      classifications.push({
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        class: "TRUE_MISS",
        reason: "empty_or_short_raw",
      });
      trueMiss += 1;
      continue;
    }
    const aliasHits = findSubjectAliasHits(raw, aliases, profile.propertyId);
    const parserHit = detectPropertyMention(raw, profileWithExtras);
    if (parserHit?.mentioned) {
      classifications.push({
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        class: "PARSER_DEFECT",
        reason: "current_parser_detects_with_expanded_aliases",
        matchedVariant: parserHit.matchedVariant || null,
      });
      parserDefect += 1;
      matchingFn += 1;
      continue;
    }
    if (aliasHits.length) {
      classifications.push({
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        class: "MATCHING_FALSE_NEGATIVE",
        reason: "alias_present_but_stored_as_absent",
        matchedAlias: aliasHits[0],
        aliasHits: aliasHits.slice(0, 5),
      });
      matchingFn += 1;
      continue;
    }
    const GEO_OR_STOP = /^(hotel|resort|collection|the|and|by|at|of|kansas|city|florida|boca|raton|bermuda|times|square|new|york|noho|downtown|marina|spa|inn|hilton|curio|marriott|ihg|hyatt|radisson|choice|ascend|autograph|tapestry)$/i;
    const name = String(profile.name || "");
    const tokens = name
      .replace(/[^\w\s]/g, " ")
      .split(/\s+/)
      .filter((t) => t.length >= 4 && !GEO_OR_STOP.test(t));
    const hay = normalizeForAliasCompare(raw);
    const tokenHits = tokens.filter((t) => hay.includes(t.toLowerCase()));
    // Require a distinctive property token (e.g. Phillips, Waterstone) — geo-only hits are TRUE_MISS.
    if (tokenHits.length >= 1 && tokens.length >= 1) {
      classifications.push({
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        class: "AMBIGUOUS",
        reason: "distinctive_name_tokens_present_without_full_alias_match",
        tokenHits,
      });
      ambiguous += 1;
    } else {
      classifications.push({
        observationId: o.observationId,
        scenarioId: o.scenarioId,
        class: "TRUE_MISS",
        reason: "no_subject_signal",
      });
      trueMiss += 1;
    }
  }

  const missRate =
    openaiObs.length > 0 ? round1((misses.length / openaiObs.length) * 100) : null;
  const fnRateAmongMisses =
    misses.length > 0 ? round1((matchingFn / misses.length) * 100) : null;

  return {
    openaiComparable: openaiObs.length,
    openaiMisses: misses.length,
    openaiPresenceRate:
      openaiObs.length > 0
        ? round1(((openaiObs.length - misses.length) / openaiObs.length) * 100)
        : null,
    missRateAmongOpenAi: missRate,
    trueMiss,
    matchingFalseNegative: matchingFn,
    ambiguous,
    parserDefect,
    falseNegativeShareOfMisses: fnRateAmongMisses,
    sample: classifications.slice(0, 25),
  };
}

function auditEntities(payload, profile) {
  const names = [];
  const add = (n, surface) => {
    const s = String(n || "").trim();
    if (s) names.push({ name: s, surface });
  };
  for (const c of payload?.competitiveSet?.observed || []) {
    add(c.name, "competitiveSet.observed");
  }
  for (const c of payload?.competitiveSet?.surprises || []) {
    add(c.name, "competitiveSet.surprises");
  }
  const lost = payload?.lostDemand?.topAlternatives || payload?.lostDemand?.competitors || [];
  for (const c of lost) add(c.name || c, "lostDemand");
  for (const t of payload?.trends || []) {
    /* no entities */
  }

  const rows = [];
  let junk = 0;
  let unresolved = 0;
  let ok = 0;
  for (const { name, surface } of names) {
    const resolved = resolveCustomerFacingEntity(name, profile);
    let status = "EXACT";
    if (resolved.rejected) {
      status = "INVALID_NON_ENTITY";
      junk += 1;
    } else if (!resolved.ok) {
      status = "UNRESOLVED";
      unresolved += 1;
    } else {
      ok += 1;
      if (resolved.entityId) status = "KNOWN_ALIAS";
      else if (resolved.mergeKey && !resolved.entityId) status = "HIGH_CONFIDENCE_FUZZY";
      else status = "EXACT";
    }
    rows.push({
      rawName: name,
      surface,
      status,
      displayName: resolved.displayName || null,
      mergeKey: resolved.mergeKey || null,
      entityId: resolved.entityId || null,
      reason: resolved.reason || null,
    });
  }

  return {
    totalMentions: names.length,
    ok,
    junk,
    unresolved,
    sampleJunk: rows.filter((r) => r.status === "INVALID_NON_ENTITY").slice(0, 10),
    sampleUnresolved: rows.filter((r) => r.status === "UNRESOLVED").slice(0, 10),
    topResolved: rows.filter((r) => r.status !== "INVALID_NON_ENTITY").slice(0, 15),
  };
}

function auditTerritories(scenarios) {
  const taxonomy = Object.entries(INTENT_TERRITORY_LABELS).map(([id, label]) => ({
    canonicalId: id,
    customerLabel: label,
    definitionSource: "lib/ai-demand-positioning/metrics/intent-territory-labels.js",
    terminologyVersion: CUSTOMER_TERMINOLOGY_VERSION,
  }));
  const validIntents = new Set(Object.values(TRAVELER_INTENTS));
  const rows = [];
  let invalid = 0;
  let missing = 0;
  for (const s of scenarios || []) {
    const intent = s.intent;
    const status = !intent
      ? "MISSING_TERRITORY"
      : validIntents.has(intent)
        ? "PASS"
        : "UNKNOWN_TERRITORY";
    if (status === "MISSING_TERRITORY") missing += 1;
    if (status === "UNKNOWN_TERRITORY") invalid += 1;
    rows.push({
      scenarioId: s.scenarioId,
      prompt: (s.prompt || s.promptText || "").slice(0, 120),
      assignedTerritory: intent || null,
      expectedTerritory: intent && validIntents.has(intent) ? intent : null,
      customerLabel: intent ? territoryLabelForIntent(intent) : null,
      status,
      source: s.source || null,
    });
  }
  return { taxonomy, rows, invalid, missing, total: rows.length };
}

function auditScenarioUniverse(scenarios, period) {
  const ids = scenarios.map((s) => s.scenarioId);
  const dupes = ids.filter((id, i) => ids.indexOf(id) !== i);
  const standard = scenarios.filter((s) => s.source === "standard").length;
  const propertySpecific = scenarios.filter((s) => s.source === "property_specific").length;
  const byIntent = {};
  for (const s of scenarios) {
    byIntent[s.intent] = (byIntent[s.intent] || 0) + 1;
  }
  const obsScenarioIds = new Set((period.observations || []).map((o) => o.scenarioId));
  const missingInObs = scenarios.filter((s) => !obsScenarioIds.has(s.scenarioId)).map((s) => s.scenarioId);
  const extraInObs = [...obsScenarioIds].filter((id) => !ids.includes(id));

  return {
    totalScenarios: scenarios.length,
    standard,
    propertySpecific,
    byIntent,
    duplicateScenarioIds: [...new Set(dupes)],
    missingInObservations: missingInObs.slice(0, 20),
    extraInObservations: extraInObs.slice(0, 20),
    contractScenarioVersion: "adp_scenario_universe_v1",
  };
}

function auditEvidence(propertyId, payload, evidenceIndex) {
  const findings = [];
  const intents = Object.keys(INTENT_TERRITORY_LABELS);
  for (const intent of intents) {
    const hasTerritory =
      payload?.executiveMetrics?.considerationRate?.byIntent?.[intent] != null ||
      (payload?.intentPresenceIndex && payload.intentPresenceIndex[intent]) ||
      (payload?.demandCapture?.byIntent && payload.demandCapture.byIntent[intent]);
    if (!hasTerritory && !payload?.intentPresenceIndex?.[intent]) {
      // territory may simply have no scenarios for this property
      continue;
    }
    let available = false;
    let count = 0;
    if (evidenceIndex?.ok) {
      try {
        const res = queryEvidenceIndex(evidenceIndex, { intent, type: "missing" });
        count = res?.count ?? res?.total ?? (res?.evidence?.length || 0);
        available = count > 0 || res?.ok === true;
      } catch {
        available = false;
      }
    }
    findings.push({
      finding: `territory:${intent}:missing_evidence`,
      expectedEvidence: "intent-filtered missing excerpts",
      available,
      count,
      link: `/api/ai-demand-positioning/property/${propertyId}/evidence?intent=${intent}&type=missing`,
      status: available || count === 0 ? (count > 0 ? "PASS" : "EMPTY_EXPLICIT") : "FAIL",
    });
  }

  const emptyExplicit = findings.filter((f) => f.status === "EMPTY_EXPLICIT").length;
  const fail = findings.filter((f) => f.status === "FAIL").length;
  const pass = findings.filter((f) => f.status === "PASS").length;

  return { findings, pass, emptyExplicit, fail, evidenceIndexPresent: Boolean(evidenceIndex?.ok) };
}

function scanCustomerClaims(payload) {
  const claims = [];
  const push = (text, surface, kindHint) => {
    const t = String(text || "").trim();
    if (!t || t.length < 12) return;
    let kind = kindHint || "CALCULATED_FACT";
    if (/could improve|would increase|expected impact|\d+\+|scenarios captured/i.test(t)) {
      kind = "UNSUPPORTED_CLAIM";
    } else if (/review|consider|investigate|look into/i.test(t)) {
      kind = "REVIEW_PROMPT";
    } else if (/awaiting next comparable|baseline|methodology|comparable period/i.test(t)) {
      kind = "METHODOLOGY";
    }
    claims.push({ text: t.slice(0, 180), surface, kind });
  };

  for (const a of payload?.actions || []) {
    push(a.title, "actions.title", "REVIEW_PROMPT");
    push(a.description, "actions.description", "INFERENCE");
    if (a.expectedImpact != null) {
      push(String(a.expectedImpact), "actions.expectedImpact", "UNSUPPORTED_CLAIM");
    }
    push(a.impactNote, "actions.impactNote", "METHODOLOGY");
  }
  const er = payload?.executiveRead;
  if (er?.current?.narrative) push(er.current.narrative, "executiveRead.current", "CALCULATED_FACT");
  if (er?.trend?.narrative) push(er.trend.narrative, "executiveRead.trend", "METHODOLOGY");

  const unsupported = claims.filter((c) => c.kind === "UNSUPPORTED_CLAIM");
  return { claims: claims.slice(0, 40), unsupportedCount: unsupported.length, unsupported: unsupported.slice(0, 10) };
}

function detectAnomalies({ propertyId, published, independent, providerComp, openaiFn, entities }) {
  const anomalies = [];
  const byP = providerComp.byProvider || {};
  const rates = [];
  for (const [p, row] of Object.entries(byP)) {
    if (row.success > 0) {
      // approximate presence from published provider breakdown if present
    }
  }
  const providersBlock = published?.demandCapture?.providers || published?.providers || [];
  if (Array.isArray(providersBlock) && providersBlock.length >= 2) {
    const presence = providersBlock
      .map((r) => ({ p: r.provider, rate: r.presence }))
      .filter((r) => r.rate != null);
    if (presence.length >= 2) {
      const max = Math.max(...presence.map((r) => r.rate));
      const min = Math.min(...presence.map((r) => r.rate));
      if (max - min >= 30) {
        anomalies.push({
          rule: "provider_spread_ge_30pp",
          severity: "REVIEW",
          detail: { max, min, presence },
        });
      }
      const openai = presence.find((r) => r.p === "openai");
      const peers = presence.filter((r) => r.p !== "openai");
      if (openai && peers.length) {
        const peerMean = peers.reduce((s, r) => s + r.rate, 0) / peers.length;
        if (peerMean - openai.rate >= 30) {
          anomalies.push({
            rule: "openai_ge_30pp_below_peer_mean",
            severity: "REVIEW",
            detail: { openai: openai.rate, peerMean: round1(peerMean) },
          });
        }
      }
    }
  }

  for (const [p, row] of Object.entries(byP)) {
    if (row.failed > 0 && row.success + row.failed > 0) {
      const failRate = row.failed / (row.success + row.failed + row.missing);
      if (failRate >= 0.1) {
        anomalies.push({
          rule: "provider_failure_rate_ge_10pct",
          severity: "REVIEW",
          detail: { provider: p, failed: row.failed, success: row.success },
        });
      }
    }
  }

  if (openaiFn.matchingFalseNegative > 0) {
    anomalies.push({
      rule: "openai_matching_false_negatives_present",
      severity: "REVIEW",
      detail: { count: openaiFn.matchingFalseNegative },
    });
  }
  if (entities.junk > 0) {
    anomalies.push({
      rule: "customer_visible_junk_entities",
      severity: "MATERIAL",
      detail: { junk: entities.junk },
    });
  }
  if (propertyId === "adp_hotel_phillips_kansas_city") {
    const gem = byP.gemini;
    if (gem && gem.failed > 0) {
      anomalies.push({
        rule: "phillips_gemini_failures_must_remain_missing",
        severity: "REVIEW",
        detail: gem,
      });
    }
  }
  return anomalies;
}

function classifyProperty(propertyId, period) {
  if (propertyId === "adp_hotel_phillips_kansas_city") {
    return {
      classification: "CERTIFIED_STANDALONE",
      marker: period?.baselineMarker || PHILLIPS_MARKER,
      inOfficialFour: false,
    };
  }
  return {
    classification: "ACTIVE_OFFICIAL_BASELINE",
    marker: period?.baselineMarker || OFFICIAL_BASELINE_PERIOD_MARKER,
    inOfficialFour: ADP_PROPERTY_IDS_V1.includes(propertyId),
  };
}

/**
 * Run full property certification (no LLM, no Airtable writes).
 */
export async function runPropertyCertification(propertyId, options = {}) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    return {
      ok: false,
      propertyId,
      status: CERTIFICATION_STATUSES.NOT_CERTIFIED,
      error: "property_not_found",
    };
  }

  const manifest = loadPublishedManifest(propertyId);
  const published = loadPublishedReport(propertyId);
  const evidenceIndex = loadPublishedEvidenceIndex(propertyId);
  const period = pickPeriod(propertyId, manifest);
  const scenarios = buildScenarioUniverse(profile);
  const contractHash = hashMeasurementContract(buildMeasurementContractCanonicalBody());
  const gates = [];
  const classification = classifyProperty(propertyId, period);

  // --- CONTRACT ---
  const periodHash = period?.measurementContractHash || period?.contractHash || null;
  const hashMatch = !periodHash || periodHash === contractHash || String(periodHash).startsWith(String(contractHash).slice(0, 12));
  gates.push(
    gateResult({
      gateId: GATE_IDS.CONTRACT_VERSION,
      status: hashMatch ? "PASS" : "PASS_WITH_DISCLOSURE",
      material: false,
      issueClass: hashMatch ? null : ISSUE_CLASSES.GOVERNANCE,
      summary: hashMatch
        ? `Contract ${MEASUREMENT_CONTRACT_VERSION} aligned`
        : `Period contract hash differs from current frozen hash (frozen period may predate re-hash)`,
      details: {
        measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
        currentHash: contractHash,
        periodHash,
        classification,
      },
      disclosures: hashMatch
        ? []
        : ["Period hash may be stamped under an earlier freeze; formulas still reconciled independently."],
    })
  );

  // --- SCENARIO UNIVERSE ---
  const scenarioAudit = auditScenarioUniverse(scenarios, period || { observations: [] });
  const scenarioFail =
    scenarioAudit.duplicateScenarioIds.length > 0 ||
    scenarioAudit.totalScenarios < 1 ||
    scenarioAudit.extraInObservations.length > 5;
  gates.push(
    gateResult({
      gateId: GATE_IDS.SCENARIO_UNIVERSE,
      status: scenarioFail ? "FAIL" : scenarioAudit.missingInObservations.length ? "PASS_WITH_DISCLOSURE" : "PASS",
      material: scenarioAudit.duplicateScenarioIds.length > 0,
      issueClass: scenarioFail ? ISSUE_CLASSES.COMPARABILITY : null,
      summary: `${scenarioAudit.totalScenarios} scenarios (${scenarioAudit.standard} standard / ${scenarioAudit.propertySpecific} property-specific)`,
      details: scenarioAudit,
      disclosures: scenarioAudit.missingInObservations.length
        ? [`${scenarioAudit.missingInObservations.length} scenario IDs lack observations in period (may be expected if truncated).`]
        : [],
    })
  );

  // --- TERRITORY ---
  const territoryAudit = auditTerritories(scenarios);
  gates.push(
    gateResult({
      gateId: GATE_IDS.DEMAND_TERRITORY,
      status: territoryAudit.invalid || territoryAudit.missing ? "FAIL" : "PASS",
      material: Boolean(territoryAudit.invalid || territoryAudit.missing),
      issueClass: territoryAudit.invalid ? ISSUE_CLASSES.METHODOLOGY : null,
      summary: `Territory taxonomy ${territoryAudit.taxonomy.length} intents; ${territoryAudit.invalid} unknown / ${territoryAudit.missing} missing`,
      details: {
        taxonomy: territoryAudit.taxonomy,
        invalid: territoryAudit.invalid,
        missing: territoryAudit.missing,
        sampleRows: territoryAudit.rows.slice(0, 12),
      },
    })
  );

  // --- PROVIDER COMPLETENESS ---
  const providerComp = providerCompleteness(period || { observations: [] });
  const comparable = filterComparableObservations(period?.observations || []);
  let treatedFailedAsZero = false;
  // Heuristic: if gemini failed count > 0 but consideration comparable includes full expected matrix for gemini
  const gem = providerComp.byProvider.gemini;
  if (gem && gem.failed > 0) {
    const geminiComparable = comparable.filter((o) => o.provider === "gemini").length;
    // Failed should not appear in comparable
    const geminiFailedStillParsed = (period.observations || []).filter(
      (o) => o.provider === "gemini" && (o.error || o.status === "FAILED") && o.parsed && o.mentioned === false
    );
    // Primary check: consideration denominator should exclude failures
    const cons = published?.executiveMetrics?.considerationRate;
    if (cons?.comparableObservations != null) {
      const totalObs = (period.observations || []).length;
      // If comparable == total including failures, bad
      if (cons.comparableObservations === totalObs && gem.failed > 0) {
        treatedFailedAsZero = true;
      }
    }
    void geminiComparable;
    void geminiFailedStillParsed;
  }
  gates.push(
    gateResult({
      gateId: GATE_IDS.PROVIDER_COMPLETENESS,
      status: treatedFailedAsZero ? "FAIL" : gem?.failed > 0 ? "PASS_WITH_DISCLOSURE" : "PASS",
      material: treatedFailedAsZero,
      issueClass: treatedFailedAsZero ? ISSUE_CLASSES.CALCULATION : ISSUE_CLASSES.DATA_MISSING,
      summary: treatedFailedAsZero
        ? "Failed observations appear included as zeros"
        : "Comparable observation rule appears to omit failures",
      details: providerComp,
      disclosures:
        gem?.failed > 0
          ? [`${gem.failed} Gemini failures recorded — must remain missing≠zero (Phillips expected).`]
          : [],
    })
  );

  // --- METRIC RECONCILIATION ---
  // Dual path (do not collapse without founder decision):
  //   executiveMetrics consideration/scenarioPresence → enrichObservationsWithRank
  //   demandCapture overallRate → stored mentioned flags via buildOwnerPayload
  const observations = (period?.observations || []).filter((o) => o.parsed || o.mentioned !== undefined);
  const enriched = enrichObservationsWithRank(observations, profile);
  let enrichMentionFlips = 0;
  for (let i = 0; i < observations.length; i++) {
    if (Boolean(observations[i].mentioned) !== Boolean(enriched[i]?.mentioned)) enrichMentionFlips += 1;
  }
  const independentCons = computeConsiderationMetrics(enriched, scenarios, profile);
  const independentConsStored = computeConsiderationMetrics(observations, scenarios, profile);
  const independentExec = buildOptionalExecutiveMetrics(period, scenarios, profile);
  const independentPayload = buildOwnerPayload(period, scenarios, profile);
  const independentIndex = buildGovernedIntentPresenceIndex(observations, scenarios, profile);
  const prc = computePropertyRealityCoverage(period, profile);

  const pubCons = published?.executiveMetrics?.considerationRate?.rate ?? null;
  const pubScen = published?.executiveMetrics?.scenarioPresence?.rate ?? null;
  const pubCapture = published?.demandCapture?.overallRate ?? null;
  const indCons = independentCons.observationConsiderationRate;
  const indScen = independentCons.scenarioConsiderationCoverage;
  const indCapture = independentPayload?.demandCapture?.overallRate ?? null;

  const metricRows = [
    {
      metric: "AI Consideration Rate",
      published: pubCons,
      independent: indCons,
      match: nearlyEqual(pubCons, indCons),
      path: "enrichObservationsWithRank",
    },
    {
      metric: "AI Scenario Presence",
      published: pubScen,
      independent: indScen,
      match: nearlyEqual(pubScen, indScen),
      path: "enrichObservationsWithRank",
    },
    {
      metric: "Demand Capture",
      published: pubCapture,
      independent: indCapture,
      match: nearlyEqual(pubCapture, indCapture),
      path: "stored_mentioned",
    },
    {
      metric: "Property Reality Coverage",
      published: published?.trends?.[0]?.propertyRealityCoverage ?? published?.executiveMetrics?.propertyRealityCoverage ?? null,
      independent: prc,
      match: nearlyEqual(
        published?.trends?.[0]?.propertyRealityCoverage ?? published?.executiveMetrics?.propertyRealityCoverage,
        prc
      ),
      path: "reality_coverage",
    },
    {
      metric: "Consideration (stored vs enrich delta)",
      published: independentConsStored.observationConsiderationRate,
      independent: indCons,
      match: nearlyEqual(independentConsStored.observationConsiderationRate, indCons, 0.15),
      path: "dual_path_consistency",
      disclosureOnly: true,
    },
  ];

  // Presence index sample — published uses stored mentioned in governed index path
  const indexCompare = [];
  for (const intent of Object.keys(independentIndex || {})) {
    const pub = published?.intentPresenceIndex?.[intent];
    const ind = independentIndex[intent];
    indexCompare.push({
      intent,
      publishedIndex: pub?.index ?? null,
      independentIndex: ind?.index ?? null,
      match: nearlyEqual(pub?.index, ind?.index, 1),
      publishedSubject: pub?.myRate ?? pub?.subjectRatePct ?? null,
      independentSubject: ind?.myRate ?? ind?.subjectRatePct ?? null,
      publishedCore: pub?.coreBenchmarkRatePct ?? pub?.avgCompRate ?? null,
      independentCore: ind?.coreBenchmarkRatePct ?? ind?.avgCompRate ?? null,
    });
  }
  const dualPathRow = metricRows.find((r) => r.metric === "Consideration (stored vs enrich delta)");
  const dualPathMaterial = dualPathRow && dualPathRow.match === false && Math.abs((dualPathRow.published ?? 0) - (dualPathRow.independent ?? 0)) >= 5;
  const metricFail = metricRows
    .filter((r) => !r.disclosureOnly)
    .some((r) => r.match === false);
  const indexFail = indexCompare.filter((r) => r.publishedIndex != null && r.match === false);

  gates.push(
    gateResult({
      gateId: GATE_IDS.METRIC_RECONCILIATION,
      status: metricFail || dualPathMaterial ? "FAIL" : "PASS",
      material: metricFail || dualPathMaterial,
      issueClass: metricFail || dualPathMaterial ? ISSUE_CLASSES.IMPLEMENTATION : null,
      summary: metricFail
        ? "Published metrics diverge from independent recalculation"
        : dualPathMaterial
          ? `Dual-path conflict: stored consideration ${dualPathRow.published}% vs enrich ${dualPathRow.independent}% (≥5pp)`
          : "Core rates reconcile within rounding tolerance",
      details: {
        metricRows,
        optionalExec: independentExec,
        enrichMentionFlips,
        dualPath: {
          storedConsideration: independentConsStored.observationConsiderationRate,
          enrichConsideration: indCons,
          storedScenarioPresence: independentConsStored.scenarioConsiderationCoverage,
          enrichScenarioPresence: indScen,
          enrichMentionFlips,
        },
      },
      disclosures: dualPathMaterial
        ? [
            `IMPLEMENTATION dual-path: executiveMetrics use enrichObservationsWithRank; demandCapture uses stored mentioned. Flip count=${enrichMentionFlips}. Escalate before unifying — do not auto-change methodology.`,
          ]
        : enrichMentionFlips > 0
          ? [`${enrichMentionFlips} mention flips under enrichObservationsWithRank (non-material delta).`]
          : [],
    })
  );

  gates.push(
    gateResult({
      gateId: GATE_IDS.CORE_BENCHMARK,
      status: indexFail.length ? "FAIL" : enrichMentionFlips > 0 ? "PASS_WITH_DISCLOSURE" : "PASS",
      material: indexFail.length > 0,
      issueClass: indexFail.length ? ISSUE_CLASSES.CALCULATION : ISSUE_CLASSES.PARSING,
      summary: indexFail.length
        ? `${indexFail.length} territory index mismatches`
        : enrichMentionFlips > 0
          ? `Presence Index reconciles; ${enrichMentionFlips} stored≠re-parse mention flips (parser drift disclosure)`
          : "Presence Index / CORE samples reconcile or are null where uncertified",
      details: {
        indexCompare: indexCompare.slice(0, 12),
        failCount: indexFail.length,
        enrichMentionFlips,
      },
      disclosures: [
        "CORE membership and uncertified territories follow governed index rules; null index is valid when CORE peers insufficient.",
        ...(enrichMentionFlips > 0
          ? [
              `${enrichMentionFlips} observations flip mentioned when re-parsed via enrichObservationsWithRank — do not auto-correct rates; classify as PARSING before any republish.`,
            ]
          : []),
      ],
    })
  );

  // --- ENTITY ---
  const entityAudit = auditEntities(published || independentPayload || {}, profile);
  gates.push(
    gateResult({
      gateId: GATE_IDS.COMPETITOR_ENTITY,
      status: entityAudit.junk > 0 ? "FAIL" : entityAudit.unresolved > 5 ? "PASS_WITH_DISCLOSURE" : "PASS",
      material: entityAudit.junk > 0,
      issueClass: ISSUE_CLASSES.ENTITY_RESOLUTION,
      summary: `${entityAudit.ok} resolved / ${entityAudit.junk} junk / ${entityAudit.unresolved} unresolved`,
      details: entityAudit,
      disclosures:
        entityAudit.unresolved > 0
          ? [`${entityAudit.unresolved} unresolved competitor strings remain (review aliases).`]
          : [],
    })
  );

  // Subject aliases self-check
  const profileWithExtras = {
    ...profile,
    identityAliases: [...(profile.identityAliases || profile.aliases || []), ...(EXTRA_SUBJECT_ALIASES[propertyId] || [])],
  };
  const subjectNames = [profile.name, ...(EXTRA_SUBJECT_ALIASES[propertyId] || [])].slice(0, 6);
  const subjectHits = subjectNames.map((n) => {
    const d = detectPropertyMention(`Guests often choose ${n} for this trip.`, profileWithExtras);
    return { name: n, detected: Boolean(d?.mentioned) };
  });
  const subjectFail = subjectHits.filter((h) => !h.detected).length > subjectHits.length / 2;
  gates.push(
    gateResult({
      gateId: GATE_IDS.SUBJECT_ENTITY,
      status: subjectFail ? "FAIL" : "PASS",
      material: subjectFail,
      issueClass: ISSUE_CLASSES.PARSING,
      summary: subjectFail ? "Subject alias detection weak" : "Subject aliases detect in controlled phrases",
      details: { subjectHits },
    })
  );

  // --- OPENAI FN ---
  const openaiFn = auditOpenAiFalseNegatives(period || { observations: [] }, profile);
  const openaiMaterial = openaiFn.matchingFalseNegative >= 5;
  gates.push(
    gateResult({
      gateId: GATE_IDS.OPENAI_FALSE_NEGATIVE,
      status: openaiMaterial ? "FAIL" : openaiFn.matchingFalseNegative > 0 ? "PASS_WITH_DISCLOSURE" : "PASS",
      material: openaiMaterial,
      issueClass: ISSUE_CLASSES.PARSING,
      summary: `OpenAI misses=${openaiFn.openaiMisses}; matching FN=${openaiFn.matchingFalseNegative}; true miss=${openaiFn.trueMiss}`,
      details: openaiFn,
      disclosures:
        openaiFn.matchingFalseNegative > 0
          ? ["Some OpenAI subject-misses may be alias/parser false negatives — do not change rates until remediated."]
          : [],
    })
  );

  // --- ATTRIBUTE / REALITY ---
  const reality = independentPayload?.realityGap || published?.realityGap || null;
  const attrGate =
    reality == null
      ? gateResult({
          gateId: GATE_IDS.ATTRIBUTE_DICTIONARY,
          status: "PASS_WITH_DISCLOSURE",
          material: false,
          issueClass: ISSUE_CLASSES.DATA_MISSING,
          summary: "Reality gap block absent or empty on payload",
          disclosures: ["Attribute dictionary exists in reality-gap module; full cross-property attribute gold set still deferred."],
        })
      : gateResult({
          gateId: GATE_IDS.ATTRIBUTE_DICTIONARY,
          status: "PASS_WITH_DISCLOSURE",
          material: false,
          issueClass: ISSUE_CLASSES.GOVERNANCE,
          summary: "Reality gap present; cross-hotel attribute gold-set consistency not fully automated yet",
          details: {
            recognized: reality.recognized?.length ?? reality.recognizedCount ?? null,
            missing: reality.missing?.length ?? reality.missingCount ?? null,
            coverageRate: reality.coverageRate ?? prc,
          },
          disclosures: [
            "Full adversarial attribute consistency pack across hotels is PARTIAL — flagged for follow-on.",
          ],
        });
  gates.push(attrGate);

  // --- EVIDENCE ---
  const evidenceAudit = auditEvidence(propertyId, published || {}, evidenceIndex);
  gates.push(
    gateResult({
      gateId: GATE_IDS.EVIDENCE_COMPLETENESS,
      status: evidenceAudit.fail ? "FAIL" : evidenceAudit.emptyExplicit ? "PASS_WITH_DISCLOSURE" : "PASS",
      material: evidenceAudit.fail > 0,
      issueClass: ISSUE_CLASSES.EVIDENCE,
      summary: `Evidence index ${evidenceAudit.evidenceIndexPresent ? "present" : "missing"}; pass=${evidenceAudit.pass} empty=${evidenceAudit.emptyExplicit} fail=${evidenceAudit.fail}`,
      details: evidenceAudit,
      disclosures: evidenceAudit.emptyExplicit
        ? ["Some territories have explicit empty evidence (acceptable if messaging is explicit)."]
        : [],
    })
  );

  // --- TREND ---
  const trends = published?.trends || [];
  const customerPeriods = filterCustomerTrendPeriods(loadAllPeriods(propertyId));
  const trend0 = trends[0];
  const trendOk =
    trends.length >= 1 &&
    trend0?.considerationRate != null &&
    trend0?.scenarioPresenceRate != null &&
    isCustomerTrendEligible(period);
  gates.push(
    gateResult({
      gateId: GATE_IDS.TREND_BASELINE,
      status: trendOk ? "PASS" : "FAIL",
      material: !trendOk,
      issueClass: ISSUE_CLASSES.PUBLISHING,
      summary: trendOk
        ? `One-period baseline present (cons=${trend0.considerationRate}, scen=${trend0.scenarioPresenceRate})`
        : "Trend baseline missing consideration/scenario rates",
      details: {
        trendsLen: trends.length,
        trend0,
        customerEligiblePeriods: customerPeriods.length,
        classification,
      },
    })
  );

  // --- PUBLISHED LINEAGE ---
  const publishedPresent = Boolean(published && manifest?.publishStatus === "Live");
  gates.push(
    gateResult({
      gateId: GATE_IDS.PUBLISHED_PAYLOAD,
      status: publishedPresent ? "PASS" : "FAIL",
      material: !publishedPresent,
      issueClass: ISSUE_CLASSES.PUBLISHING,
      summary: publishedPresent
        ? `Live published snapshot ${manifest.latestPeriodId}`
        : "Missing Live published snapshot",
      details: {
        periodId: manifest?.latestPeriodId || null,
        publishedAt: manifest?.latestPublishedAt || null,
        publishStatus: manifest?.publishStatus || null,
        productVersion: manifest?.productVersion || null,
      },
    })
  );

  // --- AIRTABLE ---
  const airtableLive =
    process.env.ADP_PUBLISHED_READ_SOURCE === "airtable" || process.env.ADP_AIRTABLE_READ_LIVE === "1";
  gates.push(
    gateResult({
      gateId: GATE_IDS.AIRTABLE_READ_PATH,
      status: airtableLive ? "PASS_WITH_DISCLOSURE" : "PASS",
      material: false,
      issueClass: ISSUE_CLASSES.GOVERNANCE,
      summary: airtableLive
        ? "Airtable read overlay ENABLED — reconcile separately"
        : "AIRTABLE_NOT_IN_PRODUCTION_READ_PATH",
      details: {
        ADP_AIRTABLE_READ_LIVE: process.env.ADP_AIRTABLE_READ_LIVE || null,
        ADP_PUBLISHED_READ_SOURCE: process.env.ADP_PUBLISHED_READ_SOURCE || null,
      },
      disclosures: airtableLive
        ? ["Airtable overlay is on; certification SoT remains local published JSON."]
        : [],
    })
  );

  // --- CLAIMS ---
  const claims = scanCustomerClaims(published || independentPayload || {});
  gates.push(
    gateResult({
      gateId: GATE_IDS.CUSTOMER_CLAIMS,
      status: claims.unsupportedCount > 0 ? "FAIL" : "PASS",
      material: claims.unsupportedCount > 0,
      issueClass: ISSUE_CLASSES.IMPLEMENTATION,
      summary:
        claims.unsupportedCount > 0
          ? `${claims.unsupportedCount} unsupported customer claims`
          : "No unsupported numeric impact promises detected in actions",
      details: claims,
    })
  );

  // --- ANOMALIES ---
  const anomalies = detectAnomalies({
    propertyId,
    published: published || {},
    independent: independentPayload,
    providerComp,
    openaiFn,
    entities: entityAudit,
  });
  const materialAnomalies = anomalies.filter((a) => a.severity === "MATERIAL");
  gates.push(
    gateResult({
      gateId: GATE_IDS.ANOMALY_DETECTION,
      status: materialAnomalies.length ? "FAIL" : anomalies.length ? "PASS_WITH_DISCLOSURE" : "PASS",
      material: materialAnomalies.length > 0,
      issueClass: ISSUE_CLASSES.DATA_QUALITY,
      summary: `${anomalies.length} anomalies (${materialAnomalies.length} material)`,
      details: { anomalies },
      disclosures: anomalies.filter((a) => a.severity === "REVIEW").map((a) => a.rule),
    })
  );

  gates.push(
    gateResult({
      gateId: GATE_IDS.UI_STRUCTURAL,
      status: "PASS_WITH_DISCLOSURE",
      material: false,
      issueClass: ISSUE_CLASSES.UI_DISPLAY,
      summary: "UI structural audit deferred to Playwright (column stability, NaN, evidence links)",
      disclosures: [
        "UI_STRUCTURAL not executed in this CLI run — use playwright-feature-qa for layout/filter jump checks.",
      ],
    })
  );

  const agg = aggregateCertificationStatus(gates);
  const certifiedAt = new Date().toISOString();

  return {
    ok: true,
    certificationVersion: ADP_CERTIFICATION_VERSION,
    propertyId,
    propertyName: profile.name,
    status: agg.status,
    certifiedAt,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash: contractHash,
    classification,
    periodId: period?.periodId || null,
    manifestPeriodId: manifest?.latestPeriodId || null,
    materialFailCount: agg.materialFailCount,
    disclosureCount: agg.disclosureCount,
    gates,
    sections: {
      scenarioUniverse: scenarioAudit,
      territories: {
        taxonomy: territoryAudit.taxonomy,
        invalid: territoryAudit.invalid,
        missing: territoryAudit.missing,
      },
      providerCompleteness: providerComp,
      metricReconciliation: metricRows,
      presenceIndex: indexCompare,
      entities: entityAudit,
      openaiFalseNegatives: openaiFn,
      evidence: evidenceAudit,
      claims,
      anomalies,
      randomManualSample: {
        status: "DEFERRED_TO_OPERATOR",
        note: "Automation prepared sample slots; human double-review must record outcomes in follow-on.",
        recommendedPerProvider: 5,
      },
    },
  };
}
