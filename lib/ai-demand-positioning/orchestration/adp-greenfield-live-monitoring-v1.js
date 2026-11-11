/**
 * ADP_GREENFIELD_LIVE_MONITORING_V1
 *
 * Live Core + Loyalty provider execution for a single founder-authorized
 * greenfield hotel. Bound to ADP_PAID_SUPERVISOR_HANDOFF_V1 authorization.
 * Does not use the 31-hotel aggregate Core/Loyalty ledger.
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import {
  loadPropertyProfile,
  savePeriod,
  PROVIDERS,
  isLoyaltyMeasurementPeriod,
  loadAllPeriods,
  loadLatestCoreCustomerPeriod,
} from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { buildIndependentUnpromptedCoreUniverse } from "../prompt-universe/adp-independent-unprompted-core-universe-v1.js";
import {
  executeMonitoringPeriod,
  estimateCost,
  callProvider,
  PROVIDER_CONFIGS,
} from "../execution/multi-provider-runner.js";
import { parsePeriodObservations as parseObs } from "../execution/response-parser.js";
import {
  writeDurableObservationArchive,
  assertDurableArchiveOrBlock,
  readbackDurableObservationArchive,
} from "../execution/durable-observation-archive-v1.js";
import { buildScenarioUniverseManifest } from "../certification/adp-scenario-universe-contract-v1.js";
import { attachFirstOfficialPropertyPeriodMetadata } from "../period-eligibility-v1.js";
import { MEASUREMENT_CONTRACT_VERSION } from "../contracts/adp-measurement-contract-v1.js";
import { OWNED_SOURCE_CLASSIFICATION_VERSION } from "../metrics/owned-source-classification-v1.js";
import { buildOwnerPayload } from "../customer/owner-payload.js";
import { saveStagedAdpReport } from "../review/adp-staged-report-store-v1.js";
import { CONDITIONAL_APPROVED_METHODOLOGY_ID } from "../intelligence/reality-gap-methodology-v2.js";
import {
  buildLoyaltyPromptTemplatesV1,
  LOYALTY_BPP_SCENARIO_STANDARD_V1,
  MEASUREMENT_FAMILY_CORE_ADP,
  MEASUREMENT_FAMILY_LOYALTY_BPP,
} from "../brand-portfolio/loyalty-bpp-scenario-standard-v1.js";
import { PORTFOLIO_TYPES } from "../brand-portfolio/brand-portfolio-position-contract-v1.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import {
  CONDESA_PROPERTY_ID,
  getCondesaPeerSetIfApproved,
} from "../greenfield/adp-condesa-bpp-peer-cohort-v1.js";
import {
  RADISSON_BLU_TORONTO_PROPERTY_ID,
  getRadissonBluTorontoPeerSetIfApproved,
} from "../greenfield/adp-radisson-blu-toronto-bpp-peer-cohort-v1.js";
import { loadAuthorizationForRun } from "./adp-paid-supervisor-handoff-v1.js";
import { auditHotelCustomerReleaseContent } from "../certification/adp-31-hotel-customer-release-acceptance-v1.js";
import { draftPeerRegistryScopeFields } from "../governance/adp-property-registry-scope-v1.js";

/**
 * Ensure a minimal on-disk profile exists for draft peer Loyalty measurement.
 * Does not create Airtable/HPC records.
 * Does not register the peer as a selectable owner/founder ADP report property.
 */
export function ensureDraftPeerProfileForLoyalty(peer, opts = {}) {
  const propertyId = peer?.canonicalEntityId || peer?.propertyId;
  if (!propertyId) return null;
  const peerOf =
    opts.peerOf ||
    peer?.peerOf ||
    peer?.subjectPropertyId ||
    CONDESA_PROPERTY_ID;
  const existing = loadPropertyProfile(propertyId);
  if (existing) {
    // Harden legacy draft peer fixtures written before registry-scope roles.
    if (
      existing.registryRole !== "BPP_RESEARCH_ONLY_PEER" ||
      existing.ownerSelectorEligible !== false ||
      existing.founderReviewVisible !== false
    ) {
      const fixturesDir = path.join(ROOT, "fixtures/ai-demand-positioning");
      const slug = String(propertyId).replace(/^adp_|^draft_/, "");
      const fp = path.join(fixturesDir, `${slug}-property-profile.json`);
      if (fs.existsSync(fp)) {
        const scope = draftPeerRegistryScopeFields({
          peerOf: existing.stewardship?.peerOf || peerOf,
        });
        const patched = {
          ...existing,
          ...scope,
          stewardship: {
            ...(existing.stewardship || {}),
            ...scope.stewardship,
            peerOf: existing.stewardship?.peerOf || peerOf,
          },
        };
        fs.writeFileSync(fp, `${JSON.stringify(patched, null, 2)}\n`);
        return loadPropertyProfile(propertyId) || patched;
      }
    }
    return existing;
  }
  const fixturesDir = path.join(ROOT, "fixtures/ai-demand-positioning");
  ensureDir(fixturesDir);
  const slug = String(propertyId).replace(/^adp_|^draft_/, "");
  const fp = path.join(fixturesDir, `${slug}-property-profile.json`);
  const scope = draftPeerRegistryScopeFields({ peerOf });
  const choiceToronto =
    peerOf === RADISSON_BLU_TORONTO_PROPERTY_ID ||
    peer?.ecosystem === "Choice Privileges" ||
    /choice privileges/i.test(peer?.affiliationMatch || "") ||
    /radisson|ascend|park inn|country inn/i.test(`${peer?.brand || ""} ${peer?.peerHotel || ""}`);
  const urbanGfa =
    !choiceToronto &&
    (String(propertyId).startsWith("draft_gfa_") ||
      /grand fiesta americana/i.test(peer.peerHotel || "") ||
      peerOf === "adp_grand_fiesta_americana_chapultepec");
  const profile = choiceToronto
    ? {
        propertyId,
        name: peer.peerHotel || propertyId,
        displayName: peer.peerHotel || propertyId,
        brand: peer.brand || "Radisson",
        parentCompany: peer.parentCompany || "Choice Hotels",
        affiliation: "Choice Privileges",
        loyaltyProgram: "Choice Privileges",
        loyaltyApplicable: true,
        portfolioLoyaltyIdentity: "Choice Privileges / Choice Hotels",
        propertyType: peer.product || "full_service_hotel",
        city: peer.market || null,
        country: "Canada",
        countryCode: "CA",
        market: peer.market || "Toronto",
        officialHotelCode: peer.officialHotelCode || null,
        officialUrl: peer.evidenceSource || null,
        website: peer.evidenceSource || null,
        aliases: [peer.peerHotel, peer.officialHotelCode].filter(Boolean),
        identityAliases: [peer.peerHotel, peer.officialHotelCode].filter(Boolean),
        attributes: ["full_service", "choice_privileges"],
        ...scope,
        officialBaselinePublished: false,
        greenfield: true,
        bppResearchOnlyPeer: true,
      }
    : {
        propertyId,
        name: peer.peerHotel || propertyId,
        displayName: peer.peerHotel || propertyId,
        brand: peer.brand || (urbanGfa ? "Grand Fiesta Americana" : "Fiesta Americana"),
        parentCompany: peer.parentCompany || "Grupo Posadas",
        affiliation: "Fiesta Rewards",
        portfolioLoyaltyIdentity: urbanGfa
          ? "Fiesta Rewards / Grand Fiesta Americana"
          : "Fiesta Rewards / Fiesta Americana",
        propertyType:
          peer.product ||
          (urbanGfa ? "luxury_full_service_hotel" : "all_inclusive_beach_resort"),
        city: peer.market || null,
        country: "Mexico",
        countryCode: "MX",
        market: peer.market || null,
        officialUrl: peer.evidenceSource || null,
        website: peer.evidenceSource || null,
        aliases: [
          peer.peerHotel,
          urbanGfa ? String(peer.peerHotel || "").replace(/^Grand\s+/i, "") : null,
        ].filter(Boolean),
        attributes: urbanGfa
          ? ["full_service", "meetings", "spa", "urban"]
          : ["resort", "all_inclusive_option", "full_service"],
        ...scope,
        officialBaselinePublished: false,
      };
  fs.writeFileSync(fp, `${JSON.stringify(profile, null, 2)}\n`);
  return loadPropertyProfile(propertyId) || profile;
}

export const ADP_GREENFIELD_LIVE_MONITORING_V1 = "ADP_GREENFIELD_LIVE_MONITORING_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function sha16(v) {
  return crypto
    .createHash("sha256")
    .update(typeof v === "string" ? v : JSON.stringify(v))
    .digest("hex")
    .slice(0, 16);
}

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

/**
 * Materialize STANDARD-28 loyalty scenarios for Fiesta Rewards / Grupo Posadas hotels.
 * Geo and hotel name come from the property profile (never hardcode Chapultepec for Condesa).
 */
export function ensureChapultepecLoyaltyManifest({
  propertyId = "adp_grand_fiesta_americana_chapultepec",
  peerSet = null,
  geo = null,
  hotelName = null,
  lensId = null,
} = {}) {
  const profile = loadPropertyProfile(propertyId);
  const dir = path.join(ROOT, "data/ai-demand-positioning/loyalty-bpp-scenario-manifests-v1");
  ensureDir(dir);
  const outPath = path.join(dir, `${propertyId}.json`);
  const peers = peerSet || getBrandPortfolioPeerSet(propertyId);
  const resolvedGeo =
    geo ||
    profile?.city ||
    profile?.market ||
    (propertyId.includes("chapultepec") ? "Mexico City" : "Cancún");
  const resolvedName =
    hotelName ||
    profile?.name ||
    profile?.displayName ||
    propertyId;
  const resolvedLens =
    lensId ||
    peers?.lensId ||
    (propertyId.includes("condesa")
      ? "fiesta_americana_all_inclusive_resort"
      : propertyId.includes("radisson_blu_toronto") ||
          peers?.lensId === "choice_privileges" ||
          /choice privileges/i.test(profile?.loyaltyProgram || "")
        ? "choice_privileges"
        : "fiesta_rewards");
  const choiceLens =
    resolvedLens === "choice_privileges" ||
    propertyId === RADISSON_BLU_TORONTO_PROPERTY_ID ||
    String(propertyId).startsWith("draft_saint_james") ||
    String(propertyId).startsWith("draft_radisson_toronto") ||
    String(propertyId).startsWith("draft_park_inn_radisson") ||
    String(propertyId).startsWith("draft_country_inn_suites_radisson");
  const loyaltyConstraint = choiceLens ? "Choice Privileges" : "Fiesta Rewards";
  const parentConstraint = choiceLens ? "Choice" : "Grupo Posadas";
  const built = buildLoyaltyPromptTemplatesV1({
    loyaltyConstraint,
    parentConstraint,
    geo: resolvedGeo,
    portfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
  });
  const scenarios = built.templates.map((t) => ({
    scenarioId: t.scenarioId,
    query: t.exactPrompt,
    prompt: t.exactPrompt,
    categoryId: t.categoryId,
    frame: t.frame,
    injectsSubjectHotelName: false,
    measurementFamily: MEASUREMENT_FAMILY_LOYALTY_BPP,
  }));
  const manifest = {
    contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
    measurementFamily: MEASUREMENT_FAMILY_LOYALTY_BPP,
    propertyId,
    hotelName: resolvedName,
    generatedAt: new Date().toISOString(),
    density: {
      id: "LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1",
      targetScenarioCount: 28,
      allowedMin: 21,
      allowedMax: 32,
      providerCount: 4,
      providers: [...PROVIDERS],
      expectedObservationsTarget: 112,
    },
    loyaltyConstraint,
    parentConstraint,
    geo: resolvedGeo,
    portfolioType: "LOYALTY_ECOSYSTEM",
    lensId: resolvedLens,
    peerSetId: peers?.peerSetId || null,
    peerCountExcludingSubject: peers?.peerCountExcludingSubject ?? 0,
    templateCount: scenarios.length,
    manifestHash: built.manifestHash,
    expectedObservations: scenarios.length * 4,
    templates: built.templates,
    scenarios,
    paidExecutionAuthorized: false,
  };
  fs.writeFileSync(outPath, `${JSON.stringify(manifest, null, 2)}\n`);
  return { ok: true, path: outPath, manifest, scenarios };
}

async function runFamily({
  propertyId,
  family,
  scenarios,
  profile,
  providers,
  maxSpendUsd,
  remainingUsd,
  runId,
  authorizationId,
  loyaltyManifest = null,
  universe = null,
}) {
  const est = estimateCost(scenarios.length, providers);
  if (est.total > remainingUsd + 1e-9) {
    return {
      ok: false,
      error: "BUDGET_WOULD_EXCEED",
      family,
      estimatedUsd: est.total,
      remainingUsd,
    };
  }
  if (est.total > Number(maxSpendUsd) + 1e-9) {
    return {
      ok: false,
      error: "HARD_MAX_WOULD_EXCEED",
      family,
      estimatedUsd: est.total,
      maxSpendUsd,
    };
  }

  const batchBudget = { remainingUsd };
  const period = await executeMonitoringPeriod({
    propertyId,
    scenarios,
    dryRun: false,
    providers,
    delayMsOverride: 200,
    checkpointEvery: family === MEASUREMENT_FAMILY_CORE_ADP ? 20 : 14,
    maxSpendUsd: Math.min(est.total + 0.5, remainingUsd),
    batchBudget,
    onProgress: (completed, total) => {
      if (completed % 28 === 0 || completed === total) {
        process.stdout.write(
          `\r  [${family}] ${propertyId} ${completed}/${total} rem=$${batchBudget.remainingUsd.toFixed(2)}   `
        );
      }
    },
  });
  process.stdout.write("\n");

  // One retry pass for missing/failed pairs (bounded)
  {
    const missing = [];
    for (const scenario of scenarios) {
      for (const provider of providers) {
        const hit = (period.observations || []).find(
          (o) => o.provider === provider && o.scenarioId === scenario.scenarioId
        );
        if (!hit || hit.error || !(hit.rawResponse || hit.response)) {
          missing.push({ scenario, provider });
        }
      }
    }
    for (const m of missing.slice(0, 12)) {
      const cfg = { ...PROVIDER_CONFIGS[m.provider], timeoutMs: 90000 };
      const nextCost = cfg.costPerCall || 0.03;
      if (batchBudget.remainingUsd + 1e-9 < nextCost) break;
      try {
        await new Promise((r) => setTimeout(r, 400));
        const result = await callProvider(m.provider, m.scenario.query, cfg);
        period.observations = (period.observations || []).filter(
          (o) =>
            !(o.provider === m.provider && o.scenarioId === m.scenario.scenarioId)
        );
        const { createObservation } = await import("../data-model.js");
        const obs = createObservation({
          propertyId,
          scenarioId: m.scenario.scenarioId,
          provider: m.provider,
          periodId: period.periodId,
          response: result.response || "",
        });
        if (result.error && !result.response) obs.error = result.error;
        else {
          obs.rawResponse = result.response;
          obs.model = result.model || cfg.model;
        }
        period.observations.push(obs);
        period.costEstimate = period.costEstimate || { total: 0, byProvider: {} };
        period.costEstimate.byProvider[m.provider] =
          (period.costEstimate.byProvider[m.provider] || 0) + nextCost;
        period.costEstimate.total = (period.costEstimate.total || 0) + nextCost;
        batchBudget.remainingUsd =
          Math.round((batchBudget.remainingUsd - nextCost) * 100) / 100;
      } catch {
        /* keep prior failure */
      }
    }
  }

  let finalPeriod = parseObs(period, profile);
  finalPeriod.measurementFamily = family;
  finalPeriod.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
  finalPeriod.authorizationRef = authorizationId;
  finalPeriod.e2eRunId = runId;
  finalPeriod.certificationStatus = "DRAFT";
  finalPeriod.publicationAuthorized = false;

  if (family === MEASUREMENT_FAMILY_CORE_ADP && universe) {
    finalPeriod = attachFirstOfficialPropertyPeriodMetadata(finalPeriod, {
      measurementContractHash: sha16(universe.scenarioUniverseId || propertyId),
      baselineMarker: `ADP_GREENFIELD_CORE_${propertyId}_20261022`,
      baselineSequence: 1,
      scenarioUniverseVersion: universe.scenarioUniverseVersion,
      entityResolutionVersion: "adp_entity_v1",
      sourceGovernanceVersion: OWNED_SOURCE_CLASSIFICATION_VERSION,
      providerSet: providers,
      certified: false,
      priorComparablePeriod: null,
    });
    finalPeriod.scenarioUniverse = universe;
    finalPeriod.scenarioUniverseId = universe.scenarioUniverseId;
    finalPeriod.scenarioIds = universe.scenarioIds;
  } else if (loyaltyManifest) {
    finalPeriod.loyaltyBppManifest = {
      contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
      manifestHash: loyaltyManifest.manifestHash,
      lensId: loyaltyManifest.lensId,
      peerSetId: loyaltyManifest.peerSetId,
      templateCount: loyaltyManifest.templateCount,
      loyaltyConstraint: loyaltyManifest.loyaltyConstraint,
    };
    finalPeriod.scenarioIds = scenarios.map((s) => s.scenarioId);
  }

  const promptsByScenario = Object.fromEntries(
    scenarios.map((s) => [s.scenarioId, s.query || s.prompt])
  );
  savePeriod(finalPeriod);
  writeDurableObservationArchive(finalPeriod, {
    requireOriginalResponse: true,
    promptsByScenario,
    measurementFamily: family,
  });
  const archiveGate = assertDurableArchiveOrBlock(finalPeriod, {
    requireOriginalResponse: true,
  });
  const readback = readbackDurableObservationArchive(propertyId, finalPeriod.periodId);
  const spent = Number(finalPeriod.costEstimate?.total || 0);
  const expected = scenarios.length * providers.length;
  const withOriginal = (finalPeriod.observations || []).filter(
    (o) => o.rawResponse || (o.response && !o.error)
  ).length;

  return {
    ok: archiveGate.ok === true && withOriginal >= expected * 0.9,
    family,
    periodId: finalPeriod.periodId,
    spentUsd: spent,
    remainingUsd: batchBudget.remainingUsd,
    observations: (finalPeriod.observations || []).length,
    withOriginal,
    expected,
    archiveGate,
    readbackOk: Boolean(readback?.ok || readback?.observations),
    period: finalPeriod,
    scenarios,
  };
}

/**
 * Execute authorized Core (+ Loyalty if applicable) for one greenfield hotel.
 */
export async function executeAuthorizedGreenfieldLiveMonitoring({
  runId,
  propertyId,
  hotelIdentity,
  authorizationRecord = null,
  hardMaxUsd = 13.83,
  runLoyalty = true,
  runCore = true,
} = {}) {
  const auth = authorizationRecord || loadAuthorizationForRun(runId, propertyId);
  if (!auth || auth.status !== "ACTIVE") {
    return { ok: false, error: "MISSING_EXACT_AUTHORIZATION", paidCalls: 0, spentUsd: 0 };
  }
  if (auth.propertyId !== propertyId || auth.runId !== runId) {
    return { ok: false, error: "AUTHORIZATION_SCOPE_MISMATCH", paidCalls: 0, spentUsd: 0 };
  }
  if (/SIMULATION|TEST_HARNESS/i.test(auth.approverIdentity || "")) {
    return { ok: false, error: "SIMULATION_APPROVER_REJECTED", paidCalls: 0, spentUsd: 0 };
  }

  const maxSpend = Math.min(Number(auth.maxSpendUsd), Number(hardMaxUsd));
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    return { ok: false, error: "PROFILE_MISSING", paidCalls: 0, spentUsd: 0 };
  }

  const providers = [...(auth.providerSet || PROVIDERS)];
  let remaining = maxSpend;
  const results = { core: null, loyalty: null, staged: null, audit: null, share: null };

  // Auth may explicitly skip Core / subject Loyalty when already complete (peer-only resume).
  const executeCore =
    runCore !== false &&
    auth.coreScope?.execute !== false &&
    auth.scope?.coreExecute !== false;
  const executeSubjectLoyalty =
    runLoyalty !== false &&
    auth.loyaltyScope?.applicable !== false &&
    auth.loyaltyScope?.execute !== false &&
    auth.scope?.subjectLoyaltyExecute !== false;

  // CORE
  if (!executeCore) {
    const existingCore = loadLatestCoreCustomerPeriod(propertyId);
    results.core = {
      ok: Boolean(existingCore?.observations?.length >= 200),
      skipped: true,
      reason: auth.coreScope?.reason || auth.scope?.coreReason || "CORE_ALREADY_COMPLETE",
      spentUsd: 0,
      observations: existingCore?.observations?.length || 0,
      period: existingCore,
      periodId: existingCore?.periodId || null,
    };
  } else {
    let coreScenarios = buildScenarioUniverse(profile);
    let independentUniverseAudit = null;
    if (profile.requireIndependentUnpromptedCore !== false) {
      const rebuilt = buildIndependentUnpromptedCoreUniverse(profile);
      independentUniverseAudit = rebuilt.audit;
      if (!rebuilt.ok) {
        return {
          ok: false,
          error: "INDEPENDENT_UNPROMPTED_CORE_UNIVERSE_FAILED",
          independentUniverseAudit,
          paidCalls: 0,
          spentUsd: 0,
        };
      }
      coreScenarios = rebuilt.scenarios;
    }
    const universe = buildScenarioUniverseManifest(profile, { scenarios: coreScenarios });
    if (independentUniverseAudit) {
      universe.independentUnpromptedAudit = independentUniverseAudit;
    }
    if (coreScenarios.length < 60 || coreScenarios.length > 65) {
      return {
        ok: false,
        error: `CORE_SCENARIO_COUNT_${coreScenarios.length}_OUT_OF_BAND`,
        paidCalls: 0,
        spentUsd: 0,
      };
    }
    results.core = await runFamily({
      propertyId,
      family: MEASUREMENT_FAMILY_CORE_ADP,
      scenarios: coreScenarios,
      profile,
      providers,
      maxSpendUsd: maxSpend,
      remainingUsd: remaining,
      runId,
      authorizationId: auth.authorizationId,
      universe,
    });
    remaining = results.core.remainingUsd ?? remaining - (results.core.spentUsd || 0);
  }

  // LOYALTY (subject)
  const loyaltyApplicable = executeSubjectLoyalty;
  const peerSet =
    propertyId === CONDESA_PROPERTY_ID
      ? getCondesaPeerSetIfApproved() || getBrandPortfolioPeerSet(propertyId)
      : propertyId === RADISSON_BLU_TORONTO_PROPERTY_ID
        ? getRadissonBluTorontoPeerSetIfApproved() || getBrandPortfolioPeerSet(propertyId)
        : propertyId === "adp_grand_fiesta_americana_chapultepec"
          ? getBrandPortfolioPeerSet(propertyId) // resolves founder-approved Chapultepec cohort
          : getBrandPortfolioPeerSet(propertyId);
  // Comparable GTA Choice Privileges universe uses one shared geo for subject + peers.
  const sharedLoyaltyGeo =
    propertyId === RADISSON_BLU_TORONTO_PROPERTY_ID || peerSet?.lensId === "choice_privileges"
      ? "Toronto"
      : null;
  if (loyaltyApplicable) {
    const loy = ensureChapultepecLoyaltyManifest({
      propertyId,
      peerSet,
      geo: sharedLoyaltyGeo || undefined,
      lensId: peerSet?.lensId || undefined,
    });
    results.loyalty = await runFamily({
      propertyId,
      family: MEASUREMENT_FAMILY_LOYALTY_BPP,
      scenarios: loy.scenarios,
      profile,
      providers,
      maxSpendUsd: maxSpend,
      remainingUsd: remaining,
      runId,
      authorizationId: auth.authorizationId,
      loyaltyManifest: loy.manifest,
    });
    remaining = results.loyalty.remainingUsd ?? remaining - (results.loyalty.spentUsd || 0);
  } else {
    const existingLoy = (loadAllPeriods(propertyId) || []).find((p) =>
      isLoyaltyMeasurementPeriod(p)
    );
    results.loyalty = {
      ok: true,
      skipped: true,
      reason:
        auth.loyaltyScope?.reason ||
        auth.scope?.subjectLoyaltyReason ||
        "SUBJECT_LOYALTY_ALREADY_COMPLETE_OR_NOT_APPLICABLE",
      spentUsd: 0,
      observations: existingLoy?.observations?.length || 0,
      periodId: existingLoy?.periodId || null,
    };
  }

  // PEER LOYALTY — only when authorization binds peer roster and peers lack corpora
  results.peerLoyalty = [];
  const authorizedPeers = Array.isArray(auth.approvedPeerCanonicalIds)
    ? auth.approvedPeerCanonicalIds
    : (peerSet?.included || []).map((p) => p.canonicalEntityId || p.propertyId);
  const runPeerLoyalty = auth.peerLoyaltyAuthorized === true && authorizedPeers.length > 0;
  if (runPeerLoyalty) {
    for (const peer of peerSet?.included || []) {
      const peerId = peer.canonicalEntityId || peer.propertyId;
      if (!peerId || !authorizedPeers.includes(peerId)) continue;
      const existingLoy = (loadAllPeriods(peerId) || []).find((p) =>
        isLoyaltyMeasurementPeriod(p)
      );
      if ((existingLoy?.observations || []).length >= 28 * providers.length * 0.9) {
        results.peerLoyalty.push({
          ok: true,
          reused: true,
          propertyId: peerId,
          peerHotel: peer.peerHotel,
          observations: existingLoy.observations.length,
          spentUsd: 0,
        });
        continue;
      }
      const peerProfile = ensureDraftPeerProfileForLoyalty(peer, {
        peerOf: propertyId || CONDESA_PROPERTY_ID,
      });
      if (!peerProfile) {
        results.peerLoyalty.push({
          ok: false,
          propertyId: peerId,
          peerHotel: peer.peerHotel,
          error: "PEER_PROFILE_UNAVAILABLE",
          spentUsd: 0,
        });
        continue;
      }
      const peerLoy = ensureChapultepecLoyaltyManifest({
        propertyId: peerId,
        peerSet,
        geo: sharedLoyaltyGeo || peer.market || peerProfile.city,
        hotelName: peer.peerHotel || peerProfile.name,
        lensId:
          peerSet?.lensId ||
          (sharedLoyaltyGeo ? "choice_privileges" : "fiesta_americana_all_inclusive_resort"),
      });
      const peerRun = await runFamily({
        propertyId: peerId,
        family: MEASUREMENT_FAMILY_LOYALTY_BPP,
        scenarios: peerLoy.scenarios,
        profile: peerProfile,
        providers,
        maxSpendUsd: maxSpend,
        remainingUsd: remaining,
        runId,
        authorizationId: auth.authorizationId,
        loyaltyManifest: peerLoy.manifest,
      });
      remaining = peerRun.remainingUsd ?? remaining - (peerRun.spentUsd || 0);
      results.peerLoyalty.push({
        ...peerRun,
        propertyId: peerId,
        peerHotel: peer.peerHotel,
      });
      if (!peerRun.ok) break;
    }
  }

  const peerSpent = (results.peerLoyalty || []).reduce(
    (s, r) => s + (Number(r.spentUsd) || 0),
    0
  );
  const peerCalls = (results.peerLoyalty || []).reduce(
    (s, r) => s + (Number(r.observations) || 0),
    0
  );
  const spentUsd = Number(
    ((results.core?.spentUsd || 0) + (results.loyalty?.spentUsd || 0) + peerSpent).toFixed(2)
  );
  // Only count observations that incurred spend this run (skipped Core/subject reuse = 0).
  const paidCalls =
    (results.core?.skipped ? 0 : results.core?.observations || 0) +
    (results.loyalty?.skipped ? 0 : results.loyalty?.observations || 0) +
    peerCalls;

  // Downstream: payload + staged report from Core period.
  // When Core is skipped (already complete), do NOT rebuild staged here —
  // that lacks Core scenarios and would wipe certified Core / BPP intel.
  // Peer-only and subject+peer Loyalty resumes rely on mergeComparableCohortBpp…
  const skipStagedRebuildBecauseCoreAlreadyComplete = results.core?.skipped === true;
  if (results.core?.ok && results.core.period && !skipStagedRebuildBecauseCoreAlreadyComplete) {
    const payload = buildOwnerPayload(results.core.period, results.core.scenarios, profile, {
      allPeriods: [results.core.period],
      realityGapMethodologyId: CONDITIONAL_APPROVED_METHODOLOGY_ID,
      // Core greenfield monitoring uses the independent-unprompted universe for appearance KPIs.
      unpromptedIndependentDiscovery: profile.requireIndependentUnpromptedCore !== false,
    });
    // Governed BPP N/A: stamp accurate customer disclosure (never invent #1-of-1 rank).
    const bppNaStamp =
      profile?.bppPeerEligibility === "BPP_RANK_NOT_APPLICABLE" ||
      (profile?.loyaltyApplicable === false &&
        !getBrandPortfolioPeerSet(propertyId) &&
        !executeSubjectLoyalty &&
        auth.peerLoyaltyAuthorized !== true);
    if (bppNaStamp && payload?.ok !== false) {
      payload.brandPortfolioPosition = {
        status: "BPP_NOT_APPLICABLE",
        customerState: "BPP_NOT_APPLICABLE",
        disclosureReason: "LENS_NOT_APPLICABLE",
        sectionVisible: true,
        renderMode: "STATUS_ONLY",
        headline: "Brand & Portfolio not applicable",
        body:
          profile?.bppPeerEligibilityNote ||
          "Brand & Portfolio Position does not apply: no governed multi-property same-lens peer cohort exists for this hotel, and no loyalty program is measured. Portfolio rank is not calculated.",
        kpis: [],
        peerLoyaltyHotels: 0,
        measuredRank: "BPP_NOT_APPLICABLE",
      };
    }
    results.staged = saveStagedAdpReport({
      propertyId,
      periodId: results.core.periodId,
      runId,
      workflowVersion: ADP_GREENFIELD_LIVE_MONITORING_V1,
      payload,
      certification: {
        status: "DRAFT_GREENFIELD",
        publicationAuthorized: false,
        hpcRecordId: profile.hpcHotelId || profile.censusRecordId || null,
      },
    });

    // Subject-measured BPP: populate Presence/#1/Top-3 from Loyalty even when peers pending.
    if (results.loyalty?.ok && !results.loyalty?.skipped) {
      try {
        const { mergeSubjectMeasuredPartialIntoStagedOwnerReport } = await import(
          "../brand-portfolio/bpp-subject-measured-partial-v1.js"
        );
        results.bppSubjectPartial = mergeSubjectMeasuredPartialIntoStagedOwnerReport(propertyId, {
          periodId: results.loyalty.periodId || null,
        });
        if (results.bppSubjectPartial?.stagedVersionId) {
          results.staged = {
            ...results.staged,
            versionId: results.bppSubjectPartial.stagedVersionId,
            artifactHash: results.bppSubjectPartial.artifactHash,
            bppMeasurementState: "BPP_SUBJECT_MEASURED",
          };
        }
      } catch (e) {
        results.bppSubjectPartial = { ok: false, error: e.message };
      }
    }

    // Auto-register for founder owner-UI discoverability (not customer publication).
    try {
      const { ensureStagedReportRegisteredForOwnerUiDiscoverability } = await import(
        "../certification/adp-owner-ui-staged-report-discoverability-v1.js"
      );
      results.discoverability = ensureStagedReportRegisteredForOwnerUiDiscoverability({
        propertyId,
        stagedVersionId: results.staged?.versionId || null,
        artifactHash: results.staged?.artifactHash || null,
        periodId: results.core.periodId,
        runId,
      });
    } catch (e) {
      results.discoverability = { ok: false, error: e.message };
    }

    try {
      results.audit = auditHotelCustomerReleaseContent({ propertyId });
    } catch (e) {
      results.audit = { error: e.message, p0Count: 99, customerReleaseVerdict: "BLOCKED" };
    }

    // Version-pinned share requires staged PDF — greenfield may stage report before PDF render.
    results.share = {
      ok: false,
      deferred: true,
      reason: "PDF_REQUIRED_FOR_PINNED_SHARE — staged report present; PDF/share follow after render",
      stagedVersionId: results.staged?.versionId || null,
    };
  } else if (peerOnlyResume) {
    results.staged = {
      skipped: true,
      reason: "PEER_ONLY_RESUME_PRESERVE_EXISTING_STAGED",
    };
  }

  const peerLoyaltyOk =
    !runPeerLoyalty ||
    ((results.peerLoyalty || []).length > 0 &&
      (results.peerLoyalty || []).every((r) => r.ok === true || r.reused === true));
  const ok =
    results.core?.ok === true &&
    (results.loyalty?.ok === true || results.loyalty?.skipped) &&
    peerLoyaltyOk;
  return {
    ok,
    contract: ADP_GREENFIELD_LIVE_MONITORING_V1,
    runId,
    propertyId,
    hotelIdentity,
    authorizationId: auth.authorizationId,
    spentUsd,
    remainingAuthorizedUsd: Number((maxSpend - spentUsd).toFixed(2)),
    hardMaxUsd: maxSpend,
    paidCalls,
    exceededHardMax: spentUsd > maxSpend + 1e-9,
    results,
    bppMeasuredRank: (() => {
      const bppNa =
        profile?.bppPeerEligibility === "BPP_RANK_NOT_APPLICABLE" ||
        (profile?.loyaltyApplicable === false && !getBrandPortfolioPeerSet(propertyId));
      if (bppNa && !runPeerLoyalty && !executeSubjectLoyalty) {
        return "BPP_NOT_APPLICABLE";
      }
      const peerOk =
        !runPeerLoyalty ||
        ((results.peerLoyalty || []).length > 0 &&
          (results.peerLoyalty || []).every((r) => r.ok === true));
      if (results.loyalty?.ok && peerOk && runPeerLoyalty) {
        return "SUBJECT_AND_PEER_LOYALTY_EXECUTED_RANK_PENDING_CERTIFICATION";
      }
      if (results.loyalty?.ok) {
        return "SUBJECT_MEASURED_PEER_MEMBERSHIP_APPROVED_PEER_CORPORA_PENDING";
      }
      return "BPP_NOT_MEASURED";
    })(),
    publicationAuthorized: false,
    peerLoyaltyAuthorized: runPeerLoyalty,
    peerLoyaltyResults: results.peerLoyalty,
  };
}
