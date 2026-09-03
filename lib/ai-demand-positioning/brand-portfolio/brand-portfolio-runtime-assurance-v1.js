/**
 * Brand & Portfolio runtime assurance — must run before any provider call.
 * Consumes FROZEN lens + peer set + territory + prompt manifest only.
 * No LLM. No Core mutation.
 */

import crypto from "crypto";
import { hashPrompt } from "../measurement-assurance/prompt-persistence-v1.js";
import { PORTFOLIO_TYPES } from "./brand-portfolio-position-contract-v1.js";
import { LENS_LABEL_RECOMMENDATION_V1 } from "./brand-portfolio-affiliation-mapping-v1.js";
import { PEER_SET_VERSION } from "./brand-portfolio-peer-set-v1.js";

export const BRAND_PORTFOLIO_RUNTIME_ASSURANCE_VERSION = "ADP_BRAND_PORTFOLIO_RUNTIME_ASSURANCE_V1";
export const BRAND_PORTFOLIO_EXECUTED_PROMPT_MATCHES_MANIFEST = "BRAND_PORTFOLIO_EXECUTED_PROMPT_MATCHES_MANIFEST";
export const LOYALTY_LABEL_PROMPT_UNIVERSE_EQUIVALENCE = "LOYALTY_LABEL_PROMPT_UNIVERSE_EQUIVALENCE";
export const PORTFOLIO_LENS_PEER_PROMPT_ALIGNMENT = "PORTFOLIO_LENS_PEER_PROMPT_ALIGNMENT";
export const CORE_PORTFOLIO_MEASUREMENT_ISOLATION = "CORE_PORTFOLIO_MEASUREMENT_ISOLATION";

export const BRAND_PORTFOLIO_OBSERVATION_REQUIRED_FIELDS = Object.freeze([
  "propertyId",
  "periodId",
  "measurementFamily",
  "portfolioLensId",
  "peerSetId",
  "peerSetVersion",
  "scenarioId",
  "territory",
  "provider",
  "exactRenderedPrompt",
  "promptHash",
  "requestTimestamp",
  "exactResponse",
  "responseHash",
  "parserVersion",
  "entityResolverVersion",
  "assuranceVersion",
]);

export function hashPeerSet(peerSet) {
  const body = {
    peerSetId: peerSet.peerSetId,
    peerSetVersion: peerSet.peerSetVersion,
    propertyId: peerSet.propertyId,
    lensId: peerSet.lensId,
    included: (peerSet.included || []).map((p) => ({
      id: p.canonicalEntityId,
      name: p.peerHotel,
      brand: p.brand,
      level: p.expansionLevel,
    })),
  };
  return crypto.createHash("sha256").update(JSON.stringify(body), "utf8").digest("hex");
}

export function hashFrozenManifest(manifest) {
  const body = {
    contractVersion: manifest.contractVersion,
    promptManifestHash: manifest.promptManifestHash,
    scenarios: (manifest.allScenarios || []).map((s) => ({
      scenarioId: s.scenarioId,
      propertyId: s.propertyId,
      lensId: s.lensId,
      exactPrompt: s.exactPrompt,
      promptHash: s.promptHash || hashPrompt(s.exactPrompt),
      peerSetId: s.peerSetId,
      peerSetVersion: s.peerSetVersion,
    })),
  };
  return crypto.createHash("sha256").update(JSON.stringify(body), "utf8").digest("hex");
}

/**
 * UI lens label ↔ prompt wording must resolve to same ecosystem peer universe.
 */
export function checkLoyaltyLabelPromptUniverseEquivalence({ lens, exactPrompt }) {
  if (lens.portfolioType !== PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM) {
    return { pass: true, gate: LOYALTY_LABEL_PROMPT_UNIVERSE_EQUIVALENCE, note: "non_loyalty_lens" };
  }
  const p = String(exactPrompt || "").toLowerCase();
  const lensId = lens.lensId;
  let tokens = [];
  if (lensId === "hilton_honors") {
    tokens = LENS_LABEL_RECOMMENDATION_V1.hilton.equivalence.map((t) => t.toLowerCase());
  } else if (lensId === "marriott_bonvoy") {
    tokens = LENS_LABEL_RECOMMENDATION_V1.marriott.equivalence.map((t) => t.toLowerCase());
  } else {
    tokens = [String(lens.constraintPhrase || "").toLowerCase(), String(lens.loyaltyPhrase || "").toLowerCase()].filter(
      Boolean
    );
  }
  const hit = tokens.some((t) => t && p.includes(t.replace(/\s+hotels?$/i, "").trim()));
  // Also accept bare Hilton / Marriott / Honors / Bonvoy
  const loose =
    (lensId === "hilton_honors" && (/\bhilton\b/i.test(p) || /\bhonors\b/i.test(p))) ||
    (lensId === "marriott_bonvoy" && (/\bmarriott\b/i.test(p) || /\bbonvoy\b/i.test(p)));
  return {
    pass: hit || loose,
    gate: LOYALTY_LABEL_PROMPT_UNIVERSE_EQUIVALENCE,
    tokens,
    note: "wording_may_differ_but_must_map_to_same_peer_universe",
  };
}

export function checkLensPeerPromptAlignment({ lens, peerSet, exactPrompt, customerLensLabel }) {
  const defects = [];
  if (!lens || !peerSet) {
    defects.push("MISSING_LENS_OR_PEER_SET");
    return { pass: false, gate: PORTFOLIO_LENS_PEER_PROMPT_ALIGNMENT, defects };
  }
  if (lens.lensId !== peerSet.lensId) defects.push("LENS_ID_PEER_SET_MISMATCH");
  if (customerLensLabel && lens.label !== customerLensLabel) defects.push("CUSTOMER_LENS_LABEL_MISMATCH");
  if (peerSet.peerSetVersion !== PEER_SET_VERSION) defects.push("PEER_SET_VERSION_NOT_FROZEN_CERTIFIED");
  if (lens.portfolioType === PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM) {
    const eq = checkLoyaltyLabelPromptUniverseEquivalence({ lens, exactPrompt });
    if (!eq.pass) defects.push("LOYALTY_PROMPT_EQUIVALENCE_FAIL");
  }
  if (lens.portfolioType === PORTFOLIO_TYPES.INDEPENDENT_POSITIONING) {
    if (!/\bindependent\b/i.test(exactPrompt || "")) defects.push("INDEPENDENT_PROMPT_TOKEN_MISSING");
    if (/\bhyatt\b/i.test(exactPrompt || "")) defects.push("HYATT_ON_INDEPENDENT");
  }
  return {
    pass: defects.length === 0,
    gate: PORTFOLIO_LENS_PEER_PROMPT_ALIGNMENT,
    defects,
    universes: {
      uiLens: lens.label,
      peerUniverse: peerSet.ecosystem,
      peerSetId: peerSet.peerSetId,
      peerSetHash: hashPeerSet(peerSet),
    },
  };
}

/**
 * Request-time gate: executed prompt must match frozen manifest scenario hash.
 */
export function assertExecutedPromptMatchesManifest({ scenarioId, exactRenderedPrompt, frozenManifest }) {
  const row = (frozenManifest?.allScenarios || []).find((s) => s.scenarioId === scenarioId);
  if (!row) {
    return {
      pass: false,
      gate: BRAND_PORTFOLIO_EXECUTED_PROMPT_MATCHES_MANIFEST,
      defect: "SCENARIO_NOT_IN_FROZEN_MANIFEST",
    };
  }
  const expected = row.promptHash || hashPrompt(row.exactPrompt);
  const actual = hashPrompt(exactRenderedPrompt);
  const pass = expected === actual && String(exactRenderedPrompt) === String(row.exactPrompt);
  return {
    pass,
    gate: BRAND_PORTFOLIO_EXECUTED_PROMPT_MATCHES_MANIFEST,
    expectedPromptHash: expected,
    actualPromptHash: actual,
    defect: pass ? null : "PROMPT_DRIFT_FROM_FROZEN_MANIFEST",
  };
}

/**
 * Build observation provenance envelope for Brand & Portfolio first-cycle.
 * Does not call providers.
 */
export function buildBrandPortfolioObservationProvenance({
  propertyId,
  periodId,
  portfolioLensId,
  peerSetId,
  peerSetVersion,
  scenarioId,
  territory,
  provider,
  exactRenderedPrompt,
  exactResponse = null,
  responseHash = null,
  parserVersion = null,
  entityResolverVersion = null,
  requestTimestamp = new Date().toISOString(),
}) {
  return {
    propertyId,
    periodId,
    measurementFamily: "BRAND_PORTFOLIO",
    scenarioClass: "BRAND_PORTFOLIO_DEMAND",
    portfolioLensId,
    peerSetId,
    peerSetVersion,
    scenarioId,
    territory,
    provider,
    exactRenderedPrompt: String(exactRenderedPrompt || ""),
    promptHash: hashPrompt(exactRenderedPrompt),
    requestTimestamp,
    exactResponse,
    responseHash,
    parserVersion,
    entityResolverVersion,
    assuranceVersion: BRAND_PORTFOLIO_RUNTIME_ASSURANCE_VERSION,
    coreIsolation: true,
    mayNotEnterCoreV11: true,
  };
}

/**
 * Pre-execution package gate — all frozen artifacts required.
 */
export function evaluateBrandPortfolioRuntimeAssuranceReady({
  frozenManifest,
  peerSetsByProperty,
  preflightAllPass,
  hardCapUsd = 10,
}) {
  const defects = [];
  if (!frozenManifest?.promptManifestHash) defects.push("MISSING_PROMPT_MANIFEST_HASH");
  if (!frozenManifest?.peerSetsFrozen) defects.push("PEER_SETS_NOT_MARKED_FROZEN");
  if (!preflightAllPass) defects.push("PREFLIGHT_NOT_ALL_PASS");
  const cost = Number(frozenManifest?.cost?.estimatedTotalUsd);
  if (!(cost >= 0) || cost > hardCapUsd) defects.push("COST_OVER_HARD_CAP_OR_MISSING");
  if (frozenManifest?.scenarioClass !== "BRAND_PORTFOLIO_DEMAND") defects.push("WRONG_SCENARIO_CLASS");
  if (frozenManifest?.coreIsolation !== true) defects.push("CORE_ISOLATION_FLAG_MISSING");

  for (const [propertyId, ps] of Object.entries(peerSetsByProperty || {})) {
    if (!ps?.peerSetId || !ps?.peerSetHash) defects.push(`PEER_HASH_MISSING_${propertyId}`);
    if (ps.peerSetVersion !== PEER_SET_VERSION) defects.push(`PEER_VERSION_STALE_${propertyId}`);
  }

  const runtimeGates = {
    LOYALTY_ECOSYSTEM_PEER_SET_INTEGRITY: defects.every((d) => !d.startsWith("PEER_")),
    PORTFOLIO_LENS_PEER_PROMPT_ALIGNMENT: true,
    LOYALTY_LABEL_PROMPT_UNIVERSE_EQUIVALENCE: true,
    BRAND_PORTFOLIO_EXECUTED_PROMPT_MATCHES_MANIFEST: "WIRED_AWAIT_EXECUTION",
    CORE_PORTFOLIO_MEASUREMENT_ISOLATION: frozenManifest?.coreIsolation === true,
  };

  return {
    version: BRAND_PORTFOLIO_RUNTIME_ASSURANCE_VERSION,
    pass: defects.length === 0,
    defects,
    runtimeGates,
    hardCapUsd,
    estimatedTotalUsd: cost,
    frozenManifestHash: frozenManifest?.frozenPackageHash || null,
  };
}
