/**
 * Source tier classification + scrape triage for ownership research.
 */
import { SOURCE_TIER } from "./constants.js";
import { isSyntheticOrTestResult } from "./synthetic-result.js";

const TIER1_HOST_RE =
  /\.gov\.br|receita\.fazenda|gov\.br\/|cvm\.gov|b3\.com\.br|sec\.gov|edgar|juntacomercial|redesim\.gov|empresometro|cnpj\.biz|casadosdados|econodata|cnpja|brasil\.io|transparencia/i;

const TIER1_BLOB_RE =
  /\b(CNPJ|raz[aã]o\s+social|NFS-?e|Receita\s+Federal|CVM|quadro\s+de\s+s[oó]cios|QSA|pol[ií]tica\s+de\s+privacidade|termos\s+de\s+uso|nota\s+fiscal|escritura|matricula|filing|10-K|investor\s+relations)\b/i;

const TIER2_HOST_RE =
  /casadosdados|econodata|cnpj\.biz|cnpja|empresas\.|socios?\.|portfolio|investor|ri\./i;

const TIER3_BLOB_RE =
  /\b(linkedin\.com|aquisi[cç][aã]o|adquiriu|investidor|incorporadora|desenvolvimento|hoteleir)/i;

const TIER4_HOST_RE =
  /booking\.com|tripadvisor|trip\.com|agoda|expedia|hotels\.com|decolar\.com|trivago|kayak|yelp\.|hotel\.info|mindtrip|hoteisdopantanal|all\.accor\.com\/hotel|wyndhamhotels\.com\/.*\/overview/i;

const TIER4_BLOB_RE =
  /\b(reserv|room rate|amenities|guest review|best price|book now|check[- ]in)\b/i;

const OWNERSHIP_LEAD_RE =
  /\b(CNPJ|raz[aã]o\s+social|s[oó]cio|administrador|propriet[aá]rio|investidor|aquisi[cç][aã]o|venda|incorporadora|administradora|FII|CVM|pol[ií]tica\s+de\s+privacidade|termos\s+de\s+uso)\b/i;

/**
 * @param {{ url?: string, title?: string, snippet?: string }} row
 * @param {{ hotel_address?: string }} [ctx]
 */
export function classifySourceTier(row = {}, ctx = {}) {
  const url = String(row.url || "");
  const blob = `${row.title || ""} ${row.snippet || ""} ${url}`;
  const reasons = [];
  let tier = SOURCE_TIER.TIER_3_CORROBORATING;

  if (isSyntheticOrTestResult(row)) {
    return {
      tier: SOURCE_TIER.TIER_4_WEAK,
      reasons: ["SYNTHETIC_OR_TEST_RESULT"],
      blob_has_ownership_lead: false,
      synthetic: true,
    };
  }

  if (TIER4_HOST_RE.test(url) || TIER4_BLOB_RE.test(blob)) {
    tier = SOURCE_TIER.TIER_4_WEAK;
    reasons.push("WEAK_OTA_OR_TRAVEL");
  }
  if (TIER1_HOST_RE.test(url) || TIER1_BLOB_RE.test(blob)) {
    tier = SOURCE_TIER.TIER_1_PRIMARY_REGULATORY;
    reasons.push("PRIMARY_REGULATORY_OR_LEGAL");
  } else if (TIER2_HOST_RE.test(url)) {
    tier = Math.min(tier, SOURCE_TIER.TIER_2_STRONG_CORPORATE);
    reasons.push("STRONG_CORPORATE_DATA");
  } else if (TIER3_BLOB_RE.test(blob) && tier > SOURCE_TIER.TIER_3_CORROBORATING) {
    tier = SOURCE_TIER.TIER_3_CORROBORATING;
    reasons.push("CORROBORATING_PRESS_OR_PERSON");
  }

  const addr = String(ctx.hotel_address || "").trim();
  if (addr) {
    const addrTokens = addr
      .toLowerCase()
      .split(/[^a-z0-9áéíóúâêôãõç]+/i)
      .filter((t) => t.length >= 3);
    const hit = addrTokens.filter((t) => blob.toLowerCase().includes(t)).length;
    if (hit >= Math.min(3, addrTokens.length)) {
      reasons.push("EXACT_ADDRESS_MATCH_IN_SNIPPET");
      if (tier > SOURCE_TIER.TIER_2_STRONG_CORPORATE) tier = SOURCE_TIER.TIER_2_STRONG_CORPORATE;
    }
  }

  if (OWNERSHIP_LEAD_RE.test(blob)) reasons.push("OWNERSHIP_LEGAL_LEAD_IN_SNIPPET");

  return { tier, reasons: [...new Set(reasons)], blob_has_ownership_lead: OWNERSHIP_LEAD_RE.test(blob) };
}

/**
 * Decide whether to pay-scrape a SERP row.
 * Tier 4: only if snippet has a specific ownership/legal lead.
 * @param {{ url?: string, title?: string, snippet?: string }} row
 * @param {{ hotel_address?: string }} [ctx]
 */
export function shouldScrapeForOwnership(row = {}, ctx = {}) {
  const classified = classifySourceTier(row, ctx);
  if (classified.synthetic) {
    return {
      select: false,
      reason: "SYNTHETIC_OR_TEST_RESULT_REJECTED",
      tier: classified.tier,
      reasons: classified.reasons,
    };
  }
  if (classified.tier <= SOURCE_TIER.TIER_2_STRONG_CORPORATE) {
    return {
      select: true,
      reason: `TIER_${classified.tier}_HIGH_VALUE`,
      tier: classified.tier,
      tier_reasons: classified.reasons,
    };
  }
  if (classified.tier === SOURCE_TIER.TIER_3_CORROBORATING && classified.blob_has_ownership_lead) {
    return {
      select: true,
      reason: "TIER_3_WITH_OWNERSHIP_LEAD",
      tier: classified.tier,
      tier_reasons: classified.reasons,
    };
  }
  if (classified.tier === SOURCE_TIER.TIER_4_WEAK) {
    if (classified.blob_has_ownership_lead) {
      return {
        select: true,
        reason: "TIER_4_EXCEPTION_OWNERSHIP_LEAD_IN_SNIPPET",
        tier: classified.tier,
        tier_reasons: classified.reasons,
      };
    }
    return {
      select: false,
      reason: "TIER_4_SUPPRESS_NO_OWNERSHIP_LEAD",
      tier: classified.tier,
      tier_reasons: classified.reasons,
    };
  }
  return {
    select: false,
    reason: "INSUFFICIENT_OWNERSHIP_SIGNAL",
    tier: classified.tier,
    tier_reasons: classified.reasons,
  };
}

/**
 * Score adjustment for rankOwnershipSourcesDetailed (+boost / −penalty).
 * @param {{ url?: string, title?: string, snippet?: string }} row
 * @param {{ hotel_address?: string }} [ctx]
 */
export function sourceTierRankDelta(row = {}, ctx = {}) {
  const { tier, reasons, blob_has_ownership_lead } = classifySourceTier(row, ctx);
  let delta = 0;
  const rank_reasons = [];
  if (tier === SOURCE_TIER.TIER_1_PRIMARY_REGULATORY) {
    delta += 8;
    rank_reasons.push("STRATEGY_TIER_1");
  } else if (tier === SOURCE_TIER.TIER_2_STRONG_CORPORATE) {
    delta += 5;
    rank_reasons.push("STRATEGY_TIER_2");
  } else if (tier === SOURCE_TIER.TIER_3_CORROBORATING) {
    delta += 1;
    rank_reasons.push("STRATEGY_TIER_3");
  } else if (tier === SOURCE_TIER.TIER_4_WEAK) {
    delta -= 12;
    rank_reasons.push("STRATEGY_TIER_4_PENALTY");
    if (!blob_has_ownership_lead) {
      delta -= 8;
      rank_reasons.push("STRATEGY_OTA_SUPPRESS");
    }
  }
  if (reasons.includes("EXACT_ADDRESS_MATCH_IN_SNIPPET")) {
    delta += 3;
    rank_reasons.push("STRATEGY_ADDRESS_MATCH");
  }
  if (blob_has_ownership_lead) {
    delta += 2;
    rank_reasons.push("STRATEGY_LEGAL_LEAD");
  }
  return { delta, tier, rank_reasons, reasons };
}

/** Alias for callers that import the pluralized name. */
export { classifySourceTier as classifySourceTiers };
