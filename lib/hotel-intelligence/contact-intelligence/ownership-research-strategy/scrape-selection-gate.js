/**
 * Brazil V1.1 — hard source-selection gate between SERP and paid scrape.
 * Goal-aware ranking; OTA suppression when high-value candidates exist;
 * synthetic/test rejection. No hotel-ID hard-codes.
 */
import { SOURCE_TIER, SOURCE_CLASS, RESEARCH_GOAL_KIND } from "./constants.js";
import { classifySourceTier } from "./source-tier.js";
import {
  isSyntheticOrTestResult,
  normalizeSyntheticDetectionBlob,
  SYNTHETIC_LABEL_RE as SYNTHETIC_RE,
} from "./synthetic-result.js";

const OTA_HOST_RE =
  /booking\.com|tripadvisor|trip\.com|agoda|expedia|hotels\.com|decolar\.com|trivago|kayak|yelp\.|orbitz\.|wotif\.|travelocity|skyscanner|hoteis\.com|hotel\.info|mindtrip|google\.[^/]+\/travel|google\.com\/travel/i;

const PRIMARY_REG_HOST_RE =
  /\.gov\.br|receita\.fazenda|cvm\.gov|b3\.com\.br|juntacomercial|redesim\.gov|transparencia/i;

const REGISTRY_DERIVED_HOST_RE =
  /econodata|cnpj\.biz|cnpjcheck|casadosdados|cnpja|empresometro|brasil\.io|serasaexperian|empresas\.serasa/i;

const FII_BLOB_RE =
  /\bFII\b|fundo\s+(?:imobili[aá]rio|de\s+investimento)|BTG\s+Pactual|CVM\b|ticker|[A-Z]{4}\d{1,2}\b/i;

const LEGAL_PRIVACY_RE =
  /pol[ií]tica\s+de\s+privacidade|termos\s+de\s+uso|privacy\s+policy|terms\s+of\s+(?:use|service)|controladora|raz[aã]o\s+social/i;

const QSA_BLOB_RE =
  /quadro\s+de\s+s[oó]cios|QSA|s[oó]cios?\s+e\s+administradores|s[oó]cio[- ]administrador|administrador(?:a|es)?\b/i;

/**
 * Infer research goal kind from goal text / ladder rung.
 * @param {{ researchGoal?: string, evidence_gap?: string, ladder_rung?: string, query?: string }} goal
 */
export function inferResearchGoalKind(goal = {}) {
  const blob = `${goal.researchGoal || ""} ${goal.evidence_gap || ""} ${goal.ladder_rung || ""} ${goal.query || ""}`.toLowerCase();
  if (/fii|fundo imobili|cvm|investment structure|real.?estate.?fund/.test(blob)) {
    return RESEARCH_GOAL_KIND.FII_INVESTMENT;
  }
  if (/qsa|s[oó]cio|principal|administrador|officer|decision.?maker/.test(blob)) {
    return RESEARCH_GOAL_KIND.PRINCIPALS;
  }
  if (/lineage|rebrand|successor|baixada|historical|current.?identity|renome/.test(blob)) {
    return RESEARCH_GOAL_KIND.PROPERTY_LINEAGE;
  }
  if (/property.?owner|titled|spv|acquisition|propriet[aá]rio|deed|escritura/.test(blob)) {
    return RESEARCH_GOAL_KIND.PROPERTY_OWNER;
  }
  if (/physical|address|identity.?only|contact.?fact/.test(blob)) {
    return RESEARCH_GOAL_KIND.PHYSICAL_IDENTITY;
  }
  if (/cnpj|raz[aã]o|legal.?entity|operat|administradora|privacy|privacidade/.test(blob)) {
    return RESEARCH_GOAL_KIND.LEGAL_ENTITY;
  }
  return RESEARCH_GOAL_KIND.LEGAL_ENTITY;
}

/**
 * @param {{ url?: string, title?: string, snippet?: string }} row
 */
export function classifySourceClass(row = {}) {
  const url = String(row.url || "");
  const blob = `${row.title || ""} ${row.snippet || ""} ${url}`;
  if (isSyntheticOrTestResult(row)) return SOURCE_CLASS.SYNTHETIC_OR_TEST;
  if (OTA_HOST_RE.test(url)) return SOURCE_CLASS.LOW_VALUE_TRAVEL;
  if (PRIMARY_REG_HOST_RE.test(url)) return SOURCE_CLASS.PRIMARY_REGULATORY;
  if (REGISTRY_DERIVED_HOST_RE.test(url)) return SOURCE_CLASS.STRONG_REGISTRY_DERIVED;
  if (FII_BLOB_RE.test(blob) && !OTA_HOST_RE.test(url)) return SOURCE_CLASS.FII_OR_INVESTOR;
  if (LEGAL_PRIVACY_RE.test(blob) && !OTA_HOST_RE.test(url)) return SOURCE_CLASS.OFFICIAL_LEGAL_OR_PRIVACY;
  if (/\bcnpj\b|raz[aã]o\s+social/i.test(blob) && !OTA_HOST_RE.test(url)) {
    return SOURCE_CLASS.STRONG_REGISTRY_DERIVED;
  }
  const tier = classifySourceTier(row);
  if (tier.tier === SOURCE_TIER.TIER_4_WEAK) return SOURCE_CLASS.LOW_VALUE_TRAVEL;
  if (tier.tier === SOURCE_TIER.TIER_3_CORROBORATING) return SOURCE_CLASS.CORROBORATING;
  return SOURCE_CLASS.UNKNOWN;
}

function isHighValueOwnershipClass(sourceClass) {
  return [
    SOURCE_CLASS.PRIMARY_REGULATORY,
    SOURCE_CLASS.STRONG_REGISTRY_DERIVED,
    SOURCE_CLASS.OFFICIAL_LEGAL_OR_PRIVACY,
    SOURCE_CLASS.FII_OR_INVESTOR,
  ].includes(sourceClass);
}

function isLowValueTravel(sourceClass) {
  return sourceClass === SOURCE_CLASS.LOW_VALUE_TRAVEL;
}

/**
 * Goal-aware priority boost (higher = better for paid ownership scrape).
 * @param {string} sourceClass
 * @param {string} goalKind
 * @param {{ title?: string, snippet?: string, url?: string }} row
 */
function goalAwareBoost(sourceClass, goalKind, row = {}) {
  const blob = `${row.title || ""} ${row.snippet || ""} ${row.url || ""}`;
  let boost = 0;
  switch (goalKind) {
    case RESEARCH_GOAL_KIND.LEGAL_ENTITY:
      if (sourceClass === SOURCE_CLASS.PRIMARY_REGULATORY) boost += 20;
      if (sourceClass === SOURCE_CLASS.STRONG_REGISTRY_DERIVED) boost += 18;
      if (sourceClass === SOURCE_CLASS.OFFICIAL_LEGAL_OR_PRIVACY) boost += 22;
      if (/\bcnpj\b|raz[aã]o/i.test(blob)) boost += 6;
      break;
    case RESEARCH_GOAL_KIND.PROPERTY_OWNER:
    case RESEARCH_GOAL_KIND.FII_INVESTMENT:
      if (sourceClass === SOURCE_CLASS.FII_OR_INVESTOR) boost += 24;
      if (sourceClass === SOURCE_CLASS.PRIMARY_REGULATORY) boost += 18;
      if (sourceClass === SOURCE_CLASS.STRONG_REGISTRY_DERIVED) boost += 12;
      if (FII_BLOB_RE.test(blob)) boost += 10;
      break;
    case RESEARCH_GOAL_KIND.PRINCIPALS:
      if (QSA_BLOB_RE.test(blob)) boost += 20;
      if (sourceClass === SOURCE_CLASS.STRONG_REGISTRY_DERIVED) boost += 14;
      if (sourceClass === SOURCE_CLASS.PRIMARY_REGULATORY) boost += 16;
      break;
    case RESEARCH_GOAL_KIND.PROPERTY_LINEAGE:
      if (sourceClass === SOURCE_CLASS.OFFICIAL_LEGAL_OR_PRIVACY) boost += 12;
      if (/baixada|rebrand|successor|histor/i.test(blob)) boost += 8;
      break;
    case RESEARCH_GOAL_KIND.PHYSICAL_IDENTITY:
      // OTAs allowed for identity-only goals
      if (isLowValueTravel(sourceClass)) boost += 2;
      break;
    default:
      break;
  }
  if (isLowValueTravel(sourceClass) && goalKind !== RESEARCH_GOAL_KIND.PHYSICAL_IDENTITY) {
    boost -= 30;
  }
  return boost;
}

/**
 * Evaluate one SERP row for paid scrape eligibility.
 * @param {{ url?: string, title?: string, snippet?: string, score?: number }} row
 * @param {{
 *   hotel?: object,
 *   research_goal_kind?: string,
 *   researchGoal?: string,
 *   evidence_gap?: string,
 *   high_value_unsatisfied_exists?: boolean,
 * }} ctx
 */
export function evaluateScrapeCandidate(row = {}, ctx = {}) {
  const url = String(row.url || "");
  const title = String(row.title || "");
  const snippet = String(row.snippet || row.description || "");
  const blob = `${title} ${snippet} ${url}`;
  const goalKind =
    ctx.research_goal_kind ||
    inferResearchGoalKind({
      researchGoal: ctx.researchGoal,
      evidence_gap: ctx.evidence_gap,
    });
  const sourceClass = classifySourceClass(row);
  const tierInfo = classifySourceTier(row, {
    hotel_address: ctx.hotel?.address || ctx.hotel?.street_address || "",
  });
  const ownership_signals = [...(tierInfo.reasons || [])];
  const negative_signals = [];
  if (isLowValueTravel(sourceClass)) negative_signals.push("LOW_VALUE_TRAVEL");
  if (sourceClass === SOURCE_CLASS.SYNTHETIC_OR_TEST) {
    negative_signals.push("SYNTHETIC_OR_TEST_RESULT");
  }

  const hotelName = String(ctx.hotel?.hotel_name || ctx.hotel?.official_name || "").toLowerCase();
  const city = String(ctx.hotel?.city || "").toLowerCase();
  let property_identity_match = "UNKNOWN";
  if (hotelName && blob.toLowerCase().includes(hotelName.split(/\s+/).slice(0, 2).join(" "))) {
    property_identity_match = city && blob.toLowerCase().includes(city) ? "STRONG" : "NAME_ONLY";
  }

  const baseScore = Number(row.score) || 0;
  const scrape_priority_score =
    baseScore + goalAwareBoost(sourceClass, goalKind, row) + (tierInfo.tier === 1 ? 5 : 0);

  let scrape_decision = "SELECT";
  let scrape_decision_reason = "HIGH_VALUE_OR_GOAL_ALIGNED";

  if (sourceClass === SOURCE_CLASS.SYNTHETIC_OR_TEST) {
    scrape_decision = "REJECT";
    scrape_decision_reason = "SYNTHETIC_OR_TEST_RESULT_REJECTED";
  } else if (
    isLowValueTravel(sourceClass) &&
    goalKind !== RESEARCH_GOAL_KIND.PHYSICAL_IDENTITY &&
    ctx.high_value_unsatisfied_exists === true
  ) {
    scrape_decision = "REJECT";
    scrape_decision_reason = "SCRAPE_REJECTED_HIGHER_VALUE_SOURCE_AVAILABLE";
  } else if (
    isLowValueTravel(sourceClass) &&
    goalKind !== RESEARCH_GOAL_KIND.PHYSICAL_IDENTITY &&
    !OWNERSHIP_LEAD_IN_OTA(blob)
  ) {
    scrape_decision = "REJECT";
    scrape_decision_reason = "TIER_4_SUPPRESS_NO_OWNERSHIP_LEAD";
  }

  return {
    url,
    host: safeHost(url),
    title,
    snippet: snippet.slice(0, 400),
    source_class: sourceClass,
    source_tier: tierInfo.tier,
    ownership_signals,
    negative_signals,
    property_identity_match,
    entity_identity_match: null,
    research_goal_match: goalKind,
    scrape_priority_score,
    scrape_decision,
    scrape_decision_reason,
  };
}

function OWNERSHIP_LEAD_IN_OTA(blob) {
  return /\bCNPJ\b|raz[aã]o\s+social|s[oó]cio|administradora|FII\b|propriet[aá]rio/i.test(blob);
}

function safeHost(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

/**
 * Apply hard selection gate to a ranked SERP list.
 * Preserves rejected URLs for later physical-identity goals.
 *
 * @param {Array} rankedRows — from rankOwnershipSourcesDetailed
 * @param {{ hotel?: object, researchGoal?: string, evidence_gap?: string, research_goal_kind?: string }} ctx
 */
export function selectUrlsForPaidScrape(rankedRows = [], ctx = {}) {
  const goalKind =
    ctx.research_goal_kind ||
    inferResearchGoalKind({
      researchGoal: ctx.researchGoal,
      evidence_gap: ctx.evidence_gap,
    });

  const pre = (rankedRows || []).map((r) => {
    const sourceClass = classifySourceClass(r);
    return { row: r, sourceClass, isHigh: isHighValueOwnershipClass(sourceClass) };
  });
  const highValueExists = pre.some((p) => p.isHigh);

  const evaluated = pre.map(({ row }) => {
    const ev = evaluateScrapeCandidate(row, {
      ...ctx,
      research_goal_kind: goalKind,
      high_value_unsatisfied_exists: highValueExists,
    });
    return { ...row, scrape_evaluation: ev };
  });

  evaluated.sort(
    (a, b) =>
      Number(b.scrape_evaluation?.scrape_priority_score || 0) -
      Number(a.scrape_evaluation?.scrape_priority_score || 0)
  );

  const selected = evaluated.filter((r) => r.scrape_evaluation?.scrape_decision === "SELECT");
  const rejected = evaluated.filter((r) => r.scrape_evaluation?.scrape_decision === "REJECT");

  return {
    version: "brazil-v1.1-scrape-selection-gate",
    research_goal_kind: goalKind,
    high_value_unsatisfied_exists: highValueExists,
    selected,
    rejected,
    deferred_low_value: rejected.filter(
      (r) =>
        r.scrape_evaluation?.scrape_decision_reason ===
        "SCRAPE_REJECTED_HIGHER_VALUE_SOURCE_AVAILABLE"
    ),
  };
}

/**
 * Build follow-up query goals from SERP snippet leads (no scrape required).
 * @param {Array} searchHits
 * @param {object} hotel
 */
export function buildSerpLeadFollowUpGoals(searchHits = [], hotel = {}) {
  const goals = [];
  const seen = new Set();
  const city = hotel.city || "";
  const addr = hotel.address || hotel.street_address || "";

  for (const hit of searchHits || []) {
    const blob = `${hit.title || ""} ${hit.snippet || ""} ${hit.url || ""}`;
    const cnpjMatch = blob.match(/\b(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2})\b/);
    if (cnpjMatch) {
      const digits = cnpjMatch[1].replace(/\D/g, "");
      if (digits.length === 14 && !seen.has(`cnpj:${digits}`)) {
        seen.add(`cnpj:${digits}`);
        goals.push({
          query: `"${digits}" CNPJ`,
          researchGoal: "Resolve exact CNPJ discovered in SERP snippet",
          evidence_gap: "missing_legal_entity",
          ladder_rung: "LEGAL_ENTITY_DISCOVERY",
          from: "serp_cnpj_pivot",
        });
        goals.push({
          query: `"${digits}" QSA OR "sócios e administradores"`,
          researchGoal: "Extract QSA / principals for discovered CNPJ",
          evidence_gap: "missing_principals",
          ladder_rung: "PRINCIPAL_QSA_DISCOVERY",
          from: "serp_cnpj_qsa_pivot",
        });
      }
    }
    const razao = blob.match(
      /(?:raz[aã]o\s+social|controladora)\s*[:\-]?\s*([A-ZÁÉÍÓÚÂÊÔÃÕÇ0-9][^.\n|]{4,90}(?:LTDA|Ltda|S\.?\s?A\.?))/i
    );
    if (razao && !seen.has(`razao:${razao[1].toLowerCase()}`)) {
      seen.add(`razao:${razao[1].toLowerCase()}`);
      goals.push({
        query: `"${razao[1].trim()}" CNPJ`,
        researchGoal: "Resolve exact legal entity from SERP",
        evidence_gap: "missing_legal_entity",
        ladder_rung: "LEGAL_ENTITY_DISCOVERY",
        from: "serp_razao_pivot",
      });
    }
    if (/\badministradora\s+hoteleira\b/i.test(blob)) {
      const key = "admin_hoteleira";
      if (!seen.has(key)) {
        seen.add(key);
        goals.push({
          query: `"${hotel.hotel_name || ""}" "administradora hoteleira" CNPJ`.trim(),
          researchGoal: "Resolve Administradora Hoteleira as operating/management entity",
          evidence_gap: "missing_operating_entity",
          ladder_rung: "ENTITY_ROLE_CLASSIFICATION",
          from: "serp_administradora_pivot",
        });
        goals.push({
          query: addr
            ? `"${addr}" CNPJ OR proprietário OR FII`
            : `"${hotel.hotel_name || ""}" ${city} (FII OR CVM OR "fundo imobiliário" OR proprietário)`,
          researchGoal: "Continue PROPERTY_OWNER separately from operating entity",
          evidence_gap: "missing_property_owner",
          ladder_rung: "PROPERTY_OWNERSHIP_DISCOVERY",
          from: "serp_owner_after_operator_pivot",
        });
      }
    }
    const fii = blob.match(/\b([A-Z]{4}\d{1,2})\b.*\bFII\b|\bFII\b.*\b([A-Z]{4}\d{1,2})\b|BTG\s+Pactual\s+Hot[eé]is/i);
    if (fii && !seen.has("fii")) {
      seen.add("fii");
      const ticker = fii[1] || fii[2] || "FII";
      goals.push({
        query: `"${ticker}" FII ${hotel.hotel_name || city || ""}`.trim(),
        researchGoal: "Research FII/investment structure lead from SERP",
        evidence_gap: "missing_fii_structure",
        ladder_rung: "PROPERTY_OWNERSHIP_DISCOVERY",
        from: "serp_fii_pivot",
      });
    }
  }

  // Address pivots when name-only ambiguity risk (caller may also trigger after CONFLICTING_LOCATION)
  if (addr && !seen.has("addr_cnpj")) {
    seen.add("addr_cnpj");
    goals.push({
      query: `"${addr}" CNPJ`,
      researchGoal: "Exact-address CNPJ after name ambiguity risk",
      evidence_gap: "missing_legal_entity",
      ladder_rung: "LEGAL_ENTITY_DISCOVERY",
      from: "address_cnpj_pivot",
      address_based: true,
    });
  }

  return goals;
}

export {
  OTA_HOST_RE,
  SYNTHETIC_RE,
  isHighValueOwnershipClass,
  isLowValueTravel,
  isSyntheticOrTestResult,
  normalizeSyntheticDetectionBlob,
};
