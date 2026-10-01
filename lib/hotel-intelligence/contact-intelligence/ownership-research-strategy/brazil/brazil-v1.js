/**
 * Brazil Ownership Resolution Ladder V1 — country strategy.
 * Extensible via resolveOwnershipResearchStrategy(country); not a Brazil-only engine fork.
 */
import {
  STRATEGY_VERSION,
  OWNERSHIP_CONCLUSION_STATE,
} from "../constants.js";
import {
  extractLegalEntityBundle,
  isNonAuthoritativeCnpj,
  normalizeCnpj,
} from "../legal-entity-extract.js";
import { shouldScrapeForOwnership, sourceTierRankDelta, classifySourceTiers } from "../source-tier.js";
import { assessPropertyLineage } from "../property-lineage.js";
import {
  computeOwnershipResearchProgress,
  concludeFromEntityEvidence,
} from "../progress-metrics.js";
import { planScrapeFailureRecovery } from "../scrape-failure-recovery.js";

export const BRAZIL_V1_ID = "BRAZIL_V1";

export const BRAZIL_TERMS = Object.freeze([
  "CNPJ",
  "Razão Social",
  "Nome Fantasia",
  "Sócio",
  "Sócio-Administrador",
  "Administrador",
  "Proprietário",
  "Propriedade",
  "Administradora Hoteleira",
  "Hotelaria",
  "Empreendimento",
  "Incorporação",
  "Incorporadora",
  "Investidor",
  "Fundo de Investimento Imobiliário",
  "FII",
  "Aquisição",
  "Adquiriu",
  "Venda",
  "Comprou",
  "CVM",
  "Receita Federal",
  "NFS-e",
]);

/**
 * Normalize BR street address tokens for search.
 * @param {string} address
 * @param {{ city?: string, state?: string, postal_code?: string }} [extra]
 */
export function normalizeBrazilAddress(address, extra = {}) {
  const raw = String(address || "").trim();
  let street = raw
    .replace(/\b(Rua|R\.|Avenida|Av\.|Alameda|Al\.)\b/gi, (m) => m.replace(/\./g, ""))
    .replace(/\s+/g, " ")
    .trim();
  // Split trailing number if present
  const numMatch = street.match(/^(.*?)[,\s]+(\d+[A-Za-z\-]*)\s*$/);
  let number = null;
  if (numMatch) {
    street = numMatch[1].trim();
    number = numMatch[2];
  }
  return {
    street,
    number,
    postal_code: extra.postal_code || null,
    city: extra.city || null,
    state: extra.state || extra.state_region || null,
    search_phrase: [street, number, extra.city, extra.state || extra.state_region]
      .filter(Boolean)
      .join(" "),
  };
}

/**
 * Generate Brazil legal-entity resolution ladder queries.
 * Address-based CNPJ queries are mandatory when an address exists.
 * @param {object} hotel
 */
export function buildBrazilLegalEntityQueries(hotel = {}) {
  const name = String(hotel.hotel_name || hotel.official_name || "").replace(/["\n\r]/g, " ").trim();
  const city = hotel.city || "";
  const addrNorm = normalizeBrazilAddress(hotel.address || hotel.street_address || "", {
    city: hotel.city,
    state: hotel.state_region || hotel.state,
    postal_code: hotel.postal_code,
  });
  const qs = [];

  if (name) {
    qs.push(`"${name}" CNPJ`);
    if (city) qs.push(`"${name}" "${city}" CNPJ`);
    qs.push(`"${name}" "razão social"`);
    qs.push(`"${name}" "administradora hoteleira"`);
    qs.push(`"${name}" "política de privacidade"`);
    qs.push(`"${name}" "termos de uso"`);
    qs.push(`"${name}" "nota fiscal"`);
    qs.push(`"${name}" proprietário`);
    qs.push(`"${name}" aquisição`);
    qs.push(`"${name}" investidor`);
    qs.push(`"${name}" (FII OR CVM OR "fundo imobiliário")`);
  }
  if (addrNorm.search_phrase) {
    qs.push(`"${addrNorm.search_phrase}" CNPJ`);
    if (addrNorm.street && addrNorm.number) {
      qs.push(`"${addrNorm.street}" ${addrNorm.number} CNPJ`);
      if (city) qs.push(`"${addrNorm.street}, ${addrNorm.number}" "${city}" CNPJ`);
    }
  }

  return [...new Set(qs.filter(Boolean))];
}

/**
 * Entry goals for the iterative loop — legal-entity ladder first.
 * @param {object} hotel
 * @param {number} [breadth]
 */
export function generateBrazilEntryGoals(hotel = {}, breadth = 6) {
  const qs = buildBrazilLegalEntityQueries(hotel).slice(0, Math.max(1, breadth));
  return qs.map((query, i) => ({
    query,
    researchGoal:
      i < 3
        ? "Resolve legal entity (CNPJ / razão social) for this exact property"
        : "Locate operating entity, acquisition, or investor evidence with registry grounding",
    evidence_gap: i < 3 ? "missing_legal_entity" : "missing_ownership_or_operator_entity",
    strategy_id: BRAZIL_V1_ID,
    ladder_rung: i < 3 ? "LEGAL_ENTITY_DISCOVERY" : "OWNERSHIP_OR_OPERATOR_DISCOVERY",
    address_based: /CNPJ/.test(query) && Boolean(hotel.address || hotel.street_address),
  }));
}

/**
 * Whether address-based legal-entity work remains before soft-stopping.
 * @param {object} hotel
 * @param {string[]} queriesAttempted
 */
export function hasUnattemptedAddressLegalEntityLead(hotel = {}, queriesAttempted = []) {
  const addr = hotel.address || hotel.street_address;
  if (!addr) return false;
  const required = buildBrazilLegalEntityQueries(hotel).filter((q) =>
    /CNPJ/i.test(q) && String(q).toLowerCase().includes(String(addr).toLowerCase().slice(0, 12).toLowerCase())
  );
  // Broader: any address-normalized CNPJ query
  const addrQs = buildBrazilLegalEntityQueries(hotel).filter((q) => {
    const n = normalizeBrazilAddress(addr, { city: hotel.city });
    return n.street && q.toLowerCase().includes(n.street.toLowerCase().slice(0, 10));
  });
  const pool = addrQs.length ? addrQs : required;
  if (!pool.length) {
    // Fallback: any "street number CNPJ" style still unattempted
    const allAddr = buildBrazilLegalEntityQueries(hotel).filter((q) => /"\s*.+\s+\d+/.test(q) || /CNPJ/.test(q));
    const attempted = new Set(queriesAttempted.map((q) => String(q).toLowerCase().trim()));
    return allAddr.some((q) => !attempted.has(q.toLowerCase().trim()) && /CNPJ/i.test(q) && hotel.address);
  }
  const attempted = new Set(queriesAttempted.map((q) => String(q).toLowerCase().trim()));
  return pool.some((q) => !attempted.has(q.toLowerCase().trim()));
}

/**
 * Full address/legal-entity continuation goals (not truncated entry breadth).
 * Production-safe helper used by iterative-loop continuation reassessment.
 *
 * @param {object} hotel
 * @param {{ queries_attempted?: string[], completed_work_keys?: Iterable<string>, queryKeyFn?: Function, normalizeCompletedKeyFn?: Function }} [ctx]
 */
export function buildAddressLegalEntityContinuationGoals(hotel = {}, ctx = {}) {
  const addr = hotel.address || hotel.street_address;
  if (!addr) return [];
  const queryKeyFn =
    typeof ctx.queryKeyFn === "function"
      ? ctx.queryKeyFn
      : (q) => String(q || "").toLowerCase().trim();
  const normalizeCompletedKeyFn =
    typeof ctx.normalizeCompletedKeyFn === "function"
      ? ctx.normalizeCompletedKeyFn
      : (op, k) => `${op}:${k}`;
  const attempted = new Set((ctx.queries_attempted || []).map((q) => queryKeyFn(q)));
  const completed = new Set(
    [...(ctx.completed_work_keys || [])].map((k) => String(k).toLowerCase())
  );
  const addrSlice = String(addr).toLowerCase().slice(0, 12);
  const goals = [];
  for (const query of buildBrazilLegalEntityQueries(hotel)) {
    const isAddrCnpj =
      /CNPJ/i.test(query) && String(query).toLowerCase().includes(addrSlice);
    if (!isAddrCnpj) continue;
    const k = queryKeyFn(query);
    if (!k || attempted.has(k)) continue;
    const ck = String(normalizeCompletedKeyFn("search", k)).toLowerCase();
    if (completed.has(ck)) continue;
    goals.push({
      query,
      researchGoal: "Resolve legal entity (CNPJ / razão social) for this exact property",
      evidence_gap: "missing_legal_entity",
      ladder_rung: "LEGAL_ENTITY_DISCOVERY",
      address_based: true,
      from: "address_legal_entity_continuation",
      strategy_id: BRAZIL_V1_ID,
    });
  }
  return goals;
}

export function createBrazilOwnershipStrategyV1() {
  return {
    id: BRAZIL_V1_ID,
    version: `${STRATEGY_VERSION}+${BRAZIL_V1_ID}`,
    country_codes: ["BR", "BRA", "BRAZIL", "BRASIL"],
    terms: BRAZIL_TERMS,
    normalizeAddress: normalizeBrazilAddress,
    buildLegalEntityQueries: buildBrazilLegalEntityQueries,
    generateEntryGoals: generateBrazilEntryGoals,
    hasUnattemptedAddressLegalEntityLead,
    classifySourceTiers,
    shouldScrapeForOwnership,
    sourceTierRankDelta,
    extractLegalEntityBundle,
    isNonAuthoritativeCnpj,
    normalizeCnpj,
    assessPropertyLineage,
    computeOwnershipResearchProgress,
    concludeFromEntityEvidence,
    planScrapeFailureRecovery,
    /** Ladder sequence for docs / reports */
    ladder: [
      "PROPERTY_IDENTITY",
      "CURRENT_HISTORICAL_PROPERTY_LINEAGE",
      "LEGAL_ENTITY_DISCOVERY",
      "ENTITY_ROLE_CLASSIFICATION",
      "PRINCIPAL_QSA_DISCOVERY",
      "PROPERTY_OWNERSHIP_DISCOVERY",
      "OWNER_SPONSOR_CONCLUSION",
      "DECISION_MAKER_DISCOVERY",
      "CONTACT_ENRICHMENT_OUT_OF_SCOPE",
    ],
    default_conclusion: OWNERSHIP_CONCLUSION_STATE.UNRESOLVED,
  };
}
