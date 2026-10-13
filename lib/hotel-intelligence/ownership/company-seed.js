/**
 * Company-identity seed for Ownership Intelligence (P1.7).
 * A seed is a hotel→registered/legal/operating company hypothesis.
 * Only VALIDATED / strong PROBABLE seeds enter expensive ownership research.
 */

import { websiteHost } from "../../independent-census/match-current-census.js";
import {
  normalizeMatchText,
  scoreHotelRegistryMatch,
} from "./research/adapters/match-utils.js";
import { scoreGeoMatch } from "./research/brazil-cnpj-seed-validation.js";
import { SEED_VALIDATION_STATUSES } from "./research/brazil-cnpj-seed-validation.js";
import { isDiscoveryMethod } from "./discovery-methods.js";
import { FORBIDDEN_EVIDENCE_SOURCES } from "./ontology.js";

export const COMPANY_SEED_VERSION = "ownership-company-seed-v1";

export { SEED_VALIDATION_STATUSES };

const WEAK_DIRECTORY_HOSTS = Object.freeze([
  "econodata.com.br",
  "casadosdados.com.br",
  "cnpja.com",
  "consultacnpj.com",
  "empresascnpj.com",
  "cnpj.biz",
  "cnpj.info",
  "listamais.com.br",
  "telelistas.net",
]);

/**
 * @param {string} url
 */
export function isWeakBusinessDirectoryHost(url) {
  const host = websiteHost(url);
  if (!host) return false;
  return WEAK_DIRECTORY_HOSTS.some((d) => host === d || host.endsWith(`.${d}`));
}

/**
 * @param {string} hotelAddr
 * @param {string} seedAddr
 */
export function scoreAddressMatch(hotelAddr, seedAddr) {
  const a = normalizeMatchText(hotelAddr);
  const b = normalizeMatchText(seedAddr);
  if (!a || !b) {
    return { score: null, available: Boolean(a || b), matched: false };
  }
  if (a === b) return { score: 1, available: true, matched: true };
  const tokensA = new Set(a.split(" ").filter((t) => t.length > 2));
  const tokensB = new Set(b.split(" ").filter((t) => t.length > 2));
  if (!tokensA.size || !tokensB.size) {
    return { score: 0.2, available: true, matched: false };
  }
  let inter = 0;
  for (const t of tokensA) if (tokensB.has(t)) inter += 1;
  const score = inter / new Set([...tokensA, ...tokensB]).size;
  return { score: Number(score.toFixed(3)), available: true, matched: score >= 0.5 };
}

/**
 * @param {string} hotelWebsite
 * @param {string} seedWebsite
 */
export function scoreDomainMatch(hotelWebsite, seedWebsite) {
  const a = websiteHost(hotelWebsite);
  const b = websiteHost(seedWebsite);
  if (!a || !b) {
    return { score: null, available: Boolean(a || b), matched: false };
  }
  if (a === b) return { score: 1, available: true, matched: true };
  if (a.endsWith(`.${b}`) || b.endsWith(`.${a}`)) {
    return { score: 0.85, available: true, matched: true };
  }
  return { score: 0, available: true, matched: false };
}

/**
 * @param {object} partial
 */
export function createCompanySeed(partial = {}) {
  const source = String(partial.source || "").trim().toLowerCase();
  if (FORBIDDEN_EVIDENCE_SOURCES.includes(source)) {
    throw new Error(`forbidden_company_seed_source:${source}`);
  }
  const method = partial.discovery_method || "other";
  const validationStatus =
    partial.validation_status ||
    partial.validation?.status ||
    "AMBIGUOUS";
  const confidence =
    partial.confidence != null
      ? Number(partial.confidence)
      : Number(partial.validation?.composite_score || 0);

  const seed = {
    schema_version: COMPANY_SEED_VERSION,
    hotel_id: partial.hotel_id || null,
    entity_candidate: {
      legal_name: partial.entity_candidate?.legal_name || partial.legal_name || null,
      display_name:
        partial.entity_candidate?.display_name ||
        partial.display_name ||
        partial.entity_candidate?.legal_name ||
        null,
      jurisdiction: partial.entity_candidate?.jurisdiction || partial.jurisdiction || null,
    },
    identifier: partial.identifier || null,
    source: partial.source || "unknown",
    source_url: partial.source_url || null,
    source_semantics: partial.source_semantics || null,
    hotel_name_match: partial.hotel_name_match || { score: null, matched: false },
    city_state_match: partial.city_state_match || { score: null, matched: false },
    address_match: partial.address_match || {
      score: null,
      available: false,
      matched: false,
    },
    domain_match: partial.domain_match || {
      score: null,
      available: false,
      matched: false,
    },
    validation_status: validationStatus,
    confidence,
    discovery_method: isDiscoveryMethod(method) ? method : "other",
    directory_unvalidated: Boolean(partial.directory_unvalidated),
    reasons: Array.isArray(partial.reasons) ? partial.reasons : [],
  };
  seed.proceed_to_ownership_research = seedProceedsToOwnershipResearch(seed);
  return seed;
}

/**
 * Build a seed from hotel + Cadastur/registry row + optional CNPJ validation.
 * @param {object} hotel
 * @param {object} row
 * @param {object} [validation]
 * @param {object} [meta]
 */
export function companySeedFromRegistryRow(hotel, row, validation = null, meta = {}) {
  const legal = row.legal_name || row.razao_social || row.property_name || "";
  const commercial = row.commercial_name || row.nome_fantasia || "";
  const hotelName = hotel?.hotel_name || hotel?.name || "";
  const hotelCity = hotel?.city || "";
  const nameScore = Math.max(
    scoreHotelRegistryMatch(hotelName, legal, hotelCity, row.city || ""),
    scoreHotelRegistryMatch(hotelName, commercial, hotelCity, row.city || ""),
    scoreHotelRegistryMatch(hotelName, row.property_name || "", hotelCity, row.city || "")
  );
  const geo = scoreGeoMatch(
    hotelCity,
    hotel?.state_region || hotel?.country || "",
    row.city || row.municipio || "",
    row.state_uf || row.uf || ""
  );
  const address = scoreAddressMatch(
    hotel?.address || hotel?.location?.address_line_1 || "",
    row.address || row.endereco || ""
  );
  const domain = scoreDomainMatch(
    hotel?.website || hotel?.digital?.website || "",
    row.website || ""
  );
  const cnpj = String(row.cnpj || row.identifier?.value || "").replace(/\D/g, "");
  const rfc = String(row.rfc || "").replace(/[^A-Z0-9Ñ&]/gi, "").toUpperCase();
  let identifier = null;
  if (cnpj.length === 14) {
    identifier = { kind: "cnpj", value: cnpj, country: "Brazil" };
  } else if (rfc.length >= 12) {
    identifier = { kind: "rfc", value: rfc, country: "Mexico" };
  } else if (meta.identifier) {
    identifier = meta.identifier;
  }

  return createCompanySeed({
    hotel_id: hotel?.hotel_id || null,
    entity_candidate: {
      legal_name: legal || commercial || null,
      display_name: commercial || legal || null,
      jurisdiction: meta.jurisdiction || (identifier?.kind === "rfc" ? "MX" : "BR"),
    },
    identifier,
    source: meta.source || "tourism_registry_identity",
    source_url: row.source_url || meta.source_url || null,
    source_semantics:
      meta.source_semantics ||
      "Registered lodging / legal company identity — not automatic PropCo OWNED_BY",
    hotel_name_match: { score: nameScore, matched: nameScore >= 0.72 },
    city_state_match: {
      score: geo.score,
      matched: geo.score >= 0.85,
      note: geo.note,
    },
    address_match: address,
    domain_match: domain,
    validation,
    validation_status: validation?.status || "AMBIGUOUS",
    confidence: validation?.composite_score ?? nameScore,
    discovery_method: meta.discovery_method || "tourism_registry_identity",
    directory_unvalidated: Boolean(meta.directory_unvalidated),
    reasons: validation?.reasons || [],
  });
}

/**
 * Only VALIDATED, or sufficiently strong PROBABLE, enter corporate climb.
 * Directory-only hits must be VALIDATED after official registry confirmation.
 * @param {object} seed
 */
export function seedProceedsToOwnershipResearch(seed) {
  const status = String(seed?.validation_status || "").toUpperCase();
  if (status === "REJECTED" || status === "AMBIGUOUS") return false;
  if (seed?.directory_unvalidated && status !== "VALIDATED") return false;
  if (status === "VALIDATED") return true;
  if (status === "PROBABLE") {
    const geo = Number(seed?.city_state_match?.score);
    const conf = Number(seed?.confidence);
    return (Number.isFinite(geo) ? geo >= 0.85 : false) && conf >= 0.7;
  }
  return false;
}
