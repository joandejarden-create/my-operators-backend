/**
 * Brazil Cadastur lodging identity match — extracted from brazil-registry-feasibility scripts.
 * REGISTERED_BUSINESS identity only — never auto-promotes to property ownership.
 * Import-safe (no network, no XLSX load on import).
 */
import { normalizeBrazilText } from "./brazil-cadastur-open-data-adapter.js";

export const BRAZIL_CADASTUR_MATCH_VERSION = "brazil-cadastur-match-v1";

export const CADASTUR_RELATIONSHIP = Object.freeze({
  REGISTERED_BUSINESS: "REGISTERED_BUSINESS",
});

function norm(s) {
  return normalizeBrazilText(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

/**
 * Score a hotel against a Cadastur mapped/normalized row.
 * @param {object} hotel — hotel_name / property_name / city
 * @param {object} mapped — mapBrazilCadasturRowToNormalized output or { n: {...} }
 * @returns {number} score; -1 = city mismatch / reject
 */
export function scoreCadasturHotelMatch(hotel, mapped) {
  const n = mapped?.n || mapped || {};
  const nameN = norm(hotel.hotel_name || hotel.property_name || hotel.name || "");
  const cityN = norm(hotel.city || "");
  if (!nameN || !cityN) return -1;
  const blob = norm([n.property_name, n.commercial_name, n.legal_name, n.nome_fantasia, n.razao_social, n.address].join(" "));
  let score = 0;
  const cityCad = norm(n.city || n.municipio || "");
  if (cityCad === cityN || cityCad.includes(cityN) || cityN.includes(cityCad)) score += 5;
  else return -1;
  const tokens = nameN
    .split(/\s+/)
    .filter(
      (t) =>
        t.length > 3 &&
        !["hotel", "by", "the", "collection", "hilton", "tapestry", "canopy", "resort"].includes(t)
    );
  for (const t of tokens) if (blob.includes(t)) score += 2;
  if (nameN.length >= 8 && blob.includes(nameN.slice(0, 20))) score += 4;
  return score;
}

/**
 * Pick best Cadastur lodging match for a hotel from preloaded rows.
 * Does not call network. Does not treat match as ownership.
 */
export function matchHotelToCadastur(hotel, rows = [], { minScore = 9 } = {}) {
  const scored = [];
  for (const row of rows || []) {
    const mapped = row?.n ? row : { n: row };
    const score = scoreCadasturHotelMatch(hotel, mapped);
    if (score < 0) continue;
    scored.push({ row: mapped.n || row, score });
  }
  scored.sort((a, b) => b.score - a.score);
  const best = scored[0] || null;
  const runner = scored[1] || null;
  const accepted =
    Boolean(best) &&
    best.score >= minScore &&
    (!runner || best.score - runner.score >= 2 || best.score >= minScore + 4);

  return {
    version: BRAZIL_CADASTUR_MATCH_VERSION,
    accepted,
    relationship: CADASTUR_RELATIONSHIP.REGISTERED_BUSINESS,
    auto_ownership: false,
    best: best
      ? {
          score: best.score,
          legal_name: best.row.legal_name || best.row.razao_social || null,
          commercial_name: best.row.commercial_name || best.row.nome_fantasia || best.row.property_name || null,
          cnpj: best.row.cnpj || null,
          city: best.row.city || best.row.municipio || null,
          website: best.row.website || best.row.official_property_url || null,
          responsavel: best.row.responsavel || best.row.nome_responsavel || null,
          raw: best.row,
        }
      : null,
    candidates: scored.slice(0, 5).map((s) => ({
      score: s.score,
      legal_name: s.row.legal_name || s.row.razao_social || null,
      cnpj: s.row.cnpj || null,
    })),
    note: "Cadastur match is REGISTERED_BUSINESS identity — administrators/shareholders are not automatic property owners.",
  };
}
