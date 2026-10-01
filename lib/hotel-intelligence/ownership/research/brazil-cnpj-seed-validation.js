/**
 * CNPJ / registry seed validation gate (P1.6 / A′-BR-02).
 * Reject bad seeds before corporate climbing.
 */

import { normalizeMatchText, scoreHotelRegistryMatch } from "./adapters/match-utils.js";
import { mapBrasilApiCnpj } from "./brazil-cnpj-client.js";

export const CNPJ_SEED_VALIDATION_VERSION = "ownership-cnpj-seed-validation-v1";

export const SEED_VALIDATION_STATUSES = Object.freeze([
  "VALIDATED",
  "PROBABLE",
  "AMBIGUOUS",
  "REJECTED",
]);

const HOTEL_CNAE_PREFIXES = ["5510", "5511", "5512", "5590", "6810", "6820", "6821"];

/**
 * @param {string} hotelCity
 * @param {string} hotelStateOrCountry
 * @param {string} registryCity
 * @param {string} registryUf
 */
export function scoreGeoMatch(hotelCity, hotelStateOrCountry, registryCity, registryUf) {
  const hc = normalizeMatchText(hotelCity);
  const rc = normalizeMatchText(registryCity);
  const hu = normalizeMatchText(hotelStateOrCountry);
  const ru = normalizeMatchText(registryUf);

  if (!hc && !rc) return { score: 0.5, note: "geo_unknown_both" };
  if (!hc || !rc) return { score: 0.45, note: "geo_partial" };

  if (hc === rc) return { score: 1, note: "city_exact" };

  // Substring / regional (e.g. Vitoria vs Vitoria ES metro)
  if (hc.includes(rc) || rc.includes(hc)) {
    return { score: 0.85, note: "city_substring" };
  }

  // State-only match when cities differ materially — penalize heavily (Portal lesson)
  if (hu && ru && (hu === ru || hu === normalizeMatchText("Brazil") && ru.length === 2)) {
    return { score: 0.25, note: "state_only_city_mismatch" };
  }

  return { score: 0.05, note: "city_mismatch" };
}

/**
 * @param {object} hotel
 * @param {object} seed — { legal_name, display_name?, city?, state?, cnpj?, cnae?, source?, seed_source? }
 * @param {object} [registryRecord] — mapped BrasilAPI record
 */
export function validateCnpjSeed(hotel, seed, registryRecord = null) {
  const reasons = [];
  const hotelName = hotel?.hotel_name || hotel?.name || "";
  const hotelCity = hotel?.city || "";
  const hotelCountry = hotel?.country || "Brazil";
  const seedSource = seed?.seed_source || seed?.source || null;
  const tourismRegistrySeed = seedSource === "tourism_registry_identity";

  const reg = registryRecord || (seed?.registry ? mapBrasilApiCnpj(seed.registry) : null);

  const seedName = seed?.legal_name || seed?.display_name || reg?.razao_social || "";
  const seedCity = seed?.city || reg?.municipio || "";
  const seedUf = seed?.uf || reg?.uf || "";

  let nameScore = Math.max(
    scoreHotelRegistryMatch(hotelName, seedName, hotelCity, seedCity),
    scoreHotelRegistryMatch(hotelName, reg?.nome_fantasia || "", hotelCity, seedCity)
  );
  reasons.push(`name_score:${nameScore.toFixed(2)}`);

  const geo = scoreGeoMatch(hotelCity, hotelCountry, seedCity, seedUf);
  reasons.push(`geo:${geo.note}:${geo.score.toFixed(2)}`);

  let activityScore = 0.5;
  const cnae = String(reg?.cnae_fiscal || seed?.cnae || "").replace(/\D/g, "").slice(0, 4);
  if (cnae) {
    if (HOTEL_CNAE_PREFIXES.some((p) => cnae.startsWith(p))) {
      activityScore = 0.9;
      reasons.push("cnae_hospitality_compatible");
    } else if (cnae.startsWith("41")) {
      activityScore = 0.75;
      reasons.push("cnae_real_estate_dev");
    } else {
      activityScore = 0.4;
      reasons.push(`cnae_other:${cnae}`);
    }
  }

  // Tourism registry seeds link hotel→registered lodging PJ; names often differ (Sleep Inn vs Four Towers)
  const nameWeight = tourismRegistrySeed ? 0.15 : 0.45;
  const geoWeight = tourismRegistrySeed ? 0.6 : 0.4;
  const activityWeight = tourismRegistrySeed ? 0.25 : 0.15;

  let composite = nameScore * nameWeight + geo.score * geoWeight + activityScore * activityWeight;

  if (tourismRegistrySeed && geo.score >= 0.85 && activityScore >= 0.75) {
    composite = Math.max(composite, 0.72);
    reasons.push("tourism_registry_geo_activity_boost");
  }

  // Hard reject on strong city mismatch even with name overlap (Portal Ouroeste case)
  if (geo.score <= 0.15 && nameScore < 0.95) {
    composite = Math.min(composite, 0.35);
    reasons.push("hard_penalty_city_mismatch");
  }

  /** @type {typeof SEED_VALIDATION_STATUSES[number]} */
  let status = "REJECTED";
  if (composite >= 0.82) status = "VALIDATED";
  else if (composite >= 0.62) status = "PROBABLE";
  else if (composite >= 0.42) status = "AMBIGUOUS";

  return {
    status,
    composite_score: Number(composite.toFixed(3)),
    name_score: nameScore,
    geo_score: geo.score,
    activity_score: activityScore,
    reasons,
    registry: reg,
    reject_from_ownership_research: status === "REJECTED",
  };
}
