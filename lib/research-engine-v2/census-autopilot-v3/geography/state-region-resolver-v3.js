/**
 * State / Region resolver V3 — deterministic Dealality geography.
 * Order: official structured → address parse → city alias → name cue → bbox from coords.
 * Never Cvent / legacy / STR. Does not use SerpApi labels as State values.
 */

import { resolveStateRegion as resolveStateRegionV1 } from "../state-region-pipeline.js";
import { getAdminLevel } from "./country-admin-levels.js";
import { lookupCityAdmin, lookupNameAdmin, normGeoLabel } from "./admin-city-aliases.js";
import { lookupAdminByBbox, ADMIN_BBOX_SOURCE } from "./admin-bbox.js";

const BR_UF = Object.freeze({
  AC: "Acre",
  AL: "Alagoas",
  AP: "Amapá",
  AM: "Amazonas",
  BA: "Bahia",
  CE: "Ceará",
  DF: "Distrito Federal",
  ES: "Espírito Santo",
  GO: "Goiás",
  MA: "Maranhão",
  MT: "Mato Grosso",
  MS: "Mato Grosso do Sul",
  MG: "Minas Gerais",
  PA: "Pará",
  PB: "Paraíba",
  PR: "Paraná",
  PE: "Pernambuco",
  PI: "Piauí",
  RJ: "Rio de Janeiro",
  RN: "Rio Grande do Norte",
  RS: "Rio Grande do Sul",
  RO: "Rondônia",
  RR: "Roraima",
  SC: "Santa Catarina",
  SP: "São Paulo",
  SE: "Sergipe",
  TO: "Tocantins",
});

function parseAdminFromAddress(country, address) {
  const addr = String(address || "");
  if (!addr) return null;

  if (country === "Brazil") {
    const m = addr.match(
      /\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b/
    );
    if (m && BR_UF[m[1]]) {
      return { value: BR_UF[m[1]], method: "brazil_uf_from_address" };
    }
    // "Belém, Pará" style in city field sometimes duplicated in address
    const named = addr.match(
      /\b(Pará|Para|Amazonas|São Paulo|Sao Paulo|Rio de Janeiro|Paraná|Parana|Bahia|Minas Gerais)\b/i
    );
    if (named) {
      const raw = named[1];
      const map = {
        para: "Pará",
        pará: "Pará",
        amazonas: "Amazonas",
        "sao paulo": "São Paulo",
        "são paulo": "São Paulo",
        "rio de janeiro": "Rio de Janeiro",
        parana: "Paraná",
        paraná: "Paraná",
        bahia: "Bahia",
        "minas gerais": "Minas Gerais",
      };
      const v = map[normGeoLabel(raw)];
      if (v) return { value: v, method: "brazil_name_from_address" };
    }
  }

  if (country === "Costa Rica") {
    const m = addr.match(
      /\b(Guanacaste|Puntarenas|San Jos[eé]|Alajuela|Heredia|Cartago|Lim[oó]n)\s+Province\b/i
    );
    if (m) {
      let v = m[1];
      if (/san jos/i.test(v)) v = "San José";
      if (/lim/i.test(v)) v = "Limón";
      return { value: v, method: "costa_rica_province_from_address" };
    }
  }

  if (country === "Argentina") {
    const m = addr.match(/\b(W\d{4}|C\d{4}|[A-Z]\d{4})\s+([A-Za-zÁÉÍÓÚáéíóúñÑ\s]+)/);
    if (m && /corrientes/i.test(m[2])) {
      return { value: "Corrientes", method: "argentina_postal_locality" };
    }
  }

  if (country === "Jamaica") {
    if (/st\.?\s*james|saint\s*james|rose\s*hall/i.test(addr)) {
      return { value: "St. James", method: "jamaica_parish_from_address" };
    }
    if (/kingston/i.test(addr)) return { value: "Kingston", method: "jamaica_parish_from_address" };
    if (/westmoreland|bluefields/i.test(addr)) {
      return { value: "Westmoreland", method: "jamaica_parish_from_address" };
    }
    if (/port\s*antonio|portland/i.test(addr)) {
      return { value: "Portland", method: "jamaica_parish_from_address" };
    }
  }

  if (country === "Barbados") {
    if (/christ\s*church/i.test(addr)) return { value: "Christ Church", method: "barbados_parish_from_address" };
    if (/st\.?\s*philip|saint\s*philip/i.test(addr)) {
      return { value: "Saint Philip", method: "barbados_parish_from_address" };
    }
    if (/bridgetown|st\.?\s*michael|saint\s*michael/i.test(addr)) {
      return { value: "Saint Michael", method: "barbados_parish_from_address" };
    }
  }

  if (country === "Dominican Republic") {
    if (/sos[uú]a|puerto\s*plata/i.test(addr)) {
      return { value: "Puerto Plata", method: "dr_province_from_address" };
    }
  }

  return null;
}

/**
 * @param {{
 *   country?: string,
 *   city?: string,
 *   address?: string,
 *   name?: string,
 *   official_state?: string,
 *   latitude?: number|null,
 *   longitude?: number|null,
 *   coords_production_eligible?: boolean,
 *   address_production_eligible?: boolean,
 * }} input
 */
export function resolveStateRegionV3(input = {}) {
  const country = String(input.country || "").trim();
  const adminMeta = getAdminLevel(country);
  const evidence = [];
  const parentClaims = [];

  const finish = (hit, confidence) => {
    const coordsEligible = input.coords_production_eligible !== false;
    const addrEligible = input.address_production_eligible !== false;
    let production_eligible = true;
    // If sole method is bbox and coords are SerpApi-blocked, do not launder.
    if (hit.method === "dealality_admin_bbox" && input.coords_production_eligible === false) {
      production_eligible = false;
    }
    if (
      (hit.method.includes("address") || hit.method.includes("uf_from_address")) &&
      input.address_production_eligible === false
    ) {
      production_eligible = false;
    }
    return {
      ok: true,
      normalized_state_region: hit.value,
      administrative_type: adminMeta.administrative_type,
      derivation: hit.method,
      method: hit.method,
      source: hit.boundary_source || "dealality_geography",
      confidence: confidence || hit.confidence || "High",
      boundary_source: hit.boundary_source || null,
      evidence,
      parent_claim_ids: parentClaims,
      production_eligible,
      coords_eligible_used: coordsEligible,
      address_eligible_used: addrEligible,
    };
  };

  if (input.official_state) {
    evidence.push({ kind: "official_state", value: input.official_state });
    return finish({ value: String(input.official_state).trim(), method: "official_structured" }, "High");
  }

  // Prefer existing V1 for Mexico / DR city maps (already battle-tested)
  const v1 = resolveStateRegionV1({
    country,
    city: input.city,
    address: input.address,
    official_state: null,
  });
  if (v1.ok && v1.normalized_state_region) {
    evidence.push({ kind: "v1_pipeline", derivation: v1.derivation });
    return finish(
      { value: v1.normalized_state_region, method: v1.derivation || "state_region_v1" },
      v1.confidence || "High"
    );
  }

  const fromAddr = parseAdminFromAddress(country, input.address);
  if (fromAddr) {
    evidence.push({ kind: "address_parse", value: fromAddr.value });
    parentClaims.push("address");
    return finish(fromAddr, "High");
  }

  const fromCity = lookupCityAdmin(country, input.city);
  if (fromCity) {
    evidence.push({ kind: "city_alias", value: fromCity.value, city: input.city });
    parentClaims.push("city");
    return finish(fromCity, "High");
  }

  const fromName = lookupNameAdmin(country, input.name);
  if (fromName) {
    evidence.push({ kind: "name_cue", value: fromName.value });
    parentClaims.push("name");
    return finish(fromName, "Medium");
  }

  if (input.latitude != null && input.longitude != null) {
    const bbox = lookupAdminByBbox(country, input.latitude, input.longitude);
    if (bbox) {
      evidence.push({ kind: "bbox", ...bbox, source: ADMIN_BBOX_SOURCE });
      parentClaims.push("coordinates");
      return finish(bbox, bbox.confidence);
    }
  }

  return {
    ok: false,
    normalized_state_region: null,
    administrative_type: adminMeta.administrative_type,
    derivation: "unresolved",
    method: "unresolved",
    source: null,
    confidence: "No Match",
    boundary_source: null,
    evidence,
    parent_claim_ids: parentClaims,
    production_eligible: false,
  };
}
