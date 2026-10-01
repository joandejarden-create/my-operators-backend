/**
 * Physical identity guard — match discovered legal entities to the target property.
 * Wrong-city same-name entities must not stage as the hotel's legal entity.
 */
import { PROPERTY_ENTITY_MATCH } from "./constants.js";

function norm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokens(s, minLen = 3) {
  return norm(s)
    .split(" ")
    .filter((t) => t.length >= minLen);
}

function cityStateMatch(a, b) {
  const na = norm(a);
  const nb = norm(b);
  if (!na || !nb) return null;
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  return false;
}

/**
 * Extract location cues from entity evidence text / structured fields.
 * @param {{ city?: string, state?: string, state_region?: string, address?: string, text?: string, legal_name?: string }} entity
 */
export function extractEntityLocationHints(entity = {}) {
  const text = `${entity.address || ""} ${entity.text || ""} ${entity.legal_name || ""}`;
  const city =
    entity.city ||
    text.match(
      /(?:em|em\s+|cidade\s+de\s+|localizada?\s+em\s+|sede\s+em\s+)([A-ZÁÉÍÓÚ][A-Za-záéíóúâêôãõç\s]{2,40})(?:\s*[-–,/\n]|\s+[A-Z]{2}\b)/
    )?.[1]?.trim() ||
    text.match(/\b([A-ZÁÉÍÓÚ][a-záéíóú]+(?:\s+[A-ZÁÉÍÓÚ][a-záéíóú]+)?)\s*[-–]\s*([A-Z]{2})\b/)?.[1] ||
    null;
  const state =
    entity.state ||
    entity.state_region ||
    text.match(/\b([A-Z]{2})\b(?:\s*,|\s*$|\s*CEP)/)?.[1] ||
    text.match(/\b(Rio Grande do Sul|São Paulo|Mato Grosso do Sul|Minas Gerais)\b/i)?.[1] ||
    null;
  // Common "Americana, SP" / "Pelotas - RS" patterns
  const pair = text.match(
    /\b([A-ZÁÉÍÓÚ][A-Za-záéíóúâêôãõç ]{2,40})\s*[,/-]\s*(SP|RS|MS|RJ|MG|PR|SC|BA|PE|CE|GO|DF)\b/i
  );
  return {
    city: entity.city || pair?.[1]?.trim() || city,
    state: entity.state || entity.state_region || pair?.[2]?.toUpperCase() || state,
    address: entity.address || null,
    postal_code: entity.postal_code || text.match(/\b(\d{5}-?\d{3})\b/)?.[1] || null,
  };
}

/**
 * Classify whether a discovered legal entity belongs to the target hotel property.
 *
 * @param {object} hotel — target property (name, address, city, state_region, …)
 * @param {object} entity — candidate (legal_name, trade_name, text, city, state, address, …)
 */
export function classifyPropertyEntityMatch(hotel = {}, entity = {}) {
  const hotelName = norm(hotel.hotel_name || hotel.official_name || hotel.name || "");
  const entityName = norm(entity.legal_name || entity.trade_name || entity.name || "");
  const loc = extractEntityLocationHints(entity);
  const hotelCity = norm(hotel.city || "");
  const hotelState = norm(hotel.state_region || hotel.state || "");
  const entityCity = norm(loc.city || "");
  const entityState = norm(loc.state || "");
  const hotelAddr = norm(hotel.address || hotel.street_address || "");
  const entityAddr = norm(loc.address || entity.address || "");
  const reasons = [];

  const nameTokens = tokens(hotelName).filter((t) => !/^(hotel|pousada|ibis|tryp|the|by)$/.test(t));
  const entityTokens = tokens(entityName);
  const nameOverlap =
    nameTokens.length > 0
      ? nameTokens.filter((t) => entityTokens.some((e) => e.includes(t) || t.includes(e))).length /
        nameTokens.length
      : 0;

  if (nameOverlap >= 0.6) reasons.push("NAME_OVERLAP");
  else if (nameOverlap > 0) reasons.push("WEAK_NAME_OVERLAP");

  let cityOk = null;
  let stateOk = null;
  if (hotelCity && entityCity) {
    cityOk = cityStateMatch(hotelCity, entityCity);
    reasons.push(cityOk ? "CITY_MATCH" : "CITY_MISMATCH");
  }
  if (hotelState && entityState) {
    // Compare abbreviated vs full
    const hs = hotelState.slice(0, 2);
    const es = entityState.length === 2 ? entityState : entityState;
    stateOk =
      hotelState === entityState ||
      hs === es ||
      hotelState.includes(entityState) ||
      entityState.includes(hotelState);
    // Map common full names
    if (!stateOk) {
      const map = {
        sp: "sao paulo",
        rs: "rio grande do sul",
        ms: "mato grosso do sul",
      };
      const hFull = map[hotelState] || hotelState;
      const eFull = map[entityState] || entityState;
      stateOk = hFull === eFull || hFull.includes(eFull) || eFull.includes(hFull);
    }
    reasons.push(stateOk ? "STATE_MATCH" : "STATE_MISMATCH");
  }

  let addressOk = null;
  if (hotelAddr && entityAddr) {
    const ht = tokens(hotelAddr, 2);
    const et = tokens(entityAddr, 2);
    const hit = ht.filter((t) => et.includes(t)).length;
    addressOk = hit >= Math.min(3, ht.length);
    reasons.push(addressOk ? "ADDRESS_MATCH" : "ADDRESS_PARTIAL_OR_MISS");
  } else if (hotelAddr && entity.text) {
    const ht = tokens(hotelAddr, 3);
    const hit = ht.filter((t) => norm(entity.text).includes(t)).length;
    if (hit >= Math.min(3, ht.length)) {
      addressOk = true;
      reasons.push("ADDRESS_MATCH_IN_TEXT");
    }
  }

  // Conflicting location: name matches but city/state clearly differ
  if (nameOverlap >= 0.5 && cityOk === false) {
    return {
      match: PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION,
      reasons,
      name_overlap: nameOverlap,
      hotel_city: hotel.city || null,
      entity_city: loc.city || null,
      hotel_state: hotel.state_region || hotel.state || null,
      entity_state: loc.state || null,
      note: "Same/similar hotel trade name with different city — do not stage as target legal entity",
    };
  }
  if (nameOverlap >= 0.5 && stateOk === false && cityOk !== true) {
    return {
      match: PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION,
      reasons,
      name_overlap: nameOverlap,
      note: "Same/similar name with conflicting state",
    };
  }

  if (addressOk === true && (cityOk !== false)) {
    return {
      match:
        nameOverlap >= 0.4
          ? PROPERTY_ENTITY_MATCH.EXACT_PROPERTY_MATCH
          : PROPERTY_ENTITY_MATCH.STRONG_PROPERTY_MATCH,
      reasons,
      name_overlap: nameOverlap,
    };
  }

  if (nameOverlap >= 0.6 && (cityOk === true || (!entityCity && cityOk == null))) {
    return {
      match: PROPERTY_ENTITY_MATCH.STRONG_PROPERTY_MATCH,
      reasons,
      name_overlap: nameOverlap,
    };
  }

  if (nameOverlap >= 0.5 && cityOk == null && stateOk == null && addressOk == null) {
    return {
      match: PROPERTY_ENTITY_MATCH.NAME_ONLY_MATCH,
      reasons,
      name_overlap: nameOverlap,
      note: "Name match only — require address/city corroboration before staging as target entity",
    };
  }

  if (nameOverlap < 0.3 && cityOk === false) {
    return {
      match: PROPERTY_ENTITY_MATCH.WRONG_PROPERTY,
      reasons,
      name_overlap: nameOverlap,
    };
  }

  if (nameOverlap > 0 || addressOk || cityOk) {
    return {
      match: PROPERTY_ENTITY_MATCH.PARTIAL_MATCH,
      reasons,
      name_overlap: nameOverlap,
    };
  }

  return {
    match: PROPERTY_ENTITY_MATCH.UNKNOWN,
    reasons,
    name_overlap: nameOverlap,
  };
}

/**
 * Exact-address continuation queries after location conflict.
 * @param {object} hotel
 */
export function buildExactAddressContinuationQueries(hotel = {}) {
  const name = String(hotel.hotel_name || hotel.official_name || "").trim();
  const addr = String(hotel.address || hotel.street_address || "").trim();
  const city = hotel.city || "";
  const state = hotel.state_region || hotel.state || "";
  const postal = hotel.postal_code || "";
  const qs = [];
  if (addr) {
    qs.push(`"${addr}" CNPJ`);
    qs.push(`"${addr}" "razão social"`);
  }
  // street + number style
  const num = addr.match(/(\d+[A-Za-z\-]*)\s*$/);
  const street = addr.replace(/,?\s*\d+[A-Za-z\-]*\s*$/, "").trim();
  if (street && num) {
    qs.push(`"${street}" ${num[1]} CNPJ`);
  }
  if (name && city && state) {
    qs.push(`"${name}" "${city}" "${state}" CNPJ`);
  }
  if (name && postal) {
    qs.push(`"${name}" "${postal}"`);
  }
  return [...new Set(qs.filter(Boolean))];
}
