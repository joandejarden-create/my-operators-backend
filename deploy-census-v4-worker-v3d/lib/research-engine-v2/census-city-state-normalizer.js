/**
 * Hotel Property Census — City / State-Region normalizer (High confidence only).
 *
 * Never invent city from hotel name, country, or coordinates alone.
 * Never Brand Setup / Brand Explorer / VIC writes.
 */

export const CITY_STATE_NORMALIZER_VERSION = "census-city-state-normalizer-v1";

/** Descriptor / non-place city values — reject or steward. */
export const CITY_DESCRIPTOR_PATTERNS = Object.freeze([
  /^unknown$/i,
  /^n\/?a$/i,
  /^null$/i,
  /^none$/i,
  /^tbd$/i,
  /^adults?\s*only$/i,
  /^-?\s*adults?\s*only$/i,
  /^an\s*[-–—]?\s*adults?\s*only$/i,
  /^all\s*inclusive$/i,
  /^beach\s*resort$/i,
  /^resort$/i,
  /^hotel$/i,
  /^hotels$/i,
  /^collection$/i,
  /^by\s+.+$/i,
  /^soft\s*brand$/i,
  /^independent$/i,
  /^boutique$/i,
  /adults?\s*only/i,
  /all[-\s]?inclusive/i,
  /autograph\s*collection/i,
]);

/**
 * Canonical CALA city spellings (ASCII key → display form).
 * Used only for case / accent normalization of an already-present city token.
 */
export const CALA_CITY_CANONICAL = Object.freeze({
  cancun: "Cancún",
  "cancún": "Cancún",
  "playa del carmen": "Playa del Carmen",
  "san jose del cabo": "San José del Cabo",
  "san josé del cabo": "San José del Cabo",
  "los cabos": "Los Cabos",
  queretaro: "Querétaro",
  "querétaro": "Querétaro",
  merida: "Mérida",
  "mérida": "Mérida",
  "mexico city": "Mexico City",
  "ciudad de mexico": "Mexico City",
  "ciudad de méxico": "Mexico City",
  cdmx: "Mexico City",
  guadalajara: "Guadalajara",
  monterrey: "Monterrey",
  "puerto vallarta": "Puerto Vallarta",
  "cabo san lucas": "Cabo San Lucas",
  "san miguel de allende": "San Miguel de Allende",
  oaxaca: "Oaxaca",
  puebla: "Puebla",
  toluca: "Toluca",
  leon: "León",
  "león": "León",
  "guadalupe": "Guadalupe",
  "cd guadalupe": "Guadalupe",
  "cd. guadalupe": "Guadalupe",
  bogota: "Bogotá",
  "bogotá": "Bogotá",
  medellin: "Medellín",
  "medellín": "Medellín",
  cartagena: "Cartagena",
  barranquilla: "Barranquilla",
  cali: "Cali",
  cucuta: "Cúcuta",
  "cúcuta": "Cúcuta",
  "punta cana": "Punta Cana",
  "santo domingo": "Santo Domingo",
  "puerto plata": "Puerto Plata",
  "la romana": "La Romana",
  "panama city": "Panama City",
  "ciudad de panama": "Panama City",
  "ciudad de panamá": "Panama City",
  "san jose": "San José",
  "san josé": "San José",
  "liberia": "Liberia",
  "tamarindo": "Tamarindo",
  "lima": "Lima",
  "cusco": "Cusco",
  "quito": "Quito",
  "guayaquil": "Guayaquil",
  "buenos aires": "Buenos Aires",
  "santiago": "Santiago",
  "rio de janeiro": "Rio de Janeiro",
  "sao paulo": "São Paulo",
  "são paulo": "São Paulo",
  "san juan": "San Juan",
  kingston: "Kingston",
  "montego bay": "Montego Bay",
  // Mexico secondary markets (case / accent only — never invent from blank)
  tijuana: "Tijuana",
  irapuato: "Irapuato",
  tlalnepantla: "Tlalnepantla",
  aguascalientes: "Aguascalientes",
  mazatlan: "Mazatlán",
  "mazatlán": "Mazatlán",
  "nuevo vallarta": "Nuevo Vallarta",
  naucalpan: "Naucalpan",
  torreon: "Torreón",
  "torreón": "Torreón",
  zapopan: "Zapopan",
  acapulco: "Acapulco",
  chihuahua: "Chihuahua",
  saltillo: "Saltillo",
  "tuxtla gutierrez": "Tuxtla Gutiérrez",
  "tuxtla gutiérrez": "Tuxtla Gutiérrez",
  veracruz: "Veracruz",
  villahermosa: "Villahermosa",
  zacatecas: "Zacatecas",
  // Marriott Unknown-city URL slug lexicon (property URL structure — not hotel-name inference)
  nogales: "Nogales",
  "ciudad obregon": "Ciudad Obregón",
  "ciudad obregón": "Ciudad Obregón",
  tuxpan: "Tuxpan",
  tapachula: "Tapachula",
  "punta mita": "Punta Mita",
  "punta de mita": "Punta Mita",
  rosarito: "Rosarito",
  tuxtepec: "Tuxtepec",
  "santa marta": "Santa Marta",
  salamanca: "Salamanca",
  minatitlan: "Minatitlán",
  "minatitlán": "Minatitlán",
  "san luis potosi": "San Luis Potosí",
  "san luis potosí": "San Luis Potosí",
  caborca: "Caborca",
  xalapa: "Xalapa",
  "ciudad del carmen": "Ciudad del Carmen",
  reynosa: "Reynosa",
  tepotzotlan: "Tepotzotlán",
  "tepotzotlán": "Tepotzotlán",
  tepic: "Tepic",
  campeche: "Campeche",
  "lazaro cardenas": "Lázaro Cárdenas",
  "lázaro cárdenas": "Lázaro Cárdenas",
  durango: "Durango",
  "salina cruz": "Salina Cruz",
  "lagos de moreno": "Lagos de Moreno",
  cananea: "Cananea",
  silao: "Silao",
  "nuevo laredo": "Nuevo Laredo",
  "ciudad victoria": "Ciudad Victoria",
  guaymas: "Guaymas",
  apizaco: "Apizaco",
  matamoros: "Matamoros",
  comitan: "Comitán",
  "comitán": "Comitán",
  celaya: "Celaya",
  manzanillo: "Manzanillo",
  tula: "Tula",
  holbox: "Holbox",
  paraiso: "Paraíso",
  "paraíso": "Paraíso",
  "piedras negras": "Piedras Negras",
  tehuacan: "Tehuacán",
  "tehuacán": "Tehuacán",
  "los mochis": "Los Mochis",
  chetumal: "Chetumal",
  "puerto escondido": "Puerto Escondido",
  belen: "Belén",
  "belén": "Belén",
  "hacienda belen": "Belén",
  // Colombia secondary
  chia: "Chía",
  "chía": "Chía",
  apartado: "Apartadó",
  "apartadó": "Apartadó",
  itagui: "Itagüí",
  "itagüi": "Itagüí",
  "itagüí": "Itagüí",
});

export const CALA_STATE_CANONICAL = Object.freeze({
  "nuevo leon": "Nuevo León",
  "nuevo león": "Nuevo León",
  "quintana roo": "Quintana Roo",
  "baja california sur": "Baja California Sur",
  "baja california": "Baja California",
  "ciudad de mexico": "Ciudad de México",
  "ciudad de méxico": "Ciudad de México",
  "mexico city": "Ciudad de México",
  jalisco: "Jalisco",
  "yucatan": "Yucatán",
  "yucatán": "Yucatán",
  "guanajuato": "Guanajuato",
  "cundinamarca": "Cundinamarca",
  antioquia: "Antioquia",
  "bolivar": "Bolívar",
  "bolívar": "Bolívar",
  "distrito nacional": "Distrito Nacional",
  "la altagracia": "La Altagracia",
  panama: "Panamá",
  "panamá": "Panamá",
  "provincia de panama": "Panamá",
  "provincia de panamá": "Panamá",
  "san jose": "San José",
  "san josé": "San José",
});

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

export function normalizePlaceKey(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\./g, "")
    .replace(/\s+/g, " ")
    .trim();
}

export function isDescriptorCity(city) {
  const c = String(city || "").trim();
  if (!c) return true;
  return CITY_DESCRIPTOR_PATTERNS.some((re) => re.test(c));
}

export function isAllCapsCity(city) {
  const c = String(city || "").trim();
  if (c.length < 3) return false;
  const letters = c.replace(/[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ]/g, "");
  if (letters.length < 3) return false;
  return letters === letters.toUpperCase() && /[A-ZÁÉÍÓÚÜÑ]/.test(letters);
}

export function isAllLowerCity(city) {
  const c = String(city || "").trim();
  if (c.length < 3) return false;
  const letters = c.replace(/[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ]/g, "");
  if (letters.length < 3) return false;
  return letters === letters.toLowerCase() && /[a-záéíóúüñ]/.test(letters);
}

/**
 * Proper-case a multi-word place with small-word exceptions.
 */
export function toProperCasePlace(raw) {
  const small = new Set(["de", "del", "la", "las", "los", "y", "da", "do", "dos", "das"]);
  return String(raw || "")
    .trim()
    .split(/\s+/)
    .map((w, i) => {
      const lower = w.toLowerCase();
      if (i > 0 && small.has(lower)) return lower;
      if (/^cd\.?$/i.test(w)) return "Cd.";
      return lower.charAt(0).toUpperCase() + lower.slice(1);
    })
    .join(" ");
}

/**
 * Lookup canonical CALA city spelling by normalized key.
 */
export function canonicalCalaCity(city) {
  const key = normalizePlaceKey(city);
  if (!key) return null;
  if (CALA_CITY_CANONICAL[key]) return CALA_CITY_CANONICAL[key];
  // Try without leading "cd "
  const noCd = key.replace(/^cd\s+/, "");
  if (CALA_CITY_CANONICAL[noCd]) return CALA_CITY_CANONICAL[noCd];
  return null;
}

export function canonicalCalaState(state) {
  const key = normalizePlaceKey(state);
  if (!key) return null;
  return CALA_STATE_CANONICAL[key] || null;
}

/**
 * High-confidence City, State split from "City, State" patterns only.
 * e.g. "Cd. Guadalupe, Nuevo Leon" → Guadalupe + Nuevo León
 */
export function trySplitCityState(rawCity) {
  const s = String(rawCity || "").trim();
  if (!s || !s.includes(",")) return { ok: false, reason: "no_comma" };

  const parts = s.split(",").map((p) => p.trim()).filter(Boolean);
  if (parts.length !== 2) {
    return { ok: false, reason: "ambiguous_multi_comma" };
  }

  let [left, right] = parts;
  // Strip Cd. / Ciudad prefix from city token
  left = left.replace(/^cd\.?\s+/i, "").replace(/^ciudad\s+(de\s+)?/i, "").trim();
  if (!left || left.length < 3) return { ok: false, reason: "city_token_too_short" };
  if (isDescriptorCity(left)) return { ok: false, reason: "left_is_descriptor" };

  const cityCanon = canonicalCalaCity(left) || toProperCasePlace(left);
  const stateCanon = canonicalCalaState(right) || toProperCasePlace(right);

  // Require state token to look like a region (not a country dump)
  if (/mexico|colombia|panama|dominican|costa rica|jamaica/i.test(right) && !canonicalCalaState(right)) {
    return { ok: false, reason: "right_looks_like_country" };
  }

  return {
    ok: true,
    confidence: "High",
    city: cityCanon,
    state_region: stateCanon,
    reason: "exact_city_comma_state_split",
    original: s,
  };
}

export const CITY_CLASS = Object.freeze({
  CLEAN: "city_clean",
  CASE_NORMALIZE: "city_case_normalize",
  ACCENT_NORMALIZE: "city_accent_or_canonical_normalize",
  SPLIT_CITY_STATE: "city_state_split",
  UNKNOWN: "city_unknown",
  DESCRIPTOR: "city_descriptor",
  MIXED_UNRESOLVED: "city_state_mixed_unresolved",
  BLANK: "city_blank",
  STEWARD: "city_steward",
});

/**
 * Classify and optionally propose High City / State fixes.
 * @param {{ City?: string, "State / Region"?: string, Country?: string }} fields
 */
export function classifyAndNormalizeCityState(fields = {}) {
  const rawCity = String(fields.City || "").trim();
  const rawState = String(fields["State / Region"] || "").trim();
  const country = String(fields.Country || "").trim();

  if (isBlank(rawCity)) {
    return {
      class: CITY_CLASS.BLANK,
      write_allowed: false,
      reason: "city_blank",
      patch: null,
      city_clean: false,
    };
  }

  if (/^unknown$/i.test(rawCity) || /^n\/?a$/i.test(rawCity)) {
    return {
      class: CITY_CLASS.UNKNOWN,
      write_allowed: false,
      reason: "city_unknown",
      patch: null,
      city_clean: false,
      existing_city: rawCity,
    };
  }

  if (isDescriptorCity(rawCity) && !rawCity.includes(",")) {
    return {
      class: CITY_CLASS.DESCRIPTOR,
      write_allowed: false,
      reason: "city_descriptor",
      patch: null,
      city_clean: false,
      existing_city: rawCity,
    };
  }

  // City, State mixed
  if (rawCity.includes(",")) {
    const split = trySplitCityState(rawCity);
    if (split.ok && split.confidence === "High") {
      /** @type {Record<string, string>} */
      const patch = { City: split.city };
      if (isBlank(rawState) && split.state_region) {
        patch["State / Region"] = split.state_region;
      } else if (
        !isBlank(rawState) &&
        split.state_region &&
        normalizePlaceKey(rawState) === normalizePlaceKey(split.state_region)
      ) {
        // equivalent — optionally normalize state accents
        const st = canonicalCalaState(rawState);
        if (st && st !== rawState) patch["State / Region"] = st;
      } else if (!isBlank(rawState) && split.state_region) {
        const stEq =
          normalizePlaceKey(rawState) === normalizePlaceKey(split.state_region);
        if (!stEq) {
          return {
            class: CITY_CLASS.STEWARD,
            write_allowed: false,
            reason: "city_state_split_conflicts_existing_state",
            patch: null,
            city_clean: false,
            existing_city: rawCity,
            existing_state: rawState,
            proposed: split,
          };
        }
      }
      return {
        class: CITY_CLASS.SPLIT_CITY_STATE,
        write_allowed: true,
        confidence: "High",
        reason: split.reason,
        patch,
        city_clean: true,
        before: { City: rawCity, "State / Region": rawState || null },
        after: patch,
      };
    }
    return {
      class: CITY_CLASS.MIXED_UNRESOLVED,
      write_allowed: false,
      reason: split.reason || "city_state_mixed_unresolved",
      patch: null,
      city_clean: false,
      existing_city: rawCity,
    };
  }

  // Canonical CALA spelling / accent
  const canon = canonicalCalaCity(rawCity);
  if (canon && canon !== rawCity) {
    return {
      class: CITY_CLASS.ACCENT_NORMALIZE,
      write_allowed: true,
      confidence: "High",
      reason: "cala_canonical_city_spelling",
      patch: { City: canon },
      city_clean: true,
      before: { City: rawCity },
      after: { City: canon },
    };
  }

  // Case-only normalize when already a known CALA city (never invent unknown places)
  if (isAllCapsCity(rawCity) || isAllLowerCity(rawCity)) {
    if (canon) {
      return {
        class: CITY_CLASS.CASE_NORMALIZE,
        write_allowed: true,
        confidence: "High",
        reason: "case_to_cala_canonical",
        patch: { City: canon },
        city_clean: true,
        before: { City: rawCity },
        after: { City: canon },
      };
    }
    // All-caps/lowercase unknown place → steward (do not invent)
    return {
      class: CITY_CLASS.STEWARD,
      write_allowed: false,
      reason: "case_dirty_unknown_place_needs_source",
      patch: null,
      city_clean: false,
      existing_city: rawCity,
    };
  }

  // State accent normalize only
  /** @type {Record<string, string>} */
  const patch = {};
  if (!isBlank(rawState)) {
    const st = canonicalCalaState(rawState);
    if (st && st !== rawState) patch["State / Region"] = st;
  }

  if (Object.keys(patch).length) {
    return {
      class: CITY_CLASS.ACCENT_NORMALIZE,
      write_allowed: true,
      confidence: "High",
      reason: "state_canonical_spelling",
      patch,
      city_clean: true,
      before: { "State / Region": rawState },
      after: patch,
    };
  }

  // Already clean enough
  if (!isDescriptorCity(rawCity) && !/^unknown$/i.test(rawCity)) {
    return {
      class: CITY_CLASS.CLEAN,
      write_allowed: false,
      reason: "city_already_clean",
      patch: null,
      city_clean: true,
      existing_city: rawCity,
    };
  }

  return {
    class: CITY_CLASS.STEWARD,
    write_allowed: false,
    reason: "city_needs_steward",
    patch: null,
    city_clean: false,
    existing_city: rawCity,
  };
}

/**
 * Whether city is clean enough for coordinates / public readiness.
 */
export function isCityCleanForCoordinates(fields = {}) {
  const c = classifyAndNormalizeCityState(fields);
  return Boolean(c.city_clean) && c.class === CITY_CLASS.CLEAN;
}
