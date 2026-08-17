/**
 * Discovery Factory city inference + normalization.
 * Expands Cvent URL slug inference with accent/alias canonicalization.
 */

export const CITY_INFER_VERSION = "discovery-factory-city-infer-v2";

const NON_CITY_SLUGS = new Set([
  "hotel",
  "hotels",
  "resort",
  "resorts",
  "venues",
  "venue",
  "www",
  "cvent",
  "com",
  "search",
  "event",
  "events",
  "meeting",
  "meetings",
  "destination",
  "destinations",
]);

/** Canonical display form → accepted aliases (ASCII + common misspellings). */
const CITY_CANONICAL = Object.freeze([
  ["São Paulo", ["sao paulo", "s. paulo", "s paulo", "saopaulo"]],
  ["Rio de Janeiro", ["rio de janeiro", "rio", "rj", "rio janeiro"]],
  ["Brasília", ["brasilia", "brasilia df", "bsb"]],
  ["Belém", ["belem"]],
  ["Curitiba", ["curitiba"]],
  ["Florianópolis", ["florianopolis", "floripa"]],
  ["Maceió", ["maceio"]],
  ["João Pessoa", ["joao pessoa"]],
  ["Vitória", ["vitoria"]],
  ["Goiânia", ["goiania"]],
  ["Cuiabá", ["cuiaba"]],
  ["São Luís", ["sao luis", "sao luis"]],
  ["Natal", ["natal"]],
  ["Recife", ["recife"]],
  ["Salvador", ["salvador", "salvador bahia", "salvador ba"]],
  ["Fortaleza", ["fortaleza"]],
  ["Manaus", ["manaus"]],
  ["Porto Alegre", ["porto alegre"]],
  ["Belo Horizonte", ["belo horizonte", "bh"]],
  ["Campinas", ["campinas"]],
  ["Guarulhos", ["guarulhos"]],
  ["Cancún", ["cancun", "cancún"]],
  ["Ciudad de México", ["ciudad de mexico", "cdmx", "mexico city", "mexico d f", "mexico df"]],
  ["Ciudad de Panamá", ["ciudad de panama", "panama city", "panama city panama"]],
  ["San José", ["san jose", "san jose costa rica"]],
  ["Bogotá", ["bogota", "bogota dc"]],
  ["Medellín", ["medellin"]],
  ["Cusco", ["cusco", "cuzco"]],
  ["São José", ["sao jose"]],
  ["Asunción", ["asuncion"]],
  ["Córdoba", ["cordoba", "cordoba argentina"]],
  ["Querétaro", ["queretaro"]],
  ["Mérida", ["merida"]],
  ["Playa del Carmen", ["playa del carmen", "pdc"]],
  ["Punta Cana", ["punta cana"]],
  ["Santo Domingo", ["santo domingo"]],
  ["San Juan", ["san juan", "san juan pr"]],
  // Brazil secondary markets (common in Cvent holds)
  ["Gramado", ["gramado"]],
  ["Foz do Iguaçu", ["foz do iguacu", "foz do iguaçu", "foz iguacu"]],
  ["Porto Seguro", ["porto seguro"]],
  ["Ribeirão Preto", ["ribeirao preto", "ribeirão preto"]],
  ["Campos do Jordão", ["campos do jordao", "campos do jordão"]],
  ["Bauru", ["bauru"]],
  ["Campo Grande", ["campo grande"]],
  ["Palmas", ["palmas"]],
  ["Balneário Camboriú", ["balneario camboriu", "balneário camboriú"]],
  ["Petrópolis", ["petropolis", "petrópolis"]],
  ["Paraty", ["paraty", "parati"]],
  ["Londrina", ["londrina"]],
  ["Ponta Grossa", ["ponta grossa"]],
  ["Ubatuba", ["ubatuba"]],
  ["Armação dos Búzios", ["armacao dos buzios", "armação dos búzios", "buzios"]],
  ["São Sebastião", ["sao sebastiao", "são sebastião"]],
  ["São Lourenço", ["sao lourenco", "são lourenço"]],
  // Caribbean / Track B localities
  ["Providenciales", ["providenciales", "provo"]],
  ["Grace Bay", ["grace bay"]],
  ["South Caicos", ["south caicos"]],
  ["North Caicos", ["north caicos"]],
  ["Kralendijk", ["kralendijk"]],
  ["Rincon", ["rincon bonaire"]],
  ["Charlotte Amalie", ["charlotte amalie"]],
  ["Christiansted", ["christiansted"]],
  ["Cruz Bay", ["cruz bay"]],
  ["Frederiksted", ["frederiksted"]],
  ["St. Thomas", ["st thomas", "saint thomas", "st. thomas"]],
  ["St. Croix", ["st croix", "saint croix", "st. croix"]],
  ["St. John", ["st john", "saint john", "st. john"]],
  ["The Valley", ["the valley", "valley anguilla"]],
  ["Plymouth", ["plymouth montserrat"]],
  ["Brades", ["brades"]],
  ["St. John's", ["st john s", "st johns", "saint johns", "st. john's"]],
  ["Castries", ["castries"]],
  ["Gros Islet", ["gros islet", "gros-islet"]],
  ["Soufrière", ["soufriere", "soufrière"]],
  ["Fort-de-France", ["fort de france", "fort-de-france"]],
  ["Les Trois-Îlets", ["les trois ilets", "les trois-ilets", "trois ilets"]],
  ["Le Marin", ["le marin"]],
  ["Gosier", ["gosier", "le gosier"]],
  ["Sainte-Anne", ["sainte anne", "sainte-anne"]],
  ["Pointe-à-Pitre", ["pointe a pitre", "pointe-à-pitre", "pointe a pitre"]],
]);

/**
 * Cities that strongly imply a specific country/territory.
 * Used to catch Cvent URL slug cross-island pollution (e.g. Willemstad on Bonaire).
 * Keys are foldCityKey forms.
 */
export const CITY_IMPLIES_COUNTRY = Object.freeze({
  willemstad: ["curacao", "curaçao"],
  castries: ["saint lucia", "st lucia"],
  "gros islet": ["saint lucia", "st lucia"],
  soufriere: ["saint lucia", "st lucia"],
  "charlotte amalie": [
    "u s virgin islands",
    "us virgin islands",
    "united states virgin islands",
    "u.s. virgin islands",
  ],
  christiansted: [
    "u s virgin islands",
    "us virgin islands",
    "united states virgin islands",
    "u.s. virgin islands",
  ],
  "cruz bay": [
    "u s virgin islands",
    "us virgin islands",
    "united states virgin islands",
    "u.s. virgin islands",
  ],
  "st thomas": [
    "u s virgin islands",
    "us virgin islands",
    "united states virgin islands",
    "u.s. virgin islands",
  ],
  "st croix": [
    "u s virgin islands",
    "us virgin islands",
    "united states virgin islands",
    "u.s. virgin islands",
  ],
  "the valley": ["anguilla"],
  kralendijk: ["bonaire"],
  providenciales: ["turks and caicos", "turks and caicos islands"],
  "grace bay": ["turks and caicos", "turks and caicos islands"],
  "fort de france": ["martinique"],
  "les trois ilets": ["martinique"],
  gosier: ["guadeloupe"],
  "pointe a pitre": ["guadeloupe"],
  "st john s": ["antigua", "antigua and barbuda"],
  "st johns": ["antigua", "antigua and barbuda"],
});

/** Small-island territories: city==country (or bare island label) → primary commercial locality. */
export const ISLAND_PRIMARY_LOCALITY = Object.freeze({
  bonaire: { city: "Kralendijk", aliases: ["bonaire"] },
  anguilla: { city: "The Valley", aliases: ["anguilla"] },
  montserrat: { city: "Brades", aliases: ["montserrat"] },
  martinique: { city: "Fort-de-France", aliases: ["martinique"] },
  guadeloupe: { city: "Pointe-à-Pitre", aliases: ["guadeloupe"] },
  "saint lucia": { city: "Castries", aliases: ["saint lucia", "st lucia"] },
  "turks and caicos": {
    city: "Providenciales",
    aliases: ["turks and caicos", "turks and caicos islands", "turks"],
  },
  "turks and caicos islands": {
    city: "Providenciales",
    aliases: ["turks and caicos", "turks and caicos islands", "turks"],
  },
});

function countryKeysMatch(a, b) {
  const ak = foldCityKey(a);
  const bk = foldCityKey(b);
  if (!ak || !bk) return false;
  return ak === bk || ak.includes(bk) || bk.includes(ak);
}

/**
 * True when city is known to belong to a different country/territory than provided.
 */
export function cityConflictsWithCountry(city, country) {
  const cityKey = foldCityKey(city);
  const countryKey = foldCityKey(country);
  if (!cityKey || !countryKey) return false;
  const implied = CITY_IMPLIES_COUNTRY[cityKey];
  if (!implied) return false;
  const ok = implied.some((c) => countryKeysMatch(countryKey, c));
  return !ok;
}

/**
 * Map bare island/territory labels to primary commercial locality when country matches.
 */
export function resolveIslandPrimaryLocality(city, country) {
  const countryKey = foldCityKey(country);
  const cityKey = foldCityKey(city);
  if (!countryKey || !cityKey) return null;
  const entry =
    ISLAND_PRIMARY_LOCALITY[countryKey] ||
    Object.entries(ISLAND_PRIMARY_LOCALITY).find(([k]) =>
      countryKeysMatch(countryKey, k)
    )?.[1];
  if (!entry) return null;
  const aliasHit = (entry.aliases || []).some((a) => foldCityKey(a) === cityKey);
  if (aliasHit || cityKey === countryKey) {
    return {
      city: entry.city,
      method: "island_primary_locality",
      confidence: 0.86,
      known_city: true,
      inferred: true,
    };
  }
  return null;
}

function applyCountryCityGuard(result, country) {
  if (!result?.city || !country) return result;
  if (!cityConflictsWithCountry(result.city, country)) return result;
  return {
    ...result,
    country_city_conflict: true,
    conflict_city: result.city,
    city: result.city,
    confidence: Math.min(Number(result.confidence) || 0, 0.55),
    known_city: false,
    method: `${result.method || "city"}_country_conflict`,
  };
}

const ALIAS_TO_CANONICAL = (() => {
  const m = new Map();
  for (const [canon, aliases] of CITY_CANONICAL) {
    m.set(foldCityKey(canon), canon);
    for (const a of aliases) m.set(foldCityKey(a), canon);
  }
  return m;
})();

export function foldCityKey(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Normalize a city label to canonical accented form when known.
 * @returns {{ city: string|null, normalized: boolean, confidence_boost: number }}
 */
export function normalizeCityLabel(raw) {
  const s = String(raw || "").trim();
  if (!s) return { city: null, normalized: false, confidence_boost: 0 };
  const hit = ALIAS_TO_CANONICAL.get(foldCityKey(s));
  if (hit) {
    return {
      city: hit,
      normalized: hit !== s,
      confidence_boost: 0.08,
    };
  }
  return {
    city: titleCaseSlug(s.replace(/[-_]+/g, " ")),
    normalized: false,
    confidence_boost: 0,
  };
}

/**
 * @param {string|null|undefined} url
 */
export function inferCityFromCventUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return { city: null, method: null, confidence: 0, slug: null };

  let pathname = "";
  try {
    pathname = new URL(raw).pathname || "";
  } catch {
    const m = raw.match(/cvent\.com(\/[^?\s#]*)/i);
    pathname = m ? m[1] : "";
  }

  // Decode repeatedly for double-encoded paths
  let decoded = pathname;
  for (let i = 0; i < 3; i += 1) {
    try {
      const next = decodeURIComponent(decoded);
      if (next === decoded) break;
      decoded = next;
    } catch {
      break;
    }
  }

  const parts = decoded
    .split("/")
    .map((p) => String(p || "").trim())
    .filter(Boolean);

  const venuesIdx = parts.findIndex((p) => p.toLowerCase() === "venues");
  if (venuesIdx >= 0 && parts[venuesIdx + 1]) {
    const slug = parts[venuesIdx + 1];
    if (!NON_CITY_SLUGS.has(slug.toLowerCase()) && /[a-z\u00C0-\u024F]/i.test(slug)) {
      const titled = titleCaseSlug(slug);
      const norm = normalizeCityLabel(titled);
      const known = Boolean(ALIAS_TO_CANONICAL.get(foldCityKey(norm.city)));
      return {
        city: norm.city,
        method: "cvent_url_venues_slug",
        confidence: Math.min(0.95, (known ? 0.88 : 0.78) + norm.confidence_boost),
        slug,
        known_city: known,
      };
    }
  }

  const hotelIdx = parts.findIndex((p) => p.toLowerCase() === "hotel");
  if (hotelIdx > 0) {
    const slug = parts[hotelIdx - 1];
    if (!NON_CITY_SLUGS.has(slug.toLowerCase()) && /[a-z\u00C0-\u024F]/i.test(slug)) {
      const titled = titleCaseSlug(slug);
      const norm = normalizeCityLabel(titled);
      const known = Boolean(ALIAS_TO_CANONICAL.get(foldCityKey(norm.city)));
      return {
        city: norm.city,
        method: "cvent_url_pre_hotel_slug",
        confidence: Math.min(0.9, (known ? 0.8 : 0.65) + norm.confidence_boost),
        slug,
        known_city: known,
      };
    }
  }

  return { city: null, method: null, confidence: 0, slug: null };
}

/**
 * Infer city from hotel name when it ends with a known city (e.g. "Grand Hyatt Sao Paulo").
 */
export function inferCityFromHotelName(name, country = null) {
  const n = String(name || "").trim();
  if (!n) return { city: null, method: null, confidence: 0 };
  const folded = foldCityKey(n);
  // Longest alias first
  const aliases = [...ALIAS_TO_CANONICAL.entries()].sort(
    (a, b) => b[0].length - a[0].length
  );
  for (const [aliasKey, canon] of aliases) {
    if (aliasKey.length < 4) continue; // skip "rio", "bh" alone in name mid-string risk
    if (folded.endsWith(` ${aliasKey}`) || folded === aliasKey) {
      // Soft country filter for ambiguous short aliases only
      if (aliasKey.length <= 5 && country) {
        /* keep */
      }
      return {
        city: canon,
        method: "name_suffix_known_city",
        confidence: aliasKey.length >= 8 ? 0.82 : 0.74,
        known_city: true,
      };
    }
  }
  return { city: null, method: null, confidence: 0 };
}

/**
 * @param {object} candidate
 */
export function resolveDiscoveryCity(candidate = {}) {
  const country = candidate.country || candidate.origin_country || null;
  const explicitRaw = String(candidate.origin_city || candidate.city || "").trim();
  if (explicitRaw) {
    const island = resolveIslandPrimaryLocality(explicitRaw, country);
    if (island) return island;
    const norm = normalizeCityLabel(explicitRaw);
    return applyCountryCityGuard(
      {
        city: norm.city,
        method: "explicit",
        confidence: Math.min(1, 0.96 + norm.confidence_boost),
        inferred: false,
        known_city: Boolean(ALIAS_TO_CANONICAL.get(foldCityKey(norm.city))),
      },
      country
    );
  }

  const urlHit = inferCityFromCventUrl(
    candidate.origin_url || candidate.source_url || candidate.website
  );
  const nameHit = inferCityFromHotelName(
    candidate.property_name || candidate.origin_name || candidate.name,
    country
  );

  // Prefer URL if present; boost when name agrees
  if (urlHit.city && nameHit.city) {
    const same =
      foldCityKey(urlHit.city) === foldCityKey(nameHit.city) ||
      ALIAS_TO_CANONICAL.get(foldCityKey(urlHit.city)) ===
        ALIAS_TO_CANONICAL.get(foldCityKey(nameHit.city));
    if (same) {
      const island = resolveIslandPrimaryLocality(urlHit.city, country);
      if (island) return island;
      return applyCountryCityGuard(
        {
          city: urlHit.city,
          method: "cvent_url_and_name_agree",
          confidence: Math.min(0.97, Math.max(urlHit.confidence, nameHit.confidence) + 0.08),
          inferred: true,
          known_city: true,
          corroboration: ["url", "name"],
        },
        country
      );
    }
    // Conflict → lower confidence, keep URL as primary, flag multi-city
    return applyCountryCityGuard(
      {
        city: urlHit.city,
        alternate_city: nameHit.city,
        method: "cvent_url_name_conflict",
        confidence: Math.min(urlHit.confidence, 0.68),
        inferred: true,
        known_city: urlHit.known_city,
        multi_city: true,
        corroboration: ["url"],
      },
      country
    );
  }

  if (urlHit.city) {
    const island = resolveIslandPrimaryLocality(urlHit.city, country);
    if (island) return island;
    return applyCountryCityGuard({ ...urlHit, inferred: true }, country);
  }
  if (nameHit.city) {
    const island = resolveIslandPrimaryLocality(nameHit.city, country);
    if (island) return island;
    return applyCountryCityGuard({ ...nameHit, inferred: true }, country);
  }
  return { city: null, method: null, confidence: 0, inferred: false };
}

export function titleCaseSlug(slug) {
  return String(slug)
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .map((w) => {
      if (!w) return w;
      if (/^(de|da|do|dos|das|del|la|le|los|las|el|y|e|a|o)$/i.test(w)) {
        return w.toLowerCase();
      }
      return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    })
    .join(" ")
    .replace(/\bDe Janeiro\b/i, "de Janeiro");
}
