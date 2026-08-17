/**
 * City / Locality Resolver V4 — Dealality Hotel Census.
 *
 * Priority (production-eligible):
 * 1. official structured locality
 * 2. official property address parse
 * 3. official URL locality slug (brand directory structure)
 *
 * Research-only (not production Address/City write without rights):
 * 4. SerpApi Exact/High address locality parse
 *
 * Never derive City from: hotel title, marketing name, brand, description,
 * Cvent, legacy Census, Country/State dump.
 */

import { validateCitySemantics, CITY_STATUS } from "../golden-field-semantics.js";
import { isDescriptorCity } from "../../census-city-state-normalizer.js";
import { titleCaseCitySlug } from "../../census-autopilot-choice-radisson-steward-resolution.js";

export const CITY_RESOLVER_V4_VERSION = "city-resolver-v4-2026-08-08";

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

const AR_PROVINCE = Object.freeze({
  caba: "Ciudad Autónoma de Buenos Aires",
  "ciudad autonoma de buenos aires": "Ciudad Autónoma de Buenos Aires",
  "buenos aires": "Buenos Aires",
  cordoba: "Córdoba",
  córdoba: "Córdoba",
  mendoza: "Mendoza",
  salta: "Salta",
  tucuman: "Tucumán",
  tucumán: "Tucumán",
  misiones: "Misiones",
  "santa fe": "Santa Fe",
  neuquen: "Neuquén",
  neuquén: "Neuquén",
  "santiago del estero": "Santiago del Estero",
  "rio negro": "Río Negro",
  "río negro": "Río Negro",
});

const CR_PROVINCE = Object.freeze([
  "San José",
  "Alajuela",
  "Cartago",
  "Heredia",
  "Guanacaste",
  "Puntarenas",
  "Limón",
]);

/** IHG URL city-slug → canonical Census City (accent / spelling). */
const IHG_CITY_SLUG_CANON = Object.freeze({
  "sao-paulo": "São Paulo",
  "belo-horizonte": "Belo Horizonte",
  goiania: "Goiânia",
  manaus: "Manaus",
  fortaleza: "Fortaleza",
  farroupilha: "Farroupilha",
  curitiba: "Curitiba",
  "rio-de-janeiro": "Rio de Janeiro",
  brasilia: "Brasília",
  salvador: "Salvador",
  recife: "Recife",
  "porto-alegre": "Porto Alegre",
  florianopolis: "Florianópolis",
  campinas: "Campinas",
  "buenos-aires": "Buenos Aires",
  mendoza: "Mendoza",
  rosario: "Rosario",
  "mexico-city": "Mexico City",
  cancun: "Cancún",
  guadalajara: "Guadalajara",
  monterrey: "Monterrey",
  "san-jose": "San José",
  liberia: "Liberia",
  "punta-cana": "Punta Cana",
  "santo-domingo": "Santo Domingo",
});

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

export function isBrazilCep(value) {
  const v = String(value || "").trim();
  return /^\d{5}-?\d{3}$/.test(v) || /^\d{2}\.\d{3}-\d{3}$/.test(v);
}

export function isArgentinaPostalFragment(value) {
  const v = String(value || "").trim();
  return /^[A-Z]?\d{4}[A-Z]{0,3}$/i.test(v) || /^[A-Z]\d{4}[A-Z]{3}$/i.test(v);
}

export function isStreetLineAsCity(value) {
  const v = String(value || "").trim();
  if (!v) return false;
  if (/^(av\.?|avenida|blvd\.?|boulevard|calle|calzada|camino|rua|r\.|strada|highway|km\.?)\b/i.test(v))
    return true;
  if (/\d/.test(v) && /(av\.|avenida|blvd|calle|no\.|#|norte|nte|sur|poniente|oriente)/i.test(v))
    return true;
  if (/parque industrial|fracc\.|residencial|interior|sector/i.test(v)) return true;
  if (v.length > 48) return true;
  return false;
}

export function isPostalAsCity(value, country) {
  const v = String(value || "").trim();
  if (!v) return false;
  if (country === "Brazil" && isBrazilCep(v)) return true;
  if (country === "Argentina" && isArgentinaPostalFragment(v)) return true;
  if (/\d{4,}/.test(v) && !/[A-Za-zÁÉÍÓÚáéíóúñÑ]{3,}/.test(v)) return true;
  return false;
}

/**
 * Classify current City label (do not clear plausible cities without stronger evidence).
 */
export function classifyCityLabel(city, country) {
  const raw = String(city || "").trim();
  if (!raw) return { bucket: "CITY_BLANK", status: CITY_STATUS.BLANK };
  if (/^unknown$/i.test(raw)) return { bucket: "CITY_UNKNOWN", status: CITY_STATUS.UNKNOWN };
  if (isPostalAsCity(raw, country)) return { bucket: "POSTAL_CODE_AS_CITY", status: CITY_STATUS.INVALID };
  if (isStreetLineAsCity(raw)) return { bucket: "CITY_INVALID", status: CITY_STATUS.INVALID, reason: "street_line_as_city" };
  const sem = validateCitySemantics(raw, country);
  if (sem.reason === "country_as_city" || sem.reason === "country_equals_city") {
    return { bucket: "COUNTRY_AS_CITY", status: CITY_STATUS.INVALID };
  }
  if (sem.status === CITY_STATUS.INVALID) {
    return { bucket: "CITY_INVALID", status: CITY_STATUS.INVALID, reason: sem.reason };
  }
  if (sem.ok) return { bucket: "CITY_PLAUSIBLE", status: CITY_STATUS.VALID };
  return { bucket: "CITY_INVALID", status: CITY_STATUS.INVALID, reason: sem.reason };
}

/**
 * Parse Brazil address → municipality, UF, CEP.
 */
export function parseBrazilAddress(address) {
  const addr = String(address || "").trim();
  if (!addr) return { ok: false, reason: "blank" };

  const cepM = addr.match(/\b(\d{5}-?\d{3})\b/);
  const cep = cepM ? cepM[1].replace(/^(\d{5})(\d{3})$/, "$1-$2") : null;

  let uf = null;
  const ufM = addr.match(
    /\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b/
  );
  if (ufM) uf = BR_UF[ufM[1]];

  let municipality = null;
  const m1 = addr.match(
    /,\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\s.'-]{2,40})\s*-\s*(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b/
  );
  if (m1) {
    municipality = m1[1].trim();
    uf = uf || BR_UF[m1[2]];
  }
  if (!municipality) {
    const m2 = addr.match(
      /\b([A-Za-zÁÉÍÓÚáéíóúñÑ\s.'-]{2,40})\s*-\s*(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b/
    );
    if (m2 && !/rua|avenida|av\.|alameda|travessa/i.test(m2[1])) {
      municipality = m2[1].trim();
      uf = uf || BR_UF[m2[2]];
    }
  }

  if (municipality && (isDescriptorCity(municipality) || isPostalAsCity(municipality, "Brazil"))) {
    municipality = null;
  }

  return {
    ok: Boolean(municipality || uf || cep),
    city: municipality,
    state: uf,
    postal_code: cep,
    method: "brazil_address_parser_v4",
  };
}

/**
 * Parse Argentina address → locality + province.
 */
export function parseArgentinaAddress(address) {
  const addr = String(address || "").trim();
  if (!addr) return { ok: false, reason: "blank" };

  const postalM = addr.match(/\b([A-Z]?\d{4}[A-Z]{0,3})\b/i);
  const postal = postalM ? postalM[1].toUpperCase() : null;

  let province = null;
  let city = null;

  for (const [k, v] of Object.entries(AR_PROVINCE)) {
    if (norm(addr).includes(norm(k))) {
      province = v;
      break;
    }
  }

  const locM = addr.match(
    /\b[A-Z]?\d{4}[A-Z]{0,3}\s+([A-Za-zÁÉÍÓÚáéíóúñÑ\s.'-]{2,40})(?:,|\s+Argentina|$)/i
  );
  if (locM) {
    city = locM[1].trim().replace(/,\s*$/, "");
    if (/argentina|province/i.test(city)) city = null;
  }

  if (postal && /^C\d/i.test(postal)) {
    province = "Ciudad Autónoma de Buenos Aires";
    if (!city || /^buenos aires$/i.test(city)) city = "Buenos Aires";
  }

  if (city && (isDescriptorCity(city) || isArgentinaPostalFragment(city))) city = null;

  return {
    ok: Boolean(city || province || postal),
    city,
    state: province,
    postal_code: postal,
    method: "argentina_address_parser_v4",
  };
}

/**
 * Costa Rica address / province cues.
 */
export function parseCostaRicaAddress(address) {
  const addr = String(address || "").trim();
  if (!addr) return { ok: false, reason: "blank" };

  let province = null;
  for (const p of CR_PROVINCE) {
    if (norm(addr).includes(norm(p))) {
      province = p;
      break;
    }
  }

  let city = null;
  const m = addr.match(
    /\b(Liberia|Tamarindo|Jac[oó]|San Jos[eé]|Escaz[uú]|Santa Ana|Guanacaste|Conchal|Papagayo|La Fortuna|Quepos|Manuel Antonio)\b/i
  );
  if (m) {
    city = m[1];
    if (/san jos/i.test(city)) city = "San José";
    if (/jac/i.test(city)) city = "Jacó";
  }

  return {
    ok: Boolean(city || province),
    city,
    state: province,
    method: "costa_rica_address_parser_v4",
  };
}

/**
 * Extract locality from official brand property URL (structure parse — not title).
 */
export function extractCityFromOfficialUrl(url, country) {
  const u = String(url || "").trim();
  if (!u) return { ok: false, reason: "no_url" };

  const ihg = u.match(/ihg\.com\/[^/]+\/hotels\/[a-z]{2}\/en\/([a-z0-9-]+)\/[a-z0-9]+\/hoteldetail/i);
  if (ihg) {
    const slug = ihg[1].toLowerCase();
    const city = IHG_CITY_SLUG_CANON[slug] || titleCaseCitySlug(slug);
    const sem = validateCitySemantics(city, country);
    if (sem.ok) {
      return {
        ok: true,
        city: sem.value,
        method: "official_ihg_url_city_slug",
        production_eligible: true,
        slug,
      };
    }
  }

  const choice = u.match(/choicehotels\.com\/([a-z-]+)\/([a-z0-9-]+)\/[a-z0-9-]+\/[a-z]{2}\d+/i);
  if (choice) {
    const city = titleCaseCitySlug(choice[2]);
    const sem = validateCitySemantics(city, country);
    if (sem.ok) {
      return {
        ok: true,
        city: sem.value,
        method: "official_choice_url_city_slug",
        production_eligible: true,
        state_slug: choice[1],
      };
    }
  }

  return { ok: false, reason: "no_url_city_structure" };
}

/**
 * Resolve City V4 from available evidence.
 */
export function resolveCityV4(input = {}) {
  const country = String(input.country || "").trim();
  const current = String(input.city || "").trim();
  const currentClass = classifyCityLabel(current, country);

  const layers = {
    official_locality: null,
    municipality: null,
    city: null,
    tourism_destination: null,
  };

  /** @type {object[]} */
  const evidence = [];

  if (input.official_locality) {
    const sem = validateCitySemantics(input.official_locality, country);
    if (sem.ok && !isStreetLineAsCity(sem.value) && !isPostalAsCity(sem.value, country)) {
      layers.official_locality = sem.value;
      layers.city = sem.value;
      evidence.push({ method: "official_structured_locality", value: sem.value, production_eligible: true });
    }
  }

  if (!layers.city && input.address) {
    let parsed = null;
    if (country === "Brazil") parsed = parseBrazilAddress(input.address);
    else if (country === "Argentina") parsed = parseArgentinaAddress(input.address);
    else if (country === "Costa Rica") parsed = parseCostaRicaAddress(input.address);
    else {
      const parts = String(input.address)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      const cand = parts.find(
        (p) =>
          validateCitySemantics(p, country).ok &&
          !isPostalAsCity(p, country) &&
          !isStreetLineAsCity(p) &&
          norm(p) !== norm(country)
      );
      if (cand) parsed = { ok: true, city: cand, method: "generic_address_locality" };
    }
    if (parsed?.ok && parsed.city && !isStreetLineAsCity(parsed.city)) {
      const sem = validateCitySemantics(parsed.city, country);
      if (sem.ok) {
        layers.municipality = sem.value;
        layers.city = sem.value;
        evidence.push({
          method: parsed.method,
          value: sem.value,
          state: parsed.state || null,
          postal_code: parsed.postal_code || null,
          production_eligible: true,
        });
      }
    }
  }

  if (!layers.city && input.official_url) {
    const fromUrl = extractCityFromOfficialUrl(input.official_url, country);
    if (fromUrl.ok) {
      layers.city = fromUrl.city;
      evidence.push({ ...fromUrl, production_eligible: true });
    }
  }

  if (!layers.city && currentClass.bucket === "CITY_PLAUSIBLE" && !isStreetLineAsCity(current)) {
    layers.city = current;
    evidence.push({ method: "retain_plausible_production_city", value: current, production_eligible: true });
  }

  if (!layers.city && input.research_address) {
    let parsed = null;
    if (country === "Brazil") parsed = parseBrazilAddress(input.research_address);
    else if (country === "Argentina") parsed = parseArgentinaAddress(input.research_address);
    else if (country === "Costa Rica") parsed = parseCostaRicaAddress(input.research_address);
    else {
      const parts = String(input.research_address)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      const cand = parts.find(
        (p) => validateCitySemantics(p, country).ok && !isStreetLineAsCity(p) && !isPostalAsCity(p, country)
      );
      if (cand) parsed = { ok: true, city: cand, method: "generic_research_address_locality" };
    }
    if (parsed?.ok && parsed.city && !isStreetLineAsCity(parsed.city) && validateCitySemantics(parsed.city, country).ok) {
      layers.city = parsed.city;
      evidence.push({
        method: `${parsed.method}_research`,
        value: parsed.city,
        state: parsed.state || null,
        production_eligible: false,
        rights: "BLOCKED_RIGHTS_OR_RESEARCH_ONLY",
        serpapi_used: true,
      });
    }
  }

  const best = evidence[0] || null;
  const cityOut = layers.city || null;
  let finalStatus = CITY_STATUS.UNKNOWN;
  if (cityOut && validateCitySemantics(cityOut, country).ok) finalStatus = CITY_STATUS.VALID;

  const invalidClear =
    ["POSTAL_CODE_AS_CITY", "COUNTRY_AS_CITY", "CITY_INVALID"].includes(currentClass.bucket) &&
    (!cityOut || finalStatus === CITY_STATUS.UNKNOWN || !best?.production_eligible);

  return {
    version: CITY_RESOLVER_V4_VERSION,
    ok: finalStatus === CITY_STATUS.VALID,
    city: cityOut,
    status: finalStatus,
    layers,
    evidence,
    method: best?.method || null,
    production_eligible: best?.production_eligible === true,
    state_hint: best?.state || evidence.find((e) => e.state)?.state || null,
    postal_code: best?.postal_code || null,
    current_class: currentClass,
    invalid_clear_current: invalidClear,
    serpapi_used: evidence.some((e) => e.serpapi_used),
    cvent_used: false,
    legacy_used: false,
  };
}

export { BR_UF, AR_PROVINCE, CR_PROVINCE, IHG_CITY_SLUG_CANON };
