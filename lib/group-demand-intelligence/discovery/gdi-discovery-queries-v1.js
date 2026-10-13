/**
 * Shared language-aware GDI discovery query generator.
 * Market-agnostic · language-aware · hotel-aware · source-aware.
 * Does not invent demand; emits native + English control queries with provenance.
 */

import { GDI_BASE_OF_DEMAND } from "../ten-bases-of-demand-v1/taxonomy.js";
import { buildBaseQueries } from "../ten-bases-of-demand-v1/base-queries.js";
import {
  resolveGdiMarketLanguages,
  languagesForScoutPass,
} from "../discovery-expansion-v3/market-languages.js";

const B = GDI_BASE_OF_DEMAND;

/** Extra native terminology by language (local forms, not 1:1 EN translation). */
const NATIVE_LEXICON = Object.freeze({
  es: {
    event: [
      "congreso",
      "convención",
      "foro",
      "cumbre",
      "seminario",
      "jornadas",
      "asamblea",
      "simposio",
      "encuentro",
      "feria",
    ],
    lodging: [
      "alojamiento",
      "hospedaje",
      "hotel oficial",
      "hoteles recomendados",
      "bloque de habitaciones",
      "tarifa preferencial",
      "convenio hotelero",
      "hotel sede",
    ],
    buyer: [
      "secretaría técnica",
      "comité organizador",
      "responsable de eventos",
      "responsable de alojamiento",
      "agencia de viajes",
      "expositores",
      "patrocinadores",
      "delegación",
    ],
    institutional: [
      "licitación",
      "contratación",
      "ministerio",
      "cámara",
      "universidad",
      "ONG",
    ],
  },
  gl: {
    event: ["congreso", "xornadas", "encontro", "asemblea", "feira", "simposio"],
    lodging: ["aloxamento", "hoteis recomendados", "hotel oficial", "bloque de habitacións"],
    buyer: ["secretaría técnica", "comité organizador", "delegación", "expositores"],
    institutional: [
      "licitación",
      "contratación",
      "universidade",
      "investigación",
      "enerxía",
      "naval",
      "marítimo",
      "infraestrutura",
      "proxecto",
      "formación",
    ],
  },
  en: {
    event: ["congress", "conference", "summit", "forum", "symposium", "trade fair"],
    lodging: [
      "official hotel",
      "hotel block",
      "room block",
      "recommended hotels",
      "preferential rate",
    ],
    buyer: [
      "technical secretariat",
      "organizing committee",
      "housing bureau",
      "exhibitor services",
      "travel desk",
    ],
    institutional: ["RFP", "procurement", "ministry", "chamber", "university"],
  },
  de: {
    event: [
      "Kongress",
      "Tagung",
      "Jahrestagung",
      "Konferenz",
      "Messe",
      "Fachmesse",
      "Symposium",
      "Forum",
      "Veranstaltung",
    ],
    lodging: [
      "Unterkunft",
      "Übernachtung",
      "Hotelkontingent",
      "Zimmerkontingent",
      "Partnerhotel",
      "Tagungshotel",
      "offizielles Hotel",
      "Hotelbuchung",
    ],
    buyer: [
      "Veranstaltungsmanagement",
      "Tagungsmanagement",
      "Reisemanagement",
      "Einkauf",
      "Beschaffung",
      "Kongressbüro",
      "Teilnehmermanagement",
      "Ausstellerservice",
      "PCO",
      "Eventagentur",
    ],
    institutional: [
      "Ausschreibung",
      "Vergabe",
      "Rahmenvertrag",
      "Firmentagung",
      "Vertriebstagung",
      "Schulung",
      "Incentive",
      "Pharma",
      "Automobil",
      "Technologie",
    ],
  },
});

const SERP_GL_BY_COUNTRY = Object.freeze({
  Spain: "es",
  ES: "es",
  "Dominican Republic": "do",
  DO: "do",
  Switzerland: "ch",
  CH: "ch",
  "United States": "us",
  US: "us",
  Germany: "de",
  DE: "de",
  Deutschland: "de",
});

function placeNamesForHotel(hotelProfile = {}, market = "") {
  const territory = hotelProfile.demandTerritory || {};
  const includes = Array.isArray(territory.includes) ? territory.includes : [];
  const city = hotelProfile.city || hotelProfile.capabilityProfile?.city || "";
  const label = territory.label || market || city || "";
  const names = [city, label.split("/")[0]?.trim()].filter(Boolean);
  // Pull short keyword tokens from classificationKeywords CORE if present
  const core = territory.classificationKeywords?.TERRITORY_CORE || [];
  for (const k of core.slice(0, 4)) {
    if (k && !names.some((n) => String(n).toLowerCase().includes(String(k).toLowerCase()))) {
      names.push(k);
    }
  }
  for (const line of includes.slice(0, 2)) {
    const short = String(line)
      .replace(/^PRIMARY:\s*/i, "")
      .replace(/^SECONDARY:\s*/i, "")
      .split(/[,/]/)[0]
      .trim();
    if (short && short.length < 48) names.push(short);
  }
  return [...new Set(names.map((n) => String(n).trim()).filter(Boolean))];
}

function pushUnique(arr, item, max) {
  if (arr.length >= max) return;
  const key = `${item.queryLanguage}|${item.query}`;
  if (arr.some((x) => `${x.queryLanguage}|${x.query}` === key)) return;
  arr.push(item);
}

/**
 * @param {{
 *   market?: string,
 *   country?: string,
 *   languages?: string[],
 *   baseOfDemand?: string,
 *   hotelProfile?: object,
 *   hotelId?: string,
 *   lane?: "NATIVE"|"ENGLISH_CONTROL"|"COMBINED",
 *   maxPerBase?: number,
 * }} opts
 */
export function buildGdiDiscoveryQueries(opts = {}) {
  const hotelProfile = opts.hotelProfile || {};
  const hotelId = opts.hotelId || hotelProfile.hotelId || null;
  const market =
    opts.market ||
    hotelProfile.demandTerritory?.label ||
    hotelProfile.city ||
    "unknown";
  const country =
    opts.country ||
    hotelProfile.country ||
    hotelProfile.demandTerritory?.country ||
    hotelProfile.capabilityProfile?.country ||
    "";
  const langProfile = resolveGdiMarketLanguages(
    { hotelId, market, label: hotelProfile.displayName },
    market
  );
  const configLangs = hotelProfile.locale
    ? [
        hotelProfile.locale.primary,
        ...(hotelProfile.locale.secondary || []),
      ].filter(Boolean)
    : null;
  const languages =
    opts.languages ||
    configLangs ||
    languagesForScoutPass(langProfile, { allowSelective: opts.allowSelective === true });

  const bases = opts.baseOfDemand
    ? [opts.baseOfDemand]
    : Object.values(GDI_BASE_OF_DEMAND);
  const lane = String(opts.lane || "COMBINED").toUpperCase();
  const maxPerBase = opts.maxPerBase ?? 4;
  const places = placeNamesForHotel(hotelProfile, market);
  const place = places[0] || market;
  const serpGl = SERP_GL_BY_COUNTRY[country] || langProfile.primaryLanguage || "us";

  const out = [];

  for (const base of bases) {
    const hotelForBase = {
      languages:
        lane === "ENGLISH_CONTROL"
          ? ["en"]
          : lane === "NATIVE"
            ? languages.filter((l) => l !== "en").concat(languages.includes("en") ? [] : [])
                .length
              ? languages.filter((l) => l !== "en")
              : [langProfile.primaryLanguage]
            : languages,
      placeNames: places,
      destinationMarket: place,
      market: place,
      feederMarkets: langProfile.feederMarkets || [],
      competitors: hotelProfile.competitors || [],
    };
    if (!hotelForBase.languages.length) {
      hotelForBase.languages = [langProfile.primaryLanguage || "es"];
    }

    // For NATIVE lane with multiple local languages (e.g. es+gl), generate per-language
    // so secondary locals are not starved by primary-only buildBaseQueries.
    const langPasses =
      lane === "NATIVE" && hotelForBase.languages.length > 1
        ? hotelForBase.languages.map((lang) => ({
            ...hotelForBase,
            languages: [lang],
          }))
        : [hotelForBase];

    for (const passHotel of langPasses) {
      const baseQs = buildBaseQueries(passHotel, base, {
        maxQueries: Math.max(1, Math.ceil(maxPerBase / langPasses.length)),
        priority: "HIGH",
      });

      for (const q of baseQs) {
        const queryLanguage = q.language || passHotel.languages[0] || "en";
        if (lane === "ENGLISH_CONTROL" && queryLanguage !== "en") continue;
        if (lane === "NATIVE" && queryLanguage === "en") continue;
        pushUnique(
          out,
          {
            query: q.query,
            queryLanguage,
            sourceLanguage: queryLanguage,
            queryFamily: `${base}:${queryLanguage}`,
            baseOfDemand: base,
            market,
            country,
            hotelId,
            lane,
            sourceType: "DISCOVERY_QUERY",
            sourceDomain: null,
            serpLocale: {
              hl: queryLanguage === "gl" ? "gl" : queryLanguage,
              gl: serpGl,
            },
            marketRole: q.marketRole || "DESTINATION",
            feederMarket: q.feederMarket || null,
          },
          bases.length * maxPerBase * 3
        );
      }
    }

    // Native lexicon boosters (local forms) — one lodging + one event per native lang
    if (lane !== "ENGLISH_CONTROL") {
      for (const lang of hotelForBase.languages.filter((l) => l !== "en")) {
        const lex = NATIVE_LEXICON[lang];
        if (!lex) continue;
        const eventTerm = lex.event[0];
        const lodgeTerm = lex.lodging[0];
        pushUnique(
          out,
          {
            query: `${place} ${eventTerm} 2026 OR 2027 ${lodgeTerm}`,
            queryLanguage: lang,
            sourceLanguage: lang,
            queryFamily: `${base}:native_lexicon:${lang}`,
            baseOfDemand: base,
            market,
            country,
            hotelId,
            lane: lane === "COMBINED" ? "NATIVE" : lane,
            sourceType: "DISCOVERY_QUERY",
            sourceDomain: null,
            serpLocale: { hl: lang, gl: serpGl },
            marketRole: "DESTINATION",
            feederMarket: null,
          },
          bases.length * maxPerBase * 3
        );
      }
    }

    // English control always available in COMBINED / ENGLISH_CONTROL
    if (lane !== "NATIVE") {
      pushUnique(
        out,
        {
          query: `${place} conference OR congress 2026 OR 2027 official hotel OR room block`,
          queryLanguage: "en",
          sourceLanguage: "en",
          queryFamily: `${base}:english_control`,
          baseOfDemand: base,
          market,
          country,
          hotelId,
          lane: "ENGLISH_CONTROL",
          sourceType: "DISCOVERY_QUERY",
          sourceDomain: null,
          serpLocale: { hl: "en", gl: serpGl },
          marketRole: "DESTINATION",
          feederMarket: null,
        },
        bases.length * maxPerBase * 3
      );
    }
  }

  return {
    hotelId,
    market,
    country,
    languages,
    langProfile,
    lane,
    queryCount: out.length,
    queries: out,
    generatorId: "buildGdiDiscoveryQueries_v1",
  };
}

export { NATIVE_LEXICON, SERP_GL_BY_COUNTRY, placeNamesForHotel };
