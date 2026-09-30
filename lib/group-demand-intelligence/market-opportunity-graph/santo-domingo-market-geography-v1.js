/**
 * Santo Domingo GDI market geography V1 — hotel-agnostic hierarchy.
 * METRO → sector → submarket → micro-area. No NYC bleed.
 */

import { MARKET_LEVEL } from "./market-geography-v1.js";

export const SANTO_DOMINGO_HOTELS = Object.freeze({
  JW: "recESHsNsWUFYZrxR",
  RADISSON: "recUOyzOXn2Zdp98I",
});

export const SANTO_DOMINGO_METRO_LABEL = "Santo Domingo";

export const SANTO_DOMINGO_BOROUGHS = Object.freeze([
  {
    label: "Distrito Nacional",
    borough: "Distrito Nacional",
    patterns: [
      /\bdistrito nacional\b/,
      // Do NOT match bare "santo domingo" — that is metro-only
    ],
  },
  {
    label: "Santo Domingo Este",
    borough: "Santo Domingo Este",
    patterns: [/\bsanto domingo este\b/],
  },
]);

export const SANTO_DOMINGO_SUBMARKETS = Object.freeze([
  {
    label: "Piantini / Blue Mall",
    borough: "Distrito Nacional",
    patterns: [
      /\bpiantini\b/,
      /\bblue mall\b/,
      /\bwinston churchill\b/,
      /\bensanche piantini\b/,
    ],
  },
  {
    label: "Naco / Tiradentes",
    borough: "Distrito Nacional",
    patterns: [
      /\bnaco\b/,
      /\btiradentes\b/,
      /\bpresidente gonz[aá]lez\b/,
      /\bav\.?\s*tiradentes\b/,
    ],
  },
  {
    label: "Zona Colonial",
    borough: "Distrito Nacional",
    patterns: [/\bzona colonial\b/, /\bciudad colonial\b/],
  },
  {
    label: "Malecón",
    borough: "Distrito Nacional",
    patterns: [/\bmalec[oó]n\b/, /\bgcorge washington\b/, /\bgeorge washington\b/],
  },
  {
    label: "Bella Vista",
    borough: "Distrito Nacional",
    patterns: [/\bbella vista\b/],
  },
]);

export const SANTO_DOMINGO_MICRO_AREAS = Object.freeze([
  {
    label: "Winston Churchill / Blue Mall node",
    submarket: "Piantini / Blue Mall",
    borough: "Distrito Nacional",
    patterns: [/\bwinston churchill\b/, /\bblue mall\b/],
  },
  {
    label: "Tiradentes / Presidente González node",
    submarket: "Naco / Tiradentes",
    borough: "Distrito Nacional",
    patterns: [/\btiradentes\b/, /\bpresidente gonz[aá]lez\b/],
  },
  {
    label: "Acropolis / corporate node",
    submarket: "Piantini / Blue Mall",
    borough: "Distrito Nacional",
    patterns: [/\bac[oó]polis\b/, /\bacropolis\b/],
  },
]);

/** Reject early — not Santo Domingo city demand. */
export const SANTO_DOMINGO_NEGATIVE_MARKETS = Object.freeze([
  /\bpunta cana\b(?!.*santo domingo)/i,
  /\bcap cana\b/i,
  /\bpuerto plata\b/i,
  /\bsantiago\b(?!.*santo domingo)/i,
  /\bla romana\b/i,
  /\bbavaro\b/i,
  /\bcanc[uú]n\b/i,
  /\bmiami\b(?!.*santo domingo)/i,
]);

export function santoDomingoMarketHints() {
  return {
    metroLabel: SANTO_DOMINGO_METRO_LABEL,
    metroPatterns: [
      /\bsanto domingo\b/,
      /\bdistrito nacional\b/,
      /\bpiantini\b/,
      /\bnaco\b/,
      /\btiradentes\b/,
    ],
    boroughs: SANTO_DOMINGO_BOROUGHS,
    submarkets: SANTO_DOMINGO_SUBMARKETS,
    microAreas: SANTO_DOMINGO_MICRO_AREAS,
  };
}

/**
 * True when blob is Santo Domingo metro (not feeder-only DR destinations).
 */
export function isSantoDomingoInMarket(text = "") {
  const t = String(text || "").toLowerCase();
  if (!t) return false;
  for (const re of SANTO_DOMINGO_NEGATIVE_MARKETS) {
    if (re.test(t) && !/\bsanto domingo\b|\bdistrito nacional\b|\bpiantini\b|\bnaco\b/.test(t)) {
      return false;
    }
  }
  return (
    /\bsanto domingo\b/.test(t) ||
    /\bdistrito nacional\b/.test(t) ||
    /\bpiantini\b/.test(t) ||
    /\bnaco\b/.test(t) ||
    /\btiradentes\b/.test(t) ||
    /\bblue mall\b/.test(t) ||
    /\bwinston churchill\b/.test(t) ||
    /\bzona colonial\b/.test(t) ||
    /\bcidac\b/.test(t) ||
    /\bquisqueya\b/.test(t)
  );
}

/**
 * Market-level SERP query pack (Spanish-first). Discover once — not per hotel.
 */
export function buildSantoDomingoMarketQueries({ max = 55 } = {}) {
  const y1 = 2026;
  const y2 = 2027;
  const y3 = 2028;
  const city = "Santo Domingo";
  const queries = [];
  const push = (family, query) => {
    if (queries.length >= max) return;
    queries.push({ family, query, queryId: `sd_${family}_${queries.length}` });
  };

  // Official event / congress
  push("OFFICIAL_EVENT_PAGE", `"${city}" (${y2} OR ${y3}) (congreso OR conferencia OR symposium) (hotel OR alojamiento OR hospedaje)`);
  push("OFFICIAL_EVENT_PAGE", `"Distrito Nacional" (${y2} OR ${y3}) congreso (hotel OR alojamiento)`);
  push("OFFICIAL_EVENT_PAGE", `"${city}" ${y1} (congreso OR reunión anual) (alojamiento OR "hotel oficial")`);
  push("OFFICIAL_EVENT_PAGE", `"${city}" (${y2} OR ${y3}) "annual meeting" OR "annual conference" (hotel OR housing OR accommodation)`);

  // Housing / host hotel
  push("HOUSING_PAGE", `"${city}" ("hotel oficial" OR "host hotel" OR "room block" OR alojamiento) (${y2} OR ${y3})`);
  push("HOUSING_PAGE", `"${city}" ("hotel TBD" OR "sede por determinar" OR "venue TBD" OR "hotel por determinar") (${y2} OR ${y3})`);
  push("HOUSING_PAGE", `"${city}" (hospedaje OR "group rate" OR "tarifa grupo") congreso ${y2}`);

  // Venues
  push("VENUE_PAGE", `(CIDAC OR Acropolis OR Acrópolis OR "Centro de Convenciones") "${city}" (${y2} OR ${y3}) (hotel OR alojamiento)`);
  push("VENUE_PAGE", `"Blue Mall" OR Piantini (evento OR congreso OR gala) (${y2} OR ${y3}) hotel`);

  // Association / professional
  push("ASSOCIATION_PAGE", `"${city}" (asociación OR association) (congreso OR reunión) (${y2} OR ${y3}) (hotel OR alojamiento)`);
  push("ASSOCIATION_PAGE", `República Dominicana (congreso médico OR congreso científico) (${y2} OR ${y3}) "Santo Domingo" hotel`);
  push("ASSOCIATION_PAGE", `"${city}" (abogados OR legal OR insurance OR seguros) (congreso OR foro) ${y2} hotel`);
  push("ASSOCIATION_PAGE", `"${city}" (banca OR financiero OR finanzas) (foro OR conferencia) (${y2} OR ${y3}) hotel`);

  // Medical / scientific
  push("ASSOCIATION_PAGE", `"${city}" (congreso médico OR jornadas médicas OR symposium médico) (${y2} OR ${y3}) alojamiento`);
  push("UNIVERSITY_PAGE", `(INTEC OR UASD OR UNIBE OR PUCMM) "${city}" (congreso OR symposium) (${y2} OR ${y3}) hotel`);

  // Corporate / regional
  push("ORGANIZATION_SITE", `"${city}" (corporate meeting OR board meeting OR offsite) (${y2} OR ${y3}) hotel`);
  push("ORGANIZATION_SITE", `"${city}" (cumbre OR summit OR foro regional) Caribe OR Latam (${y2} OR ${y3}) hotel`);

  // Government
  push("GOVERNMENT_PAGE", `(ministerio OR gobierno) "Santo Domingo" (evento OR foro OR cumbre) (${y2} OR ${y3}) (hotel OR alojamiento)`);

  // Sports
  push("SPORTS_PAGE", `"${city}" (torneo OR championship OR campeonato) (${y2} OR ${y3}) (hotel OR "team hotel" OR alojamiento)`);
  push("SPORTS_PAGE", `"Estadio Quisqueya" OR Quisqueya (torneo OR serie) (${y2} OR ${y3}) hotel`);

  // Trade / travel
  push("ASSOCIATION_PAGE", `"${city}" (turismo OR travel trade OR feria) (${y2} OR ${y3}) (hotel OR alojamiento)`);
  push("OFFICIAL_EVENT_PAGE", `site:.do "${city}" (congreso OR conferencia) (${y2} OR ${y3}) (alojamiento OR hotel)`);

  // Submarket-specific (still market-level — not hotel-branched)
  push("OFFICIAL_EVENT_PAGE", `Piantini OR "Blue Mall" (${y2} OR ${y3}) (evento OR gala OR conferencia) (hotel OR alojamiento)`);
  push("OFFICIAL_EVENT_PAGE", `Naco OR Tiradentes "${city}" (${y2} OR ${y3}) (reunión OR conferencia OR foro) hotel`);

  // English selective
  push("OFFICIAL_EVENT_PAGE", `"Santo Domingo" Dominican Republic (${y2} OR ${y3}) conference (hotel OR housing OR accommodation)`);
  push("HOUSING_PAGE", `"Santo Domingo" ("preferred hotel" OR "official hotel" OR housing) ${y2} OR ${y3}`);

  return queries.slice(0, max);
}

export function santoDomingoMarketDevelopmentState({
  lodgingEvidence,
  commercialStatus,
  validEntity,
  futureCycle,
} = {}) {
  if (!validEntity) return "DISCOVERED";
  if (!futureCycle) return "VALIDATED";
  const lodgingOk =
    lodgingEvidence &&
    lodgingEvidence !== "NONE" &&
    lodgingEvidence !== "UNKNOWN";
  if (!lodgingOk) return "VALIDATED";
  const status = String(commercialStatus || "");
  const open =
    /OPEN|TBD|RFP|UNRESOLVED|OVERFLOW|PARTIALLY|DESTINATION_SET|HOTEL_SELECTION|HOTEL \/ VENUE/i.test(
      status
    );
  const closed =
    /FULLY_PLACED|CURRENT CYCLE CLOSED|NO_OVERFLOW EVIDENCE/i.test(status) &&
    !/OVERFLOW POSSIBLE/i.test(status);
  if (closed) return "COMMERCIAL_STATUS_RESOLVED";
  // Lodging-supported + future cycle is matchable even if commercial still UNKNOWN
  // (hotel layer must still pass openness / readiness independently).
  if (open || lodgingEvidence === "DIRECT" || lodgingEvidence === "STRONG_INFERENCE") {
    return "HOTEL_MATCHABLE";
  }
  return "LODGING_SUPPORTED";
}
