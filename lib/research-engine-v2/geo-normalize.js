/**
 * Geography hard constraints + explicit alias map for Research Engine V2.1.
 * Do not loosen identity with fuzzy geo matching.
 */

/** Explicit aliases only — never invent Cancun↔Riviera Maya auto-match. */
export const GEO_ALIAS_MAP_V1_1 = Object.freeze({
  // city canonical → aliases
  "playa del carmen": ["playa del carmen", "pcm", "solidaridad"],
  tulum: ["tulum"],
  cancun: ["cancun", "cancún", "benito juarez", "benito juárez"],
  // Riviera Maya is a region label — NOT an automatic city match to Cancun or Tulum
  "riviera maya": ["riviera maya"],
  "mexico city": ["mexico city", "ciudad de mexico", "ciudad de méxico", "cdmx", "mexico", "mex"],
  polanco: ["polanco", "mexico city", "ciudad de mexico"],
  guadalajara: ["guadalajara", "gdl", "zapopan", "providencia"],
  // Providencia is a district of Guadalajara — alias allowed explicitly
  providencia: ["providencia", "guadalajara"],
  "san miguel de allende": ["san miguel de allende", "san miguel"],
  "la paz": ["la paz"],
  "los cabos": ["los cabos", "cabo san lucas", "san jose del cabo", "san josé del cabo", "east cape"],
  "san jose del cabo": ["san jose del cabo", "san josé del cabo", "los cabos"],
  "cabo san lucas": ["cabo san lucas", "los cabos"],
  tijuana: ["tijuana", "tij"],
  guanajuato: ["guanajuato"],
  merida: ["merida", "mérida"],
  holbox: ["holbox", "isla holbox"],
  bogota: ["bogota", "bogotá"],
  cali: ["cali"],
  medellin: ["medellin", "medellín"],
  barranquilla: ["barranquilla"],
  panama: ["panama", "panamá", "panama city"],
  lima: ["lima", "miraflores"],
  miraflores: ["miraflores", "lima"],
  barbados: ["barbados", "bridgetown"],
  bridgetown: ["bridgetown", "barbados"],
  "grand cayman": ["grand cayman", "cayman islands", "george town"],
  "cayman islands": ["cayman islands", "grand cayman"],
});

/**
 * @param {unknown} raw
 */
export function normalizeGeoLabel(raw) {
  return String(raw || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * @param {unknown} a
 * @param {unknown} b
 */
export function countriesAlign(a, b) {
  const na = normalizeCountry(a);
  const nb = normalizeCountry(b);
  if (!na || !nb) return true; // unknown country → do not hard-reject here (city/name must carry)
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  // mexico region strings
  if ((na === "mexico" || na === "mx") && (nb === "mexico" || nb === "mx")) return true;
  return false;
}

function normalizeCountry(raw) {
  const s = normalizeGeoLabel(raw);
  if (!s) return "";
  if (s === "mx" || s.startsWith("mexico")) return "mexico";
  if (s === "co" || s.startsWith("colombia")) return "colombia";
  if (s === "pa" || s.startsWith("panama")) return "panama";
  if (s === "pe" || s.startsWith("peru")) return "peru";
  if (s === "ar" || s.startsWith("argentina")) return "argentina";
  if (s === "br" || s.startsWith("brazil") || s.startsWith("brasil")) return "brazil";
  if (s.includes("dominican")) return "dominican republic";
  if (s.includes("cayman")) return "cayman islands";
  if (s.includes("barbados")) return "barbados";
  if (s.includes("costa rica")) return "costa rica";
  if (s.includes("honduras")) return "honduras";
  if (s.includes("grenada")) return "grenada";
  if (s.includes("dominica") && !s.includes("dominican")) return "dominica";
  if (s.includes("ecuador")) return "ecuador";
  if (s.includes("chile")) return "chile";
  if (s.includes("paraguay")) return "paraguay";
  if (s.includes("jamaica")) return "jamaica";
  return s;
}

/**
 * @param {unknown} cityA
 * @param {unknown} cityB
 * @param {{ hotelName?: string, candidateName?: string }} [ctx]
 * @returns {{ status: "align"|"alias"|"unknown"|"reject", note?: string }}
 */
export function citiesAlign(cityA, cityB, ctx = {}) {
  const a = normalizeGeoLabel(cityA);
  const b = normalizeGeoLabel(cityB);
  const nameBlob = normalizeGeoLabel(`${ctx.hotelName || ""} ${ctx.candidateName || ""}`);

  // Infer city from hotel names when census city blank
  const inferredA = a || inferCityFromName(nameBlob.split(/\s+/).slice(0, 8).join(" ")) || inferCityFromName(ctx.hotelName);
  const inferredB = b || inferCityFromName(ctx.candidateName);

  if (!inferredA && !inferredB) return { status: "unknown", note: "both cities blank" };
  if (!inferredA || !inferredB) {
    // One side blank: try name embeds
    const fromNames = cityMentionedInNames(ctx.hotelName, ctx.candidateName);
    if (fromNames) return fromNames;
    return { status: "unknown", note: "one city blank" };
  }

  if (inferredA === inferredB) return { status: "align" };
  if (inferredA.includes(inferredB) || inferredB.includes(inferredA)) return { status: "align" };

  // Hard reject known incompatible pairs
  if (incompatiblePair(inferredA, inferredB)) {
    return { status: "reject", note: `${inferredA} vs ${inferredB} incompatible` };
  }

  if (aliasAlign(inferredA, inferredB)) return { status: "alias", note: "explicit geo alias" };

  // Cancun must not match Riviera Maya / Tulum / Playa automatically
  if (
    (isCancun(inferredA) && isRivieraish(inferredB) && !isCancun(inferredB)) ||
    (isCancun(inferredB) && isRivieraish(inferredA) && !isCancun(inferredA))
  ) {
    return { status: "reject", note: "Cancun vs Riviera Maya / corridor mismatch" };
  }

  return { status: "reject", note: `${inferredA} ≠ ${inferredB}` };
}

function inferCityFromName(name) {
  const n = normalizeGeoLabel(name);
  if (!n) return "";
  const keys = Object.keys(GEO_ALIAS_MAP_V1_1).sort((a, b) => b.length - a.length);
  for (const key of keys) {
    if (n.includes(key)) return key;
  }
  // trailing ", City" patterns already normalized
  return "";
}

function cityMentionedInNames(hotelName, candidateName) {
  const a = normalizeGeoLabel(hotelName);
  const b = normalizeGeoLabel(candidateName);
  const keys = Object.keys(GEO_ALIAS_MAP_V1_1);
  /** @type {string[]} */
  const aHits = keys.filter((k) => a.includes(k));
  /** @type {string[]} */
  const bHits = keys.filter((k) => b.includes(k));
  if (!aHits.length && !bHits.length) return { status: "unknown", note: "no city tokens in names" };
  if (!aHits.length || !bHits.length) {
    // One name carries a city token — treat as weak align if the other name is otherwise specific
    return { status: "unknown", note: "city only in one name" };
  }
  for (const x of aHits) {
    for (const y of bHits) {
      if (x === y || aliasAlign(x, y)) return { status: "alias", note: "city tokens in both names" };
      if (incompatiblePair(x, y)) return { status: "reject", note: `name cities incompatible ${x}/${y}` };
    }
  }
  return { status: "unknown", note: "name cities inconclusive" };
}

function aliasAlign(a, b) {
  const na = normalizeGeoLabel(a);
  const nb = normalizeGeoLabel(b);
  for (const [canonical, aliases] of Object.entries(GEO_ALIAS_MAP_V1_1)) {
    const set = new Set([canonical, ...aliases].map(normalizeGeoLabel));
    if (set.has(na) && set.has(nb)) return true;
  }
  return false;
}

function incompatiblePair(a, b) {
  const pairs = [
    ["tulum", "cancun"],
    ["tulum", "playa del carmen"],
    ["tulum", "mexico city"],
    ["playa del carmen", "cancun"],
    ["playa del carmen", "mexico city"],
    ["tijuana", "mexico city"],
    ["tijuana", "guadalajara"],
    ["guanajuato", "guadalajara"],
    ["la paz", "los cabos"],
    ["cali", "bogota"],
    ["cali", "medellin"],
    ["casa nizuc", "casa francia"],
  ];
  const na = normalizeGeoLabel(a);
  const nb = normalizeGeoLabel(b);
  return pairs.some(
    ([x, y]) => (na.includes(x) && nb.includes(y)) || (na.includes(y) && nb.includes(x))
  );
}

function isCancun(s) {
  return /cancun/.test(normalizeGeoLabel(s));
}
function isRivieraish(s) {
  const n = normalizeGeoLabel(s);
  return /riviera maya|tulum|playa del carmen|holbox/.test(n);
}
