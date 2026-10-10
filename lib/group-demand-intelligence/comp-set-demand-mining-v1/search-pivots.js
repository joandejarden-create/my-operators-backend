/**
 * buildCompSetPublicSearchPivots() — name / alias / phone / address / domain / meeting room / brand+city.
 */

const EVENT_TERMS_BY_LANG = Object.freeze({
  en: [
    "conference",
    "accommodation",
    "congress",
    "hotel block",
    "meeting",
    "tournament",
    "delegation",
    "wedding",
    "official hotel",
    "room block",
    "housing",
  ],
  fr: [
    "conférence",
    "hébergement",
    "congrès",
    "bloc de chambres",
    "réunion",
    "délégation",
    "mariage",
    "hôtel officiel",
  ],
  es: [
    "conferencia",
    "alojamiento",
    "congreso",
    "bloque de habitaciones",
    "reunión",
    "delegación",
    "boda",
    "hotel oficial",
  ],
  gl: ["congreso", "aloxamento", "hotel oficial", "reunión"],
  de: ["Konferenz", "Unterkunft", "Kongress", "Zimmerkontingent", "Tagung", "offizielles Hotel"],
});

/**
 * @returns {Array<{pivotId, pivotType, competitorHotelId, query, language, phoneVariant}>}
 */
export function buildCompSetPublicSearchPivots(competitor = {}, targetHotel = {}, opts = {}) {
  const maxPerComp = opts.maxPivotsPerCompetitor ?? 8;
  const langs = (targetHotel.languages || ["en"]).slice(0, 2);
  const rows = [];
  let n = 0;
  const push = (row) => {
    if (rows.length >= maxPerComp) return;
    n += 1;
    rows.push({
      pivotId: `${competitor.competitorHotelId}_p${String(n).padStart(2, "0")}`,
      competitorHotelId: competitor.competitorHotelId,
      canonicalName: competitor.canonicalName,
      targetHotelKey: targetHotel.hotelKey || targetHotel.targetHotelKey,
      ...row,
    });
  };

  const name = competitor.canonicalName;
  const otaExclude =
    "-site:booking.com -site:tripadvisor.com -site:expedia.com -site:hotels.com -site:trivago.com -site:kayak.com -site:traveloka.com -site:laterooms.com";

  // A. Exact hotel name — prefer third-party lodging-use language (not bare "conference")
  const highValueNameQueries = [
    `"${name}" ("official hotel" OR "host hotel" OR "hotel block" OR "room block")`,
    `"${name}" (accommodation OR housing OR hébergement OR alojamiento) (congress OR conference OR association OR congress)`,
    `"${name}" ("hotel block" OR "official hotel" OR "preferred hotel") (2024 OR 2025 OR 2026 OR 2027)`,
    `"${name}" (wedding OR incentive OR delegation OR tournament) (hotel OR accommodation)`,
  ];
  for (const q of highValueNameQueries) {
    push({
      pivotType: "EXACT_HOTEL_NAME",
      language: langs[0],
      query: `${q} ${otaExclude}`,
      phoneVariant: "",
    });
  }
  // Secondary language lodging terms
  for (const lang of langs.slice(1)) {
    const terms = EVENT_TERMS_BY_LANG[lang] || [];
    for (const term of terms.filter((t) => /hôtel officiel|hébergement|alojamiento|hotel oficial|bloc|bloque|Unterkunft|offizielles/i.test(t)).slice(0, 2)) {
      push({
        pivotType: "EXACT_HOTEL_NAME",
        language: lang,
        query: `"${name}" ${term} ${otaExclude}`,
        phoneVariant: "",
      });
    }
  }

  // B. Former names / aliases
  for (const alias of [...(competitor.formerNames || []), ...(competitor.knownAliases || [])].slice(0, 2)) {
    if (!alias || alias === name) continue;
    push({
      pivotType: "ALIAS_OR_FORMER_NAME",
      language: langs[0],
      query: `"${alias}" (conference OR congress OR "hotel block" OR accommodation OR meeting)`,
      phoneVariant: "",
    });
  }

  // C. Public phone variants
  for (const phone of (competitor.formattedPhoneVariants || []).slice(0, 3)) {
    push({
      pivotType: "PUBLIC_PHONE",
      language: langs[0],
      query: `"${phone}"`,
      phoneVariant: phone,
    });
    const phoneTerms =
      langs[0] === "fr"
        ? ["conférence", "hébergement", "congrès", "hôtel officiel"]
        : langs[0] === "es"
          ? ["congreso", "alojamiento", "hotel oficial", "reunión"]
          : ["conference", "accommodation", "congress", "hotel block", "meeting", "official hotel"];
    for (const term of phoneTerms.slice(0, 3)) {
      push({
        pivotType: "PUBLIC_PHONE_EVENT",
        language: langs[0],
        query: `"${phone}" ${term}`,
        phoneVariant: phone,
      });
    }
  }

  // D. Street address
  if (competitor.address) {
    push({
      pivotType: "STREET_ADDRESS",
      language: langs[0],
      query: `"${competitor.address}" (conference OR meeting OR congress OR wedding OR accommodation)`,
      phoneVariant: "",
    });
  }

  // E. Domain / booking URL
  if (competitor.domain) {
    push({
      pivotType: "DOMAIN_TRACE",
      language: "en",
      query: `"${competitor.domain}" (conference OR "hotel block" OR accommodation OR housing OR congress)`,
      phoneVariant: "",
    });
  }

  // F. Meeting space names
  for (const room of (competitor.meetingSpaceNames || []).slice(0, 2)) {
    push({
      pivotType: "MEETING_SPACE_NAME",
      language: langs[0],
      query: `"${room}" "${name}" (event OR conference OR wedding OR meeting)`,
      phoneVariant: "",
    });
  }

  // G. Brand + city
  const city = String(competitor.market || targetHotel.market || "").split(/[\/,]/)[0].trim();
  if (competitor.brand && city) {
    push({
      pivotType: "BRAND_CITY",
      language: langs[0],
      query: `"${competitor.brand}" ${city} (host hotel OR "official hotel" OR "room block" OR accommodation)`,
      phoneVariant: "",
    });
  }

  return rows.slice(0, maxPerComp);
}

/**
 * Select bounded high-value pivots for a controlled run.
 */
export function selectPriorityPivots(pivots = [], { max = 6 } = {}) {
  const rank = (p) => {
    let s = 0;
    if (p.pivotType === "PUBLIC_PHONE_EVENT") s += 100;
    if (p.pivotType === "PUBLIC_PHONE") s += 90;
    if (p.pivotType === "EXACT_HOTEL_NAME") s += 95;
    if (/official hotel|hotel block|host hotel|room block/i.test(p.query || "")) s += 30;
    if (p.pivotType === "DOMAIN_TRACE") s += 70;
    if (p.pivotType === "ALIAS_OR_FORMER_NAME") s += 65;
    if (p.pivotType === "STREET_ADDRESS") s += 60;
    if (p.pivotType === "MEETING_SPACE_NAME") s += 55;
    if (p.pivotType === "BRAND_CITY") s += 50;
    if (/hotel block|official hotel|hébergement|alojamiento|room block/i.test(p.query)) s += 15;
    return s;
  };
  return [...pivots].sort((a, b) => rank(b) - rank(a)).slice(0, max);
}
