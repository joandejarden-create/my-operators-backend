/**
 * Hotel-name ownership search queries (Stage C baseline).
 * @param {object} hotel
 * @param {object} [opts]
 */
export function buildOwnershipQueries(hotel, opts = {}) {
  const name = String(hotel.hotel_name || hotel.name || hotel.official_name || "").trim();
  const city = String(hotel.city || "").trim();
  const country = String(hotel.country || "").trim();
  const loc = [city, country].filter(Boolean).join(" ");
  const base = loc ? `"${name}" ${loc}` : `"${name}"`;
  const lang = String(opts.language_hint || inferLang(country)).toLowerCase();

  const en = [
    `${base} owner`,
    `${base} owned by`,
    `${base} acquisition`,
    `${base} developer`,
  ];
  const es = [
    `${base} propietario`,
    `${base} propiedad de`,
    `${base} adquirió`,
    `${base} desarrollador`,
  ];
  const pt = [
    `${base} proprietário`,
    `${base} propriedade de`,
    `${base} pertence a`,
    `${base} aquisição`,
    `${base} incorporadora`,
    `${base} operado por`,
  ];

  let pool = en;
  if (lang === "es") pool = [...es, ...en];
  else if (lang === "pt") pool = [...pt, ...en, ...es.slice(0, 2)];
  else pool = [...en, ...es];

  const max = Math.min(5, Math.max(1, Number(opts.maxQueries || 3)));
  return pool.slice(0, max);
}

/**
 * Entity-driven ownership search queries (P1.5).
 * Uses registered/operating company from country stage as research seed.
 */

function inferLang(country) {
  const c = String(country || "").toLowerCase();
  if (/(brazil|brasil)/.test(c)) return "pt";
  if (
    /(mexico|spain|colombia|peru|chile|argentina|ecuador|guatemala|panama|costa rica|dominican)/.test(
      c
    )
  ) {
    return "es";
  }
  return "en";
}

/**
 * @param {object} hotel
 * @param {object} registeredEntity — { legal_name, identifiers?, jurisdiction? }
 * @param {{ maxQueries?: number }} [opts]
 */
export function buildEntityDrivenOwnershipQueries(hotel, registeredEntity, opts = {}) {
  const company = String(registeredEntity?.legal_name || "").trim();
  if (!company) return [];

  const country = hotel?.country || "";
  const lang = inferLang(country);
  const quoted = `"${company}"`;
  const hotelName = String(hotel?.hotel_name || hotel?.name || "").trim();
  const city = String(hotel?.city || "").trim();

  const en = [
    `${quoted} hotel acquisition`,
    `${quoted} parent company`,
    `${quoted} hospitality investment`,
    `${quoted} portfolio hotel`,
    `${quoted} owner`,
    hotelName ? `"${hotelName}" ${quoted}` : null,
  ].filter(Boolean);

  const es = [
    `${quoted} adquisición hotel`,
    `${quoted} empresa matriz`,
    `${quoted} inversión hotelera`,
    `${quoted} portafolio hotelero`,
    `${quoted} propietario`,
    `${quoted} grupo hotelero`,
    hotelName ? `"${hotelName}" ${quoted}` : null,
  ].filter(Boolean);

  const pt = [
    `${quoted} aquisição hotel`,
    `${quoted} empresa controladora`,
    `${quoted} investimento hoteleiro`,
    `${quoted} portfólio hotel`,
    `${quoted} proprietário`,
    `${quoted} grupo hoteleiro`,
    hotelName ? `"${hotelName}" ${quoted}` : null,
  ].filter(Boolean);

  let pool = en;
  if (lang === "es") pool = [...es, ...en.slice(0, 2)];
  else if (lang === "pt") pool = [...pt, ...en.slice(0, 2)];

  const max = Math.min(6, Math.max(1, Number(opts.maxQueries || 4)));
  return pool.slice(0, max);
}

/**
 * Hotel-name economic owner queries (supplement entity-driven).
 */
export function buildEconomicOwnerQueries(hotel, opts = {}) {
  const name = String(hotel?.hotel_name || hotel?.name || "").trim();
  const city = String(hotel?.city || "").trim();
  const country = String(hotel?.country || "").trim();
  const loc = [city, country].filter(Boolean).join(" ");
  const base = loc ? `"${name}" ${loc}` : `"${name}"`;
  const lang = inferLang(country);

  const en = [
    `${base} acquisition`,
    `${base} investment group`,
    `${base} portfolio owner`,
    `${base} sponsored by`,
  ];
  const es = [
    `${base} adquisición`,
    `${base} grupo inversor`,
    `${base} portafolio`,
    `${base} respaldado por`,
  ];
  const pt = [
    `${base} aquisição`,
    `${base} grupo investidor`,
    `${base} portfólio`,
    `${base} patrocinado por`,
  ];

  let pool = lang === "pt" ? [...pt, ...en.slice(0, 2)] : lang === "es" ? [...es, ...en.slice(0, 2)] : en;
  const max = Math.min(4, Number(opts.maxQueries || 2));
  return pool.slice(0, max);
}
