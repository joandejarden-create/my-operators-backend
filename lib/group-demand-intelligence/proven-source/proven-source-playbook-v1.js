/**
 * GDI Proven-Source Replication V1 — playbook from forensic success families.
 * Provider-agnostic. No Webhound. No opportunity fabrication.
 */

export const PROVEN_SOURCE_FAMILY = Object.freeze({
  OFFICIAL_EVENT_PAGE: "OFFICIAL_EVENT_PAGE",
  HOUSING_PAGE: "HOUSING_PAGE",
  VENUE_PAGE: "VENUE_PAGE",
  SPORTS_PAGE: "SPORTS_PAGE",
  ORGANIZATION_SITE: "ORGANIZATION_SITE",
  GOVERNMENT_PAGE: "GOVERNMENT_PAGE",
  UNIVERSITY_PAGE: "UNIVERSITY_PAGE",
  ASSOCIATION_PAGE: "ASSOCIATION_PAGE",
});

/** Historical ready share from Success-Source Forensics V1 (n=63). */
export const HISTORICAL_READY_SHARE = Object.freeze({
  [PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE]: 18,
  [PROVEN_SOURCE_FAMILY.HOUSING_PAGE]: 10,
  [PROVEN_SOURCE_FAMILY.VENUE_PAGE]: 10,
  [PROVEN_SOURCE_FAMILY.SPORTS_PAGE]: 9,
  [PROVEN_SOURCE_FAMILY.ORGANIZATION_SITE]: 7,
  [PROVEN_SOURCE_FAMILY.GOVERNMENT_PAGE]: 5,
  [PROVEN_SOURCE_FAMILY.UNIVERSITY_PAGE]: 3,
  [PROVEN_SOURCE_FAMILY.ASSOCIATION_PAGE]: 1,
});

export const COMMERCIAL_STATUS = Object.freeze({
  OPEN_UNRESOLVED: "OPEN / UNRESOLVED",
  RFP_ACTIVE_SOURCING: "RFP / ACTIVE SOURCING",
  HOTEL_VENUE_TBD: "HOTEL / VENUE TBD",
  PARTIALLY_PLACED: "PARTIALLY PLACED",
  PRIMARY_OVERFLOW_POSSIBLE: "PRIMARY VENUE SELECTED / OVERFLOW POSSIBLE",
  PRIMARY_NO_OVERFLOW: "PRIMARY SELECTED / NO OVERFLOW EVIDENCE",
  FULLY_PLACED: "FULLY PLACED",
  CURRENT_CYCLE_CLOSED: "CURRENT CYCLE CLOSED",
  UNKNOWN: "UNKNOWN",
});

export const LODGING_EVIDENCE = Object.freeze({
  DIRECT: "DIRECT",
  STRONG_INFERENCE: "STRONG_INFERENCE",
  WEAK_INFERENCE: "WEAK_INFERENCE",
  NONE: "NONE",
});

const OPEN_STATUSES = new Set([
  COMMERCIAL_STATUS.OPEN_UNRESOLVED,
  COMMERCIAL_STATUS.RFP_ACTIVE_SOURCING,
  COMMERCIAL_STATUS.HOTEL_VENUE_TBD,
  COMMERCIAL_STATUS.PARTIALLY_PLACED,
  COMMERCIAL_STATUS.PRIMARY_OVERFLOW_POSSIBLE,
]);

export function isCommerciallyOpen(status) {
  return OPEN_STATUSES.has(status);
}

/** Lodging / TBD / overflow signal lexicon (EN + ES + GL). */
export const PROVEN_SIGNAL_PATTERNS = Object.freeze({
  directLodging: [
    /host\s*hotel/i,
    /official\s*hotel/i,
    /hotel\s*oficial/i,
    /room\s*block/i,
    /hotel\s*block/i,
    /group\s*rate/i,
    /housing\s*(bureau|page|information|open)/i,
    /accommodation\s*(page|information|details)/i,
    /alojamiento/i,
    /aloxamento/i,
    /reservas?\s*de\s*hotel/i,
    /team\s*hotel/i,
    /tournament\s*hotel/i,
    /delegation\s*hotel/i,
    /book(ing)?\s*link/i,
    /block\s*deadline/i,
    /overflow\s*(hotel|housing|lodging)/i,
    /alternate\s*hotel/i,
  ],
  tbdOpen: [
    /hotel\s*tbd/i,
    /venue\s*tbd/i,
    /destination\s*tbd/i,
    /accommodation\s*tbd/i,
    /sede\s*por\s*determinar/i,
    /hotel\s*por\s*determinar/i,
    /location\s*(tba|tbd|to\s*be\s*(announced|determined))/i,
    /site\s*selection/i,
    /hosting\s*bid/i,
    /future\s*host/i,
    /rfp/i,
    /request\s*for\s*proposal/i,
    /housing\s*open/i,
    /hotel\s*information\s*(forthcoming|coming\s*soon|to\s*follow)/i,
  ],
  overflow: [
    /overflow/i,
    /alternate\s*hotel/i,
    /additional\s*hotels?/i,
    /spillover/i,
    /otros?\s*hoteles/i,
  ],
  travelLodging: [
    /travel\s*(and|&)?\s*accommodation/i,
    /registration\s*.{0,40}hotel/i,
    /hotel\s*.{0,40}registration/i,
    /overnight\s*accommodation/i,
    /lodging/i,
    /hospedaje/i,
  ],
});

export function detectProvenSignals(text = "") {
  const t = String(text || "");
  const hits = [];
  for (const [kind, patterns] of Object.entries(PROVEN_SIGNAL_PATTERNS)) {
    for (const re of patterns) {
      if (re.test(t)) hits.push({ kind, match: re.source });
    }
  }
  return hits;
}

export function classifyLodgingEvidenceFromText(text = "") {
  const hits = detectProvenSignals(text);
  if (hits.some((h) => h.kind === "directLodging")) return LODGING_EVIDENCE.DIRECT;
  if (hits.some((h) => h.kind === "overflow" || h.kind === "travelLodging")) {
    return LODGING_EVIDENCE.STRONG_INFERENCE;
  }
  if (hits.some((h) => h.kind === "tbdOpen")) return LODGING_EVIDENCE.STRONG_INFERENCE;
  if (/participants?|attendees?|delegat|equipo|cohort|tournament|congress|congreso/i.test(text)) {
    return LODGING_EVIDENCE.WEAK_INFERENCE;
  }
  return LODGING_EVIDENCE.NONE;
}

export function classifyCommercialStatus(text = "") {
  const t = String(text || "");
  if (/fully\s*placed|sold\s*out|block\s*closed|registration\s*closed|evento\s*finalizado|already\s*took\s*place/i.test(t)) {
    return COMMERCIAL_STATUS.FULLY_PLACED;
  }
  if (/\b(201[0-9]|202[0-4])\b/.test(t) && !/\b202[6-9]\b/.test(t)) {
    return COMMERCIAL_STATUS.CURRENT_CYCLE_CLOSED;
  }
  if (/overflow|additional\s*hotels?|alternate\s*hotel/i.test(t) && /official\s*hotel|host\s*hotel|primary/i.test(t)) {
    return COMMERCIAL_STATUS.PRIMARY_OVERFLOW_POSSIBLE;
  }
  if (/hotel\s*tbd|venue\s*tbd|sede\s*por\s*determinar|hotel\s*por\s*determinar|location\s*tbd/i.test(t)) {
    return COMMERCIAL_STATUS.HOTEL_VENUE_TBD;
  }
  if (/rfp|site\s*selection|hosting\s*bid|request\s*for\s*proposal/i.test(t)) {
    return COMMERCIAL_STATUS.RFP_ACTIVE_SOURCING;
  }
  if (/housing\s*open|hotel\s*information\s*(forthcoming|coming)|accommodation\s*details\s*to\s*follow/i.test(t)) {
    return COMMERCIAL_STATUS.OPEN_UNRESOLVED;
  }
  if (/partially\s*placed|some\s*hotels?\s*confirmed/i.test(t)) {
    return COMMERCIAL_STATUS.PARTIALLY_PLACED;
  }
  if (/official\s*hotel|host\s*hotel|hotel\s*oficial/.test(t) && !/overflow|tbd|open/i.test(t)) {
    return COMMERCIAL_STATUS.PRIMARY_NO_OVERFLOW;
  }
  if (detectProvenSignals(t).length) return COMMERCIAL_STATUS.OPEN_UNRESOLVED;
  return COMMERCIAL_STATUS.UNKNOWN;
}

export function classifyProvenSourceFamily({ url = "", title = "", snippet = "", text = "" } = {}) {
  const blob = `${url} ${title} ${snippet} ${String(text).slice(0, 2000)}`.toLowerCase();
  if (/hous(ing|e)|hotel.?block|room.?block|accommodation|alojamiento|aloxamento|travel.?hotel/.test(blob)) {
    return PROVEN_SOURCE_FAMILY.HOUSING_PAGE;
  }
  if (/tournament|regatta|championship|fixture|team.?hotel|sports|deport/.test(blob)) {
    return PROVEN_SOURCE_FAMILY.SPORTS_PAGE;
  }
  if (/venue|arena|convention.?center|palexco|expocoruña|stadium|recinto/.test(blob)) {
    return PROVEN_SOURCE_FAMILY.VENUE_PAGE;
  }
  if (/\.edu|universidad|university|commencement|campus/.test(blob)) {
    return PROVEN_SOURCE_FAMILY.UNIVERSITY_PAGE;
  }
  if (/gov\.|gob\.|municip|xunta|conseller|ministerio|federal|regional/.test(blob)) {
    return PROVEN_SOURCE_FAMILY.GOVERNMENT_PAGE;
  }
  if (/association|asociaci|society|federation|federaci|chamber|c[aá]mara/.test(blob)) {
    return PROVEN_SOURCE_FAMILY.ASSOCIATION_PAGE;
  }
  if (/\/events?\/|conference|congress|congreso|summit|forum|symposium|annual.?meeting/.test(blob)) {
    return PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE;
  }
  if (/^https?:/.test(url)) return PROVEN_SOURCE_FAMILY.ORGANIZATION_SITE;
  return PROVEN_SOURCE_FAMILY.ORGANIZATION_SITE;
}

/**
 * Build structured queries for proven source families (motion/structure, not generic travel).
 */
export function buildProvenSourceQueries(hotelId, cfg = {}, { max = 40 } = {}) {
  const isAc = hotelId === "rec2PVBDavppGpenm";
  const y1 = 2027;
  const y2 = 2028;
  const city = isAc ? "A Coruña" : "Grenada";
  const region = isAc ? "Galicia" : "Caribbean";

  const queries = [];

  const push = (family, query) => {
    if (queries.length >= max) return;
    queries.push({ family, query, queryId: `${family}_${queries.length}` });
  };

  if (isAc) {
    push(PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE, `"${city}" ${y1} (congreso OR conference OR symposium) (hotel OR alojamiento OR accommodation)`);
    push(PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE, `"${city}" ${y2} (congreso OR conferencia) (hotel OR alojamiento)`);
    push(PROVEN_SOURCE_FAMILY.HOUSING_PAGE, `"${city}" ("host hotel" OR "hotel oficial" OR "room block" OR "hotel block") ${y1} OR ${y2}`);
    push(PROVEN_SOURCE_FAMILY.HOUSING_PAGE, `"${city}" ("hotel TBD" OR "venue TBD" OR "sede por determinar" OR "alojamiento") ${y1}`);
    push(PROVEN_SOURCE_FAMILY.VENUE_PAGE, `Palexco OR Expocoruña ${y1} OR ${y2} (hotel OR alojamiento OR overflow OR "hoteles")`);
    push(PROVEN_SOURCE_FAMILY.SPORTS_PAGE, `"${city}" OR ${region} (torneo OR championship OR regata) ${y1} (hotel OR alojamiento OR "team hotel")`);
    push(PROVEN_SOURCE_FAMILY.ASSOCIATION_PAGE, `${region} (asociación OR association) (congreso OR reunión) ${y1} (hotel OR alojamiento)`);
    push(PROVEN_SOURCE_FAMILY.UNIVERSITY_PAGE, `universidad "${city}" OR UDC OR USC (congreso OR symposium) ${y1} (alojamiento OR hotel)`);
    push(PROVEN_SOURCE_FAMILY.GOVERNMENT_PAGE, `(Xunta OR "A Coruña") ${y1} (evento OR congreso) (alojamiento OR hotel)`);
    push(PROVEN_SOURCE_FAMILY.ORGANIZATION_SITE, `"${city}" ${y1} "group rate" OR "reservas hotel" congreso`);
    push(PROVEN_SOURCE_FAMILY.HOUSING_PAGE, `site:.es "${city}" (alojamiento OR hospedaje) (congreso OR torneo) ${y1}`);
    push(PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE, `site:.gal "${city}" (aloxamento OR hotel) (congreso OR evento) ${y1}`);
    push(PROVEN_SOURCE_FAMILY.SPORTS_PAGE, `"${city}" (deportivo OR tournament) ${y2} (hotel OR pernocta OR alojamiento)`);
    push(PROVEN_SOURCE_FAMILY.HOUSING_PAGE, `"${city}" overflow hotel OR "hoteles alternativos" ${y1}`);
    push(PROVEN_SOURCE_FAMILY.ASSOCIATION_PAGE, `Galicia medical OR científico congreso ${y1} hotel alojamiento`);
  } else {
    push(PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE, `"Grenada" ${y1} (conference OR meeting OR summit) (accommodation OR hotel OR "host hotel")`);
    push(PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE, `"Grenada" ${y2} (conference OR congress) (hotel OR accommodation)`);
    push(PROVEN_SOURCE_FAMILY.HOUSING_PAGE, `"Grenada" ("host hotel" OR "official hotel" OR "room block" OR "group rate") ${y1} OR ${y2}`);
    push(PROVEN_SOURCE_FAMILY.HOUSING_PAGE, `"Grenada" ("hotel TBD" OR "venue TBD" OR "housing open" OR accommodation) ${y1}`);
    push(PROVEN_SOURCE_FAMILY.VENUE_PAGE, `"Grenada" (venue OR "convention" OR "Spice Island" OR "Grand Anse") ${y1} hotel accommodation`);
    push(PROVEN_SOURCE_FAMILY.SPORTS_PAGE, `"Grenada" (regatta OR tournament OR championship) ${y1} (accommodation OR hotel OR "team hotel")`);
    push(PROVEN_SOURCE_FAMILY.ASSOCIATION_PAGE, `"Grenada" association (meeting OR conference) ${y1} accommodation hotel`);
    push(PROVEN_SOURCE_FAMILY.UNIVERSITY_PAGE, `"Grenada" (university OR "St. George's University" OR SGU) ${y1} (conference OR housing OR accommodation)`);
    push(PROVEN_SOURCE_FAMILY.GOVERNMENT_PAGE, `"Grenada" (government OR ministerial OR CARICOM) ${y1} meeting accommodation`);
    push(PROVEN_SOURCE_FAMILY.ORGANIZATION_SITE, `"Grenada" incentive OR "travel trade" ${y1} (hotel OR accommodation OR "group rate")`);
    push(PROVEN_SOURCE_FAMILY.SPORTS_PAGE, `"Grenada" sailing OR yacht OR regatta ${y2} accommodation hotel`);
    push(PROVEN_SOURCE_FAMILY.HOUSING_PAGE, `"Grenada" overflow hotel OR "alternate hotel" OR "room reservations" ${y1}`);
    push(PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE, `Caribbean "${city}" ${y1} "official hotel" conference`);
    push(PROVEN_SOURCE_FAMILY.ASSOCIATION_PAGE, `"Grenada" (regional meeting OR chapter meeting) ${y1} hotel`);
    push(PROVEN_SOURCE_FAMILY.HOUSING_PAGE, `"Grenada" wedding OR incentive "group rate" OR "room block" ${y1} -tourism -visit`);
  }

  return queries.slice(0, max);
}

/** Jev/default next-source routing for proven families — context-aware, not HOUSING_PDF-only. */
export function defaultProvenSourcePriority({
  family = null,
  sourcesChecked = [],
  lodgingGap = true,
} = {}) {
  const checked = new Set(sourcesChecked);
  const f = String(family || "");
  const prefer = [];

  if (/SPORTS/i.test(f)) {
    prefer.push(
      PROVEN_SOURCE_FAMILY.SPORTS_PAGE,
      PROVEN_SOURCE_FAMILY.HOUSING_PAGE,
      PROVEN_SOURCE_FAMILY.VENUE_PAGE,
      PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE
    );
  } else if (/UNIVERSITY|GOVERNMENT/i.test(f)) {
    prefer.push(
      PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE,
      PROVEN_SOURCE_FAMILY.HOUSING_PAGE,
      f.includes("UNIVERSITY")
        ? PROVEN_SOURCE_FAMILY.UNIVERSITY_PAGE
        : PROVEN_SOURCE_FAMILY.GOVERNMENT_PAGE
    );
  } else if (/ASSOCIATION/i.test(f)) {
    prefer.push(
      PROVEN_SOURCE_FAMILY.ASSOCIATION_PAGE,
      PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE,
      PROVEN_SOURCE_FAMILY.HOUSING_PAGE
    );
  } else if (/VENUE/i.test(f)) {
    prefer.push(
      PROVEN_SOURCE_FAMILY.VENUE_PAGE,
      PROVEN_SOURCE_FAMILY.HOUSING_PAGE,
      PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE
    );
  } else if (/HOUSING/i.test(f)) {
    prefer.push(
      PROVEN_SOURCE_FAMILY.HOUSING_PAGE,
      PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE,
      PROVEN_SOURCE_FAMILY.VENUE_PAGE
    );
  } else {
    prefer.push(
      PROVEN_SOURCE_FAMILY.OFFICIAL_EVENT_PAGE,
      PROVEN_SOURCE_FAMILY.HOUSING_PAGE,
      PROVEN_SOURCE_FAMILY.VENUE_PAGE,
      PROVEN_SOURCE_FAMILY.SPORTS_PAGE,
      PROVEN_SOURCE_FAMILY.ORGANIZATION_SITE
    );
  }

  if (lodgingGap && !checked.has(PROVEN_SOURCE_FAMILY.HOUSING_PAGE)) {
    prefer.unshift(PROVEN_SOURCE_FAMILY.HOUSING_PAGE);
  }

  for (const p of prefer) {
    if (!checked.has(p)) return p;
  }
  return "STOP";
}

/** Chain link keywords to follow from a fetched page. */
export const CHAIN_LINK_HINTS = [
  /accommodation/i,
  /alojamiento/i,
  /aloxamento/i,
  /hotel/i,
  /housing/i,
  /travel/i,
  /venue/i,
  /registration/i,
  / hospedaje/i,
  /reserv/i,
  /overflow/i,
  /delegat/i,
  /team/i,
  /future/i,
  /bid/i,
  /host/i,
  /\.pdf$/i,
];

export function extractChainUrls(html = "", baseUrl = "", { max = 8 } = {}) {
  const urls = [];
  const re = /href=["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(html)) && urls.length < max * 3) {
    let href = m[1];
    if (!href || href.startsWith("#") || href.startsWith("mailto:")) continue;
    try {
      const abs = new URL(href, baseUrl).toString();
      if (!/^https?:/i.test(abs)) continue;
      if (CHAIN_LINK_HINTS.some((h) => h.test(abs) || h.test(href))) {
        urls.push(abs);
      }
    } catch {
      /* ignore bad urls */
    }
  }
  return [...new Set(urls)].slice(0, max);
}

export function evaluateNativeSuccessGate({
  readyCount = 0,
  openTbdLodgingSupported = 0,
  structuresFound = {},
} = {}) {
  const a = readyCount >= 2;
  const b = openTbdLodgingSupported >= 5;
  const structureHits = [
    structuresFound.OFFICIAL_EVENT_PAGE || 0,
    structuresFound.HOUSING_PAGE || 0,
    structuresFound.VENUE_PAGE || 0,
    structuresFound.SPORTS_PAGE || 0,
  ].filter((n) => n > 0).length;
  const openWithLodging =
    (structuresFound.openTbdWithLodging || 0) >= 2 && structureHits >= 2;
  const c = openWithLodging;

  return {
    pass: a || b || c,
    criteria: { A_ready2: a, B_openTbd5: b, C_structures: c },
    structureHits,
  };
}
