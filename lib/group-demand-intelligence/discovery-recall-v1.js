/**
 * GDI Discovery Recall V1 — hotel demand territories + query families.
 * Search hypotheses only — not automatic qualification.
 */

export const DISCOVERY_RECALL_V1 = "gdi_discovery_recall_v1";

export const HOTEL_RECALL_PROFILES = Object.freeze({
  recGkME49yYuxQl0u: {
    slot: "A",
    slug: "now-now-noho",
    hotelId: "recGkME49yYuxQl0u",
    name: "NOW NOW NOHO",
    demandHypothesis: [
      "small meetings / boutique buyouts",
      "creative / media / industry events",
      "association chapter meetings",
      "nearby conference overflow / citywide compression",
      "university / NYU-adjacent academic",
      "social / entertainment / SMERF",
    ],
    geoNodes: {
      primary: ["NoHo", "SoHo", "Greenwich Village", "Lower Manhattan"],
      secondary: ["Tribeca", "East Village", "Hudson Square", "Financial District"],
      feeder: ["Midtown Manhattan", "Brooklyn", "NYC"],
      districts: ["SoHo", "NYU", "Washington Square", "Broadway downtown"],
      corporateClusters: ["Hudson Square media", "Silicon Alley", "FiDi finance"],
      medicalUniversityGov: ["NYU", "Cooper Union", "downtown hospitals"],
      sportsVenues: ["Madison Square Garden overflow", "Brooklyn sports"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  recIwaP1etgx2g9nA: {
    slot: "B",
    slug: "cambridge-beaches",
    hotelId: "recIwaP1etgx2g9nA",
    name: "Cambridge Beaches Resort & Spa",
    demandHypothesis: [
      "executive / corporate retreats",
      "incentive travel",
      "destination weddings / social",
      "insurance / reinsurance / finance offshore",
      "small association meetings",
      "wellness / spa programs",
    ],
    geoNodes: {
      primary: ["Bermuda", "Somerset", "Sandys Parish", "West End Bermuda"],
      secondary: ["Hamilton Bermuda", "Dockyard Bermuda"],
      feeder: ["New York Bermuda", "Boston Bermuda", "East Coast Bermuda"],
      districts: ["Mangrove Bay", "Royal Naval Dockyard"],
      corporateClusters: ["Bermuda insurance", "reinsurance Hamilton", "captive insurance"],
      medicalUniversityGov: [],
      sportsVenues: ["Butterfield Bermuda Championship", "Cup Match"],
      resortNodes: ["Bermuda destination wedding", "Bermuda incentive"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  recsn3BUKJ9PNfeZW: {
    slot: "C",
    slug: "jw-monterrey",
    hotelId: "recsn3BUKJ9PNfeZW",
    name: "JW Marriott Hotel Monterrey Valle",
    demandHypothesis: [
      "corporate / industrial / manufacturing meetings",
      "automotive / finance / medical",
      "regional LATAM / US-MX business",
      "conventions / association",
      "executive events",
      "university / sports when fit",
    ],
    geoNodes: {
      primary: [
        "Monterrey Valle",
        "San Pedro Garza García",
        "Valle Oriente",
        "Monterrey",
      ],
      secondary: ["Nuevo León", "Santa Catarina", "San Nicolás"],
      feeder: ["Saltillo", "Northern Mexico", "Texas Mexico corridor"],
      districts: ["Valle del Campestre", "San Pedro business district"],
      corporateClusters: [
        "Monterrey industrial",
        "automotive Nuevo León",
        "manufacturing Monterrey",
        "finance San Pedro",
      ],
      medicalUniversityGov: ["ITESM", "UANL", "medical Monterrey"],
      sportsVenues: ["Estadio BBVA", "Monterrey sports events"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  // Controlled Expansion Wave 1
  recRXmrakhSAuctwz: {
    slot: "A",
    slug: "st-regis-mexico-city",
    hotelId: "recRXmrakhSAuctwz",
    name: "The St. Regis Mexico City",
    demandHypothesis: [
      "corporate / finance / embassies meetings",
      "luxury association / industry conferences",
      "executive events / board retreats",
      "medical / professional society",
      "citywide overflow when Reforma housing fits",
      "cultural / gala / SMERF when luxury fit",
    ],
    geoNodes: {
      primary: ["Mexico City Reforma", "Paseo de la Reforma", "Juárez CDMX", "Cuauhtémoc"],
      secondary: ["Polanco", "Zona Rosa", "Roma Norte", "Condesa"],
      feeder: ["Toluca", "Puebla", "Querétaro"],
      districts: ["Reforma", "Centro Histórico", "Polanco"],
      corporateClusters: ["Reforma finance", "Polanco corporate", "CDMX embassies"],
      medicalUniversityGov: ["UNAM", "IPN", "medical Mexico City"],
      sportsVenues: ["Estadio Azteca overflow", "CDMX sports"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  recN76iEE6yAaPh8H: {
    slot: "B",
    slug: "st-regis-cap-cana",
    hotelId: "recN76iEE6yAaPh8H",
    name: "The St. Regis Cap Cana Resort",
    demandHypothesis: [
      "incentive travel / executive retreats",
      "destination weddings / social",
      "luxury association meetings",
      "corporate offsites",
      "wellness / spa programs",
      "Caribbean feeder corporate",
    ],
    geoNodes: {
      primary: ["Cap Cana", "Punta Cana Cap Cana"],
      secondary: ["Punta Cana", "Bávaro", "La Altagracia"],
      feeder: ["Santo Domingo Cap Cana", "US East Coast Dominican Republic", "Miami Dominican Republic"],
      districts: ["Cap Cana marina", "Juanillo"],
      corporateClusters: ["Dominican incentive", "Caribbean luxury meetings"],
      medicalUniversityGov: [],
      sportsVenues: ["Cap Cana golf", "Punta Cana sports"],
      resortNodes: ["Cap Cana destination wedding", "Punta Cana incentive"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  rec8hHupaSwiWI3r7: {
    slot: "C",
    slug: "hotel-phillips-kansas-city",
    hotelId: "rec8hHupaSwiWI3r7",
    name: "Hotel Phillips Kansas City, Curio Collection by Hilton",
    demandHypothesis: [
      "downtown association / chapter meetings",
      "corporate / regional business",
      "medical / university adjacent",
      "mid-size conferences",
      "social / SMERF / Power & Light",
      "sports / event overflow",
    ],
    geoNodes: {
      primary: [
        "Downtown Kansas City",
        "Power and Light District",
        "Kansas City MO downtown",
      ],
      secondary: ["Crossroads Kansas City", "Country Club Plaza", "Midtown Kansas City"],
      feeder: ["Overland Park", "Lawrence Kansas", "Kansas City metro"],
      districts: ["Power & Light", "12th Street", "Crossroads Arts"],
      corporateClusters: ["Downtown KC corporate", "Crown Center", "Plaza business"],
      medicalUniversityGov: ["UMKC", "University of Kansas Medical", "Children's Mercy"],
      sportsVenues: ["T-Mobile Center", "Kauffman Stadium overflow", "Arrowhead overflow"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  // Controlled Expansion Wave 2
  recESHsNsWUFYZrxR: {
    slot: "A",
    slug: "jw-marriott-santo-domingo",
    hotelId: "recESHsNsWUFYZrxR",
    name: "JW Marriott Hotel Santo Domingo",
    demandHypothesis: [
      "corporate / finance / embassy meetings",
      "association / industry conferences",
      "executive events / board retreats",
      "medical / professional society",
      "citywide overflow when Piantini housing fits",
      "gala / SMERF when luxury fit",
    ],
    geoNodes: {
      primary: ["Santo Domingo Piantini", "Piantini", "Blue Mall Santo Domingo"],
      secondary: ["Naco Santo Domingo", "Tiradentes", "Bella Vista Santo Domingo"],
      feeder: ["Santiago Dominican Republic", "Punta Cana Santo Domingo feeder"],
      districts: ["Piantini", "Distrito Nacional", "Winston Churchill"],
      corporateClusters: ["Piantini corporate", "Santo Domingo finance", "Blue Mall business"],
      medicalUniversityGov: ["UASD", "INTEC", "medical Santo Domingo"],
      sportsVenues: ["Estadio Quisqueya overflow", "Santo Domingo sports"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  recCEpdskZeUBvQwG: {
    slot: "B",
    slug: "hotel-caribe-cartagena",
    hotelId: "recCEpdskZeUBvQwG",
    name: "Hotel Caribe by Faranda Grand, Cartagena",
    demandHypothesis: [
      "incentive travel / executive retreats",
      "destination weddings / social",
      "association meetings Cartagena",
      "corporate offsites",
      "Caribbean Colombia feeder corporate",
      "trade / tourism events Bocagrande",
    ],
    geoNodes: {
      primary: ["Cartagena Bocagrande", "Bocagrande Cartagena"],
      secondary: ["Cartagena de Indias", "Castillo Grande", "El Laguito"],
      feeder: ["Barranquilla Cartagena", "Bogotá Cartagena incentive", "US East Coast Cartagena"],
      districts: ["Bocagrande", "Centro Histórico Cartagena"],
      corporateClusters: ["Cartagena incentive", "Colombia Caribbean meetings"],
      medicalUniversityGov: [],
      sportsVenues: ["Cartagena sports", "Bocagrande events"],
      resortNodes: ["Cartagena destination wedding", "Cartagena incentive"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  recD17Kxn6BcJjGFh: {
    slot: "C",
    slug: "westin-monterrey-valle",
    hotelId: "recD17Kxn6BcJjGFh",
    name: "The Westin Monterrey Valle",
    demandHypothesis: [
      "corporate / Valle Oriente business meetings",
      "association / regional conferences",
      "executive wellness / offsites",
      "medical / university adjacent",
      "small-to-mid meetings at Punto Valle",
      "Monterrey metro overflow when Valle housing fits",
    ],
    geoNodes: {
      primary: ["Monterrey Valle Oriente", "San Pedro Garza García", "Punto Valle"],
      secondary: ["Santa Catarina NL", "San Nicolás Monterrey", "Cumbres Monterrey"],
      feeder: ["Saltillo Monterrey", "Texas Mexico corridor Monterrey"],
      districts: ["Valle Oriente", "San Pedro", "Punto Valle"],
      corporateClusters: ["Valle Oriente corporate", "San Pedro business", "Monterrey finance"],
      medicalUniversityGov: ["TEC de Monterrey", "UANL", "medical Monterrey"],
      sportsVenues: ["Estadio BBVA overflow", "Monterrey sports"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  // Pilot hotel — DMV demand hypotheses from hotel config (data, not delta rules)
  recLuxvwwxID7U2B8: {
    slot: "PILOT",
    slug: "bethesda-marriott",
    hotelId: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    demandHypothesis: [
      "NIH / medical / scientific association meetings",
      "federal health / government contractor meetings",
      "Montgomery County / Bethesda corporate",
      "DMV association conferences with open hotel sourcing",
      "university / alumni / weekend sports housing",
      "Walter Reed / North Bethesda corridor meetings",
    ],
    geoNodes: {
      primary: ["Bethesda", "North Bethesda", "Rockville", "Montgomery County MD"],
      secondary: ["Silver Spring", "Chevy Chase", "Gaithersburg", "College Park"],
      feeder: ["Washington DC", "Northern Virginia", "DMV", "Arlington", "Alexandria"],
      districts: ["NIH campus", "Walter Reed", "Bethesda Row", "White Flint"],
      corporateClusters: [
        "Marriott HQ adjacent corporate",
        "government contractor Bethesda",
        "biotech Rockville",
        "federal health research",
      ],
      medicalUniversityGov: ["NIH", "Walter Reed", "Uniformed Services University", "FDA White Oak"],
      sportsVenues: ["Bethesda Premier Cup", "Maryland sports weekend"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
});

const VERTICAL_FAMILIES = [
  {
    id: "association_conference",
    hints: [
      "association annual meeting",
      "professional conference",
      "chapter conference",
      "society annual meeting",
    ],
  },
  {
    id: "corporate_organizational",
    hints: [
      "corporate summit",
      "executive retreat",
      "leadership meeting",
      "sales kickoff",
      "offsite meeting",
    ],
  },
  {
    id: "medical",
    hints: ["medical conference", "healthcare summit", "clinical meeting"],
  },
  {
    id: "university",
    hints: ["university conference", "academic symposium", "alumni weekend"],
  },
  {
    id: "government",
    hints: ["government conference", "public sector meeting", "municipal summit"],
  },
  {
    id: "sports_tournament",
    hints: [
      "sports tournament hotel block",
      "championship housing",
      "tournament lodging",
    ],
  },
  {
    id: "trade_show",
    hints: ["trade show", "expo exhibitors hotel", "industry exposition"],
  },
  {
    id: "incentive_retreat",
    hints: ["incentive travel", "corporate incentive", "destination retreat"],
  },
  {
    id: "smerf_social",
    hints: ["wedding block hotel", "reunion hotel block", "social group lodging"],
  },
  {
    id: "housing_overflow",
    hints: [
      "official hotel block",
      "overflow housing",
      "housing bureau",
      "Cvent hotel",
      "room block RFP",
      "venue TBD hotel",
    ],
  },
];

/**
 * Build expanded Serp query tasks for discovery recall V1.
 */
export function buildRecallV1SearchTasks(hotelId, contract = {}) {
  const profile = HOTEL_RECALL_PROFILES[hotelId];
  if (!profile) {
    throw new Error(`no_recall_profile_${hotelId}`);
  }

  const yearNear = `(${profile.yearPriority.join(" OR ")})`;
  const yearAll = `(${[...profile.yearPriority, ...profile.yearAllow].join(" OR ")})`;
  const geos = [
    ...profile.geoNodes.primary,
    ...profile.geoNodes.secondary.slice(0, 3),
    ...(profile.geoNodes.corporateClusters || []).slice(0, 2),
    ...(profile.geoNodes.resortNodes || []).slice(0, 2),
  ].filter(Boolean);

  const tasks = [];
  for (const vertical of VERTICAL_FAMILIES) {
    const queries = [];
    for (const geo of geos.slice(0, 4)) {
      for (const hint of vertical.hints.slice(0, 2)) {
        queries.push(`"${geo}" ${hint} ${yearNear}`);
      }
    }
    // Sourcing / open venue signals
    queries.push(
      `"${geos[0]}" (${vertical.hints[0]}) ("venue TBD" OR "hotel TBD" OR RFP OR "room block" OR housing) ${yearNear}`
    );
    tasks.push({
      vertical: vertical.id,
      note: vertical.id,
      queries: [...new Set(queries)].slice(0, 6),
    });
  }

  // Feeder / access queries (near-term only)
  for (const geo of (profile.geoNodes.feeder || []).slice(0, 2)) {
    tasks.push({
      vertical: "housing_overflow",
      note: "feeder_geo",
      queries: [
        `"${geo}" conference (hotel OR lodging OR "room block") ${yearNear}`,
        `"${geo}" (summit OR meeting) ("official hotel" OR overflow) ${yearNear}`,
      ],
    });
  }

  // Explicit open-sourcing family
  tasks.push({
    vertical: "housing_overflow",
    note: "open_sourcing",
    queries: [
      `"${profile.geoNodes.primary[0]}" ("RFP" OR "request for proposal") hotel ${yearNear}`,
      `"${profile.geoNodes.primary[0]}" ("save the date" OR "destination announced") conference ${yearAll}`,
      `"${profile.geoNodes.primary[0]}" (housing OR "hotel block") (2026 OR 2027) (open OR available OR TBD)`,
    ],
  });

  return {
    version: DISCOVERY_RECALL_V1,
    profile,
    contractHints: {
      market: contract.market,
      rooms: contract.rooms,
      peakBand: [contract.peakRoomsMin, contract.peakRoomsMax],
    },
    tasks,
    completenessChecklist: {
      sourceFamilies: [
        "OFFICIAL_EVENT",
        "ASSOCIATION_CALENDAR",
        "CONFERENCE_CALENDAR",
        "SPORTS_BODY",
        "UNIVERSITY",
        "MEDICAL",
        "GOVERNMENT",
        "CORPORATE",
        "HOUSING_BUREAU",
        "TRADE_SHOW",
        "RFP_HOUSING_DOC",
      ],
      eventTypes: VERTICAL_FAMILIES.map((v) => v.id),
      geographicZones: Object.keys(profile.geoNodes),
    },
  };
}

export function renderRecallV1Brief(hotelId, contract = {}) {
  const profile = HOTEL_RECALL_PROFILES[hotelId];
  return [
    "DEALALITY GDI DISCOVERY RECALL V1",
    `Hotel: ${profile.name} (${hotelId})`,
    `Market: ${contract.market || profile.geoNodes.primary[0]}`,
    `Rooms: ${contract.rooms ?? "n/a"} | Peak band: ${contract.peakRoomsMin ?? "?"}-${contract.peakRoomsMax ?? "?"}`,
    "Demand hypotheses (search only — not auto-qualify):",
    ...profile.demandHypothesis.map((h) => `- ${h}`),
    "Primary geo nodes:",
    profile.geoNodes.primary.join(", "),
    "Rules:",
    "- Prefer near-term (0–24 months) over distant future filler.",
    "- Prefer events with venue TBD, hotel TBD, RFP, housing open, or overflow signals.",
    "- Prefer first-party / official / association / sports governing sources.",
    "- Separate attendance from room demand; never invent room blocks.",
    "- Do not invent events. Empty is better than fabricated.",
    "- Label opportunityType when evidenced: PRIMARY_PURSUIT | OVERFLOW_HOUSING | FUTURE_CYCLE | REACTIVATION.",
    "- Capture venueStatus / sourcingStatus / roomDemandEvidence explicitly.",
  ].join("\n");
}

export const RECALL_EXTRACT_SYSTEM = `You are Dealality Native group-demand discovery extraction (Recall V1).
Extract ONLY concrete group-demand opportunities supported by the evidence.
Prioritize commercially useful signals:
- venue TBD / hotel TBD / city selected venue open
- RFP active / housing open / room block not finalized
- overflow / multi-hotel / housing bureau
- near-term dates (next 24 months) over distant future
Rules:
- Prefer first-party / official association, organizer, tournament, institutional sources.
- Do not invent events.
- Separate attendance from room demand; only set peakRoomEstimate when evidenced or clearly estimated (claimKinds).
- Return JSON: { "candidates": [ {...} ] }
Candidate fields: eventName, organization, opportunityType (PRIMARY_PURSUIT|OVERFLOW_HOUSING|FUTURE_CYCLE|REACTIVATION), startDate, endDate, location, venue, venueStatus, sourcingStatus, attendance, peakRoomEstimate, roomDemandEvidence, housingEvidence, officialSource, supportingSources ([{url,title}]), geographyEvidence, futureCycleEvidence, whyRelevantToHotel, whyNow, evidenceConfidence (0-100), claimKinds, demandTerritoryFit, vertical, segment.`;
