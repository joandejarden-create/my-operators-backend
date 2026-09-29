/**
 * GDI Native blind discovery contract.
 * Blind = hotel identity + market territory + capability only.
 * No Webhound seeds. No demand-type targetSegments seeding.
 */

export const GDI_DISCOVERY_CONTRACT_VERSION = "gdi_discovery_contract_v1_1_resort_lanes";

const CORE_VERTICALS = Object.freeze([
  {
    id: "association_conference",
    label: "Association / conference",
    queryHints: [
      "association conference",
      "annual meeting hotel",
      "chapter summit",
      "professional conference 2026 OR 2027",
    ],
  },
  {
    id: "corporate_organizational",
    label: "Corporate / organizational",
    queryHints: [
      "corporate meeting",
      "leadership retreat",
      "sales kickoff hotel",
      "executive offsite",
    ],
  },
  {
    id: "sports_tournament",
    label: "Sports / tournament",
    queryHints: [
      "tournament hotel block",
      "championship housing",
      "sports event lodging",
    ],
  },
  {
    id: "event_operator_housing",
    label: "Event / operator / housing",
    queryHints: [
      "official hotel block",
      "overflow housing",
      "event housing partner",
      "Cvent hotel",
    ],
  },
  {
    id: "future_cycle",
    label: "Future cycle / RFP",
    queryHints: [
      "RFP hotel 2027 OR 2028",
      "save the date conference",
      "destination announced meeting",
    ],
  },
]);

/** Resort / luxury destination verticals — generic for CALA / Caribbean / island / AI resorts. */
const RESORT_VERTICALS = Object.freeze([
  {
    id: "incentive_travel",
    label: "Incentive / reward travel",
    queryHints: [
      "incentive travel program hotel OR resort",
      "sales incentive trip hotel block",
      "corporate reward travel destination",
      "incentive house RFP destination",
    ],
  },
  {
    id: "executive_retreat",
    label: "Executive / leadership retreat",
    queryHints: [
      "executive retreat beach resort hotel",
      "leadership offsite island resort",
      "board retreat resort lodging",
    ],
  },
  {
    id: "destination_wedding",
    label: "Destination wedding / social",
    queryHints: [
      "destination wedding planner hotel block",
      "destination wedding agency resort",
      "group wedding resort buyout",
    ],
  },
  {
    id: "travel_advisor_fam",
    label: "Travel advisor / FAM",
    queryHints: [
      "luxury travel advisor FAM resort",
      "advisor consortium destination trip",
      "host agency retreat resort",
    ],
  },
  {
    id: "regional_association",
    label: "Regional association / delegation",
    queryHints: [
      "regional association annual meeting hotel",
      "caribbean association conference lodging",
      "regional development agency meeting venue",
    ],
  },
  {
    id: "marine_yachting",
    label: "Marine / yachting groups",
    queryHints: [
      "yacht charter crew lodging resort",
      "marine industry meeting hotel",
      "yacht owner event lodging",
    ],
  },
  {
    id: "future_cycle",
    label: "Future cycle / RFP",
    queryHints: [
      "incentive RFP resort 2027 OR 2028",
      "destination wedding hotel RFP",
      "save the date retreat resort",
    ],
  },
]);

function isResortDestinationContract({ capability = {}, cfg = {}, identity = {} } = {}) {
  const blob = [
    capability.classification,
    capability.serviceLevel,
    cfg.capabilityProfile?.classification,
    cfg.capabilityProfile?.serviceLevel,
    identity.market,
    identity.submarket,
    identity.country,
    cfg.demandTerritory?.label,
  ]
    .join(" ")
    .toLowerCase();
  return /resort|beach|island|caribbean|all.?inclusive|honeymoon|wedding|destination.?leisure/.test(
    blob
  );
}

function selectDiscoveryVerticals(ctx) {
  return isResortDestinationContract(ctx) ? RESORT_VERTICALS : CORE_VERTICALS;
}

function geoTokens(contract) {
  const out = [];
  const push = (v) => {
    const s = String(v || "").trim();
    if (s && !out.includes(s)) out.push(s);
  };
  push(contract.market);
  push(contract.submarket);
  push(contract.city);
  push(contract.state);
  push(contract.country);
  for (const t of contract.territoryIncludes || []) push(t);
  for (const t of contract.coreGeoKeywords || []) push(t);
  return out.slice(0, 8);
}

/**
 * Build discovery contract from hotel profile + onboard config.
 */
export function buildGdiDiscoveryContract({ hotelId, profile = null, config = null } = {}) {
  const cfg = config || {};
  const identity = profile?.identity || {};
  const capability = profile?.capability || cfg.capabilityProfile || {};
  const territory = cfg.demandTerritory || {};
  const coreKw = territory.classificationKeywords?.TERRITORY_CORE || [];
  const nearbyKw = territory.classificationKeywords?.TERRITORY_NEARBY || [];

  const rooms =
    capability.totalGuestrooms ??
    profile?.rooms ??
    cfg.capabilityProfile?.totalGuestrooms ??
    null;
  const meetingSqFt =
    capability.totalMeetingSpaceSqFt ??
    cfg.capabilityProfile?.totalMeetingSpaceSqFt ??
    null;

  const verticals = selectDiscoveryVerticals({ capability, cfg, identity });

  return {
    version: GDI_DISCOVERY_CONTRACT_VERSION,
    hotelId: String(hotelId || "").trim(),
    hotelName: identity.hotelName || cfg.displayName || hotelId,
    market: identity.market || null,
    submarket: identity.submarket || null,
    city: identity.city || null,
    state: identity.state || null,
    country: identity.country || null,
    rooms,
    meetingSqFt,
    largestTheaterCapacity:
      capability.largestTheaterCapacity ??
      cfg.capabilityProfile?.largestTheaterCapacity ??
      null,
    peakRoomsMin: cfg.commercialPriorities?.coreTargetPeakRoomsMin ?? null,
    peakRoomsMax: cfg.commercialPriorities?.coreTargetPeakRoomsMax ?? null,
    territoryLabel: territory.label || null,
    territoryIncludes: territory.includes || [],
    coreGeoKeywords: coreKw,
    nearbyGeoKeywords: nearbyKw,
    evaluationQuestion:
      territory.evaluationQuestion ||
      `Could ${identity.hotelName || cfg.displayName || "this hotel"} realistically compete for this group demand?`,
    discoveryArchetype: isResortDestinationContract({ capability, cfg, identity })
      ? "RESORT_DESTINATION"
      : "URBAN_MEETING",
    verticals: verticals.map((v) => ({ id: v.id, label: v.label })),
    _verticalDefs: verticals,
    constraints: {
      noWebhound: true,
      noDemandTypeSeeding: true,
      emptyTargetSegmentsRequired: true,
      preferFirstPartySources: true,
    },
  };
}

/**
 * Build SerpAPI search tasks from contract (geography × vertical).
 */
export function buildDiscoverySearchTasks(contract) {
  const geos = geoTokens(contract);
  const primaryGeo = geos[0] || contract.market || contract.city || "hotel market";
  const secondaryGeo = geos[1] || geos[0] || primaryGeo;
  const yearClause = "(2026 OR 2027 OR 2028)";
  const tasks = [];
  const verticals = contract._verticalDefs || CORE_VERTICALS;

  for (const vertical of verticals) {
    const queries = [];
    for (const hint of vertical.queryHints.slice(0, 2)) {
      queries.push(`"${primaryGeo}" ${hint} ${yearClause}`);
      if (secondaryGeo !== primaryGeo) {
        queries.push(`"${secondaryGeo}" ${hint} ${yearClause}`);
      }
    }
    // Room-block / housing angle once per vertical
    queries.push(
      `"${primaryGeo}" (${hintFamily(vertical.id)}) (hotel OR lodging OR "room block" OR housing) ${yearClause}`
    );

    tasks.push({
      vertical: vertical.id,
      note: vertical.label,
      queries: [...new Set(queries)].slice(0, 4),
    });
  }

  // Hotel-credible overflow / destination queries (still blind — no known opp names)
  if (contract.discoveryArchetype === "RESORT_DESTINATION") {
    tasks.push({
      vertical: "incentive_travel",
      note: "Hidden demand — planner / DMC / advisor layer",
      queries: [
        `"${primaryGeo}" (DMC OR "incentive house" OR "destination wedding planner") (hotel OR resort OR lodging) ${yearClause}`,
        `"Caribbean" (incentive OR "advisor FAM" OR "executive retreat") ("destination TBD" OR "hotel TBD" OR RFP) ${yearClause}`,
      ],
    });
  } else {
    tasks.push({
      vertical: "event_operator_housing",
      note: "Overflow / destination housing",
      queries: [
        `"${primaryGeo}" ("official hotel" OR "host hotel" OR "room block") ${yearClause}`,
        `"${primaryGeo}" (conference OR summit OR tournament) (overflow OR "housing block") ${yearClause}`,
      ],
    });
  }

  return tasks;
}

function hintFamily(verticalId) {
  switch (verticalId) {
    case "association_conference":
    case "regional_association":
      return "conference OR summit OR \"annual meeting\"";
    case "corporate_organizational":
    case "executive_retreat":
      return "retreat OR \"corporate meeting\" OR offsite OR incentive";
    case "sports_tournament":
      return "tournament OR championship OR cup";
    case "incentive_travel":
      return "incentive OR \"reward travel\" OR \"sales trip\"";
    case "destination_wedding":
      return "wedding OR honeymoon OR \"social group\"";
    case "travel_advisor_fam":
      return "FAM OR \"travel advisor\" OR consortium";
    case "marine_yachting":
      return "yacht OR marina OR marine OR charter";
    case "future_cycle":
      return "RFP OR \"save the date\" OR destination";
    default:
      return "event OR meeting OR conference OR group";
  }
}

/**
 * Human/LLM brief for extraction or Parallel escalation.
 */
export function renderDiscoveryContractBrief(contract, { gaps = [] } = {}) {
  const lines = [
    "DEALALITY GDI NATIVE BLIND DISCOVERY CONTRACT",
    `Hotel: ${contract.hotelName} (${contract.hotelId})`,
    `Market: ${contract.market || "n/a"} | Submarket: ${contract.submarket || "n/a"}`,
    `City/State/Country: ${[contract.city, contract.state, contract.country].filter(Boolean).join(", ") || "n/a"}`,
    `Rooms: ${contract.rooms ?? "n/a"} | Meeting sq ft: ${contract.meetingSqFt ?? "n/a"} | Theater cap: ${contract.largestTheaterCapacity ?? "n/a"}`,
    `Peak room band (planning): ${contract.peakRoomsMin ?? "?"}–${contract.peakRoomsMax ?? "?"}`,
    `Territory: ${contract.territoryLabel || "n/a"}`,
    `Evaluation question: ${contract.evaluationQuestion}`,
    `Core geo tokens: ${(contract.coreGeoKeywords || []).slice(0, 12).join(", ") || "n/a"}`,
    "Rules:",
    "- Blind discovery only — do not invent events.",
    "- Prefer first-party / official sources.",
    "- No Webhound. No curated seed lists. No demand-type assumptions.",
    "- Include association, corporate, sports, housing/overflow, and future-cycle when evidenced.",
    `- Verticals in scope: ${(contract.verticals || []).map((v) => v.id).join(", ")}`,
  ];
  if (gaps.length) {
    lines.push("Escalation gaps to fill:");
    for (const g of gaps) {
      lines.push(`- ${g.code}: ${g.detail || ""}`);
    }
  }
  return lines.join("\n");
}
