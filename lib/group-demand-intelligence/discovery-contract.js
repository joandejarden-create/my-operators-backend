/**
 * GDI Native blind discovery contract.
 * Blind = hotel identity + market territory + capability only.
 * No Webhound seeds. No demand-type targetSegments seeding.
 */

export const GDI_DISCOVERY_CONTRACT_VERSION = "gdi_discovery_contract_v1";

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
    verticals: CORE_VERTICALS.map((v) => ({ id: v.id, label: v.label })),
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

  for (const vertical of CORE_VERTICALS) {
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
  tasks.push({
    vertical: "event_operator_housing",
    note: "Overflow / destination housing",
    queries: [
      `"${primaryGeo}" ("official hotel" OR "host hotel" OR "room block") ${yearClause}`,
      `"${primaryGeo}" (conference OR summit OR tournament) (overflow OR "housing block") ${yearClause}`,
    ],
  });

  return tasks;
}

function hintFamily(verticalId) {
  switch (verticalId) {
    case "association_conference":
      return "conference OR summit OR \"annual meeting\"";
    case "corporate_organizational":
      return "retreat OR \"corporate meeting\" OR offsite";
    case "sports_tournament":
      return "tournament OR championship OR cup";
    case "future_cycle":
      return "RFP OR \"save the date\" OR destination";
    default:
      return "event OR meeting OR conference";
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
