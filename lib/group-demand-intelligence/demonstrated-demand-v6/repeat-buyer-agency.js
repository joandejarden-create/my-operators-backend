/**
 * Cross-hotel / cross-comp repeat buyer & agency patterns.
 */

function orgKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 60);
}

/**
 * RepeatBuyerPattern — same organization across comps / cycles.
 */
export function buildRepeatBuyerPatterns(leads = [], traces = []) {
  const byOrg = new Map();
  for (const row of [...leads, ...traces]) {
    const k = orgKey(row.organizationName || row.organization || row.buyerEntity);
    if (!k || k.length < 5) continue;
    if (!byOrg.has(k)) byOrg.set(k, []);
    byOrg.get(k).push(row);
  }
  const out = [];
  for (const [k, group] of byOrg) {
    const hotels = [
      ...new Set(
        group
          .flatMap((g) => [
            g.competitorHotel,
            ...(Array.isArray(g.historicHotels) ? g.historicHotels : String(g.historicHotels || "").split("|")),
          ])
          .filter(Boolean)
      ),
    ];
    const markets = [...new Set(group.map((g) => g.market || g.lodgingMarket).filter(Boolean))];
    if (group.length < 2 && hotels.length < 2) continue;
    out.push({
      patternId: `rbp_${k.replace(/\s+/g, "_")}`.slice(0, 80),
      patternType: "RepeatBuyerPattern",
      organization: group[0].organizationName || group[0].organization || group[0].buyerEntity,
      occurrences: group.length,
      historicHotels: hotels.join("|"),
      historicMarkets: markets.join("|"),
      demandEngine: group[0].demandEngine || "",
      targetHotels: [...new Set(group.map((g) => g.targetHotelKey || g.hotelKey).filter(Boolean))].join(
        "|"
      ),
    });
  }
  return out.sort((a, b) => b.occurrences - a.occurrences);
}

/**
 * RepeatAgencyPattern — same agency/DMC across events.
 */
export function buildRepeatAgencyPatterns(leads = []) {
  const byAgency = new Map();
  for (const row of leads) {
    const agency = row.agency || row.housingPartner;
    if (!agency || agency === "HOUSING_PARTNER_DETECTED" || agency === "DMC_DETECTED") continue;
    const k = orgKey(agency);
    if (!k) continue;
    if (!byAgency.has(k)) byAgency.set(k, []);
    byAgency.get(k).push(row);
  }
  const out = [];
  for (const [k, group] of byAgency) {
    if (group.length < 2) continue;
    out.push({
      patternId: `rap_${k.replace(/\s+/g, "_")}`.slice(0, 80),
      patternType: "RepeatAgencyPattern",
      agency: group[0].agency || group[0].housingPartner,
      occurrences: group.length,
      organizations: [...new Set(group.map((g) => g.organizationName || g.organization).filter(Boolean))].join(
        "|"
      ),
      targetHotels: [...new Set(group.map((g) => g.targetHotelKey || g.hotelKey).filter(Boolean))].join(
        "|"
      ),
    });
  }
  return out;
}

/**
 * RepeatMarketPattern — same association returning to same market.
 */
export function buildRepeatMarketPatterns(leads = []) {
  const byKey = new Map();
  for (const row of leads) {
    const org = orgKey(row.organizationName || row.organization);
    const market = String(row.market || row.lodgingMarket || "").toLowerCase().slice(0, 40);
    if (!org || !market) continue;
    const k = `${org}|${market}`;
    if (!byKey.has(k)) byKey.set(k, []);
    byKey.get(k).push(row);
  }
  const out = [];
  for (const [, group] of byKey) {
    const years = [
      ...new Set(group.map((g) => g.eventYear || String(g.eventStartDate || "").slice(0, 4)).filter(Boolean)),
    ];
    if (group.length < 2 && years.length < 2) continue;
    const orgSlug = orgKey(group[0].organizationName || group[0].organization).replace(/\s+/g, "_");
    const marketSlug = String(group[0].market || "")
      .toLowerCase()
      .replace(/\W+/g, "_");
    out.push({
      patternId: `rmp_${orgSlug}_${marketSlug}`.slice(0, 80),
      patternType: "RepeatMarketPattern",
      organization: group[0].organizationName || group[0].organization,
      market: group[0].market || group[0].lodgingMarket,
      occurrences: group.length,
      years: years.join("|"),
    });
  }
  return out;
}
