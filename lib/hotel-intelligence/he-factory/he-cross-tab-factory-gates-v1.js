/**
 * Hotel Explorer Intelligence Factory — cross-tab / submarket / org gates (V1).
 * Consumed by Audit / Validate / Full / Refresh modes.
 */

import { validateCrossTabParity, HE_CROSS_TAB_PARITY_GATES } from "../he-cross-tab-parity-v1.js";
import { buildHpcSubmarketSnapshot } from "../he-hpc-submarket-snapshot-v1.js";
import { buildSharedHotelViewModel } from "../he-shared-hotel-view-model-v1.js";
import { buildOwnerPortfolioOperatorView } from "../ownership/owner-portfolio-operator-view-v1.js";
import { formatHotelPropertyCensusAsBrandPresenceHotel, HOTEL_PROPERTY_CENSUS_TABLE } from "../radar-internal-preview-hotels-v1.js";

export { HE_CROSS_TAB_PARITY_GATES };

export const HE_CROSS_TAB_FACTORY_GATES_V1 = "he-cross-tab-factory-gates-v1";

async function loadHpcHotel(hotelId) {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) return { hotel: null, error: "airtable_not_configured" };
  const Airtable = (await import("airtable")).default;
  const base = new Airtable({ apiKey: key }).base(baseId);
  try {
    const rec = await base(HOTEL_PROPERTY_CENSUS_TABLE).find(hotelId);
    return { hotel: formatHotelPropertyCensusAsBrandPresenceHotel(rec), error: null };
  } catch (err) {
    return { hotel: null, error: String(err?.message || err).slice(0, 240) };
  }
}

/**
 * Run permanent cross-tab / submarket / org hydration gates for one hotel.
 */
export async function runCrossTabFactoryGates({
  hotelId,
  ownership = null,
  overview = null,
  skipLiveAirtable = false,
} = {}) {
  const gates = Object.fromEntries(
    Object.values(HE_CROSS_TAB_PARITY_GATES).map((g) => [g, "NO"])
  );

  let hpcHotel = overview || null;
  let submarketSnapshot = null;
  let ownershipPayload = ownership;

  if (!skipLiveAirtable && hotelId) {
    const loaded = await loadHpcHotel(hotelId);
    if (loaded.hotel) hpcHotel = loaded.hotel;
    submarketSnapshot = await buildHpcSubmarketSnapshot({ hotelId });
  }

  const shared = buildSharedHotelViewModel({
    hpcHotel,
    ownership: ownershipPayload,
    hotelId,
  });

  const ownedSeed =
    ownershipPayload?.organization?.owned_controlled_hotels ||
    ownershipPayload?.report?.organization_and_portfolio?.hotels ||
    (shared.hotelId
      ? [
          {
            airtable_record_id: shared.hotelId,
            focus_hotel: true,
            hotel_name: shared.name,
            economic_owner_verified: true,
            include_as_owned: true,
          },
        ]
      : []);

  const portfolioView = buildOwnerPortfolioOperatorView(ownedSeed, {
    ownerDisplayName: shared.economicOwner || "",
    sharedHotelView: shared,
    focusHotelId: hotelId || shared.hotelId,
  });

  if (!submarketSnapshot) {
    submarketSnapshot = {
      ok: true,
      selectedHotel: hpcHotel,
      coverage: {
        partial: true,
        disclosed: true,
        marketShareFabricated: false,
        researchNeeded: true,
      },
      cohorts: {
        matchedHpcInventory: { hotels: hpcHotel ? [hpcHotel] : [] },
        geographicSubmarketInventory: { hotels: hpcHotel ? [hpcHotel] : [] },
        competitiveSet: { hotels: hpcHotel ? [hpcHotel] : [], researchNeeded: true },
      },
    };
  }

  const parity = validateCrossTabParity({
    hpcHotel,
    ownership: ownershipPayload,
    overview: overview || {
      rooms: shared.rooms,
      brand: shared.brand,
      market: shared.market,
      operator: shared.operator,
      propertyCompany: shared.propertyCompany,
      economicOwnerClassification: shared.economicOwnerClassification,
      chainScale: shared.chainScale,
      status: shared.status,
      location: shared.location,
    },
    organizationPortfolio: {
      ownedHotels: portfolioView.ownedHotels,
      propertyCompany: shared.propertyCompany,
      economicOwnerClassification: shared.economicOwnerClassification,
    },
    submarketSnapshot,
    hotelId: hotelId || shared.hotelId,
  });

  Object.assign(gates, parity.gates);

  return {
    ok: parity.ok && gates[HE_CROSS_TAB_PARITY_GATES.HE_CROSS_TAB_PARITY_VALIDATED] === "YES",
    version: HE_CROSS_TAB_FACTORY_GATES_V1,
    hotelId: hotelId || shared.hotelId,
    shared,
    portfolioFocus: portfolioView.ownedHotels.find(
      (h) => h.airtable_record_id === (hotelId || shared.hotelId)
    ),
    submarketCoverage: submarketSnapshot?.coverage || null,
    cohorts: submarketSnapshot?.cohorts
      ? {
          matched: submarketSnapshot.cohorts.matchedHpcInventory?.hotels?.length ?? null,
          geographic:
            submarketSnapshot.cohorts.geographicSubmarketInventory?.hotels?.length ?? null,
          competitive: submarketSnapshot.cohorts.competitiveSet?.hotels?.length ?? null,
        }
      : null,
    conflicts: parity.conflicts,
    gates,
  };
}
