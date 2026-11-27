/**
 * Hotel Explorer — cross-tab shared attribute parity (V1).
 *
 * Conflicting representations of the same hotel attribute across tabs
 * must fail validation rather than silently display inconsistent values.
 */

import { buildSharedHotelViewModel } from "./he-shared-hotel-view-model-v1.js";

export const HE_CROSS_TAB_PARITY_V1 = "he-cross-tab-parity-v1";

export const HE_CROSS_TAB_PARITY_GATES = Object.freeze({
  HE_SUBMARKET_COHORT_VALIDATED: "HE_SUBMARKET_COHORT_VALIDATED",
  HE_SUBMARKET_COVERAGE_DISCLOSED: "HE_SUBMARKET_COVERAGE_DISCLOSED",
  HE_ORG_PORTFOLIO_HYDRATION_VALIDATED: "HE_ORG_PORTFOLIO_HYDRATION_VALIDATED",
  HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT: "HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT",
  HE_CROSS_TAB_PARITY_VALIDATED: "HE_CROSS_TAB_PARITY_VALIDATED",
});

const SHARED_FIELDS = [
  "rooms",
  "brand",
  "market",
  "operator",
  "propertyCompany",
  "economicOwnerClassification",
  "location",
  "chainScale",
  "status",
];

function norm(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  return String(v).trim().toLowerCase().replace(/\s+/g, " ");
}

function roomsOf(v) {
  if (v == null || v === "" || v === "—") return null;
  const n = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
}

/**
 * Compare tab consumers against the shared canonical view.
 * @returns {{ ok: boolean, conflicts: object[], gates: Record<string,string>, shared: object }}
 */
export function validateCrossTabParity({
  hpcHotel = null,
  ownership = null,
  physicalProduct = null,
  overview = null,
  organizationPortfolio = null,
  submarketSnapshot = null,
  hotelId = null,
} = {}) {
  const shared = buildSharedHotelViewModel({
    hpcHotel,
    ownership,
    physicalProduct,
    hotelId,
  });

  const consumers = [];
  if (overview && typeof overview === "object") {
    consumers.push({
      tab: "hotel_overview",
      rooms: roomsOf(overview.rooms ?? overview.totalGuestrooms),
      brand: overview.brand || overview.currentBrand || null,
      market: overview.market || null,
      operator: overview.operator || overview.managementCompany || null,
      propertyCompany: overview.propertyCompany || null,
      economicOwnerClassification: overview.economicOwnerClassification || null,
      location: overview.location || [overview.city, overview.country].filter(Boolean).join(", "),
      chainScale: overview.chainScale || null,
      status: overview.status || null,
    });
  }

  if (organizationPortfolio && typeof organizationPortfolio === "object") {
    const focus =
      (organizationPortfolio.ownedHotels || organizationPortfolio.hotels || []).find(
        (h) =>
          h?.focus_hotel ||
          h?.hydrated_from_shared_view ||
          (shared.hotelId &&
            String(h?.airtable_record_id || h?.id || "") === String(shared.hotelId))
      ) || null;
    consumers.push({
      tab: "organization_portfolio",
      rooms: roomsOf(focus?.rooms),
      brand: focus?.brand_display || focus?.brand || null,
      market: focus?.market_display || focus?.market || null,
      operator: focus?.operator || focus?.management_company || null,
      propertyCompany:
        organizationPortfolio.propertyCompany ||
        ownership?.report?.ownership_and_control?.property_company?.tradingName ||
        ownership?.report?.ownership_and_control?.property_company?.name ||
        null,
      economicOwnerClassification:
        organizationPortfolio.economicOwnerClassification ||
        shared.economicOwnerClassification ||
        null,
      location: focus?.market_display || focus?.city || null,
      chainScale: focus?.chain_scale || focus?.chainScale || null,
      status: focus?.status || null,
      focusPresent: Boolean(focus),
      roomsKnown: focus?.rooms_known === true || roomsOf(focus?.rooms) != null,
    });
  }

  if (submarketSnapshot && typeof submarketSnapshot === "object") {
    const selected = submarketSnapshot.selectedHotel || {};
    consumers.push({
      tab: "submarket_snapshot",
      rooms: roomsOf(selected.rooms),
      brand: selected.brand || null,
      market: selected.market || null,
      operator: selected.managementCompany || selected.operator || null,
      propertyCompany: null, // not a submarket surface
      economicOwnerClassification: null,
      location: [selected.city, selected.country].filter(Boolean).join(", ") || null,
      chainScale: selected.chainScale || null,
      status: selected.status || selected.radarStatus || null,
    });
  }

  const conflicts = [];
  for (const field of SHARED_FIELDS) {
    const canonical = shared[field];
    if (canonical == null || canonical === "") continue;
    for (const c of consumers) {
      // Property company / ownership classification are not required on Submarket.
      if (
        c.tab === "submarket_snapshot" &&
        (field === "propertyCompany" || field === "economicOwnerClassification")
      ) {
        continue;
      }
      const actual = c[field];
      if (actual == null || actual === "") {
        // Missing on a consumer that should show it is a mapping defect when canonical exists.
        if (field === "rooms" && c.tab === "organization_portfolio") {
          conflicts.push({
            field,
            tab: c.tab,
            kind: "MAPPING_RENDERING_DEFECT",
            canonical,
            actual: null,
            note: "Canonical rooms exist but Org Portfolio focus row unresolved",
          });
        }
        continue;
      }
      if (field === "rooms") {
        if (roomsOf(actual) !== roomsOf(canonical)) {
          conflicts.push({
            field,
            tab: c.tab,
            kind: "VALUE_CONFLICT",
            canonical,
            actual,
          });
        }
        continue;
      }
      if (norm(actual) !== norm(canonical)) {
        // Soft location compare: city/country vs market label are documented transforms.
        if (field === "location") {
          const a = norm(actual);
          const b = norm(canonical);
          if (a && b && (a.includes(b) || b.includes(a))) continue;
          // Org portfolio often shows market_display in the market column — not a location conflict.
          if (c.tab === "organization_portfolio") continue;
        }
        conflicts.push({
          field,
          tab: c.tab,
          kind: "VALUE_CONFLICT",
          canonical,
          actual,
        });
      }
    }
  }

  const orgConsumer = consumers.find((c) => c.tab === "organization_portfolio");
  const sub = submarketSnapshot || {};
  const cohorts = sub.cohorts || {};
  const coverage = sub.coverage || {};

  const gates = {
    [HE_CROSS_TAB_PARITY_GATES.HE_SUBMARKET_COHORT_VALIDATED]:
      cohorts.matchedHpcInventory &&
      cohorts.geographicSubmarketInventory &&
      cohorts.competitiveSet
        ? "YES"
        : sub.ok === false
          ? "NO"
          : "NO",
    [HE_CROSS_TAB_PARITY_GATES.HE_SUBMARKET_COVERAGE_DISCLOSED]:
      coverage.disclosed === true && coverage.marketShareFabricated === false
        ? "YES"
        : coverage.partial != null
          ? "PARTIAL"
          : "NO",
    [HE_CROSS_TAB_PARITY_GATES.HE_ORG_PORTFOLIO_HYDRATION_VALIDATED]:
      orgConsumer && orgConsumer.focusPresent && orgConsumer.roomsKnown
        ? "YES"
        : orgConsumer
          ? "NO"
          : "SKIP",
    [HE_CROSS_TAB_PARITY_GATES.HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT]:
      conflicts.length === 0 ? "YES" : "NO",
    [HE_CROSS_TAB_PARITY_GATES.HE_CROSS_TAB_PARITY_VALIDATED]:
      conflicts.length === 0 &&
      (gatesOrYes(
        cohorts.matchedHpcInventory &&
          cohorts.geographicSubmarketInventory &&
          cohorts.competitiveSet
      ) ||
        sub.ok === true)
        ? conflicts.length === 0
          ? "YES"
          : "NO"
        : conflicts.length === 0
          ? "PARTIAL"
          : "NO",
  };

  // Finalize cross-tab gate after others known
  gates[HE_CROSS_TAB_PARITY_GATES.HE_CROSS_TAB_PARITY_VALIDATED] =
    conflicts.length === 0 &&
    gates[HE_CROSS_TAB_PARITY_GATES.HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT] === "YES" &&
    gates[HE_CROSS_TAB_PARITY_GATES.HE_SUBMARKET_COHORT_VALIDATED] === "YES" &&
    (gates[HE_CROSS_TAB_PARITY_GATES.HE_ORG_PORTFOLIO_HYDRATION_VALIDATED] === "YES" ||
      gates[HE_CROSS_TAB_PARITY_GATES.HE_ORG_PORTFOLIO_HYDRATION_VALIDATED] === "SKIP")
      ? "YES"
      : "NO";

  return {
    ok: conflicts.length === 0,
    version: HE_CROSS_TAB_PARITY_V1,
    shared,
    consumers,
    conflicts,
    gates,
  };
}

function gatesOrYes(v) {
  return Boolean(v);
}

/**
 * Pure fixture validator for Bethesda golden attributes (no Airtable).
 */
export function validateBethesdaGoldenParityFixture() {
  const hpcHotel = {
    id: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    rooms: 407,
    brand: "Marriott Hotels",
    market: "Bethesda / Montgomery County",
    submarket: "Pooks Hill / Rockville Pike",
    managementCompany: "Marriott International",
    chainScale: "Upper Upscale",
    status: "Open",
    city: "Bethesda",
    country: "United States",
  };
  const ownership = {
    hotel: { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott", rooms: 407 },
    report: {
      ownership_and_control: {
        property_company: { tradingName: "Marbeth Acquisition", verification_state: "Validated" },
        economic_owner: {
          tradingName: "Mullen Real Estate Capital",
          founder_classification: "Proposed",
        },
        operator: { tradingName: "Marriott International" },
      },
    },
  };
  const ownedHotels = [
    {
      hotel_name: "Bethesda Marriott",
      airtable_record_id: "recLuxvwwxID7U2B8",
      focus_hotel: true,
      rooms: 407,
      rooms_known: true,
      brand_display: "Marriott Hotels",
      market_display: "Bethesda / Montgomery County",
      operator: "Marriott International",
      status: "Open",
      economic_owner_verified: true,
      include_as_owned: true,
    },
  ];
  const submarketSnapshot = {
    ok: true,
    selectedHotel: hpcHotel,
    coverage: {
      partial: true,
      disclosed: true,
      marketShareFabricated: false,
      researchNeeded: true,
      inventGeography: false,
    },
    cohorts: {
      matchedHpcInventory: { hotels: [hpcHotel], metrics: { totalHotels: 1 } },
      geographicSubmarketInventory: { hotels: [hpcHotel], metrics: { totalHotels: 1 } },
      competitiveSet: { hotels: [hpcHotel], metrics: { totalHotels: 1 }, researchNeeded: true },
    },
  };

  return validateCrossTabParity({
    hpcHotel,
    ownership,
    overview: {
      rooms: 407,
      brand: "Marriott Hotels",
      market: "Bethesda / Montgomery County",
      operator: "Marriott International",
      propertyCompany: "Marbeth Acquisition",
      economicOwnerClassification: "Proposed",
      chainScale: "Upper Upscale",
      status: "Open",
      location: "Bethesda, United States",
    },
    organizationPortfolio: {
      ownedHotels,
      propertyCompany: "Marbeth Acquisition",
      economicOwnerClassification: "Proposed",
    },
    submarketSnapshot,
    hotelId: "recLuxvwwxID7U2B8",
  });
}
