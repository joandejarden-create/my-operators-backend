/**
 * Hotel Explorer — consolidated cross-tab consistency report (V1).
 *
 * Produced on every Audit / Validate / Full / Refresh factory run.
 * Verified canonical values missing or contradicted across tabs → FAIL (not "missing research").
 * Genuine unverified facts → UNVERIFIED (never invent).
 * Browser-skipped checks → NOT_TESTED (never PASS).
 */

import fs from "node:fs";
import path from "node:path";
import { buildSharedHotelViewModel } from "../he-shared-hotel-view-model-v1.js";
import { HE_CROSS_TAB_PARITY_GATES } from "../he-cross-tab-parity-v1.js";

export const HE_CROSS_TAB_CONSISTENCY_REPORT_V1 = "he-cross-tab-consistency-report-v1";

export const HE_CONSISTENCY_RESULT = Object.freeze({
  PASS: "PASS",
  FAIL: "FAIL",
  UNVERIFIED: "UNVERIFIED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  NOT_TESTED: "NOT_TESTED",
});

/** 11 active HE tabs (Guest Sentiment disabled by founder). */
export const HE_ACTIVE_CONSISTENCY_TABS = Object.freeze([
  "hotel",
  "rooms",
  "ownership",
  "contacts",
  "organization",
  "relationships",
  "submarket",
  "area",
  "demand",
  "access",
  "sources",
]);

const ATTR_DEFS = [
  {
    id: "rooms",
    label: "Guestrooms (Rooms / Keys)",
    unit: "rooms",
    tabs: ["hotel", "rooms", "organization", "submarket", "area", "ownership"],
  },
  {
    id: "brand",
    label: "Current brand",
    unit: "brand_name",
    tabs: ["hotel", "ownership", "organization", "submarket", "area", "sources"],
  },
  {
    id: "market",
    label: "Dealality market",
    unit: "market_label",
    tabs: ["hotel", "submarket", "area", "organization", "ownership"],
  },
  {
    id: "submarket",
    label: "Dealality submarket / corridor",
    unit: "submarket_label",
    tabs: ["hotel", "submarket", "area"],
  },
  {
    id: "operator",
    label: "Operator / management company",
    unit: "org_name",
    tabs: ["hotel", "ownership", "organization", "relationships", "sources"],
  },
  {
    id: "propertyCompany",
    label: "Property company (legal / PropCo)",
    unit: "org_name",
    tabs: ["hotel", "ownership", "organization", "relationships", "sources"],
  },
  {
    id: "economicOwner",
    label: "Economic owner / sponsor",
    unit: "org_name",
    tabs: ["hotel", "ownership", "organization", "relationships", "contacts", "sources"],
  },
  {
    id: "economicOwnerClassification",
    label: "Economic ownership classification",
    unit: "classification",
    tabs: ["hotel", "ownership", "organization", "relationships", "sources"],
  },
  {
    id: "chainScale",
    label: "Chain scale",
    unit: "chain_scale",
    tabs: ["hotel", "submarket", "area"],
  },
  {
    id: "status",
    label: "Operating / radar status",
    unit: "status",
    tabs: ["hotel", "submarket", "area", "organization"],
  },
  {
    id: "meetingSpaceTotal",
    label: "Total meeting space",
    unit: "sq_ft",
    tabs: ["rooms", "hotel", "sources"],
  },
  {
    id: "meetingRoomCount",
    label: "Meeting room count",
    unit: "rooms",
    tabs: ["rooms", "hotel", "sources"],
  },
  {
    id: "contactsPresence",
    label: "Contact intelligence presence",
    unit: "boolean_or_count",
    tabs: ["contacts", "organization"],
  },
  {
    id: "portfolioFocusRooms",
    label: "Portfolio focus-hotel rooms",
    unit: "rooms",
    tabs: ["organization"],
  },
  {
    id: "submarketCohortId",
    label: "Submarket cohort identity (like-for-like)",
    unit: "cohort_id",
    tabs: ["submarket"],
  },
];

function text(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

function norm(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  return String(v)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function roomsOf(v) {
  if (v == null || v === "" || v === "—") return null;
  const n = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(n) && n > 0 ? Math.round(n) : null;
}

function partyName(party) {
  if (!party) return null;
  if (typeof party === "string") return text(party);
  return (
    text(party.tradingName) ||
    text(party.legalName) ||
    text(party.name) ||
    text(party.display_name) ||
    null
  );
}

function classificationOf(party) {
  if (!party || typeof party !== "object") return null;
  return (
    text(party.founder_classification) ||
    text(party.classification) ||
    text(party.verification_state) ||
    text(party.status) ||
    text(party.confidence) ||
    null
  );
}

/** Raw HI economic classification (may illicitly say Validated). */
function rawEconomicClassification(control = {}) {
  const party = control.economic_owner_or_group || control.economic_owner || {};
  return classificationOf(party);
}

/** Founder-facing classification for expected values / shared VM. */
function founderEconomicClassification(control = {}, fallback = null) {
  const raw = rawEconomicClassification(control);
  if (!raw && !fallback) return null;
  if (raw && /^validat/i.test(raw)) return "Proposed"; // never promote Validated into expected
  if (raw && /^proposed/i.test(raw)) return "Proposed";
  if (fallback && /^validat/i.test(fallback)) return "Proposed";
  return raw || fallback;
}

/**
 * Canonical expected values from HPC + HI (never invent).
 */
export function buildCanonicalExpectations({
  hotelId,
  hpcHotel = null,
  hpcRecordId = null,
  ownership = null,
  physicalProduct = null,
  shared = null,
} = {}) {
  const vm =
    shared ||
    buildSharedHotelViewModel({
      hpcHotel,
      ownership,
      physicalProduct,
      hotelId,
    });
  const control =
    ownership?.report?.ownership_and_control ||
    ownership?.ownership_and_control ||
    {};
  const inv = physicalProduct || ownership?.guestRoomsMeetingSpace || null;
  const meetingTotal =
    roomsOf(inv?.meetingSpace?.totalSqFt?.value) ||
    roomsOf(inv?.meetingSpace?.totalSqFt) ||
    roomsOf(inv?.totalMeetingSpaceSqFt) ||
    null;
  const meetingCount =
    roomsOf(inv?.meetingSpace?.roomCount?.value) ||
    roomsOf(inv?.meetingSpace?.roomCount) ||
    roomsOf(inv?.meetingRoomCount) ||
    null;

  const economicOwnerName =
    vm.economicOwner ||
    partyName(control.economic_owner) ||
    partyName(control.economicOwner) ||
    null;
  const economicClass =
    vm.economicOwnerClassification ||
    founderEconomicClassification(control, economicOwnerName ? "Proposed" : null) ||
    null;

  const contacts =
    ownership?.report?.corporate_contacts ||
    ownership?.contacts ||
    ownership?.contact_intelligence ||
    null;

  function row(id, opts) {
    const verified = opts.verificationStatus === "Verified";
    return {
      attributeId: id,
      canonicalSource: opts.canonicalSource,
      canonicalRecordId: opts.canonicalRecordId || hpcRecordId || hotelId || null,
      expectedValue: opts.expectedValue,
      verificationStatus: opts.verificationStatus, // Verified | Proposed | Unverified | Unknown
      unit: opts.unit,
      notes: opts.notes || null,
      failIfMissingOnApplicableTabs: verified,
    };
  }

  return {
    hotelId: hotelId || vm.hotelId,
    hotelName: vm.name,
    attributes: {
      rooms: row("rooms", {
        canonicalSource: "Hotel Property Census · Rooms / Keys",
        expectedValue: vm.rooms,
        verificationStatus: vm.rooms != null ? "Verified" : "Unverified",
        unit: "rooms",
      }),
      brand: row("brand", {
        canonicalSource: "Hotel Property Census · Current Brand / Affiliation",
        expectedValue: vm.brand,
        verificationStatus: vm.brand ? "Verified" : "Unverified",
        unit: "brand_name",
      }),
      market: row("market", {
        canonicalSource: "Hotel Property Census · Market (Dealality)",
        expectedValue: vm.market,
        verificationStatus: vm.market ? "Verified" : "Unverified",
        unit: "market_label",
      }),
      submarket: row("submarket", {
        canonicalSource: "Hotel Property Census · Submarket (Dealality corridor)",
        expectedValue: vm.submarket,
        verificationStatus: vm.submarket ? "Verified" : "Unverified",
        unit: "submarket_label",
      }),
      operator: row("operator", {
        canonicalSource: vm.sources?.operator === "hi_relationship"
          ? "HI Hotel–Org Relationships · Operator"
          : "Hotel Property Census · Operator / Management Company",
        expectedValue: vm.operator,
        verificationStatus: vm.operator ? "Verified" : "Unverified",
        unit: "org_name",
      }),
      propertyCompany: row("propertyCompany", {
        canonicalSource: "HI Hotel–Org Relationships · Legal Owner / PropCo",
        expectedValue: vm.propertyCompany,
        verificationStatus: vm.propertyCompany ? "Verified" : "Unverified",
        unit: "org_name",
      }),
      economicOwner: row("economicOwner", {
        canonicalSource: "HI Hotel–Org Relationships · Economic Owner",
        expectedValue: vm.economicOwner,
        verificationStatus: economicClass
          ? /proposed/i.test(economicClass)
            ? "Proposed"
            : /validat/i.test(economicClass)
              ? "Proposed" // founder: do not silently promote; treat graph Validated Mullen as Proposed until certified
              : "Unverified"
          : vm.economicOwner
            ? "Proposed"
            : "Unverified",
        unit: "org_name",
        notes:
          "Proposed economic sponsorship must not silently render as Validated in any tab.",
      }),
      economicOwnerClassification: row("economicOwnerClassification", {
        canonicalSource: "Founder classification / HI Economic Owner",
        expectedValue: economicClass || (vm.economicOwner ? "Proposed" : null),
        verificationStatus: economicClass || vm.economicOwner ? "Proposed" : "Unverified",
        unit: "classification",
        notes: "Classification drift Validated↔Proposed across tabs is FAIL.",
      }),
      chainScale: row("chainScale", {
        canonicalSource: "Hotel Property Census · Chain Scale",
        expectedValue: vm.chainScale,
        verificationStatus: vm.chainScale ? "Verified" : "Unverified",
        unit: "chain_scale",
      }),
      status: row("status", {
        canonicalSource: "Hotel Property Census · Radar Status",
        expectedValue: vm.status,
        verificationStatus: vm.status ? "Verified" : "Unverified",
        unit: "status",
      }),
      meetingSpaceTotal: row("meetingSpaceTotal", {
        canonicalSource: "HPC physical-product / staged meeting inventory",
        expectedValue: meetingTotal,
        verificationStatus: meetingTotal != null ? "Verified" : "Unverified",
        unit: "sq_ft",
        notes: "Compare sq ft only; never mix marketing private-space figures with capacity-chart totals.",
      }),
      meetingRoomCount: row("meetingRoomCount", {
        canonicalSource: "HPC physical-product / staged meeting inventory",
        expectedValue: meetingCount,
        verificationStatus: meetingCount != null ? "Verified" : "Unverified",
        unit: "rooms",
      }),
      contactsPresence: row("contactsPresence", {
        canonicalSource: "Contact Intelligence / HI contacts",
        expectedValue: contacts ? true : null,
        verificationStatus: contacts ? "Verified" : "Unverified",
        unit: "boolean_or_count",
      }),
      portfolioFocusRooms: row("portfolioFocusRooms", {
        canonicalSource: "HPC Rooms / Keys via shared hotel view (Org Portfolio focus row)",
        expectedValue: vm.rooms,
        verificationStatus: vm.rooms != null ? "Verified" : "Unverified",
        unit: "rooms",
        notes: "Verified rooms missing on Org focus row = FAIL (mapping defect).",
      }),
      submarketCohortId: row("submarketCohortId", {
        canonicalSource: "HE HPC Submarket Snapshot · matched_hpc_inventory cohort",
        expectedValue: "matched_hpc_inventory",
        verificationStatus: "Verified",
        unit: "cohort_id",
        notes: "Totals must identify cohort; compare like-for-like only.",
      }),
    },
  };
}

function extractTabApiValues({
  hpcHotel,
  ownership,
  physicalProduct,
  portfolioView,
  submarketSnapshot,
  areaHotels = null,
  contactsPayload = null,
  shared = null,
}) {
  const control =
    ownership?.report?.ownership_and_control ||
    ownership?.ownership_and_control ||
    {};
  // Ownership API hotel stub is often id/name only — prefer shared VM / HPC for identity.
  const hotel = ownership?.hotel || {};
  const inv = physicalProduct || ownership?.guestRoomsMeetingSpace || null;
  const focus =
    (portfolioView?.ownedHotels || []).find((h) => h?.focus_hotel || h?.hydrated_from_shared_view) ||
    (portfolioView?.ownedHotels || [])[0] ||
    null;
  const selected = submarketSnapshot?.selectedHotel || hpcHotel || {};
  const cohortId =
    submarketSnapshot?.cohorts?.matchedHpcInventory?.id ||
    (submarketSnapshot?.hotels ? "matched_hpc_inventory" : null);

  const meetingTotal =
    roomsOf(inv?.meetingSpace?.totalSqFt?.value) ||
    roomsOf(inv?.meetingSpace?.totalSqFt) ||
    roomsOf(inv?.totalMeetingSpaceSqFt) ||
    null;
  const meetingCount =
    roomsOf(inv?.meetingSpace?.roomCount?.value) ||
    roomsOf(inv?.meetingSpace?.roomCount) ||
    roomsOf(inv?.meetingRoomCount) ||
    null;

  const economicOwner =
    (shared && shared.economicOwner) ||
    partyName(control.economic_owner) ||
    partyName(control.economic_owner_or_group) ||
    text(hotel.economicOwner) ||
    null;

  // Shared / founder-facing classification (Proposed preserved).
  const economicClass =
    (shared && shared.economicOwnerClassification) ||
    founderEconomicClassification(control, economicOwner ? "Proposed" : null) ||
    text(hotel.economic_owner_classification) ||
    (economicOwner ? "Proposed" : null);

  // Raw HI may still say Validated. Prefer explicit hotel/tab leak, else shared/founder Proposed.
  // (Graph-only VALIDATED without tab leak does not fail — shared VM remaps to Proposed.)
  const rawClass = rawEconomicClassification(control);
  const leakedClass =
    text(hotel.economic_owner_classification) ||
    text(hotel.economicOwnerClassification) ||
    null;
  const ownershipClassApi = leakedClass || economicClass;
  const illicitValidated =
    Boolean(ownershipClassApi) &&
    /^validat/i.test(String(ownershipClassApi)) &&
    Boolean(economicClass) &&
    /proposed/i.test(String(economicClass));

  const identityRooms =
    roomsOf(shared?.rooms) ||
    roomsOf(hpcHotel?.rooms) ||
    roomsOf(hotel.rooms) ||
    null;
  // Brand affiliation ≠ HI brand-parent org (e.g. Marriott Hotels ≠ Marriott International).
  const identityBrand =
    text(shared?.brand) ||
    text(hpcHotel?.brand) ||
    text(hotel.brand || hotel.currentBrand) ||
    null;
  const identityMarket =
    text(shared?.market) ||
    text(hpcHotel?.market) ||
    text(hotel.market) ||
    null;
  const identitySubmarket =
    text(shared?.submarket) ||
    text(hpcHotel?.submarket) ||
    text(hotel.submarket) ||
    null;
  const identityOperator =
    text(shared?.operator) ||
    text(hpcHotel?.managementCompany) ||
    partyName(control.operator) ||
    text(hotel.managementCompany || hotel.operator) ||
    null;
  const identityPropCo =
    (shared && shared.propertyCompany) ||
    partyName(control.property_company) ||
    partyName(control.legal_property_owner_propco) ||
    text(hotel.propertyCompany) ||
    null;
  const identityChain =
    text(shared?.chainScale) ||
    text(hpcHotel?.chainScale) ||
    text(hotel.chainScale) ||
    null;
  const identityStatus =
    text(shared?.status) ||
    text(hpcHotel?.status || hpcHotel?.radarStatus) ||
    text(hotel.status || hotel.radarStatus) ||
    null;

  const contacts =
    contactsPayload ||
    ownership?.report?.corporate_contacts ||
    ownership?.contact_intelligence ||
    null;

  return {
    hotel: {
      rooms: identityRooms,
      brand: identityBrand,
      market: identityMarket,
      submarket: identitySubmarket,
      operator: identityOperator,
      propertyCompany: identityPropCo,
      economicOwner,
      economicOwnerClassification: economicClass,
      chainScale: identityChain,
      status: identityStatus,
      meetingSpaceTotal: meetingTotal,
      meetingRoomCount: meetingCount,
      __illicitValidatedPromotion: illicitValidated || undefined,
    },
    rooms: {
      rooms: identityRooms || roomsOf(inv?.guestRooms?.totalGuestrooms?.value),
      meetingSpaceTotal: meetingTotal,
      meetingRoomCount: meetingCount,
      brand: identityBrand,
    },
    ownership: {
      // Ownership tab consumes shared hotel identity + HI control — never HI brand-parent as brand.
      rooms: identityRooms,
      brand: identityBrand,
      market: identityMarket,
      operator: identityOperator,
      propertyCompany: identityPropCo,
      economicOwner,
      economicOwnerClassification: ownershipClassApi,
      __illicitValidatedPromotion: illicitValidated || undefined,
      __rawEconomicClassification: rawClass || undefined,
    },
    contacts: {
      contactsPresence: contacts ? true : null,
      economicOwner,
      propertyCompany: identityPropCo,
    },
    organization: {
      // Focus-row rooms must come from portfolio hydration — do not backfill from shared
      // (missing verified rooms on Org focus row is a mapping defect).
      rooms: focus ? roomsOf(focus.rooms) : identityRooms,
      brand: text(focus?.brand_display || focus?.brand) || identityBrand,
      market: text(focus?.market_display || focus?.market) || identityMarket,
      operator: text(focus?.operator || focus?.management_company) || identityOperator,
      propertyCompany: identityPropCo,
      economicOwner,
      economicOwnerClassification: economicClass,
      status: text(focus?.status) || identityStatus,
      portfolioFocusRooms: focus ? roomsOf(focus.rooms) : null,
      contactsPresence: contacts ? true : null,
    },
    relationships: {
      operator: identityOperator,
      propertyCompany: identityPropCo,
      economicOwner,
      economicOwnerClassification: ownershipClassApi,
      __illicitValidatedPromotion: illicitValidated || undefined,
    },
    submarket: {
      rooms: roomsOf(selected.rooms) ?? identityRooms,
      brand: text(selected.brand) || identityBrand,
      market: text(selected.market) || identityMarket,
      submarket: text(selected.submarket) || identitySubmarket,
      chainScale: text(selected.chainScale) || identityChain,
      status: text(selected.status || selected.radarStatus) || identityStatus,
      submarketCohortId: cohortId,
      matchedCohortHotels: submarketSnapshot?.cohorts?.matchedHpcInventory?.hotels?.length ??
        submarketSnapshot?.hotels?.length ??
        null,
      geographicCohortHotels:
        submarketSnapshot?.cohorts?.geographicSubmarketInventory?.hotels?.length ?? null,
    },
    area: {
      rooms: identityRooms,
      brand: identityBrand,
      market: identityMarket,
      submarket: identitySubmarket,
      chainScale: identityChain,
      status: identityStatus,
      areaHotelCount: Array.isArray(areaHotels) ? areaHotels.length : null,
    },
    demand: {
      // Demand tab does not restate property rooms/brand as SoT; N/A for most identity attrs
    },
    access: {},
    sources: {
      brand: identityBrand,
      operator: identityOperator,
      propertyCompany: identityPropCo,
      economicOwner,
      economicOwnerClassification: economicClass,
      meetingSpaceTotal: meetingTotal,
      meetingRoomCount: meetingCount,
    },
  };
}

function valuesEqual(attrId, expected, actual) {
  if (expected == null && actual == null) return true;
  if (attrId === "rooms" || attrId === "meetingRoomCount" || attrId === "portfolioFocusRooms" || attrId === "meetingSpaceTotal") {
    return roomsOf(expected) === roomsOf(actual);
  }
  if (attrId === "contactsPresence") {
    return Boolean(expected) === Boolean(actual);
  }
  if (attrId === "economicOwnerClassification") {
    const a = norm(actual);
    const b = norm(expected);
    if (!a || !b) return a === b;
    // Proposed must not equal Validated
    if (/proposed/i.test(b) && /validat/i.test(a)) return false;
    if (/validat/i.test(b) && /proposed/i.test(a)) return false;
    return a === b || a.includes(b) || b.includes(a);
  }
  const a = norm(actual);
  const b = norm(expected);
  if (!a || !b) return a === b;
  return a === b || a.includes(b) || b.includes(a);
}

function scoreAttribute({
  def,
  expectation,
  tabApi,
  renderedByTab = {},
  browserEnabled = false,
  ownershipLoaded = true,
}) {
  const rows = [];
  const expected = expectation?.expectedValue;
  const verificationStatus = expectation?.verificationStatus || "Unknown";
  const applicableTabs = def.tabs.filter((t) => HE_ACTIVE_CONSISTENCY_TABS.includes(t));
  const hiTabs = new Set(["ownership", "relationships", "contacts", "organization", "sources"]);

  for (const tab of applicableTabs) {
    const apiBag = tabApi?.[tab] || {};
    const hasKey = Object.prototype.hasOwnProperty.call(apiBag, def.id);
    const apiValue = hasKey ? apiBag[def.id] : undefined;
    const rendered = renderedByTab?.[tab]?.[def.id];

    let result = HE_CONSISTENCY_RESULT.NOT_TESTED;
    let rootCause = null;
    let requiredCorrection = null;
    let affectedComponent = `Hotel Explorer · ${tab} tab`;

    // Demand/access rarely carry property identity — mark N/A when API bag empty for attr
    if ((tab === "demand" || tab === "access") && !hasKey) {
      rows.push({
        tab,
        apiValue: null,
        renderedValue: browserEnabled ? rendered ?? null : null,
        result: HE_CONSISTENCY_RESULT.NOT_APPLICABLE,
        apiResult: HE_CONSISTENCY_RESULT.NOT_APPLICABLE,
        browserResult: browserEnabled
          ? HE_CONSISTENCY_RESULT.NOT_APPLICABLE
          : HE_CONSISTENCY_RESULT.NOT_TESTED,
        rootCause: null,
        requiredCorrection: null,
        affectedComponent,
      });
      continue;
    }

    // HI graph not loaded — do not FAIL verified HPC attrs on HI tabs
    if (
      !ownershipLoaded &&
      hiTabs.has(tab) &&
      (def.id === "operator" ||
        def.id === "propertyCompany" ||
        def.id === "economicOwner" ||
        def.id === "economicOwnerClassification" ||
        def.id === "contactsPresence" ||
        def.id === "portfolioFocusRooms")
    ) {
      rows.push({
        tab,
        apiValue: null,
        renderedValue: null,
        result: HE_CONSISTENCY_RESULT.NOT_TESTED,
        apiResult: HE_CONSISTENCY_RESULT.NOT_TESTED,
        browserResult: HE_CONSISTENCY_RESULT.NOT_TESTED,
        rootCause: "Ownership/HI payload not loaded for this run — HI tab check NOT_TESTED",
        requiredCorrection: "Re-run with ownership API available (local server + Airtable)",
        affectedComponent,
      });
      continue;
    }

    if (expected == null || expected === "") {
      result = HE_CONSISTENCY_RESULT.UNVERIFIED;
      rootCause = "Canonical value not verified — do not invent to satisfy tests";
    } else if (verificationStatus === "Unverified" || verificationStatus === "Unknown") {
      result = HE_CONSISTENCY_RESULT.UNVERIFIED;
      rootCause = "Fact remains unverified in Airtable / research";
    } else if (!hasKey || apiValue == null || apiValue === "") {
      // Verified/Proposed canonical missing from consuming tab API
      if (verificationStatus === "Verified" || verificationStatus === "Proposed") {
        result = HE_CONSISTENCY_RESULT.FAIL;
        rootCause =
          "MAPPING_RENDERING_DEFECT — verified/proposed canonical value missing from tab API (not a research gap)";
        requiredCorrection = `Hydrate ${def.id} on ${tab} from ${expectation.canonicalSource}`;
      } else {
        result = HE_CONSISTENCY_RESULT.UNVERIFIED;
      }
    } else if (!valuesEqual(def.id, expected, apiValue)) {
      result = HE_CONSISTENCY_RESULT.FAIL;
      if (
        def.id === "economicOwnerClassification" &&
        /proposed/i.test(String(expected)) &&
        /validat/i.test(String(apiValue))
      ) {
        rootCause =
          "CLASSIFICATION_DRIFT — Proposed ownership silently rendered as Validated";
        requiredCorrection =
          "Preserve founder Proposed classification on all ownership surfaces";
      } else if (def.id === "submarketCohortId") {
        rootCause = "COHORT_MISMATCH — submarket totals not tied to matched_hpc_inventory";
        requiredCorrection = "Identify cohort on Submarket Snapshot and compare like-for-like only";
      } else {
        rootCause = "VALUE_CONFLICT — tab API contradicts canonical verified value";
        requiredCorrection = `Align ${tab} ${def.id} to canonical ${JSON.stringify(expected)} (${expectation.unit})`;
      }
    } else {
      result = HE_CONSISTENCY_RESULT.PASS;
    }

    // Browser layer
    let browserResult = HE_CONSISTENCY_RESULT.NOT_TESTED;
    let browserRootCause = null;
    let browserCorrection = null;
    if (!browserEnabled) {
      browserResult = HE_CONSISTENCY_RESULT.NOT_TESTED;
      browserRootCause = "Browser validation skipped — must not be recorded as PASS";
    } else if (result === HE_CONSISTENCY_RESULT.NOT_APPLICABLE) {
      browserResult = HE_CONSISTENCY_RESULT.NOT_APPLICABLE;
    } else if (rendered == null || rendered === "") {
      if (result === HE_CONSISTENCY_RESULT.PASS && (verificationStatus === "Verified" || verificationStatus === "Proposed")) {
        browserResult = HE_CONSISTENCY_RESULT.FAIL;
        browserRootCause =
          "MAPPING_RENDERING_DEFECT — API consistent but rendered UI missing verified value";
        browserCorrection = `Fix ${tab} UI binding for ${def.id}`;
      } else if (result === HE_CONSISTENCY_RESULT.UNVERIFIED) {
        browserResult = HE_CONSISTENCY_RESULT.UNVERIFIED;
      } else {
        browserResult = result;
        browserRootCause = rootCause;
        browserCorrection = requiredCorrection;
      }
    } else if (!valuesEqual(def.id, expected, rendered)) {
      browserResult = HE_CONSISTENCY_RESULT.FAIL;
      browserRootCause = "RENDER_CONFLICT — browser value contradicts canonical";
      browserCorrection = `Fix rendered ${def.id} on ${tab}`;
    } else {
      browserResult = HE_CONSISTENCY_RESULT.PASS;
    }

    // Overall row result: worst of API vs browser when browser enabled
    let overall = result;
    if (browserEnabled) {
      if (browserResult === HE_CONSISTENCY_RESULT.FAIL || result === HE_CONSISTENCY_RESULT.FAIL) {
        overall = HE_CONSISTENCY_RESULT.FAIL;
      } else if (
        browserResult === HE_CONSISTENCY_RESULT.UNVERIFIED ||
        result === HE_CONSISTENCY_RESULT.UNVERIFIED
      ) {
        overall = HE_CONSISTENCY_RESULT.UNVERIFIED;
      } else if (result === HE_CONSISTENCY_RESULT.PASS && browserResult === HE_CONSISTENCY_RESULT.PASS) {
        overall = HE_CONSISTENCY_RESULT.PASS;
      } else if (browserResult === HE_CONSISTENCY_RESULT.NOT_TESTED) {
        overall = result;
      }
    }

    const failCause =
      overall === HE_CONSISTENCY_RESULT.FAIL
        ? result === HE_CONSISTENCY_RESULT.FAIL
          ? rootCause
          : browserRootCause || rootCause
        : rootCause;
    const failCorrection =
      overall === HE_CONSISTENCY_RESULT.FAIL
        ? result === HE_CONSISTENCY_RESULT.FAIL
          ? requiredCorrection
          : browserCorrection || requiredCorrection
        : requiredCorrection;

    rows.push({
      tab,
      apiValue: hasKey ? apiValue : null,
      renderedValue: browserEnabled ? rendered ?? null : null,
      result: overall,
      apiResult: result,
      browserResult,
      rootCause: failCause,
      requiredCorrection: failCorrection,
      affectedComponent,
    });
  }

  const failCount = rows.filter((r) => r.result === HE_CONSISTENCY_RESULT.FAIL).length;
  const attributeResult =
    failCount > 0
      ? HE_CONSISTENCY_RESULT.FAIL
      : rows.every(
            (r) =>
              r.result === HE_CONSISTENCY_RESULT.NOT_APPLICABLE ||
              r.result === HE_CONSISTENCY_RESULT.UNVERIFIED ||
              r.result === HE_CONSISTENCY_RESULT.NOT_TESTED
          )
        ? rows.some((r) => r.result === HE_CONSISTENCY_RESULT.UNVERIFIED)
          ? HE_CONSISTENCY_RESULT.UNVERIFIED
          : HE_CONSISTENCY_RESULT.NOT_TESTED
        : rows.some((r) => r.result === HE_CONSISTENCY_RESULT.PASS)
          ? HE_CONSISTENCY_RESULT.PASS
          : HE_CONSISTENCY_RESULT.UNVERIFIED;

  return {
    attributeId: def.id,
    label: def.label,
    unit: def.unit,
    canonicalSource: expectation?.canonicalSource || null,
    canonicalRecordId: expectation?.canonicalRecordId || null,
    expectedValue: expected,
    verificationStatus,
    notes: expectation?.notes || null,
    attributeResult,
    tabResults: rows,
  };
}

/**
 * Build consolidated cross-tab consistency report for one hotel.
 */
export function buildCrossTabConsistencyReport({
  hotelId,
  mode = "Validate",
  hpcHotel = null,
  hpcRecordId = null,
  ownership = null,
  physicalProduct = null,
  portfolioView = null,
  submarketSnapshot = null,
  areaHotels = null,
  contactsPayload = null,
  renderedByTab = null,
  browserEnabled = false,
  shared = null,
} = {}) {
  const vm =
    shared ||
    buildSharedHotelViewModel({
      hpcHotel,
      ownership,
      physicalProduct,
      hotelId,
    });
  const expectations = buildCanonicalExpectations({
    hotelId,
    hpcHotel,
    hpcRecordId: hpcRecordId || hotelId,
    ownership,
    physicalProduct,
    shared: vm,
  });
  const tabApi = extractTabApiValues({
    hpcHotel,
    ownership,
    physicalProduct,
    portfolioView,
    submarketSnapshot,
    areaHotels,
    contactsPayload,
    shared: vm,
  });

  // Without an ownership payload, HI-dependent tabs cannot be audited → NOT_TESTED (not FAIL).
  const ownershipLoaded = Boolean(
    ownership && (ownership.report || ownership.organization || ownership.ownership_and_control)
  );
  if (!ownershipLoaded) {
    for (const tab of ["ownership", "relationships", "contacts", "organization", "sources"]) {
      if (!tabApi[tab]) tabApi[tab] = {};
      tabApi[tab].__ownershipPayloadMissing = true;
    }
  }

  const attributes = ATTR_DEFS.map((def) =>
    scoreAttribute({
      def,
      expectation: expectations.attributes[def.id],
      tabApi,
      renderedByTab: renderedByTab || {},
      browserEnabled: Boolean(browserEnabled),
      ownershipLoaded,
    })
  );

  const failAttrs = attributes.filter((a) => a.attributeResult === HE_CONSISTENCY_RESULT.FAIL);
  const unresolvedVerifiedFails = failAttrs.filter(
    (a) => a.verificationStatus === "Verified" || a.verificationStatus === "Proposed"
  );

  const e2eCertification =
    unresolvedVerifiedFails.length === 0
      ? "PASS"
      : "FAIL — unresolved cross-tab inconsistency on verified/proposed shared attribute(s)";

  // Audit coverage: applicable shared attributes vs attributes actually compared,
  // plus fields intentionally excluded from automated checking.
  const applicableSharedAttributes = ATTR_DEFS.map((d) => d.id);
  const comparedAttributes = attributes
    .filter(
      (a) =>
        a.attributeResult !== HE_CONSISTENCY_RESULT.NOT_TESTED &&
        a.attributeResult !== HE_CONSISTENCY_RESULT.NOT_APPLICABLE
    )
    .map((a) => a.attributeId);
  const notComparedAttributes = attributes
    .filter(
      (a) =>
        a.attributeResult === HE_CONSISTENCY_RESULT.NOT_TESTED ||
        a.attributeResult === HE_CONSISTENCY_RESULT.NOT_APPLICABLE
    )
    .map((a) => ({
      attributeId: a.attributeId,
      attributeResult: a.attributeResult,
      reason:
        a.attributeResult === HE_CONSISTENCY_RESULT.NOT_APPLICABLE
          ? "Not applicable for this hotel / tab set"
          : "Not tested (missing payload, browser skip, or tab not evaluated)",
    }));
  const fieldsExcludedFromAutomatedChecking = [
    {
      field: "guestSentiment",
      reason: "Guest Sentiment tab disabled by founder — excluded from 11-tab consistency set",
    },
    {
      field: "demandSignalStrength",
      reason: "Demand-tab commercial signals are research-derived; cross-tab parity deferred to demand gates",
    },
    {
      field: "accessDriveTimes",
      reason: "Access-tab drive-time matrices use separate TI audit contracts",
    },
    {
      field: "sourceProvenanceUrls",
      reason: "Sources-tab URL inventories are presence-checked via contacts/sources hooks, not full URL equality",
    },
  ];
  const auditCoverage = {
    version: "he-cross-tab-audit-coverage-v1",
    applicableSharedAttributeCount: applicableSharedAttributes.length,
    applicableSharedAttributes,
    attributesActuallyComparedCount: comparedAttributes.length,
    attributesActuallyCompared: comparedAttributes,
    attributesNotComparedCount: notComparedAttributes.length,
    attributesNotCompared: notComparedAttributes,
    fieldsExcludedFromAutomatedChecking,
    coverageRatio:
      applicableSharedAttributes.length > 0
        ? Number(
            (comparedAttributes.length / applicableSharedAttributes.length).toFixed(4)
          )
        : null,
  };

  const gates = {
    [HE_CROSS_TAB_PARITY_GATES.HE_SHARED_HOTEL_ATTRIBUTES_CONSISTENT]:
      unresolvedVerifiedFails.length === 0 ? "YES" : "NO",
    [HE_CROSS_TAB_PARITY_GATES.HE_CROSS_TAB_PARITY_VALIDATED]:
      unresolvedVerifiedFails.length === 0 ? "YES" : "NO",
    [HE_CROSS_TAB_PARITY_GATES.HE_ORG_PORTFOLIO_HYDRATION_VALIDATED]:
      attributes.find((a) => a.attributeId === "portfolioFocusRooms")?.attributeResult ===
      HE_CONSISTENCY_RESULT.PASS
        ? "YES"
        : attributes.find((a) => a.attributeId === "portfolioFocusRooms")?.verificationStatus ===
            "Unverified"
          ? "SKIP"
          : "NO",
    [HE_CROSS_TAB_PARITY_GATES.HE_SUBMARKET_COHORT_VALIDATED]:
      attributes.find((a) => a.attributeId === "submarketCohortId")?.attributeResult ===
        HE_CONSISTENCY_RESULT.PASS ||
      attributes.find((a) => a.attributeId === "submarketCohortId")?.tabResults?.some(
        (t) => t.apiResult === HE_CONSISTENCY_RESULT.PASS
      )
        ? "YES"
        : "NO",
    [HE_CROSS_TAB_PARITY_GATES.HE_SUBMARKET_COVERAGE_DISCLOSED]:
      submarketSnapshot?.coverage?.disclosed === true &&
      submarketSnapshot?.coverage?.marketShareFabricated === false
        ? "YES"
        : submarketSnapshot?.coverage
          ? "PARTIAL"
          : "NO",
    HE_CROSS_TAB_CONSISTENCY_REPORT_GENERATED: "YES",
    HE_E2E_CROSS_TAB_CERTIFICATION: unresolvedVerifiedFails.length === 0 ? "YES" : "NO",
  };

  return {
    version: HE_CROSS_TAB_CONSISTENCY_REPORT_V1,
    generatedAt: new Date().toISOString(),
    mode,
    hotelId: expectations.hotelId,
    hotelName: expectations.hotelName,
    activeTabs: [...HE_ACTIVE_CONSISTENCY_TABS],
    browserEnabled: Boolean(browserEnabled),
    browserPolicy: browserEnabled
      ? "Browser checks evaluated against renderedByTab"
      : "Browser checks forced NOT_TESTED (SkipBrowser) — never PASS",
    rules: [
      "Verified canonical values missing from a tab → FAIL (not missing research)",
      "Genuine unverified facts → UNVERIFIED (never invent)",
      "Proposed ownership must not silently become Validated",
      "Submarket totals must identify cohort; compare like-for-like only",
      "Consistent units/definitions across tabs",
      "Browser-skipped checks → NOT_TESTED, never PASS",
      "Any unresolved verified/proposed cross-tab FAIL → E2E certification FAIL",
    ],
    attributes,
    summary: {
      attributeCount: attributes.length,
      pass: attributes.filter((a) => a.attributeResult === HE_CONSISTENCY_RESULT.PASS).length,
      fail: failAttrs.length,
      unverified: attributes.filter((a) => a.attributeResult === HE_CONSISTENCY_RESULT.UNVERIFIED)
        .length,
      notTested: attributes.filter((a) => a.attributeResult === HE_CONSISTENCY_RESULT.NOT_TESTED)
        .length,
      unresolvedVerifiedOrProposedFails: unresolvedVerifiedFails.map((a) => a.attributeId),
    },
    auditCoverage,
    e2eCertification,
    // Full 11-tab × 8-hotel browser certification remains a separate founder gate.
    FULL_HE_E2E_VALIDATED: "NO",
    ok: unresolvedVerifiedFails.length === 0,
    gates,
  };
}

export function renderCrossTabConsistencyMarkdown(report) {
  const lines = [];
  lines.push(`# Hotel Explorer Cross-Tab Consistency Report`);
  lines.push("");
  lines.push(`- **Version:** ${report.version}`);
  lines.push(`- **Generated:** ${report.generatedAt}`);
  lines.push(`- **Mode:** ${report.mode}`);
  lines.push(`- **Hotel:** ${report.hotelName || "—"} (\`${report.hotelId}\`)`);
  lines.push(`- **Browser:** ${report.browserEnabled ? "enabled" : "skipped → NOT_TESTED"}`);
  lines.push(`- **E2E certification:** **${report.e2eCertification}**`);
  lines.push(`- **FULL_HE_E2E_VALIDATED:** **${report.FULL_HE_E2E_VALIDATED || "NO"}**`);
  lines.push("");
  lines.push(`## Summary`);
  lines.push("");
  lines.push(
    `| PASS | FAIL | UNVERIFIED | NOT_TESTED | Unresolved verified/proposed FAILs |`
  );
  lines.push(`| --- | --- | --- | --- | --- |`);
  lines.push(
    `| ${report.summary.pass} | ${report.summary.fail} | ${report.summary.unverified} | ${report.summary.notTested} | ${(report.summary.unresolvedVerifiedOrProposedFails || []).join(", ") || "—"} |`
  );
  if (report.auditCoverage) {
    const ac = report.auditCoverage;
    lines.push("");
    lines.push(`## Audit coverage`);
    lines.push("");
    lines.push(
      `| Applicable shared attributes | Actually compared | Not compared | Coverage ratio |`
    );
    lines.push(`| --- | --- | --- | --- |`);
    lines.push(
      `| ${ac.applicableSharedAttributeCount} | ${ac.attributesActuallyComparedCount} | ${ac.attributesNotComparedCount} | ${ac.coverageRatio ?? "—"} |`
    );
    lines.push("");
    lines.push(`### Fields excluded from automated checking`);
    lines.push("");
    for (const ex of ac.fieldsExcludedFromAutomatedChecking || []) {
      lines.push(`- **${ex.field}** — ${ex.reason}`);
    }
  }
  lines.push("");
  lines.push(`## Gates`);
  lines.push("");
  for (const [k, v] of Object.entries(report.gates || {})) {
    lines.push(`- \`${k}\` = **${v}**`);
  }
  lines.push("");
  lines.push(`## Attributes`);
  lines.push("");
  for (const attr of report.attributes || []) {
    lines.push(`### ${attr.label} (\`${attr.attributeId}\`) — ${attr.attributeResult}`);
    lines.push("");
    lines.push(`- Canonical source: ${attr.canonicalSource || "—"}`);
    lines.push(`- Canonical record: \`${attr.canonicalRecordId || "—"}\``);
    lines.push(`- Expected (${attr.verificationStatus}): \`${JSON.stringify(attr.expectedValue)}\` (${attr.unit})`);
    if (attr.notes) lines.push(`- Notes: ${attr.notes}`);
    lines.push("");
    lines.push(`| Tab | API value | Rendered | Result | Root cause | Correction |`);
    lines.push(`| --- | --- | --- | --- | --- | --- |`);
    for (const row of attr.tabResults || []) {
      lines.push(
        `| ${row.tab} | ${JSON.stringify(row.apiValue)} | ${JSON.stringify(row.renderedValue)} | ${row.result} | ${row.rootCause || "—"} | ${row.requiredCorrection || "—"} |`
      );
    }
    lines.push("");
  }
  lines.push(`## Rules`);
  lines.push("");
  for (const r of report.rules || []) lines.push(`- ${r}`);
  lines.push("");
  return lines.join("\n");
}

export function writeCrossTabConsistencyArtifacts(report, {
  reportsDir,
  hotelId,
} = {}) {
  const id = hotelId || report.hotelId || "unknown";
  const dir = reportsDir;
  fs.mkdirSync(dir, { recursive: true });
  const jsonPath = path.join(dir, `HE-CROSS-TAB-CONSISTENCY-${id}.json`);
  const mdPath = path.join(dir, `HE-CROSS-TAB-CONSISTENCY-${id}.md`);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  fs.writeFileSync(mdPath, renderCrossTabConsistencyMarkdown(report));
  return { jsonPath, mdPath };
}
