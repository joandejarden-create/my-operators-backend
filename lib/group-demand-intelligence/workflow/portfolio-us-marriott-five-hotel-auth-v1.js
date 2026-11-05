/**
 * Five-hotel US Marriott Full E2E authorization registry (independent ceilings).
 *
 * GDI: $12 / hotel × 5 = $60 max
 * ADP: $15 / hotel × 4 = $60 max (Bethesda eligible for ADP portfolio;
 *      paid remonitor budget not allocated in this five-hotel wave)
 * Total max: $120
 *
 * No cross-hotel or cross-product transfers. Fail closed at each ledger cap.
 * Tampa Airport reinstatement requires this registry auth + identity separation.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

export const PORTFOLIO_AUTH_WAVE_ID = "portfolio_us_marriott_five_hotel_e2e_v1";
export const PORTFOLIO_AUTH_AUTHORIZED_AT = "2026-10-19T06:50:00.000Z";

export const TAMPA_AIRPORT_MARRIOTT_HOTEL_ID = "recSM9ARs8U9UZ3AZ";
export const WESTIN_TAMPA_WATERSIDE_HOTEL_ID = "recdmo0CnhIZAwHGC";

/** @typedef {"GDI"|"ADP"} PortfolioProduct */

/**
 * @typedef {object} PortfolioHotelAuth
 * @property {string} hotelId
 * @property {string} displayName
 * @property {string} marriottCode
 * @property {string} [adpPropertyId]
 * @property {string} slug
 * @property {boolean} gdiAuthorized
 * @property {boolean} adpAuthorized
 * @property {string|null} gdiAuthorizationId
 * @property {string|null} adpAuthorizationId
 * @property {number} gdiBudgetUsd
 * @property {number} adpBudgetUsd
 */

/** @type {PortfolioHotelAuth[]} */
export const PORTFOLIO_HOTELS = Object.freeze([
  {
    hotelId: "recbRIFhro6tLmSrO",
    displayName: "Charlotte Marriott City Center",
    marriottCode: "CLTCC",
    adpPropertyId: "adp_charlotte_marriott_city_center",
    slug: "charlotte-marriott-city-center",
    gdiAuthorized: true,
    adpAuthorized: true,
    gdiAuthorizationId: "auth_gdi_charlotte_marriott_city_center_12usd_v1",
    adpAuthorizationId: "auth_adp_charlotte_marriott_city_center_15usd_v1",
    gdiBudgetUsd: 12,
    adpBudgetUsd: 15,
  },
  {
    hotelId: TAMPA_AIRPORT_MARRIOTT_HOTEL_ID,
    displayName: "Tampa Airport Marriott",
    marriottCode: "TPAAP",
    adpPropertyId: "adp_tampa_airport_marriott",
    slug: "tampa-airport-marriott",
    gdiAuthorized: true,
    adpAuthorized: true,
    gdiAuthorizationId: "auth_gdi_tampa_airport_marriott_12usd_v1",
    adpAuthorizationId: "auth_adp_tampa_airport_marriott_15usd_v1",
    gdiBudgetUsd: 12,
    adpBudgetUsd: 15,
    westinMustRemainSeparate: WESTIN_TAMPA_WATERSIDE_HOTEL_ID,
  },
  {
    hotelId: "recLuxvwwxID7U2B8",
    displayName: "Bethesda Marriott",
    marriottCode: "WASBT",
    adpPropertyId: "adp_bethesda_marriott",
    slug: "bethesda-marriott",
    gdiAuthorized: true,
    // Portfolio-eligible; this wave's paid ADP budget was not allocated to Bethesda.
    adpAuthorized: false,
    adpPortfolioEligible: true,
    gdiAuthorizationId: "auth_gdi_bethesda_marriott_12usd_v1",
    adpAuthorizationId: null,
    gdiBudgetUsd: 12,
    adpBudgetUsd: 0,
    adpNote:
      "Bethesda is ADP portfolio-eligible (31/31). Paid ADP remonitor not allocated in this five-hotel wave budget — separate authorization required for paid calls.",
  },
  {
    hotelId: "recRkGGNKslJptFIL",
    displayName: "Falls Church Marriott Fairview Park",
    marriottCode: "WASFP",
    adpPropertyId: "adp_falls_church_marriott_fairview_park",
    slug: "falls-church-marriott-fairview-park",
    gdiAuthorized: true,
    adpAuthorized: true,
    gdiAuthorizationId: "auth_gdi_falls_church_marriott_fairview_park_12usd_v1",
    adpAuthorizationId: "auth_adp_falls_church_marriott_fairview_park_15usd_v1",
    gdiBudgetUsd: 12,
    adpBudgetUsd: 15,
  },
  {
    hotelId: "recyZciNlXq5KulnD",
    displayName: "Renaissance Arlington Capital View Hotel",
    marriottCode: "WASPY",
    adpPropertyId: "adp_renaissance_arlington_capital_view",
    slug: "renaissance-arlington-capital-view",
    gdiAuthorized: true,
    adpAuthorized: true,
    gdiAuthorizationId: "auth_gdi_renaissance_arlington_capital_view_12usd_v1",
    adpAuthorizationId: "auth_adp_renaissance_arlington_capital_view_15usd_v1",
    gdiBudgetUsd: 12,
    adpBudgetUsd: 15,
  },
]);

export const PORTFOLIO_GDI_MAX_USD = 60;
export const PORTFOLIO_ADP_MAX_USD = 60;
export const PORTFOLIO_TOTAL_MAX_USD = 120;

const SEPARATE_FROM = Object.freeze([
  "auth_gdi_renaissance_ts_12usd_v1",
  "auth_adp_renaissance_ts_15usd_v1",
  "auth_gdi_westin_tampa_waterside_6usd_v1",
  "Mayflower/San Diego shared $11 GDI ledger",
  "Nobu GDI authorization",
  "Proposed six-hotel remaining-five $30/$57 (superseded by this wave for the five hotels only)",
]);

export function portfolioAuthDataRoot(env = process.env) {
  if (env.PORTFOLIO_US_MARRIOTT_FIVE_HOTEL_AUTH_ROOT) {
    return path.resolve(env.PORTFOLIO_US_MARRIOTT_FIVE_HOTEL_AUTH_ROOT);
  }
  return path.join(
    ROOT,
    "data/group-demand-intelligence/portfolio-us-marriott-five-hotel-e2e-v1"
  );
}

export function findPortfolioHotel(hotelId) {
  const id = String(hotelId || "").trim();
  return PORTFOLIO_HOTELS.find((h) => h.hotelId === id) || null;
}

export function resolvePortfolioAuthorization({
  hotelId,
  product = "GDI",
  authorizationRef = "",
} = {}) {
  const hotel = findPortfolioHotel(hotelId);
  const prod = String(product || "GDI").toUpperCase();
  const ref = String(authorizationRef || "").trim();

  if (!hotel) {
    return {
      ok: false,
      reason: "hotel_not_in_portfolio_auth_wave",
      hotel: null,
      authorizationId: null,
      budgetUsd: 0,
      ledgerPath: null,
    };
  }

  if (prod === "ADP" && !hotel.adpAuthorized) {
    return {
      ok: false,
      // Not portfolio-ineligible — paid budget for this wave was not allocated.
      reason: "adp_paid_budget_not_allocated_this_wave",
      hotel,
      authorizationId: null,
      budgetUsd: 0,
      ledgerPath: null,
      portfolioEligible: hotel.adpPortfolioEligible !== false,
      note:
        hotel.adpNote ||
        "ADP portfolio-eligible; paid remonitor not authorized in this wave",
    };
  }

  const expectedId =
    prod === "ADP" ? hotel.adpAuthorizationId : hotel.gdiAuthorizationId;
  const budgetUsd = prod === "ADP" ? hotel.adpBudgetUsd : hotel.gdiBudgetUsd;

  if (!expectedId) {
    return {
      ok: false,
      reason: "authorization_id_missing_for_product",
      hotel,
      authorizationId: null,
      budgetUsd: 0,
      ledgerPath: null,
    };
  }

  if (ref && ref !== expectedId) {
    return {
      ok: false,
      reason: "authorization_ref_mismatch",
      hotel,
      authorizationId: expectedId,
      providedAuthorizationRef: ref,
      budgetUsd,
      ledgerPath: portfolioLedgerPath(hotel, prod),
    };
  }

  return {
    ok: true,
    reason: "authorized",
    hotel,
    product: prod,
    authorizationId: expectedId,
    budgetUsd,
    ledgerPath: portfolioLedgerPath(hotel, prod),
    separateFrom: SEPARATE_FROM,
    noCrossHotelTransfer: true,
    noCrossProductTransfer: true,
  };
}

export function portfolioLedgerPath(hotel, product = "GDI") {
  const root = portfolioAuthDataRoot();
  const prod = String(product || "GDI").toUpperCase() === "ADP" ? "adp" : "gdi";
  return path.join(root, prod, hotel.slug, "authorization-ledger-v1.json");
}

/**
 * Tampa Airport may run only with its independent auth IDs after identity separation.
 */
export function isTampaAirportReinstatementAuthorized({
  hotelId,
  product = "GDI",
  authorizationRef = "",
  identitySeparationVerified = null,
} = {}) {
  if (String(hotelId || "").trim() !== TAMPA_AIRPORT_MARRIOTT_HOTEL_ID) {
    return { ok: false, reason: "not_tampa_airport_hotel" };
  }
  const resolved = resolvePortfolioAuthorization({
    hotelId,
    product,
    authorizationRef,
  });
  if (!resolved.ok) {
    return { ok: false, reason: resolved.reason, resolved };
  }
  const sep =
    identitySeparationVerified === true ||
    identitySeparationVerified == null
      ? readTampaIdentitySeparationVerified()
      : { verified: identitySeparationVerified === true };

  if (!sep.verified) {
    return {
      ok: false,
      reason: "tampa_identity_separation_not_verified",
      separation: sep,
      resolved,
    };
  }
  return {
    ok: true,
    reason: "reinstated_under_independent_auth",
    authorizationId: resolved.authorizationId,
    product: resolved.product,
    separation: sep,
    resolved,
  };
}

export function readTampaIdentitySeparationVerified() {
  const p = path.join(
    portfolioAuthDataRoot(),
    "tampa-airport",
    "IDENTITY_SEPARATION_VERIFICATION.json"
  );
  if (!fs.existsSync(p)) {
    return { verified: false, path: p, reason: "verification_file_missing" };
  }
  try {
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    return {
      verified: doc.verified === true && doc.westinHotelId === WESTIN_TAMPA_WATERSIDE_HOTEL_ID,
      path: p,
      doc,
    };
  } catch (err) {
    return {
      verified: false,
      path: p,
      reason: err?.message || String(err),
    };
  }
}

/**
 * Forbidden hotel IDs for a coordinator run.
 * Default: keep Airport forbidden unless this run is the reinstated Airport auth.
 */
export function forbiddenHotelIdsForRun({
  hotelId,
  product = "GDI",
  authorizationRef = "",
} = {}) {
  const reinstate = isTampaAirportReinstatementAuthorized({
    hotelId,
    product,
    authorizationRef,
  });
  if (reinstate.ok) return [];
  return [TAMPA_AIRPORT_MARRIOTT_HOTEL_ID];
}

export function buildAuthorizationRegister() {
  const hotels = PORTFOLIO_HOTELS.map((h) => ({
    ...h,
    gdiLedgerPath: h.gdiAuthorized
      ? path.relative(ROOT, portfolioLedgerPath(h, "GDI")).replace(/\\/g, "/")
      : null,
    adpLedgerPath: h.adpAuthorized
      ? path.relative(ROOT, portfolioLedgerPath(h, "ADP")).replace(/\\/g, "/")
      : null,
  }));
  const gdiTotal = hotels.reduce((s, h) => s + (h.gdiAuthorized ? h.gdiBudgetUsd : 0), 0);
  const adpTotal = hotels.reduce((s, h) => s + (h.adpAuthorized ? h.adpBudgetUsd : 0), 0);
  return {
    version: "portfolio-us-marriott-five-hotel-auth-v1",
    waveId: PORTFOLIO_AUTH_WAVE_ID,
    authorizedAt: PORTFOLIO_AUTH_AUTHORIZED_AT,
    currency: "USD",
    ceilings: {
      gdiPerHotelUsd: 12,
      adpPerHotelUsd: 15,
      gdiMaxUsd: PORTFOLIO_GDI_MAX_USD,
      adpMaxUsd: PORTFOLIO_ADP_MAX_USD,
      totalMaxUsd: PORTFOLIO_TOTAL_MAX_USD,
      gdiSumUsd: gdiTotal,
      adpSumUsd: adpTotal,
      totalSumUsd: gdiTotal + adpTotal,
    },
    rules: {
      noCrossHotelTransfer: true,
      noCrossProductTransfer: true,
      failClosedAtEachCap: true,
      bethesdaAdpExcluded: true,
      censusBaseAuthoritative: "AIRTABLE_BASE_ID_ALT",
      censusBaseId: "appCCUsuGsE1ifoLk",
      censusTable: "Hotel Property Census",
      censusTableId: "tbl9aY5ijiuIzzWam",
      ownerFacingRequiresCensusOnlyReconcile: true,
      tampaReinstatementRequiresIdentitySeparation: true,
    },
    separateFrom: SEPARATE_FROM,
    hotels,
  };
}

export function emptyGdiLedger(hotel) {
  return {
    version: "gdi-parallel-budget-ledger-v1",
    currency: "USD",
    budgetUsd: hotel.gdiBudgetUsd,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    actualBilledUsd: null,
    accountingHoldUsd: 0,
    authorization: hotel.gdiAuthorizationId,
    hotelId: hotel.hotelId,
    displayName: hotel.displayName,
    product: "GDI",
    waveId: PORTFOLIO_AUTH_WAVE_ID,
    allowedHotelIds: [hotel.hotelId],
    forbiddenHotelIds:
      hotel.hotelId === TAMPA_AIRPORT_MARRIOTT_HOTEL_ID
        ? [WESTIN_TAMPA_WATERSIDE_HOTEL_ID]
        : hotel.hotelId === WESTIN_TAMPA_WATERSIDE_HOTEL_ID
          ? [TAMPA_AIRPORT_MARRIOTT_HOTEL_ID]
          : [TAMPA_AIRPORT_MARRIOTT_HOTEL_ID].filter(
              (id) => id !== hotel.hotelId
            ),
    separateFrom: SEPARATE_FROM,
    noCrossHotelTransfer: true,
    noCrossProductTransfer: true,
    entries: [],
    note: `Independent GDI ceiling $${hotel.gdiBudgetUsd} under ${hotel.gdiAuthorizationId}. Fail closed at cap.`,
    authorizedAt: PORTFOLIO_AUTH_AUTHORIZED_AT,
    updatedAt: new Date().toISOString(),
  };
}

export function emptyAdpLedger(hotel) {
  return {
    version: "adp-authorization-ledger-v1",
    currency: "USD",
    budgetUsd: hotel.adpBudgetUsd,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    actualBilledUsd: null,
    authorization: {
      authorizationId: hotel.adpAuthorizationId,
      scope: `${hotel.displayName} ADP Full E2E only`,
      propertyId: hotel.adpPropertyId,
      hotelId: hotel.hotelId,
      authorizedAt: PORTFOLIO_AUTH_AUTHORIZED_AT,
      authorizedUsd: hotel.adpBudgetUsd,
      waveId: PORTFOLIO_AUTH_WAVE_ID,
      separateFrom: SEPARATE_FROM,
      noCrossHotelTransfer: true,
      noCrossProductTransfer: true,
    },
    hotelId: hotel.hotelId,
    allowedHotelIds: [hotel.hotelId],
    forbiddenHotelIds:
      hotel.hotelId === TAMPA_AIRPORT_MARRIOTT_HOTEL_ID
        ? [WESTIN_TAMPA_WATERSIDE_HOTEL_ID]
        : [],
    entries: [],
    note: `Independent ADP ceiling $${hotel.adpBudgetUsd} under ${hotel.adpAuthorizationId}. Fail closed at cap.`,
    updatedAt: new Date().toISOString(),
  };
}

/**
 * Write register + empty ledgers. Never overwrites a ledger that already has spend entries.
 */
export function materializePortfolioAuthorizationLedgers({ forceEmpty = false } = {}) {
  const root = portfolioAuthDataRoot();
  fs.mkdirSync(root, { recursive: true });
  const register = buildAuthorizationRegister();
  const registerPath = path.join(root, "AUTHORIZATION_REGISTER.json");
  fs.writeFileSync(registerPath, JSON.stringify(register, null, 2) + "\n");

  const written = [];
  const preserved = [];

  for (const hotel of PORTFOLIO_HOTELS) {
    if (hotel.gdiAuthorized) {
      const p = portfolioLedgerPath(hotel, "GDI");
      fs.mkdirSync(path.dirname(p), { recursive: true });
      if (fs.existsSync(p) && !forceEmpty) {
        const existing = JSON.parse(fs.readFileSync(p, "utf8"));
        if ((existing.entries || []).length > 0 || Number(existing.estimatedSpentUsd || 0) > 0) {
          preserved.push({ product: "GDI", path: p, reason: "existing_spend_preserved" });
          continue;
        }
      }
      const ledger = emptyGdiLedger(hotel);
      ledger.path = p;
      fs.writeFileSync(p, JSON.stringify(ledger, null, 2) + "\n");
      written.push({ product: "GDI", path: p, authorizationId: hotel.gdiAuthorizationId });
    }
    if (hotel.adpAuthorized) {
      const p = portfolioLedgerPath(hotel, "ADP");
      fs.mkdirSync(path.dirname(p), { recursive: true });
      if (fs.existsSync(p) && !forceEmpty) {
        const existing = JSON.parse(fs.readFileSync(p, "utf8"));
        if ((existing.entries || []).length > 0 || Number(existing.estimatedSpentUsd || 0) > 0) {
          preserved.push({ product: "ADP", path: p, reason: "existing_spend_preserved" });
          continue;
        }
      }
      const ledger = emptyAdpLedger(hotel);
      ledger.path = p;
      fs.writeFileSync(p, JSON.stringify(ledger, null, 2) + "\n");
      written.push({ product: "ADP", path: p, authorizationId: hotel.adpAuthorizationId });
    }
  }

  return { registerPath, register, written, preserved, root };
}
