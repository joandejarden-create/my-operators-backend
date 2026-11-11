/**
 * Hotel execution-scope preflight V1
 *
 * A city name (e.g. "Tampa") cannot select a property. Paid submissions and
 * publication writes require an explicit match on:
 *   - approved hotel name (full property name, not city/market alone)
 *   - canonical hotel ID
 *   - product (GDI / ADP)
 *   - authorization id
 *
 * Fail-closed on mismatch. Dry-run may still diagnose without spending.
 */

import { isTampaAirportReinstatementAuthorized } from "./portfolio-us-marriott-five-hotel-auth-v1.js";

const CITY_ONLY_BLOCK_REASON = "city_name_cannot_select_property";

function text(v) {
  return String(v ?? "")
    .trim()
    .replace(/\s+/g, " ");
}

function norm(v) {
  return text(v).toLowerCase();
}

/**
 * True when the selector is only a city/market label (or empty), not a property name.
 * @param {string} selector
 * @param {{ city?: string, market?: string, displayName?: string, identity?: { city?: string, market?: string } }} hotelConfig
 */
export function isCityOnlyPropertySelector(selector, hotelConfig = {}) {
  const s = norm(selector);
  if (!s) return true;
  const city = norm(
    hotelConfig.city ||
      hotelConfig.identity?.city ||
      hotelConfig.geo?.city ||
      ""
  );
  const market = norm(
    hotelConfig.market ||
      hotelConfig.identity?.market ||
      hotelConfig.demandTerritory?.label ||
      ""
  );
  const display = norm(hotelConfig.displayName || hotelConfig.identity?.hotelName || "");

  if (city && (s === city || s === `${city} fl` || s === `${city} florida`)) {
    return true;
  }
  if (market && (s === market || s.startsWith(`${city} /`) || s.startsWith(`${city}/`))) {
    // Bare market / "Tampa / Harbour Island…" without property brand tokens
    if (display && !s.includes("westin") && !s.includes("marriott") && !s.includes("waterside")) {
      return true;
    }
    if (!display) return true;
  }

  // Single-token city-like selector (no brand/property words)
  const tokens = s.split(/[\s,/.-]+/).filter(Boolean);
  if (tokens.length <= 2) {
    const brandHints = [
      "westin",
      "marriott",
      "hilton",
      "hyatt",
      "wyndham",
      "renaissance",
      "nobu",
      "waterside",
      "bayside",
      "airport",
      "grand",
      "hotel",
      "resort",
      "inn",
    ];
    const hasBrand = tokens.some((t) => brandHints.includes(t));
    if (!hasBrand && city && tokens.every((t) => city.includes(t) || t === "fl" || t === "florida")) {
      return true;
    }
  }
  return false;
}

/**
 * @param {object} opts
 * @returns {{ ok: boolean, blockers: string[], checks: Record<string, unknown> }}
 */
export function assertHotelExecutionScope(opts = {}) {
  const blockers = [];
  const checks = {};

  const hotelId = text(opts.hotelId);
  const approvedHotelId = text(opts.approvedHotelId || opts.hotelId);
  const approvedHotelName = text(opts.approvedHotelName);
  const product = text(opts.product || "GDI").toUpperCase();
  const approvedProduct = text(opts.approvedProduct || product).toUpperCase();
  const authorizationRef = text(opts.authorizationRef);
  const approvedAuthorizationId = text(
    opts.approvedAuthorizationId || authorizationRef
  );
  const hotelConfig = opts.hotelConfig || {};
  const configHotelId = text(hotelConfig.hotelId);
  const configDisplayName = text(
    hotelConfig.displayName || hotelConfig.identity?.hotelName
  );
  const requirePaidGate = opts.requirePaidGate === true;
  const forbiddenHotelIds = Array.isArray(opts.forbiddenHotelIds)
    ? opts.forbiddenHotelIds.map(text).filter(Boolean)
    : [];

  checks.hotelId = hotelId || null;
  checks.approvedHotelId = approvedHotelId || null;
  checks.approvedHotelName = approvedHotelName || null;
  checks.product = product;
  checks.approvedProduct = approvedProduct;
  checks.authorizationRef = authorizationRef || null;
  checks.approvedAuthorizationId = approvedAuthorizationId || null;

  if (!hotelId) {
    blockers.push("canonical_hotel_id_missing");
  }
  if (approvedHotelId && hotelId && approvedHotelId !== hotelId) {
    blockers.push("canonical_hotel_id_mismatch");
  }
  if (configHotelId && hotelId && configHotelId !== hotelId) {
    blockers.push("config_hotel_id_mismatch");
  }
  if (forbiddenHotelIds.includes(hotelId)) {
    blockers.push("forbidden_hotel_id");
  }

  if (!approvedHotelName) {
    if (requirePaidGate) blockers.push("approved_hotel_name_required");
  } else if (isCityOnlyPropertySelector(approvedHotelName, hotelConfig)) {
    blockers.push(CITY_ONLY_BLOCK_REASON);
  } else if (
    configDisplayName &&
    norm(approvedHotelName) !== norm(configDisplayName) &&
    !norm(configDisplayName).includes(norm(approvedHotelName)) &&
    !norm(approvedHotelName).includes(norm(configDisplayName))
  ) {
    // Allow minor punctuation differences; block hard name swaps (Airport vs Waterside)
    const approvedTokens = new Set(norm(approvedHotelName).split(/\s+/));
    const configTokens = new Set(norm(configDisplayName).split(/\s+/));
    const distinctive = [...approvedTokens].filter(
      (t) => t.length > 3 && !["tampa", "hotel", "the", "and"].includes(t)
    );
    const overlap = distinctive.filter((t) => configTokens.has(t));
    if (overlap.length < Math.min(2, distinctive.length)) {
      blockers.push("approved_hotel_name_mismatch");
    }
  }

  if (approvedProduct && product !== approvedProduct) {
    blockers.push("product_mismatch");
  }

  if (requirePaidGate && !authorizationRef) {
    blockers.push("authorization_ref_required");
  }
  if (
    authorizationRef &&
    approvedAuthorizationId &&
    authorizationRef !== approvedAuthorizationId
  ) {
    blockers.push("authorization_mismatch");
  }

  // Tampa Airport Marriott: superseded after mistaken Westin activation unless
  // independently reinstated (distinct auth + identity separation verified).
  if (
    hotelId === "recSM9ARs8U9UZ3AZ" ||
    /tampa airport marriott/i.test(approvedHotelName) ||
    /tampa airport marriott/i.test(configDisplayName)
  ) {
    let reinstate =
      opts.tampaAirportReinstatementAuthorized === true ||
      opts.blockAirportMarriott === false;
    // Auto-resolve from portfolio auth wave when caller passes hotel+product+auth
    // but omits the boolean (dry-run preflight / shared coordinators).
    if (
      !reinstate &&
      opts.tampaAirportReinstatementAuthorized !== false &&
      opts.blockAirportMarriott !== true &&
      authorizationRef
    ) {
      const check = isTampaAirportReinstatementAuthorized({
        hotelId,
        product,
        authorizationRef,
      });
      reinstate = check.ok === true;
      checks.tampaAirportReinstatementLookup = check.reason || null;
    }
    checks.tampaAirportReinstatementAuthorized = reinstate;
    if (!reinstate) {
      blockers.push("tampa_airport_marriott_superseded");
    }
  }

  return {
    ok: blockers.length === 0,
    blockers,
    checks,
    cityOnlyBlockReason: CITY_ONLY_BLOCK_REASON,
  };
}

/**
 * Throws when scope fails and paid/write work is requested.
 */
export function enforceHotelExecutionScopeOrThrow(opts = {}) {
  const result = assertHotelExecutionScope(opts);
  if (result.ok) return result;
  const err = new Error(
    `hotel_execution_scope_blocked:${result.blockers.join(",")}`
  );
  err.code = "HOTEL_EXECUTION_SCOPE_BLOCKED";
  err.blockers = result.blockers;
  err.checks = result.checks;
  throw err;
}

export { CITY_ONLY_BLOCK_REASON };
