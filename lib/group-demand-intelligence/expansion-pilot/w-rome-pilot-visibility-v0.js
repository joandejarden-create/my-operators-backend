/**
 * Leaf visibility helpers for W Rome expansion pilot V0.
 * Kept free of readiness/corpus imports to avoid circular dependency with customer-visibility.
 */

export const W_ROME_HOTEL_ID = "rece0or38cxo3Fymb";
export const W_ROME_EXPANSION_PILOT_ID = "w_rome_evidence_backed_expansion_pilot_v0";
export const W_ROME_EXPANSION_PILOT_ENV = "GDI_WROME_EXPANSION_PILOT_V0";

function truthy(raw) {
  const v = String(raw || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

export function isWRomeExpansionPilotEnabled(env = process.env) {
  return truthy(env[W_ROME_EXPANSION_PILOT_ENV]);
}

/**
 * QUALIFIED pilot rows may appear on the customer surface when the W Rome
 * pilot flag is on — without treating them as Ready/ACTIONABLE.
 */
export function isWRomePilotQualifiedCustomerVisible(opp = {}, env = process.env) {
  if (!isWRomeExpansionPilotEnabled(env)) return false;
  if (String(opp.hotelId || "") !== W_ROME_HOTEL_ID) return false;
  if (opp.expansionPilotId !== W_ROME_EXPANSION_PILOT_ID) return false;
  if (opp.isTestData === true || opp.isDemandGenerator === true) return false;
  const maturity = String(opp.gdiMaturityState || "");
  if (maturity !== "QUALIFIED" && maturity !== "ACTIONABLE") return false;
  if (opp.lodgingVerified === true) return false;
  if (!opp.modeledDemandDisclaimer && opp.modeledRoomsMin == null) return false;
  if (!opp.travelingCohortSummary) return false;
  if (!opp.organizationName) return false;
  return true;
}
