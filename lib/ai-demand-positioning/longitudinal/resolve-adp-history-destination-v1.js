/**
 * Resolve ADP longitudinal history Airtable destination.
 *
 * Production history uses dedicated ADP base tables (9 history tables).
 * Never uses "AI Demand Positioning - Published Reports" as longitudinal SoT.
 */

import { ADP_PUBLISHED_REPORTS_TABLE } from "../airtable-field-map.js";

export const PRODUCTION_HISTORY_DESTINATION_ISOLATION =
  "PRODUCTION_HISTORY_DESTINATION_ISOLATION";

export const LIVE_OVERLAY_TABLE = ADP_PUBLISHED_REPORTS_TABLE;

/** Governed ADP operational base (Live overlay table lives here; history = separate tables). */
export const ADP_GOVERNED_BASE_ID = "appa2cE7FTRmIbB32";
export const ADP_GOVERNED_BASE_NAME = "AI Demand Positioning";

export function isAdpHistoryWritesEnabled() {
  const v = process.env.ADP_HISTORY_WRITES_ENABLED;
  return v === "true" || v === "1";
}

export function resolveSandboxHistoryBaseId() {
  return (
    process.env.ADP_HISTORY_SANDBOX_BASE_ID ||
    process.env.AIRTABLE_BASE_ID_SANDBOX ||
    ""
  );
}

/**
 * Resolve production history base.
 * Prefer explicit ADP_HISTORY_AIRTABLE_BASE_ID; else governed ADP base when confirmed.
 */
export function resolveProductionHistoryDestination({
  confirmGovernedAdpBase = false,
} = {}) {
  const sandboxId = resolveSandboxHistoryBaseId();
  const mainProductId = process.env.AIRTABLE_BASE_ID || "";
  const explicit = (process.env.ADP_HISTORY_AIRTABLE_BASE_ID || "").trim();
  const adpOverlayBase =
    (process.env.ADP_AIRTABLE_BASE_ID || "").trim() || ADP_GOVERNED_BASE_ID;

  let baseId = explicit;
  let resolution = explicit ? "ADP_HISTORY_AIRTABLE_BASE_ID" : null;

  if (!baseId && confirmGovernedAdpBase) {
    baseId = ADP_GOVERNED_BASE_ID;
    resolution = "GOVERNED_ADP_BASE_CONFIRMED";
  }

  const defects = [];
  if (!baseId) {
    defects.push({
      code: "PRODUCTION_HISTORY_BASE_UNRESOLVED",
      detail:
        "Set ADP_HISTORY_AIRTABLE_BASE_ID or confirm governed ADP base appa2cE7FTRmIbB32",
    });
  }
  if (baseId && sandboxId && baseId === sandboxId) {
    defects.push({
      code: "EQUALS_SANDBOX",
      detail: `${baseId} equals sandbox`,
    });
  }

  const isolation = {
    gate: PRODUCTION_HISTORY_DESTINATION_ISOLATION,
    productionHistoryBaseId: baseId || null,
    sandboxBaseId: sandboxId || null,
    mainProductBaseId: mainProductId || null,
    liveOverlayBaseId: adpOverlayBase,
    liveOverlayTable: LIVE_OVERLAY_TABLE,
    liveOverlayTableUsedForHistory: false,
    notSandbox: Boolean(baseId && baseId !== sandboxId),
    notMainProductAccidental: baseId !== mainProductId || null,
    adpAirtableReadLiveOff:
      process.env.ADP_AIRTABLE_READ_LIVE !== "1" &&
      process.env.ADP_AIRTABLE_READ_LIVE !== "true",
  };

  isolation.pass =
    Boolean(baseId) &&
    isolation.notSandbox &&
    isolation.liveOverlayTableUsedForHistory === false &&
    isolation.adpAirtableReadLiveOff &&
    defects.length === 0;

  return {
    ok: isolation.pass,
    baseId: baseId || null,
    baseName: baseId === ADP_GOVERNED_BASE_ID ? ADP_GOVERNED_BASE_NAME : null,
    purpose:
      "ADP longitudinal history (9 tables). Live Published Reports remains overlay-only.",
    coreHistoryIntendedHere: true,
    measurementFamilyIsolation: true,
    resolution,
    isolation,
    defects,
  };
}
