/**
 * Canonical data availability + connection states (Phase 3C.1).
 * Never use 0 when connection is missing.
 */

export const DISCOVERABILITY_DATA_STATES_VERSION =
  "ai_visibility_discoverability_data_states_v1";

export const DATA_STATE = Object.freeze({
  MEASURED: "MEASURED",
  MEASURABLE_PUBLICLY: "MEASURABLE_PUBLICLY",
  CONNECTION_REQUIRED: "CONNECTION_REQUIRED",
  NOT_MONITORED: "NOT_MONITORED",
  UNAVAILABLE: "UNAVAILABLE",
});

export const CONNECTION_STATE = Object.freeze({
  CONNECTED: "CONNECTED",
  CONNECTION_REQUIRED: "CONNECTION_REQUIRED",
  CONFIGURATION_REQUIRED: "CONFIGURATION_REQUIRED",
  ERROR: "ERROR",
  NOT_SUPPORTED: "NOT_SUPPORTED",
});

export const CONNECTION_REQUIRED_COPY = Object.freeze({
  ANALYTICS: "Analytics Connection Required",
  SERVER_CDN_LOG: "Server/CDN Log Connection Required",
  SEARCH_CONSOLE: "Search Console Connection Required",
  EVENT_CONFIG: "Event Configuration Required",
  CRM: "CRM Connection Required",
  URL_INVENTORY: "Priority Page URL Required",
  GENERIC: "Connection Required",
});

/**
 * Resolve UI display for a metric given its data state.
 * @param {{ dataState: string, value?: number|null, label?: string }} args
 */
export function resolveDataStateDisplay(args = {}) {
  const state = args.dataState || DATA_STATE.UNAVAILABLE;
  switch (state) {
    case DATA_STATE.MEASURED:
      return {
        dataState: state,
        value: args.value ?? null,
        display: args.value != null ? String(args.value) : "Measured",
        synthetic: false,
      };
    case DATA_STATE.MEASURABLE_PUBLICLY:
      return {
        dataState: state,
        value: args.value ?? null,
        display:
          args.value != null
            ? String(args.value)
            : args.connectionCopy || "Public crawl baseline not yet run",
        synthetic: false,
      };
    case DATA_STATE.CONNECTION_REQUIRED:
      return {
        dataState: state,
        value: null,
        display: args.connectionCopy || CONNECTION_REQUIRED_COPY.GENERIC,
        synthetic: false,
      };
    case DATA_STATE.NOT_MONITORED:
      return {
        dataState: state,
        value: null,
        display: "Not Monitored",
        synthetic: false,
      };
    default:
      return {
        dataState: DATA_STATE.UNAVAILABLE,
        value: null,
        display: "Unavailable",
        synthetic: false,
      };
  }
}

/**
 * Guard: never coerce missing connection to zero.
 */
export function guardAgainstSyntheticZero(dataState, value) {
  if (
    dataState === DATA_STATE.CONNECTION_REQUIRED ||
    dataState === DATA_STATE.NOT_MONITORED ||
    dataState === DATA_STATE.UNAVAILABLE
  ) {
    return null;
  }
  return value;
}
