/**
 * Detail Provider Presence panel — factual per-provider measures only.
 * Never coerce non-success → 0%. No recommendation metrics.
 */

import {
  formatProviderLabel,
  KNOWN_AI_VISIBILITY_PROVIDER_IDS,
} from "./provider-dimension.js";
import { AVAILABILITY, CLIENT_FAILURE_STATE_COPY } from "./availability-states.js";
import { computeComparablePresenceDelta } from "./trend-comparability.js";

export const PROVIDER_PRESENCE_PANEL_VERSION =
  "ai_visibility_provider_presence_panel_v1";

/**
 * Normalize a provider monitoring row into panel status grammar.
 */
export function classifyProviderMonitoringStatus(row = {}) {
  if (row.providerError || row.availability === AVAILABILITY.PROVIDER_ERROR) {
    return {
      MONITORING_STATUS: "PROVIDER_ERROR",
      display: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.PROVIDER_ERROR],
      showPresenceRate: false,
    };
  }
  if (row.partial || row.availability === AVAILABILITY.PARTIAL_MONITORING) {
    return {
      MONITORING_STATUS: "PARTIAL_MONITORING",
      display: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.PARTIAL_MONITORING],
      showPresenceRate: typeof row.presenceRate === "number",
    };
  }
  if (row.notComparable || row.availability === AVAILABILITY.NOT_COMPARABLE) {
    return {
      MONITORING_STATUS: "NOT_COMPARABLE",
      display: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NOT_COMPARABLE],
      showPresenceRate: false,
    };
  }
  if (!row.monitored) {
    return {
      MONITORING_STATUS: "NOT_MONITORED",
      display: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NOT_MONITORED],
      showPresenceRate: false,
    };
  }
  if (
    typeof row.presenceRate === "number" &&
    row.presenceRate === 0 &&
    (row.monitoredN || 0) > 0
  ) {
    return {
      MONITORING_STATUS: "NO_PRESENCE_OBSERVED",
      display: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NO_PRESENCE_OBSERVED],
      showPresenceRate: true,
    };
  }
  if (typeof row.presenceRate === "number") {
    return {
      MONITORING_STATUS: "MONITORED",
      display: "Monitored",
      showPresenceRate: true,
    };
  }
  return {
    MONITORING_STATUS: "NOT_MONITORED",
    display: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NOT_MONITORED],
    showPresenceRate: false,
  };
}

function rateDisplay(value, show) {
  if (!show || typeof value !== "number") return "—";
  const pct = Math.round(value * 1000) / 10;
  return `${pct.toFixed(1)}%`;
}

/**
 * Build first-class Provider Presence panel for Brand Detail.
 * @param {{
 *   brandId: string,
 *   providers: Array<object>,
 *   currentPeriod?: string|null,
 *   priorPeriod?: string|null,
 * }} input
 */
export function buildProviderPresencePanel(input = {}) {
  const providers = Array.isArray(input.providers) ? input.providers : [];
  const known = new Set(KNOWN_AI_VISIBILITY_PROVIDER_IDS);
  const byId = new Map();
  for (const p of providers) {
    const id = p.provider || p.id;
    if (id) byId.set(id, p);
  }

  const orderedIds = [
    ...KNOWN_AI_VISIBILITY_PROVIDER_IDS.filter((id) => byId.has(id)),
    ...[...byId.keys()].filter((id) => !known.has(id)),
  ];

  // Also show known providers with no data as NOT_MONITORED
  for (const id of KNOWN_AI_VISIBILITY_PROVIDER_IDS) {
    if (!byId.has(id)) orderedIds.push(id);
  }
  const uniqueIds = [...new Set(orderedIds)];

  const rows = uniqueIds.map((id) => {
    const raw = byId.get(id) || { provider: id, monitored: false };
    const status = classifyProviderMonitoringStatus(raw);
    const currentPeriod =
      raw.currentPeriod ||
      raw.monitoringWindow ||
      input.currentPeriod ||
      null;
    const priorPeriod =
      raw.priorPeriod || input.priorPeriod || null;

    let delta = null;
    let deltaStatus = null;
    if (
      status.showPresenceRate &&
      typeof raw.presenceRate === "number" &&
      typeof raw.priorPresenceRate === "number"
    ) {
      const trend = computeComparablePresenceDelta(
        {
          value: raw.priorPresenceRate,
          provider: id,
          geographyKey: raw.geography,
          language: raw.language,
          periodId: priorPeriod,
        },
        {
          value: raw.presenceRate,
          provider: id,
          geographyKey: raw.geography,
          language: raw.language,
          periodId: currentPeriod,
        }
      );
      if (trend?.ok) {
        delta = trend.deltaPp ?? trend.value;
        deltaStatus = "COMPARABLE";
      } else {
        deltaStatus =
          trend?.status === AVAILABILITY.INSUFFICIENT_HISTORY
            ? "INSUFFICIENT_HISTORY"
            : "NOT_COMPARABLE";
      }
    } else if (!priorPeriod || raw.priorPresenceRate == null) {
      deltaStatus = "INSUFFICIENT_HISTORY";
    }

    const citationRate =
      typeof raw.citationRate === "number" ? raw.citationRate : null;
    const ownedCitationRate =
      typeof raw.ownedSourceCitationRate === "number"
        ? raw.ownedSourceCitationRate
        : null;

    return {
      PROVIDER: id,
      PROVIDER_LABEL: formatProviderLabel(id),
      PRESENCE_RATE: status.showPresenceRate ? raw.presenceRate ?? null : null,
      PRESENCE_RATE_DISPLAY: rateDisplay(raw.presenceRate, status.showPresenceRate),
      PRESENT_N: status.showPresenceRate ? raw.presentN ?? null : null,
      MONITORED_N: status.showPresenceRate ? raw.monitoredN ?? null : null,
      MONITORING_STATUS: status.MONITORING_STATUS,
      MONITORING_STATUS_DISPLAY: status.display,
      CURRENT_PERIOD: currentPeriod,
      PRIOR_COMPARABLE_PERIOD:
        deltaStatus === "COMPARABLE" || deltaStatus === "INSUFFICIENT_HISTORY"
          ? priorPeriod
          : deltaStatus === "NOT_COMPARABLE"
            ? priorPeriod
            : null,
      DELTA_PP: delta,
      DELTA_STATUS: deltaStatus,
      DELTA_DISPLAY:
        deltaStatus === "INSUFFICIENT_HISTORY"
          ? "Insufficient History"
          : deltaStatus === "NOT_COMPARABLE"
            ? "Not Comparable"
            : typeof delta === "number"
              ? `${delta > 0 ? "+" : ""}${Math.round(delta * 10) / 10} pp`
              : "—",
      QUESTIONS_MISSING_N:
        status.showPresenceRate && typeof raw.questionsMissingN === "number"
          ? raw.questionsMissingN
          : status.MONITORING_STATUS === "NOT_MONITORED"
            ? null
            : raw.questionsMissingN ?? null,
      CITATION_RATE: citationRate,
      CITATION_RATE_DISPLAY:
        citationRate == null
          ? status.MONITORING_STATUS === "NOT_MONITORED"
            ? "—"
            : "No citations returned in this monitored cohort."
          : rateDisplay(citationRate, true),
      OWNED_SOURCE_CITATION_RATE: ownedCitationRate,
      OWNED_SOURCE_CITATION_RATE_DISPLAY:
        ownedCitationRate == null
          ? raw.ownedDomainStatus === "MISSING_GOVERNED_SOURCE"
            ? "No official brand website has been configured."
            : "—"
          : rateDisplay(ownedCitationRate, true),
      RECOMMENDATION_METRICS: false,
      drilldown: {
        provider: id,
        supportsPromptFamily: true,
        supportsQuestion: true,
        supportsEvidence: true,
        lazyEvidenceEndpoint: true,
      },
    };
  });

  return {
    version: PROVIDER_PRESENCE_PANEL_VERSION,
    brandId: input.brandId || null,
    CURRENT_PERIOD: input.currentPeriod || null,
    PRIOR_COMPARABLE_PERIOD: input.priorPeriod || null,
    rows,
    RECOMMENDATION_METRICS: false,
    MISSING_IS_NOT_ZERO: true,
    CLIENT_COPY:
      "Per-provider Observed Presence from successful monitoring. Not Monitored and Provider Error are never shown as 0%.",
  };
}
