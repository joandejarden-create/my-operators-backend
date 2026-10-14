/**
 * Webhound is RETIRED as a live GDI research provider.
 * Historical evidence (webhoundUsed stamps, sources, run artifacts) remains valid.
 * Do not reconnect the API, spawn obsolete Webhound scripts, or gate workflow on it.
 */

export const WEBHOUND_PROVIDER_STATUS = Object.freeze({
  status: "RETIRED",
  liveInvocationAllowed: false,
  preserveHistoricalEvidence: true,
  note:
    "Webhound historical packets and source URLs remain reusable research evidence. New discovery uses Parallel (optional) or other active providers.",
});

export function isWebhoundLiveInvocationAllowed() {
  return false;
}

export function annotateWebhoundHistorical(opp = {}) {
  if (!opp || typeof opp !== "object") return opp;
  if (opp.webhoundUsed !== true && !/webhound/i.test(String(opp.discoverySource || ""))) {
    return opp;
  }
  return {
    ...opp,
    researchProviderHistorical: "WEBHOUND",
    researchProviderLive: null,
    webhoundRetiredNote: WEBHOUND_PROVIDER_STATUS.note,
  };
}
