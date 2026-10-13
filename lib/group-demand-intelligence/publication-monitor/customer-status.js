/**
 * Customer-safe Watch card fields for publication monitoring.
 * No crawler/debug/hash exposure.
 */

import { CUSTOMER_MONITORING_LABEL } from "./constants.js";

/**
 * Build customer-facing monitoring status patch for a Future Watch opportunity.
 */
export function buildCustomerWatchMonitorStatus(monitor = {}, opts = {}) {
  const labels =
    Array.isArray(monitor.customerMonitoringFor) && monitor.customerMonitoringFor.length
      ? monitor.customerMonitoringFor
      : (monitor.watchForTypes || [])
          .map((t) => CUSTOMER_MONITORING_LABEL[t] || null)
          .filter(Boolean);

  const unique = [...new Set(labels)];
  const lastChecked = monitor.lastCheckedAt
    ? String(monitor.lastCheckedAt).slice(0, 10)
    : null;
  const nextExpected =
    monitor.expectedPublicationWindowStart ||
    monitor.nextCheckAt ||
    monitor.milestoneDates?.abstractDeadline ||
    monitor.milestoneDates?.standAllocation ||
    null;

  const monitoringLine =
    unique.length > 0
      ? `Monitoring for: ${unique.join("; ")}`
      : "Monitoring official sources for list publication";

  return {
    watchCardMonitoringStatus: monitoringLine,
    watchCardMonitoringLastChecked: lastChecked,
    watchCardMonitoringNextExpected: nextExpected
      ? `Next expected trigger window: ${String(nextExpected).slice(0, 10)}`
      : null,
    // Keep legacy next-trigger field aligned without exposing hashes
    watchCardNextTrigger:
      opts.preserveExistingNextTrigger || monitor.customerNextTriggerNote
        ? opts.preserveExistingNextTrigger || monitor.customerNextTriggerNote
        : unique.length
          ? `${monitoringLine}${nextExpected ? ` · expected from ${String(nextExpected).slice(0, 10)}` : ""}`
          : null,
  };
}

/**
 * Apply customer-safe monitor fields onto opportunity objects linked to a campaign.
 * Does not change Ready/Watch gates.
 */
export function applyMonitorStatusToOpportunities(opportunities = [], monitor = {}) {
  const campId = monitor.campaignId;
  const patch = buildCustomerWatchMonitorStatus(monitor);
  return (opportunities || []).map((o) => {
    const linked =
      o.campaignId === campId ||
      o.parentCampaignId === campId ||
      (Array.isArray(o.campaignIds) && o.campaignIds.includes(campId)) ||
      String(o.id || "").includes(String(campId || "").slice(0, 20));
    if (!linked && !optsMatchByTitle(o, monitor)) return o;
    return {
      ...o,
      ...patch,
      monitoringTriggerType: monitor.triggerType || o.monitoringTriggerType,
      monitoringTriggerSource: monitor.triggerSourceUrl || o.monitoringTriggerSource,
      nextResearchDate: monitor.nextCheckAt || o.nextResearchDate,
    };
  });
}

function optsMatchByTitle(o, monitor) {
  const blob = `${o.displayTitle || ""} ${o.title || ""} ${o.organizationName || ""}`.toLowerCase();
  const key = String(monitor.campaignKey || "").toLowerCase();
  if (!key) return false;
  if (key === "iaps") return /iaps|spaces in transition/i.test(blob);
  if (key === "biocultura") return /biocultura/i.test(blob);
  if (key === "rif") return /filosof|rif|iberoamerican/i.test(blob);
  if (key === "cielo") return /cielo/i.test(blob);
  if (key === "autoamericas") return /autoamericas|autoam[eé]ricas/i.test(blob);
  return false;
}
