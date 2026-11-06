/**
 * Customer-facing GDI property-bar research status.
 * Never surface run IDs, costs, campaign IDs, or raw internal codes.
 *
 * @param {{ runStatus?: string, runStatusLabel?: string }|null|undefined} summary
 * @returns {string}
 */
export function customerSafeRunStatusLabel(summary) {
  const s = summary || {};
  const code = String(s.runStatus || "").trim().toUpperCase();
  const label = String(s.runStatusLabel || "").trim();
  if (!code && !label) return "History unavailable";
  if (code === "NEVER_RUN" || code === "NOT_YET_RUN") return "Not run yet";
  if (code === "STATUS_UNAVAILABLE" || code === "HISTORY_UNAVAILABLE") {
    return "History unavailable";
  }
  if (
    code === "COMPLETED" ||
    code === "COMPLETE" ||
    code === "SUCCESS" ||
    /complet/i.test(label)
  ) {
    return "Research Completed";
  }
  if (/fail|error/i.test(code) || /fail|error/i.test(label)) {
    return "Research unsuccessful";
  }
  if (
    /run|progress|active|pending|started/i.test(code) ||
    /progress|running/i.test(label)
  ) {
    return "Research in progress";
  }
  if (/^gdi_run_/i.test(label) || /\$|webhound|campaign/i.test(label)) {
    return "Research Completed";
  }
  if (label && label.length <= 40 && !/[0-9a-f]{8,}/i.test(label)) {
    return label;
  }
  return "Research Completed";
}

/**
 * Strip internal research-audit fields from customer/share summary payloads.
 * @param {object|null|undefined} summary
 * @returns {object|null|undefined}
 */
export function redactCustomerFacingGdiSummary(summary) {
  if (!summary || typeof summary !== "object") return summary;
  const out = { ...summary };
  delete out.latestRunId;
  delete out.researchCostUsd;
  delete out.costBreakdown;
  delete out.runHistoryProvenance;
  out.runStatusLabel = customerSafeRunStatusLabel(out);
  return out;
}
