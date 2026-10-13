/**
 * Customer-safe finalization card fields — no internal enums.
 */

export function buildCustomerFinalizationCard({
  lodgingDecision = {},
  fit = {},
  selectionProcess = {},
  evidenceRequest = {},
  disposition = {},
  hotelName = "",
} = {}) {
  const fitLine = fitCustomerLine(fit.targetHotelFit, hotelName);
  return {
    lodgingDecision: lodgingDecision.customerSafe?.lodgingDecisionLine || null,
    hotelSelectionStatus: lodgingDecision.customerSafe?.hotelSelectionLine || null,
    whoControlsIt: lodgingDecision.lodgingControllerName
      ? `Managed by ${lodgingDecision.lodgingControllerName}`
      : null,
    decisionWindow: formatWindow(lodgingDecision.selectionDecisionWindow),
    whyThisHotelFits: fitLine,
    nextAction: evidenceRequest.bestAsk || evidenceRequest.nextEvidenceQuestion || null,
    nextTrigger: triggerLine(selectionProcess, disposition),
    bestRouteIn: lodgingDecision.customerSafe?.bestRouteIn || evidenceRequest.bestContactPath || null,
  };
}

function fitCustomerLine(fitClass, hotelName) {
  const h = hotelName || "This hotel";
  switch (String(fitClass || "").toUpperCase()) {
    case "STRONG_FIT":
      return `${h} shows strong evidenced fit for this demand (location plus supporting capacity / program factors).`;
    case "PLAUSIBLE_FIT":
      return `${h} is a plausible fit based on destination alignment and supporting hotel facts — not confirmed placement.`;
    case "WEAK_FIT":
      return `${h} currently shows weak fit (for example host already locked without evidenced secondary path).`;
    case "NO_FIT":
      return `${h} does not fit this demand geography or requirements.`;
    default:
      return "Target hotel fit is not yet evidenced enough to confirm.";
  }
}

function formatWindow(w = {}) {
  const parts = [];
  if (w.open) parts.push(`opens ${w.open}`);
  if (w.close) parts.push(`closes ${w.close}`);
  if (w.listPublish) parts.push(`list expected ${w.listPublish}`);
  if (w.rateDeadline) parts.push(`rates by ${w.rateDeadline}`);
  return parts.length ? parts.join(" · ") : null;
}

function triggerLine(selectionProcess = {}, disposition = {}) {
  if (disposition.waitingOn) {
    return `Waiting on: ${String(disposition.waitingOn).replace(/_/g, " ").toLowerCase()}`;
  }
  if (selectionProcess.hotelListPublishDate) {
    return `Watch for hotel list / circular around ${selectionProcess.hotelListPublishDate}`;
  }
  if (selectionProcess.selectionStatus === "EXPECTED") {
    return "Watch official accommodation / secretariat pages for hotel-list publication";
  }
  return null;
}
