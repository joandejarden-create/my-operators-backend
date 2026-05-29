/**
 * Shared Brand Deal Request pipeline buckets and persona KPI counts.
 * Owner "Awaiting brand" uses the same rules as brand "Brand action" (ball with brand).
 * Owner "Owner action" uses the same rules as brand "Awaiting owner" (awaiting-info bucket).
 */

export const BRAND_ACTION_LABELS = new Set([
  "Review new opportunity",
  "Mark decision",
  "Send NDA",
  "Open deal room",
  "Review documents",
  "Prepare preliminary terms",
  "Request missing owner information",
  "Internal review",
  "Follow up with owner",
  "Awaiting signed NDA",
]);

export const OWNER_ACTION_LABELS = new Set([
  "Provide requested information",
  "Send term entry link",
  "Owner to compare terms",
  "Open Deal Room",
  "Track LOI & feasibility",
  "Complete checklist",
  "Platform exit",
]);

const PASSED_STATUSES = new Set(["Declined", "Responded - Declined"]);

/**
 * @param {object} row - BDR-shaped row (status or _requestStatus, ndaStatus, etc.)
 */
export function normalizeWorkspaceRow(row) {
  const r = row && typeof row === "object" ? row : {};
  const status = String(r._requestStatus ?? r.status ?? "").trim();
  const proposalStatus =
    r.proposalStatus != null
      ? String(r.proposalStatus)
      : r.proposal && r.proposal.proposalStatus != null
        ? String(r.proposal.proposalStatus)
        : "";
  return {
    ...r,
    _requestStatus: status,
    ndaStatus: r.ndaStatus != null ? String(r.ndaStatus).trim() : "",
    dealRoomAccess: r.dealRoomAccess != null ? String(r.dealRoomAccess).trim() : "",
    proposalStatus: String(proposalStatus || "").trim(),
    requestSentAt: r.requestSentAt ?? "",
    nextFollowupDate: r.nextFollowupDate ?? null,
    lastUpdated: r.lastUpdated ?? "",
    responseDate: r.responseDate ?? "",
    lastActivity: r.lastActivity ?? null,
  };
}

export function parseDateMs(v) {
  if (v == null || v === "") return null;
  const d = new Date(String(v).trim());
  const t = d.getTime();
  return Number.isNaN(t) ? null : t;
}

export function computeLastActivityMs(row) {
  const r = normalizeWorkspaceRow(row);
  const times = [
    parseDateMs(r.lastUpdated),
    parseDateMs(r.responseDate),
    parseDateMs(r.requestSentAt),
    parseDateMs(r.lastActivity),
  ].filter((t) => t != null);
  if (!times.length) return null;
  return Math.max(...times);
}

export function deriveWorkspaceBucket(row) {
  try {
    const r = normalizeWorkspaceRow(row);
    const st = r._requestStatus;
    if (["Declined", "Responded - Declined", "Archived"].includes(st)) return "archived";
    if (st === "Revisit Later") return "advanced";
    if (st === "More Info Requested") return "awaiting-info";
    const nda = r.ndaStatus;
    const dra = r.dealRoomAccess;
    const prop = r.proposalStatus;
    const inNdaFlow =
      st === "Deal Room Active" ||
      nda === "Not Sent" ||
      nda === "Sent" ||
      (nda === "Signed - Owner Confirmed" && (!dra || dra !== "Granted"));
    if (inNdaFlow) return "nda-room";
    if (["Pre-LOI", "Pre-LOI / Term Comparison"].includes(st) || prop === "Draft" || prop === "Submitted")
      return "terms-proposal";
    if (
      ["Finalist", "Feasibility", "Feasibility In Progress", "LOI Signed", "LOI Signed / Platform Exit"].includes(st)
    )
      return "advanced";
    if (["Accepted", "Responded - Accepted"].includes(st)) return "awaiting-info";
    if (["Brand Viewed", "Operator Viewed", "Viewed"].includes(st)) return "active-review";
    if (["New", "Sent / Awaiting Response"].includes(st) || !st) return "new";
    return "awaiting-info";
  } catch {
    return "new";
  }
}

export function deriveBrandNextAction(row) {
  const r = normalizeWorkspaceRow(row);
  const st = r._requestStatus;
  const nda = r.ndaStatus;
  const dra = r.dealRoomAccess;
  const prop = r.proposalStatus;
  if (["Declined", "Archived", "Responded - Declined"].includes(st)) return "No action required";
  if (st === "New" || st === "Sent / Awaiting Response" || !st) return "Review new opportunity";
  if (st === "More Info Requested") return "Follow up with owner";
  if (st === "Revisit Later") return "Revisit later";
  if (st === "Brand Viewed" || st === "Operator Viewed" || st === "Viewed") return "Mark decision";
  if (nda === "Not Sent" || nda === "") return "Send NDA";
  if (nda === "Sent") return "Awaiting signed NDA";
  if (nda === "Signed - Owner Confirmed" && dra !== "Granted") return "Open deal room";
  if (dra === "Granted" && prop !== "Submitted") return "Review documents";
  if (prop === "Draft") return "Prepare preliminary terms";
  if (["Pre-LOI", "Pre-LOI / Term Comparison"].includes(st)) return "Prepare preliminary terms";
  if (prop === "Submitted") return "Follow up with owner";
  if (["Accepted", "Responded - Accepted"].includes(st)) return "Request missing owner information";
  if (["Finalist", "Feasibility", "Feasibility In Progress"].includes(st)) return "Internal review";
  if (["LOI Signed", "LOI Signed / Platform Exit"].includes(st)) return "Revisit later";
  return "Follow up with owner";
}

/** Neutral next-step label for operator workspace (Phase 4). */
export function deriveOperatorNextAction(row) {
  const r = normalizeWorkspaceRow(row);
  const st = r._requestStatus;
  if (["Declined", "Archived", "Responded - Declined"].includes(st)) return "No further action";
  if (st === "New" || st === "Sent / Awaiting Response" || !st) return "Review owner request";
  if (st === "Operator Viewed" || st === "Viewed" || st === "Brand Viewed") {
    return "Record review considerations";
  }
  if (st === "More Info Requested") return "Awaiting owner response";
  if (["Accepted", "Responded - Accepted"].includes(st)) return "Prepare next step";
  if (st === "Revisit Later") return "Revisit when ready";
  if (["Pre-LOI", "Pre-LOI / Term Comparison"].includes(st)) return "Terms review (future phase)";
  if (["Finalist", "Feasibility", "Feasibility In Progress"].includes(st)) {
    return "Advanced review (future phase)";
  }
  if (["LOI Signed", "LOI Signed / Platform Exit"].includes(st)) return "Revisit when ready";
  if (st === "Deal Room Active") return "Shared workspace (future phase)";
  const nda = r.ndaStatus;
  if (nda === "Not Sent" || nda === "Sent") return "Confidentiality step (future phase)";
  return "Update follow-up";
}

export function deriveOwnerNextAction(row) {
  const r = normalizeWorkspaceRow(row);
  const st = r._requestStatus || "New";
  if (["New", "Viewed", "Sent / Awaiting Response"].includes(st)) return "Awaiting brand response";
  if (["Accepted", "Responded - Accepted"].includes(st)) return "Send term entry link";
  if (["Pre-LOI", "Pre-LOI / Term Comparison"].includes(st)) return "Owner to compare terms";
  if (st === "Finalist") return "Open Deal Room";
  if (st === "Deal Room Active") return "Track LOI & feasibility";
  if (["Feasibility", "Feasibility In Progress"].includes(st)) return "Complete checklist";
  if (["LOI Signed", "LOI Signed / Platform Exit"].includes(st)) return "Platform exit";
  if (["Declined", "Archived", "Responded - Declined"].includes(st)) return "—";
  if (st === "More Info Requested") return "Provide requested information";
  return "—";
}

export function enrichWorkspaceRow(row) {
  const base = normalizeWorkspaceRow(row);
  const workspaceBucket = deriveWorkspaceBucket(base);
  const brandNextAction = deriveBrandNextAction(base);
  const ownerNextAction = deriveOwnerNextAction(base);
  const lastActivitySort = computeLastActivityMs(base);
  return {
    ...base,
    workspaceBucket,
    brandNextAction,
    ownerNextAction,
    lastActivitySort,
  };
}

export function isPassedArchived(row) {
  const st = normalizeWorkspaceRow(row)._requestStatus;
  return PASSED_STATUSES.has(st);
}

export function isStalledRow(row) {
  const e = enrichWorkspaceRow(row);
  if (e.workspaceBucket === "archived") return false;
  const fu = parseDateMs(e.nextFollowupDate);
  if (fu != null) {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    if (fu < start.getTime()) return true;
  }
  if (e.lastActivitySort != null && Date.now() - e.lastActivitySort > 14 * 86400000) return true;
  return false;
}

/** Ball with brand — mirrors brand "Brand action" KPI. */
export function isAwaitingBrand(row) {
  const e = enrichWorkspaceRow(row);
  if (e.workspaceBucket === "archived") return false;
  if (e.workspaceBucket === "awaiting-info") return false;
  if (e.workspaceBucket === "new" || e.workspaceBucket === "active-review") return true;
  return BRAND_ACTION_LABELS.has(e.brandNextAction);
}

/** Ball with owner — mirrors brand "Awaiting owner" KPI. */
export function isAwaitingOwner(row) {
  const e = enrichWorkspaceRow(row);
  if (e.workspaceBucket === "archived") return false;
  if (e.workspaceBucket === "awaiting-info") return true;
  // NDA / intake / brand review — brand's turn even if owner has a generic next-step label
  if (["new", "active-review", "nda-room"].includes(e.workspaceBucket)) return false;
  return OWNER_ACTION_LABELS.has(e.ownerNextAction);
}

export function countRequestSentInRange(rows, startMs, endMs) {
  return rows.filter((r) => {
    const t = parseDateMs(normalizeWorkspaceRow(r).requestSentAt);
    return t != null && t >= startMs && t < endMs;
  }).length;
}

export function pipelineStageMeta(stageRows) {
  const now = Date.now();
  const d7 = now - 7 * 86400000;
  let n = 0;
  let a = 0;
  let s = 0;
  stageRows.forEach((r) => {
    const e = enrichWorkspaceRow(r);
    const sent = parseDateMs(e.requestSentAt);
    if (sent != null && sent >= d7) n += 1;
    const last = e.lastActivitySort;
    if (last != null && last >= d7 && (sent == null || sent < d7)) a += 1;
    if (isStalledRow(e)) s += 1;
  });
  const parts = [];
  if (n) parts.push({ html: "New (7d): " + n, warn: false });
  if (a) parts.push({ html: "Active (7d): " + a, warn: false });
  if (s) parts.push({ html: "Stalled: " + s, warn: true });
  if (!parts.length) return "";
  return parts;
}

/**
 * @param {object[]} rows - BDR rows
 * @param {'brand'|'owner'|'operator'} persona
 */
export function computeWorkspaceKpiSnapshot(rows, persona) {
  const enriched = (rows || []).map(enrichWorkspaceRow);
  const active = enriched.filter((r) => r.workspaceBucket !== "archived");
  const now = Date.now();

  const awaitingBrand = enriched.filter(isAwaitingBrand).length;
  const awaitingOwner = enriched.filter(isAwaitingOwner).length;
  const brandAction = enriched.filter((r) => {
    if (r.workspaceBucket === "archived") return false;
    return BRAND_ACTION_LABELS.has(r.brandNextAction);
  }).length;
  const ownerAction = enriched.filter((r) => {
    if (r.workspaceBucket === "archived") return false;
    return OWNER_ACTION_LABELS.has(r.ownerNextAction);
  }).length;
  const atRisk = enriched.filter(isStalledRow).length;
  const newRolling7d = countRequestSentInRange(enriched, now - 7 * 86400000, now + 1);
  const inReview = active.filter((r) => r.workspaceBucket === "active-review").length;

  const pNew = enriched.filter((r) => r.workspaceBucket === "new");
  const pReview = enriched.filter((r) => r.workspaceBucket === "active-review");
  const pBid = enriched.filter((r) => r.workspaceBucket === "terms-proposal");
  const pNeg = enriched.filter((r) => r.workspaceBucket === "nda-room" || r.workspaceBucket === "advanced");
  const pClosed = enriched.filter((r) => r.workspaceBucket === "archived" && !isPassedArchived(r));
  const pPassed = enriched.filter((r) => r.workspaceBucket === "archived" && isPassedArchived(r));

  const mirror = {
    awaitingBrand,
    awaitingOwner,
    brandAction,
    ownerAction,
    brandActionMatchesAwaitingBrand: brandAction === awaitingBrand,
    ownerActionMatchesAwaitingOwner: ownerAction === awaitingOwner,
    crossAwaitingBrandEqualsBrandAction: awaitingBrand === brandAction,
    crossAwaitingOwnerEqualsOwnerAction: awaitingOwner === ownerAction,
  };

  /** Canonical cross-persona counts (same BDR rows → mirrored cards). */
  const flow = {
    ownerNeedsAction: awaitingOwner,
    ownerAwaitingBrand: awaitingBrand,
    brandNeedsAction: awaitingBrand,
    brandAwaitingOwner: awaitingOwner,
    atRisk,
    newRolling7d,
    inReview,
  };

  if (persona === "owner") {
    return {
      persona,
      needsAction: flow.ownerNeedsAction,
      awaitingCounterparty: flow.ownerAwaitingBrand,
      atRisk: flow.atRisk,
      newRolling7d: flow.newRolling7d,
      inReview: flow.inReview,
      mirror: {
        ...mirror,
        flow,
        ties: {
          ownerAwaitingBrandEqualsBrandNeedsAction: flow.ownerAwaitingBrand === flow.brandNeedsAction,
          ownerNeedsActionEqualsBrandAwaitingOwner: flow.ownerNeedsAction === flow.brandAwaitingOwner,
        },
      },
      pipeline: {
        newInbound: pNew.length,
        underReview: pReview.length,
        bidSubmitted: pBid.length,
        negotiation: pNeg.length,
        closed: pClosed.length,
        passed: pPassed.length,
      },
      rowCount: enriched.length,
    };
  }

  if (persona === "operator") {
    return {
      persona: "operator",
      needsAction: flow.brandNeedsAction,
      awaitingCounterparty: flow.brandAwaitingOwner,
      atRisk: flow.atRisk,
      newRolling7d: flow.newRolling7d,
      inReview: flow.inReview,
      mirror: {
        ...mirror,
        flow,
        ties: {
          ownerAwaitingBrandEqualsBrandNeedsAction: flow.ownerAwaitingBrand === flow.brandNeedsAction,
          ownerNeedsActionEqualsBrandAwaitingOwner: flow.ownerNeedsAction === flow.brandAwaitingOwner,
        },
      },
      pipeline: {
        newInbound: pNew.length,
        underReview: pReview.length,
        bidSubmitted: pBid.length,
        negotiation: pNeg.length,
        closed: pClosed.length,
        passed: pPassed.length,
      },
      rowCount: enriched.length,
    };
  }

  return {
    persona: "brand",
    needsAction: flow.brandNeedsAction,
    awaitingCounterparty: flow.brandAwaitingOwner,
    atRisk: flow.atRisk,
    newRolling7d: flow.newRolling7d,
    inReview: flow.inReview,
    mirror: {
      ...mirror,
      flow,
      ties: {
        ownerAwaitingBrandEqualsBrandNeedsAction: flow.ownerAwaitingBrand === flow.brandNeedsAction,
        ownerNeedsActionEqualsBrandAwaitingOwner: flow.ownerNeedsAction === flow.brandAwaitingOwner,
      },
    },
    pipeline: {
      newInbound: pNew.length,
      underReview: pReview.length,
      bidSubmitted: pBid.length,
      negotiation: pNeg.length,
      closed: pClosed.length,
      passed: pPassed.length,
    },
    rowCount: enriched.length,
  };
}

/**
 * Audit KPI mirror invariants for a single BDR row set (same records on owner + brand UIs).
 * @param {object[]} rows
 */
export function auditWorkspaceKpiMirror(rows) {
  const owner = computeWorkspaceKpiSnapshot(rows, "owner");
  const brand = computeWorkspaceKpiSnapshot(rows, "brand");
  const violations = [];
  const f = owner.mirror?.flow || {};
  if (f.ownerAwaitingBrand !== f.brandNeedsAction) {
    violations.push({
      code: "awaiting-brand",
      owner: f.ownerAwaitingBrand,
      brand: f.brandNeedsAction,
      message: "Owner Awaiting brand must equal Brand Brand action (same BDR rows).",
    });
  }
  if (f.ownerNeedsAction !== f.brandAwaitingOwner) {
    violations.push({
      code: "owner-action",
      owner: f.ownerNeedsAction,
      brand: f.brandAwaitingOwner,
      message: "Owner Owner action must equal Brand Awaiting owner (same BDR rows).",
    });
  }
  if (owner.atRisk !== brand.atRisk) {
    violations.push({
      code: "at-risk",
      owner: owner.atRisk,
      brand: brand.atRisk,
      message: "Stuck / at risk must match when rows include follow-up and activity dates.",
    });
  }
  const awaitingInfoCount = rows.filter(
    (r) => enrichWorkspaceRow(r).workspaceBucket === "awaiting-info"
  ).length;
  const pipelineSum =
    owner.pipeline.newInbound +
    owner.pipeline.underReview +
    owner.pipeline.bidSubmitted +
    owner.pipeline.negotiation +
    owner.pipeline.closed +
    owner.pipeline.passed;
  // awaiting-info rows are in the flow strip (Awaiting owner / Owner action), not a pipeline column
  if (pipelineSum + awaitingInfoCount !== owner.rowCount) {
    violations.push({
      code: "pipeline-partition",
      expected: owner.rowCount,
      actual: pipelineSum + awaitingInfoCount,
      pipelineOnly: pipelineSum,
      awaitingInfo: awaitingInfoCount,
      message:
        "Pipeline columns + awaiting-info rows should equal all BDR rows (one bucket each).",
    });
  }
  return {
    rowCount: owner.rowCount,
    owner,
    brand,
    ok: violations.length === 0,
    violations,
  };
}

export function buildKpiScopeKey(persona, filterParts) {
  const p = filterParts && typeof filterParts === "object" ? filterParts : {};
  const keys = Object.keys(p)
    .sort()
    .map((k) => `${k}=${String(p[k] ?? "_").slice(0, 64)}`);
  return ["v2", persona, ...keys].join("|").slice(0, 512);
}

export function isoWeekKey(d = new Date()) {
  const t = new Date(d.getTime());
  t.setHours(0, 0, 0, 0);
  const day = t.getDay() || 7;
  t.setDate(t.getDate() + 4 - day);
  const yearStart = new Date(t.getFullYear(), 0, 1);
  const week = Math.ceil(((t - yearStart) / 86400000 + 1) / 7);
  return `${t.getFullYear()}-W${String(week).padStart(2, "0")}`;
}

export function prevIsoWeekKey(wk) {
  const parts = String(wk || "").split("-W");
  if (parts.length !== 2) return null;
  let y = parseInt(parts[0], 10);
  let w = parseInt(parts[1], 10) - 1;
  if (w < 1) {
    y -= 1;
    w = 52;
  }
  return `${y}-W${String(w).padStart(2, "0")}`;
}
