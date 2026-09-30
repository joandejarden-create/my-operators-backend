/**
 * Hotel Intelligence domain status model V1.
 * Zero rows ≠ incomplete — only NOT_RESEARCHED / ERROR / STALE_UNVERIFIED block onboard.
 */

export const HI_DOMAIN_STATUS = Object.freeze({
  POPULATED: "POPULATED",
  RESEARCHED_EMPTY: "RESEARCHED_EMPTY",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
  NOT_RESEARCHED: "NOT_RESEARCHED",
  ERROR: "ERROR",
  STALE_UNVERIFIED: "STALE_UNVERIFIED",
});

export const HI_DOMAIN = Object.freeze({
  COMMERCIAL_PROFILE: "COMMERCIAL_PROFILE",
  EVENT_SPACES: "EVENT_SPACES",
  DEMAND_NODES: "DEMAND_NODES",
  SEASONALITY_NEED_PERIODS: "SEASONALITY_NEED_PERIODS",
  HI_EVIDENCE: "HI_EVIDENCE",
  ADP_ATTRIBUTES: "ADP_ATTRIBUTES",
});

/** Required domains for every active ADP/GDI hotel. */
export const REQUIRED_HI_DOMAINS = Object.freeze([
  HI_DOMAIN.COMMERCIAL_PROFILE,
  HI_DOMAIN.EVENT_SPACES,
  HI_DOMAIN.DEMAND_NODES,
  HI_DOMAIN.SEASONALITY_NEED_PERIODS,
  HI_DOMAIN.HI_EVIDENCE,
  HI_DOMAIN.ADP_ATTRIBUTES,
]);

export const RESOLVED_DOMAIN_STATUSES = Object.freeze([
  HI_DOMAIN_STATUS.POPULATED,
  HI_DOMAIN_STATUS.RESEARCHED_EMPTY,
  HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING,
]);

export const BLOCKING_DOMAIN_STATUSES = Object.freeze([
  HI_DOMAIN_STATUS.NOT_RESEARCHED,
  HI_DOMAIN_STATUS.ERROR,
  HI_DOMAIN_STATUS.STALE_UNVERIFIED,
]);

export const OVERALL_HI_STATUS = Object.freeze({
  HI_COMPLETE: "HI_COMPLETE",
  HI_INCOMPLETE: "HI_INCOMPLETE",
  HI_ERROR: "HI_ERROR",
  HI_BACKFILL_REQUIRED: "HI_BACKFILL_REQUIRED",
});

export function isDomainResolved(status) {
  return RESOLVED_DOMAIN_STATUSES.includes(status);
}

export function isDomainBlocking(status) {
  return BLOCKING_DOMAIN_STATUSES.includes(status);
}

/**
 * Derive domain status from row counts + explicit research ledger.
 * Explicit ledger status wins over row-count inference.
 */
export function deriveDomainStatus(domain, opts = {}) {
  const ledger = opts.ledgerEntry || null;
  if (ledger?.domainStatus && Object.values(HI_DOMAIN_STATUS).includes(ledger.domainStatus)) {
    return {
      domain,
      domainStatus: ledger.domainStatus,
      lastResearchedAt: ledger.lastResearchedAt || null,
      researchRunId: ledger.researchRunId || null,
      evidenceCount: ledger.evidenceCount ?? opts.evidenceCount ?? 0,
      rowCount: ledger.rowCount ?? opts.rowCount ?? 0,
      sourceCoverage: ledger.sourceCoverage || opts.sourceCoverage || [],
      notes: ledger.notes || null,
      blocker: ledger.blocker || null,
      derivedFrom: "ledger",
    };
  }

  const rowCount = Number(opts.rowCount || 0);
  const researched = opts.researchAttempted === true;
  const exhausted = opts.researchExhausted === true;
  const errored = opts.error === true;

  if (errored) {
    return {
      domain,
      domainStatus: HI_DOMAIN_STATUS.ERROR,
      lastResearchedAt: opts.lastResearchedAt || null,
      researchRunId: opts.researchRunId || null,
      evidenceCount: opts.evidenceCount || 0,
      rowCount,
      sourceCoverage: opts.sourceCoverage || [],
      notes: opts.notes || "domain_error",
      blocker: opts.blocker || "ERROR",
      derivedFrom: "inference",
    };
  }

  if (rowCount > 0) {
    return {
      domain,
      domainStatus: HI_DOMAIN_STATUS.POPULATED,
      lastResearchedAt: opts.lastResearchedAt || null,
      researchRunId: opts.researchRunId || null,
      evidenceCount: opts.evidenceCount || 0,
      rowCount,
      sourceCoverage: opts.sourceCoverage || [],
      notes: opts.notes || null,
      blocker: null,
      derivedFrom: "row_count",
    };
  }

  if (researched && exhausted) {
    return {
      domain,
      domainStatus: HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING,
      lastResearchedAt: opts.lastResearchedAt || null,
      researchRunId: opts.researchRunId || null,
      evidenceCount: opts.evidenceCount || 0,
      rowCount: 0,
      sourceCoverage: opts.sourceCoverage || [],
      notes: opts.notes || "bounded_research_exhausted_no_supportable_rows",
      blocker: "PUBLIC_DATA_CEILING",
      derivedFrom: "inference",
    };
  }

  if (researched && opts.researchedEmpty === true) {
    return {
      domain,
      domainStatus: HI_DOMAIN_STATUS.RESEARCHED_EMPTY,
      lastResearchedAt: opts.lastResearchedAt || null,
      researchRunId: opts.researchRunId || null,
      evidenceCount: opts.evidenceCount || 0,
      rowCount: 0,
      sourceCoverage: opts.sourceCoverage || [],
      notes: opts.notes || "research_performed_no_supportable_record",
      blocker: null,
      derivedFrom: "inference",
    };
  }

  return {
    domain,
    domainStatus: HI_DOMAIN_STATUS.NOT_RESEARCHED,
    lastResearchedAt: null,
    researchRunId: null,
    evidenceCount: opts.evidenceCount || 0,
    rowCount,
    sourceCoverage: opts.sourceCoverage || [],
    notes: opts.notes || "no_research_state_and_zero_or_unknown_rows",
    blocker: "NOT_RESEARCHED",
    derivedFrom: "inference",
    falseCompletenessFlag:
      rowCount === 0 ? "ROW_EMPTY_WITH_NO_RESEARCH_STATE" : null,
  };
}

export function summarizeOverallHiStatus(domainStatuses = {}) {
  const statuses = REQUIRED_HI_DOMAINS.map(
    (d) => domainStatuses[d]?.domainStatus || HI_DOMAIN_STATUS.NOT_RESEARCHED
  );
  if (statuses.includes(HI_DOMAIN_STATUS.ERROR)) {
    return OVERALL_HI_STATUS.HI_ERROR;
  }
  if (statuses.every((s) => isDomainResolved(s))) {
    return OVERALL_HI_STATUS.HI_COMPLETE;
  }
  return OVERALL_HI_STATUS.HI_INCOMPLETE;
}

export function domainCoverageScore(domainStatuses = {}) {
  const resolved = REQUIRED_HI_DOMAINS.filter((d) =>
    isDomainResolved(domainStatuses[d]?.domainStatus)
  ).length;
  return {
    resolved,
    required: REQUIRED_HI_DOMAINS.length,
    label: `${resolved}/${REQUIRED_HI_DOMAINS.length}`,
    notResearched: REQUIRED_HI_DOMAINS.filter(
      (d) =>
        (domainStatuses[d]?.domainStatus || HI_DOMAIN_STATUS.NOT_RESEARCHED) ===
        HI_DOMAIN_STATUS.NOT_RESEARCHED
    ).length,
  };
}
