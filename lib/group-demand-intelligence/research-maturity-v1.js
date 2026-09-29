/**
 * GDI Research Maturity V1
 *
 * Distinguishes INITIALIZED_ONLY from RESEARCHED_NO_READY.
 * A Research Run row alone does NOT prove substantive research.
 */

export const GDI_RESEARCH_MATURITY = Object.freeze({
  CUSTOMER_READY: "CUSTOMER_READY",
  RESEARCHED_NO_READY: "RESEARCHED_NO_READY",
  PARTIALLY_RESEARCHED: "PARTIALLY_RESEARCHED",
  INITIALIZED_ONLY: "INITIALIZED_ONLY",
  RESEARCH_STATE_UNKNOWN: "RESEARCH_STATE_UNKNOWN",
});

/** System / product states (generic hotel GDI state model). */
export const GDI_SYSTEM_STATE = Object.freeze({
  INITIALIZED: "INITIALIZED",
  RESEARCH_IN_PROGRESS: "RESEARCH_IN_PROGRESS",
  RESEARCHED_NO_READY: "RESEARCHED_NO_READY",
  CUSTOMER_READY: "CUSTOMER_READY",
  STALE_RESEARCH: "STALE_RESEARCH",
  RESEARCH_BLOCKED: "RESEARCH_BLOCKED",
});

export const ZERO_OPP_ROOT_CAUSE = Object.freeze({
  INITIALIZED_NOT_RESEARCHED: "INITIALIZED_NOT_RESEARCHED",
  INSUFFICIENT_RESEARCH_COVERAGE: "INSUFFICIENT_RESEARCH_COVERAGE",
  SOURCE_DEPTH_WEAK: "SOURCE_DEPTH_WEAK",
  DEMAND_SIGNALS_FOUND_BUT_NOT_LODGING_SUPPORTED:
    "DEMAND_SIGNALS_FOUND_BUT_NOT_LODGING_SUPPORTED",
  HOTEL_FIT_TOO_WEAK: "HOTEL_FIT_TOO_WEAK",
  TIMING_UNCONFIRMED: "TIMING_UNCONFIRMED",
  WHO_ACTION_PATH_TOO_WEAK: "WHO_ACTION_PATH_TOO_WEAK",
  VALID_RESEARCH_NO_READY_OPPORTUNITIES: "VALID_RESEARCH_NO_READY_OPPORTUNITIES",
  OTHER: "OTHER",
});

export const NEXT_CYCLE_BUCKET = Object.freeze({
  NEXT_CYCLE_A: "NEXT_CYCLE_A",
  NEXT_CYCLE_B: "NEXT_CYCLE_B",
  DEFER: "DEFER",
  NO_ADDITIONAL_RESEARCH_NEEDED_NOW: "NO_ADDITIONAL_RESEARCH_NEEDED_NOW",
});

const INIT_NOTE_RE =
  /initialization footprint|INIT_FOOTPRINT|universe_reconciliation|seed\/init|not a live discovery/i;

/**
 * @param {object} run — Airtable Research Run fields (or normalized)
 */
export function isInitFootprintRun(run = {}) {
  const notes = String(run.notes || run.Notes || "");
  const payload =
    typeof run.payload === "object" && run.payload
      ? run.payload
      : safeJson(run.payloadJson || run["Run Payload JSON"]);
  if (INIT_NOTE_RE.test(notes)) return true;
  if (payload?.kind === "INIT_FOOTPRINT") return true;
  if (payload?.source === "universe_reconciliation_v1") return true;
  const queries = num(run.queries ?? run.Queries);
  const fetches = num(run.fetches ?? run.Fetches);
  const attempted = num(run.targetsAttempted ?? run["Targets Attempted"]);
  const completed = num(run.targetsCompleted ?? run["Targets Completed"]);
  const newSignals = num(run.newSignals ?? run["New Signals"]);
  // Init rows are typically zero-activity COMPLETED stubs
  if (
    queries === 0 &&
    fetches === 0 &&
    attempted === 0 &&
    completed === 0 &&
    newSignals === 0 &&
    /init|seed|onboard|reconciliation/i.test(notes || String(run.runType || ""))
  ) {
    return true;
  }
  return false;
}

/**
 * Evidence that substantive target-or-hotel research occurred.
 */
export function assessSubstantiveResearchEvidence({
  runs = [],
  targetRuns = [],
  localRuns = [],
  cycleSummaries = [],
  signals = 0,
  candidates = 0,
  customerReady = 0,
} = {}) {
  const substantiveRuns = [];
  const initRuns = [];
  for (const r of runs) {
    if (isInitFootprintRun(r)) initRuns.push(r);
    else if (runLooksSubstantive(r)) substantiveRuns.push(r);
  }

  const researchedTargetRuns = targetRuns.filter((tr) => targetRunLooksResearched(tr));
  const localSubstantive = localRuns.filter((lr) => localRunLooksSubstantive(lr));
  const cycleSubstantive = cycleSummaries.filter((c) => cycleLooksSubstantive(c));

  const queries =
    sumField(substantiveRuns, ["queries", "Queries"]) +
    sumField(researchedTargetRuns, ["queriesUsed", "Queries Used"]) +
    sumField(cycleSubstantive, ["discovery.queries", "queries"]);
  const fetches =
    sumField(substantiveRuns, ["fetches", "Fetches"]) +
    sumField(researchedTargetRuns, ["fetchesUsed", "Fetches Used"]) +
    sumField(cycleSubstantive, ["discovery.fetches", "fetches"]);

  const proof = {
    airtableSubstantiveRuns: substantiveRuns.length,
    airtableInitRuns: initRuns.length,
    researchedTargetRuns: researchedTargetRuns.length,
    localSubstantiveRuns: localSubstantive.length,
    cycleSummaries: cycleSubstantive.length,
    queries: queries || null,
    fetches: fetches || null,
    signals: num(signals) || 0,
    candidates: num(candidates) || 0,
    customerReady: num(customerReady) || 0,
  };

  const hasTargetLedgerProof = researchedTargetRuns.length > 0;
  const hasHotelCycleProof =
    substantiveRuns.length > 0 ||
    localSubstantive.filter((lr) => !lr.seedIngestOnly).length > 0 ||
    cycleSubstantive.length > 0 ||
    (proof.queries || 0) > 0 ||
    (proof.fetches || 0) > 0;

  // Seed-ingest-only local runs (0 queries, candidates from seeds) count as weak/partial
  const seedOnlyLocal =
    !hasTargetLedgerProof &&
    !cycleSubstantive.length &&
    substantiveRuns.length === 0 &&
    localSubstantive.length > 0 &&
    localSubstantive.every((lr) => lr.seedIngestOnly);

  let strength = "NONE";
  if (customerReady > 0 || hasTargetLedgerProof || (hasHotelCycleProof && !seedOnlyLocal)) {
    strength = "SUBSTANTIVE";
  } else if (seedOnlyLocal || (proof.candidates > 0 && proof.queries === 0 && proof.fetches === 0)) {
    strength = "WEAK_PARTIAL";
  } else if (initRuns.length > 0 || runs.length > 0) {
    strength = "INIT_ONLY";
  } else if (runs.length === 0 && targetRuns.length === 0 && !localRuns.length) {
    strength = "ABSENT";
  }

  return {
    strength,
    proof,
    hasTargetLedgerProof,
    hasHotelCycleProof,
    seedOnlyLocal: Boolean(seedOnlyLocal),
    substantiveRunIds: substantiveRuns.map((r) => r.runId || r["Run ID"] || r.id).filter(Boolean),
    researchedTargetIds: [
      ...new Set(
        researchedTargetRuns.map((tr) => tr.targetId || tr["Target ID"]).filter(Boolean)
      ),
    ],
  };
}

/**
 * Classify one hotel into a single maturity label.
 */
export function classifyGdiResearchMaturity({
  totalTargets = 0,
  targetsResearched = 0,
  customerReady = 0,
  evidence = null,
} = {}) {
  const ev =
    evidence ||
    assessSubstantiveResearchEvidence({
      customerReady,
    });

  if (customerReady > 0) {
    return {
      maturity: GDI_RESEARCH_MATURITY.CUSTOMER_READY,
      systemState: GDI_SYSTEM_STATE.CUSTOMER_READY,
      rationale: ">=1 customer-ready opportunity",
    };
  }

  if (ev.strength === "ABSENT" && totalTargets === 0) {
    return {
      maturity: GDI_RESEARCH_MATURITY.RESEARCH_STATE_UNKNOWN,
      systemState: GDI_SYSTEM_STATE.RESEARCH_BLOCKED,
      rationale: "no fits/targets/runs/evidence",
    };
  }

  if (ev.strength === "INIT_ONLY" || ev.strength === "NONE" || ev.strength === "ABSENT") {
    return {
      maturity: GDI_RESEARCH_MATURITY.INITIALIZED_ONLY,
      systemState: GDI_SYSTEM_STATE.INITIALIZED,
      rationale: "fits/targets/init run exist without substantive target research",
    };
  }

  if (ev.strength === "WEAK_PARTIAL") {
    return {
      maturity: GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED,
      systemState: GDI_SYSTEM_STATE.RESEARCH_IN_PROGRESS,
      rationale: "seed/qualification activity without proven query/fetch target research",
    };
  }

  // Substantive research occurred
  if (ev.hasTargetLedgerProof && totalTargets > 0) {
    const coverage = targetsResearched / totalTargets;
    if (coverage < 0.85 && targetsResearched < totalTargets) {
      return {
        maturity: GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED,
        systemState: GDI_SYSTEM_STATE.RESEARCH_IN_PROGRESS,
        rationale: `target ledger coverage ${(coverage * 100).toFixed(0)}% — meaningful population untouched`,
      };
    }
    return {
      maturity: GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY,
      systemState: GDI_SYSTEM_STATE.RESEARCHED_NO_READY,
      rationale: "substantive target research with 0 customer-ready opportunities",
    };
  }

  // Hotel-level cycles without target-run ledger: treat as researched (gates ran) but
  // coverage incomplete at target grain → PARTIALLY if targets remain untouched in ledger
  if (ev.hasHotelCycleProof && totalTargets > 0 && targetsResearched === 0) {
    return {
      maturity: GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED,
      systemState: GDI_SYSTEM_STATE.RESEARCH_IN_PROGRESS,
      rationale:
        "hotel-level discovery cycles proved research occurred; target-run ledger empty so target population remains unproven",
    };
  }

  if (ev.hasHotelCycleProof) {
    return {
      maturity: GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY,
      systemState: GDI_SYSTEM_STATE.RESEARCHED_NO_READY,
      rationale: "substantive hotel-level research with 0 customer-ready opportunities",
    };
  }

  return {
    maturity: GDI_RESEARCH_MATURITY.RESEARCH_STATE_UNKNOWN,
    systemState: GDI_SYSTEM_STATE.RESEARCH_BLOCKED,
    rationale: "persistence cannot prove whether research occurred",
  };
}

export function classifyZeroOppRootCause({
  maturity,
  evidence,
  customerReady = 0,
  targetsResearched = 0,
  totalTargets = 0,
  signals = 0,
  candidates = 0,
  qualified = 0,
  watchlistHeavy = false,
} = {}) {
  if (customerReady > 0) return null;
  if (maturity === GDI_RESEARCH_MATURITY.INITIALIZED_ONLY) {
    return ZERO_OPP_ROOT_CAUSE.INITIALIZED_NOT_RESEARCHED;
  }
  if (maturity === GDI_RESEARCH_MATURITY.RESEARCH_STATE_UNKNOWN) {
    return ZERO_OPP_ROOT_CAUSE.OTHER;
  }
  if (
    maturity === GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED &&
    targetsResearched < Math.max(1, Math.floor(totalTargets * 0.5))
  ) {
    return ZERO_OPP_ROOT_CAUSE.INSUFFICIENT_RESEARCH_COVERAGE;
  }
  if (signals > 0 && candidates === 0) {
    return ZERO_OPP_ROOT_CAUSE.DEMAND_SIGNALS_FOUND_BUT_NOT_LODGING_SUPPORTED;
  }
  if ((evidence?.proof?.fetches || 0) > 0 && (evidence?.proof?.fetches || 0) < 10 && candidates > 0) {
    return ZERO_OPP_ROOT_CAUSE.SOURCE_DEPTH_WEAK;
  }
  if (watchlistHeavy && qualified > 0 && customerReady === 0) {
    return ZERO_OPP_ROOT_CAUSE.VALID_RESEARCH_NO_READY_OPPORTUNITIES;
  }
  if (maturity === GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY) {
    return ZERO_OPP_ROOT_CAUSE.VALID_RESEARCH_NO_READY_OPPORTUNITIES;
  }
  return ZERO_OPP_ROOT_CAUSE.OTHER;
}

/**
 * Deterministic next-cycle bucket (no prestige ranking).
 */
export function prioritizeNextCycle({
  maturity,
  rootCause,
  hotelName = "",
  isBethesda = false,
  coveragePct = 0,
  totalTargets = 0,
  hiScore = 0,
  customerReady = 0,
  hasFocusedLaneGap = false,
  pilotImportance = 0,
} = {}) {
  if (isBethesda || /bethesda/i.test(hotelName)) {
    return {
      bucket: NEXT_CYCLE_BUCKET.NO_ADDITIONAL_RESEARCH_NEEDED_NOW,
      why: "founding pilot — preserve opportunity set; no experimental broad discovery",
    };
  }
  if (customerReady > 0 && coveragePct >= 70) {
    return {
      bucket: NEXT_CYCLE_BUCKET.DEFER,
      why: "already customer-ready with material coverage — refresh later, not next batch",
    };
  }
  if (maturity === GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY && hasFocusedLaneGap) {
    return {
      bucket: NEXT_CYCLE_BUCKET.NEXT_CYCLE_A,
      why: "real research exhausted general pass; focused lane gap remains",
    };
  }
  if (
    maturity === GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED &&
    (hasFocusedLaneGap || (evidenceCycles(hotelName) && coveragePct < 50))
  ) {
    return {
      bucket: NEXT_CYCLE_BUCKET.NEXT_CYCLE_A,
      why: "partial research with clear uncovered lanes or empty target ledger after hotel cycles",
    };
  }
  if (maturity === GDI_RESEARCH_MATURITY.INITIALIZED_ONLY) {
    // Prefer stronger HI / more targets / pilot importance for first real cycles
    const score = hiScore * 10 + Math.min(totalTargets, 30) + pilotImportance * 20;
    if (score >= 50 || pilotImportance >= 2) {
      return {
        bucket: NEXT_CYCLE_BUCKET.NEXT_CYCLE_A,
        why: "initialized only — high HI/target readiness for first substantive cycle",
      };
    }
    return {
      bucket: NEXT_CYCLE_BUCKET.NEXT_CYCLE_B,
      why: "initialized only — first research needed but lower readiness than A cohort",
    };
  }
  if (maturity === GDI_RESEARCH_MATURITY.RESEARCHED_NO_READY) {
    return {
      bucket: NEXT_CYCLE_BUCKET.NEXT_CYCLE_B,
      why: "researched with zero ready — optional deeper pass after A cohort",
    };
  }
  if (maturity === GDI_RESEARCH_MATURITY.PARTIALLY_RESEARCHED) {
    return {
      bucket: NEXT_CYCLE_BUCKET.NEXT_CYCLE_B,
      why: "partial coverage — continue after focused A hotels",
    };
  }
  if (rootCause === ZERO_OPP_ROOT_CAUSE.INITIALIZED_NOT_RESEARCHED) {
    return {
      bucket: NEXT_CYCLE_BUCKET.NEXT_CYCLE_B,
      why: "never researched",
    };
  }
  return {
    bucket: NEXT_CYCLE_BUCKET.DEFER,
    why: "no urgent research gap for next bounded batch",
  };
}

function evidenceCycles(name) {
  return /a coru[nñ]a|spice island|hilton.*times|w rome/i.test(String(name));
}

function runLooksSubstantive(run = {}) {
  if (isInitFootprintRun(run)) return false;
  const queries = num(run.queries ?? run.Queries);
  const fetches = num(run.fetches ?? run.Fetches);
  const attempted = num(run.targetsAttempted ?? run["Targets Attempted"]);
  const completed = num(run.targetsCompleted ?? run["Targets Completed"]);
  const newSignals = num(run.newSignals ?? run["New Signals"]);
  const newOpps = num(run.newOpportunities ?? run["New Opportunities"]);
  return queries > 0 || fetches > 0 || attempted > 0 || completed > 0 || newSignals > 0 || newOpps > 0;
}

function targetRunLooksResearched(tr = {}) {
  const status = String(tr.executionStatus || tr["Execution Status"] || "").toUpperCase();
  const queries = num(tr.queriesUsed ?? tr["Queries Used"]);
  const fetches = num(tr.fetchesUsed ?? tr["Fetches Used"]);
  const sources = num(tr.sourceCount ?? tr["Source Count"]);
  if (status === "RESEARCHED") return true;
  if (queries > 0 || fetches > 0 || sources > 0) return true;
  if (tr.researchExecuted === true) return true;
  return false;
}

function localRunLooksSubstantive(lr = {}) {
  if (lr.seedIngestOnly) return true; // weak — flagged separately
  const q = num(lr.queries);
  const f = num(lr.fetches);
  const c = num(lr.candidates);
  const sources = num(lr.sourceCount);
  const webhound = num(lr.webhoundUsd);
  return q > 0 || f > 0 || webhound > 0 || (c > 0 && sources > 0);
}

function cycleLooksSubstantive(c = {}) {
  const q = num(c.queries ?? c.discovery?.queries);
  const f = num(c.fetches ?? c.discovery?.fetches);
  const cand = num(c.candidates ?? c.discovery?.candidates);
  return q > 0 || f > 0 || cand > 0;
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function sumField(rows, paths) {
  let t = 0;
  for (const row of rows) {
    for (const p of paths) {
      const v = dig(row, p);
      if (v != null && v !== "") {
        t += num(v);
        break;
      }
    }
  }
  return t;
}

function dig(obj, path) {
  if (!path.includes(".")) return obj?.[path];
  return path.split(".").reduce((a, k) => (a == null ? undefined : a[k]), obj);
}

function safeJson(s) {
  if (!s) return null;
  if (typeof s === "object") return s;
  try {
    return JSON.parse(String(s));
  } catch {
    return null;
  }
}
