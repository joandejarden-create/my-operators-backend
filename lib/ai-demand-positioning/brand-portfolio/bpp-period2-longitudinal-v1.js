/**
 * Brand & Portfolio Period-1 → Period-2 longitudinal comparison (PRIOR_RUN).
 * Reads Period-1 structured history SoT; compares to Period-2 execution metrics.
 * Does not recompute Period-1.
 */
import { existsSync, readFileSync } from "fs";
import { join } from "path";

export const BPP_PERIOD_1_TO_PERIOD_2_LONGITUDINAL_RECONCILIATION =
  "BPP_PERIOD_1_TO_PERIOD_2_LONGITUDINAL_RECONCILIATION";

export const PERIOD_1_ID = "bpp_first_cycle_2026-08-21T2057";
export const PERIOD_2_ID = "bpp_second_cycle_2026-09-02T1947";

export const PROPERTY_IDS = Object.freeze([
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_hotel_phillips_kansas_city",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
]);

export const PROVIDERS = Object.freeze(["openai", "gemini", "perplexity", "claude"]);

const HISTORY_PERIOD_ROOT = join(
  process.cwd(),
  "data/ai-demand-positioning/brand-portfolio-history/periods",
  PERIOD_1_ID
);

/** Founder-locked Period-1 baselines (pp display) — history SoT is authoritative. */
export const PERIOD_1_PRESENCE_BASELINE_PP = Object.freeze({
  adp_waterstone_boca_raton: 50.0,
  adp_renaissance_times_square: 47.2,
  adp_hotel_phillips_kansas_city: 41.7,
  adp_cambridge_beaches_bermuda: 65.6,
  adp_now_now_noho: 3.1,
});

const MATERIAL_PP = 5;
const MODEST_PP = 1;

export function loadPeriod1Structured(propertyId) {
  const p = join(HISTORY_PERIOD_ROOT, propertyId, "structured.json");
  if (!existsSync(p)) throw new Error(`Missing Period-1 structured history: ${p}`);
  return JSON.parse(readFileSync(p, "utf8"));
}

function round1(n) {
  if (n == null || Number.isNaN(n)) return null;
  return Math.round(Number(n) * 10) / 10;
}

function rateToPp(rate) {
  if (rate == null) return null;
  return round1(Number(rate) * 100);
}

function ppDelta(currentPp, priorPp) {
  if (currentPp == null || priorPp == null) return null;
  return round1(currentPp - priorPp);
}

export function classifyMateriality({ presencePpDelta, rankDelta }) {
  const absPp = presencePpDelta == null ? 0 : Math.abs(presencePpDelta);
  const absRank = rankDelta == null ? 0 : Math.abs(rankDelta);
  if (absPp >= MATERIAL_PP || absRank >= 1) return "material";
  if (absPp >= MODEST_PP) return "modest";
  return "essentially_unchanged";
}

export function formatPpDelta(pp) {
  if (pp == null) return "—";
  if (Math.abs(pp) < 0.05) return "0.0 pp";
  const sign = pp > 0 ? "+" : "";
  return `${sign}${pp.toFixed(1)} pp`;
}

export function formatRankMovement({ priorRank, currentRank, state }) {
  if (state === "UNCHANGED") return { label: "Unchanged", display: priorRank != null ? `#${priorRank} → #${priorRank}` : "—" };
  if (state === "MOVED" && priorRank != null && currentRank != null) {
    const arrow = currentRank < priorRank ? "↑" : currentRank > priorRank ? "↓" : "→";
    return { label: "Moved", display: `#${priorRank} → #${currentRank} (${arrow}${Math.abs(currentRank - priorRank)})` };
  }
  if (state === "NEW_TO_RANKING") return { label: "New", display: `— → #${currentRank}` };
  if (state === "RETURNED") return { label: "Returned", display: `#${priorRank ?? "—"} → #${currentRank}` };
  if (state === "EXITED") return { label: "Exited", display: `#${priorRank} → —` };
  return { label: state || "—", display: "—" };
}

function setDiff(priorIds = [], currentIds = []) {
  const prior = new Set(priorIds);
  const current = new Set(currentIds);
  const persistent = [...current].filter((id) => prior.has(id)).sort();
  const neu = [...current].filter((id) => !prior.has(id)).sort();
  const disappeared = [...prior].filter((id) => !current.has(id)).sort();
  return {
    persistent,
    new: neu,
    disappeared,
    priorCount: prior.size,
    currentCount: current.size,
    countChange: current.size - prior.size,
  };
}

function providerRateMap(structuredProviders = [], metricsByProvider = null) {
  const out = {};
  for (const p of PROVIDERS) out[p] = null;
  if (metricsByProvider) {
    for (const p of PROVIDERS) {
      const row = metricsByProvider[p];
      out[p] = row?.presenceRate ?? null;
    }
    return out;
  }
  for (const row of structuredProviders) {
    const id = row.providerId || row.provider;
    if (PROVIDERS.includes(id)) out[id] = row.presenceRate ?? null;
  }
  return out;
}

function rankState(priorRank, currentRank, priorHad, currentHad) {
  if (!priorHad && currentHad) return "NEW_TO_RANKING";
  if (priorHad && !currentHad) return "EXITED";
  if (!priorHad && !currentHad) return "UNCHANGED";
  if (priorRank === currentRank) return "UNCHANGED";
  // Peer set unchanged — treat reappearance after exit as RETURNED only if prior missing mid-period (N/A here)
  return "MOVED";
}

/**
 * @param {object} opts
 * @param {string} opts.propertyId
 * @param {object} opts.period2Metrics - computeBrandPortfolioMetricsV1 output (or snapshot.metrics)
 * @param {object} [opts.period1Structured]
 */
export function comparePropertyPeriod1ToPeriod2({ propertyId, period2Metrics, period1Structured }) {
  const prior = period1Structured || loadPeriod1Structured(propertyId);
  const pm = prior.periodMetrics || {};
  const cur = period2Metrics || {};

  const priorPresence = pm.portfolioAiPresence;
  const currentPresence = cur.portfolioAiPresence;
  const priorPresencePp = rateToPp(priorPresence);
  const currentPresencePp = rateToPp(currentPresence);
  const presencePpDelta = ppDelta(currentPresencePp, priorPresencePp);

  // Sanity vs founder baselines
  const baselinePp = PERIOD_1_PRESENCE_BASELINE_PP[propertyId];
  const baselineMatch =
    baselinePp == null || priorPresencePp == null
      ? true
      : Math.abs(priorPresencePp - baselinePp) <= 0.15;

  const priorRank = pm.portfolioRank;
  const currentRank = cur.portfolioRank;
  const subjectRankState = rankState(priorRank, currentRank, priorRank != null, currentRank != null);
  const rankDelta =
    priorRank != null && currentRank != null ? currentRank - priorRank : null;

  const priorBench = pm.portfolioBenchmark;
  const currentBench = cur.portfolioBenchmark;
  const priorIndex = pm.portfolioPresenceIndex;
  const currentIndex = cur.portfolioPresenceIndex;
  const cambridgeSuppressed =
    propertyId === "adp_cambridge_beaches_bermuda" &&
    priorBench == null &&
    currentBench == null &&
    priorIndex == null &&
    currentIndex == null;

  const priorProviders = providerRateMap(prior.providers);
  const currentProviders = providerRateMap(null, cur.byProvider);
  const providerMovement = {};
  for (const p of PROVIDERS) {
    const priorPp = rateToPp(priorProviders[p]);
    const currentPp = rateToPp(currentProviders[p]);
    providerMovement[p] = {
      priorRate: priorProviders[p],
      currentRate: currentProviders[p],
      priorPp,
      currentPp,
      ppDelta: ppDelta(currentPp, priorPp),
      display: `${priorPp ?? "—"}% → ${currentPp ?? "—"}% (${formatPpDelta(ppDelta(currentPp, priorPp))})`,
    };
  }

  const priorTerrMap = new Map((prior.territories || []).map((t) => [t.territoryId, t]));
  const curTerrs = cur.byTerritory || cur.territories || {};
  const territoryIds = [
    ...new Set([...priorTerrMap.keys(), ...Object.keys(curTerrs)]),
  ].sort();
  const territoryMovement = territoryIds.map((tid) => {
    const pt = priorTerrMap.get(tid);
    const ct = curTerrs[tid] || null;
    const priorPp = rateToPp(pt?.aiPresence);
    const currentPp = rateToPp(ct?.aiPresence ?? ct?.presenceRate);
    const pRank = pt?.rank ?? null;
    const cRank = ct?.rank ?? ct?.portfolioRank ?? null;
    return {
      territoryId: tid,
      priorPresencePp: priorPp,
      currentPresencePp: currentPp,
      presencePpDelta: ppDelta(currentPp, priorPp),
      priorRank: pRank,
      currentRank: cRank,
      rankState: rankState(pRank, cRank, pRank != null, cRank != null),
      priorBenchmark: pt?.benchmark ?? null,
      currentBenchmark: ct?.benchmark ?? ct?.portfolioBenchmark ?? null,
      priorIndex: pt?.presenceIndex ?? null,
      currentIndex: ct?.presenceIndex ?? ct?.portfolioPresenceIndex ?? null,
    };
  });

  const priorN1 = pm.numberOneAppearanceRate;
  const priorTop3 = pm.topThreeAppearanceRate;
  const currentN1 = cur.numberOneAppearance;
  const currentTop3 = cur.top3Appearance;

  const displacementMovement = setDiff(
    prior.displacement?.scenarioIds || [],
    cur.displacement?.scenarioIds || []
  );

  const sharedMovement = setDiff(
    prior.scenariosShared?.scenarioIds || [],
    cur.scenariosShared?.scenarioIds || []
  );

  // Shared by peer: count deltas
  const priorSharedByPeer = new Map(
    (prior.scenariosShared?.byPeer || []).map((r) => [r.canonicalEntityId, r.scenariosShared || 0])
  );
  const curSharedByPeer = new Map(
    (cur.scenariosShared?.byPeer || []).map((r) => [r.canonicalEntityId, r.scenariosShared || 0])
  );
  const peerIds = [...new Set([...priorSharedByPeer.keys(), ...curSharedByPeer.keys()])];
  const sharedByPeerMovement = peerIds
    .map((id) => {
      const priorCount = priorSharedByPeer.get(id) ?? 0;
      const currentCount = curSharedByPeer.get(id) ?? 0;
      const delta = currentCount - priorCount;
      let classif = "unchanged";
      if (priorCount === 0 && currentCount > 0) classif = "new_overlap";
      else if (priorCount > 0 && currentCount === 0) classif = "lost_overlap";
      else if (delta > 0) classif = "increased_co_consideration";
      else if (delta < 0) classif = "decreased_co_consideration";
      return { canonicalEntityId: id, priorCount, currentCount, delta, classification: classif };
    })
    .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));

  // Ranking universe movement
  const priorRankMap = new Map(
    (prior.rankingOverall || []).map((r) => [r.canonicalEntityId, r])
  );
  const curRankRows = cur.tableRows || cur.rankingUniverse || [];
  const curRankMap = new Map(curRankRows.map((r) => [r.canonicalEntityId, r]));
  const allEntities = [...new Set([...priorRankMap.keys(), ...curRankMap.keys()])];
  const rankingMovement = allEntities.map((id) => {
    const pr = priorRankMap.get(id);
    const cr = curRankMap.get(id);
    const state = rankState(pr?.rank, cr?.rank, !!pr, !!cr);
    const priorPresencePp = rateToPp(pr?.presenceRate);
    const currentPresencePp = rateToPp(cr?.presenceRate);
    const presencePpDelta = ppDelta(currentPresencePp, priorPresencePp);
    const rankDeltaLocal =
      pr?.rank != null && cr?.rank != null ? cr.rank - pr.rank : null;
    let deltaDisplay = "—";
    if (state === "UNCHANGED" && presencePpDelta != null && Math.abs(presencePpDelta) < 0.05) {
      deltaDisplay = "0";
    } else if (presencePpDelta != null) {
      const sign = presencePpDelta > 0 ? "+" : "";
      deltaDisplay = `${sign}${presencePpDelta.toFixed(1)}`;
    } else if (state === "NEW_TO_RANKING") deltaDisplay = "NEW";
    else if (state === "EXITED") deltaDisplay = "EXIT";
    return {
      canonicalEntityId: id,
      name: cr?.name || cr?.displayName || pr?.displayName || id,
      isSubject: Boolean(cr?.isSubject || pr?.isSubject),
      priorRank: pr?.rank ?? null,
      currentRank: cr?.rank ?? null,
      state,
      priorPresencePp,
      currentPresencePp,
      presencePpDelta,
      rankDelta: rankDeltaLocal,
      deltaDisplay,
      movement: formatRankMovement({
        priorRank: pr?.rank,
        currentRank: cr?.rank,
        state,
      }),
    };
  });

  const subjectRow = rankingMovement.find((r) => r.isSubject) || null;
  const materiality = classifyMateriality({
    presencePpDelta,
    rankDelta: rankDelta == null ? null : -rankDelta, // improve = lower rank number
  });

  return {
    propertyId,
    comparisonMode: "PRIOR_RUN",
    priorPeriodId: PERIOD_1_ID,
    currentPeriodId: PERIOD_2_ID,
    peerSetComparability: "PEER_SET_UNCHANGED",
    presence: {
      prior: priorPresence,
      current: currentPresence,
      priorPp: priorPresencePp,
      currentPp: currentPresencePp,
      ppDelta: presencePpDelta,
      founderBaselinePp: baselinePp,
      founderBaselineMatch: baselineMatch,
    },
    rank: {
      prior: priorRank,
      current: currentRank,
      priorOf: pm.portfolioRankOf,
      currentOf: cur.portfolioRankOf,
      delta: rankDelta,
      state: subjectRankState,
      movement: formatRankMovement({
        priorRank,
        currentRank,
        state: subjectRankState,
      }),
    },
    benchmark: {
      prior: priorBench,
      current: currentBench,
      ppDelta: ppDelta(rateToPp(currentBench), rateToPp(priorBench)),
      suppressed: cambridgeSuppressed || (priorBench == null && currentBench == null),
    },
    index: {
      prior: priorIndex,
      current: currentIndex,
      delta:
        priorIndex != null && currentIndex != null ? currentIndex - priorIndex : null,
      suppressed: cambridgeSuppressed || (priorIndex == null && currentIndex == null),
    },
    providerMovement,
    territoryMovement,
    numberOne: {
      prior: priorN1,
      current: currentN1,
      priorPp: rateToPp(priorN1),
      currentPp: rateToPp(currentN1),
      ppDelta: ppDelta(rateToPp(currentN1), rateToPp(priorN1)),
    },
    top3: {
      prior: priorTop3,
      current: currentTop3,
      priorPp: rateToPp(priorTop3),
      currentPp: rateToPp(currentTop3),
      ppDelta: ppDelta(rateToPp(currentTop3), rateToPp(priorTop3)),
    },
    displacementMovement,
    sharedMovement,
    sharedByPeerMovement,
    rankingMovement,
    subjectRow,
    materiality,
    materialityThresholds: { materialPp: MATERIAL_PP, modestPp: MODEST_PP },
  };
}

export function reconcileLongitudinalProperty(comparison, period2Metrics) {
  const defects = [];
  const near = (a, b, label, tol = 0.0015) => {
    if (a == null && b == null) return;
    if (a == null || b == null || Math.abs(Number(a) - Number(b)) > tol) defects.push(label);
  };

  // current headline ↔ subject rank row
  const subject = (period2Metrics.tableRows || []).find((r) => r.isSubject);
  if (subject && period2Metrics.portfolioRank != null && subject.rank !== period2Metrics.portfolioRank) {
    defects.push("CURRENT_HEADLINE_RANK_MISMATCH");
  }
  if (subject && period2Metrics.portfolioAiPresence != null) {
    const tablePct = parseFloat(String(subject.presenceDisplay));
    const kpiPct = rateToPp(period2Metrics.portfolioAiPresence);
    if (!Number.isNaN(tablePct) && Math.abs(tablePct - kpiPct) > 0.2) {
      defects.push("CURRENT_HEADLINE_PRESENCE_MISMATCH");
    }
  }

  // prior headline ↔ Period-1 history (founder baseline)
  if (!comparison.presence.founderBaselineMatch) {
    defects.push("PRIOR_PRESENCE_BASELINE_MISMATCH");
  }

  // AI Presence delta arithmetic
  const expectedPresenceDelta = ppDelta(comparison.presence.currentPp, comparison.presence.priorPp);
  if (
    comparison.presence.ppDelta != null &&
    expectedPresenceDelta != null &&
    Math.abs(comparison.presence.ppDelta - expectedPresenceDelta) > 0.05
  ) {
    defects.push("PRESENCE_DELTA_ARITHMETIC");
  }

  for (const p of PROVIDERS) {
    const m = comparison.providerMovement[p];
    const expected = ppDelta(m.currentPp, m.priorPp);
    if (m.ppDelta != null && expected != null && Math.abs(m.ppDelta - expected) > 0.05) {
      defects.push(`PROVIDER_DELTA_${p}`);
    }
  }

  if (
    comparison.rank.prior != null &&
    comparison.rank.current != null &&
    comparison.rank.delta !== comparison.rank.current - comparison.rank.prior
  ) {
    defects.push("RANK_DELTA_ARITHMETIC");
  }

  if (!comparison.benchmark.suppressed) {
    const exp = ppDelta(rateToPp(comparison.benchmark.current), rateToPp(comparison.benchmark.prior));
    if (
      comparison.benchmark.ppDelta != null &&
      exp != null &&
      Math.abs(comparison.benchmark.ppDelta - exp) > 0.05
    ) {
      defects.push("BENCHMARK_DELTA_ARITHMETIC");
    }
  }

  if (!comparison.index.suppressed) {
    const exp =
      comparison.index.prior != null && comparison.index.current != null
        ? comparison.index.current - comparison.index.prior
        : null;
    if (comparison.index.delta != null && exp != null && comparison.index.delta !== exp) {
      defects.push("INDEX_DELTA_ARITHMETIC");
    }
  }

  near(
    comparison.displacementMovement.currentCount,
    (period2Metrics.displacement?.scenarioIds || []).length,
    "DISPLACEMENT_SET_COUNT"
  );
  near(
    comparison.sharedMovement.currentCount,
    (period2Metrics.scenariosShared?.scenarioIds || []).length,
    "SHARED_SET_COUNT"
  );

  if (comparison.peerSetComparability !== "PEER_SET_UNCHANGED") {
    defects.push("PEER_SET_COMPARABILITY");
  }

  return {
    propertyId: comparison.propertyId,
    gate: BPP_PERIOD_1_TO_PERIOD_2_LONGITUDINAL_RECONCILIATION,
    pass: defects.length === 0,
    defects,
  };
}

export function buildPeriod2Narratives(comparison) {
  const { presence, rank, providerMovement, materiality, propertyId } = comparison;
  const drivers = Object.entries(providerMovement)
    .map(([provider, m]) => ({ provider, pp: m.ppDelta || 0 }))
    .sort((a, b) => Math.abs(b.pp) - Math.abs(a.pp));
  const topDriver = drivers[0];

  const abs =
    presence.currentPp != null
      ? `Absolute Portfolio AI Presence is ${presence.currentPp.toFixed(1)}% in this period.`
      : "Absolute Portfolio AI Presence is unavailable.";
  const rel =
    rank.current != null
      ? `Relative position is #${rank.current} of ${rank.currentOf} in the frozen peer universe.`
      : "Relative rank is unavailable.";
  const move =
    materiality === "essentially_unchanged"
      ? `Movement vs Prior Run is essentially unchanged (${formatPpDelta(presence.ppDelta)}; rank ${rank.movement.display}).`
      : materiality === "modest"
        ? `Movement vs Prior Run is modest (${formatPpDelta(presence.ppDelta)}; rank ${rank.movement.display}).`
        : `Movement vs Prior Run is material (${formatPpDelta(presence.ppDelta)}; rank ${rank.movement.display}).`;
  const prov =
    topDriver && Math.abs(topDriver.pp) >= 1
      ? `Largest provider driver: ${topDriver.provider} (${formatPpDelta(topDriver.pp)}).`
      : "No single provider dominates the period-to-period change.";

  const terr = (comparison.territoryMovement || [])
    .filter((t) => t.presencePpDelta != null && Math.abs(t.presencePpDelta) >= MODEST_PP)
    .sort((a, b) => Math.abs(b.presencePpDelta) - Math.abs(a.presencePpDelta))[0];
  const terrLine = terr
    ? `Territory driver: ${terr.territoryId} (${formatPpDelta(terr.presencePpDelta)}).`
    : "No material territory presence swing versus Prior Run.";

  return {
    propertyId,
    absolutePosition: abs,
    relativePosition: rel,
    movement: move,
    providerDrivers: prov,
    territoryDrivers: terrLine,
    materiality,
    portfolioRead: [abs, rel, move, prov, terrLine].join(" "),
    distinguishesAbsoluteVsRelative: true,
    customerFacing: true,
  };
}

/**
 * Enrich ranking rows + KPI meta with Prior Run movement for local Period-2 UI.
 */
export function applyMovementToLocalPayload(payload, comparison) {
  if (!payload || !comparison) return payload;
  const byId = new Map(comparison.rankingMovement.map((r) => [r.canonicalEntityId, r]));
  const next = { ...payload };
  next.periodId = PERIOD_2_ID;
  next.priorPeriodId = PERIOD_1_ID;
  next.comparisonMode = "PRIOR_RUN";
  next.customerPublished = false;
  next.hasPriorPeriod = true;
  next.longitudinal = {
    materiality: comparison.materiality,
    presencePpDelta: comparison.presence.ppDelta,
    rankMovement: comparison.rank.movement,
    providerMovement: comparison.providerMovement,
  };
  next.narrative = buildPeriod2Narratives(comparison);

  if (Array.isArray(next.kpis)) {
    next.kpis = next.kpis.map((k) => {
      if (k.id === "portfolioAiPresence" && comparison.presence.ppDelta != null) {
        return {
          ...k,
          deltaDisplay: formatPpDelta(comparison.presence.ppDelta),
          meta: `${k.meta || ""} · Prior Run ${formatPpDelta(comparison.presence.ppDelta)}`.trim(),
        };
      }
      if (k.id === "portfolioRank" && comparison.rank.movement) {
        return {
          ...k,
          deltaDisplay: comparison.rank.movement.display,
          meta: `${k.meta || ""} · ${comparison.rank.movement.display}`.trim(),
        };
      }
      if (k.id === "portfolioBenchmark" && comparison.benchmark.ppDelta != null) {
        return { ...k, deltaDisplay: formatPpDelta(comparison.benchmark.ppDelta) };
      }
      if (k.id === "portfolioPresenceIndex" && comparison.index.delta != null) {
        const sign = comparison.index.delta > 0 ? "+" : "";
        return { ...k, deltaDisplay: `${sign}${comparison.index.delta}` };
      }
      if (k.id === "numberOneAppearance" && comparison.numberOne.ppDelta != null) {
        return { ...k, deltaDisplay: formatPpDelta(comparison.numberOne.ppDelta) };
      }
      if (k.id === "top3Appearance" && comparison.top3.ppDelta != null) {
        return { ...k, deltaDisplay: formatPpDelta(comparison.top3.ppDelta) };
      }
      return k;
    });
  }

  if (next.ranking?.rows) {
    next.ranking = {
      ...next.ranking,
      hasPriorPeriod: true,
      firstPeriodDeltaLabel: null,
      rows: next.ranking.rows.map((row) => {
        const m = byId.get(row.canonicalEntityId);
        return {
          ...row,
          deltaDisplay: m?.deltaDisplay ?? "—",
          priorRank: m?.priorRank ?? null,
          rankMovementState: m?.state ?? null,
        };
      }),
    };
  }

  next.providerPresence = {
    rows: PROVIDERS.map((p) => {
      const m = comparison.providerMovement[p];
      return {
        provider: p,
        label: p.charAt(0).toUpperCase() + p.slice(1),
        presenceRate: m.currentRate,
        priorPresenceRate: m.priorRate,
        ppDelta: m.ppDelta,
        display: m.display,
        subjectHits: null,
        observations: null,
      };
    }),
  };

  return next;
}

export function runFivePropertyLongitudinal({ snapshotsByProperty }) {
  const byProperty = {};
  const reconciliations = {};
  const defects = [];

  for (const propertyId of PROPERTY_IDS) {
    const snap = snapshotsByProperty[propertyId];
    if (!snap?.metrics) {
      defects.push({ propertyId, defect: "MISSING_PERIOD_2_SNAPSHOT" });
      continue;
    }
    const comparison = comparePropertyPeriod1ToPeriod2({
      propertyId,
      period2Metrics: snap.metrics,
    });
    const recon = reconcileLongitudinalProperty(comparison, snap.metrics);
    byProperty[propertyId] = {
      ...comparison,
      narrative: buildPeriod2Narratives(comparison),
      reconciliation: recon,
    };
    reconciliations[propertyId] = recon;
    if (!recon.pass) defects.push(...recon.defects.map((d) => ({ propertyId, defect: d })));
  }

  const pass = defects.length === 0 && PROPERTY_IDS.every((id) => reconciliations[id]?.pass);
  return {
    gate: BPP_PERIOD_1_TO_PERIOD_2_LONGITUDINAL_RECONCILIATION,
    pass,
    priorPeriodId: PERIOD_1_ID,
    currentPeriodId: PERIOD_2_ID,
    comparisonMode: "PRIOR_RUN",
    byProperty,
    reconciliations,
    defects,
  };
}
