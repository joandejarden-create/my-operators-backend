/**
 * Research depth allocation V6 — commercial accessibility & diversity first.
 * Does not execute paid calls.
 */

import { NEED_DATE_V6_WORKFLOW_PRIORITY } from "./need-date-deep-discovery-v6-contract.js";
import { NEED_DATE_RESEARCH_STRATEGY } from "./need-date-demand-contract-v1.js";

const DISCOVERY_USD = 0.1;
const DEEP_USD = 0.5;

/**
 * Score allocation weight for a workflow coverage row.
 */
export function scoreDepthWeight(row = {}) {
  let w = 0;
  if (row.priority === NEED_DATE_V6_WORKFLOW_PRIORITY.PRIMARY) w += 40;
  else if (row.priority === NEED_DATE_V6_WORKFLOW_PRIORITY.SECONDARY) w += 22;
  else if (row.priority === NEED_DATE_V6_WORKFLOW_PRIORITY.CONDITIONAL) w += 10;
  else return 0;

  if (row.weddingSpecific) w += 18;
  if (row.mandatoryDeepIfCandidates) w += 8;
  if (row.strategyId === NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS) w -= 12; // prior over-allocation penalty
  return Math.max(0, w);
}

/**
 * Build depth allocation plan across need months.
 * @param {object} coveragePlan from buildWaterstoneFourteenWorkflowCoveragePlan
 * @param {object} opts budget + prior spend awareness
 */
export function buildResearchDepthAllocationV6(coveragePlan, opts = {}) {
  const needMonths = coveragePlan?.needMonths || ["2027-08", "2027-09", "2027-10"];
  const active = (coveragePlan?.workflows || []).filter(
    (w) => w.priority !== NEED_DATE_V6_WORKFLOW_PRIORITY.NOT_APPLICABLE,
  );
  const weights = active.map((w) => ({ ...w, weight: scoreDepthWeight(w) }));
  const totalW = weights.reduce((a, b) => a + b.weight, 0) || 1;

  const scenarioBudget = Number(opts.scenarioBudgetUsd || 0);
  const perMonthBudget =
    opts.perMonthBudgetUsd ||
    needMonths.map(() => Number((scenarioBudget / needMonths.length).toFixed(2)));

  const monthPlans = needMonths.map((mk, idx) => {
    const monthCap = Number(perMonthBudget[idx] ?? perMonthBudget[0] ?? 0);
    const discoveries = [];
    const deepSlots = [];

    // Every PRIMARY gets ≥1 discovery per campaign (rotated across months for breadth)
    for (const w of weights) {
      if (w.priority === NEED_DATE_V6_WORKFLOW_PRIORITY.PRIMARY) {
        // Assign primary discoveries: social mandatory every month; others rotate
        if (w.weddingSpecific || idx === weights.indexOf(w) % needMonths.length) {
          discoveries.push({
            strategyId: w.strategyId,
            mode: "discovery",
            costUsd: DISCOVERY_USD,
            objective: discoveryObjective(w, mk),
          });
        }
      }
    }

    // Ensure wedding discovery every month
    if (!discoveries.some((d) => d.strategyId === NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS)) {
      discoveries.push({
        strategyId: NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS,
        mode: "discovery",
        costUsd: DISCOVERY_USD,
        objective: discoveryObjective(
          weights.find((x) => x.weddingSpecific) || {
            strategyId: NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS,
            weddingSpecific: true,
          },
          mk,
        ),
      });
    }

    // SECONDARY: at least one discovery on Sep (gap month) and one other
    if (mk === "2027-09" || idx === 2) {
      for (const w of weights.filter((x) => x.priority === NEED_DATE_V6_WORKFLOW_PRIORITY.SECONDARY)) {
        if (discoveries.length >= 6) break;
        discoveries.push({
          strategyId: w.strategyId,
          mode: "discovery",
          costUsd: DISCOVERY_USD,
          objective: discoveryObjective(w, mk),
        });
      }
    }

    const discoveryCost = discoveries.length * DISCOVERY_USD;
    const deepBudget = Math.max(0, monthCap - discoveryCost);
    const deepCount = Math.floor(deepBudget / DEEP_USD);

    // Allocate deep slots by weight — association capped
    const deepEligible = weights
      .filter((w) => w.mandatoryDeepIfCandidates || w.priority === NEED_DATE_V6_WORKFLOW_PRIORITY.PRIMARY)
      .sort((a, b) => b.weight - a.weight);

    let assocDeep = 0;
    const assocCap = Math.max(1, Math.floor(deepCount / 3)); // diversity: max ~1/3 association

    for (let i = 0; i < deepCount; i++) {
      const pick = deepEligible[i % deepEligible.length];
      if (!pick) break;
      if (pick.strategyId === NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS) {
        if (assocDeep >= assocCap) {
          const alt = deepEligible.find(
            (d) => d.strategyId !== NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS,
          );
          if (alt) {
            deepSlots.push({
              strategyId: alt.strategyId,
              mode: "deep_research",
              costUsd: DEEP_USD,
              condition: "candidate_clears_Wave_C_and_not_competitor_locked_without_overflow_evidence",
            });
            continue;
          }
        }
        assocDeep += 1;
      }
      deepSlots.push({
        strategyId: pick.strategyId,
        mode: "deep_research",
        costUsd: DEEP_USD,
        condition: "candidate_clears_Wave_C_and_not_competitor_locked_without_overflow_evidence",
      });
    }

    const plannedSpend = Number(
      (discoveries.length * DISCOVERY_USD + deepSlots.length * DEEP_USD).toFixed(2),
    );

    return {
      monthKey: mk,
      monthCapUsd: monthCap,
      discoveries,
      deepSlots,
      plannedSpendUsd: plannedSpend,
      associationDeepCap: assocCap,
    };
  });

  const diversityCheck = buildDiversityCheck(monthPlans, coveragePlan);

  return {
    version: "gdi-need-date-research-depth-allocation-v6",
    scenarioBudgetUsd: scenarioBudget,
    allocationFactors: [
      "Expected commercial accessibility",
      "Strength of demand evidence",
      "Need-month relevance",
      "Buyer discoverability",
      "Hotel-specific fit",
      "Research gap resolvability",
      "Incremental opportunity value",
      "Segment diversity (anti association-monopoly)",
    ],
    weightBasis: weights.map((w) => ({
      strategyId: w.strategyId,
      priority: w.priority,
      weight: w.weight,
      share: Number((w.weight / totalW).toFixed(3)),
    })),
    monthPlans,
    plannedAggregateUsd: Number(
      monthPlans.reduce((a, m) => a + m.plannedSpendUsd, 0).toFixed(2),
    ),
    diversityCheck,
    note: "Plan only — paid execution requires separate founder authorization.",
  };
}

function discoveryObjective(w, monthKey) {
  if (w.weddingSpecific || w.strategyId === NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS) {
    return `${monthKey} wedding/social demand-owner discovery — destination weddings, planners, guestroom blocks, off-site venues needing lodging; reject directories.`;
  }
  if (w.strategyId === NEED_DATE_RESEARCH_STRATEGY.MEETINGS_INCENTIVES) {
    return `${monthKey} corporate meetings/training/offsites — buyer-owned programmes, not hotel event calendars.`;
  }
  if (w.strategyId === NEED_DATE_RESEARCH_STRATEGY.SCALING_COMPANIES) {
    return `${monthKey} corporate accounts with plausible Boca/Palm Beach lodging or meeting need.`;
  }
  if (w.strategyId === NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS) {
    return `${monthKey} association programmes without named competing host/block — demand owner first.`;
  }
  return `${monthKey} ${w.strategyId} diversified discovery — demand owners and commercial activities.`;
}

function buildDiversityCheck(monthPlans, coveragePlan) {
  const primary = coveragePlan?.summary?.primary || [];
  const discoveryByStrategy = {};
  const deepByStrategy = {};
  for (const m of monthPlans) {
    for (const d of m.discoveries) {
      discoveryByStrategy[d.strategyId] = (discoveryByStrategy[d.strategyId] || 0) + 1;
    }
    for (const d of m.deepSlots) {
      deepByStrategy[d.strategyId] = (deepByStrategy[d.strategyId] || 0) + 1;
    }
  }

  const primaryMissingDiscovery = primary.filter((id) => !discoveryByStrategy[id]);
  const weddingDiscovery = discoveryByStrategy[NEED_DATE_RESEARCH_STRATEGY.SOCIAL_SPECIAL_EVENTS] || 0;
  const assocDeep = deepByStrategy[NEED_DATE_RESEARCH_STRATEGY.ASSOCIATION_EVENTS] || 0;
  const totalDeep = Object.values(deepByStrategy).reduce((a, b) => a + b, 0) || 1;

  const failures = [];
  if (weddingDiscovery < 3) {
    failures.push("WEDDING_DISCOVERY_INSUFFICIENT — require discovery every need month");
  }
  if (primaryMissingDiscovery.length) {
    failures.push(`PRIMARY_WITHOUT_DISCOVERY: ${primaryMissingDiscovery.join(", ")}`);
  }
  if (assocDeep / totalDeep > 0.4) {
    failures.push("ASSOCIATION_DEEP_OVERCONCENTRATION — exceeds 40% of deep slots");
  }

  return {
    ok: failures.length === 0,
    failures,
    discoveryByStrategy,
    deepByStrategy,
    rule: "If weddings, corporate accounts or local catering are PRIMARY but receive no meaningful deep path when candidates exist, campaign must explain why.",
  };
}
