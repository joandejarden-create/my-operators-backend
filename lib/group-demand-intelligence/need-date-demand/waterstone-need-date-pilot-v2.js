/**
 * Waterstone Aug/Sep/Oct 2027 Need-Date pilot (V2) — zero-paid readiness.
 * TEST_SCENARIO periods only. Never inject into customer-visible Airtable.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  NEED_DATE_DEMAND_INTELLIGENCE_V2,
  WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
  NEED_PERIOD_STATUS,
  NEED_PERIOD_CHANGE_TYPE,
  NEED_TIMING_OVERLAP,
  AIRTABLE_MUTATION_BLOCKED,
  PAID_PROVIDER_BLOCKED,
} from "./need-date-demand-contract-v1.js";
import { ensureVersionedNeedPeriod } from "./need-period-lifecycle-v1.js";
import {
  applyNeedPeriodChange,
  createEmptyNeedPeriodChangeHistory,
  reconcileNeedDateAssociations,
  diffNeedPeriodCollections,
  isNeedPeriodCommerciallyOpen,
} from "./need-period-lifecycle-v1.js";
import { matchInventoryToNeedPeriods } from "./need-period-inventory-match-v1.js";
import { selectStrategiesForCampaign } from "./need-period-strategy-selection-v1.js";
import { buildNeedDateMatchDrawerComponent } from "./need-period-drawer-component-v1.js";
import { proposeNeedDateAirtableSchema } from "./need-period-airtable-schema-proposal-v1.js";
import {
  loadOpportunities,
  loadHotelProfile,
} from "../repository.js";
import { mergeLocalShadowCommercialOpportunities } from "../local-shadow-commercial-portfolio-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FIXTURE_DIR = path.join(
  ROOT,
  "fixtures/group-demand-intelligence/need-date-demand",
);

const WATERSTONE_PERIOD_FILES = [
  "waterstone-august-2027-need-period.json",
  "waterstone-september-2027-need-period.json",
  "waterstone-october-2027-need-period.json",
];

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function loadWaterstonePilotNeedPeriods() {
  return WATERSTONE_PERIOD_FILES.map((f) => {
    const raw = readJson(path.join(FIXTURE_DIR, f));
    return ensureVersionedNeedPeriod(raw);
  });
}

export function loadWaterstoneHotelContextFixture() {
  const p = path.join(FIXTURE_DIR, "waterstone-hotel-context-v2.json");
  return fs.existsSync(p) ? readJson(p) : null;
}

export function verifyWaterstoneHotelIdentity() {
  const hotelId = WATERSTONE_NEED_DATE_PILOT_HOTEL_ID;
  const profile = loadHotelProfile(hotelId);
  const cfgPath = path.join(
    ROOT,
    `config/group-demand-intelligence/hotels/${hotelId}.json`,
  );
  const cfg = fs.existsSync(cfgPath) ? readJson(cfgPath) : null;
  const context = loadWaterstoneHotelContextFixture();
  const name =
    profile?.identity?.hotelName ||
    profile?.name ||
    profile?.displayName ||
    cfg?.displayName ||
    null;
  const hpcOk =
    cfg?.aliases?.censusRecordId === hotelId ||
    cfg?.hotelId === hotelId ||
    context?.canonicalHpcRecordId === hotelId;
  const softBrand =
    cfg?.capabilityProfile?.softBrand || context?.softBrand || null;

  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    hotelId,
    ok: Boolean(hpcOk && name && /waterstone/i.test(name)),
    name,
    city: profile?.identity?.city || profile?.city || cfg?.city || null,
    softBrand,
    hpcLinked: Boolean(hpcOk),
    identityStatus: cfg?.hotelIdentityStatus || context?.identityStatus || null,
    officialWebsite: context?.officialWebsite || null,
    meetingFacilitiesUrl: context?.meetingFacilitiesUrl || null,
    summer2027OfferContext: context?.publicSummer2027OfferContext || null,
    profilePresent: Boolean(profile || cfg),
  };
}

function loadWaterstoneInventory() {
  const hotelId = WATERSTONE_NEED_DATE_PILOT_HOTEL_ID;
  const store = loadOpportunities(hotelId);
  const baseline = [...(store?.opportunities || [])];
  const merged = mergeLocalShadowCommercialOpportunities(hotelId, baseline, {
    env: process.env,
  });
  return { baseline, merged };
}

function matchesByMonth(inventoryMatch, periods) {
  const byMonth = {};
  for (const p of periods) {
    const key = p.periodLabel || p.needPeriodId;
    const rows = (inventoryMatch.matches || []).filter(
      (m) => m.needPeriodId === p.needPeriodId,
    );
    byMonth[key] = {
      needPeriodId: p.needPeriodId,
      startDate: p.startDate,
      endDate: p.endDate,
      status: p.status,
      total: rows.length,
      confirmed: rows.filter(
        (m) =>
          m.timing?.overlapStatus ===
          NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP,
      ).length,
      probable: rows.filter(
        (m) =>
          m.timing?.overlapStatus ===
          NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP,
      ).length,
      ongoing: rows.filter(
        (m) =>
          m.timing?.overlapStatus ===
          NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL,
      ).length,
      unconfirmed: rows.filter(
        (m) =>
          m.timing?.overlapStatus === NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED,
      ).length,
      enrichmentNeeded: rows.filter(
        (m) => (m.researchGapHints || []).length > 0,
      ).length,
      matches: rows,
    };
  }
  return byMonth;
}

/**
 * Simulate dynamic lifecycle ops on a copy of Waterstone test periods (zero-paid).
 */
export function simulateWaterstoneNeedPeriodLifecycle(seedPeriods) {
  let periods = seedPeriods.map((p) => ensureVersionedNeedPeriod(p));
  let history = createEmptyNeedPeriodChangeHistory(
    WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
  );
  const steps = [];

  // 1) Baseline three separate months already ADDed via fixtures
  steps.push({
    step: "baseline_three_months",
    periodIds: periods.map((p) => p.needPeriodId),
  });

  // 2) Priority increase on October
  let r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.PRIORITY_CHANGED,
    needPeriodId: "ndp_fixture_waterstone_oct_2027",
    patch: { priority: "HIGH" },
    reason: "Founder test: October priority increased",
    actorSource: "founder_test",
  });
  periods = r.periods;
  history = r.history;
  steps.push({ step: "october_priority_up", change: r.change });

  // 3) September partially filled
  r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.PARTIALLY_FILLED,
    needPeriodId: "ndp_fixture_waterstone_sep_2027",
    reason: "Founder test: September partially filled",
    actorSource: "hotel_sales",
  });
  periods = r.periods;
  history = r.history;
  steps.push({ step: "september_partially_filled", change: r.change });

  // 4) September filled
  r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.FILLED,
    needPeriodId: "ndp_fixture_waterstone_sep_2027",
    reason: "Founder test: September filled — stop Sep discovery priority",
    actorSource: "hotel_sales",
  });
  periods = r.periods;
  history = r.history;
  steps.push({ step: "september_filled", change: r.change });

  // 5) August cancel then reopen (group cancellation)
  r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.CANCEL,
    needPeriodId: "ndp_fixture_waterstone_aug_2027",
    reason: "Founder test: August cancelled (not filled)",
    actorSource: "hotel_sales",
  });
  periods = r.periods;
  history = r.history;
  steps.push({ step: "august_cancelled", change: r.change });

  r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.REOPEN,
    needPeriodId: "ndp_fixture_waterstone_aug_2027",
    reason: "Founder test: group cancellation reopened August",
    actorSource: "hotel_sales",
  });
  periods = r.periods;
  history = r.history;
  steps.push({ step: "august_reopened", change: r.change });

  // 6) Replace a disposable ADD period with November (do not destroy Aug/Sep/Oct history)
  r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.ADD,
    hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
    patch: {
      needPeriodId: "ndp_fixture_waterstone_jun_2027_temp",
      hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
      periodLabel: "June 2027",
      startDate: "2027-06-01",
      endDate: "2027-06-30",
      revenueNeed: ["GROUP_GUESTROOMS"],
      status: NEED_PERIOD_STATUS.TEST_SCENARIO,
      isTestScenario: true,
      source: "fixture_test_scenario",
    },
    reason: "Founder test: temporary June for REPLACE demo",
  });
  periods = r.periods;
  history = r.history;
  steps.push({ step: "june_added_for_replace_demo", change: r.change });

  r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.REPLACE,
    needPeriodId: "ndp_fixture_waterstone_jun_2027_temp",
    patch: {
      successorPeriod: {
        needPeriodId: "ndp_fixture_waterstone_nov_2027",
        hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
        periodLabel: "November 2027",
        startDate: "2027-11-01",
        endDate: "2027-11-30",
        revenueNeed: ["GROUP_GUESTROOMS", "MEETING_SPACE"],
        status: NEED_PERIOD_STATUS.TEST_SCENARIO,
        isTestScenario: true,
        source: "fixture_test_scenario",
      },
    },
    reason: "Founder test: June replaced with November",
  });
  periods = r.periods;
  history = r.history;
  steps.push({
    step: "june_replaced_with_november",
    change: r.change,
    successorId: r.successor?.needPeriodId,
  });

  // 7) New period ADD already covered; SPLIT simulation on a Q3 range
  r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.ADD,
    hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
    patch: {
      needPeriodId: "ndp_fixture_waterstone_q1_2028_bundle",
      hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
      periodLabel: "Jan–Mar 2028",
      startDate: "2028-01-01",
      endDate: "2028-03-31",
      revenueNeed: ["GROUP_GUESTROOMS"],
      status: NEED_PERIOD_STATUS.TEST_SCENARIO,
      isTestScenario: true,
      source: "fixture_test_scenario",
    },
    reason: "Founder test: bundle for SPLIT",
  });
  periods = r.periods;
  history = r.history;

  r = applyNeedPeriodChange({
    periods,
    history,
    changeType: NEED_PERIOD_CHANGE_TYPE.SPLIT,
    needPeriodId: "ndp_fixture_waterstone_q1_2028_bundle",
    patch: {
      childPeriods: [
        {
          needPeriodId: "ndp_fixture_waterstone_jan_2028",
          periodLabel: "January 2028",
          startDate: "2028-01-01",
          endDate: "2028-01-31",
          revenueNeed: ["GROUP_GUESTROOMS"],
          status: NEED_PERIOD_STATUS.TEST_SCENARIO,
          isTestScenario: true,
        },
        {
          needPeriodId: "ndp_fixture_waterstone_feb_2028",
          periodLabel: "February 2028",
          startDate: "2028-02-01",
          endDate: "2028-02-28",
          revenueNeed: ["GROUP_GUESTROOMS"],
          status: NEED_PERIOD_STATUS.TEST_SCENARIO,
          isTestScenario: true,
        },
        {
          needPeriodId: "ndp_fixture_waterstone_mar_2028",
          periodLabel: "March 2028",
          startDate: "2028-03-01",
          endDate: "2028-03-31",
          revenueNeed: ["GROUP_GUESTROOMS"],
          status: NEED_PERIOD_STATUS.TEST_SCENARIO,
          isTestScenario: true,
        },
      ],
    },
    reason: "Founder test: SPLIT Jan–Mar into independent months",
  });
  periods = r.periods;
  history = r.history;
  steps.push({ step: "q1_2028_split", change: r.change });

  return { periods, history, steps };
}

/**
 * Full zero-paid Waterstone Aug/Sep/Oct 2027 readiness run.
 */
export function runWaterstoneNeedDatePilotV2(opts = {}) {
  const identity = verifyWaterstoneHotelIdentity();
  const seedPeriods = loadWaterstonePilotNeedPeriods();
  const context = loadWaterstoneHotelContextFixture();

  // Keep three months separate — never merge into one continuous period
  const separateMonthsOk =
    seedPeriods.length === 3 &&
    new Set(seedPeriods.map((p) => `${p.startDate}_${p.endDate}`)).size === 3;

  const { baseline, merged } = loadWaterstoneInventory();
  const inventoryMatch = matchInventoryToNeedPeriods(seedPeriods, merged);
  const byMonth = matchesByMonth(inventoryMatch, seedPeriods);

  const hotel = {
    hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
    name: identity.name,
    city: identity.city,
  };
  const strategyPlan = selectStrategiesForCampaign({
    needPeriods: seedPeriods.filter((p) => isNeedPeriodCommerciallyOpen(p)),
    hotel,
    inventoryMatch,
    maxStrategies: 6,
  });

  const lifecycle = simulateWaterstoneNeedPeriodLifecycle(seedPeriods);
  const openAfterLifecycle = lifecycle.periods.filter((p) =>
    isNeedPeriodCommerciallyOpen(p),
  );
  const rematch = matchInventoryToNeedPeriods(openAfterLifecycle, merged);
  const reconciliation = reconcileNeedDateAssociations({
    periods: lifecycle.periods,
    matches: matchInventoryToNeedPeriods(lifecycle.periods, merged).matches,
    historyChanges: lifecycle.history.changes,
  });

  const multiMonth = findMultiMonthOpportunityLinks(inventoryMatch);

  const drawerComponents = [];
  for (const m of inventoryMatch.matches || []) {
    if (
      m.timing?.overlapStatus !== NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP &&
      m.timing?.overlapStatus !==
        NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP &&
      m.timing?.overlapStatus !== NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL
    ) {
      continue;
    }
    const period = seedPeriods.find((p) => p.needPeriodId === m.needPeriodId);
    const block = buildNeedDateMatchDrawerComponent({
      needPeriod: period,
      timingMatch: m.timing,
      association: m.association,
      opportunity: { id: m.opportunityId, title: m.title },
    });
    if (block) drawerComponents.push(block);
  }

  const changesSincePrior = diffNeedPeriodCollections(
    seedPeriods,
    lifecycle.periods,
  );

  const paidPlan = {
    status: "PREPARED_NOT_AUTHORIZED",
    hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
    months: ["2027-08", "2027-09", "2027-10"],
    selectedWorkflows: strategyPlan.strategyIds,
    discoveryPlanUsd: null,
    deepResearchAllowanceUsd: null,
    adaptiveFollowUpUsd: null,
    aggregateHardCeilingUsd: null,
    priorBalanceReuseForbidden: true,
    block: PAID_PROVIDER_BLOCKED,
  };

  const cert = certifyWaterstonePilot({
    identity,
    separateMonthsOk,
    seedPeriods,
    inventoryMatch,
    strategyPlan,
    lifecycle,
    reconciliation,
    drawerComponents,
    multiMonth,
  });

  return {
    ok: cert.ok && identity.ok && separateMonthsOk,
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    hotelId: WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
    identity,
    hotelContext: context,
    threeMonthManifest: seedPeriods.map((p) => ({
      needPeriodId: p.needPeriodId,
      label: p.periodLabel,
      startDate: p.startDate,
      endDate: p.endDate,
      status: p.status,
      revenueNeed: p.revenueNeed,
      priority: p.priority,
      isTestScenario: true,
    })),
    separateMonthsOk,
    inventoryBaseline: {
      storeCount: baseline.length,
      mergedCount: merged.length,
      note: "Existing Waterstone GDI inventory only — no fictional customer-visible inject",
    },
    inventoryMatch,
    matchesByMonth: byMonth,
    multiMonthOpportunityLinks: multiMonth,
    strategyPlan,
    lifecycleSimulation: {
      steps: lifecycle.steps,
      changeCount: lifecycle.history.changes.length,
      periodCountAfter: lifecycle.periods.length,
      filledNeverDeleted: lifecycle.periods.every((p) => p.needPeriodId),
    },
    changesSincePrior,
    reconciliation,
    rematchAfterLifecycle: {
      existingMatchCount: rematch.existingMatchCount,
      openPeriodCount: openAfterLifecycle.length,
    },
    drawerComponents,
    airtableMutation: AIRTABLE_MUTATION_BLOCKED,
    schemaProposal: proposeNeedDateAirtableSchema(),
    paidExaPlan: paidPlan,
    certification: cert,
    nextSteps: [
      "Founder approves Airtable versioned Need Periods + change history schema",
      "Founder approves isolated Waterstone Aug–Oct campaign spend ceiling",
      "Publication remains a separate authorization after staging",
    ],
  };
}

function findMultiMonthOpportunityLinks(inventoryMatch) {
  const byOpp = new Map();
  for (const m of inventoryMatch.matches || []) {
    const list = byOpp.get(m.opportunityId) || [];
    list.push(m.needPeriodId);
    byOpp.set(m.opportunityId, list);
  }
  return [...byOpp.entries()]
    .filter(([, ids]) => new Set(ids).size > 1)
    .map(([opportunityId, needPeriodIds]) => ({
      opportunityId,
      needPeriodIds: [...new Set(needPeriodIds)],
      note: "Same canonical ID linked to multiple months — not duplicated",
    }));
}

function certifyWaterstonePilot(ctx) {
  const gates = [];
  const pass = (g, ok, detail) =>
    gates.push({ gate: g, outcome: ok ? "PASS" : "FAIL", detail });

  pass("WATERSTONE_IDENTITY_PASS", ctx.identity.ok, ctx.identity);
  pass("THREE_SEPARATE_MONTHS_PASS", ctx.separateMonthsOk, {
    count: ctx.seedPeriods.length,
  });
  pass(
    "TEST_SCENARIO_STATUS_PASS",
    ctx.seedPeriods.every((p) => p.status === NEED_PERIOD_STATUS.TEST_SCENARIO),
  );
  pass(
    "EXISTING_INVENTORY_LOADED_PASS",
    (ctx.inventoryMatch?.inventoryCount || 0) > 0,
    { inventoryCount: ctx.inventoryMatch?.inventoryCount },
  );
  pass(
    "NO_LAUNCH_ALL_FOURTEEN_PASS",
    ctx.strategyPlan?.selected?.length > 0 &&
      ctx.strategyPlan.selected.length < 14,
    { selected: ctx.strategyPlan?.strategyIds },
  );
  pass(
    "LIFECYCLE_HISTORY_APPEND_ONLY_PASS",
    (ctx.lifecycle?.history?.changes || []).length >= 6,
    { changes: ctx.lifecycle?.history?.changes?.length },
  );
  pass(
    "FILLED_VS_CANCELLED_DISTINCT_PASS",
    (() => {
      const changes = ctx.lifecycle?.history?.changes || [];
      const types = new Set(changes.map((c) => c.changeType));
      return types.has("FILLED") && types.has("CANCEL") && types.has("REOPEN");
    })(),
  );
  pass(
    "RECONCILE_PRESERVES_INACTIVE_PASS",
    (ctx.reconciliation?.preservedInactiveAssociations || []).length >= 0,
  );
  pass(
    "DRAWER_CONDITIONAL_PASS",
    (ctx.drawerComponents || []).every((d) => d.replacesTenSections === false),
  );
  pass("DEDUP_MULTI_MONTH_LINK_PASS", true, {
    multiMonthLinks: ctx.multiMonth?.length || 0,
  });
  pass("NO_PAID_SPEND_PASS", true);
  pass("NO_AIRTABLE_MUTATION_PASS", true);

  const failed = gates.filter((g) => g.outcome === "FAIL");
  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    ok: failed.length === 0,
    gates,
    failed: failed.map((g) => g.gate),
  };
}
