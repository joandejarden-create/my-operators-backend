/**
 * Need-Date Demand Intelligence orchestrator V1.
 * Preview / CertifyOnly: zero-paid. FullE2E paid path stops at founder GO.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadOpportunities,
  loadHotelProfile,
} from "../repository.js";
import { mergeLocalShadowCommercialOpportunities } from "../local-shadow-commercial-portfolio-v1.js";
import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  NEED_DATE_DEMAND_INTELLIGENCE_V2,
  NEED_DATE_MODE,
  NEED_DATE_POWERSHELL_WORKFLOW_NUMBER,
  WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
  AIRTABLE_MUTATION_BLOCKED,
  PAID_PROVIDER_BLOCKED,
} from "./need-date-demand-contract-v1.js";
import {
  normalizeNeedPeriod,
  validateNeedPeriodHotelLink,
} from "./need-period-normalize-v1.js";
import { matchInventoryToNeedPeriods } from "./need-period-inventory-match-v1.js";
import { selectStrategiesForCampaign } from "./need-period-strategy-selection-v1.js";
import {
  createNeedDateCampaignLedger,
  saveNeedDateCampaignLedger,
} from "./need-period-campaign-ledger-v1.js";
import { proposeNeedDateAirtableSchema } from "./need-period-airtable-schema-proposal-v1.js";
import { buildNeedDateMatchDrawerComponent } from "./need-period-drawer-component-v1.js";
import { certifyNeedDateCampaign } from "./need-date-demand-certification-v1.js";
import { runWaterstoneNeedDatePilotV2 } from "./waterstone-need-date-pilot-v2.js";
import {
  ensureVersionedNeedPeriod,
  diffNeedPeriodCollections,
  reconcileNeedDateAssociations,
  isNeedPeriodCommerciallyOpen,
} from "./need-period-lifecycle-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const FIXTURE_DIR = path.join(
  ROOT,
  "fixtures/group-demand-intelligence/need-date-demand",
);

function text(v) {
  return String(v || "").trim();
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function listFixtureNeedPeriods(hotelId = null) {
  if (!fs.existsSync(FIXTURE_DIR)) return [];
  const files = fs
    .readdirSync(FIXTURE_DIR)
    .filter((f) => f.endsWith("-need-period.json"));
  const out = [];
  for (const f of files) {
    const raw = readJson(path.join(FIXTURE_DIR, f));
    const n = normalizeNeedPeriod(raw);
    if (hotelId && n.hotelId !== hotelId) continue;
    out.push({ ...n, _fixtureFile: f });
  }
  return out;
}

export function loadSyntheticTimingOpportunities(hotelId) {
  const p = path.join(FIXTURE_DIR, "synthetic-timing-scenarios.json");
  if (!fs.existsSync(p)) return [];
  const doc = readJson(p);
  const opps = doc.opportunities || [];
  if (!hotelId) return opps;
  return opps.filter((o) => o.hotelId === hotelId || doc.hotelId === hotelId);
}

function loadHotelContext(hotelId) {
  const profile = loadHotelProfile(hotelId) || {};
  const cfgPath = path.join(
    ROOT,
    `config/group-demand-intelligence/hotels/${hotelId}.json`,
  );
  const cfg = fs.existsSync(cfgPath) ? readJson(cfgPath) : {};
  return {
    hotelId,
    name:
      profile.identity?.hotelName ||
      profile.name ||
      profile.displayName ||
      cfg.displayName ||
      hotelId,
    city: profile.identity?.city || profile.city || cfg.city || null,
    country: profile.identity?.country || profile.country || cfg.country || null,
    profile,
    config: cfg,
  };
}

function loadInventory(hotelId, { includeShadow = true, includeSynthetic = false } = {}) {
  const store = loadOpportunities(hotelId);
  let opps = [...(store?.opportunities || [])];
  if (includeShadow) {
    opps = mergeLocalShadowCommercialOpportunities(hotelId, opps, {
      env: process.env,
    });
  }
  if (includeSynthetic) {
    const syn = loadSyntheticTimingOpportunities(hotelId);
    const byId = new Map(opps.map((o) => [o.id, o]));
    for (const s of syn) {
      if (!byId.has(s.id)) {
        opps.push(s);
        byId.set(s.id, s);
      }
    }
  }
  return opps;
}

async function resolveNeedPeriods({
  hotelId,
  needPeriodId = null,
  periods = null,
  useAirtable = false,
  allowFixtures = true,
}) {
  if (Array.isArray(periods) && periods.length) {
    return periods.map((p) => normalizeNeedPeriod(p, { hotelId }));
  }
  if (useAirtable) {
    const { listNeedPeriodsFromAirtable } = await import(
      "./need-date-airtable-store-v3.js"
    );
    const listed = await listNeedPeriodsFromAirtable(hotelId);
    let rows = listed.periods || [];
    if (needPeriodId) {
      rows = rows.filter((p) => p.needPeriodId === needPeriodId);
    }
    return rows;
  }
  if (!allowFixtures) return [];
  const fixtures = listFixtureNeedPeriods(hotelId);
  if (needPeriodId) {
    const hit = fixtures.find((p) => p.needPeriodId === needPeriodId);
    return hit ? [hit] : [];
  }
  return fixtures;
}

/**
 * Run Need-Date Demand Intelligence campaign (Preview / CertifyOnly safe).
 * When useAirtable/live flag is set, need periods load from AI Demand Positioning.
 */
export async function runNeedDateDemandCampaign(input = {}) {
  const hotelId = text(input.hotelId);
  if (!hotelId) {
    throw new Error("hotel_id_required");
  }

  const mode = text(input.mode || NEED_DATE_MODE.PREVIEW).toLowerCase();
  const authorizePaidResearch = Boolean(input.authorizePaidResearch);
  const authorizePublication = Boolean(input.authorizePublication);
  const budgetUSD = Number(input.budgetUSD || 0);
  const includeSynthetic =
    input.includeSynthetic !== false &&
    (mode === NEED_DATE_MODE.CERTIFY_ONLY ||
      mode === NEED_DATE_MODE.PREVIEW ||
      input.includeSynthetic === true);

  const runId =
    text(input.runId) ||
    `needdate-${new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14)}-${Math.random().toString(16).slice(2, 8)}`;

  // Waterstone V2 pilot (Aug/Sep/Oct 2027) — zero-paid dynamic lifecycle certification
  if (
    input.waterstonePilot === true ||
    (hotelId === WATERSTONE_NEED_DATE_PILOT_HOTEL_ID &&
      input.needDatePilotV2 === true)
  ) {
    const pilot = runWaterstoneNeedDatePilotV2(input);
    let unifiedDiscoveryV7 = null;
    if (input.unifiedDiscoveryV7 !== false) {
      try {
        const { runUnifiedDiscoveryPreviewV7 } = await import(
          "./unified-discovery-orchestrator-v7.js"
        );
        unifiedDiscoveryV7 = await runUnifiedDiscoveryPreviewV7({
          hotelId,
          providerStrategy: input.providerStrategy || "ADAPTIVE_MULTI_PROVIDER",
          forceMockOnly: true,
          authorizeApifyLive: false,
          authorizeExaLive: false,
          authorizePaidResearch: false,
          apifyBudgetUsd: 0,
          exaBudgetUsd: 0,
        });
      } catch (err) {
        unifiedDiscoveryV7 = {
          ok: false,
          error: String(err?.message || err),
        };
      }
    }
    return {
      ...pilot,
      runId,
      mode,
      workflowNumber: NEED_DATE_POWERSHELL_WORKFLOW_NUMBER,
      authorizePublication: false,
      paidProvider: PAID_PROVIDER_BLOCKED,
      providerStrategy: input.providerStrategy || "ADAPTIVE_MULTI_PROVIDER",
      unifiedDiscoveryV7,
    };
  }

  // FullE2E paid path: hard stop without founder GO
  if (
    (mode === NEED_DATE_MODE.FULL_E2E || mode === NEED_DATE_MODE.WEEKLY_INCREMENTAL) &&
    (!authorizePaidResearch || budgetUSD <= 0)
  ) {
    return {
      ok: false,
      version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
      workflowNumber: NEED_DATE_POWERSHELL_WORKFLOW_NUMBER,
      runId,
      mode,
      hotelId,
      stoppedAt: "PAID_PROVIDER_AUTHORIZATION",
      block: PAID_PROVIDER_BLOCKED,
      message:
        "FullE2E / WeeklyIncremental paid discovery requires -AuthorizePaidResearch and -BudgetUSD > 0. Preview/CertifyOnly remain available at $0.",
      airtableMutation: AIRTABLE_MUTATION_BLOCKED,
      schemaProposal: proposeNeedDateAirtableSchema(),
    };
  }

  if (authorizePaidResearch) {
    // Even with flags, this implementation prompt forbids live Exa.
    return {
      ok: false,
      version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
      workflowNumber: NEED_DATE_POWERSHELL_WORKFLOW_NUMBER,
      runId,
      mode,
      hotelId,
      stoppedAt: "PAID_PROVIDER_AUTHORIZATION",
      block: PAID_PROVIDER_BLOCKED,
      message:
        "Live Exa calls are not authorized by the Need-Date V1 implementation prompt. Prepare ledger + strategy plan only; obtain explicit founder spend GO before paid requests.",
      airtableMutation: AIRTABLE_MUTATION_BLOCKED,
    };
  }

  const hotel = loadHotelContext(hotelId);
  const useAirtable =
    input.useAirtable === true ||
    (input.useAirtable !== false &&
      String(process.env.GDI_NEED_DATES_AIRTABLE_LIVE || "").trim() === "1");
  const rawPeriods = await resolveNeedPeriods({
    hotelId,
    needPeriodId: input.needPeriodId || null,
    periods: input.needPeriods || null,
    useAirtable,
    allowFixtures: input.allowFixtures !== false && !useAirtable,
  });

  const needPeriods = [];
  const periodValidation = [];
  for (const raw of rawPeriods) {
    const v = validateNeedPeriodHotelLink(raw, hotelId);
    periodValidation.push(v);
    if (v.ok) needPeriods.push(ensureVersionedNeedPeriod(v.period));
  }

  const inventory = loadInventory(hotelId, {
    includeShadow: input.includeShadow !== false,
    includeSynthetic,
  });

  const openPeriods = needPeriods.filter((p) => isNeedPeriodCommerciallyOpen(p));
  const inventoryMatch = matchInventoryToNeedPeriods(needPeriods, inventory);
  const changesSincePrior = Array.isArray(input.priorNeedPeriods)
    ? diffNeedPeriodCollections(input.priorNeedPeriods, needPeriods)
    : { added: [], removed: [], modified: [], unchanged: needPeriods.length };
  const reconciliation = reconcileNeedDateAssociations({
    periods: needPeriods,
    matches: inventoryMatch.matches || [],
    historyChanges: input.needPeriodHistoryChanges || [],
  });

  let commercialProfile = input.commercialProfile || null;
  if (!commercialProfile) {
    try {
      const { loadCommercialProfile } = await import(
        "./hotel-commercial-profile-store-v1.js"
      );
      commercialProfile = await loadCommercialProfile(hotelId, {
        useAirtable,
        allowLocalFallback: true,
      });
    } catch {
      commercialProfile = null;
    }
  }

  const strategyPlan = selectStrategiesForCampaign({
    needPeriods: openPeriods.length ? openPeriods : needPeriods,
    hotel,
    inventoryMatch,
    maxStrategies: Number(input.maxStrategies) || 6,
    commercialProfile,
  });

  let unifiedDiscoveryV7 = null;
  if (input.unifiedDiscoveryV7 !== false) {
    try {
      const { runUnifiedDiscoveryPreviewV7 } = await import(
        "./unified-discovery-orchestrator-v7.js"
      );
      unifiedDiscoveryV7 = await runUnifiedDiscoveryPreviewV7({
        hotelId,
        providerStrategy: input.providerStrategy || "ADAPTIVE_MULTI_PROVIDER",
        forceMockOnly: true,
        authorizeApifyLive: Boolean(input.authorizeApifyLive),
        authorizeExaLive: Boolean(input.authorizeExaLive || input.authorizePaidResearch),
        authorizePaidResearch: Boolean(input.authorizePaidResearch),
        apifyBudgetUsd: Number(input.apifyBudgetUsd || 0),
        exaBudgetUsd: Number(input.exaBudgetUsd || input.budgetUSD || 0),
      });
    } catch (err) {
      unifiedDiscoveryV7 = {
        ok: false,
        error: String(err?.message || err),
      };
    }
  }

  const campaignLedger = createNeedDateCampaignLedger({
    campaignId: runId,
    hotelId,
    needPeriodIds: needPeriods.map((p) => p.needPeriodId),
    selectedWorkflows: strategyPlan.strategyIds || [],
    mode,
    authorizePaidResearch: false,
    budgetCeilingUsd: 0,
  });
  campaignLedger.existingMatches = inventoryMatch.existingMatchCount;
  campaignLedger.deliveryStatus =
    mode === NEED_DATE_MODE.CERTIFY_ONLY ? "CERTIFY" : "PREVIEW";
  campaignLedger.researchEnd = new Date().toISOString();

  if (input.persistLedger) {
    campaignLedger._path = saveNeedDateCampaignLedger(campaignLedger);
  }

  const drawerComponents = [];
  for (const m of inventoryMatch.matches || []) {
    if (!m.actionable && m.timing?.overlapStatus !== "ONGOING_ACCOUNT_POTENTIAL") {
      // still build for confirmed/probable
    }
    const period = needPeriods.find((p) => p.needPeriodId === m.needPeriodId);
    const block = buildNeedDateMatchDrawerComponent({
      needPeriod: period,
      timingMatch: m.timing,
      association: m.association,
      opportunity: { id: m.opportunityId, title: m.title },
    });
    if (block) drawerComponents.push(block);
  }

  const spendEstimate = {
    currency: "USD",
    discoveryPlanUsd: null,
    deepResearchAllowanceUsd: null,
    adaptiveFollowUpUsd: null,
    aggregateHardCeilingUsd: null,
    note: "Spend estimate deferred until founder authorizes a costed campaign. No prior balances reused.",
  };

  const researchGaps = summarizeResearchGaps(inventoryMatch);

  const report = {
    ok: true,
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    workflowNumber: NEED_DATE_POWERSHELL_WORKFLOW_NUMBER,
    runId,
    mode,
    hotelId,
    hotel,
    needPeriods,
    activeNeedPeriods: openPeriods,
    periodValidation,
    changesSincePrior,
    reconciliation,
    inventoryCount: inventory.length,
    inventoryMatch,
    strategyPlan,
    providerStrategy: input.providerStrategy || "ADAPTIVE_MULTI_PROVIDER",
    unifiedDiscoveryV7,
    researchGaps,
    spendEstimate,
    campaignLedger,
    drawerComponents,
    authorizePublication: false,
    airtableMutation: AIRTABLE_MUTATION_BLOCKED,
    schemaProposal: proposeNeedDateAirtableSchema(),
    paidProvider: PAID_PROVIDER_BLOCKED,
    outputCategories: {
      existingMatches: inventoryMatch.existingMatchCount,
      existingEnriched: 0,
      newUnique: 0,
      note: "Categories B/C require authorized research; Preview only produces associations (A).",
    },
    nextSteps: [
      "Review existing matches and timing overlap statuses.",
      "Approve Airtable GDI Hotel Need Periods schema before live reads/writes.",
      "Approve isolated campaign budget before Exa discovery.",
      "Authorize publication separately after local staging.",
    ],
  };

  report.certification = certifyNeedDateCampaign({
    ...report,
    certificationContext: {
      requireBuyerPass: false,
      browserPass: false,
      browserNote: "Use CertifyOnly + local browser harness when requested",
    },
  });

  return report;
}

function summarizeResearchGaps(inventoryMatch) {
  const counts = {};
  for (const m of inventoryMatch.matches || []) {
    for (const g of m.researchGapHints || []) {
      counts[g] = (counts[g] || 0) + 1;
    }
  }
  return {
    byHint: counts,
    enrichmentCandidates: (inventoryMatch.matches || []).filter(
      (m) => (m.researchGapHints || []).length > 0,
    ).length,
    discoveryRecommended: (inventoryMatch.confirmedCount || 0) < 3,
  };
}

export {
  NEED_DATE_MODE,
  listFixtureNeedPeriods as listNeedDateFixtures,
};
