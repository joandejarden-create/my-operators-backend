/**
 * Unified Discovery Engine V7 orchestrator (zero-paid default).
 *
 * Wires Apify V1 provider into Need-Date / PowerShell #15 path without
 * bypassing ORS or auto-starting paid Exa/Apify.
 */

import {
  UNIFIED_DISCOVERY_ENGINE_V7,
  V7_PROVIDER_STRATEGY,
  V7_PROVIDER_STRATEGY_META,
  V7_PIPELINE,
  WATERSTONE_V7_HOTEL,
} from "./unified-discovery-engine-v7-contract.js";
import {
  buildFourteenWorkflowProviderRoutingV7,
  buildV6WaveProviderIntegrationV7,
  selectProvidersForObjectiveV7,
} from "./unified-discovery-provider-routing-v7.js";
import { buildSharedCandidatePoolV7 } from "./unified-discovery-candidate-schema-v7.js";
import { buildUnifiedDiscoveryBudgetOptionsV7 } from "./unified-discovery-budget-options-v7.js";
import {
  runApifyDiscovery,
  createApifySpendLedger,
  isApifyLiveAuthorized,
} from "../research/apify/apify-discovery-provider-v1.js";
import { listEnabledApifyActors } from "../research/apify/apify-actor-registry-v1.js";
import { NEED_DATE_V6_ACCESSIBILITY } from "./need-date-deep-discovery-v6-contract.js";

function assertProviderAuthIsolation(input = {}) {
  const authorizeApify = Boolean(input.authorizeApifyLive);
  const authorizeExa = Boolean(input.authorizeExaLive || input.authorizePaidResearch);
  const apifyBudget = Number(input.apifyBudgetUsd || 0);
  const exaBudget = Number(input.exaBudgetUsd || input.budgetUSD || 0);

  return {
    authorizeApify,
    authorizeExa,
    apifyBudget,
    exaBudget,
    apifyLiveWouldRun: isApifyLiveAuthorized({ authorizeApifyLive: authorizeApify }),
    exaLiveWouldRun: authorizeExa && exaBudget > 0,
    isolated: true,
    note: "Enabling Apify does not authorize Exa; enabling Exa does not authorize Apify",
  };
}

/**
 * Zero-paid preview / mock execution of unified discovery for a hotel campaign.
 */
export async function runUnifiedDiscoveryPreviewV7(input = {}) {
  const hotelId = String(input.hotelId || WATERSTONE_V7_HOTEL.hotelId);
  const providerStrategy =
    input.providerStrategy || V7_PROVIDER_STRATEGY.ADAPTIVE_MULTI_PROVIDER;
  if (!Object.values(V7_PROVIDER_STRATEGY).includes(providerStrategy)) {
    throw new Error(`invalid_provider_strategy:${providerStrategy}`);
  }

  const auth = assertProviderAuthIsolation(input);
  const forceMock = input.forceMockOnly !== false;
  const paidBlocked =
    forceMock ||
    (!auth.apifyLiveWouldRun && !auth.exaLiveWouldRun) ||
    input.authorizePaidResearch !== true;

  // Hard stop: this V7 implementation prompt forbids live paid calls
  if (auth.apifyLiveWouldRun || auth.exaLiveWouldRun) {
    return {
      ok: false,
      version: UNIFIED_DISCOVERY_ENGINE_V7,
      hotelId,
      stoppedAt: "PAID_PROVIDER_AUTHORIZATION",
      message:
        "Unified Discovery V7 zero-paid pack forbids live Apify/Exa. Obtain separate founder GO for Option A/B/C.",
      auth,
      paidProvidersBlocked: true,
      publicationBlocked: true,
    };
  }

  const routing = buildFourteenWorkflowProviderRoutingV7({ providerStrategy });
  const waves = buildV6WaveProviderIntegrationV7();
  const budgetOptions = buildUnifiedDiscoveryBudgetOptionsV7();

  const hotel = {
    hotelId,
    name: WATERSTONE_V7_HOTEL.name,
    city: "Boca Raton",
  };

  const apifyLedger = createApifySpendLedger();
  const exaLedger = {
    version: "gdi-exa-spend-ledger-v7-isolated",
    provider: "exa",
    spentUsd: 0,
    entries: [],
    note: "Isolated from Apify ledger — mock/zero-paid",
  };

  const meta = V7_PROVIDER_STRATEGY_META[providerStrategy];
  const mockApifyCandidates = [];
  const mockExaCandidates = [];

  // Wedding pathway mock (mandatory certification path)
  const weddingSel = selectProvidersForObjectiveV7({
    question: "Find wedding planners and venues near Boca Raton",
    providerStrategy,
  });

  if (meta.usesApify) {
    const knot = await runApifyDiscovery({
      actorId: "dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads",
      hotel,
      researchObjective: "wedding_planners_directory",
      maxItems: 5,
      forceMockOnly: true,
      authorizeApifyLive: false,
      ledger: apifyLedger,
      campaignCeilingUsd: 1,
    });
    if (knot.ok) mockApifyCandidates.push(...(knot.candidates || []));

    const events = await runApifyDiscovery({
      actorId: "solidcode/eventbrite-scraper",
      hotel,
      researchObjective: "dated_public_events",
      maxItems: 5,
      forceMockOnly: true,
      authorizeApifyLive: false,
      ledger: apifyLedger,
      campaignCeilingUsd: 1,
      mockItems: [
        {
          title: "MOCK Boca Raton Business Summit",
          startDate: "2027-09-12",
          endDate: "2027-09-13",
          organizerName: "MOCK South Florida Business Council",
          venueName: "Boca Convention Annex",
          url: "https://example.com/mock-eventbrite-2027",
          city: "Boca Raton",
        },
      ],
    });
    if (events.ok) mockApifyCandidates.push(...(events.candidates || []));
  }

  if (meta.usesExa) {
    mockExaCandidates.push({
      title: "MOCK Exa-discovered Palm Beach Incentive Programme",
      organizationName: "MOCK Incentive Travel Collective",
      startDate: "2027-10-05",
      endDate: "2027-10-08",
      sourceUrl: "https://example.com/mock-exa-incentive",
      city: "Palm Beach",
      exaRequestId: "mock_exa_req_v7",
      discoveryProvider: "exa",
    });
  }

  const pool = buildSharedCandidatePoolV7({
    apifyCandidates: mockApifyCandidates,
    exaCandidates: mockExaCandidates,
    inventory: {
      gdi: [
        {
          title: "Existing Waterstone GDI Competitor Hosted Conference",
          organizationName: "Hilton Boca",
          sourceUrl: "https://example.com/existing-gdi",
        },
      ],
      continuation: WATERSTONE_V7_HOTEL.priorCampaign.watchContinuation.map((code) => ({
        title: code,
        organizationName: code,
      })),
    },
  });

  return {
    ok: true,
    version: UNIFIED_DISCOVERY_ENGINE_V7,
    mode: "preview_mock",
    hotelId,
    hotel,
    needPeriods: WATERSTONE_V7_HOTEL.needPeriods,
    needNote: WATERSTONE_V7_HOTEL.needNote,
    providerStrategy,
    providerStrategyMeta: meta,
    pipeline: V7_PIPELINE,
    routing,
    waves,
    weddingPathway: {
      mandatory: true,
      adaptiveSelection: weddingSel,
      mockExecuted: meta.usesApify,
      note: "Mock pathway only — not a live wedding discovery benchmark",
    },
    sharedCandidatePool: pool,
    ledgers: {
      apify: apifyLedger,
      exa: exaLedger,
      isolation: auth,
    },
    budgetOptions,
    accessibilityModel: NEED_DATE_V6_ACCESSIBILITY,
    paidProvidersBlocked: paidBlocked,
    publicationBlocked: true,
    surfeBlocked: true,
    firecrawlBlocked: true,
    enabledApifyActors: listEnabledApifyActors().map((a) => a.actorId),
    note: "MOCK / ZERO-PAID — not LIVE RESEARCH PASS",
  };
}
