/**
 * FullE2E Apify orchestration bridge V1.
 *
 * When the adaptive provider strategy selects Apify, the orchestrator MUST
 * either dispatch a real/mocked Actor run or return an explicit blocked status.
 * Budget reservation alone is an ORCHESTRATION_DEFECT.
 *
 * Default: mock / fail-closed. Live requires authorizeApifyLive + GDI_APIFY_LIVE=1.
 */
import {
  selectNextDiscoveryProvider,
} from "./apify-adaptive-provider-selection-v1.js";
import {
  runApifyDiscovery,
  createApifySpendLedger,
  isApifyLiveAuthorized,
  APIFY_DISCOVERY_PROVIDER_V1,
} from "./apify-discovery-provider-v1.js";
import {
  dedupeApifyAgainstInventory,
  commercialRelevanceScreen,
} from "./apify-cross-provider-dedup-v1.js";
import { listEnabledApifyActors } from "./apify-actor-registry-v1.js";

export const APIFY_FULLE2E_ORCHESTRATION_V1 =
  "gdi-apify-fulle2e-orchestration-v1";

/** Workflows where Apify is a preferred primary discovery provider. */
export const APIFY_PRIMARY_WORKFLOW_IDS = Object.freeze([
  "sports_entertainment",
  "social_special_events",
  "preferred_lodging_partnerships",
  "preferred_lodging",
  "professional_event_planners",
  "local_catering_day_meetings",
  "local_events_and_meetings",
]);

export function workflowPrefersApify(workflowId = "") {
  return APIFY_PRIMARY_WORKFLOW_IDS.includes(String(workflowId || ""));
}

export function buildApifyResearchObjective(workflow = {}, hotel = {}) {
  const id = workflow.id || workflow.workflowId || "";
  const hotelName = hotel.displayName || hotel.name || "hotel";
  const market = hotel.market || hotel.city || "";
  if (id === "sports_entertainment") {
    return `Sports entertainment tournament calendar events near ${hotelName} ${market}`;
  }
  if (id === "social_special_events") {
    return `Wedding planners and social event venues near ${hotelName} ${market}`;
  }
  if (id === "professional_event_planners") {
    return `Professional event planners wedding vendor directory near ${hotelName}`;
  }
  if (id === "preferred_lodging_partnerships") {
    return `Local venues without rooms medical university business park near ${hotelName}`;
  }
  if (id === "local_catering_day_meetings") {
    return `Local public events meeting calendar Eventbrite Meetup near ${hotelName} ${market}`;
  }
  return `Local demand generators near ${hotelName} ${market}`;
}

/**
 * Select + optionally dispatch Apify for a FullE2E workflow discovery step.
 */
export async function orchestrateApifyDiscoveryForWorkflow({
  workflow = {},
  hotel = {},
  inventory = [],
  authorizeApifyLive = false,
  forceMockOnly = true,
  campaignCeilingUsd = null,
  ledger = null,
  mockItems = null,
  priorProviders = [],
} = {}) {
  const researchObjective = buildApifyResearchObjective(workflow, hotel);
  const selection = selectNextDiscoveryProvider({
    question: researchObjective,
    researchObjective,
    priorProviders,
  });

  const prefers = workflowPrefersApify(workflow.id || workflow.workflowId);
  const selectedApify =
    selection.provider === "apify" ||
    (prefers && selection.provider !== "exa" && (selection.actors || []).length);

  if (!selectedApify && !prefers) {
    return {
      version: APIFY_FULLE2E_ORCHESTRATION_V1,
      status: "SKIPPED_EXA_PREFERRED",
      selection,
      dispatched: false,
      candidates: [],
      ledger: ledger || createApifySpendLedger(),
    };
  }

  const actorId =
    (selection.actors || [])[0]?.actorId ||
    (listEnabledApifyActors()[0] || {}).actorId ||
    null;

  if (!actorId) {
    return {
      version: APIFY_FULLE2E_ORCHESTRATION_V1,
      status: "BLOCKED_NO_ENABLED_ACTOR",
      selection,
      dispatched: false,
      candidates: [],
      ledger: ledger || createApifySpendLedger(),
      error: "No enabled Apify Actor available for dispatch",
    };
  }

  // When strategy requires Apify but live is not authorized and no mocks:
  // return explicit blocked — never silently skip after budgeting Apify.
  if (
    !forceMockOnly &&
    authorizeApifyLive &&
    !isApifyLiveAuthorized({ authorizeApifyLive }) &&
    !mockItems
  ) {
    return {
      version: APIFY_FULLE2E_ORCHESTRATION_V1,
      status: "BLOCKED_LIVE_NOT_AUTHORIZED",
      selection,
      actorId,
      dispatched: false,
      candidates: [],
      ledger: ledger || createApifySpendLedger(),
      error:
        "Apify selected by strategy but live authorization / GDI_APIFY_LIVE missing",
    };
  }

  const spendLedger = ledger || createApifySpendLedger();
  const run = await runApifyDiscovery({
    actorId,
    input: {
      maxItems: 15,
      query: researchObjective,
      location: hotel.city || hotel.market || "",
    },
    hotel,
    researchObjective,
    authorizeApifyLive: authorizeApifyLive && !forceMockOnly,
    forceMockOnly: forceMockOnly || !authorizeApifyLive,
    campaignCeilingUsd,
    ledger: spendLedger,
    mockItems:
      mockItems ||
      [
        {
          title: `Mock ${workflow.id || "apify"} discovery event`,
          name: `Mock organizer for ${hotel.displayName || hotel.name || "hotel"}`,
          url: "https://example.com/mock-apify-event",
          startDate: "2027-03-15",
          location: hotel.city || "Boca Raton",
          organizer: { name: "Mock Event Organizer LLC" },
        },
      ],
  });

  if (!run.ok) {
    return {
      version: APIFY_FULLE2E_ORCHESTRATION_V1,
      status: run.status || "FAILED",
      selection,
      actorId,
      dispatched: true,
      run,
      candidates: [],
      ledger: spendLedger,
      error: run.error || "Apify discovery failed",
    };
  }

  const normalized = run.candidates || [];
  const inventoryBag =
    Array.isArray(inventory)
      ? { gdi: inventory }
      : inventory && typeof inventory === "object"
        ? inventory
        : { gdi: [] };
  const deduped = dedupeApifyAgainstInventory(normalized, inventoryBag);
  const screened = (deduped.uniqueCandidates || []).map((c) =>
    commercialRelevanceScreen(c),
  );

  return {
    version: APIFY_FULLE2E_ORCHESTRATION_V1,
    provider: APIFY_DISCOVERY_PROVIDER_V1,
    status: "DISPATCHED_OK",
    selection,
    actorId,
    dispatched: true,
    run,
    candidates: screened,
    dedup: deduped,
    ledger: spendLedger,
    publicationBlocked: true,
    orsHandoffPreserved: true,
    commercialPrequalExecuted: true,
    classified: true,
  };
}
