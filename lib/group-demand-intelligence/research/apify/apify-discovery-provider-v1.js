/**
 * Shared Apify Discovery Provider V1.
 *
 * Default: mock / fail-closed.
 * Live HTTP requires:
 *   opts.authorizeApifyLive === true
 *   AND process.env.GDI_APIFY_LIVE === "1"
 *   AND process.env.APIFY_TOKEN
 *
 * Does not bypass ORS. Does not publish. Browser never receives tokens.
 */

import { randomBytes } from "node:crypto";
import {
  getApifyActorById,
  listEnabledApifyActors,
  APIFY_ACTOR_REGISTRY_V1,
} from "./apify-actor-registry-v1.js";
import { normalizeApifyDatasetItems } from "./apify-source-normalize-v1.js";

export const APIFY_DISCOVERY_PROVIDER_V1 = "gdi-apify-discovery-provider-v1";

const APIFY_API = "https://api.apify.com/v2";

export function isApifyLiveAuthorized(opts = {}) {
  return (
    opts.authorizeApifyLive === true &&
    String(process.env.GDI_APIFY_LIVE || "") === "1" &&
    Boolean(String(process.env.APIFY_TOKEN || "").trim())
  );
}

export function createApifySpendLedger() {
  return {
    version: "gdi-apify-spend-ledger-v1",
    provider: "apify",
    spentUsd: 0,
    entries: [],
    halted: false,
    haltReason: null,
  };
}

function estimateRunCostUsd(actor, maxItems = 25) {
  const id = actor?.actorId || "";
  // Conservative planning estimates from Store Bronze pricing (verify at run time).
  if (id.includes("eventbrite") && id.includes("parseforge")) {
    return Number((0.006 + maxItems * 0.0045).toFixed(4));
  }
  if (id.includes("crawler-google-places")) {
    return Number((0.001 + maxItems * 0.004).toFixed(4));
  }
  if (id.includes("facebook-events")) {
    return Number((0.001 + maxItems * 0.01).toFixed(4));
  }
  if (id.includes("website-content-crawler")) {
    return Number((0.05 + maxItems * 0.01).toFixed(4)); // compute-usage proxy
  }
  if (id.includes("meetup")) {
    return Number((0.001 + maxItems * 0.001).toFixed(4));
  }
  if (id.includes("wedding") || id.includes("knot") || id.includes("weddingwire")) {
    return Number((0.001 + maxItems * 0.006).toFixed(4));
  }
  return Number((0.01 + maxItems * 0.005).toFixed(4));
}

export function validateApifyActorInput(actorId, input = {}) {
  const actor = getApifyActorById(actorId);
  if (!actor) {
    return { ok: false, error: "ACTOR_NOT_IN_REGISTRY", actorId };
  }
  if (!actor.enabled) {
    return { ok: false, error: "ACTOR_DISABLED", actorId };
  }
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return { ok: false, error: "INPUT_MUST_BE_OBJECT", actorId };
  }
  return { ok: true, actor, estimatedMaxUsd: estimateRunCostUsd(actor, Number(input.maxItems || input.maxEvents || input.maxResults || 25)) };
}

/**
 * Execute Apify Actor discovery (mock by default).
 */
export async function runApifyDiscovery({
  actorId,
  input = {},
  hotel = {},
  researchObjective = "",
  maxItems = 25,
  timeoutMs = 120000,
  authorizeApifyLive = false,
  forceMockOnly = true,
  campaignCeilingUsd = null,
  ledger = null,
  mockItems = null,
} = {}) {
  const validation = validateApifyActorInput(actorId, {
    ...input,
    maxItems: input.maxItems ?? maxItems,
  });
  if (!validation.ok) {
    return {
      ok: false,
      provider: "apify",
      error: validation.error,
      actorId,
      spentUsd: 0,
    };
  }

  const actor = validation.actor;
  const estimate = validation.estimatedMaxUsd;
  const spendLedger = ledger || createApifySpendLedger();

  if (
    campaignCeilingUsd != null &&
    Number(spendLedger.spentUsd) + estimate > Number(campaignCeilingUsd)
  ) {
    spendLedger.halted = true;
    spendLedger.haltReason = "APIFY_CAMPAIGN_CEILING_WOULD_EXCEED";
    return {
      ok: false,
      provider: "apify",
      error: "BUDGET_HARD_STOP",
      actorId,
      estimateUsd: estimate,
      spentUsd: spendLedger.spentUsd,
      ledger: spendLedger,
    };
  }

  const runId = `apify_run_${randomBytes(6).toString("hex")}`;
  const live =
    !forceMockOnly &&
    isApifyLiveAuthorized({ authorizeApifyLive });

  let items = [];
  let mode = "mock";
  let datasetId = null;
  let apifyRunId = null;

  if (live) {
    mode = "live";
    const token = String(process.env.APIFY_TOKEN || "").trim();
    const startRes = await fetch(
      `${APIFY_API}/acts/${encodeURIComponent(actorId)}/runs?waitForFinish=120`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          ...input,
          maxItems: input.maxItems ?? maxItems,
        }),
        signal: AbortSignal.timeout(timeoutMs),
      },
    );
    if (!startRes.ok) {
      const body = await startRes.text().catch(() => "");
      return {
        ok: false,
        provider: "apify",
        error: "APIFY_RUN_FAILED",
        status: startRes.status,
        body: String(body).slice(0, 500),
        actorId,
        spentUsd: 0,
      };
    }
    const runJson = await startRes.json();
    const data = runJson.data || runJson;
    apifyRunId = data.id || data.runId || null;
    datasetId = data.defaultDatasetId || null;
    if (datasetId) {
      const dsRes = await fetch(
        `${APIFY_API}/datasets/${encodeURIComponent(datasetId)}/items?format=json&limit=${maxItems}`,
        {
          headers: {
            Authorization: `Bearer ${token}`,
            Accept: "application/json",
          },
          signal: AbortSignal.timeout(timeoutMs),
        },
      );
      if (dsRes.ok) {
        const raw = await dsRes.json();
        items = Array.isArray(raw) ? raw : raw.items || [];
      }
    }
  } else {
    items = Array.isArray(mockItems)
      ? mockItems
      : buildMockItems({ actorId, hotel, maxItems: Math.min(maxItems, 5) });
  }

  const bounded = items.slice(0, maxItems);
  const normalized = normalizeApifyDatasetItems(bounded, {
    actorId,
    hotel,
    researchObjective,
    runId,
  });

  const charged = live ? estimate : 0;
  spendLedger.spentUsd = Number((Number(spendLedger.spentUsd) + charged).toFixed(4));
  spendLedger.entries.push({
    runId,
    apifyRunId,
    actorId,
    mode,
    costUsd: charged,
    estimateUsd: estimate,
    itemCount: bounded.length,
    researchObjective,
    recordedAt: new Date().toISOString(),
  });

  return {
    ok: true,
    provider: "apify",
    version: APIFY_DISCOVERY_PROVIDER_V1,
    registryVersion: APIFY_ACTOR_REGISTRY_V1,
    mode,
    actorId,
    runId,
    apifyRunId,
    datasetId,
    rawCount: bounded.length,
    candidates: normalized.candidates,
    rejected: normalized.rejected,
    spentUsd: charged,
    estimateUsd: estimate,
    ledger: spendLedger,
    note: live
      ? "Live Apify run — still requires ORS/Exa qualification before staging."
      : "Mock/zero-paid run — not a live discovery benchmark.",
  };
}

function buildMockItems({ actorId, hotel, maxItems }) {
  const city = hotel.city || "Boca Raton";
  const base = [];
  if (String(actorId).includes("eventbrite") || String(actorId).includes("meetup") || String(actorId).includes("facebook-events")) {
    base.push({
      title: `MOCK ${city} Professional Development Summit`,
      name: `MOCK ${city} Professional Development Summit`,
      startDate: "2027-09-14",
      endDate: "2027-09-15",
      venueName: `${city} Convention Annex`,
      organizerName: "MOCK South Florida Association Chapter",
      url: "https://example.com/mock-event-not-live",
      city,
    });
  } else if (String(actorId).includes("google-places") || String(actorId).includes("wedding") || String(actorId).includes("knot")) {
    base.push({
      title: `MOCK Waterfront Wedding Venue ${city}`,
      name: `MOCK Waterfront Wedding Venue ${city}`,
      category: "Wedding venue",
      website: "https://example.com/mock-venue",
      phone: null,
      address: `${city}, FL`,
      city,
      url: "https://example.com/mock-venue",
    });
  } else {
    base.push({
      title: `MOCK discovery record for ${actorId}`,
      url: "https://example.com/mock",
      city,
    });
  }
  return base.slice(0, maxItems);
}

export function listRunnableApifyActors() {
  return listEnabledApifyActors();
}
