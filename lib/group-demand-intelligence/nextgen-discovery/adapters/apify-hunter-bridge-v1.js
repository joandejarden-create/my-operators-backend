/**
 * Thin Apify bridge for specialist hunters — reuses Apify discovery provider V1.
 * Default forceMockOnly. Never live-runs from nextgen wave without founder auth.
 */

import {
  listRunnableApifyActors,
  validateApifyActorInput,
} from "../../research/apify/apify-discovery-provider-v1.js";
import { resolveHunterProviderPlan } from "../hunter-provider-routing-v1.js";

export const APIFY_HUNTER_BRIDGE_V1 = "gdi-nextgen-apify-hunter-bridge-v1";

/**
 * Plan Apify retrieval for a hunter without executing paid actors.
 */
export function planApifyHunterRetrieval({
  hunterId,
  budgetUsd = 0,
  authorizePaid = false,
  sampleInput = null,
} = {}) {
  const providerPlan = resolveHunterProviderPlan(hunterId, {
    dryRun: true,
    cacheFirst: true,
    allowPaidApify: false,
    allowPaidExa: false,
  });
  const apify = (providerPlan.providers || []).find((p) => p.category === "apify");
  const actors = apify?.actors || [];
  const runnable = listRunnableApifyActors();
  const validations = actors.slice(0, 3).map((a) => {
    const id = a.actorId || a.id || a;
    return {
      actorId: id,
      validation: validateApifyActorInput(id, sampleInput || {}),
      inRunnableRegistry: runnable.some(
        (r) => String(r.actorId || r.id) === String(id)
      ),
    };
  });

  return {
    schema: APIFY_HUNTER_BRIDGE_V1,
    provider: "apify",
    execute: false,
    forceMockOnly: true,
    paidAuthorized: false,
    authorizePaidRequested: authorizePaid === true,
    budgetUsd: Number(budgetUsd) || 0,
    providerPlanRole: apify?.role || "NONE",
    actorsPlanned: actors,
    validations,
    note:
      "Apify bridge validates actor wiring only. Live actor runs remain fail-closed.",
  };
}
