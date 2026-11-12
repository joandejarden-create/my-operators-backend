/**
 * Provider routing for specialist hunters — Apify / Exa / public APIs.
 * Reuses apify-actor-registry-v1 + fourteen-workflow coverage; does not hardcode
 * live actor IDs as required. Zero-paid default; live remain fail-closed.
 */

import {
  APIFY_ACTOR_REGISTRY,
  listApifyActorsForSegment,
} from "../research/apify/apify-actor-registry-v1.js";
import {
  APIFY_COVERAGE_CLASS,
  buildFourteenWorkflowApifyCoverageMatrix,
} from "../research/apify/apify-fourteen-workflow-coverage-v1.js";
import {
  SPECIALIST_HUNTER,
  SPECIALIST_HUNTER_META,
  getSpecialistHunterMeta,
} from "./specialist-hunter-contract-v1.js";

export const PROVIDER_CATEGORY = Object.freeze({
  PUBLIC_API: "public_api",
  APIFY: "apify",
  EXA: "exa",
});

/** Historical V7 lessons retained for routing decisions. */
export const APIFY_V7_LESSONS_RETAINED = Object.freeze([
  "Party Bus / social leisure organizers fail commercial-relevance gates — keep organizer + lodging thesis gates",
  "Apify discoveries are research evidence, not Sales Ready opportunities",
  "Sports/entertainment schedules are PRIMARY_APIFY; government/scaling stay EXA_PRIMARY",
  "Dedup against Exa event discovery is HIGH for association/meetup surfaces",
  "Actor registry must allow replacement without changing opportunity schema",
  "Full E2E production rail remains Exa-primary until hotel-adaptive yield proven",
]);

const HUNTER_TO_WORKFLOW_STRATEGY = Object.freeze({
  [SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT]: "government_public_sector",
  [SPECIALIST_HUNTER.ASSOCIATION_ROTATION]: "association_events",
  [SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS]: "meetings_incentives",
  [SPECIALIST_HUNTER.CLINICAL_TRIALS_MEDICAL]: "education_scientific",
  [SPECIALIST_HUNTER.PROJECT_WORKFORCE]: "projects_infrastructure",
  [SPECIALIST_HUNTER.SPORTS_HOUSING]: "sports_entertainment",
  [SPECIALIST_HUNTER.UNIVERSITY_DEMAND]: "education_scientific",
  [SPECIALIST_HUNTER.LEGAL_REGULATORY]: "association_events",
  [SPECIALIST_HUNTER.EMERGENCY_DISPLACEMENT]: "relocation_extended_stay",
  [SPECIALIST_HUNTER.CREW_PRODUCTION]: "sports_entertainment",
});

/**
 * Resolve provider plan for a hunter. Never enables paid live calls.
 */
export function resolveHunterProviderPlan(hunterId, options = {}) {
  const meta = getSpecialistHunterMeta(hunterId);
  if (!meta) {
    return {
      ok: false,
      error: `unknown hunter: ${hunterId}`,
      providers: [],
    };
  }

  const allowPaidApify = options.allowPaidApify === true;
  const allowPaidExa = options.allowPaidExa === true;
  const dryRun = options.dryRun !== false;
  const cacheFirst = options.cacheFirst !== false;

  const workflowStrategy =
    HUNTER_TO_WORKFLOW_STRATEGY[hunterId] || meta.workflowIds[0];
  const coverageMatrix = buildFourteenWorkflowApifyCoverageMatrix();
  const coverageRow =
    (coverageMatrix?.workflows || []).find(
      (r) =>
        r.strategyId === workflowStrategy ||
        String(r.strategyId || "").endsWith(workflowStrategy) ||
        r.segmentId === workflowStrategy
    ) || null;

  // Apify registry uses fourteen-workflow segment ids (e.g. sports_entertainment)
  const apifySegmentIds = [
    coverageRow?.segmentId,
    ...meta.workflowIds,
    ...(meta.researchSegments || []),
  ].filter(Boolean);

  const registryActors = [];
  for (const seg of [...new Set(apifySegmentIds)]) {
    const listed = listApifyActorsForSegment(seg) || [];
    for (const a of listed) {
      if (!registryActors.find((x) => x.actorId === a.actorId)) {
        registryActors.push(a);
      }
    }
  }

  for (const actor of APIFY_ACTOR_REGISTRY || []) {
    const segs = actor.demandSegments || [];
    const hit = meta.workflowIds.some((w) => segs.includes(w));
    if (hit && !registryActors.find((x) => x.actorId === actor.actorId)) {
      registryActors.push(actor);
    }
  }

  const providers = [];

  if ((meta.primaryProviders || []).includes(PROVIDER_CATEGORY.PUBLIC_API)) {
    providers.push({
      category: PROVIDER_CATEGORY.PUBLIC_API,
      mode: dryRun || cacheFirst ? "CACHE_OR_FIXTURE" : "LIVE_PUBLIC",
      paid: false,
      rationale: "Official/public structured sources preferred when available",
    });
  }

  const apifyRole = meta.apifyRole || "NONE";
  if (apifyRole !== "NONE") {
    const classification =
      coverageRow?.classification ||
      (apifyRole === "PRIMARY"
        ? APIFY_COVERAGE_CLASS.PRIMARY_APIFY_DISCOVERY
        : APIFY_COVERAGE_CLASS.SUPPLEMENTAL_APIFY_DISCOVERY);
    providers.push({
      category: PROVIDER_CATEGORY.APIFY,
      mode:
        dryRun || !allowPaidApify
          ? "CACHE_REGISTRY_ONLY"
          : "LIVE_AUTHORIZED",
      paid: Boolean(allowPaidApify && !dryRun),
      role: apifyRole,
      classification,
      actors: registryActors.map((a) => ({
        id: a.actorId,
        actorId: a.actorId,
        title: a.title || null,
        version: a.version || null,
        sourceType: (a.discoveryFamilies || [])[0] || null,
        countries: a.countries || ["US"],
        historicalQuality: a.rating ?? a.reliabilityNote ?? null,
        perRunCostHint: a.pricingSummary || null,
        enabled: a.enabled === true,
        replaceable: true,
      })),
      liveGate: "GDI_APIFY_LIVE",
      rationale:
        "Reuse existing Apify actor registry — replace actor without schema change",
    });
  }

  if ((meta.primaryProviders || []).includes(PROVIDER_CATEGORY.EXA)) {
    providers.push({
      category: PROVIDER_CATEGORY.EXA,
      mode: dryRun || !allowPaidExa ? "CACHE_FIRST" : "LIVE_AUTHORIZED",
      paid: Boolean(allowPaidExa && !dryRun),
      liveGate: "GDI_EXA_AGENT_LIVE",
      rationale: "Exa for verification / buyer research when authorized",
    });
  }

  return {
    ok: true,
    hunterId,
    label: meta.label,
    workflowStrategy,
    workflowIds: meta.workflowIds,
    researchSegments: meta.researchSegments,
    tenBases: meta.tenBases,
    dryRun,
    cacheFirst,
    allowPaidApify: false, // hard default; only true when options + gates
    allowPaidExa: false,
    paidAuthorized: false,
    providers,
    coverageRow: coverageRow
      ? {
          classification: coverageRow.classification,
          priority: coverageRow.priority,
          dupRisk: coverageRow.dupRisk,
        }
      : null,
    v7Lessons: APIFY_V7_LESSONS_RETAINED,
  };
}

export function buildHunterProviderRoutingMatrix() {
  const hunters = Object.values(SPECIALIST_HUNTER);
  return {
    schema: "gdi-hunter-provider-routing-matrix-v1",
    generatedAt: new Date().toISOString(),
    paidDefault: false,
    rows: hunters.map((id) => {
      const plan = resolveHunterProviderPlan(id, { dryRun: true });
      const meta = SPECIALIST_HUNTER_META[id];
      return {
        hunterId: id,
        label: meta.label,
        wave: meta.wave,
        capability: meta.capability,
        apifyRole: meta.apifyRole,
        providerCategories: (plan.providers || []).map((p) => p.category),
        actorCount: plan.providers?.find((p) => p.category === "apify")?.actors
          ?.length || 0,
        workflowIds: meta.workflowIds,
      };
    }),
    apifyRegistrySize: (APIFY_ACTOR_REGISTRY || []).length,
    lessonsRetained: APIFY_V7_LESSONS_RETAINED,
  };
}
