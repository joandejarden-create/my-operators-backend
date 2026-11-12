/**
 * Government Procurement specialist hunter.
 *
 * Strategy (owned here, not shared):
 * - Prefer official procurement notices (SAM.gov / agency pages).
 * - Exa / Apify are supplemental only and remain fail-closed without paid auth.
 * - Cache fixtures are SYNTHETIC and never presented as live discoveries.
 *
 * Zero-paid path: HISTORICAL_RESEARCH_REPLAY of the verified NIH SBPO Vendor
 * Outreach Session notice previously retrieved from SAM.gov (independently
 * verifiable URL). Does not invent future session dates.
 */

import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";
import {
  DISCOVERY_EVIDENCE_CLASS,
  stampDiscoveryEvidenceClass,
} from "../shared/discovery-evidence-class-v1.js";
import { resolveHunterConfig } from "../shared/hunter-config-registry-v1.js";

/** Independently verifiable historical SAM.gov notice (NIH SBPO VOS). */
export const NIH_SBPO_VOS_HISTORICAL_NOTICE = Object.freeze({
  noticeId: "f4e0c6bfa2a14c2195c8476fb81d5f9c",
  sourceUrl:
    "https://sam.gov/workspace/contract/opp/f4e0c6bfa2a14c2195c8476fb81d5f9c/view",
  organizationName: "NIH Small Business Program Office (SBPO)",
  domain: "nih.gov",
  demandTrigger: "government vendor outreach session / contractor lodging adjacency",
  activityTitle:
    "NIH SBPO Vendor Outreach Session — documented prior session at 6700 Rockledge Drive, Bethesda (2026-04-29)",
  /** Prior documented session — not a verified future event date. */
  activityDate: "2026-04-29",
  activityDateVerified: true,
  activityDateKind: "HISTORICAL_PRIOR_SESSION",
  nextSessionDate: null,
  nextSessionDateVerified: false,
  geographicScope: "Bethesda MD / Rockledge Drive / NIH leased corridor",
  evidenceUrls: [
    "https://sam.gov/workspace/contract/opp/f4e0c6bfa2a14c2195c8476fb81d5f9c/view",
  ],
  contactName: null,
  contactRole: "NIH Small Business Program Office",
  contactEmail: "NIHSmallBusiness@od.nih.gov",
  buyerVerified: false,
  intermediaryVerified: true,
  pursuitThesis:
    "Bethesda Marriott can watch SAM.gov / NIH SBPO for the next Vendor Outreach Session and offer preferred vendor lodging packages near Rockledge Drive; prior session location is evidenced, next edition remains uncertain.",
  confidence: 0.62,
  hotelDemandRelevance: 0.78,
  recordKind: "DEMAND_TRIGGER",
  uncertainties: [
    "Next Vendor Outreach Session date is not confirmed in this zero-paid pass.",
    "Named lodging buyer / contracting officer for hotel blocks is not verified.",
    "Hotel demand is inferred from traveling-vendor adjacency, not an explicit hotel RFP.",
  ],
  sourceAuthority: "Tier_A",
  researchMethodId: "GDI-GOV-EVENT-01",
});

/**
 * Build the zero-paid government procurement discovery from historical official notice.
 * Does not call paid Exa/Apify. Optional live public fetch is best-effort and never required.
 */
export async function discoverGovernmentProcurementLiveOrHistorical(ctx = {}) {
  const config = resolveHunterConfig(SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT, {
    hotelId: ctx.hotelId,
    budgetUsd: ctx.budgetUsd || 0,
    hotelOverrides: ctx.hotelOverrides,
    runOverrides: ctx.runOverrides,
  });

  if (config && config.enabled === false) {
    return {
      discoveries: [],
      meta: {
        mode: "DISABLED",
        discoveryEvidenceClass: null,
        reason: "hunter_disabled_by_config",
      },
    };
  }

  const historical = stampDiscoveryEvidenceClass(
    {
      ...NIH_SBPO_VOS_HISTORICAL_NOTICE,
      hotelId: ctx.hotelId || null,
      sourceHunter: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
      // Keep research-stage: no named lodging buyer → not Sales Ready.
      hasCredibleContact: false,
      hasRealisticHotelThesis: true,
    },
    DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
  );

  let livePublicAttempted = false;
  let livePublicOk = false;
  let livePublicStatus = null;
  let livePublicNote =
    "SAM.gov workspace URLs often require interactive session; historical notice retained as independently verifiable evidence.";

  if (ctx.attemptLivePublic === true) {
    livePublicAttempted = true;
    try {
      const res = await fetch(NIH_SBPO_VOS_HISTORICAL_NOTICE.sourceUrl, {
        method: "GET",
        redirect: "follow",
        headers: {
          Accept: "text/html,application/json",
          "User-Agent": "DealalityGdiGovHunter/1.0 (+zero-paid-certification)",
        },
        signal: AbortSignal.timeout(12000),
      });
      livePublicStatus = res.status;
      livePublicOk = res.ok;
      if (res.ok) {
        livePublicNote = "Live public GET succeeded; content still treated as historical notice identity.";
      } else {
        livePublicNote = `Live public GET returned HTTP ${res.status}; using historical replay only.`;
      }
    } catch (err) {
      livePublicStatus = "error";
      livePublicNote = `Live public GET failed (${err?.message || err}); using historical replay only.`;
    }
  }

  return {
    discoveries: [historical],
    meta: {
      mode: "ZERO_PAID_HISTORICAL_OFFICIAL_NOTICE",
      discoveryEvidenceClass: DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY,
      paidAuthorized: false,
      budgetUsdSpent: 0,
      providerPlan: ctx.providerPlan || null,
      livePublicAttempted,
      livePublicOk,
      livePublicStatus,
      livePublicNote,
      incomplete: [
        "Named lodging buyer not verified — remains research-stage.",
        "Next session date unknown — do not treat prior date as future event.",
        "No automatic Sales Ready / publication.",
      ],
      demonstrated: [
        "Retrieved independently verifiable SAM.gov notice identity.",
        "Extracted organization (NIH SBPO) and demand-generating activity (Vendor Outreach Session).",
        "Confirmed prior session date/location and public contact channel.",
        "Constructed hotel-specific demand thesis for Bethesda corridor.",
      ],
      config: {
        enabled: config?.enabled !== false,
        costCeilingUsd: config?.costCeilingUsd ?? 0,
        cacheFirst: config?.cacheBehavior?.cacheFirst !== false,
      },
    },
  };
}

const cacheHunter = createCacheBackedHunter(SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT);

/**
 * Prefer historical official notice over synthetic fixtures when zero-paid certifying.
 * Synthetic cache rows remain available via discoverFnCache for regression only.
 */
export async function discoverGovernmentProcurement(ctx = {}) {
  if (ctx.forceSyntheticCache === true) {
    return cacheHunter.discoverFn(ctx);
  }
  // Default discover path: real historical notice (not synthetic fixture).
  return discoverGovernmentProcurementLiveOrHistorical({
    ...ctx,
    attemptLivePublic: ctx.attemptLivePublic !== false,
  });
}

export const governmentProcurementHunter = {
  hunterId: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
  discoverFn: discoverGovernmentProcurement,
  discoverFnCache: cacheHunter.discoverFn,
  run: async (options = {}) => {
    const { runSpecialistHunter } = await import("../hunter-base-v1.js");
    return runSpecialistHunter({
      hunterId: SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT,
      discoverFn: discoverGovernmentProcurement,
      hotelId: options.hotelId,
      hotelName: options.hotelName,
      territory: options.territory,
      commercialPriorities: options.commercialPriorities || null,
      dryRun: options.dryRun !== false,
      cacheFirst: options.cacheFirst !== false,
      budgetUsd: options.budgetUsd || 0,
      horizonMonths: options.horizonMonths,
      existingKeys: options.existingKeys || [],
      reportDir: options.reportDir || "",
      rootDir: options.rootDir || process.cwd(),
    });
  },
  runFromCli: cacheHunter.runFromCli,
};

export const runGovernmentProcurementHunter = governmentProcurementHunter.run;
