import { GTM_TRACKS } from './constants.js';
import { routeIcp } from './icp-router.js';
import { normalizeMaturity } from './product-maturity.js';

/**
 * Dual-track GTM router. Never collapses ADP and owner dealmaking into one wedge.
 */
export function routeGtm(input = {}) {
  const icpResult = routeIcp(input);
  const initiatives = [];

  for (const route of icpResult.routes) {
    if (route.track === GTM_TRACKS.ADP || route.entry === 'ADP' || route.entry === 'ADP_BRAND_AI') {
      initiatives.push({
        gtmTrack: GTM_TRACKS.ADP,
        icp: route.icp,
        problem: input.problem || 'AI demand visibility / positioning',
        trigger: input.signals?.trigger || null,
        product: input.product || 'AI Demand Positioning',
        productMaturity: input.productMaturity ? normalizeMaturity(input.productMaturity) : 'PILOT_READY',
        commercialObjective: input.commercialObjective || 'adoption_validation_proof_usage_revenue',
        proofRequirement: input.proofRequirement || 'pilot instrumentation + action adoption',
        cta: input.cta || null,
        approvalPath: 'JOAN_APPROVAL_REQUIRED_FOR_EXTERNAL',
      });
    }
    if (route.track === GTM_TRACKS.OWNER_DEALMAKING || route.entry === 'OWNER_DECISION_WORKFLOW') {
      initiatives.push({
        gtmTrack: GTM_TRACKS.OWNER_DEALMAKING,
        icp: route.icp,
        problem: input.problem || 'live hotel dealmaking decision',
        trigger: input.signals?.liveDealmakingTrigger || input.signals?.trigger || true,
        product: input.product || 'Owner Decision Workflow',
        productMaturity: input.productMaturity ? normalizeMaturity(input.productMaturity) : 'PILOT_READY',
        commercialObjective: input.commercialObjective || 'support_live_decision',
        proofRequirement: input.proofRequirement || 'process clarity / anonymized outcome when available',
        cta: input.cta || null,
        approvalPath: 'JOAN_APPROVAL_REQUIRED_FOR_EXTERNAL',
        manufactureUrgencyForbidden: true,
      });
    }
    if (route.icp === 'ICP-A1') {
      initiatives.push({
        gtmTrack: null,
        icp: 'ICP-A1',
        problem: 'qualified owner introduction / opt-in',
        trigger: input.signals?.qualifiedOwnerRelationship || null,
        product: 'Amplifier / owner opt-in path',
        productMaturity: 'PILOT_READY',
        commercialObjective: 'owner_opt_in',
        proofRequirement: 'owner engagement',
        cta: input.cta || null,
        approvalPath: 'JOAN_APPROVAL_REQUIRED_FOR_EXTERNAL',
        primaryEconomicBuyer: false,
      });
    }
  }

  const tracks = [...new Set(initiatives.map((i) => i.gtmTrack).filter(Boolean))];
  return {
    collapseForbidden: true,
    activeTracksPreserved: tracks,
    dualTrackRequired: true,
    icpRouting: icpResult,
    initiatives,
  };
}
