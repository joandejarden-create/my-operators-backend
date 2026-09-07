/**
 * Weekly CMO Pack builder — internal only; never publishes.
 * Phase 6B: every material recommendation is validated against Operating Law v1.
 */
import {
  buildPrepareEnvelope,
  evaluateCompanyHeadline,
  gateCta,
  gateProductClaim,
  normalizeMaturity,
  normalizePricingClass,
  routeIcp,
  canHelenaSelfApprove,
  getOperatingLawMeta,
  classifyClaimText,
} from './operating-law/index.js';
import { classifyEpistemic, classifyConfidence } from './epistemic.js';

const MAX_JOAN_DECISIONS = 5;
const MAX_GROWTH = 3;

/**
 * Normalize a material recommendation and gate it against Operating Law v1.
 */
export function validateRecommendationAgainstLaw(item = {}) {
  const flags = [];
  const violations = [];

  let epistemic;
  let confidence;
  try {
    epistemic = classifyEpistemic(item.epistemic || item.evidenceClass || 'RECOMMENDATION');
  } catch (e) {
    epistemic = 'RECOMMENDATION';
    flags.push(`invalid_epistemic:${e.message}`);
  }
  try {
    confidence = classifyConfidence(item.confidence || 'MEDIUM');
  } catch {
    confidence = 'MEDIUM';
    flags.push('invalid_confidence_defaulted_MEDIUM');
  }

  const productMaturity = item.productMaturity
    ? normalizeMaturity(item.productMaturity)
    : item.product
      ? 'PILOT_READY'
      : null;

  if (productMaturity && item.claimAsAvailableForPurchase) {
    const mg = gateProductClaim({
      maturity: productMaturity,
      claimAsAvailableForPurchase: true,
      claimAsPilot: Boolean(item.claimAsPilot),
    });
    if (!mg.allowed) {
      violations.push({ gate: 'product_maturity', ...mg });
    }
  }

  let claimClass = item.claimClass || null;
  if (item.claimText || item.body || item.headline) {
    const c = classifyClaimText(item.claimText || item.body || item.headline);
    claimClass = c.claimClass;
    if (c.action === 'BLOCK' || c.action === 'BLOCK_OR_REWRITE') {
      violations.push({ gate: 'claim', ...c });
    }
  }

  let ctaGate = null;
  if (item.cta) {
    ctaGate = gateCta({
      cta: item.cta,
      productMaturity: productMaturity || 'PILOT_READY',
    });
    if (!ctaGate.allowed) violations.push({ gate: 'cta', ...ctaGate });
  }

  let pricingClass = null;
  if (item.pricingClass) {
    try {
      pricingClass = normalizePricingClass(item.pricingClass);
    } catch (e) {
      violations.push({ gate: 'pricing_class', reason: e.message });
    }
  }

  if (item.headline) {
    const h = evaluateCompanyHeadline(item.headline);
    if (h.blocked) violations.push({ gate: 'company_headline', ...h });
  }

  let icp = item.icp || null;
  let gtmTrack = item.gtmTrack || null;
  if (item.clientType && !icp) {
    const routed = routeIcp({ clientType: item.clientType, signals: item.signals || {} });
    icp = routed.routes[0]?.icp || null;
    gtmTrack = gtmTrack || routed.routes[0]?.track || null;
  }

  const prepare = buildPrepareEnvelope({
    icp: icp || item.icp || 'ICP-O2',
    gtmTrack: gtmTrack || item.gtmTrack || 'ADP',
    product: item.product || 'Dealality',
    productMaturity: productMaturity || 'PILOT_READY',
    claimClass: claimClass || 'GREEN',
    evidence: item.evidence || item.provenance || null,
    confidence,
    cta: item.cta || 'SEE HOW IT WORKS',
    pricingClass,
    provenance: item.provenance || item.source || null,
    body: item.body || item.summary || null,
    approvalStatus: 'DRAFT',
  });

  const status = violations.length ? 'REJECTED_OR_FLAGGED' : flags.length ? 'FLAGGED' : 'PASSED';

  return {
    status,
    lawVersion: getOperatingLawMeta().version,
    executeEnabled: false,
    helenaSelfApprove: canHelenaSelfApprove(),
    fields: {
      evidenceClass: epistemic,
      confidence,
      icp: icp || item.icp || null,
      gtmTrack: gtmTrack || item.gtmTrack || null,
      product: item.product || null,
      productMaturity: productMaturity || null,
      claimClass: claimClass || 'GREEN',
      pricingClass,
      cta: item.cta || prepare.envelope.cta,
      approvalRequired: true,
      founderLockAlignment: item.founderLockAlignment || item.locks || [],
      provenance: item.provenance || item.source || null,
    },
    flags,
    violations,
    prepare,
    original: {
      id: item.id || null,
      title: item.title || item.summary || null,
      type: item.type || 'recommendation',
    },
  };
}

export function buildWeeklyCmoPack({
  tables = {},
  observations = [],
  learnings = [],
  growthItems = [],
  preparedItems = [],
  approvalIds = [],
  techItems = [],
  sectionsOverride = null,
  synthetic = false,
  weekLabel = '2026-W36',
  executiveSummary = null,
  joanDecisions = [],
} = {}) {
  const content = tables['Content Library'] || [];
  const published = content.filter((r) => r.fields?.Status === 'PUBLISHED');
  const drafts = content.filter((r) => r.fields?.Status === 'DRAFT');
  const performance = tables.Performance || [];
  const lawMeta = getOperatingLawMeta();

  const gatedGrowth = (growthItems.length ? growthItems : growthItems)
    .slice(0, MAX_GROWTH)
    .map((g) => validateRecommendationAgainstLaw(typeof g === 'string' ? { id: g, title: g } : g));

  const gatedPrepared = preparedItems
    .map((p) => validateRecommendationAgainstLaw(p))
    .filter((p) => p.status !== 'REJECTED_OR_FLAGGED' || p.violations?.length);

  const rejected = [...gatedGrowth, ...gatedPrepared].filter((g) => g.status === 'REJECTED_OR_FLAGGED');
  const passedPrepared = gatedPrepared.filter((g) => g.status === 'PASSED' || g.status === 'FLAGGED');

  const decisions = (joanDecisions.length
    ? joanDecisions
    : [
        ...approvalIds.map((id) => ({ id, type: 'Approval Queue', action: 'Review PENDING item' })),
        ...gatedGrowth
          .filter((g) => g.status === 'PASSED')
          .slice(0, 2)
          .map((g) => ({
            id: g.original.id,
            type: 'Growth Opportunity',
            action: 'Prioritize or park',
          })),
      ]
  ).slice(0, MAX_JOAN_DECISIONS);

  const baseSections = {
    '1_executive_cmo_summary': {
      summary:
        executiveSummary ||
        (synthetic
          ? 'Synthetic cycle: Marketing OS present; EXECUTE disabled; Joan approval preserved.'
          : 'Manual assisted CMO cycle under Operating Law v1.'),
      contentInventory: {
        linkedInPublished: published.length,
        linkedInDraft: drafts.length,
        performanceRows: performance.length,
        products: (tables.Products || []).length,
      },
      law: { version: lawMeta.version, recurringHelenaEnabled: false, executeEnabled: false },
    },
    '2_what_changed': sectionsOverride?.whatChanged || [],
    '3_what_matters_now': sectionsOverride?.whatMattersNow || [],
    '4_owner_signals': sectionsOverride?.ownerSignals || [],
    '5_adp_signals': sectionsOverride?.adpSignals || [],
    '6_owner_dealmaking_signals': sectionsOverride?.ownerDealmakingSignals || [],
    '7_website_findings': sectionsOverride?.websiteFindings || [],
    '8_linkedin_content_learning': sectionsOverride?.linkedinLearning || {
      publishedCount: published.length,
      draftCount: drafts.length,
      performanceAttribution: 'UNKNOWN',
    },
    '9_seo_discovery_learning': sectionsOverride?.seoLearning || [],
    '10_product_marketing_gaps': sectionsOverride?.productMarketingGaps || [],
    '11_top_priorities': sectionsOverride?.topPriorities || [],
    '12_top_growth_opportunities': gatedGrowth,
    '13_what_to_stop': sectionsOverride?.whatToStop || [],
    '14_what_to_double_down': sectionsOverride?.whatToDoubleDown || [],
    '15_prepared_items': passedPrepared,
    '16_approval_requests': decisions.filter((d) => /approval/i.test(d.type || '')),
    '17_technical_work_items': techItems,
    '18_data_gaps': sectionsOverride?.dataGaps || [],
    '19_fyi': sectionsOverride?.fyi || observations.map((o) => o.id || o),
    law_gate_summary: {
      growthValidated: gatedGrowth.length,
      preparedValidated: gatedPrepared.length,
      rejectedOrFlagged: rejected.length,
      rejectedIds: rejected.map((r) => r.original?.id).filter(Boolean),
    },
  };

  return {
    schemaVersion: 'helena-cmo-weekly-pack-v1-law-gated',
    weekLabel,
    generatedAt: new Date().toISOString(),
    synthetic,
    executionEnabled: false,
    recurringHelenaEnabled: false,
    operatingLawVersion: lawMeta.version,
    sections: baseSections,
    joanDecisionCap: {
      max: MAX_JOAN_DECISIONS,
      proposed: decisions,
      count: decisions.length,
      note: 'Prefer ≤3; hard cap 5 for normal weekly packs',
    },
    observations,
    learnings,
  };
}
