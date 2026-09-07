/**
 * Helena CMO Founder Console V2 — baseline/strategy view model.
 * Machine JSON is SoT; markdown reports are provenance.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { buildFounderBriefViewModel } from './brief-view-model.js';
import { getHelenaMarketingIntelligenceStatus } from '../analytics/cmo-analytics-reader.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '../../..');
const BASELINE = path.join(ROOT, 'reports/helena-cmo-baseline-v1/helena-cmo-baseline-v1.json');
const STRATEGY = path.join(ROOT, 'reports/helena-cmo-baseline-v1/helena-cmo-strategy-v1.json');
const VALIDATION = path.join(
  ROOT,
  'reports/helena-cmo-baseline-validation-v1/helena-cmo-baseline-validation-v1.json',
);
const STRATEGY_DECISION = path.join(
  ROOT,
  'reports/helena-cmo-strategy-decision-v1/helena-cmo-strategy-decision-v1.json',
);
const DEEP_V2 = path.join(ROOT, 'reports/helena-cmo-deep-baseline-v2/helena-cmo-deep-baseline-v2.json');
const ACTIONS_PATH = path.join(ROOT, 'data/helena-cmo/founder-console-actions.json');

function readJson(file, fallback = null) {
  try {
    if (!fs.existsSync(file)) return fallback;
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
}

function strategyOverrideStatus() {
  const actions = readJson(ACTIONS_PATH, {});
  const row = (actions.decisions || []).find((d) => d.decisionId === 'STRATEGY-V1');
  return row?.status || null;
}

export function buildFounderConsoleV2ViewModel() {
  const baseline = readJson(BASELINE);
  const strategy = readJson(STRATEGY);
  const validation = readJson(VALIDATION);
  const strategyDecision = readJson(STRATEGY_DECISION);
  const deepV2 = readJson(DEEP_V2);
  const weekBrief = buildFounderBriefViewModel();
  const marketingIntelligence = getHelenaMarketingIntelligenceStatus();

  if (!baseline || !strategy) {
    return {
      ok: false,
      error: 'BASELINE_PACK_MISSING',
      message: 'Helena CMO baseline/strategy JSON not found',
      attentionCount: 0,
    };
  }

  const override = strategyOverrideStatus();
  let strategyState =
    strategyDecision?.meta?.strategyState ||
    strategy.state ||
    'STRATEGY_PENDING_FOUNDER_REVIEW';
  if (override === 'LOCKED' || override === 'APPROVED') strategyState = 'LOCKED';
  if (override === 'AMENDED') strategyState = 'AMENDED';
  if (override === 'HOLD' || override === 'REJECTED') {
    strategyState = strategyDecision
      ? 'STRATEGY_PENDING_FOUNDER_DISCUSSION'
      : 'STRATEGY_PENDING_FOUNDER_REVIEW';
  }

  const pendingStrategyReview =
    strategyState === 'STRATEGY_PENDING_FOUNDER_REVIEW' ||
    strategyState === 'STRATEGY_PENDING_FOUNDER_DISCUSSION';
  const primaryCta = strategyDecision?.meta?.primaryCta || 'DISCUSS_STRATEGY';
  const strategyApprovalRequested = false;

  const tacticalActions = (strategy.proposedTacticalActions || []).map((a) => ({
    ...a,
    status: pendingStrategyReview ? 'PROPOSED_PENDING_STRATEGY_REVIEW' : a.status,
  }));

  const weekDecisions = (weekBrief.joanNeedsToDecide || []).map((d) => ({
    ...d,
    displayStatus: pendingStrategyReview
      ? 'PROPOSED — PENDING STRATEGY REVIEW'
      : d.status || 'PENDING',
    strategicParent: (tacticalActions.find((t) => t.id === d.id) || {}).strategicPillarId || null,
    priorityParent: (tacticalActions.find((t) => t.id === d.id) || {}).priorityId || null,
    orphaned: false,
  }));

  const weekApprovals = (weekBrief.approvals || []).map((a) => ({
    ...a,
    displayStatus: pendingStrategyReview
      ? 'PROPOSED — PENDING STRATEGY REVIEW'
      : a.status || 'PENDING',
  }));

  // Attention: strategy review first; tactical counts only after strategy accepted
  let attentionCount = 0;
  const attentionItems = [];
  if (pendingStrategyReview) {
    attentionCount = 1;
    attentionItems.push('STRATEGY-V1');
  } else {
    attentionCount = weekBrief.attentionCount || 0;
    attentionItems.push(...(weekBrief.joanNeedsToDecide || []).filter((d) => d.status === 'PENDING').map((d) => d.id));
  }

  const coverage = validation?.sourceCompleteness || null;
  const revisedHealth = deepV2?.scores?.overall
    ? {
        revised: deepV2.scores.overall.score,
        prior: validation?.revisedScores?.overallMarketingHealth?.revised ?? 4.8,
        why: deepV2.centralDiagnosis,
      }
    : validation?.revisedScores?.overallMarketingHealth || null;
  const strategyChallenge = validation?.strategyChallenge || null;
  const rec = deepV2?.recommendedStrategy || strategyDecision?.recommended || null;
  const choice = strategyDecision?.strategicChoice || null;
  const analysisByDomain = deepV2?.scores || null;
  const ga4Live = marketingIntelligence?.sources?.some((s) => s.id === 'GA4' && s.status === 'LIVE');
  const gscLive = marketingIntelligence?.sources?.some((s) => s.id === 'GSC' && s.status === 'LIVE');

  return {
    ok: true,
    consoleVersion: 'v3.6fb',
    defaultTab: 'assessment',
    meta: {
      ...baseline.meta,
      strategyState,
      pendingStrategyReview,
      executeEnabled: false,
      recurringHelenaEnabled: false,
      marketingOsDecisionWriteConnected: false,
      strategyApprovalRequested,
      primaryCta,
      validationPack: validation ? 'helena-cmo-baseline-validation-v1' : null,
      strategyDecisionPack: strategyDecision ? 'helena-cmo-strategy-decision-v1' : null,
      deepBaselinePack: deepV2 ? 'helena-cmo-deep-baseline-v2' : null,
      liveDataReconciliationPack: marketingIntelligence?.pack || null,
      baselineConfidence: deepV2 ? 'HIGH_CHANNELS_MEDIUM_COMMERCIAL' : coverage?.baselineConfidence || baseline.meta?.confidence || 'MEDIUM',
      dataCompletenessDecision: strategyDecision?.dataCompletenessDecision?.answer || null,
      assessmentFreshness: ga4Live && gscLive ? 'LIVE_CHANNELS' : marketingIntelligence?.sources?.some((s) => s.status === 'LIVE')
        ? 'PARTIAL_LIVE'
        : 'STALE_HEAVY',
    },
    marketingIntelligence,
    deepBaseline: deepV2
      ? {
          generatedAt: deepV2.generatedAt,
          overallScore: deepV2.scores?.overall?.score,
          recommendationId: deepV2.recommendedStrategy?.id,
          recommendationName: deepV2.recommendedStrategy?.name,
          recommendationScore: deepV2.recommendedStrategy?.score,
          centralDiagnosis: deepV2.centralDiagnosis,
          liveHeadline: deepV2.liveHeadline,
          sourceStatus: deepV2.sourceStatus,
          prioritiesNow: deepV2.prioritiesNow,
          deprioritized: deepV2.deprioritized,
          strategicOptions: deepV2.strategicOptions,
          founderDiscussion: deepV2.founderDiscussion,
          founderReviewPath: deepV2.meta?.founderReviewPath,
          analysisByDomain,
        }
      : null,
    sourceCoverage: coverage
      ? {
          reviewed: coverage.totalReviewed,
          relevant: coverage.totalRelevant,
          coveragePercent: coverage.coveragePercent,
          fresh: coverage.freshCurrentApprox,
          stale: coverage.staleOrPointInTimeApprox,
          missing: coverage.totalMissingOrUnavailable,
          confidence: coverage.baselineConfidence,
          honesty: coverage.honesty,
          sources: coverage.sources || [],
        }
      : null,
    validation: validation
      ? {
          generatedAt: validation.generatedAt,
          strategyChallenge,
          revisedScores: validation.revisedScores,
          liveRefresh: validation.liveRefresh,
        }
      : null,
    strategyDecision: strategyDecision
      ? {
          generatedAt: strategyDecision.generatedAt,
          dataCompletenessDecision: strategyDecision.dataCompletenessDecision,
          blockingGaps: strategyDecision.blockingGaps,
          connectors: strategyDecision.connectors,
          recommended: strategyDecision.recommended,
          alternatives: strategyDecision.alternatives,
          strategicChoice: strategyDecision.strategicChoice,
          positioning: strategyDecision.positioning,
          channelRoles: strategyDecision.channelRoles,
          proofSequence: strategyDecision.proofSequence,
          scorecard90d: strategyDecision.scorecard90d,
          assumptions: strategyDecision.assumptions,
          website: strategyDecision.website,
        }
      : null,
    safety: {
      executeEnabled: false,
      recurringHelenaEnabled: false,
      marketingOsDecisionWriteConnected: false,
    },
    workflow: baseline.workflow,
    executiveAssessment: {
      ...baseline.executiveAssessment,
      overallHealth: revisedHealth
        ? {
            score: revisedHealth.revised,
            label: deepV2 ? 'DEEP BASELINE V2 (LIVE EVIDENCE)' : 'REVISED AFTER 6D VALIDATION',
            priorScore: revisedHealth.prior,
            whyRevised: revisedHealth.why,
          }
        : baseline.executiveAssessment?.overallHealth,
      centralProblem:
        deepV2?.centralDiagnosis ||
        (strategyDecision
          ? 'Dealality can build and explain products, but cannot yet run a closed commercial learning loop.'
          : baseline.executiveAssessment?.centralProblem),
      centralProblemConfidence: 'HIGH_CONFIDENCE',
      strategyChallengeDecision: strategyChallenge?.decision || null,
      revisedStrategyThesis:
        deepV2?.recommendedStrategy?.name ||
        choice?.competeWhere ||
        strategyChallenge?.revisedThesis ||
        null,
      helenaRecommendationId: rec?.id || null,
      helenaRecommendationName: rec?.name || null,
      helenaRecommendationScore: rec?.score || null,
      strategicOptions: (deepV2?.strategicOptions || strategyDecision?.alternatives || []).map((o) => ({
        id: o.id,
        name: o.name,
        score: o.score,
        recommended: !!(o.recommended || (rec && o.id === rec.id)),
      })),
      willNotPrioritize: (deepV2?.deprioritized || [])
        .map((d) => d.item)
        .concat(choice?.willNotDo || [])
        .slice(0, 12),
      uncertainties: deepV2
        ? [
            'ADP-fit within Connected list (test via activation)',
            'Willingness to pay / pilot price band',
            'Post-form_start path',
            'LinkedIn impressions still PERMISSION_LIMITED',
            'Commercial outcomes UNKNOWN until events + join',
          ]
        : [
            'Named warm ADP-relevant accounts',
            'Live GA4/GSC conversion grain',
            'LinkedIn commercial ROI',
            'Paid pilot conversion at stated economics',
          ],
      dataCompletenessDecision: strategyDecision?.dataCompletenessDecision || null,
      strategyApprovalRequested: false,
      primaryCta,
      founderDiscussionPrompt: deepV2
        ? 'Discuss B2 (warm ADP entry + measured dealmaking + instrumentation + SEO hygiene). Do not lock until price band and activation commitment are honest.'
        : 'Discuss Option B. Do not lock until warm-access honesty and assumptions are accepted.',
      deepReviewCta: 'VIEW_DEEP_CMO_REVIEW',
    },
    overallMarketingHealth: revisedHealth
      ? {
          score: revisedHealth.revised,
          label: deepV2 ? 'DEEP BASELINE V2 (LIVE EVIDENCE)' : 'REVISED AFTER EVIDENCE VALIDATION',
          rationale: revisedHealth.why,
          priorScore: revisedHealth.prior,
        }
      : baseline.overallMarketingHealth,
    baselineSourceCoverage: baseline.sourceCoverage,
    scorecard: deepV2?.scores
      ? Object.fromEntries(
          Object.entries(deepV2.scores).map(([k, v]) => [
            k,
            {
              score: v.score,
              currentState: v.verdict,
              why: v.evidence,
              biggestGap: v.bad,
              opportunity: v.opportunity,
              analysisKey: k,
            },
          ]),
        )
      : baseline.scorecard,
    domains: baseline.domains,
    findings: baseline.findings,
    historicalActivities: baseline.historicalActivities,
    diagnosis: deepV2
      ? { central: deepV2.centralDiagnosis, pack: 'helena-cmo-deep-baseline-v2' }
      : baseline.diagnosis,
    strategy: {
      ...strategy,
      state: strategyState,
      pendingStrategyReview,
      recommendation: rec,
      choice,
    },
    roadmap: strategy.roadmap,
    proposedTacticalActions: tacticalActions,
    weekBrief: weekBrief.ok ? weekBrief : null,
    joanNeedsToDecide: weekDecisions,
    approvals: weekApprovals,
    approvalsWaiting: weekApprovals,
    attentionCount,
    attentionItems,
    founderStrategyDecision: {
      id: 'STRATEGY-V1',
      decision: deepV2
        ? 'Discuss Helena Deep Baseline V2 strategy (B2) — amend or proceed toward later lock'
        : 'Discuss Helena recommended near-term GTM strategy (Option B) — amend or proceed toward later lock',
      helenaRecommendation: rec?.name || 'ADP entry / dealmaking architecture',
      status: strategyState,
      whyNow: deepV2
        ? 'Live GA4/GSC evidence enabled Deep Baseline V2; discussion before any lock'
        : 'Explicit strategic choice is ready for founder discussion; approval/lock is not requested yet',
      primaryCta: 'DISCUSS_STRATEGY',
    },
  };
}

export function getHelenaConsoleAttentionCount() {
  const vm = buildFounderConsoleV2ViewModel();
  return {
    ok: true,
    count: vm.attentionCount || 0,
    items: vm.attentionItems || [],
    strategyState: vm.meta?.strategyState,
  };
}
