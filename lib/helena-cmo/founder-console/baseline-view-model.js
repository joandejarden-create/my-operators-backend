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
  const revisedHealth = validation?.revisedScores?.overallMarketingHealth || null;
  const strategyChallenge = validation?.strategyChallenge || null;
  const rec = strategyDecision?.recommended || null;
  const choice = strategyDecision?.strategicChoice || null;

  return {
    ok: true,
    consoleVersion: 'v2.6e',
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
      liveDataReconciliationPack: marketingIntelligence?.pack || null,
      baselineConfidence: coverage?.baselineConfidence || baseline.meta?.confidence || 'MEDIUM',
      dataCompletenessDecision: strategyDecision?.dataCompletenessDecision?.answer || null,
      assessmentFreshness: marketingIntelligence?.sources?.some((s) => s.status === 'LIVE')
        ? 'PARTIAL_LIVE'
        : 'STALE_HEAVY',
    },
    marketingIntelligence,
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
            label: baseline.executiveAssessment?.overallHealth?.label || 'REVISED AFTER 6D VALIDATION',
            priorScore: revisedHealth.prior,
            whyRevised: revisedHealth.why,
          }
        : baseline.executiveAssessment?.overallHealth,
      centralProblem:
        strategyDecision
          ? 'Dealality can build and explain products, but cannot yet run a closed commercial learning loop: missing named activation, conversion-grade proof, attribution, and a public ADP entry offer aligned with locked dual-track strategy — while live public identity correctly stays owner dealmaking.'
          : baseline.executiveAssessment?.centralProblem,
      centralProblemConfidence: strategyDecision ? 'HIGH_CONFIDENCE' : 'HIGH_CONFIDENCE',
      strategyChallengeDecision: strategyChallenge?.decision || null,
      revisedStrategyThesis:
        choice?.competeWhere || strategyChallenge?.revisedThesis || null,
      helenaRecommendationId: rec?.id || null,
      helenaRecommendationName: rec?.name || null,
      helenaRecommendationScore: rec?.score || null,
      strategicOptions: (strategyDecision?.alternatives || []).concat(
        rec
          ? [
              {
                id: rec.id,
                name: rec.name,
                score: rec.score,
                recommended: true,
              },
            ]
          : [],
      ),
      willNotPrioritize: choice?.willNotDo || [],
      uncertainties: [
        'Named warm ADP-relevant accounts (DATA_GAP — partially strategy-material)',
        'Live GA4/GSC conversion grain',
        'LinkedIn commercial ROI',
        'Paid pilot conversion at stated economics',
      ],
      dataCompletenessDecision: strategyDecision?.dataCompletenessDecision || null,
      strategyApprovalRequested: false,
      primaryCta,
      founderDiscussionPrompt:
        'Discuss Option B (ADP entry / dealmaking architecture). Do not lock until warm-access honesty and assumptions are accepted.',
    },
    overallMarketingHealth: revisedHealth
      ? {
          score: revisedHealth.revised,
          label: 'REVISED AFTER EVIDENCE VALIDATION',
          rationale: revisedHealth.why,
          priorScore: revisedHealth.prior,
        }
      : baseline.overallMarketingHealth,
    baselineSourceCoverage: baseline.sourceCoverage,
    scorecard: baseline.scorecard,
    domains: baseline.domains,
    findings: baseline.findings,
    historicalActivities: baseline.historicalActivities,
    diagnosis: baseline.diagnosis,
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
      decision: 'Discuss Helena recommended near-term GTM strategy (Option B) — amend or proceed toward later lock',
      helenaRecommendation: rec?.name || 'ADP entry / dealmaking architecture',
      status: strategyState,
      whyNow: 'Explicit strategic choice is ready for founder discussion; approval/lock is not requested yet',
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
