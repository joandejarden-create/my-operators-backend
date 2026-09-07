/**
 * Helena CMO Founder Console V2 — baseline/strategy view model.
 * Machine JSON is SoT; markdown reports are provenance.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { buildFounderBriefViewModel } from './brief-view-model.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '../../..');
const BASELINE = path.join(ROOT, 'reports/helena-cmo-baseline-v1/helena-cmo-baseline-v1.json');
const STRATEGY = path.join(ROOT, 'reports/helena-cmo-baseline-v1/helena-cmo-strategy-v1.json');
const VALIDATION = path.join(
  ROOT,
  'reports/helena-cmo-baseline-validation-v1/helena-cmo-baseline-validation-v1.json',
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
  const weekBrief = buildFounderBriefViewModel();

  if (!baseline || !strategy) {
    return {
      ok: false,
      error: 'BASELINE_PACK_MISSING',
      message: 'Helena CMO baseline/strategy JSON not found',
      attentionCount: 0,
    };
  }

  const override = strategyOverrideStatus();
  let strategyState = strategy.state || 'STRATEGY_PENDING_FOUNDER_REVIEW';
  if (override === 'LOCKED' || override === 'APPROVED') strategyState = 'LOCKED';
  if (override === 'AMENDED') strategyState = 'AMENDED';
  if (override === 'HOLD' || override === 'REJECTED') strategyState = 'STRATEGY_PENDING_FOUNDER_REVIEW';

  const pendingStrategyReview = strategyState === 'STRATEGY_PENDING_FOUNDER_REVIEW';

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

  return {
    ok: true,
    consoleVersion: 'v2',
    defaultTab: 'assessment',
    meta: {
      ...baseline.meta,
      strategyState,
      pendingStrategyReview,
      executeEnabled: false,
      recurringHelenaEnabled: false,
      marketingOsDecisionWriteConnected: false,
      strategyApprovalRequested: false,
      validationPack: validation ? 'helena-cmo-baseline-validation-v1' : null,
      baselineConfidence: coverage?.baselineConfidence || baseline.meta?.confidence || 'MEDIUM',
    },
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
      centralProblemConfidence: 'HIGH_CONFIDENCE',
      strategyChallengeDecision: strategyChallenge?.decision || null,
      revisedStrategyThesis: strategyChallenge?.revisedThesis || null,
      strategyApprovalRequested: false,
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
      decision: 'Review and accept (or amend) Helena CMO Baseline + Strategy V1',
      helenaRecommendation: 'Lock strategy before normalizing tactical PREPARE approvals',
      status: strategyState,
      whyNow: 'Founder must understand diagnosis before approving tactics',
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
