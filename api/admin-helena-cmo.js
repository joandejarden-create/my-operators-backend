/**
 * Admin Helena CMO Founder Console API (V2)
 * GET  /api/admin/helena-cmo/brief          (legacy week brief — still available)
 * GET  /api/admin/helena-cmo/console        (V2 primary payload)
 * GET  /api/admin/helena-cmo/attention-count
 * POST /api/admin/helena-cmo/decisions/:id/action
 * POST /api/admin/helena-cmo/approvals/:id/action
 *
 * Persistence: local action log only. Marketing OS sync NOT connected.
 * EXECUTE / publish / recurring Helena remain OFF.
 */
import {
  buildFounderBriefViewModel,
  getHelenaAttentionCount as getWeekAttentionCount,
  recordFounderConsoleDecision,
  recordFounderConsoleApproval,
} from '../lib/helena-cmo/founder-console/brief-view-model.js';
import {
  buildFounderConsoleV2ViewModel,
  getHelenaConsoleAttentionCount,
} from '../lib/helena-cmo/founder-console/baseline-view-model.js';

export function getHelenaCmoBrief(_req, res) {
  const brief = buildFounderBriefViewModel();
  if (!brief.ok) {
    return res.status(404).json(brief);
  }
  return res.status(200).json({ ok: true, success: true, brief });
}

export function getHelenaCmoConsole(_req, res) {
  const consoleVm = buildFounderConsoleV2ViewModel();
  if (!consoleVm.ok) {
    return res.status(404).json(consoleVm);
  }
  return res.status(200).json({ ok: true, success: true, console: consoleVm });
}

export function getHelenaCmoAttentionCount(_req, res) {
  return res.status(200).json({
    ok: true,
    success: true,
    ...getHelenaConsoleAttentionCount(),
    weekAttention: getWeekAttentionCount(),
  });
}

export function postHelenaCmoDecisionAction(req, res) {
  const decisionId = String(req.params.id || '').trim();
  const action = req.body?.action;
  const note = req.body?.note || '';
  if (!decisionId) {
    return res.status(400).json({ ok: false, error: 'MISSING_DECISION_ID' });
  }
  const result = recordFounderConsoleDecision({ decisionId, action, note, actor: 'Joan' });
  if (!result.ok) {
    return res.status(400).json(result);
  }
  return res.status(200).json({
    ok: true,
    success: true,
    ...result,
    brief: buildFounderBriefViewModel(),
    console: buildFounderConsoleV2ViewModel(),
  });
}

export function postHelenaCmoApprovalAction(req, res) {
  const approvalId = String(req.params.id || '').trim();
  const action = req.body?.action;
  const note = req.body?.note || '';
  if (!approvalId) {
    return res.status(400).json({ ok: false, error: 'MISSING_APPROVAL_ID' });
  }
  const consoleVm = buildFounderConsoleV2ViewModel();
  if (consoleVm.meta?.pendingStrategyReview && action === 'APPROVE') {
    return res.status(400).json({
      ok: false,
      error: 'STRATEGY_REVIEW_REQUIRED',
      message:
        'Strategy is STRATEGY_PENDING_FOUNDER_REVIEW. Review strategy before approving tactical PREPARE items.',
      console: consoleVm,
    });
  }
  const result = recordFounderConsoleApproval({ approvalId, action, note, actor: 'Joan' });
  if (!result.ok) {
    return res.status(400).json(result);
  }
  return res.status(200).json({
    ok: true,
    success: true,
    ...result,
    reminder: 'APPROVED means PREPARE-only. Does NOT publish, send, deploy, or merge.',
    brief: buildFounderBriefViewModel(),
    console: buildFounderConsoleV2ViewModel(),
  });
}
