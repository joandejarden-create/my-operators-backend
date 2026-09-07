/**
 * Admin Helena CMO Founder Console API
 * GET  /api/admin/helena-cmo/brief
 * GET  /api/admin/helena-cmo/attention-count
 * POST /api/admin/helena-cmo/decisions/:id/action
 * POST /api/admin/helena-cmo/approvals/:id/action
 *
 * Persistence: local action log only. Marketing OS sync NOT connected.
 * EXECUTE / publish / recurring Helena remain OFF.
 */
import {
  buildFounderBriefViewModel,
  getHelenaAttentionCount,
  recordFounderConsoleDecision,
  recordFounderConsoleApproval,
} from '../lib/helena-cmo/founder-console/brief-view-model.js';

export function getHelenaCmoBrief(_req, res) {
  const brief = buildFounderBriefViewModel();
  if (!brief.ok) {
    return res.status(404).json(brief);
  }
  return res.status(200).json({ ok: true, success: true, brief });
}

export function getHelenaCmoAttentionCount(_req, res) {
  return res.status(200).json({ ok: true, success: true, ...getHelenaAttentionCount() });
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
  });
}

export function postHelenaCmoApprovalAction(req, res) {
  const approvalId = String(req.params.id || '').trim();
  const action = req.body?.action;
  const note = req.body?.note || '';
  if (!approvalId) {
    return res.status(400).json({ ok: false, error: 'MISSING_APPROVAL_ID' });
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
  });
}
