#!/usr/bin/env node
/**
 * Helena CMO Founder Console v1 — view-model + safety gates.
 * node scripts/test-helena-cmo-founder-console-v1.mjs
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  buildFounderBriefViewModel,
  getHelenaAttentionCount,
  recordFounderConsoleDecision,
  recordFounderConsoleApproval,
} from '../lib/helena-cmo/founder-console/brief-view-model.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'reports/helena-cmo-founder-console-v1');
const ACTIONS = path.join(ROOT, 'data/helena-cmo/founder-console-actions.json');

const results = [];
function test(id, name, fn) {
  try {
    const detail = fn() || {};
    results.push({ id, name, pass: true, ...detail });
  } catch (e) {
    results.push({ id, name, pass: false, error: e.message || String(e) });
  }
}

const actionsBackup = fs.existsSync(ACTIONS) ? fs.readFileSync(ACTIONS, 'utf8') : null;

try {
  if (fs.existsSync(ACTIONS)) fs.unlinkSync(ACTIONS);

  test('FC-01', 'Brief loads Manual Week 1', () => {
    const b = buildFounderBriefViewModel();
    if (!b.ok) throw new Error(b.error || 'brief not ok');
    if (!b.sourceWeek?.path?.includes('helena-cmo-manual-week-01')) throw new Error('wrong week source');
    return { week: b.sourceWeek.label };
  });

  test('FC-02', 'Top 3 + ≤3 decisions', () => {
    const b = buildFounderBriefViewModel();
    if ((b.top3 || []).length === 0 || (b.top3 || []).length > 3) throw new Error('top3 size');
    if ((b.joanNeedsToDecide || []).length === 0 || (b.joanNeedsToDecide || []).length > 3) {
      throw new Error('decisions size');
    }
    return { top3: b.top3.length, decisions: b.joanNeedsToDecide.length };
  });

  test('FC-03', 'UNKNOWN / DATA_GAP preserved', () => {
    const b = buildFounderBriefViewModel();
    const t1 = b.commercialScoreboard.tier1;
    const gaps = [t1.qualifiedOpportunities, t1.proposals, t1.revenue, t1.retentionExpansion];
    if (!gaps.some((g) => g === 'DATA_GAP' || g === 'UNKNOWN')) throw new Error('expected gaps');
    if (!(b.whatIsUnknown || []).length) throw new Error('unknowns empty');
    const blob = JSON.stringify(b);
    if (/Hilton Conrad|Marriott Marquis|fake hotel/i.test(blob)) throw new Error('fabricated account');
    return { unknowns: b.whatIsUnknown.length };
  });

  test('FC-04', 'Tier 1 present; safety flags OFF', () => {
    const b = buildFounderBriefViewModel();
    if (!b.commercialScoreboard?.tier1) throw new Error('no tier1');
    if (b.safety.executeEnabled || b.safety.recurringHelenaEnabled) throw new Error('unsafe flags');
    if (b.safety.openInCursorSupported) throw new Error('cursor deep-link must be omitted');
    if (b.safety.marketingOsDecisionWriteConnected) throw new Error('OS write must stay disconnected');
    return { execute: false, recurring: false };
  });

  test('FC-05', 'APPROVE is PREPARE-only', () => {
    const r = recordFounderConsoleApproval({
      approvalId: 'W1-PREP-ADP-ONEPAGER',
      action: 'APPROVE',
      note: 'test',
    });
    if (!r.ok) throw new Error(r.error);
    if (r.publishTriggered || r.executeEnabled) throw new Error('publish/execute leaked');
    if (r.entry.status !== 'APPROVED_PREPARE_ONLY') throw new Error(r.entry.status);
    if (r.marketingOsSync) throw new Error('os sync should be false');
    return { status: r.entry.status };
  });

  test('FC-06', 'Decision local persistence without EXECUTE', () => {
    const r = recordFounderConsoleDecision({
      decisionId: 'JD-W1-002',
      action: 'LOCK',
      note: 'test lock',
    });
    if (!r.ok || r.executeEnabled || r.marketingOsSync) throw new Error('bad decision persist');
    const b = buildFounderBriefViewModel();
    const d = b.joanNeedsToDecide.find((x) => x.id === 'JD-W1-002');
    if (!d || d.status !== 'LOCKED') throw new Error('status not applied');
    return { status: d.status };
  });

  test('FC-07', 'Attention counts action items only', () => {
    const a = getHelenaAttentionCount();
    if (!a.ok) throw new Error('attention fail');
    // JD-W1-002 locked → not pending; approvals remaining + other decisions
    if (typeof a.count !== 'number' || a.count < 1) throw new Error('expected attention');
    return { count: a.count };
  });

  test('FC-08', 'UI + API files exist', () => {
    const files = [
      'public/app/admin/helena-cmo.html',
      'public/js/admin-helena-cmo.js',
      'public/css/admin-helena-cmo.css',
      'api/admin-helena-cmo.js',
    ];
    for (const f of files) {
      if (!fs.existsSync(path.join(ROOT, f))) throw new Error(`missing ${f}`);
    }
    return { files: files.length };
  });
} finally {
  if (actionsBackup != null) fs.writeFileSync(ACTIONS, actionsBackup);
  else if (fs.existsSync(ACTIONS)) fs.unlinkSync(ACTIONS);
}

const failed = results.filter((r) => !r.pass);
fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, 'test-results.json'),
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      pass: failed.length === 0,
      passed: results.filter((r) => r.pass).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);

console.log(failed.length === 0 ? `PASS ${results.length}/${results.length}` : `FAIL ${failed.length}`);
for (const r of results) {
  console.log(`${r.pass ? '✓' : '✗'} ${r.id} ${r.name}`);
}
process.exit(failed.length ? 1 : 0);
