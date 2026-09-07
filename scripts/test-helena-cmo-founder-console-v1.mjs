#!/usr/bin/env node
/**
 * Helena CMO Founder Console v1 — view-model + safety gates.
 * node scripts/test-helena-cmo-founder-console-v1.mjs
 */
import fs from 'fs';
import path from 'path';
import { spawnSync } from 'child_process';
import { fileURLToPath } from 'url';
import {
  buildFounderBriefViewModel,
  getHelenaAttentionCount,
  recordFounderConsoleDecision,
  recordFounderConsoleApproval,
} from '../lib/helena-cmo/founder-console/brief-view-model.js';
import {
  buildFounderConsoleV2ViewModel,
  getHelenaConsoleAttentionCount,
} from '../lib/helena-cmo/founder-console/baseline-view-model.js';
import { canAccessHelenaCmoAdmin } from '../middleware/requireHelenaCmoAdminAccess.js';
import { requireAdminAccess } from '../middleware/requireAdminAccess.js';

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
      'middleware/requireHelenaCmoAdminAccess.js',
    ];
    for (const f of files) {
      if (!fs.existsSync(path.join(ROOT, f))) throw new Error(`missing ${f}`);
    }
    return { files: files.length };
  });

  test('FC-09', 'Browser-delivered JS parses (node --check)', () => {
    const files = [
      'public/js/admin-helena-cmo.js',
      'public/js/support-admin-gate.js',
    ];
    for (const rel of files) {
      const r = spawnSync(process.execPath, ['--check', path.join(ROOT, rel)], {
        encoding: 'utf8',
      });
      if (r.status !== 0) {
        throw new Error(`${rel}: ${r.stderr || r.stdout || 'parse failed'}`);
      }
    }
    return { checked: files.length };
  });

  test('FC-10', 'Founder/admin auth policy (not bare requireAdminAccess)', () => {
    // Owner+Brand demo constellation — same gate as canUseDemoFounderNavOverrides
    // (isDemo alone is intentionally insufficient).
    const founderOnly = {
      isAdmin: false,
      flags: {},
      workspaceAccess: [],
      isDemo: true,
      demoStakeholderMode: true,
      companyIds: ['recr0XXnseXNlIlxk', 'reciQEtqmxz6ZroVc'],
    };
    if (!canAccessHelenaCmoAdmin(founderOnly)) {
      throw new Error('founder constellation must pass Helena gate');
    }
    let adminRejected = false;
    requireAdminAccess(
      { dealalityUser: founderOnly },
      {
        status(code) {
          this.statusCode = code;
          return this;
        },
        json() {
          adminRejected = this.statusCode === 403;
          return this;
        },
      },
      () => {
        adminRejected = false;
      },
    );
    if (!adminRejected) {
      throw new Error('expected bare requireAdminAccess to reject founder-only (documents prior mismatch)');
    }
    const serverJs = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
    if (!serverJs.includes('helenaCmoAdminAuth') || !serverJs.includes('requireHelenaCmoAdminAccess')) {
      throw new Error('server must wire helenaCmoAdminAuth');
    }
    if (/app\.get\("\/api\/admin\/helena-cmo\/attention-count", \.\.\.adminAuth/.test(serverJs)) {
      throw new Error('attention-count still on bare adminAuth');
    }
    return { founderAllowed: true, bareAdminRejectsFounder: true };
  });

  test('FC-11', 'Console V2 loads baseline + strategy pending review', () => {
    const vm = buildFounderConsoleV2ViewModel();
    if (!vm.ok) throw new Error(vm.error || 'console v2 not ok');
    if (vm.defaultTab !== 'assessment') throw new Error('default must be assessment');
    if (vm.meta?.strategyState !== 'STRATEGY_PENDING_FOUNDER_REVIEW') {
      throw new Error('expected STRATEGY_PENDING_FOUNDER_REVIEW');
    }
    if (!vm.executiveAssessment?.centralProblem) throw new Error('missing diagnosis');
    if (!(vm.roadmap?.NOW || []).length) throw new Error('missing NOW roadmap');
    return { health: vm.overallMarketingHealth?.score, strategy: vm.meta.strategyState };
  });

  test('FC-12', 'V2 attention is strategy gate while pending', () => {
    const a = getHelenaConsoleAttentionCount();
    if (!a.ok || a.count !== 1 || !a.items.includes('STRATEGY-V1')) {
      throw new Error('attention should be strategy review only while pending');
    }
    return { count: a.count };
  });

  test('FC-13', 'Tactical actions marked pending strategy review', () => {
    const vm = buildFounderConsoleV2ViewModel();
    const tactical = vm.joanNeedsToDecide || [];
    if (!tactical.length) throw new Error('missing tactical');
    if (!tactical.every((d) => /PENDING STRATEGY REVIEW/i.test(d.displayStatus || ''))) {
      throw new Error('tactical must show pending strategy review');
    }
    const orphan = (vm.proposedTacticalActions || []).filter((a) => a.orphaned);
    if (orphan.length) throw new Error('orphaned actions present');
    return { tactical: tactical.length };
  });

  test('FC-14', 'HTML nav defaults to Executive Assessment', () => {
    const html = fs.readFileSync(path.join(ROOT, 'public/app/admin/helena-cmo.html'), 'utf8');
    if (!html.includes('data-tab="assessment"') || !html.includes('Executive Assessment')) {
      throw new Error('missing assessment tab');
    }
    if (!html.includes('Actions &amp; Approvals') && !html.includes('Actions & Approvals')) {
      throw new Error('missing actions tab');
    }
    if (!fs.existsSync(path.join(ROOT, 'reports/helena-cmo-baseline-v1/helena-cmo-baseline-v1.json'))) {
      throw new Error('baseline json missing');
    }
    if (!fs.existsSync(path.join(ROOT, 'reports/helena-cmo-baseline-v1/helena-cmo-strategy-v1.json'))) {
      throw new Error('strategy json missing');
    }
    return { nav: 'v2' };
  });
  test('FC-15', 'Phase 6D validation coverage on console', () => {
    const vm = buildFounderConsoleV2ViewModel();
    if (!vm.sourceCoverage || vm.sourceCoverage.coveragePercent < 50) {
      throw new Error('expected validation source coverage');
    }
    if (vm.executiveAssessment?.strategyApprovalRequested) {
      throw new Error('strategy approval must not be requested in 6D');
    }
    if (vm.executiveAssessment?.strategyChallengeDecision !== 'AMEND') {
      throw new Error('expected strategy challenge AMEND');
    }
    return {
      coverage: vm.sourceCoverage.coveragePercent,
      health: vm.overallMarketingHealth?.score,
    };
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
