/**
 * Helena CMO Founder Console — canonical view model from machine-readable Week packs.
 * Markdown reports are provenance only; this module is the Admin UI contract.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '../../..');
const WEEK01 = path.join(ROOT, 'reports/helena-cmo-manual-week-01/helena-cmo-manual-week-01.json');
const LAW = path.join(ROOT, 'reports/helena-cmo-operating-law-v1/helena-cmo-operating-law-v1.json');
const ACTIONS_PATH = path.join(ROOT, 'data/helena-cmo/founder-console-actions.json');

function readJson(file, fallback = null) {
  try {
    if (!fs.existsSync(file)) return fallback;
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
}

function ensureActionsStore() {
  const dir = path.dirname(ACTIONS_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  if (!fs.existsSync(ACTIONS_PATH)) {
    fs.writeFileSync(
      ACTIONS_PATH,
      JSON.stringify(
        {
          schemaVersion: 'helena-cmo-founder-console-actions-v1',
          note: 'Local admin persistence only — NOT Marketing OS. External EXECUTE remains OFF.',
          marketingOsSync: false,
          executeEnabled: false,
          recurringHelenaEnabled: false,
          decisions: [],
          approvals: [],
        },
        null,
        2,
      ),
    );
  }
  return readJson(ACTIONS_PATH, { decisions: [], approvals: [] });
}

export function getHelenaAttentionCount() {
  const brief = buildFounderBriefViewModel();
  return {
    ok: true,
    count: brief.attentionCount,
    items: brief.joanNeedsToDecide.map((d) => d.id),
  };
}

export function buildFounderBriefViewModel() {
  const week = readJson(WEEK01);
  const law = readJson(LAW, {});
  const actions = ensureActionsStore();

  if (!week) {
    return {
      ok: false,
      error: 'WEEK_PACK_MISSING',
      message: 'Manual Week 1 machine pack not found',
      attentionCount: 0,
    };
  }

  const decisionOverrides = new Map(
    (actions.decisions || []).map((d) => [d.decisionId, d]),
  );

  const joanNeedsToDecide = [
    {
      id: 'JD-W1-001',
      decision: 'Provide/confirm 3–7 warm ADP targets Joan will personally advance this week',
      helenaRecommendation: 'Supply named warm owner/hotel (or brand/operator) accounts — Marketing OS has class-level candidates only',
      whyNow: 'Without names, ADP commercialization cannot leave audience-class targeting',
      evidence: 'Week 1 ADP pipeline DATA_GAP for named CRM accounts; JD-6B-001 prioritizes paid pilots',
      confidence: 'HIGH',
      founderLock: 'DEC-2026-09-07-JD-6B-001-90-DAY-ACTIVATION',
      icp: 'ICP-O2',
      gtmTrack: 'ADP',
      product: 'AI Demand Positioning',
      productMaturity: 'PILOT_READY',
      consequenceOfDoingNothing: 'Week stays PREPARE-only; no paid pilot conversations advance',
      artifacts: ['01_ADP_COMMERCIAL_FOCUS.md', 'W1-PREP-ADP-ONEPAGER'],
      status: decisionOverrides.get('JD-W1-001')?.status || 'PENDING',
    },
    {
      id: 'JD-W1-002',
      decision: 'Approve ADP Pilot Scorecard as mandatory for any pilot start',
      helenaRecommendation: 'LOCK scorecard requirement before first paid pilot starts',
      whyNow: 'JD-6B-001: proof/instrumentation precedes low-evidence content',
      evidence: '02_ADP_PROOF_INSTRUMENTATION.md · 14_ADP_PILOT_SCORECARD.md',
      confidence: 'HIGH',
      founderLock: 'DEC-2026-09-07-JD-6B-001-90-DAY-ACTIVATION',
      icp: 'ICP-O2',
      gtmTrack: 'ADP',
      product: 'AI Demand Positioning',
      productMaturity: 'PILOT_READY',
      consequenceOfDoingNothing: 'Pilots may start without measurable proof system',
      artifacts: ['14_ADP_PILOT_SCORECARD.md', 'W1-PREP-ADP-SCORECARD'],
      status: decisionOverrides.get('JD-W1-002')?.status || 'PENDING',
    },
    {
      id: 'JD-W1-003',
      decision: 'Approve or HOLD P0 tracking Technical Work Items (no implementation until approved)',
      helenaRecommendation: 'Approve P0 TWIs for Cursor handoff OR explicitly HOLD',
      whyNow: 'Attribution chain LinkedIn→site→lead→pilot→revenue is BROKEN',
      evidence: '12_MEASUREMENT_ATTRIBUTION_AUDIT.md · 16_TRACKING_TECHNICAL_WORK_ITEMS.md',
      confidence: 'HIGH',
      founderLock: 'DEC-2026-09-07-JD-6B-002-PREPARE-AUTHORITY',
      icp: null,
      gtmTrack: null,
      product: 'Marketing measurement',
      productMaturity: 'IN_DEVELOPMENT',
      consequenceOfDoingNothing: 'Week 2+ still cannot learn what marketing works',
      artifacts: ['16_TRACKING_TECHNICAL_WORK_ITEMS.md'],
      status: decisionOverrides.get('JD-W1-003')?.status || 'PENDING',
    },
  ];

  const pendingDecisions = joanNeedsToDecide.filter((d) => d.status === 'PENDING').length;

  const approvals = [
    {
      id: 'W1-PREP-ADP-ONEPAGER',
      type: 'Sales support',
      title: 'ADP owner/hotel market-test pilot one-pager',
      why: 'Enable discuss-pilot conversations under D5 MARKET_TEST_DEFAULT',
      icp: 'ICP-O2',
      gtmTrack: 'ADP',
      claimClass: 'GREEN',
      productMaturity: 'PILOT_READY',
      cta: 'DISCUSS A PILOT',
      status: findApprovalStatus(actions, 'W1-PREP-ADP-ONEPAGER'),
      lastUpdated: week.generatedAt,
      approveDoesNotMean: ['PUBLISH', 'SEND', 'DEPLOY', 'MERGE'],
    },
    {
      id: 'W1-PREP-ADP-SCORECARD',
      type: 'Proof system',
      title: 'ADP pilot proof/instrumentation scorecard',
      why: 'Mandatory measurement before/during pilots',
      icp: 'ICP-O2',
      gtmTrack: 'ADP',
      claimClass: 'GREEN',
      productMaturity: 'PILOT_READY',
      cta: 'SEE HOW IT WORKS',
      status: findApprovalStatus(actions, 'W1-PREP-ADP-SCORECARD'),
      lastUpdated: week.generatedAt,
      approveDoesNotMean: ['PUBLISH', 'SEND', 'DEPLOY', 'MERGE'],
    },
    {
      id: 'W1-PREP-SITE-BRIEF',
      type: 'Website brief',
      title: 'Website dual-track page-restructure brief',
      why: 'Fix hierarchy: ADP missing from public narrative',
      icp: 'ICP-O1 / ICP-O2',
      gtmTrack: 'BOTH',
      claimClass: 'GREEN',
      productMaturity: 'PILOT_READY',
      cta: 'SEE HOW IT WORKS',
      status: findApprovalStatus(actions, 'W1-PREP-SITE-BRIEF'),
      lastUpdated: week.generatedAt,
      approveDoesNotMean: ['PUBLISH', 'SEND', 'DEPLOY', 'MERGE', 'WEBSITE CHANGE'],
    },
    {
      id: 'W1-PREP-LI-01',
      type: 'LinkedIn draft',
      title: 'LinkedIn — ADP owner stakes (commercial education)',
      why: 'JD-6B-003 commercial LinkedIn role; not vanity TL',
      icp: 'ICP-O2',
      gtmTrack: 'ADP',
      claimClass: 'GREEN',
      productMaturity: 'PILOT_READY',
      cta: 'SEE HOW IT WORKS',
      status: findApprovalStatus(actions, 'W1-PREP-LI-01'),
      lastUpdated: week.generatedAt,
      approveDoesNotMean: ['PUBLISH', 'SCHEDULE', 'SEND'],
    },
  ];

  const pendingApprovals = approvals.filter((a) => a.status === 'PENDING' || a.status === 'DRAFT').length;

  return {
    ok: true,
    schemaVersion: 'helena-cmo-founder-brief-v1',
    generatedAt: new Date().toISOString(),
    sourceWeek: {
      label: week.weekLabel,
      path: 'reports/helena-cmo-manual-week-01/helena-cmo-manual-week-01.json',
      generatedAt: week.generatedAt,
    },
    safety: {
      recurringHelenaEnabled: false,
      executeEnabled: false,
      websiteImplemented: false,
      marketingOsDecisionWriteConnected: false,
      localActionLogConnected: true,
      openInCursorSupported: false,
    },
    // Attention = founder action only (pending decisions + pending/draft approvals). Not informational.
    attentionCount: pendingDecisions + pendingApprovals,
    thisWeekInOneParagraph:
      week.weeklyPack?.sections?.['1_executive_cmo_summary']?.summary ||
      'Week 1 under JD-6B locks: prioritize ADP paid pilots + proof; keep dealmaking trigger-only; PREPARE site brief and commercial LinkedIn without publishing. Named CRM accounts are a data gap.',
    top3: (week.top3ThisWeek || []).slice(0, 3).map((text, i) => ({
      rank: i + 1,
      recommendation: text,
      whyItMatters:
        i === 0
          ? 'Primary proactive commercialization under JD-6B-001'
          : i === 1
            ? 'Proof/instrumentation takes precedence over low-evidence content'
            : 'Public narrative still selection-era; dual-track hierarchy blocked',
      gtmTrack: i === 2 ? 'BOTH' : i === 0 || i === 1 ? 'ADP' : 'OWNER_DEALMAKING',
      icp: i === 2 ? 'ICP-O1 / ICP-O2' : 'ICP-O2',
      status: 'ACTIVE',
    })),
    commercialScoreboard: {
      tier1: {
        paidPilots: week.commercial_outcomes?.paidPilotsKnownInOs ?? 0,
        qualifiedOpportunities: 'DATA_GAP',
        proposals: 'DATA_GAP',
        customers: 'DATA_GAP',
        revenue: 'UNKNOWN',
        retentionExpansion: 'UNKNOWN',
      },
      tier2: {
        demoPilotConversations: 'UNKNOWN',
        qualifiedLeads: 'DATA_GAP',
        targetAccountMovement: 'DATA_GAP',
      },
      tier3Note: 'Channel metrics are diagnostic only — not commercial success',
      phase3bDiagnostics: {
        ga4Sessions: week.commercial_outcomes?.phase3bGa4Sessions ?? null,
        organicSessions: week.commercial_outcomes?.phase3bOrganicSessions ?? null,
      },
    },
    whatIsWorking: [
      'Operating Law v1 + D1–D5 + JD-6B locks give clear strategy',
      'Joan LinkedIn engagement exists (Tier 3) relative to AO company page',
      'ADP market-test package is real and reusable under D5 for comparable 2-hotel pilots',
    ],
    whatIsNotWorking: [
      'Named ADP pipeline not in Marketing OS — cannot commercialize without Joan warm list',
      'Public site hierarchy hides ADP',
      'No attributed commercial outcomes from content (Commercial Outcome UNKNOWN)',
    ],
    whatIsUnknown: week.unknowns || [],
    helenaRecommends: (week.threeThingsIfOnlyThree || []).slice(0, 5),
    joanNeedsToDecide,
    nextWeek: [
      'Advance any Joan-confirmed ADP conversations',
      'Instantiate scorecard if a pilot starts',
      'Monitor P0 tracking TWI status after Joan decision',
      'Do not auto-start Week 2 content factory',
    ],
    redFlags: [
      {
        id: 'RF-ATTRIBUTION',
        severity: 'HIGH',
        text: 'LinkedIn → site → lead → pilot → revenue chain is BROKEN',
      },
      {
        id: 'RF-NO-NAMED-ACCOUNTS',
        severity: 'HIGH',
        text: 'No fabricated accounts — DATA_GAP for named CRM targets',
      },
      {
        id: 'RF-EXECUTE-OFF',
        severity: 'INFO',
        text: 'EXECUTE and recurring Helena remain OFF by design',
      },
    ],
    approvals,
    pendingApprovals,
    performance: {
      accountabilityRows: [
        {
          initiative: 'ADP commercialization',
          did: 'PREPARE materials; request warm list',
          expected: 'Named advances',
          happened: 'Pending Joan names',
          learned: 'OS lacks CRM join',
          change: 'JD-W1-001',
        },
        {
          initiative: 'ADP proof system',
          did: 'Scorecard defined',
          expected: 'Mandatory on start',
          happened: 'Not yet used',
          learned: 'Need JD-W1-002',
          change: 'Enforce before pilot',
        },
        {
          initiative: 'Website brief',
          did: 'PREPARE brief written',
          expected: 'Unblock future Cursor',
          happened: 'No build (correct)',
          learned: 'Hierarchy problem clear',
          change: 'Hold build',
        },
        {
          initiative: 'Measurement',
          did: 'Audit + P0 TWIs drafted',
          expected: 'Attribution path',
          happened: 'Still broken',
          learned: 'P0 required',
          change: 'JD-W1-003',
        },
      ],
      measurementState: week.measurement_state || {},
      attributionState: week.attribution_state || {},
    },
    adpPilots: {
      note: 'No live pilot instances in Marketing OS / local store yet',
      pilots: [],
      candidates: week.adpPipeline?.candidates || [],
      emptyState: 'No paid ADP pilots recorded. Candidate classes only — no named hotels fabricated.',
    },
    history: [
      {
        week: week.weekLabel,
        topPriorities: week.top3ThisWeek || [],
        decisionsMade: (week.founderLocksRecorded || []).map((d) => d.decisionId),
        majorLearnings: [
          'Strategy clear enough to operate',
          'Named account CRM join is the commercialization blocker',
          'Attribution insufficient for learning loops',
        ],
        commercialOutcomes: week.commercial_outcomes || {},
        packPath: 'reports/helena-cmo-manual-week-01/',
      },
    ],
    evidence: {
      label: 'SUPPORTING EVIDENCE',
      links: [
        { title: 'Manual Week 1 pack', path: 'reports/helena-cmo-manual-week-01/00_FOUNDER_WEEKLY_PACK.md' },
        { title: 'Operating Law v1', path: 'reports/helena-cmo-operating-law-v1/01_OPERATING_LAW_V1.md' },
        { title: 'Measurement audit', path: 'reports/helena-cmo-manual-week-01/12_MEASUREMENT_ATTRIBUTION_AUDIT.md' },
        { title: 'Website restructure brief', path: 'reports/helena-cmo-manual-week-01/04_WEBSITE_RESTRUCTURE_BRIEF.md' },
        { title: 'JD-6B-001 Activation', path: 'reports/helena-cmo-founder-decisions/JD_6B_001_90_DAY_ACTIVATION.md' },
        { title: 'Machine week JSON', path: 'reports/helena-cmo-manual-week-01/helena-cmo-manual-week-01.json' },
        { title: 'Operating Law JSON', path: 'reports/helena-cmo-operating-law-v1/helena-cmo-operating-law-v1.json' },
      ],
    },
    lawMeta: {
      version: law.version || '1.0.0',
      recurringHelenaEnabled: false,
      executeEnabled: false,
    },
    dataSourceMap: {
      thisWeekInOneParagraph: 'helena-cmo-manual-week-01.json → weeklyPack.sections.1_executive_cmo_summary',
      top3: 'helena-cmo-manual-week-01.json → top3ThisWeek',
      commercialScoreboard: 'helena-cmo-manual-week-01.json → commercial_outcomes (+ DATA_GAP fillers)',
      decisions: 'Week 1 Joan decisions + data/helena-cmo/founder-console-actions.json overrides',
      approvals: 'Week 1 PREPARE artifacts + local action log',
      adpPilots: 'empty pilots + adpPipeline.candidates (no fabricated names)',
    },
  };
}

function findApprovalStatus(actions, id) {
  const hit = (actions.approvals || []).find((a) => a.approvalId === id);
  return hit?.status || 'DRAFT';
}

/**
 * Persist founder decision action locally. Does NOT write Marketing OS.
 * Does NOT enable EXECUTE/publish.
 */
export function recordFounderConsoleDecision({ decisionId, action, note = '', actor = 'Joan' }) {
  const allowed = new Set(['LOCK', 'APPROVE', 'AMEND', 'HOLD', 'REJECT']);
  const normalized = String(action || '').toUpperCase();
  if (!allowed.has(normalized)) {
    return { ok: false, error: 'INVALID_ACTION', allowed: [...allowed] };
  }
  const store = ensureActionsStore();
  const entry = {
    decisionId,
    status: normalized === 'LOCK' || normalized === 'APPROVE' ? 'LOCKED' : normalized,
    action: normalized,
    note: String(note || '').slice(0, 2000),
    actor,
    at: new Date().toISOString(),
    marketingOsSynced: false,
    executeTriggered: false,
  };
  store.decisions = (store.decisions || []).filter((d) => d.decisionId !== decisionId);
  store.decisions.push(entry);
  fs.writeFileSync(ACTIONS_PATH, JSON.stringify(store, null, 2));
  return {
    ok: true,
    persisted: true,
    persistenceTarget: 'data/helena-cmo/founder-console-actions.json',
    marketingOsSync: false,
    executeEnabled: false,
    entry,
  };
}

export function recordFounderConsoleApproval({ approvalId, action, note = '', actor = 'Joan' }) {
  const allowed = new Set(['APPROVE', 'AMEND', 'HOLD', 'REJECT']);
  const normalized = String(action || '').toUpperCase();
  if (!allowed.has(normalized)) {
    return { ok: false, error: 'INVALID_ACTION', allowed: [...allowed] };
  }
  const store = ensureActionsStore();
  const entry = {
    approvalId,
    status: normalized === 'APPROVE' ? 'APPROVED_PREPARE_ONLY' : normalized,
    action: normalized,
    note: String(note || '').slice(0, 2000),
    actor,
    at: new Date().toISOString(),
    approveDoesNotMean: ['PUBLISH', 'SEND', 'DEPLOY', 'MERGE', 'LAUNCH'],
    executeTriggered: false,
  };
  store.approvals = (store.approvals || []).filter((a) => a.approvalId !== approvalId);
  store.approvals.push(entry);
  fs.writeFileSync(ACTIONS_PATH, JSON.stringify(store, null, 2));
  return {
    ok: true,
    persisted: true,
    persistenceTarget: 'data/helena-cmo/founder-console-actions.json',
    marketingOsSync: false,
    publishTriggered: false,
    executeEnabled: false,
    entry,
  };
}
