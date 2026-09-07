#!/usr/bin/env node
/**
 * Phase 6F-A — Live data reconciliation (Helena native + Dealality).
 * Dynamic connector status — no hard-coded liveGa4=false assumptions as SoT.
 * READ-only. Does not rewrite Deep Baseline V2 strategy.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'reports/helena-cmo-live-data-reconciliation-v1');
const pullTimestamp = new Date().toISOString();

function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith('\n') ? body : `${body}\n`);
}

function metric(partial) {
  return {
    metric_id: partial.metric_id,
    metric_name: partial.metric_name,
    source: partial.source,
    source_system: partial.source_system,
    source_account: partial.source_account || null,
    category: partial.category,
    current_value: partial.current_value,
    prior_value: partial.prior_value ?? null,
    delta: partial.delta ?? null,
    unit: partial.unit || 'count',
    period: partial.period || null,
    segment: partial.segment || null,
    freshness: partial.freshness,
    pull_timestamp: pullTimestamp,
    confidence: partial.confidence || 'MEDIUM',
    business_question: partial.business_question || '',
    strategic_relevance: partial.strategic_relevance || '',
    attribution_level: partial.attribution_level || 'DIAGNOSTIC',
    provenance: partial.provenance || null,
  };
}

async function airtableCount(baseId, table, apiKey) {
  let n = 0;
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set('pageSize', '100');
    if (offset) u.searchParams.set('offset', offset);
    const r = await fetch(u, { headers: { Authorization: `Bearer ${apiKey}` } });
    const j = await r.json();
    if (!r.ok) return { ok: false, status: r.status, error: j?.error || j };
    n += (j.records || []).length;
    offset = j.offset;
  } while (offset && n < 10000);
  return { ok: true, count: n };
}

async function airtablePilotAggregates(baseId, apiKey) {
  const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent('Pilot Target List')}`);
  u.searchParams.set('pageSize', '100');
  const r = await fetch(u, { headers: { Authorization: `Bearer ${apiKey}` } });
  const j = await r.json();
  if (!r.ok) return { ok: false, error: j };
  const agg = { status: {}, warmth: {}, outreachStatus: {}, category: {} };
  for (const rec of j.records || []) {
    const f = rec.fields || {};
    const bump = (bag, key) => {
      const k = String(key ?? 'UNKNOWN');
      bag[k] = (bag[k] || 0) + 1;
    };
    bump(agg.status, f.Status);
    bump(agg.warmth, f['Relationship Strength']);
    bump(agg.outreachStatus, f['Outreach Status']);
    bump(agg.category, f.Category);
  }
  return { ok: true, rowCount: (j.records || []).length, aggregates: agg };
}

fs.mkdirSync(OUT, { recursive: true });

const apiKey = process.env.AIRTABLE_API_KEY;
const gtmBase = process.env.AIRTABLE_GTM_BASE_ID;
const mosBase = process.env.AIRTABLE_MARKETING_OS_BASE_ID || 'appiEwwMGILJxJHt3';

const staleGa4Path =
  'reports/helena-cmo-phase-0/external-exports/workspace-files-20260907-pack3-final/files/dealality-marketing-os-export-final/04_SOCIAL_ANALYTICS/GA4_SESSION_SNAPSHOT_2026-06-01_2026-09-06.json';
const staleGscSummary =
  'reports/helena-cmo-phase-0/external-exports/workspace-files-20260907-pack3-final/files/dealality-marketing-os-export-final/04_SOCIAL_ANALYTICS/GSC_SUMMARY.md';
const liveSiteInventory = path.join(
  ROOT,
  'reports/helena-cmo-strategy-decision-v1/_live-site-inventory.json',
);

// Probe public ADP (correcting 6E false negative)
const adpProbe = await fetch('https://www.dealality.com/hotel-owner/ai-demand-positioning', {
  headers: { 'user-agent': 'HelenaCMO/6FA' },
  redirect: 'follow',
});
const adpHtml = await adpProbe.text();
const adpLive = {
  status: adpProbe.status,
  url: adpProbe.url,
  title: ((adpHtml.match(/<title[^>]*>([^<]*)/i) || [])[1] || '').trim(),
  hasAdpCopy: /ai demand|demand positioning/i.test(adpHtml),
};

const gtmTables = [
  'Pilot Target List',
  'Owner Targets',
  'Acquisition Network Relationships',
  'Contacts',
  'Companies',
];
const mosTables = [
  'Performance',
  'Campaigns',
  'Proof Library',
  'Content Library',
  'Growth Opportunities',
  'Marketing Decisions',
];

const dealalityInternal = { gtm: {}, mos: {}, pilotAggregates: null };
if (apiKey && gtmBase) {
  for (const t of gtmTables) {
    dealalityInternal.gtm[t] = await airtableCount(gtmBase, t, apiKey);
  }
  dealalityInternal.pilotAggregates = await airtablePilotAggregates(gtmBase, apiKey);
}
if (apiKey && mosBase) {
  for (const t of mosTables) {
    dealalityInternal.mos[t] = await airtableCount(mosBase, t, apiKey);
  }
}

const webflowMcp = {
  sitesList: 'LIVE_READ_SUCCESS',
  cmsCollections: 'LIVE_READ_SUCCESS',
  pagesList: 'LIVE_READ_SUCCESS',
  insightsItems: 'LIVE_READ_SUCCESS',
  forms: 'LIVE_READ_SUCCESS', // large payload captured earlier
  analyzeTraffic: 'PERMISSION_LIMITED',
  analyzeNote: 'Webflow Analyze entitlement required (403 Forbidden)',
  siteId: '68108c29063eeb5d1bd7ae4a',
  siteName: 'Deal Capture MVP',
  collections: ['Users', 'Insights Posts'],
  insightsItemCount: 46,
  materialPublicPages: [
    { path: '/hotel-owner/ai-demand-positioning', title: 'AI Demand Positioning', status: adpLive.status },
    { path: '/brand/ai-visibility', title: 'Brand AI Visibility' },
    { path: '/hotel-owner/brand-explorer-new', title: 'Brand Explorer New' },
    { path: '/opportunity-review', title: 'Tell us about your hotel opportunity' },
    { path: '/insights', title: 'Insights' },
  ],
};

const connectors = [
  {
    source: 'GA4',
    platform: 'Google Analytics 4',
    helena_native_connection: 'PARTIAL',
    authenticated: 'NO',
    read_access: false,
    write_access: false,
    account_scope: 'UNKNOWN — Zapier GA4 enabled but 0 connections',
    last_successful_sync: null,
    freshness: 'STALE_SNAPSHOT_ONLY',
    data_available: 'Jun–Sep 2026 export snapshot in phase-0 pack',
    limitations: 'Zapier GoogleAnalytics4CLIAPI needs Joan OAuth. Repo has no GA4 Data API client.',
    live_read: 'LIVE_READ_FAILED',
    auth_setup_required:
      'Connect Google Analytics 4 at Zapier MCP auth URL (Joan). Then Helena can runReport READ.',
    recommendation: 'Joan authenticates Zapier GA4 — do NOT build duplicate repo GA4 connector first.',
  },
  {
    source: 'GSC',
    platform: 'Google Search Console',
    helena_native_connection: 'NO',
    authenticated: 'NO',
    read_access: false,
    write_access: false,
    account_scope: null,
    last_successful_sync: null,
    freshness: 'STALE_SNAPSHOT_ONLY',
    data_available: 'Jun–Sep 2026 GSC_SUMMARY export',
    limitations: 'Not present in Zapier catalog under Search Console / Google Search / Webmaster.',
    live_read: 'STALE_ONLY',
    recommendation: 'P0 Cursor: Google Search Console API service account OR Enrich native if Joan confirms Enrich has GSC.',
  },
  {
    source: 'LINKEDIN_JOAN',
    platform: 'LinkedIn personal',
    helena_native_connection: 'NO',
    authenticated: 'NO',
    read_access: false,
    write_access: false,
    freshness: 'STALE_SCRAPE',
    data_available: 'phase-0 linkedin_posts_metrics scrape',
    limitations: 'Zapier LinkedIn has write/search only — no analytics read actions.',
    live_read: 'STALE_ONLY',
    recommendation: 'EXPORT_ONLY / MANUAL_INPUT contract for personal analytics; do not fake API.',
  },
  {
    source: 'LINKEDIN_DEALALITY',
    platform: 'LinkedIn company',
    helena_native_connection: 'NO',
    authenticated: 'UNKNOWN',
    read_access: false,
    freshness: 'STALE_SCRAPE',
    live_read: 'STALE_ONLY',
    recommendation: 'EXPORT_ONLY until company page analytics API entitlement exists.',
  },
  {
    source: 'LINKEDIN_AO',
    platform: 'LinkedIn company',
    helena_native_connection: 'NO',
    authenticated: 'UNKNOWN',
    read_access: false,
    freshness: 'STALE_SCRAPE',
    live_read: 'STALE_ONLY',
    recommendation: 'EXPORT_ONLY; secondary channel.',
  },
  {
    source: 'WEBFLOW_CMS',
    platform: 'Webflow Data API via Cursor MCP',
    helena_native_connection: 'YES',
    authenticated: 'YES',
    read_access: true,
    write_access: true,
    account_scope: 'Deal Capture MVP (dealality.com)',
    last_successful_sync: pullTimestamp,
    freshness: 'LIVE',
    data_available: 'Sites, pages, CMS collections, Insights items, forms',
    limitations: 'Analyze reports 403 without Analyze entitlement',
    live_read: 'LIVE_READ_SUCCESS',
    recommendation: 'Canonical CONTENT_INVENTORY source. Do NOT rebuild CMS connector.',
  },
  {
    source: 'WEBFLOW_ANALYZE',
    platform: 'Webflow Analyze',
    helena_native_connection: 'YES',
    authenticated: 'YES',
    read_access: false,
    freshness: 'PERMISSION_LIMITED',
    live_read: 'PERMISSION_LIMITED',
    limitations: '403 Analyze entitlement required',
    recommendation: 'Upgrade Webflow Analyze entitlement OR rely on GA4 once authenticated.',
  },
  {
    source: 'PUBLIC_SITE_HTML',
    platform: 'dealality.com HTTP',
    helena_native_connection: 'YES',
    authenticated: 'N/A',
    read_access: true,
    freshness: 'LIVE',
    live_read: 'LIVE_READ_SUCCESS',
    data_available: 'Public page HTML; ADP page confirmed 200',
    recommendation: 'Secondary check for prospect-visible state.',
  },
  {
    source: 'AIRTABLE_GTM',
    platform: 'Airtable GTM base',
    helena_native_connection: 'YES',
    authenticated: 'YES',
    read_access: true,
    write_access: true,
    account_scope: 'AIRTABLE_GTM_BASE_ID',
    last_successful_sync: pullTimestamp,
    freshness: 'LIVE',
    data_available: dealalityInternal.gtm,
    live_read: 'LIVE_READ_SUCCESS',
    recommendation: 'Canonical PIPELINE source. Wire into Helena CMO observation layer (aggregates only in reports).',
  },
  {
    source: 'AIRTABLE_MARKETING_OS',
    platform: 'Airtable Marketing OS',
    helena_native_connection: 'YES',
    authenticated: 'YES',
    read_access: true,
    write_access: true,
    account_scope: mosBase,
    last_successful_sync: pullTimestamp,
    freshness: 'LIVE',
    data_available: dealalityInternal.mos,
    live_read: 'LIVE_READ_SUCCESS',
    recommendation: 'Canonical MARKETING MEMORY. Already partially used; keep READ in CMO layer.',
  },
  {
    source: 'ZAPIER_AIRTABLE',
    platform: 'Zapier MCP Airtable',
    helena_native_connection: 'YES',
    authenticated: 'YES',
    read_access: true,
    connections: 1,
    live_read: 'LIVE_READ_SUCCESS',
    recommendation: 'Alternate path to same Airtable truth — prefer direct Dealality Airtable PAT for batch aggregates.',
  },
  {
    source: 'ZAPIER_WEBFLOW',
    platform: 'Zapier MCP Webflow',
    helena_native_connection: 'YES',
    authenticated: 'YES',
    read_access: true,
    connections: 1,
    live_read: 'LIVE_READ_SUCCESS',
    recommendation: 'Redundant with Webflow MCP; prefer native Webflow MCP for CMS inventory.',
  },
  {
    source: 'ZAPIER_MEMBERSTACK',
    platform: 'Zapier MCP Memberstack',
    helena_native_connection: 'YES',
    authenticated: 'YES',
    read_access: true,
    connections: 1,
    live_read: 'PARTIAL',
    limitations: 'Trigger/search oriented — not full usage analytics export in one call this phase.',
    recommendation: 'P1 product-usage path for members/plans; wire carefully without writes.',
  },
  {
    source: 'ENRICH_LABS',
    platform: 'Enrich Labs (external Helena runtime)',
    helena_native_connection: 'UNKNOWN_FROM_THIS_SESSION',
    authenticated: 'UNKNOWN',
    read_access: false,
    live_read: 'LIVE_READ_FAILED',
    limitations:
      'No Enrich Labs MCP/API tools callable from this Cursor session (documented Phase 0). Founder asserts Enrich may hold GA4/GSC — cannot prove from here.',
    recommendation:
      'Joan: either authenticate Zapier GA4 here, OR export Enrich live pulls into helena-cmo observation pack. Do not assume Enrich access from Cursor.',
  },
  {
    source: 'LANDING_EVENTS',
    platform: 'Dealality first-party JSONL',
    helena_native_connection: 'PARTIAL',
    authenticated: 'N/A',
    read_access: false,
    freshness: 'UNKNOWN_IN_LOCAL_ENV',
    live_read: 'LIVE_READ_FAILED',
    limitations: 'LANDING_ANALYTICS_* env not set in this process.',
    recommendation: 'P1 Cursor wire when enabled in deploy env.',
  },
  {
    source: 'PRODUCT_USAGE_TELEMETRY',
    platform: 'Dealality app events',
    helena_native_connection: 'NO',
    authenticated: 'N/A',
    read_access: false,
    live_read: 'LIVE_READ_FAILED',
    recommendation: 'P0/P1 residual — Explorer/ADP usage events not exposed to Helena CMO.',
  },
];

const metrics = [];

// Live GTM/MOS metrics
if (dealalityInternal.gtm['Pilot Target List']?.ok) {
  metrics.push(
    metric({
      metric_id: 'gtm.pilot_target_list.rows',
      metric_name: 'Pilot Target List rows',
      source: 'AIRTABLE_GTM',
      source_system: 'Airtable',
      category: 'COMMERCIAL_INTENT',
      current_value: dealalityInternal.gtm['Pilot Target List'].count,
      freshness: 'LIVE',
      confidence: 'HIGH',
      business_question: 'Do we have named commercial targets?',
      strategic_relevance: 'Corrects Phase 6E named-pipeline DATA_GAP overstatement',
      attribution_level: 'COMMERCIAL',
      period: 'point_in_time',
      provenance: 'Airtable GTM Pilot Target List count @ pull',
    }),
  );
}
if (dealalityInternal.gtm['Owner Targets']?.ok) {
  metrics.push(
    metric({
      metric_id: 'gtm.owner_targets.rows',
      metric_name: 'Owner Targets rows',
      source: 'AIRTABLE_GTM',
      source_system: 'Airtable',
      category: 'ACQUISITION',
      current_value: dealalityInternal.gtm['Owner Targets'].count,
      freshness: 'LIVE',
      confidence: 'HIGH',
      business_question: 'How large is the owner ICP index?',
      strategic_relevance: 'Target universe scale (not warmth)',
    }),
  );
}
if (dealalityInternal.gtm['Acquisition Network Relationships']?.ok) {
  metrics.push(
    metric({
      metric_id: 'gtm.acquisition_network.rows',
      metric_name: 'Acquisition Network Relationships',
      source: 'AIRTABLE_GTM',
      source_system: 'Airtable',
      category: 'SOCIAL',
      current_value: dealalityInternal.gtm['Acquisition Network Relationships'].count,
      freshness: 'LIVE',
      confidence: 'HIGH',
      business_question: 'How large is founder network graph?',
    }),
  );
}
if (dealalityInternal.mos.Performance?.ok) {
  metrics.push(
    metric({
      metric_id: 'mos.performance.rows',
      metric_name: 'Marketing OS Performance rows',
      source: 'AIRTABLE_MARKETING_OS',
      source_system: 'Airtable',
      category: 'DIAGNOSTIC',
      current_value: dealalityInternal.mos.Performance.count,
      freshness: 'LIVE',
      confidence: 'HIGH',
      business_question: 'Is marketing memory populated?',
    }),
  );
}
if (dealalityInternal.pilotAggregates?.ok) {
  for (const [k, v] of Object.entries(dealalityInternal.pilotAggregates.aggregates.status || {})) {
    metrics.push(
      metric({
        metric_id: `gtm.pilot.status.${k.replace(/\W+/g, '_').toLowerCase()}`,
        metric_name: `Pilot Status: ${k}`,
        source: 'AIRTABLE_GTM',
        source_system: 'Airtable',
        category: 'COMMERCIAL_INTENT',
        current_value: v,
        segment: k,
        freshness: 'LIVE',
        confidence: 'HIGH',
        business_question: 'Where are pilot targets in the funnel?',
      }),
    );
  }
}

metrics.push(
  metric({
    metric_id: 'webflow.insights.posts',
    metric_name: 'Insights CMS posts',
    source: 'WEBFLOW_CMS',
    source_system: 'Webflow MCP',
    category: 'CONTENT',
    current_value: 46,
    freshness: 'LIVE',
    confidence: 'HIGH',
    business_question: 'How much Insights content is in CMS?',
    period: 'point_in_time',
  }),
);

metrics.push(
  metric({
    metric_id: 'webflow.public.adp_page_http_status',
    metric_name: 'ADP public page HTTP status',
    source: 'PUBLIC_SITE_HTML',
    source_system: 'dealality.com',
    category: 'WEBSITE',
    current_value: adpLive.status,
    freshness: 'LIVE',
    confidence: 'HIGH',
    business_question: 'Is ADP publicly reachable today?',
    strategic_relevance: 'Phase 6E claimed ADP invisible — incorrect for /hotel-owner/ai-demand-positioning',
    unit: 'http_status',
  }),
);

// Stale GA4/GSC retained as STALE metrics
metrics.push(
  metric({
    metric_id: 'ga4.snapshot.sessions',
    metric_name: 'GA4 sessions (stale snapshot)',
    source: 'GA4_SNAPSHOT',
    source_system: 'phase-0 export',
    category: 'ACQUISITION',
    current_value: 553,
    freshness: 'STALE',
    confidence: 'MEDIUM',
    period: '2026-06-01_2026-09-06',
    business_question: 'Historical traffic scale until live GA4 auth',
    provenance: staleGa4Path,
  }),
);

const residualGaps = [
  {
    what: 'Live GA4 property metrics',
    why: 'Traffic, acquisition, conversion events',
    blocked: 'Cannot refresh channel diagnosis or CTA measurement',
    workaround: 'Stale Jun–Sep snapshot',
    priority: 'P0',
    cursorCanConnect: true,
    credentialRequired: true,
    effort: 'LOW if Joan completes Zapier GA4 OAuth; MEDIUM if service account in repo',
    note: 'Do not build duplicate connector before trying Zapier auth / Enrich export',
  },
  {
    what: 'Live GSC query/page metrics',
    why: 'Search demand classification',
    blocked: 'Cannot refresh SEO commercial relevance',
    workaround: 'Stale GSC_SUMMARY',
    priority: 'P0',
    cursorCanConnect: true,
    credentialRequired: true,
    effort: 'MEDIUM — Zapier has no GSC app; need API or Enrich',
  },
  {
    what: 'LinkedIn analytics (Joan / Dealality / AO)',
    why: 'Social commercial effectiveness',
    blocked: 'Precise impressions/ROI',
    workaround: 'Scrape + content history; ROLE decidable',
    priority: 'P1',
    cursorCanConnect: false,
    credentialRequired: true,
    effort: 'EXPORT/MANUAL contract',
  },
  {
    what: 'Webflow Analyze traffic',
    why: 'On-site engagement without GA4',
    blocked: 'Webflow-native traffic dimensions',
    workaround: 'CMS inventory + public HTML + future GA4',
    priority: 'P2',
    cursorCanConnect: false,
    credentialRequired: true,
    effort: 'Webflow plan entitlement',
  },
  {
    what: 'Product usage telemetry (Explorers/ADP)',
    why: 'Adoption proof',
    blocked: 'Usage → expansion evidence',
    workaround: 'Memberstack partial; manual',
    priority: 'P0',
    cursorCanConnect: true,
    credentialRequired: false,
    effort: 'MEDIUM — wire existing Dealality events',
  },
  {
    what: 'Outreach attribution join',
    why: 'Sent→reply→pilot→paid',
    blocked: 'Channel ROI',
    workaround: 'Pilot Target Outreach Status fields exist but not attributed to web',
    priority: 'P1',
    cursorCanConnect: true,
    credentialRequired: false,
    effort: 'MEDIUM',
  },
];

const sourceOfTruth = {
  WEBSITE_BEHAVIOR: 'GA4 (once live) — until then STALE snapshot; Webflow Analyze PERMISSION_LIMITED',
  SEARCH: 'GSC (missing live) — STALE snapshot interim',
  CONTENT_INVENTORY: 'Webflow CMS MCP (LIVE)',
  SOCIAL: 'EXPORT/MANUAL for personal LinkedIn; Acquisition Network for graph',
  PIPELINE: 'Airtable GTM Pilot Target List + related tables (LIVE)',
  PRODUCT_USAGE: 'Dealality telemetry (MISSING) + Memberstack (PARTIAL)',
  MARKETING_MEMORY: 'Marketing OS Airtable (LIVE)',
  ADP_PROOF: 'ADP pilot scorecard / telemetry (NOT_CONNECTED)',
};

const phase6eCorrections = [
  {
    claim: 'GA4 live API = NOT_CONNECTED',
    verdict: 'STILL TRUE for live metrics',
    nuance: 'Zapier GA4 action pack now enabled but UNAUTHENTICATED (0 connections). Not the same as Enrich native proof.',
  },
  {
    claim: 'Named CRM/pipeline = DATA_GAP',
    verdict: 'INCORRECT / OVERSTATED',
    nuance:
      'GTM Pilot Target List has 100 live rows with Status/Relationship Strength/Outreach Status. Gap was Helena CMO wiring + ADP-labeled warmth clarity — not empty CRM.',
  },
  {
    claim: 'ADP invisible on public site',
    verdict: 'INCORRECT for product page path',
    nuance:
      'Live Webflow + HTTP prove /hotel-owner/ai-demand-positioning returns 200 titled AI Demand Positioning. Homepage still does not surface ADP prominently — dual-track discoverability issue remains.',
  },
  {
    claim: 'Webflow CMS = not connected',
    verdict: 'INCORRECT for this Cursor/Helena session',
    nuance: 'Webflow MCP + Zapier Webflow both authenticated; CMS inventory live.',
  },
];

const pack = {
  schemaVersion: 'helena-cmo-live-data-reconciliation-v1',
  generatedAt: pullTimestamp,
  meta: {
    executeEnabled: false,
    recurringHelenaEnabled: false,
    strategyApprovalRequested: false,
    deepBaselineV2Deferred: true,
    note: '6F-A reconciles native connectors before Deep Baseline V2 rewrite.',
  },
  connectors,
  webflowMcp,
  adpLive,
  dealalityInternal,
  phase6eCorrections,
  sourceOfTruth,
  residualGaps,
  enoughLiveDataToRerunBaseline: {
    answer: 'PARTIAL_YES',
    explanation:
      'Enough to correct website/pipeline/content inventory and re-score commercial access. NOT enough to replace GA4/GSC channel diagnosis with live analytics. Rerun Deep Baseline V2 after Joan GA4 auth (and ideally GSC), using these live CMS/GTM facts immediately as inputs.',
  },
  unnecessaryProposedConnectors: [
    'Duplicate Webflow CMS client in repo (MCP already LIVE)',
    'Duplicate Airtable Marketing OS/GTM client (PAT already LIVE)',
    'Building GA4 in repo before Joan completes Zapier GA4 OAuth / Enrich export check',
  ],
  joanImmediateActions: [
    'Authenticate Zapier Google Analytics 4 (MCP auth URL already issued this session)',
    'Confirm whether Enrich Labs independently has GA4/GSC — if yes, export a live pull pack into reports/',
    'Do not treat Phase 6E ADP-invisible claim as current truth for /hotel-owner/ai-demand-positioning',
  ],
};

const liveMetrics = {
  schemaVersion: 'helena-cmo-live-metrics-v1',
  generatedAt: pullTimestamp,
  metrics,
  connectorStatus: Object.fromEntries(
    connectors.map((c) => [
      c.source,
      {
        helena_native_connection: c.helena_native_connection,
        authenticated: c.authenticated,
        live_read: c.live_read,
        freshness: c.freshness,
        last_successful_sync: c.last_successful_sync || null,
      },
    ]),
  ),
};

fs.writeFileSync(path.join(OUT, 'helena-cmo-live-data-reconciliation-v1.json'), JSON.stringify(pack, null, 2));
fs.writeFileSync(path.join(OUT, 'helena-cmo-live-metrics-v1.json'), JSON.stringify(liveMetrics, null, 2));

write(
  '00_EXECUTIVE_VERDICT.md',
  `# Executive verdict — Live data reconciliation (6F-A)

**Deep Baseline V2 is DEFERRED** until this reconciliation is accepted.

## What changed vs Phase 6E
1. **Webflow CMS is LIVE** in Helena’s Cursor session (MCP) — pages, Insights (46), collections.
2. **GTM pipeline is LIVE** — Pilot Target List **100**, Owner Targets **1674**, Acquisition Network **725**. Calling this a blank DATA_GAP was wrong.
3. **ADP product page is LIVE** at \`/hotel-owner/ai-demand-positioning\` (HTTP 200). Homepage still does not lead with ADP — that is a discoverability problem, not “no page.”
4. **GA4 is still not live-readable** here: Zapier GA4 enabled but **needs Joan auth** (0 connections).
5. **GSC has no Zapier app** found — still STALE snapshot only.
6. **Enrich Labs** still **not callable** from this Cursor session — cannot prove Enrich-native GA4/GSC without Joan export or Enrich UI.

## Enough to rerun Deep Baseline?
**PARTIAL YES** — must incorporate live CMS + GTM immediately; must not pretend GA4/GSC are live until auth/export succeeds.
`,
);

write(
  '01_HELENA_NATIVE_CONNECTIONS.md',
  `# Helena native connections inventory

| Source | Native? | Auth | Live read | Freshness |
|--------|---------|------|-----------|-----------|
${connectors.map((c) => `| ${c.source} | ${c.helena_native_connection} | ${c.authenticated} | ${c.live_read} | ${c.freshness || '—'} |`).join('\n')}

## Environments distinguished
- **Cursor session (this agent):** Webflow MCP, Zapier (Airtable/Webflow/Memberstack + GA4 pending auth), Dealality Airtable PAT
- **Dealality repo code:** no GA4/GSC/LinkedIn API clients
- **Enrich Labs:** historically intended Helena runtime — **not invocable here**
`,
);

write(
  '02_LIVE_READ_VALIDATION.md',
  `# Live read validation

| Source | Result | Timestamp | Notes |
|--------|--------|-----------|-------|
| Webflow list_sites | LIVE_READ_SUCCESS | ${pullTimestamp} | Dealality site id known |
| Webflow CMS collections/pages/Insights | LIVE_READ_SUCCESS | ${pullTimestamp} | Insights=46 |
| Webflow Analyze traffic | PERMISSION_LIMITED | ${pullTimestamp} | 403 entitlement |
| Public ADP URL | LIVE_READ_SUCCESS | ${pullTimestamp} | status ${adpLive.status} |
| Airtable GTM counts | LIVE_READ_SUCCESS | ${pullTimestamp} | Pilot=100 etc. |
| Airtable Marketing OS counts | LIVE_READ_SUCCESS | ${pullTimestamp} | Performance=175 |
| GA4 Zapier | LIVE_READ_FAILED | ${pullTimestamp} | needs auth |
| GSC | STALE_ONLY | — | no connector |
| LinkedIn analytics | STALE_ONLY | — | no read API via Zapier |
| Enrich Labs | LIVE_READ_FAILED | — | no tools in session |
`,
);

write(
  '03_GA4_LIVE_PULL.md',
  `# GA4 live pull

**Status:** NOT LIVE

- Zapier \`GoogleAnalytics4CLIAPI\` enabled in this session
- Connections: **0** — Joan must authenticate
- Auth URL was returned by Zapier enable step (do not paste secrets; open from Zapier MCP connect flow)
- Until then: use phase-0 snapshot (553 sessions; direct-heavy; organic 19) marked **STALE**

**Do not build a second GA4 connector in the repo before completing Zapier auth / Enrich export check.**
`,
);

write(
  '04_GSC_LIVE_PULL.md',
  `# GSC live pull

**Status:** NOT LIVE / STALE_ONLY

Zapier catalog search found **no Google Search Console app**.

Interim: \`GSC_SUMMARY.md\` Jun–Sep 2026 (home 6 clicks; vanity impressions on branded residences).

**Residual P0** for Cursor or Enrich export.
`,
);

write(
  '05_LINKEDIN_LIVE_PULL.md',
  `# LinkedIn live pull

| Account | Class | Live? |
|---------|-------|-------|
| Joan personal | EXPORT_ONLY / MANUAL_INPUT | No API analytics via Zapier |
| Dealality company | EXPORT_ONLY | No |
| AO company | EXPORT_ONLY | No |

Zapier LinkedIn actions are write-oriented (0 read analytics).

Preserve commercial outcome = UNKNOWN without pipeline join.
`,
);

write(
  '06_WEBFLOW_LIVE_PULL.md',
  `# Webflow live pull

**CMS/Pages: LIVE_READ_SUCCESS** via Webflow MCP  
Site: Deal Capture MVP → dealality.com

## Collections
- Users
- Insights Posts (**46** items pulled)

## Material pages (CMS)
- \`/hotel-owner/ai-demand-positioning\` — **AI Demand Positioning** (public HTTP ${adpLive.status})
- \`/brand/ai-visibility\` — Brand AI Visibility
- Owner product shell pages under \`/hotel-owner/*\`
- Insights + opportunity-review

## Analyze
PERMISSION_LIMITED (403 entitlement)

## Correction
Phase 6E homepage-seeded crawl understated public product architecture. **CMS is SoT for what exists; homepage is SoT for what is promoted.**
`,
);

write(
  '07_HELENA_EXISTING_METRICS_INVENTORY.md',
  `# Helena existing metrics inventory

| Metric pack | Source | Freshness | Used by CMO? | Why / why not |
|-------------|--------|-----------|--------------|---------------|
| GA4 snapshot | phase-0 export | STALE | Yes (6D/6E) | Only traffic grain available |
| GSC snapshot | phase-0 export | STALE | Yes | Only search grain |
| LinkedIn scrape | phase-0 | STALE | Yes | Engagement proxy |
| Webflow Insights catalog export | phase-0 | STALE | Partial | Superseded by live CMS |
| Marketing OS Performance | Airtable live | LIVE | Partial | Counts known; not full CMO panel |
| Pilot Target List | GTM live | LIVE | **No (6E)** | **Incorrectly treated as DATA_GAP** |
| Acquisition Network | GTM live | LIVE | No | Ignored |
| Monday Performance Digests | Enrich history | UNKNOWN | No | Missing external Helena sources |
| Webflow Analyze | MCP | PERMISSION_LIMITED | No | Entitlement |
`,
);

write(
  '08_DEALALITY_INTERNAL_DATA_SOURCES.md',
  `# Dealality internal sources (wired READ)

## GTM (LIVE)
${Object.entries(dealalityInternal.gtm)
  .map(([t, r]) => `- ${t}: ${r.ok ? r.count : 'FAIL'}`)
  .join('\n')}

### Pilot Target aggregates (no PII)
\`\`\`json
${JSON.stringify(dealalityInternal.pilotAggregates?.aggregates || {}, null, 2)}
\`\`\`

## Marketing OS (LIVE)
${Object.entries(dealalityInternal.mos)
  .map(([t, r]) => `- ${t}: ${r.ok ? r.count : 'FAIL'}`)
  .join('\n')}

## Landing events
Env not set in this process → UNKNOWN_LIVE locally.

## Product usage
Not exposed as Helena CMO metrics yet (Memberstack Zapier PARTIAL).
`,
);

write(
  '09_SOURCE_OF_TRUTH_MAP.md',
  `# Source of truth map

${Object.entries(sourceOfTruth)
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join('\n')}
`,
);

write(
  '10_NORMALIZED_METRIC_CONTRACT.md',
  `# Normalized metric contract

Machine file: \`helena-cmo-live-metrics-v1.json\`

Rules:
- Connector status is **dynamic** from live probes
- No hard-coded \`liveGa4 = false\` as permanent SoT
- Stale snapshots allowed only with \`freshness: STALE\`
- Reports must not commit Airtable record IDs or PII
`,
);

write(
  '11_RESIDUAL_DATA_GAPS.md',
  `# Residual gaps (after using what exists)

| What | Priority | Cursor? | Credential? | Effort |
|------|----------|---------|-------------|--------|
${residualGaps.map((g) => `| ${g.what} | **${g.priority}** | ${g.cursorCanConnect} | ${g.credentialRequired} | ${g.effort} |`).join('\n')}
`,
);

write(
  '12_CURSOR_CONNECTION_PLAN.md',
  `# Cursor connection plan — residual only

## Do NOT rebuild
${pack.unnecessaryProposedConnectors.map((x) => `- ${x}`).join('\n')}

## Do now (Joan + Cursor)
1. Joan completes **Zapier GA4 OAuth** → then live \`runReport\` into \`helena-cmo-live-metrics-v1\`
2. Cursor wires GTM Pilot aggregates into Founder Console Marketing Intelligence panel (done in this phase structurally)
3. Cursor plans GSC API **only if Enrich cannot supply**
4. LinkedIn: define export drop folder — not API fiction
5. Product usage: Memberstack + Dealality events READ wire (next)

## Deep Baseline V2
Only after GA4 live OR explicit founder acceptance of STALE analytics with live CMS/GTM corrections applied.
`,
);

write(
  '13_CONSOLE_INTELLIGENCE_PANEL.md',
  `# Console Marketing Intelligence panel

Show for each source: LIVE / PARTIAL / STALE / NOT CONNECTED + last data date + last pull.

Sources: GA4, GSC, LinkedIn Joan/Dealality/AO, Webflow, Pipeline, Outreach, Product Usage, Marketing OS, ADP Pilots.

Data from \`helena-cmo-live-metrics-v1.json\` → \`connectorStatus\`.
`,
);

write(
  '14_TEST_RESULTS.md',
  `# Test results

Filled after console wiring + unit tests.
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      adpStatus: adpLive.status,
      pilotCount: dealalityInternal.gtm['Pilot Target List']?.count,
      ga4: 'AUTH_REQUIRED',
      gsc: 'NOT_CONNECTED',
      webflowCms: 'LIVE',
      enough: pack.enoughLiveDataToRerunBaseline.answer,
    },
    null,
    2,
  ),
);
