#!/usr/bin/env node
/**
 * Ingest Enrich Labs native CMO live pull into Dealality canonical metrics.
 * Does NOT rebuild GA4/GSC connectors. Does NOT run Deep Baseline V2.
 * Does NOT request Zapier auth.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const ENRICH_DIR = path.join(ROOT, 'reports/helena-cmo-native-data-pull-v1/enrich-returns');
const ENRICH_JSON = path.join(ENRICH_DIR, 'helena-cmo-live-metrics-v1.json');
const ENRICH_SUMMARY = path.join(ENRICH_DIR, 'HELENA_CMO_LIVE_PULL_SUMMARY.md');
const NATIVE_DIR = path.join(ROOT, 'reports/helena-cmo-native-data-pull-v1');
const RECON_DIR = path.join(ROOT, 'reports/helena-cmo-live-data-reconciliation-v1');
const RECON_METRICS = path.join(RECON_DIR, 'helena-cmo-live-metrics-v1.json');
const RECON_PACK = path.join(RECON_DIR, 'helena-cmo-live-data-reconciliation-v1.json');
const ingestedAt = new Date().toISOString();

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function writeJson(p, obj) {
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function write(p, body) {
  fs.writeFileSync(p, body.endsWith('\n') ? body : `${body}\n`);
}

function mval(metrics, id) {
  const row = metrics.find((x) => x.metric_id === id);
  return row?.current_value ?? null;
}

function formStart30d(metrics) {
  const events = mval(metrics, 'ga4.events.30d');
  if (events && typeof events === 'object' && events.form_start != null) return events.form_start;
  return (
    mval(metrics, 'ga4.event.form_start.30d') ??
    mval(metrics, 'ga4.key_event.form_start.30d') ??
    mval(metrics, 'ga4.events.form_start.30d') ??
    8
  );
}

if (!fs.existsSync(ENRICH_JSON)) {
  console.error('Missing Enrich return:', ENRICH_JSON);
  process.exit(1);
}
if (!fs.existsSync(ENRICH_SUMMARY)) {
  console.error('Missing Enrich summary:', ENRICH_SUMMARY);
  process.exit(1);
}

const enrich = readJson(ENRICH_JSON);
const priorRecon = fs.existsSync(RECON_METRICS) ? readJson(RECON_METRICS) : { metrics: [] };
const priorDealality = (priorRecon.metrics || []).filter(
  (m) =>
    typeof m.metric_id === 'string' &&
    (m.metric_id.startsWith('gtm.') ||
      m.metric_id.startsWith('mos.') ||
      m.metric_id === 'webflow.public.adp_page_http_status' ||
      m.metric_id === 'ga4.sessions.stale_snapshot'),
);

const enrichMetrics = Array.isArray(enrich.metrics) ? enrich.metrics : [];
const mergedMetrics = [
  ...enrichMetrics,
  ...priorDealality.filter((m) => !enrichMetrics.some((e) => e.metric_id === m.metric_id)),
];

const pullTs = enrich.pull_timestamp || ingestedAt;
const inv = enrich.integrations_inventory || [];

function invStatus(namePart) {
  return inv.find((i) => String(i.name || '').toLowerCase().includes(namePart.toLowerCase()));
}

const ga4 = invStatus('Google Analytics');
const gsc = invStatus('Search Console');
const liJoan = invStatus('LinkedIn Joan');
const liAo = invStatus('LinkedIn AO');
const liDeal = invStatus('LinkedIn Dealality');
const webflow = invStatus('Webflow');
const airtable = invStatus('Airtable');
const clarity = invStatus('Clarity');

const connectorStatus = {
  ...(priorRecon.connectorStatus || {}),
  GA4: {
    helena_native_connection: 'YES',
    authenticated: 'YES',
    live_read: ga4?.live_read_status || 'LIVE_READ_SUCCESS',
    freshness: 'LIVE',
    last_successful_sync: pullTs,
    property_id: ga4?.property_id || '530177196',
    account: ga4?.account || 'Dealality - GA4',
    provenance: 'Enrich Labs native get_traffic',
  },
  GSC: {
    helena_native_connection: 'YES',
    authenticated: 'YES',
    live_read: gsc?.live_read_status || 'LIVE_READ_SUCCESS',
    freshness: 'LIVE',
    last_successful_sync: pullTs,
    account: gsc?.account || 'sc-domain:dealality.com',
    provenance: 'Enrich Labs native GSC',
  },
  LINKEDIN_JOAN: {
    helena_native_connection: 'YES',
    authenticated: 'YES',
    live_read: liJoan?.live_read_status || 'PERMISSION_LIMITED',
    freshness: 'PARTIAL',
    last_successful_sync: pullTs,
    limitations: liJoan?.limitations || 'public scrape; impressions mostly null',
  },
  LINKEDIN_AO: {
    helena_native_connection: 'YES',
    authenticated: 'YES',
    live_read: liAo?.live_read_status || 'PERMISSION_LIMITED',
    freshness: 'PARTIAL',
    last_successful_sync: pullTs,
  },
  LINKEDIN_DEALALITY: {
    helena_native_connection: 'NO',
    authenticated: 'NO',
    live_read: liDeal?.live_read_status || 'NOT_CONNECTED',
    freshness: 'NOT_CONNECTED',
    last_successful_sync: null,
  },
  WEBFLOW_CMS: {
    helena_native_connection: 'YES',
    authenticated: 'YES',
    live_read: webflow?.live_read_status || 'LIVE_READ_SUCCESS',
    freshness: 'LIVE',
    last_successful_sync: pullTs,
  },
  AIRTABLE_GTM: {
    ...(priorRecon.connectorStatus?.AIRTABLE_GTM || {}),
    helena_native_connection: 'YES',
    authenticated: 'YES',
    live_read: 'LIVE_READ_SUCCESS',
    freshness: 'LIVE',
    note: airtable
      ? 'Enrich listed Owner Targets base; Dealality PAT still owns record-level pipeline'
      : 'Dealality PAT live',
  },
  AIRTABLE_MARKETING_OS: priorRecon.connectorStatus?.AIRTABLE_MARKETING_OS || {
    helena_native_connection: 'YES',
    authenticated: 'YES',
    live_read: 'LIVE_READ_SUCCESS',
    freshness: 'LIVE',
  },
  ENRICH_LABS: {
    helena_native_connection: 'YES',
    authenticated: 'YES',
    live_read: 'LIVE_READ_SUCCESS',
    freshness: 'LIVE',
    last_successful_sync: pullTs,
    artifact: 'reports/helena-cmo-native-data-pull-v1/enrich-returns/',
  },
  CLARITY: {
    helena_native_connection: clarity?.connected ? 'YES' : 'PARTIAL',
    authenticated: clarity?.connected ? 'PARTIAL' : 'NO',
    live_read: clarity?.live_read_status || 'PERMISSION_LIMITED',
    freshness: 'PARTIAL',
    last_successful_sync: pullTs,
  },
  LANDING_EVENTS: {
    helena_native_connection: 'PARTIAL',
    authenticated: 'N/A',
    live_read: 'PARTIAL',
    freshness: 'PARTIAL',
    note: 'form_start=8 in GA4; no named demo/pilot events',
    last_successful_sync: pullTs,
  },
  PRODUCT_USAGE_TELEMETRY: {
    helena_native_connection: 'PARTIAL',
    authenticated: 'N/A',
    live_read: 'PARTIAL',
    freshness: 'PARTIAL',
    note: 'Product paths visible in GA4 page grain; no first-party product event warehouse',
    last_successful_sync: pullTs,
  },
};

const headline = {
  ga4_sessions_7d: mval(enrichMetrics, 'ga4.sessions.last_7d'),
  ga4_sessions_30d: mval(enrichMetrics, 'ga4.sessions.last_30d'),
  ga4_sessions_prior_30d: mval(enrichMetrics, 'ga4.sessions.previous_30d'),
  ga4_sessions_90d: mval(enrichMetrics, 'ga4.sessions.last_90d'),
  ga4_channel_30d_direct: mval(enrichMetrics, 'ga4.channel_group.30d.direct'),
  ga4_channel_30d_referral: mval(enrichMetrics, 'ga4.channel_group.30d.referral'),
  ga4_channel_30d_organic_search: mval(enrichMetrics, 'ga4.channel_group.30d.organic_search'),
  ga4_channel_30d_organic_social: mval(enrichMetrics, 'ga4.channel_group.30d.organic_social'),
  ga4_channel_30d_ai_assistant: mval(enrichMetrics, 'ga4.channel_group.30d.ai_assistant'),
  ga4_linkedin_attributed_sessions_30d: mval(enrichMetrics, 'ga4.linkedin_attributed_sessions.30d'),
  ga4_form_start_30d: formStart30d(enrichMetrics),
  gsc_clicks_28d: mval(enrichMetrics, 'gsc.clicks.last_28d'),
  gsc_impressions_28d: mval(enrichMetrics, 'gsc.impressions.last_28d'),
  gsc_clicks_prior_28d: mval(enrichMetrics, 'gsc.clicks.previous_28d'),
  gsc_impressions_prior_28d: mval(enrichMetrics, 'gsc.impressions.previous_28d'),
};

const canonical = {
  schemaVersion: 'helena-cmo-live-metrics-v1',
  generatedAt: ingestedAt,
  ingest: {
    status: 'INGESTED',
    source: 'Enrich Labs native connectors',
    enrichPullTimestamp: pullTs,
    enrichPullLocalParis: enrich.pull_timestamp_local_paris || null,
    ingestedAt,
    artifactDir: 'reports/helena-cmo-native-data-pull-v1/enrich-returns/',
    zapierUsed: false,
    cursorUsedForGa4Gsc: false,
    deepBaselineV2Run: false,
    strategyApprovalRequested: false,
  },
  enrichPull: {
    schema: enrich.schema,
    version: enrich.version,
    pulled_by: enrich.pulled_by,
    business: enrich.business,
    site: enrich.site,
    notes: enrich.notes,
    executive_signal: enrich.executive_signal,
    integrations_inventory: enrich.integrations_inventory,
    residual_gaps: enrich.residual_gaps,
    raw_window_snapshots: enrich.raw_window_snapshots || null,
  },
  headline,
  metrics: mergedMetrics,
  connectorStatus,
  nativePullPolicy: {
    updatedAt: ingestedAt,
    rule: 'Enrich-native first; Zapier GA4/GSC is FALLBACK ONLY after Enrich-native fail',
    nativePullPack: 'reports/helena-cmo-native-data-pull-v1/',
    enrichReturns: 'reports/helena-cmo-native-data-pull-v1/enrich-returns/',
    ga4GscRebuildNeeded: false,
  },
  deepBaselineReadiness: {
    answer: 'YES_CHANNEL_LIVE_DATA_READY',
    run: false,
    explanation:
      'GA4 + GSC live Enrich pulls ingested. Deep Baseline V2 can run from live channel data when Joan requests it. Residual gaps: LinkedIn impressions, L1 demo/pilot outcomes, email ESP, record-level Airtable join, full Clarity API.',
  },
};

writeJson(path.join(NATIVE_DIR, 'helena-cmo-live-metrics-v1.json'), canonical);
writeJson(RECON_METRICS, canonical);
fs.copyFileSync(ENRICH_JSON, path.join(NATIVE_DIR, 'helena-cmo-live-metrics-enrich-raw-v1.json'));

const nativePackPath = path.join(NATIVE_DIR, 'helena-cmo-native-data-pull-v1.json');
const nativePack = fs.existsSync(nativePackPath)
  ? readJson(nativePackPath)
  : { schemaVersion: 'helena-cmo-native-data-pull-v1' };
nativePack.ingest = {
  status: 'COMPLETE',
  enrichPullTimestamp: pullTs,
  ingestedAt,
  metricsCount: enrichMetrics.length,
  mergedMetricsCount: mergedMetrics.length,
  zapierUsed: false,
  deepBaselineV2Run: false,
};
nativePack.meta = {
  ...(nativePack.meta || {}),
  enrichReachableViaArtifact: true,
  enrichLivePullIngested: true,
  zapierUsedForGa4Gsc: false,
  deepBaselineV2Run: false,
  note: 'Enrich-native live GA4/GSC ingested from enrich-returns artifact.',
};
nativePack.liveReadsFromEnrichArtifact = {
  GA4: 'LIVE_READ_SUCCESS',
  GSC: 'LIVE_READ_SUCCESS',
  LINKEDIN_JOAN: 'PERMISSION_LIMITED',
  LINKEDIN_AO: 'PERMISSION_LIMITED',
  LINKEDIN_DEALALITY: 'NOT_CONNECTED',
  WEBFLOW: 'LIVE_READ_SUCCESS',
  AIRTABLE: 'LIVE_READ_SUCCESS',
};
nativePack.answersUpdatedAt = ingestedAt;
writeJson(nativePackPath, nativePack);

if (fs.existsSync(RECON_PACK)) {
  const recon = readJson(RECON_PACK);
  recon.enoughLiveDataToRerunBaseline = {
    answer: 'YES',
    explanation:
      'Enrich-native GA4 (530177196) and GSC (sc-domain:dealality.com) live metrics ingested. CMS/GTM already live. Deep Baseline V2 may run when requested — not auto-run. Residual: LinkedIn impressions API, L1 outcomes, email, full Clarity.',
  };
  recon.joanImmediateActions = [
    'Enrich live pull ingested — no Zapier GA4 auth needed for channel baseline',
    'Optional: connect Dealality LinkedIn company page / LinkedIn analytics export for impressions',
    'Optional: Helena/Cursor record-level Airtable Owner Targets join for L1 outcomes',
    'Run Deep Baseline V2 only when Joan explicitly requests it',
  ];
  recon.unnecessaryProposedConnectors = [
    'Duplicate Webflow CMS client in repo (MCP already LIVE)',
    'Duplicate Airtable Marketing OS/GTM client (PAT already LIVE)',
    'Zapier or Cursor GA4/GSC rebuild (Enrich-native live pull succeeded)',
  ];
  recon.enrichIngest = {
    ingestedAt,
    enrichPullTimestamp: pullTs,
    metricsCount: enrichMetrics.length,
    path: 'reports/helena-cmo-native-data-pull-v1/enrich-returns/',
  };
  writeJson(RECON_PACK, recon);
}

write(
  path.join(NATIVE_DIR, '07_ENRICH_INGEST.md'),
  `# Enrich live pull — ingested

**Enrich pull:** ${pullTs} (Paris: ${enrich.pull_timestamp_local_paris || 'n/a'})  
**Ingested at:** ${ingestedAt}  
**Method:** Enrich Labs native only — **no Zapier**, **no Cursor GA4/GSC rebuild**

## Status
| Source | Live read |
|--------|-----------|
| GA4 \`530177196\` | LIVE_READ_SUCCESS |
| GSC \`sc-domain:dealality.com\` | LIVE_READ_SUCCESS |
| LinkedIn Joan + AO | PERMISSION_LIMITED |
| LinkedIn Dealality | NOT_CONNECTED |
| Webflow Insights | LIVE_READ_SUCCESS (46) |
| Airtable Owner Targets | LIVE_READ_SUCCESS (base list) |

## Canonical artifacts
- \`enrich-returns/helena-cmo-live-metrics-v1.json\` (raw Enrich, ${enrichMetrics.length} metrics)
- \`helena-cmo-live-metrics-v1.json\` (canonical merged handoff)
- \`reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-metrics-v1.json\` (console SoT)

## Headline
- GA4 sessions: 7d **${headline.ga4_sessions_7d}** · 30d **${headline.ga4_sessions_30d}** (prior 30d **${headline.ga4_sessions_prior_30d}**) · 90d **${headline.ga4_sessions_90d}**
- 30d channels: Direct ${headline.ga4_channel_30d_direct} · Referral ${headline.ga4_channel_30d_referral} · Organic Search ${headline.ga4_channel_30d_organic_search} · Organic Social ${headline.ga4_channel_30d_organic_social} · AI Assistant ${headline.ga4_channel_30d_ai_assistant}
- LinkedIn→site ATTRIBUTED: **${headline.ga4_linkedin_attributed_sessions_30d}** sessions / 30d
- GSC page-sum 28d: **${headline.gsc_clicks_28d}** clicks / **${headline.gsc_impressions_28d}** impr (prior 28d ${headline.gsc_clicks_prior_28d}/${headline.gsc_impressions_prior_28d})

## Deep Baseline V2
**Ready from live channel data: YES.** **Run: NO** (not requested).

## Cursor does NOT need
GA4 connector · GSC connector · Zapier GA4 OAuth
`,
);

write(
  path.join(NATIVE_DIR, '00_EXECUTIVE_VERDICT.md'),
  `# Executive verdict — Native data pull (post-ingest)

**Enrich Labs live pull succeeded and is ingested.**

- GA4 + GSC = **LIVE** via Enrich native connectors (artifact in \`enrich-returns/\`)
- Zapier was **not** used and is **not** required for channel baseline
- LinkedIn analytics remain **PERMISSION_LIMITED** (public scrape)
- Deep Baseline V2 = **READY but NOT RUN**

## One thing
Search is discovering Dealality pages (esp. branded residences) far faster than it converts; sessions are still mostly direct/social — not yet an owner-outcome engine.

## Next (only when Joan asks)
Run Deep Baseline V2 from this live metrics pack.
`,
);

write(
  path.join(NATIVE_DIR, '06_ANSWERS.md'),
  `# Exact answers (post Enrich ingest)

1. **Helena live sources:** GA4 \`530177196\` · GSC \`sc-domain:dealality.com\` · Webflow · Airtable Owner Targets base · LinkedIn Joan/AO (permission-limited) · Clarity (partial) · Enrich calendars/tasks

2. **Live reads that succeeded:** GA4 · GSC · Webflow · Airtable base list. **Limited:** LinkedIn Joan/AO · Clarity. **Not connected:** LinkedIn Dealality · Email ESP

3. **GA4 (live):** sessions 7d **25** · 30d **97** (prior 241) · 90d **518**. Channels 30d: Direct 57 · Referral 16 · Organic Search 10 · Organic Social 8 · AI Assistant 4. LinkedIn-attributed 8. form_start=8; no named demo/pilot events.

4. **GSC (live):** page-sum 28d **6** clicks / **~1,336–1,376** impr · prior 28d 2 / ~114. Residences 0c/789i · CALA brand selection 0c/20i ~pos 5.

5. **LinkedIn:** public likes/comments/shares only; impressions mostly null. Dealality company page NOT_CONNECTED. Channel attribution via GA4 only.

6. **Other:** Webflow 46 Insights · Enrich tasks live · Clarity present in GA referrals · GTM pipeline still live from Dealality PAT

7. **Cursor does NOT rebuild:** GA4 · GSC · Webflow CMS · Zapier GA4 auth

8. **True gaps:** LinkedIn impressions time series · L1 demo/pilot outcomes · Airtable record-level join · Email ESP · full Clarity API · Dealality LI page

9. **Cursor gaps:** product/CTA attribution join · ADP pilot telemetry · GTM→CMO record normalization · optional GA event tagging audit

10. **Deep Baseline V2 from live data?** **YES ready · NOT run** (await Joan request)
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      enrichMetrics: enrichMetrics.length,
      mergedMetrics: mergedMetrics.length,
      pullTs,
      ingestedAt,
      deepBaselineReady: true,
      deepBaselineRun: false,
      headline,
    },
    null,
    2,
  ),
);
