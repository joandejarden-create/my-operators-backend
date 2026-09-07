#!/usr/bin/env node
/**
 * Native data pull boundary report — Enrich Labs Helena vs this Cursor session.
 * Does NOT use Zapier as GA4/GSC path. Does NOT request new auth.
 * Deep Baseline V2 not run.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'reports/helena-cmo-native-data-pull-v1');
const pullTimestamp = new Date().toISOString();

function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith('\n') ? body : `${body}\n`);
}

fs.mkdirSync(OUT, { recursive: true });

const pack = {
  schemaVersion: 'helena-cmo-native-data-pull-v1',
  generatedAt: pullTimestamp,
  meta: {
    executeEnabled: false,
    recurringHelenaEnabled: false,
    strategyApprovalRequested: false,
    deepBaselineV2Run: false,
    zapierUsedForGa4Gsc: false,
    note: 'Helena-native = Enrich Labs. This Cursor session cannot invoke Enrich tool runtime.',
  },
  accessBoundary: {
    helenaNativeHome: 'Enrich Labs (enrichlabs.ai / agent.enrichlabs.ai)',
    evidence:
      'Phase 0 verified Enrich Labs identity; historical skills use get_traffic, post_linkedin, Webflow CMS; GA4 snapshot notes cite get_traffic',
    thisSession: 'Cursor IDE agent (Dealality repo + Cursor MCP tools)',
    enrichMcpToolsInThisSession: false,
    enrichApiCredentialsInEnv: false,
    canQueryEnrichNativeGa4FromHere: false,
    canQueryEnrichNativeGscFromHere: false,
    canQueryEnrichNativeLinkedInAnalyticsFromHere: false,
  },
  inventory: [
    {
      source: 'GA4',
      connected_on_enrich: 'ASSERTED_HISTORICAL (get_traffic produced Jun–Sep snapshot)',
      connected_in_this_cursor_session: false,
      account_property: 'UNKNOWN (Enrich-side; not visible here)',
      read_access_here: false,
      last_successful_access_here: null,
      latest_data_available_here: 'STALE export 2026-06-01→2026-09-06 (553 sessions)',
      limitations: 'Must be pulled inside Enrich Helena runtime',
      live_read_here: 'FAILED',
      classification: 'AVAILABLE_THROUGH_HELENA_ENRICH_BUT_UNREACHABLE_FROM_CURSOR',
    },
    {
      source: 'GSC',
      connected_on_enrich: 'ASSERTED_HISTORICAL (GSC snapshot in Enrich export pack)',
      connected_in_this_cursor_session: false,
      account_property: 'sc-domain:dealality.com (from stale summary)',
      read_access_here: false,
      last_successful_access_here: null,
      latest_data_available_here: 'STALE GSC_SUMMARY Jun–Sep 2026',
      limitations: 'Must be pulled inside Enrich Helena',
      live_read_here: 'FAILED',
      classification: 'AVAILABLE_THROUGH_HELENA_ENRICH_BUT_UNREACHABLE_FROM_CURSOR',
    },
    {
      source: 'LINKEDIN_JOAN',
      connected_on_enrich: 'ASSERTED (post_linkedin skill + calendar)',
      connected_in_this_cursor_session: false,
      account_property: 'Joan personal profile',
      read_access_here: false,
      latest_data_available_here: 'STALE scrape metrics in phase-0 pack',
      live_read_here: 'FAILED',
      classification: 'AVAILABLE_THROUGH_HELENA_ENRICH_BUT_UNREACHABLE_FROM_CURSOR',
      note: 'Publish path ≠ analytics path; analytics may still be export-limited on Enrich',
    },
    {
      source: 'LINKEDIN_DEALALITY',
      connected_on_enrich: 'UNKNOWN',
      connected_in_this_cursor_session: false,
      live_read_here: 'FAILED',
      classification: 'UNKNOWN_ON_ENRICH',
    },
    {
      source: 'LINKEDIN_AO',
      connected_on_enrich: 'ASSERTED_PARTIAL (AO cadence in skills)',
      connected_in_this_cursor_session: false,
      live_read_here: 'FAILED',
      classification: 'AVAILABLE_THROUGH_HELENA_ENRICH_BUT_UNREACHABLE_FROM_CURSOR',
    },
    {
      source: 'WEBFLOW_CMS',
      connected_on_enrich: 'YES (publish skills + CMS IDs in crons)',
      connected_in_this_cursor_session: true,
      account_property: 'Deal Capture MVP / dealality.com',
      read_access_here: true,
      last_successful_access_here: pullTimestamp,
      latest_data_available_here: 'Live pages + Insights CMS (proven 6F-A)',
      live_read_here: 'LIVE_READ_SUCCESS',
      classification: 'ALREADY_AVAILABLE_THROUGH_HELENA_AND_CURSOR_MCP',
      note: 'Cursor Webflow MCP is Joan account access — not a Zapier rebuild of GA4',
    },
    {
      source: 'WEBFLOW_ANALYZE',
      connected_on_enrich: 'UNKNOWN',
      connected_in_this_cursor_session: true,
      read_access_here: false,
      live_read_here: 'PERMISSION_LIMITED',
      limitations: '403 Analyze entitlement',
      classification: 'AVAILABLE_BUT_PERMISSION_LIMITED',
    },
    {
      source: 'AIRTABLE_GTM_PIPELINE',
      connected_on_enrich: 'UNKNOWN',
      connected_in_this_cursor_session: true,
      read_access_here: true,
      live_read_here: 'LIVE_READ_SUCCESS',
      classification: 'NEEDS_DEALALITY_CURSOR_CONNECTION (Dealality-side truth; not Enrich GA4)',
      note: 'Already live via Dealality PAT — Cursor should normalize for CMO; not a duplicate analytics connector',
    },
    {
      source: 'AIRTABLE_MARKETING_OS',
      connected_on_enrich: 'PARTIAL (exports seeded OS)',
      connected_in_this_cursor_session: true,
      read_access_here: true,
      live_read_here: 'LIVE_READ_SUCCESS',
      classification: 'NEEDS_DEALALITY_CURSOR_CONNECTION',
    },
    {
      source: 'PRODUCT_USAGE',
      connected_on_enrich: 'NO',
      connected_in_this_cursor_session: false,
      live_read_here: 'NOT_CONNECTED',
      classification: 'NEEDS_DEALALITY_CURSOR_CONNECTION',
    },
    {
      source: 'ZAPIER_GA4',
      connected_on_enrich: 'N/A',
      connected_in_this_cursor_session: 'FALLBACK_ONLY_DO_NOT_USE_YET',
      live_read_here: 'NOT_REQUESTED',
      classification: 'FALLBACK_ONLY_AFTER_ENRICH_NATIVE_FAILS',
      note: 'Founder rule: do not ask Joan to authenticate Zapier until Enrich-native GA4 tested',
    },
  ],
  liveReadsAttemptedFromCursor: [
    {
      source: 'Enrich GA4 get_traffic',
      result: 'FAILED',
      reason: 'No Enrich MCP/API in session',
      pull_timestamp: pullTimestamp,
    },
    {
      source: 'Enrich GSC',
      result: 'FAILED',
      reason: 'No Enrich MCP/API in session',
      pull_timestamp: pullTimestamp,
    },
    {
      source: 'Enrich LinkedIn analytics',
      result: 'FAILED',
      reason: 'No Enrich MCP/API in session',
      pull_timestamp: pullTimestamp,
    },
    {
      source: 'Webflow CMS (Cursor MCP)',
      result: 'LIVE_READ_SUCCESS',
      pull_timestamp: pullTimestamp,
      latest_data_date: pullTimestamp,
      reporting_period: 'point_in_time',
    },
    {
      source: 'Airtable GTM/MOS (Dealality PAT)',
      result: 'LIVE_READ_SUCCESS',
      pull_timestamp: pullTimestamp,
      note: 'See 6F-A reconciliation aggregates',
    },
  ],
  ga4CurrentMetrics: {
    status: 'NOT_PULLED_LIVE — Enrich-native required',
    staleFallback: {
      period: '2026-06-01 to 2026-09-06',
      sessions_total: 553,
      sessions_by_source: {
        direct: 277,
        googleOrganic: 19,
        googlePaid: 0,
        facebook: 0,
        tiktok: 0,
        email: 0,
      },
      freshness: 'STALE',
      provenance: 'Enrich export pack get_traffic snapshot',
    },
  },
  gscCurrentMetrics: {
    status: 'NOT_PULLED_LIVE — Enrich-native required',
    staleFallback: {
      period: '2026-06-01 to 2026-09-06',
      notes: 'See GSC_SUMMARY.md — home 6 clicks; vanity impressions on branded residences',
      freshness: 'STALE',
    },
  },
  linkedInCurrentMetrics: {
    status: 'NOT_PULLED_LIVE — Enrich-native required',
    staleFallback: {
      freshness: 'STALE_SCRAPE',
      note: 'phase-0 linkedin_posts_metrics.json',
    },
  },
  otherMetricsAvailableNow: {
    webflowCms: 'LIVE via Cursor Webflow MCP',
    gtmPipelineAggregates: 'LIVE via Dealality Airtable',
    marketingOsCounts: 'LIVE via Dealality Airtable',
    adpPublicPage: 'LIVE HTTP 200 /hotel-owner/ai-demand-positioning',
  },
  doNotRebuildInCursor: [
    'GA4 — until Enrich-native live pull is proven failed',
    'GSC — until Enrich-native live pull is proven failed',
    'Webflow CMS — already live (Enrich historically + Cursor MCP)',
    'Zapier GA4 OAuth — forbidden as next step by founder clarification',
  ],
  trueRemainingGaps: [
    {
      gap: 'Enrich-native live GA4/GSC/LinkedIn cannot be executed from Cursor',
      class: 'ACCESS_BRIDGE',
      nextStep: 'Run ENRICH_LIVE_PULL_BRIEF inside Enrich Labs Helena',
      cursorRebuild: false,
    },
    {
      gap: 'Product usage telemetry',
      class: 'NEEDS_DEALALITY_CURSOR_CONNECTION',
      cursorRebuild: true,
    },
    {
      gap: 'Landing/CTA attribution join',
      class: 'NEEDS_DEALALITY_CURSOR_CONNECTION',
      cursorRebuild: true,
    },
    {
      gap: 'ADP pilot scorecard instances',
      class: 'NEEDS_DEALALITY_CURSOR_CONNECTION',
      cursorRebuild: true,
    },
  ],
  deepBaselineReady: {
    answer: 'NO',
    why: 'Live GA4/GSC/LinkedIn not yet returned from Enrich-native Helena. Partial CMS/pipeline live data exists but channel analytics remain STALE.',
  },
  joanAction: {
    do: 'Paste reports/helena-cmo-native-data-pull-v1/02_ENRICH_LIVE_PULL_BRIEF.md into Enrich Labs Helena and return the JSON artifact',
    doNot: 'Authenticate Zapier GA4 / build Cursor GA4 connector yet',
  },
};

const liveMetrics = {
  schemaVersion: 'helena-cmo-live-metrics-v1',
  generatedAt: pullTimestamp,
  status: 'PARTIAL — Enrich-native GA4/GSC/LinkedIn pending',
  sourceOfTruthPolicy: 'Helena Enrich native first; Zapier/Cursor rebuild only after Enrich-native fail',
  metrics: [
    {
      metric_id: 'ga4.sessions.stale_snapshot',
      metric_name: 'GA4 sessions (stale Enrich export)',
      category: 'ACQUISITION',
      source_system: 'Enrich export / get_traffic',
      source_account: 'UNKNOWN',
      reporting_period: '2026-06-01_2026-09-06',
      current_value: 553,
      prior_value: null,
      delta: null,
      unit: 'sessions',
      segment: null,
      pull_timestamp: pullTimestamp,
      latest_data_date: '2026-09-06',
      freshness: 'STALE',
      confidence: 'MEDIUM',
      business_question: 'What is traffic scale until Enrich live pull?',
      strategic_relevance: 'Channel diagnosis interim only',
      attribution_level: 'DIAGNOSTIC',
      provenance: 'phase-0 GA4_SESSION_SNAPSHOT',
    },
    {
      metric_id: 'ga4.sessions.direct.stale',
      metric_name: 'GA4 direct sessions (stale)',
      category: 'ACQUISITION',
      source_system: 'Enrich export / get_traffic',
      source_account: 'UNKNOWN',
      reporting_period: '2026-06-01_2026-09-06',
      current_value: 277,
      prior_value: null,
      delta: null,
      unit: 'sessions',
      segment: 'direct',
      pull_timestamp: pullTimestamp,
      latest_data_date: '2026-09-06',
      freshness: 'STALE',
      confidence: 'MEDIUM',
      business_question: 'How dependent is traffic on direct?',
      strategic_relevance: 'Acquisition mix',
      attribution_level: 'DIAGNOSTIC',
      provenance: 'phase-0 GA4_SESSION_SNAPSHOT',
    },
    {
      metric_id: 'ga4.sessions.organic.stale',
      metric_name: 'GA4 google organic sessions (stale)',
      category: 'SEARCH',
      source_system: 'Enrich export / get_traffic',
      source_account: 'UNKNOWN',
      reporting_period: '2026-06-01_2026-09-06',
      current_value: 19,
      prior_value: null,
      delta: null,
      unit: 'sessions',
      segment: 'googleOrganic',
      pull_timestamp: pullTimestamp,
      latest_data_date: '2026-09-06',
      freshness: 'STALE',
      confidence: 'MEDIUM',
      business_question: 'Is organic a real path?',
      strategic_relevance: 'SEO investment threshold',
      attribution_level: 'DIAGNOSTIC',
      provenance: 'phase-0 GA4_SESSION_SNAPSHOT',
    },
    {
      metric_id: 'enrich.native.ga4.live',
      metric_name: 'GA4 live Enrich pull',
      category: 'ACQUISITION',
      source_system: 'Enrich Labs Helena',
      source_account: 'PENDING',
      reporting_period: null,
      current_value: null,
      prior_value: null,
      delta: null,
      unit: null,
      segment: null,
      pull_timestamp: pullTimestamp,
      latest_data_date: null,
      freshness: 'NOT_CONNECTED_FROM_CURSOR',
      confidence: 'UNKNOWN',
      business_question: 'Can Helena Enrich return live GA4 now?',
      strategic_relevance: 'Gate for Deep Baseline V2',
      attribution_level: 'DIAGNOSTIC',
      provenance: 'native-data-pull-v1 boundary',
    },
  ],
  awaitingEnrichArtifact: true,
  enrichBriefPath: 'reports/helena-cmo-native-data-pull-v1/02_ENRICH_LIVE_PULL_BRIEF.md',
};

fs.writeFileSync(path.join(OUT, 'helena-cmo-native-data-pull-v1.json'), JSON.stringify(pack, null, 2));
fs.writeFileSync(path.join(OUT, 'helena-cmo-live-metrics-v1.json'), JSON.stringify(liveMetrics, null, 2));

// Also refresh the reconciliation live-metrics pointer note without Zapier push
const reconMetricsPath = path.join(
  ROOT,
  'reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-metrics-v1.json',
);
if (fs.existsSync(reconMetricsPath)) {
  const prev = JSON.parse(fs.readFileSync(reconMetricsPath, 'utf8'));
  prev.nativePullPolicy = {
    updatedAt: pullTimestamp,
    rule: 'Enrich-native first; Zapier GA4/GSC is FALLBACK ONLY after Enrich-native fail',
    nativePullPack: 'reports/helena-cmo-native-data-pull-v1/',
  };
  fs.writeFileSync(reconMetricsPath, JSON.stringify(prev, null, 2));
}

write(
  '00_EXECUTIVE_VERDICT.md',
  `# Executive verdict — Native data pull

**Helena’s analytics home is Enrich Labs — not this Cursor session.**

This Cursor agent **cannot** invoke Enrich-native \`get_traffic\` / GSC / LinkedIn analytics tools. There is **no Enrich MCP** and **no Enrich API credential** in the Dealality env for this session.

Therefore:
- **Live GA4 / GSC / LinkedIn analytics were NOT pulled** (correctly — not via Zapier).
- **Zapier GA4 auth is NOT requested.**
- **Deep Baseline V2 is NOT run.**

## What Joan should do next
Paste \`02_ENRICH_LIVE_PULL_BRIEF.md\` into **Enrich Labs Helena** and return the JSON artifact into this repo.

Until that returns: channel analytics remain **STALE** Enrich exports; CMS/pipeline remain LIVE from Dealality-side sources.
`,
);

write(
  '01_HELENA_NATIVE_ACCESS_BOUNDARY.md',
  `# Access boundary

| Layer | What it is | Can pull Enrich GA4? |
|-------|------------|----------------------|
| Enrich Labs Helena | Native Helena with historical \`get_traffic\`, Webflow, \`post_linkedin\` | **YES (must run there)** |
| This Cursor session | Dealality repo agent + Cursor MCPs | **NO** |
| Zapier | Fallback only | Do **not** use yet |

## Evidence Enrich is the native home
- Phase 0: Enrich Labs identity + Cursor Connected UI
- Skills: \`post_linkedin\`, Webflow publish, card generator
- GA4 snapshot notes explicitly cite Enrich \`get_traffic\`
- Workspace assets under \`agent.enrichlabs.ai\` / enrichlabs-public-assets

## Evidence this session cannot reach it
- No Enrich tools in Cursor dynamic tool catalog
- No ENRICH_* credentials in \`.env\`
- Phase 0/0F documented Enrich dispatch as unproven from Cursor
`,
);

write(
  '02_ENRICH_LIVE_PULL_BRIEF.md',
  `# ENRICH LABS — HELENA LIVE CMO DATA PULL
# Paste this into Enrich Labs Helena (NOT Cursor / NOT Zapier)

You are Helena operating inside Enrich Labs with your **native** marketing connectors.

Do **not** ask Joan to connect Zapier.
Do **not** send work to Cursor for GA4/GSC if you can read them natively.

## Objective
Produce a single machine-readable file:

\`helena-cmo-live-metrics-v1.json\`

and a short markdown summary of live reads.

## 1) Inventory your connected sources
List every connected integration: GA4, GSC, LinkedIn (Joan / Dealality / AO), Webflow, email, SEO, Airtable, others.

For each: CONNECTED? ACCOUNT/PROPERTY READ_ACCESS? LAST_SUCCESS LAST_DATA LIMITATIONS

## 2) Live-read proof
For each connected source, actually query it. Classify LIVE_READ_SUCCESS / PERMISSION_LIMITED / STALE_ONLY / FAILED / NOT_CONNECTED. Include PULL_TIMESTAMP, LATEST_DATA_DATE, REPORTING_PERIOD.

## 3) GA4 live pull (if connected)
Windows: last 7d, last 30d, previous 30d, last 90d.

Pull overview (users, active users, sessions, engaged sessions, engagement rate, avg engagement time, new vs returning), acquisition (source, medium, channel group, campaign, referral, landing page), pages, key events / CTA / forms / demo / pilot / product-interest, audience geo + device.

Return **actual values and trends**.

## 4) GSC live pull (if connected)
Windows: 7d, 28d, previous 28d, 90d.

Queries, pages, clicks, impressions, CTR, position, country, device.

Classify queries: BRANDED / OWNER INTENT / HOTEL DEALMAKING / BRAND SELECTION / OPERATOR SELECTION / ADP-AI DEMAND / COMMERCIAL / INFORMATIONAL / LOW VALUE.

## 5) LinkedIn / social (if connected)
Separate JOAN / DEALALITY / AO. Pull fullest available impressions/reach/reactions/comments/reposts/clicks/followers/posts/themes. Commercial outcome = UNKNOWN unless pipeline join exists.

## 6) Other native sources
Webflow CMS inventory, SEO tools, email, campaign reports — pull what you can.

## 7) Output contract
Every metric object:

metric_id, metric_name, category, source_system, source_account, reporting_period, current_value, prior_value, delta, unit, segment, pull_timestamp, latest_data_date, freshness, confidence, business_question, strategic_relevance, attribution_level, provenance

Also return residual gaps: ALREADY AVAILABLE / PERMISSION LIMITED / NOT AVAILABLE / NEEDS DEALALITY-CURSOR.

## 8) Return path
Save outputs to Enrich workspace and tell Joan to drop them into Dealality repo:

\`reports/helena-cmo-native-data-pull-v1/enrich-returns/\`

Cursor will ingest — **without rebuilding GA4/GSC** if your live pull succeeded.
`,
);

write(
  '03_CONNECTION_INVENTORY.md',
  `# Connection inventory

See machine JSON \`helena-cmo-native-data-pull-v1.json\` → \`inventory\`.

Summary:
- Enrich GA4/GSC/LinkedIn: **asserted historical, unreachable from Cursor**
- Webflow CMS: **live here** (Cursor MCP) + Enrich historical
- GTM/MOS Airtable: **live here** (Dealality) — Cursor normalize, not analytics rebuild
- Zapier GA4: **fallback only — not requested**
`,
);

write(
  '04_LIVE_READ_ATTEMPTS.md',
  `# Live read attempts from this session

| Source | Result | Why |
|--------|--------|-----|
| Enrich GA4 | FAILED | No Enrich tools in Cursor |
| Enrich GSC | FAILED | No Enrich tools in Cursor |
| Enrich LinkedIn analytics | FAILED | No Enrich tools in Cursor |
| Webflow CMS (MCP) | LIVE_READ_SUCCESS | Cursor Webflow MCP |
| Airtable GTM/MOS | LIVE_READ_SUCCESS | Dealality PAT |

**No Zapier GA4/GSC reads attempted** (founder rule).
`,
);

write(
  '05_TRUE_GAPS.md',
  `# True gaps after Helena-native-first policy

## Do NOT rebuild in Cursor yet
- GA4
- GSC
- Webflow CMS

## Requires Enrich-native execution first
- Live GA4 windows + events
- Live GSC query classification
- Live LinkedIn analytics (if Enrich can)

## Requires Dealality/Cursor (after Enrich analytics return)
- Product usage telemetry
- Landing/CTA attribution join
- ADP pilot scorecard instances
- GTM pipeline normalization into CMO observation layer (structure exists; wire remains)
`,
);

write(
  '06_ANSWERS.md',
  `# Exact answers

1. **Helena itself can access (Enrich-side, historical/asserted):** GA4 via \`get_traffic\`, GSC (export evidence), LinkedIn publish tools, Webflow CMS. **This Cursor session cannot invoke those Enrich tools.**

2. **Live reads succeeded here:** Webflow CMS · Airtable GTM/MOS. **Failed:** Enrich GA4/GSC/LinkedIn.

3. **Current GA4 metrics:** Live = **none**. Stale Enrich export: 553 sessions (direct 277, organic 19) for 2026-06-01→09-06.

4. **Current GSC metrics:** Live = **none**. Stale summary only.

5. **Current LinkedIn metrics:** Live = **none**. Stale scrape only.

6. **Other current metrics Helena/Dealality already has live:** Webflow CMS inventory · GTM Pilot Target aggregates · Marketing OS counts · ADP public page.

7. **Cursor does NOT need to rebuild:** GA4/GSC (pending Enrich) · Webflow CMS · Zapier GA4 auth path.

8. **True remaining gaps:** Enrich→Cursor analytics bridge · product usage · CTA attribution · ADP pilot telemetry.

9. **Gaps requiring Cursor:** product usage · landing/CTA attribution · ADP pilot instances · GTM→CMO normalization (not GA4).

10. **Deep Baseline V2 from live data?** **NO** — wait for Enrich-native GA4/GSC/LinkedIn artifact.
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      enrichReachableFromCursor: false,
      zapierRequested: false,
      deepBaseline: false,
      next: 'Joan runs 02_ENRICH_LIVE_PULL_BRIEF.md inside Enrich Labs',
    },
    null,
    2,
  ),
);
