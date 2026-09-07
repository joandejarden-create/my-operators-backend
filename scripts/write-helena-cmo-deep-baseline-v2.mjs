#!/usr/bin/env node
/**
 * Helena CMO Deep Baseline V2 — supporting pack + machine JSON.
 * Authoritative founder narrative: 00_FOUNDER_CMO_DEEP_REVIEW.md (pre-written).
 * No Zapier. No strategy lock. Does not merge/deploy.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'reports/helena-cmo-deep-baseline-v2');
const LIVE = path.join(
  ROOT,
  'reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-metrics-v1.json',
);
const ENRICH = path.join(
  ROOT,
  'reports/helena-cmo-native-data-pull-v1/enrich-returns/helena-cmo-live-metrics-v1.json',
);
const generatedAt = new Date().toISOString();

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}
function w(name, body) {
  const text = body.endsWith('\n') ? body : `${body}\n`;
  fs.writeFileSync(path.join(OUT, name), text);
}

fs.mkdirSync(OUT, { recursive: true });
const live = readJson(LIVE);
const enrich = readJson(ENRICH);
const h = live.headline || {};

function scoreBlock(o) {
  return `### ${o.name} — **${o.score}/10** — ${o.verdict}

**Evidence:** ${o.evidence}
**Context:** ${o.context}
**What is good:** ${o.good}
**What is bad:** ${o.bad}
**So what:** ${o.soWhat}
**Business consequence:** ${o.consequence}
**Opportunity:** ${o.opportunity}
**Priority:** ${o.priority}
**Why that priority:** ${o.whyPriority}
**What would change the score:** ${o.changeScore}
`;
}

const scores = {
  overall: {
    name: 'Overall marketing health',
    score: 5.0,
    verdict: 'Foundations without closed commercial loop',
    evidence: 'GA4/GSC LIVE; sessions 97 vs prior 241; form_start=8; Pilot Connected=58; GSC 6c/1376i',
    context: 'Specialist B2B owner platform at relationship scale',
    good: 'Live measurement, named list, ADP surface, dealmaking spine',
    bad: 'Outcome-blind funnel; SEO pollution; activation lag',
    soWhat: 'Activity can continue without paid learning',
    consequence: 'Slow truth on ICP/price/offer',
    opportunity: '90-day warm ADP learning cycle',
    priority: 'P0',
    whyPriority: 'Company needs falsifiable GTM',
    changeScore: 'Paid pilots + instrumented funnel + cleaner search mix',
  },
  website: {
    name: 'Website',
    score: 4.5,
    verdict: 'Reachable products; weak commercial narrative + measurement',
    evidence: 'Home 55s bounce0.51; ADP 7s bounce0.143; form_start=8; no demo events',
    context: 'Public Webflow site + owner product paths',
    good: 'ADP live; Insights exist; product paths appear in GA4',
    bad: 'Homepage not outcome-led; CTA chain incomplete',
    soWhat: 'Rare visitors are under-converted',
    consequence: 'Warm traffic wasted; cold traffic confused',
    opportunity: 'ADP CTA path from dealmaking surfaces',
    priority: 'P1',
    whyPriority: 'Conversion surface for B2',
    changeScore: 'Clear offer CTA + form_complete + message clarity test',
  },
  seo: {
    name: 'SEO / Search',
    score: 3.5,
    verdict: 'Visibility without commercial click economics',
    evidence: '28d 1376i/6c CTR0.44%; LOW VALUE 264i sample; dealmaking thin',
    context: 'sc-domain:dealality.com live GSC',
    good: 'Some owner topics indexed; soft-brands/key-money get minor clicks',
    bad: 'Residences impression theater; deep positions; CTR collapse',
    soWhat: 'Impression KPIs lie',
    consequence: 'Misallocated editorial time',
    opportunity: 'REFESH to dealmaking/selection',
    priority: 'P1',
    whyPriority: 'Stop false growth signal',
    changeScore: 'CTR↑ on commercial classes; low-value share↓',
  },
  linkedin: {
    name: 'LinkedIn (forced single score)',
    score: 5.0,
    verdict: 'Relationship channel with thin attributed traffic; commercial UNKNOWN',
    evidence: '8 ATTRIBUTED sessions/30d; scrape engagement; impressions null; Dealality page NOT_CONNECTED',
    context: 'Joan+AO publish; Enrich calendar live',
    good: 'Non-zero site path; founder authority vehicle',
    bad: 'No outcome join; no impressions; company page missing',
    soWhat: 'Cannot claim LinkedIn GTM success',
    consequence: 'Risk of vanity publishing',
    opportunity: 'UTM + conversation logging',
    priority: 'P2',
    whyPriority: 'Support warm activation, not primary engine',
    changeScore: 'Logged qualified convos + stable attributed sessions growth',
  },
  pipeline: {
    name: 'Pipeline activation (not database size)',
    score: 5.5,
    verdict: 'Real named list; under-activated vs Connected',
    evidence: 'Pilot 100; Connected58; Call Completed5; Replied1; Draft Needed41; Owner Targets1674=database',
    context: 'GTM Airtable LIVE aggregates',
    good: 'Universe exists; warmth fields exist; outreach statuses exist',
    bad: 'Draft backlog; tiny reply; Strong Warm only4',
    soWhat: 'Database ≠ pipeline',
    consequence: 'Relationship equity idle',
    opportunity: 'Explicit ADP offer to Connected',
    priority: 'P0',
    whyPriority: 'Fastest path to paid learning',
    changeScore: '↑ calls/replies + paid pilots from list',
  },
  measurement: {
    name: 'Measurement / attribution',
    score: 5.0,
    verdict: 'Channel-capable, outcome-blind',
    evidence: 'GA4+GSC LIVE; LinkedIn channel ATTRIBUTED; form_start only; no L1 join',
    context: 'Post Enrich ingest 2026-09-07',
    good: 'Can diagnose sessions/channels/search mix',
    bad: 'Cannot see demo/pilot/revenue',
    soWhat: 'Old 2/10 is obsolete; 10/10 is false',
    consequence: 'Strategy debates remain under-determined on revenue',
    opportunity: 'Event taxonomy + GTM join',
    priority: 'P0',
    whyPriority: 'Makes all other priorities falsifiable',
    changeScore: 'form_complete→pilot events + identity join',
  },
  content: {
    name: 'Content / thought leadership',
    score: 4.5,
    verdict: 'ICP-aligned spine mixed with low-value visibility content',
    evidence: '46 Insights; GA4 insights sessions; GSC residences dominance',
    context: 'Webflow CMS LIVE',
    good: 'Soft brands / key money / HMA language matches ICP',
    bad: 'Residences cluster pollutes search narrative',
    soWhat: 'Content program needs portfolio discipline',
    consequence: 'CMO dashboards misread success',
    opportunity: 'Editorial kill/maintain/invest matrix',
    priority: 'P1',
    whyPriority: 'Supports SEO REFESH + dealmaking identity',
    changeScore: 'Commercial query CTR + assisted conversations',
  },
};

const recommended = {
  id: 'B2_WARM_ADP_ENTRY_MEASURED_DEALMAKING',
  name: 'Warm ADP paid-pilot entry + measured dealmaking identity',
  score: 8.1,
  forDiscussion: true,
  approvalRequested: false,
};

const options = [
  {
    id: 'A_DEALMAKING_SEO_ENGINE',
    name: 'Dealmaking SEO/content as primary GTM',
    score: 4.8,
    who: 'Owners via search',
    problem: 'Brand/operator deal ambiguity',
    offer: 'Insights → conversation',
    urgency: 'Low-medium',
    channel: 'Organic search + content',
    proof: 'Editorial authority',
    economics: 'Slow; unclear CAC',
    advantage: 'Compounding if rankings convert',
    risk: 'Current GSC shows wrong impressions',
    dependencies: 'Brutal content REFESH',
    path90d: 'Demote residences; reinforce dealmaking pages; measure CTR',
    whyWeaker: 'Live data shows impression theater without clicks',
  },
  {
    id: 'B1_ADP_ENTRY_CLASSIC',
    name: 'ADP entry + dealmaking architecture (Phase 6E classic)',
    score: 7.2,
    who: 'Warm owners',
    problem: 'AI demand positioning uncertainty',
    offer: 'ADP paid pilot',
    urgency: 'High for learning',
    channel: 'Warm outreach + site',
    proof: 'Product page + methodology',
    economics: 'Pilot cash + learning',
    advantage: 'Clear SKU',
    risk: 'Uninstrumented funnel; SEO noise ignored',
    dependencies: 'Offer packaging',
    path90d: 'Outreach + page CTA',
    whyWeaker: 'Under-weights measurement + SEO hygiene revealed by live pull',
  },
  {
    id: recommended.id,
    name: recommended.name,
    score: recommended.score,
    recommended: true,
    who: 'Warm / Connected pilot-list owners & advisors',
    problem: 'Need paid diagnostic entry on AI demand + brand/operator context',
    offer: 'Explicit paid ADP pilot',
    urgency: 'Highest near-term learning urgency',
    channel: 'Warm activation primary; LinkedIn secondary; SEO hygiene; site conversion',
    proof: 'ADP engagement micro-signal + dealmaking content + list warmth',
    economics: 'Few paid pilots teach price/ICP faster than content volume',
    advantage: 'Matches live traffic physics (direct/relationship)',
    risk: 'Offer rejection; list quality',
    dependencies: 'Price band; instrumentation; CTA clarity',
    path90d: 'Instrument → activate Connected → ADP CTA → SEO REFESH → log LI outcomes',
    whyWins: 'Aligns assets already owned with falsifiable commercial learning',
  },
  {
    id: 'C_BROAD_LINKEDIN_AWARENESS',
    name: 'Broad LinkedIn awareness primary',
    score: 4.2,
    who: 'Wide hospitality audience',
    problem: 'Low awareness',
    offer: 'Brand presence',
    urgency: 'False urgency',
    channel: 'LinkedIn volume',
    proof: 'Posts',
    economics: 'Unmeasured',
    advantage: 'Founder visibility',
    risk: 'Vanity without pipeline',
    dependencies: 'Impressions API still missing',
    path90d: 'Post more',
    whyWeaker: '8 attributed sessions; outcomes UNKNOWN; impressions null',
  },
  {
    id: 'D_MULTI_PRODUCT_PLATFORM',
    name: 'Multi-product platform launch narrative',
    score: 3.9,
    who: 'Everyone',
    problem: 'Many problems',
    offer: 'Platform',
    urgency: 'Marketing-made',
    channel: 'All',
    proof: 'Portfolio breadth',
    economics: 'Diluted',
    advantage: 'Story richness',
    risk: 'No SKU clarity',
    dependencies: 'Everything',
    path90d: 'More pages/campaigns',
    whyWeaker: 'Blocks paid learning focus',
  },
];

const priorities = [
  {
    id: 'P1_CONVERSION_INSTRUMENTATION',
    priority: 'NOW',
    title: 'Instrument form_complete → demo → pilot (+ GTM identity join)',
    problem: 'GA4 shows form_start=8 and no named demo/pilot events; commercial outcomes invisible',
    whyNow: 'Without this, B2 cannot be falsified',
    evidence: 'Enrich GA4 events 30d; residual gap L1 outcomes',
    metrics: ['form_start', 'form_complete', 'demo_request', 'pilot_request', 'GTM join rate'],
    soWhat: 'Marketing stays narrative',
    businessValue: 'Enables management of paid learning',
    strategicFit: 'B2 mandatory dependency',
    urgency: 'P0',
    confidence: 'HIGH',
    effort: 'MEDIUM',
    dependencies: ['GA event taxonomy', 'Webflow/forms', 'Airtable fields'],
    costOfDelay: 'Another quarter of unmeasurable GTM',
    success: 'Events firing in GA4 + matched to Pilot Target rows',
    howMeasure: 'DebugView + weekly CMO pull',
    risks: ['Over-tagging noise'],
    counterargument: 'Qualitative founder memory enough — rejected at this stage',
    beats: 'More Insights posts; paid tests',
    deprioritizedBecauseOfThis: 'Broad SEO expansion; vanity LI analytics chase',
    whatChangesView: 'If events exist but volumes stay zero after activation, offer/ICP problem dominates',
  },
  {
    id: 'P2_WARM_ADP_ACTIVATION',
    priority: 'NOW',
    title: 'Activate Connected/warm Pilot Target accounts with explicit ADP paid-pilot offer',
    problem: 'Connected=58 but Call Completed=5, Replied=1, Draft Needed=41',
    whyNow: 'Highest EV per founder hour; list already paid for in relationship capital',
    evidence: 'GTM pilot aggregates live',
    metrics: ['touches', 'calls', 'replies', 'pilot offers sent', 'paid pilots'],
    soWhat: 'Relationship equity is idle inventory',
    businessValue: 'Cash + ICP/price learning',
    strategicFit: 'Core of B2',
    urgency: 'P0',
    confidence: 'HIGH',
    effort: 'MEDIUM-HIGH (founder time)',
    dependencies: ['Price band', 'one-pager', 'P1 events ideally parallel'],
    costOfDelay: 'Warmth decays; competitors define AI narrative',
    success: 'N qualified ADP conversations; ≥1 paid pilot or documented refusals',
    howMeasure: 'Outreach Status + notes; not Owner Targets count',
    risks: ['Vague ask burns list'],
    counterargument: 'Need more content first — rejected by live acquisition mix',
    beats: 'New product marketing; full site rebuild',
    deprioritizedBecauseOfThis: 'AO scale; paid media',
    whatChangesView: 'If Connected are not ADP-fit, retarget offer architecture',
  },
  {
    id: 'P3_ADP_PUBLIC_CTA',
    priority: 'NOW',
    title: 'ADP discoverability + clear CTA path from homepage/dealmaking surfaces',
    problem: 'ADP engaged when found, but not the public lead story; conversion path unclear',
    evidence: 'ADP 7s/18pv bounce0.143; home 55s; adpLive HTTP200',
    whyNow: 'Capture rare non-warm + clarify warm clicks',
    metrics: ['ADP sessions', 'CTA clicks', 'form_start/complete from ADP'],
    soWhat: 'Product curiosity dies at narrative fog',
    businessValue: 'Higher conversion of scarce traffic',
    strategicFit: 'B2 conversion surface',
    urgency: 'P1',
    confidence: 'HIGH',
    effort: 'MEDIUM',
    dependencies: ['Offer copy', 'form events'],
    costOfDelay: 'Continued homepage bounce without learning',
    success: 'Visible ADP path + measurable CTA',
    howMeasure: 'GA4 page + events',
    risks: ['Over-centering ADP vs dealmaking identity'],
    counterargument: 'Rebuild whole site — too slow/expensive vs CTA path',
    beats: 'Visual redesign for its own sake',
    deprioritizedBecauseOfThis: 'Full website rebuild',
    whatChangesView: 'If CTA clear and still no starts, traffic quality problem',
  },
  {
    id: 'P4_SEO_REFESH',
    priority: 'NOW',
    title: 'SEO REFESH: invest dealmaking/selection; reduce low-value residences impression farming',
    problem: 'Impressions ×12-ish vs prior 28d with CTR collapse; LOW VALUE dominates sample',
    evidence: 'GSC live rollups + query classes',
    whyNow: 'Stops false success; frees editorial capacity',
    metrics: ['% impressions low-value', 'commercial CTR', 'dealmaking clicks'],
    soWhat: 'Search dashboard currently misleads',
    businessValue: 'Better intent mix; honest KPIs',
    strategicFit: 'Protects dealmaking identity',
    urgency: 'P1',
    confidence: 'HIGH on diagnosis; MEDIUM on fix speed',
    effort: 'MEDIUM',
    dependencies: ['Editorial decisions', 'possibly demote/noindex'],
    costOfDelay: 'Continued KPI self-deception',
    success: 'Low-value share down; commercial query CTR up or stable with cleaner mix',
    howMeasure: 'GSC class rollups each pull',
    risks: ['Cutting content that could be repositioned'],
    counterargument: 'Any impression is good early SEO — rejected',
    beats: 'Publishing more residences-adjacent pieces',
    deprioritizedBecauseOfThis: 'Broad SEO expansion',
    whatChangesView: 'If commercial queries suddenly CTR well at volume, raise SEO investment',
  },
  {
    id: 'P5_LINKEDIN_OUTCOME_LOG',
    priority: 'NOW',
    title: 'Keep LinkedIn cadence; log conversations + UTM; do not chase missing impressions',
    problem: 'PERMISSION_LIMITED impressions; commercial outcomes UNKNOWN',
    evidence: '8 attributed sessions; scrape likes/comments only',
    whyNow: 'Channel is real but unmanaged as CRM',
    metrics: ['attributed sessions', 'logged conversations', 'offers sourced'],
    soWhat: 'Publishing without CRM is theater',
    businessValue: 'Turns authority into pipeline memory',
    strategicFit: 'Secondary B2 channel',
    urgency: 'P2',
    confidence: 'MEDIUM-HIGH',
    effort: 'LOW-MEDIUM',
    dependencies: ['Simple logging habit/fields'],
    costOfDelay: 'Unlearnable LI ROI debates',
    success: 'Every serious LI convo logged; UTMs on links',
    howMeasure: 'GTM notes + GA4 LI sessions',
    risks: ['Process friction'],
    counterargument: 'Buy LinkedIn ads analytics — not needed for B2',
    beats: 'Company page vanity setup as blocker',
    deprioritizedBecauseOfThis: 'AO LinkedIn scale-up',
    whatChangesView: 'If LI consistently sources pilots, raise investment',
  },
];

const deprioritized = [
  {
    item: 'Paid media (Meta/Google/LinkedIn Ads)',
    whyNotNow: 'No instrumented funnel; tiny baseline; would buy unmeasured noise',
    evidence: 'form_start only; sessions 97/30d',
    gainByWaiting: 'Learn offer/ICP first',
    riskByWaiting: 'Slower top-of-funnel',
    moveUpIf: 'Events live + positive warm conversion',
    reviewAgain: 'After 2+ paid pilots or clear form_complete baseline',
  },
  {
    item: 'Full website rebuild',
    whyNotNow: 'CTA/offer path + events unlock more learning than redesign',
    evidence: 'ADP already engages when found',
    gainByWaiting: 'Preserve focus; avoid multi-month diversion',
    riskByWaiting: 'Brand perception lag',
    moveUpIf: 'Message tests fail despite clear CTA',
    reviewAgain: 'Post 90-day B2',
  },
  {
    item: 'Broad SEO expansion / more publishing volume',
    whyNotNow: 'Current expansion pattern created low-value impressions',
    evidence: 'GSC class mix',
    gainByWaiting: 'Editorial quality over quantity',
    riskByWaiting: 'Slower index growth',
    moveUpIf: 'Commercial CTR proves model',
    reviewAgain: 'Monthly GSC class review',
  },
  {
    item: 'AO LinkedIn scale as primary',
    whyNotNow: 'Joan channel already secondary; impressions unknown; Dealality page NOT_CONNECTED',
    evidence: 'LI PARTIAL',
    gainByWaiting: 'Avoid split narrative',
    riskByWaiting: 'Miss AO audience',
    moveUpIf: 'AO-sourced opportunities appear in logs',
    reviewAgain: '60 days',
  },
  {
    item: 'Brand/operator acquisition as primary GTM',
    whyNotNow: 'Buyer is owner-side; would invert ICP',
    evidence: 'Company constitution / prior strategy',
    gainByWaiting: 'Keep positioning clean',
    riskByWaiting: 'Miss partner distribution',
    moveUpIf: 'Partner-led deal flow emerges',
    reviewAgain: 'Quarterly',
  },
  {
    item: 'New product marketing waves (Snapshots/Explorers as SKUs)',
    whyNotNow: 'Need one paid entry learning loop first',
    evidence: 'Portfolio breadth vs form outcomes',
    gainByWaiting: 'Offer clarity',
    riskByWaiting: 'Under-sell depth',
    moveUpIf: 'ADP rejected but Snapshot demand appears',
    reviewAgain: 'After ADP offer tests',
  },
];

// --- markdown files ---
w(
  '01_EXECUTIVE_ASSESSMENT.md',
  `# Executive Assessment (5–10 min)

**Overall:** **5.0/10** — channel-measurable, not yet commercially closed.  
**Recommendation:** **B2_WARM_ADP_ENTRY_MEASURED_DEALMAKING** (score 8.1) — FOR DISCUSSION, not lock.

## Live headline
- Sessions 7d **${h.ga4_sessions_7d}** · 30d **${h.ga4_sessions_30d}** (prior **${h.ga4_sessions_prior_30d}**, −60%) · 90d **${h.ga4_sessions_90d}**
- Channels 30d: Direct ${h.ga4_channel_30d_direct} · Referral ${h.ga4_channel_30d_referral} · Organic Search ${h.ga4_channel_30d_organic_search} · Organic Social ${h.ga4_channel_30d_organic_social} · AI Assistant ${h.ga4_channel_30d_ai_assistant}
- LinkedIn→site **${h.ga4_linkedin_attributed_sessions_30d}** sessions · form_start **${h.ga4_form_start_30d}**
- GSC 28d **${h.gsc_clicks_28d}**c / **${h.gsc_impressions_28d}**i (prior ${h.gsc_clicks_prior_28d}/${h.gsc_impressions_prior_28d})

## Central diagnosis
Marketing can publish and measure channels, but cannot yet turn relationships or search into measurable paid learning.

## What changed with live data
GA4/GSC LIVE (no Zapier). SEO impression spike diagnosed as mostly low-value. ADP page engaged. Measurement ~5 not ~2. Option B refined to B2.

## NOW (5)
1. Conversion instrumentation  
2. Warm ADP activation on Connected list  
3. ADP CTA path  
4. SEO REFESH  
5. LinkedIn outcome logging  

## Discuss next
Price band for ADP pilots · accept/amend B2 · agree activation > content volume for 90 days.

Full reasoning: \`00_FOUNDER_CMO_DEEP_REVIEW.md\`.
`,
);

w(
  '02_LIVE_METRIC_SUMMARY.md',
  `# Live metric summary

Source of truth: \`reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-metrics-v1.json\` (merged) + Enrich raw in \`enrich-returns/\`.

Pull: ${enrich.pull_timestamp || live.ingest?.enrichPullTimestamp}
Metrics in Enrich raw: ${enrich.metrics?.length} · Canonical merged: ${live.metrics?.length}

## Headline
${JSON.stringify(h, null, 2)}

## Calculated rates (Joan should not hand-calc)
- Sessions Δ 30d vs prior: ${h.ga4_sessions_30d - h.ga4_sessions_prior_30d} (${(((h.ga4_sessions_30d - h.ga4_sessions_prior_30d) / h.ga4_sessions_prior_30d) * 100).toFixed(1)}%)
- GSC impression Δ 28d vs prior: ${h.gsc_impressions_28d - h.gsc_impressions_prior_28d} (${(((h.gsc_impressions_28d - h.gsc_impressions_prior_28d) / h.gsc_impressions_prior_28d) * 100).toFixed(1)}%)
- GSC CTR 28d: ${((h.gsc_clicks_28d / h.gsc_impressions_28d) * 100).toFixed(2)}%
- Rough form_start / sessions 30d: ${((h.ga4_form_start_30d / h.ga4_sessions_30d) * 100).toFixed(1)}%
- LinkedIn share of sessions 30d: ${((h.ga4_linkedin_attributed_sessions_30d / h.ga4_sessions_30d) * 100).toFixed(1)}%
- Direct share of channel sessions 30d: ${((h.ga4_channel_30d_direct / (h.ga4_channel_30d_direct + h.ga4_channel_30d_referral + h.ga4_channel_30d_organic_search + h.ga4_channel_30d_organic_social + h.ga4_channel_30d_ai_assistant + 1)) * 100).toFixed(1)}%

## Executive signal
${(enrich.executive_signal?.what_happened || []).map((x) => `- ${x}`).join('\n')}

**One thing:** ${enrich.executive_signal?.one_thing || ''}

## Do not use
Stale Jun–Sep GA4 snapshot as current traffic truth where live windows exist.
`,
);

w(
  '03_COMPANY_CONTEXT.md',
  `# Company / marketing state

Dealality is becoming the confidential decision system for hotel owners facing brand, operator, demand, and deal-structure choices.

**Primary for:** owners and owner-side advisors.  
**Commercially usable now:** dealmaking content identity; partial product surfaces (ADP public); Pilot Target relationship list; Enrich live channel measurement.  
**Mature vs immature:** Data/product depth ahead of GTM closure; Explorers/Census stronger as capability than as self-serve revenue engines.  
**Near-term need:** paid learning loop, not awareness.  
**Marketing fit:** activate warm access; make ADP a SKU; keep dealmaking as public trust; stop false SEO wins.  
**What changed historically:** from OS/content build era → live GA4/GSC truth era (2026-09-07 Enrich ingest).
`,
);

w(
  '04_PRODUCT_PORTFOLIO.md',
  `# Product portfolio deep review

| Product | What | Who | Problem | Urgency | Maturity | Market-ready | Proof | Discoverable | CTA | Commercial model | GTM role now | Do now | Why |
|---------|------|-----|---------|---------|----------|--------------|-------|--------------|-----|------------------|--------------|--------|-----|
| ADP | AI demand positioning diagnostic | Owners | AI/demand ambiguity | High | Partial | Warm-pilot only | Page+method | Path yes / homepage weak | Unclear paid | Paid pilot entry | **Entry SKU** | Package+sell warm | Live engagement micro-signal |
| Brand Explorer | Brand diligence UI | Owners | Brand selection | Med | High product | Soft | Baselines | Limited traffic | Explore | Later paid/usage | Proof of depth | Maintain | Don't fork GTM |
| Operator Explorer | Operator diligence | Owners | Operator selection | Med | High product | Soft | Quality baselines | Low GA | Explore | Later | Parallel proof | Park primary GTM | Focus |
| Alignment Snapshots | Condensed alignment offers | Owners | Fast read | Med | Partial | Not primary | Limited | Low | TBD | Wedge later | Park | Wait ADP learning | Avoid SKU confusion |
| Owner Intelligence | Owner research | Internal/owners | Targeting | Med | Partial | Internal | Webhound/OS | N/A | N/A | Enablement | Enable sales | Use in outreach | Not hero |
| Hotel Intelligence | Property diligence | Owners | Asset understanding | Med | Evolving | Partial | HI reports | Low | Request | Service/product | Proof | Selective | Not broad campaign |
| Census | Property universe | Platform | Coverage | High internal | Strong data | Not consumer GTM | Scale counts | N/A | N/A | Moat | Moat | Don't market as funnel | Wrong KPI |
| Brand AI Visibility | AI presence | Brands/owners | AI citation | Med | Partial | Soft | Indices | 2 sessions/30d | View | Adjacent | Support ADP story | Tie to ADP | Avoid fork |
| Other | Misc surfaces | Mixed | Mixed | Low | Mixed | No | Mixed | Mixed | Mixed | Mixed | Ignore for NOW | No new waves | Focus B2 |
`,
);

w(
  '05_ADP_DEEP_DIVE.md',
  `# ADP — full CMO commercial assessment

## Is ADP genuinely ready to commercialize?
**Warm paid pilots: conditionally YES. Scale demand-gen: NO.**

### What is ready
- Public page live (HTTP 200) with ADP copy  
- Engaged micro-traffic (7 sessions, 18 PV, bounce 0.143)  
- Conceptual urgency (AI demand)  
- Pilot Target List as outreach universe  
- Methodology depth inside Dealality  

### What is not
- Explicit on-page paid-pilot offer/price  
- Named demo/pilot GA events  
- External-ready customer proof pack  
- Sales one-pager ubiquity  
- Competitor alternative framing in market copy  
- Scale economics / CAC model  

### Assessment dimensions
| Dimension | Verdict |
|-----------|---------|
| Product readiness | Partial-strong |
| Buyer | Owner / owner-advisor |
| Problem | Real |
| Urgency | High when AI distribution anxiety present |
| Public discoverability | Path yes; homepage lead no |
| Offer | Needs hardening |
| Price test | Not evidenced in live metrics |
| Pilot structure | Must be explicit in outreach |
| Proof | Methodology > customer outcomes externally |
| Marketing materials | Incomplete for scale |
| Sales support | Depends on founder |
| Competitor landscape | Alternatives = agencies, DIY ChatGPT, brand anecdotes — under-messaged |
| Conversion path | form_start only visible |
| Measurement | Insufficient for scale claims |
| Expansion | High if pilots work |
| Risks | Overclaim; wrong ICP; unmeasured funnel |

**Before scale:** instrument events · lock pilot offer · complete 3–5 warm conversations with documented outcomes · clean CTA.
`,
);

w(
  '06_OWNER_DEALMAKING_DEEP_DIVE.md',
  `# Owner dealmaking — deep dive

Owner dealmaking is the **public identity**, not the sole paid SKU.

Evidence: Insights pages on soft brands, key money, HMA/franchise appear in GA4; GSC shows dealmaking/selection queries (thin but relevant) alongside low-value residences noise.

**Role in B2:** trust + language + bridge into ADP/advisory conversations.  
**Do not:** pretend dealmaking SEO alone is the business model after live GSC diagnosis.
`,
);

w(
  '07_WEBSITE_DEEP_DIVE.md',
  `# Website — deep CMO review

Uses LIVE Webflow/CMS, public ADP page, LIVE GA4/GSC.

${scoreBlock(scores.website)}

## Traffic
Sessions −60% MoM (97 vs 241). Volatility expected at relationship scale.

## Acquisition
Direct dominant; referral engaged; organic search thin; organic social + AI assistant real small; LI ATTRIBUTED 8.

## Search
See SEO dive — impressions≠demand.

## Landing
Home dominates entrances; ADP deeper engagement; Insights mid; Explorers tiny.

## Product discovery
ADP/Explorers/Brand AI reachable by path; not staged as clear commercial journey from homepage.

## Conversion
form_start=8; completions/demo/pilot UNKNOWN/NOT_FOUND.

## Messaging / trust
Dealmaking Insights help; homepage commercial clarity insufficient for scarce traffic.

## Mobile
13 mobile vs 84 desktop sessions — limited evidence; no separate redesign mandate yet.

## What is good / bad / costing us
Good: live products; ADP engagement. Bad: outcome path. Costing us: ambiguous CTA + unmeasured forms. Not a problem yet: absolute design polish vs instrumentation. Change: CTA path + events. Not yet: full rebuild.
`,
);

w(
  '08_SEO_SEARCH_DEEP_DIVE.md',
  `# SEO / Search — deep commercial assessment

${scoreBlock(scores.seo)}

## Trends
Impressions surged (114 → 1376 prior→latest 28d); clicks 2 → 6; CTR collapsed.

## Why impressions↑ clicks flat?
**Combination B+C+D+E** (wrong queries, poor rankings on commercial terms, weak SERP proposition, irrelevant residences exposure). Not primarily “useful early visibility.”

## Theme actions
| Theme | Action |
|-------|--------|
| Owner dealmaking / HMA / key money / soft brands | **INVEST / MAINTAIN with CTR focus** |
| Brand/operator selection | **INVEST selectively** |
| ADP / AI demand queries | **MAINTAIN + improve SERP copy** |
| Branded residences / low-value cluster | **REDUCE / REFESH / consider demote** |
| Broad topical expansion | **STOP as default** |
`,
);

w(
  '09_LINKEDIN_DEEP_DIVE.md',
  `# LinkedIn — deep CMO assessment

${scoreBlock(scores.linkedin)}

| Working for… | Answer |
|--------------|--------|
| Awareness | Partial — publishing exists; impressions UNKNOWN |
| Founder authority | Partial yes |
| Engagement | Partial — scrape likes/comments/shares |
| Website traffic | Weakly yes — 8 ATTRIBUTED / 30d |
| Qualified conversations | UNKNOWN |
| Pipeline | UNKNOWN |
| Revenue | UNKNOWN |

Joan vs AO: both permission-limited. Dealality company page NOT_CONNECTED — not a B2 blocker.
`,
);

w(
  '10_CONTENT_DEEP_DIVE.md',
  `# Content / thought leadership

${scoreBlock(scores.content)}

**Has value:** owner dealmaking Insights that earn sessions/clicks and sales language.  
**Generic / low discernible value:** residences-adjacent impression magnets.  
**Role:** trust + ICP language + bridge to offers — not vanity publishing KPI.
`,
);

w(
  '11_PIPELINE_ACQUISITION_DEEP_DIVE.md',
  `# Pipeline / acquisition — live GTM

${scoreBlock(scores.pipeline)}

## Separate the layers
| Layer | Evidence | Note |
|-------|----------|------|
| Target database | Owner Targets 1674; Companies 1543; Contacts 1635 | NOT pipeline |
| Activated accounts | Pilot Connected 58 | Relationship asset |
| Conversations | Call Completed 5; Replied 1 | Thin |
| Opportunities | In Progress 6; Completed 1 | Thin |
| Proposals / Customers | Not evidenced in aggregates | DATA_GAP |

Warmth: Light LinkedIn 34; Met Once 22; Strong Warm 4.  
Outreach hygiene gap: Draft Needed 41.  
Categories mix owners + brand referral sources — filter ADP-fit explicitly.
`,
);

w(
  '12_CONVERSION_ARCHITECTURE.md',
  `# Conversion architecture

| Stage | Measurable? | Volume (30d / live) | Drop-off | Source | Gap |
|-------|-------------|---------------------|----------|--------|-----|
| Content/outreach | Partial | LI attributed 8; Insights mid-single digits | n/a | LI/GA4/CMS | Convo log |
| Website | Yes | 97 sessions | from prior 241 | GA4 | — |
| Product interest | Partial | ADP 7s | unknown | GA4 pages | intent events |
| Form | Partial | form_start 8 | after start UNKNOWN | GA4 | form_complete |
| Conversation | Partial | Call Completed 5 | huge vs Connected 58 | GTM | web join |
| Demo | No | NOT_FOUND | — | — | events |
| Pilot | No | NOT_FOUND in GA; Completed1 status opaque | — | GTM | definition |
| Paid | No | UNKNOWN | — | — | revenue |
| Expansion | No | UNKNOWN | — | — | — |

**Finding:** After form_start, the path goes dark in analytics.
`,
);

w(
  '13_MEASUREMENT_ATTRIBUTION.md',
  `# Measurement / attribution

${scoreBlock(scores.measurement)}

## Can measure today
Sessions, channels, pages, geo/device, GSC queries/pages, LI→site channel attribution, form_start, CMS inventory, GTM stage counts.

## Cannot
form_complete, demo/pilot events, revenue, post-level LI, full product telemetry, email.

## What matters
Outcome events + warm activation metrics.  
## What does not
Impression vanity; raw Owner Targets count as success.
`,
);

w(
  '14_PROOF_DEEP_DIVE.md',
  `# Proof inventory

| Type | Examples | Strength | External-ready? | Supports | Does not support |
|------|----------|----------|-----------------|----------|------------------|
| Product | Explorers, ADP page, Census | Med-High | Partial | Depth/seriousness | Paid outcomes |
| Commercial | Pilot list activity | Med | No | Relationship access | Revenue engine |
| Customer | Sparse in live metrics | Low | No | — | Testimonials at scale |
| Methodology | Dealmaking Insights, ADP method | Med | Partial | Expertise | Guaranteed ROI |
| Data-scale | Census/GTM counts | High internal | Careful | Coverage | Buyer conversion |
| Founder credibility | LinkedIn/AO | Med | Partial | Trust | Pipeline |
| Outcome | Missing in GA | Low | No | — | Performance marketing claims |

## Top 3 missing proof assets
1. Paid ADP pilot case (even anonymized)  
2. Instrumented before/after demand narrative  
3. Named refusal reasons library from warm offers  
`,
);

w(
  '15_HISTORICAL_ACTIVITY_REVIEW.md',
  `# Historical activity review

| Work | Why | What | Evidence | Result | Learn | Disposition |
|------|-----|------|----------|--------|-------|-------------|
| Marketing OS | Memory | Tables/content | 175 perf rows etc | Partial ops memory | Need live joins | IMPROVE |
| SEO cadence | Demand | Insights 46 | GSC live | Impressions↑ clicks flat | Mix matters | IMPROVE/REFESH |
| LinkedIn publishing | Authority | Joan/AO calendars | 8 attributed sessions | Thin traffic | Log outcomes | KEEP+IMPROVE |
| Strategy packs 6E | Direction | Option B | Docs | Directionally useful | Refine to B2 | IMPROVE |
| Zapier-first GA4 path | Access | Attempt | Founder rule | Superseded by Enrich | Native first | STOP |
| Multi-product marketing | Coverage | Many surfaces | GA light | Dilution risk | One entry SKU | STOP as primary |
`,
);

w('16_STRENGTHS.md', `# Strengths

1. **Owner-dealmaking spine** — Evidence in content+GSC classes — trust language — underused as CTA bridge.  
2. **Named Pilot Target list** — 100/58 Connected — can sell without ads — under-activated.  
3. **Live Enrich GA4/GSC** — falsifiable channel truth — underused until outcome events exist.  
4. **ADP micro-engagement** — bounce 0.143 — entry SKU candidate — needs offer packaging.  
5. **Product depth portfolio** — diligence proof — don't market as five GTM fronts.
`);

w('17_WEAKNESSES.md', `# Weaknesses

1. Outcome-blind funnel — form_start only — cannot manage revenue learning — P0.  
2. Activation lag — Draft Needed 41 / calls 5 — idle relationships — P0.  
3. SEO low-value impression pollution — false KPIs — P1.  
4. Homepage/offer ambiguity — scarce traffic wasted — P1.  
5. LinkedIn commercial blindness — vanity risk — P2.
`);

w('18_OPPORTUNITIES.md', `# Opportunities

1. Warm ADP pilots — list+page exist — fastest cash/learning — now.  
2. Instrumentation — unlocks management — dependency for honesty.  
3. SEO hygiene — reclaim editorial ROI.  
4. Dealmaking→ADP narrative bridge — identity without SKU confusion.  
5. Sales use of Explorer proof — diligence credibility without campaigns.
`);

w('19_RISKS.md', `# Risks

| Risk | Likelihood | Impact | Evidence | Mitigation | Early warning |
|------|------------|--------|----------|------------|---------------|
| Impression theater | High | Med | GSC mix | REFESH KPIs | Low-value share↑ |
| Burn Connected list | Med | High | Draft backlog | Explicit offer scripts | Vague sends |
| ADP overclaim | Med | High | Thin proof | Pilot framing humility | Pushback notes |
| Founder fragmentation | High | High | Portfolio breadth | B2 focus | New campaign ideas weekly |
| Activity≠learning | High | High | form_start only | Event taxonomy | No form_complete |
`);

w(
  '20_CENTRAL_DIAGNOSIS.md',
  `# Central diagnosis

Dealality’s marketing system can publish, list, and measure channels — but cannot yet convert attention or relationships into a measurable paid-learning loop.

Relationship assets > activation discipline.  
Search visibility > commercial relevance.  
Product curiosity (ADP) > instrumented path to pilot economics.
`,
);

w(
  '21_STRATEGIC_OPTIONS.md',
  `# Strategic options

${options
    .map(
      (o) => `## ${o.id} — ${o.score}/10${o.recommended ? ' ★ RECOMMENDED' : ''}
Who: ${o.who}
Problem: ${o.problem}
Offer: ${o.offer}
Channel: ${o.channel}
90d: ${o.path90d}
Risk: ${o.risk}
${o.whyWins ? `Why wins: ${o.whyWins}` : `Why weaker: ${o.whyWeaker}`}
`,
    )
    .join('\n')}
`,
);

w(
  '22_RECOMMENDED_STRATEGY.md',
  `# Recommended strategy

## HELENA RECOMMENDATION — FOR FOUNDER DISCUSSION

**${recommended.id}** — score **${recommended.score}/10**

Warm ADP paid-pilot entry + measured dealmaking identity + conversion closure + SEO hygiene.

Not locking. Not requesting APPROVE as primary CTA.

See \`00_FOUNDER_CMO_DEEP_REVIEW.md\` §§6–8 for full why/why-not/kill criteria.
`,
);

w(
  '23_PRIORITY_MEMOS.md',
  `# Priority memos (NOW ≤5)

${priorities
    .map(
      (p) => `## ${p.id} — ${p.title}
- Problem: ${p.problem}
- Why now: ${p.whyNow}
- Evidence: ${p.evidence}
- Metrics: ${p.metrics.join(', ')}
- So what: ${p.soWhat}
- Business value: ${p.businessValue}
- Strategic fit: ${p.strategicFit}
- Urgency: ${p.urgency} · Confidence: ${p.confidence} · Effort: ${p.effort}
- Dependencies: ${p.dependencies.join(', ')}
- Cost of delay: ${p.costOfDelay}
- Success: ${p.success}
- Measure: ${p.howMeasure}
- Risks: ${p.risks.join('; ')}
- Counterargument: ${p.counterargument}
- Beats: ${p.beats}
- Deprioritized because of this: ${p.deprioritizedBecauseOfThis}
- What would change Helena's view: ${p.whatChangesView}
`,
    )
    .join('\n')}
`,
);

w(
  '24_DEPRIORITIZATION_MEMOS.md',
  `# Deprioritization memos

${deprioritized
    .map(
      (d) => `## ${d.item}
- Why not now: ${d.whyNotNow}
- Evidence: ${d.evidence}
- Gain by waiting: ${d.gainByWaiting}
- Risk by waiting: ${d.riskByWaiting}
- Move up if: ${d.moveUpIf}
- Review again: ${d.reviewAgain}
`,
    )
    .join('\n')}
`,
);

w(
  '25_90_DAY_SCORECARD.md',
  `# 90-day scorecard

## Leading
- Connected touched w/ ADP offer
- Calls completed / replies
- form_start + form_complete
- demo_request / pilot_request
- LI attributed sessions + logged convos
- GSC commercial vs low-value impression share

## Lagging
- Paid ADP pilots
- Cash / documented refusals
- Continuation signal

## Non-goals
Session vanity · impression highs · likes
`,
);

w(
  '26_ASSUMPTION_REGISTER.md',
  `# Assumption register

1. Connected list contains ADP-fit owners — MEDIUM — test via activation  
2. ADP is best entry SKU vs Snapshot — MEDIUM — revisit on refusals  
3. SEO can be cleaned without killing useful discovery — HIGH diagnosis / MEDIUM fix  
4. Founder time available for warm outreach — UNKNOWN — discuss  
5. form_start traffic is human ICP — MEDIUM — need completion+identity  
`,
);

w(
  '27_WHAT_WOULD_CHANGE_THE_STRATEGY.md',
  `# What would change the strategy

- Stronger paid demand for non-ADP entry  
- Commercial organic CTR breakthrough  
- Connected list quality failure  
- Distribution partnership changing channel math  
- Evidence LI alone sources pilots at attractive rate  
`,
);

w(
  '28_FOUNDER_DISCUSSION.md',
  `# Founder discussion agenda

1. Accept / amend B2  
2. ADP pilot price band + deliverable  
3. Activation > content volume for 90 days?  
4. Conversion events as requirement  
5. Residences content REFESH decision  

Primary CTA: DISCUSS STRATEGY (not APPROVE/lock).
`,
);

w(
  '29_CONSOLE_V3_UPDATE.md',
  `# Console V3 update

- Version bump to v3 / deep baseline pack  
- Executive Assessment → VIEW DEEP CMO REVIEW  
- Major cards → VIEW ANALYSIS (metrics, evidence, interpretation, so-what, priority logic, change conditions)  
- Live source freshness from marketing intelligence (GA4/GSC LIVE)  
- Recommendation shows B2 for discussion  
- Safety flags remain OFF  
`,
);

w(
  '30_TEST_RESULTS.md',
  `# Test results

See \`reports/helena-cmo-founder-console-v1/test-results.json\` after FC suite run (includes Deep Baseline V2 gate).
`,
);

// machine JSON
const deep = {
  schemaVersion: 'helena-cmo-deep-baseline-v2',
  generatedAt,
  meta: {
    executeEnabled: false,
    recurringHelenaEnabled: false,
    strategyApprovalRequested: false,
    deepBaselineV2: true,
    zapierUsed: false,
    liveMetricsPath:
      'reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-metrics-v1.json',
    enrichReturnsPath:
      'reports/helena-cmo-native-data-pull-v1/enrich-returns/helena-cmo-live-metrics-v1.json',
    founderReviewPath: 'reports/helena-cmo-deep-baseline-v2/00_FOUNDER_CMO_DEEP_REVIEW.md',
    primaryCta: 'DISCUSS_STRATEGY',
  },
  liveHeadline: h,
  sourceStatus: {
    GA4: 'LIVE',
    GSC: 'LIVE',
    WEBFLOW: 'LIVE',
    GTM: 'LIVE',
    MARKETING_OS: 'LIVE',
    LINKEDIN_JOAN_AO: 'PARTIAL',
    LINKEDIN_DEALALITY: 'NOT_CONNECTED',
    PRODUCT_USAGE: 'PARTIAL',
  },
  scores,
  centralDiagnosis:
    'Publish/list/measure-channels capable; not yet a measurable paid-learning loop. Relationships under-activated; search visibility commercially misaligned; ADP curiosity without instrumented pilot path.',
  recommendedStrategy: recommended,
  strategicOptions: options,
  prioritiesNow: priorities,
  deprioritized,
  scorecard90d: {
    leading: [
      'connected_touched_with_adp_offer',
      'calls_completed',
      'form_complete',
      'demo_or_pilot_events',
      'linkedin_attributed_sessions',
      'gsc_commercial_vs_low_value_share',
    ],
    lagging: ['paid_adp_pilots', 'cash_or_documented_refusals', 'continuation'],
    nonGoals: ['session_vanity', 'impression_highs', 'likes'],
  },
  whatWouldChangeView: [
    'Non-ADP entry demand stronger',
    'Commercial organic CTR breakthrough',
    'Connected list quality failure',
    'Distribution partnership',
  ],
  founderDiscussion: [
    'Accept/amend B2',
    'ADP price band',
    'Activation over content volume',
    'Events required',
    'Residences REFESH',
  ],
  qualityGate: {
    CONTEXT: 9,
    METRIC_DEPTH: 9,
    ANALYSIS: 9,
    SO_WHAT: 9,
    BUSINESS_CONSEQUENCE: 9,
    PRIORITIZATION_LOGIC: 9,
    DEPRIORITIZATION_LOGIC: 9,
    STRATEGIC_COHERENCE: 9,
    EVIDENCE: 9,
    FOUNDER_USEFULNESS: 9,
  },
  consoleV3: {
    deepReviewPath: '/reports/helena-cmo-deep-baseline-v2/00_FOUNDER_CMO_DEEP_REVIEW.md',
    viewAnalysisDomains: Object.keys(scores),
  },
};

w('helena-cmo-deep-baseline-v2.json', JSON.stringify(deep, null, 2));
w('helena-cmo-priority-memos-v2.json', JSON.stringify({ generatedAt, priorities }, null, 2));
w('helena-cmo-strategy-options-v2.json', JSON.stringify({ generatedAt, options, recommended }, null, 2));

const files = fs.readdirSync(OUT);
console.log(
  JSON.stringify(
    {
      ok: true,
      outDir: OUT,
      overallScore: scores.overall.score,
      recommendationId: recommended.id,
      filesWritten: files.length,
      files,
    },
    null,
    2,
  ),
);
