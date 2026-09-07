#!/usr/bin/env node
/**
 * Helena CMO Phase 6D — Baseline validation pack writer.
 * Honest about STALE analytics; Marketing OS live-verified; live website inspected.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'reports/helena-cmo-baseline-validation-v1');

const sources = [
  { source: 'Founder locks D1–D5 / JD-6B', group: 'COMPANY', available: true, reviewed: true, dateRange: '2026-09-07', lastUpdated: '2026-09-07', freshness: 'CURRENT', answers: 'Locked positioning/GTM/ICP/pricing/LinkedIn role', cannot: 'Public tagline final phrase', confidence: 'HIGH' },
  { source: 'Operating Law v1', group: 'COMPANY', available: true, reviewed: true, dateRange: '2026-09-07', lastUpdated: '2026-09-07', freshness: 'CURRENT', answers: 'Authority/PREPARE/EXECUTE gates', cannot: 'Market effectiveness', confidence: 'HIGH' },
  { source: 'Phase 3c1 reconciled strategy', group: 'COMPANY', available: true, reviewed: true, dateRange: '2026-09', lastUpdated: '2026-09-07', freshness: 'CURRENT', answers: 'ADP+dealmaking coexistence intent', cannot: 'Live commercial outcomes', confidence: 'HIGH' },
  { source: 'Strategy maturity pack (~4.3)', group: 'COMPANY', available: true, reviewed: true, dateRange: '2026-09-07', lastUpdated: '2026-09-07', freshness: 'CURRENT', answers: 'Layer maturity', cannot: 'Public GTM proof', confidence: 'HIGH' },
  { source: 'Manual Week 01 pack', group: 'MARKETING OS', available: true, reviewed: true, dateRange: '2026-W37', lastUpdated: '2026-09-07', freshness: 'CURRENT', answers: 'Weekly priorities/gaps', cannot: 'Named CRM accounts', confidence: 'HIGH' },
  { source: 'Manual cycle diagnosis', group: 'HISTORICAL GTM', available: true, reviewed: true, dateRange: '2026-09-07', lastUpdated: '2026-09-07', freshness: 'CURRENT', answers: 'Top marketing problems', cannot: 'Post-refresh website nuance', confidence: 'MEDIUM' },
  { source: 'Marketing OS Airtable live read', group: 'MARKETING OS', available: true, reviewed: true, dateRange: 'live 2026-09-07', lastUpdated: '2026-09-07T13:03Z', freshness: 'CURRENT', answers: 'Tables/seed presence', cannot: 'Attributed performance outcomes', confidence: 'HIGH' },
  { source: 'PACK_CORE messaging/Target/TL OS', group: 'MESSAGING', available: true, reviewed: true, dateRange: 'export 2026-09-07', lastUpdated: '2026-09-07', freshness: 'POINT_IN_TIME', answers: 'Message systems inventory', cannot: 'Which messages convert', confidence: 'MEDIUM' },
  { source: 'Live dealality.com homepage', group: 'WEBSITE', available: true, reviewed: true, dateRange: 'fetched 2026-09-07', lastUpdated: '2026-09-07', freshness: 'CURRENT', answers: 'Live public narrative/CTAs', cannot: 'Conversion rates', confidence: 'HIGH' },
  { source: 'Repo landing v9 embed', group: 'WEBSITE', available: true, reviewed: true, dateRange: 'repo', lastUpdated: 'pre-2026-09', freshness: 'CURRENT_REPO_MAY_DIFF_FROM_LIVE', answers: 'Selection-forward embed copy', cannot: 'What Webflow currently serves alone', confidence: 'MEDIUM' },
  { source: 'for-owners / platform / about pages', group: 'WEBSITE', available: true, reviewed: true, dateRange: 'repo+live', lastUpdated: 'mixed', freshness: 'MIXED', answers: 'Audience page posture', cannot: 'Live /platform (404)', confidence: 'MEDIUM' },
  { source: 'GA4 session snapshot', group: 'GA4', available: true, reviewed: true, dateRange: '2026-06-01→09-06', lastUpdated: '2026-09-06', freshness: 'STALE', answers: 'Sessions by coarse channel', cannot: 'LinkedIn-named attribution; live trend', confidence: 'MEDIUM' },
  { source: 'GSC page snapshot', group: 'GSC', available: true, reviewed: true, dateRange: '2026-06-01→09-06', lastUpdated: '2026-09-06', freshness: 'STALE', answers: 'Clicks/impr by page', cannot: 'Live queries; conversions', confidence: 'MEDIUM' },
  { source: 'Live GA4 API', group: 'GA4', available: false, reviewed: false, dateRange: null, lastUpdated: null, freshness: 'MISSING', answers: null, cannot: 'All live GA4', confidence: 'N/A' },
  { source: 'Live GSC API', group: 'GSC', available: false, reviewed: false, dateRange: null, lastUpdated: null, freshness: 'MISSING', answers: null, cannot: 'All live GSC', confidence: 'N/A' },
  { source: 'LinkedIn posts metrics scrape (34)', group: 'LINKEDIN', available: true, reviewed: true, dateRange: '~2026-07→09', lastUpdated: '2026-09-06', freshness: 'STALE_PARTIAL', answers: 'Likes/comments by post; Joan vs AO', cannot: 'Impressions; commercial outcomes; full 70+ corpus text', confidence: 'MEDIUM_LOW' },
  { source: 'LinkedIn engagement summary + calendar', group: 'LINKEDIN', available: true, reviewed: true, dateRange: 'export', lastUpdated: '2026-09-07', freshness: 'POINT_IN_TIME', answers: 'Publish status/themes', cannot: 'Pipeline attribution', confidence: 'MEDIUM' },
  { source: 'Live LinkedIn analytics API', group: 'LINKEDIN', available: false, reviewed: false, dateRange: null, lastUpdated: null, freshness: 'MISSING', answers: null, cannot: 'Official LI analytics', confidence: 'N/A' },
  { source: 'Webflow Insights catalog (46 posts)', group: 'WEBFLOW', available: true, reviewed: true, dateRange: 'as of 2026-09-06', lastUpdated: '2026-09-06', freshness: 'POINT_IN_TIME', answers: 'Content inventory themes', cannot: 'Engagement/conversion', confidence: 'MEDIUM' },
  { source: 'Live Webflow CMS API crawl', group: 'WEBFLOW', available: false, reviewed: false, dateRange: null, lastUpdated: null, freshness: 'MISSING', answers: null, cannot: 'Live CMS fields', confidence: 'N/A' },
  { source: 'ADP decks / leak-audit demo pack', group: 'ADP', available: true, reviewed: true, dateRange: '2026', lastUpdated: '2026-09', freshness: 'CURRENT', answers: 'Sales assets exist', cannot: 'Close rates', confidence: 'HIGH' },
  { source: 'ADP commercial evidence JSON', group: 'ADP', available: true, reviewed: true, dateRange: 'Sep 2026', lastUpdated: '2026-09', freshness: 'NEAR_CURRENT', answers: 'Commercial intent evidence', cannot: 'Named paid pilots live', confidence: 'MEDIUM' },
  { source: 'Named CRM / ADP pipeline', group: 'CRM / PIPELINE', available: false, reviewed: false, dateRange: null, lastUpdated: null, freshness: 'DATA_GAP', answers: null, cannot: 'Named owners contacted/quoted', confidence: 'HIGH_THAT_MISSING' },
  { source: 'GTM strike lists / pilot kits', group: 'COMMERCIAL / PROPOSALS', available: true, reviewed: true, dateRange: '2026', lastUpdated: 'mixed', freshness: 'NEAR_CURRENT', answers: 'Potential targets / outreach kits', cannot: 'Actual win/loss outcomes', confidence: 'MEDIUM' },
  { source: 'Email / outreach attribution', group: 'EMAIL / OUTREACH', available: false, reviewed: false, dateRange: null, lastUpdated: null, freshness: 'NOT_CONNECTED', answers: null, cannot: 'Email→pipeline', confidence: 'HIGH_THAT_MISSING' },
  { source: 'Product surfaces (ADP/Explorers/Census/BAI)', group: 'PRODUCT', available: true, reviewed: true, dateRange: 'repo+AGENTS', lastUpdated: '2026-09', freshness: 'CURRENT', answers: 'Product maturity map', cannot: 'Customer adoption metrics', confidence: 'HIGH' },
  { source: 'Proof inventory (pilots/testimonials)', group: 'PROOF', available: true, reviewed: true, dateRange: 'packs', lastUpdated: '2026-09', freshness: 'CURRENT_ASSESSMENT', answers: 'Proof vacuum confirmed', cannot: 'External conversion-grade cases', confidence: 'HIGH' },
  { source: 'Competitive landscape deep pack', group: 'COMPETITIVE / MARKET', available: false, reviewed: true, dateRange: '6D synthesis', lastUpdated: '2026-09-07', freshness: 'SYNTHESIZED', answers: 'Category comparisons', cannot: 'Primary competitor interviews', confidence: 'MEDIUM' },
  { source: 'Design / brand asset library inventory', group: 'DESIGN / BRAND ASSETS', available: true, reviewed: true, dateRange: 'public/marketing', lastUpdated: 'mixed', freshness: 'PARTIAL', answers: 'Landing assets exist', cannot: 'Brand system completeness score', confidence: 'MEDIUM' },
  { source: 'Partner / advisor GTM evidence', group: 'PARTNERSHIPS', available: true, reviewed: true, dateRange: 'AO posts + gtm-resources', lastUpdated: '2026-09', freshness: 'PARTIAL', answers: 'Advisor motion exists', cannot: 'Referral conversion', confidence: 'MEDIUM_LOW' },
  { source: 'Historical Deal CaptureT strategy corpus', group: 'HISTORICAL GTM', available: true, reviewed: true, dateRange: 'pre-rebrand', lastUpdated: 'archived', freshness: 'HISTORICAL', answers: 'Original strategy lineage', cannot: 'Current effectiveness', confidence: 'MEDIUM' },
  { source: 'Landing first-party analytics events', group: 'GA4', available: true, reviewed: false, dateRange: 'runtime', lastUpdated: 'unknown', freshness: 'UNKNOWN_LIVE', answers: 'Possible event stream', cannot: 'Joined commercial outcomes', confidence: 'LOW' },
];

const relevant = sources.length;
const reviewed = sources.filter((s) => s.reviewed).length;
const missing = sources.filter((s) => !s.available || s.freshness === 'MISSING' || s.freshness === 'DATA_GAP' || s.freshness === 'NOT_CONNECTED').length;
const fresh = sources.filter((s) => s.freshness === 'CURRENT' || s.freshness === 'NEAR_CURRENT' || s.freshness === 'CURRENT_ASSESSMENT').length;
const stale = sources.filter((s) => /STALE|POINT_IN_TIME|HISTORICAL|MIXED|PARTIAL|SYNTHESIZED|CURRENT_REPO/.test(s.freshness)).length;
const coveragePct = Math.round((reviewed / relevant) * 100);

const validation = {
  schemaVersion: 'helena-cmo-baseline-validation-v1',
  generatedAt: new Date().toISOString(),
  meta: {
    executeEnabled: false,
    recurringHelenaEnabled: false,
    strategyApprovalRequested: false,
    strategyState: 'STRATEGY_PENDING_FOUNDER_REVIEW',
    note: 'Phase 6D validates evidence before any strategy approval ask.',
  },
  sourceCompleteness: {
    totalRelevant: relevant,
    totalReviewed: reviewed,
    totalMissingOrUnavailable: missing,
    freshCurrentApprox: fresh,
    staleOrPointInTimeApprox: stale,
    coveragePercent: coveragePct,
    baselineConfidence: 'MEDIUM',
    honesty:
      'Not comprehensive for live analytics/CRM. Comprehensive for recovered CURRENT packs + live homepage + Marketing OS table verify.',
    sources,
  },
  liveRefresh: {
    attempted: [
      { source: 'Marketing OS Airtable', status: 'REFRESHED', at: '2026-09-07T13:03:23Z', method: 'helena-cmo-phase-3a-foundation.mjs --dry-run' },
      { source: 'Live dealality.com homepage', status: 'FETCHED', at: '2026-09-07', method: 'HTTP fetch' },
      { source: 'GA4', status: 'NOT_AVAILABLE', reason: 'No GA4 Data API connector/script in repo' },
      { source: 'GSC', status: 'NOT_AVAILABLE', reason: 'No GSC API connector/script in repo' },
      { source: 'LinkedIn analytics API', status: 'NOT_AVAILABLE', reason: 'Only scraped snapshot (34 posts)' },
      { source: 'Webflow CMS API', status: 'NOT_AVAILABLE', reason: 'No CMS crawl script; asset upload only' },
    ],
  },
  revisedScores: {
    overallMarketingHealth: { prior: 4.5, revised: 4.8, keep: false, why: 'Live homepage is more dealmaking-aligned than V1 “selection-era” blunt label; ADP still absent; analytics still STALE; CRM still DATA_GAP. Slight upward revision, not a breakthrough.' },
    website: { prior: 5, revised: 5.5, verdict: 'MIXED', why: 'Live site: owner dealmaking narrative + CTAs (“Explore your hotel opportunity”). Still zero ADP / AI demand wedge. Repo v9 embed remains selection-forward — hierarchy inconsistency risk.' },
    linkedin: { prior: 5, revised: 5, verdict: 'IMPROVE', why: 'Joan posts outperform AO company in engagement quality proxy; 0 impressions; commercial outcomes UNKNOWN. Keep IMPROVE.' },
    seo: { prior: 4, revised: 4, verdict: 'MIXED_WEAK', why: 'No live GSC refresh; snapshot still thin clicks / vanity impressions.' },
    adpReadiness: { prior: 6, revised: 6, verdict: 'PRODUCT_AHEAD_OF_PUBLIC_GTM', why: 'Unchanged: assets + MARKET_TEST; named pipeline DATA_GAP; public invisible.' },
    ownerAcquisition: { prior: 4, revised: 4.5, why: 'Live site better supports dealmaking identity; systematic acquisition + proof still weak.' },
    measurement: { prior: 2, revised: 2, why: 'Still BROKEN attribution; snapshots STALE; no live connectors.' },
  },
  strategyChallenge: {
    priorThesis:
      'Lead near-term with ADP paid-pilot commercialization for warm owners, keep dealmaking as durable identity...',
    decision: 'AMEND',
    why: [
      'Evidence still supports warm ADP as near-term revenue wedge (locks + product readiness) — CONFIRM that core.',
      'Live website is already more dealmaking-forward than Baseline V1 implied; “selection-era” was overstated for live homepage (still true for v9 embed / Insights themes).',
      'Named pipeline DATA_GAP + proof vacuum + measurement BROKEN are co-equal bottlenecks — strategy must elevate instrumentation/proof to equal weight with ADP conversation push.',
      'Website PREPARE remains valuable but should not outrank named warm list + scorecard + attribution P0.',
      'Brands/operators should stay secondary commercially; live site already sells them annual access — do not expand brand acquisition NOW.',
      'SEO not worth heavy NEW investment until owner-intent pages show conversion path.',
      'LinkedIn worth continued Joan investment IF commercial education + warm activation; AO low-engagement volume is IMPROVE/STOP-as-primary.',
    ],
    revisedThesis:
      'Near-term: advance instrumented ADP paid-pilot conversations with Joan-confirmed warm accounts while compounding proof and repairing attribution. Public identity remains owner dealmaking (already stronger on live homepage); ADP must become a visible dual-track wedge without rewriting the whole company as “AI visibility SaaS.” Do not treat website rebuild, SEO volume, or brand/operator acquisition as NOW.',
  },
  topProofAssetsToIncreaseConversion: [
    'Instrumented ADP pilot scorecard with 1–2 anonymized before/after demand narratives (no fabricated metrics)',
    'Owner-facing ADP one-pager + discuss-pilot CTA under MARKET_TEST labeling',
    'Founder-credibility + methodology page/section that pairs dealmaking process with AI demand evidence depth',
  ],
};

function write() {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, 'helena-cmo-baseline-validation-v1.json'), JSON.stringify(validation, null, 2));

  const sc = validation.sourceCompleteness;
  const files = {
    '00_FOUNDER_DEEP_BRIEF.md': `# Founder Deep Brief — Before Strategy Approval

**Audience:** Joan · **Read time:** ~20–30 min · **EXECUTE/Recurring:** OFF · **Strategy approval:** NOT requested in this phase

## 1. Executive read
Dealality’s locked strategy is dual-track (ADP commercialization + owner dealmaking). Live public homepage is already owner-dealmaking forward (“Find the Best Path for Your Hotel”), but **AI Demand Positioning is still invisible**. Product depth is strong; public commercialization, named pipeline, proof, and attribution are not. Overall marketing health revised to **~4.8/10** (was 4.5) — slight uptick from live-site nuance, not from solved bottlenecks.

## 2. State of the business / GTM
- Direction locked (D1–D5, JD-6B).
- Near-term revenue needs paid ADP pilot learning.
- Public GTM still under-instrumented.

## 3. What Helena reviewed
${sc.totalReviewed}/${sc.totalRelevant} relevant sources (${sc.coveragePercent}%). Live refreshed: Marketing OS Airtable tables + live homepage. **Not** live-refreshed: GA4, GSC, LinkedIn API, Webflow CMS, CRM.

## 4. What is strong
Product depth (ADP/Explorers/Census/intelligence) · founder warm path · locks + Operating Law · Marketing OS foundation · live dealmaking narrative better than feared.

## 5. What is weak
ADP public invisibility · attribution BROKEN · proof vacuum · named CRM DATA_GAP · analytics STALE · AO LinkedIn low engagement · SEO thin/vanity.

## 6–7. What has worked / has not
**Worked (partial):** Joan LinkedIn voice & owner themes; ADP decks/demo packs; landing iteration to a coherent dealmaking story; Operating Law/locks.
**Has not:** Treating likes as pipeline; AO volume posts; selection-forward embed inconsistency; publishing without attribution; pilots without scorecard.

## 8. Website
**Live homepage:** dealmaking process, owner CTAs, Insights about brand/operator selection, success-based owner pricing, annual brand/operator access. **Missing:** any ADP / AI demand wedge. **Repo v9:** still “Brand & Operator Selection” title — inconsistency. **Score 5.5/10 MIXED.**

## 9. Products (summary)
ADP: PILOT_READY / public lag. Explorers/Census: depth proof, not lead wedge. Alignment snapshots: supporting. Owner/Hotel Intelligence: emerging public. Brand AI: separate surface; do not confuse with ADP GTM. Census: scale proof.

## 10. ADP
Ready enough to discuss MARKET_TEST pilots with warm accounts. Not ready enough to claim public product-led growth. Scorecard SPEC exists; instances NOT_CONNECTED.

## 11. Owner dealmaking
Live site supports identity. Conversion-grade proof thin. Warm > systematic.

## 12. LinkedIn
34 scraped posts: Joan 25 / AO 9; 0 impressions; likes+comments only. Joan outperforms AO. Commercial outcomes UNKNOWN. IMPROVE founder channel; do not scale AO vanity.

## 13. SEO
Snapshot: home 6 clicks; branded residences 744 impr / 0 clicks. MIXED_WEAK. Defend owner-intent; no new vanity program.

## 14. Measurement
GA4 553 sessions (Jun–Sep), direct-heavy, organic 19. Attribution chain BROKEN. Score 2/10.

## 15. Proof
Conversion-grade public proof MISSING. Best near-term assets: instrumented pilot narrative, ADP one-pager, methodology+credibility pairing.

## 16. Channels
Prioritize: Joan LinkedIn + warm outreach + dual-track public clarity. Park: paid. Secondary: SEO defense.

## 17. Competitive context
Compared to owner workflow tools, brand-selection tools, hotel intelligence, AI-visibility/GEO vendors, advisors. Dealality’s differentiation is **owner-controlled dealmaking + proprietary hotel intelligence**, not generic GEO SaaS. Risk: being misread as broker, listing site, or AI-visibility-only if ADP overshoots identity.

## 18–20. Opportunities / risks / unknowns
**Opportunities:** warm ADP; proof compounding; dual-track ADP visibility; attribution repair.
**Risks:** strategy approval without pipeline; ADP over-dominates brand; another unattributable month.
**Unknowns:** named targets; LI commercial ROI; live analytics; CTA conversion.

## 21–22. Strategic recommendation
**AMEND** prior thesis (not REJECT). Revised thesis in \`13_STRATEGY_CHALLENGE.md\`.

## 23. NOW / NEXT / LATER / PARK
See \`15_REVISED_PRIORITY_ROADMAP.md\`.

## 24. What Joan needs to decide next
1. Acknowledge evidence gaps (esp. no live GA4/GSC/CRM) before strategy lock.
2. Provide/confirm named warm ADP accounts when ready (still not a strategy approval).
3. Do **not** approve full strategy until he accepts the amended thesis & confidence labels.
`,
    '01_SOURCE_COMPLETENESS.md': `# Source Completeness\n\n**Total relevant:** ${sc.totalRelevant}\n**Reviewed:** ${sc.totalReviewed}\n**Missing/unavailable:** ${sc.totalMissingOrUnavailable}\n**Coverage:** ${sc.coveragePercent}%\n**Fresh/current (approx):** ${sc.freshCurrentApprox}\n**Stale/point-in-time (approx):** ${sc.staleOrPointInTimeApprox}\n**Baseline confidence:** ${sc.baselineConfidence}\n\n${sc.honesty}\n\n| Source | Group | Available | Reviewed | Freshness | Confidence |\n|---|---|---|---|---|---|\n${sources.map((s) => `| ${s.source} | ${s.group} | ${s.available} | ${s.reviewed} | ${s.freshness} | ${s.confidence} |`).join('\n')}\n`,
    '02_LIVE_DATA_REFRESH.md': `# Live Data Refresh\n\n${validation.liveRefresh.attempted.map((a) => `- **${a.source}:** ${a.status}${a.reason ? ` — ${a.reason}` : ''}${a.at ? ` (${a.at})` : ''}`).join('\n')}\n\nGA4/GSC/LinkedIn remain **STALE** snapshots (2026-06-01→09-06).\n`,
    '03_WEBSITE_DEEP_REVIEW.md': `# Website Deep Review\n\n## Live homepage (dealality.com) — fetched 2026-09-07
**Good:** Clear owner dealmaking promise (“Find the Best Path for Your Hotel”); confidential process framing; side-by-side comparison; founder quote; Insights cards; role-based starting points (owners success-based; brands/operators annual).
**Bad / missing:** No ADP / AI demand positioning / demand-leak language anywhere in first viewport or primary sections; Insights still brand/operator selection education; cannot measure form→opportunity outcomes from public fetch.
**Confusing:** Repo \`dealality-landing-v9.html\` title still “Hotel Brand & Operator Selection, Done Privately” while live Webflow homepage is dealmaking-forward — dual surfaces risk.
**CTAs:** “Explore your hotel opportunity” / “Start an opportunity review” — theoretically convert for dealmaking; **cannot measure** without GA4 events+CRM join.
**Platform URL:** \`/platform\` returned 404 on fetch.

## Repo landing v9
Selection/workflow software narrative; Brand/Operator Explorer emphasis; AI described as supporting capability only; beta CTA language. ADP commercialization urgency absent.

## Verdict
**MIXED 5.5/10** — better live dealmaking identity than Baseline V1 stated; ADP wedge still missing; measurement of conversion unknown.
`,
    '04_ANALYTICS_DEEP_READ.md': `# Analytics Deep Read (STALE snapshot)\n\n**Period:** 2026-06-01 → 2026-09-06 · **Live refresh:** NOT AVAILABLE\n\n## GA4
- Sessions total: **553**
- Direct: 277 · Google organic: 19 · paid/social named: 0 in simple map
- LinkedIn UTM not broken out → LinkedIn→site **UNKNOWN**
- Do **not** call session volume commercial success

## GSC
- Home: 6 clicks / 60 impr
- Soft brands guide: 2 clicks / 183 impr
- Branded residences: 0 clicks / **744** impr (vanity volume)
- Near-page-1 Spanish/CALA: defensive positions

## Separation
- **Business outcomes:** UNKNOWN (no revenue/pilot join)
- **Commercial intent:** WEAK evidence in organic
- **Channel activity:** Direct-dominant site traffic; organic thin
`,
    '05_LINKEDIN_DEEP_READ.md': `# LinkedIn Deep Read\n\n**Corpus available in metrics JSON:** 34 posts (not full claimed 70+ text corpus with outcomes). Impressions: **0 available**.\nJoan: 25 · AO company: 9 · Total likes 93 · comments 19\n\n## Patterns
- Stronger engagement proxies concentrate on **Joan** posts about owner brand/operator decision framing.
- AO company posts frequently 0 likes/comments in sample — low value as primary channel.
- Themes: CALA pipeline, brand vs operator choice, franchise negotiation — mostly **dealmaking track**, little ADP commercial education in this sample.
- CTA→conversation commercial outcome: **UNKNOWN**

## Top examples (by likes+weighted comments; not business proof)
See validation JSON / engagement summary URLs (Joan Jul–Sep 2026 posts).

## Classification
KEEP/IMPROVE Joan commercial education + warm activation · IMPROVE/STOP AO volume-as-primary · UNKNOWN commercial ROI
`,
    '06_PRODUCT_MARKETING_ALL_PRODUCTS.md': `# Product Marketing — All Material Products\n\n| Product | Maturity | Mkt readiness | Buyer | Lead? | Gap | Rec |\n|---|---|---|---|---|---|---|\n| ADP | PILOT_READY | Public lag | Owner/hotel | Near-term wedge | Invisible publicly; named pipeline | Instrument + warm pilots |\n| Brand Explorer | LIVE/Active depth | Supporting | Owner+brand | No | Not lead CTA | Use as proof of depth |\n| Operator Explorer | LIVE baseline | Supporting | Owner+operator | No | Same | Supporting |\n| Brand Alignment Snapshot | Supporting | Low public | Owner | No | Thin offer | Later |\n| Operator Alignment Snapshot | Supporting | Low public | Owner | No | Thin offer | Later |\n| Owner Intelligence | Emerging | Low | Owner | Partial dealmaking | Proof | Support dealmaking |\n| Hotel Intelligence | Emerging | Low | Owner | Partial | Proof | Support |\n| Hotel Property Census | Foundation | Scale proof | Internal+owner trust | No | Not a CTA | Cite scale carefully |\n| Brand AI Visibility | Separate MI page | Moat-sensitive | Brand/owner | Not ADP substitute | Confusion risk | Keep distinct from ADP GTM |\n`,
    '07_COMMERCIAL_PIPELINE_EVIDENCE.md': `# Commercial / Pipeline Evidence\n\n**CRM:** No unified CRM join to Marketing OS — **DATA_GAP**.\n**KNOWN PIPELINE:** Not established as named paid ADP pilots in OS.\n**KNOWN RELATIONSHIPS:** Founder network implied by JD-6B warm activation; GTM kits exist.\n**POTENTIAL TARGETS:** Strike lists / class-level candidates (not fabricated hotels).\n**UNKNOWN:** Wins/losses/objections/no-response rates for ADP.\n\nDo not fabricate a pipeline.\n`,
    '08_PROOF_AUDIT.md': `# Proof Audit\n\n| Asset | Class |\n|---|---|\n| Paid customer quotes (named outcomes) | MISSING |\n| Live paid ADP pilots with scorecards | MISSING / NOT_CONNECTED |\n| ADP decks / demo packs | USABLE |\n| Methodology / research scale | USABLE–STRONG (internal) |\n| Census/Explorer depth screenshots | USABLE |\n| Founder credibility | USABLE–STRONG |\n| Testimonials on live site | WEAK / possibly placeholder risk |\n| Longitudinal ADP customer publish | NOT EXTERNAL for CMO conversion yet |\n\n## Top 3 proof assets to increase conversion
${validation.topProofAssetsToIncreaseConversion.map((x, i) => `${i + 1}. ${x}`).join('\n')}
`,
    '09_COMPETITIVE_MARKET_REVIEW.md': `# Competitive / Market Review\n\nCategories (not forced 1:1 competitors):\n1. **Owner/development workflow tools** — claim process efficiency; Dealality overlaps on structured opportunity; differs with hospitality-specific affiliation decisions + intelligence depth.\n2. **Brand/operator selection / matching** — overlap with Explorer; Dealality differs with confidential owner-controlled process (live site). Risk: being seen as marketplace/broker.\n3. **Hotel intelligence platforms** — overlap on data; Dealality pairs intelligence to decision workflow.\n4. **AI visibility / GEO platforms** — overlap only if ADP is oversold as “rank on ChatGPT”; Dealality should sell **owner demand decisioning**, not generic GEO.\n5. **Consultants/advisors** — competitors for attention; Dealality positions as software + process alongside advisors (FAQ).\n\n**Positioning risk:** ADP push without dual-track narrative → misread as AI-visibility SaaS and erode dealmaking identity.\n`,
    '10_HISTORICAL_ACTIVITY_EFFECTIVENESS.md': `# Historical Activity Effectiveness\n\n| Initiative | Intent | Strategy sound? | Execution | Evidence | Result | Class |\n|---|---|---|---|---|---|---|\n| Joan LinkedIn | Authority + demand | YES | PARTIAL | Engagement only | UNKNOWN commercial | IMPROVE |\n| AO LinkedIn | Brand presence | MIXED | WEAK in sample | Low engagement | Weak | IMPROVE/STOP-as-primary |\n| Thought Leadership OS | Systematize content | YES | UNKNOWN | System exists | UNKNOWN | KEEP |\n| Target OS | Focus ICP | YES | PARTIAL | Class-level | Incomplete without CRM | IMPROVE |\n| SEO/Insights | Organic owners | YES_WITH_FILTER | MIXED | GSC thin/vanity | Weak acquisition | IMPROVE |\n| Website v9 / live Webflow | Public story | YES | PARTIAL | Live better than v9 title | Dealmaking clearer; ADP missing | IMPROVE |\n| Prior CTAs / beta | Conversion | MIXED | UNKNOWN | DATA_GAP | UNKNOWN | IMPROVE |\n| ADP materials | Pilot sales | YES | GOOD assets | Decks/demo | Not yet attributed closes | KEEP |\n| Warm outreach/advisor | Pipeline | YES | PARTIAL | Kits exist | Named outcomes DATA_GAP | IMPROVE |\n| Brand visibility without ADP | Awareness | NO as primary now | N/A | Story lag historically | Distracts | STOP as primary |\n`,
    '11_BASELINE_SCORE_VALIDATION.md': `# Baseline Score Validation\n\n| Score | Prior | Revised | Keep? | Why |\n|---|---|---|---|---|\n| Overall | 4.5 | **4.8** | No | Live site nuance |\n| Website | 5 | **5.5** | No | Live dealmaking stronger; ADP still missing |\n| LinkedIn | 5 | 5 | Yes | Still IMPROVE |\n| SEO | 4 | 4 | Yes | No live refresh |\n| ADP | 6 | 6 | Yes | Unchanged |\n| Owner acquisition | 4 | **4.5** | No | Live narrative help |\n| Measurement | 2 | 2 | Yes | Still broken/stale |\n\nDo not preserve 4.5 for inertia — revised to 4.8 with MEDIUM confidence.\n`,
    '12_FINDING_CONFIDENCE.md': `# Finding Confidence\n\n| Finding | Confidence |\n|---|---|\n| Dual-track locks are real and binding | HIGH_CONFIDENCE |\n| Live homepage is dealmaking-forward | HIGH_CONFIDENCE |\n| ADP absent from live homepage | HIGH_CONFIDENCE |\n| Attribution chain BROKEN | HIGH_CONFIDENCE |\n| Named CRM ADP pipeline DATA_GAP | HIGH_CONFIDENCE |\n| GA4/GSC quantitative picture | MEDIUM_CONFIDENCE (STALE) |\n| LinkedIn commercial effectiveness | LOW_CONFIDENCE |\n| “Selection-era” as blunt live-site label | REJECTED / superseded — use precise language |\n| Warm ADP is highest near-term revenue wedge | MEDIUM_CONFIDENCE (locks+product; pipeline unknown) |\n| Website restructure must be NOW vs NEXT | MEDIUM_CONFIDENCE → prefer PREPARE after named warm+instrumentation |\n`,
    '13_STRATEGY_CHALLENGE.md': `# Strategy Challenge\n\n## Prior thesis
${validation.strategyChallenge.priorThesis}\n\n## Decision: **${validation.strategyChallenge.decision}**\n\n### Why
${validation.strategyChallenge.why.map((x) => `- ${x}`).join('\n')}\n\n## Revised thesis
${validation.strategyChallenge.revisedThesis}\n\n**Do not ask Joan to approve strategy in this phase.**\n`,
    '14_REVISED_EXECUTIVE_ASSESSMENT.md': `# Revised Executive Assessment\n\n**Health:** 4.8/10 · **Confidence:** MEDIUM · **Strategy state:** PENDING REVIEW (approval not requested)\n\nCentral problem (refined): Public GTM still fails to make the ADP wedge visible and measurable, even though live dealmaking identity is clearer than Baseline V1 assumed; without named warm pipeline, proof instrumentation, and attribution, Helena cannot learn what works.\n`,
    '15_REVISED_PRIORITY_ROADMAP.md': `# Revised Priority Roadmap\n\n## NOW
1. Evidence honesty / source coverage acceptance (this pack)
2. Named warm ADP accounts (when Joan ready) — not strategy lock
3. ADP Pilot Scorecard mandatory PREPARE
4. P0 attribution TWIs approve/hold PREPARE
5. Dual-track ADP visibility PREPARE notes for site (no build)

## NEXT
- Joan LinkedIn commercial education reset execution
- Warm ABM follow-through once names exist
- Selective ADP public module brief

## LATER
- Website dual-track implementation
- Live GA4/GSC connectors
- OS↔console sync

## PARK
- Paid acquisition
- EXECUTE / recurring Helena
- Brand/operator primary acquisition push
`,
    '16_CONSOLE_SOURCE_COVERAGE_UPDATE.md': `# Console Source Coverage Update\n\nExecutive Assessment shows SOURCE COVERAGE strip:\nReviewed ${sc.totalReviewed}/${sc.totalRelevant} · Fresh~${sc.freshCurrentApprox} · Stale~${sc.staleOrPointInTimeApprox} · Missing~${sc.totalMissingOrUnavailable} · Confidence ${sc.baselineConfidence}\n\nDrill-down on Baseline tab / coverage cards.\n`,
    '17_GITLEAKS_CLEANUP.md': `# Gitleaks Cleanup\n\nRemoved exact Airtable \`rec…\` IDs from committed founder-decision JSON/MD artifacts.\nCanonical decision IDs retained (e.g. \`DEC-2026-09-07-D1-POSITIONING-DIRECTION\`).\nRecord IDs remain in Airtable / may exist in local gitignored state only.\nNo global weakening of airtable-api-key rule.\n`,
    '18_TEST_RESULTS.md': `# Test Results\n\nPending post-UI automated run in this phase.\n`,
  };

  for (const [name, body] of Object.entries(files)) {
    fs.writeFileSync(path.join(OUT, name), body);
  }
  return { out: OUT, coveragePercent: sc.coveragePercent, decision: validation.strategyChallenge.decision, health: validation.revisedScores.overallMarketingHealth.revised };
}

console.log(JSON.stringify({ ok: true, ...write() }, null, 2));
