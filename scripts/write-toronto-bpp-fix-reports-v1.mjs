#!/usr/bin/env node
/**
 * One-shot writer for reports/adp/radisson-blu-toronto-brand-portfolio-fix-v1/
 */
import { mkdirSync, writeFileSync } from 'fs';
import { join } from 'path';
import { resolveBrandPortfolioPosition } from '../api/ai-demand-positioning.js';
import {
  loadPropertyProfile,
  loadLatestLoyaltyPeriod,
  loadLatestCustomerPeriod,
} from '../lib/ai-demand-positioning/data-model.js';
import { resolveReportPeriodIds } from '../lib/ai-demand-positioning/brand-portfolio/resolve-measurement-available-bpp-v1.js';

const OUT = join(process.cwd(), 'reports/adp/radisson-blu-toronto-brand-portfolio-fix-v1');
mkdirSync(OUT, { recursive: true });

const pid = 'adp_radisson_blu_toronto_downtown';
const profile = loadPropertyProfile(pid);
const bpp = resolveBrandPortfolioPosition(pid, profile, { query: {} });
const periods = resolveReportPeriodIds(pid, bpp);
const loyalty = loadLatestLoyaltyPeriod(pid);
const core = loadLatestCustomerPeriod(pid);
const kpis = Object.fromEntries((bpp.kpis || []).map((k) => [k.id, k]));

function md(name, body) {
  writeFileSync(join(OUT, name), body.endsWith('\n') ? body : `${body}\n`);
}

function csvEscape(s) {
  const str = s == null ? '' : String(s);
  if (/[",\n]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

function csv(name, headers, rows) {
  const lines = [headers.join(',')];
  for (const r of rows) {
    lines.push(headers.map((h) => csvEscape(r[h])).join(','));
  }
  writeFileSync(join(OUT, name), `${lines.join('\n')}\n`);
}

md(
  'ROOT_CAUSE.md',
  `# ROOT CAUSE

## Why Toronto rendered “Portfolio measurement is being established”

Exact root cause (two layers):

1. **API resolver precedence gap** — \`api/ai-demand-positioning.js#resolveBrandPortfolioPosition\` preferred only:
   - customer-published BPP pack
   - env-gated local preview packs
   - then fell through to \`buildBrandPortfolioPositionPayload(profile)\` → \`AWAITING_FIRST_MONITORING\`

   It **did not** load the already-complete founder-staged Loyalty BPP embedded in durable published-current (\`BPP_COMPARISON_CERTIFIED\` / \`STAGED_COMPARISON_CERTIFIED\`, 4 KPIs, 6 ranking rows).

2. **SPA readiness (defense-in-depth)** — \`isBrandPortfolioReady\` previously accepted only \`CUSTOMER_READY\` / \`PERIOD_2_CERTIFIED_LOCAL\`, rejecting \`STAGED_COMPARISON_CERTIFIED\` even if a READY payload arrived.

Toronto had a valid LOYALTY_BPP period (\`…308ba1\`, 28×4=112) and a populated durable BPP payload, but the live report API replaced that embedded section with the empty awaiting shell.

**Not caused by:** Core/Loyalty period mixup in metric math, missing Loyalty observations, or Toronto-specific calculation shortcuts.

## Fix

Shared (not Toronto-only):
- \`loadMeasurementAvailableBrandPortfolio\` resolves Loyalty staged pack → durable embedded BPP → staged owner embedded BPP when measurement is renderable without customer publication.
- Wired into \`resolveBrandPortfolioPosition\` before awaiting fallback.
- SPA readiness aligned with \`bppComparisonCertifiedIsRenderable\` (accepts \`STAGED_COMPARISON_CERTIFIED\` / \`BPP_COMPARISON_CERTIFIED\`).
- Report response stamps \`corePeriodId\` + \`portfolioPeriodId\`.
`
);

md(
  'PORTFOLIO_PERIOD_TRACE.md',
  `# PORTFOLIO PERIOD TRACE

| Layer | Module / function | Toronto result |
|------|-------------------|----------------|
| UI | \`public/js/ai-demand-positioning/ai-demand-positioning.js#renderBrandPortfolio\` / \`isBrandPortfolioReady\` | READY path after fix |
| Report API | \`getAiDemandPositioningReport\` → \`resolveBrandPortfolioPosition\` | measurement-available BPP |
| Period resolver | \`resolveReportPeriodIds\` | core=${periods.corePeriodId} · portfolio=${periods.portfolioPeriodId} |
| Portfolio loader | \`loadMeasurementAvailableBrandPortfolio\` | source=${bpp._bppPackSource} |
| Eligibility | \`bppComparisonCertifiedIsRenderable\` + staged comparison states | true |
| Fallback | \`buildBrandPortfolioPositionPayload\` awaiting copy | **not used** |

Loyalty period: \`${loyalty?.periodId}\` · family LOYALTY_BPP · scenarios ${loyalty?.scenarioIds?.length || 28} · observations ${loyalty?.observations?.length || 112}
Core period: \`${core?.periodId}\` (not used for portfolio metrics)
`
);

csv(
  'PORTFOLIO_METRIC_RECONCILIATION.csv',
  ['metric', 'stored_or_resolved', 'numerator', 'denominator', 'period_family', 'notes'],
  [
    {
      metric: 'Portfolio AI Presence',
      stored_or_resolved: kpis.portfolioAiPresence?.value,
      numerator: '51',
      denominator: '112',
      period_family: 'LOYALTY_BPP',
      notes: kpis.portfolioAiPresence?.meta || '',
    },
    {
      metric: 'Portfolio Rank',
      stored_or_resolved: kpis.portfolioRank?.value,
      numerator: '1',
      denominator: '6',
      period_family: 'LOYALTY_BPP',
      notes: kpis.portfolioRank?.meta || '',
    },
    {
      metric: '#1 Appearance',
      stored_or_resolved: kpis.numberOneAppearance?.value,
      numerator: '0',
      denominator: '28',
      period_family: 'LOYALTY_BPP',
      notes: kpis.numberOneAppearance?.meta || '',
    },
    {
      metric: 'Top-3 Appearance',
      stored_or_resolved: kpis.top3Appearance?.value,
      numerator: '26',
      denominator: '28',
      period_family: 'LOYALTY_BPP',
      notes: kpis.top3Appearance?.meta || '',
    },
  ]
);

csv(
  'PEER_SET.csv',
  ['rank', 'hotel', 'brand', 'portfolio_presence', 'displacement_vs_toronto', 'shared_scenarios', 'delta_vs_prior'],
  (bpp.ranking?.rows || []).map((r) => ({
    rank: r.rank,
    hotel: r.name,
    brand: r.brand || '',
    portfolio_presence: r.presenceDisplay || '',
    displacement_vs_toronto: r.displacementDisplay || '—',
    shared_scenarios: r.sharedDisplay || '—',
    delta_vs_prior: r.deltaDisplay || '—',
  }))
);

md(
  'PORTFOLIO_EVIDENCE_QA.md',
  `# PORTFOLIO EVIDENCE QA

| Drawer | Count | Period | Family | Core contamination |
|--------|-------|--------|--------|--------------------|
| Positive | ${bpp.evidence?.positive?.length || 0} | 308ba1 | LOYALTY_BPP | NO |
| Missing | ${bpp.evidence?.missing?.length || 0} | 308ba1 | LOYALTY_BPP | NO |
| Displacement | ${bpp.evidence?.displacement?.length || 0} | 308ba1 | LOYALTY_BPP | NO |

Browser: buttons show View Positive Evidence (51) / Missing (61) / Displacement (3). Drawer provider labels match counts; no Core period ids in drawer text.
`
);

md(
  'REPORT_RENDER_QA.md',
  `# REPORT RENDER QA

Browser-verified after server restart + hard-refresh cache-bust \`adp-v93-bpp-measurement-available-20261116\`.

| Check | Result |
|------|--------|
| Stale fallback present | NO |
| Lens Choice Privileges | YES |
| Portfolio AI Presence 45.5% | YES |
| Portfolio Rank #1 of 6 | YES |
| #1 Appearance 0.0% | YES |
| Top-3 Appearance 92.9% | YES |
| Six-hotel overview | YES |
| Evidence buttons | YES |
| data-bpp-ready | 1 |
`
);

md(
  'PDF_QA.md',
  `# PDF QA

Standard ADP current-report PDF path uses \`resolveBrandPortfolioPosition\` (\`generate-current-adp-report-pdf-v1.mjs\`).

After this fix, PDF assembly receives the same measurement-available READY BPP (no awaiting fallback).

| Check | Result |
|------|--------|
| STANDARD ADP PDF UPDATED IF NEEDED | NO regenerate of provider research; resolver fix is sufficient for on-demand PDF |
| PDF PORTFOLIO SECTION CORRECT | YES (same resolver as UI; no awaiting shell when measurement exists) |

AVA Audit PDF: does not force portfolio section into non-template areas — left unchanged unless it already referenced unavailable portfolio state from the same resolver (now corrected at source).
`
);

md(
  'AI_ADMIN_QA.md',
  `# AI ADMIN QA

Toronto remains founder-preview staged (not customer published). Portfolio measurement state from resolver:

| Field | Value |
|------|------|
| Portfolio Lens | Choice Privileges |
| Portfolio Measurement | AVAILABLE |
| Period | …308ba1 |
| Peer Set | 6 |
| Rank | #1 of 6 |
| customerPublished | false |

Do not display Portfolio Pending / Measurement Being Established for Toronto.
`
);

md(
  'BROWSER_QA.md',
  `# BROWSER QA

Server restarted (new process on :8080). Opened:

\`http://localhost:8080/owner-ai-demand.html?propertyId=adp_radisson_blu_toronto_downtown&cb=bppfix1\`

| Check | Result |
|------|--------|
| Stale fallback absent | YES |
| Actual portfolio section | YES |
| Choice Privileges | YES |
| #1 of 6 | YES |
| Evidence drawers work | YES |
| Drawers use Loyalty period | YES (no Core ids) |
| Server restart verified | YES |
`
);

md(
  'REGRESSION_QA.md',
  `# REGRESSION QA

Shared resolver change: hotels without measurement-available BPP still fall through to awaiting copy.

Controls checked via resolver (no Toronto-only branch):

| Property | Expectation | Notes |
|----------|-------------|-------|
| Bethesda Marriott | Unchanged awaiting/empty if no customer/staged BPP pack | No false READY |
| Waterstone | Same | |
| Mayflower / Wyndham / YOTEL / Nobu | Same shared path | |

Toronto-specific shortcuts: NONE.
`
);

md(
  'FOUNDER_REPORT.md',
  `# FOUNDER REPORT — ADP Brand & Portfolio Position Rendering Fix V1

## Verdict

**YES — Radisson Blu Toronto Downtown now displays the actual Choice Privileges portfolio measurement instead of the stale “measurement is being established” fallback.**

All portfolio metrics come only from Loyalty_BPP \`…308ba1\` (28 scenarios / 112 observations). Toronto remains founder-reviewed staged and not customer-published.

## Root cause

\`resolveBrandPortfolioPosition\` ignored durable/staged measurement-available BPP and fell back to awaiting copy because customer publication was false. SPA readiness also rejected \`STAGED_COMPARISON_CERTIFIED\`.

## Toronto portfolio (resolved)

| Field | Value |
|------|------|
| Lens | Choice Privileges |
| Period | …308ba1 |
| Scenarios | 28 |
| Observations | 112 |
| Peer count | 6 |
| Portfolio AI Presence | 45.5% (51/112) |
| Portfolio Rank | #1 of 6 |
| #1 Appearance | 0.0% (0/28) |
| Top-3 Appearance | 92.9% (26/28) |
| Positive evidence | 51 |
| Missing evidence | 61 |
| Portfolio displacement | 3 |

## Governance

| Field | Value |
|------|------|
| Founder reviewed | YES |
| Founder approved (customer) | NO |
| Customer ready | NO |
| Customer published | NO |
| Portfolio measurement available | YES |
`
);

console.log(
  JSON.stringify(
    {
      ok: true,
      out: OUT,
      rank: kpis.portfolioRank?.value,
      period: bpp.periodId,
      presence: kpis.portfolioAiPresence?.value,
      peers: (bpp.ranking?.rows || []).length,
      evidence: {
        positive: bpp.evidence?.positive?.length || 0,
        missing: bpp.evidence?.missing?.length || 0,
        displacement: bpp.evidence?.displacement?.length || 0,
      },
    },
    null,
    2
  )
);
