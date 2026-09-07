#!/usr/bin/env node
/**
 * Writes Helena CMO Baseline V1 machine packs + founder markdown reports.
 * SoT = embedded structured objects → JSON + MD under reports/helena-cmo-baseline-v1/
 * EXECUTE / recurring remain OFF. No live analytics refresh.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'reports/helena-cmo-baseline-v1');

function scoreRow(score, currentState, why, evidence, biggestGap, opportunity, confidence) {
  return { score, currentState, why, evidence, biggestGap, opportunity, confidence };
}

const baseline = {
  schemaVersion: 'helena-cmo-baseline-v1',
  meta: {
    baselineVersion: 'v1',
    lastDeepReviewDate: '2026-09-07',
    confidence: 'MEDIUM_HIGH_FOR_DIRECTION_MEDIUM_FOR_PUBLIC_GTM_EFFECTIVENESS',
    dataFreshness: 'POINT_IN_TIME_2026-09-07',
    strategyState: 'STRATEGY_PENDING_FOUNDER_REVIEW',
    executeEnabled: false,
    recurringHelenaEnabled: false,
    marketingOsSync: false,
    deepBaselineCadenceNote:
      'Deep baseline is one-time until quarterly/major-event refresh. Weekly Helena updates deltas only.',
  },
  workflow: [
    'RESEARCH',
    'BASELINE',
    'DIAGNOSE',
    'SYNTHESIZE_FINDINGS',
    'STRATEGY',
    'PRIORITIZE',
    'PLAN',
    'PREPARE',
    'FOUNDER_APPROVAL',
    'MANUAL_EXECUTION',
    'MEASURE',
    'LEARN',
    'ADJUST',
  ],
  overallMarketingHealth: {
    score: 4.5,
    label: 'GOVERNANCE AHEAD OF PUBLIC GTM',
    rationale:
      'Founder locks + Operating Law give directional clarity (~7–8/10 governance), but public story, proof, attribution, and conversion architecture remain immature (~3/10). Prior maturity pack ~4.3/10; locks improved governance without fixing public commercialization.',
  },
  sourceCoverage: [
    {
      area: 'Company / strategy',
      sourcesAvailable: [
        'reports/helena-cmo-founder-decisions/*',
        'reports/helena-cmo-operating-law-v1/*',
        'reports/helena-cmo-phase-3c1/*',
        'reports/helena-cmo-strategy-lock-and-maturity/*',
      ],
      sourcesReviewed: [
        'D1–D5 + JD-6B locks',
        'Operating Law v1',
        '3c1 reconciled strategy',
        'strategy maturity ~4.3',
      ],
      sourcesMissing: ['Live Meet recordings completeness', 'Final public category phrase (OPEN under D1)'],
      freshness: '2026-09-07',
      confidence: 'HIGH',
    },
    {
      area: 'Products',
      sourcesAvailable: ['AGENTS.md product map', 'ADP/Explorer/Census docs', 'phase-3c1 ADP GTM'],
      sourcesReviewed: ['ADP maturity PILOT_READY', 'Explorer/Census as platform depth', 'Alignment snapshots'],
      sourcesMissing: ['Unified product marketing readiness scorecard in Marketing OS'],
      freshness: '2026-09-07',
      confidence: 'MEDIUM_HIGH',
    },
    {
      area: 'Website',
      sourcesAvailable: ['public/marketing/dealality-landing-v9.html', 'README', 'Week01 website PREPARE brief', 'manual-cycle website review'],
      sourcesReviewed: ['v9 landing', 'dual-track restructure PREPARE brief', 'manual-cycle B restructure verdict'],
      sourcesMissing: ['Live Webflow CMS crawl today', 'Full mobile UX lab session'],
      freshness: 'POINT_IN_TIME_REPO_V9',
      confidence: 'MEDIUM',
    },
    {
      area: 'Messaging',
      sourcesAvailable: ['PACK_CORE messaging-guide', 'D1 positioning lock', 'NAMING_AND_COPY_GUIDE', 'phase-0 message evolution'],
      sourcesReviewed: ['Clear-not-clever principles', 'D1 territory lock', 'historical Deal CaptureT messaging'],
      sourcesMissing: ['Founder-approved final tagline'],
      freshness: 'MIXED',
      confidence: 'MEDIUM',
    },
    {
      area: 'LinkedIn / social',
      sourcesAvailable: [
        '04_SOCIAL_ANALYTICS/LINKEDIN_ENGAGEMENT_SUMMARY.md',
        'linkedin_posts_metrics.json',
        'JD_6B_003',
        'Week01 LinkedIn pack',
        'calendar export',
      ],
      sourcesReviewed: ['34 scraped posts sample', '61 published items commercial outcome UNKNOWN (cycle)', 'Joan role lock'],
      sourcesMissing: ['Complete impression series', 'CRM-attributed pipeline outcomes'],
      freshness: 'POINT_IN_TIME_JUN_SEP_2026',
      confidence: 'MEDIUM_LOW_FOR_COMMERCIAL_EFFECTIVENESS',
    },
    {
      area: 'SEO / discovery',
      sourcesAvailable: ['GSC_SUMMARY.md', 'GSC snapshot JSON', 'Webflow insights catalog', 'Week01 SEO priority'],
      sourcesReviewed: ['GSC 2026-06-01→09-06', 'insights posts catalog'],
      sourcesMissing: ['Live GSC API', 'AI discovery measurement pack'],
      freshness: 'POINT_IN_TIME_2026-09-06',
      confidence: 'MEDIUM',
    },
    {
      area: 'Analytics / attribution',
      sourcesAvailable: ['GA4_SUMMARY', 'GA4 snapshot', 'Week01 measurement audit', 'tracking TWIs'],
      sourcesReviewed: ['GA4 PARTIAL', 'attribution BROKEN'],
      sourcesMissing: ['CRM↔Marketing OS join', 'email/outreach attribution', 'ADP pilot scorecard instances'],
      freshness: '2026-09-07',
      confidence: 'HIGH_THAT_GAP_EXISTS',
    },
    {
      area: 'Proof',
      sourcesAvailable: ['phase-3c1 proof strategy', 'ADP commercial evidence JSON', 'leak-audit demo pack', 'decks'],
      sourcesReviewed: ['Proof vacuum verdict from manual cycle', 'pilot scorecard SPEC only'],
      sourcesMissing: ['Conversion-grade named customer proof', 'public case metrics'],
      freshness: '2026-09-07',
      confidence: 'HIGH',
    },
    {
      area: 'Sales / commercial support',
      sourcesAvailable: ['docs/gtm-resources', 'strike lists', 'ADP one-pager PREPARE', 'outreach APIs'],
      sourcesReviewed: ['Warm ADP path', 'GTM pilot kits'],
      sourcesMissing: ['Named Week1 CRM targets (DATA_GAP)', 'Systematic ABM attribution'],
      freshness: '2026-09-07',
      confidence: 'MEDIUM',
    },
  ],
  executiveAssessment: {
    whereWeStand:
      'Dealality has strong product depth and newly locked dual-track GTM direction (ADP commercialization + owner dealmaking), but public marketing still largely sells a selection-era platform story. ADP is commercially real and pilot-ready, yet nearly invisible on the website. LinkedIn volume exists without attributed commercial outcomes. Measurement can partially observe traffic/engagement but cannot yet prove what marketing creates pilots or revenue.',
    overallHealth: { score: 4.5, label: 'GOVERNANCE AHEAD OF PUBLIC GTM' },
    topStrengths: [
      'Product depth: ADP, Brand/Operator Explorer, Census, Owner Intelligence',
      'Founder network + warm-account path for ADP pilots',
      'Locked strategy direction: owner-first, dual-track GTM, marketplace role, ICP v1, ADP pricing authority',
      'Operating Law + Marketing OS foundation (PREPARE governance, epistemic honesty)',
      'Research/intelligence differentiation vs generic hotel SaaS marketing',
    ],
    topOpportunities: [
      'Make ADP visible and urgent in public narrative without abandoning dealmaking',
      'Instrument paid pilots so proof compounds',
      'Fix attribution chain so learning is possible',
      'Reset Joan LinkedIn to commercial education + warm activation (not vanity engagement)',
      'Use existing Marketing OS assets (messaging, Target OS, Thought Leadership OS) instead of inventing more systems',
    ],
    whatIsNotWorking: [
      'Public story lag vs locked dual-track strategy',
      'Attribution chain LinkedIn→site→lead→pilot→revenue BROKEN',
      'Proof vacuum for conversion-grade GTM',
      'Website hierarchy underweights ADP urgency',
      'Engagement treated as proxy for commercial effectiveness',
    ],
    unknowns: [
      'Which LinkedIn themes create warm conversations vs noise',
      'True organic owner acquisition contribution of SEO/insights',
      'Which warm ADP accounts Joan will personally advance this week (named CRM DATA_GAP)',
      'Whether current landing CTAs convert owners into qualified conversations',
      'Live GA4/GSC trend since 2026-09-06 snapshot',
    ],
    centralProblem:
      'Public marketing still communicates a selection-era Dealality while the locked strategy is dual-track (ADP commercialization + owner dealmaking); ADP is commercially real but nearly invisible; outcomes are unattributed; proof is insufficient for conversion-grade GTM.',
    recommendedStrategyThesis:
      'Lead near-term marketing with ADP paid-pilot commercialization for warm owner/hotel accounts, while keeping owner-dealmaking as the durable identity track—public narrative, LinkedIn, and website hierarchy must reflect both tracks, with proof and attribution as non-optional learning systems.',
    pillars: [
      { id: 'P1', name: 'Dual-track public narrative' },
      { id: 'P2', name: 'ADP pilot commercialization + instrumentation' },
      { id: 'P3', name: 'Warm ABM / founder activation' },
      { id: 'P4', name: 'Measurement & attribution repair' },
      { id: 'P5', name: 'Proof compounding' },
    ],
    whatShouldHappenFirst: [
      'Founder reviews and accepts (or amends) this CMO strategy before tactical PREPARE approvals become normal workflow',
      'Confirm 3–7 named warm ADP pilot conversations',
      'Require ADP Pilot Scorecard at any pilot start',
      'Approve/hold P0 attribution tracking TWIs (PREPARE only)',
      'Finish website dual-track restructure PREPARE brief (no build this week)',
    ],
    founderReviewCta: 'REVIEW STRATEGY',
  },
  scorecard: {
    company_positioning: scoreRow(6, 'Territory locked; public phrase OPEN', 'D1 locks dealmaking territory but not final tagline; public still selection-forward', ['D1_POSITIONING_DIRECTION_LOCK', 'manual-cycle public story lag'], 'Final public category/urgency phrase', 'Approve dual-track public framing', 'HIGH'),
    owner_relevance: scoreRow(7, 'Owner-first doctrine locked', 'ICP + locks prioritize owners; public proof of owner outcomes thin', ['D4_ICP_V1_LOCK', 'JD-6B'], 'Owner outcome proof', 'Owner-specific ADP + dealmaking proof assets', 'HIGH'),
    urgency: scoreRow(4, 'Strategic urgency clear internally; public WEAK–MODERATE', 'Manual cycle: ADP urgency missing on site', ['manual-cycle-00'], 'Public ADP urgency', 'Lead with AI demand risk / opportunity for existing hotels', 'MEDIUM'),
    product_market_communication: scoreRow(4, 'Many products, unclear lead hierarchy publicly', 'Explorers/Census deeper than public hierarchy; ADP under-communicated', ['landing v9', 'product map'], 'Lead product hierarchy', 'ADP + dealmaking as dual lead; others support', 'MEDIUM'),
    adp_commercialization_readiness: scoreRow(6, 'Product PILOT_READY; public GTM lagging', 'Pricing MARKET_TEST; scorecard SPEC; named pipeline DATA_GAP', ['D5', 'Week01 ADP packs'], 'Named warm pipeline + instrumentation', 'Run instrumented paid pilots', 'HIGH'),
    dealmaking_commercialization_readiness: scoreRow(5, 'Identity track locked; conversion proof thin', 'Signals/ABM packs exist; public dealmaking CTA path mixed', ['D1', 'D2', 'Week01 owner signals'], 'Conversion-grade dealmaking proof', 'Warm dealmaking ABM with tracked outcomes', 'MEDIUM'),
    website: scoreRow(5, 'MIXED — v9 exists; hierarchy lag', 'Restructure class B recommended; ADP visibility weak; build NOT this week', ['landing v9', '04_WEBSITE_RESTRUCTURE_BRIEF'], 'Dual-track hierarchy', 'PREPARE then later implement', 'MEDIUM'),
    messaging: scoreRow(5, 'Principles exist; consistency mixed', 'Clear-not-clever + locks; historical messaging still present', ['PACK_CORE messaging-guide', 'D1'], 'Approved dual-track message architecture', 'Lock short public message set', 'MEDIUM'),
    founder_linkedin: scoreRow(5, 'Volume exists; commercial role reset needed', 'JD-6B-003 locks Joan role reset; outcomes UNKNOWN', ['JD_6B_003', 'LinkedIn summary'], 'Attributed conversations', 'Commercial education + warm activation cadence', 'MEDIUM_LOW'),
    seo_discovery: scoreRow(4, 'Thin clicks; mixed intent payoff', 'GSC: home 6 clicks; branded residences 744 impr / 0 clicks', ['GSC_SUMMARY'], 'Owner-intent pages that convert', 'Defend CALA/owner pages; stop vanity volume', 'MEDIUM'),
    proof: scoreRow(3, 'Proof vacuum', 'Neither ADP nor dealmaking has conversion-grade public proof', ['manual-cycle-00', '10_PROOF_GAPS'], 'Named outcomes + methodology proof', 'Pilot scorecard → publishable proof later', 'HIGH'),
    conversion_architecture: scoreRow(4, 'Paths exist; join broken', 'Forms/outreach exist; attribution/CRM join DATA_GAP', ['Week01 measurement'], 'End-to-end conversion map', 'P0 tracking TWIs + CRM join', 'HIGH'),
    measurement_attribution: scoreRow(2, 'GA4 PARTIAL; attribution BROKEN', 'Explicit Week01 audit', ['12_MEASUREMENT_ATTRIBUTION_AUDIT'], 'LinkedIn→revenue chain', 'Approve P0 TWIs then implement', 'HIGH'),
    owner_acquisition: scoreRow(4, 'Warm path stronger than systematic', 'Named Week1 targets DATA_GAP', ['Week01 ADP focus'], 'Named warm list + follow-up system', 'Joan-led warm ADP activation', 'MEDIUM'),
    brand_operator_acquisition: scoreRow(4, 'Secondary track; not primary near-term', 'D4 secondary commercial; public still brand-heavy historically', ['D4'], 'Avoid distraction from owner ADP', 'Secondary ABM only when owner track instrumented', 'MEDIUM'),
    channel_strategy: scoreRow(5, 'Channels known; roles not enforced publicly', 'LinkedIn primary; SEO secondary; paid PARK', ['3c1 LinkedIn/ABM'], 'Channel role discipline', 'Enforce stop list', 'MEDIUM'),
    marketing_operating_system: scoreRow(6, 'OS seeded; console local-only', 'Marketing OS exists; Founder Console not syncing writes', ['phase-2c', 'console docs'], 'OS↔console sync later', 'Use OS as source library now', 'HIGH'),
    cmo_execution_readiness: scoreRow(5, 'PREPARE-ready; EXECUTE intentionally OFF', 'Law + weekly pack + console; strategy review gate missing until V2', ['operating law', 'Week01'], 'Founder strategy acceptance', 'Strategy review then tactical PREPARE', 'HIGH'),
  },
  domains: {
    website: {
      verdict: 'MIXED',
      score: 5,
      rationale:
        'Landing v9 is a real production surface with clearer structure than early archives, but the public narrative hierarchy still underweights ADP urgency relative to locked dual-track strategy. Manual cycle recommended page restructure (B), not full rebuild. Build is correctly NOT this week.',
      good: ['v9 exists and is maintained as current', 'Audience pages exist (owners/brands/operators)', 'Analytics helpers present on landing'],
      bad: ['ADP nearly invisible / low urgency', 'Selection-era hierarchy still dominates first read', 'Proof/trust conversion assets thin', 'Attribution from site actions incomplete'],
    },
    linkedin: {
      verdict: 'IMPROVE',
      whatWorked: ['Sustained publishing cadence', 'Owner-relevant CALA/brand themes', 'Founder voice capability'],
      whatDidNot: ['Treating likes as business outcomes', 'Weak ADP commercial education publicly', 'Unclear CTA→conversation path'],
      unknown: ['Which posts create warm intros or pilots', 'True impression reach (often null)', 'Company vs founder channel ROI'],
      why: 'Volume without attribution cannot guide spend of founder attention.',
    },
    seo: {
      verdict: 'MIXED_WEAK',
      score: 4,
      rationale:
        'GSC Jun–Sep 2026 shows very thin click volume (home 6 clicks). Some owner-intent articles get impressions without clicks (branded residences 744 impr / 0 clicks). Near-page-1 Spanish/CALA pages are defensive assets. Not a primary near-term acquisition engine.',
    },
    adp: {
      verdict: 'PRODUCT_AHEAD_OF_PUBLIC_GTM',
      score: 6,
      rationale:
        'ADP is PILOT_READY with MARKET_TEST pricing authority and decks/demo packs, but public commercialization, named warm pipeline, and live scorecard instances lag.',
    },
    ownerAcquisition: {
      verdict: 'WARM_PARTIAL_SYSTEMATIC_WEAK',
      score: 4,
      rationale: 'Warm founder path is the real near-term channel; systematic owner acquisition and proof are weak.',
    },
    measurement: {
      verdict: 'WEAK',
      score: 2,
      canMeasure: ['Some GA4 sessions (PARTIAL)', 'Some GSC clicks/impr (snapshot)', 'Some LinkedIn likes/comments when present'],
      partiallyMeasure: ['Landing analytics events', 'Marketing OS performance fields'],
      cannotMeasure: ['LinkedIn→site→lead→pilot→revenue', 'ADP pilot commercial outcomes in OS', 'CRM join completeness'],
    },
  },
  findings: {
    strengths: [
      { id: 'S1', title: 'Product depth', evidence: 'ADP/Explorers/Census/Owner Intelligence in product' },
      { id: 'S2', title: 'Founder network', evidence: 'JD-6B warm ADP activation priority' },
      { id: 'S3', title: 'Locked dual-track direction', evidence: 'D1–D5 + JD-6B' },
      { id: 'S4', title: 'Operating Law / epistemic honesty', evidence: 'operating-law-v1 + console DATA_GAP rules' },
      { id: 'S5', title: 'Research differentiation', evidence: 'intelligence products vs generic hotel SaaS' },
      { id: 'S6', title: 'Marketing OS corpus recovered', evidence: 'phase-0 PACK_CORE + analytics snapshots' },
      { id: 'S7', title: 'Pilot kits exist', evidence: 'gtm-resources + ADP decks' },
    ],
    improvements: [
      { issue: 'Public story lag', evidence: 'manual-cycle-00', consequence: 'Market hears wrong Dealality', opportunity: 'Dual-track narrative', urgency: 'NOW' },
      { issue: 'Attribution broken', evidence: 'Week01 measurement audit', consequence: 'Cannot learn', opportunity: 'P0 TWIs', urgency: 'NOW' },
      { issue: 'Proof vacuum', evidence: 'manual-cycle + proof gaps', consequence: 'Hard to convert cold', opportunity: 'Instrumented pilots', urgency: 'NOW' },
      { issue: 'ADP public invisibility', evidence: 'website review', consequence: 'Misses near-term revenue wedge', opportunity: 'Website hierarchy PREPARE', urgency: 'NOW' },
      { issue: 'LinkedIn commercial reset incomplete', evidence: 'JD-6B-003', consequence: 'Founder time under-leveraged', opportunity: 'Role-aligned content', urgency: 'NEXT' },
      { issue: 'Named warm pipeline DATA_GAP', evidence: 'Week01 ADP focus', consequence: 'Stays class-level targeting', opportunity: 'Joan named list', urgency: 'NOW' },
      { issue: 'SEO vanity volume risk', evidence: 'GSC branded residences', consequence: 'Impressions without owner payoff', opportunity: 'Intent filter', urgency: 'LATER' },
      { issue: 'OS↔console write not connected', evidence: 'console contract', consequence: 'Decisions local-only', opportunity: 'Later sync', urgency: 'LATER' },
      { issue: 'Final tagline OPEN', evidence: 'D1', consequence: 'Public phrase drift', opportunity: 'Founder lock later', urgency: 'LATER' },
      { issue: 'Underused OS assets', evidence: 'PACK_CORE Target/Thought Leadership OS', consequence: 'Reinvention waste', opportunity: 'Reuse before rebuild', urgency: 'NEXT' },
    ],
    unknowns: [
      'Named Week1 ADP targets',
      'Commercial outcome of published LinkedIn corpus',
      'Live analytics since snapshot',
      'Landing CTA conversion quality',
      'Which insights pages create owner conversations',
    ],
  },
  historicalActivities: [
    { name: 'Joan LinkedIn publishing', wasGoodIdea: 'YES', executedWell: 'PARTIAL', generatedEvidence: 'ENGAGEMENT_ONLY', classification: 'IMPROVE', why: 'Keep cadence; reset to commercial education + warm activation; stop vanity KPI.' },
    { name: 'SEO / Insights content', wasGoodIdea: 'YES_WITH_FILTER', executedWell: 'MIXED', generatedEvidence: 'IMPRESSIONS_THIN_CLICKS', classification: 'IMPROVE', why: 'Defend owner-intent; stop volume without payoff.' },
    { name: 'Website landing iterations (v5→v9)', wasGoodIdea: 'YES', executedWell: 'PARTIAL', generatedEvidence: 'SURFACE_EXISTS', classification: 'IMPROVE', why: 'Restructure hierarchy for dual-track; no vanity rebuild.' },
    { name: 'Thought Leadership OS', wasGoodIdea: 'YES', executedWell: 'UNKNOWN', generatedEvidence: 'SYSTEM_EXISTS', classification: 'KEEP', why: 'Useful operating asset if subordinated to strategy.' },
    { name: 'Target OS', wasGoodIdea: 'YES', executedWell: 'PARTIAL', generatedEvidence: 'CLASS_LEVEL', classification: 'IMPROVE', why: 'Needs named warm CRM join for ADP week.' },
    { name: 'ADP decks / demo packs', wasGoodIdea: 'YES', executedWell: 'GOOD', generatedEvidence: 'SALES_ASSETS', classification: 'KEEP', why: 'Core commercial support for pilots.' },
    { name: 'Advisor / AO social posts', wasGoodIdea: 'MIXED', executedWell: 'PARTIAL', generatedEvidence: 'LOW_ENGAGEMENT_SAMPLE', classification: 'UNKNOWN', why: 'Insufficient commercial attribution.' },
    { name: 'Beta / generic CTAs', wasGoodIdea: 'MIXED', executedWell: 'UNKNOWN', generatedEvidence: 'DATA_GAP', classification: 'IMPROVE', why: 'CTAs must match MARKET_TEST / pilot intent.' },
    { name: 'Content calendar volume', wasGoodIdea: 'PARTIAL', executedWell: 'YES_VOLUME', generatedEvidence: 'PUBLISH_COUNT', classification: 'IMPROVE', why: 'Volume ≠ pipeline.' },
    { name: 'Brand visibility without ADP', wasGoodIdea: 'NO_AS_PRIMARY', executedWell: 'N/A', generatedEvidence: 'STORY_LAG', classification: 'STOP', why: 'Do not lead with selection-only story while ADP is the near-term wedge.' },
  ],
  diagnosis: {
    centralProblem:
      'Public marketing still communicates a selection-era Dealality while the locked strategy is dual-track (ADP commercialization + owner dealmaking); ADP is commercially real but nearly invisible; outcomes are unattributed; proof is insufficient for conversion-grade GTM.',
    whyExists: [
      'Strategy locks progressed faster than public narrative updates',
      'Content systems optimized for publishing cadence over commercial instrumentation',
      'ADP matured as product before marketing hierarchy caught up',
      'Attribution/CRM joins never became a hard gate',
    ],
    ifDoNothing: [
      'Founder time continues on unattributable content',
      'ADP pilots risk starting without proof systems',
      'Market keeps misunderstanding Dealality as selection-only',
      'Near-term revenue wedge under-supported publicly',
    ],
    leverage: [
      'Named warm ADP pilot activation + scorecard',
      'Dual-track public narrative (website PREPARE + LinkedIn reset)',
      'Attribution repair so learning compounds',
    ],
    doNotSolveYet: [
      'Full website rebuild',
      'Paid acquisition',
      'EXECUTE / recurring Helena',
      'Sales CSO takeover of CMO',
      'Final brand tagline perfection before dual-track hierarchy',
      'Broad brand/operator acquisition push',
    ],
  },
};

const strategy = {
  schemaVersion: 'helena-cmo-strategy-v1',
  state: 'STRATEGY_PENDING_FOUNDER_REVIEW',
  executeEnabled: false,
  recurringHelenaEnabled: false,
  thesis: baseline.executiveAssessment.recommendedStrategyThesis,
  who: 'Hotel owners (and owner-aligned developers) first; brands/operators secondary commercial.',
  problem: 'Owners need decision-grade clarity on AI demand exposure and affiliation/operator choices—Dealality currently under-communicates the AI demand wedge publicly.',
  urgency: 'Near-term revenue needs paid ADP pilot learning; public story lag wastes that window.',
  leadCapabilities: ['AI Demand Positioning (lead wedge)', 'Owner dealmaking intelligence (durable identity)', 'Explorers/Census as proof of depth'],
  adpAndDealmakingCoexistence:
    'ADP is the near-term commercialization wedge; owner dealmaking remains the durable category identity. Public surfaces must show both without pretending ADP is the whole company.',
  proofNeeded: ['Instrumented pilot outcomes', 'Methodology transparency', 'Owner-relevant before/after demand narrative (no fabricated metrics)'],
  channelsPrioritized: ['Founder LinkedIn + warm outreach', 'Website dual-track hierarchy', 'ABM warm', 'SEO defense (secondary)', 'PARK paid'],
  websiteRole: 'Explain dual-track; make ADP visible/urgent; support pilot CTA; do not rebuild this week.',
  linkedinRole: 'Joan: commercial education + warm activation per JD-6B-003; stop vanity engagement as success.',
  abmRole: 'Warm named accounts only until attribution works.',
  seoRole: 'Defend owner-intent; do not chase impression vanity.',
  stopDoing: [
    'Selection-only public lead story',
    'Publishing volume without commercial intent',
    'Pilot starts without scorecard',
    'Treating likes as pipeline',
    'Website rebuild this week',
  ],
  successMeasures: [
    'Named warm ADP conversations advancing',
    'Scorecard on every pilot start',
    'Attribution chain status moves from BROKEN toward PARTIAL/WORKING',
    'Public narrative hierarchy explicitly dual-track (even if PREPARE-only)',
    'Strategy accepted or amended by founder',
  ],
  pillars: baseline.executiveAssessment.pillars.map((p) => ({
    ...p,
    rationale:
      p.id === 'P1'
        ? 'Public surfaces must match locked strategy or demand is mis-educated.'
        : p.id === 'P2'
          ? 'Near-term revenue and learning depend on instrumented ADP pilots.'
          : p.id === 'P3'
            ? 'Warm founder path is the highest-probability conversion channel now.'
            : p.id === 'P4'
              ? 'Without measurement, CMO cannot improve.'
              : 'Proof compounds commercial trust; vacuum blocks conversion.',
  })),
  risks: [
    'Founder never reviews strategy → tactics continue unanchored',
    'ADP over-dominates brand identity',
    'Tracking work stalls → another unattributable month',
    'Named warm list never provided → class-level theater',
  ],
  dependencies: ['Founder strategy review', 'Named warm targets', 'PREPARE approvals for scorecard/tracking/website brief', 'EXECUTE remains OFF'],
  measurement: {
    weekly: 'Delta / execution / measurement / decisions',
    monthly: 'Deeper performance + strategy challenge',
    quarterlyOrMajorEvent: 'Full baseline refresh',
    scheduled: false,
  },
  roadmap: {
    NOW: [
      {
        id: 'NOW-1',
        title: 'Founder strategy review gate',
        whyNow: 'Tactics must not lead without accepted diagnosis',
        expectedValue: 'Anchored CMO operating model',
        dependency: 'This baseline pack',
        evidence: 'V1 console flaw: approvals before strategy',
        successLooksLike: 'Strategy LOCKED or AMENDED by Joan',
        strategicPillarId: 'P1',
      },
      {
        id: 'NOW-2',
        title: 'Named warm ADP pilot conversations (3–7)',
        whyNow: 'JD-6B-001 90-day activation',
        expectedValue: 'Real commercial motion',
        dependency: 'Joan CRM knowledge',
        evidence: 'Week01 ADP DATA_GAP for names',
        successLooksLike: 'Named accounts advancing under MARKET_TEST',
        strategicPillarId: 'P2',
      },
      {
        id: 'NOW-3',
        title: 'ADP Pilot Scorecard mandatory',
        whyNow: 'Proof before low-evidence content',
        expectedValue: 'Learning system',
        dependency: 'Scorecard PREPARE artifact',
        evidence: '14_ADP_PILOT_SCORECARD',
        successLooksLike: 'No pilot start without scorecard',
        strategicPillarId: 'P5',
      },
      {
        id: 'NOW-4',
        title: 'P0 attribution tracking TWIs approve/hold',
        whyNow: 'Attribution BROKEN',
        expectedValue: 'Ability to learn Week 2+',
        dependency: 'Founder PREPARE approval',
        evidence: '12_MEASUREMENT_ATTRIBUTION_AUDIT',
        successLooksLike: 'P0 TWIs approved or explicitly held',
        strategicPillarId: 'P4',
      },
      {
        id: 'NOW-5',
        title: 'Website dual-track restructure PREPARE (no build)',
        whyNow: 'Public story lag',
        expectedValue: 'Ready brief for later implementation',
        dependency: 'Strategy review',
        evidence: '04_WEBSITE_RESTRUCTURE_BRIEF',
        successLooksLike: 'PREPARE brief founder-ready',
        strategicPillarId: 'P1',
      },
    ],
    NEXT: [
      {
        id: 'NEXT-1',
        title: 'Joan LinkedIn commercial reset execution',
        whyNow: 'After strategy acceptance',
        expectedValue: 'Founder channel aligned to GTM',
        dependency: 'JD-6B-003',
        evidence: 'LinkedIn packs',
        successLooksLike: 'Cadence maps to ADP + dealmaking education',
        strategicPillarId: 'P3',
      },
      {
        id: 'NEXT-2',
        title: 'Warm ABM follow-through system',
        whyNow: 'After named list exists',
        expectedValue: 'Repeatable warm motion',
        dependency: 'NOW-2',
        evidence: '06_ABM_WARM_ACTIONS',
        successLooksLike: 'Tracked touch → conversation outcomes',
        strategicPillarId: 'P3',
      },
    ],
    LATER: [
      {
        id: 'LATER-1',
        title: 'Website dual-track implementation',
        whyNow: 'After PREPARE + capacity',
        expectedValue: 'Public hierarchy matches strategy',
        dependency: 'NOW-5',
        evidence: 'manual-cycle website B',
        successLooksLike: 'ADP visible in first viewport hierarchy without selection erasure',
        strategicPillarId: 'P1',
      },
      {
        id: 'LATER-2',
        title: 'Marketing OS ↔ console decision sync',
        whyNow: 'After local workflow trusted',
        expectedValue: 'Durable decision memory',
        dependency: 'Console V2 stable',
        evidence: 'OS write not connected',
        successLooksLike: 'Approved decisions land in OS',
        strategicPillarId: 'P4',
      },
    ],
    PARK: [
      {
        id: 'PARK-1',
        title: 'Paid acquisition',
        whyNow: 'Attribution broken; proof weak',
        expectedValue: 'N/A',
        dependency: 'Measurement + proof',
        evidence: 'doNotSolveYet',
        successLooksLike: 'Remains parked',
        strategicPillarId: 'P4',
      },
      {
        id: 'PARK-2',
        title: 'EXECUTE / recurring Helena',
        whyNow: 'Safety + strategy gate',
        expectedValue: 'N/A',
        dependency: 'Founder enablement decision later',
        evidence: 'Operating Law',
        successLooksLike: 'Remains OFF',
        strategicPillarId: 'P4',
      },
    ],
  },
  proposedTacticalActions: [
    {
      id: 'JD-W1-001',
      title: 'Confirm 3–7 warm ADP targets',
      status: 'PROPOSED_PENDING_STRATEGY_REVIEW',
      strategicPillarId: 'P2',
      priorityId: 'NOW-2',
      orphaned: false,
    },
    {
      id: 'JD-W1-002',
      title: 'Approve ADP Pilot Scorecard mandatory',
      status: 'PROPOSED_PENDING_STRATEGY_REVIEW',
      strategicPillarId: 'P5',
      priorityId: 'NOW-3',
      orphaned: false,
    },
    {
      id: 'JD-W1-003',
      title: 'Approve/hold P0 tracking TWIs',
      status: 'PROPOSED_PENDING_STRATEGY_REVIEW',
      strategicPillarId: 'P4',
      priorityId: 'NOW-4',
      orphaned: false,
    },
    {
      id: 'W1-PREP-WEBSITE',
      title: 'Website dual-track PREPARE brief',
      status: 'PROPOSED_PENDING_STRATEGY_REVIEW',
      strategicPillarId: 'P1',
      priorityId: 'NOW-5',
      orphaned: false,
    },
    {
      id: 'W1-LINKEDIN-RESET',
      title: 'Joan LinkedIn role reset materials',
      status: 'PROPOSED_PENDING_STRATEGY_REVIEW',
      strategicPillarId: 'P3',
      priorityId: 'NEXT-1',
      orphaned: false,
    },
    {
      id: 'W1-ABM-WARM',
      title: 'ABM warm actions',
      status: 'PROPOSED_PENDING_STRATEGY_REVIEW',
      strategicPillarId: 'P3',
      priorityId: 'NEXT-2',
      orphaned: false,
    },
  ],
};

function mdEscape(s) {
  return String(s ?? '');
}

function writeReports() {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, 'helena-cmo-baseline-v1.json'), JSON.stringify(baseline, null, 2));
  fs.writeFileSync(path.join(OUT, 'helena-cmo-strategy-v1.json'), JSON.stringify(strategy, null, 2));

  const ea = baseline.executiveAssessment;
  const files = {
    '00_EXECUTIVE_CMO_ASSESSMENT.md': `# Executive CMO Assessment\n\n**Baseline:** ${baseline.meta.baselineVersion} · **Date:** ${baseline.meta.lastDeepReviewDate} · **Strategy state:** ${baseline.meta.strategyState}\n**EXECUTE:** OFF · **Recurring Helena:** OFF\n\n## Where we stand\n${ea.whereWeStand}\n\n## Overall marketing health\n**${ea.overallHealth.score}/10 — ${ea.overallHealth.label}**\n\n${baseline.overallMarketingHealth.rationale}\n\n## Top strengths\n${ea.topStrengths.map((x) => `- ${x}`).join('\n')}\n\n## Top opportunities\n${ea.topOpportunities.map((x) => `- ${x}`).join('\n')}\n\n## What is not working\n${ea.whatIsNotWorking.map((x) => `- ${x}`).join('\n')}\n\n## What is unknown\n${ea.unknowns.map((x) => `- ${x}`).join('\n')}\n\n## CMO strategic read\n${ea.centralProblem}\n\n## Recommended strategy\n${ea.recommendedStrategyThesis}\n\n### Pillars\n${ea.pillars.map((p) => `- **${p.id} ${p.name}**`).join('\n')}\n\n## What should happen first\n${ea.whatShouldHappenFirst.map((x, i) => `${i + 1}. ${x}`).join('\n')}\n\n## Founder review\n**${ea.founderReviewCta}** — do not lead with tactical approvals until strategy is reviewed.\n`,
    '01_SOURCE_COVERAGE.md': `# Source Coverage\n\n${baseline.sourceCoverage
      .map(
        (s) =>
          `## ${s.area}\n- **Available:** ${s.sourcesAvailable.join('; ')}\n- **Reviewed:** ${s.sourcesReviewed.join('; ')}\n- **Missing:** ${s.sourcesMissing.join('; ')}\n- **Freshness:** ${s.freshness}\n- **Confidence:** ${s.confidence}\n`,
      )
      .join('\n')}\n\n**Honesty note:** This baseline is comprehensive relative to recovered CURRENT packs + point-in-time analytics snapshots. It is **not** a live GSC/GA4/LinkedIn API refresh.\n`,
    '02_COMPANY_POSITIONING_BASELINE.md': `# Company Positioning Baseline\n\n**Score:** ${baseline.scorecard.company_positioning.score}/10\n\n${baseline.scorecard.company_positioning.why}\n\nEvidence: ${baseline.scorecard.company_positioning.evidence.join(', ')}\n\nBiggest gap: ${baseline.scorecard.company_positioning.biggestGap}\nOpportunity: ${baseline.scorecard.company_positioning.opportunity}\n`,
    '03_PRODUCT_MARKETING_BASELINE.md': `# Product Marketing Baseline\n\nProducts assessed at marketing maturity (not engineering completeness):\n\n| Product | Maturity (mkt) | Notes |\n|---|---|---|\n| ADP | PILOT_READY / public lag | Lead wedge |\n| Brand Explorer | SUPPORTING | Depth proof |\n| Operator Explorer | SUPPORTING | Depth proof |\n| Brand/Operator Alignment Snapshots | SUPPORTING | Secondary |\n| Owner Intelligence | EMERGING_PUBLIC | Dealmaking support |\n| Hotel Intelligence | EMERGING_PUBLIC | Dealmaking support |\n| Hotel Property Census | FOUNDATION | Scale proof |\n\nPublic hierarchy should lead with ADP + owner dealmaking, not a flat product zoo.\n`,
    '04_WEBSITE_BASELINE.md': `# Website Baseline\n\n**Verdict:** ${baseline.domains.website.verdict} · **Score:** ${baseline.domains.website.score}/10\n\n${baseline.domains.website.rationale}\n\n## Good\n${baseline.domains.website.good.map((x) => `- ${x}`).join('\n')}\n\n## Bad / weak\n${baseline.domains.website.bad.map((x) => `- ${x}`).join('\n')}\n\n**Current surface:** \`public/marketing/dealality-landing-v9.html\`\n**This week:** PREPARE restructure brief only — no build.\n`,
    '05_MESSAGING_BASELINE.md': `# Messaging Baseline\n\n**Score:** ${baseline.scorecard.messaging.score}/10\n\n${baseline.scorecard.messaging.why}\n\nClarity is better in internal locks than on public surfaces. Urgency for ADP is weak publicly. Differentiation exists in product truth but is under-expressed. Over-complexity risk remains if all products are presented equally.\n`,
    '06_ADP_BASELINE.md': `# ADP Baseline\n\n**Verdict:** ${baseline.domains.adp.verdict} · **Score:** ${baseline.domains.adp.score}/10\n\n${baseline.domains.adp.rationale}\n\nPricing authority: MARKET_TEST (D5). Pilot scorecard: SPEC exists; instances NOT_CONNECTED. Named warm pipeline: DATA_GAP.\n`,
    '07_OWNER_DEALMAKING_BASELINE.md': `# Owner Dealmaking Baseline\n\n**Verdict:** ${baseline.domains.ownerAcquisition.verdict} · **Score:** ${baseline.domains.ownerAcquisition.score}/10\n\n${baseline.domains.ownerAcquisition.rationale}\n\nDealmaking remains the durable identity track (D1/D2) but conversion-grade proof is thin.\n`,
    '08_LINKEDIN_BASELINE.md': `# LinkedIn Baseline\n\n**Verdict:** ${baseline.domains.linkedin.verdict}\n\n## What worked\n${baseline.domains.linkedin.whatWorked.map((x) => `- ${x}`).join('\n')}\n\n## What did not\n${baseline.domains.linkedin.whatDidNot.map((x) => `- ${x}`).join('\n')}\n\n## Unknown\n${baseline.domains.linkedin.unknown.map((x) => `- ${x}`).join('\n')}\n\n## Why\n${baseline.domains.linkedin.why}\n\nJoan role: JD-6B-003 reset — commercial education + warm activation.\n`,
    '09_SEO_DISCOVERY_BASELINE.md': `# SEO / Discovery Baseline\n\n**Verdict:** ${baseline.domains.seo.verdict} · **Score:** ${baseline.domains.seo.score}/10\n\n${baseline.domains.seo.rationale}\n`,
    '10_PROOF_BASELINE.md': `# Proof Baseline\n\n**Score:** ${baseline.scorecard.proof.score}/10\n\n| Class | Status |\n|---|---|\n| Customers / named outcomes | MISSING / UNKNOWN publicly |\n| Pilots | SUPPORTED as intent; instrumentation SPEC |\n| Methodology | SUPPORTED |\n| Decks / demo packs | SUPPORTED |\n| Testimonials | ANECDOTAL / thin |\n| Research scale | SUPPORTED (product) |\n\nConversion-grade public proof: **MISSING**.\n`,
    '11_ANALYTICS_ATTRIBUTION_BASELINE.md': `# Analytics / Attribution Baseline\n\n**Verdict:** ${baseline.domains.measurement.verdict} · **Score:** ${baseline.domains.measurement.score}/10\n\n### CAN MEASURE\n${baseline.domains.measurement.canMeasure.map((x) => `- ${x}`).join('\n')}\n\n### PARTIALLY MEASURE\n${baseline.domains.measurement.partiallyMeasure.map((x) => `- ${x}`).join('\n')}\n\n### CANNOT MEASURE\n${baseline.domains.measurement.cannotMeasure.map((x) => `- ${x}`).join('\n')}\n`,
    '12_CONVERSION_GTM_BASELINE.md': `# Conversion / GTM Support Baseline\n\nHelena remains CMO, not Sales CSO.\n\nWarm outreach + founder conversations are the strongest near-term conversion supports. Demos/decks exist for ADP. Objections packs exist historically. Follow-up attribution is weak. Systematic ABM without names is theater.\n`,
    '13_CHANNEL_BASELINE.md': `# Channel Baseline\n\nPrioritize: Founder LinkedIn, warm outreach/ABM, website, SEO defense.\nPark: paid.\nSecondary: email, partnerships, events, PR — only after instrumentation.\n`,
    '14_RESOURCE_CAPABILITY_BASELINE.md': `# Resource / Capability Baseline\n\n**Underused:** Marketing OS PACK_CORE (messaging, Target OS, Thought Leadership OS), recovered analytics snapshots, ADP decks, GTM pilot kits, Helena Operating Law.\n\n**Available:** Marketing OS Airtable base (seeded), Founder Console (local), Cursor/Helena PREPARE workflow.\n\nDo not invent parallel systems.\n`,
    '15_CMO_BASELINE_SCORECARD.md': `# CMO Baseline Scorecard\n\nDirectional 1–10 only.\n\n${Object.entries(baseline.scorecard)
      .map(
        ([k, v]) =>
          `## ${k}\n- **Score:** ${v.score}/10\n- **Current:** ${v.currentState}\n- **Why:** ${v.why}\n- **Evidence:** ${v.evidence.join('; ')}\n- **Biggest gap:** ${v.biggestGap}\n- **Opportunity:** ${v.opportunity}\n- **Confidence:** ${v.confidence}\n`,
      )
      .join('\n')}`,
    '16_STRENGTHS_OPPORTUNITIES_RISKS.md': `# Strengths / Opportunities / Risks\n\n## Strengths\n${baseline.findings.strengths.map((s) => `- **${s.title}** — ${s.evidence}`).join('\n')}\n\n## Areas of improvement\n${baseline.findings.improvements.map((i, n) => `${n + 1}. **${i.issue}** (${i.urgency}) — ${i.consequence}`).join('\n')}\n\n## Unknowns\n${baseline.findings.unknowns.map((u) => `- ${u}`).join('\n')}\n`,
    '17_HISTORICAL_ACTIVITY_ASSESSMENT.md': `# Historical Activity Assessment\n\n| Activity | Good idea? | Executed well? | Evidence | Class | Why |\n|---|---|---|---|---|---|\n${baseline.historicalActivities.map((a) => `| ${a.name} | ${a.wasGoodIdea} | ${a.executedWell} | ${a.generatedEvidence} | **${a.classification}** | ${a.why} |`).join('\n')}\n`,
    '18_CENTRAL_MARKETING_DIAGNOSIS.md': `# Central Marketing Diagnosis\n\n## Central problem\n${baseline.diagnosis.centralProblem}\n\n## Why it exists\n${baseline.diagnosis.whyExists.map((x) => `- ${x}`).join('\n')}\n\n## If we do nothing\n${baseline.diagnosis.ifDoNothing.map((x) => `- ${x}`).join('\n')}\n\n## Biggest leverage (≤3)\n${baseline.diagnosis.leverage.map((x) => `- ${x}`).join('\n')}\n\n## Do not solve yet\n${baseline.diagnosis.doNotSolveYet.map((x) => `- ${x}`).join('\n')}\n`,
    '19_RECOMMENDED_CMO_STRATEGY.md': `# Recommended CMO Strategy\n\n**State:** ${strategy.state}\n\n## Thesis\n${strategy.thesis}\n\n## Who / problem / urgency\n- **Who:** ${strategy.who}\n- **Problem:** ${strategy.problem}\n- **Urgency:** ${strategy.urgency}\n\n## Lead capabilities\n${strategy.leadCapabilities.map((x) => `- ${x}`).join('\n')}\n\n## ADP + dealmaking coexistence\n${strategy.adpAndDealmakingCoexistence}\n\n## Pillars\n${strategy.pillars.map((p) => `### ${p.id} — ${p.name}\n${p.rationale}`).join('\n\n')}\n\n## Channel roles\n- Website: ${strategy.websiteRole}\n- LinkedIn: ${strategy.linkedinRole}\n- ABM: ${strategy.abmRole}\n- SEO: ${strategy.seoRole}\n\n## Stop doing\n${strategy.stopDoing.map((x) => `- ${x}`).join('\n')}\n\n## Measurement\n${strategy.successMeasures.map((x) => `- ${x}`).join('\n')}\n\n## Risks / dependencies\n${strategy.risks.map((x) => `- Risk: ${x}`).join('\n')}\n${strategy.dependencies.map((x) => `- Dep: ${x}`).join('\n')}\n`,
    '20_PRIORITY_ROADMAP.md': `# Priority Roadmap\n\n${['NOW', 'NEXT', 'LATER', 'PARK']
      .map(
        (bucket) =>
          `## ${bucket}\n${strategy.roadmap[bucket].map((i) => `### ${i.id} — ${i.title}\n- Why: ${i.whyNow}\n- Value: ${i.expectedValue}\n- Dependency: ${i.dependency}\n- Evidence: ${i.evidence}\n- Success: ${i.successLooksLike}\n- Pillar: ${i.strategicPillarId}\n`).join('\n')}`,
      )
      .join('\n')}`,
    '21_FOUNDER_STRATEGY_REVIEW.md': `# Founder Strategy Review Gate\n\n**Required state before normal tactical approvals:** \`STRATEGY_PENDING_FOUNDER_REVIEW\` → founder LOCK or AMEND.\n\nUntil reviewed:\n- Helena may research / analyze / draft / PREPARE materials\n- Tactical actions appear as **PROPOSED — PENDING STRATEGY REVIEW**\n- Console must not lead with tactical approval asks\n\nEmergency safety/technical actions remain allowed.\n\nPrimary CTA: **REVIEW STRATEGY**\n`,
    '22_CONSOLE_V2_VIEW_MODEL.md': `# Console V2 View Model\n\nPrimary dataset: \`helena-cmo-baseline-v1.json\` + \`helena-cmo-strategy-v1.json\` via \`lib/helena-cmo/founder-console/baseline-view-model.js\`.\n\nNav: Executive Assessment (default) → Baseline → Findings → Strategy → Priorities → Actions & Approvals → Performance → ADP Pilots → History → Evidence.\n\nWeekly pack remains available inside Actions / Performance as delta context. Tactical decisions/approvals stay PREPARE-only; EXECUTE OFF.\n`,
    '23_TEST_RESULTS.md': `# Test Results\n\nPending Founder Console V2 UI test run after implementation.\n`,
  };

  for (const [name, body] of Object.entries(files)) {
    fs.writeFileSync(path.join(OUT, name), body);
  }

  return { out: OUT, mdCount: Object.keys(files).length };
}

const result = writeReports();
console.log(JSON.stringify({ ok: true, ...result, baselineScore: baseline.overallMarketingHealth.score, strategyState: strategy.state }, null, 2));
