#!/usr/bin/env node
/**
 * Phase 6E — write helena-cmo-strategy-decision-v1 pack (discussion, not lock).
 * READ-only analytics; no EXECUTE; no strategy approval ask.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { readHelenaCmoAnalyticsContract } from '../lib/helena-cmo/analytics/cmo-analytics-reader.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'reports/helena-cmo-strategy-decision-v1');

function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith('\n') ? body : `${body}\n`);
}

fs.mkdirSync(OUT, { recursive: true });

const analytics = readHelenaCmoAnalyticsContract();
const siteInv = JSON.parse(
  fs.readFileSync(path.join(OUT, '_live-site-inventory.json'), 'utf8'),
);

const generatedAt = new Date().toISOString();

const recommended = {
  id: 'B_ADP_ENTRY_DEALMAKING_ARCHITECTURE',
  name: 'ADP paid-pilot entry for warm owners · owner dealmaking as company architecture',
  score: 7.6,
  scores: {
    marketUrgency: 7,
    productReadiness: 8,
    differentiation: 8,
    accessToBuyers: 6,
    revenuePotential: 7,
    adoptionFriction: 6,
    proofAbility: 5,
    recurringPotential: 7,
    companyIdentityCoherence: 7,
    longTermStrategicValue: 9,
  },
};

const alternatives = [
  {
    id: 'A_DEALMAKING_FIRST',
    name: 'Owner dealmaking first · ADP as supporting product',
    score: 6.2,
    whyNotNow:
      'Live site already leads with dealmaking; without a paid entry wedge and proof loop, marketing stays narrative-heavy and revenue-light. ADP would remain invisible longer.',
  },
  {
    id: 'C_OWNER_INTELLIGENCE_PLATFORM',
    name: 'Owner intelligence / decision platform first',
    score: 5.4,
    whyNotNow:
      'Strong long-term category, but packaging is broader than current GTM readiness, proof, and named pipeline. Higher message risk and slower paid learning.',
  },
];

const pack = {
  schemaVersion: 'helena-cmo-strategy-decision-v1',
  generatedAt,
  meta: {
    executeEnabled: false,
    recurringHelenaEnabled: false,
    strategyApprovalRequested: false,
    strategyState: 'STRATEGY_PENDING_FOUNDER_DISCUSSION',
    primaryCta: 'DISCUSS_STRATEGY',
    note: 'Phase 6E presents an explicit strategic choice for founder discussion — not lock/approval.',
  },
  dataCompletenessDecision: {
    answer: 'PARTIALLY',
    explanation:
      'Remaining gaps (live GA4/GSC, LinkedIn API, outreach attribution) are unlikely to flip category/positioning. Named warm-owner access confirmation COULD change confidence in ADP-entry sequencing or force a temporary dealmaking-only commercial focus — so gaps are partially material to execution sequencing, not to the recommended strategic territory.',
    requestFounderLock: false,
    requestFounderDiscussion: true,
  },
  blockingGaps: {
    minimumEvidenceThreshold:
      'Locked founder GTM/ICP/positioning + product maturity for entry offer + public identity check (live site) + honest measurement status + LinkedIn role evidence + commercial access hypothesis (warm network OR explicit cold motion). Named CRM rows and live GA4 are NOT required to recommend a near-term GTM strategy.',
    gaps: [
      {
        gap: 'Live GA4 API',
        classification: 'IMPORTANT_BUT_NON_BLOCKING',
        why: 'Changes channel diagnostics and CTA measurement; unlikely to change ICP/GTM category choice.',
      },
      {
        gap: 'Live GSC API',
        classification: 'OPERATIONAL_LATER',
        why: 'SEO priority already MIXED_WEAK from stale snapshot; live refresh refines defense, not strategy.',
      },
      {
        gap: 'LinkedIn analytics API',
        classification: 'IMPORTANT_BUT_NON_BLOCKING',
        why: 'Role can be set from content history + engagement quality proxy; ROI remains UNKNOWN.',
      },
      {
        gap: 'Webflow CMS API inventory',
        classification: 'IMPORTANT_BUT_NON_BLOCKING',
        why: 'Public HTML inventory resolves what prospects see TODAY; CMS helps editorial ops later.',
      },
      {
        gap: 'Named commercial pipeline (warm ADP accounts)',
        classification: 'BLOCKING_FOR_STRATEGY',
        nuance:
          'Blocking for confident ADP-entry EXECUTION sequencing and for strategy LOCK confidence on access — not blocking for discussing the recommended strategic choice. Founder relationship input closes this.',
        why: 'If warm ADP-relevant owners do not exist, near-term paid-pilot thesis weakens and sequencing must shift.',
      },
      {
        gap: 'Email/outreach attribution',
        classification: 'OPERATIONAL_LATER',
        why: 'Required for learning loops; does not choose category/positioning.',
      },
      {
        gap: 'Conversion-grade public proof',
        classification: 'IMPORTANT_BUT_NON_BLOCKING',
        why: 'Affects conversion rate and messaging strength; strategy still leads with paid learning to CREATE proof.',
      },
    ],
  },
  connectors: {
    ga4: 'NOT_CONNECTED',
    gsc: 'NOT_CONNECTED',
    linkedInAnalytics: 'NOT_CONNECTED',
    webflowCms: 'NOT_CONNECTED',
    publicWebsiteHtml: 'CONNECTED_AND_WORKING',
    marketingOsAirtable: 'CONNECTED_BUT_NOT_WIRED_TO_HELENA',
    gtmPilotTargetList: 'CONNECTED_BUT_NOT_WIRED_TO_HELENA',
    gtmOwnerTargets: 'CONNECTED_BUT_NOT_WIRED_TO_HELENA',
    acquisitionNetwork: 'CONNECTED_BUT_NOT_WIRED_TO_HELENA',
    landingEvents: 'CONNECTED_BUT_NOT_WIRED_TO_HELENA',
    emailAttribution: 'NOT_CONNECTED',
    staleAnalyticsSnapshots: 'CONNECTED_AND_WORKING',
  },
  analytics,
  website: {
    method: siteInv.method,
    ok200: siteInv.ok200,
    adpVisibleOnPublicSite: siteInv.adpVisibleOnAny200,
    homepageTitle: 'Dealality | Find the best path for your hotel',
    homepageIdentity: 'OWNER_DEALMAKING',
    insightsTheme: 'BRAND_OPERATOR_SELECTION_HEAVY',
    brandExplorerPublicPage: false,
    operatorExplorerPublicPage: false,
    opportunityReviewPath: '/opportunity-review',
    repoV9Note: 'Repo marketing/dealality-landing-v9.html remains selection-forward — NOT what prospects see on live homepage.',
    authority: 'LIVE_WEBSITE',
  },
  commercialPipelineModel: {
    preferredReuse: [
      'GTM Pilot Target List (outreach contacts)',
      'GTM Acquisition Network Relationships (founder LinkedIn graph)',
      'GTM Decision Opportunities (project-level)',
      'Marketing OS Growth Opportunities / Audiences (class-level only)',
    ],
    doNotCreateGiantCrm: true,
    canonicalFields: [
      'ACCOUNT',
      'CONTACT_RELATIONSHIP',
      'CLIENT_TYPE',
      'ICP',
      'GTM_TRACK',
      'HOTELS_PORTFOLIO',
      'WARMTH',
      'CURRENT_NEED',
      'TRIGGER',
      'LAST_CONTACT',
      'CURRENT_STAGE',
      'NEXT_STEP',
      'PRODUCT',
      'PRICING_STATUS',
      'OBJECTION',
      'OWNER',
      'SOURCE',
      'EVIDENCE',
      'CONFIDENCE',
    ],
    warmthRule: 'Helena must NOT fabricate relationship warmth — founder-supplied or evidenced only.',
    namedPipelineStatus: 'DATA_GAP',
  },
  founderRelationshipInputContract: {
    purpose: 'Capture high-value known relationships without inventing warmth',
    fields: [
      'person',
      'company',
      'relationshipStrength',
      'role',
      'likelyRelevance',
      'hotelsPortfolioIfKnown',
      'lastInteraction',
      'notes',
      'gtmTrackHint',
      'productHint',
    ],
    helenaMayThen: ['research public facts', 'score opportunity against ICP/GTM', 'propose next step'],
    helenaMustNot: ['invent warmth', 'auto-outreach', 'EXECUTE'],
    uiStatus: 'CONTRACT_ONLY_THIS_PHASE',
  },
  strategicChoice: {
    competeWhere:
      'Owner-controlled hotel dealmaking platform with proprietary intelligence — entered commercially via AI Demand Positioning paid diagnostics for owners who need a clearer demand/path decision now.',
    winFirst:
      'Warm owner / owner-advisor relationships with an active or near-term brand/operator/path decision OR visible AI-demand uncertainty on an existing/asset hotel — high access, high willingness to pay for a bounded pilot, strong proof potential.',
    leadProblem:
      'Owners make path and demand decisions with incomplete, brand-biased, or uninstrumented information — costly when the first brand/operator conversation already constrains outcomes, and when AI demand representation is invisible or weak.',
    whyDealality:
      'Combines owner-controlled dealmaking workflow + hotel/brand/operator intelligence depth + AI demand positioning evidence — not generic GEO SaaS, not a broker, not a listing site, not advice-only consulting.',
    alternatives: [
      'Do nothing / delay',
      'Spreadsheets and manual research',
      'Consultants/advisors',
      'Brand/operator direct outreach first',
      'Hospitality data platforms',
      'GEO / AI visibility tools',
      'Broker-led processes',
    ],
    entryOffer: 'MARKET_TEST ADP paid pilot / diagnostic with scorecard for warm accounts',
    expansionPath:
      'Pilot → proof narrative → broader dealmaking modules (Explorers, alignment, opportunity review) when the owner’s next decision is path/selection — never forced cross-sell.',
    willNotDo: [
      'Brand/operator primary acquisition as NOW GTM',
      'Paid media scale',
      'SEO vanity expansion before conversion paths',
      'AO LinkedIn volume as primary growth engine',
      'Website full rebuild before dual-track ADP visibility PREPARE',
      'EXECUTE / recurring Helena / external publish without founder path',
      'Claiming ROI without instrumentation',
    ],
  },
  recommended,
  alternatives,
  positioning: {
    internal:
      'Dealality is building the owner-controlled operating system for hotel path decisions — demand, brand, operator, and deal structure — starting commercial learning with AI Demand Positioning pilots while the public brand remains dealmaking-forward.',
    owner:
      'Dealality helps you find the best path for your hotel with owner-controlled process and evidence — so you are not forced into the first brand or operator conversation unprepared.',
    adpEntry:
      'If AI systems already shape how guests discover hotels, you need to know how your hotel (and alternatives) show up — and what to do about it — before you lock a path. ADP is a paid diagnostic designed for that.',
    brandOperator:
      'Brands and operators get annual intelligence access; they are not the near-term acquisition priority while owner paid learning is the commercial constraint.',
  },
  channelRoles: [
    {
      channel: 'JOAN_LINKEDIN',
      rank: 'CORE',
      job: 'Founder trust + commercial education + warm activation toward ADP/dealmaking conversations — not vanity reach.',
    },
    {
      channel: 'WARM_OUTREACH',
      rank: 'CORE',
      job: 'Convert known relationships into instrumented paid-pilot discussions.',
    },
    {
      channel: 'ADVISORS',
      rank: 'SUPPORTING',
      job: 'Trusted introductions into owner decisions; not a volume channel.',
    },
    {
      channel: 'WEBSITE',
      rank: 'CORE',
      job: 'Public identity (dealmaking) + dual-track ADP discoverability + opportunity capture — conversion surface, not blog vanity.',
    },
    {
      channel: 'SEO',
      rank: 'SUPPORTING',
      job: 'Defend owner-intent pages already ranking; do not fund vanity impression programs.',
    },
    {
      channel: 'ADP_DIAGNOSTIC',
      rank: 'CORE',
      job: 'Entry commercial offer that creates paid learning and proof.',
    },
    {
      channel: 'AO_COMPANY_LINKEDIN',
      rank: 'EXPERIMENT',
      job: 'Secondary distribution only if it supports Joan/warm motion; not primary.',
    },
    {
      channel: 'PAID_ACQUISITION',
      rank: 'PARK',
      job: 'Park until proof + attribution exist.',
    },
  ],
  proofSequence: [
    'People will pay (paid pilot commitment)',
    'They use the output (engagement with ADP deliverable)',
    'They act on recommendations (documented decisions/next steps)',
    'Value persists / expands (repeat, portfolio, adjacent modules)',
    'Commercial outcome where attribution permits',
  ],
  scorecard90d: [
    { metric: 'Named warm accounts activated', businessQuestion: 'Do we have real commercial targets in motion?' },
    { metric: 'Meaningful commercial conversations', businessQuestion: 'Are owners engaging beyond polite interest?' },
    { metric: 'Paid pilots started', businessQuestion: 'Will they pay for ADP learning?' },
    { metric: 'Pilot conversion rate (discuss→paid)', businessQuestion: 'Is the offer clear and valued?' },
    { metric: 'Pilot usage of deliverable', businessQuestion: 'Is the product used, not just bought?' },
    { metric: 'Proof assets created (anonymized)', businessQuestion: 'Can marketing show evidence without inventing ROI?' },
    { metric: 'Expansion signals (next module interest)', businessQuestion: 'Does ADP open dealmaking naturally?' },
    { metric: 'Attributed revenue (when join exists)', businessQuestion: 'What cash did this motion produce?' },
    { metric: 'Attribution gaps closed (P0 events)', businessQuestion: 'Can we learn from the funnel at all?' },
    { metric: 'Public ADP dual-track visibility PREPARE ready', businessQuestion: 'Can prospects find the entry offer without identity rewrite?' },
  ],
  assumptions: [
    {
      assumption: 'Owners care enough about AI demand positioning to pay for a bounded pilot.',
      whyMatters: 'Core revenue wedge',
      evidence: 'Product MARKET_TEST + founder locks; no public conversion proof yet',
      confidence: 'MEDIUM',
      howTest: 'Warm paid-pilot closes',
      disprove: 'Consistent polite interest + refusal to pay after clear offer',
    },
    {
      assumption: 'Joan’s warm network can produce the first ADP cohort.',
      whyMatters: 'Access is the near-term bottleneck',
      evidence: 'Locks + GTM structures exist; named ADP warmth DATA_GAP',
      confidence: 'MEDIUM_LOW',
      howTest: 'Founder relationship input + outreach outcomes',
      disprove: 'No ADP-relevant warm accounts after honest inventory',
    },
    {
      assumption: 'ADP creates recurring / expansion value beyond one-shot PDF.',
      whyMatters: 'Long-term SaaS coherence',
      evidence: 'Product depth; longitudinal capability; unproven commercially',
      confidence: 'MEDIUM',
      howTest: 'Pilot usage + expansion asks',
      disprove: 'One-and-done with no revisit or adjacent need',
    },
    {
      assumption: 'Owner dealmaking remains coherent public identity while ADP is entry offer.',
      whyMatters: 'Brand risk if ADP overshoots',
      evidence: 'Live homepage already dealmaking-forward; ADP absent',
      confidence: 'HIGH',
      howTest: 'Dual-track PREPARE messaging tests',
      disprove: 'Prospects misread Dealality as GEO-only or broker',
    },
  ],
};

// --- Markdown outputs ---

write(
  '00_FOUNDER_STRATEGY_DISCUSSION.md',
  `# Founder Strategy Discussion — Helena CMO Phase 6E

**Read time:** ~15–20 min · **CTA:** DISCUSS STRATEGY (not approve/lock) · **EXECUTE/Recurring:** OFF

## 1. What I learned
Dealality’s locked direction is dual-track, but public GTM is still one-track: live homepage sells owner dealmaking (“Find the Best Path for Your Hotel”) while **ADP is invisible**. Product readiness for a MARKET_TEST ADP pilot is ahead of commercialization. Measurement and named pipeline remain weak. Live analytics APIs are still not connected; stale Jun–Sep snapshots are enough for channel diagnosis, not for commercial proof.

## 2. What evidence is strong
- Founder locks D1–D5 / JD-6B + Operating Law
- Live homepage identity = owner dealmaking (public HTML inventory, ${siteInv.ok200} OK pages; ADP visible = **false**)
- ADP product maturity vs public GTM lag
- Joan LinkedIn outperforms AO on engagement-quality proxy
- Marketing OS foundation exists; GTM Pilot Target / Acquisition structures exist (not yet Helena CRM)

## 3. What remains uncertain
- Named warm ADP-relevant accounts (DATA_GAP — **partially strategy-material**)
- Live GA4/GSC trends & conversion events
- LinkedIn commercial ROI
- Whether owners will pay at stated pilot economics

## 4. The central marketing problem
**Dealality can build and explain products, but cannot yet run a closed commercial learning loop:** missing named activation list, conversion-grade proof, attribution, and a public entry offer that matches the locked ADP wedge — while the live brand correctly stays dealmaking-forward.

## 5. The strategic options considered
| Option | Score | One-line |
|--------|------:|----------|
| **A — Dealmaking first / ADP supporting** | ${alternatives[0].score} | Strengthen path story; ADP stays backstage |
| **B — ADP entry / dealmaking architecture (recommended)** | **${recommended.score}** | Paid ADP pilots for warm owners; public identity stays dealmaking |
| **C — Owner intelligence platform first** | ${alternatives[1].score} | Broad decision OS as lead category now |

## 6. Recommended choice
**Option B — ADP paid-pilot entry for warm owners, with owner dealmaking as company architecture and public identity.**

## 7. Why this choice
- Highest combination of product readiness + paid learning speed + long-term strategic value
- Does not fight the live website’s dealmaking identity
- Creates the proof the rest of marketing lacks
- Avoids premature “GEO SaaS” repositioning and avoids pure narrative dealmaking with no wedge

## 8. What we deliberately will NOT prioritize
${pack.strategicChoice.willNotDo.map((x) => `- ${x}`).join('\n')}

## 9. How ADP fits
ADP is the **entry commercial offer** (diagnostic/pilot), not the whole company. It must become **discoverable** on a dual-track site without rewriting Dealality as AI-visibility-only.

## 10. How owner dealmaking fits
Owner dealmaking is the **durable public identity and expansion architecture** — Explorers, opportunity review, path decisions. It is what the homepage already sells.

## 11. Buyer / urgency
Warm owners (or advisors to owners) facing a path/demand decision soon — where incomplete information is costly and a bounded paid diagnostic is rational.

## 12. Channel strategy
CORE: Joan LinkedIn · Warm outreach · Website (identity + ADP discoverability) · ADP diagnostic  
SUPPORTING: Advisors · SEO defense  
EXPERIMENT: AO company LinkedIn  
PARK: Paid acquisition

## 13. Proof strategy
Pay → use → act → persist/expand → attributed outcome (do not leap to ROI).

## 14. Measurement
90-day scorecard max 10 metrics — see \`11_90_DAY_SCORECARD.md\`. Channel metrics are diagnostic only.

## 15. Biggest assumptions
1. Owners will pay for ADP pilots  
2. Warm network can seed the first cohort  
3. ADP expands beyond one-shot  
4. Dealmaking identity stays coherent with ADP entry  

## 16. 90-day strategic priorities
1. Founder relationship input → named warm list  
2. Instrumented ADP paid-pilot conversations  
3. Pilot scorecard + proof assets  
4. Attribution P0 PREPARE  
5. Dual-track ADP visibility PREPARE (no full rebuild)

## 17. Risks
- Discussing strategy forever without naming accounts  
- ADP overshoots public identity  
- Another unattributable quarter  
- Treating traffic/likes as success  

## 18. What Joan needs to discuss / amend / lock
**Discuss now:** Does Option B match your commercial reality? Any warm ADP-relevant relationships to seed? Any amendment to exclusions?  
**Do not lock yet** until you accept assumptions + access honesty.  
**Not asking:** tactical PREPARE approvals, EXECUTE, publish, recurring Helena.
`,
);

write(
  '01_BLOCKING_DATA_GAPS.md',
  `# Blocking vs non-blocking data gaps

## Minimum evidence threshold for a strategy recommendation
${pack.blockingGaps.minimumEvidenceThreshold}

## Gap classifications
| Gap | Class | Why |
|-----|-------|-----|
${pack.blockingGaps.gaps.map((g) => `| ${g.gap} | **${g.classification}** | ${g.why}${g.nuance ? ` _${g.nuance}_` : ''} |`).join('\n')}

## Data completeness decision
**${pack.dataCompletenessDecision.answer}** — ${pack.dataCompletenessDecision.explanation}

Founder lock requested: **NO**  
Founder discussion requested: **YES**
`,
);

write(
  '02_LIVE_CONNECTOR_AUDIT.md',
  `# Live connector audit (READ)

| Source | Status |
|--------|--------|
| GA4 live API | ${pack.connectors.ga4} |
| GSC live API | ${pack.connectors.gsc} |
| LinkedIn analytics API | ${pack.connectors.linkedInAnalytics} |
| Webflow CMS API | ${pack.connectors.webflowCms} |
| Public website HTML | ${pack.connectors.publicWebsiteHtml} |
| Stale analytics snapshots | ${pack.connectors.staleAnalyticsSnapshots} |
| Marketing OS Airtable | ${pack.connectors.marketingOsAirtable} |
| GTM Pilot Target List | ${pack.connectors.gtmPilotTargetList} |
| GTM Owner Targets | ${pack.connectors.gtmOwnerTargets} |
| Acquisition Network | ${pack.connectors.acquisitionNetwork} |
| Landing events | ${pack.connectors.landingEvents} |
| Email/outreach attribution | ${pack.connectors.emailAttribution} |

## Wired this phase (READ)
- \`lib/helena-cmo/analytics/cmo-analytics-reader.js\` — snapshot → CMO analytics contract
- Live public HTML inventory script (no CMS token)
- Strategy pack consumes both

## Not created
New Google/LinkedIn/Webflow credentials. No writes.
`,
);

write(
  '03_CURRENT_ANALYTICS.md',
  `# Current analytics (CMO contract)

**Live GA4/GSC:** NOT available. Blockers documented in analytics reader.

## Acquisition (stale snapshot Jun–Sep 2026)
- Sessions: ~553; Direct 277; Organic 19; Paid 0; Email 0
- New vs returning / geography: UNKNOWN in usable grain
- Conclusion: channel activity exists; **not** business outcomes

## Commercial intent
UNKNOWN — snapshot lacks CTA/demo/pilot/form conversion grain.

## Organic discovery (GSC snapshot)
- Homepage 6 clicks / 60 impr
- Branded residences 744 impr / 0 clicks (vanity)
- Insights selection content drives residual organic

## Trend
Cannot compute 30 vs prior 30 without live API.

## Separated conclusions
- **Business outcomes:** none attributable
- **Commercial intent:** not measurable yet
- **Channel activity:** direct-heavy, organic thin, LinkedIn→site UNKNOWN

Machine: \`helena-cmo-strategy-decision-v1.json\` → \`analytics\`
`,
);

write(
  '04_CURRENT_WEBSITE_CONTENT.md',
  `# Current website / content inventory

**Authority: LIVE website** (not repo v9).

Method: public HTML fetch (${siteInv.ok200} HTTP 200 pages). ADP visible anywhere: **${siteInv.adpVisibleOnAny200}**.

## What prospects see TODAY
- Homepage: owner dealmaking — “Find the Best Path for Your Hotel”
- CTAs include Explore your hotel opportunity / talk / request
- \`/opportunity-review\` capture path exists
- Insights: heavy brand/operator selection education; AI visibility article exists; **no ADP product surface**
- No public Brand Explorer / Operator Explorer marketing pages found in crawl
- ES locale homepage exists

## Repo v9 inconsistency
\`public/marketing/dealality-landing-v9.html\` remains selection-forward. **Do not base CMO strategy on v9.** Treat as historical/embed debt.

## Implications
Strategy must keep live dealmaking identity and add dual-track ADP discoverability — not a selection-era rewrite narrative.
`,
);

write(
  '05_COMMERCIAL_PIPELINE_MODEL.md',
  `# Commercial pipeline model

## Status
Named ADP pipeline: **DATA_GAP** (do not fabricate).

## Prefer reuse (do not build giant CRM)
${pack.commercialPipelineModel.preferredReuse.map((x) => `- ${x}`).join('\n')}

## Canonical Helena commercial awareness fields
${pack.commercialPipelineModel.canonicalFields.map((f) => `- \`${f}\``).join('\n')}

## Warmth rule
${pack.commercialPipelineModel.warmthRule}

## Founder relationship input contract
See pack JSON \`founderRelationshipInputContract\`. UI: contract-only this phase unless trivial reuse later.
`,
);

write(
  '06_STRATEGIC_OPTIONS.md',
  `# Strategic options scored

## Recommended: ${recommended.name}
**Score: ${recommended.score}/10**

| Dimension | Score |
|-----------|------:|
${Object.entries(recommended.scores)
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join('\n')}

## Alternative A — ${alternatives[0].name}
Score **${alternatives[0].score}**. ${alternatives[0].whyNotNow}

## Alternative C — ${alternatives[1].name}
Score **${alternatives[1].score}**. ${alternatives[1].whyNotNow}

## Challenge to Phase 6D thesis
6D AMEND thesis is **kept and sharpened into Option B**. Not CONFIRM-as-written (proof/attribution/access elevated). Not REJECT.
`,
);

write(
  '07_RECOMMENDED_STRATEGY.md',
  `# Recommended near-term GTM strategy

## Answers
1. **Compete where:** ${pack.strategicChoice.competeWhere}
2. **Win first:** ${pack.strategicChoice.winFirst}
3. **Lead problem:** ${pack.strategicChoice.leadProblem}
4. **Why Dealality:** ${pack.strategicChoice.whyDealality}
5. **Alternatives:** ${pack.strategicChoice.alternatives.join('; ')}
6. **Entry offer:** ${pack.strategicChoice.entryOffer}
7. **Expansion:** ${pack.strategicChoice.expansionPath}
8. **Will NOT do:**
${pack.strategicChoice.willNotDo.map((x) => `   - ${x}`).join('\n')}
`,
);

write(
  '08_POSITIONING_WORKING_EXPRESSIONS.md',
  `# Working strategic expressions (not final taglines)

## Internal
${pack.positioning.internal}

## Owner
${pack.positioning.owner}

## ADP entry
${pack.positioning.adpEntry}

## Brand / operator
${pack.positioning.brandOperator}
`,
);

write(
  '09_CHANNEL_ROLES.md',
  `# Channel roles

| Channel | Rank | Job |
|---------|------|-----|
${pack.channelRoles.map((c) => `| ${c.channel} | **${c.rank}** | ${c.job} |`).join('\n')}
`,
);

write(
  '10_PROOF_STRATEGY.md',
  `# Proof sequence

1. ${pack.proofSequence.join('\n2. ')}

Do not leap to ROI. First proof asset priority remains: paid pilot + usage narrative + owner one-pager under MARKET_TEST labeling.
`,
);

write(
  '11_90_DAY_SCORECARD.md',
  `# 90-day strategic scorecard (max 10)

| Metric | Business question |
|--------|-------------------|
${pack.scorecard90d.map((m) => `| ${m.metric} | ${m.businessQuestion} |`).join('\n')}

Channel likes/sessions remain diagnostic only.
`,
);

write(
  '12_ASSUMPTION_REGISTER.md',
  `# Assumption register

| Assumption | Why | Evidence | Confidence | Test | Disprove |
|------------|-----|----------|------------|------|----------|
${pack.assumptions
  .map(
    (a) =>
      `| ${a.assumption} | ${a.whyMatters} | ${a.evidence} | ${a.confidence} | ${a.howTest} | ${a.disprove} |`,
  )
  .join('\n')}
`,
);

write(
  '13_FOUNDER_DECISIONS.md',
  `# What Joan needs to discuss / amend / lock

## Discuss (now)
1. Accept or amend **Option B** as near-term GTM choice
2. Confirm whether warm ADP-relevant relationships exist (input contract)
3. Confirm exclusions (what we will NOT prioritize)
4. Agree proof sequence + 90-day scorecard as learning frame

## Amend (if needed)
- Buyer situation / entry offer economics
- Relative weight of dealmaking vs ADP in public dual-track

## Lock (later — not this phase)
Only after discussion + access honesty. This pack does **not** request lock.

## Explicitly not asking
Tactical PREPARE approvals · EXECUTE · publish · recurring Helena · merge/deploy
`,
);

write(
  '14_CONSOLE_UPDATE.md',
  `# Console update (6E)

Executive Assessment separates:
- CURRENT STATE
- STRATEGIC DIAGNOSIS
- STRATEGIC OPTIONS
- HELENA RECOMMENDATION
- UNCERTAINTIES
- FOUNDER DISCUSSION

Primary CTA: **DISCUSS STRATEGY** (not APPROVE STRATEGY).
\`strategyApprovalRequested: false\`
\`primaryCta: DISCUSS_STRATEGY\`
`,
);

write(
  '15_TEST_RESULTS.md',
  `# Test results

Filled by test run after console wiring.
`,
);

fs.writeFileSync(path.join(OUT, 'helena-cmo-strategy-decision-v1.json'), JSON.stringify(pack, null, 2));
console.log(JSON.stringify({ ok: true, out: OUT, recommendation: recommended.id, score: recommended.score, dataDecision: pack.dataCompletenessDecision.answer }, null, 2));
