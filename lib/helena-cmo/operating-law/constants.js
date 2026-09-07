/**
 * Helena CMO Operating Law v1.0 — canonical constants (D1–D5 + governance).
 * Source of truth for machine enforcement. Recurring Helena OFF. EXECUTE OFF.
 */
export const LAW_META = Object.freeze({
  version: '1.0.0',
  id: 'helena-cmo-operating-law-v1',
  effectiveDate: '2026-09-07',
  status: 'ACTIVE',
  recurringHelenaEnabled: false,
  executeEnabled: false,
  autonomousPrioritizationEnabled: false,
});

export const FOUNDER_LOCKS = Object.freeze({
  D1: 'DEC-2026-09-07-D1-POSITIONING-DIRECTION',
  D2: 'DEC-2026-09-07-D2-MARKETPLACE-ROLE',
  D3: 'DEC-2026-09-07-D3-DUAL-TRACK-GTM',
  D4: 'DEC-2026-09-07-D4-ICP-V1',
  D5: 'DEC-2026-09-07-D5-ADP-PRICING-AUTHORITY',
  CARD_TRIAD: 'DEC-2026-09-07-CARD-TRIAD',
  CTA_FAMILIES: 'DEC-2026-09-07-CTA-FAMILIES',
  KC001: 'DEC-2026-09-07-KC001',
  APPROVAL_RULES: 'DEC-2026-09-07-APPROVAL-RULES',
  TARGET_OS: 'DEC-2026-08-08-TARGET-OS',
  CLEAR: 'DEC-2026-08-08-CLEAR',
});

export const AUTHORITY_RANK = Object.freeze([
  'CURRENT_FOUNDER_LOCK',
  'CURRENT_COMMERCIAL_EVIDENCE',
  'CURRENT_PRODUCT_REALITY',
  'CURRENT_APPROVED_MARKETING_DECISIONS',
  'HISTORICAL_STRATEGY',
  'ANALYTICAL_RECOMMENDATION',
  'HYPOTHESIS',
]);

export const POSITIONING = Object.freeze({
  territory: 'hotel dealmaking in plain owner language',
  workingExpressionNotTagline:
    'Dealality helps hotel owners make and manage better hotel deals.',
  messagingOrder: ['CLEAR_FIRST', 'STAKES_SECOND', 'PRODUCT_THIRD'],
  hotelDealmakingMeans:
    "the owner's process of evaluating, structuring, comparing and managing important hotel decisions and counterparties",
  hotelDealmakingDoesNotMean: Object.freeze([
    'brokerage',
    'property sales',
    'transaction intermediation',
    'OTA/distribution',
    'sales engine',
    'public deal-listing marketplace',
  ]),
  publicTaglineCategory: 'TESTING_OPEN',
  forbiddenCompanyFrames: Object.freeze(['AI company', 'AI marketing platform']),
  cardTriad: Object.freeze(['ONE HOTEL.', 'BETTER DECISIONS.', 'MORE VALUE.']),
  reduceFriction: 'TESTING',
});

export const MARKETPLACE = Object.freeze({
  role: 'IMPORTANT_UNDERLYING_PLATFORM_ECOSYSTEM_LAYER',
  notCompanyIdentity: true,
  notPublicCategory: true,
  doNotLeadMessagingByDefault: true,
  ownerControlOverridesLiquidity: true,
  forbiddenImplications: Object.freeze([
    'public listing',
    'open confidential data',
    'brokerage',
    'OTA',
    'automatic opportunity exposure',
  ]),
});

export const GTM_TRACKS = Object.freeze({
  ADP: 'ADP',
  OWNER_DEALMAKING: 'OWNER_DEALMAKING',
});

export const ICP_LABELS = Object.freeze({
  'ICP-O1': Object.freeze({
    name: 'Owner / Developer — Live Dealmaking Decision',
    priority: 'STRATEGIC_P1',
    entry: 'OWNER_DECISION_WORKFLOW',
    track: 'OWNER_DEALMAKING',
  }),
  'ICP-O2': Object.freeze({
    name: 'Owner / Ownership Group — ADP / Operating Hotel Opportunity',
    priority: 'STRATEGIC_P1_ACTIVE_COMMERCIAL',
    entry: 'ADP',
    track: 'ADP',
  }),
  'ICP-B1': Object.freeze({
    name: 'Brand — Active Commercial / ADP Opportunity',
    priority: 'ACTIVE_SECONDARY_COMMERCIAL',
    entry: 'ADP_BRAND_AI',
    track: 'ADP',
  }),
  'ICP-OP1': Object.freeze({
    name: 'Operator — Active Commercial / ADP Opportunity',
    priority: 'ACTIVE_SECONDARY_COMMERCIAL',
    entry: 'ADP',
    track: 'ADP',
  }),
  'ICP-A1': Object.freeze({
    name: 'Advisor / Lawyer / Consultant — Owner Amplifier',
    priority: 'P1_CHANNEL_AMPLIFIER',
    entry: 'OWNER_OPT_IN_AMPLIFIER',
    track: null,
  }),
});

export const ADP_MARKET_TEST = Object.freeze({
  monthlyUsd: 3500,
  hotels: 2,
  perHotelArithmeticUsd: 1750,
  pilotMonths: 6,
  totalUsd: 21000,
  pricingClass: 'MARKET_TEST_DEFAULT',
  labels: Object.freeze(['MARKET-TEST PRICING', 'CURRENT PILOT PRICING']),
});

export const PRICING_CLASSES = Object.freeze([
  'HISTORICAL',
  'ILLUSTRATIVE',
  'MARKET_TEST_DEFAULT',
  'APPROVED_STANDARD',
  'CUSTOM_FOUNDER_REVIEW',
  'ENTERPRISE_TBD',
]);

export const PRODUCT_MATURITY = Object.freeze([
  'LIVE_SELLABLE',
  'PILOT_READY',
  'DEMO_READY',
  'IN_DEVELOPMENT',
  'CONCEPT',
  'RETIRED',
]);

export const CTA_FAMILIES = Object.freeze({
  SEE_HOW_IT_WORKS: 'SEE HOW IT WORKS',
  REQUEST_A_DEMO: 'REQUEST A DEMO',
  DISCUSS_A_PILOT: 'DISCUSS A PILOT',
});

export const CTA_RETIRED = Object.freeze(['REQUEST BETA ACCESS', 'Request Beta Access']);

export const CLAIM_CLASSES = Object.freeze(['GREEN', 'YELLOW', 'RED']);

export const APPROVAL_STATUSES = Object.freeze([
  'DRAFT',
  'PENDING',
  'APPROVED',
  'APPROVED_WITH_CHANGES',
  'REJECTED',
  'HOLD',
]);

export const ALLOWED_ACTIONS = Object.freeze([
  'OBSERVE',
  'THINK',
  'INVENT',
  'PREPARE',
]);

export const PROHIBITED_ACTIONS = Object.freeze([
  'PUBLISH',
  'POST',
  'SCHEDULE',
  'SEND',
  'LAUNCH',
  'EXECUTE',
  'DEPLOY',
  'MERGE_PRODUCTION',
  'SELF_APPROVE',
  'CHANGE_LIVE_PRICING',
  'CHANGE_LIVE_WEBSITE',
  'ENABLE_RECURRING_HELENA',
]);
