import { CLAIM_CLASSES } from './constants.js';

export const ADP_ALLOWED_CLAIMS = Object.freeze([
  'understand how a hotel appears in AI-generated answers',
  'track visibility over time',
  'identify demand-segment representation',
  'compare competitor visibility',
  'examine source/narrative patterns',
  'identify possible actions',
  'build longitudinal observation history',
]);

export const ADP_FORBIDDEN_CLAIMS = Object.freeze([
  'guaranteed ranking improvement',
  'guaranteed bookings',
  'guaranteed direct-booking lift',
  'guaranteed revenue impact',
  'guaranteed ROI',
]);

const RED_PATTERNS = [
  /(?<!no\s)(?<!not\s)(?<!without\s)guaranteed?\s+(roi|bookings?|ranking|revenue|direct[- ]?booking)/i,
  /(?<!not\s)proven\s+roi/i,
  /pays\s+for\s+itself/i,
  /fabricated|fake\s+customer|invented\s+pricing/i,
  /full\s+hotel\s+lifecycle/i,
  /manages?\s+the\s+(entire|full)\s+hotel\s+lifecycle/i,
];

const YELLOW_PATTERNS = [
  /outperform|better than|superior|market leader/i,
  /\d+%\s+(more|lift|increase)/i,
  /roi|return on investment/i,
  /enterprise[- ]ready/i,
];

const BLOCKED_COMPANY_FRAMES = [
  /ai[- ]powered\s+marketing\s+platform/i,
  /dealality\s+is\s+an?\s+ai\s+company/i,
  /ai\s+marketing\s+platform\s+for\s+hotels/i,
];

const BLOCKED_MARKETPLACE_IDENTITY = [
  /global\s+marketplace\s+for\s+hotel\s+deals/i,
  /dealality\s+is\s+(the\s+)?(a\s+)?hotel\s+marketplace/i,
  /dealality\s+is\s+the\s+marketplace/i,
];

export function classifyClaimText(text = '') {
  const t = String(text);
  for (const re of BLOCKED_COMPANY_FRAMES) {
    if (re.test(t)) {
      return {
        claimClass: 'RED',
        action: 'BLOCK',
        reason: 'Violates D1/KC-001 — company must not be framed as AI company / AI marketing platform',
      };
    }
  }
  for (const re of BLOCKED_MARKETPLACE_IDENTITY) {
    if (re.test(t)) {
      return {
        claimClass: 'RED',
        action: 'BLOCK_OR_REWRITE',
        reason: 'Violates D2 — marketplace is not company identity/public category',
      };
    }
  }
  for (const re of RED_PATTERNS) {
    if (re.test(t)) {
      return {
        claimClass: 'RED',
        action: 'BLOCK',
        reason: 'Red claim class — requires explicit founder approval + evidence; typically forbidden',
      };
    }
  }
  for (const re of YELLOW_PATTERNS) {
    if (re.test(t)) {
      return {
        claimClass: 'YELLOW',
        action: 'EVIDENCE_OR_JOAN_REVIEW',
        reason: 'Yellow claim — evidence check or Joan review required',
      };
    }
  }
  return { claimClass: 'GREEN', action: 'ALLOW_IF_SUPPORTED', reason: 'No red/yellow pattern matched' };
}

export function gateAdpRoiClaim(text = '') {
  const t = String(text);
  if (/\b(no|not|without|never)\b.{0,20}\bguarantee/i.test(t)) {
    return { allowed: true, action: 'ALLOW', reason: 'Negated guarantee language' };
  }
  if (/guarantee/i.test(t) && /(booking|roi|revenue|ranking|direct)/i.test(t)) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: 'ADP must not promise guaranteed bookings/ROI/ranking/revenue',
    };
  }
  if (/investment[- ]hurdle/i.test(t) && /proven\s+roi|guarantee/i.test(t)) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: 'Investment hurdle is economic context, not ROI proof',
    };
  }
  if (/pays\s+for\s+itself|(?<!\bnot\s)proven\s+roi/i.test(t)) {
    return {
      allowed: false,
      action: 'BLOCK',
      reason: 'Investment-hurdle illustration is not ROI proof',
    };
  }
  return { allowed: true, action: 'ALLOW', reason: 'No forbidden ADP ROI pattern' };
}

export function gateLifecycleClaim(text = '') {
  const t = String(text);
  if (/full\s+hotel\s+lifecycle|entire\s+hotel\s+lifecycle|manages?\s+the\s+(full|entire)\s+lifecycle/i.test(t)) {
    return {
      allowed: false,
      action: 'REWRITE',
      reason:
        'External claims must describe what Dealality can do TODAY — full lifecycle is ambition, not current sellable claim',
      allowedFraming:
        'Explain long-term ambition separately from current capabilities; do not claim full lifecycle management today',
    };
  }
  return { allowed: true, action: 'ALLOW', reason: 'No full-lifecycle overclaim' };
}

export function gateMarketplaceIdentityClaim(text = '') {
  const t = String(text);
  for (const re of BLOCKED_MARKETPLACE_IDENTITY) {
    if (re.test(t)) {
      return {
        allowed: false,
        action: 'BLOCK_OR_REWRITE',
        reason: 'Do not define Dealality solely as a marketplace',
        allowedFraming:
          'Marketplace/matching/participation is an underlying platform layer supporting owner-driven hotel dealmaking',
      };
    }
  }
  if (/is\s+dealality\s+a\s+marketplace/i.test(t)) {
    return {
      allowed: true,
      action: 'EXPLAIN_LAYER',
      reason: 'Answer may explain underlying layer without equating company identity to marketplace',
      allowedFraming:
        'Dealality is not simply a hotel marketplace; marketplace functionality is an important underlying platform layer',
    };
  }
  return { allowed: true, action: 'ALLOW', reason: 'No marketplace-identity overclaim' };
}

export function normalizeClaimClass(c) {
  const v = String(c || '').toUpperCase();
  if (!CLAIM_CLASSES.includes(v)) throw new Error(`Invalid claim class: ${c}`);
  return v;
}
