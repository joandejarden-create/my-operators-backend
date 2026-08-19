/**
 * Operator-only alias overlay. Separate from Brand runtime-alias-overlay-phase2b.
 * Longest-match. Short parent names require operating-context.
 */

import { normalizeMatchKey } from "../normalize-entities.js";
import { OPERATOR_AI_UNIVERSE } from "./universe.js";

export const OPERATOR_ALIAS_OVERLAY_VERSION = "operator_ai_alias_overlay_v1";

const OPERATING_CONTEXT_RE =
  /\b(operat(?:e|es|ed|ing|or|ors)|management[- ]company|hotel management|third[- ]party|hma\b|managed by|management agreement|operating partner|who should operate|management platform)\b/i;

const SHORT_PARENT_KEYS = new Set(["marriott", "hilton", "ihg", "he", "brh", "ghl"]);

/** Aliases that must never resolve (too generic or brand-only). */
export const BLOCKED_OPERATOR_ALIASES = Object.freeze([
  "HE",
  "Marriott Bonvoy",
  "Hilton Honors",
  "IHG One Rewards",
  "a Marriott hotel",
  "Marriott hotel",
  "Hilton hotel",
  "Arbor",
]);

const ALIASES_BY_SLUG = Object.freeze({
  "marriott-international-managed": [
    "Marriott International (Managed)",
    "Marriott International",
    "Managed by Marriott",
    "MxM",
    "Marriott management",
    "Marriott",
  ],
  "ihg-managed": [
    "IHG Hotels & Resorts (Managed)",
    "IHG Hotels & Resorts",
    "InterContinental Hotels Group",
    "IHG management",
    "IHG",
  ],
  "hilton-managed": [
    "Hilton (Managed)",
    "Hilton Worldwide",
    "Hilton Management Services",
    "Hilton management",
    "Hilton",
  ],
  "aimbridge-latam": [
    "Aimbridge Hospitality (LATAM)",
    "Aimbridge LATAM",
    "Aimbridge Hospitality",
    "Aimbridge",
  ],
  "hotel-equities-cala": [
    "Hotel Equities (CALA)",
    "Hotel Equities CALA",
    "Hotel Equities",
  ],
  "arbor-lodging-cala": [
    "Arbor Lodging (CALA)",
    "Arbor Lodging CALA",
    "Arbor Lodging",
  ],
  "ghl-hoteles": [
    "GHL Hoteles (GHL Holding)",
    "GHL Hoteles",
    "GHL Hotels",
    "GHL Holding",
    "GHL",
  ],
  "brittain-resorts-hotels": [
    "Brittain Resorts & Hotels (BRH)",
    "Brittain Resorts & Hotels",
    "Brittain Resorts",
    "BRH",
  ],
  "remington-hospitality-cala": [
    "Remington Hospitality (CALA)",
    "Remington Hospitality CALA",
    "Remington Hospitality Caribbean & Latin America",
    "Remington Hospitality",
    "Remington Hotels",
  ],
});

/** Short forms that need operating context before Presence. */
const CONTEXT_REQUIRED_ALIASES = Object.freeze([
  "Marriott",
  "Hilton",
  "IHG",
  "GHL",
  "BRH",
  "MxM",
]);

export const OPERATOR_AMBIGUITY_LIST = Object.freeze([
  {
    surface: "Marriott",
    risk: "Brand company vs managed-operator vs Autograph/Tribute brand family",
    rule: "Require operating-context or longer alias (Marriott International, Managed by Marriott, MxM)",
  },
  {
    surface: "Hilton",
    risk: "Parent company vs hotel brand vs operator",
    rule: "Require operating-context or Hilton (Managed) / Hilton Management Services",
  },
  {
    surface: "IHG",
    risk: "Brand company vs operator",
    rule: "Require operating-context or IHG Hotels & Resorts",
  },
  {
    surface: "Aimbridge",
    risk: "Global Aimbridge Hospitality vs LATAM monitored scope",
    rule: "Resolve to canonical Aimbridge Hospitality (LATAM); keep MONITORED_SCOPE=LATAM",
  },
  {
    surface: "Hotel Equities / HE",
    risk: "Bare HE is blocked; CALA vs enterprise",
    rule: "Hotel Equities (+ optional CALA). Never alias HE",
  },
  {
    surface: "Arbor",
    risk: "Ordinary-language tree/canopy collision",
    rule: "Arbor Lodging only",
  },
  {
    surface: "GHL",
    risk: "GHL vs GHL Hoteles vs GHL Holding",
    rule: "Longest match to GHL Hoteles (GHL Holding)",
  },
  {
    surface: "Brittain / BRH",
    risk: "Brittain Resorts vs Brittain Resorts & Hotels",
    rule: "Resolve to Brittain Resorts & Hotels (BRH); BRH needs operating context",
  },
  {
    surface: "Remington",
    risk: "Remington Hospitality vs unrelated Remington brands (firearms, appliances, etc.)",
    rule: "Require Remington Hospitality or longer alias; bare Remington is not aliased",
  },
  {
    surface: "Remington Hospitality",
    risk: "Enterprise U.S. scale vs CALA monitored scope",
    rule: "Resolve to Remington Hospitality (CALA); keep MONITORED_SCOPE=CALA",
  },
]);

export function buildOperatorEntities() {
  return OPERATOR_AI_UNIVERSE.map((row) => ({
    id: row.canonicalId,
    name: row.canonicalName,
    entityType: "operator",
    aliases: [...(ALIASES_BY_SLUG[row.slug] || [])],
    firstPartyDomains: [row.domain, row.parentDomain].filter(Boolean),
    parentCompany: row.parentPlatform,
    isParentCompanyLabel: false,
    monitoredScope: row.monitoredScope,
    operatorLens: row.operatorLens,
    slug: row.slug,
    sourceSystem: "operator_ai_universe_v1",
  }));
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Word-boundary only when the alias starts/ends with a word char. Parenthetical aliases must still match. */
function aliasPattern(label) {
  const escaped = escapeRegExp(label);
  const leading = /^\w/.test(label) ? "\\b" : "(?<!\\w)";
  const trailing = /\w$/.test(label) ? "\\b" : "(?!\\w)";
  return new RegExp(`${leading}${escaped}${trailing}`, "gi");
}

function spanNeedsOperatingContext(rawMention) {
  const key = normalizeMatchKey(rawMention);
  if (SHORT_PARENT_KEYS.has(key)) return true;
  return CONTEXT_REQUIRED_ALIASES.some((a) => normalizeMatchKey(a) === key);
}

function isBlockedAlias(rawMention) {
  const key = normalizeMatchKey(rawMention);
  return BLOCKED_OPERATOR_ALIASES.some((a) => normalizeMatchKey(a) === key);
}

/**
 * Operator-only span finder. Does not use Brand findEntitySpans because
 * Brand's bare-parent blocklist drops "Marriott International" / "IHG Hotels & Resorts".
 */
export function findOperatorSpans(text) {
  const source = String(text || "");
  if (!source) return [];
  const aliasRows = [];
  for (const entity of buildOperatorEntities()) {
    const labels = [entity.name, ...(entity.aliases || [])].filter(Boolean);
    for (const label of labels) {
      if (isBlockedAlias(label)) continue;
      aliasRows.push({ label, entity });
    }
  }
  aliasRows.sort((a, b) => b.label.length - a.label.length);
  const occupied = [];
  const spans = [];
  for (const row of aliasRows) {
    const re = aliasPattern(row.label);
    let m;
    while ((m = re.exec(source))) {
      const start = m.index;
      const end = start + m[0].length;
      if (occupied.some((s) => start < s.end && end > s.start)) continue;
      if (spanNeedsOperatingContext(m[0]) && !OPERATING_CONTEXT_RE.test(source)) continue;
      occupied.push({ start, end });
      spans.push({
        start,
        end,
        rawMention: m[0],
        matchedAlias: row.label,
        entity: row.entity,
      });
    }
  }
  return spans.sort((a, b) => a.start - b.start);
}

export function hasOperatingContext(text) {
  return OPERATING_CONTEXT_RE.test(String(text || ""));
}
