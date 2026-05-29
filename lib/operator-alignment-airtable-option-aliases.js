/**
 * Alias → live Airtable label (validated at runtime) + canonical scoring category.
 * Aliases are matching keys (normalized); live labels must exist in Airtable when writing.
 */

import { normalizeOptionKey, buildLiveOptionLookup } from "./operator-alignment-airtable-options-loader.js";

/** @type {Record<string, { canonical: string, aliases: string[] }>} */
export const OAS_OPTION_ALIAS_GROUPS = {
  management_structures: {
    canonical: "third_party_management",
    aliases: [
      "full third-party management",
      "full third party management",
      "third-party management",
      "third party management",
      "management agreement",
      "hotel management",
      "operator management",
      "franchise with third-party operator",
      "franchise with third party operator",
      "franchise support",
      "franchise + third-party operator",
    ],
  },
  brand_agreement_franchise: {
    canonical: "franchise",
    aliases: ["franchise", "franchise only", "franchise agreement"],
  },
  operating_model_third_party: {
    canonical: "third_party_managed",
    aliases: [
      "third-party managed",
      "third party managed",
      "third-party management",
      "third party management",
    ],
  },
  operating_model_owner: {
    canonical: "owner_operated",
    aliases: ["owner-operated", "owner operated", "self-managed", "self managed"],
  },
  service_full_mgmt: {
    canonical: "full_hotel_management",
    aliases: ["full hotel management", "full management", "hotel management"],
  },
  service_preopening: {
    canonical: "pre_opening_planning",
    aliases: [
      "pre-opening planning",
      "pre opening planning",
      "pre-opening",
      "pre-opening support",
      "pre-opening / transition support",
    ],
  },
  service_opening_transition: {
    canonical: "opening_transition_support",
    aliases: ["opening / transition support", "opening transition support", "opening support"],
  },
  service_revenue_mgmt: {
    canonical: "revenue_management",
    aliases: ["revenue management", "revenue management support", "rm"],
  },
  service_owner_reporting: {
    canonical: "owner_reporting",
    aliases: ["owner reporting", "owner reporting support", "monthly operating review"],
  },
  service_brand_compliance: {
    canonical: "brand_compliance_support",
    aliases: ["brand compliance support", "brand compliance", "brand standards support"],
  },
  reporting_monthly: {
    canonical: "monthly_operating_review",
    aliases: ["monthly operating review", "monthly", "monthly review", "monthly operating"],
  },
  reporting_institutional: {
    canonical: "institutional_reporting",
    aliases: ["institutional reporting", "institutional", "investor reporting"],
  },
  market_cancun: {
    canonical: "market_cancun",
    aliases: ["cancun", "cancún", "cancun mexico"],
  },
  market_sao_paulo: {
    canonical: "market_sao_paulo",
    aliases: ["sao paulo", "são paulo", "sao paolo"],
  },
  country_mexico: {
    canonical: "country_mexico",
    aliases: ["mexico", "méxico", "mx"],
  },
  chain_upper_midscale: {
    canonical: "upper_midscale",
    aliases: ["upper midscale", "upper-midscale", "upper mid scale"],
  },
  chain_independent: {
    canonical: "independent",
    aliases: ["independent", "independent / boutique", "boutique", "independent boutique"],
  },
  scope_full_management: {
    canonical: "full_management",
    aliases: ["full management", "full hotel management"],
  },
  scope_preopening: {
    canonical: "pre_opening_support",
    aliases: ["pre-opening support", "pre opening support"],
  },
  preopening_capability_advanced: {
    canonical: "preopening_advanced",
    aliases: ["advanced", "strong", "advanced pre-opening"],
  },
  preopening_capability_standard: {
    canonical: "preopening_standard",
    aliases: ["standard", "moderate"],
  },
  unknown_value: {
    canonical: "unknown",
    aliases: ["unknown", "not provided", "n/a", "na", "not applicable"],
  },
};

/** alias normalized key → canonical */
const _aliasToCanonical = new Map();
for (const group of Object.values(OAS_OPTION_ALIAS_GROUPS)) {
  for (const a of group.aliases) {
    _aliasToCanonical.set(normalizeOptionKey(a), group.canonical);
  }
}

export function getCanonicalCategory(labelOrAlias) {
  const k = normalizeOptionKey(labelOrAlias);
  if (!k) return null;
  if (_aliasToCanonical.has(k)) return _aliasToCanonical.get(k);
  return k.replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "") || null;
}

/**
 * Build alias map for a field's live options: normalized alias → exact live label.
 */
export function buildAliasToLiveMap(allowedOptions) {
  const lookup = buildLiveOptionLookup(allowedOptions);
  const out = {};

  for (const liveLabel of allowedOptions || []) {
    const nk = normalizeOptionKey(liveLabel);
    out[nk] = liveLabel;
  }

  for (const group of Object.values(OAS_OPTION_ALIAS_GROUPS)) {
    for (const alias of group.aliases) {
      const ak = normalizeOptionKey(alias);
      if (out[ak]) continue;
      for (const liveLabel of allowedOptions || []) {
        const lk = normalizeOptionKey(liveLabel);
        if (lk === ak || lk.includes(ak) || ak.includes(lk)) {
          out[ak] = liveLabel;
          break;
        }
      }
    }
  }

  return out;
}

/**
 * Resolve alias string to live label if possible.
 */
export function resolveAliasToLiveLabel(value, allowedOptions) {
  const map = buildAliasToLiveMap(allowedOptions);
  const nk = normalizeOptionKey(value);
  return map[nk] || null;
}

/**
 * Canonical sets for scoring overlap (two lists match if canonical intersects).
 */
export function labelsToCanonicalSet(labels) {
  const out = new Set();
  for (const l of labels || []) {
    const c = getCanonicalCategory(l);
    if (c) out.add(c);
  }
  return out;
}

export function canonicalOverlapScore(dealCanonicals, operatorCanonicals, partialScore = 42) {
  const d = dealCanonicals instanceof Set ? dealCanonicals : labelsToCanonicalSet(dealCanonicals);
  const o = operatorCanonicals instanceof Set ? operatorCanonicals : labelsToCanonicalSet(operatorCanonicals);
  if (d.size === 0 || o.size === 0) return null;
  let hit = 0;
  for (const x of d) if (o.has(x)) hit += 1;
  if (hit === 0) {
    for (const x of d) {
      for (const y of o) {
        if (x.includes(y) || y.includes(x)) {
          hit += 0.5;
          break;
        }
      }
    }
  }
  if (hit === 0) return partialScore;
  const ratio = hit / d.size;
  return Math.min(100, Math.max(0, Math.round((40 + ratio * 60) * 10) / 10));
}
