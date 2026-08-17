/**
 * Mexico Cancún — Travel Infrastructure submarket + city backfill mapping.
 */

import { DEFAULT_CANCUN_KEYWORDS } from "./audit-market-travel-infrastructure.js";

export const MEXICO_CANCUN_TI_SUBMARKET_TARGETS = [
  "Cancún Hotel Zone",
  "Cancún Airport Corridor",
  "Puerto Juárez / Isla Mujeres Ferry Corridor",
  "Riviera Maya / Playa del Carmen",
  "Tulum",
  "Cozumel",
  "Isla Mujeres",
  "Other",
];

/** @type {Array<{ patterns: RegExp[], submarket: string, city?: string|null }>} */
export const MEXICO_CANCUN_TI_RECORD_RULES = [
  {
    patterns: [/canc[uú]n international airport/i, /\bCUN\b/],
    submarket: "Cancún Airport Corridor",
    city: "Cancún",
  },
  {
    patterns: [/tren maya canc[uú]n airport/i, /tren maya.*canc[uú]n/i],
    submarket: "Cancún Airport Corridor",
    city: "Cancún",
  },
  {
    patterns: [/canc[uú]n.*convention/i, /convention.*canc[uú]n/i],
    submarket: "Cancún Hotel Zone",
    city: "Cancún",
  },
  {
    patterns: [/felipe carrillo|tulum international|tren maya.*felipe|tren maya tulum/i, /\bTQO\b/],
    submarket: "Tulum",
    city: "Tulum",
  },
  {
    patterns: [/^tren maya tulum station/i],
    submarket: "Tulum",
    city: "Tulum",
  },
  {
    patterns: [/cozumel international airport/i, /\bCZM\b/],
    submarket: "Cozumel",
    city: "Cozumel",
  },
  {
    patterns: [/cozumel.*ferry/i, /cozumel.*cruise/i, /puerto cozumel/i],
    submarket: "Cozumel",
    city: "Cozumel",
  },
  {
    patterns: [/playa del carmen.*maritime|playa del carmen.*ferry|ultramar.*playa/i, /tren maya playa del carmen/i],
    submarket: "Riviera Maya / Playa del Carmen",
    city: "Playa del Carmen",
  },
  {
    patterns: [/puerto ju[aá]rez.*ferry|puerto ju[aá]rez.*maritime|canc[uú]n maritime terminal/i],
    submarket: "Puerto Juárez / Isla Mujeres Ferry Corridor",
    city: "Cancún",
  },
  {
    patterns: [/gran puerto isla mujeres/i, /isla mujeres.*ferry terminal/i],
    submarket: "Isla Mujeres",
    city: "Isla Mujeres",
  },
];

function norm(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function haystack(record) {
  return [record.name, record.city, record.submarket, record.notes]
    .map(norm)
    .join(" | ");
}

function matchesKeywords(record, keywords) {
  const h = haystack(record);
  return (keywords || DEFAULT_CANCUN_KEYWORDS).some((kw) => h.includes(norm(kw)));
}

function matchRule(record) {
  const h = `${record.name || ""} ${record.city || ""}`;
  for (const rule of MEXICO_CANCUN_TI_RECORD_RULES) {
    if (rule.patterns.some((rx) => rx.test(h))) return rule;
  }
  return null;
}

/**
 * Pick submarket value allowed by Airtable schema.
 * @param {string} target
 * @param {Set<string>} allowed
 */
export function resolveAllowedSubmarket(target, allowed) {
  if (!target) return null;
  if (!allowed || !allowed.size) return target;
  if (allowed.has(target)) return target;
  const partial = [...allowed].find(
    (opt) =>
      opt !== "Other" &&
      (norm(opt).includes(norm(target)) || norm(target).includes(norm(opt)))
  );
  if (partial) return partial;
  if (allowed.has("Other")) return "Other";
  return null;
}

/**
 * @param {object} record
 * @param {object} options
 * @param {Set<string>} [options.allowedSubmarkets]
 */
export function buildTiSubmarketPatch(record, options = {}) {
  if (!matchesKeywords(record, options.keywords)) {
    return { needsUpdate: false, reason: "not_market_matched" };
  }

  const rule = matchRule(record);
  if (!rule) {
    return { needsUpdate: false, reason: "no_mapping_rule", name: record.name };
  }

  const allowed = options.allowedSubmarkets || null;
  const targetSubmarket = resolveAllowedSubmarket(rule.submarket, allowed);
  const currentSubmarket = String(record.submarket || "").trim();
  const currentCity = String(record.city || "").trim();
  const targetCity = rule.city || currentCity;

  const patch = {};
  const changes = [];

  if (targetSubmarket && targetSubmarket !== currentSubmarket) {
    patch.submarket = targetSubmarket;
    changes.push(`submarket: ${currentSubmarket || "(empty)"} → ${targetSubmarket}`);
  } else if (rule.submarket && !targetSubmarket) {
    return {
      needsUpdate: false,
      reason: "submarket_option_missing",
      missingOption: rule.submarket,
      name: record.name,
    };
  }

  if (targetCity && norm(targetCity) !== norm(currentCity)) {
    patch.city = targetCity;
    changes.push(`city: ${currentCity || "(empty)"} → ${targetCity}`);
  }

  if (!Object.keys(patch).length) {
    return { needsUpdate: false, reason: "unchanged", name: record.name };
  }

  if (
    patch.submarket === "Other" &&
    rule.submarket &&
    rule.submarket !== "Other" &&
    options.appendSubmarketNotes !== false
  ) {
    const notes = String(record.notes || "").trim();
    const prefix = `Submarket: ${rule.submarket}.`;
    if (!notes.toLowerCase().includes(rule.submarket.toLowerCase())) {
      patch.notes = notes ? `${notes} ${prefix}` : prefix;
      changes.push(`notes: append "${prefix}"`);
    }
  }

  return {
    needsUpdate: true,
    recordId: record.id,
    name: record.name,
    patch,
    changes,
    targetSubmarket: rule.submarket,
    resolvedSubmarket: targetSubmarket,
  };
}

/**
 * @param {object[]} records
 * @param {object} [options]
 */
export function planMexicoCancunTiSubmarketBackfill(records, options = {}) {
  const country = options.country || "Mexico";
  const scoped = (records || []).filter((r) => norm(r.country) === norm(country));
  const marketMatched = scoped.filter((r) => matchesKeywords(r, options.keywords));

  const results = marketMatched.map((r) =>
    buildTiSubmarketPatch(r, { ...options, allowedSubmarkets: options.allowedSubmarkets })
  );

  const needingUpdate = results.filter((r) => r.needsUpdate);
  const missingOptions = results.filter((r) => r.reason === "submarket_option_missing");
  const noRule = results.filter((r) => r.reason === "no_mapping_rule");

  return {
    scanned: scoped.length,
    marketMatched: marketMatched.length,
    needingUpdate: needingUpdate.length,
    missingSubmarketOptions: missingOptions,
    noMappingRule: noRule,
    updates: needingUpdate,
    samples: needingUpdate.slice(0, 10),
  };
}
