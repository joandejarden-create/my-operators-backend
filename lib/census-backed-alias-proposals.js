/**
 * Census-backed Brand Alias Mapping proposal helpers (read-only).
 */

import { exactMatchKey, normalizeParentCompanyKey } from "./hotel-census/brand-alias-resolve.js";
import {
  CENSUS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
  HOTEL_CENSUS_TABLE,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "./hotel-census/fields.js";
import { shouldIncludeRowForBrandExplorer } from "./hotel-census/census-governance.js";

export { HOTEL_CENSUS_TABLE };

const EXAMPLE_LIMIT = 3;

/** Never propose parent-company strings as affiliation aliases. */
export const BLOCKED_ALIAS_KEYS = new Set(
  [
    "choice hotels",
    "choice hotels international",
    "choice hotels international inc",
    "marriott international",
    "marriott international inc",
    "marriott",
    "hilton",
    "choice",
    "radisson hotel group",
    "hyatt hotels corporation",
    "ihg hotels & resorts",
    "ihg",
    "hyatt hotels corporation",
    "hyatt",
    "accor",
    "accor hotels",
    "accor group",
  ].map((s) => s.toLowerCase())
);

const BY_OPERATOR_SUFFIX =
  /\s+by\s+(Marriott|Hilton|Choice|Radisson|IHG|Hyatt)\s*$/i;
const PAREN_SUFFIX = /\s*\([^)]+\)\s*$/;

/** Known display name → census Affiliation (exact census strings). */
export const KNOWN_CENSUS_AFFILIATION_RULES = [
  {
    id: "radisson_blu_choice",
    match: (name) => exactMatchKey(name) === exactMatchKey("Radisson Blu (Choice)"),
    aliases: ["Radisson Blu by Choice"],
    confidence: "High",
    reason: "Known Choice census naming: Radisson Blu by Choice",
  },
  {
    id: "radisson_individual_choice",
    match: (name) => exactMatchKey(name) === exactMatchKey("Radisson Individual (Choice)"),
    aliases: ["Radisson Individuals by Choice"],
    confidence: "High",
    reason: "Known Choice census naming: Radisson Individuals by Choice (plural)",
  },
  {
    id: "radisson_red_choice",
    match: (name) => exactMatchKey(name) === exactMatchKey("Radisson RED  (Choice)"),
    aliases: ["Radisson RED by Choice"],
    confidence: "High",
    reason: "Known Choice census naming: Radisson RED by Choice",
  },
  {
    id: "radisson_choice",
    match: (name) => exactMatchKey(name) === exactMatchKey("Radisson (Choice)"),
    aliases: ["Radisson by Choice"],
    confidence: "High",
    reason: "Known Choice census naming: Radisson by Choice",
  },
  {
    id: "park_inn_choice",
    match: (name) => exactMatchKey(name) === exactMatchKey("Park Inn by Radisson (Choice)"),
    aliases: ["Park Inn by Choice"],
    confidence: "High",
    reason: "Known Choice census naming: Park Inn by Choice",
  },
  {
    id: "park_plaza_choice",
    match: (name) => exactMatchKey(name) === exactMatchKey("Park Plaza (Choice)"),
    aliases: ["Park Plaza by Choice"],
    confidence: "High",
    reason: "Known Choice census naming: Park Plaza by Choice",
  },
  {
    id: "country_inn_choice",
    match: (name) =>
      exactMatchKey(name) === exactMatchKey("Country Inn & Suites by Radisson (Choice)"),
    aliases: ["Country Inn & Suites by Choice"],
    confidence: "High",
    reason: "Known Choice census naming: Country Inn & Suites by Choice",
  },
  {
    id: "ascend_collection",
    match: (name) => exactMatchKey(name) === exactMatchKey("Ascend Hotel Collection"),
    aliases: ["Ascend Collection"],
    confidence: "High",
    reason: "Known Choice census naming: Ascend Collection",
  },
  {
    id: "comfort_rollup",
    match: (name) => exactMatchKey(name) === exactMatchKey("Comfort Inn & Suites"),
    aliases: ["Comfort Inn", "Comfort", "Comfort Suites"],
    confidence: "Medium",
    reason: "Known Comfort census split affiliations rollup to Comfort Inn & Suites",
  },
  {
    id: "courtyard_marriott",
    match: (name) => exactMatchKey(name) === exactMatchKey("Courtyard by Marriott"),
    aliases: ["Courtyard"],
    confidence: "High",
    reason: "Known Marriott census short name: Courtyard",
  },
  {
    id: "ac_hotels_marriott",
    match: (name) => exactMatchKey(name) === exactMatchKey("AC Hotels by Marriott"),
    aliases: ["AC Hotels", "AC Hotel by Marriott"],
    confidence: "High",
    reason: "Known Marriott census variants for AC Hotels by Marriott",
  },
  {
    id: "city_express_marriott",
    match: (name) => exactMatchKey(name) === exactMatchKey("City Express by Marriott"),
    aliases: ["City Express"],
    confidence: "High",
    reason: "Known Marriott census short name: City Express",
  },
  {
    id: "marriott_hotels_resorts",
    match: (name) => exactMatchKey(name) === exactMatchKey("Marriott Hotels & Resorts"),
    aliases: ["Marriott", "Marriott Hotels"],
    confidence: "Medium",
    reason: "Known Marriott census affiliations for Marriott Hotels & Resorts",
  },
];

export function stripParentheticalLabel(name) {
  return exactMatchKey(String(name || "").replace(PAREN_SUFFIX, "").trim());
}

export function stripByOperatorSuffix(name) {
  return exactMatchKey(String(name || "").replace(BY_OPERATOR_SUFFIX, "").trim());
}

export function proposalParentCompany(mvpParent, censusParent) {
  const census = exactMatchKey(censusParent);
  if (census) return census;
  const p = exactMatchKey(mvpParent);
  if (/choice/i.test(p)) return "Choice Hotels International, Inc.";
  if (/marriott/i.test(p)) return "Marriott International";
  if (/hilton/i.test(p)) return "Hilton";
  if (/hyatt/i.test(p)) return "Hyatt Hotels Corporation";
  if (/ihg/i.test(p)) return "IHG Hotels & Resorts";
  if (/accor/i.test(p)) return "Accor";
  return p;
}

export function parentCompanyPlausible(mvpParent, censusParent) {
  if (!mvpParent || !censusParent) return true;
  const a = normalizeParentCompanyKey(mvpParent);
  const b = normalizeParentCompanyKey(censusParent);
  if (!a || !b) return true;
  if (a === b) return true;
  if (a === "choice hotels" && b.includes("choice")) return true;
  if (b === "choice hotels" && a.includes("choice")) return true;
  if (a.includes("marriott") && b.includes("marriott")) return true;
  if (a.includes("hilton") && b.includes("hilton")) return true;
  if (a.includes("hyatt") && b.includes("hyatt")) return true;
  if (a.includes("ihg") && b.includes("ihg")) return true;
  if (a.includes("accor") && b.includes("accor")) return true;
  return false;
}

/** Census rows exist for brand name but MVP parent does not match census parent. */
export function detectParentCompanyMismatch(brandName, mvpParent, affiliationIndex) {
  const canonical = exactMatchKey(brandName);
  if (!canonical || !mvpParent) return false;

  const variants = new Set([
    canonical,
    stripParentheticalLabel(canonical),
    stripByOperatorSuffix(canonical),
  ]);
  for (const v of variants) {
    if (!v) continue;
    const groups = affiliationIndex.get(v) || [];
    for (const g of groups) {
      if ((g.openHotels > 0 || g.pipelineHotels > 0) && !parentCompanyPlausible(mvpParent, g.parentCompany)) {
        return true;
      }
    }
  }
  return false;
}

function parseNum(v) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : 0;
}

function normalizeStatus(raw) {
  const s = exactMatchKey(raw);
  if (s === "open" || s === STATUS_OPEN) return STATUS_OPEN;
  if (s === "pipeline" || s === STATUS_PIPELINE) return STATUS_PIPELINE;
  return s || "Other";
}

function pushExample(arr, value) {
  const v = exactMatchKey(value);
  if (!v || arr.includes(v)) return;
  if (arr.length < EXAMPLE_LIMIT) arr.push(v);
}

function groupKey(affiliation, parentCompany) {
  return `${affiliation}\u0001${parentCompany}`;
}

/**
 * @param {import("airtable").Record[]} records
 * @param {object} governance
 */
export function buildAffiliationInventory(records, governance) {
  const groups = new Map();

  for (const rec of records) {
    const f = rec.fields || {};
    const affiliation = exactMatchKey(f[CENSUS_FIELDS.affiliation]) || "";
    if (!affiliation || affiliation === CENSUS_INDEPENDENT_AFFILIATION) continue;

    if (
      !shouldIncludeRowForBrandExplorer(
        f,
        CENSUS_FIELDS.includeInBrandExplorer,
        governance.includeInBrandExplorer
      )
    ) {
      continue;
    }

    const parentCompany = exactMatchKey(f[CENSUS_FIELDS.parentCompany]) || "";
    const key = groupKey(affiliation, parentCompany);

    if (!groups.has(key)) {
      groups.set(key, {
        affiliation,
        parentCompany,
        openHotels: 0,
        openKeys: 0,
        pipelineHotels: 0,
        countries: new Set(),
        exampleHotelNames: [],
        exampleCountries: [],
      });
    }

    const g = groups.get(key);
    const status = normalizeStatus(f[CENSUS_FIELDS.status]);
    const rooms = parseNum(f[CENSUS_FIELDS.rooms]);
    const country = exactMatchKey(f[CENSUS_FIELDS.country]);

    if (status === STATUS_OPEN) {
      g.openHotels += 1;
      g.openKeys += rooms;
      if (country) g.countries.add(country);
      pushExample(g.exampleHotelNames, f[CENSUS_FIELDS.name]);
      pushExample(g.exampleCountries, country);
    } else if (status === STATUS_PIPELINE) {
      g.pipelineHotels += 1;
    }
  }

  return [...groups.values()].map((g) => ({
    ...g,
    countryCount: [...g.countries].filter((c) => c && c !== "Unknown").length,
    exampleCountries: [...g.countries].sort().slice(0, EXAMPLE_LIMIT),
  }));
}

export function buildAffiliationIndex(inventory) {
  const byAffiliation = new Map();
  for (const g of inventory) {
    const k = exactMatchKey(g.affiliation);
    if (!byAffiliation.has(k)) byAffiliation.set(k, []);
    byAffiliation.get(k).push(g);
  }
  return byAffiliation;
}

function censusEvidenceFromGroup(g) {
  return {
    openHotels: g.openHotels,
    openKeys: g.openKeys,
    pipelineHotels: g.pipelineHotels,
    countryCount: g.countryCount,
    parentCompanyInCensus: g.parentCompany,
    exampleHotelNames: g.exampleHotelNames,
    exampleCountries: g.exampleCountries,
  };
}

function isBlockedAlias(alias) {
  const a = exactMatchKey(alias);
  if (!a) return true;
  return BLOCKED_ALIAS_KEYS.has(a.toLowerCase());
}

/**
 * @typedef {{ alias: string, reason: string, confidence: string, requiresHumanReview: boolean, matchType: string }} MatchHit
 */

/**
 * Generate candidate alias strings from brand display name (proposal transforms only).
 * @returns {MatchHit[]}
 */
export function generateBrandMatchCandidates(brandName) {
  const canonical = exactMatchKey(brandName);
  if (!canonical) return [];

  const hits = [];
  const seen = new Set();

  function add(alias, reason, confidence, requiresHumanReview, matchType) {
    const a = exactMatchKey(alias);
    if (!a || isBlockedAlias(a)) return;
    const key = `${a}\u0001${matchType}`;
    if (seen.has(key)) return;
    seen.add(key);
    hits.push({ alias: a, reason, confidence, requiresHumanReview, matchType });
  }

  add(canonical, "Exact normalized display name", "High", false, "exact_display");

  const noParen = stripParentheticalLabel(canonical);
  if (noParen && noParen !== canonical) {
    add(
      noParen,
      "Exact match after removing parenthetical label e.g. (Choice)",
      "High",
      true,
      "strip_parenthetical"
    );
  }

  const noByOp = stripByOperatorSuffix(canonical);
  if (noByOp && noByOp !== canonical) {
    add(
      noByOp,
      "Exact match after removing by-operator suffix (Marriott/Hilton/Choice/Radisson)",
      "Medium",
      true,
      "strip_by_operator"
    );
  }

  if (noParen && noByOp !== noParen) {
    const both = stripByOperatorSuffix(noParen);
    if (both && both !== canonical && both !== noParen) {
      add(
        both,
        "Exact match after parenthetical and by-operator strip",
        "Medium",
        true,
        "strip_paren_and_by"
      );
    }
  }

  for (const rule of KNOWN_CENSUS_AFFILIATION_RULES) {
    if (!rule.match(canonical)) continue;
    for (const aff of rule.aliases) {
      add(aff, rule.reason, rule.confidence, true, `known_rule:${rule.id}`);
    }
  }

  return hits;
}

/**
 * @param {string} brandName
 * @param {string} mvpParent
 * @param {ReturnType<typeof buildAffiliationInventory>} inventory
 * @param {Map<string, object[]>} affiliationIndex
 */
export function proposeAliasesForBrand(brandName, mvpParent, inventory, affiliationIndex) {
  const canonical = exactMatchKey(brandName);
  const proposals = [];
  const proposalKeys = new Set();

  const candidates = generateBrandMatchCandidates(brandName);

  for (const cand of candidates) {
    const groups = affiliationIndex.get(cand.alias) || [];
    for (const g of groups) {
      if (g.openHotels === 0 && g.pipelineHotels === 0) continue;
      if (!parentCompanyPlausible(mvpParent, g.parentCompany)) continue;

      const parent = proposalParentCompany(mvpParent, g.parentCompany);
      const rowKey = `${canonical}\u0001${cand.alias}\u0001${parent}`;
      if (proposalKeys.has(rowKey)) continue;
      proposalKeys.add(rowKey);

      proposals.push({
        "Canonical Brand Name": canonical,
        "Alias / Source Brand Name": cand.alias,
        "Parent Company": parent,
        Active: false,
        "Match Confidence": cand.confidence,
        Notes: "Proposal only — set Approved true in reviewed JSON to seed",
        "Proposal Reason": `${cand.matchType}: ${cand.reason}`,
        "Requires Human Review": cand.requiresHumanReview,
        Approved: false,
        censusEvidence: censusEvidenceFromGroup(g),
      });
    }
  }

  return proposals.sort(
    (a, b) =>
      (b.censusEvidence?.openHotels || 0) - (a.censusEvidence?.openHotels || 0) ||
      (b.censusEvidence?.openKeys || 0) - (a.censusEvidence?.openKeys || 0)
  );
}

/**
 * @param {Map<string, { record: object, active: boolean }>} aliasKeyIndex
 */
export function existingAliasStatusForProposal(aliasKeyIndex, canonical, alias, parent) {
  const key = [canonical, alias, parent].map((s) => exactMatchKey(s)).join("\u0001");
  const found = aliasKeyIndex.get(key);
  if (!found) return "missing";
  return found.active ? "alreadyActive" : "alreadyInactive";
}

export function attachExistingAliasStatus(proposals, aliasKeyIndex) {
  return proposals.map((p) => ({
    ...p,
    existingAliasStatus: existingAliasStatusForProposal(
      aliasKeyIndex,
      p["Canonical Brand Name"],
      p["Alias / Source Brand Name"],
      p["Parent Company"]
    ),
  }));
}

/** @returns {Map<string, { active: string[], inactive: string[] }>} */
export function buildAliasesByCanonical(aliasRecords) {
  const byCanonical = new Map();
  for (const rec of aliasRecords) {
    const f = rec.fields || {};
    const canonical = exactMatchKey(f["Canonical Brand Name"]);
    const alias = exactMatchKey(f["Alias / Source Brand Name"]);
    if (!canonical || !alias) continue;
    if (!byCanonical.has(canonical)) {
      byCanonical.set(canonical, { active: [], inactive: [] });
    }
    const bucket = byCanonical.get(canonical);
    const v = f.Active ?? f.active;
    const active =
      v === false
        ? false
        : ["yes", "true", "1", "active"].includes(String(v ?? "").trim().toLowerCase());
    if (active) bucket.active.push(alias);
    else bucket.inactive.push(alias);
  }
  return byCanonical;
}

export function buildAliasKeyIndex(aliasRecords) {
  const index = new Map();
  for (const rec of aliasRecords) {
    const f = rec.fields || {};
    const canonical = exactMatchKey(f["Canonical Brand Name"]);
    const alias = exactMatchKey(f["Alias / Source Brand Name"]);
    const parent = exactMatchKey(f["Parent Company"]);
    const key = [canonical, alias, parent].join("\u0001");
    const v = f.Active ?? f.active;
    const active =
      v === false
        ? false
        : ["yes", "true", "1", "active"].includes(String(v ?? "").trim().toLowerCase());
    index.set(key, { record: rec, active });
  }
  return index;
}

export function csvEscape(val) {
  const s = val == null ? "" : String(val);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function proposalToCsvRow(p) {
  const ev = p.censusEvidence || {};
  return {
    "Canonical Brand Name": p["Canonical Brand Name"],
    "Alias / Source Brand Name": p["Alias / Source Brand Name"],
    "Parent Company": p["Parent Company"],
    "Open Hotels": ev.openHotels ?? "",
    "Open Keys": ev.openKeys ?? "",
    Countries: (ev.exampleCountries || []).join("; "),
    "Match Confidence": p["Match Confidence"],
    "Proposal Reason": p["Proposal Reason"],
    "Requires Human Review": p["Requires Human Review"] ? "yes" : "no",
    Approved: p.Approved ? "yes" : "no",
    "Existing Alias Status": p.existingAliasStatus || "",
    Notes: p.Notes || "",
  };
}
