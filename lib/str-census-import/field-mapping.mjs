/**
 * Heuristic mapping between STR Excel columns and Hotel Census Airtable fields.
 *
 * Official geography mapping (no separate STR Market / STR Submarket columns):
 *   Excel STR Market    → Hotel Census `Market`
 *   Excel STR Submarket → Hotel Census `Submarket`
 */

import { normalizeKey } from "./normalize.mjs";

/** Canonical Hotel Census targets for STR geography import. */
export const CENSUS_STR_GEOGRAPHY_FIELDS = {
  market: "Market",
  submarket: "Submarket",
};

/** @typedef {{ name: string, id: string, type: string, excelRole: string|null, matchScore: number, matchReason: string }} FieldInventoryRow */

const EXCEL_ROLE_RULES = [
  {
    role: "strId",
    patterns: [
      /^str\s*(number|id|#|no\.?)?$/i,
      /^stridentifier$/i,
      /^property\s*id$/i,
      /^str\s*property\s*id$/i,
    ],
    keywords: ["str number", "str id", "str identifier", "property id"],
  },
  {
    role: "city",
    patterns: [/^city$/i, /^property\s*city$/i],
    keywords: ["city"],
  },
  {
    role: "hotelName",
    patterns: [/^name$/i, /^hotel\s*name$/i, /^property\s*name$/i],
    keywords: ["hotel name", "property name"],
  },
  {
    role: "country",
    patterns: [/^country$/i, /^property\s*country$/i],
    keywords: ["country"],
  },
  {
    role: "strMarket",
    patterns: [/^str\s*market(\s*name)?$/i, /^market$/i],
    keywords: ["str market", "market name"],
    exclude: [/submarket/i],
  },
  {
    role: "strSubmarket",
    patterns: [/^str\s*submarket(\s*name)?$/i, /^submarket$/i],
    keywords: ["str submarket", "submarket name"],
  },
];

function fieldNameNorm(name) {
  return normalizeKey(name).replace(/\s+/g, " ");
}

function scoreFieldForRole(fieldName, rule) {
  const n = fieldNameNorm(fieldName);
  if (!n) return { score: 0, reason: "" };

  if (rule.exclude?.some((re) => re.test(fieldName))) {
    return { score: 0, reason: "excluded by rule" };
  }

  for (const re of rule.patterns) {
    if (re.test(fieldName.trim())) {
      return { score: 100, reason: `pattern ${re}` };
    }
  }

  for (const kw of rule.keywords) {
    if (n === kw.replace(/\s+/g, " ") || n.includes(kw)) {
      return { score: 75, reason: `keyword "${kw}"` };
    }
  }

  return { score: 0, reason: "" };
}

function inventoryHasField(inventory, fieldName) {
  return (inventory || []).some((row) => row.name === fieldName);
}

/**
 * Resolve official census field for STR geography roles.
 * Prefers exact `Market` / `Submarket`; does not create or expect separate STR columns.
 */
function resolveStrGeographyField(inventory, officialName, excelRole, byRole) {
  if (inventoryHasField(inventory, officialName)) {
    return officialName;
  }
  return byRole[excelRole]?.name || null;
}

/**
 * @param {Array<{ name: string, id: string, type: string }>} airtableFields
 * @returns {FieldInventoryRow[]}
 */
export function inventoryAirtableFields(airtableFields) {
  return (airtableFields || []).map((f) => {
    let best = { role: null, score: 0, reason: "" };
    for (const rule of EXCEL_ROLE_RULES) {
      const { score, reason } = scoreFieldForRole(f.name, rule);
      if (score > best.score) {
        best = { role: rule.role, score, reason };
      }
    }
    return {
      name: f.name,
      id: f.id,
      type: f.type,
      excelRole: best.score >= 70 ? best.role : null,
      matchScore: best.score,
      matchReason: best.reason || (best.score ? "" : "no Excel mapping"),
    };
  });
}

/**
 * Among STR ID candidates, pick the field with the most populated values.
 * @param {FieldInventoryRow[]} inventory
 * @param {Array<{ fields: object }>} records
 */
export function pickBestStrIdField(inventory, records) {
  const candidates = (inventory || []).filter(
    (r) => r.excelRole === "strId" && r.matchScore >= 70
  );
  if (!candidates.length) return { field: null, populatedCount: 0, candidates: [] };

  let bestName = null;
  let bestPop = -1;
  for (const c of candidates) {
    let pop = 0;
    for (const rec of records) {
      const v = fieldValue(rec.fields, c.name);
      if (String(v ?? "").trim() !== "" && String(v) !== "0") pop++;
    }
    if (pop > bestPop) {
      bestPop = pop;
      bestName = c.name;
    }
  }
  return {
    field: bestName,
    populatedCount: bestPop,
    candidates: candidates.map((c) => c.name),
  };
}

/**
 * Pick best census field per Excel role from inventory rows.
 * @param {FieldInventoryRow[]} inventory
 * @param {Array<{ fields: object }>} [records] optional — used to pick populated STR ID field
 */
export function recommendCensusFieldMapping(inventory, records = []) {
  const byRole = {};
  for (const row of inventory) {
    if (!row.excelRole) continue;
    const prev = byRole[row.excelRole];
    if (!prev || row.matchScore > prev.matchScore) {
      byRole[row.excelRole] = row;
    }
  }

  const strIdPick = records.length ? pickBestStrIdField(inventory, records) : null;
  const mapping = {
    strId: strIdPick?.field || byRole.strId?.name || null,
    city: byRole.city?.name || null,
    hotelName: byRole.hotelName?.name || null,
    country: byRole.country?.name || null,
    strMarket: resolveStrGeographyField(
      inventory,
      CENSUS_STR_GEOGRAPHY_FIELDS.market,
      "strMarket",
      byRole
    ),
    strSubmarket: resolveStrGeographyField(
      inventory,
      CENSUS_STR_GEOGRAPHY_FIELDS.submarket,
      "strSubmarket",
      byRole
    ),
  };

  const strMarketExists = !!mapping.strMarket;
  const strSubmarketExists = !!mapping.strSubmarket;

  const recommendations = [];

  if (!mapping.strId) {
    recommendations.push(
      "BLOCKER: No clear STR ID field on Hotel Census. Add or identify STR Number / STR ID before import."
    );
  } else {
    recommendations.push(`Use "${mapping.strId}" as the census STR ID key for matching.`);
    if (strIdPick && strIdPick.candidates.length > 1) {
      recommendations.push(
        `STR ID candidates: ${strIdPick.candidates.join(", ")} — selected "${mapping.strId}" (${strIdPick.populatedCount} populated rows).`
      );
    }
    if (strIdPick && strIdPick.populatedCount === 0) {
      recommendations.push(
        "BLOCKER: STR ID field detected but no values populated in census. Matching will fall back to Name+City+Country only."
      );
    }
  }

  if (strMarketExists) {
    recommendations.push(
      `Excel STR Market → Hotel Census "${mapping.strMarket}" (official STR market field; do not create a separate STR Market column).`
    );
  } else {
    recommendations.push(
      `BLOCKER: Hotel Census "${CENSUS_STR_GEOGRAPHY_FIELDS.market}" field not found — required for STR market import.`
    );
  }

  if (strSubmarketExists) {
    recommendations.push(
      `Excel STR Submarket → Hotel Census "${mapping.strSubmarket}" (official STR submarket field; do not create a separate STR Submarket column).`
    );
  } else {
    recommendations.push(
      `BLOCKER: Hotel Census "${CENSUS_STR_GEOGRAPHY_FIELDS.submarket}" field not found — required for STR submarket import.`
    );
  }

  recommendations.push(
    "Suggested import scope (review before apply): enrich Market and Submarket from STR Excel first; only update City/Country/Hotel Name when census values are blank or flagged Needs Review."
  );
  recommendations.push(
    "Resolve duplicate STR IDs and Name+City+Country conflicts in census and Excel before any Airtable update."
  );

  return {
    mapping,
    strIdPick,
    strMarketExists,
    strSubmarketExists,
    recommendations,
  };
}

export function fieldValue(fields, fieldName) {
  if (!fieldName || !fields) return "";
  const v = fields[fieldName];
  if (v == null) return "";
  if (Array.isArray(v)) return v.join(", ");
  if (typeof v === "object" && v !== null) {
    if ("name" in v) return String(v.name);
    return JSON.stringify(v);
  }
  return String(v);
}
